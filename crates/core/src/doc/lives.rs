use std::sync::Arc;

use anyhow::Result;
use reblessive::tree::Stk;

use super::IgnoreError;
use crate::catalog::{Permission, SubscriptionDefinition};
use crate::ctx::{Context, MutableContext};
use crate::dbs::{Action, Notification, Options, Statement};
use crate::doc::{CursorDoc, Document};
use crate::err::Error;
use crate::expr::FlowResultExt as _;
use crate::expr::paths::{AC, RD, TK};
use crate::val::Value;

impl Document {
	/// Processes any LIVE SELECT statements which
	/// have been defined for the table which this
	/// record belongs to. This functions loops
	/// through the live queries and processes them
	/// all within the currently running transaction.
	pub(super) async fn process_table_lives(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<()> {
		// Check import
		if opt.import {
			return Ok(());
		}

		// Check if we can send notifications
		let Some(chn) = opt.sender.as_ref() else {
			// no channel so nothing to do.
			return Ok(());
		};

		// Check if changed
		if !self.changed() {
			return Ok(());
		}

		// Get all live queries for this table
		let live_subscriptions = self.lv(ctx, opt).await?;

		// If there are no live queries, we can skip the rest of the function
		if live_subscriptions.is_empty() {
			return Ok(());
		}

		// Loop through all index statements
		for live_subscription in live_subscriptions.iter() {
			// Get the event action
			let met = if stm.is_delete() {
				Value::from("DELETE")
			} else if self.is_new() {
				Value::from("CREATE")
			} else {
				Value::from("UPDATE")
			};
			// Get the record if of this docunent
			let rid = self
				.id
				.clone()
				.ok_or_else(|| {
					Error::unreachable("Processing live query for record without a Record ID")
				})
				.map_err(anyhow::Error::new)?;
			// Get the current and initial docs
			// These are only used for EVENTS, so they should not be reduced
			let current = self.current.doc.as_arc();
			let initial = self.initial.doc.as_arc();
			// Ensure that a session exists on the LIVE query
			let sess = match live_subscription.session.as_ref() {
				Some(v) => v,
				None => continue,
			};
			// Ensure that auth info exists on the LIVE query
			let auth = match live_subscription.auth.clone() {
				Some(v) => v,
				None => continue,
			};
			// We need to create a new context which we will
			// use for processing this LIVE query statement.
			// This ensures that we are using the session
			// of the user who created the LIVE query.
			let mut lqctx = MutableContext::background();
			// Set the current transaction on the new LIVE
			// query context to prevent unreachable behaviour
			// and ensure that queries can be executed.
			lqctx.set_transaction(ctx.tx());
			// Add the session params to this LIVE query, so
			// that queries can use these within field
			// projections and WHERE clauses.
			lqctx.add_value("access", sess.pick(AC.as_ref()).into());
			lqctx.add_value("auth", sess.pick(RD.as_ref()).into());
			lqctx.add_value("token", sess.pick(TK.as_ref()).into());
			lqctx.add_value("session", sess.clone().into());
			// Add $before, $after, $value, and $event params
			// to this LIVE query so the user can use these
			// within field projections and WHERE clauses.
			lqctx.add_value("event", met.into());
			lqctx.add_value("value", current.clone());
			lqctx.add_value("after", current);
			lqctx.add_value("before", initial);
			// Freeze the context
			let lqctx = lqctx.freeze();
			// We need to create a new options which we will
			// use for processing this LIVE query statement.
			// This ensures that we are using the auth data
			// of the user who created the LIVE query.
			let lqopt = opt.new_with_perms(true).with_auth(Arc::from(auth));

			// Get the document to check against and to return based on lq context
			// We need to clone the document as we will potentially modify it with computed fields
			// The outcome for every computed field can be different based on the context of the
			// user
			let mut doc = match (self.check_reduction_required(&lqopt)?, stm.is_delete()) {
				(true, true) => {
					self.compute_reduced_target(stk, &lqctx, &lqopt, &self.initial).await?
				}
				(true, false) => {
					self.compute_reduced_target(stk, &lqctx, &lqopt, &self.current).await?
				}
				(false, true) => self.initial.clone(),
				(false, false) => self.current.clone(),
			};

			if let Ok(rid) = self.id() {
				let fields = self.fd(ctx, opt).await?;
				Document::computed_fields_inner(
					stk,
					ctx,
					opt,
					rid.as_ref(),
					fields.as_ref(),
					&mut doc,
				)
				.await?;
			};

			// First of all, let's check to see if the WHERE
			// clause of the LIVE query is matched by this
			// document. If it is then we can continue.
			match self.lq_check(stk, &lqctx, &lqopt, live_subscription, &doc).await {
				Err(IgnoreError::Ignore) => continue,
				Err(IgnoreError::Error(e)) => return Err(e),
				Ok(_) => (),
			}
			// Secondly, let's check to see if any PERMISSIONS
			// clause for this table allows this document to
			// be viewed by the user who created this LIVE
			// query. If it does, then we can continue.
			match self.lq_allow(stk, &lqctx, &lqopt).await {
				Err(IgnoreError::Ignore) => continue,
				Err(IgnoreError::Error(e)) => return Err(e),
				Ok(_) => (),
			}
			// Let's check what type of statement
			// caused this LIVE query to run, and obtain
			// the relevant result.
			let (action, mut result) = if stm.is_delete() {
				// Prepare a DELETE notification
				if opt.id()? == live_subscription.node {
					// Ensure futures are run
					// Output the full document before any changes were applied
					let result = doc.doc.as_ref().clone();
					(Action::Delete, result)
				} else {
					// TODO: Send to message broker
					continue;
				}
			} else if self.is_new() {
				// Prepare a CREATE notification
				if opt.id()? == live_subscription.node {
					// An error ignore here is about livequery not the query which invoked the
					// livequery trigger. So we should catch the ignore and skip this entry in this
					// case.
					let result =
						match self.lq_pluck(stk, &lqctx, &lqopt, live_subscription, &doc).await {
							Err(IgnoreError::Ignore) => continue,
							Err(IgnoreError::Error(e)) => return Err(e),
							Ok(x) => x,
						};
					(Action::Create, result)
				} else {
					// TODO: Send to message broker
					continue;
				}
			} else {
				// Prepare a UPDATE notification
				if opt.id()? == live_subscription.node {
					// An error ignore here is about livequery not the query which invoked the
					// livequery trigger. So we should catch the ignore and skip this entry in this
					// case.
					let result =
						match self.lq_pluck(stk, &lqctx, &lqopt, live_subscription, &doc).await {
							Err(IgnoreError::Ignore) => continue,
							Err(IgnoreError::Error(e)) => return Err(e),
							Ok(x) => x,
						};
					(Action::Update, result)
				} else {
					// TODO: Send to message broker
					continue;
				}
			};

			// Process any potential `FETCH` clause on the live statement
			if let Some(fetchs) = &live_subscription.fetch {
				let mut idioms = Vec::with_capacity(fetchs.len());
				for fetch in fetchs.iter() {
					fetch.compute(stk, &lqctx, &lqopt, &mut idioms).await?;
				}
				for i in &idioms {
					stk.run(|stk| result.fetch(stk, &lqctx, &lqopt, i)).await?;
				}
			}

			// Send the notification
			let res = chn
				.send(Notification {
					id: live_subscription.id.into(),
					action,
					record: Value::RecordId(rid.as_ref().clone()),
					result,
				})
				.await;

			if res.is_err() {
				// channel was closed, that means a transaction probably failed.
				// just return as nothing can be send.
				return Ok(());
			}
		}
		// Carry on
		Ok(())
	}
	/// Check the WHERE clause for a LIVE query
	async fn lq_check(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		live_subscription: &SubscriptionDefinition,
		doc: &CursorDoc,
	) -> Result<(), IgnoreError> {
		// Check where condition
		if let Some(cond) = live_subscription.cond.as_ref() {
			// Check if the expression is truthy
			if !stk
				.run(|stk| cond.compute(stk, ctx, opt, Some(doc)))
				.await
				.catch_return()?
				.is_truthy()
			{
				// Ignore this document
				return Err(IgnoreError::Ignore);
			}
		}
		// Carry on
		Ok(())
	}
	/// Check any PERRMISSIONS for a LIVE query
	async fn lq_allow(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
	) -> Result<(), IgnoreError> {
		// Should we run permissions checks?
		// Live queries are always
		if opt.check_perms(crate::iam::Action::View)? {
			// Get the table
			let tb = self.tb(ctx, opt).await?;
			// Process the table permissions
			match &tb.permissions.select {
				Permission::None => return Err(IgnoreError::Ignore),
				Permission::Full => return Ok(()),
				Permission::Specific(e) => {
					// Retrieve the document to check permissions against
					let doc = &self.current;

					// Disable permissions
					let opt = &opt.new_with_perms(false);
					// Process the PERMISSION clause
					if !stk
						.run(|stk| e.compute(stk, ctx, opt, Some(doc)))
						.await
						.catch_return()
						.is_ok_and(|x| x.is_truthy())
					{
						return Err(IgnoreError::Ignore);
					}
				}
			}
		}
		// Carry on
		Ok(())
	}

	async fn lq_pluck(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		live_subscription: &SubscriptionDefinition,
		doc: &CursorDoc,
	) -> Result<Value, IgnoreError> {
		live_subscription
			.fields
			.compute(stk, ctx, opt, Some(doc), false)
			.await
			.map_err(IgnoreError::from)
	}
}
