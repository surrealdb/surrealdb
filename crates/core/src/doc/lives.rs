use crate::ctx::{Context, MutableContext};
use crate::dbs::Action;
use crate::dbs::Notification;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::CursorDoc;
use crate::doc::Document;
use crate::err::Error;
use crate::expr::paths::AC;
use crate::expr::paths::META;
use crate::expr::paths::RD;
use crate::expr::paths::TK;
use crate::expr::permission::Permission;
use crate::expr::{FlowResultExt as _, Value};
use anyhow::Result;
use reblessive::tree::Stk;
use std::sync::Arc;

use super::IgnoreError;

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
		let lvs = self.lv(ctx, opt).await?;
		// Loop through all index statements
		for lv in lvs.iter() {
			// Create a new statement
			let lq = Statement::from(lv);
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
					Error::Unreachable(
						"Processing live query for record without a Record ID".to_owned(),
					)
				})
				.map_err(anyhow::Error::new)?;
			// Get the current and initial docs
			let current = self.current.doc.as_arc();
			let initial = self.initial.doc.as_arc();
			// Check if this is a delete statement
			let doc = match stm.is_delete() {
				true => &self.initial,
				false => &self.current,
			};
			// Ensure that a session exists on the LIVE query
			let sess = match lv.session.as_ref() {
				Some(v) => v,
				None => continue,
			};
			// Ensure that auth info exists on the LIVE query
			let auth = match lv.auth.clone() {
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
			// We need to create a new options which we will
			// use for processing this LIVE query statement.
			// This ensures that we are using the auth data
			// of the user who created the LIVE query.
			let lqopt = opt.new_with_perms(true).with_auth(Arc::from(auth));
			// First of all, let's check to see if the WHERE
			// clause of the LIVE query is matched by this
			// document. If it is then we can continue.
			let lqctx = lqctx.freeze();
			match self.lq_check(stk, &lqctx, &lqopt, &lq, doc).await {
				Err(IgnoreError::Ignore) => continue,
				Err(IgnoreError::Error(e)) => return Err(e),
				Ok(_) => (),
			}
			// Secondly, let's check to see if any PERMISSIONS
			// clause for this table allows this document to
			// be viewed by the user who created this LIVE
			// query. If it does, then we can continue.
			match self.lq_allow(stk, &lqctx, &lqopt, &lq, doc).await {
				Err(IgnoreError::Ignore) => continue,
				Err(IgnoreError::Error(e)) => return Err(e),
				Ok(_) => (),
			}
			// Let's check what type of statement
			// caused this LIVE query to run, and obtain
			// the relevant result.
			let (action, mut result) = if stm.is_delete() {
				// Prepare a DELETE notification
				if opt.id()? == lv.node.0 {
					// Ensure futures are run
					let lqopt: &Options = &lqopt.new_with_futures(true);
					// Output the full document before any changes were applied
					let mut result = doc
						.doc
						.as_ref()
						.compute(stk, &lqctx, lqopt, Some(doc))
						.await
						.catch_return()?;
					// Remove metadata fields on output
					result.del(stk, &lqctx, lqopt, &*META).await?;
					(Action::Delete, result)
				} else {
					// TODO: Send to message broker
					continue;
				}
			} else if self.is_new() {
				// Prepare a CREATE notification
				if opt.id()? == lv.node.0 {
					// An error ignore here is about livequery not the query which invoked the
					// livequery trigger. So we should catch the ignore and skip this entry in this
					// case.
					let result = match self.pluck(stk, &lqctx, &lqopt, &lq).await {
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
				if opt.id()? == lv.node.0 {
					// An error ignore here is about livequery not the query which invoked the
					// livequery trigger. So we should catch the ignore and skip this entry in this
					// case.
					let result = match self.pluck(stk, &lqctx, &lqopt, &lq).await {
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
			if let Some(fetchs) = &lv.fetch {
				let mut idioms = Vec::with_capacity(fetchs.0.len());
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
					id: lv.id,
					action,
					record: Value::Thing(rid.as_ref().clone()),
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
		stm: &Statement<'_>,
		doc: &CursorDoc,
	) -> Result<(), IgnoreError> {
		// Check where condition
		if let Some(cond) = stm.cond() {
			// Check if the expression is truthy
			if !cond.compute(stk, ctx, opt, Some(doc)).await.catch_return()?.is_truthy() {
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
		stm: &Statement<'_>,
		doc: &CursorDoc,
	) -> Result<(), IgnoreError> {
		// Should we run permissions checks?
		if opt.check_perms(stm.into())? {
			// Get the table
			let tb = self.tb(ctx, opt).await?;
			// Process the table permissions
			match &tb.permissions.select {
				Permission::None => return Err(IgnoreError::Ignore),
				Permission::Full => return Ok(()),
				Permission::Specific(e) => {
					// Disable permissions
					let opt = &opt.new_with_perms(false);
					// Process the PERMISSION clause
					if !e
						.compute(stk, ctx, opt, Some(doc))
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
}
