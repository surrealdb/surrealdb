use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use async_channel::Sender;
use futures::future::try_join_all;
use reblessive::TreeStack;
use reblessive::tree::Stk;

use super::IgnoreError;
use crate::catalog::{Permission, SubscriptionDefinition, SubscriptionFields};
use crate::ctx::{Context, FrozenContext};
use crate::dbs::{MessageBroker, Options, Statement};
use crate::doc::{CursorDoc, Document};
use crate::err::Error;
use crate::expr::FlowResultExt as _;
use crate::expr::paths::{AC, ID, RD, TK};
use crate::kvs::Transaction;
use crate::types::{PublicAction, PublicNotification};
use crate::val::{Value, convert_value_to_public_value};

impl Document {
	/// Processes any LIVE SELECT statements which
	/// have been defined for the table which this
	/// record belongs to. This functions loops
	/// through the live queries and processes them
	/// all within the currently running transaction.
	pub(super) async fn process_table_lives(
		&mut self,
		_stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<()> {
		// Check import
		if opt.import {
			return Ok(());
		}

		// Check if we can send notifications
		if opt.broker.is_none() {
			// no sender, so nothing to do.
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

		// Get the event action
		let (met, is_delete): (Arc<Value>, _) = if stm.is_delete() {
			(Value::from("DELETE").into(), true)
		} else if self.is_new() {
			(Value::from("CREATE").into(), false)
		} else {
			(Value::from("UPDATE").into(), false)
		};

		// Get the current and initial docs
		// These are only used for EVENTS, so they should not be reduced
		let initial = self.initial.doc.as_arc();
		let current = self.current.doc.as_arc();

		// Move self to a shared reference
		let doc: &Self = &*self;

		let mut tasks = Vec::with_capacity(live_subscriptions.len());
		// Loop through all index statements
		for live_subscription in live_subscriptions.iter() {
			// We need to create a new options which we will
			// use for processing this LIVE query statement.
			// This ensures that we are using the auth data
			// of the user who created the LIVE query.
			let lqopt = opt.new_with_perms(true);
			let (met, current, initial) = (met.clone(), current.clone(), initial.clone());
			tasks.push(async move {
				let mut stack = TreeStack::new();
				stack
					.enter(|stk| {
						doc.lq_compute(
							stk,
							live_subscription.clone(),
							lqopt,
							ctx.tx(),
							(met, initial, current),
							is_delete,
						)
					})
					.finish()
					.await
			});
		}
		// Run the tasks concurrently
		try_join_all(tasks).await?;
		// Carry on
		Ok(())
	}

	async fn lq_compute(
		&self,
		stk: &mut Stk,
		live_subscription: SubscriptionDefinition,
		opt: Options,
		tx: Arc<Transaction>,
		(met, initial, current): (Arc<Value>, Arc<Value>, Arc<Value>),
		is_delete: bool,
	) -> Result<()> {
		// Ensure that a session exists on the LIVE query
		let sess = match live_subscription.session.as_ref() {
			Some(v) => v,
			None => return Ok(()),
		};
		// Ensure that auth info exists on the LIVE query
		let auth = match live_subscription.auth.clone() {
			Some(v) => v,
			None => return Ok(()),
		};
		let opt = opt.with_auth(auth.into());

		let Some(sender) = opt.broker.as_ref() else {
			return Ok(());
		};

		// Get the record id of this document
		let rid = self
			.id
			.clone()
			.ok_or_else(|| {
				Error::unreachable("Processing live query for record without a Record ID")
			})
			.map_err(anyhow::Error::new)?;

		// We need to create a new context which we will
		// use for processing this LIVE query statement.
		// This ensures that we are using the session
		// of the user who created the LIVE query.
		let mut ctx = Context::background();
		// Set the current transaction on the new LIVE
		// query context to prevent unreachable behaviour
		// and ensure that queries can be executed.
		ctx.set_transaction(tx);
		// Add the session params to this LIVE query, so
		// that queries can use these within field
		// projections and WHERE clauses.
		ctx.add_value("access", sess.pick(AC.as_ref()).into());
		ctx.add_value("auth", sess.pick(RD.as_ref()).into());
		ctx.add_value("token", sess.pick(TK.as_ref()).into());
		ctx.add_value("session", sess.clone().into());
		// Add $before, $after, $value, and $event params
		// to this LIVE query so the user can use these
		// within field projections and WHERE clauses.
		ctx.add_value("event", met);
		ctx.add_value("value", current.clone());
		ctx.add_value("after", current);
		ctx.add_value("before", initial);
		// Add the variables to the context
		ctx.add_values(live_subscription.vars.clone());
		// Freeze the context
		let ctx = ctx.freeze();

		// Get the document to check against and to return based on lq context
		// We need to clone the document as we will potentially modify it with computed fields
		// The outcome for every computed field can be different based on the context of the
		// user
		let mut doc = match (self.check_reduction_required(&opt)?, is_delete) {
			(true, true) => self.compute_reduced_target(stk, &ctx, &opt, &self.initial).await?,
			(true, false) => self.compute_reduced_target(stk, &ctx, &opt, &self.current).await?,
			(false, true) => self.initial.clone(),
			(false, false) => self.current.clone(),
		};

		if let Ok(rid) = self.id() {
			let fields = self.fd(&ctx, &opt).await?;
			Document::computed_fields_inner(
				stk,
				&ctx,
				&opt,
				rid.as_ref(),
				fields.as_ref(),
				&mut doc,
			)
			.await?;
		};

		// First of all, let's check to see if the WHERE
		// clause of the LIVE query is matched by this
		// document. If it is then we can continue.
		match self.lq_check(stk, &ctx, &opt, &live_subscription, &doc).await {
			Err(IgnoreError::Ignore) => return Ok(()),
			Err(IgnoreError::Error(e)) => return Err(e),
			Ok(_) => (),
		}
		// Secondly, let's check to see if any PERMISSIONS
		// clause for this table allows this document to
		// be viewed by the user who created this LIVE
		// query. If it does, then we can continue.
		match self.lq_allow(stk, &ctx, &opt).await {
			Err(IgnoreError::Ignore) => return Ok(()),
			Err(IgnoreError::Error(e)) => return Err(e),
			Ok(_) => (),
		}
		if !sender.can_be_sent(&opt, &live_subscription)? {
			return Ok(());
		}
		// Let's check what type of statement
		// caused this LIVE query to run, and obtain
		// the relevant result.
		let (action, mut result) = match live_subscription.fields {
			SubscriptionFields::Diff => {
				// DIFF mode: return JSON patch operations instead of full document
				if is_delete {
					// For DELETE: compute diff from initial document to empty object
					let operations = self.initial.doc.as_ref().diff(&Value::None);
					let result = Value::Array(
						operations.into_iter().map(|op| Value::Object(op.into_object())).collect(),
					);
					(PublicAction::Delete, result)
				} else if self.is_new() {
					// For CREATE: compute diff from empty object to current document
					let operations = Value::None.diff(doc.doc.as_ref());
					let result = Value::Array(
						operations.into_iter().map(|op| Value::Object(op.into_object())).collect(),
					);
					(PublicAction::Create, result)
				} else {
					// For UPDATE: compute diff from initial to current document
					let operations = self.initial.doc.as_ref().diff(doc.doc.as_ref());
					let result = Value::Array(
						operations.into_iter().map(|op| Value::Object(op.into_object())).collect(),
					);
					(PublicAction::Update, result)
				}
			}
			SubscriptionFields::Select(x) => {
				if is_delete {
					// Prepare a DELETE notification
					// An error ignore here is about livequery not the query which invoked the
					// livequery trigger. So we should catch the ignore and skip this entry in this
					// case.
					let result = match x
						.compute(stk, &ctx, &opt, Some(&doc))
						.await
						.map_err(IgnoreError::from)
					{
						Err(IgnoreError::Ignore) => return Ok(()),
						Err(IgnoreError::Error(e)) => return Err(e),
						Ok(x) => x,
					};
					(PublicAction::Delete, result)
				} else if self.is_new() {
					// Prepare a CREATE notification
					// An error ignore here is about livequery not the query which invoked the
					// livequery trigger. So we should catch the ignore and skip this entry in this
					// case.
					let result = match x
						.compute(stk, &ctx, &opt, Some(&doc))
						.await
						.map_err(IgnoreError::from)
					{
						Err(IgnoreError::Ignore) => return Ok(()),
						Err(IgnoreError::Error(e)) => return Err(e),
						Ok(x) => x,
					};
					(PublicAction::Create, result)
				} else {
					// Prepare a UPDATE notification
					// An error ignore here is about livequery not the query which invoked the
					// livequery trigger. So we should catch the ignore and skip this entry in this
					// case.
					let result = match x
						.compute(stk, &ctx, &opt, Some(&doc))
						.await
						.map_err(IgnoreError::from)
					{
						Err(IgnoreError::Ignore) => return Ok(()),
						Err(IgnoreError::Error(e)) => return Err(e),
						Ok(x) => x,
					};
					(PublicAction::Update, result)
				}
			}
		};

		// Process any potential `FETCH` clause on the live statement
		if let Some(fetchs) = live_subscription.fetch {
			let mut idioms = Vec::with_capacity(fetchs.len());
			for fetch in fetchs.iter() {
				fetch.compute(stk, &ctx, &opt, &mut idioms).await?;
			}
			for i in &idioms {
				stk.run(|stk| result.fetch(stk, &ctx, &opt, i)).await?;
			}
		}

		// Extract the session ID from the session value
		let session_id = match sess.pick(ID.as_ref()) {
			Value::Uuid(uuid) => Some(uuid.into()),
			Value::String(s) => s.parse::<crate::val::Uuid>().ok().map(|uuid| uuid.into()),
			_ => None,
		};

		let notification = PublicNotification::new(
			live_subscription.id.into(),
			session_id,
			action,
			convert_value_to_public_value(Value::RecordId(rid.as_ref().clone()))?,
			convert_value_to_public_value(result)?,
		);

		// Send the notification
		sender.send(notification).await;

		Ok(())
	}

	/// Check the WHERE clause for a LIVE query
	async fn lq_check(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
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
		ctx: &FrozenContext,
		opt: &Options,
	) -> Result<(), IgnoreError> {
		// Should we run permissions checks?
		// Live queries are always
		if opt.check_perms(crate::iam::Action::View)? {
			// Get the table
			let tb = self.tb().await?;
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
}

#[derive(Clone, Debug)]
pub(crate) struct DefaultBroker(Sender<PublicNotification>);

impl DefaultBroker {
	pub(crate) fn new(sender: Sender<PublicNotification>) -> Arc<Self> {
		Arc::new(Self(sender))
	}
}
impl MessageBroker for DefaultBroker {
	fn can_be_sent(&self, opt: &Options, subscription: &SubscriptionDefinition) -> Result<bool> {
		Ok(opt.id() == subscription.node)
	}

	fn send(
		&self,
		notification: PublicNotification,
	) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
		Box::pin(async move {
			// If there is an error, we can just ignore it,
			// as it means that the channel was closed.
			let _ = self.0.send(notification).await;
		})
	}
}
