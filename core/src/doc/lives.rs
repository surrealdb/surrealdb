use crate::ctx::Context;
use crate::dbs::Notification;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::{Action, Transaction};
use crate::doc::CursorDoc;
use crate::doc::Document;
use crate::err::Error;
use crate::fflags::FFLAGS;
use crate::sql::paths::META;
use crate::sql::paths::SC;
use crate::sql::paths::SD;
use crate::sql::paths::TK;
use crate::sql::permission::Permission;
use crate::sql::statements::LiveStatement;
use crate::sql::Value;
use channel::Sender;
use std::ops::Deref;
use std::sync::Arc;
use uuid::Uuid;

impl<'a> Document<'a> {
	pub async fn lives(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check if changed
		if !self.changed() {
			return Ok(());
		}
		// Under the new mechanism, live query notifications only come from polling the change feed
		// This check can be moved up the call stack, as this entire method will become unnecessary
		if FFLAGS.change_feed_live_queries.enabled() {
			return Ok(());
		}
		// Check if we can send notifications
		if let Some(chn) = &opt.sender {
			// Loop through all index statements
			let lq_stms = self.lv(opt, txn).await?;
			let borrows = lq_stms.iter().collect::<Vec<_>>();
			self.check_lqs_and_send_notifications(opt, stm, txn, borrows.as_slice(), chn).await?;
		}
		// Carry on
		Ok(())
	}

	/// Check the WHERE clause for a LIVE query
	async fn lq_check(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
		doc: &CursorDoc<'_>,
	) -> Result<(), Error> {
		// Check where condition
		if let Some(cond) = stm.conds() {
			// Check if the expression is truthy
			if !cond.compute(ctx, opt, txn, Some(doc)).await?.is_truthy() {
				// Ignore this document
				return Err(Error::Ignore);
			}
		}
		// Carry on
		Ok(())
	}
	/// Check any PERRMISSIONS for a LIVE query
	async fn lq_allow(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
		doc: &CursorDoc<'_>,
	) -> Result<(), Error> {
		// Should we run permissions checks?
		if opt.check_perms(stm.into()) {
			// Get the table
			let tb = self.tb(opt, txn).await?;
			// Process the table permissions
			match &tb.permissions.select {
				Permission::None => return Err(Error::Ignore),
				Permission::Full => return Ok(()),
				Permission::Specific(e) => {
					// Disable permissions
					let opt = &opt.new_with_perms(false);
					// Process the PERMISSION clause
					if !e.compute(ctx, opt, txn, Some(doc)).await?.is_truthy() {
						return Err(Error::Ignore);
					}
				}
			}
		}
		// Carry on
		Ok(())
	}

	/// Process live query for notifications
	pub(crate) async fn check_lqs_and_send_notifications(
		&self,
		opt: &Options,
		stm: &Statement<'_>,
		txn: &Transaction,
		live_statements: &[&LiveStatement],
		sender: &Sender<Notification>,
	) -> Result<(), Error> {
		trace!(
			"Called check_lqs_and_send_notifications with {} live statements",
			live_statements.len()
		);
		for lv in live_statements {
			// Create a new statement
			let lq = Statement::from(*lv);
			// Get the event action
			let met = if stm.is_delete() {
				Value::from("DELETE")
			} else if self.is_new() {
				Value::from("CREATE")
			} else {
				Value::from("UPDATE")
			};
			// Check if this is a delete statement
			let doc = match stm.is_delete() {
				true => &self.initial,
				false => &self.current,
			};
			// Ensure that a session exists on the LIVE query
			let sess = match lv.session.as_ref() {
				Some(v) => v,
				None => {
					trace!("live query did not have a session, skipping");
					continue;
				}
			};
			// Ensure that auth info exists on the LIVE query
			let auth = match lv.auth.clone() {
				Some(v) => v,
				None => {
					trace!("live query did not have auth info, skipping");
					continue;
				}
			};
			// We need to create a new context which we will
			// use for processing this LIVE query statement.
			// This ensures that we are using the session
			// of the user who created the LIVE query.
			let mut lqctx = Context::background();
			lqctx.add_value("auth", sess.pick(SD.as_ref()));
			lqctx.add_value("scope", sess.pick(SC.as_ref()));
			lqctx.add_value("token", sess.pick(TK.as_ref()));
			lqctx.add_value("session", sess);
			// We need to create a new options which we will
			// use for processing this LIVE query statement.
			// This ensures that we are using the auth data
			// of the user who created the LIVE query.
			let lqopt = opt.new_with_perms(true).with_auth(Arc::from(auth));
			// Add $before, $after, $value, and $event params
			// to this LIVE query so that user can use these
			// within field projections and WHERE clauses.
			lqctx.add_value("event", met);
			lqctx.add_value("value", self.current.doc.deref());
			lqctx.add_value("after", self.current.doc.deref());
			lqctx.add_value("before", self.initial.doc.deref());
			// First of all, let's check to see if the WHERE
			// clause of the LIVE query is matched by this
			// document. If it is then we can continue.
			match self.lq_check(&lqctx, &lqopt, txn, &lq, doc).await {
				Err(Error::Ignore) => {
					trace!("live query did not match the where clause, skipping");
					continue;
				}
				Err(e) => return Err(e),
				Ok(_) => (),
			}
			// Secondly, let's check to see if any PERMISSIONS
			// clause for this table allows this document to
			// be viewed by the user who created this LIVE
			// query. If it does, then we can continue.
			match self.lq_allow(&lqctx, &lqopt, txn, &lq, doc).await {
				Err(Error::Ignore) => {
					trace!("live query did not have permission to view this document, skipping");
					continue;
				}
				Err(e) => return Err(e),
				Ok(_) => (),
			}
			// Finally, let's check what type of statement
			// caused this LIVE query to run, and send the
			// relevant notification based on the statement.
			let default_node_id = Uuid::default();
			let node_id = opt.id().unwrap_or(default_node_id);
			// This bool is deprecated since lq v2 on cf
			// We check against defaults because clients register live queries with their local node id
			// But the cf scanner uses the server node id, which is different from the client
			let node_matches_live_query =
				node_id == default_node_id || lv.node.0 == default_node_id || node_id == lv.node.0;
			trace!(
				"Notification node matches live query: {} ({} != {})",
				node_matches_live_query,
				node_id,
				lv.node.0
			);
			if stm.is_delete() {
				// Send a DELETE notification
				if node_matches_live_query {
					trace!("Sending lq delete notification");
					sender
						.send(Notification {
							id: lv.id,
							action: Action::Delete,
							result: {
								// Ensure futures are run
								let lqopt: &Options = &lqopt.new_with_futures(true);
								// Output the full document before any changes were applied
								let mut value =
									doc.doc.compute(&lqctx, lqopt, txn, Some(doc)).await?;
								// Remove metadata fields on output
								value.del(&lqctx, lqopt, txn, &*META).await?;
								// Output result
								value
							},
						})
						.await?;
				}
			} else if self.is_new() {
				// Send a CREATE notification
				if node_matches_live_query {
					trace!("Sending lq create notification");
					sender
						.send(Notification {
							id: lv.id,
							action: Action::Create,
							result: self.pluck(&lqctx, &lqopt, txn, &lq).await?,
						})
						.await?;
				}
			} else {
				// Send a UPDATE notification
				if node_matches_live_query {
					trace!("Sending lq update notification");
					sender
						.send(Notification {
							id: lv.id,
							action: Action::Update,
							result: self.pluck(&lqctx, &lqopt, txn, &lq).await?,
						})
						.await?;
				}
			};
		}
		trace!("Ended check_lqs_and_send_notifications");
		Ok(())
	}
}
