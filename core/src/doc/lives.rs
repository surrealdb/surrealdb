use crate::ctx::{Context, MutableContext};
use crate::dbs::Action;
use crate::dbs::Notification;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::CursorDoc;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::paths::AC;
use crate::sql::paths::META;
use crate::sql::paths::RD;
use crate::sql::paths::TK;
use crate::sql::permission::Permission;
use crate::sql::Value;
use reblessive::tree::Stk;
use std::sync::Arc;

impl Document {
	/// Processes any LIVE SELECT statements which
	/// have been defined for the table which this
	/// record belongs to. This functions loops
	/// through the live queries and processes them
	/// all within the currently running transaction.
	pub async fn process_table_lives(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
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
			let rid = self.id.as_ref().unwrap();
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
				Err(Error::Ignore) => continue,
				Err(e) => return Err(e),
				Ok(_) => (),
			}
			// Secondly, let's check to see if any PERMISSIONS
			// clause for this table allows this document to
			// be viewed by the user who created this LIVE
			// query. If it does, then we can continue.
			match self.lq_allow(stk, &lqctx, &lqopt, &lq, doc).await {
				Err(Error::Ignore) => continue,
				Err(e) => return Err(e),
				Ok(_) => (),
			}
			// Finally, let's check what type of statement
			// caused this LIVE query to run, and send the
			// relevant notification based on the statement.
			if stm.is_delete() {
				// Send a DELETE notification
				if opt.id()? == lv.node.0 {
					// Ensure futures are run
					let lqopt: &Options = &lqopt.new_with_futures(true);
					// Output the full document before any changes were applied
					let mut result =
						doc.doc.as_ref().compute(stk, &lqctx, lqopt, Some(doc)).await?;
					// Remove metadata fields on output
					result.del(stk, &lqctx, lqopt, &*META).await?;
					let res = chn
						.send(Notification {
							id: lv.id,
							action: Action::Delete,
							record: Value::Thing(rid.as_ref().clone()),
							result,
						})
						.await;

					if res.is_err() {
						// channel was closed, that means a transaction probably failed.
						// just return as nothing can be send.
						return Ok(());
					}
				} else {
					// TODO: Send to message broker
				}
			} else if self.is_new() {
				// Send a CREATE notification
				if opt.id()? == lv.node.0 {
					let res = chn
						.send(Notification {
							id: lv.id,
							action: Action::Create,
							record: Value::Thing(rid.as_ref().clone()),
							result: self.pluck(stk, &lqctx, &lqopt, &lq).await?,
						})
						.await;

					if res.is_err() {
						// channel was closed, that means a transaction probably failed.
						// just return as nothing can be send.
						return Ok(());
					}
				} else {
					// TODO: Send to message broker
				}
			} else {
				// Send a UPDATE notification
				if opt.id()? == lv.node.0 {
					let res = chn
						.send(Notification {
							id: lv.id,
							action: Action::Update,
							record: Value::Thing(rid.as_ref().clone()),
							result: self.pluck(stk, &lqctx, &lqopt, &lq).await?,
						})
						.await;

					if res.is_err() {
						// channel was closed, that means a transaction probably failed.
						// just return as nothing can be send.
						return Ok(());
					}
				} else {
					// TODO: Send to message broker
				}
			};
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
	) -> Result<(), Error> {
		// Check where condition
		if let Some(cond) = stm.conds() {
			// Check if the expression is truthy
			if !cond.compute(stk, ctx, opt, Some(doc)).await?.is_truthy() {
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
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
		doc: &CursorDoc,
	) -> Result<(), Error> {
		// Should we run permissions checks?
		if opt.check_perms(stm.into())? {
			// Get the table
			let tb = self.tb(ctx, opt).await?;
			// Process the table permissions
			match &tb.permissions.select {
				Permission::None => return Err(Error::Ignore),
				Permission::Full => return Ok(()),
				Permission::Specific(e) => {
					// Disable permissions
					let opt = &opt.new_with_perms(false);
					// Process the PERMISSION clause
					if !e.compute(stk, ctx, opt, Some(doc)).await?.is_truthy() {
						return Err(Error::Ignore);
					}
				}
			}
		}
		// Carry on
		Ok(())
	}
}
