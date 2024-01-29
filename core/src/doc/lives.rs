use crate::ctx::Context;
use crate::dbs::Notification;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::{Action, Transaction};
use crate::doc::CursorDoc;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::paths::META;
use crate::sql::paths::SC;
use crate::sql::paths::SD;
use crate::sql::paths::TK;
use crate::sql::permission::Permission;
use crate::sql::Value;
use std::ops::Deref;
use std::sync::Arc;

impl<'a> Document<'a> {
	pub async fn lives(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check if forced
		if !opt.force && !self.changed() {
			return Ok(());
		}
		// Check if we can send notifications
		if let Some(chn) = &opt.sender {
			// Loop through all index statements
			for lv in self.lv(opt, txn).await?.iter() {
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
					Err(Error::Ignore) => continue,
					Err(e) => return Err(e),
					Ok(_) => (),
				}
				// Secondly, let's check to see if any PERMISSIONS
				// clause for this table allows this document to
				// be viewed by the user who created this LIVE
				// query. If it does, then we can continue.
				match self.lq_allow(&lqctx, &lqopt, txn, &lq, doc).await {
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
						chn.send(Notification {
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
					} else {
						// TODO: Send to storage
					}
				} else if self.is_new() {
					// Send a CREATE notification
					if opt.id()? == lv.node.0 {
						chn.send(Notification {
							id: lv.id,
							action: Action::Create,
							result: self.pluck(&lqctx, &lqopt, txn, &lq).await?,
						})
						.await?;
					} else {
						// TODO: Send to storage
					}
				} else {
					// Send a UPDATE notification
					if opt.id()? == lv.node.0 {
						chn.send(Notification {
							id: lv.id,
							action: Action::Update,
							result: self.pluck(&lqctx, &lqopt, txn, &lq).await?,
						})
						.await?;
					} else {
						// TODO: Send to storage
					}
				};
			}
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
}
