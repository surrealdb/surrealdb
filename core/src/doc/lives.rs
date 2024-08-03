use crate::ctx::Context;
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
use std::ops::Deref;
use std::sync::Arc;

impl<'a> Document<'a> {
	pub async fn lives(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check import
		if opt.import {
			return Ok(());
		}
		// Check if changed
		if !self.changed() {
			return Ok(());
		}
		// Check if we can send notifications
		if let Some(chn) = &opt.sender {
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
				// Check if this is a delete statement
				let doc = match stm.is_delete() {
					true => &self.initial,
					false => &self.current,
				};

				let mut lqctx = match lv.context(ctx) {
					Some(ctx) => ctx,
					None => continue,
				};

				// Add $before, $after, $value, and $event params
				// to this LIVE query so the user can use these
				// within field projections and WHERE clauses.
				lqctx.add_value("event", met);
				lqctx.add_value("value", self.current.doc.deref());
				lqctx.add_value("after", self.current.doc.deref());
				lqctx.add_value("before", self.initial.doc.deref());

				let lqopt = match lv.options(opt) {
					Some(opt) => opt,
					None => continue,
				};

				// First of all, let's check to see if the WHERE
				// clause of the LIVE query is matched by this
				// document. If it is then we can continue.
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
						chn.send(Notification {
							id: lv.id,
							action: Action::Delete,
							result: {
								// Ensure futures are run
								let lqopt: &Options = &lqopt.new_with_futures(true);
								// Output the full document before any changes were applied
								let mut value =
									doc.doc.compute(stk, &lqctx, lqopt, Some(doc)).await?;
								// Remove metadata fields on output
								value.del(stk, &lqctx, lqopt, &*META).await?;
								// Output result
								value
							},
						})
						.await?;
					} else {
						// TODO: Send to message broker
					}
				} else if self.is_new() {
					// Send a CREATE notification
					if opt.id()? == lv.node.0 {
						chn.send(Notification {
							id: lv.id,
							action: Action::Create,
							result: self.pluck(stk, &lqctx, &lqopt, &lq).await?,
						})
						.await?;
					} else {
						// TODO: Send to message broker
					}
				} else {
					// Send a UPDATE notification
					if opt.id()? == lv.node.0 {
						chn.send(Notification {
							id: lv.id,
							action: Action::Update,
							result: self.pluck(stk, &lqctx, &lqopt, &lq).await?,
						})
						.await?;
					} else {
						// TODO: Send to message broker
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
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
		doc: &CursorDoc<'_>,
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
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
		doc: &CursorDoc<'_>,
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
