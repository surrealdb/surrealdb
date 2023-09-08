use crate::ctx::Context;
use crate::dbs::Notification;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::{Action, Transaction};
use crate::doc::Document;
use crate::err::Error;
use crate::sql;
use crate::sql::Value;
use std::sync::Arc;

impl<'a> Document<'a> {
	pub async fn lives(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		println!("LIVES WAS INVOKED");
		// Check if forced
		if !opt.force && !self.changed() {
			return Ok(());
		}
		// Get the record id
		let rid = self.id.as_ref().unwrap();
		// Check if we can send notifications
		// if let Some(chn) = &opt.sender {
		match &opt.sender {
			None => {
				warn!("Lives was invoked, but no sender attached to options")
			}
			Some(chn) => {
				println!("LIVES HAS SENDER");
				// Clone the sending channel
				let chn = chn.clone();
				// Loop through all index statements
				for lv in self.lv(opt, txn).await?.iter() {
					// Create a new statement
					let lq = Statement::from(lv);
					println!("HANDLING LIVE QUERY {:?}", lq);
					// Check LIVE SELECT where condition
					if let Some(cond) = lq.conds() {
						// Check if this is a delete statement
						let doc = match stm.is_delete() {
							true => &self.initial,
							false => &self.current,
						};
						// Check if the expression is truthy
						if !cond.compute(ctx, opt, txn, Some(doc)).await?.is_truthy() {
							continue;
						}
					}
					// Check authorization
					trace!("Checking live query auth: {:?}", lv);
					let lq_options = Options::new_with_perms(opt, true)
						.with_auth(Arc::from(lv.auth.clone().ok_or(Error::UnknownAuth)?));
					if self.allow(ctx, &lq_options, txn, &lq).await.is_err() {
						continue;
					}
					// Check what type of data change this is
					{
						let mut tx = txn.lock().await;
						let ts = tx.clock().await;
						let not_id = sql::Uuid::new_v4();
						if stm.is_delete() {
							// Send a DELETE notification
							let thing = (*rid).clone();
							let notification = Notification {
								live_id: lv.id.clone(),
								node_id: lv.node.clone(),
								notification_id: not_id.clone(),
								action: Action::Delete,
								result: Value::Thing(thing),
								timestamp: ts.clone(),
							};
							if opt.id()? == lv.node.0 {
								// TODO read pending remote notifications
								chn.send(notification).await?;
							} else {
								tx.putc_tbnt(
									opt.ns(),
									opt.db(),
									&self.id.unwrap().tb,
									lv.id.clone(),
									ts,
									not_id,
									notification,
									None,
								)
								.await?;
							}
						} else if self.is_new() {
							// Send a CREATE notification
							println!("Handling create notification");
							let notification = Notification {
								live_id: lv.id.clone(),
								node_id: lv.node.clone(),
								notification_id: not_id.clone(),
								action: Action::Create,
								result: self.pluck(ctx, opt, txn, &lq).await?,
								timestamp: ts.clone(),
							};
							if opt.id()? == lv.node.0 {
								// TODO read pending remote notifications
								println!("Sent notification to channel");
								chn.send(notification).await?;
							} else {
								println!("Record notification in db");
								tx.putc_tbnt(
									opt.ns(),
									opt.db(),
									&self.id.unwrap().tb,
									lv.id.clone(),
									ts,
									not_id,
									notification,
									None,
								)
								.await?;
							}
						} else {
							// Send a UPDATE notification
							let notification = Notification {
								live_id: lv.id.clone(),
								node_id: lv.node.clone(),
								notification_id: not_id.clone(),
								action: Action::Update,
								result: self.pluck(ctx, opt, txn, &lq).await?,
								timestamp: ts.clone(),
							};
							if opt.id()? == lv.node.0 {
								// TODO read pending remote notifications
								chn.send(notification).await?;
							} else {
								tx.putc_tbnt(
									opt.ns(),
									opt.db(),
									&self.id.unwrap().tb,
									lv.id.clone(),
									ts,
									not_id,
									notification,
									None,
								)
								.await?;
							}
						};
					}
				}
			}
		}
		// Carry on
		Ok(())
	}
}
