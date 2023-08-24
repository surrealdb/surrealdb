use crate::ctx::Context;
use crate::dbs::node::Timestamp;
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
		// Check if forced
		if !opt.force && !self.changed() {
			return Ok(());
		}
		// Get the record id
		let rid = self.id.as_ref().unwrap();
		// Check if we can send notifications
		if let Some(chn) = &opt.sender {
			// Clone the sending channel
			let chn = chn.clone();
			// Loop through all index statements
			for lv in self.lv(opt, txn).await?.iter() {
				// Create a new statement
				let lq = Statement::from(lv);
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
					let ts = tx.clock();
					let not_id = sql::Uuid::new_v4();
					if stm.is_delete() {
						// Send a DELETE notification
						let notification = Notification {
							live_id: lv.id.clone(),
							node_id: lv.node.clone(),
							notification_id: not_id,
							action: Action::Delete,
							result: Value::Thing((*rid).clone()),
							timestamp: ts,
						};
						if opt.id()? == lv.node.0 {
							let thing = (*rid).clone();
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
						if opt.id()? == lv.node.0 {
							chn.send(Notification {
								live_id: lv.id.clone(),
								action: Action::Create,
								result: self.pluck(ctx, opt, txn, &lq).await?,
							})
							.await?;
						} else {
							// TODO: Send to storage
						}
					} else {
						// Send a UPDATE notification
						if opt.id()? == lv.node.0 {
							chn.send(Notification {
								live_id: lv.id.clone(),
								action: Action::Update,
								result: self.pluck(ctx, opt, txn, &lq).await?,
							})
							.await?;
						} else {
							// TODO: Send to storage
						}
					};
				}
			}
		}
		// Carry on
		Ok(())
	}
}
