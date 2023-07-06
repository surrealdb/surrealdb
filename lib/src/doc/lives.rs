use crate::ctx::Context;
use crate::dbs::Notification;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::{Action, Transaction};
use crate::doc::Document;
use crate::err::Error;
use crate::sql::Value;

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
				if self.check(ctx, opt, txn, &lq).await.is_err() {
					continue;
				}
				// Check what type of data change this is
				if stm.is_delete() {
					// Send a DELETE notification
					if opt.id()? == lv.node.0 {
						let thing = (*rid).clone();
						chn.send(Notification {
							id: lv.id.0,
							action: Action::Delete,
							result: Value::Thing(thing),
						})
						.await?;
					} else {
						// TODO: Send to storage
					}
				} else if self.is_new() {
					// Send a CREATE notification
					if opt.id()? == lv.node.0 {
						chn.send(Notification {
							id: lv.id.0,
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
							id: lv.id.0,
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
		// Carry on
		Ok(())
	}
}
