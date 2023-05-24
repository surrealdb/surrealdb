use crate::ctx::Context;
use crate::dbs::Action;
use crate::dbs::Notification;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::doc::Document;
use crate::err::Error;

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
		let id = self.id.as_ref().unwrap();
		// Loop through all index statements
		for lv in self.lv(opt, txn).await?.iter() {
			// Create a new statement
			let lq = Statement::from(lv);
			// Check LIVE SELECT where condition
			if self.check(ctx, opt, txn, stm).await.is_err() {
				continue;
			}
			// Check what type of data change this is
			if stm.is_delete() {
				// Send a DELETE notification
				if opt.id() == &lv.node.0 {
					opt.sender
						.send(Notification {
							id: lv.id.clone(),
							action: Action::Delete,
							result: id.clone().into(),
						})
						.await?;
				} else {
					// TODO: Send to storage
				}
			} else if self.is_new() {
				// Send a CREATE notification
				if opt.id() == &lv.node.0 {
					opt.sender
						.send(Notification {
							id: lv.id.clone(),
							action: Action::Create,
							result: self.pluck(ctx, opt, txn, &lq).await?,
						})
						.await?;
				} else {
					// TODO: Send to storage
				}
			} else {
				// Send a UPDATE notification
				if opt.id() == &lv.node.0 {
					opt.sender
						.send(Notification {
							id: lv.id.clone(),
							action: Action::Update,
							result: self.pluck(ctx, opt, txn, &lq).await?,
						})
						.await?;
				} else {
					// TODO: Send to storage
				}
			};
		}
		// Carry on
		Ok(())
	}
}
