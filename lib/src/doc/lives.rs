use crate::ctx::Context;
use crate::dbs::Action;
use crate::dbs::Notification;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::Value;

impl<'a> Document<'a> {
	pub async fn lives(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check if forced
		if !opt.force && !self.changed() {
			return Ok(());
		}
		// Clone transaction
		let txn = ctx.try_clone_transaction()?;
		// Get the record id
		let id = self.id.as_ref().unwrap();
		// Loop through all index statements
		for lv in self.lv(opt, &txn).await?.iter() {
			// Create a new statement
			let lq = Statement::from(lv);
			// Check LIVE SELECT where condition
			if self.check(ctx, opt, &lq).await.is_err() {
				continue;
			}
			// Check what type of data change this is
			if stm.is_delete() {
				// Send a DELETE notification
				if opt.id() == &lv.node.0 {
					let thing = (*id).clone();
					opt.sender
						.send(Notification {
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
				if opt.id() == &lv.node.0 {
					opt.sender
						.send(Notification {
							id: lv.id.0,
							action: Action::Create,
							result: self.pluck(ctx, opt, &lq).await?,
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
							id: lv.id.0,
							action: Action::Update,
							result: self.pluck(ctx, opt, &lq).await?,
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
