use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::permission::Permission;

impl<'a> Document<'a> {
	pub async fn allow(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check permission clause
		if opt.perms && opt.auth.perms() && self.id.is_some() {
			// Get the table
			let tb = self.tb(opt, txn).await?;
			// Get the permission clause
			let perms = if stm.is_delete() {
				&tb.permissions.delete
			} else if stm.is_select() {
				&tb.permissions.select
			} else if self.is_new() {
				&tb.permissions.create
			} else {
				&tb.permissions.update
			};
			// Match the permission clause
			match perms {
				Permission::None => return Err(Error::Ignore),
				Permission::Full => return Ok(()),
				Permission::Specific(e) => {
					// Disable permissions
					let opt = &opt.perms(false);
					// Process the PERMISSION clause
					if !e.compute(ctx, opt, txn, Some(&self.current)).await?.is_truthy() {
						return Err(Error::Ignore);
					}
				}
			}
		}
		// Carry on
		Ok(())
	}
}
