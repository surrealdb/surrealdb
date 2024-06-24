use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::permission::Permission;
use reblessive::tree::Stk;

impl<'a> Document<'a> {
	pub async fn allow(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check if this record exists
		if self.id.is_some() {
			// Should we run permissions checks?
			if opt.check_perms(stm.into())? {
				// Get the table
				let tb = self.tb(ctx, opt).await?;
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
				// Process the table permissions
				match perms {
					Permission::None => return Err(Error::Ignore),
					Permission::Full => return Ok(()),
					Permission::Specific(e) => {
						// Disable permissions
						let opt = &opt.new_with_perms(false);
						// Process the PERMISSION clause
						if !e
							.compute(
								stk,
								ctx,
								opt,
								Some(match stm.is_delete() {
									true => &self.initial,
									false => &self.current,
								}),
							)
							.await?
							.is_truthy()
						{
							return Err(Error::Ignore);
						}
					}
				}
			}
		}
		// Carry on
		Ok(())
	}
}
