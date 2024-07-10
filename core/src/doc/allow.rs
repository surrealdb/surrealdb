use crate::ctx::Context;
use crate::dbs::Statement;
use crate::dbs::{Options, Transaction};
use crate::doc::Document;
use crate::err::Error;
use crate::iam::Level;
use crate::sql::permission::Permission;

impl<'a> Document<'a> {
	pub async fn allow(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check if this record exists
		if self.id.is_some() {
			// Should we run permissions checks?
			if opt.check_perms(stm.into()) {
				// Check that authentication matches session
				match opt.auth.level() {
					Level::Namespace(ns) => {
						opt.valid_for_ns()?;
						if ns != opt.ns() {
							return Err(Error::NsNotAllowed {
								ns: opt.ns().into(),
							});
						}
					}
					Level::Database(ns, db) | Level::Scope(ns, db, _) => {
						opt.valid_for_db()?;
						if ns != opt.ns() {
							return Err(Error::NsNotAllowed {
								ns: opt.ns().into(),
							});
						}
						if db != opt.db() {
							return Err(Error::DbNotAllowed {
								db: opt.db().into(),
							});
						}
					}
					_ => {}
				}
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
								ctx,
								opt,
								txn,
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
