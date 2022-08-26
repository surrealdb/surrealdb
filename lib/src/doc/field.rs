use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::permission::Permission;
use crate::sql::value::Value;

impl<'a> Document<'a> {
	pub async fn field(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check fields
		if !opt.fields {
			return Ok(());
		}
		// Loop through all field statements
		for fd in self.fd(opt, txn).await?.iter() {
			// Loop over each field in document
			for (k, mut val) in self.current.walk(&fd.name).into_iter() {
				// Get the initial value
				let old = self.initial.pick(&k);
				// Check for a VALUE clause
				if let Some(expr) = &fd.value {
					// Configure the context
					let mut ctx = Context::new(ctx);
					ctx.add_value("value".into(), &val);
					ctx.add_value("after".into(), &val);
					ctx.add_value("before".into(), &old);
					// Process the VALUE clause
					val = expr.compute(&ctx, opt, txn, Some(&self.current)).await?;
				}
				// Check for a TYPE clause
				if let Some(kind) = &fd.kind {
					val = val.convert_to(kind);
				}
				// Check for a ASSERT clause
				if let Some(expr) = &fd.assert {
					// Configure the context
					let mut ctx = Context::new(ctx);
					ctx.add_value("value".into(), &val);
					ctx.add_value("after".into(), &val);
					ctx.add_value("before".into(), &old);
					// Process the ASSERT clause
					if !expr.compute(&ctx, opt, txn, Some(&self.current)).await?.is_truthy() {
						return Err(Error::FieldValue {
							value: val.to_string(),
							field: fd.name.clone(),
							check: expr.to_string(),
						});
					}
				}
				// Check for a PERMISSIONS clause
				if opt.perms && opt.auth.perms() {
					// Get the permission clause
					let perms = if stm.is_delete() {
						&fd.permissions.delete
					} else if self.is_new() {
						&fd.permissions.create
					} else {
						&fd.permissions.update
					};
					// Match the permission clause
					match perms {
						Permission::Full => (),
						Permission::None => val = old,
						Permission::Specific(e) => {
							// Configure the context
							let mut ctx = Context::new(ctx);
							ctx.add_value("value".into(), &val);
							ctx.add_value("after".into(), &val);
							ctx.add_value("before".into(), &old);
							// Process the PERMISSION clause
							if !e.compute(&ctx, opt, txn, Some(&self.current)).await?.is_truthy() {
								val = old
							}
						}
					}
				}
				// Set the value of the field
				match val {
					Value::None => self.current.to_mut().del(ctx, opt, txn, &k).await?,
					_ => self.current.to_mut().set(ctx, opt, txn, &k, val).await?,
				};
			}
		}
		// Carry on
		Ok(())
	}
}
