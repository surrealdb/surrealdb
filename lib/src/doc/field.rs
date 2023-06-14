use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::permission::Permission;
use crate::sql::value::Value;

impl<'a> Document<'a> {
	pub async fn field(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check fields
		if !opt.fields {
			return Ok(());
		}
		// Get the record id
		let rid = self.id.as_ref().unwrap();
		// Get the user applied input
		let inp = self.initial.changed(self.current.as_ref());
		// Clone transaction
		let txn = ctx.clone_transaction()?;
		// Loop through all field statements
		for fd in self.fd(opt, &txn).await?.iter() {
			// Loop over each field in document
			for (k, mut val) in self.current.walk(&fd.name).into_iter() {
				// Get the initial value
				let old = self.initial.pick(&k);
				// Get the input value
				let inp = inp.pick(&k);
				// Check for a TYPE clause
				if let Some(kind) = &fd.kind {
					if !val.is_none() {
						val = val.coerce_to(kind).map_err(|e| match e {
							// There was a conversion error
							Error::CoerceTo {
								from,
								..
							} => Error::FieldCheck {
								thing: rid.to_string(),
								field: fd.name.clone(),
								value: from.to_string(),
								check: kind.to_string(),
							},
							// There was a different error
							e => e,
						})?;
					}
				}
				// Check for a VALUE clause
				if let Some(expr) = &fd.value {
					// Configure the context
					let mut ctx = Context::new(ctx);
					ctx.add_value("input", &inp);
					ctx.add_value("value", &val);
					ctx.add_value("after", &val);
					ctx.add_value("before", &old);
					ctx.add_cursor_doc(&self.current);
					// Process the VALUE clause
					val = expr.compute(&ctx, opt).await?;
				}
				// Check for a TYPE clause
				if let Some(kind) = &fd.kind {
					val = val.coerce_to(kind).map_err(|e| match e {
						// There was a conversion error
						Error::CoerceTo {
							from,
							..
						} => Error::FieldCheck {
							thing: rid.to_string(),
							field: fd.name.clone(),
							value: from.to_string(),
							check: kind.to_string(),
						},
						// There was a different error
						e => e,
					})?;
				}
				// Check for a ASSERT clause
				if let Some(expr) = &fd.assert {
					// Configure the context
					let mut ctx = Context::new(ctx);
					ctx.add_value("input", &inp);
					ctx.add_value("value", &val);
					ctx.add_value("after", &val);
					ctx.add_value("before", &old);
					ctx.add_cursor_doc(&self.current);
					// Process the ASSERT clause
					if !expr.compute(&ctx, opt).await?.is_truthy() {
						return Err(Error::FieldValue {
							thing: rid.to_string(),
							field: fd.name.clone(),
							value: val.to_string(),
							check: expr.to_string(),
						});
					}
				}
				// Check for a PERMISSIONS clause
				if opt.perms && opt.auth.perms() {
					// Get the permission clause
					let perms = if self.is_new() {
						&fd.permissions.create
					} else {
						&fd.permissions.update
					};
					// Match the permission clause
					match perms {
						Permission::Full => (),
						Permission::None => val = old,
						Permission::Specific(e) => {
							// Disable permissions
							let opt = &opt.perms(false);
							// Configure the context
							let mut ctx = Context::new(ctx);
							ctx.add_value("input", &inp);
							ctx.add_value("value", &val);
							ctx.add_value("after", &val);
							ctx.add_value("before", &old);
							ctx.add_cursor_doc(&self.current);
							// Process the PERMISSION clause
							if !e.compute(&ctx, opt).await?.is_truthy() {
								val = old
							}
						}
					}
				}
				// Set the value of the field
				match val {
					Value::None => self.current.to_mut().del(ctx, opt, &k).await?,
					_ => self.current.to_mut().set(ctx, opt, &k, val).await?,
				};
			}
		}
		// Carry on
		Ok(())
	}
}
