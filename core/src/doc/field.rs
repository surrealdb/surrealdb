use crate::ctx::Context;
use crate::dbs::Statement;
use crate::dbs::{Options, Transaction};
use crate::doc::Document;
use crate::err::Error;
use crate::iam::Action;
use crate::sql::permission::Permission;
use crate::sql::value::Value;

impl<'a> Document<'a> {
	pub async fn field(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check import
		if opt.import {
			return Ok(());
		}
		// Get the record id
		let rid = self.id.as_ref().unwrap();
		// Get the user applied input
		let inp = self.initial.doc.changed(self.current.doc.as_ref());
		// Loop through all field statements
		for fd in self.fd(opt, txn).await?.iter() {
			// Loop over each field in document
			for (k, mut val) in self.current.doc.walk(&fd.name).into_iter() {
				// Get the initial value
				let old = self.initial.doc.pick(&k);
				// Get the input value
				let inp = inp.pick(&k);
				// Get the default value
				let def = match &fd.default {
					Some(v) => Some(v),
					_ => match &fd.value {
						Some(v) if v.is_static() => Some(v),
						_ => None,
					},
				};
				// Check for a DEFAULT clause
				if let Some(expr) = def {
					if self.is_new() && val.is_none() {
						// Configure the context
						let mut ctx = Context::new(ctx);
						ctx.add_value("input", &inp);
						ctx.add_value("value", &val);
						ctx.add_value("after", &val);
						ctx.add_value("before", &old);
						// Process the VALUE clause
						val = expr.compute(&ctx, opt, txn, Some(&self.current)).await?;
					}
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
				// Check for a VALUE clause
				if let Some(expr) = &fd.value {
					// Configure the context
					let mut ctx = Context::new(ctx);
					ctx.add_value("input", &inp);
					ctx.add_value("value", &val);
					ctx.add_value("after", &val);
					ctx.add_value("before", &old);
					// Process the VALUE clause
					val = expr.compute(&ctx, opt, txn, Some(&self.current)).await?;
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
					// Process the ASSERT clause
					if !expr.compute(&ctx, opt, txn, Some(&self.current)).await?.is_truthy() {
						return Err(Error::FieldValue {
							thing: rid.to_string(),
							field: fd.name.clone(),
							value: val.to_string(),
							check: expr.to_string(),
						});
					}
				}
				// Check for a PERMISSIONS clause
				if opt.check_perms(Action::Edit) {
					// Get the permission clause
					let perms = if self.is_new() {
						&fd.permissions.create
					} else {
						&fd.permissions.update
					};
					// Match the permission clause
					match perms {
						// The field PERMISSIONS clause
						// is FULL, enabling this field
						// to be updated without checks.
						Permission::Full => (),
						// The field PERMISSIONS clause
						// is NONE, meaning that this
						// change will be reverted.
						Permission::None => val = old,
						// The field PERMISSIONS clause
						// is a custom expression, so
						// we check the expression and
						// revert the field if denied.
						Permission::Specific(e) => {
							// Disable permissions
							let opt = &opt.new_with_perms(false);
							// Configure the context
							let mut ctx = Context::new(ctx);
							ctx.add_value("input", &inp);
							ctx.add_value("value", &val);
							ctx.add_value("after", &val);
							ctx.add_value("before", &old);
							// Process the PERMISSION clause
							if !e.compute(&ctx, opt, txn, Some(&self.current)).await?.is_truthy() {
								val = old
							}
						}
					}
				}
				// Set the value of the field
				match val {
					Value::None => self.current.doc.to_mut().del(ctx, opt, txn, &k).await?,
					_ => self.current.doc.to_mut().set(ctx, opt, txn, &k, val).await?,
				};
			}
		}
		// Carry on
		Ok(())
	}
}
