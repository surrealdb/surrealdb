use crate::ctx::{Context, MutableContext};
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;
use crate::iam::Action;
use crate::sql::permission::Permission;
use crate::sql::value::Value;
use crate::sql::{Idiom, Kind};
use reblessive::tree::Stk;
use std::sync::Arc;

impl Document {
	pub async fn field(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check import
		if opt.import {
			return Ok(());
		}
		// Get the record id
		let rid = self.id.as_ref().unwrap();
		// Get the user applied input
		let inp = self.initial.doc.as_ref().changed(self.current.doc.as_ref());
		// If set, the loop will skip certain clauses as long
		// as the field name starts with the set idiom
		let mut skip: Option<Idiom> = None;
		// Loop through all field statements
		for fd in self.fd(ctx, opt).await?.iter() {
			let skipped = match skip {
				Some(ref inner) => {
					let skipped = fd.name.starts_with(inner);
					if !skipped {
						skip = None;
					}

					skipped
				}
				None => false,
			};

			// Loop over each field in document
			for (k, mut val) in self.current.doc.as_ref().walk(&fd.name).into_iter() {
				// Get the initial value
				let old = Arc::new(self.initial.doc.as_ref().pick(&k));
				// Get the input value
				let inp = Arc::new(inp.pick(&k));
				// Check for READONLY clause
				if fd.readonly && !self.is_new() && val.ne(&old) {
					return Err(Error::FieldReadonly {
						field: fd.name.clone(),
						thing: rid.to_string(),
					});
				}
				if !skipped {
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
							let mut ctx = MutableContext::new(ctx);
							let v = Arc::new(val);
							ctx.add_value("input", inp.clone());
							ctx.add_value("value", v.clone());
							ctx.add_value("after", v);
							ctx.add_value("before", old.clone());
							let ctx = ctx.freeze();
							// Process the VALUE clause
							val = expr.compute(stk, &ctx, opt, Some(&self.current)).await?;
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
						// Only run value clause for mutable and new fields
						if !fd.readonly || self.is_new() {
							// Configure the context
							let v = Arc::new(val);
							let mut ctx = MutableContext::new(ctx);
							ctx.add_value("input", inp.clone());
							ctx.add_value("value", v.clone());
							ctx.add_value("after", v);
							ctx.add_value("before", old.clone());
							let ctx = ctx.freeze();
							// Process the VALUE clause
							val = expr.compute(stk, &ctx, opt, Some(&self.current)).await?;
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
					// Check for a ASSERT clause
					if let Some(expr) = &fd.assert {
						match (&val, &fd.kind) {
							// The field TYPE is optional, and the field
							// value was not set or a NONE value was
							// specified, so let's ignore the ASSERT clause
							(Value::None, Some(Kind::Option(_))) => (),
							// Otherwise let's process the ASSERT clause
							_ => {
								// Configure the context
								let mut ctx = MutableContext::new(ctx);
								let v = Arc::new(val.clone());
								ctx.add_value("input", inp.clone());
								ctx.add_value("value", v.clone());
								ctx.add_value("after", v);
								ctx.add_value("before", old.clone());
								let ctx = ctx.freeze();
								// Process the ASSERT clause
								if !expr
									.compute(stk, &ctx, opt, Some(&self.current))
									.await?
									.is_truthy()
								{
									return Err(Error::FieldValue {
										thing: rid.to_string(),
										field: fd.name.clone(),
										value: val.to_string(),
										check: expr.to_string(),
									});
								}
							}
						}
					}
				}
				// Check for a PERMISSIONS clause
				if opt.check_perms(Action::Edit)? {
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
						Permission::None => val = old.as_ref().clone(),
						// The field PERMISSIONS clause
						// is a custom expression, so
						// we check the expression and
						// revert the field if denied.
						Permission::Specific(e) => {
							// Disable permissions
							let opt = &opt.new_with_perms(false);
							// Configure the context
							let mut ctx = MutableContext::new(ctx);
							let v = Arc::new(val.clone());
							ctx.add_value("input", inp);
							ctx.add_value("value", v.clone());
							ctx.add_value("after", v);
							ctx.add_value("before", old.clone());
							let ctx = ctx.freeze();
							// Process the PERMISSION clause
							if !e.compute(stk, &ctx, opt, Some(&self.current)).await?.is_truthy() {
								val = old.as_ref().clone()
							}
						}
					}
				}

				if !skipped {
					if matches!(val, Value::None) && matches!(fd.kind, Some(Kind::Option(_))) {
						skip = Some(fd.name.to_owned());
					}

					// Set the value of the field
					match val {
						Value::None => self.current.doc.to_mut().del(stk, ctx, opt, &k).await?,
						v => self.current.doc.to_mut().set(stk, ctx, opt, &k, v).await?,
					};
				}
			}
		}
		// Carry on
		Ok(())
	}
}
