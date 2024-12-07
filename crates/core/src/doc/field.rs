use crate::ctx::{Context, MutableContext};
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;
use crate::iam::Action;
use crate::sql::data::Data;
use crate::sql::idiom::Idiom;
use crate::sql::kind::Kind;
use crate::sql::permission::Permission;
use crate::sql::value::Value;
use reblessive::tree::Stk;
use std::sync::Arc;

impl Document {
	/// Ensures that any remaining fields on a
	/// SCHEMAFULL table are cleaned up and removed.
	/// If a field is defined as FLEX, then any
	/// nested fields or array values are untouched.
	pub(super) async fn cleanup_table_fields(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Get the table
		let tb = self.tb(ctx, opt).await?;
		// This table is schemafull
		if tb.full {
			// Create a vector to store the keys
			let mut keys: Vec<Idiom> = vec![];
			// Loop through all field statements
			for fd in self.fd(ctx, opt).await?.iter() {
				// Is this a schemaless field?
				match fd.flex || fd.kind.as_ref().is_some_and(|k| k.is_literal_nested()) {
					false => {
						// Loop over this field in the document
						for k in self.current.doc.each(&fd.name).into_iter() {
							keys.push(k);
						}
					}
					true => {
						// Loop over every field under this field in the document
						for k in self.current.doc.every(Some(&fd.name), true, true).into_iter() {
							keys.push(k);
						}
					}
				}
			}
			// Loop over every field in the document
			for fd in self.current.doc.every(None, true, true).iter() {
				if !keys.contains(fd) {
					match fd {
						// Built-in fields
						fd if fd.is_id() => continue,
						fd if fd.is_in() => continue,
						fd if fd.is_out() => continue,
						fd if fd.is_meta() => continue,
						// Custom fields
						fd => match opt.strict {
							// If strict, then throw an error on an undefined field
							true => {
								return Err(Error::FieldUndefined {
									table: tb.name.to_raw(),
									field: fd.to_owned(),
								})
							}
							// Otherwise, delete the field silently and don't error
							false => self.current.doc.to_mut().del(stk, ctx, opt, fd).await?,
						},
					}
				}
				// NONE values should never be stored
				if self.current.doc.pick(fd).is_none() {
					self.current.doc.to_mut().del(stk, ctx, opt, fd).await?;
				}
			}
		} else {
			// Loop over every field in the document
			for fd in self.current.doc.every(None, true, true).iter() {
				// NONE values should never be stored
				if self.current.doc.pick(fd).is_none() {
					self.current.doc.to_mut().del(stk, ctx, opt, fd).await?;
				}
			}
		}
		// Carry on
		Ok(())
	}
	/// Processes `DEFINE FIELD` statements which
	/// have been defined on the table for this
	/// record. These fields are executed for
	/// every matching field in the input document.
	pub(super) async fn process_table_fields(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check import
		if opt.import {
			return Ok(());
		}
		// Get the record id
		let rid = self.id()?;
		// Get the user applied input
		let inp = self.initial.doc.as_ref().changed(self.current.doc.as_ref());
		// When set, any matching embedded object fields
		// which are prefixed with the specified idiom
		// will be skipped, as the parent object is optional
		let mut skip: Option<&Idiom> = None;
		// Loop through all field statements
		for fd in self.fd(ctx, opt).await?.iter() {
			// Check if we should skip this field
			let skipped = match skip {
				// We are skipping a parent field
				Some(inner) => {
					// Check if this field is a child field
					let skipped = fd.name.starts_with(inner);
					// Let's stop skipping fields if not
					if !skipped {
						skip = None;
					}
					// Specify whether we should skip
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
				// Check for the `id` field
				if fd.name.is_id() {
					if !self.is_new() && val.ne(&old) {
						return Err(Error::FieldReadonly {
							field: fd.name.clone(),
							thing: rid.to_string(),
						});
					} else if !self.is_new() {
						continue;
					}
				}
				// If the field is READONLY then we
				// will check that the field has not
				// been modified. If it has just been
				// omitted then we reset it, otherwise
				// we throw a field readonly error.
				if fd.readonly {
					// Check if we are updating the
					// document, and check if the new
					// field value is now different to
					// the old field value in any way.
					if !self.is_new() && val.ne(&old) {
						// Check the data clause type
						match stm.data() {
							Some(Data::ContentExpression(_)) => {
							// If the field is NONE, we assume
							// that the field was ommitted when
							// using a CONTENT clause, and we
							// revert the value to the old value.
								self.current
									.doc
									.to_mut()
									.set(stk, ctx, opt, &k, old.as_ref().clone())
									.await?;
								continue;
							}
							// If the field has been modified
							// and the user didn't use a CONTENT
							// clause, then this should not be
							// allowed, and we throw an error.
							_ => {
								return Err(Error::FieldReadonly {
									field: fd.name.clone(),
									thing: rid.to_string(),
								});
							}
						}
					}
					// If this field was not modified then
					// we can continue without needing to
					// process the field in any other way.
					else if !self.is_new() {
						continue;
					}
				}
				// Skip this field?
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
						// Only run value clause for new and empty fields
						if self.is_new() && val.is_none() {
							// Arc the current value
							let now = Arc::new(val);
							// Configure the context
							let mut ctx = MutableContext::new(ctx);
							ctx.add_value("before", old.clone());
							ctx.add_value("input", inp.clone());
							ctx.add_value("after", now.clone());
							ctx.add_value("value", now);
							// Freeze the new context
							let ctx = ctx.freeze();
							// Process the VALUE clause
							val = expr.compute(stk, &ctx, opt, Some(&self.current)).await?;
						}
					}
					// Check for a TYPE clause
					if let Some(kind) = &fd.kind {
						// If this is the `id` field, it must be a record
						let cast = match &fd.name {
							name if name.is_id() => match kind.to_owned() {
								Kind::Option(v) if v.is_record() => &*v.to_owned(),
								Kind::Record(r) => &Kind::Record(r),
								_ => &Kind::Record(vec![]),
							},
							_ => kind,
						};
						// Check the type of the field value
						val = val.coerce_to(cast).map_err(|e| match e {
							// There was a conversion error
							Error::CoerceTo {
								from,
								..
							} => Error::FieldCheck {
								thing: rid.to_string(),
								field: fd.name.clone(),
								value: from.to_string(),
								check: cast.to_string(),
							},
							// There was a different error
							e => e,
						})?;
						// If this is the `id` field, check the inner type
						if fd.name.is_id() {
							if let Value::Thing(id) = &val {
								// Get the value of the ID only
								let inner = Value::from(id.clone().id);
								// Check the type of the ID part
								inner.coerce_to(kind).map_err(|e| match e {
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
					}
					// Check for a VALUE clause
					if let Some(expr) = &fd.value {
						// Arc the current value
						let now = Arc::new(val);
						// Configure the context
						let mut ctx = MutableContext::new(ctx);
						ctx.add_value("before", old.clone());
						ctx.add_value("input", inp.clone());
						ctx.add_value("after", now.clone());
						ctx.add_value("value", now);
						// Freeze the new context
						let ctx = ctx.freeze();
						// Process the VALUE clause
						val = expr.compute(stk, &ctx, opt, Some(&self.current)).await?;
					}
					// Check for a TYPE clause
					if let Some(kind) = &fd.kind {
						// If this is the `id` field, it must be a record
						let cast = match &fd.name {
							name if name.is_id() => match kind.to_owned() {
								Kind::Option(v) if v.is_record() => &*v.to_owned(),
								Kind::Record(r) => &Kind::Record(r),
								_ => &Kind::Record(vec![]),
							},
							_ => kind,
						};
						// Check the type of the field value
						val = val.coerce_to(cast).map_err(|e| match e {
							// There was a conversion error
							Error::CoerceTo {
								from,
								..
							} => Error::FieldCheck {
								thing: rid.to_string(),
								field: fd.name.clone(),
								value: from.to_string(),
								check: cast.to_string(),
							},
							// There was a different error
							e => e,
						})?;
						// If this is the `id` field, check the inner type
						if fd.name.is_id() {
							if let Value::Thing(id) = &val {
								let inner = Value::from(id.clone().id);
								inner.coerce_to(kind).map_err(|e| match e {
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
								// Arc the current value
								let now = Arc::new(val.clone());
								// Configure the context
								let mut ctx = MutableContext::new(ctx);
								ctx.add_value("before", old.clone());
								ctx.add_value("input", inp.clone());
								ctx.add_value("after", now.clone());
								ctx.add_value("value", now.clone());
								// Freeze the new context
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
										check: expr.to_string(),
										value: now.to_string(),
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
							// Arc the current value
							let now = Arc::new(val.clone());
							// Disable permissions
							let opt = &opt.new_with_perms(false);
							// Configure the context
							let mut ctx = MutableContext::new(ctx);
							ctx.add_value("before", old.clone());
							ctx.add_value("input", inp.clone());
							ctx.add_value("after", now.clone());
							ctx.add_value("value", now.clone());
							// Freeze the new context
							let ctx = ctx.freeze();
							// Process the PERMISSION clause
							if !e.compute(stk, &ctx, opt, Some(&self.current)).await?.is_truthy() {
								val = old.as_ref().clone()
							}
						}
					}
				}
				// Skip this field?
				if !skipped {
					// If the field is empty, mark child fields as skippable
					if val.is_none() && fd.kind.as_ref().is_some_and(Kind::can_be_none) {
						skip = Some(&fd.name);
					}
					// Set the new value of the field, or delete it if empty
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
