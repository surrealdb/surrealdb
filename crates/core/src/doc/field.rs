use crate::ctx::{Context, MutableContext};
use crate::dbs::capabilities::ExperimentalTarget;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;
use crate::iam::Action;
use crate::sql::data::Data;
use crate::sql::idiom::Idiom;
use crate::sql::kind::Kind;
use crate::sql::permission::Permission;
use crate::sql::reference::Refs;
use crate::sql::statements::DefineFieldStatement;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use crate::sql::Part;
use reblessive::tree::Stk;
use std::sync::Arc;

impl Document {
	/// Ensures that any remaining fields on a
	/// SCHEMAFULL table are cleaned up and removed.
	/// If a field is defined as FLEX, then any
	/// nested fields or array values are untouched.
	pub(super) async fn cleanup_table_fields(
		&mut self,
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
				match fd.flex || fd.kind.as_ref().is_some_and(Kind::is_literal_nested) {
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
							false => self.current.doc.to_mut().cut(fd),
						},
					}
				}
				// NONE values should never be stored
				if self.current.doc.pick(fd).is_none() {
					self.current.doc.to_mut().cut(fd);
				}
			}
		} else {
			// Loop over every field in the document
			for fd in self.current.doc.every(None, true, true).iter() {
				// NONE values should never be stored
				if self.current.doc.pick(fd).is_none() {
					self.current.doc.to_mut().cut(fd);
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
							// If the field is NONE, we assume
							// that the field was ommitted when
							// using a CONTENT clause, and we
							// revert the value to the old value.
							Some(Data::ContentExpression(_)) if val.is_none() => {
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
				// Generate the field context
				let mut field = FieldEditContext {
					context: None,
					doc: self,
					rid: rid.clone(),
					def: fd,
					stk,
					ctx,
					opt,
					old,
					inp,
				};
				// Process a potential `references` TYPE
				let res = field.process_refs_type().await?;
				if let Some(v) = res {
					// We found a `references` TYPE
					// No other clauses will be present, so no need to process them
					val = v;
				} else {
					// Skip this field?
					if !skipped {
						// Process any DEFAULT clause
						val = field.process_default_clause(val).await?;
						// Process any TYPE clause
						val = field.process_type_clause(val).await?;
						// Process any VALUE clause
						val = field.process_value_clause(val).await?;
						// Process any TYPE clause
						val = field.process_type_clause(val).await?;
						// Process any ASSERT clause
						val = field.process_assert_clause(val).await?;
						// Process any REFERENCE clause
						field.process_reference_clause(&val).await?;
					}
				}
				// Process any PERMISSIONS clause
				val = field.process_permissions_clause(val).await?;
				// Skip this field?
				if !skipped {
					// If the field is empty, mark child fields as skippable
					if val.is_none() && fd.kind.as_ref().is_some_and(Kind::can_be_none) {
						skip = Some(&fd.name);
					}
					// Set the new value of the field, or delete it if empty
					match val.is_none() {
						false => self.current.doc.to_mut().put(&k, val),
						true => self.current.doc.to_mut().cut(&k),
					};
				}
			}
		}
		// Carry on
		Ok(())
	}
}

struct FieldEditContext<'a> {
	/// The mutable request context
	context: Option<MutableContext>,
	/// The defined field statement
	def: &'a DefineFieldStatement,
	/// The current request stack
	stk: &'a mut Stk,
	/// The current request context
	ctx: &'a Context,
	/// The current request options
	opt: &'a Options,
	/// The current document record being processed
	doc: &'a Document,
	/// The record id of the document that we are processing
	rid: Arc<Thing>,
	/// The initial value of the field before being modified
	old: Arc<Value>,
	/// The user input value of the field edited by the user
	inp: Arc<Value>,
}

enum RefAction<'a> {
	Set(&'a Thing),
	Delete(Vec<&'a Thing>, String),
	Ignore,
}

impl<'a> FieldEditContext<'a> {
	/// Process any TYPE clause for the field definition
	async fn process_type_clause(&self, val: Value) -> Result<Value, Error> {
		// Check for a TYPE clause
		if let Some(kind) = &self.def.kind {
			// Check if this is the `id` field
			if self.def.name.is_id() {
				// Ensure that the outer value is a record
				if let Value::Thing(ref id) = val {
					// See if we should check the inner type
					if !kind.is_record() {
						// Get the value of the ID only
						let inner = Value::from(id.id.clone());
						// Check the type of the ID part
						inner.coerce_to(kind).map_err(|e| match e {
							// There was a conversion error
							Error::CoerceTo {
								from,
								..
							} => Error::FieldCheck {
								thing: self.rid.to_string(),
								field: self.def.name.clone(),
								check: kind.to_string(),
								value: from.to_string(),
							},
							// There was a different error
							e => e,
						})?;
					}
				}
				// The outer value should be a record
				else {
					// There was a field check error
					return Err(Error::FieldCheck {
						thing: self.rid.to_string(),
						field: self.def.name.clone(),
						check: kind.to_string(),
						value: val.to_string(),
					});
				}
			}
			// This is not the `id` field
			else {
				// Check the type of the field value
				let val = val.coerce_to(kind).map_err(|e| match e {
					// There was a conversion error
					Error::CoerceTo {
						from,
						..
					} => Error::FieldCheck {
						thing: self.rid.to_string(),
						field: self.def.name.clone(),
						check: kind.to_string(),
						value: from.to_string(),
					},
					// There was a different error
					e => e,
				})?;
				// Return the modified value
				return Ok(val);
			}
		}
		// Return the original value
		Ok(val)
	}
	/// Process any DEFAULT clause for the field definition
	async fn process_default_clause(&mut self, val: Value) -> Result<Value, Error> {
		// This field has a value specified
		if !val.is_none() {
			return Ok(val);
		}
		// The document is not being created
		if !self.doc.is_new() {
			return Ok(val);
		}
		// Get the default value
		let def = match &self.def.default {
			Some(v) => Some(v),
			_ => match &self.def.value {
				// The VALUE clause doesn't
				Some(v) if v.is_static() => Some(v),
				_ => None,
			},
		};
		// Check for a DEFAULT clause
		if let Some(expr) = def {
			// Arc the current value
			let now = Arc::new(val);
			// Get the current document
			let doc = Some(&self.doc.current);
			// Configure the context
			let ctx = match self.context.take() {
				Some(mut ctx) => {
					ctx.add_value("after", now.clone());
					ctx.add_value("value", now);
					ctx
				}
				None => {
					let mut ctx = MutableContext::new(self.ctx);
					ctx.add_value("before", self.old.clone());
					ctx.add_value("input", self.inp.clone());
					ctx.add_value("after", now.clone());
					ctx.add_value("value", now);
					ctx
				}
			};
			// Freeze the new context
			let ctx = ctx.freeze();
			// Process the VALUE clause
			let val = expr.compute(self.stk, &ctx, self.opt, doc).await?;
			// Unfreeze the new context
			self.context = Some(MutableContext::unfreeze(ctx)?);
			// Return the modified value
			return Ok(val);
		}
		// Return the original value
		Ok(val)
	}
	/// Process any VALUE clause for the field definition
	async fn process_value_clause(&mut self, val: Value) -> Result<Value, Error> {
		// Check for a VALUE clause
		if let Some(expr) = &self.def.value {
			// Arc the current value
			let now = Arc::new(val);
			// Get the current document
			let doc = Some(&self.doc.current);
			// Configure the context
			let ctx = match self.context.take() {
				Some(mut ctx) => {
					ctx.add_value("after", now.clone());
					ctx.add_value("value", now);
					ctx
				}
				None => {
					let mut ctx = MutableContext::new(self.ctx);
					ctx.add_value("before", self.old.clone());
					ctx.add_value("input", self.inp.clone());
					ctx.add_value("after", now.clone());
					ctx.add_value("value", now);
					ctx
				}
			};
			// Freeze the new context
			let ctx = ctx.freeze();
			// Process the VALUE clause
			let val = expr.compute(self.stk, &ctx, self.opt, doc).await?;
			// Unfreeze the new context
			self.context = Some(MutableContext::unfreeze(ctx)?);
			// Return the modified value
			return Ok(val);
		}
		// Return the original value
		Ok(val)
	}
	/// Process any ASSERT clause for the field definition
	async fn process_assert_clause(&mut self, val: Value) -> Result<Value, Error> {
		// If the field TYPE is optional, and the
		// field value was not set or is NONE we
		// ignore any defined ASSERT clause.
		if val.is_none() && self.def.kind.as_ref().is_some_and(Kind::can_be_none) {
			return Ok(val);
		}
		// Check for a ASSERT clause
		if let Some(expr) = &self.def.assert {
			// Arc the current value
			let now = Arc::new(val.clone());
			// Get the current document
			let doc = Some(&self.doc.current);
			// Configure the context
			let ctx = match self.context.take() {
				Some(mut ctx) => {
					ctx.add_value("after", now.clone());
					ctx.add_value("value", now.clone());
					ctx
				}
				None => {
					let mut ctx = MutableContext::new(self.ctx);
					ctx.add_value("before", self.old.clone());
					ctx.add_value("input", self.inp.clone());
					ctx.add_value("after", now.clone());
					ctx.add_value("value", now.clone());
					ctx
				}
			};
			// Freeze the new context
			let ctx = ctx.freeze();
			// Process the ASSERT clause
			let res = expr.compute(self.stk, &ctx, self.opt, doc).await?;
			// Unfreeze the new context
			self.context = Some(MutableContext::unfreeze(ctx)?);
			// Check the ASSERT clause result
			if !res.is_truthy() {
				return Err(Error::FieldValue {
					thing: self.rid.to_string(),
					field: self.def.name.clone(),
					check: expr.to_string(),
					value: now.to_string(),
				});
			}
		}
		// Return the original value
		Ok(val)
	}
	/// Process any PERMISSIONS clause for the field definition
	async fn process_permissions_clause(&mut self, val: Value) -> Result<Value, Error> {
		// Check for a PERMISSIONS clause
		if self.opt.check_perms(Action::Edit)? {
			// Get the permission clause
			let perms = if self.doc.is_new() {
				&self.def.permissions.create
			} else {
				&self.def.permissions.update
			};
			// Match the permission clause
			let val = match perms {
				// The field PERMISSIONS clause
				// is FULL, enabling this field
				// to be updated without checks.
				Permission::Full => val,
				// The field PERMISSIONS clause
				// is NONE, meaning that this
				// change will be reverted.
				Permission::None => match val.eq(&self.old) {
					false => self.old.as_ref().clone(),
					true => val,
				},
				// The field PERMISSIONS clause
				// is a custom expression, so
				// we check the expression and
				// revert the field if denied.
				Permission::Specific(expr) => {
					// Arc the current value
					let now = Arc::new(val.clone());
					// Get the current document
					let doc = Some(&self.doc.current);
					// Disable permissions
					let opt = &self.opt.new_with_perms(false);
					// Configure the context
					// Configure the context
					let ctx = match self.context.take() {
						Some(mut ctx) => {
							ctx.add_value("after", now.clone());
							ctx.add_value("value", now);
							ctx
						}
						None => {
							let mut ctx = MutableContext::new(self.ctx);
							ctx.add_value("before", self.old.clone());
							ctx.add_value("input", self.inp.clone());
							ctx.add_value("after", now.clone());
							ctx.add_value("value", now);
							ctx
						}
					};
					// Freeze the new context
					let ctx = ctx.freeze();
					// Process the PERMISSION clause
					let res = expr.compute(self.stk, &ctx, opt, doc).await?;
					// Unfreeze the new context
					self.context = Some(MutableContext::unfreeze(ctx)?);
					// If the specific permissions
					// expression was not truthy,
					// then this field could not be
					// updated, meanint that this
					// change will be reverted.
					match res.is_truthy() {
						false => match val.eq(&self.old) {
							false => self.old.as_ref().clone(),
							true => val,
						},
						true => val,
					}
				}
			};
			// Return the modified value
			return Ok(val);
		}
		// Return the original value
		Ok(val)
	}
	/// Process any REFERENCE clause for the field definition
	async fn process_reference_clause(&mut self, val: &Value) -> Result<(), Error> {
		if !self.ctx.get_capabilities().allows_experimental(&ExperimentalTarget::RecordReferences) {
			return Ok(());
		}

		// Is there a `REFERENCE` clause?
		if self.def.reference.is_some() {
			let doc = Some(&self.doc.current);
			let old = self.old.as_ref();

			// If the value has not changed, there is no need to update any references
			let action = if val == old {
				RefAction::Ignore
			// Check if the old value was a record id
			} else if let Value::Thing(thing) = old {
				// We need to check if this reference is contained in an array
				let others = self
					.doc
					.current
					.doc
					.get(self.stk, self.ctx, self.opt, doc, &self.def.name)
					.await?;
				// If the reference is contained in an array, we only delete it from the array
				// if there is no other reference to the same record id in the array
				if let Value::Array(arr) = others {
					if arr.iter().any(|v| v == old) {
						RefAction::Ignore
					} else {
						RefAction::Delete(vec![thing], self.def.name.to_string())
					}
				} else {
					// Otherwise we delete the reference
					RefAction::Delete(vec![thing], self.def.name.to_string())
				}
			} else if let Value::Array(oldarr) = old {
				// If the new value is still an array, we only filter out the record ids that are not present in the new array
				let removed = if let Value::Array(newarr) = val {
					oldarr
						.iter()
						.filter_map(|v| {
							// If the record id is still present in the new array, we do not remove the reference
							if newarr.contains(v) {
								None
							} else if let Value::Thing(thing) = v {
								Some(thing)
							} else {
								None
							}
						})
						.collect()

				// If the new value is not an array, then all record ids in the old array are removed
				} else {
					oldarr
						.iter()
						.filter_map(|v| {
							if let Value::Thing(thing) = v {
								Some(thing)
							} else {
								None
							}
						})
						.collect()
				};

				RefAction::Delete(removed, self.def.name.to_owned().push(Part::All).to_string())
			// We found a new reference, let's create the link
			} else if let Value::Thing(thing) = val {
				RefAction::Set(thing)
			} else {
				// This value is not a record id, nothing to process
				// This can be a containing array for record ids, for example
				RefAction::Ignore
			};

			// Process the action
			match action {
				// Nothing to process
				RefAction::Ignore => Ok(()),
				// Create the reference, if it does not exist yet.
				RefAction::Set(thing) => {
					let key = crate::key::r#ref::new(
						self.opt.ns()?,
						self.opt.db()?,
						&thing.tb,
						&thing.id,
						&self.rid.tb,
						&self.def.name.to_string(),
						&self.rid.id,
					)
					.encode()
					.unwrap();

					self.ctx.tx().set(key, vec![], None).await?;

					Ok(())
				}
				// Delete the reference, if it exists
				RefAction::Delete(things, ff) => {
					for thing in things {
						let key = crate::key::r#ref::new(
							self.opt.ns()?,
							self.opt.db()?,
							&thing.tb,
							&thing.id,
							&self.rid.tb,
							&ff,
							&self.rid.id,
						)
						.encode()
						.unwrap();

						self.ctx.tx().del(key).await?;
					}

					Ok(())
				}
			}
		} else {
			Ok(())
		}
	}
	/// Process any `TYPE reference` clause for the field definition
	async fn process_refs_type(&mut self) -> Result<Option<Value>, Error> {
		if !self.ctx.get_capabilities().allows_experimental(&ExperimentalTarget::RecordReferences) {
			return Ok(None);
		}

		let refs = match &self.def.kind {
			// We found a reference type for this field
			// In this case, we force the value to be a reference
			Some(Kind::References(ft, ff)) => Refs(vec![(ft.clone(), ff.clone())]),
			Some(Kind::Either(kinds)) => {
				if !kinds.iter().all(|k| matches!(k, Kind::References(_, _))) {
					return Ok(None);
				}

				// Extract all reference types
				let pairs: Vec<_> = kinds
					.iter()
					.filter_map(|k| {
						if let Kind::References(ft, ff) = k {
							Some((ft.clone(), ff.clone()))
						} else {
							None
						}
					})
					.collect();

				// If the length does not match, there were non-reference types
				if pairs.len() != kinds.len() {
					return Err(Error::RefsMismatchingVariants);
				}

				// All ok
				Refs(pairs)
			}
			// This is not a reference type, continue as normal
			_ => return Ok(None),
		};

		Ok(Some(Value::Refs(refs)))
	}
}
