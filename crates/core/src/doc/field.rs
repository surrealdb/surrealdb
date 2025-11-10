use std::sync::Arc;

use anyhow::{Result, bail, ensure};
use reblessive::tree::Stk;

use crate::catalog::{self, FieldDefinition};
use crate::ctx::{Context, MutableContext};
use crate::dbs::capabilities::ExperimentalTarget;
use crate::dbs::{Options, Statement};
use crate::doc::Document;
use crate::err::Error;
use crate::expr::data::Data;
use crate::expr::idiom::{Idiom, IdiomTrie, IdiomTrieContains};
use crate::expr::kind::Kind;
use crate::expr::{FlowResultExt as _, Part};
use crate::iam::Action;
use crate::val::value::CoerceError;
use crate::val::value::every::ArrayBehaviour;
use crate::val::{RecordId, Value};

/// Removes `NONE` values recursively from objects, but not when `NONE` is a direct child of an
/// array
fn clean_none(v: &mut Value) -> bool {
	match v {
		Value::None => false,
		Value::Object(o) => {
			o.retain(|_, v| clean_none(v));
			true
		}
		Value::Array(x) => {
			x.iter_mut().for_each(|x| {
				clean_none(x);
			});
			true
		}
		_ => true,
	}
}

impl Document {
	/// Ensures that any remaining fields on a
	/// SCHEMAFULL table are cleaned up and removed.
	pub(super) async fn cleanup_table_fields(
		&mut self,
		ctx: &Context,
		opt: &Options,
		_stm: &Statement<'_>,
	) -> Result<()> {
		// Get the table
		let tb = self.tb(ctx, opt).await?;

		// This table is schemafull
		if tb.schemafull {
			// Prune unspecified fields from the document that are not defined via
			// `DefineFieldStatement`s.

			// Create a trie to store which fields are defined and allow nested fields
			let mut defined_field_names = IdiomTrie::new();
			// Create a set to track explicitly defined fields
			let mut explicitly_defined = std::collections::HashSet::new();
			// Create a set to track which fields preserve their nested values
			let mut preserve_nested = std::collections::HashSet::new();

			// First pass: collect all explicitly defined field names
			let mut explicit_field_names = std::collections::HashSet::new();
			for fd in self.fd(ctx, opt).await?.iter() {
				explicit_field_names.insert(fd.name.clone());
			}

			// Loop through all field definitions
			for fd in self.fd(ctx, opt).await?.iter() {
				let is_literal = fd.field_kind.as_ref().is_some_and(Kind::contains_literal);
				let is_any = fd.field_kind.as_ref().is_some_and(Kind::is_any);

				// Check if the kind contains object (including option<object>, array<object>, etc.)
				fn kind_contains_object(kind: &Kind) -> bool {
					match kind {
						Kind::Object => true,
						Kind::Either(kinds) => kinds.iter().any(kind_contains_object),
						Kind::Array(inner, _) | Kind::Set(inner, _) => kind_contains_object(inner),
						_ => false,
					}
				}
				let contains_object = fd.field_kind.as_ref().is_some_and(kind_contains_object);

				// In SCHEMAFULL tables:
				// - TYPE any: allows nested (inherent)
				// - TYPE containing object without FLEXIBLE: strict (does NOT allow arbitrary
				//   nested)
				// - TYPE containing object with FLEXIBLE: allows nested
				// - Literal types: allow nested
				let allows_nested = is_any || is_literal || (contains_object && fd.flexible);

				// Preserve nested fields if they allow nested
				let should_preserve = allows_nested;

				// Expand the field name to actual document values (handles array wildcards)
				// and insert each into the trie
				for k in self.current.doc.as_ref().each(&fd.name).into_iter() {
					// Insert the field - mark as allowing nested based on calculation above
					defined_field_names.insert(&k, allows_nested);
					// Track this as an explicitly defined field
					explicitly_defined.insert(k.clone());
					// Track if this field preserves nested values
					if should_preserve {
						preserve_nested.insert(k.clone());
					}
					// Also insert all ancestor paths
					// BUT only mark them as allowing nested if they don't have their own explicit
					// definition
					for i in 1..k.len() {
						let ancestor = Idiom(k[..i].to_vec());
						if !explicit_field_names.contains(&ancestor) {
							// This ancestor doesn't have an explicit definition, treat as
							// schemaless object
							defined_field_names.insert(&k[..i], true);
							if should_preserve {
								preserve_nested.insert(k[..i].to_vec().into());
							}
						}
					}
				}
			}

			// Loop over every field in the document
			let mut fields_to_remove = Vec::new();
			for current_doc_field_idiom in
				self.current.doc.as_ref().every(None, true, ArrayBehaviour::Full).iter()
			{
				if current_doc_field_idiom.is_special() {
					// This field is a built-in field, so we can skip it.
					continue;
				}

				// Check if the field is defined in the schema
				match defined_field_names.contains(current_doc_field_idiom) {
					IdiomTrieContains::Exact(_) => {
						// This field exists in the trie. Check if it's explicitly defined.
						if !explicitly_defined.contains(current_doc_field_idiom) {
							// This is an ancestor path, not an explicit field definition.
							// Check if we should preserve it:
							// 1. If any ancestor preserves nested fields
							// 2. If this field has explicitly defined descendants
							let mut should_preserve = false;

							// Check ancestors
							for i in 1..=current_doc_field_idiom.len() {
								let ancestor = Idiom(current_doc_field_idiom[..i].to_vec());
								if preserve_nested.contains(&ancestor) {
									should_preserve = true;
									break;
								}
							}

							// Check if this field has explicitly defined descendants
							if !should_preserve {
								for explicit_field in explicitly_defined.iter() {
									if explicit_field.starts_with(current_doc_field_idiom)
										&& explicit_field.len() > current_doc_field_idiom.len()
									{
										should_preserve = true;
										break;
									}
								}
							}

							if !should_preserve {
								// Ancestor field is not preserved and has no defined children -
								// remove it
								fields_to_remove.push(current_doc_field_idiom.clone());
							}
						}
						// Otherwise, it's explicitly defined, keep it
						continue;
					}
					IdiomTrieContains::Ancestor(true) => {
						// This field is not explicitly defined, but it is a child of a field
						// that allows nested values. Check if the parent preserves nested fields.
						// Look for any ancestor in preserve_nested set
						let mut should_preserve = false;
						for i in 1..=current_doc_field_idiom.len() {
							let ancestor = Idiom(current_doc_field_idiom[..i].to_vec());
							if preserve_nested.contains(&ancestor) {
								should_preserve = true;
								break;
							}
						}
						if !should_preserve {
							// Nested fields are allowed but not preserved - remove them
							fields_to_remove.push(current_doc_field_idiom.clone());
						}
					}
					IdiomTrieContains::Ancestor(false) => {
						if let Some(part) = current_doc_field_idiom.last() {
							// This field is an array index, so it is automatically allowed.
							if part.is_index() {
								// This field is an array index, so we can skip it.
								continue;
							}
						}

						// This field is not explicitly defined in the schema or it is not a child
						// of a flex field.
						bail!(Error::FieldUndefined {
							table: tb.name.clone(),
							field: current_doc_field_idiom.to_owned(),
						});
					}

					IdiomTrieContains::None => {
						// This field is not explicitly defined in the schema or it is not a child
						// of a flex field.
						bail!(Error::FieldUndefined {
							table: tb.name.clone(),
							field: current_doc_field_idiom.to_owned(),
						});
					}
				}
			}

			// Remove fields that were marked for removal
			for field in fields_to_remove {
				self.current.doc.to_mut().cut(&field);
			}
		}

		// Loop over every field in the document
		// NONE values should never be stored
		clean_none(self.current.doc.to_mut());
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
	) -> Result<()> {
		// Check import
		if opt.import {
			return Ok(());
		}
		// Get the record id
		let rid = self.id()?;
		// Get the user applied input
		let inp = self.compute_input_value(stk, ctx, opt, stm).await?.unwrap_or_default();
		// When set, any matching embedded object fields
		// which are prefixed with the specified idiom
		// will be skipped, as the parent object is optional
		let mut skip: Option<&Idiom> = None;
		// Loop through all field statements
		for fd in self.fd(ctx, opt).await?.iter() {
			// Check if we should skip this field
			let skipped = match skip {
				// We are skipping a parent field
				// Check if this field is a child field
				Some(inner) => fd.name.starts_with(inner),
				None => false,
			};

			// Let's stop skipping fields if not
			// Specify whether we should skip
			if !skipped {
				skip = None;
			}

			// Loop over each field in document
			for (k, mut val) in self.current.doc.as_ref().walk(&fd.name).into_iter() {
				// Get the initial value
				let old = Arc::new(self.initial.doc.as_ref().pick(&k));
				// Get the input value
				let inp = Arc::new(inp.pick(&k));
				// Check for the `id` field
				if fd.name.is_id() {
					ensure!(
						self.is_new() || val == *old,
						Error::FieldReadonly {
							field: fd.name.clone(),
							record: rid.to_string(),
						}
					);

					if !self.is_new() {
						continue;
					}
				}
				// If the field is READONLY then we
				// will check that the field has not
				// been modified. If it has just been
				// omitted then we reset it, otherwise
				// we throw a field readonly error.
				//
				// Check if we are updating the
				// document, and check if the new
				// field value is now different to
				// the old field value in any way.
				if fd.readonly && !self.is_new() {
					if val.ne(&*old) {
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
								bail!(Error::FieldReadonly {
									field: fd.name.clone(),
									record: rid.to_string(),
								});
							}
						}
					}
					// If this field was not modified then
					// we can continue without needing to
					// process the field in any other way.
					continue;
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
					user_input: inp,
				};
				// Skip this field?
				if !skipped {
					if field.def.computed.is_some() {
						// The value will be computed by the `COMPUTED` clause, so we set it to NONE
						val = Value::None;
					} else {
						// Process any DEFAULT clause
						val = field.process_default_clause(val).await?;
						// Check for the existance of a VALUE clause
						if field.def.value.is_some() {
							// If the value is NONE (field doesn't exist), process VALUE first
							// Otherwise, do TYPE check first to validate explicit input
							if val.is_none() {
								// Process any VALUE clause first when field is missing
								val = field.process_value_clause(val).await?;
								// Process any TYPE clause
								val = field.process_type_clause(val).await?;
							} else {
								// Process any TYPE clause first for explicit values
								val = field.process_type_clause(val).await?;
								// Process any VALUE clause
								val = field.process_value_clause(val).await?;
							}
						} else {
							// Process any TYPE clause
							val = field.process_type_clause(val).await?;
						}
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
					if val.is_none() && fd.field_kind.as_ref().is_some_and(Kind::can_be_none) {
						skip = Some(&fd.name);
					}
					// Set the new value of the field, or delete it if empty
					self.current.doc.to_mut().put(&k, val);
				}
			}
		}
		// Carry on
		Ok(())
	}
	/// Processes `DEFINE FIELD` statements which
	/// have been defined on the table for this
	/// record, with a `REFERENCE` clause, and remove
	/// all possible references this record has made.
	pub(super) async fn cleanup_table_references(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
	) -> Result<()> {
		// Check import
		if opt.import {
			return Ok(());
		}
		// Get the record id
		let rid = self.id()?;
		// Loop through all field statements
		for fd in self.fd(ctx, opt).await?.iter() {
			// Only process reference fields
			if fd.reference.is_none() {
				continue;
			}

			// Loop over each value in document
			for (_, val) in self.current.doc.as_ref().walk(&fd.name).into_iter() {
				// Skip if the value is empty
				if val.is_none() || val.is_empty_array() {
					continue;
				}

				// Prepare the field edit context
				let mut field = FieldEditContext {
					context: None,
					doc: self,
					rid: rid.clone(),
					def: fd,
					stk,
					ctx,
					opt,
					old: val.into(),
					user_input: Value::None.into(),
				};

				// Pass an empty value to delete all the existing references
				field.process_reference_clause(&Value::None).await?;
			}
		}

		Ok(())
	}
}

struct FieldEditContext<'a> {
	/// The mutable request context
	context: Option<MutableContext>,
	/// The defined field statement
	def: &'a FieldDefinition,
	/// The current request stack
	stk: &'a mut Stk,
	/// The current request context
	ctx: &'a Context,
	/// The current request options
	opt: &'a Options,
	/// The current document record being processed
	doc: &'a Document,
	/// The record id of the document that we are processing
	rid: Arc<RecordId>,
	/// The initial value of the field before being modified
	old: Arc<Value>,
	/// The user input value of the field edited by the user
	user_input: Arc<Value>,
}

enum RefAction<'a> {
	Set(&'a RecordId, String),
	Delete(&'a RecordId, String),
}

impl FieldEditContext<'_> {
	/// Process any TYPE clause for the field definition
	async fn process_type_clause(&self, val: Value) -> Result<Value> {
		// Check for a TYPE clause
		if let Some(kind) = &self.def.field_kind {
			// Check if this is the `id` field
			if self.def.name.is_id() {
				// Ensure that the outer value is a record
				if let Value::RecordId(ref id) = val {
					// See if we should check the inner type
					if !kind.is_record() {
						// Get the value of the ID only
						let inner = id.key.clone().into_value();

						// Check the type of the ID part
						inner.coerce_to_kind(kind).map_err(|e| Error::FieldCoerce {
							record: self.rid.to_string(),
							field_name: self.def.name.to_string(),
							error: Box::new(e),
						})?;
					}
				}
				// The outer value should be a record
				else {
					// There was a field check error
					bail!(Error::FieldCoerce {
						record: self.rid.to_string(),
						field_name: "id".to_string(),
						error: Box::new(CoerceError::InvalidKind {
							from: val,
							into: "record".to_string(),
						}),
					});
				}
			}
			// This is not the `id` field
			else {
				// Check the type of the field value
				let val = val.coerce_to_kind(kind).map_err(|e| Error::FieldCoerce {
					record: self.rid.to_string(),
					field_name: self.def.name.to_string(),
					error: Box::new(e),
				})?;
				// Return the modified value
				return Ok(val);
			}
		}
		// Return the original value
		Ok(val)
	}
	/// Process any DEFAULT clause for the field definition
	async fn process_default_clause(&mut self, val: Value) -> Result<Value> {
		// This field has a value specified
		if !val.is_none() {
			return Ok(val);
		}
		// The document is not being created
		if !self.doc.is_new() && !matches!(self.def.default, catalog::DefineDefault::Always(_)) {
			return Ok(val);
		}
		// Get the default value
		let def = match &self.def.default {
			catalog::DefineDefault::Set(v) | catalog::DefineDefault::Always(v) => Some(v),
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
					ctx.add_value("input", self.user_input.clone());
					ctx.add_value("after", now.clone());
					ctx.add_value("value", now);
					ctx
				}
			};
			// Freeze the new context
			let ctx = ctx.freeze();
			// Process the VALUE clause
			let val =
				self.stk.run(|stk| expr.compute(stk, &ctx, self.opt, doc)).await.catch_return()?;
			// Unfreeze the new context
			self.context = Some(MutableContext::unfreeze(ctx)?);
			// Return the modified value
			return Ok(val);
		}
		// Return the original value
		Ok(val)
	}
	/// Process any VALUE clause for the field definition
	async fn process_value_clause(&mut self, val: Value) -> Result<Value> {
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
					ctx.add_value("input", self.user_input.clone());
					ctx.add_value("after", now.clone());
					ctx.add_value("value", now);
					ctx
				}
			};
			// Freeze the new context
			let ctx = ctx.freeze();
			// Process the VALUE clause
			let val =
				self.stk.run(|stk| expr.compute(stk, &ctx, self.opt, doc)).await.catch_return()?;
			// Unfreeze the new context
			self.context = Some(MutableContext::unfreeze(ctx)?);
			// Return the modified value
			return Ok(val);
		}
		// Return the original value
		Ok(val)
	}
	/// Process any ASSERT clause for the field definition
	async fn process_assert_clause(&mut self, val: Value) -> Result<Value> {
		// If the field TYPE is optional, and the
		// field value was not set or is NONE we
		// ignore any defined ASSERT clause.
		if val.is_none() && self.def.field_kind.as_ref().is_some_and(Kind::can_be_none) {
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
					ctx.add_value("input", self.user_input.clone());
					ctx.add_value("after", now.clone());
					ctx.add_value("value", now.clone());
					ctx
				}
			};
			// Freeze the new context
			let ctx = ctx.freeze();
			// Process the ASSERT clause
			let res =
				self.stk.run(|stk| expr.compute(stk, &ctx, self.opt, doc)).await.catch_return()?;
			// Unfreeze the new context
			self.context = Some(MutableContext::unfreeze(ctx)?);
			// Check the ASSERT clause result
			ensure!(
				res.is_truthy(),
				Error::FieldValue {
					record: self.rid.to_string(),
					field: self.def.name.clone(),
					check: expr.to_string(),
					value: now.to_string(),
				}
			);
		}
		// Return the original value
		Ok(val)
	}
	/// Process any PERMISSIONS clause for the field definition
	async fn process_permissions_clause(&mut self, val: Value) -> Result<Value> {
		// Check for a PERMISSIONS clause
		if self.opt.check_perms(Action::Edit)? {
			// Get the permission clause
			let perms = if self.doc.is_new() {
				&self.def.create_permission
			} else {
				&self.def.update_permission
			};
			// Match the permission clause
			let val = match perms {
				// The field PERMISSIONS clause
				// is FULL, enabling this field
				// to be updated without checks.
				catalog::Permission::Full => val,
				// The field PERMISSIONS clause
				// is NONE, meaning that this
				// change will be reverted.
				catalog::Permission::None => {
					if val != *self.old {
						self.old.as_ref().clone()
					} else {
						val
					}
				}
				// The field PERMISSIONS clause
				// is a custom expression, so
				// we check the expression and
				// revert the field if denied.
				catalog::Permission::Specific(expr) => {
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
							ctx.add_value("input", self.user_input.clone());
							ctx.add_value("after", now.clone());
							ctx.add_value("value", now);
							ctx
						}
					};
					// Freeze the new context
					let ctx = ctx.freeze();
					// Process the PERMISSION clause
					let res = self
						.stk
						.run(|stk| expr.compute(stk, &ctx, opt, doc))
						.await
						.catch_return()?;
					// Unfreeze the new context
					self.context = Some(MutableContext::unfreeze(ctx)?);
					// If the specific permissions
					// expression was not truthy,
					// then this field could not be
					// updated, meanint that this
					// change will be reverted.
					if res.is_truthy() || val == *self.old {
						val
					} else {
						self.old.as_ref().clone()
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
	async fn process_reference_clause(&mut self, val: &Value) -> Result<()> {
		if !self.ctx.get_capabilities().allows_experimental(&ExperimentalTarget::RecordReferences) {
			return Ok(());
		}

		// Is there a `REFERENCE` clause?
		if self.def.reference.is_some() {
			let doc = Some(&self.doc.current);
			let old = self.old.as_ref();

			// The current value might be contained inside an array of references
			// Try to find other references with a similar path to the current one
			let mut check_others = async || -> Result<Vec<Value>> {
				let others = self
					.doc
					.current
					.doc
					.as_ref()
					.get(self.stk, self.ctx, self.opt, doc, &self.def.name)
					.await
					.catch_return()?;

				if let Value::Array(arr) = others {
					Ok(arr.0)
				} else {
					Ok(vec![])
				}
			};

			// Check if the value has actually changed
			if old == val {
				// Nothing changed
				return Ok(());
			}

			let mut actions = vec![];

			// A value might be contained inside an array of references
			// If so, we skip it. Otherwise, we delete the reference.
			if let Value::RecordId(rid) = old {
				let others = check_others().await?;
				if !others.iter().any(|v| v == old) {
					actions.push(RefAction::Delete(rid, self.def.name.to_string()));
				}
			}

			// New references, wether on their own or inside an array
			// are always processed through here. Always add the new reference
			// if the key already exists it will just overwrite which is fine.
			if let Value::RecordId(rid) = val {
				actions.push(RefAction::Set(rid, self.def.name.to_string()));
			}

			// Values removed from an array are not always processed via the above
			// Try to delete the references here where needed
			if let Value::Array(oldarr) = old {
				// For array based references, we always store the foreign field as the nested field
				let ff = self.def.name.clone().push(Part::All).to_string();
				// If the new value is still an array, we only filter out the record ids that
				// are not present in the new array
				if let Value::Array(newarr) = val {
					for old_rid in oldarr.iter() {
						if newarr.contains(old_rid) {
							continue;
						}

						if let Value::RecordId(rid) = old_rid {
							actions.push(RefAction::Delete(rid, ff.clone()));
						}
					}

					// If the new value is not an array, then all record ids in the old array are
					// removed
				} else {
					for old_rid in oldarr.iter() {
						if let Value::RecordId(rid) = old_rid {
							actions.push(RefAction::Delete(rid, ff.clone()));
						}
					}
				}
			}

			// Process the actions
			for action in actions.into_iter() {
				match action {
					RefAction::Set(rid, ff) => {
						let (ns, db) = self.ctx.expect_ns_db_ids(self.opt).await?;
						let key = crate::key::r#ref::new(
							ns,
							db,
							&rid.table,
							&rid.key,
							&self.rid.table,
							&self.rid.key,
							&ff,
						);

						self.ctx.tx().set(&key, &(), None).await?;
					}
					RefAction::Delete(rid, ff) => {
						let (ns, db) = self.ctx.expect_ns_db_ids(self.opt).await?;
						let key = crate::key::r#ref::new(
							ns,
							db,
							&rid.table,
							&rid.key,
							&self.rid.table,
							&self.rid.key,
							&ff,
						);

						self.ctx.tx().del(&key).await?;
					}
				}
			}
		}

		Ok(())
	}
}
