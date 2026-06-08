use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{Result, bail, ensure};
use reblessive::tree::Stk;
use surrealdb_types::ToSql;

use crate::catalog::{self, FieldDefinition};
use crate::ctx::{Context, FrozenContext};
use crate::dbs::{Options, Statement};
use crate::doc::Document;
use crate::err::Error;
use crate::expr::FlowResultExt as _;
use crate::expr::data::Data;
use crate::expr::idiom::{Idiom, IdiomTrie, IdiomTrieContains};
use crate::expr::kind::{Kind, KindLiteral};
use crate::iam::{Action, AuthLimit};
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
	/// Removes undefined fields from SCHEMAFULL tables and cleans all NONE values.
	///
	/// For records in SCHEMAFULL tables, this function will:
	/// - Remove fields not explicitly defined via DEFINE FIELD
	/// - Leaves special fields (`id`, `in`, `out`) in place
	/// - Leaves fields with FLEXIBLE modifier untouched
	/// - Leaves nested fields under TYPE any untouched
	/// - Leaves literal type fields untouched
	///
	/// For all records, this function will:
	/// - Recursively remove all NONE values from the document
	/// - Leaves array elements which are NONE untouched
	pub(super) fn cleanup_table_fields(&mut self) -> Result<()> {
		// Get the table
		let tb = self.doc_ctx.tb()?;
		// This table is schemafull
		if tb.schemafull {
			// Prune unspecified fields from the document that are not defined via
			// `DefineFieldStatement`s.

			// Create a vector to store the keys
			let mut defined_field_names = IdiomTrie::new();

			// First pass: collect all explicitly defined field names
			let mut explicit_field_names = HashSet::new();
			for fd in self.doc_ctx.fd()?.iter() {
				explicit_field_names.insert(fd.name.clone());
			}

			// Check if the kind contains object (including option<object>, array<object>, etc.)
			fn kind_contains_object(kind: &Kind) -> bool {
				match kind {
					Kind::Object => true,
					Kind::Either(kinds) => kinds.iter().any(kind_contains_object),
					Kind::Array(inner, _) | Kind::Set(inner, _) => kind_contains_object(inner),
					Kind::Literal(KindLiteral::Object(_)) => true,
					Kind::Literal(KindLiteral::Array(x)) => x.iter().any(kind_contains_object),
					_ => false,
				}
			}

			// Loop through all field definitions
			for fd in self.doc_ctx.fd()?.iter() {
				// Check if the field type is an any
				let is_any = fd.field_kind.as_ref().is_some_and(Kind::is_any);
				// Check if the field type is a literal
				let is_literal = fd.field_kind.as_ref().is_some_and(Kind::contains_literal);
				// Check if the field type contains an object
				let contains_object = fd.field_kind.as_ref().is_some_and(kind_contains_object);
				// In SCHEMAFULL tables:
				// - TYPE any: allows nested
				// - TYPE literal: literal types allow nested
				// - TYPE containing object with FLEXIBLE: allows nested
				// - TYPE containing object without FLEXIBLE: does NOT allow nested
				let allows_nested = is_any || is_literal || (contains_object && fd.flexible);

				for k in self.current.doc.as_ref().each(&fd.name) {
					defined_field_names.insert(&k, allows_nested);

					// Also insert all ancestor paths
					// BUT only mark them as allowing nested if they don't have their own explicit
					// definition
					for i in 1..k.len() {
						let ancestor = Idiom(k[..i].to_vec());
						if !explicit_field_names.contains(&ancestor) {
							// This ancestor doesn't have an explicit definition, treat as
							// schemaless object
							defined_field_names.insert(&k[..i], true);
						}
					}
				}
			}

			// Loop over every field in the document
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
						// This field is defined in the schema, so we can skip it.
						continue;
					}
					IdiomTrieContains::Ancestor(true) => {
						// This field is not explicitly defined in the schema, but it is a child of
						// a flex or literal field. If the field is a child of a flex field,
						// then any nested fields are allowed. If the field is a child of a
						// literal field, then allow any fields as they will be caught during
						// coercion.
						continue;
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
						ensure!(
							!tb.schemafull,
							// If strict, then throw an error on an undefined field
							Error::FieldUndefined {
								table: tb.name.as_str().to_string(),
								field: current_doc_field_idiom.clone(),
							}
						);

						// Otherwise, delete the field silently and don't error
						self.current.doc.to_mut().cut(current_doc_field_idiom);
					}

					IdiomTrieContains::None => {
						// This field is not explicitly defined in the schema or it is not a child
						// of a flex field.
						ensure!(
							!tb.schemafull,
							// If strict, then throw an error on an undefined field
							Error::FieldUndefined {
								table: tb.name.as_str().to_string(),
								field: current_doc_field_idiom.clone(),
							}
						);

						// Otherwise, delete the field silently and don't error
						self.current.doc.to_mut().cut(current_doc_field_idiom);
					}
				}
			}
		}

		// Loop over every field in the document
		// NONE values should never be stored
		clean_none(self.current.doc.to_mut());
		// Carry on
		Ok(())
	}

	/// Processes all DEFINE FIELD statements for each matching field in the document.
	///
	/// Applies field-level logic in the following order:
	/// - READONLY keyword - prevents modification of readonly fields
	/// - DEFAULT clause - applies default values for missing fields on new records
	/// - TYPE clause - validates the field value against the field type
	/// - VALUE clause - sets or processes the field value
	/// - ASSERT clause - validates field constraints
	/// - REFERENCE clause - manages foreign key references
	/// - PERMISSIONS clause - enforces field-level permissions
	///
	/// Certain fields have special behaviors:
	/// - `id` field: readonly after creation, and enforced for existing records
	/// - Optional fields: child fields are skipped when parent is NONE and type allows it
	/// - READONLY fields: reverted to old value when omitted within CONTENT clause
	/// - COMPUTED fields: any value is removed, as computed fields are processed later
	pub(super) async fn process_table_fields(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
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
		for fd in self.doc_ctx.fd()?.iter() {
			// Limit auth
			let opt = AuthLimit::try_from(&fd.auth_limit)?.limit_opt(opt);
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
			for (k, mut val) in self.current.doc.as_ref().walk(&fd.name) {
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
							record: rid.to_sql(),
						}
					);

					if !self.is_new() {
						continue;
					}
				}
				// If the field is READONLY then we need to check
				// that the field has not been modified. If it has
				// just been omitted then we reset it, otherwise
				// we throw a field readonly error.
				//
				// Check if we are updating the document, and check
				// if the new field value is now different to the
				// old field value in any way.
				if fd.readonly && !self.is_new() {
					if val.ne(&*old) {
						// Check the data clause type
						match stm.data() {
							// If the field is NONE, and a CONTENT clause was
							// used, then we assume that the field was omitted
							// and we revert the value to the old value.
							Some(Data::ContentExpression(_)) if val.is_none() => {
								self.current
									.doc
									.to_mut()
									.set(stk, ctx, &opt, &k, old.as_ref().clone())
									.await?;
								continue;
							}
							// If the field has been modified and the user
							// didn't use a CONTENT clause, then this should
							// not be allowed, and we throw an error.
							_ => {
								bail!(Error::FieldReadonly {
									field: fd.name.clone(),
									record: rid.to_sql(),
								});
							}
						}
					}
					// If this field was not modified then we can continue
					// without needing to process the field in any other way.
					continue;
				}
				// Generate the field context
				let mut field = FieldEditContext {
					context: None,
					doc: self,
					rid: Arc::clone(&rid),
					def: fd,
					stk,
					ctx,
					opt: &opt,
					old,
					user_input: inp,
				};
				// Skip this field?
				if !skipped {
					// Check if this is a COMPUTED field
					if field.def.computed.is_some() {
						// The value will be computed later, so we set it to NONE
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
								// Re-validate that VALUE output conforms to TYPE
								val = field.process_type_clause(val).await?;
							}
						} else {
							// Process any TYPE clause
							val = field.process_type_clause(val).await?;
						}
						// Process any ASSERT clause
						val = field.process_assert_clause(val).await?;
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
					// Write the processed value back. `put` on a NONE
					// preserves array element positions; `cut` would
					// shrink the array, dropping nullable items the
					// caller meant to keep.
					self.current.doc.to_mut().put(&k, val);
				}
			}
		}
		// Note: COMPUTED fields are NOT evaluated here. Storing
		// computed values at write time produces incorrect behaviour
		// for selective projections — a computed field that the read
		// did not request must not be evaluated (issue #7094). The
		// `output_document!` macro on the read side invokes
		// `Document::compute_fields(needed_roots = ...)` after
		// reduction and only evaluates the closure of computed fields
		// the projection actually consumes.
		// Carry on
		Ok(())
	}

	/// Processes reference clauses for all REFERENCE fields on this table.
	/// Called after permission checks succeed so that reference keys are
	/// only written for operations that are actually permitted.
	pub(super) async fn process_table_references(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
	) -> Result<()> {
		// Check import
		if opt.import {
			return Ok(());
		}
		// Get the record id
		let rid = self.id()?;
		// Loop through all field statements
		for fd in self.doc_ctx.fd()?.iter() {
			// Only process reference fields
			if fd.reference.is_none() {
				continue;
			}

			// Limit auth
			let opt = AuthLimit::try_from(&fd.auth_limit)?.limit_opt(opt);

			// Loop over each field in the current document
			for (k, val) in self.current.doc.as_ref().walk(&fd.name) {
				// Get the initial value for diff comparison
				let old = Arc::new(self.initial.doc.as_ref().pick(&k));

				let mut field = FieldEditContext {
					context: None,
					doc: self,
					rid: Arc::clone(&rid),
					def: fd,
					stk,
					ctx,
					opt: &opt,
					old,
					user_input: Value::None.into(),
				};

				field.process_reference_clause(&val).await?;
			}
		}

		Ok(())
	}

	/// Processes `DEFINE FIELD` statements which
	/// have been defined on the table for this
	/// record, with a `REFERENCE` clause, and remove
	/// all possible references this record has made.
	pub(super) async fn cleanup_table_references(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
	) -> Result<()> {
		// Check import
		if opt.import {
			return Ok(());
		}
		// Get the record id
		let rid = self.id()?;
		// Loop through all field statements
		for fd in self.doc_ctx.fd()?.iter() {
			// Only process reference fields
			if fd.reference.is_none() {
				continue;
			}

			// Limit auth
			let opt = AuthLimit::try_from(&fd.auth_limit)?.limit_opt(opt);

			// Loop over each value in document
			for (_, val) in self.current.doc.as_ref().walk(&fd.name) {
				// Skip if the value is empty
				if val.is_none() || val.is_empty_array() {
					continue;
				}

				// Prepare the field edit context
				let mut field = FieldEditContext {
					context: None,
					doc: self,
					rid: Arc::clone(&rid),
					def: fd,
					stk,
					ctx,
					opt: &opt,
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
	context: Option<Context>,
	/// The defined field statement
	def: &'a FieldDefinition,
	/// The current request stack
	stk: &'a mut Stk,
	/// The current request context
	ctx: &'a FrozenContext,
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
	Set(&'a RecordId),
	Delete(&'a RecordId),
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
							record: self.rid.to_sql(),
							field_name: self.def.name.to_sql(),
							error: Box::new(e),
						})?;
					}
				}
				// The outer value should be a record
				else {
					// There was a field check error
					bail!(Error::FieldCoerce {
						record: self.rid.to_sql(),
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
					record: self.rid.to_sql(),
					field_name: self.def.name.to_sql(),
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
					ctx.add_value("after", Arc::clone(&now));
					ctx.add_value("value", now);
					ctx
				}
				None => {
					let mut ctx = Context::new_child(self.ctx);
					ctx.add_value("before", Arc::clone(&self.old));
					ctx.add_value("input", Arc::clone(&self.user_input));
					ctx.add_value("after", Arc::clone(&now));
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
			self.context = Some(Context::unfreeze(ctx)?);
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
					ctx.add_value("after", Arc::clone(&now));
					ctx.add_value("value", now);
					ctx
				}
				None => {
					let mut ctx = Context::new_child(self.ctx);
					ctx.add_value("before", Arc::clone(&self.old));
					ctx.add_value("input", Arc::clone(&self.user_input));
					ctx.add_value("after", Arc::clone(&now));
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
			self.context = Some(Context::unfreeze(ctx)?);
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
					ctx.add_value("after", Arc::clone(&now));
					ctx.add_value("value", Arc::clone(&now));
					ctx
				}
				None => {
					let mut ctx = Context::new_child(self.ctx);
					ctx.add_value("before", Arc::clone(&self.old));
					ctx.add_value("input", Arc::clone(&self.user_input));
					ctx.add_value("after", Arc::clone(&now));
					ctx.add_value("value", Arc::clone(&now));
					ctx
				}
			};
			// Freeze the new context
			let ctx = ctx.freeze();
			// Process the ASSERT clause
			let res =
				self.stk.run(|stk| expr.compute(stk, &ctx, self.opt, doc)).await.catch_return()?;
			// Unfreeze the new context
			self.context = Some(Context::unfreeze(ctx)?);
			// Check the ASSERT clause result
			ensure!(
				res.is_truthy(),
				Error::FieldValue {
					record: self.rid.to_sql(),
					field: self.def.name.clone(),
					check: expr.to_sql(),
					value: now.to_sql(),
				}
			);
		}
		// Return the original value
		Ok(val)
	}

	/// Process any PERMISSIONS clause for the field definition
	async fn process_permissions_clause(&mut self, val: Value) -> Result<Value> {
		// Check for a PERMISSIONS clause
		if self.ctx.check_perms(self.opt, Action::Edit)? {
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
							ctx.add_value("after", Arc::clone(&now));
							ctx.add_value("value", now);
							ctx
						}
						None => {
							let mut ctx = Context::new_child(self.ctx);
							ctx.add_value("before", Arc::clone(&self.old));
							ctx.add_value("input", Arc::clone(&self.user_input));
							ctx.add_value("after", Arc::clone(&now));
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
					self.context = Some(Context::unfreeze(ctx)?);
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
		// Is there a `REFERENCE` clause?
		if self.def.reference.is_some() {
			// Check if the value has actually changed
			let old = self.old.as_ref();
			if old == val {
				// Nothing changed
				return Ok(());
			}

			// Create a vector to store the actions
			let mut actions = vec![];

			fn collect_rids(v: &Value) -> HashSet<&RecordId> {
				match v {
					Value::Array(arr) => {
						arr.iter().filter_map(|v| v.as_record()).collect::<HashSet<_>>()
					}
					Value::Set(set) => {
						set.iter().filter_map(|v| v.as_record()).collect::<HashSet<_>>()
					}
					Value::RecordId(rid) => HashSet::from([rid]),
					_ => HashSet::new(),
				}
			}

			let old = collect_rids(old);
			let new = collect_rids(val);

			for rid in old.difference(&new) {
				actions.push(RefAction::Delete(rid));
			}

			for rid in new.difference(&old) {
				actions.push(RefAction::Set(rid));
			}

			// Process the actions
			let ff = self.def.name.to_sql();
			for action in actions {
				match action {
					RefAction::Set(rid) => {
						let (ns, db) = self.ctx.expect_ns_db_ids(self.opt).await?;
						let key = crate::key::r#ref::new(
							ns,
							db,
							&rid.table,
							&rid.key,
							&self.rid.table,
							&ff,
							&self.rid.key,
						);

						self.ctx.tx().set(&key, &()).await?;
					}
					RefAction::Delete(rid) => {
						let (ns, db) = self.ctx.expect_ns_db_ids(self.opt).await?;
						let key = crate::key::r#ref::new(
							ns,
							db,
							&rid.table,
							&rid.key,
							&self.rid.table,
							&ff,
							&self.rid.key,
						);

						self.ctx.tx().del(&key).await?;
					}
				}
			}
		}
		Ok(())
	}
}
