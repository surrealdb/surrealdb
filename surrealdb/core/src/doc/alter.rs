use std::sync::Arc;

use anyhow::{Result, bail, ensure};
use reblessive::tree::Stk;
use surrealdb_types::ToSql;

use crate::catalog::{LATEST_EDGE_VARIANT, RecordType};
use crate::ctx::{Context, FrozenContext};
use crate::dbs::{Options, Statement};
use crate::doc::{Document, Extras};
use crate::err::Error;
use crate::expr::data::Data;
use crate::expr::paths::{ID, IN, OUT};
use crate::expr::{AssignOperator, FlowResultExt, Idiom, Part};
use crate::val::{RecordId, Value};

impl Document {
	/// Generate a record ID for CREATE, UPSERT, and UPDATE statements
	///
	/// This method handles record ID generation from various sources:
	/// - Existing document IDs
	/// - Data clause specified IDs (including function calls and expressions)
	/// - Randomly generated IDs when no ID is specified
	///
	/// The method ensures that all expressions are properly evaluated before
	/// being used as record IDs.
	pub(super) fn generate_record_id(&mut self) -> Result<()> {
		// Check if we need to generate a record id
		if let Some(tb) = &self.r#gen {
			// This is a CREATE, UPSERT, UPDATE, RELATE statement
			// Check if the document already has an ID from the current data
			let existing_id = self.current.doc.as_ref().pick(&ID);
			let id = if existing_id.is_some() {
				// The document already has an ID, use it
				existing_id.generate(tb.clone(), false)?
			} else {
				// Fetch the record id if specified
				match &self.input_data {
					// There is a data clause so fetch a record id
					Some(data) => match data.pick(ID.as_ref()) {
						Value::None => RecordId::random_for_table(tb.clone()),
						// Generate a new id from the id field
						id => id.generate(tb.clone(), false)?,
						// Generate a new random table id
					},
					// There is no data clause so create a record id
					None => RecordId::random_for_table(tb.clone()),
				}
			};
			// The id field can not be a record range
			ensure!(
				!id.key.is_range(),
				Error::IdInvalid {
					value: id.to_sql(),
				}
			);
			// Set the document id
			self.id = Some(Arc::new(id));
		}
		//
		Ok(())
	}

	/// Clears all of the content of this document.
	/// This is used to empty the current content
	/// of the document within a `DELETE` statement.
	/// This function only clears the document in
	/// memory, and does not store this on disk.
	pub(super) fn clear_record_data(&mut self) {
		*self.current.doc = Default::default();
	}

	/// Sets the default field data that should be
	/// present on this document. For normal records
	/// the `id` field is always specified, and for
	/// relation records, the `in`, `out`, and the
	/// hidden `edge` field are always present. This
	/// ensures that any user modifications of these
	/// fields are reset back to the original state.
	pub(super) fn default_record_data(&mut self) -> Result<()> {
		// Get the record id
		let rid = self.id()?;
		// Set default field values
		self.current.doc.to_mut().def(RecordId::clone(&rid));
		// This is a RELATE statement, so reset fields
		if let Extras::Relate(l, r, _) = &self.extras {
			// Stamp the record-type marker to the current adjacency-key
			// generation. This runs before `store_record_data` writes the
			// record to disk, so the on-disk metadata always matches the
			// keys `store_edges_data` will emit later in the pipeline.
			//
			// Three cases are folded together by this single condition:
			//   * Brand-new edge (`current` not yet an edge): stamp it.
			//   * Re-RELATE of a stale-variant edge (e.g. legacy variant 1 being migrated to 2):
			//     advance the stamp so the post-migration record reflects the upgraded layout.
			//   * Re-RELATE of a current-variant edge: no-op skip, avoiding an `Arc::make_mut`
			//     clone for nothing.
			//
			// `current` starts as a clone of `initial`, and users can't
			// address `metadata` themselves, so the only way `current`
			// can already carry the current variant here is if `initial`
			// did — i.e. there's no risk of a stale write masking a
			// genuine migration.
			if self.current.doc.edge_variant() != Some(LATEST_EDGE_VARIANT) {
				self.current.doc.set_record_type(RecordType::Edge {
					variant: LATEST_EDGE_VARIANT,
				});
			}
			// If this document existed before, check the `in` field
			match (self.initial.doc.as_ref().pick(&IN), self.is_new()) {
				// If the document id matches, then all good
				(Value::RecordId(id), false) if id == *l => {
					self.current.doc.to_mut().put(&IN, l.clone().into());
				}
				// If the document is new then all good
				(_, true) => {
					self.current.doc.to_mut().put(&IN, l.clone().into());
				}
				// Otherwise this is attempting to override the `in` field
				(v, _) => {
					bail!(Error::InOverride {
						value: v.to_sql(),
					})
				}
			}
			// If this document existed before, check the `out` field
			match (self.initial.doc.as_ref().pick(&OUT), self.is_new()) {
				// If the document id matches, then all good
				(Value::RecordId(id), false) if id == *r => {
					self.current.doc.to_mut().put(&OUT, r.clone().into());
				}
				// If the document is new then all good
				(_, true) => {
					self.current.doc.to_mut().put(&OUT, r.clone().into());
				}
				// Otherwise this is attempting to override the `in` field
				(v, _) => {
					bail!(Error::OutOverride {
						value: v.to_sql(),
					})
				}
			}
		}
		// This is an UPDATE of a graph edge, so reset its `in` / `out`
		// fields to whatever the prior record held. The edge marker
		// itself doesn't need to be re-stamped: `current` is a clone
		// of `initial`, and only `data` (not `metadata`) is reachable
		// through `to_mut()`, so the variant on the prior edge flows
		// through to the new write untouched.
		if self.initial.doc.is_edge() {
			self.current.doc.to_mut().put(&IN, self.initial.doc.as_ref().pick(&IN));
			self.current.doc.to_mut().put(&OUT, self.initial.doc.as_ref().pick(&OUT));
		}
		// Carry on
		Ok(())
	}

	/// Updates the current document using the data
	/// passed in to each document. This is relevant
	/// for INSERT and RELATE queries where each
	/// document has its own data block. This
	/// function also ensures that standard default
	/// fields are set and reset before and after the
	/// document data is modified.
	pub(super) fn process_merge_data(&mut self) -> Result<()> {
		// Get the record id
		let rid = self.id()?;
		// Set default field values
		self.current.doc.to_mut().def(RecordId::clone(&rid));
		// This is an INSERT statement
		if let Extras::Insert(v) = &self.extras {
			self.current.doc.to_mut().merge(Value::clone(v))?;
		}
		// This is an INSERT RELATION statement
		if let Extras::Relate(_, _, Some(v)) = &self.extras {
			self.current.doc.to_mut().merge(Value::clone(v))?;
		}
		// Carry on
		Ok(())
	}

	/// Updates the current document using the data
	/// clause present on the statement. This can be
	/// one of CONTENT, REPLACE, MERGE, PATCH, SET,
	/// UNSET, or ON DUPLICATE KEY UPDATE. This
	/// function also ensures that standard default
	/// fields are set and reset before and after the
	/// document data is modified.
	pub(super) async fn process_record_data(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
	) -> Result<()> {
		// The statement has a data clause
		if let Some(v) = self.input_data.clone() {
			match v {
				ComputedData::Patch(data) => {
					self.current.doc.to_mut().patch(data.as_ref().clone())?
				}
				ComputedData::Merge(data) => {
					self.current.doc.to_mut().merge(data.as_ref().clone())?
				}
				ComputedData::Replace(data) => {
					self.current.doc.to_mut().replace(data.as_ref().clone())?
				}
				ComputedData::Content(data) => {
					self.current.doc.to_mut().replace(data.as_ref().clone())?
				}
				ComputedData::Unset(i) => {
					for i in i.iter() {
						self.current.doc.to_mut().cut(i);
					}
				}
				ComputedData::Set(x) => {
					// The assignment right-hand sides were already evaluated
					// against the reduced view of `current` in
					// `compute_input_data`, so here we just write the
					// pre-computed values into `self.current` — the actual
					// storage-bound document. Writing to the reduced view
					// would leave `self.current` unchanged and the mutation
					// would be invisible to subsequent pipeline steps that
					// re-reduce from `current` (e.g. `output_after`).
					apply_assignments(stk, ctx, opt, self.current.doc.to_mut(), &x).await?;
				}
			};
			// Every arm mutates `self.current.doc`, so the reduced view
			// cached earlier in the pipeline (e.g. by `compute_input_data`
			// or `check_where_condition`) is now stale. Invalidate
			// it so any downstream caller that re-reduces sees the new
			// field values rather than relying on `output_*` to do this
			// implicitly.
			self.current_reduced = None;
		};
		// Carry on
		Ok(())
	}

	/// Evaluate the statement's data clause once and cache the result.
	///
	/// The expressions inside `SET`/`CONTENT`/`MERGE`/`PATCH`/`REPLACE`/`UNSET`
	/// can reference `$input` (for `INSERT … ON DUPLICATE KEY UPDATE` and
	/// `RELATE`) and the current document fields, so the data clause must be
	/// computed against a reduced view of `current` that has had field-level
	/// permissions applied. The first call materialises that reduced view via
	/// [`Self::reduce_current`], computes each expression against it, and
	/// stores the resulting [`ComputedData`] on `self`. Subsequent calls in
	/// the same pipeline (e.g. the UPSERT retry path, or `process_record_data`
	/// reusing the value computed by an earlier permission check) are no-ops
	/// and return the cached value without re-evaluating any user expression.
	///
	/// Returns `Ok(None)` when the statement has no data clause.
	pub(super) async fn compute_input_data(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Option<&ComputedData>> {
		// Check if the input data has been computed
		if self.input_data.is_some() {
			return Ok(self.input_data.as_ref());
		}
		// Check if there is a data clause on the statement
		if let Some(data) = stm.data() {
			// Snapshot `$input` before reduce_current takes &mut self
			let input_value: Option<Arc<Value>> = match &self.extras {
				Extras::Insert(value) => Some(Arc::clone(value)),
				Extras::Relate(_, _, Some(value)) => Some(Arc::clone(value)),
				_ => None,
			};
			// Reduce the document with permissions
			let doc = self.reduce_current(stk, ctx, opt).await?;
			// Compote the input data from the statement
			self.input_data = Some(match data {
				// This is a UNSET expression
				Data::UnsetExpression(data) => ComputedData::Unset(data.clone()),
				// This is a PATCH expression
				Data::PatchExpression(data) => ComputedData::Patch(Arc::new(
					data.compute(stk, ctx, opt, Some(doc)).await.catch_return()?,
				)),
				// This is a MERGE expression
				Data::MergeExpression(data) => ComputedData::Merge(Arc::new(
					data.compute(stk, ctx, opt, Some(doc)).await.catch_return()?,
				)),
				// This is a REPLACE expression
				Data::ReplaceExpression(data) => ComputedData::Replace(Arc::new(
					data.compute(stk, ctx, opt, Some(doc)).await.catch_return()?,
				)),
				// This is a CONTENT expression
				Data::ContentExpression(data) => ComputedData::Content(Arc::new(
					data.compute(stk, ctx, opt, Some(doc)).await.catch_return()?,
				)),
				// This is a SET or ON DUPLICATE KEY UPDATE expression
				x @ Data::SetExpression(data) | x @ Data::UpdateExpression(data) => {
					let ctx = if matches!(x, Data::UpdateExpression(_)) {
						// Duplicate context
						let mut ctx = Context::new_child(ctx);
						// Add insertable value
						if let Some(value) = input_value {
							ctx.add_value("input", value);
						}
						// Freeze the context
						ctx.freeze()
					} else {
						Arc::clone(ctx)
					};

					let mut assignments = Vec::with_capacity(data.len());
					for x in data.iter() {
						assignments.push(ComputedAssignment {
							place: x.place.clone(),
							operator: x.operator.clone(),
							value: x
								.value
								.compute(stk, &ctx, opt, Some(doc))
								.await
								.catch_return()?,
						});
					}

					ComputedData::Set(assignments)
				}
				x => bail!("Unexpected data clause type: {x:?}"),
			});
		}

		Ok(self.input_data.as_ref())
	}

	/// Compute the statement's data clause (if needed) and materialise it as a
	/// single synthetic [`Value`] suitable for binding to `$input` or feeding
	/// into permission checks.
	///
	/// This is a thin wrapper around [`Self::compute_input_data`] followed by
	/// [`ComputedData::materialize`]: the data clause is evaluated lazily on
	/// the first call and cached, then projected into a concrete object/array
	/// value. Use this when you need the user-supplied data as a `Value`;
	/// reach for [`Self::compute_input_data`] directly when you only need to
	/// dispatch on the data variant (PATCH/MERGE/SET/…).
	///
	/// Returns `Ok(None)` when the statement has no data clause.
	pub(super) async fn compute_input_value(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Option<Arc<Value>>> {
		// Make sure the input data clause has been computed.
		if self.compute_input_data(stk, ctx, opt, stm).await?.is_none() {
			return Ok(None);
		}
		// Re-borrow self.input_data so the &mut self borrow from
		// compute_input_data is released before the await below.
		let data = self.input_data.as_ref().expect("just verified Some above");
		Ok(Some(data.materialize(stk, ctx, opt).await?))
	}

	/// Materialize the synthetic input value from the already-computed
	/// `input_data`. Unlike [`Self::compute_input_value`] this never falls
	/// back to evaluating the statement's data clause: callers must have
	/// already run [`Self::compute_input_data`] (typically via
	/// `process_record_data`) before getting here.
	pub(super) async fn materialize_input_value(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
	) -> Result<Option<Arc<Value>>> {
		match self.input_data.as_ref() {
			Some(data) => Ok(Some(data.materialize(stk, ctx, opt).await?)),
			None => Ok(None),
		}
	}
}

/// The result of evaluating a statement's data clause once, cached on the
/// [`Document`] so that downstream pipeline steps (permission checks,
/// `process_record_data`, `$input` materialisation, the UPSERT retry path)
/// can reuse it without re-running any user-supplied expression.
///
/// Each variant mirrors a SurrealQL data-clause form:
/// - [`ComputedData::Patch`] — `PATCH [{op: "...", path: "...", value: ...}, …]`
/// - [`ComputedData::Merge`] — `MERGE {…}`
/// - [`ComputedData::Replace`] — `REPLACE {…}`
/// - [`ComputedData::Content`] — `CONTENT {…}`
/// - [`ComputedData::Unset`] — `UNSET field, …`
/// - [`ComputedData::Set`] — `SET …` / `ON DUPLICATE KEY UPDATE …`
///
/// The materialised value variants store an `Arc<Value>` so the payload can
/// be cheaply shared between the cached entry and the consumers that need it
/// by value (e.g. `merge`/`replace`/`patch` on `current.doc`).
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(super) enum ComputedData {
	Patch(Arc<Value>),
	Merge(Arc<Value>),
	Replace(Arc<Value>),
	Content(Arc<Value>),
	Unset(Vec<Idiom>),
	Set(Vec<ComputedAssignment>),
}

impl ComputedData {
	/// Returns `true` when this data clause is a `PATCH` expression.
	///
	/// Used by the pipeline to special-case PATCH semantics (which operate on
	/// the full document via JSON-Patch ops) where the other variants are
	/// treated more uniformly as object overlays/replacements.
	pub(super) fn is_patch(&self) -> bool {
		matches!(self, ComputedData::Patch(_))
	}

	/// Synchronously pick a value at the given path from the user-supplied
	/// data clause. For SET this scans the assignments for a matching
	/// `Assign` operator at `path`; it deliberately ignores compound
	/// operators (+=, -=, +?) because they need the existing field value
	/// to evaluate and so cannot be resolved without the initial document.
	pub(super) fn pick(&self, path: &[Part]) -> Value {
		match self {
			ComputedData::Patch(v) => v.pick(path),
			ComputedData::Merge(v) => v.pick(path),
			ComputedData::Replace(v) => v.pick(path),
			ComputedData::Content(v) => v.pick(path),
			ComputedData::Unset(_) => Value::None,
			ComputedData::Set(assignments) => {
				for a in assignments {
					if a.operator == AssignOperator::Assign && a.place.0.as_slice() == path {
						return a.value.clone();
					}
				}
				Value::None
			}
		}
	}

	/// Asynchronously materialize the synthetic input value used by `$input`
	/// in DEFINE EVENT and DEFINE FIELD VALUE / ASSERT expressions. For SET
	/// this applies the assignments to an empty object so compound operators
	/// resolve against `Value::None` — the same semantics as on a freshly
	/// created record. Other data clauses already carry the materialized
	/// value and clone the `Arc`.
	pub(super) async fn materialize(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
	) -> Result<Arc<Value>> {
		match self {
			ComputedData::Patch(v) => Ok(Arc::clone(v)),
			ComputedData::Merge(v) => Ok(Arc::clone(v)),
			ComputedData::Replace(v) => Ok(Arc::clone(v)),
			ComputedData::Content(v) => Ok(Arc::clone(v)),
			ComputedData::Unset(_) => Ok(Arc::new(Value::None)),
			ComputedData::Set(assignments) => {
				let mut input = Value::Object(Default::default());
				apply_assignments(stk, ctx, opt, &mut input, assignments).await?;
				Ok(Arc::new(input))
			}
		}
	}
}

/// A single pre-evaluated assignment from a `SET …` / `ON DUPLICATE KEY
/// UPDATE …` data clause, cached as part of [`ComputedData::Set`].
///
/// The right-hand side has already been evaluated against the reduced
/// `current` document by [`Document::compute_input_data`], so re-applying
/// the assignment (e.g. on the UPSERT retry path) does not re-run any
/// user-supplied expression.
///
/// Fields:
/// - `place` — target field path (the left-hand `Idiom`).
/// - `operator` — how the value combines with the existing field (plain `=`, compound
///   `+=`/`-=`/`+?`).
/// - `value` — the already-evaluated right-hand side.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(super) struct ComputedAssignment {
	pub place: Idiom,
	pub operator: AssignOperator,
	pub value: Value,
}

/// Apply a list of pre-evaluated `SET` assignments to `doc` in order.
///
/// Each assignment dispatches to the matching `Value` mutator based on its
/// operator:
/// - `Assign` (`=`) — `set` for non-`NONE`, `del` when the right-hand side evaluated to `NONE`
///   (treated as field removal).
/// - `Add` (`+=`) — `increment`.
/// - `Subtract` (`-=`) — `decrement`.
/// - `Extend` (`+?`) — `extend` (array/object union).
///
/// Used both by `process_record_data` (to apply the assignments to the
/// current document) and by [`ComputedData::materialize`] (to project the
/// assignments onto an empty object so compound operators resolve against
/// `NONE`, matching freshly-created-record semantics for `$input`).
async fn apply_assignments(
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: &mut Value,
	assignments: &[ComputedAssignment],
) -> Result<()> {
	for x in assignments {
		match &x.operator {
			AssignOperator::Assign => match &x.value {
				Value::None => doc.del(stk, ctx, opt, &x.place).await?,
				_ => doc.set(stk, ctx, opt, &x.place, x.value.clone()).await?,
			},
			AssignOperator::Add => doc.increment(stk, ctx, opt, &x.place, x.value.clone()).await?,
			AssignOperator::Subtract => {
				doc.decrement(stk, ctx, opt, &x.place, x.value.clone()).await?
			}
			AssignOperator::Extend => doc.extend(stk, ctx, opt, &x.place, x.value.clone()).await?,
		}
	}
	Ok(())
}
