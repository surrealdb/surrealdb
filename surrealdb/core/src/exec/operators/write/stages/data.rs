//! The statement's own clauses, applied to one record.
//!
//! Every stage whose input is something the *statement* wrote rather than
//! something the *table* declares: the data clause, and the `WHERE` gate.
//!
//! These are the stages that read
//! [`WriteCtx::clauses`](super::super::ctx::WriteCtx), and the only ones that
//! do.
//!
//! Record-id derivation is the one stage here that also reads the table's
//! schema. It settles what the statement claimed about the record's identity
//! against the `id` field's declared kind and `DEFAULT` in one place, because
//! the key it produces *is* the record's storage key: the field kernel that
//! applies every other field's declaration leaves `id` alone rather than
//! coercing a value that is already committed to.
//!
//! # The pre-mutation snapshot
//!
//! The `WHERE` condition and the data clause read one image of the record:
//! the field-level-`select`-reduced pre-mutation record with its `COMPUTED`
//! fields derived. Sharing it is what makes a predicate and a data-clause
//! expression in the same statement agree about what a computed field holds,
//! and what keeps a caller's data clause from observing a field the caller may
//! not select. Neither reader ever observes the statement's own writes: every
//! expression of the data clause is evaluated against this image before any of
//! them is applied, which is what makes `SET a = a + 1, b = a + 1` assign `b`
//! from the old `a`.

use std::sync::Arc;

use surrealdb_types::ToSql;

use crate::catalog::{
	DefineDefault, FieldDefinition, LATEST_EDGE_VARIANT, RecordType, TableDefinition, TableType,
};
use crate::doc::Error as DocError;
use crate::doc::alter::ComputedAssignment;
use crate::exe::FlowResultExt as _;
use crate::exec::operators::scan::pipeline::{
	FieldState, compute_fields_for_value, filter_fields_by_permission,
	materialise_fields_with_permissions,
};
use crate::exec::operators::write::apply;
use crate::exec::operators::write::clauses::{WriteClauses, WriteData};
use crate::exec::operators::write::ctx::WriteCtx;
use crate::exec::operators::write::metadata::{edge_variant, is_edge, set_record_type};
use crate::exec::operators::write::row::{ComputedInput, WriteExtras, WriteRow};
use crate::exec::operators::write::stage::{StageId, StageOutcome, WriteStage};
use crate::exec::permission::{field_default_frame, field_default_physical};
use crate::exec::{BoxFut, Error as ExecError, EvalContext, ExecutionContext, PhysicalExpr};
use crate::expr::paths::{ID, IN, OUT};
use crate::expr::{
	AssignOperator, ControlFlow, Error as ExprError, FlowResult, Idiom, Kind, KindLiteral,
	Operation, Part,
};
use crate::val::{Array, RecordId, RecordIdKey, TableName, Value};

/// This module's stages, by id.
///
/// `None` is a stage whose body has not landed:
/// [`WritePlan::resolve`](crate::exec::operators::write::plan::WritePlan)
/// refuses to bind a plan that names one, rather than running a partial order.
#[allow(
	clippy::wildcard_enum_match_arm,
	reason = "a stage this module does not own is not this module's to name, and `super::lookup` \
	          routes none of them here"
)]
pub(crate) fn stage(id: StageId) -> Option<Arc<dyn WriteStage>> {
	match id {
		StageId::CheckWhereCondition => Some(Arc::new(CheckWhereCondition)),
		StageId::ComputeInputData => Some(Arc::new(ComputeInputData)),
		StageId::CheckDataFields => Some(Arc::new(CheckDataFields)),
		StageId::ProcessMergeData => Some(Arc::new(ProcessMergeData)),
		StageId::DefaultRecordData => Some(Arc::new(DefaultRecordData)),
		StageId::ProcessRecordData => Some(Arc::new(ProcessRecordData)),
		StageId::GenerateRecordId => Some(Arc::new(GenerateRecordId)),
		// `super::lookup` routes only this module's stages here, so anything
		// else is a routing mistake rather than a missing body. Answering
		// `None` leaves it to `resolve` to name, as it names any unbound stage.
		_ => None,
	}
}

// ===========================================================================
// The pre-mutation snapshot
// ===========================================================================

/// Derive the image the `WHERE` condition and the data clause read, onto the
/// row, and leave it where [`pre_mutation_view`] finds it.
///
/// Both halves are memoised, so a record that runs both stages pays for at
/// most one reduction and one `COMPUTED`-field pass: the reduction on
/// [`WriteRow::current_reduced`], the derivation on
/// [`WriteRow::fields_computed`]. A stage that changes the record's field
/// values clears them, which is what makes the memo safe to read.
///
/// `COMPUTED` fields are derived only for a record that already exists. A
/// creating shape has no pre-image to derive them from, and a body reading an
/// absent field raises rather than yielding `NONE`, so the fields read as
/// absent there.
///
/// The table's own `PERMISSIONS FOR select` is deliberately **not** applied: it
/// decides whether the caller is shown the record, which is
/// `check_select_permissions`' question and is asked of the post-image. A
/// caller that may write a record it may not select still evaluates its
/// condition and its data clause against the record's readable fields.
async fn materialise_pre_mutation_snapshot(
	row: &mut WriteRow,
	ctx: &WriteCtx,
) -> Result<(), ControlFlow> {
	let exec = ctx.exec();
	let table = ctx.table.name.clone();
	// A record with no id is not addressable, so neither a field guard nor a
	// `COMPUTED` body has a subject; the record as it stands is the image.
	if row.id.is_none() {
		return Ok(());
	}
	// Whether there is a pre-image to derive `COMPUTED` fields from.
	let derive = !row.initial.is_none();
	let skip_fetch_perms = exec.root().skip_fetch_perms;
	let mut views = ctx.view_checked.lock().await;
	let view = views.view_for(exec, &table).await?;
	let state = view.field_state();
	if view.checked() {
		if row.current_reduced.is_none() {
			let mut reduced = row.current.clone();
			reduce_to_readable(exec, state, &mut reduced, derive, skip_fetch_perms).await?;
			row.current_reduced = Some(reduced);
		}
		return Ok(());
	}
	if derive && !row.fields_computed {
		compute_fields_for_value(exec, state, &mut row.current, skip_fetch_perms).await?;
		row.fields_computed = true;
	}
	Ok(())
}

/// The image [`materialise_pre_mutation_snapshot`] left: the reduced view when
/// the caller's reads are checked, and the record itself otherwise.
fn pre_mutation_view(row: &WriteRow) -> &Value {
	row.current_reduced.as_ref().unwrap_or(&row.current)
}

/// Cut from `value` what the caller may not select, deriving `COMPUTED` fields
/// first where `derive` says there is a pre-image to derive them from. The one
/// reduction every image the caller's reads are judged against goes through.
async fn reduce_to_readable(
	exec: &ExecutionContext,
	state: &FieldState,
	value: &mut Value,
	derive: bool,
	skip_fetch_perms: bool,
) -> Result<(), ControlFlow> {
	if derive {
		materialise_fields_with_permissions(exec, state, value, skip_fetch_perms, true).await
	} else {
		filter_fields_by_permission(exec, state, value).await
	}
}

/// `value` as the caller may read it, reduced as
/// [`materialise_pre_mutation_snapshot`] reduces the record, or `None` where the
/// caller's reads are not checked and `value` is already what they read.
/// `derive` is whether the record has a pre-image to derive `COMPUTED` fields
/// from.
async fn readable_image(
	ctx: &WriteCtx,
	value: &Value,
	derive: bool,
) -> Result<Option<Value>, ControlFlow> {
	let exec = ctx.exec();
	let mut views = ctx.view_checked.lock().await;
	let view = views.view_for(exec, &ctx.table.name).await?;
	if !view.checked() {
		return Ok(None);
	}
	let mut reduced = value.clone();
	reduce_to_readable(
		exec,
		view.field_state(),
		&mut reduced,
		derive,
		exec.root().skip_fetch_perms,
	)
	.await?;
	Ok(Some(reduced))
}

// ===========================================================================
// The stages
// ===========================================================================

/// The statement's `WHERE`, judged once per candidate record.
///
/// Sits ahead of [`ComputeInputData`] on every updating order and on `DELETE`,
/// so a data clause with side effects runs only for records the condition
/// accepts — which is what keeps a full-scan plan and an index-backed plan
/// observably equivalent. It sits behind the order's pre-write permission gate,
/// so a caller with no write permission never runs its own predicate against a
/// record.
///
/// A record the condition rejects is [`StageOutcome::SkipRow`]: the statement
/// succeeds and emits nothing for it.
#[derive(Debug)]
struct CheckWhereCondition;

impl WriteStage for CheckWhereCondition {
	fn name(&self) -> &'static str {
		StageId::CheckWhereCondition.name()
	}

	fn apply<'a>(
		&'a self,
		row: &'a mut WriteRow,
		ctx: &'a WriteCtx,
	) -> BoxFut<'a, Result<StageOutcome, ControlFlow>> {
		Box::pin(async move {
			let Some(cond) = ctx.clauses.cond.as_ref() else {
				row.matched = true;
				return Ok(StageOutcome::Continue);
			};
			materialise_pre_mutation_snapshot(row, ctx).await?;
			let snapshot = pre_mutation_view(row);
			let eval = EvalContext::from_exec_ctx(ctx.exec()).with_value_and_doc(snapshot);
			match flatten(cond.evaluate(eval).await)?.is_truthy() {
				// Recorded before carrying on, because everything downstream may
				// still decline the record — `RETURN NONE`, `OPTION IMPORT` and
				// the select gate all do, after the write has happened — and it is
				// the flag, not the statement's output, that tells a statement
				// which matched records apart from one that matched none.
				true => {
					row.matched = true;
					Ok(StageOutcome::Continue)
				}
				false => Ok(StageOutcome::SkipRow),
			}
		})
	}
}

/// Evaluate the statement's data clause into [`ComputedInput`], once.
///
/// Memoised on [`WriteRow::input`], because recomputing re-runs the user's
/// expressions: `process_record_data`, `process_merge_data`,
/// `check_data_fields` and the `$input` binding all read the one evaluation.
/// A creating attempt that is rolled back and re-run as an update clears the
/// memo, because the record the clause is evaluated against has changed.
///
/// Every expression is evaluated against the pre-mutation snapshot before any
/// of them is applied.
#[derive(Debug)]
struct ComputeInputData;

impl WriteStage for ComputeInputData {
	fn name(&self) -> &'static str {
		StageId::ComputeInputData.name()
	}

	fn apply<'a>(
		&'a self,
		row: &'a mut WriteRow,
		ctx: &'a WriteCtx,
	) -> BoxFut<'a, Result<StageOutcome, ControlFlow>> {
		Box::pin(async move {
			if row.input.is_some() {
				return Ok(StageOutcome::Continue);
			}
			let Some((data, binds_input)) = statement_data(&ctx.clauses) else {
				return Ok(StageOutcome::Continue);
			};
			materialise_pre_mutation_snapshot(row, ctx).await?;
			// The borrow of the row ends with the block, so the evaluation can
			// read the snapshot and the statement's payload while the result is
			// still owned, and the memo is written once it has.
			let computed = {
				let snapshot = pre_mutation_view(row);
				let input = binds_input.then(|| supplied_value(&row.extras)).flatten();
				evaluate_data(data, snapshot, input, ctx).await?
			};
			row.input = Some(computed);
			Ok(StageOutcome::Continue)
		})
	}
}

/// The record's special fields — `id`, and an edge's `in` and `out` — are
/// consistent with the statement's own target.
///
/// Stands down entirely when an id is still to be derived, which is what lets
/// `CREATE foo CONTENT { id: bar:123 }` take its key from the content: there
/// the content *is* the source of the id rather than a second claim about it.
///
/// A `PATCH` clause is not checked, because a JSON Patch document says what to
/// do to the record rather than what the record holds, so no static reading of
/// it answers what `id` will be.
#[derive(Debug)]
struct CheckDataFields;

impl WriteStage for CheckDataFields {
	fn name(&self) -> &'static str {
		StageId::CheckDataFields.name()
	}

	fn apply<'a>(
		&'a self,
		row: &'a mut WriteRow,
		_ctx: &'a WriteCtx,
	) -> BoxFut<'a, Result<StageOutcome, ControlFlow>> {
		Box::pin(async move {
			check_data_fields(row)?;
			Ok(StageOutcome::Continue)
		})
	}
}

/// The body of [`CheckDataFields`]: refuse a data clause, or a `RELATE`'s
/// value, whose `id`, `in` or `out` disagrees with the record the statement
/// named.
pub(crate) fn check_data_fields(row: &WriteRow) -> Result<(), ControlFlow> {
	if row.generate_in.is_some() {
		return Ok(());
	}
	let rid = record_id(row)?;
	if rid.key.is_range() {
		return Err(fail(ExprError::IdInvalid {
			value: rid.to_sql(),
		}));
	}
	let data = row.input.as_ref();
	if data.is_some_and(|data| matches!(data, ComputedInput::Patch(_))) {
		return Ok(());
	}
	match &row.extras {
		WriteExtras::Normal => {
			if let Some(data) = data {
				check_special_field(&picked(data, &ID), rid)?;
			}
		}
		WriteExtras::Relate(from, to, value) => {
			if let Some(data) = data {
				check_special_field(&picked(data, &ID), rid)?;
				check_special_field(&picked(data, &IN), from)?;
				check_special_field(&picked(data, &OUT), to)?;
			} else if let Some(value) = value {
				check_special_field(&value.pick(&ID), rid)?;
				check_special_field(&value.pick(&IN), from)?;
				check_special_field(&value.pick(&OUT), to)?;
			}
		}
		// An `INSERT`'s row value is the statement's subject rather than a
		// claim about one, and is merged by `process_merge_data` after the id
		// is settled.
		WriteExtras::Insert(_) => {}
	}
	Ok(())
}

/// `INSERT` create's data application: the row's own value merged onto the
/// record, rather than a statement-level clause applied to it.
///
/// The id is stamped before the merge, so a row value carrying no `id` still
/// leaves the record with the one the statement derived.
#[derive(Debug)]
struct ProcessMergeData;

impl WriteStage for ProcessMergeData {
	fn name(&self) -> &'static str {
		StageId::ProcessMergeData.name()
	}

	fn apply<'a>(
		&'a self,
		row: &'a mut WriteRow,
		_ctx: &'a WriteCtx,
	) -> BoxFut<'a, Result<StageOutcome, ControlFlow>> {
		Box::pin(async move {
			let rid = record_id(row)?.clone();
			row.current.def(rid);
			let merged = match &row.extras {
				WriteExtras::Insert(value) => Some(Value::clone(value)),
				WriteExtras::Relate(_, _, Some(value)) => Some(Value::clone(value)),
				WriteExtras::Relate(_, _, None) | WriteExtras::Normal => None,
			};
			if let Some(merged) = merged {
				row.current.merge(merged).map_err(ControlFlow::Err)?;
			}
			// The record's field values have changed, so anything derived from
			// the previous ones is stale.
			row.fields_computed = false;
			Ok(StageOutcome::Continue)
		})
	}
}

/// Apply the statement's evaluated data clause to the record.
///
/// The whole reason the clause was evaluated. Every form lands here:
/// `CONTENT` and `REPLACE` replace the record, `MERGE` overlays it, `PATCH`
/// applies its operations, `UNSET` cuts the paths it names, and `SET` applies
/// its assignments through the record mutators.
///
/// The evaluated clause is put back on the row afterwards, because two later
/// readers need it: the `$input` binding the table's own clauses are evaluated
/// under, and the retry path that decides whether to re-evaluate it.
///
/// The record's field values have changed by the time this returns, so both
/// halves of the pre-mutation snapshot are dropped: a later reader re-reduces
/// and re-derives from the new values rather than carrying the pre-mutation
/// ones. Leaving either standing makes a `COMPUTED` field that reads an
/// assigned field keep its old value, or disappear from the returned record
/// once the field kernel strips it.
#[derive(Debug)]
struct ProcessRecordData;

impl WriteStage for ProcessRecordData {
	fn name(&self) -> &'static str {
		StageId::ProcessRecordData.name()
	}

	fn apply<'a>(
		&'a self,
		row: &'a mut WriteRow,
		ctx: &'a WriteCtx,
	) -> BoxFut<'a, Result<StageOutcome, ControlFlow>> {
		Box::pin(async move {
			// Taken rather than cloned so the record can be mutated while the
			// clause is read, and put back before this returns either way.
			let Some(input) = row.input.take() else {
				return Ok(StageOutcome::Continue);
			};
			let applied = apply_input(&input, row, ctx).await;
			row.input = Some(input);
			applied?;
			row.current_reduced = None;
			row.fields_computed = false;
			Ok(StageOutcome::Continue)
		})
	}
}

/// Stamp the fields the record always carries, whatever the data clause did
/// with them: `id` on every record, and `in`, `out` and the record-type marker
/// on an edge.
///
/// Runs after the data clause has been applied, so a clause that wrote one of
/// these is reset here rather than reaching storage. On a `RELATE` the marker
/// is stamped before the record is written, so the stored marker always matches
/// the edge keys the later stages emit; an update of an existing edge re-stamps
/// nothing and carries the marker the before-image held.
///
/// Endpoints are immutable: a `RELATE` over an existing edge whose `in` or
/// `out` names a different record than the statement does fails rather than
/// re-pointing the edge.
#[derive(Debug)]
struct DefaultRecordData;

impl WriteStage for DefaultRecordData {
	fn name(&self) -> &'static str {
		StageId::DefaultRecordData.name()
	}

	fn apply<'a>(
		&'a self,
		row: &'a mut WriteRow,
		_ctx: &'a WriteCtx,
	) -> BoxFut<'a, Result<StageOutcome, ControlFlow>> {
		Box::pin(async move {
			stamp_record_fields(row)?;
			Ok(StageOutcome::Continue)
		})
	}
}

/// The body of [`DefaultRecordData`]: stamp `id`, and an edge's `in`, `out`
/// and record-type marker, onto the post-image.
pub(crate) fn stamp_record_fields(row: &mut WriteRow) -> Result<(), ControlFlow> {
	let rid = record_id(row)?.clone();
	row.current.def(rid);
	if let WriteExtras::Relate(from, to, _) = &row.extras {
		let from = from.clone();
		let to = to.clone();
		// Three cases fold into one condition: a brand-new edge is
		// stamped, an edge written against a stale layout generation is
		// advanced, and an edge already on the current generation is
		// left alone.
		if edge_variant(&row.metadata) != Some(LATEST_EDGE_VARIANT) {
			set_record_type(
				&mut row.metadata,
				RecordType::Edge {
					variant: LATEST_EDGE_VARIANT,
				},
			);
		}
		// A record this statement is creating has no endpoint to
		// disagree with; one it is updating must name the same pair.
		let new = row.initial.is_none();
		match (row.initial.pick(&IN), new) {
			(Value::RecordId(id), false) if id == from => {
				row.current.put(&IN, Value::RecordId(from))
			}
			(_, true) => row.current.put(&IN, Value::RecordId(from)),
			(found, _) => {
				return Err(fail(DocError::InOverride {
					value: found.to_sql(),
				}));
			}
		}
		match (row.initial.pick(&OUT), new) {
			(Value::RecordId(id), false) if id == to => row.current.put(&OUT, Value::RecordId(to)),
			(_, true) => row.current.put(&OUT, Value::RecordId(to)),
			(found, _) => {
				return Err(fail(DocError::OutOverride {
					value: found.to_sql(),
				}));
			}
		}
	}
	// An update of an edge by any other form restores the endpoints the
	// prior record held. The marker itself needs no re-stamping: it is
	// carried on the row from the before-image and the data clause
	// cannot address it.
	if is_edge(&row.initial_metadata) {
		row.current.put(&IN, row.initial.pick(&IN));
		row.current.put(&OUT, row.initial.pick(&OUT));
	}
	// `id`, `in` and `out` are field values like any other, so whatever
	// was derived from the record before this ran is stale.
	row.fields_computed = false;
	Ok(())
}

/// Settle the record this row writes at: derive one where the statement named
/// a table, and hold whichever key it ends up with to the `id` field's
/// declared kind.
///
/// Four sources answer for the key of a target that named a table, in the
/// order a creating statement means them:
///
/// 1. an `id` the record already carries, which is where a data clause that wrote one lands on
///    every creating order that applies its clause first;
/// 2. an `id` the evaluated data clause holds, which is the same claim before it is applied;
/// 3. the `id` field's `DEFAULT`;
/// 4. a key synthesised for the declared kind — a random string with no kind declared.
///
/// A target that names the record itself derives nothing: there the whole
/// stage is the kind check, which the derived key runs too, so a supplied key
/// and a derived one are held to the same declaration.
///
/// The derived key is the record's storage key, which is why the field kernel
/// leaves `id` alone: coercing the value a second time there could leave the
/// record naming a key it is not stored under.
///
/// # `generate_in` stays set
///
/// Deriving the id does not clear [`WriteRow::generate_in`], and must not:
/// [`CheckDataFields`] stands down while it is set — which is what lets
/// `CREATE foo CONTENT { id: bar:123 }` take its key from the content — and
/// `is_specific_record_id` reads it after the create attempt to decide whether
/// a unique-index conflict is retryable as an update. Clearing it is silent in
/// both.
#[derive(Debug)]
struct GenerateRecordId;

impl WriteStage for GenerateRecordId {
	fn name(&self) -> &'static str {
		StageId::GenerateRecordId.name()
	}

	fn apply<'a>(
		&'a self,
		row: &'a mut WriteRow,
		ctx: &'a WriteCtx,
	) -> BoxFut<'a, Result<StageOutcome, ControlFlow>> {
		Box::pin(async move {
			if let IdSettling::Default {
				table,
			} = settle_record_id(row, ctx)?
			{
				let id_field = ctx.id_field.and_then(|index| ctx.fields.get(index));
				let id = match id_default(ctx, id_field).await? {
					Some((frame, expr)) => {
						let eval = EvalContext::from_exec_ctx(&frame)
							.with_value_and_doc(id_default_doc(row));
						let value = flatten(expr.evaluate(eval).await)?;
						value.generate(table, false).map_err(ControlFlow::Err)?
					}
					None => generate_typed_id(&table, id_kind(ctx))?,
				};
				finish_record_id(row, ctx, id)?;
			}
			Ok(StageOutcome::Continue)
		})
	}
}

/// What settling a record's id still needs, once what the record, the
/// statement and its clause claim about it has been read.
pub(crate) enum IdSettling {
	/// The id is settled.
	Settled,
	/// Nothing claimed a key in `table`, whose `id` field declares a
	/// `DEFAULT`: its value, generated in `table`, is the id, which
	/// [`finish_record_id`] then settles.
	Default {
		table: TableName,
	},
}

/// The `id` field's declared kind.
fn id_kind(ctx: &WriteCtx) -> Option<&Kind> {
	ctx.id_field.and_then(|index| ctx.fields.get(index)).and_then(|fd| fd.field_kind.as_ref())
}

/// Settle the record's id as [`GenerateRecordId`] does, up to the `id`
/// field's `DEFAULT`: a lightweight edge's canonical id, the named record's
/// key held to the declared kind, a key the record or the data clause
/// supplied, or — where the field declares no `DEFAULT` — a synthesised key.
///
/// The `DEFAULT` is the caller's to evaluate, against [`id_default_doc`] in
/// the field's `DEFAULT` frame; the id it yields is settled by
/// [`finish_record_id`].
pub(crate) fn settle_record_id(
	row: &mut WriteRow,
	ctx: &WriteCtx,
) -> Result<IdSettling, ControlFlow> {
	if lightweight_edge_id(row, ctx)? {
		return Ok(IdSettling::Settled);
	}
	let id_field = ctx.id_field.and_then(|index| ctx.fields.get(index));
	let kind = id_field.and_then(|fd| fd.field_kind.as_ref());
	let Some(table) = row.generate_in.clone() else {
		// The statement named the record itself. Range targets never reach a
		// write operator: `WriteRow::for_target` refuses one.
		if kind.is_some()
			&& let Some(existing) = row.id.take()
		{
			row.id = Some(coerce_id_key(existing, kind)?);
		}
		return Ok(IdSettling::Settled);
	};
	// The record's own `id` answers first and the evaluated data clause
	// second, so a clause that named the record is honoured whether or not it
	// has been applied to the record yet. `DEFAULT` answers only where
	// neither did, and a key is synthesised only where none of the three
	// named one.
	let existing = row.current.pick(&ID);
	let supplied = if existing.is_some() {
		existing
	} else {
		row.input.as_ref().map(|data| picked(data, &ID)).unwrap_or(Value::None)
	};
	let id = if supplied.is_some() {
		supplied.generate(table, false).map_err(ControlFlow::Err)?
	} else if id_field.is_some_and(|fd| matches!(fd.default, DefineDefault::Set(_))) {
		return Ok(IdSettling::Default {
			table,
		});
	} else {
		generate_typed_id(&table, kind)?
	};
	finish_record_id(row, ctx, id)?;
	Ok(IdSettling::Settled)
}

/// The record a `DEFAULT` on the `id` field reads: the record as it stands,
/// so a `DEFAULT` naming another field resolves against the fields the
/// statement supplied — except under `INSERT`, whose id has to be known before
/// the row's own value is merged, so the payload is read instead. Nothing is
/// bound as `$value`, `$before`, `$after` or `$input`.
pub(crate) fn id_default_doc(row: &WriteRow) -> &Value {
	match &row.extras {
		WriteExtras::Insert(value) => value.as_ref(),
		WriteExtras::Relate(_, _, _) | WriteExtras::Normal => &row.current,
	}
}

/// Settle `id`, derived for the record, as its id: refused as a range, and
/// its key held to the `id` field's declared kind.
pub(crate) fn finish_record_id(
	row: &mut WriteRow,
	ctx: &WriteCtx,
	id: RecordId,
) -> Result<(), ControlFlow> {
	if id.key.is_range() {
		return Err(fail(ExprError::IdInvalid {
			value: id.to_sql(),
		}));
	}
	row.id = Some(coerce_id_key(id, id_kind(ctx))?);
	Ok(())
}

// ===========================================================================
// Shared helpers
// ===========================================================================

/// The clause this statement evaluates per record, and whether it is the one
/// that binds `$input`.
///
/// `ON DUPLICATE KEY UPDATE` is the data clause of an `INSERT`: the rows
/// themselves are the statement's subjects and reach the operator as its
/// source, so an `INSERT` carries no `data` of its own and the two slots are
/// never both filled. The clause that binds `$input` is the update one, where
/// `$input` is the row the insert attempted.
fn statement_data(clauses: &WriteClauses) -> Option<(&WriteData, bool)> {
	match (&clauses.data, &clauses.duplicate_update) {
		(Some(data), _) => Some((data, false)),
		(None, Some(update)) => Some((update, true)),
		(None, None) => None,
	}
}

/// The value the statement supplied for this record, which `$input` names.
/// `None` for a form that supplies none.
fn supplied_value(extras: &WriteExtras) -> Option<&Value> {
	match extras {
		WriteExtras::Insert(value) => Some(value),
		WriteExtras::Relate(_, _, Some(value)) => Some(value),
		WriteExtras::Relate(_, _, None) | WriteExtras::Normal => None,
	}
}

/// A clause of the statement, evaluated to a value.
///
/// A user expression may unwind rather than produce a value, and a clause is
/// not a place a loop or a function body can be left from. `RETURN x` inside
/// one is the clause's value; `BREAK` and `CONTINUE` have nothing here to
/// leave and are [`ExecError::InvalidControlFlow`](crate::exec::Error), which
/// is what stops a signal raised inside a record's clause from reaching — and
/// silently steering — a loop the statement is written inside.
pub(crate) fn flatten(result: FlowResult<Value>) -> Result<Value, ControlFlow> {
	result.catch_return().map_err(ControlFlow::Err)
}

/// Evaluate one data clause against `snapshot`.
///
/// `input` binds `$input` where the clause is the one that takes it. Each
/// right-hand side of a `SET` is evaluated against the same image, and the
/// assignment operator is recorded rather than applied: a compound assignment
/// combines with the record's value when the assignment is written, not when
/// its right-hand side is evaluated.
async fn evaluate_data(
	data: &WriteData,
	snapshot: &Value,
	input: Option<&Value>,
	ctx: &WriteCtx,
) -> Result<ComputedInput, ControlFlow> {
	let mut eval = EvalContext::from_exec_ctx(ctx.exec()).with_value_and_doc(snapshot);
	eval.write_input = input;
	Ok(match data {
		// An `UNSET` removes what its paths name; nothing about the record is
		// read to decide that, so the clause evaluates to the paths themselves.
		WriteData::Unset(places) => {
			ComputedInput::Unset(places.iter().map(|place| place.path().clone()).collect())
		}
		WriteData::Patch(expr) => {
			ComputedInput::Patch(Arc::new(flatten(expr.evaluate(eval).await)?))
		}
		WriteData::Merge(expr) => {
			ComputedInput::Merge(Arc::new(flatten(expr.evaluate(eval).await)?))
		}
		WriteData::Replace(expr) => {
			ComputedInput::Replace(Arc::new(flatten(expr.evaluate(eval).await)?))
		}
		WriteData::Content(expr) => {
			ComputedInput::Content(Arc::new(flatten(expr.evaluate(eval).await)?))
		}
		WriteData::Set(assignments) => {
			let mut computed = Vec::with_capacity(assignments.len());
			for assignment in assignments {
				computed.push(ComputedAssignment {
					place: assignment.place.path().clone(),
					operator: assignment.operator.clone(),
					value: flatten(assignment.value.evaluate(eval.clone()).await)?,
				});
			}
			ComputedInput::Set(computed)
		}
	})
}

/// The compiled clause an evaluated one came from.
///
/// [`ComputeInputData`] evaluates whichever clause [`statement_data`] names, so
/// that is the clause every reader of [`WriteRow::input`] pairs it with. The
/// two are read together rather than carried together because the evaluated
/// side is per record and the compiled side per statement: a place holds the
/// expressions its path embeds, compiled once, and the evaluated side holds
/// what this record's evaluation produced.
fn evaluated_clause(clauses: &WriteClauses) -> Result<&WriteData, ControlFlow> {
	statement_data(clauses).map(|(data, _)| data).ok_or_else(|| {
		ControlFlow::Err(anyhow::anyhow!("an evaluated data clause with no clause behind it"))
	})
}

/// Apply one evaluated data clause to the record.
///
/// `CONTENT` and `REPLACE` are the same mutation under two names: both replace
/// the record outright, and the fields it must carry regardless are stamped
/// back by [`DefaultRecordData`] afterwards.
async fn apply_input(
	input: &ComputedInput,
	row: &mut WriteRow,
	ctx: &WriteCtx,
) -> Result<(), ControlFlow> {
	match input {
		ComputedInput::Patch(data) => {
			check_patch_read_pointers(row, ctx, data).await?;
			row.current.patch(Value::clone(data)).map_err(ControlFlow::Err)?;
		}
		ComputedInput::Merge(data) => {
			row.current.merge(Value::clone(data)).map_err(ControlFlow::Err)?;
		}
		ComputedInput::Replace(data) | ComputedInput::Content(data) => {
			row.current.replace(Value::clone(data)).map_err(ControlFlow::Err)?;
		}
		// An `UNSET` names constant paths to cut out of the record. The
		// mutators are not reached: cutting a path out of a value reads
		// nothing and evaluates nothing.
		ComputedInput::Unset(paths) => {
			for path in paths.iter() {
				row.current.cut(path);
			}
		}
		ComputedInput::Set(computed) => {
			let WriteData::Set(clause) = evaluated_clause(&ctx.clauses)? else {
				return Err(ControlFlow::Err(anyhow::anyhow!(
					"an evaluated SET clause paired with a clause that is not a SET"
				)));
			};
			apply::apply_set(&mut row.current, clause, computed, ctx.exec()).await?;
		}
	}
	Ok(())
}

/// The pointer an operation reads and then carries somewhere other than
/// where it read it. Matched exhaustively, so an operation added without
/// being classified here does not default to carrying nothing out.
pub(crate) fn exposed_read_pointer(op: &Operation) -> Option<Vec<Part>> {
	let path = match op {
		// `copy` and `move` read at `from` and write the result at `path`.
		Operation::Copy {
			from,
			..
		}
		| Operation::Move {
			from,
			..
		} => from,
		// `test` reads at `path` and reports what it found in the
		// mismatch error.
		Operation::Test {
			path,
			..
		} => path,
		// The rest carry the value they write, or write back to the path
		// they read, so none of them carries a value out of the field it
		// came from.
		Operation::Add {
			..
		}
		| Operation::Remove {
			..
		}
		| Operation::Replace {
			..
		}
		| Operation::Change {
			..
		} => return None,
	};
	Some(Operation::path_to_parts(path))
}

/// Refuse a `PATCH` whose `copy`/`move` `from` or `test` `path` pointer reads a
/// field the caller may not select.
///
/// `Value::patch` resolves those pointers against the record as stored, so
/// without this gate a caller holding write permission reads a field denied by
/// `PERMISSIONS FOR select`: `copy` and `move` land the value at a destination
/// of the caller's choosing, and `test` reports it in the mismatch error. The
/// other clause forms are unaffected, because their expressions are evaluated
/// against the reduced snapshot and already read `NONE` there.
///
/// A pointer is judged by comparing what it reads on the record against what it
/// reads on the record's reduced image. Any difference means the pointer reaches
/// what the reduction removed, whether it names the guarded field itself or an
/// object or array containing it — and where the caller's reads are not
/// checked the image *is* the record, so nothing differs and nothing is
/// refused. Delegating to the reduction rather than re-deriving which paths are
/// guarded keeps this in step with the field-permission kernel.
///
/// Two properties make the judged location the one the patch reads:
///
/// - A pointer is mapped to parts by `Operation::path_to_parts`, the mapping `Value::patch` itself
///   uses, so a numeric token reaching into an array is judged at the element rather than at a
///   member name no array has.
/// - A patch is a sequence. Each pointer is judged against the record as it stands when its own
///   operation runs, the earlier operations applied, so an `add` or `remove` that shifts an array
///   cannot walk a restricted field into range of a later pointer.
///
/// `add` and `change` read a pointer too, but each writes its result back to
/// the path it read, so neither carries a value out of the field it came from.
async fn check_patch_read_pointers(
	row: &mut WriteRow,
	ctx: &WriteCtx,
	ops: &Value,
) -> Result<(), ControlFlow> {
	let operations = Operation::value_to_operations(ops.clone())
		.map_err(ExprError::InvalidPatch)
		.map_err(fail)?;
	// Nothing to enforce when no operation carries a value out of the location
	// it read, which spares the reduction on the common patch.
	if !operations.iter().any(|op| exposed_read_pointer(op).is_some()) {
		return Ok(());
	}
	materialise_pre_mutation_snapshot(row, ctx).await?;
	// Where the caller's reads are not checked there is no reduced image, and
	// the record is what they read.
	let Some(reduced) = row.current_reduced.clone() else {
		return Ok(());
	};
	// A copy of the record advanced one operation at a time, and its reduced
	// image, re-derived only when a later pointer is judged against it.
	let derive = !row.initial.is_none();
	let mut working = row.current.clone();
	let mut image = Some(reduced);
	for op in operations {
		if let Some(pointer) = exposed_read_pointer(&op) {
			let readable = match image.take() {
				Some(readable) => readable,
				None => match readable_image(ctx, &working, derive).await? {
					Some(readable) => readable,
					None => return Ok(()),
				},
			};
			if working.pick(&pointer) != readable.pick(&pointer) {
				return Err(fail(DocError::PatchPointerNotAllowed(Idiom(pointer).to_sql())));
			}
		}
		// An operation that fails here fails the statement when the patch is
		// applied for real, so the walk stops and leaves it to report.
		if working.patch(Operation::operations_to_value(vec![op])).is_err() {
			return Ok(());
		}
		image = None;
	}
	Ok(())
}

/// The value `$input` names for this record, or `None` where the statement
/// supplies none.
///
/// The table's own clauses — a field's `VALUE` and `ASSERT`, an event body —
/// are evaluated with this bound. It is derived from the evaluated data clause
/// rather than from the record, so it is what the *statement* supplied and not
/// what the record ended up holding.
///
/// Callers must run after [`ComputeInputData`]: a statement with a data clause
/// and no evaluation behind it answers `None` here, which would silently bind
/// `$input` to nothing.
pub(crate) async fn input_value(
	row: &WriteRow,
	ctx: &WriteCtx,
) -> Result<Option<Arc<Value>>, ControlFlow> {
	let Some(input) = row.input.as_ref() else {
		return Ok(None);
	};
	let data = evaluated_clause(&ctx.clauses)?;
	Ok(Some(apply::materialise_input(data, input, ctx.exec()).await?))
}

/// What an evaluated data clause holds at `path`.
///
/// A compound assignment is deliberately invisible here: it needs the record's
/// existing value to resolve, so it makes no claim about the record's special
/// fields that could be checked before the record is built.
pub(crate) fn picked(data: &ComputedInput, path: &[Part]) -> Value {
	match data {
		ComputedInput::Patch(value)
		| ComputedInput::Merge(value)
		| ComputedInput::Replace(value)
		| ComputedInput::Content(value) => value.pick(path),
		ComputedInput::Unset(_) => Value::None,
		ComputedInput::Set(assignments) => assignments
			.iter()
			.find(|a| a.operator == AssignOperator::Assign && a.place.0.as_slice() == path)
			.map(|a| a.value.clone())
			.unwrap_or(Value::None),
	}
}

/// Judge one special field of a data clause against the record the statement
/// named.
///
/// Absence is allowed: it is what a statement that named the record itself
/// leaves behind. A bare key is accepted where it matches the expected key,
/// which is the shorthand `CREATE city CONTENT { id: 'london' }` uses.
fn check_special_field(found: &Value, expected: &RecordId) -> Result<(), ControlFlow> {
	if let Value::RecordId(value) = found {
		if value.key.is_range() {
			return Err(fail(ExprError::IdInvalid {
				value: value.to_sql(),
			}));
		}
		if value == expected {
			return Ok(());
		}
	}
	// A bare key is compared against the expected one, which is what admits a
	// record id named in full and one named by its key alone under the same
	// rule.
	if found.is_none() || expected.key == *found {
		return Ok(());
	}
	Err(fail(DocError::IdMismatch {
		value: found.to_sql(),
	}))
}

/// The record this row writes at. Absent only for a creating order whose id
/// has still to be derived, which every caller here runs after.
fn record_id(row: &WriteRow) -> Result<&RecordId, ControlFlow> {
	row.id
		.as_ref()
		.ok_or_else(|| ControlFlow::Err(anyhow::anyhow!("Expected a record id to be present")))
}

/// A lightweight relation's edge id, which is canonical rather than derived:
/// the pair `[in, out]` its endpoints name.
///
/// The id is the edge's entire identity and its entire storage — the record is
/// synthesized on read from the adjacency keys — so an arbitrary id would name
/// a different edge than the one related. A `RELATE` that supplied one is
/// refused unless it supplied exactly this pair.
///
/// Answers whether the id is settled here, which is the whole of the stage for
/// such an edge: nothing is derived, and no declared kind applies to a key the
/// endpoints fix.
fn lightweight_edge_id(row: &mut WriteRow, ctx: &WriteCtx) -> Result<bool, ControlFlow> {
	let WriteExtras::Relate(from, to, _) = &row.extras else {
		return Ok(false);
	};
	if !table_is_lightweight(&ctx.table) {
		return Ok(false);
	}
	let canonical =
		RecordIdKey::Array(Array(vec![Value::RecordId(from.clone()), Value::RecordId(to.clone())]));
	if let Some(existing) = row.id.as_ref()
		&& existing.key != canonical
	{
		return Err(fail(ExecError::Thrown(format!(
			"a LIGHTWEIGHT relation's edge id is canonical: expected {}:{}, found {}",
			ctx.table.name,
			canonical.to_sql(),
			existing.to_sql(),
		))));
	}
	row.id = Some(RecordId {
		table: ctx.table.name.clone(),
		key: canonical,
	});
	Ok(true)
}

/// Whether the table's edges are vertex-side adjacency keys alone, synthesized
/// on read from their canonical `[in, out]` ids.
fn table_is_lightweight(table: &TableDefinition) -> bool {
	matches!(&table.table_type, TableType::Relation(rel) if rel.lightweight)
}

/// The `id` field's `DEFAULT`, compiled, and the frame it is evaluated in;
/// `None` where the field declares none.
///
/// ADR 0001: the clause runs under the authority of whoever defined the field,
/// narrowed from the caller's, so a lower-privileged definer's `DEFAULT` cannot
/// exercise the caller's privileges. Permissions are otherwise enforced exactly
/// as the statement enforces them, and a write in the clause is not refused:
/// the frame is the statement's own.
///
/// Only the plain spelling supplies a record id. `DEFAULT ALWAYS` would
/// recompute the key of an immutable record on every write, and is refused
/// where the `id` field is defined; a field carrying one is not read here
/// either, so the two agree.
pub(crate) async fn id_default(
	ctx: &WriteCtx,
	id_field: Option<&FieldDefinition>,
) -> Result<Option<(ExecutionContext, Arc<dyn PhysicalExpr>)>, ControlFlow> {
	let Some(fd) = id_field.filter(|fd| matches!(fd.default, DefineDefault::Set(_))) else {
		return Ok(None);
	};
	let frame = field_default_frame(ctx.exec(), fd).map_err(ControlFlow::Err)?;
	let planner =
		crate::exec::vm::descriptor::write_unit_planner(frame.ctx(), frame.function_registry());
	let compiled =
		field_default_physical(fd, &planner).await.map_err(|e| ControlFlow::Err(e.into()))?;
	Ok(compiled.map(|expr| (frame, expr)))
}

/// Synthesise a key for a record nothing supplied one for, conforming to the
/// declared `id` kind.
///
/// A kind that admits more than one value and cannot be sampled — an `int`, an
/// `object`, a union — has no key to synthesise, and the statement is told so
/// rather than given a key the declaration forbids.
pub(crate) fn generate_typed_id(
	table: &TableName,
	kind: Option<&Kind>,
) -> Result<RecordId, ControlFlow> {
	let key = match kind {
		// No declared kind, or an unconstrained / string id: a random string.
		None | Some(Kind::Any | Kind::String) => RecordIdKey::rand(),
		// A uuid id: a fresh, time-ordered UUIDv7.
		Some(Kind::Uuid) => RecordIdKey::uuid(),
		// A singleton scalar literal has exactly one valid value, so the
		// declaration itself is the key.
		Some(Kind::Literal(KindLiteral::Integer(i))) => RecordIdKey::Number(*i),
		Some(Kind::Literal(KindLiteral::String(s))) => RecordIdKey::String(s.clone()),
		Some(other) => {
			return Err(fail(DocError::IdFieldGenerateUnsupported {
				table: table.to_string(),
				kind: other.to_sql(),
			}));
		}
	};
	Ok(RecordId {
		table: table.clone(),
		key,
	})
}

/// Hold a record id's key to the declared `id` kind.
///
/// Returns the id unchanged where no kind is declared, and where the kind
/// constrains the outer record rather than the key. Otherwise the key is
/// converted to a value, coerced, and converted back through the same path a
/// user-supplied id takes, so a synthesised key and a supplied one end up
/// under one rule.
fn coerce_id_key(id: RecordId, kind: Option<&Kind>) -> Result<RecordId, ControlFlow> {
	let Some(kind) = kind else {
		return Ok(id);
	};
	if kind.is_record() {
		return Ok(id);
	}
	// The key is cloned so the original id stays whole for the failure, which
	// is rendered only when there is one rather than on every typed-id write.
	match id.key.clone().into_value().coerce_to_kind(kind) {
		Ok(coerced) => coerced.generate(id.table, false).map_err(ControlFlow::Err),
		Err(error) => Err(fail(DocError::FieldCoerce {
			record: id.to_sql(),
			field_name: "id".to_string(),
			error: Box::new(error),
		})),
	}
}

/// Abandon the statement with a named failure.
fn fail(error: impl std::error::Error + Send + Sync + 'static) -> ControlFlow {
	ControlFlow::Err(anyhow::Error::new(error))
}

#[cfg(all(test, feature = "kv-mem"))]
#[allow(clippy::unwrap_used)]
mod tests {
	use super::*;
	use crate::dbs::Session;
	use crate::exec::ExecutionContext;
	use crate::exec::operators::test_util::{TestDb, physical_expr, val, write_place};
	use crate::exec::operators::write::clauses::WriteAssignment;
	use crate::exec::operators::write::ctx::TableExistence;
	use crate::kvs::TransactionType;
	use crate::val::{RecordIdKey, TableName};

	/// An anonymous session on `test`/`test`, whose reads are permission
	/// checked under a datastore built with server auth enabled.
	fn anonymous() -> Session {
		Session::default().with_ns("test").with_db("test")
	}

	/// The write context one statement resolves for `table`.
	async fn write_ctx(exec: &ExecutionContext, table: &str, clauses: WriteClauses) -> WriteCtx {
		WriteCtx::resolve(
			exec,
			&TableName::from(table),
			TableExistence::Required,
			None,
			Arc::new(clauses),
		)
		.await
		.expect("the write context resolves")
	}

	/// `SET <place> = <src>`, compiled.
	async fn set(place: &str, src: &str, exec: &ExecutionContext) -> WriteData {
		WriteData::Set(vec![WriteAssignment {
			place: write_place(place, exec).await,
			operator: AssignOperator::Assign,
			value: physical_expr(src, exec).await,
		}])
	}

	/// The value a `SET` clause evaluated for its one assignment.
	fn assigned(row: &WriteRow) -> &Value {
		let input = row.input.as_ref().expect("the data clause was evaluated");
		let ComputedInput::Set(assignments) = input else {
			panic!("expected a SET clause, got {input:?}");
		};
		&assignments[0].value
	}

	/// `<table>:<n>`.
	fn rid(table: &str, n: i64) -> RecordId {
		RecordId {
			table: table.into(),
			key: RecordIdKey::Number(n),
		}
	}

	/// A record the condition rejects is abandoned, not failed: the statement
	/// succeeds and every other record it touches still runs.
	#[tokio::test]
	async fn a_condition_that_does_not_hold_skips_the_record() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS; CREATE t:1 SET n = 1;").await;
		let exec = db.exec_ctx_as(&TestDb::owner(), TransactionType::Read).await;
		let clauses = WriteClauses {
			cond: Some(physical_expr("n > 10", &exec).await),
			..Default::default()
		};
		let ctx = write_ctx(&exec, "t", clauses).await;
		let stage = stage(StageId::CheckWhereCondition).expect("the stage is registered");

		let mut rejected = WriteRow::for_record(val("{ id: t:1, n: 1 }").await).unwrap();
		assert_eq!(stage.apply(&mut rejected, &ctx).await.unwrap(), StageOutcome::SkipRow);

		let mut accepted = WriteRow::for_record(val("{ id: t:2, n: 11 }").await).unwrap();
		assert_eq!(stage.apply(&mut accepted, &ctx).await.unwrap(), StageOutcome::Continue);
	}

	/// The data clause reads the record as its caller may read it, so a field
	/// the caller may not select cannot be copied into one it may.
	#[tokio::test]
	async fn the_data_clause_reads_the_permission_reduced_pre_image() {
		let db = TestDb::new_with_auth(
			"
			DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
			DEFINE FIELD secret ON t TYPE option<string> PERMISSIONS FOR select NONE;
			CREATE t:1 SET secret = 's';
			",
		)
		.await;
		let record = val("{ id: t:1, secret: 's' }").await;

		let checked = db.exec_ctx_as(&anonymous(), TransactionType::Read).await;
		let clauses = WriteClauses {
			data: Some(set("copy", "secret", &checked).await),
			..Default::default()
		};
		let ctx = write_ctx(&checked, "t", clauses).await;
		let stage = stage(StageId::ComputeInputData).expect("the stage is registered");
		let mut row = WriteRow::for_record(record.clone()).unwrap();
		assert_eq!(stage.apply(&mut row, &ctx).await.unwrap(), StageOutcome::Continue);
		assert_eq!(assigned(&row), &Value::None, "a denied field reads as absent");

		// The same clause for a caller whose reads are not checked reads the
		// field, so the absence above is the guard's answer and not a blanket
		// emptiness of the snapshot.
		let unchecked = db.exec_ctx_as(&TestDb::owner(), TransactionType::Read).await;
		let clauses = WriteClauses {
			data: Some(set("copy", "secret", &unchecked).await),
			..Default::default()
		};
		let ctx = write_ctx(&unchecked, "t", clauses).await;
		let mut row = WriteRow::for_record(record).unwrap();
		assert_eq!(stage.apply(&mut row, &ctx).await.unwrap(), StageOutcome::Continue);
		assert_eq!(assigned(&row), &Value::from("s"));
	}

	/// The evaluation is memoised, because re-running it would re-run the
	/// user's expressions.
	#[tokio::test]
	async fn the_data_clause_is_evaluated_once_per_record() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS; CREATE t:1 SET n = 1;").await;
		let exec = db.exec_ctx_as(&TestDb::owner(), TransactionType::Read).await;
		let clauses = WriteClauses {
			data: Some(set("m", "n", &exec).await),
			..Default::default()
		};
		let ctx = write_ctx(&exec, "t", clauses).await;
		let stage = stage(StageId::ComputeInputData).expect("the stage is registered");

		let mut row = WriteRow::for_record(val("{ id: t:1, n: 1 }").await).unwrap();
		stage.apply(&mut row, &ctx).await.unwrap();
		assert_eq!(assigned(&row), &Value::from(1));

		// A second pass over a record whose value has since changed keeps the
		// first evaluation rather than producing a second one.
		row.current = val("{ id: t:1, n: 99 }").await;
		stage.apply(&mut row, &ctx).await.unwrap();
		assert_eq!(assigned(&row), &Value::from(1));
	}

	/// A creating statement whose target named a table takes its key from the
	/// data clause, so the clause's `id` is the record's identity rather than
	/// a second claim about it.
	#[tokio::test]
	async fn the_id_check_stands_down_when_an_id_is_still_to_be_derived() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let exec = db.exec_ctx_as(&TestDb::owner(), TransactionType::Read).await;
		let ctx = write_ctx(&exec, "t", WriteClauses::default()).await;
		let stage = stage(StageId::CheckDataFields).expect("the stage is registered");
		let content = ComputedInput::Content(Arc::new(val("{ id: t:7 }").await));

		let mut deriving = WriteRow::for_target(Value::Table("t".into())).unwrap();
		deriving.input = Some(content.clone());
		assert!(deriving.generate_in.is_some(), "the target named a table");
		assert_eq!(stage.apply(&mut deriving, &ctx).await.unwrap(), StageOutcome::Continue);

		// The same clause against a statement that named the record itself is
		// a disagreement about which record is being written, and fails.
		let mut named = WriteRow::for_target(Value::RecordId(rid("t", 1))).unwrap();
		named.input = Some(content);
		let err = stage.apply(&mut named, &ctx).await.expect_err("the ids disagree");
		let ControlFlow::Err(err) = err else {
			panic!("expected a failure, got a loop or return signal");
		};
		assert!(err.to_string().contains("id"), "the failure names the field: {err}");
	}

	/// An edge's endpoints are its identity: a statement relating a pair that
	/// is not the pair the stored edge holds fails rather than re-pointing it.
	#[tokio::test]
	async fn an_edge_endpoint_cannot_be_repointed() {
		let db = TestDb::new("DEFINE TABLE e SCHEMALESS;").await;
		let exec = db.exec_ctx_as(&TestDb::owner(), TransactionType::Read).await;
		let ctx = write_ctx(&exec, "e", WriteClauses::default()).await;
		let stage = stage(StageId::DefaultRecordData).expect("the stage is registered");

		let mut same = WriteRow::for_record(val("{ id: e:1, in: a:1, out: b:1 }").await).unwrap();
		same.extras = WriteExtras::Relate(rid("a", 1), rid("b", 1), None);
		assert_eq!(stage.apply(&mut same, &ctx).await.unwrap(), StageOutcome::Continue);
		assert_eq!(
			same.metadata.as_ref().map(|m| m.record_type),
			Some(RecordType::Edge {
				variant: LATEST_EDGE_VARIANT,
			}),
			"the marker is stamped before the record is written"
		);

		let mut moved = WriteRow::for_record(val("{ id: e:1, in: a:2, out: b:1 }").await).unwrap();
		moved.extras = WriteExtras::Relate(rid("a", 1), rid("b", 1), None);
		let err = stage.apply(&mut moved, &ctx).await.expect_err("the endpoint disagrees");
		let ControlFlow::Err(err) = err else {
			panic!("expected a failure, got a loop or return signal");
		};
		assert!(err.to_string().contains("`in` field"), "the failure names the endpoint: {err}");
	}

	/// An `INSERT`'s row value is merged onto a record that already carries the
	/// id the statement derived, so a row supplying none still gets one.
	#[tokio::test]
	async fn the_inserted_row_is_merged_onto_the_derived_id() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let exec = db.exec_ctx_as(&TestDb::owner(), TransactionType::Read).await;
		let ctx = write_ctx(&exec, "t", WriteClauses::default()).await;
		let stage = stage(StageId::ProcessMergeData).expect("the stage is registered");

		let mut row = WriteRow::for_target(Value::RecordId(rid("t", 1))).unwrap();
		row.extras = WriteExtras::Insert(Arc::new(val("{ n: 1 }").await));
		assert_eq!(stage.apply(&mut row, &ctx).await.unwrap(), StageOutcome::Continue);
		assert_eq!(row.current, val("{ id: t:1, n: 1 }").await);
		assert!(!row.fields_computed, "the record's values changed");
	}

	/// A clause of the statement is not a place a loop can be left from.
	///
	/// `BREAK` and `CONTINUE` inside one have no loop here to leave, so they
	/// are an error rather than a signal that escapes the statement. Without
	/// this, `FOR $i IN 0..3 { UPDATE t SET n = 1 WHERE ({ BREAK }) }` would
	/// end the loop instead of failing, which is not what the record's clause
	/// asked for and not what the same query does on the document pipeline.
	#[tokio::test]
	async fn a_loop_signal_raised_in_a_clause_does_not_escape_the_statement() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS; CREATE t:1 SET n = 1;").await;
		let exec = db.exec_ctx_as(&TestDb::owner(), TransactionType::Read).await;

		for (clauses, id) in [
			(
				WriteClauses {
					cond: Some(physical_expr("{ BREAK }", &exec).await),
					..Default::default()
				},
				StageId::CheckWhereCondition,
			),
			(
				WriteClauses {
					data: Some(set("n", "{ CONTINUE }", &exec).await),
					..Default::default()
				},
				StageId::ComputeInputData,
			),
		] {
			let ctx = write_ctx(&exec, "t", clauses).await;
			let stage = stage(id).expect("the stage is registered");
			let mut row = WriteRow::for_record(val("{ id: t:1, n: 1 }").await).unwrap();
			match stage.apply(&mut row, &ctx).await {
				Err(ControlFlow::Err(err)) => assert!(
					err.to_string().to_lowercase().contains("break")
						|| err.to_string().to_lowercase().contains("continue"),
					"{} raised {err}, which does not name the invalid signal",
					id.name()
				),
				other => panic!("{} let a loop signal escape: {other:?}", id.name()),
			}
		}
	}

	/// `RETURN x` inside a clause is that clause's value, not a return from
	/// the statement. It is how a block-shaped clause answers at all.
	#[tokio::test]
	async fn a_return_inside_a_clause_is_the_clause_value() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS; CREATE t:1 SET n = 1;").await;
		let exec = db.exec_ctx_as(&TestDb::owner(), TransactionType::Read).await;
		let clauses = WriteClauses {
			data: Some(set("n", "{ RETURN 7 }", &exec).await),
			..Default::default()
		};
		let ctx = write_ctx(&exec, "t", clauses).await;
		let stage = stage(StageId::ComputeInputData).expect("the stage is registered");
		let mut row = WriteRow::for_record(val("{ id: t:1, n: 1 }").await).unwrap();
		assert_eq!(stage.apply(&mut row, &ctx).await.unwrap(), StageOutcome::Continue);
		assert_eq!(assigned(&row), &Value::from(7));
	}
	/// The whole point: the evaluated clause reaches the record.
	///
	/// The evaluated clause is left on the row, because the `$input` binding
	/// and the retry path both read it after this; the derived halves of the
	/// pre-mutation snapshot are dropped, because the record they described has
	/// changed.
	#[tokio::test]
	async fn the_data_clause_reaches_the_record() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS; CREATE t:1 SET n = 1;").await;
		let exec = db.exec_ctx_as(&TestDb::owner(), TransactionType::Read).await;
		let clauses = WriteClauses {
			data: Some(set("m", "n + 1", &exec).await),
			..Default::default()
		};
		let ctx = write_ctx(&exec, "t", clauses).await;
		let mut row = WriteRow::for_record(val("{ id: t:1, n: 1 }").await).unwrap();
		row.current_reduced = Some(val("{ id: t:1, n: 1 }").await);
		row.fields_computed = true;

		let compute = stage(StageId::ComputeInputData).expect("the stage is registered");
		compute.apply(&mut row, &ctx).await.unwrap();
		let apply = stage(StageId::ProcessRecordData).expect("the stage is registered");
		assert_eq!(apply.apply(&mut row, &ctx).await.unwrap(), StageOutcome::Continue);

		assert_eq!(row.current, val("{ id: t:1, n: 1, m: 2 }").await);
		assert!(row.input.is_some(), "the evaluated clause is still there for `$input`");
		assert!(row.current_reduced.is_none(), "the reduced view describes the old record");
		assert!(!row.fields_computed, "the computed fields describe the old record");
	}

	/// Every clause form the statement can carry lands on the record, and each
	/// lands as itself: `CONTENT` and `REPLACE` replace, `MERGE` overlays,
	/// `UNSET` cuts, `PATCH` applies its operations.
	#[tokio::test]
	async fn every_clause_form_is_applied_as_itself() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let exec = db.exec_ctx_as(&TestDb::owner(), TransactionType::Read).await;
		let record = "{ id: t:1, a: 1, b: 2 }";
		let cases = [
			(WriteData::Content(physical_expr("{ c: 3 }", &exec).await), "{ c: 3 }"),
			(WriteData::Replace(physical_expr("{ c: 3 }", &exec).await), "{ c: 3 }"),
			(WriteData::Merge(physical_expr("{ b: 9 }", &exec).await), "{ id: t:1, a: 1, b: 9 }"),
			(WriteData::Unset(vec![write_place("a", &exec).await]), "{ id: t:1, b: 2 }"),
			(
				WriteData::Patch(
					physical_expr("[{ op: 'replace', path: '/a', value: 7 }]", &exec).await,
				),
				"{ id: t:1, a: 7, b: 2 }",
			),
		];
		for (data, expected) in cases {
			let rendered = format!("{data:?}");
			let clauses = WriteClauses {
				data: Some(data),
				..Default::default()
			};
			let ctx = write_ctx(&exec, "t", clauses).await;
			let mut row = WriteRow::for_record(val(record).await).unwrap();
			stage(StageId::ComputeInputData)
				.expect("the stage is registered")
				.apply(&mut row, &ctx)
				.await
				.unwrap();
			stage(StageId::ProcessRecordData)
				.expect("the stage is registered")
				.apply(&mut row, &ctx)
				.await
				.unwrap();
			assert_eq!(row.current, val(expected).await, "applying {rendered}");
		}
	}

	/// A `PATCH` pointer that reads a field the caller may not select is
	/// refused: `copy` and `move` would land the value where the caller can see
	/// it, and `test` would report it in the mismatch. The other clause forms
	/// need no such gate, because their expressions read the reduced snapshot
	/// and already see `NONE` there.
	#[tokio::test]
	async fn a_patch_pointer_into_a_denied_field_is_refused() {
		let db = TestDb::new_with_auth(
			"
			DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
			DEFINE FIELD secret ON t TYPE option<string> PERMISSIONS FOR select NONE;
			CREATE t:1 SET secret = 's';
			",
		)
		.await;
		let record = val("{ id: t:1, secret: 's' }").await;
		let ops = "[{ op: 'copy', from: '/secret', path: '/leak' }]";

		let checked = db.exec_ctx_as(&anonymous(), TransactionType::Read).await;
		let clauses = WriteClauses {
			data: Some(WriteData::Patch(physical_expr(ops, &checked).await)),
			..Default::default()
		};
		let ctx = write_ctx(&checked, "t", clauses).await;
		let mut row = WriteRow::for_record(record.clone()).unwrap();
		stage(StageId::ComputeInputData)
			.expect("the stage is registered")
			.apply(&mut row, &ctx)
			.await
			.unwrap();
		let err = stage(StageId::ProcessRecordData)
			.expect("the stage is registered")
			.apply(&mut row, &ctx)
			.await
			.expect_err("the pointer reads a denied field");
		let ControlFlow::Err(err) = err else {
			panic!("expected a failure, got a loop or return signal");
		};
		assert!(err.to_string().contains("secret"), "the refusal names the pointer: {err}");
		assert_eq!(row.current, record, "a refused patch leaves the record alone");

		// The same patch for a caller whose reads are not checked reads the
		// field and applies, so the refusal above is the guard's answer rather
		// than a blanket ban on the pointer.
		let unchecked = db.exec_ctx_as(&TestDb::owner(), TransactionType::Read).await;
		let clauses = WriteClauses {
			data: Some(WriteData::Patch(physical_expr(ops, &unchecked).await)),
			..Default::default()
		};
		let ctx = write_ctx(&unchecked, "t", clauses).await;
		let mut row = WriteRow::for_record(record).unwrap();
		stage(StageId::ComputeInputData)
			.expect("the stage is registered")
			.apply(&mut row, &ctx)
			.await
			.unwrap();
		stage(StageId::ProcessRecordData)
			.expect("the stage is registered")
			.apply(&mut row, &ctx)
			.await
			.unwrap();
		assert_eq!(row.current, val("{ id: t:1, secret: 's', leak: 's' }").await);
	}

	/// A statement that named a table gets a record id derived in it, and the
	/// table it was derived in stays on the row: `check_data_fields` stands
	/// down while it is there, and a unique-index conflict is retryable as an
	/// update only while it is there.
	#[tokio::test]
	async fn a_target_naming_a_table_gets_an_id_and_keeps_the_table() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let exec = db.exec_ctx_as(&TestDb::owner(), TransactionType::Read).await;
		let ctx = write_ctx(&exec, "t", WriteClauses::default()).await;
		let stage = stage(StageId::GenerateRecordId).expect("the stage is registered");

		let mut row = WriteRow::for_target(Value::Table("t".into())).unwrap();
		assert_eq!(stage.apply(&mut row, &ctx).await.unwrap(), StageOutcome::Continue);
		let id = row.id.clone().expect("an id was derived");
		assert_eq!(id.table, TableName::from("t"));
		assert!(matches!(id.key, RecordIdKey::String(_)), "a table with no `id` kind: {id:?}");
		assert_eq!(
			row.generate_in,
			Some(TableName::from("t")),
			"the table an id was derived in is still on the row"
		);

		// An id the record already carries is the record's identity rather
		// than something to derive one over, which is what makes
		// `CREATE t CONTENT { id: t:7 }` write at `t:7`.
		let mut supplied = WriteRow::for_target(Value::Table("t".into())).unwrap();
		supplied.current = val("{ id: t:7, n: 1 }").await;
		assert_eq!(stage.apply(&mut supplied, &ctx).await.unwrap(), StageOutcome::Continue);
		assert_eq!(supplied.id, Some(rid("t", 7)));
		assert!(supplied.generate_in.is_some(), "a supplied id derives nothing and clears nothing");
	}

	/// A statement that named the record itself derives nothing.
	#[tokio::test]
	async fn a_target_naming_a_record_keeps_its_own_id() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let exec = db.exec_ctx_as(&TestDb::owner(), TransactionType::Read).await;
		let ctx = write_ctx(&exec, "t", WriteClauses::default()).await;
		let stage = stage(StageId::GenerateRecordId).expect("the stage is registered");

		let mut row = WriteRow::for_target(Value::RecordId(rid("t", 1))).unwrap();
		assert_eq!(stage.apply(&mut row, &ctx).await.unwrap(), StageOutcome::Continue);
		assert_eq!(row.id, Some(rid("t", 1)));
		assert!(row.generate_in.is_none(), "nothing was to be derived");
	}

	/// The `id` field's `DEFAULT` supplies the key where the statement did
	/// not, and reads the record it is deriving an id for.
	///
	/// `INSERT` is the one form whose id is derived before its row value is
	/// merged, so there the clause reads that payload instead — which is what
	/// makes the same clause name the same fields under both forms.
	#[tokio::test]
	async fn the_id_fields_default_supplies_the_key() {
		let db = TestDb::new(
			"
			DEFINE TABLE t SCHEMALESS;
			DEFINE FIELD name ON t TYPE option<string>;
			DEFINE FIELD id ON t TYPE string DEFAULT name;
			",
		)
		.await;
		let exec = db.exec_ctx_as(&TestDb::owner(), TransactionType::Read).await;
		let ctx = write_ctx(&exec, "t", WriteClauses::default()).await;
		let stage = stage(StageId::GenerateRecordId).expect("the stage is registered");

		let mut created = WriteRow::for_target(Value::Table("t".into())).unwrap();
		created.current = val("{ name: 'bob' }").await;
		assert_eq!(stage.apply(&mut created, &ctx).await.unwrap(), StageOutcome::Continue);
		assert_eq!(
			created.id.as_ref().map(|id| id.key.clone()),
			Some(RecordIdKey::from("bob".to_string()))
		);

		let mut inserted = WriteRow::for_target(Value::Table("t".into())).unwrap();
		inserted.extras = WriteExtras::Insert(Arc::new(val("{ name: 'ann' }").await));
		assert_eq!(stage.apply(&mut inserted, &ctx).await.unwrap(), StageOutcome::Continue);
		assert_eq!(
			inserted.id.as_ref().map(|id| id.key.clone()),
			Some(RecordIdKey::from("ann".to_string()))
		);
	}

	/// The `id` field's declared kind governs the key whichever way it was
	/// arrived at: a synthesised key conforms to it, a supplied key that does
	/// not is refused, and one that does is left alone.
	#[tokio::test]
	async fn a_declared_id_kind_governs_the_key() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS; DEFINE FIELD id ON t TYPE uuid;").await;
		let exec = db.exec_ctx_as(&TestDb::owner(), TransactionType::Read).await;
		let ctx = write_ctx(&exec, "t", WriteClauses::default()).await;
		let stage = stage(StageId::GenerateRecordId).expect("the stage is registered");

		let mut derived = WriteRow::for_target(Value::Table("t".into())).unwrap();
		assert_eq!(stage.apply(&mut derived, &ctx).await.unwrap(), StageOutcome::Continue);
		let key = derived.id.expect("an id was derived").key;
		assert!(matches!(key, RecordIdKey::Uuid(_)), "the declared kind synthesised: {key:?}");

		// A key the kind admits is carried through unchanged, so the refusal
		// below is the declaration's answer and not a ban on naming a record.
		let admitted = RecordId {
			table: "t".into(),
			key: RecordIdKey::uuid(),
		};
		let mut named = WriteRow::for_target(Value::RecordId(admitted.clone())).unwrap();
		assert_eq!(stage.apply(&mut named, &ctx).await.unwrap(), StageOutcome::Continue);
		assert_eq!(named.id, Some(admitted));

		let mut wrong = WriteRow::for_target(Value::RecordId(rid("t", 1))).unwrap();
		let err = stage.apply(&mut wrong, &ctx).await.expect_err("an int is not a uuid");
		let ControlFlow::Err(err) = err else {
			panic!("expected a failure, got a loop or return signal");
		};
		assert!(err.to_string().contains("id"), "the failure names the field: {err}");
	}

	/// `$input` is what the statement supplied, not what the record ended up
	/// holding: a `SET` clause materialises against nothing, so a compound
	/// operator in it resolves against `NONE`.
	#[tokio::test]
	async fn the_input_value_is_the_clause_the_statement_supplied() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS; CREATE t:1 SET n = 40;").await;
		let exec = db.exec_ctx_as(&TestDb::owner(), TransactionType::Read).await;
		let clauses = WriteClauses {
			data: Some(WriteData::Set(vec![WriteAssignment {
				place: write_place("n", &exec).await,
				operator: AssignOperator::Add,
				value: physical_expr("2", &exec).await,
			}])),
			..Default::default()
		};
		let ctx = write_ctx(&exec, "t", clauses).await;
		let mut row = WriteRow::for_record(val("{ id: t:1, n: 40 }").await).unwrap();

		// Before the clause is evaluated there is nothing to materialise, which
		// is why every caller runs after `ComputeInputData`.
		assert!(input_value(&row, &ctx).await.unwrap().is_none());

		stage(StageId::ComputeInputData)
			.expect("the stage is registered")
			.apply(&mut row, &ctx)
			.await
			.unwrap();
		let input = input_value(&row, &ctx).await.unwrap().expect("the clause was evaluated");
		assert_eq!(*input, val("{ n: 2 }").await, "`$input` resolves against NONE, not 40");

		// The record itself still combines with what it holds.
		stage(StageId::ProcessRecordData)
			.expect("the stage is registered")
			.apply(&mut row, &ctx)
			.await
			.unwrap();
		assert_eq!(row.current, val("{ id: t:1, n: 42 }").await);
	}
}
