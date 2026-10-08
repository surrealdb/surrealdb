//! The table's field schema, applied to one record.
//!
//! The field kernel: types, defaults, `VALUE`, `ASSERT`, the cleanup of what the schema leaves
//! empty, and the `COMPUTED` derivation the record's observers read.
//!
//! Definer's-rights narrowing applies here (ADR 0001): a field clause runs
//! with the definer's authority, not the caller's, and so does the decision of
//! whether that field's `PERMISSIONS` are enforced at all. The narrowing is
//! per field, because the fields of one table need not share a definer.
//!
//! # A stored clause needs a compilation route
//!
//! The kernel evaluates clauses the catalog stores as text, and the
//! engine reaches one through [`crate::exec::permission`], which compiles the
//! logical body the definition carries and, for a clause carrying none,
//! carries it as an [`UncompilableClause`](crate::exec::permission::UncompilableClause)
//! that fails where it is evaluated. A field carries a logical body for its
//! `DEFAULT`, its `VALUE`, its `ASSERT`, its `COMPUTED` and its `PERMISSIONS
//! FOR select`; its `PERMISSIONS FOR create` / `update` predicate carries none
//! and is therefore always the carried form. [`clause_frame`] binds the
//! parameters every clause reads, so a clause means the same thing whichever
//! form it takes. A clause that fails closed fails the write that reaches it
//! rather than letting the record through unjudged.

use std::collections::HashSet;
use std::sync::Arc;

use surrealdb_types::ToSql;

use crate::catalog::{DefineDefault, FieldDefinition, LogicalBody};
use crate::doc::Error as DocError;
use crate::err::EngineError;
use crate::exe::FlowResultExt as _;
use crate::exec::operators::scan::pipeline::compute_fields_for_value;
use crate::exec::operators::write::apply::materialise_input;
use crate::exec::operators::write::clauses::WriteData;
use crate::exec::operators::write::ctx::WriteCtx;
use crate::exec::operators::write::row::WriteRow;
use crate::exec::operators::write::stage::{StageId, StageOutcome, WriteStage};
use crate::exec::permission::{
	GuardKind, PhysicalPermission, check_write_permission_for_value, field_default_frame,
	field_default_physical, permission_to_physical, should_check_perms,
};
use crate::exec::planner::Planner;
use crate::exec::{BoxFut, EvalContext, ExecutionContext, PhysicalExpr};
use crate::expr::idiom::{Idiom, IdiomTrie, IdiomTrieContains};
use crate::expr::kind::Kind;
use crate::expr::part::Part;
use crate::expr::statements::define::kind_contains_object;
use crate::expr::{ControlFlow, ControlFlowExt, Expr, FlowResult};
use crate::iam::Action;
use crate::val::value::every::ArrayBehaviour;
use crate::val::{RecordId, Value};

/// This module's stages, by id.
///
/// `None` is a stage whose body has not landed:
/// [`WritePlan::resolve`](crate::exec::operators::write::plan::WritePlan)
/// refuses to bind a plan that names one, rather than running a partial order.
// `stages::lookup` is the total router over `StageId` and holds no wildcard, so
// a new stage still has to be given an owner before it compiles. The wildcard
// below is the arm for a stage this module does not own; enumerating every
// other variant here would restate `lookup`'s table in nine places and go stale
// independently of it.
#[allow(clippy::wildcard_enum_match_arm)]
pub(crate) fn stage(id: StageId) -> Option<Arc<dyn WriteStage>> {
	match id {
		StageId::CleanupTableFields => Some(Arc::new(CleanupTableFields)),
		StageId::MaterialiseObservedFields => Some(Arc::new(MaterialiseObservedFields)),
		StageId::ProcessTableFields => Some(Arc::new(ProcessTableFields)),
		// `super::lookup` routes only this module's stages here, so anything
		// else is a routing mistake rather than a missing body. Answering
		// `None` leaves it to `resolve` to name, as it names any unbound stage.
		_ => None,
	}
}

/// Apply the table's field schema to the record the statement built.
///
/// Sits after the data clause and the special-field stamp and before the
/// cleanup and the post-write permission gate, so what it produces is what
/// that gate judges and, once the cleanup has run, what storage receives.
///
/// Fields are visited in the dependency order
/// [`WriteCtx::field_order`](crate::exec::operators::write::ctx::WriteCtx)
/// carries, so a clause reading another field of the same record sees that
/// field's processed value rather than the one the statement started from. The
/// order also carries the cycle the table's producing clauses form when they
/// form one: the *record* is rejected there, not the table, because reading the
/// table is unaffected by it.
///
/// Each field is applied at every place its path names in the record — a path
/// spanning an array names one place per element — and at each place:
///
/// - the `id` field may not change once the record exists, and is otherwise left alone: its kind
///   and its `DEFAULT` are enforced where the record id is derived, and coercing it again here
///   would desync the value from the storage key it became;
/// - a `READONLY` field may not change either. A place left empty by a `CONTENT` clause is restored
///   instead of refused, because `CONTENT` restates the whole record and an omission there is not a
///   change;
/// - `DEFAULT` supplies a value for a place that has none — on a record being created, and on every
///   write for `DEFAULT ALWAYS`. A field declaring no `DEFAULT` takes a static `VALUE` as one, an
///   expression that reads nothing of the record, of the statement or of the environment;
/// - `TYPE` coerces what the place holds, and a value that will not coerce fails the statement;
/// - `VALUE` rewrites the value and reorders the type check around itself. A place the record left
///   empty is produced by the clause and coerced after it; a place that holds a value is coerced
///   first, so the clause reads the value under the kind the field declares rather than the one the
///   caller wrote, and coerced again after it, which holds the clause's own output to that kind
///   too;
/// - the field's `PERMISSIONS FOR create` / `update` reverts a change the caller may not make —
///   `NONE`, or a predicate judged against the record with the place's own values bound. It is
///   neither an error nor a skipped record: the place keeps the value the record already held and
///   the rest of the write proceeds. Whether the clause is consulted at all is the field's narrowed
///   authority's to answer, not the caller's.
///
/// A place that comes out `NONE` under a kind admitting `NONE` makes the
/// field's children skippable: their clauses do not run, because there is no
/// parent object for them to be about.
///
/// A `COMPUTED` field is derived rather than processed, and only when another
/// field's clause reads it. Every computed value is stripped again before this
/// returns — a computed field is never stored — and
/// [`WriteRow::fields_computed`] is cleared with it.
///
/// # The validation pass
///
/// Every `ASSERT` is deferred to a second pass, which runs once every field of
/// the record has been produced. Each clause therefore reads the record as it
/// will be stored: a sibling holds its final value whatever order production
/// happened in, and two fields asserting against each other
/// (`ASSERT in != out`) judge one record rather than each other's half-built
/// state. `$value` is read back from the record there rather than carried over
/// from production, because the field's `PERMISSIONS` clause may have reverted
/// it. An absent optional value is not judged at all — the field declared that
/// absence acceptable.
///
/// The pass runs before the computed values are stripped, so an `ASSERT`
/// reading a `COMPUTED` field reads the derived value; the field order carries
/// which computed fields the asserts and the producing clauses between them
/// name.
#[derive(Debug)]
struct ProcessTableFields;

impl WriteStage for ProcessTableFields {
	fn name(&self) -> &'static str {
		StageId::ProcessTableFields.name()
	}

	fn apply<'a>(
		&'a self,
		row: &'a mut WriteRow,
		ctx: &'a WriteCtx,
	) -> BoxFut<'a, Result<StageOutcome, ControlFlow>> {
		Box::pin(async move {
			// An import replays records as they were exported, and the state a
			// write derives around them was derived when they were first
			// written.
			if ctx.exec().options().is_some_and(|options| options.import) {
				return Ok(StageOutcome::Continue);
			}
			// Producing clauses that read each other in a cycle have no
			// evaluation order, so there is no record this write could
			// correctly produce.
			if let Some(cycle) = &ctx.field_order.cycle {
				return Err(fail(DocError::FieldDependencyCycle {
					table: ctx.table.name.as_str().to_string(),
					fields: cycle.join(", "),
				}));
			}
			let rid = record_id(row)?;
			// What the *statement* supplied, which the clauses below read at
			// the place each is about — not what the record ended up holding.
			// Derived once per record: for a `SET` clause it is the
			// assignments applied to an empty object, so a compound operator
			// there resolves against `NONE`.
			let input = statement_input(row, ctx).await?;
			// The before-image test, which is what decides whether a record is
			// being created — not the order the statement selected, which a
			// data clause carrying a record id can contradict.
			let creating = row.initial.is_none();
			// The path of a field left empty under a kind admitting `NONE`,
			// whose children are therefore not processed.
			let mut skip: Option<&Idiom> = None;
			// The places whose `ASSERT` the validation pass below judges, as
			// the field's index and the path within the record.
			let mut deferred_asserts: Vec<(usize, Idiom)> = Vec::new();

			for &index in ctx.field_order.order.iter() {
				let fd = &ctx.fields[index];
				// A computed value is derived, never taken from the record, so
				// it is not processed like the others. One that another
				// field's clause reads is derived here — the order places it
				// ahead of that clause — and the rest are left to the read
				// side, which evaluates only what a projection asks for.
				// Either way the value is stripped again below.
				if fd.computed.is_some() {
					if ctx.field_order.required_computed.contains(&index) {
						derive_computed_field(row, ctx, fd).await?;
					}
					continue;
				}
				// Every clause of this field, and the question of whether its
				// `PERMISSIONS` are enforced, is answered under the field's own
				// authority rather than the caller's.
				let frame = field_frame(ctx, fd)?;
				let guarded = field_permissions_enforced(&frame)?;
				// Compiled once per field rather than once per place: a
				// carried predicate clones the stored expression, and a field
				// path spanning an array names one place per element.
				let permission = match guarded {
					true => Some(field_permission(&frame, fd, creating).await?),
					false => None,
				};
				let skipped = skip.is_some_and(|parent| fd.name.starts_with(parent));
				if !skipped {
					skip = None;
				}
				let is_id = Some(index) == ctx.id_field;
				for (place, mut val) in row.current.walk(&fd.name) {
					let old = row.initial.pick(&place);
					// `$before` and `$input` are the place's own values, as
					// the field's clauses read them, not the record's.
					let supplied = input.pick(&place);
					// The key the record is stored under, which this stage
					// only checks: the id's kind and default were enforced
					// where it was derived.
					if is_id && !creating {
						if val != old {
							return Err(fail(DocError::FieldReadonly {
								field: fd.name.clone(),
								record: rid.to_sql(),
							}));
						}
						continue;
					}
					// A `READONLY` field may not change once the record
					// exists. A place a `CONTENT` clause left empty is the
					// clause restating the record rather than changing the
					// field, so the prior value is restored.
					if fd.readonly && !creating {
						if val == old {
							continue;
						}
						if val.is_none() && matches!(ctx.clauses.data, Some(WriteData::Content(_)))
						{
							row.current.put(&place, old);
							continue;
						}
						return Err(fail(DocError::FieldReadonly {
							field: fd.name.clone(),
							record: rid.to_sql(),
						}));
					}
					if !skipped {
						val =
							produce(&frame, fd, &row.current, &rid, &old, &supplied, creating, val)
								.await?;
						// Judged in the validation pass below rather than
						// here, against the record as it will be stored.
						if fd.assert.is_some() {
							deferred_asserts.push((index, place.clone()));
						}
					}
					// The permission clause is consulted for a skipped field
					// too, and its answer is discarded with the value, because
					// a skipped field is not written back.
					if let Some(permission) = &permission {
						val =
							permitted_value(&frame, permission, &row.current, &old, &supplied, val)
								.await?;
					}
					if !skipped {
						if optional_and_absent(fd, &val) {
							skip = Some(&fd.name);
						}
						// `put` on a `NONE` keeps an array's element
						// positions, which `cut` would shift.
						row.current.put(&place, val);
					}
				}
				normalise_set_elements(row, fd);
			}

			// The validation pass. Every `ASSERT` runs here, after every field
			// has been produced, so each one judges the record as it will be
			// stored rather than as it stood part-way through production. The
			// place's value is read back from the record for the same reason:
			// the field's `PERMISSIONS` clause may have reverted what the
			// field produced. Order is not a question — no clause here writes
			// anything back.
			for (index, path) in deferred_asserts {
				let fd = &ctx.fields[index];
				// The clause runs under the field's own authority, as every
				// other clause of it does, and the narrowing is per field.
				let frame = field_frame(ctx, fd)?;
				let val = row.current.pick(&path);
				let old = row.initial.pick(&path);
				let supplied = input.pick(&path);
				assert_clause(&frame, fd, &row.current, &rid, &old, &supplied, &val).await?;
			}

			// A computed value is never stored: the read side derives it from
			// the stored fields, so a projection that does not ask for one
			// never pays to evaluate it. Whatever the clauses above
			// materialised goes, along with any value the caller tried to
			// assign to a computed field.
			for fd in ctx.fields.iter() {
				if fd.computed.is_some() {
					row.current.cut(&fd.name);
				}
			}
			// SECURITY: the write permission gate judges its predicate against
			// a scratch copy of the post-image and derives onto it only the
			// computed values this flag says the post-image lacks. Left
			// standing with the values stripped, the copy inherits `true`, a
			// predicate naming a computed field reads `NONE`, and a clause
			// written to deny opens (GHSA-f6h4-5xf2-86q9).
			row.fields_computed = false;
			// The record's field values have changed, so the reduced image
			// derived from the previous ones no longer describes it.
			row.current_reduced = None;
			Ok(StageOutcome::Continue)
		})
	}
}

/// The value `$input` names for this record: what the *statement* supplied,
/// before any clause of the schema rewrote it, and `NONE` where it supplied
/// nothing.
///
/// Taken by exclusive reference rather than shared, as every write helper
/// spanning an await is: a `&WriteRow` held across one would require
/// `WriteRow: Sync`, which its memoised comparison cell deliberately is not.
/// Nothing here mutates the row.
/// [`input_value`](super::data::input_value) is the same derivation over a
/// shared borrow, which no stage can await.
///
/// The compiled clause it is paired with is the one
/// [`ComputeInputData`](super::data) evaluated: a statement's own data clause,
/// or the `ON DUPLICATE KEY UPDATE` clause of an `INSERT`, which never both
/// carry one.
pub(crate) async fn statement_input(
	row: &mut WriteRow,
	ctx: &WriteCtx,
) -> Result<Arc<Value>, ControlFlow> {
	let Some(computed) = row.input.as_ref() else {
		return Ok(Arc::new(Value::None));
	};
	let data =
		ctx.clauses.data.as_ref().or(ctx.clauses.duplicate_update.as_ref()).ok_or_else(|| {
			ControlFlow::Err(anyhow::anyhow!("an evaluated data clause with no clause behind it"))
		})?;
	materialise_input(data, computed, ctx.exec()).await
}

/// The frame every clause of one field is evaluated in.
///
/// ADR 0001: the caller's authority, narrowed to the field's `AUTH LIMIT`, so
/// a clause cannot exercise more privilege than the identity that defined the
/// field. It is [`crate::exec::permission`]'s narrowing for the field's
/// `DEFAULT`, which is the whole field's: the stage narrows once per
/// field and evaluates every clause of it — and decides whether that field's
/// `PERMISSIONS` are enforced — under the result. The frame is privileged: it
/// holds none of the caller's own bindings.
fn field_frame(ctx: &WriteCtx, fd: &FieldDefinition) -> Result<ExecutionContext, ControlFlow> {
	field_default_frame(ctx.exec(), fd).map_err(ControlFlow::Err)
}

/// The parameters a field clause reads, bound on `frame` for one place:
/// `$before` what the record held there before the statement, `$input` what
/// the statement supplied there, and `$value` and `$after` the value the
/// clause is about.
///
/// Bound on the context rather than on the evaluation ([`EvalContext`]'s write
/// bindings) because a plan the clause carries — a subquery — is executed
/// against the context rather than the evaluation, and so sees a parameter
/// only through the context chain. A compiled clause reads them either way:
/// parameter resolution consults the write bindings first and the context
/// chain after.
fn clause_frame(
	frame: &ExecutionContext,
	before: &Value,
	input: &Value,
	value: &Value,
) -> ExecutionContext {
	frame.with_params([
		("before", before.clone()),
		("input", input.clone()),
		("after", value.clone()),
		("value", value.clone()),
	])
}

/// Whether one field's `PERMISSIONS` clauses are enforced at all, under the
/// authority `frame` carries.
///
/// Answered per field rather than once per stage, because the answer is the
/// narrowed authority's: a caller the datastore exempts is still enforced
/// against a field whose definer held less than it does.
///
/// False for a principal whose role and actor level exempt it, with auth
/// disabled, and inside a permission predicate — where re-entering would
/// recurse without end on a cyclic link. Such a write is therefore not
/// reverted by the field's clause, and never reaches one.
fn field_permissions_enforced(frame: &ExecutionContext) -> Result<bool, ControlFlow> {
	let db_ctx = frame.database().context("the field kernel requires a database context")?;
	should_check_perms(db_ctx, Action::Edit).map_err(|e| ControlFlow::Err(e.into()))
}

/// The field's `PERMISSIONS FOR create` / `update` as the engine
/// applies it.
///
/// Which of the two clauses governs is the before-image test's to say, the
/// same test the rest of the stage reads.
///
/// The catalog holds no logical form of either clause — a field compiles a
/// logical body for its `DEFAULT`, its `COMPUTED` and its `PERMISSIONS FOR
/// select` and for nothing else — so the logical half handed to
/// [`permission_to_physical`] is the error that says so, and a predicate comes
/// back as a clause that fails when it is evaluated, which a permission
/// treats as a refusal of the statement. `FULL` and `NONE` come back as the
/// two unconditional answers, which evaluate nothing.
///
/// The planner is built without a transaction, as the write gates' is: a
/// planner that resolves no table context cannot recurse into a
/// self-referential predicate, and a clause compiled per record shares no
/// parent cycle guard that would otherwise break the recursion.
async fn field_permission(
	frame: &ExecutionContext,
	fd: &FieldDefinition,
	creating: bool,
) -> Result<PhysicalPermission, ControlFlow> {
	let permission = match creating {
		true => &fd.create_permission,
		false => &fd.update_permission,
	};
	let planner =
		crate::exec::vm::descriptor::write_unit_planner(frame.ctx(), frame.function_registry());
	let no_logical_form =
		Err(anyhow::anyhow!("a field's write PERMISSIONS clause carries no logical body"));
	permission_to_physical(permission, no_logical_form, GuardKind::Write, &planner)
		.await
		.map_err(|e| ControlFlow::Err(e.into()))
}

/// Produce one place's value: the field's `DEFAULT`, its `TYPE` and its
/// `VALUE`, in the order a `VALUE` clause imposes.
///
/// With no `VALUE` clause the order is `DEFAULT` then `TYPE`. With one, the
/// place's state at that point decides:
///
/// - a place holding nothing is produced by the clause and coerced after it, because there is no
///   value for a type check to be about until the clause has supplied one;
/// - a place holding a value is coerced first, so the clause reads it under the kind the field
///   declares rather than as the caller wrote it, then rewritten by the clause, then coerced again
///   — which holds the clause's own output to that kind as well.
///
/// `record` is the record as it stands, which is what a clause reading a bare
/// field path reads: the fields processed before this one hold their processed
/// values.
#[allow(clippy::too_many_arguments, reason = "a field clause's frame is this many values")]
async fn produce(
	frame: &ExecutionContext,
	fd: &FieldDefinition,
	record: &Value,
	rid: &RecordId,
	old: &Value,
	input: &Value,
	creating: bool,
	val: Value,
) -> Result<Value, ControlFlow> {
	let val = default_value(frame, fd, record, old, input, creating, val).await?;
	// Both halves name the same clause, so they are `Some` together; a field
	// with no `VALUE` has neither.
	let (Some(expr), Some(body)) = (fd.value.as_ref(), fd.value_body.as_ref()) else {
		return coerce_to_field_kind(fd, rid, val);
	};
	if val.is_none() {
		let val = value_clause(frame, expr, body, record, old, input, val).await?;
		return coerce_to_field_kind(fd, rid, val);
	}
	let val = coerce_to_field_kind(fd, rid, val)?;
	let val = value_clause(frame, expr, body, record, old, input, val).await?;
	coerce_to_field_kind(fd, rid, val)
}

/// Which clause supplies a field's default.
enum DefaultClause<'a> {
	/// The field's own `DEFAULT`, in either spelling.
	Declared,
	/// The field's `VALUE`, for a field that declares no `DEFAULT`. Only a
	/// static clause stands in for one: an expression that reads nothing of
	/// the record, of the statement or of the environment produces the same
	/// value here as it does when the clause itself runs, a moment later.
	StaticValue(&'a Expr, &'a LogicalBody),
}

/// The clause that supplies this field's default, or `None` for a field that
/// has neither a `DEFAULT` nor a static `VALUE`.
fn default_clause(fd: &FieldDefinition) -> Option<DefaultClause<'_>> {
	if fd.default.expression().is_some() {
		return Some(DefaultClause::Declared);
	}
	match (fd.value.as_ref(), fd.value_body.as_ref()) {
		(Some(expr), Some(body)) if expr.is_static() => {
			Some(DefaultClause::StaticValue(expr, body))
		}
		_ => None,
	}
}

/// The field's default, applied to a place that holds nothing.
#[allow(clippy::too_many_arguments, reason = "a field clause's frame is this many values")]
async fn default_value(
	frame: &ExecutionContext,
	fd: &FieldDefinition,
	record: &Value,
	old: &Value,
	input: &Value,
	creating: bool,
	val: Value,
) -> Result<Value, ControlFlow> {
	// Three things settle the clause without evaluating it: a place that
	// already holds a value, a plain `DEFAULT` on a record that already exists
	// — `DEFAULT ALWAYS` is the spelling that applies to every write rather
	// than to creation alone — and a field with no clause to supply one.
	if !val.is_none() {
		return Ok(val);
	}
	if !creating && !matches!(fd.default, DefineDefault::Always(_)) {
		return Ok(val);
	}
	let Some(clause) = default_clause(fd) else {
		return Ok(val);
	};
	let planner =
		crate::exec::vm::descriptor::write_unit_planner(frame.ctx(), frame.function_registry());
	// A `DEFAULT` carries a logical body the definition compiled once; a
	// `VALUE` standing in for one carries none, so each takes its own route.
	let compiled = match clause {
		DefaultClause::Declared => {
			field_default_physical(fd, &planner).await.map_err(|e| ControlFlow::Err(e.into()))?
		}
		DefaultClause::StaticValue(expr, body) => {
			Some(field_clause_physical(expr, body, &planner).await?)
		}
	};
	let Some(expr) = compiled else {
		return Ok(val);
	};
	// `$value` and `$after` are the place's own value, which is the `NONE`
	// that brought the clause here; `$before` is what the record held before
	// the statement, and `$input` what the statement supplied.
	let bound = clause_frame(frame, old, input, &val);
	flatten(expr.evaluate(EvalContext::from_exec_ctx(&bound).with_value_and_doc(record)).await)
}

/// The field's `VALUE`, applied to one place.
///
/// `val` is the value the clause is about — the place's own, after the
/// `DEFAULT` and, where the place held a value, after the type check — and is
/// what `$value` and `$after` are bound to. The clause's output replaces it,
/// whatever it was: a `VALUE` decides what the record holds.
#[allow(clippy::too_many_arguments, reason = "a field clause's frame is this many values")]
async fn value_clause(
	frame: &ExecutionContext,
	expr: &Expr,
	body: &LogicalBody,
	record: &Value,
	old: &Value,
	input: &Value,
	val: Value,
) -> Result<Value, ControlFlow> {
	let planner =
		crate::exec::vm::descriptor::write_unit_planner(frame.ctx(), frame.function_registry());
	let compiled = field_clause_physical(expr, body, &planner).await?;
	let bound = clause_frame(frame, old, input, &val);
	flatten(compiled.evaluate(EvalContext::from_exec_ctx(&bound).with_value_and_doc(record)).await)
}

/// The field's `ASSERT`, judged against one place of the record as it will be
/// stored.
///
/// The clause validates rather than produces: a truthy answer leaves the
/// record alone and anything else fails the statement, naming the value, the
/// field, the record and the clause. `val` is the place's stored value, read
/// back from the record by the caller, and is what `$value` and `$after` are
/// bound to.
///
/// An absent optional value is not judged — the field declared that absence
/// acceptable — and the test is made here rather than at the deferral site so
/// that it reads the stored value too.
#[allow(clippy::too_many_arguments, reason = "a field clause's frame is this many values")]
async fn assert_clause(
	frame: &ExecutionContext,
	fd: &FieldDefinition,
	record: &Value,
	rid: &RecordId,
	old: &Value,
	input: &Value,
	val: &Value,
) -> Result<(), ControlFlow> {
	let (Some(expr), Some(body)) = (fd.assert.as_ref(), fd.assert_body.as_ref()) else {
		return Ok(());
	};
	if optional_and_absent(fd, val) {
		return Ok(());
	}
	let planner =
		crate::exec::vm::descriptor::write_unit_planner(frame.ctx(), frame.function_registry());
	let compiled = field_clause_physical(expr, body, &planner).await?;
	let bound = clause_frame(frame, old, input, val);
	let answer = flatten(
		compiled.evaluate(EvalContext::from_exec_ctx(&bound).with_value_and_doc(record)).await,
	)?;
	match answer.is_truthy() {
		true => Ok(()),
		false => Err(fail(DocError::FieldValue {
			record: rid.to_sql(),
			field: fd.name.clone(),
			check: expr.to_sql(),
			value: val.to_sql(),
		})),
	}
}

/// A field's `VALUE` or `ASSERT` clause as this stage evaluates it, compiled
/// through `planner`.
///
/// `body` is the logical form the definition carries, lowered once and kept
/// for as long as the catalog holds the definition. Building one here instead
/// would render the stored expression back to text and re-parse it for every
/// record the clause is applied to. A clause whose text has no logical form,
/// or whose logical form holds a write no operator compiles, is carried as an
/// uncompilable clause that fails where it is evaluated, as the field's
/// `DEFAULT` does: a clause the engine cannot compile fails the write that
/// reaches it, never lets the record through unjudged.
///
/// Compiling a clause is the decision to run it, so this is called at the
/// place the clause is about to be evaluated and nowhere earlier: a decline,
/// which only the planner raises and only over a clause that has a logical
/// form, stands exactly where the clause's own failure would have stood, and
/// no statement fails over a clause it would not have reached.
async fn field_clause_physical(
	expr: &Expr,
	body: &LogicalBody,
	planner: &Planner<'_>,
) -> Result<Arc<dyn PhysicalExpr>, ControlFlow> {
	crate::exec::permission::field_body_physical(expr, body, planner)
		.await
		.map_err(|e| ControlFlow::Err(e.into()))
}

/// The field's `TYPE`, applied to one place.
///
/// The `id` field is left alone: its kind is enforced where the record id is
/// derived, and that result is the storage key, so coercing the value again
/// here could leave the record naming a key it is not stored under.
fn coerce_to_field_kind(
	fd: &FieldDefinition,
	rid: &RecordId,
	val: Value,
) -> Result<Value, ControlFlow> {
	let Some(kind) = fd.field_kind.as_ref() else {
		return Ok(val);
	};
	if fd.name.is_id() {
		return Ok(val);
	}
	val.coerce_to_kind(kind).map_err(|e| {
		fail(DocError::FieldCoerce {
			record: rid.to_sql(),
			field_name: fd.name.to_sql(),
			error: Box::new(e),
		})
	})
}

/// The field's compiled `PERMISSIONS FOR create` / `update`, applied to one
/// place.
///
/// A refusal reverts rather than refusing: the place keeps the value the
/// record already held, the statement succeeds, and the record is written
/// without that one change. Neither a `NONE` clause nor a denying predicate
/// fails the statement or skips the record — a caller may write the fields it
/// is allowed to and no others in the same statement.
///
/// A place whose value is unchanged keeps it whatever the answer, because
/// there is nothing there for the clause to refuse. A predicate is evaluated
/// before that is asked, so a clause with an effect of its own has that effect
/// either way.
///
/// The predicate is judged against `record` — the record as the stage has it,
/// bound as both the value under test and the document root, which is what
/// lets `$parent` inside an idiom-level filter resolve to it — with the
/// field's own values bound as the four parameters. Its reads are not
/// permission-checked and it cannot re-enter field permission checking, which
/// is what stops a cyclic record link from recursing without end. It may
/// write: this clause is reached from a statement that is already writing.
async fn permitted_value(
	frame: &ExecutionContext,
	permission: &PhysicalPermission,
	record: &Value,
	old: &Value,
	input: &Value,
	val: Value,
) -> Result<Value, ControlFlow> {
	// The parameters are bound only for a predicate: the two unconditional
	// answers evaluate nothing, and a frame built for them would cost a child
	// context at every place of every guarded field.
	let bound;
	let ctx = match permission {
		PhysicalPermission::Conditional(_) => {
			bound = clause_frame(frame, old, input, &val);
			&bound
		}
		PhysicalPermission::Allow | PhysicalPermission::Deny => frame,
	};
	// `$value` is bound above with the other three, at the place's own value,
	// so nothing is bound for it here.
	let allowed = check_write_permission_for_value(permission, record, None, ctx)
		.await
		.map_err(ControlFlow::Err)?;
	match allowed || val == *old {
		true => Ok(val),
		false => Ok(old.clone()),
	}
}

/// Whether a place holds nothing under a kind that admits nothing.
///
/// Two rules read it. An absent optional parent makes its children skippable,
/// because there is no object for their clauses to be about; and an `ASSERT`
/// is not judged against an absent optional value, because the field declared
/// that absence acceptable.
pub(crate) fn optional_and_absent(fd: &FieldDefinition, val: &Value) -> bool {
	val.is_none() && fd.field_kind.as_ref().is_some_and(Kind::can_be_none)
}

/// Restore the uniqueness of a set whose elements were rewritten.
///
/// A clause on `field[*]` is applied to each element, and coercion can
/// normalise two distinct stored elements to the same value, which leaves a
/// `TYPE set` parent holding duplicates.
pub(crate) fn normalise_set_elements(row: &mut WriteRow, fd: &FieldDefinition) {
	let parts = fd.name.0.as_slice();
	if parts.len() < 2 || !matches!(parts.last(), Some(Part::All)) {
		return;
	}
	let parent = Idiom(parts[..parts.len() - 1].to_vec());
	if let Value::Set(set) = row.current.pick(&parent) {
		row.current.put(&parent, Value::Set(set.normalize()));
	}
}

/// Derive one `COMPUTED` field onto the post-image, for the field clause that
/// reads it.
///
/// The body runs through the write's *unchecked* resolver, which is the
/// resolver for an image a stage derives rather than shows: no table guard and
/// no field guards, so the clause that reads the value sees the record whole.
/// The body itself still runs under its own definer's authority, exactly as on
/// the read side.
///
/// [`WriteRow::fields_computed`] is deliberately not set: only some of the
/// table's computed fields have been derived, and a later reader that trusted
/// the flag would take the rest for absent.
async fn derive_computed_field(
	row: &mut WriteRow,
	ctx: &WriteCtx,
	fd: &FieldDefinition,
) -> Result<(), ControlFlow> {
	// A body is derived against the record's schema, and a record with no id
	// is not addressable, so there is no subject to derive one for.
	if row.id.is_none() {
		return Ok(());
	}
	let exec = ctx.exec();
	let table = ctx.table.name.clone();
	let name = fd.name.to_raw_string();
	// Inherited rather than forced: it decides whether a record dereference
	// inside the body re-enters permission checking, and it is set precisely
	// when this record's write is already running inside a permission
	// predicate.
	let skip_fetch_perms = exec.root().skip_fetch_perms;
	let mut state = {
		let mut views = ctx.view_unchecked.lock().await;
		views.view_for(exec, &table).await?.field_state().clone()
	};
	// This field alone. The order places it ahead of the clause that reads it,
	// so deriving the rest here would run bodies the statement may never need
	// — including ones that raise.
	state.computed_fields.retain(|computed| computed.field_name() == name);
	compute_fields_for_value(exec, &state, &mut row.current, skip_fetch_perms).await
}

/// The record the schema is applied to. Every order that runs the kernel
/// derives or carries an id long before it, so a row without one is a broken
/// order rather than a rejected record.
fn record_id(row: &WriteRow) -> Result<RecordId, ControlFlow> {
	row.id.clone().ok_or_else(|| {
		ControlFlow::Err(anyhow::Error::new(EngineError::unreachable(
			"Expected a record id to be present",
		)))
	})
}

/// A clause of the table, evaluated to a value.
///
/// A clause is not a place a loop or a function body can be left from: `RETURN
/// x` written inside one is the clause's value, while `BREAK` and `CONTINUE`
/// have nothing here to leave and are
/// [`ExecError::InvalidControlFlow`](crate::exec::Error), which stops a signal
/// raised inside a field clause from steering a loop the statement is written
/// inside.
fn flatten(result: FlowResult<Value>) -> Result<Value, ControlFlow> {
	result.catch_return().map_err(ControlFlow::Err)
}

/// Abandon the statement with a named failure.
fn fail(error: impl std::error::Error + Send + Sync + 'static) -> ControlFlow {
	ControlFlow::Err(anyhow::Error::new(error))
}

/// Drop the fields the schema leaves empty, and on a `SCHEMAFULL` table refuse
/// a record carrying a field the table does not define.
///
/// Runs immediately after `process_table_fields`, on the post-image the field
/// kernel produced, and immediately before the write permission gate: what it
/// removes is therefore absent from the record that gate judges and from the
/// record storage receives.
///
/// Two rules, in this order:
///
/// - On a `SCHEMAFULL` table, every path the record carries must be reachable from a `DEFINE
///   FIELD`. The special fields (`id`, `in`, `out`) are exempt, as is an array index *below a path
///   a definition reaches*, and a path below a `TYPE any`, a literal type, or a `FLEXIBLE` object
///   is admitted without a definition of its own. Anything else is the record's failure, not a
///   field silently dropped.
/// - On every table, `NONE` is not stored: it is removed recursively from objects, but never from
///   an array, where removing an element would shift the positions of the elements after it.
#[derive(Debug)]
struct CleanupTableFields;

impl WriteStage for CleanupTableFields {
	fn name(&self) -> &'static str {
		StageId::CleanupTableFields.name()
	}

	fn apply<'a>(
		&'a self,
		row: &'a mut WriteRow,
		ctx: &'a WriteCtx,
	) -> BoxFut<'a, Result<StageOutcome, ControlFlow>> {
		Box::pin(async move {
			if ctx.table.schemafull {
				refuse_undefined_fields(&row.current, &ctx.fields, &ctx.table.name)?;
			}
			// A `NONE` is the absence of a value, and storing one would make
			// the key present on read.
			clean_none(&mut row.current);
			Ok(StageOutcome::Continue)
		})
	}
}

/// Raise on the first path of the post-image that the table's field
/// definitions do not reach.
///
/// The table is `SCHEMAFULL`, so an undefined field is refused rather than
/// dropped: a record that silently lost a field would be stored as something
/// other than what the caller wrote.
pub(crate) fn refuse_undefined_fields(
	current: &Value,
	fields: &[FieldDefinition],
	table: &crate::val::TableName,
) -> Result<(), ControlFlow> {
	// Which paths a definition reaches, and whether paths *below* each one are
	// admitted without a definition of their own.
	let mut defined: IdiomTrie<bool> = IdiomTrie::new();
	// A path that carries its own definition is never treated as the
	// schemaless interior of an ancestor, however permissive that ancestor is.
	let explicit: HashSet<&[Part]> = fields.iter().map(|fd| fd.name.0.as_slice()).collect();

	for fd in fields.iter() {
		let field_kind = fd.field_kind.as_ref();
		// `TYPE any` and a literal type admit anything beneath them — a
		// literal's interior is caught by coercion instead — and an object
		// type does so only when the field is `FLEXIBLE`.
		let allows_nested = field_kind.is_some_and(Kind::is_any)
			|| field_kind.is_some_and(Kind::contains_literal)
			|| (field_kind.is_some_and(kind_contains_object) && fd.flexible);

		for k in current.each(&fd.name) {
			defined.insert(&k, allows_nested);
			// The ancestors of an expanded path are reachable too. One with no
			// definition of its own is a schemaless object on the way to a
			// defined field, so its interior is admitted.
			for i in 1..k.len() {
				if !explicit.contains(&&k[..i]) {
					defined.insert(&k[..i], true);
				}
			}
		}
	}

	for path in current.every(None, true, ArrayBehaviour::Full).iter() {
		// `id`, `in` and `out` are the record's own, not the schema's.
		if path.is_special() {
			continue;
		}
		match defined.contains(path) {
			// Defined, or below something that admits an undefined interior.
			IdiomTrieContains::Exact(_) | IdiomTrieContains::Ancestor(true) => continue,
			// Below a definition that admits no undefined interior. An array
			// index is addressed by the definition of the array itself, so it
			// needs none of its own; anything else is undefined.
			IdiomTrieContains::Ancestor(false) if path.last().is_some_and(Part::is_index) => {
				continue;
			}
			// Below nothing the schema defines at all. The index exemption does
			// not extend here: an index reached from no definition is not
			// addressed by one, and the path that carries it is refused on its
			// own account only when the schema reaches that path.
			IdiomTrieContains::Ancestor(false) | IdiomTrieContains::None => {
				return Err(ControlFlow::Err(anyhow::anyhow!(DocError::FieldUndefined {
					table: table.as_str().to_string(),
					field: path.clone(),
				})));
			}
		}
	}
	Ok(())
}

/// Remove `NONE` recursively from objects, leaving array elements alone.
///
/// Answers whether `v` survives: an object or array always does, a `NONE`
/// never does, and anything else does. An array is walked so that a `NONE`
/// nested inside one of its objects is still removed, but a `NONE` that *is* an
/// element stays — dropping it would shift every element after it.
// The rule is about the two container shapes; every other value is carried
// through unchanged, so the wildcard is the rule rather than an omission.
#[allow(clippy::wildcard_enum_match_arm)]
pub(crate) fn clean_none(v: &mut Value) -> bool {
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

/// Derive the `COMPUTED` field values this record's observers read.
///
/// Sits after storage on every order that has it, which is what makes it
/// necessary: the field kernel strips the computed values before the record is
/// written, so the references, views, events, live queries, changefeed and
/// output projection that follow would otherwise see a record without them.
/// Because it runs after the write, the values it derives are never stored.
///
/// Both images are populated: an observer may read either, and an event body
/// reading `$before` and `$after` or a changefeed entry diffing them reads
/// both. An image holding no record is left alone — a create has no
/// before-image and a delete no after-image, and a body deriving from an absent
/// record raises rather than yielding an empty answer.
///
/// It derives *every* computed field, which is why it is gated on the table
/// actually carrying an observer. An event's `WHEN`, a live query's predicate
/// and a changefeed entry each read the record as a whole, so no analysis can
/// narrow the set. A table with no observer keeps the read side's rule that a
/// computed field a query did not ask for is never evaluated — including one
/// whose body would raise.
///
/// [`WriteRow::fields_computed`] and [`WriteRow::initial_fields_computed`]
/// track whether each image already carries the values, and are set here once
/// it does. An order that reaches an observer without running this stage —
/// view maintenance is one — leaves both standing at `false`, which is what a
/// reader recording the snapshot must report.
#[derive(Debug)]
struct MaterialiseObservedFields;

impl WriteStage for MaterialiseObservedFields {
	fn name(&self) -> &'static str {
		StageId::MaterialiseObservedFields.name()
	}

	fn apply<'a>(
		&'a self,
		row: &'a mut WriteRow,
		ctx: &'a WriteCtx,
	) -> BoxFut<'a, Result<StageOutcome, ControlFlow>> {
		Box::pin(async move {
			if !ctx.fields.iter().any(|fd| fd.computed.is_some()) {
				return Ok(StageOutcome::Continue);
			}
			if !has_record_observers(ctx) {
				return Ok(StageOutcome::Continue);
			}
			// A body is derived against the record's schema, and a record with
			// no id is not addressable, so there is no subject to derive one
			// for. The schema is the one the write context resolved, which is
			// also the one the gate above read its computed fields from: the
			// two must not be able to name different tables.
			if row.id.is_none() {
				return Ok(StageOutcome::Continue);
			}
			let table = ctx.table.name.clone();
			let exec = ctx.exec();
			// The observed image is the record as written, not as the caller
			// may read it: an event body sees fields the caller's `SELECT`
			// permissions would hide. The unchecked resolver is the one that
			// derives the bodies without applying any guard.
			let mut views = ctx.view_unchecked.lock().await;
			let state = views.view_for(exec, &table).await?.field_state();
			// Inherited rather than forced: this decides whether a record
			// dereference inside a body re-enters permission checking, and it
			// is set precisely when this record's write is already running
			// inside a permission predicate.
			let skip_fetch_perms = exec.root().skip_fetch_perms;

			if !row.current.is_none() && !row.fields_computed {
				compute_fields_for_value(exec, state, &mut row.current, skip_fetch_perms).await?;
				row.fields_computed = true;
			}
			if !row.initial.is_none() {
				compute_fields_for_value(exec, state, &mut row.initial, skip_fetch_perms).await?;
				row.initial_fields_computed = true;
			}
			Ok(StageOutcome::Continue)
		})
	}
}

/// Whether anything other than the statement's own projection reads this record
/// after it is written: a table event, a live-query subscription, or a
/// changefeed on the table or on its database.
pub(crate) fn has_record_observers(ctx: &WriteCtx) -> bool {
	!ctx.events.is_empty()
		|| !ctx.lives.is_empty()
		|| ctx.table.changefeed.is_some()
		|| ctx.db.changefeed.is_some()
}

#[cfg(all(test, feature = "kv-mem"))]
#[allow(clippy::unwrap_used)]
mod tests {
	use super::*;
	use crate::dbs::Session;
	use crate::exec::ExecutionContext;
	use crate::exec::context::{DatabaseContext, NamespaceContext};
	use crate::exec::operators::test_util::TestDb;
	use crate::exec::operators::write::clauses::WriteClauses;
	use crate::exec::operators::write::ctx::TableExistence;
	use crate::exec::operators::write::row::ComputedInput;
	use crate::exec::physical_expr::Literal;
	use crate::kvs::TransactionType;
	use crate::val::{Object, RecordIdKey, TableName};

	/// The write context for `t` on a datastore set up by `setup`, over a write
	/// transaction held by a root-Owner session.
	async fn ctx_for(db: &TestDb, table: &str) -> Arc<WriteCtx> {
		ctx_as(db, &TestDb::owner(), table, WriteClauses::default()).await
	}

	/// As [`ctx_for`], for `session` and for a statement carrying `clauses`.
	async fn ctx_as(
		db: &TestDb,
		session: &Session,
		table: &str,
		clauses: WriteClauses,
	) -> Arc<WriteCtx> {
		let exec = db.exec_ctx_as(session, TransactionType::Write).await;
		Arc::new(
			WriteCtx::resolve(
				&exec,
				&TableName::from(table),
				TableExistence::DefinedOnDemand,
				None,
				Arc::new(clauses),
			)
			.await
			.expect("the write context resolves"),
		)
	}

	/// An anonymous session on `test`/`test`, permission-checked under a
	/// datastore built with server auth enabled.
	fn anonymous() -> Session {
		Session::default().with_ns("test").with_db("test")
	}

	/// A statement carrying a `CONTENT` clause. The compiled expression is
	/// never evaluated: the kernel reads which clause form the statement used,
	/// and the value it produced reaches the row as [`ComputedInput`].
	fn content_clause() -> WriteClauses {
		WriteClauses {
			data: Some(WriteData::Content(Arc::new(Literal(Value::None)))),
			..WriteClauses::default()
		}
	}

	/// The record id every row here is written at.
	fn subject() -> RecordId {
		RecordId {
			table: "t".into(),
			key: RecordIdKey::Number(1),
		}
	}

	/// A row whose post-image is `value`, at `t:1`, with no before-image.
	fn creating_row(value: Value) -> WriteRow {
		let mut row = WriteRow::for_target(Value::RecordId(subject()))
			.expect("a record id is a valid target");
		row.current = value;
		row
	}

	/// A row for a record that already exists: `initial` as the statement read
	/// it, `current` as the stages before the kernel have left it.
	fn existing_row(initial: Value, current: Value) -> WriteRow {
		let mut row = WriteRow::for_record(initial).expect("a record value carries its id");
		row.current = current;
		row
	}

	/// Run the field kernel over one row.
	async fn kernel(row: &mut WriteRow, ctx: &WriteCtx) -> Result<StageOutcome, ControlFlow> {
		stage(StageId::ProcessTableFields).expect("the stage is registered").apply(row, ctx).await
	}

	/// The message of a failure the kernel raised.
	fn failure(result: Result<StageOutcome, ControlFlow>) -> String {
		match result {
			Err(ControlFlow::Err(err)) => err.to_string(),
			Err(_) => panic!("a field clause raised a loop signal rather than a failure"),
			Ok(outcome) => panic!("the record was accepted as {outcome:?}"),
		}
	}

	/// An object value from `(key, value)` pairs.
	fn object(fields: Vec<(&str, Value)>) -> Value {
		let mut obj = Object::default();
		for (k, v) in fields {
			obj.insert(k.to_string(), v);
		}
		Value::Object(obj)
	}

	/// A top-level field of an object, `NONE` when the key is absent.
	fn field(value: &Value, name: &str) -> Value {
		let Value::Object(obj) = value else {
			return Value::None;
		};
		obj.get(name).cloned().unwrap_or(Value::None)
	}

	/// Whether an object carries `name` at all. A cleaned field is removed, so
	/// its key is absent rather than present and `NONE`.
	fn carries(value: &Value, name: &str) -> bool {
		matches!(value, Value::Object(obj) if obj.get(name).is_some())
	}

	/// A `NONE` is removed from an object however deeply it is nested, on a
	/// schemaless table as much as a schemafull one.
	#[tokio::test]
	async fn a_none_is_never_stored() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let ctx = ctx_for(&db, "t").await;
		let mut row = creating_row(object(vec![
			("kept", Value::from(1)),
			("dropped", Value::None),
			("nested", object(vec![("gone", Value::None), ("here", Value::from(2))])),
		]));

		let outcome = stage(StageId::CleanupTableFields)
			.expect("the stage is registered")
			.apply(&mut row, &ctx)
			.await
			.expect("cleanup succeeds");

		assert_eq!(outcome, StageOutcome::Continue);
		assert!(carries(&row.current, "kept"));
		assert!(!carries(&row.current, "dropped"), "a NONE is removed, not stored as NONE");
		assert!(!carries(&field(&row.current, "nested"), "gone"), "removal is recursive");
		assert!(carries(&field(&row.current, "nested"), "here"));
	}

	/// A `NONE` element of an array stays: removing it would shift the
	/// positions of every element after it.
	#[tokio::test]
	async fn a_none_array_element_keeps_its_position() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let ctx = ctx_for(&db, "t").await;
		let mut row = creating_row(object(vec![(
			"list",
			Value::from(vec![Value::from(1), Value::None, Value::from(3)]),
		)]));

		stage(StageId::CleanupTableFields)
			.expect("the stage is registered")
			.apply(&mut row, &ctx)
			.await
			.expect("cleanup succeeds");

		let Value::Array(list) = field(&row.current, "list") else {
			panic!("the array survives");
		};
		assert_eq!(list.len(), 3, "a NONE element is kept so later elements keep their index");
		assert_eq!(list[2], Value::from(3));
	}

	/// A `SCHEMAFULL` table refuses a record carrying an undefined field, and
	/// the refusal fails the statement rather than skipping the record: writing
	/// the record without the field would store something other than what the
	/// caller wrote.
	#[tokio::test]
	async fn a_schemafull_table_refuses_an_undefined_field() {
		let db = TestDb::new(
			"
			DEFINE TABLE t SCHEMAFULL;
			DEFINE FIELD known ON t TYPE int;
			",
		)
		.await;
		let ctx = ctx_for(&db, "t").await;
		let mut row =
			creating_row(object(vec![("known", Value::from(1)), ("stray", Value::from(2))]));

		let Err(ControlFlow::Err(err)) = stage(StageId::CleanupTableFields)
			.expect("the stage is registered")
			.apply(&mut row, &ctx)
			.await
		else {
			panic!("an undefined field on a schemafull table fails the statement");
		};
		assert!(
			err.to_string().contains("stray"),
			"the failure names the offending field, got: {err}"
		);
	}

	/// An array index is addressed by the definition of the array that holds
	/// it, so a record carrying elements of a defined array needs no definition
	/// per element.
	#[tokio::test]
	async fn an_index_below_a_defined_array_needs_no_definition() {
		let db = TestDb::new(
			"
			DEFINE TABLE t SCHEMAFULL;
			DEFINE FIELD list ON t TYPE array<int>;
			",
		)
		.await;
		let ctx = ctx_for(&db, "t").await;
		let mut row =
			creating_row(object(vec![("list", Value::from(vec![Value::from(1), Value::from(2)]))]));

		stage(StageId::CleanupTableFields)
			.expect("the stage is registered")
			.apply(&mut row, &ctx)
			.await
			.expect("an element of a defined array is admitted");
	}

	/// That exemption reaches only an index the schema defines an array for. An
	/// index below a path no definition reaches is refused like any other
	/// undefined path — reachable because the special fields are skipped before
	/// the trie is consulted, so an `in` holding an array leaves its elements to
	/// answer for themselves.
	#[tokio::test]
	async fn an_index_below_nothing_defined_is_refused() {
		let db = TestDb::new("DEFINE TABLE t SCHEMAFULL;").await;
		let ctx = ctx_for(&db, "t").await;
		let mut row = creating_row(object(vec![("in", Value::from(vec![Value::from(1)]))]));

		let Err(ControlFlow::Err(err)) = stage(StageId::CleanupTableFields)
			.expect("the stage is registered")
			.apply(&mut row, &ctx)
			.await
		else {
			panic!("an index the schema reaches through no definition is undefined");
		};
		assert!(err.to_string().contains("in[0]"), "the failure names the path, got: {err}");
	}

	/// A `FLEXIBLE` object admits keys of its own, and the record's special
	/// fields need no definition at all.
	#[tokio::test]
	async fn a_flexible_object_and_the_special_fields_are_admitted() {
		let db = TestDb::new(
			"
			DEFINE TABLE t SCHEMAFULL;
			DEFINE FIELD bag ON t TYPE object FLEXIBLE;
			",
		)
		.await;
		let ctx = ctx_for(&db, "t").await;
		let mut row = creating_row(object(vec![
			(
				"id",
				Value::RecordId(RecordId {
					table: "t".into(),
					key: RecordIdKey::Number(1),
				}),
			),
			("bag", object(vec![("anything", Value::from(1))])),
		]));

		stage(StageId::CleanupTableFields)
			.expect("the stage is registered")
			.apply(&mut row, &ctx)
			.await
			.expect("a flexible object admits its own keys");

		assert!(carries(&field(&row.current, "bag"), "anything"));
		assert!(carries(&row.current, "id"));
	}

	/// A table with a `COMPUTED` field but no observer derives nothing: the
	/// read side evaluates only what a projection asks for, and a body that
	/// would raise must not raise for a write nothing observes.
	#[tokio::test]
	async fn no_observer_derives_nothing() {
		let db = TestDb::new(
			"
			DEFINE TABLE t SCHEMALESS;
			DEFINE FIELD n ON t TYPE int;
			DEFINE FIELD double ON t COMPUTED n * 2;
			",
		)
		.await;
		let ctx = ctx_for(&db, "t").await;
		let mut row = creating_row(object(vec![
			(
				"id",
				Value::RecordId(RecordId {
					table: "t".into(),
					key: RecordIdKey::Number(1),
				}),
			),
			("n", Value::from(2)),
		]));

		stage(StageId::MaterialiseObservedFields)
			.expect("the stage is registered")
			.apply(&mut row, &ctx)
			.await
			.expect("the stage succeeds");

		assert!(!carries(&row.current, "double"), "nothing observes this write");
		assert!(!row.fields_computed);
	}

	/// An event on the table makes every computed field an observed one, on the
	/// post-image the write produced.
	#[tokio::test]
	async fn an_event_makes_the_computed_fields_observed() {
		let db = TestDb::new(
			"
			DEFINE TABLE t SCHEMALESS;
			DEFINE FIELD n ON t TYPE int;
			DEFINE FIELD double ON t COMPUTED n * 2;
			DEFINE EVENT seen ON t WHEN true THEN { RETURN true; };
			",
		)
		.await;
		let ctx = ctx_for(&db, "t").await;
		let mut row = creating_row(object(vec![
			(
				"id",
				Value::RecordId(RecordId {
					table: "t".into(),
					key: RecordIdKey::Number(1),
				}),
			),
			("n", Value::from(2)),
		]));

		stage(StageId::MaterialiseObservedFields)
			.expect("the stage is registered")
			.apply(&mut row, &ctx)
			.await
			.expect("the stage succeeds");

		assert_eq!(field(&row.current, "double"), Value::from(4));
		assert!(row.fields_computed, "the post-image now carries its computed values");
	}

	/// A create has no before-image, and a body deriving from an absent record
	/// raises rather than yielding an empty answer — so the absent image is
	/// left untouched.
	#[tokio::test]
	async fn an_absent_image_is_left_alone() {
		let db = TestDb::new(
			"
			DEFINE TABLE t SCHEMALESS;
			DEFINE FIELD n ON t TYPE int;
			DEFINE FIELD double ON t COMPUTED n * 2;
			DEFINE EVENT seen ON t WHEN true THEN { RETURN true; };
			",
		)
		.await;
		let ctx = ctx_for(&db, "t").await;
		let mut row = creating_row(object(vec![
			(
				"id",
				Value::RecordId(RecordId {
					table: "t".into(),
					key: RecordIdKey::Number(1),
				}),
			),
			("n", Value::from(2)),
		]));
		assert_eq!(row.initial, Value::None, "a create has no before-image");

		stage(StageId::MaterialiseObservedFields)
			.expect("the stage is registered")
			.apply(&mut row, &ctx)
			.await
			.expect("an absent before-image is not derived from");

		assert_eq!(row.initial, Value::None);
	}

	/// A `DEFAULT` supplies a value for a place the record left empty, on a
	/// record being created.
	#[tokio::test]
	async fn a_default_supplies_an_absent_value_on_create() {
		let db = TestDb::new(
			"
			DEFINE TABLE t SCHEMALESS;
			DEFINE FIELD n ON t TYPE int DEFAULT 7;
			",
		)
		.await;
		let ctx = ctx_for(&db, "t").await;
		let mut row = creating_row(object(vec![("id", Value::RecordId(subject()))]));

		assert_eq!(kernel(&mut row, &ctx).await.unwrap(), StageOutcome::Continue);
		assert_eq!(field(&row.current, "n"), Value::from(7));
	}

	/// A plain `DEFAULT` applies to creation alone; `DEFAULT ALWAYS` applies to
	/// every write, and reads the record's prior value as `$before`.
	#[tokio::test]
	async fn a_plain_default_spares_an_existing_record_and_always_does_not() {
		let db = TestDb::new(
			"
			DEFINE TABLE t SCHEMALESS;
			DEFINE FIELD once ON t TYPE option<int> DEFAULT 7;
			DEFINE FIELD every ON t TYPE option<int> DEFAULT ALWAYS $before;
			",
		)
		.await;
		let ctx = ctx_for(&db, "t").await;
		let mut row = existing_row(
			object(vec![
				("id", Value::RecordId(subject())),
				("once", Value::from(1)),
				("every", Value::from(2)),
			]),
			object(vec![("id", Value::RecordId(subject()))]),
		);

		kernel(&mut row, &ctx).await.unwrap();

		assert_eq!(
			field(&row.current, "once"),
			Value::None,
			"a plain DEFAULT does not refill a field the statement emptied"
		);
		assert_eq!(field(&row.current, "every"), Value::from(2), "DEFAULT ALWAYS reads $before");
	}

	/// A clause reads what the *statement* supplied at its own place through
	/// `$input`, which is not what the record carries there: the record is what
	/// the statement's stages have made of it.
	#[tokio::test]
	async fn a_default_reads_the_supplied_value_at_its_own_place() {
		let db = TestDb::new(
			"
			DEFINE TABLE t SCHEMALESS;
			DEFINE FIELD a ON t TYPE option<object>;
			DEFINE FIELD a.b ON t TYPE option<int> DEFAULT $input;
			",
		)
		.await;
		let ctx = ctx_as(&db, &TestDb::owner(), "t", content_clause()).await;
		let mut row =
			creating_row(object(vec![("id", Value::RecordId(subject())), ("a", object(vec![]))]));
		row.input = Some(ComputedInput::Content(Arc::new(object(vec![(
			"a",
			object(vec![("b", Value::from(7))]),
		)]))));

		kernel(&mut row, &ctx).await.unwrap();

		assert_eq!(field(&field(&row.current, "a"), "b"), Value::from(7));
	}

	/// A `TYPE` coerces what a place holds, and a value that will not coerce
	/// fails the statement naming the field.
	#[tokio::test]
	async fn a_type_coerces_what_it_can_and_refuses_the_rest() {
		let db = TestDb::new(
			"
			DEFINE TABLE t SCHEMALESS;
			DEFINE FIELD n ON t TYPE float;
			",
		)
		.await;
		let ctx = ctx_for(&db, "t").await;

		let mut coerced =
			creating_row(object(vec![("id", Value::RecordId(subject())), ("n", Value::from(1))]));
		kernel(&mut coerced, &ctx).await.unwrap();
		assert_eq!(field(&coerced.current, "n"), Value::from(1.0f64));

		let mut refused = creating_row(object(vec![
			("id", Value::RecordId(subject())),
			("n", Value::from("nope")),
		]));
		let err = failure(kernel(&mut refused, &ctx).await);
		assert!(err.contains("n"), "the failure names the field, got: {err}");
	}

	/// A `READONLY` field may not change once the record exists.
	#[tokio::test]
	async fn a_readonly_field_refuses_a_change() {
		let db = TestDb::new(
			"
			DEFINE TABLE t SCHEMALESS;
			DEFINE FIELD n ON t TYPE option<int> READONLY;
			",
		)
		.await;
		let ctx = ctx_for(&db, "t").await;
		let mut row = existing_row(
			object(vec![("id", Value::RecordId(subject())), ("n", Value::from(1))]),
			object(vec![("id", Value::RecordId(subject())), ("n", Value::from(2))]),
		);

		let err = failure(kernel(&mut row, &ctx).await);
		assert!(err.contains("readonly"), "the failure names the rule, got: {err}");
	}

	/// A `CONTENT` clause restates the whole record, so a `READONLY` field it
	/// leaves out is restored rather than refused. Every other clause form
	/// refuses, because omission there is not a restatement.
	#[tokio::test]
	async fn a_content_omission_restores_a_readonly_field() {
		let db = TestDb::new(
			"
			DEFINE TABLE t SCHEMALESS;
			DEFINE FIELD n ON t TYPE option<int> READONLY;
			",
		)
		.await;
		let before = object(vec![("id", Value::RecordId(subject())), ("n", Value::from(1))]);
		let after = object(vec![("id", Value::RecordId(subject()))]);

		let content = ctx_as(&db, &TestDb::owner(), "t", content_clause()).await;
		let mut restored = existing_row(before.clone(), after.clone());
		kernel(&mut restored, &content).await.unwrap();
		assert_eq!(field(&restored.current, "n"), Value::from(1));

		let bare = ctx_for(&db, "t").await;
		let mut refused = existing_row(before, after);
		let err = failure(kernel(&mut refused, &bare).await);
		assert!(err.contains("readonly"), "the failure names the rule, got: {err}");
	}

	/// The `id` field may not change once the record exists: it is the key the
	/// record is stored under.
	#[tokio::test]
	async fn the_id_field_is_readonly_once_the_record_exists() {
		let db = TestDb::new(
			"
			DEFINE TABLE t SCHEMALESS;
			DEFINE FIELD id ON t TYPE int;
			",
		)
		.await;
		let ctx = ctx_for(&db, "t").await;
		let other = RecordId {
			table: "t".into(),
			key: RecordIdKey::Number(2),
		};
		let mut row = existing_row(
			object(vec![("id", Value::RecordId(subject()))]),
			object(vec![("id", Value::RecordId(other))]),
		);

		let err = failure(kernel(&mut row, &ctx).await);
		assert!(err.contains("readonly"), "the failure names the rule, got: {err}");
	}

	/// A `COMPUTED` field another field's clause reads is derived before that
	/// clause runs, and stripped again before the stage returns — a computed
	/// value is never stored. The flag that says the record carries its
	/// computed values is cleared with them.
	#[tokio::test]
	async fn a_computed_field_a_clause_reads_is_derived_then_stripped() {
		let db = TestDb::new(
			"
			DEFINE TABLE t SCHEMALESS;
			DEFINE FIELD n ON t TYPE int;
			DEFINE FIELD double ON t COMPUTED n * 2;
			DEFINE FIELD copy ON t TYPE option<int> DEFAULT double;
			",
		)
		.await;
		let ctx = ctx_for(&db, "t").await;
		let mut row =
			creating_row(object(vec![("id", Value::RecordId(subject())), ("n", Value::from(2))]));
		row.fields_computed = true;

		kernel(&mut row, &ctx).await.unwrap();

		assert_eq!(
			field(&row.current, "copy"),
			Value::from(4),
			"the clause read the derived value"
		);
		assert!(!carries(&row.current, "double"), "a computed value is never stored");
		assert!(!row.fields_computed, "the stripped record no longer carries its computed values");
	}

	/// A place left empty under a kind that admits nothing makes the field's
	/// children skippable: their clauses do not run, because there is no parent
	/// object for them to be about.
	#[tokio::test]
	async fn an_absent_optional_parent_skips_its_children() {
		let db = TestDb::new(
			"
			DEFINE TABLE t SCHEMALESS;
			DEFINE FIELD parent ON t TYPE option<object>;
			DEFINE FIELD parent.child ON t TYPE option<int> DEFAULT 5;
			",
		)
		.await;
		let ctx = ctx_for(&db, "t").await;
		let mut row = creating_row(object(vec![("id", Value::RecordId(subject()))]));

		kernel(&mut row, &ctx).await.unwrap();

		assert_eq!(
			field(&row.current, "parent"),
			Value::None,
			"the child's DEFAULT did not build a parent object"
		);
	}

	/// A field `PERMISSIONS NONE` reverts the change rather than failing the
	/// statement or skipping the record: the caller writes the fields it may
	/// and no others, in the one statement.
	#[tokio::test]
	async fn a_none_field_permission_reverts_the_change() {
		let db = TestDb::new_with_auth(
			"
			DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
			DEFINE FIELD open ON t TYPE option<int>;
			DEFINE FIELD shut ON t TYPE option<int> PERMISSIONS FOR update NONE;
			",
		)
		.await;
		let before = object(vec![
			("id", Value::RecordId(subject())),
			("open", Value::from(1)),
			("shut", Value::from(1)),
		]);
		let after = object(vec![
			("id", Value::RecordId(subject())),
			("open", Value::from(2)),
			("shut", Value::from(2)),
		]);

		let checked = ctx_as(&db, &anonymous(), "t", WriteClauses::default()).await;
		let mut guarded = existing_row(before.clone(), after.clone());
		kernel(&mut guarded, &checked).await.unwrap();
		assert_eq!(field(&guarded.current, "open"), Value::from(2));
		assert_eq!(field(&guarded.current, "shut"), Value::from(1), "the change is reverted");

		// An exempt principal is not gated at all, so the same clause admits
		// the same write.
		let exempt = ctx_for(&db, "t").await;
		let mut ungated = existing_row(before, after);
		kernel(&mut ungated, &exempt).await.unwrap();
		assert_eq!(field(&ungated.current, "shut"), Value::from(2));
	}

	/// A `VALUE` on a place the record left empty runs before the type check:
	/// there is no value for a type check to be about until the clause has
	/// supplied one, so a field whose kind admits no `NONE` is still produced.
	#[tokio::test]
	async fn a_value_clause_fills_an_empty_place_before_the_type_check() {
		let db = TestDb::new(
			"
			DEFINE TABLE t SCHEMALESS;
			DEFINE FIELD n ON t TYPE int VALUE ($value ?? 0) + 1;
			",
		)
		.await;
		let ctx = ctx_for(&db, "t").await;
		let mut row = creating_row(object(vec![("id", Value::RecordId(subject()))]));

		kernel(&mut row, &ctx).await.unwrap();

		assert_eq!(field(&row.current, "n"), Value::from(1), "the clause read the empty place");
	}

	/// A `VALUE` on a place that holds a value runs *after* the type check, so
	/// the clause reads the value under the kind the field declares rather than
	/// as the caller wrote it.
	#[tokio::test]
	async fn a_value_clause_reads_a_filled_place_after_the_type_check() {
		let db = TestDb::new(
			"
			DEFINE TABLE t SCHEMALESS;
			DEFINE FIELD n ON t TYPE float VALUE IF type::is_float($value) { 1f } ELSE { 2f };
			",
		)
		.await;
		let ctx = ctx_for(&db, "t").await;
		let mut row =
			creating_row(object(vec![("id", Value::RecordId(subject())), ("n", Value::from(1))]));

		kernel(&mut row, &ctx).await.unwrap();

		assert_eq!(
			field(&row.current, "n"),
			Value::from(1.0f64),
			"the clause read the coerced value, not the int the caller wrote"
		);
	}

	/// The type check runs again over what the `VALUE` produced, so the clause
	/// is held to the field's kind as well as the caller is.
	#[tokio::test]
	async fn a_value_clause_output_is_type_checked_too() {
		let db = TestDb::new(
			"
			DEFINE TABLE t SCHEMALESS;
			DEFINE FIELD n ON t TYPE int VALUE 'nope';
			",
		)
		.await;
		let ctx = ctx_for(&db, "t").await;
		let mut row =
			creating_row(object(vec![("id", Value::RecordId(subject())), ("n", Value::from(1))]));

		let err = failure(kernel(&mut row, &ctx).await);
		assert!(err.contains("n"), "the failure names the field, got: {err}");
	}

	/// An `ASSERT` is judged after every field has been produced, so it reads a
	/// sibling's final value however production was ordered. Here the asserting
	/// field is produced first, and the field it reads has no value at all until
	/// its own `VALUE` clause has run.
	#[tokio::test]
	async fn a_deferred_assert_reads_a_siblings_final_value() {
		let db = TestDb::new(
			"
			DEFINE TABLE t SCHEMALESS;
			DEFINE FIELD a ON t TYPE int VALUE 1 ASSERT b = 2;
			DEFINE FIELD b ON t TYPE int VALUE 2;
			",
		)
		.await;
		let ctx = ctx_for(&db, "t").await;
		let mut row = creating_row(object(vec![("id", Value::RecordId(subject()))]));

		kernel(&mut row, &ctx).await.unwrap();

		assert_eq!(field(&row.current, "a"), Value::from(1));
		assert_eq!(field(&row.current, "b"), Value::from(2));
	}

	/// A refused `ASSERT` fails the statement, naming the value, the field, the
	/// record and the clause it did not conform to.
	#[tokio::test]
	async fn a_refused_assert_names_the_value_and_the_clause() {
		let db = TestDb::new(
			"
			DEFINE TABLE t SCHEMALESS;
			DEFINE FIELD n ON t TYPE int ASSERT $value > 10;
			",
		)
		.await;
		let ctx = ctx_for(&db, "t").await;
		let mut row =
			creating_row(object(vec![("id", Value::RecordId(subject())), ("n", Value::from(1))]));

		let err = failure(kernel(&mut row, &ctx).await);
		assert!(
			err.contains("Found 1 for field `n`, with record `t:1`, but field must conform to:"),
			"the failure names the value, the field and the record, got: {err}"
		);
		assert!(err.contains("$value > 10"), "the failure names the clause, got: {err}");
	}

	/// An absent optional value is not judged: the field declared that absence
	/// acceptable, so its `ASSERT` is about a value there is none of.
	#[tokio::test]
	async fn an_absent_optional_value_is_not_asserted() {
		let db = TestDb::new(
			"
			DEFINE TABLE t SCHEMALESS;
			DEFINE FIELD n ON t TYPE option<int> ASSERT $value > 10;
			",
		)
		.await;
		let ctx = ctx_for(&db, "t").await;
		let mut row = creating_row(object(vec![("id", Value::RecordId(subject()))]));

		kernel(&mut row, &ctx).await.unwrap();

		assert_eq!(field(&row.current, "n"), Value::None);
	}

	/// `$value` is the value as it will be stored, read back from the record:
	/// the field's `PERMISSIONS` clause may have reverted what the field
	/// produced, and the `ASSERT` judges what the record ends up holding.
	#[tokio::test]
	async fn an_assert_reads_the_value_a_permission_clause_reverted() {
		let db = TestDb::new_with_auth(
			"
			DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
			DEFINE FIELD shut ON t TYPE int ASSERT $value = 1 PERMISSIONS FOR update NONE;
			",
		)
		.await;
		let ctx = ctx_as(&db, &anonymous(), "t", WriteClauses::default()).await;
		let mut row = existing_row(
			object(vec![("id", Value::RecordId(subject())), ("shut", Value::from(1))]),
			object(vec![("id", Value::RecordId(subject())), ("shut", Value::from(2))]),
		);

		kernel(&mut row, &ctx).await.unwrap();

		assert_eq!(
			field(&row.current, "shut"),
			Value::from(1),
			"the reverted value is what the ASSERT was judged against"
		);
	}

	/// A `VALUE` or `ASSERT` holding a mock record id in a branch that never
	/// runs still governs the write: the `VALUE` decides the stored value and
	/// the `ASSERT` refuses the record.
	#[tokio::test]
	async fn a_field_clause_holding_a_dead_mock_branch_still_governs_the_write() {
		let db = TestDb::new(
			"
			DEFINE TABLE sink SCHEMALESS;
			DEFINE TABLE v SCHEMALESS;
			DEFINE FIELD n ON v TYPE option<int> VALUE IF false { |sink:1| } ELSE { $value * 2 };
			DEFINE TABLE a SCHEMALESS;
			DEFINE FIELD n ON a TYPE int ASSERT IF false { |sink:1| } ELSE { $value > 10 };
			",
		)
		.await;
		let record = |table: &str| {
			object(vec![
				(
					"id",
					Value::RecordId(RecordId {
						table: table.into(),
						key: RecordIdKey::Number(1),
					}),
				),
				("n", Value::from(1)),
			])
		};

		let value_ctx = ctx_for(&db, "v").await;
		let mut value_row = creating_row(record("v"));
		kernel(&mut value_row, &value_ctx).await.expect("the VALUE runs");
		assert_eq!(
			field(&value_row.current, "n"),
			Value::from(2),
			"the VALUE decides what the record holds"
		);

		let assert_ctx = ctx_for(&db, "a").await;
		let mut assert_row = creating_row(record("a"));
		let err = failure(kernel(&mut assert_row, &assert_ctx).await);
		assert!(err.contains("n"), "the ASSERT refuses the record, got: {err}");
	}

	/// A field `PERMISSIONS FOR update WHERE …` has no compiled form on this
	/// stage, so a write that reaches one fails with the engine's error rather
	/// than passing unjudged; a field with no such clause is written as usual.
	#[tokio::test]
	async fn a_field_write_predicate_this_stage_cannot_compile_fails_the_write() {
		let db = TestDb::new_with_auth(
			"
			DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
			DEFINE FIELD open ON t TYPE option<int>;
			DEFINE FIELD shut ON t TYPE option<int> PERMISSIONS FOR update WHERE $before > 10;
			",
		)
		.await;
		let value = |n: i64| {
			object(vec![
				("id", Value::RecordId(subject())),
				("open", Value::from(n)),
				("shut", Value::from(n)),
			])
		};

		let ctx = ctx_as(&db, &anonymous(), "t", WriteClauses::default()).await;
		let mut row = existing_row(value(1), value(2));
		let err = failure(kernel(&mut row, &ctx).await);
		assert!(err.contains("cannot run this statement"), "got: {err}");
	}

	/// The four parameters a `DEFAULT ALWAYS` clause is given, where the
	/// clause is reached because the place holds nothing.
	///
	/// `$before` and `$input` are the place's own — what the record held and
	/// what the statement asked for — and `$value` and `$after` are the `NONE`
	/// the clause was reached for, not the value it is about to produce.
	#[tokio::test]
	async fn a_default_clause_reads_the_place_it_was_reached_for() {
		let db = TestDb::new(
			"
			DEFINE TABLE t SCHEMALESS;
			DEFINE FIELD compiled ON t TYPE option<array>
				DEFAULT ALWAYS [$before, $input, $value, $after];
			",
		)
		.await;
		let ctx = ctx_as(&db, &TestDb::owner(), "t", content_clause()).await;

		let mut row = existing_row(
			object(vec![("id", Value::RecordId(subject())), ("compiled", Value::from(5))]),
			object(vec![("id", Value::RecordId(subject()))]),
		);
		row.input =
			Some(ComputedInput::Content(Arc::new(object(vec![("compiled", Value::from(7))]))));

		kernel(&mut row, &ctx).await.unwrap();

		assert_eq!(
			field(&row.current, "compiled"),
			Value::from(vec![Value::from(5), Value::from(7), Value::None, Value::None]),
			"$before and $input are the place's own, $value and $after the NONE the clause was \
			 reached for"
		);
	}

	/// Whether a field's `PERMISSIONS` are enforced is decided under the
	/// authority the field's `AUTH LIMIT` narrows the caller to, not the
	/// caller's own: a caller the datastore exempts is enforced against a
	/// field whose definer held less than it does.
	///
	/// The narrowing stamp written here is the one a field the catalog creates
	/// on a definition's behalf carries — the `in` and `out` fields of a
	/// `TYPE RELATION` table, and the element field of an array type — because
	/// `DEFINE FIELD` and `ALTER FIELD` always stamp the principal running
	/// them.
	#[tokio::test]
	async fn a_fields_auth_limit_decides_whether_its_permission_is_enforced() {
		use crate::catalog::auth::AuthLimit;
		use crate::catalog::providers::TableProvider;

		let db = TestDb::new_with_auth(
			"
			DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
			DEFINE FIELD shut ON t TYPE option<int> PERMISSIONS FOR update NONE;
			",
		)
		.await;
		let before = object(vec![("id", Value::RecordId(subject())), ("shut", Value::from(1))]);
		let after = object(vec![("id", Value::RecordId(subject())), ("shut", Value::from(2))]);

		// Stamped by root Owner, which narrows nobody: the caller is root
		// Owner too, is exempt, and the clause is never consulted.
		let unnarrowed = ctx_as(&db, &TestDb::owner(), "t", WriteClauses::default()).await;
		let mut exempt = existing_row(before.clone(), after.clone());
		kernel(&mut exempt, &unnarrowed).await.unwrap();
		assert_eq!(
			field(&exempt.current, "shut"),
			Value::from(2),
			"an exempt caller reaches no field clause"
		);

		// The same caller, the same clause, and a stamp that narrows it out of
		// its exemption.
		let exec = db.exec_ctx_as(&TestDb::owner(), TransactionType::Write).await;
		let db_ctx = exec.database().expect("a database context");
		let (ns_id, db_id) = (db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id);
		let table = TableName::from("t");
		let txn = exec.txn();
		let stored = txn
			.get_tb_field(ns_id, db_id, &table, "shut", None)
			.await
			.expect("the field reads back")
			.expect("the field is defined");
		let narrowed_field = FieldDefinition {
			auth_limit: AuthLimit::default(),
			..(*stored).clone()
		};
		txn.put_tb_field(ns_id, db_id, &table, &narrowed_field).await.expect("the field is stored");
		// The table's field-cache stamp is what a reader's cached field list
		// is keyed on, so a definition written behind `DEFINE FIELD` has to
		// refresh it as `DEFINE FIELD` does.
		let tb_def = db_ctx
			.get_table_def(&table, None)
			.await
			.expect("the table reads back")
			.expect("the table is defined");
		let refreshed = crate::catalog::TableDefinition {
			cache_fields_ts: uuid::Uuid::now_v7(),
			..(*tb_def).clone()
		};
		txn.put_tb("test", "test", &refreshed).await.expect("the table is stored");

		let narrowed = Arc::new(
			WriteCtx::resolve(
				&exec,
				&table,
				TableExistence::DefinedOnDemand,
				None,
				Arc::new(WriteClauses::default()),
			)
			.await
			.expect("the write context resolves"),
		);
		let mut checked = existing_row(before, after);
		kernel(&mut checked, &narrowed).await.unwrap();
		assert_eq!(
			field(&checked.current, "shut"),
			Value::from(1),
			"the narrowed authority is enforced, so the change is reverted"
		);
	}

	/// An import replays records as they were exported, so the schema clauses
	/// that shaped them are not applied again.
	#[tokio::test]
	async fn an_import_applies_no_clause() {
		let db = TestDb::new(
			"
			DEFINE TABLE t SCHEMALESS;
			DEFINE FIELD n ON t TYPE int DEFAULT 7;
			",
		)
		.await;
		let exec = db.exec_ctx_as(&TestDb::owner(), TransactionType::Write).await;
		let options = exec.options().expect("a database context carries options").clone();
		let mut root = exec.root().clone();
		root.options = Some(options.new_with_import(true));
		let importing = match exec {
			ExecutionContext::Database(db_ctx) => ExecutionContext::Database(DatabaseContext {
				ns_ctx: NamespaceContext {
					root,
					ns: db_ctx.ns_ctx.ns,
				},
				..db_ctx
			}),
			ExecutionContext::Root(_) | ExecutionContext::Namespace(_) => {
				panic!("a write context is a database context")
			}
		};
		let ctx = Arc::new(
			WriteCtx::resolve(
				&importing,
				&TableName::from("t"),
				TableExistence::DefinedOnDemand,
				None,
				Arc::new(WriteClauses::default()),
			)
			.await
			.expect("the write context resolves"),
		);
		let mut row = creating_row(object(vec![("id", Value::RecordId(subject()))]));

		kernel(&mut row, &ctx).await.unwrap();

		assert!(!carries(&row.current, "n"), "an import derives no default");
	}
}
