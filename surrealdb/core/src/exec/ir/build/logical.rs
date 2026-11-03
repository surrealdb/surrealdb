//! Building logical ops from the planner's logical plan and the request's grouping.
//!
//! Region kinds are assigned by position, and `RETURN`, `BREAK` and `CONTINUE` bind to the region
//! the nesting rules name.
//!
//! A request binds as one container whose top region is the request. Its statements are grouped
//! into transactions by the request driver's grouping, and every statement, a transaction
//! control included, is a `logical.stmt` with a region of its own that receives the statement
//! timestamp. The ops of a statement are built by [`bind_op`], which answers what the statement
//! left in the block it was bound in.
//!
//! Position decides a region's kind. An op that stands directly in a statement list takes the
//! statement kind of its regions; an op whose result is an operand takes the value kind. An arm of
//! an `IF` and the body of a `FOR` written as a block are that region's statement list, with no
//! block of their own.
//!
//! A statement that follows a terminator of its list never runs and is still bound. A refusal
//! bound among such statements is the list owner's error, so the owner is replaced by an
//! `expr.raise`; otherwise those statements are erased as unreachable.
//!
//! A relational plan binds one op per plan node. The per-row nodes (filter, permission filter,
//! split, projection, fetch) that stand directly over one input form the body of a `logical.loop`
//! of kind `Select` over that input; the nodes that need every row (aggregate, sort, distinct,
//! limit) and the leaves (source, values, subquery, union, join) stand outside a loop and consume
//! the output of the loop below them. A node that needs every row directly over a leaf consumes
//! the output of a loop whose body delivers each row unchanged, so every `SELECT` reads its source
//! through a loop. In a loop body an op denotes its row-at-a-time form: the row a node drops
//! delivers nothing for its iteration.
//!
//! The operands a statement evaluates once (the `LIMIT` and `START` values, a `TIMEOUT` duration,
//! a `VERSION`) are bound before the input they apply to, so they are evaluated before any row is
//! read. `TIMEOUT` holds the rest of the plan in its `Deadline` region, and `VERSION` sets the
//! snapshot of every op bound beneath it.
//!
//! A permission filter and a join lifted from a permission predicate are bound under the frame
//! their plan node names, and so is every op of the regions that evaluate in it.
//!
//! A mutation binds as a `logical.loop` of kind `Write` immediately before the `logical.write` that
//! consumes its result. The loop's source is the statement's subjects: the targets gathered in an
//! array, the rows of an `INSERT` as one value, or the rows of a relational plan. Its body holds
//! one `logical.write_stage` per stage of the statement's order, in order, each with a region for
//! the code a table stores for that stage. The clauses of the statement that run per subject are
//! regions of the stage that evaluates them: the data clauses of `ComputeInputData` and the
//! condition of `CheckWhereCondition`. Each clause region binds the row names of its own frame to
//! its block arguments, so `$this` is the record the clause is evaluated against and never the
//! loop's element, which is the target the statement names. A `TIMEOUT` and an `EXPLAIN` are the
//! nodes the plan puts around the mutation, which bind as `logical.timeout` and
//! `logical.explain`.
//!
//! A per-element form of an expression is a `logical.loop` of its own, bound through
//! [`bind_loop_site`]: the closure builtins that fold, search, filter and map an array or a set,
//! element filters and broadcasts. A closure is an `expr.closure_create` whose captures are
//! operands and whose isolated `ClosureBody` region receives the statement timestamp, the
//! parameters and the captures, in that order. The names a function or closure body reads are its
//! own arguments and nothing of the region that creates or calls it.

#![deny(clippy::wildcard_enum_match_arm)]

use std::borrow::Cow;
use std::collections::HashSet;
use std::ops::ControlFlow;
use std::slice;
use std::sync::Arc;

use smallvec::SmallVec;
use surrealdb_expr::parser::TopLevelExpr;
use surrealdb_iam::AuthLimit;
use surrealdb_strand::{Strand, TableName};
use surrealdb_types::ToSql;

use super::expr::{bind_judgement, key_range_shape, range_key_exprs, record_id_list};
use super::membership::memberships_of;
use super::subscription::{bind_layer, subscriber_layer};
use super::{stage_bundle, stmt};
use crate::exec::expression_registry::resolve_order_by_alias;
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::analysis::tokens;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, IrScope};
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{
	CallAttrs, ClosureAttrs, ClosureLiteral, DispatchTarget, ExprOp, NestedStrategy, RaiseKind,
	passes_row_names,
};
use crate::exec::ir::iface::catalog::{CatalogAnswer, EffectCx, FieldView, SemanticCatalog};
use crate::exec::ir::iface::ids::{OpId, ValueId};
use crate::exec::ir::iface::region_kind::{
	ClauseRole, ErrorScope, FieldRole, RegionKind, RegionProperties, StatementCommit, StoredRole,
	TxnKind,
};
use crate::exec::ir::iface::statements::{
	ContentMode, OutputShape, Place, PlacePart, UnaddressedPart, WriteFlags, WriteForm,
};
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::logical::constraints::{RegionRejection, rejection_of};
use crate::exec::ir::logical::ops::{
	AggregateShape, AssignAttrs, ClosureLoopAttrs, ComputedReadsShape, DecisiveKind, DerivedReads,
	FieldShape, FieldsShape, ImageLayers, ImageView, LetAttrs, LetBinding, LinkedStages, LogOp,
	LookupAttrs, LookupSubjectShape, LoopKind, LoopSink, ProjectAttrs, RecurseAttrs,
	RecurseInstructionShape, RecursePathForm, SortAttrs, SortKeyShape, SortShape, SourceAttrs,
	SourceOrder, SourceRefShape, StepAttrs, Stored, TxnControl, TxnRegionMode, WindowCallShape,
	WindowShape, WriteAttrs, WriteDerived, WriteGates, WriteImage, WriteStageKind, WriteStages,
	WriteStored,
};
use crate::exec::ir::logical::scope::return_target;
use crate::exec::ir::logical::seam::{
	BindCx, BindNotes, Built, Control, ExprHost, LoopSite, NameRef, bind_body, bind_expr,
	bind_fetch_path, invariant,
};
use crate::exec::ir::logical::stages::{
	CondVerdict, Derivation, SchemaRegion, TableGates, TableIndexes, TableRepairs, TableSchema,
	TableViews, ViewBody, catalog_fixed_of, cond_verdict, derivations_of, edges_of, gates_of,
	image_of, indexes_of, into_text_of, observed_of, repairs_of, schema_of, stage_run, static_mark,
	stored_of, views_of, write_body, written_table,
};
use crate::exec::ir::logical::txn::{StatementBody, StatementSlot, TopRegion, regions_of};
use crate::exec::ir::logical::verify::{loop_body_args, timestamp_arg};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::unit::{UnitForm, UnitKind};
use crate::exec::ir::{RequestInput, RequestStatement};
use crate::exec::logical::may_read_clock;
use crate::exec::logical::pipelines::is_range_mock;
use crate::exec::operators::write::plan::{WriteFacts, WriteForm as PlanForm};
use crate::exec::operators::{SplitTarget, split_target};
use crate::exec::planner::analyse_aggregation;
use crate::exec::planner::fold::{changes_the_catalog, computed_reads, permission_subject};
use crate::exec::planner::util::check_forbidden_group_by_params;
use crate::exec::planner::write::write_facts;
use crate::exec::surface::{FULL, Surface};
use crate::exec::vm::payload::Refusal;
use crate::expr::logical::{
	EvalFrame, EvalMode, GuardKind, LogicalAggregate, LogicalAssignment, LogicalCall,
	LogicalClosure, LogicalData, LogicalEval, LogicalExpr, LogicalFetch, LogicalField,
	LogicalFields, LogicalFilter, LogicalForeach, LogicalIfOp, LogicalInsertRows, LogicalJoin,
	LogicalLet, LogicalLimit, LogicalLiteral, LogicalLookup, LogicalLookupSubject, LogicalOp,
	LogicalOrder, LogicalOrdering, LogicalPermissionFilter, LogicalProject, LogicalRecordIdKey,
	LogicalRecordIdKeyRange, LogicalRecurse, LogicalRecurseInstruction, LogicalRecursePath,
	LogicalReturn, LogicalSequence, LogicalSleep, LogicalSort, LogicalSource, LogicalSplit,
	LogicalSubquery, LogicalThrow, LogicalValues, LogicalWindow, LogicalWrite, LogicalWriteSource,
	LogicalWriteStatement, OutputPath, OutputPathPart, RequestStatementKind, Schema, SourceRef,
	lower, lower_expr, lower_fields, lower_request,
};
use crate::expr::lookup::LookupKind;
use crate::expr::{Function, Idiom, Kind, Output, Part};

/// What binding a statement left in the block it was bound in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Flow {
	/// The statement's value.
	Value(ValueId),
	/// The statement has the value `NONE` and built no op for it.
	None,
	/// The statement ended the block with a terminator.
	Diverged,
}

/// Where a statement stands, which decides the kind of the regions it owns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Pos {
	/// Directly in a statement list.
	Statement,
	/// As an operand, or in a region that is not a statement list.
	Value,
}

impl Pos {
	fn block(self) -> RegionKind {
		match self {
			Pos::Statement => RegionKind::Block,
			Pos::Value => RegionKind::ValueBlock,
		}
	}

	fn branch(self) -> RegionKind {
		match self {
			Pos::Statement => RegionKind::StatementIf,
			Pos::Value => RegionKind::ValueIf,
		}
	}

	fn foreach(self) -> RegionKind {
		match self {
			Pos::Statement => RegionKind::StatementFor,
			Pos::Value => RegionKind::ValueFor,
		}
	}
}

/// What closes a statement list's region.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Tail {
	/// A `yield` of the last statement's value.
	Value,
	/// A `yield` of nothing: the body is run for its effects.
	Discard,
}

/// Binds the statements of `input` into a container in the logical dialect and runs the
/// region-constraint check on it.
///
/// Answers an error only for a broken invariant, of the binding or of the check, or a statement
/// with no binding form; a statement the user wrote wrongly is bound as the `expr.raise`
/// that raises its error. A construct a region does not admit is not an error either: the
/// container is the program the statements mean, and the rejection is answered by
/// [`bind_request_checked`], which a caller that answers the rejection binds with.
pub(crate) async fn bind_request(
	input: &RequestInput<'_>,
	cx: &EffectCx<'_>,
) -> Result<Ir, IrError> {
	bind_request_checked(input, cx).await.map(|(ir, _)| ir)
}

/// Binds the request and judges the bound container against the region constraints, answering the
/// container and the first rejection, if any.
///
/// A rejection is not an error of the binding: the container is the program the statements
/// mean, and the caller that answers the request with the rejection takes
/// [`RegionRejection::error`]. `Err` is only an invariant break, of the binding or of the check.
///
/// The check runs only on a container whose unit is one the request wrote ([`UnitKind::checked`]):
/// a function body, a table's read unit and the stored code of a write are never judged, so a
/// definition stored before a constraint existed keeps working.
///
/// The request is bound for the entry that serves every statement family ([`FULL`]).
pub(crate) async fn bind_request_checked(
	input: &RequestInput<'_>,
	cx: &EffectCx<'_>,
) -> Result<(Ir, Option<RegionRejection>), IrError> {
	bind_request_noted(input, cx, Some(&FULL)).await.map(|(ir, rejection, _)| (ir, rejection))
}

/// [`bind_request_checked`], also answering what the binder noted of the logical plans it bound
/// ([`BindNotes`]).
pub(crate) async fn bind_request_noted(
	input: &RequestInput<'_>,
	cx: &EffectCx<'_>,
	surface: Option<&'static Surface>,
) -> Result<(Ir, Option<RegionRejection>, BindNotes), IrError> {
	let (mut ir, notes) = bind_statements(input, cx, surface)?;
	let rejection = if input.unit.checked() {
		ir.judge_constraints();
		rejection_of(&ir, cx)?
	} else {
		None
	};
	tokens::thread(&mut ir, &mut FactCache::new(), cx)?;
	Ok((ir, rejection, notes))
}

/// Binds the request synchronously, so that no binder state is held across a suspension point.
fn bind_statements(
	input: &RequestInput<'_>,
	effects: &EffectCx<'_>,
	surface: Option<&'static Surface>,
) -> Result<(Ir, BindNotes), IrError> {
	match input.unit.form() {
		UnitForm::Request => bind_requested(input, effects, surface),
		UnitForm::Statement | UnitForm::List | UnitForm::Value => {
			bind_unit(input, effects, surface).map(|ir| (ir, BindNotes::default()))
		}
		UnitForm::Catalog => stage_bundle::bind_catalog_unit(input, effects, surface)
			.map(|ir| (ir, BindNotes::default())),
		UnitForm::Extracted => Err(invariant(
			"a loop body is extracted from a planned container and never bound from a statement",
		)),
	}
}

/// Binds the statements of a request or fragment, or the one statement of a request that is
/// planned on its own, into a container whose top region is the request.
///
/// A statement that stands alone keeps the position the unit names, which is the `N` of
/// `(statement N)` in the text of a rejection.
fn bind_requested(
	input: &RequestInput<'_>,
	effects: &EffectCx<'_>,
	surface: Option<&'static Surface>,
) -> Result<(Ir, BindNotes), IrError> {
	let IrScope::Request {
		fragment,
	} = &input.scope
	else {
		return Err(invariant("a request binds into a container whose scope is a request"));
	};
	let base = input.unit.position();
	if base != 0 && input.statements.len() != 1 {
		return Err(invariant("a statement planned on its own is one statement"));
	}
	let kinds: Vec<RequestStatementKind> = input.statements.iter().map(request_kind).collect();
	// The session transaction a client holds is not part of the input, so the request is grouped
	// as one that holds none.
	let groups = lower_request(&kinds, false);
	let regions = regions_of(&groups, *fragment, None);
	let mut ir = Ir::new(input.scope.clone(), RegionKind::Request);
	let mut facts = FactCache::new();
	let notes = {
		let mut cx = BindCx::new(&mut ir, &mut facts, *effects)?;
		cx.set_serves_late(input.serves_late);
		cx.set_late_bound(input.late_bound);
		cx.set_surface(surface);
		cx.set_links_stages(
			matches!(input.unit, UnitKind::Request | UnitKind::RequestStatement { .. })
				&& effects.catalog.links_stages(),
		);
		for region in &regions {
			bind_region(&mut cx, input.statements, base, region)?;
		}
		cx.take_notes()
	};
	Ok((ir, notes))
}

/// Binds the one statement of a unit that is not a request into a container whose top region is
/// the unit's own.
///
/// The top block takes the arguments the unit states ([`UnitKind::top_args`]): the statement
/// timestamp, because no statement region around the unit supplies it, and the names a
/// subscription reads. A statement unit holds the statement as the whole of its body; a
/// list unit binds the statements of a block as the statement list of its region and closes it
/// with the value of the last; a value unit binds the statement as the value of its region. Where
/// the planner's binding step refused the whole statement, or a refusal stands among statements
/// that follow a terminator, the unit holds the `expr.raise` of the refusal alone.
fn bind_unit(
	input: &RequestInput<'_>,
	effects: &EffectCx<'_>,
	surface: Option<&'static Surface>,
) -> Result<Ir, IrError> {
	let [statement] = input.statements else {
		return Err(invariant("a unit that is not a request holds exactly one statement"));
	};
	let op = logical_of(*statement, effects.registry)?;
	let mut ir =
		Ir::with_top_args(input.scope.clone(), input.unit.top_region(), &input.unit.top_args());
	let mut facts = FactCache::new();
	{
		let mut cx = BindCx::new(&mut ir, &mut facts, *effects)?;
		cx.set_statement(input.unit.position())?;
		cx.set_late_bound(input.late_bound);
		cx.set_surface(surface);
		cx.begin_statement();
		cx.note_catalog_change_of(&op);
		if let UnitKind::StageBundle {
			table,
			..
		} = &input.unit
		{
			cx.bind_bundle_of(table.clone());
		}
		if let UnitKind::ViewRows {
			table,
		} = &input.unit
		{
			cx.bind_view_rows_of(table.clone());
		}
		match input.unit.form() {
			UnitForm::Statement => bind_logical_statement(&mut cx, &op)?,
			UnitForm::List => {
				let statements = statements_of(&op);
				let refusal = if matches!(input.unit, UnitKind::FunctionBody) {
					// The row of the caller follows the timestamp.
					let row = cx.top_arg(1)?;
					cx.with_row(row, |cx| bind_list(cx, statements, Pos::Statement, Tail::Value))?
				} else {
					bind_list(&mut cx, statements, Pos::Statement, Tail::Value)?
				};
				raise_if_refused(&mut cx, refusal)?;
			}
			UnitForm::Value => {
				let flow = if matches!(input.unit, UnitKind::Guard) {
					// The row the guard judges follows the timestamp.
					let row = cx.top_arg(1)?;
					cx.declare_row_names(row, ROW_NAMES);
					cx.with_row(row, |cx| bind_op(cx, &op, Pos::Value))?
				} else {
					bind_op(&mut cx, &op, Pos::Value)?
				};
				if let Some(value) = value_of(&mut cx, flow)? {
					cx.yield_expr(&[value])?;
				}
				raise_if_refused(&mut cx, None)?;
			}
			UnitForm::Request | UnitForm::Extracted | UnitForm::Catalog => {
				return Err(invariant("a unit is bound by the form its kind names"));
			}
		}
	}
	Ok(ir)
}

/// Replaces everything the unit bound with the `expr.raise` of the refusal the planner raises
/// for the whole statement before any of it runs, or of `refusal`, if either exists.
fn raise_if_refused(cx: &mut BindCx<'_>, refusal: Option<Refusal>) -> Result<(), IrError> {
	if let Some(refusal) = cx.take_hoisted().or(refusal) {
		erase_bound(cx, 0)?;
		cx.raise(RaiseKind::Refused(Box::new(refusal)))?;
	}
	Ok(())
}

/// The logical form of `statement`, which a unit other than a request holds as its body, with its
/// memberships written as the predicates the binder binds ([`memberships_of`]).
fn logical_of(
	statement: RequestStatement<'_>,
	registry: &FunctionRegistry,
) -> Result<LogicalOp, IrError> {
	match statement {
		RequestStatement::Logical(op) => Ok(memberships_of(op, registry, &|_| false)?.into_owned()),
		RequestStatement::Ast(TopLevelExpr::Expr(expr)) => lower(expr.clone())
			.map_err(|error| invariant(&format!("a statement without a logical form: {error}"))),
		RequestStatement::Ast(_) => {
			Err(invariant("a unit that is not a request holds an expression statement"))
		}
	}
}

/// What the request grouping makes of `statement`.
fn request_kind(statement: &RequestStatement<'_>) -> RequestStatementKind {
	match statement {
		RequestStatement::Ast(TopLevelExpr::Begin(_)) => RequestStatementKind::Begin,
		RequestStatement::Ast(TopLevelExpr::Commit) => RequestStatementKind::Commit,
		RequestStatement::Ast(TopLevelExpr::Cancel) => RequestStatementKind::Cancel,
		RequestStatement::Ast(TopLevelExpr::Option(_)) => RequestStatementKind::Option,
		RequestStatement::Ast(
			TopLevelExpr::Access(_)
			| TopLevelExpr::Kill(_)
			| TopLevelExpr::Live(_)
			| TopLevelExpr::Use(_)
			| TopLevelExpr::Show(_)
			| TopLevelExpr::Expr(_),
		)
		| RequestStatement::Logical(_) => RequestStatementKind::Run,
	}
}

/// Binds one top-level region of the request: a statement of its own, or a transaction and the
/// statements it holds.
fn bind_region(
	cx: &mut BindCx<'_>,
	statements: &[RequestStatement<'_>],
	base: usize,
	region: &TopRegion,
) -> Result<(), IrError> {
	match region {
		TopRegion::Statement(slot) => bind_slot(cx, statements, base, slot),
		TopRegion::Transaction {
			mode,
			statements: slots,
		} => {
			if let Some(first) = slots.first() {
				cx.set_statement(first.index + base)?;
			}
			let txn = cx.emit(
				OpKind::Logical(LogOp::Txn {
					mode: *mode,
				}),
				&[],
				&[TypeKnowledge::Unknown],
			)?;
			let kind = RegionKind::Transaction(match mode {
				TxnRegionMode::Explicit {
					..
				} => TxnKind::Explicit,
				TxnRegionMode::Session {
					..
				} => TxnKind::Session,
			});
			cx.within(txn, kind, &[], |cx, _| {
				for slot in slots {
					bind_slot(cx, statements, base, slot)?;
				}
				Ok(())
			})
		}
	}
}

/// Binds one statement region and the statement its slot names.
fn bind_slot(
	cx: &mut BindCx<'_>,
	statements: &[RequestStatement<'_>],
	base: usize,
	slot: &StatementSlot,
) -> Result<(), IrError> {
	cx.set_statement(slot.index + base)?;
	let stmt = cx.emit(
		OpKind::Logical(LogOp::Stmt {
			commit: slot.commit,
			control: match slot.body {
				StatementBody::Begin => Some(TxnControl::Begin),
				StatementBody::Commit => Some(TxnControl::Commit),
				StatementBody::Cancel => Some(TxnControl::Cancel),
				StatementBody::Run | StatementBody::Misplaced(..) => None,
			},
		}),
		&[],
		&[TypeKnowledge::Unknown],
	)?;
	cx.within(stmt, RegionKind::Statement(slot.commit), &[timestamp_arg()], |cx, _| {
		match slot.body {
			StatementBody::Run => {
				let statement = statements
					.get(slot.index)
					.ok_or_else(|| invariant("a statement slot names no statement"))?;
				if let RequestStatement::Logical(plan) = statement {
					cx.note_statement(stmt, plan);
				}
				bind_statement(cx, *statement)
			}
			StatementBody::Begin | StatementBody::Commit | StatementBody::Cancel => {
				let none = cx.none_literal()?;
				cx.yield_logical(&[none])
			}
			StatementBody::Misplaced(control) => cx.raise(RaiseKind::Misplaced(control)),
		}
	})
}

/// Binds `statement` into the statement region that is the current region, and closes the region
/// with the statement's value unless the statement ended it.
///
/// A refusal that the planner raises for the whole statement, before any of it runs (see
/// [`bind_let`]), replaces everything the statement bound with the `expr.raise` of the refusal.
/// A logical statement binds with its memberships written as the predicates the binder binds
/// ([`memberships_of`]), so the rows it reads through them are kept by the predicates they were.
fn bind_statement(cx: &mut BindCx<'_>, statement: RequestStatement<'_>) -> Result<(), IrError> {
	cx.begin_statement();
	let statement = match statement {
		RequestStatement::Logical(op) => {
			let op = memberships_of(op, cx.registry(), &|name| {
				matches!(cx.lookup(name), NameRef::Value(_))
			})?;
			return bind_logical_statement(cx, &op);
		}
		RequestStatement::Ast(statement) => statement,
	};
	match statement {
		TopLevelExpr::Expr(expr) => {
			let op = lower(expr.clone()).map_err(|error| {
				invariant(&format!("a statement without a logical form: {error}"))
			})?;
			bind_logical_statement(cx, &op)
		}
		TopLevelExpr::Begin(_) | TopLevelExpr::Commit | TopLevelExpr::Cancel => {
			Err(invariant("a transaction control is bound by the body its slot names"))
		}
		TopLevelExpr::Option(_)
		| TopLevelExpr::Use(_)
		| TopLevelExpr::Access(_)
		| TopLevelExpr::Kill(_)
		| TopLevelExpr::Live(_)
		| TopLevelExpr::Show(_) => {
			let flow = stmt::schema_binding(cx)?.statement(cx, statement)?;
			close_statement(cx, flow)
		}
	}
}

/// Binds the logical statement `op` into the region that is the current region, and closes the
/// region with the statement's value unless the statement ended it.
fn bind_logical_statement(cx: &mut BindCx<'_>, op: &LogicalOp) -> Result<(), IrError> {
	cx.note_catalog_change_of(op);
	let flow = bind_op(cx, op, Pos::Statement)?;
	if let Some(refusal) = cx.take_hoisted() {
		erase_bound(cx, 0)?;
		return cx.raise(RaiseKind::Refused(Box::new(refusal)));
	}
	if let Some(value) = value_of(cx, flow)? {
		cx.yield_logical(&[value])?;
	} else if !cx.ends_in_terminator()? {
		cx.yield_logical(&[])?;
	}
	Ok(())
}

/// Closes the statement region with the value `flow` left, unless the statement ended it.
fn close_statement(cx: &mut BindCx<'_>, flow: Flow) -> Result<(), IrError> {
	if let Some(value) = value_of(cx, flow)? {
		cx.yield_logical(&[value])?;
	} else if !cx.ends_in_terminator()? {
		cx.yield_logical(&[])?;
	}
	Ok(())
}

/// The value of `flow`, built as a `NONE` literal when the statement has none of its own; `None`
/// when the block has ended.
pub(super) fn value_of(cx: &mut BindCx<'_>, flow: Flow) -> Result<Option<ValueId>, IrError> {
	match flow {
		Flow::Value(value) => Ok(Some(value)),
		Flow::None => cx.none_literal().map(Some),
		Flow::Diverged => Ok(None),
	}
}

/// The answer to the expression binder for a statement bound in value position.
fn built_of(cx: &mut BindCx<'_>, flow: Flow) -> Result<Built, IrError> {
	Ok(match value_of(cx, flow)? {
		Some(value) => Built::Value(value),
		None => Built::Diverged,
	})
}

/// What planning raises for a `LET` written as an expression.
pub(crate) const LET_IN_AN_EXPRESSION: &str =
	"LET statements can only appear at the top level of a query or inside a block expression";

/// Binds a statement in value position: a block as an op of its own, a `FOR` as an op of its own
/// where it stands in a statement's region or a statement list ([`runs_statements`]) and as an
/// `expr.subquery` holding it elsewhere, a `LET` as the refusal of a `LET` written as an
/// expression, any other statement as an `expr.subquery` holding it.
///
/// Where the `LET` is planned with its top-level statement the refusal is the whole statement's,
/// raised before any of it runs, whether or not the `LET` is reached; otherwise it is raised where
/// the `LET` stands, when it is reached. Where the bind defers it ([`BindCx::defers_let`]: a
/// write's place, code the catalog stores) the `LET` is the `expr.subquery` whose region raises
/// the refusal, so it is raised only when the value is evaluated.
pub(crate) fn bind_value_statement(cx: &mut BindCx<'_>, op: &LogicalOp) -> Result<Built, IrError> {
	let flow = match op {
		LogicalOp::Sequence(sequence) => bind_sequence(cx, sequence, Pos::Value)?,
		LogicalOp::Foreach(foreach) if runs_statements(cx) => {
			bind_foreach(cx, foreach, Pos::Value)?
		}
		LogicalOp::Foreach(_) => return bind_subquery(cx, op),
		LogicalOp::Let(_) if cx.defers_let() => return refusing_subquery(cx),
		LogicalOp::Let(_) => {
			let refusal = Refusal::InvalidStatement {
				message: LET_IN_AN_EXPRESSION.to_owned(),
			};
			if cx.eager() {
				cx.hoist_refusal(refusal.clone());
				raise_refused(cx, refusal)?
			} else {
				cx.raise(RaiseKind::Refused(Box::new(refusal)))?;
				Flow::Diverged
			}
		}
		LogicalOp::Source(_)
		| LogicalOp::Values(_)
		| LogicalOp::Match(_)
		| LogicalOp::Subquery(_)
		| LogicalOp::Write(_)
		| LogicalOp::Ddl(_)
		| LogicalOp::UnionAll(_)
		| LogicalOp::Join(_)
		| LogicalOp::Filter(_)
		| LogicalOp::PermissionFilter(_)
		| LogicalOp::Split(_)
		| LogicalOp::Aggregate(_)
		| LogicalOp::Distinct(_)
		| LogicalOp::Window(_)
		| LogicalOp::Sort(_)
		| LogicalOp::Limit(_)
		| LogicalOp::Project(_)
		| LogicalOp::Fetch(_)
		| LogicalOp::Timeout(_)
		| LogicalOp::VersionScope(_)
		| LogicalOp::Only(_)
		| LogicalOp::Explain(_)
		| LogicalOp::Eval(_)
		| LogicalOp::Return(_)
		| LogicalOp::If(_)
		| LogicalOp::Throw(_)
		| LogicalOp::Break(_)
		| LogicalOp::Continue(_)
		| LogicalOp::Info(_)
		| LogicalOp::Sleep(_) => return bind_subquery(cx, op),
	};
	built_of(cx, flow)
}

/// Whether the point being bound stands in a region whose ops run as statements: a statement's
/// region, the region of an `expr.subquery`, or a statement list ([`BindCx::in_statement_list`])
/// other than the arm or body of a value `IF` or `FOR`. A `FOR` loop runs only as a statement; in
/// any other region (a clause, an operand, a value arm, a loop or closure body) its value is that
/// of a subquery holding it. A parenthesised `FOR` is an expression, so a value arm holds it in a
/// subquery of its own, which lowers in expression position.
fn runs_statements(cx: &BindCx<'_>) -> bool {
	let Some(kind) = cx.kinds().last() else {
		return cx.in_statement_list();
	};
	match kind {
		RegionKind::Statement(_) | RegionKind::Subquery => true,
		RegionKind::ValueIf | RegionKind::ValueFor => false,
		RegionKind::Request
		| RegionKind::Transaction(_)
		| RegionKind::Block
		| RegionKind::ValueBlock
		| RegionKind::StatementIf
		| RegionKind::StatementFor
		| RegionKind::Loop
		| RegionKind::FunctionBody
		| RegionKind::ClosureBody
		| RegionKind::PermissionFrame
		| RegionKind::FieldFrame(_)
		| RegionKind::EventFrame
		| RegionKind::SubscriberFrame
		| RegionKind::Clause(_)
		| RegionKind::LazyOperand
		| RegionKind::ErrorScope(_)
		| RegionKind::ChooseArm
		| RegionKind::Pipeline
		| RegionKind::JoinLookupArm
		| RegionKind::WriteStage
		| RegionKind::CatalogUnit
		| RegionKind::Deadline
		| RegionKind::StoredClause(_) => cx.in_statement_list(),
	}
}

/// The `expr.subquery` whose region raises the refusal of a `LET` written as an expression, which
/// is raised when the value is evaluated.
fn refusing_subquery(cx: &mut BindCx<'_>) -> Result<Built, IrError> {
	let subquery = cx.emit(
		OpKind::Expr(ExprOp::Subquery(NestedStrategy::Apply)),
		&[],
		&[TypeKnowledge::Unknown],
	)?;
	let result = first_result(cx, subquery)?;
	cx.within(subquery, RegionKind::Subquery, &[], |cx, _| {
		cx.raise(RaiseKind::Refused(Box::new(Refusal::InvalidStatement {
			message: LET_IN_AN_EXPRESSION.to_owned(),
		})))
	})?;
	Ok(Built::Value(result))
}

/// Binds `input`, the statement an `EXPLAIN` explains, and answers the value its plan stands for.
///
/// A statement that is a value is bound in value position. A `RETURN`, `LET`, `IF` or `FOR` is
/// bound as the one statement of the list of a `logical.statement`, whose region catches what the
/// statement diverges to: the plan holds the statement as it stands in a statement list, and
/// nothing of it runs, since the `EXPLAIN` answers its rendering. An `IF` written where a value
/// stands is that statement, each arm the statement list it holds. A `THROW`, `BREAK` or `CONTINUE`
/// plans no op of its own, so it stands as a `NONE` that carries its text.
fn bind_explained(cx: &mut BindCx<'_>, input: &LogicalOp) -> Result<Option<ValueId>, IrError> {
	let written = if let LogicalOp::Eval(eval) = input {
		Some(&eval.expr)
	} else {
		None
	};
	if let Some(text) = explained_text_of(input, written) {
		return explained_text(cx, text);
	}
	let statement;
	let input = if let Some(LogicalExpr::If(branch)) = written {
		let arm = |value: &LogicalExpr| {
			if let LogicalExpr::Subquery(op) = value {
				op.as_ref().clone()
			} else {
				LogicalOp::Eval(LogicalEval {
					expr: value.clone(),
				})
			}
		};
		statement = LogicalOp::If(LogicalIfOp::new(
			branch.branches.iter().map(|(cond, then)| (cond.clone(), arm(then))).collect(),
			branch.otherwise.as_ref().map(arm),
		));
		&statement
	} else if matches!(
		input,
		LogicalOp::Return(_) | LogicalOp::Let(_) | LogicalOp::If(_) | LogicalOp::Foreach(_)
	) {
		input
	} else {
		return bind_value(cx, input);
	};
	let owner = cx.emit(
		OpKind::Logical(LogOp::Statement {
			changes_catalog: false,
			served: None,
		}),
		&[],
		&[TypeKnowledge::Unknown],
	)?;
	let refusal = cx.within(
		owner,
		RegionKind::Statement(StatementCommit::InTxn),
		&[timestamp_arg()],
		|cx, _| bind_list(cx, std::slice::from_ref(input), Pos::Statement, Tail::Value),
	)?;
	let flow = owner_flow(cx, owner, refusal)?;
	value_of(cx, flow)
}

/// The text of `input`, a statement an `EXPLAIN` explains, when it plans no op of its own: a
/// `THROW`, `BREAK` or `CONTINUE`, written as a statement or as the expression `written`.
fn explained_text_of(input: &LogicalOp, written: Option<&LogicalExpr>) -> Option<String> {
	if let LogicalOp::Throw(throw) = input {
		return Some(format!("THROW {}", throw.value.to_sql()));
	}
	if matches!(input, LogicalOp::Break(_)) || matches!(written, Some(LogicalExpr::Break)) {
		return Some("BREAK".to_owned());
	}
	if matches!(input, LogicalOp::Continue(_)) || matches!(written, Some(LogicalExpr::Continue)) {
		return Some("CONTINUE".to_owned());
	}
	if let Some(LogicalExpr::Throw(value)) = written {
		return Some(format!("THROW {}", value.to_sql()));
	}
	None
}

/// A `NONE` that carries `text`, the statement an `EXPLAIN` explains that plans no op of its own.
fn explained_text(cx: &mut BindCx<'_>, text: String) -> Result<Option<ValueId>, IrError> {
	let none = cx.emit(
		OpKind::Expr(ExprOp::Literal(Box::new(crate::val::Value::None))),
		&[],
		&[TypeKnowledge::Unknown],
	)?;
	cx.write_text_on(none, text)?;
	first_result(cx, none).map(Some)
}

/// Binds `op` as the only statement of an `expr.subquery`, which yields its value.
fn bind_subquery(cx: &mut BindCx<'_>, op: &LogicalOp) -> Result<Built, IrError> {
	let subquery = cx.emit(
		OpKind::Expr(ExprOp::Subquery(NestedStrategy::Apply)),
		&[],
		&[TypeKnowledge::Unknown],
	)?;
	let result = first_result(cx, subquery)?;
	cx.within(subquery, RegionKind::Subquery, &[], |cx, _| {
		let flow = bind_op(cx, op, Pos::Value)?;
		if let Some(value) = value_of(cx, flow)? {
			cx.yield_expr(&[value])?;
		} else if !cx.ends_in_terminator()? {
			cx.yield_expr(&[])?;
		}
		Ok(())
	})?;
	Ok(Built::Value(result))
}

/// Binds `op`, standing at `pos`, at the end of the current block.
fn bind_op(cx: &mut BindCx<'_>, op: &LogicalOp, pos: Pos) -> Result<Flow, IrError> {
	match op {
		LogicalOp::Eval(eval) => bind_eval(cx, eval, pos),
		LogicalOp::Sequence(sequence) => bind_sequence(cx, sequence, pos),
		LogicalOp::Let(statement) => bind_let(cx, statement),
		LogicalOp::Return(statement) => bind_return(cx, statement),
		LogicalOp::Foreach(foreach) => bind_foreach(cx, foreach, pos),
		LogicalOp::If(branch) => bind_if(cx, branch, pos),
		LogicalOp::Throw(statement) => bind_throw(cx, statement),
		LogicalOp::Break(_) => bind_control(cx, Control::Break),
		LogicalOp::Continue(_) => bind_control(cx, Control::Continue),
		LogicalOp::Sleep(statement) => bind_sleep(cx, statement),
		LogicalOp::Source(_)
		| LogicalOp::Values(_)
		| LogicalOp::Subquery(_)
		| LogicalOp::UnionAll(_)
		| LogicalOp::Join(_)
		| LogicalOp::Filter(_)
		| LogicalOp::PermissionFilter(_)
		| LogicalOp::Split(_)
		| LogicalOp::Aggregate(_)
		| LogicalOp::Distinct(_)
		| LogicalOp::Window(_)
		| LogicalOp::Sort(_)
		| LogicalOp::Limit(_)
		| LogicalOp::Project(_)
		| LogicalOp::Fetch(_)
		| LogicalOp::Timeout(_)
		| LogicalOp::VersionScope(_)
		| LogicalOp::Only(_)
		| LogicalOp::Explain(_) => bind_relational(cx, op),
		LogicalOp::Write(write) => bind_write(cx, op, write),
		LogicalOp::Match(statement) => stmt::bind_match(cx, statement),
		LogicalOp::Ddl(statement) => {
			cx.note_catalog_touch();
			stmt::schema_binding(cx)?.ddl(cx, statement)
		}
		LogicalOp::Info(info) => stmt::schema_binding(cx)?.info(cx, info),
	}
}

/// Whether `op`, bound in a statement list, is a statement that runs as a group of its own: a
/// read, a write, a schema statement or a `MATCH`.
///
/// The control flow of a list (`LET`, `IF`, `FOR`, `RETURN`, `THROW`, `SLEEP`, `BREAK`, `CONTINUE`
/// and a block) and an expression are ops of the list itself.
fn runs_as_group(op: &LogicalOp) -> bool {
	match op {
		LogicalOp::Source(_)
		| LogicalOp::Values(_)
		| LogicalOp::Subquery(_)
		| LogicalOp::UnionAll(_)
		| LogicalOp::Join(_)
		| LogicalOp::Filter(_)
		| LogicalOp::PermissionFilter(_)
		| LogicalOp::Split(_)
		| LogicalOp::Aggregate(_)
		| LogicalOp::Distinct(_)
		| LogicalOp::Window(_)
		| LogicalOp::Sort(_)
		| LogicalOp::Limit(_)
		| LogicalOp::Project(_)
		| LogicalOp::Fetch(_)
		| LogicalOp::Timeout(_)
		| LogicalOp::VersionScope(_)
		| LogicalOp::Only(_)
		| LogicalOp::Explain(_)
		| LogicalOp::Write(_)
		| LogicalOp::Match(_)
		| LogicalOp::Ddl(_) => true,
		LogicalOp::Eval(_)
		| LogicalOp::Sequence(_)
		| LogicalOp::Let(_)
		| LogicalOp::Return(_)
		| LogicalOp::Foreach(_)
		| LogicalOp::If(_)
		| LogicalOp::Throw(_)
		| LogicalOp::Break(_)
		| LogicalOp::Continue(_)
		| LogicalOp::Sleep(_)
		| LogicalOp::Info(_) => false,
	}
}

/// Binds `op`, a statement of a statement list or the value of a `LET` or `RETURN` in one.
///
/// A statement that runs as a group ([`runs_as_group`]) is the `logical.statement` whose region
/// holds its ops and yields its value, which is the op's result; any other statement is bound where
/// it stands. A statement that ends the block with a terminator leaves the region ended by it, and
/// the block ended.
///
/// A group bound where the container serves late-bound statements and a statement before it may
/// have changed the catalog ([`BindCx::binds_served`]) is not bound against the catalog the request
/// was planned with: its `logical.statement` names the statement as written, which the program
/// plans when it reaches it, and its region yields `NONE`.
fn bind_listed(cx: &mut BindCx<'_>, op: &LogicalOp, pos: Pos) -> Result<Flow, IrError> {
	if !runs_as_group(op) {
		return bind_op(cx, op, pos);
	}
	let served = match cx.binds_served() {
		true => Some(cx.note_served(op)?),
		false => None,
	};
	let owner = cx.emit(
		OpKind::Logical(LogOp::Statement {
			changes_catalog: changes_the_catalog(op),
			served,
		}),
		&[],
		&[TypeKnowledge::Unknown],
	)?;
	if served.is_some() {
		cx.within(
			owner,
			RegionKind::Statement(StatementCommit::InTxn),
			&[timestamp_arg()],
			|cx, _| {
				let none = cx.none_literal()?;
				cx.yield_logical(&[none])
			},
		)?;
		return first_result(cx, owner).map(Flow::Value);
	}
	let flow = cx.within(
		owner,
		RegionKind::Statement(StatementCommit::InTxn),
		&[timestamp_arg()],
		|cx, _| {
			let flow = bind_op(cx, op, pos)?;
			if let Some(value) = value_of(cx, flow)? {
				cx.yield_logical(&[value])?;
			}
			Ok(flow)
		},
	)?;
	match flow {
		Flow::Diverged => Ok(Flow::Diverged),
		Flow::Value(_) | Flow::None => first_result(cx, owner).map(Flow::Value),
	}
}

/// The first result of `op`.
pub(super) fn first_result(cx: &BindCx<'_>, op: OpId) -> Result<ValueId, IrError> {
	cx.results(op)?.first().copied().ok_or(IrError::Stale("result"))
}

/// A bare expression: its value as an operand, or an `eval` of its own as a statement.
/// Binds what `bind` builds for `expr`, the expression a statement of a statement list evaluates,
/// as the one statement of a `logical.statement` of its own when `expr` may read the clock.
///
/// A clock read answers the instant its statement started at, and every statement of a list
/// starts at an instant of its own, so the reads of two statements of one list are never one value.
/// The region holds the expression alone: the statement's own control flow (the name a `LET`
/// binds, a `RETURN`, the arms of an `IF`, the body of a `FOR`) stays in the list. An expression
/// that cannot read the clock, and one bound where no statement list stands, is bound in place.
fn bind_instant(
	cx: &mut BindCx<'_>,
	expr: &LogicalExpr,
	bind: impl FnOnce(&mut BindCx<'_>) -> Result<Built, IrError>,
) -> Result<Built, IrError> {
	let listed = cx.in_statement_list();
	bind_instant_in(cx, listed, expr, bind)
}

/// [`bind_instant`] for a statement that stands in a statement list when `listed`, which an `IF`
/// decides once for every condition of its chain.
fn bind_instant_in(
	cx: &mut BindCx<'_>,
	listed: bool,
	expr: &LogicalExpr,
	bind: impl FnOnce(&mut BindCx<'_>) -> Result<Built, IrError>,
) -> Result<Built, IrError> {
	if !listed || !may_read_clock(expr, cx.registry()) {
		return bind(cx);
	}
	let owner = cx.emit(
		OpKind::Logical(LogOp::Statement {
			changes_catalog: false,
			served: None,
		}),
		&[],
		&[TypeKnowledge::Unknown],
	)?;
	let built = cx.within(
		owner,
		RegionKind::Statement(StatementCommit::InTxn),
		&[timestamp_arg()],
		|cx, _| {
			let built = bind(cx)?;
			if let Built::Value(value) = built {
				cx.yield_logical(&[value])?;
			}
			Ok(built)
		},
	)?;
	match built {
		Built::Value(_) => first_result(cx, owner).map(Built::Value),
		Built::Diverged => Ok(Built::Diverged),
	}
}

fn bind_eval(cx: &mut BindCx<'_>, eval: &LogicalEval, pos: Pos) -> Result<Flow, IrError> {
	// A parenthesised `LET` that is a whole statement, or the whole value of a `RETURN` or another
	// `LET`, is that `LET`, not one written as an expression.
	if let LogicalExpr::Subquery(inner) = &eval.expr
		&& let LogicalOp::Let(statement) = inner.as_ref()
	{
		return bind_let(cx, statement);
	}
	let Built::Value(value) = bind_instant(cx, &eval.expr, |cx| bind_expr(cx, &eval.expr))? else {
		return Ok(Flow::Diverged);
	};
	match pos {
		Pos::Value => Ok(Flow::Value(value)),
		Pos::Statement => cx
			.emit_value(OpKind::Logical(LogOp::Eval), &[value], TypeKnowledge::Unknown)
			.map(Flow::Value),
	}
}

/// `SLEEP`: its duration, then the op.
fn bind_sleep(cx: &mut BindCx<'_>, sleep: &LogicalSleep) -> Result<Flow, IrError> {
	let Built::Value(duration) =
		bind_instant(cx, &sleep.duration, |cx| bind_expr(cx, &sleep.duration))?
	else {
		return Ok(Flow::Diverged);
	};
	cx.emit_value(OpKind::Logical(LogOp::Sleep), &[duration], TypeKnowledge::Unknown)
		.map(Flow::Value)
}

/// `THROW`: its value, then the terminator.
fn bind_throw(cx: &mut BindCx<'_>, throw: &LogicalThrow) -> Result<Flow, IrError> {
	let Built::Value(value) = bind_instant(cx, &throw.value, |cx| bind_expr(cx, &throw.value))?
	else {
		return Ok(Flow::Diverged);
	};
	cx.emit(OpKind::Expr(ExprOp::Throw), &[value], &[])?;
	Ok(Flow::Diverged)
}

/// `BREAK` and `CONTINUE`: a terminator to the `FOR` written around them, else the raise of an
/// invalid control flow.
fn bind_control(cx: &mut BindCx<'_>, control: Control) -> Result<Flow, IrError> {
	cx.bind_control(control)?;
	Ok(Flow::Diverged)
}

/// `RETURN`: its value, then a terminator whose target the nesting of the regions decides.
///
/// A `FETCH` is taken into the value the statement yields: into each row of a read, as a `FETCH`
/// over its pipeline is, and into the one value of any other statement, as a `logical.fetch` over
/// that value whose regions are the fetch paths. A statement that runs as a group (a write, or a
/// read whose own `FETCH` the outer one follows) is the group whose value the fetch reads.
fn bind_return(cx: &mut BindCx<'_>, statement: &LogicalReturn) -> Result<Flow, IrError> {
	let flow = if let (Some(valued), true) = (let_of(&statement.what), statement.fetch.is_empty()) {
		bind_valued_let(cx, valued)?
	} else if statement.fetch.is_empty() {
		bind_listed(cx, &statement.what, Pos::Value)?
	} else if starts_a_read(&statement.what) {
		let fetched = LogicalOp::Fetch(LogicalFetch {
			input: statement.what.clone(),
			fetches: statement.fetch.clone(),
		});
		bind_listed(cx, &fetched, Pos::Value)?
	} else {
		let what = bind_listed(cx, &statement.what, Pos::Value)?;
		match value_of(cx, what)? {
			Some(value) => Flow::Value(bind_fetch(cx, value, &statement.fetch)?),
			None => Flow::Diverged,
		}
	};
	let Some(value) = value_of(cx, flow)? else {
		return Ok(Flow::Diverged);
	};
	if return_target(cx.kinds()).is_none() {
		return Err(invariant("a RETURN is written where no region catches it"));
	}
	cx.emit(OpKind::Logical(LogOp::Return), &[value], &[])?;
	Ok(Flow::Diverged)
}

/// Whether `op` is a node of a read's pipeline, whose rows a `FETCH` is taken into one by one.
fn starts_a_read(op: &LogicalOp) -> bool {
	matches!(
		op,
		LogicalOp::Source(_)
			| LogicalOp::Values(_)
			| LogicalOp::Subquery(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Join(_)
			| LogicalOp::Filter(_)
			| LogicalOp::PermissionFilter(_)
			| LogicalOp::Split(_)
			| LogicalOp::Aggregate(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Window(_)
			| LogicalOp::Sort(_)
			| LogicalOp::Limit(_)
			| LogicalOp::Project(_)
	)
}

/// A `logical.fetch` over `value` with one `Clause(Projection)` region per fetch path, evaluated
/// against the value. Answers the fetched value.
fn bind_fetch(
	cx: &mut BindCx<'_>,
	value: ValueId,
	fetches: &[LogicalExpr],
) -> Result<ValueId, IrError> {
	let op = cx.emit(OpKind::Logical(LogOp::Fetch), &[value], &[TypeKnowledge::Unknown])?;
	for path in fetches {
		fetch_path_clause(cx, op, path)?;
	}
	first_result(cx, op)
}

/// `LET`: a name bound to the value for the rest of the statement list it stands in.
///
/// A `LET` that is a whole statement sets a parameter of the session, which no later statement
/// region reads through this value; one anywhere else is local to its region. A protected name is
/// refused either way; a name the system binds in a frame (`this`, `value`, ...) is refused only
/// of a `LET` local to a region, and not where the planner wrote the `LET`. Where the `LET` is
/// planned with its top-level statement the refusal is the whole statement's, raised before any
/// of it runs, whether or not the `LET` is reached; where it follows a statement that may change
/// the catalog it is planned when its list reaches it, so the raise stands at the `LET` and is
/// not bound at all when the `LET` is never reached.
fn bind_let(cx: &mut BindCx<'_>, statement: &LogicalLet) -> Result<Flow, IrError> {
	bind_let_as(cx, statement, false)
}

/// The `LET` that `op` is when it is one, written bare or in parentheses.
fn let_of(op: &LogicalOp) -> Option<&LogicalLet> {
	if let LogicalOp::Let(statement) = op {
		return Some(statement);
	}
	if let LogicalOp::Eval(eval) = op
		&& let LogicalExpr::Subquery(inner) = &eval.expr
		&& let LogicalOp::Let(statement) = inner.as_ref()
	{
		return Some(statement);
	}
	None
}

/// A `LET` that is the whole value of a `RETURN` or of another `LET`: in code of the request it
/// binds a name local to its region and its value is `NONE`; in code the catalog stores it is a
/// `LET` written as an expression, raised where it stands when the clause reaches it.
fn bind_valued_let(cx: &mut BindCx<'_>, statement: &LogicalLet) -> Result<Flow, IrError> {
	if cx.in_stored_code() {
		cx.raise(RaiseKind::Refused(Box::new(Refusal::InvalidStatement {
			message: LET_IN_AN_EXPRESSION.to_owned(),
		})))?;
		return Ok(Flow::Diverged);
	}
	bind_let_as(cx, statement, true)
}

/// [`bind_let`], binding a name local to the region whatever region it stands in where `local`
/// says so.
fn bind_let_as(cx: &mut BindCx<'_>, statement: &LogicalLet, local: bool) -> Result<Flow, IrError> {
	// What the `LET` binds decides which names it refuses, so it is settled before the refusal: a
	// session binding is a request variable and refuses the protected names alone, a local one
	// stands in a frame and also refuses the names the system binds there.
	let binding = if !local && matches!(cx.kinds().last(), Some(RegionKind::Statement(_))) {
		LetBinding::Session
	} else {
		LetBinding::Local
	};
	if statement.is_refused(binding == LetBinding::Session) {
		let refusal = Refusal::ProtectedParam {
			name: statement.name.clone(),
		};
		if cx.eager() {
			cx.hoist_refusal(refusal.clone());
			return raise_refused(cx, refusal);
		}
		cx.raise(RaiseKind::Refused(Box::new(refusal)))?;
		return Ok(Flow::Diverged);
	}
	// The value of a block's `LET` that is a statement runs as a group of its own. A `RETURN` that
	// is the value of a block-local `LET` is bound as a parenthesised subquery, which is no
	// context: the `RETURN` ends the context the block stands in, as it ends the statement of a
	// session `LET`.
	let flow = match let_of(&statement.value) {
		Some(valued) => bind_valued_let(cx, valued)?,
		None if binding == LetBinding::Local
			&& matches!(*statement.value, LogicalOp::Return(_)) =>
		{
			match bind_subquery(cx, &statement.value)? {
				Built::Value(value) => Flow::Value(value),
				Built::Diverged => Flow::Diverged,
			}
		}
		None => match binding {
			LetBinding::Local => bind_listed(cx, &statement.value, Pos::Value)?,
			LetBinding::Session => bind_op(cx, &statement.value, Pos::Value)?,
		},
	};
	let Some(value) = value_of(cx, flow)? else {
		return Ok(Flow::Diverged);
	};
	let name = Strand::new(&statement.name);
	let bound = cx.emit_value(
		OpKind::Logical(LogOp::Let(Box::new(LetAttrs {
			name: name.clone(),
			binding,
			ty: statement.kind.clone(),
		}))),
		&[value],
		TypeKnowledge::Unknown,
	)?;
	if binding == LetBinding::Local {
		cx.declare_variable(name, bound);
	}
	Ok(Flow::None)
}

/// Binds the `expr.raise` of `refusal` and records the refusal.
pub(super) fn raise_refused(cx: &mut BindCx<'_>, refusal: Refusal) -> Result<Flow, IrError> {
	cx.note_refusal(refusal.clone());
	cx.raise(RaiseKind::Refused(Box::new(refusal)))?;
	Ok(Flow::Diverged)
}

/// Refuses a protected name (`$auth`, `$access`, `$token`, `$session`) bound as
/// a `FOR` variable or a closure parameter, the way [`bind_let_as`] refuses one
/// a `LET` names. Answers `Some` with the diverging flow when `name` is
/// protected and so refused, and `None` when it is free to bind.
///
/// SECURITY: a permission predicate reads the session's value of a protected
/// name from the frame it runs in, so a construct that bound its own would hand
/// the predicate a caller-chosen identity (GHSA-ffvm-mqh3-7986). These names are
/// refused by `LET`, `UNSET` and the `set` RPCs; the constructs that bind a
/// user-chosen name must refuse them too.
fn refuse_protected_binding(cx: &mut BindCx<'_>, name: &str) -> Result<Option<Flow>, IrError> {
	if !surrealdb_cnf::PROTECTED_PARAM_NAMES.contains(&name) {
		return Ok(None);
	}
	let refusal = Refusal::ProtectedParam {
		name: name.to_owned(),
	};
	if cx.eager() {
		cx.hoist_refusal(refusal.clone());
		return raise_refused(cx, refusal).map(Some);
	}
	cx.raise(RaiseKind::Refused(Box::new(refusal)))?;
	Ok(Some(Flow::Diverged))
}

/// The answer for `owner`, an op whose region held a statement list that bound `refusal`: the op
/// stands when none was bound, and is replaced by the raise of the refusal otherwise.
pub(super) fn owner_flow(
	cx: &mut BindCx<'_>,
	owner: OpId,
	refusal: Option<Refusal>,
) -> Result<Flow, IrError> {
	match refusal {
		None => first_result(cx, owner).map(Flow::Value),
		Some(refusal) => {
			cx.erase_unreachable(owner)?;
			raise_refused(cx, refusal)
		}
	}
}

/// `{ ... }`: a block whose value is its last statement's.
fn bind_sequence(
	cx: &mut BindCx<'_>,
	sequence: &LogicalSequence,
	pos: Pos,
) -> Result<Flow, IrError> {
	let block = cx.emit(OpKind::Logical(LogOp::Block), &[], &[TypeKnowledge::Unknown])?;
	let refusal = cx.within(block, pos.block(), &[], |cx, _| {
		bind_list(cx, &sequence.stmts, Pos::Statement, Tail::Value)
	})?;
	owner_flow(cx, block, refusal)
}

/// `FOR`: a loop over the value of its range, whose body is the statement list of the block and
/// binds the parameter to the element.
fn bind_foreach(cx: &mut BindCx<'_>, foreach: &LogicalForeach, pos: Pos) -> Result<Flow, IrError> {
	// SECURITY: a `FOR` variable may not take a protected name. `LET`, `UNSET`
	// and the `set` RPCs refuse these names, but the constructs that bind a
	// user-chosen name once bound them unchecked, and a permission predicate
	// reads the session's value of a protected name from the frame it runs in,
	// so a loop binding `$auth`, `$access`, `$token` or `$session` would hand the
	// predicate a caller-chosen identity (GHSA-ffvm-mqh3-7986).
	if let Some(refused) = refuse_protected_binding(cx, foreach.param.as_str())? {
		return Ok(refused);
	}
	let Built::Value(source) =
		bind_instant(cx, &foreach.range, |cx| bind_expr(cx, &foreach.range))?
	else {
		return Ok(Flow::Diverged);
	};
	let sink = LoopSink::Discard;
	let entry = vec![TypeKnowledge::Unknown; loop_body_args(&sink)];
	let op = cx.emit(
		OpKind::Logical(LogOp::Loop {
			kind: LoopKind::For,
			sink,
			order: SourceOrder::Positional,
			step: Some(Box::new(StepAttrs::Param(Strand::new(foreach.param.as_str())))),
		}),
		&[source],
		&[TypeKnowledge::Unknown],
	)?;
	let refusal = cx.within(op, pos.foreach(), &entry, |cx, args| {
		let element = args.first().copied().ok_or(IrError::Stale("loop element"))?;
		cx.declare_variable(Strand::new(foreach.param.as_str()), element);
		bind_list(cx, statements_of(&foreach.body), Pos::Statement, Tail::Discard)
	})?;
	owner_flow(cx, op, refusal)
}

/// `IF`: the first condition as an operand, the arm of it in the first region and, in the second,
/// the rest of the chain, the `ELSE` arm or `NONE`. An `IF` that holds only its `ELSE` arm, the
/// conditions having been settled, is the `IF true` of that arm: the arm keeps the region of a
/// branch, which passes a `RETURN` outwards, where a block would catch it.
fn bind_if(cx: &mut BindCx<'_>, branch: &LogicalIfOp, pos: Pos) -> Result<Flow, IrError> {
	let listed = cx.in_statement_list();
	if branch.conditions.is_empty() && branch.arms.len() == 1 {
		let settled = [LogicalExpr::Literal(crate::expr::logical::LogicalLiteral::Bool(true))];
		return bind_if_chain(cx, &settled, &branch.arms, pos, listed);
	}
	bind_if_chain(cx, &branch.conditions, &branch.arms, pos, listed)
}

/// The chain of `IF` and `ELSE IF` arms that starts at `conditions[0]`, whose arms are `arms`,
/// the `ELSE` arm last when there is one. A condition after the first is bound in the else region
/// of the one before it, so it is evaluated only when reached. `listed` is whether the `IF` stands
/// in a statement list, in which case each condition that may read the clock starts at an instant
/// of its own.
fn bind_if_chain(
	cx: &mut BindCx<'_>,
	conditions: &[LogicalExpr],
	arms: &[LogicalOp],
	pos: Pos,
	listed: bool,
) -> Result<Flow, IrError> {
	let Some(first) = conditions.first() else {
		return Err(invariant("an IF holds no condition"));
	};
	let Some(then) = arms.first() else {
		return Err(invariant("an IF condition holds no arm"));
	};
	let Built::Value(condition) = bind_instant_in(cx, listed, first, |cx| bind_expr(cx, first))?
	else {
		return Ok(Flow::Diverged);
	};
	let op = cx.emit(OpKind::Logical(LogOp::If), &[condition], &[TypeKnowledge::Unknown])?;
	let then_refusal = cx.within(op, pos.branch(), &[], |cx, _| bind_arm(cx, then, pos))?;
	let else_refusal = cx.within(op, pos.branch(), &[], |cx, _| {
		if conditions.len() > 1 {
			let flow = bind_if_chain(cx, &conditions[1..], &arms[1..], pos, listed)?;
			if let Some(value) = value_of(cx, flow)? {
				cx.yield_logical(&[value])?;
			} else if !cx.ends_in_terminator()? {
				cx.yield_logical(&[])?;
			}
			Ok(None)
		} else if let Some(otherwise) = arms.get(conditions.len()) {
			bind_arm(cx, otherwise, pos)
		} else {
			let none = cx.none_literal()?;
			cx.yield_logical(&[none])?;
			Ok(None)
		}
	})?;
	owner_flow(cx, op, then_refusal.or(else_refusal))
}

/// An arm of an `IF`: the statement list of its region, whether it is written as a block or as a
/// single expression.
fn bind_arm(cx: &mut BindCx<'_>, arm: &LogicalOp, pos: Pos) -> Result<Option<Refusal>, IrError> {
	bind_list(cx, statements_of(arm), pos, Tail::Value)
}

/// The statement list that `body` is: the statements of a block, or `body` alone.
pub(super) fn statements_of(body: &LogicalOp) -> &[LogicalOp] {
	if let LogicalOp::Sequence(sequence) = body {
		sequence.stmts.as_slice()
	} else {
		slice::from_ref(body)
	}
}

/// Binds `statements`, each standing at `pos`, as the statement list of the current region and
/// closes the region as `tail` says. Answers the refusal bound among the statements that follow a
/// terminator, if any.
///
/// A statement is planned eagerly when the list is and no statement before it may change the
/// catalog ([`BindCx::changes_the_catalog_for_later`]); the first such statement itself still is.
pub(super) fn bind_list(
	cx: &mut BindCx<'_>,
	statements: &[LogicalOp],
	pos: Pos,
	tail: Tail,
) -> Result<Option<Refusal>, IrError> {
	let outer = cx.eager();
	let first_change =
		statements.iter().position(|statement| cx.changes_the_catalog_for_later(statement));
	let eager_at = |at: usize| outer && first_change.is_none_or(|first| at <= first);
	let mut last = Flow::None;
	for (at, statement) in statements.iter().enumerate() {
		cx.set_eager(eager_at(at));
		last = bind_listed(cx, statement, pos)?;
		if last == Flow::Diverged {
			// A statement that diverges inside a region of its own (a group whose source breaks
			// out of the loop around it) ends the list when it is reached, and leaves the list's
			// block open: the list still closes with its own terminator, which is never reached.
			let open = !cx.ends_in_terminator()?;
			let refusal = bind_unreachable(cx, statements, at + 1, pos, &eager_at);
			cx.set_eager(outer);
			if open && matches!(refusal, Ok(None)) {
				close_list(cx, Flow::None, tail)?;
			}
			return refusal;
		}
	}
	cx.set_eager(outer);
	close_list(cx, last, tail)?;
	Ok(None)
}

/// Closes the current statement list as `tail` says, with the value `last` left.
fn close_list(cx: &mut BindCx<'_>, last: Flow, tail: Tail) -> Result<(), IrError> {
	match tail {
		Tail::Value => {
			if let Some(value) = value_of(cx, last)? {
				cx.yield_logical(&[value])?;
			}
		}
		Tail::Discard => cx.yield_logical(&[])?,
	}
	Ok(())
}

/// Binds the statements of `statements` from `from` on, which follow a terminator of their list.
/// They never run, but a refusal among them is the error of the list's owner, which the caller
/// raises in its place; a refusal the planner raises for the whole top-level statement is the
/// statement's, wherever it stands. Without one, the ops they built are erased, last first, as
/// unreachable.
fn bind_unreachable(
	cx: &mut BindCx<'_>,
	statements: &[LogicalOp],
	from: usize,
	pos: Pos,
	eager_at: &dyn Fn(usize) -> bool,
) -> Result<Option<Refusal>, IrError> {
	if from >= statements.len() {
		return Ok(None);
	}
	let before = cx.block_ops()?.len();
	let outer = cx.take_refusal();
	let bound = statements.iter().enumerate().skip(from).try_for_each(|(at, statement)| {
		cx.set_eager(eager_at(at));
		bind_listed(cx, statement, pos).map(drop)
	});
	let refusal = cx.take_refusal();
	cx.restore_refusal(outer);
	bound?;
	if refusal.is_some() {
		return Ok(refusal);
	}
	erase_bound(cx, before)?;
	Ok(None)
}

/// Erases the ops the current block holds from the `from`th on, last first, as unreachable.
fn erase_bound(cx: &mut BindCx<'_>, from: usize) -> Result<(), IrError> {
	let built: Vec<OpId> = cx.block_ops()?.get(from..).ok_or(IrError::Stale("block"))?.to_vec();
	for op in built.into_iter().rev() {
		cx.erase_unreachable(op)?;
	}
	Ok(())
}

// ---------------------------------------------------------------------------------------------
// Relational plans
// ---------------------------------------------------------------------------------------------

/// A table's `PERMISSIONS FOR select` conjunct: the predicate, the frame it is judged in and the
/// limit of the authority it runs as.
pub(crate) struct PermissionConjunct {
	pub(crate) guard: GuardKind,
	pub(crate) auth: Option<AuthLimit>,
	pub(crate) predicate: LogicalExpr,
}

/// The conjuncts of the permissions the catalog holds for `table`, in the order they apply, to be
/// bound as permission filters over the table's rows; `None` when no permission of the table is
/// readable at the catalog version the request is bound to.
///
/// A permission present in the logical plan is bound from the plan and never asked for here.
/// `SemanticCatalog` offers no lookup, so every table answers `None`.
pub(crate) fn permissions_of(
	catalog: &dyn SemanticCatalog,
	table: &TableName,
) -> Option<Vec<PermissionConjunct>> {
	let _ = (catalog, table);
	None
}

/// A stored definition whose body may be bound in place of a call of it.
#[allow(dead_code)]
pub(crate) enum DefinitionRef<'a> {
	/// A `DEFINE FUNCTION`.
	Function(&'a str),
	/// A `COMPUTED` field.
	Computed {
		table: &'a TableName,
		field: &'a str,
	},
	/// A field's `VALUE`, `DEFAULT` or `ASSERT` clause.
	FieldClause {
		table: &'a TableName,
		field: &'a str,
		role: FieldRole,
	},
	/// A `DEFINE EVENT`.
	Event {
		table: &'a TableName,
		name: &'a str,
	},
}

/// The body of a definition, bound in place of a call of it. No body is ever answered, so no value
/// of the type exists.
#[allow(dead_code)]
pub(crate) enum InlinedBody {}

/// The body of `definition` to bind in place of a call of it, or `None` when the call stays a call
/// of the definition's compiled unit.
///
/// A definition is inlined only in the first transaction of the request and before any statement
/// that may change the catalog; anywhere else the call stays, because the definition read at
/// plan time may differ from the one the call runs. Definitions are read through the per-planning
/// memo and never from a standing cache. `SemanticCatalog` offers no lookup, so every definition
/// answers `None`.
#[allow(dead_code)]
pub(crate) fn inline_of(
	catalog: &dyn SemanticCatalog,
	definition: &DefinitionRef<'_>,
) -> Option<InlinedBody> {
	let _ = (catalog, definition);
	None
}

/// Whether `op` is a node of a relational pipeline, which takes its rows from the node below.
fn is_pipeline_node(op: &LogicalOp) -> bool {
	per_row_input(op).is_some()
		|| match op {
			LogicalOp::Aggregate(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Window(_)
			| LogicalOp::Sort(_)
			| LogicalOp::Limit(_)
			| LogicalOp::Timeout(_)
			| LogicalOp::VersionScope(_)
			| LogicalOp::Only(_)
			| LogicalOp::Explain(_) => true,
			LogicalOp::Source(_)
			| LogicalOp::Values(_)
			| LogicalOp::Match(_)
			| LogicalOp::Subquery(_)
			| LogicalOp::Write(_)
			| LogicalOp::Ddl(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Join(_)
			| LogicalOp::Filter(_)
			| LogicalOp::PermissionFilter(_)
			| LogicalOp::Split(_)
			| LogicalOp::Project(_)
			| LogicalOp::Fetch(_)
			| LogicalOp::Eval(_)
			| LogicalOp::Sequence(_)
			| LogicalOp::Let(_)
			| LogicalOp::Return(_)
			| LogicalOp::Foreach(_)
			| LogicalOp::If(_)
			| LogicalOp::Throw(_)
			| LogicalOp::Break(_)
			| LogicalOp::Continue(_)
			| LogicalOp::Info(_)
			| LogicalOp::Sleep(_) => false,
		}
}

/// The input of `op` when it is a per-row node: one that judges or reshapes each row on its own.
fn per_row_input(op: &LogicalOp) -> Option<&LogicalOp> {
	match op {
		LogicalOp::Filter(node) => Some(&node.input),
		LogicalOp::PermissionFilter(node) => Some(&node.input),
		LogicalOp::Split(node) => Some(&node.input),
		LogicalOp::Project(node) => Some(&node.input),
		LogicalOp::Fetch(node) => Some(&node.input),
		LogicalOp::Source(_)
		| LogicalOp::Values(_)
		| LogicalOp::Match(_)
		| LogicalOp::Subquery(_)
		| LogicalOp::Write(_)
		| LogicalOp::Ddl(_)
		| LogicalOp::UnionAll(_)
		| LogicalOp::Join(_)
		| LogicalOp::Aggregate(_)
		| LogicalOp::Distinct(_)
		| LogicalOp::Window(_)
		| LogicalOp::Sort(_)
		| LogicalOp::Limit(_)
		| LogicalOp::Timeout(_)
		| LogicalOp::VersionScope(_)
		| LogicalOp::Only(_)
		| LogicalOp::Explain(_)
		| LogicalOp::Eval(_)
		| LogicalOp::Sequence(_)
		| LogicalOp::Let(_)
		| LogicalOp::Return(_)
		| LogicalOp::Foreach(_)
		| LogicalOp::If(_)
		| LogicalOp::Throw(_)
		| LogicalOp::Break(_)
		| LogicalOp::Continue(_)
		| LogicalOp::Info(_)
		| LogicalOp::Sleep(_) => None,
	}
}

/// Binds the relational node `op` and the plan below it, and answers the value of the rows it
/// produces.
fn bind_relational(cx: &mut BindCx<'_>, op: &LogicalOp) -> Result<Flow, IrError> {
	if let Some(rebuilt) = value_split_over_source(op) {
		return bind_row_run(cx, &rebuilt);
	}
	if let LogicalOp::Project(project) = op
		&& let Some(split) = split_after_projection(project)
	{
		return bind_relational(cx, &split);
	}
	// A `FETCH` over the projection reads the rows the split delivers.
	if let LogicalOp::Fetch(fetch) = op
		&& let LogicalOp::Project(project) = &*fetch.input
		&& let Some(split) = split_after_projection(project)
	{
		let fetched = LogicalOp::Fetch(LogicalFetch {
			input: Box::new(split),
			fetches: fetch.fetches.clone(),
		});
		return bind_relational(cx, &fetched);
	}
	match op {
		LogicalOp::Filter(_)
		| LogicalOp::PermissionFilter(_)
		| LogicalOp::Split(_)
		| LogicalOp::Project(_)
		| LogicalOp::Fetch(_) => bind_row_run(cx, op),
		LogicalOp::Aggregate(aggregate) => bind_aggregate(cx, aggregate),
		LogicalOp::Distinct(distinct) => bind_rows_op(cx, &distinct.input, LogOp::Distinct),
		LogicalOp::Window(window) => bind_window(cx, window),
		LogicalOp::Sort(sort) => bind_sort(cx, sort),
		LogicalOp::Limit(limit) => bind_limit(cx, limit),
		LogicalOp::Timeout(timeout) => {
			let Built::Value(duration) = bind_expr(cx, &timeout.timeout)? else {
				return Ok(Flow::Diverged);
			};
			let owner =
				cx.emit(OpKind::Logical(LogOp::Timeout), &[duration], &[TypeKnowledge::Unknown])?;
			cx.within(owner, RegionKind::Deadline, &[], |cx, _| {
				if let Some(value) = bind_value(cx, &timeout.input)? {
					cx.yield_logical(&[value])?;
				} else if !cx.ends_in_terminator()? {
					cx.yield_logical(&[])?;
				}
				Ok(())
			})?;
			first_result(cx, owner).map(Flow::Value)
		}
		LogicalOp::VersionScope(scope) => {
			if let Some(refusal) = version_over_statement_refusal(&scope.input) {
				if cx.eager() {
					cx.hoist_refusal(refusal.clone());
					return raise_refused(cx, refusal);
				}
				cx.raise(RaiseKind::Refused(Box::new(refusal)))?;
				return Ok(Flow::Diverged);
			}
			let Built::Value(version) = bind_expr(cx, &scope.version)? else {
				return Ok(Flow::Diverged);
			};
			let Some(input) = cx.with_version(version, |cx| bind_value(cx, &scope.input))? else {
				return Ok(Flow::Diverged);
			};
			cx.emit_value(
				OpKind::Logical(LogOp::VersionScope),
				&[input, version],
				TypeKnowledge::Unknown,
			)
			.map(Flow::Value)
		}
		LogicalOp::Only(only) => bind_wrapper(cx, &only.input, LogOp::Only),
		LogicalOp::Explain(explain) => {
			// The plan an `EXPLAIN` prints states each clause as written, so the statement it
			// explains is bound with the text of its expressions recorded.
			let Some(input) = cx.with_writing(|cx| bind_explained(cx, &explain.input))? else {
				return Ok(Flow::Diverged);
			};
			cx.emit_value(
				OpKind::Logical(LogOp::Explain {
					analyze: explain.analyze,
					format: explain.format,
				}),
				&[input],
				TypeKnowledge::Unknown,
			)
			.map(Flow::Value)
		}
		LogicalOp::Source(source) => bind_source(cx, source),
		LogicalOp::Values(values) => bind_values(cx, values),
		LogicalOp::Subquery(subquery) => bind_relational_subquery(cx, subquery),
		LogicalOp::UnionAll(union) => {
			if union.inputs.is_empty() {
				return Err(invariant("a union holds no input"));
			}
			let mut inputs = Vec::with_capacity(union.inputs.len());
			for input in &union.inputs {
				let Some(value) = bind_value(cx, input)? else {
					return Ok(Flow::Diverged);
				};
				inputs.push(value);
			}
			cx.emit_value(OpKind::Logical(LogOp::UnionAll), &inputs, TypeKnowledge::Unknown)
				.map(Flow::Value)
		}
		LogicalOp::Join(join) => bind_join(cx, join),
		LogicalOp::Match(_)
		| LogicalOp::Write(_)
		| LogicalOp::Ddl(_)
		| LogicalOp::Eval(_)
		| LogicalOp::Sequence(_)
		| LogicalOp::Let(_)
		| LogicalOp::Return(_)
		| LogicalOp::Foreach(_)
		| LogicalOp::If(_)
		| LogicalOp::Throw(_)
		| LogicalOp::Break(_)
		| LogicalOp::Continue(_)
		| LogicalOp::Info(_)
		| LogicalOp::Sleep(_) => Err(invariant("a statement that is not a relational node")),
	}
}

/// The plan `project` states with its `SPLIT` taken over the projected rows, or `None` where no
/// `SPLIT` stands beneath the projection's ordering and paging.
///
/// `SPLIT` names a field of the row the statement returns, so it splits the projected row: the
/// projection (with its `OMIT`) reads the rows the split's input delivers, the split reads what
/// the projection produced under the names it gave, and the ordering and paging read the rows the
/// split delivers.
fn split_after_projection(project: &LogicalProject) -> Option<LogicalOp> {
	// A `VALUE` projection yields bare values, which have no field for a split to name. Its split
	// stays beneath the projection; see `value_split_over_source`.
	if matches!(project.fields, LogicalFields::Value { .. }) {
		return None;
	}
	let mut sorts = Vec::new();
	let mut node = &*project.input;
	let split = loop {
		if let LogicalOp::Sort(sort) = node {
			sorts.push(Ok(sort));
			node = &sort.input;
		} else if let LogicalOp::Limit(limit) = node {
			sorts.push(Err(limit));
			node = &limit.input;
		} else if let LogicalOp::Split(split) = node {
			break split;
		} else {
			return None;
		}
	};
	let mut op = LogicalOp::Split(LogicalSplit {
		input: Box::new(LogicalOp::Project(LogicalProject {
			input: split.input.clone(),
			fields: project.fields.clone(),
			omit: project.omit.clone(),
		})),
		exprs: split.exprs.clone(),
	});
	// Each ordering or paging, innermost first, over the rows the split delivers.
	for above in sorts.into_iter().rev() {
		op = match above {
			Ok(sort) => LogicalOp::Sort(LogicalSort {
				input: Box::new(op),
				ordering: sort.ordering.clone(),
				tempfiles: sort.tempfiles,
			}),
			Err(limit) => LogicalOp::Limit(LogicalLimit {
				input: Box::new(op),
				limit: limit.limit.clone(),
				start: limit.start.clone(),
			}),
		};
	}
	Some(op)
}

/// The plan of a `VALUE` projection whose `SPLIT` stands beneath its ordering and paging, rebuilt
/// so that the split reads the expression the projection yields from each record below it.
///
/// The parser admits a `SPLIT` target only when it names that expression, so splitting the record
/// on it gives one record per element, and the projection then yields each element as the value.
/// The ordering and paging read those records, as they do for a projection with no split. `op` is
/// the projection or a fetch over it. `None` when no split stands beneath the projection's
/// ordering and paging, or when `op` is not such a projection.
fn value_split_over_source(op: &LogicalOp) -> Option<LogicalOp> {
	fn output_path_expr(path: &OutputPath) -> LogicalExpr {
		let mut parts = path.parts().iter();
		let Some(first) = parts.next() else {
			return LogicalExpr::Current;
		};
		let first_name = match first {
			OutputPathPart::Field(name) | OutputPathPart::Lookup(name) => Strand::new(name),
		};
		let mut expr = LogicalExpr::Field(first_name);
		for part in parts {
			let field = match part {
				OutputPathPart::Field(name) | OutputPathPart::Lookup(name) => Strand::new(name),
			};
			expr = LogicalExpr::Access {
				base: Box::new(expr),
				field,
			};
		}
		expr
	}
	fn rebuild(
		node: &LogicalOp,
		split_expr: &LogicalExpr,
		pre_project: Option<(&LogicalExpr, &OutputPath)>,
	) -> Option<LogicalOp> {
		match node {
			LogicalOp::Sort(sort) => Some(LogicalOp::Sort(LogicalSort {
				input: Box::new(rebuild(&sort.input, split_expr, pre_project)?),
				ordering: sort.ordering.clone(),
				tempfiles: sort.tempfiles,
			})),
			LogicalOp::Limit(limit) => Some(LogicalOp::Limit(LogicalLimit {
				input: Box::new(rebuild(&limit.input, split_expr, pre_project)?),
				limit: limit.limit.clone(),
				start: limit.start.clone(),
			})),
			LogicalOp::Split(split) => {
				let input = match pre_project {
					Some((expr, path)) => Box::new(LogicalOp::Project(LogicalProject {
						input: split.input.clone(),
						fields: LogicalFields::Select(vec![LogicalField::user(
							expr.clone(),
							Some(path.clone()),
						)]),
						omit: Vec::new(),
					})),
					None => split.input.clone(),
				};
				Some(LogicalOp::Split(LogicalSplit {
					input,
					exprs: vec![split_expr.clone()],
				}))
			}
			LogicalOp::Project(_)
			| LogicalOp::Fetch(_)
			| LogicalOp::Source(_)
			| LogicalOp::Values(_)
			| LogicalOp::Match(_)
			| LogicalOp::Subquery(_)
			| LogicalOp::Write(_)
			| LogicalOp::Ddl(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Join(_)
			| LogicalOp::Filter(_)
			| LogicalOp::PermissionFilter(_)
			| LogicalOp::Aggregate(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Window(_)
			| LogicalOp::Timeout(_)
			| LogicalOp::VersionScope(_)
			| LogicalOp::Only(_)
			| LogicalOp::Explain(_)
			| LogicalOp::Eval(_)
			| LogicalOp::Sequence(_)
			| LogicalOp::Let(_)
			| LogicalOp::Return(_)
			| LogicalOp::Foreach(_)
			| LogicalOp::If(_)
			| LogicalOp::Throw(_)
			| LogicalOp::Break(_)
			| LogicalOp::Continue(_)
			| LogicalOp::Info(_)
			| LogicalOp::Sleep(_) => None,
		}
	}
	let project_over = |project: &LogicalProject| {
		let LogicalFields::Value {
			expr,
			alias,
		} = &project.fields
		else {
			return None;
		};
		if matches!(split_target(expr), Some(SplitTarget::At(_))) {
			Some(LogicalProject {
				input: Box::new(rebuild(&project.input, expr, None)?),
				fields: project.fields.clone(),
				omit: project.omit.clone(),
			})
		} else {
			let raw_parts = alias
				.as_ref()
				.map(|path| path.parts.clone())
				.unwrap_or_else(|| expr.named_after_gates().output_path().parts);
			let target_path = if raw_parts.is_empty() {
				OutputPath::field("value")
			} else {
				OutputPath::keys(raw_parts)
			};
			let target_expr = output_path_expr(&target_path);
			Some(LogicalProject {
				input: Box::new(rebuild(&project.input, &target_expr, Some((expr, &target_path)))?),
				fields: LogicalFields::Value {
					expr: target_expr,
					alias: alias.clone(),
				},
				omit: project.omit.clone(),
			})
		}
	};
	match op {
		LogicalOp::Project(project) => project_over(project).map(LogicalOp::Project),
		LogicalOp::Fetch(fetch) => match &*fetch.input {
			LogicalOp::Project(project) => Some(LogicalOp::Fetch(LogicalFetch {
				input: Box::new(LogicalOp::Project(project_over(project)?)),
				fetches: fetch.fetches.clone(),
			})),
			LogicalOp::Source(_)
			| LogicalOp::Values(_)
			| LogicalOp::Match(_)
			| LogicalOp::Subquery(_)
			| LogicalOp::Write(_)
			| LogicalOp::Ddl(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Join(_)
			| LogicalOp::Split(_)
			| LogicalOp::Filter(_)
			| LogicalOp::PermissionFilter(_)
			| LogicalOp::Aggregate(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Window(_)
			| LogicalOp::Sort(_)
			| LogicalOp::Limit(_)
			| LogicalOp::Fetch(_)
			| LogicalOp::Timeout(_)
			| LogicalOp::VersionScope(_)
			| LogicalOp::Only(_)
			| LogicalOp::Explain(_)
			| LogicalOp::Eval(_)
			| LogicalOp::Sequence(_)
			| LogicalOp::Let(_)
			| LogicalOp::Return(_)
			| LogicalOp::Foreach(_)
			| LogicalOp::If(_)
			| LogicalOp::Throw(_)
			| LogicalOp::Break(_)
			| LogicalOp::Continue(_)
			| LogicalOp::Info(_)
			| LogicalOp::Sleep(_) => None,
		},
		LogicalOp::Source(_)
		| LogicalOp::Values(_)
		| LogicalOp::Match(_)
		| LogicalOp::Subquery(_)
		| LogicalOp::Write(_)
		| LogicalOp::Ddl(_)
		| LogicalOp::UnionAll(_)
		| LogicalOp::Join(_)
		| LogicalOp::Split(_)
		| LogicalOp::Filter(_)
		| LogicalOp::PermissionFilter(_)
		| LogicalOp::Aggregate(_)
		| LogicalOp::Distinct(_)
		| LogicalOp::Window(_)
		| LogicalOp::Sort(_)
		| LogicalOp::Limit(_)
		| LogicalOp::Timeout(_)
		| LogicalOp::VersionScope(_)
		| LogicalOp::Only(_)
		| LogicalOp::Explain(_)
		| LogicalOp::Eval(_)
		| LogicalOp::Sequence(_)
		| LogicalOp::Let(_)
		| LogicalOp::Return(_)
		| LogicalOp::Foreach(_)
		| LogicalOp::If(_)
		| LogicalOp::Throw(_)
		| LogicalOp::Break(_)
		| LogicalOp::Continue(_)
		| LogicalOp::Info(_)
		| LogicalOp::Sleep(_) => None,
	}
}

/// Binds `op` in value position and answers its value; `None` when it ended its block.
pub(super) fn bind_value(cx: &mut BindCx<'_>, op: &LogicalOp) -> Result<Option<ValueId>, IrError> {
	let flow = bind_op(cx, op, Pos::Value)?;
	value_of(cx, flow)
}

/// Binds `op`, the input of a node that needs every row, and answers the value of its rows. An
/// input that is not a node of a pipeline is read through a loop that delivers each row unchanged.
fn bind_rows(cx: &mut BindCx<'_>, op: &LogicalOp) -> Result<Option<ValueId>, IrError> {
	let Some(value) = bind_value(cx, op)? else {
		return Ok(None);
	};
	if is_pipeline_node(op) {
		return Ok(Some(value));
	}
	select_loop_over(cx, value, Some(op), |_, row| Ok(row)).map(Some)
}

/// A node of one operand and no region, over the rows of `input`, which it needs all of.
fn bind_rows_op(cx: &mut BindCx<'_>, input: &LogicalOp, kind: LogOp) -> Result<Flow, IrError> {
	let Some(rows) = bind_rows(cx, input)? else {
		return Ok(Flow::Diverged);
	};
	cx.emit_value(OpKind::Logical(kind), &[rows], TypeKnowledge::Unknown).map(Flow::Value)
}

/// A node of one operand and no region that reshapes the result of the plan `input`.
fn bind_wrapper(cx: &mut BindCx<'_>, input: &LogicalOp, kind: LogOp) -> Result<Flow, IrError> {
	let Some(value) = bind_value(cx, input)? else {
		return Ok(Flow::Diverged);
	};
	cx.emit_value(OpKind::Logical(kind), &[value], TypeKnowledge::Unknown).map(Flow::Value)
}

/// The `logical.loop` that reads `source` row by row and delivers the row `body` answers for the
/// element it is given. Answers the loop's output.
pub(super) fn select_loop(
	cx: &mut BindCx<'_>,
	source: ValueId,
	body: impl FnOnce(&mut BindCx<'_>, ValueId) -> Result<ValueId, IrError>,
) -> Result<ValueId, IrError> {
	select_loop_over(cx, source, None, body)
}

/// [`select_loop`] for the rows below `base` in a logical plan, which the loop is noted for
/// ([`BindCx::note_loop`]): the legality the plan states of the pipeline that reads `base` is the
/// legality of the loop.
pub(super) fn select_loop_over(
	cx: &mut BindCx<'_>,
	source: ValueId,
	base: Option<&LogicalOp>,
	body: impl FnOnce(&mut BindCx<'_>, ValueId) -> Result<ValueId, IrError>,
) -> Result<ValueId, IrError> {
	let sink = LoopSink::Output;
	let entry = vec![TypeKnowledge::Unknown; loop_body_args(&sink)];
	let op = cx.emit(
		OpKind::Logical(LogOp::Loop {
			kind: LoopKind::Select,
			sink,
			order: SourceOrder::Delivery,
			step: None,
		}),
		&[source],
		&[TypeKnowledge::Unknown],
	)?;
	if let Some(base) = base {
		cx.note_loop(op, base);
	}
	cx.within(op, RegionKind::Loop, &entry, |cx, args| {
		let element = args.first().copied().ok_or(IrError::Stale("loop element"))?;
		cx.declare_row_names(element, STATEMENT_ROW_NAMES);
		let row = body(cx, element)?;
		cx.yield_logical(&[row])
	})?;
	first_result(cx, op)
}

/// The per-row nodes that stand directly over one input, outermost first, bound as the body of a
/// loop over that input.
fn bind_row_run(cx: &mut BindCx<'_>, op: &LogicalOp) -> Result<Flow, IrError> {
	let mut run = Vec::new();
	let mut below = op;
	while let Some(input) = per_row_input(below) {
		run.push(below);
		below = input;
	}
	// The keys of the `ORDER BY` this projection stands over name its fields and aliases.
	let order_fields = match (run.last(), below) {
		(Some(LogicalOp::Project(project)), LogicalOp::Limit(_) | LogicalOp::Sort(_))
			if !project.is_layer() =>
		{
			Some(project.fields.clone())
		}
		_ => None,
	};
	if let Some(refusal) = knn_vector_refusal(cx, &run) {
		if cx.eager() {
			cx.hoist_refusal(refusal.clone());
			return raise_refused(cx, refusal);
		}
		cx.raise(RaiseKind::Refused(Box::new(refusal)))?;
		return Ok(Flow::Diverged);
	}
	cx.set_order_fields(order_fields);
	let source = bind_value(cx, below);
	cx.set_order_fields(None);
	let Some(source) = source? else {
		cx.take_sort_keys();
		return Ok(Flow::Diverged);
	};
	// A split is a breaker of the logical plan's pipelines: the loop holds the ops on both sides
	// of it, and the legality the plan states of the pipeline below the split does not cover the
	// ops after it.
	let base = (!run.iter().any(|node| matches!(node, LogicalOp::Split(_)))).then_some(below);
	// The projection directly above the sort is the first node the loop binds, and takes the keys
	// it shares with the sort; no other projection reads them.
	let rows = select_loop_over(cx, source, base, |cx, element| {
		run.iter().rev().try_fold(element, |row, node| bind_per_row(cx, node, row))
	});
	cx.take_sort_keys();
	Ok(Flow::Value(rows?))
}

/// What a `VERSION` clause beside a statement in `FROM` raises.
const SUBQUERY_SOURCE_VERSION: &str = "VERSION clause cannot be used with a subquery source. \
	 Place the VERSION clause inside the subquery instead.";

/// The refusal of a `VERSION` over `input`, the pipeline it scopes, when a statement is among the
/// pipeline's sources: a stamp is read at tables, and a statement in `FROM` is none, so the clause
/// belongs inside that statement. Raised before the stamp or any source is evaluated. The walk
/// descends the clauses of the statement's own pipeline; a source behind any other node is not
/// the statement's own and is not judged here.
fn version_over_statement_refusal(input: &LogicalOp) -> Option<Refusal> {
	let mut op = input;
	let leaf = loop {
		op = match op {
			LogicalOp::Project(node) if node.is_layer() => return None,
			LogicalOp::Timeout(node) => &node.input,
			LogicalOp::Fetch(node) => &node.input,
			LogicalOp::Project(node) => &node.input,
			LogicalOp::Limit(node) => &node.input,
			LogicalOp::Sort(node) => &node.input,
			LogicalOp::Aggregate(node) => &node.input,
			LogicalOp::Split(node) => &node.input,
			LogicalOp::Filter(node) => &node.input,
			LogicalOp::Source(_)
			| LogicalOp::Values(_)
			| LogicalOp::Match(_)
			| LogicalOp::Subquery(_)
			| LogicalOp::Write(_)
			| LogicalOp::Ddl(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Join(_)
			| LogicalOp::PermissionFilter(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Window(_)
			| LogicalOp::VersionScope(_)
			| LogicalOp::Only(_)
			| LogicalOp::Explain(_)
			| LogicalOp::Eval(_)
			| LogicalOp::Sequence(_)
			| LogicalOp::Let(_)
			| LogicalOp::Return(_)
			| LogicalOp::Foreach(_)
			| LogicalOp::If(_)
			| LogicalOp::Throw(_)
			| LogicalOp::Break(_)
			| LogicalOp::Continue(_)
			| LogicalOp::Info(_)
			| LogicalOp::Sleep(_) => break op,
		};
	};
	let holds_a_statement = matches!(leaf, LogicalOp::Subquery(_))
		|| matches!(leaf, LogicalOp::UnionAll(union)
			if union.inputs.iter().any(|input| matches!(input, LogicalOp::Subquery(_))));
	holds_a_statement.then(|| Refusal::Query {
		message: SUBQUERY_SOURCE_VERSION.to_owned(),
	})
}

/// The refusal of a `KNN` operator of the filters of `run`, a select loop's per-row nodes, whose
/// query vector has no value before a row is read, raised for the statement before any row is:
///
/// - more than one operator a search would serve (`<|k, DIST|>`, `<|k, EF|>`) across the filters: a
///   search answers one, and serving one while dropping the others would answer a different query;
/// - a query vector that reads the row being scanned ([`first_row_dependent_knn_vector`]), or that
///   reads `$parent` where nothing around the statement binds it, which falls back to that row
///   ([`reads_enclosing_parent`]).
///
/// A query vector that reads the row in a way its expression does not show is refused when it is
/// evaluated, with the same error ([`ForbiddenRow`](crate::exec::context::ForbiddenRow)).
fn knn_vector_refusal(cx: &BindCx<'_>, run: &[&LogicalOp]) -> Option<Refusal> {
	use crate::exec::planner::row_scope::reads_enclosing_parent;
	use crate::exec::planner::util::{
		count_knn_source_operators, first_row_dependent_knn_vector, has_knn_operator,
		knn_query_vectors, resolve_knn_field_operands,
	};
	let mut predicates = Vec::new();
	for node in run {
		if let LogicalOp::Filter(LogicalFilter {
			predicate,
			..
		}) = node && has_knn_operator(predicate)
		{
			let mut predicate = predicate.clone();
			resolve_knn_field_operands(&mut predicate, cx.registry());
			predicates.push(predicate);
		}
	}
	if predicates.is_empty() {
		return None;
	}
	let operators: usize = predicates.iter().map(count_knn_source_operators).sum();
	if operators > 1 {
		return Some(Refusal::Query {
			message: format!(
				"A WHERE clause supports a single KNN operator (<|k, …|>); found {operators}."
			),
		});
	}
	let parent_unbound = !cx.binds_parent();
	let vector = predicates
		.iter()
		.find_map(|predicate| first_row_dependent_knn_vector(predicate, cx.registry()))
		.or_else(|| {
			predicates
				.iter()
				.flat_map(knn_query_vectors)
				.find(|vector| parent_unbound && reads_enclosing_parent(vector))
		})?;
	Some(Refusal::Query {
		message: format!(
			"The KNN query vector cannot depend on the record being scanned: {}",
			vector.to_sql()
		),
	})
}

/// Binds the per-row node `node` over `row`, and answers the row it delivers.
fn bind_per_row(cx: &mut BindCx<'_>, node: &LogicalOp, row: ValueId) -> Result<ValueId, IrError> {
	match node {
		LogicalOp::Filter(LogicalFilter {
			predicate,
			..
		}) => {
			let op = cx.emit(OpKind::Logical(LogOp::Filter), &[row], &[TypeKnowledge::Unknown])?;
			row_clause(cx, op, WHERE_CLAUSE, predicate)?;
			first_result(cx, op)
		}
		LogicalOp::PermissionFilter(LogicalPermissionFilter {
			input,
			predicate,
			auth,
			guard,
		}) => {
			let computed = computed_reads_of(
				cx.registry(),
				cx.catalog(),
				permission_subject(input),
				predicate,
			);
			bind_permission_filter(cx, row, *guard, auth.as_ref(), predicate, computed)
		}
		LogicalOp::Split(split) => {
			let op = cx.emit(
				OpKind::Logical(LogOp::Split {
					targets: split_targets(&split.exprs)?,
				}),
				&[row],
				&[TypeKnowledge::Unknown],
			)?;
			for expr in &split.exprs {
				row_clause(cx, op, RegionKind::Clause(ClauseRole::Split), expr)?;
			}
			first_result(cx, op)
		}
		LogicalOp::Fetch(fetch) => bind_fetch(cx, row, &fetch.fetches),
		LogicalOp::Project(project) => bind_project(cx, project, row),
		LogicalOp::Source(_)
		| LogicalOp::Values(_)
		| LogicalOp::Match(_)
		| LogicalOp::Subquery(_)
		| LogicalOp::Write(_)
		| LogicalOp::Ddl(_)
		| LogicalOp::UnionAll(_)
		| LogicalOp::Join(_)
		| LogicalOp::Aggregate(_)
		| LogicalOp::Distinct(_)
		| LogicalOp::Window(_)
		| LogicalOp::Sort(_)
		| LogicalOp::Limit(_)
		| LogicalOp::Timeout(_)
		| LogicalOp::VersionScope(_)
		| LogicalOp::Only(_)
		| LogicalOp::Explain(_)
		| LogicalOp::Eval(_)
		| LogicalOp::Sequence(_)
		| LogicalOp::Let(_)
		| LogicalOp::Return(_)
		| LogicalOp::Foreach(_)
		| LogicalOp::If(_)
		| LogicalOp::Throw(_)
		| LogicalOp::Break(_)
		| LogicalOp::Continue(_)
		| LogicalOp::Info(_)
		| LogicalOp::Sleep(_) => Err(invariant("a loop body holds a node that is not per-row")),
	}
}

/// A row name a region binds, and the position of the block argument that holds its value after
/// the statement timestamp. Position 0 is the row the region is evaluated against.
type RowName = (&'static str, usize);

/// The names of the row itself.
const ROW_ONLY: &[RowName] = &[("this", 0), ("self", 0), ("value", 0)];
/// The names a clause of a statement gives the row it is evaluated against. `$value` is no name
/// of the row there: it reads the parameter of the frame around the statement.
const STATEMENT_ROW: &[RowName] = &[("this", 0), ("self", 0)];
/// `ON DUPLICATE KEY UPDATE`: the row, and the value the statement supplied for it.
const WITH_INPUT: &[RowName] = &[("this", 0), ("self", 0), ("input", 1)];
/// A `RETURN` projection of a write: the row is the record after the write; the record before it
/// is the second argument.
const WITH_IMAGES: &[RowName] = &[("this", 0), ("self", 0), ("after", 0), ("before", 1)];
/// A `VALUE`, `DEFAULT` or `ASSERT` clause of a field: the row is the record; `$value` and
/// `$after` are the value the clause is about, `$before` the place's old value and `$input` the
/// value the statement supplied.
const FIELD_VALUE: &[RowName] =
	&[("this", 0), ("self", 0), ("value", 1), ("after", 1), ("before", 2), ("input", 3)];
/// The `THEN` of a `REFERENCE` field's `ON DELETE` clause: the row is the id of the referencing
/// record, which `$value` names too, and `$reference` is the record the write purges.
const REPAIR: &[RowName] = &[("this", 0), ("self", 0), ("value", 0), ("reference", 1)];
/// The `WHEN` and `THEN` of an event: the row is the record the event is about, which `$value`
/// names too.
const EVENT_IMAGES: &[RowName] = &[
	("this", 0),
	("self", 0),
	("value", 0),
	("before", 1),
	("after", 2),
	("event", 3),
	("input", 4),
];

/// The row names a region of `kind` binds to its block arguments, and none for a region that
/// reads the names of the region around it.
///
/// A region isolated from above sees no name of its enclosing region, so it binds the names its
/// frame defines from its own arguments. A write's clause regions bind them too: the row they are
/// evaluated against is the record being written, and the element of the loop around them is the
/// target the statement names. A clause evaluated against each row of a read binds them as well:
/// lowering hands the clause the row as its argument, and the element of the loop the clause stands
/// in is no value of the clause.
fn region_row_names(kind: RegionKind) -> &'static [RowName] {
	match kind {
		RegionKind::Clause(role) => match role {
			ClauseRole::SetValue
			| ClauseRole::Content
			| ClauseRole::WriteCond
			| ClauseRole::IndexColumn
			| ClauseRole::ViewClause
			| ClauseRole::Where
			| ClauseRole::OrderKey
			| ClauseRole::GroupKey
			| ClauseRole::Split
			| ClauseRole::Projection => STATEMENT_ROW,
			ClauseRole::ReferenceRepair => REPAIR,
			ClauseRole::DuplicateUpdate => WITH_INPUT,
			ClauseRole::ReturnClause => WITH_IMAGES,
			ClauseRole::InsertValues
			| ClauseRole::From
			| ClauseRole::Limit
			| ClauseRole::Start
			| ClauseRole::Timeout => &[],
		},
		RegionKind::PermissionFrame => ROW_ONLY,
		RegionKind::EventFrame => EVENT_IMAGES,
		RegionKind::FieldFrame(role) => match role {
			FieldRole::Assert | FieldRole::Value | FieldRole::Default => FIELD_VALUE,
			FieldRole::Computed => ROW_ONLY,
		},
		RegionKind::StoredClause(role) => match role {
			StoredRole::Assert | StoredRole::Value | StoredRole::Default => FIELD_VALUE,
			StoredRole::Permissions
			| StoredRole::Computed
			| StoredRole::IndexColumn
			| StoredRole::LiveFields
			| StoredRole::LiveCond
			| StoredRole::LiveFetch => ROW_ONLY,
			StoredRole::When | StoredRole::Then => EVENT_IMAGES,
			StoredRole::Body
			| StoredRole::View
			| StoredRole::Authenticate
			| StoredRole::Context
			| StoredRole::Signup
			| StoredRole::Signin
			| StoredRole::Action
			| StoredRole::Fallback => &[],
		},
		RegionKind::Request
		| RegionKind::Transaction(_)
		| RegionKind::Statement(_)
		| RegionKind::Block
		| RegionKind::ValueBlock
		| RegionKind::StatementIf
		| RegionKind::ValueIf
		| RegionKind::StatementFor
		| RegionKind::ValueFor
		| RegionKind::Loop
		| RegionKind::Subquery
		| RegionKind::FunctionBody
		| RegionKind::ClosureBody
		| RegionKind::SubscriberFrame
		| RegionKind::LazyOperand
		| RegionKind::ErrorScope(_)
		| RegionKind::ChooseArm
		| RegionKind::Pipeline
		| RegionKind::JoinLookupArm
		| RegionKind::WriteStage
		| RegionKind::CatalogUnit
		| RegionKind::Deadline => &[],
	}
}

/// Whether a region of `kind` receives the row it is evaluated against, which it binds its row
/// names to.
pub(crate) fn receives_row(kind: RegionKind) -> bool {
	!region_row_names(kind).is_empty()
}

/// The arguments of the block of a region that is given the row it is evaluated against: the
/// statement timestamp when the region is isolated from above, the row, and one argument for each
/// further value the region's row names bind.
pub(crate) fn row_region_args(kind: RegionKind) -> Vec<TypeKnowledge> {
	named_region_args(kind, region_row_names(kind))
}

/// [`row_region_args`] for a region that binds the names `names` in place of those of its kind.
fn named_region_args(kind: RegionKind, names: &[RowName]) -> Vec<TypeKnowledge> {
	let extra = names.iter().map(|(_, at)| *at).max().unwrap_or(0);
	let mut args = Vec::with_capacity(2 + extra);
	if kind.isolated_from_above() {
		args.push(timestamp_arg());
	}
	args.extend(std::iter::repeat_n(TypeKnowledge::Unknown, 1 + extra));
	args
}

/// Enters a region of `kind` owned by `owner`, which receives the row it is evaluated against, and
/// runs `body` with that row after binding the region's row names to its arguments.
///
/// A region that does not inherit its enclosing names binds only the names its frame defines. One
/// that inherits them also binds `parent` to the row of the frame around it, when there is one.
pub(crate) fn enter_row_region<R>(
	cx: &mut BindCx<'_>,
	owner: OpId,
	kind: RegionKind,
	body: impl FnOnce(&mut BindCx<'_>, ValueId) -> Result<R, IrError>,
) -> Result<R, IrError> {
	enter_named_region(cx, owner, kind, region_row_names(kind), body)
}

/// [`enter_row_region`] for a region that binds the names `names` in place of those of its kind: a
/// field's permission predicate is a `PermissionFrame` region whose row is the record and whose
/// `$value`, `$before` and `$input` are the place's.
fn enter_named_region<R>(
	cx: &mut BindCx<'_>,
	owner: OpId,
	kind: RegionKind,
	names: &'static [RowName],
	body: impl FnOnce(&mut BindCx<'_>, ValueId) -> Result<R, IrError>,
) -> Result<R, IrError> {
	cx.within(owner, kind, &named_region_args(kind, names), |cx, entry| {
		let base = usize::from(kind.isolated_from_above());
		let row = entry.get(base).copied().ok_or(IrError::Stale("row"))?;
		for (name, at) in names {
			let value = entry.get(base + at).copied().ok_or(IrError::Stale("row name"))?;
			cx.declare(Strand::new(*name), value);
		}
		if !names.is_empty() && !kind.isolated_from_above() {
			cx.declare_parent();
		}
		body(cx, row)
	})
}

/// Binds `expr` as the body of a region of `kind` owned by `owner`, evaluated against the row the
/// region receives.
pub(super) fn row_clause(
	cx: &mut BindCx<'_>,
	owner: OpId,
	kind: RegionKind,
	expr: &LogicalExpr,
) -> Result<(), IrError> {
	enter_row_region(cx, owner, kind, |cx, row| cx.with_row(row, |cx| bind_body(cx, expr)))
}

/// A `FETCH` path of the fetch `owner`: a `Clause(Projection)` region evaluated against the row,
/// whose body [`bind_fetch_path`] binds.
pub(super) fn fetch_path_clause(
	cx: &mut BindCx<'_>,
	owner: OpId,
	path: &LogicalExpr,
) -> Result<(), IrError> {
	let kind = RegionKind::Clause(ClauseRole::Projection);
	enter_row_region(cx, owner, kind, |cx, row| cx.with_row(row, |cx| bind_fetch_path(cx, path)))
}

/// [`row_clause`], with every op of the region under the frame `frame` names; `None` is the frame
/// of the statement.
pub(super) fn framed_clause(
	cx: &mut BindCx<'_>,
	owner: OpId,
	kind: RegionKind,
	frame: Option<&EvalFrame>,
	expr: &LogicalExpr,
) -> Result<(), IrError> {
	let id = match frame {
		Some(eval) => cx.frame_for(eval)?,
		None => cx.frame_for(&EvalFrame::caller())?,
	};
	cx.with_frame(id, |cx| row_clause(cx, owner, kind, expr))
}

/// A permission predicate over `input`, judged in the frame of its guard under the definer's
/// limit. `computed` is what the predicate reads of the table's `COMPUTED` fields
/// ([`computed_reads_of`]): the body of each field it derives follows the predicate as a
/// `FieldFrame(Computed)` region, in the order the shape lists the fields, bound in a permission
/// frame under the field's own `AUTH LIMIT`, as a write gate binds the fields its predicate
/// derives.
fn bind_permission_filter(
	cx: &mut BindCx<'_>,
	input: ValueId,
	guard: GuardKind,
	auth: Option<&AuthLimit>,
	predicate: &LogicalExpr,
	computed: Option<ComputedReads>,
) -> Result<ValueId, IrError> {
	let auth = auth.cloned();
	let eval = match guard {
		GuardKind::Read => EvalFrame::permission(auth),
		GuardKind::Write => EvalFrame::mutable_permission(auth),
	};
	let (computed, bodies) = match computed {
		Some(ComputedReads {
			shape,
			bodies,
		}) => (Some(shape), bodies),
		None => (None, Vec::new()),
	};
	let frame = cx.frame_for(&eval)?;
	cx.with_frame(frame, |cx| {
		let op = cx.emit(
			OpKind::Logical(LogOp::PermissionFilter {
				guard,
				computed,
			}),
			&[input],
			&[TypeKnowledge::Unknown],
		)?;
		row_clause(cx, op, RegionKind::PermissionFrame, predicate)?;
		bind_derivations(cx, op, &bodies)?;
		first_result(cx, op)
	})
}

/// What a permission predicate reads of a table's `COMPUTED` fields: the shape its op states, and
/// the bodies of the fields it derives, in the order the shape lists them.
pub(super) struct ComputedReads {
	pub(super) shape: Box<ComputedReadsShape>,
	pub(super) bodies: Vec<Derivation>,
}

/// The `COMPUTED` fields of the table `subject` names that `predicate` reads, decided by the
/// planner's rule ([`computed_reads`]) from the table's schema: `None` when there is no
/// subject or the predicate reads none of its computed fields, otherwise the fields read by name,
/// sorted, or every one where the predicate reads the row other than by name (a row parameter, the
/// document, a nested statement, a call evaluated against the row, or the invocation of a body
/// the expression does not hold).
///
/// The decision is made over the bound expression, before any pass of the IR. A pass never adds a
/// read to a permission predicate: the permission passes move `WHERE` conjuncts below a
/// permission, never into it, and erase a permission that admits every row, so the stated fields
/// cover every read the lowered predicate makes.
///
/// The fields derived are those `catalog` defines for the table, in the order that
/// `guard_derivations` derives them ([`derivations_of`]): the fields read and the computed fields
/// they read in turn. A table the catalog does not define derives none; an answer that predates a
/// catalog change states [`DerivedReads::LateBound`], and a computed field with no logical form or
/// no single name [`DerivedReads::Unlowerable`].
fn computed_reads_of(
	registry: &FunctionRegistry,
	catalog: &dyn SemanticCatalog,
	subject: Option<(TableName, Arc<Schema>)>,
	predicate: &LogicalExpr,
) -> Option<ComputedReads> {
	let (table, schema) = subject?;
	let reads = computed_reads(predicate, &schema, registry)?;
	Some(derived_reads(catalog, table, reads))
}

/// What the guards of the layer whose columns are `fields`, over rows of `schema`, read of the
/// `COMPUTED` fields of `table`, the table whose rows they are, taken together, as
/// [`computed_reads_of`] decides it for one predicate: `None` when they read none.
///
/// A permission column written `IF guard THEN value ELSE NONE` reads what its guard reads. A
/// permission column on a stored field written otherwise holds its guards inside its value
/// (a nested or wildcard guard, a guard over the whole column), so it reads what the column
/// reads; its stored fields are the row's on the copy too. A column on a computed field reads its
/// value as the caller derives it, and a column in any other frame is no guard. One copy of the
/// row carries the fields every guard reads ([`ProjectAttrs::judged`]).
///
/// The decision is made over the bound expressions, before any pass of the IR, which adds no read
/// to a guard. Guards reading a computed field of rows no table names are an invariant break: the
/// resolver writes a layer over the source of a table alone.
pub(super) fn judged_reads_of(
	registry: &FunctionRegistry,
	catalog: &dyn SemanticCatalog,
	table: Option<&TableName>,
	schema: &Schema,
	fields: &LogicalFields,
) -> Result<Option<ComputedReads>, IrError> {
	let declared = declared_computed(schema);
	let mut union: Option<Option<HashSet<String>>> = None;
	for column in columns_of(fields) {
		let Some(eval) = column.frame else {
			continue;
		};
		if !matches!(eval.mode, EvalMode::Permission | EvalMode::MutablePermission) {
			continue;
		}
		let judged = match judgement_of(eval, column.expr) {
			Some((guard, _)) => guard,
			None => {
				let on_computed = column
					.path
					.as_ref()
					.and_then(OutputPath::as_field)
					.is_some_and(|name| declared.iter().any(|field| field.as_str() == name));
				if on_computed {
					continue;
				}
				column.expr
			}
		};
		let Some(reads) = computed_reads(judged, schema, registry) else {
			continue;
		};
		union = Some(match (union, reads) {
			(Some(None), _) | (_, None) => None,
			(Some(Some(mut all)), Some(reads)) => {
				all.extend(reads);
				Some(all)
			}
			(None, Some(reads)) => Some(reads),
		});
	}
	let Some(reads) = union else {
		return Ok(None);
	};
	let Some(table) = table else {
		return Err(invariant("a layer guard reads a computed field of rows of no table"));
	};
	Ok(Some(derived_reads(catalog, table.clone(), reads)))
}

/// The derivation of `reads`, the `COMPUTED` fields of `table` an expression reads by name, sorted
/// into the shape an op states — every one where `None` — with the bodies of the fields derived,
/// as [`computed_reads_of`] states it.
fn derived_reads(
	catalog: &dyn SemanticCatalog,
	table: TableName,
	reads: Option<HashSet<String>>,
) -> ComputedReads {
	let fields = reads.map(|reads| {
		let mut fields: Vec<String> = reads.into_iter().collect();
		fields.sort_unstable();
		fields
	});
	let (derived, bodies) = match catalog.table(&table) {
		CatalogAnswer::Found(snapshot) => match derivations_of(&snapshot.fields, fields.as_deref())
		{
			Some(bodies) => (
				DerivedReads::Stated(bodies.iter().map(|body| body.field.clone()).collect()),
				bodies,
			),
			None => (DerivedReads::Unlowerable, Vec::new()),
		},
		CatalogAnswer::Absent => (DerivedReads::Stated(Box::default()), Vec::new()),
		CatalogAnswer::NotLoaded | CatalogAnswer::Unknown => (DerivedReads::LateBound, Vec::new()),
	};
	ComputedReads {
		shape: Box::new(ComputedReadsShape {
			table,
			fields,
			derived,
		}),
		bodies,
	}
}

/// One projected value of a field list: its expression, the frame it is evaluated in and where it
/// lands.
pub(super) struct Column<'a> {
	pub(super) expr: &'a LogicalExpr,
	pub(super) frame: Option<&'a EvalFrame>,
	pub(super) path: Option<OutputPath>,
}

/// The columns of `fields`, in order; `*` is not one.
pub(super) fn columns_of(fields: &LogicalFields) -> Vec<Column<'_>> {
	match fields {
		LogicalFields::Value {
			expr,
			alias,
		} => vec![Column {
			expr,
			frame: None,
			path: alias.clone(),
		}],
		LogicalFields::Select(list) => list
			.iter()
			.filter_map(|field| match field {
				LogicalField::All => None,
				LogicalField::Single {
					expr,
					frame,
					alias,
					..
				} => Some(Column {
					expr,
					frame: Some(frame),
					path: Some(
						alias.clone().unwrap_or_else(|| expr.named_after_gates().output_path()),
					),
				}),
			})
			.collect(),
	}
}

/// The shape of the projection list `fields`.
pub(super) fn fields_shape(fields: &LogicalFields) -> FieldsShape {
	match fields {
		LogicalFields::Value {
			alias,
			..
		} => FieldsShape::Value {
			alias: alias.clone(),
		},
		LogicalFields::Select(list) => FieldsShape::Select(
			list.iter()
				.map(|field| match field {
					LogicalField::All => FieldShape::All,
					LogicalField::Single {
						alias,
						kind,
						..
					} => FieldShape::Single {
						alias: alias.clone(),
						kind: kind.clone(),
					},
				})
				.collect(),
		),
	}
}

/// Where each `SPLIT` expression of `exprs` reads its collection and writes each element back, as
/// the split operator reads them.
///
/// An expression with no write position is one the parser's `SPLIT` grammar does not produce, so it
/// is an invariant.
pub(super) fn split_targets(exprs: &[LogicalExpr]) -> Result<Box<[SplitTarget]>, IrError> {
	exprs
		.iter()
		.map(|expr| {
			split_target(expr).ok_or(IrError::Invariant {
				what: "a SPLIT target with no write position",
			})
		})
		.collect()
}

/// The name each field of an aggregation over `fields` lands under in the group's object, in
/// order: its alias where it has one, otherwise the path its expression names.
pub(super) fn aggregate_names(fields: &LogicalFields) -> Vec<OutputPath> {
	let LogicalFields::Select(list) = fields else {
		return Vec::new();
	};
	list.iter()
		.filter_map(|field| match field {
			LogicalField::Single {
				expr,
				alias,
				..
			} => Some(alias.clone().unwrap_or_else(|| expr.named_after_gates().output_path())),
			LogicalField::All => None,
		})
		.collect()
}

/// The name each unaliased field of `fields` lands under, in order: an expression that is a path
/// lands at the path it reads, and any other expression under its rendering as one flat key.
pub(super) fn unaliased_names(fields: &LogicalFields) -> Vec<OutputPath> {
	let LogicalFields::Select(list) = fields else {
		return Vec::new();
	};
	list.iter()
		.filter_map(|field| match field {
			LogicalField::Single {
				expr,
				alias: None,
				..
			} => {
				let named = expr.named_after_gates();
				Some(if named.is_path() {
					named.output_path()
				} else {
					OutputPath::field(named.output_path().name())
				})
			}
			LogicalField::Single {
				..
			}
			| LogicalField::All => None,
		})
		.collect()
}

/// `SELECT`'s projection over `input`. A resolver layer's columns land the stored field in their
/// projection region and are evaluated in a field frame of their own, under the frame the plan
/// gives each.
///
/// A layer carries the fields its input's schema declares `COMPUTED` ([`declared_computed`]), the
/// set the lowering of the layer reads.
fn bind_project(
	cx: &mut BindCx<'_>,
	project: &LogicalProject,
	input: ValueId,
) -> Result<ValueId, IrError> {
	let layer = project.is_layer();
	if !layer {
		return bind_projection(cx, &project.fields, &project.omit, false, Vec::new(), input);
	}
	let schema = project.input.schema();
	let table = permission_subject(&project.input).map(|(table, _)| table);
	let judged =
		judged_reads_of(cx.registry(), cx.catalog(), table.as_ref(), &schema, &project.fields)?;
	let computed = declared_computed(&schema);
	bind_layer_projection(cx, &project.fields, computed, judged, input)
}

/// The fields `schema` declares `COMPUTED`, in its order: those with a computed body whose path is
/// one field. A layer over rows of that schema takes a column under one of these names as on a
/// computed field.
pub(super) fn declared_computed(schema: &crate::expr::logical::Schema) -> Vec<Strand> {
	schema
		.fields
		.iter()
		.filter(|field| field.computed.is_some())
		.filter_map(|field| field.path.as_field().cloned())
		.collect()
}

/// A projection of `fields` over `input`: one region per column, then one per `OMIT` path, then,
/// for a layer, the frame region of each column. `computed` is the layer's declared computed
/// fields ([`ProjectAttrs::computed`]), empty for a projection that is not a layer.
pub(super) fn bind_projection(
	cx: &mut BindCx<'_>,
	fields: &LogicalFields,
	omit_paths: &[LogicalExpr],
	layer: bool,
	computed: Vec<Strand>,
	input: ValueId,
) -> Result<ValueId, IrError> {
	bind_projection_judged(cx, fields, omit_paths, layer, computed, None, input)
}

/// A layer of `fields` over `input`, as [`bind_projection`] binds one, whose guards read `judged`
/// of the table's `COMPUTED` fields ([`judged_reads_of`]): the body of each field it derives
/// follows the frame regions of the columns as a `FieldFrame(Computed)` region, in the order the
/// shape lists the fields, bound in a permission frame under the field's own `AUTH LIMIT`, as a
/// permission filter binds the fields its predicate derives ([`bind_permission_filter`]).
fn bind_layer_projection(
	cx: &mut BindCx<'_>,
	fields: &LogicalFields,
	computed: Vec<Strand>,
	judged: Option<ComputedReads>,
	input: ValueId,
) -> Result<ValueId, IrError> {
	bind_projection_judged(cx, fields, &[], true, computed, judged, input)
}

/// [`bind_projection`], with the computed fields a layer's guards read ([`bind_layer_projection`]).
fn bind_projection_judged(
	cx: &mut BindCx<'_>,
	fields: &LogicalFields,
	omit_paths: &[LogicalExpr],
	layer: bool,
	computed: Vec<Strand>,
	judged: Option<ComputedReads>,
	input: ValueId,
) -> Result<ValueId, IrError> {
	let omit = u16::try_from(omit_paths.len()).map_err(|_| IrError::Capacity("omit paths"))?;
	let sort_keys = match layer {
		true => Vec::new(),
		false => cx.take_sort_keys(),
	};
	let (judged, bodies) = match judged {
		Some(ComputedReads {
			shape,
			bodies,
		}) => (Some(shape), bodies),
		None => (None, Vec::new()),
	};
	let mut attrs = project_attrs(fields, omit, layer, computed, sort_keys);
	attrs.judged = judged;
	let op = cx.emit(
		OpKind::Logical(LogOp::Project(Box::new(attrs))),
		&[input],
		&[TypeKnowledge::Unknown],
	)?;
	let columns = columns_of(fields);
	let projection = PROJECTION_CLAUSE;
	for column in &columns {
		if layer {
			let name = column
				.path
				.as_ref()
				.and_then(OutputPath::as_field)
				.ok_or_else(|| invariant("a layer column lands under one field"))?;
			let stored = LogicalExpr::Field(Strand::new(name));
			row_clause(cx, op, projection, &stored)?;
		} else {
			framed_clause(cx, op, projection, column.frame, column.expr)?;
		}
	}
	for path in omit_paths {
		row_clause(cx, op, projection, path)?;
	}
	if layer {
		for column in &columns {
			layer_column(cx, op, column.frame, column.expr)?;
		}
		bind_derivations(cx, op, &bodies)?;
	}
	first_result(cx, op)
}

/// The `FieldFrame(Computed)` region of each of `bodies`, owned by `op`, in order: each body bound
/// in a permission frame under its field's own `AUTH LIMIT`, as the computed fields a permission
/// predicate reads are derived.
pub(super) fn bind_derivations(
	cx: &mut BindCx<'_>,
	op: OpId,
	bodies: &[Derivation],
) -> Result<(), IrError> {
	for derivation in bodies {
		let eval = EvalFrame::permission(derivation.auth.clone());
		let kind = RegionKind::FieldFrame(FieldRole::Computed);
		framed_clause(cx, op, kind, Some(&eval), &derivation.body)?;
	}
	Ok(())
}

/// The region of one column or `OMIT` path of a projection, which receives the row it projects
/// ([`row_region_args`]).
pub(crate) const PROJECTION_CLAUSE: RegionKind = RegionKind::Clause(ClauseRole::Projection);

/// The region of the predicate of a filter, which receives the row it judges
/// ([`row_region_args`]).
pub(crate) const WHERE_CLAUSE: RegionKind = RegionKind::Clause(ClauseRole::Where);

/// The attributes of a projection of `fields` that omits `omit` paths. `computed` is the layer's
/// declared computed fields ([`ProjectAttrs::computed`]), empty for a projection that is not a
/// layer, and `sort_keys` the fields that are keys of the sort beneath
/// ([`ProjectAttrs::sort_keys`]).
pub(crate) fn project_attrs(
	fields: &LogicalFields,
	omit: u16,
	layer: bool,
	computed: Vec<Strand>,
	sort_keys: Vec<(u16, u16)>,
) -> ProjectAttrs {
	ProjectAttrs {
		fields: fields_shape(fields),
		layer,
		omit,
		names: unaliased_names(fields),
		computed,
		sort_keys,
		judged: None,
	}
}

/// The block arguments of a layer column's frame region: the row, then the value the column
/// judges. Only the guard of a permission column reads the second, as `$value`.
const LAYER_COLUMN: &[RowName] = &[("this", 0), ("self", 0), ("value", 1)];

/// The frame region of one column of a layer owned by `owner`, a `FieldFrame(Computed)` region
/// whose ops run under the frame `frame` names, evaluating `expr` against the row.
///
/// The region takes the row and the value the column judges ([`LAYER_COLUMN`]). A permission
/// column written `IF guard THEN value ELSE NONE` binds its guard with `$value` naming the judged
/// value, as the guard is evaluated with the column's value in `$value`; its value and every
/// other column bind `$value` to the row, as the other names of the row are.
fn layer_column(
	cx: &mut BindCx<'_>,
	owner: OpId,
	frame: Option<&EvalFrame>,
	expr: &LogicalExpr,
) -> Result<(), IrError> {
	let kind = RegionKind::FieldFrame(FieldRole::Computed);
	let id = match frame {
		Some(eval) => cx.frame_for(eval)?,
		None => cx.frame_for(&EvalFrame::caller())?,
	};
	let judgement = frame.and_then(|eval| judgement_of(eval, expr));
	cx.with_frame(id, |cx| {
		cx.within(owner, kind, &named_region_args(kind, LAYER_COLUMN), |cx, entry| {
			let base = usize::from(kind.isolated_from_above());
			let row = entry.get(base).copied().ok_or(IrError::Stale("row"))?;
			let judged = entry.get(base + 1).copied().ok_or(IrError::Stale("judged value"))?;
			for (name, _) in ROW_ONLY {
				cx.declare(Strand::new(*name), row);
			}
			if !kind.isolated_from_above() {
				cx.declare_parent();
			}
			cx.with_row(row, |cx| {
				let Some((guard, value)) = judgement else {
					return bind_body(cx, expr);
				};
				// The guard reads the judged value as `$value`; the names bound after it read the
				// row again.
				cx.declare(Strand::new("value"), judged);
				let test = bind_expr(cx, guard)?;
				cx.declare(Strand::new("value"), row);
				let Built::Value(test) = test else {
					return Ok(());
				};
				bind_judgement(cx, test, value)
			})
		})
	})
}

/// The guard and the value of a permission column written `IF guard THEN value ELSE NONE`, as the
/// planner splits a layer column's judgement; `None` for any other column.
fn judgement_of<'a>(
	eval: &EvalFrame,
	expr: &'a LogicalExpr,
) -> Option<(&'a LogicalExpr, &'a LogicalExpr)> {
	if !matches!(eval.mode, EvalMode::Permission | EvalMode::MutablePermission) {
		return None;
	}
	let LogicalExpr::If(chain) = expr else {
		return None;
	};
	let [(guard, value)] = chain.branches.as_slice() else {
		return None;
	};
	matches!(chain.otherwise, Some(LogicalExpr::Literal(LogicalLiteral::None)))
		.then_some((guard, value))
}

/// `GROUP BY`: one region per key, each an error scope that turns an ignorable error into `NONE`,
/// then one region per field. The aggregate functions called in a field belong to the statement.
///
/// A grouped projection the planner refuses (a field that reads `$this` or `$parent`, or an
/// aggregate call inside another) is refused once its input has bound, so a refusal of the input
/// stands first; the input's ops are then erased and the refusal raised in their place, as the
/// planner replaces the whole read with it and nothing of the read runs. As for a `LET` of a
/// protected name ([`bind_let`]), where the read is planned with its top-level statement the
/// refusal is the whole statement's, raised before any of it runs; otherwise it is raised where
/// the read stands, when it is reached.
fn bind_aggregate(cx: &mut BindCx<'_>, aggregate: &LogicalAggregate) -> Result<Flow, IrError> {
	let mark = cx.block_ops()?.len();
	let Some(rows) = bind_rows(cx, &aggregate.input)? else {
		return Ok(Flow::Diverged);
	};
	if let Some(refusal) = grouping_refusal(cx.registry(), aggregate)? {
		erase_bound(cx, mark)?;
		if cx.eager() {
			cx.hoist_refusal(refusal.clone());
			return raise_refused(cx, refusal);
		}
		cx.raise(RaiseKind::Refused(Box::new(refusal)))?;
		return Ok(Flow::Diverged);
	}
	aggregate_over(cx, rows, &aggregate.group, &aggregate.fields, Vec::new()).map(Flow::Value)
}

/// The refusal the planner raises for the grouped projection of `aggregate`, in its order: a
/// field that reads a row-scoped parameter, then the analysis of the fields. Both raise only
/// query errors, so an error of another kind is an invariant break.
fn grouping_refusal(
	registry: &FunctionRegistry,
	aggregate: &LogicalAggregate,
) -> Result<Option<Refusal>, IrError> {
	let checked = check_forbidden_group_by_params(&aggregate.fields).and_then(|()| {
		analyse_aggregation(registry, &aggregate.fields, &aggregate.group).map(|_| ())
	});
	match checked {
		Ok(()) => Ok(None),
		Err(error) => Refusal::of(&error)
			.map(Some)
			.ok_or_else(|| invariant(&format!("a grouped projection refused otherwise: {error}"))),
	}
}

/// The aggregate of the rows `rows`, which it needs all of, by the keys `group` and the columns
/// `fields`: one region per key, then one per column.
pub(super) fn aggregate_over(
	cx: &mut BindCx<'_>,
	rows: ValueId,
	group: &[LogicalExpr],
	fields: &LogicalFields,
	first_value: Vec<usize>,
) -> Result<ValueId, IrError> {
	let shape = Box::new(AggregateShape {
		groups: u16::try_from(group.len()).map_err(|_| IrError::Capacity("group keys"))?,
		fields: fields_shape(fields),
		names: aggregate_names(fields),
		first_value,
	});
	let op =
		cx.emit(OpKind::Logical(LogOp::Aggregate(shape)), &[rows], &[TypeKnowledge::Unknown])?;
	for key in group {
		row_clause(cx, op, RegionKind::ErrorScope(ErrorScope::ShapeToNone), key)?;
	}
	cx.with_aggregating(|cx| {
		for column in columns_of(fields) {
			framed_clause(
				cx,
				op,
				RegionKind::Clause(ClauseRole::Projection),
				column.frame,
				column.expr,
			)?;
		}
		Ok(())
	})?;
	first_result(cx, op)
}

/// `ORDER BY`: one region per key.
///
/// A key that names a field or an alias of the projection above the sort is bound as the
/// expression that field holds, as the planner reads it, so it is read from the sort's input rows
/// and not from the output the alias lands in. Over a grouping the keys read the rows it produces,
/// under the names the projection gave them, and are bound as written.
fn bind_sort(cx: &mut BindCx<'_>, sort: &LogicalSort) -> Result<Flow, IrError> {
	let order_fields =
		cx.take_order_fields().filter(|_| !matches!(&*sort.input, LogicalOp::Aggregate(_)));
	let Some(rows) = bind_rows(cx, &sort.input)? else {
		return Ok(Flow::Diverged);
	};
	sort_over(cx, rows, &sort.ordering, sort.tempfiles, order_fields.as_ref()).map(Flow::Value)
}

/// A window over the rows of its input: a sort of the rows on the partition keys, ascending, then
/// on the order keys, then the window over the sorted rows, with one region per partition key, one
/// per order key, then one per argument of each call, in call order.
///
/// The sort delivers the partition-then-order sequence the window reads; where neither keys are
/// written the rows are one partition of peers, read as they arrive, and no sort is emitted. A
/// partition key's direction does not change any value of the window, so it sorts ascending, and
/// it sorts on the value alone so that rows equal as values share a partition. Neither the sort
/// nor the window resolves an `ORDER BY` alias of the projection: their keys read the input rows.
fn bind_window(cx: &mut BindCx<'_>, window: &LogicalWindow) -> Result<Flow, IrError> {
	let Some(rows) = bind_rows(cx, &window.input)? else {
		return Ok(Flow::Diverged);
	};
	let rows = if window.partition.is_empty() && window.order.is_empty() {
		rows
	} else {
		let keys = window
			.partition
			.iter()
			.map(|key| LogicalOrder {
				value: key.clone(),
				collate: false,
				numeric: false,
				direction: true,
			})
			.chain(window.order.iter().cloned())
			.collect();
		sort_over(cx, rows, &LogicalOrdering::Order(keys), false, None)?
	};
	let shape = Box::new(window_shape(window)?);
	let op = cx.emit(OpKind::Logical(LogOp::Window(shape)), &[rows], &[TypeKnowledge::Unknown])?;
	for key in &window.partition {
		row_clause(cx, op, ORDER_KEY, key)?;
	}
	for key in &window.order {
		row_clause(cx, op, ORDER_KEY, &key.value)?;
	}
	for call in &window.calls {
		for arg in &call.args {
			row_clause(cx, op, RegionKind::Clause(ClauseRole::Projection), arg)?;
		}
	}
	first_result(cx, op).map(Flow::Value)
}

/// The attributes of `window`: its keys' flags and its calls, each owning as many argument regions
/// as it has arguments. An order key is never read as a path ([`SortKeyShape::path`]): every key
/// is evaluated by its region.
fn window_shape(window: &LogicalWindow) -> Result<WindowShape, IrError> {
	let calls = window
		.calls
		.iter()
		.map(|call| {
			Ok(WindowCallShape {
				output: call.output.clone(),
				function: call.function,
				args: u16::try_from(call.args.len())
					.map_err(|_| IrError::Capacity("window arguments"))?,
				frame: call.frame,
			})
		})
		.collect::<Result<_, IrError>>()?;
	Ok(WindowShape {
		partitions: u16::try_from(window.partition.len())
			.map_err(|_| IrError::Capacity("window partition keys"))?,
		order: window
			.order
			.iter()
			.map(|key| SortKeyShape {
				collate: key.collate,
				numeric: key.numeric,
				ascending: key.direction,
				path: false,
			})
			.collect(),
		calls,
	})
}

/// The sort of the rows `rows`, which it needs all of, by `ordering`: one region per key. A key
/// that names a field or an alias of `order_fields` is bound as the expression that field holds;
/// any other key written as a path is read from the row as it stands ([`SortKeyShape::path`]).
pub(super) fn sort_over(
	cx: &mut BindCx<'_>,
	rows: ValueId,
	ordering: &LogicalOrdering,
	tempfiles: bool,
	order_fields: Option<&LogicalFields>,
) -> Result<ValueId, IrError> {
	let keys = match ordering {
		LogicalOrdering::Random => &[][..],
		LogicalOrdering::Order(keys) => keys.as_slice(),
	};
	let resolved: Vec<_> = keys
		.iter()
		.map(|key| {
			order_fields.and_then(|fields| {
				resolve_order_by_alias(&key.value, fields).map(|resolved| (fields, resolved))
			})
		})
		.collect();
	let attrs = sort_attrs(ordering, |at| resolved[at].is_some(), tempfiles);
	let op =
		cx.emit(OpKind::Logical(LogOp::Sort(Box::new(attrs))), &[rows], &[TypeKnowledge::Unknown])?;
	let mut shared = Vec::new();
	for (at, (key, resolved)) in keys.iter().zip(&resolved).enumerate() {
		if let Some((fields, (expr, name))) = resolved
			&& let Some(field) = aliased_field(fields, expr, name)
		{
			let at = u16::try_from(at).map_err(|_| IrError::Capacity("sort keys"))?;
			shared.push((field, at));
		}
		let value = resolved.as_ref().map_or(&key.value, |(_, (expr, _))| expr);
		row_clause(cx, op, ORDER_KEY, value)?;
	}
	if order_fields.is_some() {
		cx.set_sort_keys(shared);
	}
	first_result(cx, op)
}

/// The region of one key of a sort, which receives the row it orders ([`row_region_args`]).
pub(crate) const ORDER_KEY: RegionKind = RegionKind::Clause(ClauseRole::OrderKey);

/// The attributes of the sort by `ordering`. `aliased` states, by the position of a key, whether
/// the key is bound as the expression of a field of the projection above the sort; any other key
/// written as a path is read from the row as it stands ([`SortKeyShape::path`]).
pub(crate) fn sort_attrs(
	ordering: &LogicalOrdering,
	aliased: impl Fn(usize) -> bool,
	tempfiles: bool,
) -> SortAttrs {
	use crate::exec::planner::util::field_path_from_expr;
	let ordering = match ordering {
		LogicalOrdering::Random => SortShape::Random,
		LogicalOrdering::Order(keys) => SortShape::Keys(
			keys.iter()
				.enumerate()
				.map(|(at, key)| SortKeyShape {
					collate: key.collate,
					numeric: key.numeric,
					ascending: key.direction,
					path: !aliased(at) && field_path_from_expr(&key.value).is_ok(),
				})
				.collect(),
		),
	};
	SortAttrs {
		ordering,
		tempfiles,
	}
}

/// The position among `fields` of the field whose alias is `name` and whose expression is `expr`,
/// where `expr` is not a path: the field whose one evaluation per row is also the value an
/// `ORDER BY` naming the alias sorts by. A path is read again from the row, which answers the same
/// value, and an access path may serve its order in place of the sort.
fn aliased_field(fields: &LogicalFields, expr: &LogicalExpr, name: &str) -> Option<u16> {
	use crate::exec::planner::util::field_path_from_expr;
	if field_path_from_expr(expr).is_ok() {
		return None;
	}
	match fields {
		LogicalFields::Value {
			alias: Some(alias),
			..
		} => (alias.as_field() == Some(name)).then_some(0),
		LogicalFields::Value {
			alias: None,
			..
		} => None,
		LogicalFields::Select(list) => list
			.iter()
			.position(|field| {
				matches!(
					field,
					LogicalField::Single { expr: held, alias: Some(alias), .. }
						if held == expr && alias.name() == name
				)
			})
			.and_then(|at| u16::try_from(at).ok()),
	}
}

/// `LIMIT` and `START`: each evaluated once, `LIMIT` first, before the rows they bound are read.
fn bind_limit(cx: &mut BindCx<'_>, limit: &LogicalLimit) -> Result<Flow, IrError> {
	let order_fields = cx.take_order_fields();
	let mut bound = [None, None];
	for (slot, expr) in bound.iter_mut().zip([&limit.limit, &limit.start]) {
		if let Some(expr) = expr {
			let Built::Value(value) = bind_expr(cx, expr)? else {
				return Ok(Flow::Diverged);
			};
			*slot = Some(value);
		}
	}
	let [limit_value, start_value] = bound;
	if matches!(&*limit.input, LogicalOp::Sort(_)) {
		cx.set_order_fields(order_fields);
	}
	let Some(rows) = bind_rows(cx, &limit.input)? else {
		return Ok(Flow::Diverged);
	};
	limit_over(cx, rows, limit_value, start_value).map(Flow::Value)
}

/// The limit of the rows `rows` by the values `limit` and `start`, each already bound.
pub(super) fn limit_over(
	cx: &mut BindCx<'_>,
	rows: ValueId,
	limit_value: Option<ValueId>,
	start_value: Option<ValueId>,
) -> Result<ValueId, IrError> {
	let (kind, operands) = limit_layout(rows, limit_value, start_value);
	cx.emit_value(OpKind::Logical(kind), &operands, TypeKnowledge::Unknown)
}

/// The op that bounds the rows `rows` by the values `limit` and `start`, each present only when
/// written, and the operands it takes: the rows, then the limit, then the start. The op names the
/// operand slot of each value it holds.
pub(crate) fn limit_layout(
	rows: ValueId,
	limit_value: Option<ValueId>,
	start_value: Option<ValueId>,
) -> (LogOp, SmallVec<[ValueId; 3]>) {
	let mut operands = SmallVec::new();
	operands.push(rows);
	operands.extend(limit_value);
	operands.extend(start_value);
	let limit_slot = limit_value.map(|_| 1);
	let start_slot = start_value.map(|_| 1 + u16::from(limit_value.is_some()));
	(
		LogOp::Limit {
			limit: limit_slot,
			start: start_slot,
		},
		operands,
	)
}

/// A read of storage. The operand, when the source names one, is its value; a table is named by
/// the attribute.
pub(super) fn bind_source(cx: &mut BindCx<'_>, source: &LogicalSource) -> Result<Flow, IrError> {
	let (shape, operand) = match &source.source {
		SourceRef::Table(table) => (SourceRefShape::Table(table.clone()), None),
		SourceRef::RecordId(id) => {
			(SourceRefShape::RecordId, Some(Cow::Owned(LogicalExpr::RecordId(id.clone()))))
		}
		SourceRef::Mock(mock) => {
			(SourceRefShape::Mock, Some(Cow::Owned(LogicalExpr::Mock(mock.clone()))))
		}
		SourceRef::Param(param) => {
			(SourceRefShape::Param, Some(Cow::Owned(LogicalExpr::Param(param.clone()))))
		}
		SourceRef::Dynamic(expr) => (SourceRefShape::Dynamic, Some(Cow::Borrowed(expr))),
	};
	let value = match operand {
		Some(expr) => {
			let Built::Value(value) = bind_expr(cx, &expr)? else {
				return Ok(Flow::Diverged);
			};
			Some(value)
		}
		None => None,
	};
	let may_meet_change = cx.nested_read_may_meet_change();
	let attrs = SourceAttrs {
		source_ref: shape,
		with: source.with.clone(),
		for_update: source.for_update,
		may_be_defined: cx.tables_may_be_defined(),
		may_meet_change,
		resolved: source.schema.is_some(),
	};
	let op = cx.emit(
		OpKind::Logical(LogOp::Source(Box::new(attrs))),
		value.as_slice(),
		&[TypeKnowledge::Unknown],
	)?;
	let rows = first_result(cx, op)?;
	let SourceRef::Table(table) = &source.source else {
		return Ok(Flow::Value(rows));
	};
	// A read that may meet a changed catalog enforces the permissions it meets when it runs.
	if may_meet_change {
		return Ok(Flow::Value(rows));
	}
	let subject = source.schema.as_ref().map(|schema| (table.clone(), Arc::clone(schema)));
	match permissions_of(cx.catalog(), table) {
		Some(conjuncts) if !conjuncts.is_empty() => select_loop(cx, rows, |cx, element| {
			conjuncts.iter().try_fold(element, |row, conjunct| {
				let computed = computed_reads_of(
					cx.registry(),
					cx.catalog(),
					subject.clone(),
					&conjunct.predicate,
				);
				bind_permission_filter(
					cx,
					row,
					conjunct.guard,
					conjunct.auth.as_ref(),
					&conjunct.predicate,
					computed,
				)
			})
		})
		.map(Flow::Value),
		Some(_) | None => Ok(Flow::Value(rows)),
	}
}

/// Rows produced from a value, whose expression is the operand. A list of record ids written in
/// `FROM` is one literal ([`record_id_list`]): the source reads it whole.
fn bind_values(cx: &mut BindCx<'_>, values: &LogicalValues) -> Result<Flow, IrError> {
	let value = match record_id_list(&values.expr) {
		Some(list) => cx.emit_value(
			OpKind::Expr(ExprOp::Literal(Box::new(list))),
			&[],
			TypeKnowledge::Unknown,
		)?,
		None => {
			let Built::Value(value) = bind_expr(cx, &values.expr)? else {
				return Ok(Flow::Diverged);
			};
			value
		}
	};
	cx.emit_value(
		OpKind::Logical(LogOp::Values {
			for_update: values.for_update,
			written: values.written,
		}),
		&[value],
		TypeKnowledge::Unknown,
	)
	.map(Flow::Value)
}

/// A statement in relational position: its region holds the statement and yields its value.
fn bind_relational_subquery(
	cx: &mut BindCx<'_>,
	subquery: &LogicalSubquery,
) -> Result<Flow, IrError> {
	let op = cx.emit(OpKind::Logical(LogOp::Subquery), &[], &[TypeKnowledge::Unknown])?;
	cx.within(op, RegionKind::Subquery, &[], |cx, _| {
		if let Some(value) = bind_value(cx, &subquery.input)? {
			cx.yield_logical(&[value])?;
		} else if !cx.ends_in_terminator()? {
			cx.yield_logical(&[])?;
		}
		Ok(())
	})?;
	first_result(cx, op).map(Flow::Value)
}

/// A join of two inputs. The key of each side is a region evaluated against a row of that side,
/// left key first, and the residual filter a region evaluated against the joined row. The op and
/// its regions are bound in the frame the plan gives the join.
fn bind_join(cx: &mut BindCx<'_>, join: &LogicalJoin) -> Result<Flow, IrError> {
	let Some(left) = bind_value(cx, join.left())? else {
		return Ok(Flow::Diverged);
	};
	let Some(right) = bind_value(cx, join.right())? else {
		return Ok(Flow::Diverged);
	};
	let frame = cx.frame_for(&join.frame)?;
	cx.with_frame(frame, |cx| {
		let op = cx.emit(
			OpKind::Logical(LogOp::Join {
				kind: join.kind,
				alias: join.alias.clone(),
			}),
			&[left, right],
			&[TypeKnowledge::Unknown],
		)?;
		let clause = RegionKind::Clause(ClauseRole::Where);
		for (left_key, right_key) in &join.on {
			row_clause(cx, op, clause, left_key)?;
			row_clause(cx, op, clause, right_key)?;
		}
		if let Some(residual) = &join.filter {
			row_clause(cx, op, clause, residual)?;
		}
		first_result(cx, op)
	})
	.map(Flow::Value)
}

// ---------------------------------------------------------------------------------------------
// Writes
// ---------------------------------------------------------------------------------------------

/// A mutation: the subject loop, then the write that consumes its result.
///
/// The subjects and the `INTO` table are evaluated before the loop, so an error in them, or a
/// `BREAK` written in them, ends the statement before any subject is written. Answers the value
/// of the write.
fn bind_write(cx: &mut BindCx<'_>, op: &LogicalOp, write: &LogicalWrite) -> Result<Flow, IrError> {
	let bundle = cx.take_bundle_table();
	let named = cx
		.take_named_table()
		.or_else(|| written_table(write))
		.or_else(|| target_variable_table(cx.catalog(), write));
	// As for a `LET` of a protected name ([`bind_let`]): where the write is planned with its
	// top-level statement the refusal is the whole statement's, raised before any of it runs.
	if let Some(refusal) = upsert_knn_refusal(write).or_else(|| mixed_targets_search_refusal(write))
	{
		if cx.eager() {
			cx.hoist_refusal(refusal.clone());
			return raise_refused(cx, refusal);
		}
		cx.raise(RaiseKind::Refused(Box::new(refusal)))?;
		return Ok(Flow::Diverged);
	}
	let mut facts = write_facts(write);
	// The write of a view row runs the view table's order for the action its view's maintenance
	// settled, whatever statement stands for it.
	if bundle.as_ref().is_some_and(|bundle| bundle.view_row) {
		facts.form = PlanForm::View;
	}
	let bundled = bundle.is_some();
	let run = stage_run(facts.form, facts);
	// A write that may meet definitions the bind cannot read (one after a statement of its list
	// that may change the catalog, one in a statement that may change it while it runs, or one
	// after a write of its statement that runs code the catalog stores) binds no table's
	// definitions. Like a write that names no one table, it takes its stages from the
	// bundle of the table each record is written to, served for the definitions that record's
	// write context reads.
	let may_meet_change = !cx.eager() || cx.catalog_may_change() || cx.after_stored_code();
	// A write of request code that names its one table, whose definitions it reads as the bind
	// does, runs its stages as the bundle the compile links for that table rather than binding them
	// as ops of its own.
	let linked = match (&bundle, &named) {
		(None, Some(named)) if !may_meet_change && links_stages(cx, op, write, facts, named) => {
			Some(named.clone())
		}
		_ => None,
	};
	let table = match bundle {
		Some(bundle) => Some(bundle.table),
		None if may_meet_change || linked.is_some() => None,
		None => named.clone(),
	};
	// Such a write may meet definitions that change between its runs as well, so it checks the
	// write contexts it keeps against the catalog before taking them again.
	let catalog_fixed =
		catalog_fixed_of(write, cx.registry()) && !(table.is_none() && may_meet_change);
	cx.note_write(write, named.as_ref());
	let gates = gates_of(cx.catalog(), table.as_ref());
	let mut attrs = write_attrs_of(write, facts, &run)?;
	attrs.statement = table.is_none().then(|| Arc::new(write.clone()));
	attrs.linked = linked.map(|table| {
		Box::new(LinkedStages {
			table,
			run: run.clone().into_boxed_slice(),
		})
	});
	attrs.gates = gates.gates;
	attrs.image = image_of(cx.catalog(), table.as_ref());
	attrs.stored = stored_of(cx.catalog(), table.as_ref(), catalog_fixed);
	let schema = schema_of(cx.catalog(), cx.registry(), table.as_ref());
	attrs.stored.schema = schema.facts.clone();
	let indexes = indexes_of(cx.catalog(), table.as_ref());
	attrs.stored.index_set = indexes.facts.clone();
	// The clauses are stated only for the views the write's own facts declare, so that the facts
	// and the regions never come from two catalog answers that could differ.
	let views = match attrs.stored.views {
		Stored::Declared => views_of(cx.catalog(), table.as_ref()),
		Stored::Nothing | Stored::Dynamic | Stored::LateBound => TableViews::default(),
	};
	attrs.stored.view_set = views.facts.clone();
	// A repair is stated only by a write that purges a record, so that the references of a table,
	// a database-wide read, are asked for only by a delete.
	let repairs =
		repairs_of(cx.catalog(), table.as_ref(), run.contains(&WriteStageKind::PurgeRecordData));
	attrs.stored.referenced = repairs.referenced;
	attrs.stored.edges =
		edges_of(cx.catalog(), table.as_ref(), run.contains(&WriteStageKind::PurgeRecordData));
	attrs.stored.repair_set = repairs.facts.clone();
	// The layers of the table's field view are asked for only by a write whose own facts say the
	// view derives a value or cuts a field, so the facts and the regions never come from two
	// catalog answers that could differ.
	let field_view = field_view_of(cx.registry(), cx.catalog(), table.as_ref(), &mut attrs.image)?;
	attrs.stored.image_layers = field_view.layers;
	let observed = observed_of(cx.catalog(), table.as_ref());
	attrs.derived = gates.derived_attr(observed.as_deref());
	// A statement `UPDATE` or `DELETE` whose condition is a literal no record satisfies states it:
	// no record passes its `WHERE` stage. The stages after it are bound all the same, and the
	// physical phase decides from the table's definitions whether they are removed.
	attrs.flags.where_never = matches!(facts.form, PlanForm::Update | PlanForm::Delete)
		&& !bundled
		&& cond_verdict(write.cond.as_ref()) == CondVerdict::Never;
	let Some(into) = bind_into(cx, write)? else {
		return Ok(Flow::Diverged);
	};
	let guaranteed = match guaranteed_table(cx, write) {
		Some((table, targets)) => {
			let Some(targets) = bind_all(cx, targets)? else {
				return Ok(Flow::Diverged);
			};
			Some((table, targets))
		}
		None => None,
	};
	let (source, order, guaranteed) = match guaranteed {
		Some((table, targets)) => {
			let rows = bind_stored_rows(cx, write, table)?;
			(rows, SourceOrder::Delivery, targets)
		}
		None => {
			let Some((source, order)) = bind_subjects(cx, write)? else {
				return Ok(Flow::Diverged);
			};
			(source, order, Vec::new())
		}
	};
	let linked = attrs.linked.is_some();
	let sink = LoopSink::Write(Box::new(WriteStages(run.clone().into_boxed_slice())));
	let entry = vec![TypeKnowledge::Unknown; loop_body_args(&sink)];
	let subject_loop = cx.emit(
		OpKind::Logical(LogOp::Loop {
			kind: LoopKind::Write,
			sink,
			order,
			step: None,
		}),
		&[source],
		&[TypeKnowledge::Unknown],
	)?;
	// The legality the plan states of the pipeline that reads the rows of a write is the legality
	// of its loop.
	match &write.source {
		LogicalWriteSource::Rows(rows) => cx.note_loop(subject_loop, rows),
		// The pipeline of a write over the ids of a range stands at the write: it has no rows
		// below it to read.
		LogicalWriteSource::Targets(targets) if is_range_mock(targets) => {
			cx.note_loop(subject_loop, op);
		}
		LogicalWriteSource::Targets(_) | LogicalWriteSource::Inserts(_) => {}
	}
	cx.within(subject_loop, RegionKind::Loop, &entry, |cx, args| {
		let element = args.first().copied().ok_or(IrError::Stale("loop element"))?;
		let stored = StoredClauses {
			schema: &schema,
			indexes: &indexes,
			views: &views,
			repairs: &repairs,
		};
		if !linked {
			bind_stages(cx, write, &run, &gates, observed.as_deref(), &stored)?;
		}
		cx.yield_logical(&[element])
	})?;
	let subjects = first_result(cx, subject_loop)?;
	let mut operands = vec![subjects];
	operands.extend(into);
	operands.extend(guaranteed);
	let output = write.statement.output().and_then(|output| match output {
		Output::Fields(fields) => Some(fields),
		Output::None | Output::Null | Output::Diff | Output::After | Output::Before => None,
	});
	let op = cx.emit(
		OpKind::Logical(LogOp::Write(Box::new(attrs))),
		&operands,
		&[TypeKnowledge::Unknown],
	)?;
	if let Some(fields) = output {
		let mut fields = lower_fields(fields.clone()).map_err(|error| {
			invariant(&format!("the RETURN clause of a write has no logical form: {error}"))
		})?;
		crate::expr::logical::trace::mark_fields(&mut fields, write.trace);
		// The columns bind `$parent` from the frame around the write, as the clause does, so the
		// record the projection reads is never its own `$parent`.
		enter_row_region(cx, op, RegionKind::Clause(ClauseRole::ReturnClause), |cx, row| {
			let projected = bind_projection(cx, &fields, &[], false, Vec::new(), row)?;
			cx.yield_expr(&[projected])
		})?;
	}
	if let Some(view) = &field_view.view {
		for (fields, bodies) in view {
			bind_layer(cx, op, fields, bodies)?;
		}
	}
	first_result(cx, op).map(Flow::Value)
}

/// The table the one target of `write` names records of, where the target is a request variable
/// the plan is kept for a class of ([`SemanticCatalog::target_table`]): the variable is read when
/// the write runs, and holds a record, records or the table of that one table in every run.
#[allow(clippy::wildcard_enum_match_arm)]
fn target_variable_table(catalog: &dyn SemanticCatalog, write: &LogicalWrite) -> Option<TableName> {
	match &write.source {
		LogicalWriteSource::Targets(targets) => match targets.as_slice() {
			[LogicalExpr::Param(param)]
				if !matches!(write.statement, LogicalWriteStatement::Relate(_)) =>
			{
				catalog.target_table(param.as_str())
			}
			_ => None,
		},
		LogicalWriteSource::Rows(rows) => match rows.as_ref() {
			LogicalOp::Source(LogicalSource {
				source: SourceRef::Param(param),
				..
			}) => catalog.target_table(param.as_str()),
			_ => None,
		},
		LogicalWriteSource::Inserts(_) => None,
	}
}

/// Whether `write`, the write `op` is, of form `facts.form` and naming the one table `table`, runs
/// its stages as the bundle the compile links for that table ([`LinkedStages`]).
///
/// Only a write of request code the container links for ([`BindCx::links_stages`]) does, and only
/// one whose subjects are the records it names one by one: its targets, where no target is a mock
/// the plan may fan out and a whole table only where the write creates one record of it (a
/// `CREATE` or a `RELATE`'s edge table), or the rows of one record id or of a variable, never a
/// scan. The table is one the catalog defines, and its definitions fix what the write runs
/// ([`catalog_fixed_of`]). An `UPSERT` under a condition reads its subjects, and a condition no
/// record satisfies has its stages pruned by the physical phase, so neither is linked. Nor is a
/// write whose clauses hold a statement or a closure, whose code the region constraints judge
/// where the request wrote it.
#[allow(clippy::wildcard_enum_match_arm)]
fn links_stages(
	cx: &BindCx<'_>,
	op: &LogicalOp,
	write: &LogicalWrite,
	facts: WriteFacts,
	table: &TableName,
) -> bool {
	if !cx.links_stages() || facts.form == PlanForm::View {
		return false;
	}
	// A `CREATE` writes one record of a table it names, and a `RELATE` one record of its edge
	// table.
	let creates = matches!(
		write.statement,
		LogicalWriteStatement::Create(_) | LogicalWriteStatement::Relate(_)
	);
	let points = match &write.source {
		// Records the targets name one by one: no mock, whose records the plan may fan out, and
		// a whole table only where the write creates one record of it.
		LogicalWriteSource::Targets(targets) => targets.iter().all(|target| match target {
			LogicalExpr::Mock(_) => false,
			LogicalExpr::Table(_) => creates,
			_ => true,
		}),
		// The rows of a record, or of records a variable names: no scan the plan may cut.
		LogicalWriteSource::Rows(rows) => {
			matches!(
				rows.as_ref(),
				LogicalOp::Source(LogicalSource {
					source: SourceRef::Param(_),
					with: None,
					for_update: false,
					..
				})
			) || matches!(
				rows.as_ref(),
				LogicalOp::Source(LogicalSource {
					source: SourceRef::RecordId(rid),
					with: None,
					for_update: false,
					..
				}) if !matches!(rid.key, crate::expr::logical::LogicalRecordIdKey::Range(_))
			)
		}
		LogicalWriteSource::Inserts(_) => false,
	};
	if !points
		|| (matches!(write.statement, LogicalWriteStatement::Upsert(_)) && write.cond.is_some())
		|| cond_verdict(write.cond.as_ref()) == CondVerdict::Never
		|| !catalog_fixed_of(write, cx.registry())
		|| !matches!(cx.catalog().table(table), CatalogAnswer::Found(_))
	{
		return false;
	}
	let mut plain = true;
	let mut judge = |expr: &LogicalExpr| {
		let _ = expr.visit_deep(&mut |node| match node {
			LogicalExpr::Subquery(_) | LogicalExpr::Closure(_) => {
				plain = false;
				std::ops::ControlFlow::Break(())
			}
			_ => std::ops::ControlFlow::Continue(()),
		});
	};
	let _ = op.visit_scalars(&mut |scalar| {
		judge(scalar);
		Ok::<(), std::convert::Infallible>(())
	});
	if let Some(Output::Fields(fields)) = write.statement.output() {
		match lower_fields(fields.clone()) {
			Ok(LogicalFields::Value {
				expr,
				..
			}) => judge(&expr),
			Ok(LogicalFields::Select(fields)) => {
				for field in &fields {
					if let LogicalField::Single {
						expr,
						..
					} = field
					{
						judge(expr);
					}
				}
			}
			Err(_) => plain = false,
		}
	}
	plain
}

/// What a write states of how its table's field view is applied to an image: the attribute, and
/// the layers whose columns are the regions the write's op hosts.
struct FieldViewOf {
	layers: ImageLayers,
	/// The columns of each layer, with the bodies of the computed fields its guards read.
	view: Option<Vec<(LogicalFields, Vec<Derivation>)>>,
}

/// The field view of `table` as the catalog answers it, for a write whose `image` says the view
/// derives a value or cuts a field. Any other write states none.
///
/// An answer the catalog cannot give, because the layers are not loaded or the catalog changed
/// before the statement, leaves the write's image [`ImageView::LateBound`], so that a lowering
/// refuses and never shows an image through a view the plan did not describe.
fn field_view_of(
	registry: &FunctionRegistry,
	catalog: &dyn SemanticCatalog,
	table: Option<&TableName>,
	image: &mut WriteImage,
) -> Result<FieldViewOf, IrError> {
	let none = FieldViewOf {
		layers: ImageLayers::Unstated,
		view: None,
	};
	let (Some(table), ImageView::Cut | ImageView::Derived) = (table, image.view) else {
		return Ok(none);
	};
	match catalog.field_view(table) {
		CatalogAnswer::Found(found) => match &*found {
			FieldView::Layers(layers) => {
				let mut stack = Vec::with_capacity(layers.len());
				let mut view = Vec::with_capacity(layers.len());
				for (fields, schema) in layers {
					let (layer, bodies) =
						subscriber_layer(registry, catalog, Some(table), fields, schema)?;
					stack.push(layer);
					view.push((fields.clone(), bodies));
				}
				Ok(FieldViewOf {
					layers: ImageLayers::Stack(stack),
					view: Some(view),
				})
			}
			FieldView::Enforced => Ok(FieldViewOf {
				layers: ImageLayers::Enforced,
				view: None,
			}),
		},
		CatalogAnswer::Absent | CatalogAnswer::NotLoaded | CatalogAnswer::Unknown => {
			*image = WriteImage {
				view: ImageView::LateBound,
				keyed: false,
				id_default: false,
			};
			Ok(none)
		}
	}
}

/// The attributes of the `logical.write` of `write`.
///
/// A statement with no `RETURN` clause states [`OutputShape::Default`], which is not `RETURN
/// AFTER`: the record after the write is shown only where the write's order leaves one. `WITH` is
/// held by the source op of the rows it applies to and `EXPLAIN` by the `logical.explain` node
/// plan wraps the statement in, so the write holds neither. A statement that updates the
/// record already there, an `INSERT … ON DUPLICATE KEY UPDATE` or a `RELATE OR UPDATE`, states
/// `or_update`. The body is that of the orders `facts` names over the stages `run`.
fn write_attrs_of(
	write: &LogicalWrite,
	facts: WriteFacts,
	run: &[WriteStageKind],
) -> Result<WriteAttrs, IrError> {
	let statement = &write.statement;
	let form = match statement {
		LogicalWriteStatement::Create(_) => WriteForm::Create,
		LogicalWriteStatement::Update(_) => WriteForm::Update,
		LogicalWriteStatement::Upsert(_) => WriteForm::Upsert,
		LogicalWriteStatement::Delete(_) => WriteForm::Delete,
		LogicalWriteStatement::Insert(_) => WriteForm::Insert,
		LogicalWriteStatement::Relate(_) => WriteForm::Relate,
	};
	let output = match statement.output() {
		None => OutputShape::Default,
		Some(Output::After) => OutputShape::After,
		Some(Output::None) => OutputShape::None,
		Some(Output::Null) => OutputShape::Null,
		Some(Output::Diff) => OutputShape::Diff,
		Some(Output::Before) => OutputShape::Before,
		Some(Output::Fields(_)) => OutputShape::Fields,
	};
	let (ignore, relation, relate_or_update) = match statement {
		LogicalWriteStatement::Insert(insert) => (insert.ignore, insert.relation, false),
		LogicalWriteStatement::Relate(relate) => (false, false, relate.or_update),
		LogicalWriteStatement::Create(_)
		| LogicalWriteStatement::Update(_)
		| LogicalWriteStatement::Upsert(_)
		| LogicalWriteStatement::Delete(_) => (false, false, false),
	};
	Ok(WriteAttrs {
		form,
		only: statement.only(),
		output,
		with: None,
		explain: None,
		flags: WriteFlags {
			ignore,
			relation,
			or_update: write.duplicate_update.is_some() || relate_or_update,
			where_never: false,
		},
		body: write_body(facts.form, facts, run, write.cond.is_some())?,
		gates: WriteGates::default(),
		image: WriteImage::default(),
		stored: WriteStored::default(),
		derived: WriteDerived::default(),
		into_text: into_text_of(write),
		statement: None,
		linked: None,
	})
}

/// The `INTO` table of an `INSERT`, evaluated once before its rows. The outer `None` is a
/// statement that ended its block; the inner one is a statement with no `INTO`.
fn bind_into(
	cx: &mut BindCx<'_>,
	write: &LogicalWrite,
) -> Result<Option<Option<ValueId>>, IrError> {
	let LogicalWriteSource::Inserts(inserts) = &write.source else {
		return Ok(Some(None));
	};
	let Some(into) = &inserts.into else {
		return Ok(Some(None));
	};
	match bind_expr(cx, into)? {
		Built::Value(value) => Ok(Some(Some(value))),
		Built::Diverged => Ok(None),
	}
}

/// The table an `UPSERT` under a condition reads its subjects from, with the targets it names: the
/// records the condition admits are read through the access path a read under the same condition
/// takes, and the write guarantees the record its target names when the condition admits none.
///
/// `None` for any other write, for an `UPSERT` that names anything but one table, and for one whose
/// table the bind cannot read as defined (a table the catalog does not hold has no records to read,
/// and a write that may meet a changed catalog reads the catalog as it runs): each of those writes
/// the records its targets name.
fn guaranteed_table<'w>(
	cx: &BindCx<'_>,
	write: &'w LogicalWrite,
) -> Option<(TableName, &'w [LogicalExpr])> {
	let (LogicalWriteStatement::Upsert(_), LogicalWriteSource::Targets(targets), Some(_)) =
		(&write.statement, &write.source, &write.cond)
	else {
		return None;
	};
	if !cx.eager() || cx.catalog_may_change() || cx.after_stored_code() {
		return None;
	}
	let [LogicalExpr::Table(table)] = targets.as_slice() else {
		return None;
	};
	match cx.catalog().table(table) {
		CatalogAnswer::Found(_) => Some((table.clone(), targets.as_slice())),
		CatalogAnswer::Absent | CatalogAnswer::NotLoaded | CatalogAnswer::Unknown => None,
	}
}

/// The records of `table` as stored, the rows an `UPSERT` under a condition reads: no permission
/// or layer stands over them, since the write's gate and stages judge each one.
fn bind_stored_rows(
	cx: &mut BindCx<'_>,
	write: &LogicalWrite,
	table: TableName,
) -> Result<ValueId, IrError> {
	let with = match &write.statement {
		LogicalWriteStatement::Upsert(stmt) => stmt.with.clone(),
		LogicalWriteStatement::Create(_)
		| LogicalWriteStatement::Update(_)
		| LogicalWriteStatement::Delete(_)
		| LogicalWriteStatement::Insert(_)
		| LogicalWriteStatement::Relate(_) => None,
	};
	let attrs = SourceAttrs {
		source_ref: SourceRefShape::Table(table),
		with,
		for_update: false,
		may_be_defined: cx.tables_may_be_defined(),
		may_meet_change: cx.nested_read_may_meet_change(),
		resolved: false,
	};
	let op =
		cx.emit(OpKind::Logical(LogOp::Source(Box::new(attrs))), &[], &[TypeKnowledge::Unknown])?;
	first_result(cx, op)
}

/// The subjects of `write` as the one operand of its loop, and the order the loop consumes them
/// in. `None` when binding them ended the block.
///
/// Targets are gathered in an array, `RELATE`'s as `[from, through, to]`. An `INSERT`'s rows are
/// one value, a literal list staying one op whatever it holds. The rows of a mutation of existing
/// records are the relational plan's, in the order its access path delivers them.
fn bind_subjects(
	cx: &mut BindCx<'_>,
	write: &LogicalWrite,
) -> Result<Option<(ValueId, SourceOrder)>, IrError> {
	match &write.source {
		LogicalWriteSource::Targets(targets) => {
			let Some(values) = bind_all(cx, targets)? else {
				return Ok(None);
			};
			let array =
				cx.emit_value(OpKind::Expr(ExprOp::Array), &values, TypeKnowledge::Unknown)?;
			Ok(Some((array, SourceOrder::Positional)))
		}
		LogicalWriteSource::Inserts(inserts) => match &inserts.rows {
			LogicalInsertRows::Value(rows) => match bind_expr(cx, rows)? {
				Built::Value(value) => Ok(Some((value, SourceOrder::Positional))),
				Built::Diverged => Ok(None),
			},
			LogicalInsertRows::Tuples(tuples) => {
				let mut rows = Vec::with_capacity(tuples.len());
				for tuple in tuples {
					let Some(row) = bind_tuple(cx, tuple)? else {
						return Ok(None);
					};
					rows.push(row);
				}
				let array =
					cx.emit_value(OpKind::Expr(ExprOp::Array), &rows, TypeKnowledge::Unknown)?;
				Ok(Some((array, SourceOrder::Positional)))
			}
		},
		LogicalWriteSource::Rows(rows) => match value_targets(rows) {
			Some(ValueTargets {
				targets,
				..
			}) => {
				let Some(values) = bind_all(cx, &targets)? else {
					return Ok(None);
				};
				let array =
					cx.emit_value(OpKind::Expr(ExprOp::Array), &values, TypeKnowledge::Unknown)?;
				Ok(Some((array, SourceOrder::Positional)))
			}
			None => Ok(bind_value(cx, rows)?.map(|rows| (rows, SourceOrder::Delivery))),
		},
	}
}

/// What a refused `UPDATE` or `DELETE` is answered with when its targets mix stored records with
/// values under a nearest-neighbour condition ([`mixed_targets_search_refusal`]).
const MIXED_TARGETS_UNDER_KNN: &str = "A nearest-neighbour (KNN) condition cannot be used on a \
	write whose targets mix a table or record with a subquery, parameter or value. Write those \
	targets in separate statements instead.";

/// What a refused `UPDATE` or `DELETE` is answered with when its targets list a nested `SELECT`
/// beside other targets, and no table, under a nearest-neighbour or full-text condition
/// ([`mixed_targets_search_refusal`]).
const LISTED_SUBQUERY_UNDER_SEARCH: &str = "A nearest-neighbour (KNN) or full-text condition \
	cannot be used on a write whose targets list a subquery beside other targets but no table. \
	Write those targets in separate statements instead.";

/// The targets a write's rows name, written as targets rather than read as rows
/// ([`value_targets`]).
#[derive(Debug, PartialEq)]
struct ValueTargets {
	/// Every target, in the order the statement wrote them.
	targets: Vec<LogicalExpr>,
	/// Whether a table, a record id or range, or a mock is among them beside the values.
	stored: bool,
	/// Whether a table is among them beside the values.
	table: bool,
	/// Whether a nested `SELECT` is among them.
	subquery: bool,
}

/// The targets a write's rows name when they are values, in the order the statement wrote them.
/// `None` for rows the relational plan reads.
///
/// A value is an expression, a parameter or a call, unlocked. One alone is a target, and so is
/// every leaf of a union that holds at least one value or nested `SELECT`, when each other leaf
/// names stored records: a table, a record id or range, or a mock, unlocked. Each value is
/// classified as a target written in place is: a record id or an object whose `id` names one is
/// the stored record, which the write reads from storage; a table is its records; any other value
/// is refused when the write runs. Read as rows beside a table's records, a value would be written
/// as the record it names, its own fields in place of the stored ones, so a union holding one is
/// written as targets whole. Each target is evaluated when the write reaches it, after the
/// subjects of every target before it were written.
///
/// The rows path keeps a table alone, a union naming only stored records (whose read of a table
/// can take an index), a nested `SELECT` alone, and a locked target.
fn value_targets(rows: &LogicalOp) -> Option<ValueTargets> {
	let mut targets = Vec::new();
	let listed = if let LogicalOp::UnionAll(union) = rows {
		let listed = listed_targets(&union.inputs, &mut targets)?;
		if !listed.valued {
			return None;
		}
		listed
	} else {
		targets.push(value_target(rows)?);
		Listed::default()
	};
	Some(ValueTargets {
		targets,
		stored: listed.stored,
		table: listed.table,
		subquery: listed.subquery,
	})
}

/// What the leaves of a union of write targets hold ([`listed_targets`]).
#[derive(Clone, Copy, Default)]
struct Listed {
	/// A value or a nested `SELECT` is among them.
	valued: bool,
	/// A table, a record id or range, or a mock is among them.
	stored: bool,
	/// A table is among them.
	table: bool,
	/// A nested `SELECT` is among them.
	subquery: bool,
}

/// Pushes the target each leaf of `inputs` names onto `targets`, in order, nested unions
/// flattened. `None` when a leaf is neither a value, a nested `SELECT`, nor stored records.
fn listed_targets(inputs: &[LogicalOp], targets: &mut Vec<LogicalExpr>) -> Option<Listed> {
	let mut listed = Listed::default();
	for input in inputs {
		if let LogicalOp::UnionAll(union) = input {
			let nested = listed_targets(&union.inputs, targets)?;
			listed.valued |= nested.valued;
			listed.stored |= nested.stored;
			listed.table |= nested.table;
			listed.subquery |= nested.subquery;
		} else if let LogicalOp::Subquery(subquery) = input {
			targets.push(LogicalExpr::Subquery(subquery.input.clone()));
			listed.valued = true;
			listed.subquery = true;
		} else if let LogicalOp::Source(LogicalSource {
			source,
			for_update: false,
			..
		}) = input
		{
			if let Some(stored) = stored_target(source) {
				targets.push(stored);
				listed.stored = true;
				listed.table |= matches!(source, SourceRef::Table(_));
			} else {
				targets.push(value_target(input)?);
				listed.valued = true;
			}
		} else {
			targets.push(value_target(input)?);
			listed.valued = true;
		}
	}
	Some(listed)
}

/// The target a source naming stored records is written as: a table, a record id or range, or a
/// mock. `None` for any other source.
fn stored_target(source: &SourceRef) -> Option<LogicalExpr> {
	match source {
		SourceRef::Table(table) => Some(LogicalExpr::Table(table.clone())),
		SourceRef::RecordId(rid) => Some(LogicalExpr::RecordId(rid.clone())),
		SourceRef::Mock(mock) => Some(LogicalExpr::Mock(mock.clone())),
		SourceRef::Param(_) | SourceRef::Dynamic(_) => None,
	}
}

/// The value a leaf of a write's rows is, unlocked: a literal, a parameter or a dynamic
/// expression. `None` for any other leaf.
fn value_target(leaf: &LogicalOp) -> Option<LogicalExpr> {
	if let LogicalOp::Values(values) = leaf {
		(!values.for_update).then(|| values.expr.clone())
	} else if let LogicalOp::Source(source) = leaf {
		if !source.for_update {
			match &source.source {
				SourceRef::Param(param) => Some(LogicalExpr::Param(param.clone())),
				SourceRef::Dynamic(expr) => Some(expr.clone()),
				SourceRef::Table(_) | SourceRef::RecordId(_) | SourceRef::Mock(_) => None,
			}
		} else {
			None
		}
	} else {
		None
	}
}

/// The refusal of an `UPDATE` or `DELETE` written as a list of targets ([`value_targets`]) whose
/// condition searches an index the list cannot be answered from:
///
/// - a nearest-neighbour condition over a list holding stored records ([`ValueTargets::stored`]) or
///   a nested `SELECT` ([`ValueTargets::subquery`]) beside its other targets. A write over targets
///   runs no search, so the condition would match no record of a table the list names whole, and a
///   read of the list would write a value as the record it names;
/// - a full-text condition over a list holding a nested `SELECT` and no table
///   ([`ValueTargets::table`]). The condition is answered from the full-text index for the records
///   of a table the statement names whole only, so over such a list it would match no record, where
///   a read of the list would judge, and write, each value as the record it names.
///
/// A value, or a list of values alone, is written as the same statement naming each would be, and
/// is not refused.
/// The refusal of an `UPSERT` whose condition holds a `KNN` operator. An `UPSERT` creates a record
/// when its condition matches none, which a nearest-neighbour condition has no meaning for.
fn upsert_knn_refusal(write: &LogicalWrite) -> Option<Refusal> {
	if !matches!(write.statement, LogicalWriteStatement::Upsert(_)) {
		return None;
	}
	let cond = write.cond.as_ref()?;
	crate::exec::planner::util::has_knn_operator(cond).then(|| Refusal::Query {
		message: UPSERT_KNN_REFUSED.to_owned(),
	})
}

/// What an `UPSERT` whose condition holds a `KNN` operator is answered with
/// ([`upsert_knn_refusal`]).
const UPSERT_KNN_REFUSED: &str = "A KNN operator is not supported in the WHERE of an UPSERT; use \
	UPDATE, or UPSERT the ids a SELECT returns.";

fn mixed_targets_search_refusal(write: &LogicalWrite) -> Option<Refusal> {
	let LogicalWriteSource::Rows(rows) = &write.source else {
		return None;
	};
	let cond = write.cond.as_ref()?;
	let knn = crate::exec::planner::util::has_knn_operator(cond);
	let matches = crate::exec::planner::util::expr_contains_matches(cond);
	if !knn && !matches {
		return None;
	}
	let targets = value_targets(rows)?;
	let message = if knn && targets.stored {
		MIXED_TARGETS_UNDER_KNN
	} else if targets.subquery && (knn || !targets.table) {
		LISTED_SUBQUERY_UNDER_SEARCH
	} else {
		return None;
	};
	Some(Refusal::Query {
		message: message.to_owned(),
	})
}

/// One tuple of an `INSERT … VALUES`: an `expr.tuple_row` that writes each value at the place it is
/// paired with, in the order the tuple wrote them. `None` when a value or a subscript ended the
/// block.
///
/// The row is bound whatever its places are, a plain field included, so that every tuple takes the
/// one path the statement applies a tuple by: a later value at a place replaces an earlier one, a
/// place below one an earlier value ended at is written as the path walk writes it, and a place
/// holding a subscript or a filter is walked with them evaluated as the write reaches them. The
/// values are the first operands and the subscripts of every place follow them in order, so a
/// value is bound before the subscripts of its own place, as the statement evaluates it.
fn bind_tuple(
	cx: &mut BindCx<'_>,
	tuple: &[(Idiom, LogicalExpr)],
) -> Result<Option<ValueId>, IrError> {
	let count = u16::try_from(tuple.len()).map_err(|_| IrError::Capacity("tuple entries"))?;
	let mut values = Vec::with_capacity(tuple.len());
	let mut subscripts: Vec<ValueId> = Vec::new();
	let mut filters = Vec::new();
	let mut places = Vec::with_capacity(tuple.len());
	for (place, value) in tuple {
		if place.0.is_empty() {
			return Err(invariant("a tuple pairs a value with an empty place"));
		}
		let Built::Value(value) = bind_expr(cx, value)? else {
			return Ok(None);
		};
		values.push(value);
		let first_slot = u16::try_from(subscripts.len())
			.ok()
			.and_then(|held| held.checked_add(count))
			.ok_or(IrError::Capacity("place operands"))?;
		let first_region =
			u16::try_from(filters.len()).map_err(|_| IrError::Capacity("place filters"))?;
		let Some(bound) = bind_place(cx, place, first_slot, first_region)? else {
			return Ok(None);
		};
		subscripts.extend(bound.operands);
		filters.extend(bound.filters);
		places.push(bound.place);
	}
	values.extend(subscripts);
	let op = cx.emit(
		OpKind::Expr(ExprOp::TupleRow {
			places: places.into_boxed_slice(),
		}),
		&values,
		&[TypeKnowledge::Unknown],
	)?;
	bind_filters(cx, op, &filters)?;
	first_result(cx, op).map(Some)
}

/// Binds `exprs` in order. `None` when one of them ended the block.
fn bind_all(cx: &mut BindCx<'_>, exprs: &[LogicalExpr]) -> Result<Option<Vec<ValueId>>, IrError> {
	let mut values = Vec::with_capacity(exprs.len());
	for expr in exprs {
		let Built::Value(value) = bind_expr(cx, expr)? else {
			return Ok(None);
		};
		values.push(value);
	}
	Ok(Some(values))
}

/// The clauses a written table stores that the stages of a write host, as [`bind_stages`] binds
/// them.
#[derive(Clone, Copy)]
struct StoredClauses<'a> {
	schema: &'a TableSchema,
	indexes: &'a TableIndexes,
	views: &'a TableViews,
	repairs: &'a TableRepairs,
}

/// The body of the subject loop: one `logical.write_stage` per stage of `run`, in order.
///
/// The data clauses are bound as regions of the stage that evaluates them, and so is the
/// condition, so a clause that no stage of the run evaluates is a broken invariant and never
/// dropped. A permission gate whose clause is a predicate hosts it as a `PermissionFrame` region,
/// judged in the frame of its guard; the clause a gate judges is stated by the write's
/// [`WriteGates`] and never read from whether a region is there. The computed fields a predicate
/// derives onto its copy of the image follow it, one `FieldFrame(Computed)` region each in the
/// order [`WriteDerived`] lists them, bound in the derived frame of a gate (a permission frame
/// under the field's `AUTH LIMIT`), and `MaterialiseObservedFields` hosts every computed field of
/// the table the same way in the computed frame. `ProcessTableFields` hosts the clauses of the
/// table's fields, by kind, in the order [`WriteSchema::region_counts`] lists, each in the frame
/// of its field, `GenerateRecordId` hosts the `DEFAULT` of the `id` field, and `StoreIndexData`
/// hosts the columns and guards of the table's indexes in the order `WriteIndexes` lists, and
/// `ProcessTableViews` hosts the clauses of the views the table feeds in the order `WriteViews`
/// lists.
///
/// [`WriteSchema::region_counts`]: crate::exec::ir::logical::ops::WriteSchema::region_counts
fn bind_stages(
	cx: &mut BindCx<'_>,
	write: &LogicalWrite,
	run: &[WriteStageKind],
	gates: &TableGates,
	observed: Option<&[Derivation]>,
	stored: &StoredClauses<'_>,
) -> Result<(), IrError> {
	let StoredClauses {
		schema,
		indexes,
		views,
		repairs,
	} = *stored;
	let mut data_pending = write.data.is_some() || write.duplicate_update.is_some();
	let mut cond_pending = write.cond.is_some();
	for stage in run.iter().copied() {
		let op = cx.emit(
			OpKind::Logical(LogOp::WriteStage {
				stage,
				mark: static_mark(stage),
			}),
			&[],
			&[],
		)?;
		cx.within(op, RegionKind::WriteStage, &[timestamp_arg()], |cx, _| cx.yield_logical(&[]))?;
		if let Some((Some(predicate), guard)) = gates.hosted(stage) {
			let eval = match guard {
				GuardKind::Read => EvalFrame::permission(None),
				GuardKind::Write => EvalFrame::mutable_permission(None),
			};
			framed_clause(cx, op, RegionKind::PermissionFrame, Some(&eval), predicate)?;
			for derivation in gates.derived(stage) {
				let eval = EvalFrame::permission(derivation.auth.clone());
				let kind = RegionKind::FieldFrame(FieldRole::Computed);
				framed_clause(cx, op, kind, Some(&eval), &derivation.body)?;
			}
		}
		if stage == WriteStageKind::ProcessTableFields {
			for region in &schema.regions {
				bind_schema_region(cx, op, region)?;
			}
		}
		if stage == WriteStageKind::GenerateRecordId
			&& let Some(region) = &schema.id_default
		{
			bind_schema_region(cx, op, region)?;
		}
		if stage == WriteStageKind::StoreIndexData {
			for region in &indexes.regions {
				bind_schema_region(cx, op, region)?;
			}
		}
		if matches!(stage, WriteStageKind::ProcessTableViews(_)) {
			for body in &views.regions {
				bind_view_region(cx, op, body)?;
			}
		}
		if stage == WriteStageKind::PurgeRecordData {
			for body in &repairs.regions {
				bind_repair_region(cx, op, body)?;
			}
		}
		if stage == WriteStageKind::MaterialiseObservedFields {
			for derivation in observed.unwrap_or_default() {
				let eval = EvalFrame::computed(derivation.auth.clone());
				let kind = RegionKind::FieldFrame(FieldRole::Computed);
				framed_clause(cx, op, kind, Some(&eval), &derivation.body)?;
			}
		}
		if stage == WriteStageKind::ComputeInputData {
			data_pending = false;
			if let Some(data) = &write.data {
				bind_data(cx, op, data, false)?;
			}
			if let Some(data) = &write.duplicate_update {
				bind_data(cx, op, data, true)?;
			}
		} else if stage == WriteStageKind::CheckWhereCondition {
			cond_pending = false;
			if let Some(cond) = &write.cond {
				row_clause(cx, op, RegionKind::Clause(ClauseRole::WriteCond), cond)?;
			}
		}
	}
	if data_pending || cond_pending {
		return Err(invariant("a clause of a write is evaluated by no stage of its order"));
	}
	Ok(())
}

/// Binds the clause `body` of a view as a `ViewClause` region of `owner`, in the frame of a view:
/// permissions off and writes allowed. The region receives the image the clause is evaluated over.
fn bind_view_region(cx: &mut BindCx<'_>, owner: OpId, body: &ViewBody) -> Result<(), IrError> {
	let kind = RegionKind::Clause(ClauseRole::ViewClause);
	let eval = EvalFrame::mutable_permission(None);
	match body {
		ViewBody::Expr(expr) => framed_clause(cx, owner, kind, Some(&eval), expr),
		ViewBody::Fields(fields) => {
			let frame = cx.frame_for(&eval)?;
			cx.with_frame(frame, |cx| {
				enter_row_region(cx, owner, kind, |cx, row| {
					cx.with_row(row, |cx| {
						let projected = bind_projection(cx, fields, &[], false, Vec::new(), row)?;
						cx.yield_expr(&[projected])
					})
				})
			})
		}
	}
}

/// Binds the `THEN` expression `body` of a `REFERENCE` field as a `ReferenceRepair` region of
/// `owner`, in the frame a purge runs it in: permissions off and writes allowed. The region
/// receives the id of the referencing record as its row and the purged record as `$reference`.
fn bind_repair_region(cx: &mut BindCx<'_>, owner: OpId, body: &LogicalExpr) -> Result<(), IrError> {
	let eval = EvalFrame::mutable_permission(None);
	framed_clause(cx, owner, RegionKind::Clause(ClauseRole::ReferenceRepair), Some(&eval), body)
}

/// Binds the clause of a field `region` as a region of `owner`, in the frame it states. A clause of
/// a field's place binds the names of the place after the row.
pub(super) fn bind_schema_region(
	cx: &mut BindCx<'_>,
	owner: OpId,
	region: &SchemaRegion,
) -> Result<(), IrError> {
	if !region.place {
		return framed_clause(cx, owner, region.kind, Some(&region.frame), &region.body);
	}
	let frame = cx.frame_for(&region.frame)?;
	cx.with_frame(frame, |cx| {
		enter_named_region(cx, owner, region.kind, FIELD_VALUE, |cx, row| {
			cx.with_row(row, |cx| bind_body(cx, &region.body))
		})
	})
}

/// Binds the data clause `data`, hosted by `owner`, as one region: `SET` and the assignments of
/// `ON DUPLICATE KEY UPDATE` hold one `logical.assign` each, every other clause holds the ops that
/// say what it writes.
fn bind_data(
	cx: &mut BindCx<'_>,
	owner: OpId,
	data: &LogicalData,
	duplicate: bool,
) -> Result<(), IrError> {
	if duplicate {
		let LogicalData::Set(assignments) = data else {
			return Err(invariant("ON DUPLICATE KEY UPDATE holds assignments"));
		};
		return bind_assignments(cx, owner, ClauseRole::DuplicateUpdate, assignments);
	}
	match data {
		LogicalData::Set(assignments) => {
			bind_assignments(cx, owner, ClauseRole::SetValue, assignments)
		}
		LogicalData::Unset(paths) => {
			write_clause(cx, owner, RegionKind::Clause(ClauseRole::Content), |cx| {
				for path in paths {
					if !bind_unset(cx, path)? {
						return Ok(false);
					}
				}
				Ok(true)
			})
		}
		LogicalData::Patch(value) => bind_content(cx, owner, ContentMode::Patch, value),
		LogicalData::Merge(value) => bind_content(cx, owner, ContentMode::Merge, value),
		LogicalData::Replace(value) => bind_content(cx, owner, ContentMode::Replace, value),
		LogicalData::Content(value) => bind_content(cx, owner, ContentMode::Content, value),
		LogicalData::Single(value) => bind_content(cx, owner, ContentMode::Single, value),
		LogicalData::Values(_) => {
			Err(invariant("a data clause of tuples on a statement that is not an INSERT"))
		}
	}
}

/// The assignments of a `SET` or of `ON DUPLICATE KEY UPDATE` as the region of `role`, in order.
fn bind_assignments(
	cx: &mut BindCx<'_>,
	owner: OpId,
	role: ClauseRole,
	assignments: &[LogicalAssignment],
) -> Result<(), IrError> {
	write_clause(cx, owner, RegionKind::Clause(role), |cx| {
		for assignment in assignments {
			if !bind_assign(cx, assignment)? {
				return Ok(false);
			}
		}
		Ok(true)
	})
}

/// Binds a clause region of kind `kind` hosted by `owner`, which receives the row the clause is
/// evaluated against. `body` answers `false` when it ended the block, which it then closed itself.
fn write_clause(
	cx: &mut BindCx<'_>,
	owner: OpId,
	kind: RegionKind,
	body: impl FnOnce(&mut BindCx<'_>) -> Result<bool, IrError>,
) -> Result<(), IrError> {
	enter_row_region(cx, owner, kind, |cx, row| {
		cx.with_row(row, |cx| {
			if body(cx)? {
				cx.yield_expr(&[])?;
			}
			Ok(())
		})
	})
}

/// `CONTENT`, `MERGE`, `PATCH` and `REPLACE`: the value, then the op that stores it.
fn bind_content(
	cx: &mut BindCx<'_>,
	owner: OpId,
	mode: ContentMode,
	value: &LogicalExpr,
) -> Result<(), IrError> {
	write_clause(cx, owner, RegionKind::Clause(ClauseRole::Content), |cx| {
		let Built::Value(value) = bind_expr(cx, value)? else {
			return Ok(false);
		};
		cx.emit(
			OpKind::Logical(LogOp::Content {
				mode,
			}),
			&[value],
			&[],
		)?;
		Ok(true)
	})
}

/// One `place = value` of a `SET`: the value, then the dynamic parts of the place, then the op.
/// Answers `false` when an operand ended the block.
fn bind_assign(cx: &mut BindCx<'_>, assignment: &LogicalAssignment) -> Result<bool, IrError> {
	let Built::Value(value) = bind_expr(cx, &assignment.value)? else {
		return Ok(false);
	};
	let Some(bound) = bind_place(cx, &assignment.place, 1, 0)? else {
		return Ok(false);
	};
	let mut operands = vec![value];
	operands.extend(&bound.operands);
	let op = cx.emit(
		OpKind::Logical(LogOp::Assign(Box::new(AssignAttrs {
			place: bound.place,
			op: assignment.operator.clone(),
		}))),
		&operands,
		&[],
	)?;
	bind_filters(cx, op, &bound.filters)?;
	Ok(true)
}

/// One path of an `UNSET`: the dynamic parts of the place, then the op. Answers `false` when an
/// operand ended the block.
fn bind_unset(cx: &mut BindCx<'_>, path: &Idiom) -> Result<bool, IrError> {
	let Some(bound) = bind_place(cx, path, 0, 0)? else {
		return Ok(false);
	};
	let op = cx.emit(
		OpKind::Logical(LogOp::Unset {
			place: bound.place,
		}),
		&bound.operands,
		&[],
	)?;
	bind_filters(cx, op, &bound.filters)?;
	Ok(true)
}

/// Binds the `[WHERE ..]` predicate of each filter part of a place as a region of the op that
/// carries the place, in the order of the parts. A `LET` written as an expression in one is raised
/// when the place is evaluated for a record ([`BindCx::deferring_let`]).
fn bind_filters(cx: &mut BindCx<'_>, op: OpId, filters: &[LogicalExpr]) -> Result<(), IrError> {
	cx.deferring_let(|cx| {
		for filter in filters {
			row_clause(cx, op, RegionKind::Clause(ClauseRole::Where), filter)?;
		}
		Ok(())
	})
}

/// A place and what its parts hold.
struct BoundPlace {
	place: Place,
	/// The operands of the dynamic parts, in the order of the parts.
	operands: Vec<ValueId>,
	/// The predicates of the filter parts, in the order of the parts.
	filters: Vec<LogicalExpr>,
}

/// Copies the parts of `idiom` into a place. A subscript names the operand that holds it and a
/// filter the region that does, and the subscript is bound as that operand now, in the order of
/// the parts. `None` when an operand ended the block.
///
/// The slot of the first subscript is `first_slot` and the slot of the first filter is
/// `first_region`: a place that stands beside others in one op follows the slots they took.
///
/// A subscript is evaluated with no record in scope, so it is bound against a row that holds
/// `NONE`: a field it names reads nothing, as it does when the statement runs. A `LET` written as
/// an expression in it is raised when the place is evaluated for a record. A part no place
/// addresses (a method call, `[START n]`, a lookup, a destructure, a recursion) embeds nothing
/// into the place, so the expressions it was written with are never bound and nothing in them is
/// evaluated. A subscript the statement wrote as a number is a [`PlacePart::Index`].
fn bind_place(
	cx: &mut BindCx<'_>,
	idiom: &Idiom,
	first_slot: u16,
	first_region: u16,
) -> Result<Option<BoundPlace>, IrError> {
	let mut parts = Vec::with_capacity(idiom.0.len());
	let mut operands = Vec::new();
	let mut filters = Vec::new();
	let mut rowless: Option<ValueId> = None;
	for part in &idiom.0 {
		let bound = match part {
			Part::All => PlacePart::All,
			Part::Flatten => PlacePart::Flatten,
			Part::Last => PlacePart::Last,
			Part::First => PlacePart::First,
			Part::Field(name) => PlacePart::Field(name.clone()),
			Part::Optional => PlacePart::Optional,
			Part::Doc => PlacePart::Doc,
			Part::RepeatRecurse => PlacePart::RepeatRecurse,
			Part::Where(expr) => {
				let slot = u16::try_from(filters.len())
					.ok()
					.and_then(|held| held.checked_add(first_region))
					.ok_or(IrError::Capacity("place filters"))?;
				filters.push(lower_part(expr)?);
				PlacePart::Filter(slot)
			}
			Part::Value(expr) => {
				let row = match rowless {
					Some(row) => row,
					None => {
						let row = cx.emit_value(
							OpKind::Expr(ExprOp::Literal(Box::new(crate::val::Value::None))),
							&[],
							TypeKnowledge::Unknown,
						)?;
						rowless = Some(row);
						row
					}
				};
				let Some(slot) = cx.with_row(row, |cx| {
					cx.deferring_let(|cx| bind_operand(cx, expr, first_slot, &mut operands))
				})?
				else {
					return Ok(None);
				};
				if part.as_old_index().is_some() {
					PlacePart::Index(slot)
				} else {
					PlacePart::Dynamic(slot)
				}
			}
			Part::Start(_) => {
				PlacePart::Unaddressed(UnaddressedPart::Start, part.to_sql().into_boxed_str())
			}
			Part::Method(..) => {
				PlacePart::Unaddressed(UnaddressedPart::Method, part.to_sql().into_boxed_str())
			}
			Part::Lookup(_) => {
				PlacePart::Unaddressed(UnaddressedPart::Lookup, part.to_sql().into_boxed_str())
			}
			Part::Destructure(_) => {
				PlacePart::Unaddressed(UnaddressedPart::Destructure, part.to_sql().into_boxed_str())
			}
			Part::Recurse(..) => {
				PlacePart::Unaddressed(UnaddressedPart::Recurse, part.to_sql().into_boxed_str())
			}
		};
		parts.push(bound);
	}
	Ok(Some(BoundPlace {
		place: Place {
			parts: parts.into_boxed_slice(),
		},
		operands,
		filters,
	}))
}

/// Binds `expr`, a subscript of a place, as the next operand of the op that carries the place, and
/// answers its slot. `None` when it ended the block.
fn bind_operand(
	cx: &mut BindCx<'_>,
	expr: &crate::expr::Expr,
	first_slot: u16,
	operands: &mut Vec<ValueId>,
) -> Result<Option<u16>, IrError> {
	let logical = lower_part(expr)?;
	let Built::Value(value) = bind_expr(cx, &logical)? else {
		return Ok(None);
	};
	let slot = u16::try_from(operands.len())
		.ok()
		.and_then(|held| held.checked_add(first_slot))
		.ok_or(IrError::Capacity("place operands"))?;
	operands.push(value);
	Ok(Some(slot))
}

/// The logical form of an expression a place part holds. A place keeps the expressions it was
/// written with, and lowering one fails only for a tree no parser builds.
fn lower_part(expr: &crate::expr::Expr) -> Result<LogicalExpr, IrError> {
	lower_expr(expr.clone()).map_err(|error| {
		invariant(&format!("an expression of a place has no logical form: {error}"))
	})
}

// ---------------------------------------------------------------------------------------------
// Loop sites and closures
// ---------------------------------------------------------------------------------------------

/// The row names a guard's row and a record a lookup reached answer to.
const ROW_NAMES: &[&str] = &["this", "self", "value"];

/// The row names the element of a `SELECT` or write loop answers to. `$value` is no name of the
/// element: it reads the parameter of the frame around the statement.
const STATEMENT_ROW_NAMES: &[&str] = &["this", "self"];

/// What kind of collection a closure builtin runs over, which decides the arguments it hands the
/// closure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Collection {
	Array,
	Set,
}

/// The closure builtins that bind as a loop, named by their name within their module.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ClosureForm {
	Map,
	Filter,
	Any,
	All,
	Find,
	FindIndex,
	Fold,
	Reduce,
}

impl ClosureForm {
	/// The form of the builtin or method called `name`, without its module.
	fn named(name: &str) -> Option<Self> {
		Some(match name {
			"map" => Self::Map,
			"filter" => Self::Filter,
			"any" | "some" | "includes" => Self::Any,
			"all" | "every" => Self::All,
			"find" => Self::Find,
			"find_index" | "index_of" => Self::FindIndex,
			"fold" => Self::Fold,
			"reduce" => Self::Reduce,
			_ => return None,
		})
	}

	/// The number of arguments the builtin takes, the collection and the closure included.
	fn arguments(self) -> usize {
		match self {
			Self::Fold => 3,
			Self::Map
			| Self::Filter
			| Self::Any
			| Self::All
			| Self::Find
			| Self::FindIndex
			| Self::Reduce => 2,
		}
	}

	/// What the loop does with the results of its iterations.
	fn sink(self) -> LoopSink {
		match self {
			Self::Map => LoopSink::Output,
			Self::Filter => LoopSink::Filter,
			Self::Any => LoopSink::Decisive(DecisiveKind::Any),
			Self::All => LoopSink::Decisive(DecisiveKind::All),
			Self::Find => LoopSink::Decisive(DecisiveKind::Find),
			Self::FindIndex => LoopSink::Decisive(DecisiveKind::FindIndex),
			Self::Fold | Self::Reduce => LoopSink::Fold,
		}
	}

	/// Whether the sink reads the closure's answer as a truth value.
	fn yields_truth(self) -> bool {
		match self {
			Self::Filter | Self::Any | Self::All | Self::Find | Self::FindIndex => true,
			Self::Map | Self::Fold | Self::Reduce => false,
		}
	}

	/// The values the closure is applied to in one iteration, as the builtin hands them over: the
	/// accumulator first for a fold, then the element, then its position where the builtin passes
	/// one (an array's `map`, `fold` and `reduce`).
	fn arguments_of(
		self,
		collection: Collection,
		element: ValueId,
		position: ValueId,
		accumulator: Option<ValueId>,
	) -> Vec<ValueId> {
		let with_position = collection == Collection::Array;
		match self {
			Self::Map if with_position => vec![element, position],
			Self::Fold | Self::Reduce => {
				let mut values: Vec<ValueId> = accumulator.into_iter().collect();
				values.push(element);
				if with_position {
					values.push(position);
				}
				values
			}
			Self::Map | Self::Filter | Self::Any | Self::All | Self::Find | Self::FindIndex => {
				vec![element]
			}
		}
	}
}

/// A closure builtin whose collection kind and form are known.
#[derive(Clone, Debug)]
struct ClosureSite {
	form: ClosureForm,
	collection: Collection,
	/// The builtin as the site wrote it: the function with its module, or the method name.
	name: Strand,
	/// Whether the site is written as a method.
	method: bool,
}

/// Binds the per-element form `site` as a `logical.loop` and answers the loop's result, or the
/// call or method op the site binds to as a plain call when it is not one of the closure shapes
/// that bind as a loop.
///
/// A `Closure` or `MethodClosure` site binds as a loop of kind `Closure` over the collection when
/// the builtin's form is one of `map`, `filter`, `any`, `all`, `find`, `find_index`, `fold` and
/// `reduce`, the closure is written in the call or bound to a name, and, for the method spelling,
/// the receiver is known to be an array or a set. Anything else of that shape is the plain call or
/// method op, evaluated as a plain call. The loop's operands are the collection and, for a
/// `fold`, the initial accumulator; a `reduce` has the collection alone and starts from its first
/// element. The body applies the closure value, created once before the loop, to the iteration's
/// values.
///
/// An element filter is a loop of kind `ElementFilter` whose body reads the element as `$this`
/// and as the row, and the enclosing row as `$parent`. A broadcast is a loop of kind `Broadcast`
/// that applies its body to each element of an array and to the value itself otherwise; its body
/// reads the element as the current value and the enclosing row as `$parent`, and a broadcast
/// that flattens states [`StepAttrs::Flatten`]. A graph step is a loop of kind `GraphStep` that
/// states a [`LookupAttrs`], and a recursion a loop of kind `Recurse` that states a
/// [`RecurseAttrs`].
pub(crate) fn bind_loop_site(cx: &mut BindCx<'_>, site: LoopSite<'_>) -> Result<Built, IrError> {
	match site {
		LoopSite::Closure {
			call,
			slot,
		} => bind_closure_call(cx, call, slot),
		LoopSite::MethodClosure {
			base,
			name,
			args,
			slot,
		} => bind_method_closure(cx, base, name, args, slot),
		LoopSite::ElementFilter {
			base,
			predicate,
		} => bind_element_filter(cx, base, predicate),
		LoopSite::Broadcast {
			base,
			body,
			flatten,
		} => bind_broadcast(cx, base, body, flatten),
		LoopSite::GraphStep {
			base,
			lookup,
		} => bind_lookup(cx, base, lookup),
		LoopSite::Recurse(recurse) => bind_recurse(cx, recurse),
	}
}

/// Whether `arg` is a closure whose creation the binder sees: a closure literal, or a name bound
/// in the lexical environment.
fn is_visible_closure(cx: &BindCx<'_>, arg: &LogicalExpr) -> bool {
	if matches!(arg, LogicalExpr::Closure(_)) {
		return true;
	}
	if let LogicalExpr::Param(param) = arg {
		return matches!(cx.lookup(param.as_str()), NameRef::Value(_));
	}
	false
}

/// Binds the closure argument `arg`, which [`is_visible_closure`] holds.
fn bind_closure_arg(cx: &mut BindCx<'_>, arg: &LogicalExpr) -> Result<Built, IrError> {
	if let LogicalExpr::Closure(closure) = arg {
		return cx.bind_closure(closure).map(Built::Value);
	}
	if let LogicalExpr::Param(param) = arg
		&& let NameRef::Value(value) = cx.lookup(param.as_str())
	{
		return Ok(Built::Value(value));
	}
	Err(invariant("a closure argument is neither a closure nor a bound name"))
}

/// The collection kind `value` is known to have.
fn collection_of(cx: &BindCx<'_>, value: ValueId) -> Option<Collection> {
	match &cx.ir().value(value)?.ty {
		TypeKnowledge::Fact {
			kind: Kind::Array(..),
			..
		} => Some(Collection::Array),
		TypeKnowledge::Fact {
			kind: Kind::Set(..),
			..
		} => Some(Collection::Set),
		TypeKnowledge::Fact {
			..
		}
		| TypeKnowledge::Hint {
			..
		}
		| TypeKnowledge::Token(_)
		| TypeKnowledge::Unknown => None,
	}
}

/// Whether `closure` is a literal the builtin `name` runs in place over an array's elements, when
/// written as the closure of a call that passes the array and the closure alone: `name` is one of
/// the array builtins that run a closure per element and stop or collect by its answer, and the
/// literal declares no more parameters than the builtin passes it (the element, and for
/// `array::map` its position too). Such a body reads `$this` as the call reads it
/// ([`BindCx::mark_closure_in_place`]); `fold`, `reduce`, the `set` builtins and a closure handed
/// in by name do not run in place.
fn runs_in_place(name: &str, closure: &LogicalExpr) -> bool {
	let passes = match name {
		"array::map" => 2,
		"array::filter"
		| "array::filter_index"
		| "array::find"
		| "array::find_index"
		| "array::index_of"
		| "array::any"
		| "array::some"
		| "array::includes"
		| "array::all"
		| "array::every" => 1,
		_ => return false,
	};
	matches!(closure, LogicalExpr::Closure(literal) if literal.args.len() <= passes)
}

/// A builtin called as `module::name(collection, …, closure)`, when it binds as a loop.
fn closure_site_of_call(cx: &BindCx<'_>, call: &LogicalCall, slot: usize) -> Option<ClosureSite> {
	let Function::Normal(name) = &call.receiver else {
		return None;
	};
	let (module, bare) = name.split_once("::")?;
	let collection = match module {
		"array" => Collection::Array,
		"set" => Collection::Set,
		_ => return None,
	};
	let form = ClosureForm::named(bare)?;
	let shaped = call.args.len() == form.arguments() && slot + 1 == call.args.len();
	(shaped && is_visible_closure(cx, call.args.get(slot)?)).then_some(ClosureSite {
		form,
		collection,
		name: Strand::new(name.as_str()),
		method: false,
	})
}

/// A closure builtin called as `receiver.name(…, closure)`, when it binds as a loop: the receiver
/// must be known to be an array or a set, because the closure's arguments and the dispatch depend
/// on it.
fn closure_site_of_method(
	cx: &BindCx<'_>,
	receiver: ValueId,
	name: &str,
	args: &[LogicalExpr],
	slot: usize,
) -> Option<ClosureSite> {
	let form = ClosureForm::named(name)?;
	let shaped = args.len() + 1 == form.arguments() && slot + 1 == args.len();
	if !shaped || !is_visible_closure(cx, args.get(slot)?) {
		return None;
	}
	Some(ClosureSite {
		form,
		collection: collection_of(cx, receiver)?,
		name: Strand::new(name),
		method: true,
	})
}

/// A call of a builtin that runs a closure: a loop when the call's shape allows one, otherwise
/// the plain `expr.call`.
fn bind_closure_call(
	cx: &mut BindCx<'_>,
	call: &LogicalCall,
	slot: usize,
) -> Result<Built, IrError> {
	let in_place = if let Function::Normal(name) = &call.receiver {
		call.args.len() == 2 && slot == 1 && runs_in_place(name.as_str(), &call.args[slot])
	} else {
		false
	};
	let Some(site) = closure_site_of_call(cx, call, slot) else {
		return bind_plain_call(cx, call, in_place.then_some(slot));
	};
	let mut values = Vec::with_capacity(call.args.len());
	for (index, arg) in call.args.iter().enumerate() {
		let built = if index == slot {
			if in_place {
				cx.mark_closure_in_place();
			}
			bind_closure_arg(cx, arg)?
		} else {
			bind_expr(cx, arg)?
		};
		let Built::Value(value) = built else {
			return Ok(Built::Diverged);
		};
		values.push(value);
	}
	let source = *values.first().ok_or(IrError::Stale("collection"))?;
	let closure = *values.get(slot).ok_or(IrError::Stale("closure"))?;
	let seed = if site.form == ClosureForm::Fold {
		values.get(1).copied()
	} else {
		None
	};
	closure_loop(cx, &site, source, seed, closure)
}

/// The `expr.call` of `call`: its arguments, then the row names a callee that reads them is
/// passed. The closure literal at `in_place`, where one is given, is one the builtin runs in place
/// ([`runs_in_place`]).
fn bind_plain_call(
	cx: &mut BindCx<'_>,
	call: &LogicalCall,
	in_place: Option<usize>,
) -> Result<Built, IrError> {
	let mut operands = Vec::with_capacity(call.args.len());
	for (index, arg) in call.args.iter().enumerate() {
		if in_place == Some(index) {
			cx.mark_closure_in_place();
		}
		let Built::Value(value) = bind_expr(cx, arg)? else {
			return Ok(Built::Diverged);
		};
		operands.push(value);
	}
	let row_names = if passes_row_names(&call.receiver, cx.registry()) {
		cx.row_name_operands()
	} else {
		SmallVec::new()
	};
	let attrs = CallAttrs {
		function: call.receiver.clone(),
		row_names: u8::try_from(row_names.len())
			.map_err(|_| IrError::Capacity("row names of a call"))?,
		callee: None,
	};
	operands.extend(row_names);
	cx.emit_value(OpKind::Expr(ExprOp::Call(Box::new(attrs))), &operands, TypeKnowledge::Unknown)
		.map(Built::Value)
}

/// A method called on `base` with a closure among its arguments: a loop when the receiver is
/// known to be an array or a set and the builtin binds as a loop, otherwise the dispatch check
/// and the `expr.method` that evaluates it, with the arguments evaluated against the step's root.
fn bind_method_closure(
	cx: &mut BindCx<'_>,
	base: &LogicalExpr,
	name: &Strand,
	args: &[LogicalExpr],
	slot: usize,
) -> Result<Built, IrError> {
	let Built::Value(receiver) = bind_expr(cx, base)? else {
		return Ok(Built::Diverged);
	};
	let site = closure_site_of_method(cx, receiver, name.as_str(), args, slot);
	let in_place = args.len() == 1
		&& slot == 0
		&& cx
			.registry()
			.get_method(name.as_str())
			.and_then(|method| method.resolve(&crate::val::Value::Array(Default::default())))
			.is_some_and(|builtin| runs_in_place(builtin.name(), &args[slot]));
	let dispatch = match site {
		Some(_) => None,
		None => Some(cx.emit_value(
			OpKind::Expr(ExprOp::Dispatch(Box::new(DispatchTarget::Method(name.clone())))),
			&[receiver],
			TypeKnowledge::Unknown,
		)?),
	};
	let bound = with_step_root(cx, receiver, |cx| {
		let mut values = Vec::with_capacity(args.len());
		for (index, arg) in args.iter().enumerate() {
			if in_place && index == slot {
				cx.mark_closure_in_place();
			}
			let built = if site.is_some() && index == slot {
				bind_closure_arg(cx, arg)?
			} else {
				bind_expr(cx, arg)?
			};
			let Built::Value(value) = built else {
				return Ok(None);
			};
			values.push(value);
		}
		Ok(Some(values))
	})?;
	let Some(values) = bound else {
		return Ok(Built::Diverged);
	};
	match (site, dispatch) {
		(Some(site), _) => {
			let closure = *values.get(slot).ok_or(IrError::Stale("closure"))?;
			let seed = if site.form == ClosureForm::Fold {
				values.first().copied()
			} else {
				None
			};
			closure_loop(cx, &site, receiver, seed, closure)
		}
		(None, Some(dispatch)) => {
			let mut operands = vec![dispatch, receiver];
			operands.extend(values);
			cx.emit_value(
				OpKind::Expr(ExprOp::Method {
					name: name.clone(),
				}),
				&operands,
				TypeKnowledge::Unknown,
			)
			.map(Built::Value)
		}
		(None, None) => Err(invariant("a method call has neither a loop nor a dispatch")),
	}
}

/// Runs `body` with the document root an argument of a step is evaluated against as the row, as
/// the current value, as `$this` and `$self`, and as `$parent` in a frame with no row where no
/// name binds it, with no field base around it, and restores all of them whether `body` succeeded
/// or not.
fn with_step_root<R>(
	cx: &mut BindCx<'_>,
	receiver: ValueId,
	body: impl FnOnce(&mut BindCx<'_>) -> Result<R, IrError>,
) -> Result<R, IrError> {
	let root = cx.step_root(receiver)?;
	let names = cx.declare_row_names_until(root);
	let row = cx.replace_row(Some(root));
	let base = cx.replace_field_base(None);
	let current = cx.replace_current(Some(root));
	let outcome = body(cx);
	cx.end_row_names(names);
	cx.replace_row(row);
	cx.replace_field_base(base);
	cx.replace_current(current);
	outcome
}

/// Runs `body` with `value` as the current value, and restores the previous one whether `body`
/// succeeded or not.
fn with_current<R>(
	cx: &mut BindCx<'_>,
	value: ValueId,
	body: impl FnOnce(&mut BindCx<'_>) -> Result<R, IrError>,
) -> Result<R, IrError> {
	let outer = cx.replace_current(Some(value));
	let outcome = body(cx);
	cx.replace_current(outer);
	outcome
}

/// The `logical.loop` of kind `Closure` that applies `closure` to each element of `source`.
///
/// The loop's operands are `source` and, when the sink folds and a `seed` is given, the initial
/// accumulator; a fold without a seed starts from the first element of `source` and applies the
/// closure from the second. The body applies the closure and yields the answer, as a truth value
/// where the sink decides or filters by it.
fn closure_loop(
	cx: &mut BindCx<'_>,
	site: &ClosureSite,
	source: ValueId,
	seed: Option<ValueId>,
	closure: ValueId,
) -> Result<Built, IrError> {
	let sink = site.form.sink();
	let entry = vec![TypeKnowledge::Unknown; loop_body_args(&sink)];
	let mut operands = vec![source];
	operands.extend(seed);
	let op = cx.emit(
		OpKind::Logical(LogOp::Loop {
			kind: LoopKind::Closure,
			sink,
			order: SourceOrder::Positional,
			step: Some(Box::new(StepAttrs::Closure(ClosureLoopAttrs {
				function: site.name.clone(),
				method: site.method,
			}))),
		}),
		&operands,
		&[TypeKnowledge::Unknown],
	)?;
	cx.within(op, RegionKind::Loop, &entry, |cx, args| {
		let element = args.first().copied().ok_or(IrError::Stale("loop element"))?;
		let position = args.get(1).copied().ok_or(IrError::Stale("loop position"))?;
		let target = cx.emit_value(
			OpKind::Expr(ExprOp::Dispatch(Box::new(DispatchTarget::Invoke))),
			&[closure],
			TypeKnowledge::Unknown,
		)?;
		let mut operands = vec![target];
		operands.extend(site.form.arguments_of(
			site.collection,
			element,
			position,
			args.get(2).copied(),
		));
		let answer =
			cx.emit_value(OpKind::Expr(ExprOp::Invoke), &operands, TypeKnowledge::Unknown)?;
		let yielded = if site.form.yields_truth() {
			cx.emit_value(OpKind::Expr(ExprOp::Truthy), &[answer], TypeKnowledge::Unknown)?
		} else {
			answer
		};
		cx.yield_logical(&[yielded])
	})?;
	first_result(cx, op).map(Built::Value)
}

/// `base[WHERE predicate]`: a loop of kind `ElementFilter` over `base` that keeps the elements
/// the predicate admits. The predicate reads the element as `$this`, as the current value and as
/// the base of a field read or `@`, and the row around the filter as `$parent`; a step's argument
/// in it reads the document root.
fn bind_element_filter(
	cx: &mut BindCx<'_>,
	base: &LogicalExpr,
	predicate: &LogicalExpr,
) -> Result<Built, IrError> {
	let Built::Value(source) = bind_expr(cx, base)? else {
		return Ok(Built::Diverged);
	};
	let sink = LoopSink::Filter;
	let entry = vec![TypeKnowledge::Unknown; loop_body_args(&sink)];
	let op = cx.emit(
		OpKind::Logical(LogOp::Loop {
			kind: LoopKind::ElementFilter,
			sink,
			order: SourceOrder::Positional,
			step: None,
		}),
		&[source],
		&[TypeKnowledge::Unknown],
	)?;
	cx.within(op, RegionKind::Loop, &entry, |cx, args| {
		let element = args.first().copied().ok_or(IrError::Stale("loop element"))?;
		cx.declare_row_names(element, &["this"]);
		let base = cx.replace_field_base(Some(element));
		let bound = with_current(cx, element, |cx| match bind_expr(cx, predicate)? {
			Built::Value(admitted) => {
				let truth = cx.emit_value(
					OpKind::Expr(ExprOp::Truthy),
					&[admitted],
					TypeKnowledge::Unknown,
				)?;
				cx.yield_logical(&[truth])
			}
			Built::Diverged => Ok(()),
		});
		cx.replace_field_base(base);
		bound
	})?;
	first_result(cx, op).map(Built::Value)
}

/// `base` with `body` applied to each element: a loop of kind `Broadcast` over `base`. Over a
/// value that is not an array the body is applied to the value itself. The body reads the element
/// as the current value and as the base of a field read or `@`, the row around the broadcast as
/// `$parent`, and a step's argument in it reads the document root. A broadcast that `flatten`s
/// splices the arrays its body answers into the output by one level, which the loop states as
/// [`StepAttrs::Flatten`].
fn bind_broadcast(
	cx: &mut BindCx<'_>,
	base: &LogicalExpr,
	body: &LogicalExpr,
	flatten: bool,
) -> Result<Built, IrError> {
	let Built::Value(source) = bind_expr(cx, base)? else {
		return Ok(Built::Diverged);
	};
	let sink = LoopSink::Output;
	let entry = vec![TypeKnowledge::Unknown; loop_body_args(&sink)];
	let op = cx.emit(
		OpKind::Logical(LogOp::Loop {
			kind: LoopKind::Broadcast,
			sink,
			order: SourceOrder::Positional,
			step: flatten.then(|| Box::new(StepAttrs::Flatten)),
		}),
		&[source],
		&[TypeKnowledge::Unknown],
	)?;
	cx.within(op, RegionKind::Loop, &entry, |cx, args| {
		let element = args.first().copied().ok_or(IrError::Stale("loop element"))?;
		cx.declare_row_names(element, &[]);
		let base = cx.replace_field_base(Some(element));
		let bound = with_current(cx, element, |cx| match bind_expr(cx, body)? {
			Built::Value(value) => cx.yield_logical(&[value]),
			Built::Diverged => Ok(()),
		});
		cx.replace_field_base(base);
		bound
	})?;
	first_result(cx, op).map(Built::Value)
}

/// A graph or reference lookup that stands where no hop of its path follows it: the statement of
/// its clauses where it has clauses of its own, and otherwise its hop ([`super::lookup`]).
///
/// Where a lookup of the path below the hops this one ends has `ONLY`, the hops are bound over
/// that path read without it, and this hop states that the run's answer is unwrapped.
fn bind_lookup(
	cx: &mut BindCx<'_>,
	base: &LogicalExpr,
	lookup: &LogicalLookup,
) -> Result<Built, IrError> {
	if super::lookup::is_statement(base, lookup) {
		return bind_lookup_statement(cx, base, lookup);
	}
	if super::lookup::only_in(super::lookup::run_source(base)) {
		let base = super::lookup::unwrapped_below_run(base);
		return bind_hop(cx, &base, lookup, true);
	}
	bind_hop(cx, base, lookup, false)
}

/// The statement of `lookup`'s clauses over the records it reaches from the value `base` names
/// ([`super::lookup::statement`]), as an `expr.subquery` run for each evaluation.
///
/// `base` is read without `ONLY` on any of its lookups, before the statement and in the region the
/// lookup stands in, and the statement reads it by [`super::lookup::SOURCE`]. The statement is
/// under `ONLY` where the lookup or a lookup of `base` has it.
fn bind_lookup_statement(
	cx: &mut BindCx<'_>,
	base: &LogicalExpr,
	lookup: &LogicalLookup,
) -> Result<Built, IrError> {
	let only = lookup.only || super::lookup::only_in(base);
	let Built::Value(source) = bind_expr(cx, &super::lookup::without_only(base))? else {
		return Ok(Built::Diverged);
	};
	cx.declare_unnamed(Strand::new(super::lookup::SOURCE), source);
	bind_value_statement(cx, &super::lookup::statement(lookup, only))
}

/// What the planner raises for a graph range subject whose bound is a generated key, which names
/// no value to bound the range by. Where the lookup is planned with its top-level statement the
/// refusal is the whole statement's; otherwise it is raised where the lookup stands.
fn generated_bound_refusal(range: &LogicalRecordIdKeyRange) -> Option<Refusal> {
	let generated = [&range.start, &range.end].into_iter().any(|bound| {
		matches!(
			bound,
			std::ops::Bound::Included(LogicalRecordIdKey::Generate(_))
				| std::ops::Bound::Excluded(LogicalRecordIdKey::Generate(_))
		)
	});
	generated.then(|| Refusal::Query {
		message: "Generated keys (rand, ulid, uuid) cannot be used in graph range bounds"
			.to_owned(),
	})
}

/// One hop of a graph or reference lookup: a loop of kind `GraphStep` over the value `base` names,
/// whose elements are the records the lookup's subjects reach from it. `only_path` is
/// [`LookupAttrs::only_path`].
///
/// A subject keeps its referencing field (`FIELD`) only in a reference lookup. A graph lookup
/// reads the edges of a subject's table whatever field it names, so its subjects name none.
///
/// The operands are bound in the order [`LookupAttrs`] lists them: the source, the operands of the
/// bounds of each range subject, then `LIMIT` and `START`, each evaluated once before any record
/// is read. The body yields each reached record unchanged, and the clauses of the lookup are
/// regions of the loop after it, each evaluated against a reached record: the predicate, the
/// splits, the group keys, the order keys and the columns of the projection. The aggregate
/// functions called in the columns of a grouped lookup belong to the lookup.
fn bind_hop(
	cx: &mut BindCx<'_>,
	base: &LogicalExpr,
	lookup: &LogicalLookup,
	only_path: bool,
) -> Result<Built, IrError> {
	let Built::Value(source) = bind_expr(cx, base)? else {
		return Ok(Built::Diverged);
	};
	let mut operands = vec![source];
	let mut subjects = Vec::with_capacity(lookup.what.len());
	let reads_field = matches!(lookup.kind, LookupKind::Reference);
	for subject in &lookup.what {
		subjects.push(match subject {
			LogicalLookupSubject::Table {
				table,
				referencing_field,
			} => LookupSubjectShape {
				table: table.clone(),
				referencing_field: reads_field.then(|| referencing_field.clone()).flatten(),
				range: None,
			},
			LogicalLookupSubject::Range {
				table,
				range,
				referencing_field,
			} => {
				if let Some(refusal) = generated_bound_refusal(range) {
					if cx.eager() {
						cx.hoist_refusal(refusal.clone());
						raise_refused(cx, refusal)?;
					} else {
						cx.raise(RaiseKind::Refused(Box::new(refusal)))?;
					}
					return Ok(Built::Diverged);
				}
				let shape = key_range_shape(range)?;
				for expr in range_key_exprs(range) {
					let Built::Value(value) = bind_expr(cx, expr)? else {
						return Ok(Built::Diverged);
					};
					operands.push(value);
				}
				LookupSubjectShape {
					table: table.clone(),
					referencing_field: reads_field.then(|| referencing_field.clone()).flatten(),
					range: Some(shape),
				}
			}
		});
	}
	let mut once = [None, None];
	for (slot, expr) in once.iter_mut().zip([&lookup.limit, &lookup.start]) {
		if let Some(expr) = expr {
			let Built::Value(value) = bind_expr(cx, expr)? else {
				return Ok(Built::Diverged);
			};
			*slot = Some(
				u16::try_from(operands.len()).map_err(|_| IrError::Capacity("lookup operands"))?,
			);
			operands.push(value);
		}
	}
	let [limit, start] = once;
	let order = lookup.order.as_ref().map(|ordering| match ordering {
		LogicalOrdering::Random => SortShape::Random,
		LogicalOrdering::Order(keys) => SortShape::Keys(
			keys.iter()
				.map(|key| SortKeyShape {
					collate: key.collate,
					numeric: key.numeric,
					ascending: key.direction,
					path: false,
				})
				.collect(),
		),
	});
	let attrs = LookupAttrs {
		kind: lookup.kind.clone(),
		only: lookup.only,
		only_path,
		subjects: subjects.into_boxed_slice(),
		fields: lookup.expr.as_ref().map(fields_shape),
		order,
		alias: lookup.alias.clone(),
		cond: lookup.cond.is_some(),
		split: clause_count(lookup.split.as_deref())?,
		group: clause_count(lookup.group.as_deref())?,
		limit,
		start,
		trace: lookup.trace,
	};
	let sink = LoopSink::Output;
	let entry = vec![TypeKnowledge::Unknown; loop_body_args(&sink)];
	let op = cx.emit(
		OpKind::Logical(LogOp::Loop {
			kind: LoopKind::GraphStep,
			sink,
			order: SourceOrder::Delivery,
			step: Some(Box::new(StepAttrs::Lookup(attrs))),
		}),
		&operands,
		&[TypeKnowledge::Unknown],
	)?;
	cx.within(op, RegionKind::Loop, &entry, |cx, args| {
		let element = args.first().copied().ok_or(IrError::Stale("loop element"))?;
		cx.yield_logical(&[element])
	})?;
	if let Some(cond) = &lookup.cond {
		lookup_clause(cx, op, RegionKind::Clause(ClauseRole::Where), None, cond)?;
	}
	for expr in lookup.split.iter().flatten() {
		lookup_clause(cx, op, RegionKind::Clause(ClauseRole::Split), None, expr)?;
	}
	for expr in lookup.group.iter().flatten() {
		lookup_clause(cx, op, RegionKind::ErrorScope(ErrorScope::ShapeToNone), None, expr)?;
	}
	if let Some(LogicalOrdering::Order(keys)) = &lookup.order {
		for key in keys {
			lookup_clause(cx, op, RegionKind::Clause(ClauseRole::OrderKey), None, &key.value)?;
		}
	}
	if let Some(fields) = &lookup.expr {
		let columns = |cx: &mut BindCx<'_>| {
			for column in columns_of(fields) {
				lookup_clause(
					cx,
					op,
					RegionKind::Clause(ClauseRole::Projection),
					column.frame,
					column.expr,
				)?;
			}
			Ok(())
		};
		if lookup.group.is_some() {
			cx.with_aggregating(columns)?;
		} else {
			columns(cx)?;
		}
	}
	first_result(cx, op).map(Built::Value)
}

/// The number of expressions a lookup clause holds, `None` when the clause is not written.
fn clause_count(exprs: Option<&[LogicalExpr]>) -> Result<Option<u16>, IrError> {
	exprs
		.map(|exprs| u16::try_from(exprs.len()).map_err(|_| IrError::Capacity("lookup clauses")))
		.transpose()
}

/// Binds `expr` as the body of a clause region of `kind` that the lookup `owner` hosts, evaluated
/// against a record the lookup reached, under the frame `frame` names; `None` is the frame of the
/// statement. The region is no part of the loop's body, so it binds the row names itself: `$this`,
/// `$self` and `$value` name the record and `$parent` the row around the lookup.
fn lookup_clause(
	cx: &mut BindCx<'_>,
	owner: OpId,
	kind: RegionKind,
	frame: Option<&EvalFrame>,
	expr: &LogicalExpr,
) -> Result<(), IrError> {
	let id = match frame {
		Some(eval) => cx.frame_for(eval)?,
		None => cx.frame_for(&EvalFrame::caller())?,
	};
	cx.with_frame(id, |cx| {
		enter_row_region(cx, owner, kind, |cx, row| {
			cx.declare_row_names(row, ROW_NAMES);
			cx.with_row(row, |cx| bind_body(cx, expr))
		})
	})
}

/// A recursion: a loop of kind `Recurse` over the value `recurse.base` names.
///
/// The body receives the value of a depth as its element, which it reads as the current value and
/// as its row (a field read, `@` and a step's argument read it, as the body is a unit over that
/// value), and the depth as its position; `$parent` is the row around the recursion. It yields the
/// value the next depth starts from: the path's body where the recursion has one, with the
/// recursion marker of a trailing path standing in it, and the element itself where it has none.
/// The operand of a shortest-path instruction, the value it looks for, follows the source.
fn bind_recurse(cx: &mut BindCx<'_>, recurse: &LogicalRecurse) -> Result<Built, IrError> {
	let Built::Value(source) = bind_expr(cx, &recurse.base)? else {
		return Ok(Built::Diverged);
	};
	let mut operands = vec![source];
	let instruction = match &recurse.instruction {
		None => None,
		Some(LogicalRecurseInstruction::Path {
			inclusive,
		}) => Some(RecurseInstructionShape::Path {
			inclusive: *inclusive,
		}),
		Some(LogicalRecurseInstruction::Collect {
			inclusive,
		}) => Some(RecurseInstructionShape::Collect {
			inclusive: *inclusive,
		}),
		Some(LogicalRecurseInstruction::Shortest {
			expects,
			inclusive,
		}) => {
			let Built::Value(target) = bind_expr(cx, expects)? else {
				return Ok(Built::Diverged);
			};
			operands.push(target);
			Some(RecurseInstructionShape::Shortest {
				inclusive: *inclusive,
			})
		}
	};
	let (form, body) = match &recurse.path {
		None => (RecursePathForm::Absent, None),
		Some(LogicalRecursePath::Nested(body)) => (RecursePathForm::Nested, Some(body)),
		Some(LogicalRecursePath::Trailing(body)) => (RecursePathForm::Trailing, Some(body)),
	};
	let attrs = RecurseAttrs {
		depth: recurse.recurse.clone(),
		form,
		instruction,
	};
	let sink = LoopSink::Output;
	let entry = vec![TypeKnowledge::Unknown; loop_body_args(&sink)];
	let op = cx.emit(
		OpKind::Logical(LogOp::Loop {
			kind: LoopKind::Recurse,
			sink,
			order: SourceOrder::Positional,
			step: Some(Box::new(StepAttrs::Recurse(attrs))),
		}),
		&operands,
		&[TypeKnowledge::Unknown],
	)?;
	cx.within(op, RegionKind::Loop, &entry, |cx, args| {
		let element = args.first().copied().ok_or(IrError::Stale("loop element"))?;
		cx.declare_row_names(element, &[]);
		cx.with_row(element, |cx| {
			with_current(cx, element, |cx| match body {
				Some(body) => match bind_expr(cx, body)? {
					Built::Value(value) => cx.yield_logical(&[value]),
					Built::Diverged => Ok(()),
				},
				None => cx.yield_logical(&[element]),
			})
		})
	})?;
	first_result(cx, op).map(Built::Value)
}

/// A closure literal: the `expr.closure_create` whose operands are the values the body captures,
/// and whose `ClosureBody` region is isolated from above.
///
/// The body captures every name it reads that the lexical environment binds where the closure is
/// written, and nothing of the row there: making the closure reads no row. A literal that no
/// builtin runs in place ([`runs_in_place`]) captures `$this` too where the statement it stands in
/// was entered with one ([`BindCx::declare_statement_row_names`]): the document of the frame that
/// statement is entered from, not the row the statement reads. The region's block
/// arguments are the statement timestamp, the parameters, the captured names in operand order,
/// then, where the body reads a row by a field, `@`, the current value, `$this`, `$self` or a call
/// that reads its caller's row, the row of the frame that invokes the closure. A parameter shadows
/// a captured name. The body reads that row as its row and current value, and as `$this` and
/// `$self` where a row binds the name where the literal is written and neither a parameter nor a
/// capture names it. One that nothing binds there stays a name of the frame the body runs in. The
/// body reads no other value of the region around it: the current value and the aggregate context
/// of that region do not reach it.
pub(crate) fn bind_closure(
	cx: &mut BindCx<'_>,
	closure: &LogicalClosure,
) -> Result<ValueId, IrError> {
	// SECURITY: a closure parameter may not take a protected name, for the
	// reason a `FOR` variable may not ([`refuse_protected_binding`]): a body that
	// bound its own `$auth`, `$access`, `$token` or `$session` would hand a
	// permission predicate judged beneath it a caller-chosen identity
	// (GHSA-ffvm-mqh3-7986).
	if let Some((param, _)) = closure
		.args
		.iter()
		.find(|(param, _)| surrealdb_cnf::PROTECTED_PARAM_NAMES.contains(&param.as_str()))
	{
		// SECURITY: a closure parameter is refused the way a protected `LET` or
		// `FOR` variable is (GHSA-ffvm-mqh3-7986), and like them its refusal is the
		// whole statement's where it is planned eagerly, and raised where it stands
		// otherwise. A closure stands inside an expression whose binder appends more
		// ops to this block after this returns, so a `raise` emitted into this block
		// would not be its last op; both paths keep the raise out of it.
		if cx.eager() {
			// Planned with the top-level statement: the refusal is the whole
			// statement's, raised before any of it runs. `take_hoisted` erases what
			// the statement bound and raises it ([`bind_logical_statement`],
			// [`raise_if_refused`]); the placeholder only stands in until then.
			cx.hoist_refusal(Refusal::ProtectedParam {
				name: param.as_str().to_owned(),
			});
			let none = cx.emit(
				OpKind::Expr(ExprOp::Literal(Box::new(crate::val::Value::None))),
				&[],
				&[TypeKnowledge::Unknown],
			)?;
			return first_result(cx, none);
		}
		// Bound after a catalog change in the same block, where the statements
		// before it may already have run: the refusal is raised only if the closure
		// is reached, so those statements stand. It is the terminator of the region
		// of an `expr.subquery` whose value stands for the closure, so the raise is
		// the last op of its own block and is reached only when that value is
		// evaluated ([`refusing_subquery`]).
		let subquery = cx.emit(
			OpKind::Expr(ExprOp::Subquery(NestedStrategy::Apply)),
			&[],
			&[TypeKnowledge::Unknown],
		)?;
		let result = first_result(cx, subquery)?;
		cx.within(subquery, RegionKind::Subquery, &[], |cx, _| {
			cx.raise(RaiseKind::Refused(Box::new(Refusal::ProtectedParam {
				name: param.as_str().to_owned(),
			})))
		})?;
		return Ok(result);
	}
	let in_place = cx.take_closure_in_place();
	let params: Vec<Strand> =
		closure.args.iter().map(|(param, _)| Strand::new(param.as_str())).collect();
	let (captured, reads_row) = captures_of(cx, closure, &params, in_place);
	let reads_root = reads_row && closure_reads_root(closure);
	// A row name a row binds where the literal is written names the invoking row in the body; one
	// nothing binds there stays a name of the frame the body runs in. A `$this` the body takes only
	// to hand to the literals it holds does not name it.
	let row_named: Vec<&str> = ROW_PARAMS
		.into_iter()
		.filter(|name| {
			!params
				.iter()
				.chain(captured.iter().filter(|held| !held.entered).map(|held| &held.name))
				.any(|held| held.as_str() == *name)
				&& matches!(cx.lookup(name), NameRef::Value(_))
		})
		.collect();
	let operands: Vec<ValueId> = captured.iter().map(|held| held.value).collect();
	let attrs = ClosureAttrs {
		params: closure
			.args
			.iter()
			.map(|(param, kind)| (Strand::new(param.as_str()), kind.clone()))
			.collect(),
		returns: closure.returns.clone(),
		body_sql: Arc::clone(&closure.body_sql),
		captures: captured.iter().map(|held| held.name.clone()).collect(),
		reads_row,
		reads_root,
		literal: ClosureLiteral::of(closure),
	};
	let op = cx.emit(
		OpKind::Expr(ExprOp::ClosureCreate(Box::new(attrs))),
		&operands,
		&[TypeKnowledge::Unknown],
	)?;
	let mut entry = vec![timestamp_arg()];
	entry.extend(params.iter().map(|_| TypeKnowledge::Unknown));
	entry.extend(operands.iter().map(|_| TypeKnowledge::Unknown));
	if reads_row {
		entry.push(TypeKnowledge::Unknown);
	}
	if reads_root {
		entry.push(TypeKnowledge::Unknown);
	}
	// The body is isolated from the region around it, its `VERSION` included: it reads at the
	// stamp of the frame that invokes it.
	cx.with_current_state(|cx| {
		cx.within(op, RegionKind::ClosureBody, &entry, |cx, args| {
			let captures =
				args.get(1 + params.len()..).ok_or(IrError::Stale("closure captures"))?;
			for (held, value) in captured.iter().zip(captures) {
				if held.entered {
					cx.declare_entered(held.name.clone(), *value);
				} else {
					cx.declare_variable(held.name.clone(), *value);
				}
			}
			for (name, value) in params.iter().zip(args.iter().skip(1)) {
				cx.declare_variable(name.clone(), *value);
			}
			let (row, root) = match (reads_row, reads_root) {
				(false, _) => (None, None),
				(true, false) => (Some(*args.last().ok_or(IrError::Stale("closure row"))?), None),
				(true, true) => {
					let [row, root] = args[args.len().saturating_sub(2)..] else {
						return Err(IrError::Stale("closure row"));
					};
					(Some(row), Some(root))
				}
			};
			cx.with_isolation(row, root, |cx| {
				if let Some(row) = row {
					for name in &row_named {
						cx.declare(Strand::new(*name), row);
					}
				}
				let outer = cx.replace_closure_row(row);
				let bound = bind_body(cx, &closure.body);
				cx.replace_closure_row(outer);
				bound
			})
		})
	})?;
	first_result(cx, op)
}

/// The names a closure body reads as the row it is invoked over, unless a variable of that name
/// holds where the closure is written, which it captures, whatever rows are bound to the name
/// inside that variable's scope. A literal no builtin runs in place captures `$this` where the
/// statement it stands in was entered with one, as it captures a variable.
const ROW_PARAMS: [&str; 2] = ["this", "self"];

/// The names `closure`'s body reads that the lexical environment binds at the point the closure
/// is written, each with its value, in the order the body first reads them ([`free_params`]), and
/// whether the body reads the row, by a field, `@`, the current value, `$this` or `$self`. The
/// names are those a value of the literal captures, so the head of the unit lowered for a value
/// names them.
///
/// `in_place` says a builtin runs the literal in place ([`runs_in_place`]): its body reads `$this`
/// as the call reads it, so it captures no `$this` a statement was entered with as the name it
/// reads. It takes that `$this` only where a closure literal its body holds reads it, as an
/// entered name it hands on to that literal ([`Captured`]).
fn captures_of(
	cx: &BindCx<'_>,
	closure: &LogicalClosure,
	params: &[Strand],
	in_place: bool,
) -> (Vec<Captured>, bool) {
	let mut names = Vec::new();
	for name in free_params(closure) {
		if params.contains(&name) {
			continue;
		}
		let mut entered = false;
		let held = if !ROW_PARAMS.contains(&name.as_str()) {
			match cx.lookup(name.as_str()) {
				NameRef::Value(value) => Some(value),
				NameRef::Free => None,
			}
		} else if name.as_str() != "this" {
			cx.lookup_variable(name.as_str())
		} else if !in_place {
			cx.lookup_variable_or_entered(name.as_str())
		} else if let Some(value) = cx.lookup_variable(name.as_str()) {
			Some(value)
		} else if nested_literal_reads(&closure.body, name.as_str()) {
			entered = true;
			cx.lookup_variable_or_entered(name.as_str())
		} else {
			None
		};
		if let Some(value) = held {
			names.push(Captured {
				name,
				value,
				entered,
			});
		}
	}
	(names, closure_reads_row(closure))
}

/// A name a closure literal captures, with its value where the literal is written.
struct Captured {
	name: Strand,
	value: ValueId,
	/// Whether the body binds the name as the `$this` a statement was entered with
	/// ([`BindCx::declare_entered`]) rather than as a variable: the `$this` a literal a builtin
	/// runs in place takes only to hand on to the closure literals its body holds.
	entered: bool,
}

/// Whether a closure literal written in `expr` reads `name` as a name it does not bind, short of
/// a statement in value position, which binds the names it is entered with itself, and of a
/// literal whose parameters bind `name`.
fn nested_literal_reads(expr: &LogicalExpr, name: &str) -> bool {
	if let LogicalExpr::Closure(inner) = expr {
		return !inner.args.iter().any(|(param, _)| param.as_str() == name)
			&& free_params(inner).iter().any(|free| free.as_str() == name);
	}
	if matches!(expr, LogicalExpr::Subquery(_)) {
		return false;
	}
	let mut found = false;
	expr.for_each_child(&mut |child| found |= nested_literal_reads(child, name));
	found
}

/// The parameters `closure`'s body reads that no binding within the body binds, each once, in
/// the order the body first reads them: a parameter read as a value and a parameter read as the
/// source of a `FROM`. An element filter binds `$this` within its predicate and a nested
/// closure binds its own parameters within its body; a statement's scalars are read with the
/// bindings that hold around the statement.
pub(crate) fn free_params(closure: &LogicalClosure) -> Vec<Strand> {
	let mut names = Vec::new();
	let mut bound = Vec::new();
	free_params_of(&closure.body, &mut bound, &mut names);
	names
}

fn free_params_of<'e>(expr: &'e LogicalExpr, bound: &mut Vec<&'e str>, names: &mut Vec<Strand>) {
	if let LogicalExpr::Param(param) = expr {
		note_free(param.as_str(), bound, names);
	} else if let LogicalExpr::Subquery(plan) = expr {
		let _ = plan.visit_scalars_deep(&mut |scalar| {
			free_params_of_owned(scalar, bound, names);
			Ok::<(), ()>(())
		});
		free_sources_of(plan, bound, names);
	} else if let LogicalExpr::Filter {
		base,
		predicate,
	} = expr
	{
		free_params_of(base, bound, names);
		bound.push("this");
		free_params_of(predicate, bound, names);
		bound.pop();
	} else if let LogicalExpr::Closure(inner) = expr {
		let depth = bound.len();
		bound.extend(inner.args.iter().map(|(param, _)| param.as_str()));
		free_params_of(&inner.body, bound, names);
		bound.truncate(depth);
	} else {
		expr.for_each_child(&mut |child| free_params_of(child, bound, names));
	}
}

/// [`free_params_of`] for an expression borrowed for less than the bindings around it.
fn free_params_of_owned(expr: &LogicalExpr, bound: &[&str], names: &mut Vec<Strand>) {
	let mut bound: Vec<&str> = bound.to_vec();
	free_params_of(expr, &mut bound, names);
}

/// Records the names `plan` and the plans below it read as the source of a `FROM`.
fn free_sources_of(plan: &LogicalOp, bound: &[&str], names: &mut Vec<Strand>) {
	if let LogicalOp::Source(LogicalSource {
		source: SourceRef::Param(param),
		..
	}) = plan
	{
		note_free(param.as_str(), bound, names);
	}
	for child in plan.children() {
		free_sources_of(child, bound, names);
	}
}

/// Records `name` in `names` when no binding in `bound` holds it and it is not recorded already.
fn note_free(name: &str, bound: &[&str], names: &mut Vec<Strand>) {
	if bound.contains(&name) || names.iter().any(|known| known.as_str() == name) {
		return;
	}
	names.push(Strand::new(name));
}

/// Whether `closure`'s body reads the document root of the frame that invokes it through an index
/// or a method argument. The body then takes the root beside the row it reads by a field, which
/// differ where the frame is an element filter's body: the row is the element and the root is the
/// row the filter is applied over.
pub(crate) fn closure_reads_root(closure: &LogicalClosure) -> bool {
	crate::exec::planner::name_scope::captures_of(closure).root_by_step
}

/// Whether `closure`'s body reads the row it is invoked over. The body names a row read
/// ([`mentions_row`]) and the read is of the invoking row: the current value at the body's own
/// level or in a statement or block it holds, or the document root through an index, a method
/// argument, a statement or a `$parent` read in an element filter
/// ([`captures_of`](crate::exec::planner::name_scope::captures_of)). A read inside an element
/// filter, a lookup's clause, a recursion or a destructure of the body is of the element that
/// construct binds, not of the invoking row. The body of a literal that reads it takes the
/// invoking row ([`bind_closure`]).
pub(crate) fn closure_reads_row(closure: &LogicalClosure) -> bool {
	if !mentions_row(closure) {
		return false;
	}
	let captures = crate::exec::planner::name_scope::captures_of(closure);
	captures.reads_current
		|| captures.reads_root()
		|| captures.parent_in_block
		|| captures.parent_in_statement
		|| captures.nested_reads_root
		|| captures.nested_step_arg_reads_row
}

/// Whether `closure`'s body names a row read anywhere in it: a field, `@`, the current value,
/// `$this`, `$self` or `$parent` that no parameter names, a statement in value position that names
/// one of those three whether a parameter names it or not (the statement binds them to the
/// document it is taken over), or a call of a projection function or of a function that is not a
/// builtin, which reads the row of its caller.
fn mentions_row(closure: &LogicalClosure) -> bool {
	use crate::exec::planner::row_scope::names_row_param;
	let named = |name: &str| closure.args.iter().any(|(param, _)| param.as_str() == name);
	closure
		.body
		.visit_deep(&mut |expr| {
			let reads = if let LogicalExpr::Call(call) = expr {
				call_reads_row(&call.receiver)
			} else if let LogicalExpr::Param(param) = expr {
				(ROW_PARAMS.contains(&param.as_str()) || param.as_str() == "parent")
					&& !named(param.as_str())
			} else if let LogicalExpr::Subquery(op) = expr {
				names_row_param(op)
			} else {
				matches!(expr, LogicalExpr::Field(_) | LogicalExpr::Doc | LogicalExpr::Current)
			};
			if reads {
				ControlFlow::Break(())
			} else {
				ControlFlow::Continue(())
			}
		})
		.is_break()
}

/// Whether a call of `function` reads the row it is called on: a projection function reads its
/// fields, and a function that is not a builtin is handed its row names.
fn call_reads_row(function: &Function) -> bool {
	match function {
		Function::Normal(name) => matches!(name.as_str(), "type::field" | "type::fields"),
		Function::Custom(_)
		| Function::Module(..)
		| Function::Silo {
			..
		}
		| Function::Script(_)
		| Function::Model(_) => true,
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use surrealdb_expr::parser::TopLevelExpr;
	use surrealdb_strand::TableName;

	use super::{ComputedReadsShape, DerivedReads, bind_request, computed_reads_of};
	use crate::exec::function::FunctionRegistry;
	use crate::exec::ir::analysis::cx::NoDefinitions;
	use crate::exec::ir::core::ir::{Ir, IrScope, Walk, WalkOrder};
	use crate::exec::ir::expr::ops::ExprOp;
	use crate::exec::ir::iface::catalog::{CatalogAnswer, EffectCx, SemanticCatalog};
	use crate::exec::ir::iface::frame::FrameTable;
	use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
	use crate::exec::ir::iface::region_kind::{FieldRole, RegionKind, RegionProperties};
	use crate::exec::ir::logical::ops::LogOp;
	use crate::exec::ir::op_kind::OpKind;
	use crate::exec::ir::{RequestInput, RequestStatement};
	use crate::expr::Kind;
	use crate::expr::logical::{
		ComputedBody, EvalFrame, FieldPath, FieldSchema, LogicalExpr, LogicalField, LogicalFields,
		LogicalOp, LogicalProject, Schema, lower, lower_expr,
	};
	use crate::kvs::cache::ds::TableSnapshot;

	fn expr(sql: &str) -> LogicalExpr {
		let ast = crate::syn::parse(&format!("RETURN {sql}")).expect("expression should parse");
		let surrealdb_expr::parser::TopLevelExpr::Expr(crate::expr::Expr::Return(out)) =
			ast.expressions.into_iter().next().expect("one statement")
		else {
			panic!("expected a RETURN");
		};
		lower_expr(out.what).expect("expression should lower")
	}

	/// `t` with the stored field `a` and the computed fields `c` and `d`.
	fn subject() -> Option<(TableName, Arc<Schema>)> {
		let computed = |name: &str| {
			let mut field = FieldSchema::new(FieldPath::field(name), Kind::Int);
			field.computed = Some(ComputedBody {
				expr: expr("a + 1"),
				kind: Some(Kind::Int),
				auth: None,
			});
			field
		};
		let schema = Schema::closed(vec![
			FieldSchema::new(FieldPath::field("a"), Kind::Int),
			computed("d"),
			computed("c"),
		]);
		Some((TableName::from("t"), Arc::new(schema)))
	}

	/// The shape bound over a catalog that answers no table, so no derivation is stated.
	fn reads(predicate: &str) -> Option<Box<ComputedReadsShape>> {
		computed_reads_of(
			&FunctionRegistry::with_builtins(),
			&NoDefinitions,
			subject(),
			&expr(predicate),
		)
		.map(|reads| reads.shape)
	}

	fn named(fields: &[&str]) -> Option<Box<ComputedReadsShape>> {
		Some(Box::new(ComputedReadsShape {
			table: TableName::from("t"),
			fields: Some(fields.iter().map(|field| (*field).to_owned()).collect()),
			derived: DerivedReads::LateBound,
		}))
	}

	fn every() -> Option<Box<ComputedReadsShape>> {
		Some(Box::new(ComputedReadsShape {
			table: TableName::from("t"),
			fields: None,
			derived: DerivedReads::LateBound,
		}))
	}

	/// The frame region of the one column of a permission layer over `SELECT * FROM t` whose
	/// column is `expression`, bound with no catalog: the container, the region, its row and the
	/// value the column judges.
	async fn layer_column_region(expression: &str) -> (Ir, RegionId, ValueId, ValueId) {
		let parsed = crate::syn::parse("SELECT * FROM t").expect("the read parses").expressions;
		let Some(TopLevelExpr::Expr(read)) = parsed.into_iter().next() else {
			panic!("one statement");
		};
		let source = lower(read).expect("the read lowers");
		let layer = LogicalOp::Project(LogicalProject {
			input: Box::new(source),
			fields: LogicalFields::Select(vec![
				LogicalField::All,
				LogicalField::column("c", expr(expression), None, EvalFrame::permission(None)),
			]),
			omit: Vec::new(),
		});
		let statements = [RequestStatement::Logical(&layer)];
		let input = RequestInput::request(
			&statements,
			IrScope::Request {
				fragment: None,
			},
		);
		let registry = FunctionRegistry::with_builtins();
		let frames = FrameTable::default();
		let cx = EffectCx {
			registry: &registry,
			catalog: &NoDefinitions,
			frames: &frames,
		};
		let ir = bind_request(&input, &cx).await.expect("the layer binds");
		let mut found = None;
		ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
			if let Some(OpKind::Logical(LogOp::Project(attrs))) = ir.op(op).map(|data| &data.kind)
				&& attrs.layer
			{
				found = Some(op);
			}
			Walk::Continue
		});
		let data = ir.op(found.expect("a layer is bound")).expect("the layer is held");
		let region = *data.regions.last().expect("the layer holds its column's frame");
		let held = ir.region(region).expect("the frame region is held");
		assert_eq!(held.kind, RegionKind::FieldFrame(FieldRole::Computed));
		let block = ir.block(held.blocks[0]).expect("the frame region has an entry block");
		let base = usize::from(held.kind.isolated_from_above());
		assert_eq!(block.args.len(), base + 2, "the row and the judged value");
		let (row, judged) = (block.args[base], block.args[base + 1]);
		(ir, region, row, judged)
	}

	/// The ops of `region`'s regions and their nested regions, in pre-order, of the kind `wanted`.
	fn ops_in(ir: &Ir, region: RegionId, wanted: fn(&OpKind) -> bool) -> Vec<OpId> {
		let mut out = Vec::new();
		ir.walk(region, WalkOrder::PreOrder, &mut |op| {
			if ir.op(op).is_some_and(|data| wanted(&data.kind)) {
				out.push(op);
			}
			Walk::Continue
		});
		out
	}

	#[tokio::test]
	async fn a_permission_column_judges_its_value_as_value_and_keeps_the_row_as_value() {
		let (ir, region, row, judged) =
			layer_column_region("IF $value > 1 THEN $value ELSE NONE END").await;
		let ifs = ops_in(&ir, region, |kind| matches!(kind, OpKind::Expr(ExprOp::If)));
		let [judgement] = ifs.as_slice() else {
			panic!("one expr.if: {ifs:?}");
		};
		let judgement = ir.op(*judgement).expect("the expr.if is held");
		// The guard reads the judged value, in the frame region itself.
		let test = judgement.operands[0];
		assert!(
			ir.uses(judged)
				.iter()
				.any(|used| ir.op(used.user).is_some_and(|user| user.results.contains(&test))),
			"the guard compares the judged value"
		);
		assert!(ir.uses(judged).iter().all(|used| ir.enclosing_region(used.user) == region));
		// The kept value reads `$value` as the row.
		let kept = judgement.regions[0];
		let yields = ops_in(&ir, kept, |kind| matches!(kind, OpKind::Expr(ExprOp::Yield)));
		assert!(
			yields
				.iter()
				.any(|at| ir.op(*at).is_some_and(|data| data.operands.first() == Some(&row))),
			"the kept value yields the row"
		);
	}

	#[tokio::test]
	async fn a_column_with_no_judgement_reads_value_as_the_row() {
		let (ir, _, _, judged) = layer_column_region("$value.c").await;
		assert!(ir.uses(judged).is_empty(), "nothing reads the judged value");
	}

	#[test]
	fn a_permission_names_the_computed_fields_it_reads_by_name_sorted() {
		assert_eq!(reads("d = 1 AND c > a"), named(&["c", "d"]));
		assert_eq!(reads("a = 1"), None);
	}

	#[test]
	fn a_permission_that_reads_the_row_other_than_by_name_derives_every_computed_field() {
		for predicate in
			["fn::check(a)", "$this.a = 1", "$parent.a = 1", "@.a = 1", "a.get() = 1", "$f() = 1"]
		{
			assert_eq!(reads(predicate), every(), "{predicate}");
		}
	}

	#[test]
	fn a_method_an_object_receiver_dispatches_reads_only_what_it_names() {
		assert_eq!(reads("c.len() > 0"), named(&["c"]));
	}

	/// A catalog that answers every table with one fixed answer that holds no snapshot.
	struct TableAnswer(fn() -> CatalogAnswer<Arc<TableSnapshot>>);

	impl SemanticCatalog for TableAnswer {
		fn table(&self, _name: &TableName) -> CatalogAnswer<Arc<TableSnapshot>> {
			(self.0)()
		}
	}

	/// The derivation a permission states is the catalog's answer for its table: none for a table
	/// the catalog does not define, and late-bound where the answer is not loaded or predates a
	/// catalog change. Where none is stated, no body is bound.
	#[test]
	fn a_permission_states_the_derivation_its_catalog_answers() {
		let registry = FunctionRegistry::with_builtins();
		let predicate = expr("c = 1");
		for (answer, derived) in [
			(TableAnswer(|| CatalogAnswer::Absent), DerivedReads::Stated(Box::default())),
			(TableAnswer(|| CatalogAnswer::NotLoaded), DerivedReads::LateBound),
			(TableAnswer(|| CatalogAnswer::Unknown), DerivedReads::LateBound),
		] {
			let reads = computed_reads_of(&registry, &answer, subject(), &predicate)
				.expect("the predicate reads a computed field");
			assert_eq!(reads.shape.derived, derived);
			assert!(reads.bodies.is_empty());
		}
	}

	#[test]
	fn a_permission_over_no_schema_or_no_computed_field_states_none() {
		let registry = FunctionRegistry::with_builtins();
		assert!(computed_reads_of(&registry, &NoDefinitions, None, &expr("$this.c = 1")).is_none());
		let plain = Schema::closed(vec![FieldSchema::new(FieldPath::field("a"), Kind::Int)]);
		let subject = Some((TableName::from("t"), Arc::new(plain)));
		assert!(
			computed_reads_of(&registry, &NoDefinitions, subject, &expr("$this.a = 1")).is_none()
		);
	}
	/// The clauses a view hosts are regions of the stage that maintains the view, bound in the
	/// frame of a view over the one image each receives: an expression is bound as the body of its
	/// region, and the fields of a materialized view as the projection of the image.
	#[test]
	fn a_view_clause_binds_in_the_frame_of_a_view_over_one_image() {
		use super::{ViewBody, bind_view_region};
		use crate::doc::Action;
		use crate::exec::ir::core::facts::FactCache;
		use crate::exec::ir::iface::region_kind::ClauseRole;
		use crate::exec::ir::logical::ops::WriteStageKind;
		use crate::exec::ir::logical::seam::BindCx;
		use crate::exec::ir::logical::verify::timestamp_arg;
		use crate::expr::logical::lower_fields;

		let registry = FunctionRegistry::with_builtins();
		let frames = FrameTable::default();
		let effects = EffectCx {
			registry: &registry,
			catalog: &NoDefinitions,
			frames: &frames,
		};
		let mut ir = Ir::new(
			IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		);
		let mut facts = FactCache::new();
		let owner = {
			let mut cx = BindCx::new(&mut ir, &mut facts, effects).expect("the context builds");
			let owner = cx
				.emit(
					OpKind::Logical(LogOp::WriteStage {
						stage: WriteStageKind::ProcessTableViews(Action::Update),
						mark: crate::exec::ir::logical::ops::WriteMark::Apply,
					}),
					&[],
					&[],
				)
				.expect("the stage binds");
			cx.within(owner, RegionKind::WriteStage, &[timestamp_arg()], |cx, _| {
				cx.yield_logical(&[])
			})
			.expect("the stage's own region binds");
			let fields = crate::syn::runtime::fields_for_definition("a, b AS c").expect("fields");
			let projection = lower_fields(fields).expect("the fields lower");
			bind_view_region(&mut cx, owner, &ViewBody::Expr(expr("a > 1"))).expect("it binds");
			bind_view_region(&mut cx, owner, &ViewBody::Fields(projection)).expect("it binds");
			owner
		};
		let regions = ir.op(owner).expect("the stage").regions.clone();
		let kinds: Vec<_> =
			regions.iter().map(|region| ir.region(*region).expect("a region").kind).collect();
		let clause = RegionKind::Clause(ClauseRole::ViewClause);
		assert_eq!(kinds, [RegionKind::WriteStage, clause, clause]);
		for region in &regions[1..] {
			let held = ir.region(*region).expect("a region");
			let block = ir.block(held.blocks[0]).expect("the entry block");
			assert_eq!(block.args.len(), 1, "the image alone");
			let last = *block.ops.last().expect("the clause binds an op");
			let frame = ir.frame(ir.op(last).expect("an op").frame).expect("its frame");
			assert_eq!(frame.eval, EvalFrame::mutable_permission(None));
		}
		let projections = ops_in(
			&ir,
			regions[2],
			|kind| matches!(kind, OpKind::Logical(LogOp::Project(attrs)) if !attrs.layer),
		);
		assert_eq!(projections.len(), 1, "the fields are projected from the image");
		assert!(
			ops_in(&ir, regions[1], |kind| matches!(kind, OpKind::Logical(LogOp::Project(_))))
				.is_empty()
		);
	}

	/// A `THEN` repair is a region of the stage that purges a record, bound in the frame a purge
	/// runs it in over the id of the referencing record, which `$this`, `$self` and `$value` name,
	/// and `$reference`, the purged record.
	#[test]
	fn a_repair_clause_binds_over_the_referencing_record_and_the_purged_one() {
		use super::bind_repair_region;
		use crate::exec::ir::core::facts::FactCache;
		use crate::exec::ir::iface::region_kind::ClauseRole;
		use crate::exec::ir::logical::ops::{WriteMark, WriteStageKind};
		use crate::exec::ir::logical::seam::BindCx;
		use crate::exec::ir::logical::verify::timestamp_arg;

		let registry = FunctionRegistry::with_builtins();
		let frames = FrameTable::default();
		let effects = EffectCx {
			registry: &registry,
			catalog: &NoDefinitions,
			frames: &frames,
		};
		let mut ir = Ir::new(
			IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		);
		let mut facts = FactCache::new();
		let owner = {
			let mut cx = BindCx::new(&mut ir, &mut facts, effects).expect("the context builds");
			let owner = cx
				.emit(
					OpKind::Logical(LogOp::WriteStage {
						stage: WriteStageKind::PurgeRecordData,
						mark: WriteMark::Apply,
					}),
					&[],
					&[],
				)
				.expect("the stage binds");
			cx.within(owner, RegionKind::WriteStage, &[timestamp_arg()], |cx, _| {
				cx.yield_logical(&[])
			})
			.expect("the stage's own region binds");
			bind_repair_region(&mut cx, owner, &expr("$reference")).expect("it binds");
			bind_repair_region(&mut cx, owner, &expr("$this")).expect("it binds");
			owner
		};
		let regions = ir.op(owner).expect("the stage").regions.clone();
		let kinds: Vec<_> =
			regions.iter().map(|region| ir.region(*region).expect("a region").kind).collect();
		let clause = RegionKind::Clause(ClauseRole::ReferenceRepair);
		assert_eq!(kinds, [RegionKind::WriteStage, clause, clause]);
		for region in &regions[1..] {
			let held = ir.region(*region).expect("a region");
			let block = ir.block(held.blocks[0]).expect("the entry block");
			assert_eq!(block.args.len(), 2, "the referencing record and $reference");
			let last = *block.ops.last().expect("the clause binds an op");
			let frame = ir.frame(ir.op(last).expect("an op").frame).expect("its frame");
			assert_eq!(frame.eval, EvalFrame::mutable_permission(None));
		}
		let args_of = |region: RegionId| {
			let block = ir.block(ir.region(region).expect("a region").blocks[0]).expect("a block");
			(block.args[0], block.args[1])
		};
		let (_, reference) = args_of(regions[1]);
		let (row, _) = args_of(regions[2]);
		assert!(!ir.uses(reference).is_empty(), "`$reference` reads the second argument");
		assert!(!ir.uses(row).is_empty(), "`$this` reads the first argument");
	}

	/// The rows of the `UPDATE` `sql` lowers to.
	fn update_rows(sql: &str) -> LogicalOp {
		let ast = crate::syn::parse(sql).expect("the statement parses");
		let TopLevelExpr::Expr(statement) =
			ast.expressions.into_iter().next().expect("one statement")
		else {
			panic!("expected an expression statement");
		};
		let LogicalOp::Write(write) = lower(statement).expect("the statement lowers") else {
			panic!("expected a write");
		};
		let crate::expr::logical::LogicalWriteSource::Rows(rows) = write.source else {
			panic!("expected rows");
		};
		*rows
	}

	/// A list mixing stored records with a value, a parameter or a nested `SELECT` is written as
	/// targets, every target in the order written; a list naming only stored records, a table
	/// alone and a nested `SELECT` alone are read as rows.
	#[test]
	fn a_list_holding_a_value_is_written_as_targets() {
		use surrealdb_types::ToSql;

		use super::{ValueTargets, value_targets};
		let targets = |sql: &str| {
			let ValueTargets {
				targets,
				stored,
				..
			} = value_targets(&update_rows(sql))?;
			// A nested `SELECT` renders as its plan, so it is named by its kind alone.
			let shown = targets.iter().map(|target| {
				if let LogicalExpr::Subquery(_) = target {
					"subquery".to_owned()
				} else {
					target.to_sql()
				}
			});
			Some((shown.collect::<Vec<_>>(), stored))
		};
		let listed = |targets: &[&str], stored: bool| {
			Some((targets.iter().map(|target| (*target).to_owned()).collect::<Vec<_>>(), stored))
		};
		assert_eq!(targets("UPDATE nn, $o SET a = 1"), listed(&["nn", "$o"], true));
		assert_eq!(
			targets("UPDATE nn:1, |nn:1..2|, nn:1..=2, (SELECT VALUE nn:2 FROM ONLY 1) SET a = 1"),
			listed(&["nn:1", "|nn:1..2|", "nn:1..=2", "subquery"], true)
		);
		assert_eq!(
			targets("UPDATE (SELECT VALUE nn:2 FROM ONLY 1), { id: nn:1 } SET a = 1"),
			listed(&["subquery", "{ id: nn:1 }"], false)
		);
		assert_eq!(targets("UPDATE $a, $b SET a = 1"), listed(&["$a", "$b"], false));
		assert_eq!(targets("UPDATE $a SET a = 1"), listed(&["$a"], false));
		for rows in [
			"UPDATE nn SET a = 1",
			"UPDATE nn, nn:1 SET a = 1",
			"UPDATE (SELECT VALUE nn:2 FROM ONLY 1) SET a = 1",
		] {
			assert_eq!(targets(rows), None, "`{rows}` is read as rows");
		}
	}

	/// A list of stored targets nested in a list beside a value is written as targets, every
	/// target in the order written; nested lists naming only stored records are read as rows.
	/// SurrealQL writes no nested list, so the lists are built here.
	#[test]
	fn a_nested_list_of_stored_targets_beside_a_value_is_targets() {
		use super::{ValueTargets, value_targets};
		use crate::expr::Param;
		use crate::expr::logical::{LogicalSource, LogicalUnionAll, SourceRef};
		let source = |expr: LogicalExpr| {
			LogicalOp::Source(LogicalSource {
				source: SourceRef::classify(expr),
				schema: None,
				with: None,
				for_update: false,
			})
		};
		let table = |name: &str| LogicalExpr::Table(name.into());
		let param = |name: &str| LogicalExpr::Param(Param::from(name.to_string()));
		let union = |inputs: Vec<LogicalOp>| {
			LogicalOp::UnionAll(LogicalUnionAll {
				inputs,
			})
		};
		let op =
			union(vec![union(vec![source(table("a")), source(table("b"))]), source(param("x"))]);
		assert_eq!(
			value_targets(&op),
			Some(ValueTargets {
				targets: vec![table("a"), table("b"), param("x")],
				stored: true,
				table: true,
				subquery: false,
			})
		);
		let op =
			union(vec![union(vec![source(table("a")), source(table("b"))]), source(table("c"))]);
		assert_eq!(value_targets(&op), None);
	}
}
