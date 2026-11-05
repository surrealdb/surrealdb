//! The entry of the direct lowering: one arm per physical and expression tag.
//!
//! [`phys_coverage`] and [`expr_coverage`] are the one statement of what the direct lowering
//! emits, and [`lower`] and [`coverage_report`] read them. Each is generated, with the refusal
//! text of its tags, from one table that names every tag of its dialect and no other: a tag added
//! to a dialect fails to compile here until the table gives it an entry, and a table entry
//! decides nothing the plan did not already fix.
//!
//! Coverage is a property of a tag. A tag that is `Native` may still refuse one payload of its
//! own, such as a coercion no instruction answers; that refusal is `NotYet` at the point of
//! lowering and does not show in [`coverage_report`].
#![deny(clippy::wildcard_enum_match_arm)]

use std::sync::Arc;

use surrealdb_strand::Strand;

use super::super::leaf::{self, Class};
use super::super::program::{
	ClosureHead, Instr, Operand, Pipeline, Program, Source, TailSpec, UnitHead,
};
use super::ddl::{Reservations, schema_op};
use super::emit::Emitter;
use super::explain_hook::{explain_in, explained_served};
use super::expr::{Claim, Lowerer, Needs, invariant};
use super::info::lower_let;
use super::place::{Lowered, Provenance};
#[cfg(test)]
use super::read::{IndexResolver, NoIndexes};
use super::schema::served;
use super::units::{ClosureTemplate, LowerCx, UnitSink};
use super::write_stmt::WritePipelines;
use super::{block, matching, pipe, read, write_stmt};
use crate::exec::AccessMode;
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::analysis::cx::structural_cx;
use crate::exec::ir::analysis::effects;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{DispatchTarget, ExprOp, ExprTag};
use crate::exec::ir::iface::conversion::Phase;
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::logical::ops::LoopKind;
use crate::exec::ir::op_kind::{IsaCaps, NativeLowering, OpKind};
use crate::exec::ir::phys::ops::{PhysOp, PhysTag, WriteSubjectsShape};
use crate::exec::vm::payload::ValuesRead;
use crate::expr::logical::EffectSet;

/// How the direct lowering treats an operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Coverage {
	/// The operation lowers to instructions of its own.
	Native,
	/// The operation lowers as part of its enclosing loop, and answers `NotYet` anywhere else.
	ViaLoop,
	/// The operation belongs to a write: a row of an `INSERT`, which the write that holds it in
	/// its rows lowers into the rows of its source ([`write_source`](super::write_source)), or a
	/// stage of its subject loop or an op of a clause a stage hosts, which the write lowers
	/// through its stages ([`write_stmt`](super::write_stmt)). It is no statement and no value of
	/// a program, so reaching it anywhere else is an invariant break.
	ViaWrite,
	/// The operation is the body of a unit a subscription's notification or a table's events run,
	/// which its unit's lowering ([`units::lower_subscription`], [`super::events::lower_events`])
	/// reads from its attributes and regions. It is no statement and no value of a program, so
	/// reaching it anywhere else is an invariant break.
	ViaUnit,
	/// The operation is the request or one of the containers that group its statements, which
	/// [`request_steps`](super::control::request_steps) assembles into the request's steps. It has
	/// no program of its own, so `lower` answers `IrError::Invariant` for one.
	ViaRequest,
	/// The operation answers `IrError::NotYet` naming its mnemonic.
	NotYet,
}

/// Defines the coverage function and the refusal text of one dialect from one table that names
/// every tag of it.
macro_rules! coverage_table {
	(
		$tag:ident, $coverage:ident, $refusal:ident;
		$($variant:ident => $mnemonic:literal, $class:ident;)+
	) => {
		/// How the direct lowering treats an operation of the tag.
		pub(super) const fn $coverage(tag: $tag) -> Coverage {
			match tag {
				$($tag::$variant => Coverage::$class,)+
			}
		}

		/// What `IrError::NotYet` names for an operation of the tag the lowering refuses.
		pub(super) const fn $refusal(tag: $tag) -> &'static str {
			match tag {
				$($tag::$variant => concat!("ir_lower::lower: ", $mnemonic),)+
			}
		}
	};
}

// Native: `phys.eval`, `phys.info` and the `phys.let` of a session parameter lower to one scalar
// pipeline, and `phys.yield` ends a region the lowering already holds. `phys.loop` lowers where an
// expression reads its result when it is an element filter, a broadcast, a closure builtin or a
// recursion, and as a statement when it is a `FOR`; it answers `NotYet` naming its kind for any
// other. `phys.block`, `phys.if`, `phys.sleep`, `phys.return`, `phys.break`, `phys.next` and
// `phys.statement` are the control flow of a statement program (`block.rs`), and `phys.timeout`
// wraps the body of a read statement (`pipe.rs`). `phys.prefetch` is lowered by its loop and by
// nothing else. `phys.kill` and `phys.live` lower to one scalar pipeline each (`info.rs`), and
// `phys.show` and `phys.access` are the one pipeline of their statement's program (`access.rs`),
// and `phys.option`, `phys.use` and the session `phys.let` record the session effects the executor
// applies (`info.rs`). The containers that group the statements of a request (`phys.request`,
// `phys.txn`, `phys.stmt`) are `ViaRequest`.
// A schema statement (`phys.ddl_guard` and the `phys.define`, `phys.remove`, `phys.alter` or
// `phys.rebuild` it ends in) is lowered by the statement drivers (`schema_program` at the top,
// `block.rs` in a list) over `ddl.rs`, not by the arm of a tag: its tags are `Native`, and one
// reached anywhere else meets the refusal of a native op that stands outside its driver.
// `phys.match` is the statement of a GQL `MATCH`, and `phys.match_step` is a step of its binding
// table, lowered by the pipeline that holds or reads it (`matching.rs`, `pipe/step.rs`).
coverage_table! {
	PhysTag, phys_coverage, phys_refusal;
	TableScan => "phys.table_scan", Native;
	IndexScan => "phys.index_scan", Native;
	IndexUnion => "phys.index_union", Native;
	Bitmap => "phys.bitmap", Native;
	IndexLookup => "phys.index_lookup", Native;
	FullTextScan => "phys.fulltext_scan", Native;
	KeyRangeScan => "phys.key_range_scan", Native;
	KnnScan => "phys.knn_scan", Native;
	KnnTopK => "phys.knn_topk", Native;
	Count => "phys.count", Native;
	RecordScan => "phys.record_scan", Native;
	Vacant => "phys.vacant", Native;
	Enforced => "phys.enforced", Native;
	Values => "phys.values", Native;
	Subquery => "phys.subquery", Native;
	Probe => "phys.probe", Native;
	Union => "phys.union", Native;
	Join => "phys.join", Native;
	Distinct => "phys.distinct", Native;
	PermissionFilter => "phys.permission_filter", Native;
	Layer => "phys.layer", Native;
	Filter => "phys.filter", Native;
	Fetch => "phys.fetch", Native;
	Split => "phys.split", Native;
	Sort => "phys.sort", Native;
	Window => "phys.window", Native;
	Aggregate => "phys.aggregate", Native;
	Limit => "phys.limit", Native;
	Project => "phys.project", Native;
	Timeout => "phys.timeout", Native;
	VersionScope => "phys.version_scope", Native;
	Only => "phys.only", Native;
	Explain => "phys.explain", Native;
	Eval => "phys.eval", Native;
	Request => "phys.request", ViaRequest;
	Txn => "phys.txn", ViaRequest;
	Stmt => "phys.stmt", ViaRequest;
	Statement => "phys.statement", Native;
	Match => "phys.match", Native;
	MatchStep => "phys.match_step", Native;
	DdlGuard => "phys.ddl_guard", Native;
	Define => "phys.define", Native;
	Remove => "phys.remove", Native;
	Alter => "phys.alter", Native;
	Rebuild => "phys.rebuild", Native;
	Info => "phys.info", Native;
	Use => "phys.use", Native;
	Option => "phys.option", Native;
	Access => "phys.access", Native;
	Kill => "phys.kill", Native;
	Live => "phys.live", Native;
	Subscriber => "phys.subscriber", ViaUnit;
	Show => "phys.show", Native;
	Write => "phys.write", Native;
	WriteStage => "phys.write_stage", ViaWrite;
	Events => "phys.events", ViaUnit;
	Assign => "phys.assign", ViaWrite;
	Unset => "phys.unset", ViaWrite;
	Content => "phys.content", ViaWrite;
	Block => "phys.block", Native;
	Let => "phys.let", Native;
	Sleep => "phys.sleep", Native;
	If => "phys.if", Native;
	Loop => "phys.loop", Native;
	Pipeline => "phys.pipeline", Native;
	Choose => "phys.choose", NotYet;
	Prefetch => "phys.prefetch", ViaLoop;
	Materialize => "phys.materialize", NotYet;
	LoadUnit => "phys.load_unit", NotYet;
	Yield => "phys.yield", Native;
	Return => "phys.return", Native;
	Break => "phys.break", Native;
	Next => "phys.next", Native;
}

// Native: the operand subset of `expr.rs`, the path steps of `path.rs` and the making and the
// invocation of a closure value (`closure.rs`). `expr.dispatch` is the check `expr.method` and
// `expr.invoke` read, `expr.yield` ends the region of a lazy operand or an arm, and
// `expr.throw` and `expr.raise` end a region that raises, or a list of statements (`block.rs`). An
// arm whose payload no producer builds answers an invariant error.
coverage_table! {
	ExprTag, expr_coverage, expr_refusal;
	Literal => "expr.literal", Native;
	Array => "expr.array", Native;
	Set => "expr.set", Native;
	Object => "expr.object", Native;
	RecordId => "expr.record_id", Native;
	FreeParam => "expr.free_param", Native;
	SessionParam => "expr.session_param", Native;
	CatalogParam => "expr.catalog_param", Native;
	Table => "expr.table", Native;
	Mock => "expr.mock", Native;
	Constant => "expr.constant", Native;
	Field => "expr.field", Native;
	ComputedRead => "expr.computed_read", Native;
	Doc => "expr.doc", Native;
	Access => "expr.access", Native;
	Deref => "expr.deref", Native;
	Index => "expr.index", Native;
	First => "expr.first", Native;
	Last => "expr.last", Native;
	Each => "expr.each", Native;
	Flatten => "expr.flatten", Native;
	Dispatch => "expr.dispatch", Native;
	Method => "expr.method", Native;
	Destructure => "expr.destructure", Native;
	Optional => "expr.optional", Native;
	RepeatRecurse => "expr.repeat_recurse", Native;
	SetField => "expr.set_field", Native;
	TupleRow => "expr.tuple_row", ViaWrite;
	Prefix => "expr.prefix", Native;
	Convert => "expr.convert", Native;
	Postfix => "expr.postfix", Native;
	Invoke => "expr.invoke", Native;
	Binary => "expr.binary", Native;
	ShortCircuit => "expr.short_circuit", Native;
	Call => "expr.call", Native;
	ClosureCreate => "expr.closure_create", Native;
	If => "expr.if", Native;
	Throw => "expr.throw", Native;
	Subquery => "expr.subquery", Native;
	Yield => "expr.yield", Native;
	Truthy => "expr.truthy", Native;
	Agg => "expr.agg", Native;
	Now => "expr.now", Native;
	Raise => "expr.raise", Native;
	DepthCharge => "expr.depth_charge", Native;
}

/// What `IrError::NotYet` names for a logical operation, which a physical container holds none
/// of.
const LOGICAL_REFUSAL: &str = "ir_lower::lower: a logical operation in a physical container";

/// The mnemonic of `kind` and how the direct lowering treats it.
pub(super) fn coverage_of(kind: &OpKind) -> (&'static str, Coverage) {
	match kind {
		OpKind::Expr(op) => {
			let tag = op.tag();
			(tag.mnemonic(), expr_coverage(tag))
		}
		OpKind::Logical(op) => (op.tag().mnemonic(), Coverage::NotYet),
		OpKind::Phys(op) => {
			let tag = op.tag();
			(tag.mnemonic(), phys_coverage(tag))
		}
	}
}

/// What `IrError::NotYet` names for an operation of `kind` that is not `Native`.
pub(super) fn refusal_of(kind: &OpKind) -> &'static str {
	match kind {
		OpKind::Expr(op) => expr_refusal(op.tag()),
		OpKind::Logical(_) => LOGICAL_REFUSAL,
		OpKind::Phys(op) => phys_refusal(op.tag()),
	}
}

/// Every operation of `ir` whose [`Coverage`] is not `Native`, in walk order, with its mnemonic.
///
/// Walks every operation, in every region, and never stops early: unlike [`lower`], which stops
/// at the first refusal, this measures how much of a container the direct lowering declines.
#[allow(dead_code)] // Called by the corpus test that measures what the lowering declines.
pub(crate) fn coverage_report(ir: &Ir) -> Vec<(OpId, &'static str, Coverage)> {
	let mut report = Vec::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		if let Some(data) = ir.op(op) {
			let (mnemonic, coverage) = coverage_of(&data.kind);
			if coverage != Coverage::Native {
				report.push((op, mnemonic, coverage));
			}
		}
		Walk::Continue
	});
	report
}

/// Lowers the physical container `ir` to a program, reading no index, under the full builtin
/// registry. For tests.
///
/// A container that reads an index is refused: [`lower_indexed`] takes the resolver that names the
/// index each source opens.
#[cfg(test)]
pub(crate) fn lower(ir: &Ir, facts: &FactCache) -> Result<Lowered, IrError> {
	lower_indexed(ir, facts, &NoIndexes)
}

/// Lowers the physical container `ir` to a program, resolving the indexes its sources read through
/// `indexes`.
///
/// A container whose top region holds a `phys.write` is the write statement of
/// [`write_stmt::lower_write`](super::write_stmt::lower_write): the subjects it names, the body of
/// its orders and the clauses of its stages. A container whose top region holds `phys.pipeline`
/// ops, or yields the rows of a source alone ([`makes_rows`]), is the read statement of
/// [`pipe::lower_reads`]. Any other container is a scalar statement, which walks the operations
/// of the top region in order. An operation whose [`Coverage`] is not
/// `Native` answers `IrError::NotYet` naming its mnemonic, so a `ViaLoop` operation reached
/// outside a lowered loop is refused too. A `ViaWrite` operation, which only the write that holds
/// it lowers, answers `IrError::Invariant`. The native operations are:
///
/// - `phys.write` with the subject loop that delivers its subjects, and the values the subjects are
///   made of. A write over rows a read yields answers `NotYet`.
///
/// - `phys.eval` of a value in the operand subset, which becomes the program's one pipeline: a
///   scalar pipeline over no source that loads the value, emits it and ends its row. An eval with
///   `FETCH` regions, or a second eval, answers `NotYet`.
/// - the operations of the operand subset, which lower where `phys.eval` reads them. One that no
///   operation reads answers `NotYet`, because nothing would ever run it, and so does one that only
///   a nested region reads, because it would be skipped whenever the region is. A literal is exempt
///   from the second, as it emits no instruction.
/// - a `phys.loop` of kind element filter, broadcast, closure, recursion or graph step whose result
///   an operation reads, which lowers where it is read. A loop of any other kind, and one that
///   defines no value, answers `NotYet`.
/// - `phys.info`, which becomes the program's one pipeline in the same way
///   ([`super::schema::SchemaLowering::info`]), and so do `phys.access`
///   ([`super::schema::SchemaLowering::access`]) and `phys.show`
///   ([`super::schema::SchemaLowering::show`]), whose value is the rows of the change feed rather
///   than one row.
/// - `phys.let` of a session parameter, whose program is the value it binds, lowered as an eval of
///   it. The kind the value is coerced to is the request step's, not the program's.
/// - the control flow of a statement, when the top region holds any: a block, an `IF`, a `FOR`
///   loop, `SLEEP`, a `RETURN`, `BREAK` or `CONTINUE`, a `THROW` or an error raised where the
///   statement stands, or a `LET` of a local binding. The container is then one statement program
///   ([`block::lower_program`]).
/// - `phys.yield` with no operand, which delivers nothing. One that delivers a value out of the top
///   region answers `NotYet` for the same reason.
///
/// A scalar container holds one statement: an eval, an `INFO`, an `ACCESS`, a `SHOW CHANGES`, a
/// session `LET` or a statement program.
///
/// Answers `IrError::NotYet` for a container not past the physical boundary and for one that
/// holds no `phys.eval`. The facts are unused: the subset lowers every comparison to its generic
/// form.
///
/// The instructions each operation emitted for itself, leaving out those of its operands and of
/// its regions, are checked against what the operation claims by [`check_native_claim`]: one that
/// lies outside its claim is an internal error.
#[cfg(test)]
pub(crate) fn lower_indexed(
	ir: &Ir,
	facts: &FactCache,
	indexes: &dyn IndexResolver,
) -> Result<Lowered, IrError> {
	lower_with(&mut LowerCx::detached().with_indexes(indexes), ir, facts)
}

/// [`lower_indexed`] under the context `cx`, which holds what the lowering may read (the index
/// resolver among it) and the units it asks for: the requests it made are the `needs` of the
/// result.
pub(crate) fn lower_with(
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	facts: &FactCache,
) -> Result<Lowered, IrError> {
	// A plain `EXPLAIN` answers the rendering of its plan, which nothing of the explained statement
	// runs to produce.
	if ir.phase() >= Phase::Physical
		&& let Some((explain, false)) = explain_in(ir)
	{
		return explained_served(cx)?.lower_plain(cx, ir, explain);
	}
	// `EXPLAIN ANALYZE` answers the rendering of what the run of the explained statement measured,
	// which the statement runs as the one group of a statement program to produce.
	if ir.phase() >= Phase::Physical
		&& let Some((explain, true)) = explain_in(ir)
	{
		let mut em = Emitter::new();
		let program = block::lower_analysed_program(&mut em, cx, ir, explain)?;
		return statement_program(cx, ir, em, program);
	}
	if ir.phase() >= Phase::Physical
		&& let Some((timeout, write)) = timed_write_in(ir, ir.top())?
	{
		return lower_timed_write_statement(cx, ir, timeout, write);
	}
	if ir.phase() >= Phase::Physical
		&& let Some(write) = write_in(ir)
	{
		let lowered = lower_write_statement(cx, ir, write)?;
		return Ok(lowered);
	}
	if ir.phase() >= Phase::Physical
		&& let Some(matched) = matching::match_in(ir)
	{
		let mut lowered = matching::lower_match(cx, ir, matched)?;
		lowered.needs = cx.take_needed();
		return Ok(lowered);
	}
	// A top region that is a statement's own region, a statement copied out of an expression or a
	// `{ ... }` block run as a unit in value position ends in the yield of the statement's value,
	// which the statement's op emits as the program's rows.
	let block_unit =
		matches!(ir.region(ir.top()).map(|top| top.kind), Some(RegionKind::ValueBlock));
	let yields = block_unit
		|| matches!(
			ir.region(ir.top()).map(|top| top.kind),
			Some(RegionKind::Statement(_) | RegionKind::Subquery)
		);
	if ir.phase() >= Phase::Physical && ops_in(ir, ir.top()).any(|op| makes_rows(ir, op)) {
		let mut lowered = pipe::lower_reads(cx, ir, yields)?;
		lowered.needs = cx.take_needed();
		return Ok(lowered);
	}
	if ir.phase() < Phase::Physical {
		return Err(IrError::NotYet {
			what: "ir_lower::lower: not physical",
		});
	}
	if let Some(op) = schema_op(ir, ir.top())? {
		return schema_program(cx, ir, facts, op);
	}
	// An expression nothing reads still runs where it stands, as a statement of the list does,
	// since it may raise or write.
	if block::holds_control(ir)? || holds_unread(ir) {
		return lower_statement_program(cx, ir, None);
	}
	// A block unit of expressions alone is the value it yields, as a statement of expressions
	// copied out of an expression is.
	if block_unit
		&& ops_in(ir, ir.top()).all(|op| {
			matches!(
				ir.op(op).map(|data| &data.kind),
				Some(OpKind::Expr(_) | OpKind::Phys(PhysOp::Yield))
			)
		}) {
		return lower_value_unit(cx, ir, false);
	}
	let mut em = Emitter::new();
	let mut eval: Option<(Pipeline, Needs, Vec<Claim>)> = None;
	// The value the `phys.eval` defines, which the yield of a block unit delivers: its program
	// emits the eval's value as its row.
	let mut evaluated: Option<ValueId> = None;
	let mut other_statement = false;
	// Whether the statement's value is the rows its pipeline emits rather than its one row.
	let mut rows = false;
	for op in ops_in(ir, ir.top()) {
		let data = ir.op(op).ok_or(IrError::Stale("ir_lower::lower: op"))?;
		match coverage_of(&data.kind).1 {
			Coverage::Native => {}
			Coverage::ViaWrite => {
				return Err(IrError::Invariant {
					what: "ir_lower::lower: a row, stage or clause of a write is lowered by its write, not as a statement",
				});
			}
			Coverage::ViaRequest => {
				return Err(IrError::Invariant {
					what: "a request step is driven from its attributes and never lowered as a program",
				});
			}
			Coverage::ViaUnit => {
				return Err(IrError::Invariant {
					what: "ir_lower::lower: the body of a subscription or events unit is lowered by the unit it tops, not as a statement",
				});
			}
			Coverage::NotYet | Coverage::ViaLoop => {
				return Err(IrError::NotYet {
					what: refusal_of(&data.kind),
				});
			}
		}
		match &data.kind {
			OpKind::Phys(phys) if phys.tag() == PhysTag::Eval => {
				if eval.is_some() {
					return Err(IrError::NotYet {
						what: match other_statement {
							true => "ir_lower::lower: more than one statement",
							false => "ir_lower::lower: more than one phys.eval",
						},
					});
				}
				if !data.regions.is_empty() {
					return Err(IrError::NotYet {
						what: "ir_lower::lower: phys.eval with fetch regions",
					});
				}
				let [value] = <[ValueId; 1]>::try_from(data.operands.as_slice())
					.map_err(|_| IrError::Stale("ir_lower::lower: phys.eval operand arity"))?;
				eval = Some(scalar_pipeline(cx, &mut em, ir, op, value)?);
				evaluated = data.results.first().copied();
			}
			OpKind::Phys(phys) if phys.tag() == PhysTag::Info => {
				if eval.is_some() {
					return Err(IrError::NotYet {
						what: "ir_lower::lower: more than one statement",
					});
				}
				let info = served(cx)?.info(&mut em, cx, ir, op)?;
				eval = Some((info.pipeline, info.needs, info.claims));
				other_statement = true;
			}
			OpKind::Phys(PhysOp::Show(_)) => {
				if eval.is_some() {
					return Err(IrError::NotYet {
						what: "ir_lower::lower: more than one statement",
					});
				}
				eval = Some((served(cx)?.show(&mut em, ir, op)?, Needs::none(), Vec::new()));
				other_statement = true;
				rows = true;
			}
			OpKind::Phys(PhysOp::Access(_)) => {
				if eval.is_some() {
					return Err(IrError::NotYet {
						what: "ir_lower::lower: more than one statement",
					});
				}
				eval = Some(served(cx)?.access(cx, &mut em, ir, op)?);
				other_statement = true;
			}
			OpKind::Phys(PhysOp::Kill) => {
				if eval.is_some() {
					return Err(IrError::NotYet {
						what: "ir_lower::lower: more than one statement",
					});
				}
				let kill = served(cx)?.kill(&mut em, cx, ir, op)?;
				eval = Some((kill.pipeline, kill.needs, kill.claims));
				other_statement = true;
			}
			OpKind::Phys(PhysOp::Live(_)) => {
				if eval.is_some() {
					return Err(IrError::NotYet {
						what: "ir_lower::lower: more than one statement",
					});
				}
				let live = served(cx)?.live(&mut em, cx, ir, op)?;
				eval = Some((live.pipeline, live.needs, live.claims));
				other_statement = true;
			}
			OpKind::Phys(PhysOp::Option {
				..
			}) => {
				if eval.is_some() {
					return Err(IrError::NotYet {
						what: "ir_lower::lower: more than one statement",
					});
				}
				let option = served(cx)?.option(&mut em, cx, ir, op)?;
				eval = Some((option.pipeline, option.needs, option.claims));
				other_statement = true;
			}
			OpKind::Phys(PhysOp::Use {
				..
			}) => {
				if eval.is_some() {
					return Err(IrError::NotYet {
						what: "ir_lower::lower: more than one statement",
					});
				}
				let used = served(cx)?.use_(&mut em, cx, ir, op)?;
				eval = Some((used.pipeline, used.needs, used.claims));
				other_statement = true;
			}
			OpKind::Phys(PhysOp::Let(_)) => {
				if eval.is_some() {
					return Err(IrError::NotYet {
						what: "ir_lower::lower: more than one statement",
					});
				}
				let bound = lower_let(&mut em, cx, ir, op)?;
				eval = Some((bound.pipeline, bound.needs, bound.claims));
				other_statement = true;
			}
			OpKind::Phys(phys) if phys.tag() == PhysTag::Yield => {
				if !data.operands.is_empty() && !yields {
					return Err(IrError::NotYet {
						what: "ir_lower::lower: a yield out of the top region",
					});
				}
				if block_unit && (other_statement || data.operands.first() != evaluated.as_ref()) {
					return Err(IrError::NotYet {
						what: "ir_lower::lower: a block unit whose yield is not the value of its eval",
					});
				}
			}
			OpKind::Expr(expr) if expr.tag() == ExprTag::Yield => {
				if !data.operands.is_empty() && !yields {
					return Err(IrError::NotYet {
						what: "ir_lower::lower: a yield out of the top region",
					});
				}
			}
			OpKind::Expr(expr)
				if matches!(expr.tag(), ExprTag::Throw | ExprTag::Raise | ExprTag::DepthCharge) =>
			{
				// An op that ends a region defines no value, so no read would lower it.
				return Err(IrError::NotYet {
					what: "ir_lower::lower: a terminator out of the top region",
				});
			}
			OpKind::Phys(PhysOp::Loop {
				kind,
				..
			}) => {
				// A loop in value position lowers where its reader asks for it, as an expression
				// does. Any other loop is a statement or a source that nothing lowers here, and a
				// loop with no result would be skipped.
				let in_value_position =
					matches!(
						kind,
						LoopKind::ElementFilter
							| LoopKind::Broadcast | LoopKind::Closure
							| LoopKind::Recurse | LoopKind::GraphStep
					);
				if !in_value_position || data.results.is_empty() {
					return Err(IrError::NotYet {
						what: refusal_of(&data.kind),
					});
				}
				check_read(ir, data, false)?;
			}
			OpKind::Expr(expr) => {
				// The value lowers where its reader asks for it, in the scope of the top region.
				check_read(ir, data, expr.tag() == ExprTag::Literal)?;
			}
			// The pipeline the physical phase wraps a loop in value position in, which lowers
			// where it is read as the loop does.
			OpKind::Phys(PhysOp::Pipeline(_)) if pipe::is_value_pipeline(ir, op) => {
				check_read(ir, data, false)?;
			}
			OpKind::Phys(phys) => {
				// A native op of a pipeline that stands outside every pipeline: a source or a
				// streaming op with no pipeline to run it in.
				return Err(IrError::NotYet {
					what: phys_refusal(phys.tag()),
				});
			}
			OpKind::Logical(_) => {
				return Err(IrError::Stale("ir_lower::lower: a native tag with no arm"));
			}
		}
	}
	let Some((pipeline, needs, claims)) = eval else {
		return Err(IrError::NotYet {
			what: "ir_lower::lower: a container with no phys.eval",
		});
	};
	let mut program = em.finish_program(vec![pipeline], needs.access_mode)?;
	program.scalar = !rows;
	apply_needs(&mut program, &needs);
	if let Some(pipeline) = program.pipelines.first() {
		check_claims(ir, &claims, &pipeline.code, &program, cx.registry())?;
	}
	Ok(Lowered {
		program,
		provenance: Provenance::default(),
		needs: cx.take_needed(),
	})
}

/// Lowers the top region of `ir` as one statement program ([`block::lower_program`]): the pipelines
/// of its statements' groups, then its own over [`Source::Once`], which emits the region's value.
/// `row` is the argument of the top block that names the row the program is run over, if any.
fn lower_statement_program(
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	row: Option<ValueId>,
) -> Result<Lowered, IrError> {
	let mut em = Emitter::new();
	let program = block::lower_program(&mut em, cx, ir, row)?;
	statement_program(cx, ir, em, program)
}

/// The program the statement program `lowered` into `em` makes: the pipelines of its statements'
/// groups, then its own.
fn statement_program(
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	em: Emitter,
	lowered: block::BlockProgram,
) -> Result<Lowered, IrError> {
	let block::BlockProgram {
		pipeline,
		needs,
		claims,
		access_mode,
		rows,
		groups,
		reservations,
	} = lowered;
	let block::Groups {
		pipelines: mut grouped,
		claims: group_claims,
		specs,
		limits,
		distincts,
		sorts,
		aggregates,
		splits,
		joins,
		probes,
		provenance,
		analyses,
		served,
	} = groups;
	// The program's pipelines are those of its statements' groups, then its own: a group is
	// run by the pipelines stamped with it, and the program by the ones stamped with none.
	let own = grouped.len();
	grouped.push(pipeline);
	let mut program = em.finish_program(grouped, access_mode)?;
	program.scalar = true;
	program.rows = rows;
	program.groups = specs;
	program.limits = limits;
	program.distincts = distincts;
	program.sorts = sorts;
	program.aggregates = aggregates;
	program.splits = splits;
	program.joins = joins;
	program.probes = probes;
	program.analyses = analyses;
	program.served = served;
	apply_reservations(&mut program, reservations);
	apply_needs(&mut program, &needs);
	for (pipeline, claimed) in program.pipelines.iter().zip(&group_claims) {
		check_claims(ir, claimed, &pipeline.code, &program, cx.registry())?;
	}
	if let Some(pipeline) = program.pipelines.get(own) {
		check_claims(ir, &claims, &pipeline.code, &program, cx.registry())?;
	}
	super::super::morsel::wire::attach_units(&mut program);
	Ok(Lowered {
		program,
		provenance,
		needs: cx.take_needed(),
	})
}

/// Lowers the body of a user-defined function, a container whose top region is the body, as one
/// statement program, as main lowers a body: its statements in order, the value of the last as the
/// program's row, and a `RETURN` no scope of the body catches leaving the program with its value.
/// A body that holds no control flow is lowered the same way, so that its closing yield is the
/// program's value.
///
/// The top region takes the statement timestamp, which no instruction reads (the clock is read
/// from the context), then the row of the caller, which the unit is run over. The definition's
/// arguments are read by name from the frame the call binds them in.
pub(crate) fn lower_body_unit(cx: &mut LowerCx<'_>, ir: &Ir) -> Result<Lowered, IrError> {
	if ir.phase() < Phase::Physical {
		return Err(IrError::NotYet {
			what: "ir_lower::lower: not physical",
		});
	}
	let args = ir
		.region(ir.top())
		.and_then(|top| top.blocks.first().copied())
		.and_then(|block| ir.block(block))
		.map(|block| block.args.as_slice())
		.ok_or(IrError::Stale("ir_lower::lower: the top region's block"))?;
	let &[_, row] = args else {
		return Err(IrError::Invariant {
			what: "a function body whose top region takes other arguments than the timestamp and the row",
		});
	};
	lower_statement_program(cx, ir, Some(row))
}

/// Lowers a statement copied out of an expression as a unit: its region ends in the yield of the
/// statement's value. A read lowers as a read statement whose rows that yield delivers; a statement
/// that is expressions only is one scalar pipeline that emits the value; any other statement (a
/// write, a schema statement, control flow) lowers as the statement it is.
pub(crate) fn lower_subquery_unit(
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	facts: &FactCache,
) -> Result<Lowered, IrError> {
	if ir.phase() < Phase::Physical {
		return Err(IrError::NotYet {
			what: "ir_lower::lower: not physical",
		});
	}
	// An `EXPLAIN` and a write, alone or in the `Deadline` region of its `TIMEOUT`, hold pipelines
	// that would pass for a read's, the explained statement's and the one the physical phase wraps
	// a write's subjects in. Both go to `lower_with`, which lowers a plain `EXPLAIN` and a write
	// before it looks for a read; the rows of a read statement are never the value of an
	// `EXPLAIN ANALYZE`.
	let statement = explain_in(ir).is_some()
		|| write_in(ir).is_some()
		|| timed_write_in(ir, ir.top())?.is_some();
	if !statement && ops_in(ir, ir.top()).any(|op| makes_rows(ir, op)) {
		let mut lowered = pipe::lower_reads(cx, ir, true)?;
		lowered.needs = cx.take_needed();
		return Ok(lowered);
	}
	let expressions_only = ops_in(ir, ir.top())
		.all(|op| matches!(ir.op(op).map(|data| &data.kind), Some(OpKind::Expr(_))));
	match expressions_only {
		true => lower_value_unit(cx, ir, false),
		false => lower_with(cx, ir, facts),
	}
}

/// Lowers a unit copied out of its requester whose top region is a value: one scalar pipeline that
/// emits the value the region yields, as main lowers a recursion's body and a statement of
/// expressions in an expression. Where `row`, the region is a recursion's body, whose arguments are
/// the element the unit is run with as its row, which is not read as an object (each depth of a
/// recursion is whatever the body reached), and the depth's position, which the body never reads;
/// otherwise the region takes no argument of its own. The unit's inputs follow its own arguments.
pub(crate) fn lower_value_unit(
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	row: bool,
) -> Result<Lowered, IrError> {
	if ir.phase() < Phase::Physical {
		return Err(IrError::NotYet {
			what: "ir_lower::lower: not physical",
		});
	}
	let top = ir.top();
	let inputs = usize::from(UnitSink::unit_inputs(cx));
	let mut em = Emitter::new();
	let (pipeline, needs, claims) = {
		let mut lowerer = Lowerer::in_context(&mut em, ir, cx);
		let args = lowerer.region_args(top)?;
		let own = args.len().checked_sub(inputs).and_then(|own| args.get(..own)).ok_or(
			IrError::Invariant {
				what: "a value unit whose region takes fewer arguments than its inputs",
			},
		)?;
		match (own, row) {
			(&[element, _position], true) => lowerer.bind_row(element, false),
			(&[], false) => {}
			_ => {
				return Err(IrError::Invariant {
					what: "a value unit whose region takes other arguments than its row",
				});
			}
		}
		let value = lowerer.region_value(top)?;
		let pipeline = lowerer.emit_value_pipeline(value)?;
		(pipeline, lowerer.needs(), lowerer.take_claims())
	};
	let mut program = em.finish_program(vec![pipeline], needs.access_mode)?;
	program.scalar = true;
	apply_needs(&mut program, &needs);
	if let Some(pipeline) = program.pipelines.first() {
		check_claims(ir, &claims, &pipeline.code, &program, cx.registry())?;
	}
	Ok(Lowered {
		program,
		provenance: Provenance::default(),
		needs: cx.take_needed(),
	})
}

/// Lowers the `PERMISSIONS` predicate of a user-defined function, planned as a unit whose top
/// region is a permission frame, as main lowers the guard of a call: a program with no pipeline
/// of its own whose host pipeline `0` evaluates the predicate once, over the row the host hands
/// it, and emits its value. The call judges that host pipeline as it enters
/// (`vm::function::enter`).
///
/// The top region takes the statement timestamp, which no instruction reads, then the row, which
/// is not read as an object (main lowers a guard with `rows_are_objects` off); any other count is
/// an invariant break.
pub(crate) fn lower_guard_unit(cx: &mut LowerCx<'_>, ir: &Ir) -> Result<Lowered, IrError> {
	if ir.phase() < Phase::Physical {
		return Err(IrError::NotYet {
			what: "ir_lower::lower: not physical",
		});
	}
	let top = ir.top();
	let mut em = Emitter::new();
	let (pipeline, needs, claims) = {
		let mut lowerer = Lowerer::in_context(&mut em, ir, cx);
		let &[_, row] = lowerer.region_args(top)?.as_slice() else {
			return Err(IrError::Invariant {
				what: "a guard whose region takes other arguments than the timestamp and the row",
			});
		};
		lowerer.bind_row(row, false);
		let value = lowerer.region_value(top)?;
		let pipeline = lowerer.emit_value_pipeline(value)?;
		(pipeline, lowerer.needs(), lowerer.take_claims())
	};
	if em.host_code(pipeline)? != 0 {
		return Err(IrError::Invariant {
			what: "a guard's predicate is not its unit's first host pipeline",
		});
	}
	let mut program = em.finish_program(Vec::new(), needs.access_mode)?;
	apply_needs(&mut program, &needs);
	if let Some(pipeline) = program.host_code.first() {
		check_claims(ir, &claims, &pipeline.code, &program, cx.registry())?;
	}
	Ok(Lowered {
		program,
		provenance: Provenance::default(),
		needs: cx.take_needed(),
	})
}

/// Lowers the body of a closure literal, copied out of its requester, as the unit a value of the
/// literal runs: one scalar pipeline that emits the value the body region yields, as main lowers a
/// closure's body.
///
/// The region takes the statement timestamp, the literal's parameters, its captures in the order
/// `template` names them, then the invoking row where the body reads one and, after it, that
/// frame's document root where the body reads it apart from the row; a different count is an
/// invariant break. A parameter or a capture is a read of the unit's slot of that name
/// ([`ClosureTemplate::slots`]), which an invocation fills with its arguments and the value's
/// captures. The invoking row is the unit's row, which is not read as an object: it is whatever
/// value the invocation runs the body over. The document root is [`Instr::DocumentRoot`] of that
/// row. The timestamp is read by no instruction.
///
/// The head names the template's literal, digest, captures, row captures and slots, the frame
/// the context lowers for and the effects of the body's ops. The unit always runs in a frame of
/// its own (`frameless` is false), and it names no catalog reads beyond the stamps the template
/// states.
pub(crate) fn lower_closure_unit(
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	template: &ClosureTemplate,
) -> Result<Lowered, IrError> {
	if ir.phase() < Phase::Physical {
		return Err(IrError::NotYet {
			what: "ir_lower::lower: not physical",
		});
	}
	let top = ir.top();
	let slots = template.slots();
	let params = template.closure.args.len();
	let mut em = Emitter::new();
	let (pipeline, needs, claims) = {
		let mut lowerer = Lowerer::in_context(&mut em, ir, cx);
		let args = lowerer.region_args(top)?;
		let expected = 1
			+ params + template.captures.len()
			+ usize::from(template.row)
			+ usize::from(template.root);
		if args.len() != expected {
			return Err(IrError::Invariant {
				what: "a closure body whose region takes other arguments than its literal names",
			});
		}
		let names = template
			.closure
			.args
			.iter()
			.map(|(arg, _)| arg.as_str())
			.chain(template.captures.iter().map(String::as_str));
		for (arg, name) in args[1..].iter().zip(names) {
			let slot = slots
				.iter()
				.position(|held| held == name)
				.and_then(|slot| u16::try_from(slot).ok())
				.ok_or(IrError::Invariant {
					what: "a closure body binding a name its unit holds no slot for",
				})?;
			lowerer.bind_slot_value(*arg, slot, Some(Strand::new(name)));
		}
		let rows = &args[1 + params + template.captures.len()..];
		match (template.row, template.root, rows) {
			(false, false, []) => {}
			(true, false, [row]) => lowerer.bind_closure_row(*row),
			(true, true, [row, root]) => {
				lowerer.bind_closure_row(*row);
				lowerer.bind_closure_root(*root);
			}
			_ => {
				return Err(IrError::Invariant {
					what: "a closure body whose rows its literal does not name",
				});
			}
		}
		let value = lowerer.region_value(top)?;
		let pipeline = lowerer.emit_value_pipeline(value)?;
		(pipeline, lowerer.needs(), lowerer.take_claims())
	};
	let mut program = em.finish_program(vec![pipeline], needs.access_mode)?;
	program.scalar = true;
	apply_needs(&mut program, &needs);
	if let Some(pipeline) = program.pipelines.first() {
		check_claims(ir, &claims, &pipeline.code, &program, cx.registry())?;
	}
	program.unit = Some(UnitHead {
		sig: template.sig(),
		frame: cx.frame().clone(),
		entry: None,
		closure: Some(ClosureHead {
			digest: template.digest,
			closure: Arc::clone(&template.closure),
			isa: cx.isa(),
			captured: template.captured(),
			rows: template.rows(),
			slots,
			frameless: false,
			effects: region_effects(ir, cx.registry(), top),
			stamps: template.stamps,
			reads: None,
			version: template.version,
		}),
	});
	Ok(Lowered {
		program,
		provenance: Provenance::default(),
		needs: cx.take_needed(),
	})
}

/// What running the ops of `region` may do beyond computing values: the union of their effects,
/// the regions they hold included.
fn region_effects(ir: &Ir, registry: &FunctionRegistry, region: RegionId) -> EffectSet {
	let mut facts = FactCache::new();
	let under = structural_cx(registry, ir);
	ops_in(ir, region)
		.map(|op| effects::summary_with(ir, &mut facts, &under, op).set)
		.fold(EffectSet::PURE, EffectSet::union)
}

/// Whether `op` makes the top region of `ir` the region of a read statement: a pipeline or a
/// timed read ([`pipe::is_read_op`]), or a source or a statement in `FROM` whose rows the yield
/// that ends the region delivers, alone or under the `ONLY` or `VERSION` that wraps them, which a
/// pipeline of its own reads as the statement's output.
fn makes_rows(ir: &Ir, op: OpId) -> bool {
	if pipe::is_read_op(ir, op) {
		return true;
	}
	let Some(data) = ir.op(op) else {
		return false;
	};
	let OpKind::Phys(kind) = &data.kind else {
		return false;
	};
	(read::is_source(kind.tag()) || kind.tag() == PhysTag::Subquery) && yielded_at_top(ir, data)
}

/// Whether the yield that ends the top region of `ir` delivers the rows `data` defines, directly
/// or through the `phys.only` or `phys.version_scope` that wraps them there.
fn yielded_at_top(ir: &Ir, data: &OpData) -> bool {
	data.results.iter().any(|result| {
		ir.uses(*result).iter().any(|read| {
			ir.enclosing_region(read.user) == ir.top()
				&& ir.op(read.user).is_some_and(|user| match &user.kind {
					OpKind::Phys(PhysOp::Yield) | OpKind::Expr(ExprOp::Yield) => true,
					OpKind::Phys(PhysOp::Only | PhysOp::VersionScope) => yielded_at_top(ir, user),
					OpKind::Phys(_) | OpKind::Expr(_) | OpKind::Logical(_) => false,
				})
		})
	})
}

/// The `phys.write` the top region of `ir` holds, if it holds one.
fn write_in(ir: &Ir) -> Option<OpId> {
	write_of(ir, ir.top())
}

/// The `phys.write` the region `region` holds, if it holds one.
pub(super) fn write_of(ir: &Ir, region: RegionId) -> Option<OpId> {
	ops_in(ir, region).find(|op| {
		matches!(ir.op(*op).map(|data| &data.kind), Some(OpKind::Phys(PhysOp::Write(_))))
	})
}

/// Lowers the container of the one write `write` as its program.
///
/// The top region holds the write, the loop that delivers its subjects (alone in the pipeline
/// that wraps it, once placed) and the values the subjects are made of, and nothing else: any
/// other statement answers `NotYet`, because one program lowers one statement. A write over the
/// rows a read yields also holds the ops of the read. The program's access mode is a transaction
/// that writes and its context is at least the database, whatever its code calls.
fn lower_write_statement(cx: &mut LowerCx<'_>, ir: &Ir, write: OpId) -> Result<Lowered, IrError> {
	check_write_region(ir, ir.top(), write)?;
	let mut em = Emitter::new();
	let lowered = write_stmt::lower_write(cx, &mut em, ir, write)?;
	let (mut program, provenance) = match lowered.pipelines {
		WritePipelines::Own(pipeline) => {
			(em.finish_program(vec![pipeline], lowered.needs.access_mode)?, Provenance::default())
		}
		WritePipelines::Over(reads) => {
			pipe::program_of(em, ir, cx.registry(), reads, None, lowered.needs.access_mode)?
		}
	};
	program.write = Some(lowered.spec);
	apply_needs(&mut program, &lowered.needs);
	Ok(Lowered {
		program,
		provenance,
		needs: cx.take_needed(),
	})
}

/// The `phys.timeout` the region `region` holds around a write, and that write: the timeout's
/// `Deadline` region holds the write, and `region` holds nothing else but the values the timeout's
/// duration is made of and the yield of its value.
pub(super) fn timed_write_in(ir: &Ir, region: RegionId) -> Result<Option<(OpId, OpId)>, IrError> {
	let mut found = None;
	for op in ops_in(ir, region) {
		let data = ir.op(op).ok_or(IrError::Stale("ir_lower::lower: op"))?;
		match &data.kind {
			OpKind::Phys(PhysOp::Timeout) => {
				let write = match data.regions.as_slice() {
					[deadline] => write_of(ir, *deadline),
					_ => None,
				};
				let Some(write) = write else {
					return Ok(None);
				};
				if found.replace((op, write)).is_some() {
					return Ok(None);
				}
			}
			OpKind::Phys(PhysOp::Yield) | OpKind::Expr(_) => {}
			OpKind::Phys(_) | OpKind::Logical(_) => return Ok(None),
		}
	}
	Ok(found)
}

/// Lowers the container of the one write `write` that the `phys.timeout` `timeout` wraps as its
/// program, as [`lower_write_statement`] lowers a write with no `TIMEOUT`. The duration is the
/// program's tail: a host scalar the run evaluates when the statement starts, which bounds the
/// write's run.
fn lower_timed_write_statement(
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	timeout: OpId,
	write: OpId,
) -> Result<Lowered, IrError> {
	let data = ir.op(timeout).ok_or(IrError::Stale("ir_lower::lower: op"))?;
	let ([duration], [deadline]) = (data.operands.as_slice(), data.regions.as_slice()) else {
		return Err(invariant("a phys.timeout with other than one operand and one region"));
	};
	check_write_region(ir, *deadline, write)?;
	let mut em = Emitter::new();
	let lowered =
		write_stmt::lower_write_in(cx, &mut em, ir, write, *deadline, pipe::PoolBase::default())?;
	let mut needs = Needs::none();
	let tail = Some(TailSpec {
		version: None,
		timeout: Some(read::host_scalar_of(&mut em, cx, ir, *duration, &mut needs)?),
		only: false,
	});
	let (mut program, provenance) = match lowered.pipelines {
		WritePipelines::Own(pipeline) => {
			let mut program = em.finish_program(vec![pipeline], lowered.needs.access_mode)?;
			program.tail = tail;
			(program, Provenance::default())
		}
		WritePipelines::Over(reads) => {
			pipe::program_of(em, ir, cx.registry(), reads, tail, lowered.needs.access_mode)?
		}
	};
	program.write = Some(lowered.spec);
	apply_needs(&mut program, &lowered.needs.merged(&needs));
	Ok(Lowered {
		program,
		provenance,
		needs: cx.take_needed(),
	})
}

/// Checks that the region `region` holds the one write `write` and only what it is made of: the
/// loop that delivers its subjects (alone in the pipeline that wraps it, once placed), the values
/// the subjects are made of and, for a write over the rows a read yields, the ops of the read or
/// the statement in `FROM` that gives them. Any other statement answers `NotYet`, because one
/// program or group lowers one statement.
pub(super) fn check_write_region(ir: &Ir, region: RegionId, write: OpId) -> Result<(), IrError> {
	let over_rows = matches!(
		ir.op(write).map(|data| &data.kind),
		Some(OpKind::Phys(PhysOp::Write(attrs)))
			if matches!(attrs.subjects, WriteSubjectsShape::Rows | WriteSubjectsShape::Guaranteed)
	);
	// The value the subject loop takes its subjects from.
	let subjects = ir
		.op(write)
		.and_then(|data| crate::exec::ir::logical::stages::write_loop(ir, data))
		.and_then(|looped| ir.op(looped))
		.and_then(|looped| looped.operands.first().copied());
	for op in ops_in(ir, region) {
		let data = ir.op(op).ok_or(IrError::Stale("ir_lower::lower: op"))?;
		let own = match &data.kind {
			OpKind::Phys(_) if over_rows && pipe::is_read_of_rows(ir, op) => true,
			// A statement in `FROM` whose rows the write takes as its subjects, or the subquery
			// whose rows are its targets, lowered by the target that reads it.
			OpKind::Phys(PhysOp::Subquery) => {
				over_rows || (subjects.is_some() && data.results.first().copied() == subjects)
			}
			OpKind::Phys(PhysOp::Write(_)) => op == write,
			// The `EXPLAIN ANALYZE` that renders what the run of the write measured.
			OpKind::Phys(PhysOp::Explain {
				analyze: true,
				..
			}) => data.operands.first().is_some_and(|read| {
				ir.op(write).is_some_and(|written| written.results.first() == Some(read))
			}),
			OpKind::Phys(PhysOp::Loop {
				kind: LoopKind::Write,
				..
			})
			| OpKind::Phys(PhysOp::Yield)
			| OpKind::Expr(_) => true,
			// A value the targets are read from: a loop in value position, bare or in the pipeline
			// the physical phase wraps it in, and a `{ ... }` block in value position, each
			// lowered in place by the target that reads it, and the values it yields.
			OpKind::Phys(PhysOp::Loop {
				kind:
					LoopKind::ElementFilter
					| LoopKind::Broadcast
					| LoopKind::Closure
					| LoopKind::Recurse
					| LoopKind::GraphStep,
				..
			}) => true,
			OpKind::Phys(PhysOp::Pipeline(_)) if pipe::is_value_pipeline(ir, op) => true,
			OpKind::Phys(PhysOp::Block) => true,
			OpKind::Phys(PhysOp::Values {
				read: ValuesRead::Expression {
					..
				},
			}) => true,
			// The pipeline placement wraps the subject loop in.
			OpKind::Phys(PhysOp::Pipeline(_)) => data.regions.first().is_some_and(|body| {
				ops_in(ir, *body).all(|inner| {
					matches!(
						ir.op(inner).map(|held| &held.kind),
						Some(
							OpKind::Phys(PhysOp::Yield)
								| OpKind::Phys(PhysOp::Loop {
									kind: LoopKind::Write,
									..
								})
						)
					)
				})
			}),
			OpKind::Phys(_) | OpKind::Logical(_) => false,
		};
		if !own {
			return Err(IrError::NotYet {
				what: "ir_lower::lower: more than one statement",
			});
		}
	}
	Ok(())
}

/// Lowers the schema statement whose op is `op` in the top region as the program of one
/// statement: one pipeline over [`Source::Once`] that answers `NONE`, in a transaction that
/// writes.
///
/// The ids the statement draws are its own: nothing reserves any, so its program holds no
/// reservations of the speculative run of a block.
fn schema_program(
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	facts: &FactCache,
	op: OpId,
) -> Result<Lowered, IrError> {
	let mut em = Emitter::new();
	let mut reservations = Reservations::default();
	let needs = served(cx)?.ddl(&mut em, ir, facts, cx, op, &mut reservations)?;
	let pipeline = em.finish_pipeline(Source::Once)?;
	let mut program = em.finish_program(vec![pipeline], AccessMode::ReadWrite)?;
	program.scalar = true;
	apply_reservations(&mut program, reservations);
	apply_needs(&mut program, &needs);
	Ok(Lowered {
		program,
		provenance: Provenance::default(),
		needs: cx.take_needed(),
	})
}

/// Gives `program` the draws its schema statements took of the ids reserved for them, and the
/// tables they define for its writes to share.
fn apply_reservations(program: &mut Program, reservations: Reservations) {
	program.reservations = reservations.taken;
	program.implicit_tables = reservations.implicit_tables;
}

/// Gives `program` what the code lowered into it needs of the run: the context level its builtins
/// require, whether rows must run one after another, the tables its instructions may read
/// through a value they are given, which decide the programs a schema change makes stale, and a
/// transaction that may write where the code calls something that runs statements.
///
/// Every program a lowering finishes passes through here, so a need recorded by a [`Lowerer`]
/// reaches the program whichever entry built it. The access mode only widens: a program the
/// finisher declared as one that writes stays one.
pub(super) fn apply_needs(program: &mut Program, needs: &Needs) {
	program.required_context = needs.context;
	program.keeps_row_order = needs.keeps_row_order;
	program.reach.clone_from(&needs.reach);
	program.access_mode = program.access_mode.combine(needs.access_mode);
}

/// Whether the top region of `ir` holds an expression, other than a literal, that defines a value
/// no operation reads.
fn holds_unread(ir: &Ir) -> bool {
	ops_in(ir, ir.top()).any(|op| {
		ir.op(op).is_some_and(|data| {
			matches!(&data.kind, OpKind::Expr(expr) if !matches!(expr.tag(), ExprTag::Literal | ExprTag::Yield))
				&& !data.results.is_empty()
				&& data.results.iter().all(|result| ir.uses(*result).is_empty())
		})
	})
}

/// Checks that the value the op `data` defines is read where lowering reaches it.
///
/// One nothing reads is never run, and one only a nested region reads would be skipped when the
/// region is. A literal emits no instruction and is exempt from the second. The closure a
/// closure loop at the top applies is read by the loop's call, not by the body that names it, so
/// that read counts as one at the top.
fn check_read(ir: &Ir, data: &OpData, exempt_from_top: bool) -> Result<(), IrError> {
	let used = data.results.iter().any(|result| !ir.uses(*result).is_empty());
	let read_at_top = data.results.iter().any(|result| {
		ir.uses(*result).iter().any(|read| {
			reads_where_its_owner_stands(ir, read.user)
				|| is_callee_of_top_closure_loop(ir, read.user)
		})
	});
	if !used && !data.results.is_empty() {
		return Err(IrError::NotYet {
			what: "ir_lower::lower: an expression no operation reads",
		});
	}
	if !read_at_top && !data.results.is_empty() && !exempt_from_top {
		return Err(IrError::NotYet {
			what: "ir_lower::lower: an expression only a nested region reads",
		});
	}
	Ok(())
}

/// Whether the op `user` reads its operands whenever the top-region op it stands under runs, so
/// that a value it alone reads is lowered once, where that op stands.
///
/// The regions from `user` up to the top region decide it:
/// - the body of a value pipeline, which lowers in place where the pipeline is read;
/// - a `{ ... }` block in value position, which lowers in place or runs as a unit at the block;
/// - a statement held in an expression (`RegionKind::Subquery`), whose unit takes every value it
///   reads from outside as an input bound where the statement stands, so a region below it that
///   runs on one path only reads the input, not the value.
///
/// Any other region (a lazy operand, an arm, the body of a loop) runs on one path only or once
/// per element, so a read in it is not a read where its owner stands.
fn reads_where_its_owner_stands(ir: &Ir, user: OpId) -> bool {
	let mut region = ir.enclosing_region(user);
	let mut on_one_path = false;
	while region != ir.top() {
		let Some(owner) = ir.parent_op(region) else {
			return false;
		};
		match ir.region(region).map(|held| held.kind) {
			Some(RegionKind::Subquery) => on_one_path = false,
			Some(RegionKind::ValueBlock) => {}
			_ if pipe::is_value_pipeline(ir, owner) => {}
			_ => on_one_path = true,
		}
		region = ir.enclosing_region(owner);
	}
	!on_one_path
}

/// Whether the op `op`, an expression or a value pipeline that stands in the region of a read
/// statement, is lowered by each of its readers where that reader asks for it: a second evaluation
/// answers what the first did and leaves nothing behind ([`EffectSet::is_deterministic`]), so a
/// clause of a pipeline that evaluates it once per row reads the value the statement's other
/// readers do. One nothing reads would never run.
pub(super) fn lowers_where_read(
	ir: &Ir,
	registry: &FunctionRegistry,
	op: OpId,
	data: &OpData,
) -> bool {
	let read = data.results.iter().any(|result| !ir.uses(*result).is_empty());
	read && effects::of_with(ir, &mut FactCache::new(), &structural_cx(registry, ir), op)
		.set
		.is_deterministic()
}

/// Whether `user` is the dispatch that checks the callee of the invocation in the body of a
/// `phys.loop` of kind closure that stands in the top region.
fn is_callee_of_top_closure_loop(ir: &Ir, user: OpId) -> bool {
	let Some(OpKind::Expr(ExprOp::Dispatch(target))) = ir.op(user).map(|data| &data.kind) else {
		return false;
	};
	if !matches!(**target, DispatchTarget::Invoke) {
		return false;
	}
	let Some(owner) = ir.parent_op(ir.enclosing_region(user)) else {
		return false;
	};
	let closure_loop = matches!(
		ir.op(owner).map(|data| &data.kind),
		Some(OpKind::Phys(PhysOp::Loop {
			kind: LoopKind::Closure,
			..
		}))
	);
	// The loop stands in the top region itself, or alone in the pipeline the physical phase
	// wraps it in there.
	let at_top = |op: OpId| ir.enclosing_region(op) == ir.top();
	closure_loop
		&& (at_top(owner)
			|| ir
				.parent_op(ir.enclosing_region(owner))
				.is_some_and(|pipeline| pipe::is_value_pipeline(ir, pipeline) && at_top(pipeline)))
}

/// The pipeline that emits the value `value` of the `phys.eval` `eval` once, over no source, as the
/// lowering of a bare expression statement does: load, emit, end the row. Answers what the lowered
/// value needs of the program too, and the instructions each op emitted for itself.
fn scalar_pipeline(
	cx: &mut LowerCx<'_>,
	em: &mut Emitter,
	ir: &Ir,
	eval: OpId,
	value: ValueId,
) -> Result<(Pipeline, Needs, Vec<Claim>), IrError> {
	let mut lowerer = Lowerer::in_context(em, ir, cx);
	let src: Operand = lowerer.value(value)?;
	lowerer.emit_scalar_row(eval, src)?;
	let needs = lowerer.needs();
	let claims = lowerer.take_claims();
	Ok((em.finish_pipeline(Source::Once)?, needs, claims))
}

/// Checks the claim of every op in `claims` against the instructions it emitted for itself.
///
/// `code` is the code of the pipeline the claims were recorded in, which `finish_pipeline` only
/// rewrites the labels of, so an index in a claim is the index of the same instruction in `code`.
pub(super) fn check_claims(
	ir: &Ir,
	claims: &[Claim],
	code: &[Instr],
	program: &Program,
	registry: &FunctionRegistry,
) -> Result<(), IrError> {
	for (op, at) in claims {
		let kind = &ir.op(*op).ok_or(IrError::Stale("ir_lower::dispatch: claim op"))?.kind;
		let instr = code
			.get(*at as usize)
			.ok_or(IrError::Stale("ir_lower::dispatch: claim instruction"))?;
		check_native_claim(kind, std::slice::from_ref(instr), program, registry)?;
	}
	Ok(())
}

/// Checks that what an operation claims of its lowering holds of the instructions emitted for it.
///
/// One-directional: when the operation answers `Native { leaf_class }` on the default target, the
/// class of every instruction in `emitted` must lie within that class. An operation that answers
/// `UnitCall` is never checked, because the answers for it are conservative: a unit call may
/// lower to instructions of any class. A mismatch is an internal error, in every build.
///
/// `registry` resolves the class of the calls in `emitted`, and `program` holds their specs.
pub(super) fn check_native_claim(
	op: &OpKind,
	emitted: &[Instr],
	program: &Program,
	registry: &FunctionRegistry,
) -> Result<(), IrError> {
	let claimed = match op.lowers_natively(IsaCaps::default(), registry) {
		NativeLowering::UnitCall => return Ok(()),
		NativeLowering::Native {
			leaf_class,
		} => leaf_class,
	};
	for instr in emitted {
		let (class, _) = leaf::instr(instr, program, registry);
		if !within(class, claimed) {
			return Err(IrError::NotYet {
				what: "ir_lower::dispatch: an instruction outside the class its op claims",
			});
		}
	}
	Ok(())
}

/// Whether an instruction of class `class` lies within the claim `claimed`.
///
/// - `Leaf` admits `Leaf` alone.
/// - `Kernel(s)` admits `Leaf` and `Kernel(t)` for every `t` whose seams are all in `s`.
/// - `NonLeaf(bar)` admits `Leaf`, any `Kernel` and `NonLeaf(bar)`.
/// - `ByOperand` admits every class: the class is that of the callee or instruction serviced.
///
/// A `ByOperand` instruction is within no claim but `ByOperand`.
fn within(class: Class, claimed: Class) -> bool {
	match claimed {
		Class::ByOperand => true,
		Class::Leaf => class == Class::Leaf,
		Class::Kernel(allowed) => match class {
			Class::Leaf => true,
			Class::Kernel(seams) => seams.iter().all(|seam| allowed.contains(seam)),
			Class::NonLeaf(_) | Class::ByOperand => false,
		},
		Class::NonLeaf(bar) => match class {
			Class::Leaf | Class::Kernel(_) => true,
			Class::NonLeaf(held) => held == bar,
			Class::ByOperand => false,
		},
	}
}

#[cfg(test)]
mod tests {
	use smallvec::SmallVec;

	use super::*;
	use crate::exec::AccessMode;
	use crate::exec::ir::analysis::cx::builtin_registry;
	use crate::exec::ir::expr::ops::{
		ClosureAttrs, ClosureLiteral, ExprOp, NowWhich, ShortCircuitOp,
	};
	use crate::exec::ir::iface::region_kind::{RegionKind, StatementCommit};
	use crate::exec::ir::iface::types::TypeKnowledge;
	use crate::exec::ir::logical::ops::{ClosureLoopAttrs, LetAttrs, LetBinding, StepAttrs};
	use crate::exec::ir::phys::ops::{Batch, PrefetchKeys};
	use crate::exec::vm::ir_instr::{BatchRead, BufferScope, IrInstr, LookAhead};
	use crate::exec::vm::ir_lower::tests::Builder;
	use crate::exec::vm::leaf::{Bar, Seam};
	use crate::exec::vm::program::{ArithOp, StoreInstr};
	use crate::expr::{BinaryOperator, Function};
	use crate::val::Value;

	/// The tags of `tags` whose coverage is `coverage`, in tag order.
	fn tags_with<T: Copy>(
		tags: &[T],
		coverage_of: impl Fn(T) -> Coverage,
		coverage: Coverage,
	) -> Vec<T> {
		tags.iter().copied().filter(|tag| coverage_of(*tag) == coverage).collect()
	}

	#[test]
	fn every_phys_tag_has_coverage() {
		let native = tags_with(PhysTag::ALL, phys_coverage, Coverage::Native);
		for tag in [
			PhysTag::Eval,
			PhysTag::Info,
			PhysTag::Access,
			PhysTag::Show,
			PhysTag::Block,
			PhysTag::Let,
			PhysTag::Sleep,
			PhysTag::If,
			PhysTag::Loop,
			PhysTag::Yield,
			PhysTag::Return,
			PhysTag::Break,
			PhysTag::Next,
			PhysTag::Kill,
			PhysTag::Live,
			PhysTag::DdlGuard,
			PhysTag::Define,
			PhysTag::Remove,
			PhysTag::Alter,
			PhysTag::Rebuild,
			PhysTag::Use,
			PhysTag::Option,
		] {
			assert!(native.contains(&tag), "{tag:?}");
		}
		assert_eq!(
			tags_with(PhysTag::ALL, phys_coverage, Coverage::ViaLoop),
			vec![PhysTag::Prefetch]
		);
		assert_eq!(
			tags_with(PhysTag::ALL, phys_coverage, Coverage::ViaRequest),
			vec![PhysTag::Request, PhysTag::Txn, PhysTag::Stmt]
		);
		let refused = tags_with(PhysTag::ALL, phys_coverage, Coverage::NotYet);
		let via_write = tags_with(PhysTag::ALL, phys_coverage, Coverage::ViaWrite);
		assert_eq!(
			via_write,
			vec![PhysTag::WriteStage, PhysTag::Assign, PhysTag::Unset, PhysTag::Content]
		);
		let via_unit = tags_with(PhysTag::ALL, phys_coverage, Coverage::ViaUnit);
		assert_eq!(via_unit, vec![PhysTag::Subscriber, PhysTag::Events]);
		assert_eq!(
			refused.len() + native.len() + via_write.len() + via_unit.len() + 1 + 3,
			PhysTag::ALL.len()
		);
		for tag in PhysTag::ALL {
			let what = phys_refusal(*tag);
			assert_eq!(what.strip_prefix("ir_lower::lower: "), Some(tag.mnemonic()));
		}
	}

	#[test]
	fn every_expr_tag_has_coverage() {
		assert_eq!(
			tags_with(ExprTag::ALL, expr_coverage, Coverage::Native),
			vec![
				ExprTag::Literal,
				ExprTag::Array,
				ExprTag::Set,
				ExprTag::Object,
				ExprTag::RecordId,
				ExprTag::FreeParam,
				ExprTag::SessionParam,
				ExprTag::CatalogParam,
				ExprTag::Table,
				ExprTag::Mock,
				ExprTag::Constant,
				ExprTag::Field,
				ExprTag::ComputedRead,
				ExprTag::Doc,
				ExprTag::Access,
				ExprTag::Deref,
				ExprTag::Index,
				ExprTag::First,
				ExprTag::Last,
				ExprTag::Each,
				ExprTag::Flatten,
				ExprTag::Dispatch,
				ExprTag::Method,
				ExprTag::Destructure,
				ExprTag::Optional,
				ExprTag::RepeatRecurse,
				ExprTag::SetField,
				ExprTag::Prefix,
				ExprTag::Convert,
				ExprTag::Postfix,
				ExprTag::Invoke,
				ExprTag::Binary,
				ExprTag::ShortCircuit,
				ExprTag::Call,
				ExprTag::ClosureCreate,
				ExprTag::If,
				ExprTag::Throw,
				ExprTag::Subquery,
				ExprTag::Yield,
				ExprTag::Truthy,
				ExprTag::Agg,
				ExprTag::Now,
				ExprTag::Raise,
				ExprTag::DepthCharge,
			]
		);
		assert!(tags_with(ExprTag::ALL, expr_coverage, Coverage::ViaLoop).is_empty());
		assert_eq!(
			tags_with(ExprTag::ALL, expr_coverage, Coverage::ViaWrite),
			vec![ExprTag::TupleRow]
		);
		for tag in ExprTag::ALL {
			let what = expr_refusal(*tag);
			assert_eq!(what.strip_prefix("ir_lower::lower: "), Some(tag.mnemonic()));
		}
	}

	/// A container whose top region holds `1 + 2` and the `phys.eval` of it, which lies before
	/// the physical boundary.
	fn one_plus_two() -> Builder {
		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		let two = b.literal(Value::from(2_i64));
		let sum = b.binary(BinaryOperator::Add, one, two);
		b.add_phys(PhysOp::Eval, &[sum]);
		b
	}

	/// What lowering `b` is refused for.
	fn refused(b: &Builder) -> &'static str {
		match lower(&b.ir, &FactCache::new()) {
			Err(IrError::NotYet {
				what,
			}) => what,
			Err(error) => panic!("an unexpected error: {error}"),
			Ok(_) => panic!("the container lowers"),
		}
	}

	#[test]
	fn eval_of_a_subset_value_lowers_to_one_scalar_pipeline() {
		let mut b = one_plus_two();
		b.physical();
		let lowered = match lower(&b.ir, &FactCache::new()) {
			Ok(lowered) => lowered,
			Err(error) => panic!("the container lowers: {error}"),
		};
		let program = &lowered.program;
		assert_eq!(program.pipelines.len(), 1);
		assert!(program.scalar);
		assert!(program.groups.is_empty());
		let pipeline = &program.pipelines[0];
		assert!(matches!(pipeline.source, Source::Once));
		assert_eq!(pipeline.parallel, crate::exec::planner::placement::Parallel::Serial);
		assert_eq!(
			pipeline.code,
			vec![
				Instr::Arith {
					op: ArithOp::Add,
					left: Operand::Const(0),
					right: Operand::Const(1),
					dst: 1,
				},
				Instr::Load {
					dst: 2,
					src: Operand::Reg(1),
				},
				Instr::Emit {
					src: 2,
				},
				Instr::EndRow,
			]
		);
		assert_eq!(pipeline.registers, 3);
		// The eval pipeline stands for no `phys.pipeline` op, so it has no provenance entry.
		assert!(lowered.provenance.pipelines.is_empty());
	}

	/// A container whose top region holds `1 + 2` and a `phys.let` of it, bound as `binding`.
	fn let_of_sum(binding: LetBinding) -> Builder {
		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		let two = b.literal(Value::from(2_i64));
		let sum = b.binary(BinaryOperator::Add, one, two);
		b.add_phys(
			PhysOp::Let(Box::new(LetAttrs {
				name: surrealdb_strand::Strand::new("a"),
				binding,
				ty: None,
			})),
			&[sum],
		);
		b.physical();
		b
	}

	#[test]
	fn a_session_let_checks_its_name_evaluates_its_value_and_records_it() {
		let b = let_of_sum(LetBinding::Session);
		let lowered = match lower(&b.ir, &FactCache::new()) {
			Ok(lowered) => lowered,
			Err(error) => panic!("the container lowers: {error}"),
		};
		let program = lowered.program;
		assert!(program.scalar);
		assert_eq!(program.pipelines.len(), 1);
		let code = &program.pipelines[0].code;
		let checks = code
			.iter()
			.position(|instr| matches!(instr, Instr::Store(StoreInstr::SessionLetChecks { .. })))
			.expect("the name is checked");
		let recorded = code
			.iter()
			.position(|instr| matches!(instr, Instr::Store(StoreInstr::SessionLet { .. })))
			.expect("the value is recorded");
		// The refusal of a protected name precedes everything the value evaluates.
		assert_eq!(checks, 0, "{code:?}");
		assert!(checks < recorded, "{code:?}");
		assert_eq!(program.access_mode, AccessMode::ReadOnly);
	}

	#[test]
	fn a_local_let_is_a_statement_program() {
		let b = let_of_sum(LetBinding::Local);
		let lowered = match lower(&b.ir, &FactCache::new()) {
			Ok(lowered) => lowered,
			Err(error) => panic!("the container lowers: {error}"),
		};
		assert!(lowered.program.scalar);
		assert!(lowered.program.pipelines[0].code.iter().any(|instr| matches!(
			instr,
			Instr::Block(crate::exec::vm::program::BlockInstr::Bind { .. })
		)));
	}

	#[test]
	fn lower_refuses_what_it_does_not_lower_by_name() {
		// Not past the physical boundary.
		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		b.add_phys(PhysOp::Eval, &[one]);
		assert_eq!(refused(&b), "ir_lower::lower: not physical");

		// A tag the direct lowering has no arm for, named by its mnemonic.
		let mut b = Builder::new();
		b.add_phys(PhysOp::Filter, &[]);
		b.physical();
		assert_eq!(refused(&b), "ir_lower::lower: phys.filter");

		// A tag of a request is lowered by its steps, so lowering it as a program is an invariant
		// error.
		let mut b = Builder::new();
		b.add_phys(
			PhysOp::Request {
				fragment: None,
				nested: false,
			},
			&[],
		);
		b.physical();
		assert!(matches!(lower(&b.ir, &FactCache::new()), Err(IrError::Invariant { .. })));

		// A `ViaLoop` tag outside a lowered loop.
		let mut b = Builder::new();
		b.add_phys(
			PhysOp::Prefetch(Box::new(PrefetchKeys {
				points: SmallVec::new(),
				ranges: SmallVec::new(),
			})),
			&[],
		);
		b.physical();
		assert_eq!(refused(&b), "ir_lower::lower: phys.prefetch");

		// A row of an `INSERT` outside the write that holds it.
		let mut b = Builder::new();
		let row = b.add(
			ExprOp::TupleRow {
				places: Box::new([]),
			},
			&[],
		);
		b.add_phys(PhysOp::Eval, &[row]);
		b.physical();
		assert!(matches!(lower(&b.ir, &FactCache::new()), Err(IrError::Invariant { .. })));

		// An aggregate call outside the grouping that answers it, which the binder never builds.
		let mut b = Builder::new();
		let aggregate = b.add(
			ExprOp::Agg {
				kind: "count",
				combine: crate::exec::logical::pipelines::AggregateCombine::Exact,
			},
			&[],
		);
		b.add_phys(PhysOp::Eval, &[aggregate]);
		b.physical();
		assert!(matches!(lower(&b.ir, &FactCache::new()), Err(IrError::Invariant { .. })));

		// A depth charge out of the top region defines no value, so no read would lower it.
		let mut b = Builder::new();
		b.terminate(
			ExprOp::DepthCharge {
				levels: 1,
			},
			&[],
		);
		b.physical();
		assert_eq!(refused(&b), "ir_lower::lower: a terminator out of the top region");

		// A container with nothing to run.
		let mut b = Builder::new();
		b.physical();
		assert_eq!(refused(&b), "ir_lower::lower: a container with no phys.eval");

		// A second statement needs groups, which the direct lowering does not build.
		let mut b = one_plus_two();
		let two = b.literal(Value::from(2_i64));
		b.add_phys(PhysOp::Eval, &[two]);
		b.physical();
		assert_eq!(refused(&b), "ir_lower::lower: more than one phys.eval");

		// A value nothing reads runs where it stands, as a statement of a statement program.
		let mut b = one_plus_two();
		b.call("math::abs", &[]);
		b.physical();
		match lower(&b.ir, &FactCache::new()) {
			Ok(lowered) => assert!(lowered.program.scalar),
			Err(error) => panic!("the container lowers: {error}"),
		}

		// A value only a lazy region reads would be skipped when the region is, so it is not run
		// on that path alone.
		let mut b = Builder::new();
		let hidden = b.call("rand::float", &[]);
		let decided = b.literal(Value::Bool(false));
		let lazy = b.short_circuit(ShortCircuitOp::And, decided, |_| hidden);
		b.add_phys(PhysOp::Eval, &[lazy]);
		b.physical();
		assert_eq!(refused(&b), "ir_lower::lower: an expression only a nested region reads");
	}

	#[test]
	fn coverage_report_walks_every_op() {
		let mut b = Builder::new();
		let request = b.add_phys(
			PhysOp::Request {
				fragment: None,
				nested: false,
			},
			&[],
		);
		b.open(request, RegionKind::Request, &[]);
		let stmt = b.add_phys(
			PhysOp::Stmt {
				commit: StatementCommit::Auto,
				control: None,
			},
			&[],
		);
		b.open(stmt, RegionKind::Statement(StatementCommit::Auto), &[TypeKnowledge::Unknown]);
		let one = b.literal(Value::from(1_i64));
		b.add_phys(PhysOp::Eval, &[one]);
		b.close();
		b.close();
		b.physical();
		let report = coverage_report(&b.ir);
		// Both the root and the statement are reported: the walk does not stop at the first.
		assert_eq!(
			report
				.iter()
				.map(|(op, mnemonic, coverage)| (*op, *mnemonic, *coverage))
				.collect::<Vec<_>>(),
			vec![
				(request, "phys.request", Coverage::ViaRequest),
				(stmt, "phys.stmt", Coverage::ViaRequest),
			]
		);
		// A container the direct lowering covers reports nothing.
		let mut covered = one_plus_two();
		covered.physical();
		assert!(coverage_report(&covered.ir).is_empty());
	}

	#[test]
	fn native_claim_checked_one_way() {
		let registry = builtin_registry();
		let program = match Emitter::new().finish_program(Vec::new(), AccessMode::ReadOnly) {
			Ok(program) => program,
			Err(error) => panic!("an empty program: {error}"),
		};
		let read = Instr::Ir(IrInstr::batch_read(BatchRead {
			scope: BufferScope::Loop {
				site: 0,
			},
			ahead: LookAhead::None,
			keys: Box::new([]),
			max_positions: 1,
			max_bytes: 1,
		}));
		// A literal claims `Leaf`: an instruction that reads storage is outside it.
		let leaf_op = OpKind::Expr(ExprOp::Literal(Box::new(Value::from(1_i64))));
		assert!(matches!(
			check_native_claim(&leaf_op, std::slice::from_ref(&read), &program, registry),
			Err(IrError::NotYet { .. })
		));
		assert!(check_native_claim(&leaf_op, &[Instr::EndRow], &program, registry).is_ok());
		// A call of a stored function is a call of a unit, which is never checked.
		let unit_op = OpKind::Expr(ExprOp::call(Function::Custom("f".to_owned())));
		assert!(check_native_claim(&unit_op, &[read], &program, registry).is_ok());
	}

	#[test]
	fn lower_checks_the_claim_of_each_op_over_its_own_instructions() {
		// `expr.now` claims what the clock builtin it lowers to is, so the permission check and
		// the call it opens with are within its claim.
		let mut b = Builder::new();
		let instant = b.literal(Value::None);
		let now = b.add(
			ExprOp::Now {
				which: NowWhich::Datetime,
			},
			&[instant],
		);
		b.add_phys(PhysOp::Eval, &[now]);
		b.physical();
		let lowered = lower(&b.ir, &FactCache::new()).expect("the clock reading lowers");
		assert!(lowered.program.pipelines[0].code.iter().any(|i| matches!(i, Instr::Call { .. })));

		// An op whose operands lower to code of their own is judged by its own instructions only:
		// the sum of two literals emits one `Arith`, which is a leaf instruction.
		let mut b = one_plus_two();
		b.physical();
		assert!(lower(&b.ir, &FactCache::new()).is_ok());
	}

	#[test]
	fn the_direct_entry_lowers_under_the_limits_of_its_context() {
		let mut b = Builder::new();
		let mock = b.add(ExprOp::Mock(Box::new(crate::expr::Mock::Count("t".into(), 3))), &[]);
		b.add_phys(PhysOp::Eval, &[mock]);
		b.physical();
		let mut cx = LowerCx::detached().with_limits(super::super::expr::LowerLimits {
			generation_allocation_limit: 7,
			..Default::default()
		});
		let lowered = lower_with(&mut cx, &b.ir, &FactCache::new()).expect("the mock lowers");
		assert!(lowered.program.pipelines[0].code.iter().any(|i| matches!(
			i,
			Instr::Mock {
				limit: 7,
				..
			}
		)));
	}

	/// A closure creation that nothing at the top reads, a loop of kind `kind` over a parameter
	/// whose body invokes it, and the `phys.eval` of the loop.
	fn closure_read_by_a_loop(kind: LoopKind, step: Option<StepAttrs>) -> Builder {
		let mut b = Builder::new();
		let xs = b.add(
			ExprOp::FreeParam {
				name: "xs".into(),
			},
			&[],
		);
		let owner = b.add_with_regions(
			ExprOp::ClosureCreate(Box::new(ClosureAttrs {
				params: Box::new([]),
				returns: None,
				body_sql: "NONE".into(),
				captures: Box::new([]),
				reads_row: false,
				reads_root: false,
				literal: ClosureLiteral::none_written_as("NONE"),
			})),
			&[],
		);
		b.region_with_args(owner, RegionKind::ClosureBody, &[TypeKnowledge::Unknown], |b, _| {
			b.literal(Value::None)
		});
		let made = b.result_of(owner);
		let looped = b.add_phys_value(
			PhysOp::Loop {
				kind,
				first: false,
				batch: Batch {
					max_iterations: 1,
					max_bytes: 1,
				},
				unroll: 1,
				value: false,
				step: step.map(Box::new),
			},
			&[xs],
		);
		b.open_with(
			looped,
			RegionKind::Loop,
			&[TypeKnowledge::Unknown, TypeKnowledge::Unknown],
			|b, args| {
				let target = b.add(ExprOp::Dispatch(Box::new(DispatchTarget::Invoke)), &[made]);
				let mut invoked = vec![target];
				invoked.extend_from_slice(args);
				let answer = b.add(ExprOp::Invoke, &invoked);
				let truth = b.add(ExprOp::Truthy, &[answer]);
				b.yield_phys(truth);
			},
		);
		let result = b.result_of(looped);
		b.add_phys(PhysOp::Eval, &[result]);
		b.physical();
		b
	}

	#[test]
	fn the_closure_of_a_closure_loop_at_the_top_is_read_by_the_loop() {
		let b = closure_read_by_a_loop(
			LoopKind::Closure,
			Some(StepAttrs::Closure(ClosureLoopAttrs {
				function: surrealdb_strand::Strand::new("array::any"),
				method: false,
			})),
		);
		assert!(lower(&b.ir, &FactCache::new()).is_ok());
		// A loop that does not call the builtin leaves the closure to the body that names it,
		// which is skipped whenever the body is.
		let b = closure_read_by_a_loop(LoopKind::ElementFilter, None);
		assert_eq!(refused(&b), "ir_lower::lower: an expression only a nested region reads");
	}

	#[test]
	fn a_claim_admits_the_classes_within_it() {
		let storage = Class::Kernel(&[Seam::Storage]);
		let both = Class::Kernel(&[Seam::Storage, Seam::Catalog]);
		assert!(within(Class::Leaf, Class::Leaf));
		assert!(!within(storage, Class::Leaf));
		assert!(!within(Class::ByOperand, Class::Leaf));
		assert!(within(Class::Leaf, storage));
		assert!(within(storage, both));
		assert!(!within(both, storage));
		assert!(!within(Class::NonLeaf(Bar::Write), both));
		assert!(within(Class::Leaf, Class::NonLeaf(Bar::Write)));
		assert!(within(both, Class::NonLeaf(Bar::Write)));
		assert!(within(Class::NonLeaf(Bar::Write), Class::NonLeaf(Bar::Write)));
		assert!(!within(Class::NonLeaf(Bar::Sleep), Class::NonLeaf(Bar::Write)));
		assert!(within(Class::NonLeaf(Bar::Write), Class::ByOperand));
		assert!(within(Class::ByOperand, Class::ByOperand));
	}

	/// A read program whose code calls something that runs statements runs in a transaction that
	/// may write, whichever finisher built it; one that writes is never narrowed.
	#[test]
	fn the_access_mode_of_the_code_widens_the_program() {
		let mut needs = Needs::none();
		needs.access_mode = AccessMode::ReadWrite;
		let mut program =
			Emitter::new().finish_program(Vec::new(), AccessMode::ReadOnly).expect("a program");
		apply_needs(&mut program, &needs);
		assert_eq!(program.access_mode, AccessMode::ReadWrite);
		let mut program =
			Emitter::new().finish_program(Vec::new(), AccessMode::ReadWrite).expect("a program");
		apply_needs(&mut program, &Needs::none());
		assert_eq!(program.access_mode, AccessMode::ReadWrite);
	}
}
