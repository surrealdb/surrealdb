//! The physical facts the planner decides from: those of a pipeline, the input of the placement
//! decision, and those of a table read, the input of the e-graph's access choice.
//!
//! [`table_facts_of`] answers the facts of one table read from the catalog's answers about the
//! table; [`table_facts`] answers them for the row sources of a statement.
//!
//! [`facts_of`] reads the ops a `phys.pipeline` holds, the op that consumes its rows and the
//! statements around it, and answers the [`PhysicalFacts`] the planner's policy decides from. Every
//! fact is derived from the physical ops and the logical facts of the analysis lane; none is read
//! from emitted code and none from statistics.
//!
//! A fact the physical ops cannot state exactly is answered by the more restrictive of the values
//! the policy can read, so that the policy admits nothing it would refuse for the planner's own
//! plan: an index scan may repeat records, a statement nested in an expression is evaluated once or
//! through a probe, and a read that locks is a write.

#![deny(clippy::wildcard_enum_match_arm)]

use smallvec::SmallVec;
use surrealdb_strand::{Strand, TableName};

use super::access::{ReadVersion, RestrictedSelect, TableAnswers, admissible_access_candidates};
use super::ops::{MatchStepAttrs, PhysOp, ProjectionShape};
use super::pipeline::{chain_of, enclosing_region_of, streams};
use super::{chain, nested};
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::analysis::cx::structural_cx;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::egraph::decl::PhysTableFacts;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ExprOp, NestedStrategy};
use crate::exec::ir::iface::catalog::{CatalogAnswer, PhysCx, SourceKey};
use crate::exec::ir::iface::effects::FieldSet;
use crate::exec::ir::iface::ids::{OpId, RegionId, SnapshotId, ValueId};
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::logical::ops::LoopKind;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::logical::pipelines::SourceSplit;
use crate::exec::planner::placement::{NestedUse, PhysicalFacts, SinkKind, SourceKind};
use crate::exec::vm::payload::ValuesRead;
use crate::expr::Mock;
use crate::expr::with::With;

/// How deep the input of a pipeline is followed when its rows are classified. A pipeline nested
/// deeper answers the classification that refuses most.
const MAX_INPUT_DEPTH: usize = 32;

/// The sink of a pipeline whose rows are read by an op that is none of the sinks the planner cuts
/// a pipeline at: a union member, the build input of a probe or a join, the fallback of a bitmap.
/// Such a pipeline is not one the logical plan cuts, and its rows end in state a part cannot own,
/// which [`SinkKind::JoinPair`] states.
const UNPLACED_SINK: SinkKind = SinkKind::JoinPair;

/// How the statements written in value position are read when a pipeline's facts are derived.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NestedReading {
	/// The strategy is not read: every statement that only reads is evaluated once or through a
	/// probe, the reading that refuses most. The answer for a container whose strategies are not
	/// decided yet, where `Apply` stands for "not chosen".
	Hedged,
	/// The strategy each statement holds is read. The physical rewrites decide the container's
	/// strategies, so `Apply` states a per-row evaluation.
	Strategy,
}

/// Derives the facts of the pipeline `pipeline_op` under [`NestedReading::Hedged`].
///
/// See [`facts_reading`].
pub(crate) fn facts_of(
	ir: &Ir,
	facts: &mut FactCache,
	registry: &FunctionRegistry,
	pipeline_op: OpId,
) -> Result<PhysicalFacts, IrError> {
	facts_reading(ir, facts, registry, pipeline_op, NestedReading::Hedged)
}

/// Derives the facts of the pipeline `pipeline_op` from the physical ops it holds, the op that
/// consumes its rows and the facts of the logical ops it stands for.
///
/// `writes` is true when the pipeline's sink writes, or when a statement that encloses the
/// pipeline writes ([`writes_within`] of each enclosing `Statement` region). Nested plans inherit
/// it structurally, since a nested statement stands inside its host's region.
/// `estimate` is always `None`: the plan carries no estimate of its rows.
///
/// `reading` states how the nested statements are read ([`nested_use`]).
///
/// An op that is not a pipeline answers an error.
pub(crate) fn facts_reading(
	ir: &Ir,
	facts: &mut FactCache,
	registry: &FunctionRegistry,
	pipeline_op: OpId,
	reading: NestedReading,
) -> Result<PhysicalFacts, IrError> {
	if !matches!(phys_of(ir, pipeline_op), Some(PhysOp::Pipeline(..))) {
		return Err(IrError::Stale("not a pipeline"));
	}
	let entries = pipeline_entries(ir, pipeline_op);
	let entries = through_select_loop(ir, &entries).unwrap_or(entries);
	let (source, steps) = source_and_steps(ir, &entries);
	// The pipelines that continue this one are its steps too: its rows end in the sink of the last.
	let onward = onward_of(ir, pipeline_op);
	let mut steps = steps.to_vec();
	steps.extend(onward.steps);
	let step_kinds: Vec<&PhysOp> = steps.iter().map(|(_, kind)| *kind).collect();
	let source_kind = if entries.first().is_some_and(|head| writes_range_mock(ir, *head)) {
		SourceKind::Mock
	} else {
		source.map_or(SourceKind::Other, |(op, kind)| source_kind_of(ir, op, kind, 0))
	};
	let source_rows = source.map_or(Rows::UNKNOWN, |(op, _)| rows_of_source(ir, op, 0));
	let cut = cut_of(ir, onward.last);
	let chain = chain_facts(source.map(|(_, kind)| kind), source_rows, &step_kinds);

	let mut scanned: Vec<OpId> = steps.iter().map(|(op, _)| *op).collect();
	if matches!(
		cut.sink,
		SinkKind::Sort { .. } | SinkKind::Aggregate | SinkKind::Window | SinkKind::Write
	) {
		scanned.extend(cut.consumer);
	}
	let nesting = scan_expressions(ir, &scanned);
	let nested = nested_use(ir, facts, registry, &nesting, reading);
	let writes = cut.sink == SinkKind::Write || enclosing_writes(ir, facts, registry, pipeline_op);
	Ok(assemble(source_kind, cut.sink, writes, &chain, nested, nesting.host_blocks))
}

/// The entries of a pipeline whose first op is a select loop, read as the chain they stand for: the
/// op that produces the loop's source, the ops of the loop's body, and the entries after the loop.
///
/// The per-row ops of a bound `SELECT` stand in the body of the loop, and the source the loop
/// reads stands outside the pipeline ([`chain`]). The facts of the pipeline are those of the chain
/// it runs: the source's access path, the enforcement, the projection and the statements the body
/// holds. Answers `None` for any other pipeline, and for a loop whose source is not the result of
/// a physical op that is a source or a breaker, which keeps the loop as the source it stands for.
fn through_select_loop<'a>(
	ir: &'a Ir,
	entries: &[(OpId, &'a PhysOp)],
) -> Option<Vec<(OpId, &'a PhysOp)>> {
	let (head, rest) = entries.split_first()?;
	let (source, body) = chain::select_loop(ir, head.0)?;
	let read = producer(ir, source).and_then(|op| phys_of(ir, op).map(|kind| (op, kind)))?;
	if streams(read.1) {
		return None;
	}
	let mut through = vec![read];
	through.extend(
		ops_in(ir, body)
			.filter_map(|op| phys_of(ir, op).map(|kind| (op, kind)))
			.filter(|(_, kind)| !kind.is_terminator()),
	);
	through.extend(rest.iter().copied());
	Some(through)
}

/// The ops a pipeline holds, each with its physical kind.
type Entries<'a> = [(OpId, &'a PhysOp)];

/// The source of a pipeline whose entries are `entries`, and the steps its rows pass through.
///
/// A first op that streams reads its input from outside the pipeline: no source is held. The loop
/// of a write reads the access path of the pipeline, and stands in its steps.
fn source_and_steps<'a>(
	ir: &'a Ir,
	entries: &'a Entries<'a>,
) -> (Option<(OpId, &'a PhysOp)>, &'a Entries<'a>) {
	match entries.split_first() {
		Some((first, _)) if write_source(ir, *first).is_some() => {
			(write_source(ir, *first), entries)
		}
		Some((first, rest)) if !streams(first.1) => (Some(*first), rest),
		_ => (None, entries),
	}
}

/// The op that is the source of the pipeline `pipeline_op`, when it holds one.
pub(super) fn source_of(ir: &Ir, pipeline_op: OpId) -> Option<OpId> {
	let entries = pipeline_entries(ir, pipeline_op);
	let entries = through_select_loop(ir, &entries).unwrap_or(entries);
	source_and_steps(ir, &entries).0.map(|(op, _)| op)
}

/// The source op the write loop `head` reads, where `head` is a write loop and the op that
/// produces its rows is a source and not a streaming op.
fn write_source<'a>(ir: &'a Ir, head: (OpId, &'a PhysOp)) -> Option<(OpId, &'a PhysOp)> {
	let PhysOp::Loop {
		kind: LoopKind::Write,
		..
	} = head.1
	else {
		return None;
	};
	let source = producer(ir, *ir.op(head.0)?.operands.first()?)?;
	let kind = phys_of(ir, source)?;
	(!streams(kind)).then_some((source, kind))
}

/// Whether `head` is a write loop whose subjects are one mock that names the ids of a range.
fn writes_range_mock(ir: &Ir, head: (OpId, &PhysOp)) -> bool {
	matches!(
		head.1,
		PhysOp::Loop {
			kind: LoopKind::Write,
			..
		}
	) && subjects_are_range_mock(ir, head.0)
}

/// Whether the subjects of the write loop `loop_op` are one mock that names the ids of a range:
/// the coordinator generates the ids in order, and no stored record is read for them.
pub(super) fn subjects_are_range_mock(ir: &Ir, loop_op: OpId) -> bool {
	let Some(array) = ir
		.op(loop_op)
		.and_then(|data| data.operands.first().copied())
		.and_then(|subjects| producer(ir, subjects))
		.and_then(|op| ir.op(op))
	else {
		return false;
	};
	let [member] = array.operands.as_slice() else {
		return false;
	};
	matches!(array.kind, OpKind::Expr(ExprOp::Array))
		&& producer(ir, *member).and_then(|op| ir.op(op)).is_some_and(|data| {
			matches!(&data.kind, OpKind::Expr(ExprOp::Mock(mock)) if matches!(**mock, Mock::Range(..)))
		})
}

/// What follows a pipeline: the ops its rows pass through before they reach a sink, the op whose
/// result the sink reads, and the pipelines of the chain, the first included.
struct Onward<'a> {
	steps: Vec<(OpId, &'a PhysOp)>,
	last: OpId,
	pipelines: usize,
}

/// The ops that continue the pipeline `pipeline_op`, in data-flow order.
///
/// A streaming op that is the only reader of the rows, and a select loop that is the only reader of
/// them and heads a pipeline of its own, continue the pipeline: the ops of that loop's body and of
/// the pipeline after it are steps of the one chain the rows run through, and the lowering emits
/// the pipelines as one body. The chain ends at the first op that more than one op reads or that is
/// not one of these, and its result is the one a sink reads.
fn onward_of(ir: &Ir, pipeline_op: OpId) -> Onward<'_> {
	let mut steps: Vec<(OpId, &PhysOp)> = Vec::new();
	let mut last = pipeline_op;
	let mut pipelines = 1;
	while let Some(result) = ir.op(last).and_then(|data| data.results.first().copied()) {
		let [only] = ir.uses(result) else {
			break;
		};
		if only.operand != 0 {
			break;
		}
		let Some(OpKind::Phys(kind)) = ir.op(only.user).map(|data| &data.kind) else {
			break;
		};
		if streams(kind) {
			steps.push((only.user, kind));
			last = only.user;
			continue;
		}
		let Some(next) = ir.parent_op(ir.enclosing_region(only.user)) else {
			break;
		};
		if !matches!(phys_of(ir, next), Some(PhysOp::Pipeline(..))) {
			break;
		}
		let entries = pipeline_entries(ir, next);
		if entries.first().map(|(op, _)| *op) != Some(only.user) {
			break;
		}
		let Some(through) = through_select_loop(ir, &entries) else {
			break;
		};
		steps.extend(through.into_iter().skip(1));
		last = next;
		pipelines += 1;
	}
	Onward {
		steps,
		last,
		pipelines,
	}
}

/// The pipelines of the chain that starts at the pipeline `pipeline_op`, itself included: one when
/// no pipeline continues it.
pub(super) fn chain_pipelines(ir: &Ir, pipeline_op: OpId) -> usize {
	onward_of(ir, pipeline_op).pipelines
}

/// The facts of a pipeline from what [`facts_of`] reads of it.
fn assemble(
	source: SourceKind,
	sink: SinkKind,
	writes: bool,
	chain: &ChainFacts,
	nested: NestedUse,
	host_blocks: bool,
) -> PhysicalFacts {
	PhysicalFacts {
		source,
		enforce: chain.enforce,
		writes,
		limits: chain.limits,
		output: sink == SinkKind::Output,
		sink,
		binds_row: chain.binds_row,
		fetches_row: chain.fetches_row,
		distincts: chain.distincts,
		nested,
		host_blocks,
		paging_replayable: chain.paging_replayable,
		estimate: None,
	}
}

// ---------------------------------------------------------------------------------------------
// The source
// ---------------------------------------------------------------------------------------------

/// The access path the source op `source` reads.
///
/// Answers the kind of the op alone: a union's kind depends on its members, which this does not
/// see, so a [`PhysOp::Union`] answers [`SourceKind::Other`]; [`source_kind_of`] reads the members.
/// An index scan answers that it may repeat records, because the index's columns, which decide it,
/// are the catalog's and the op names only the index: the coordinator then lists each record once
/// in the order of its first entry. A brute-force nearest-neighbour search is no access path of its
/// own: its input's pipeline ends in it, and it is the source of a pipeline that cannot be cut. A
/// key-range scan answers [`SourceKind::Other`]: it is placed whole, as a serial read.
pub(crate) fn source_kind(source: &PhysOp) -> SourceKind {
	match source {
		PhysOp::TableScan(attrs) if attrs.lightweight => SourceKind::LightweightEdges,
		PhysOp::TableScan(..) => SourceKind::Scan,
		PhysOp::RecordScan(attrs) if attrs.lightweight && !attrs.for_update => {
			SourceKind::LightweightEdges
		}
		PhysOp::RecordScan(..) => SourceKind::Record,
		PhysOp::IndexScan(..) => SourceKind::Index {
			repeats: true,
		},
		PhysOp::IndexLookup(..) => SourceKind::Lookup,
		PhysOp::IndexUnion(..) => SourceKind::IndexUnion,
		PhysOp::Bitmap(..) => SourceKind::Bitmap,
		PhysOp::FullTextScan(..) => SourceKind::FullText,
		PhysOp::KnnScan(..) => SourceKind::Knn,
		PhysOp::KeyRangeScan(..)
		| PhysOp::KnnTopK(..)
		| PhysOp::Count(..)
		| PhysOp::Vacant {
			..
		}
		| PhysOp::Enforced {
			..
		}
		| PhysOp::Values {
			..
		}
		| PhysOp::Subquery
		| PhysOp::Probe {
			..
		}
		| PhysOp::Union
		| PhysOp::Join(..)
		| PhysOp::Distinct {
			..
		}
		| PhysOp::PermissionFilter(..)
		| PhysOp::Layer(..)
		| PhysOp::Filter
		| PhysOp::Fetch
		| PhysOp::Split {
			..
		}
		| PhysOp::Sort(..)
		| PhysOp::Window(..)
		| PhysOp::Aggregate(..)
		| PhysOp::Limit {
			..
		}
		| PhysOp::Project(..)
		| PhysOp::Eval
		| PhysOp::Request {
			..
		}
		| PhysOp::Txn {
			..
		}
		| PhysOp::Stmt {
			..
		}
		| PhysOp::Statement {
			..
		}
		| PhysOp::Match {
			..
		}
		| PhysOp::MatchStep(..)
		| PhysOp::DdlGuard {
			..
		}
		| PhysOp::Define(..)
		| PhysOp::Remove(..)
		| PhysOp::Alter(..)
		| PhysOp::Rebuild(..)
		| PhysOp::Info {
			..
		}
		| PhysOp::Use {
			..
		}
		| PhysOp::Option {
			..
		}
		| PhysOp::Access(..)
		| PhysOp::Kill
		| PhysOp::Live(..)
		| PhysOp::Subscriber(..)
		| PhysOp::Show(..)
		| PhysOp::Write(..)
		| PhysOp::WriteStage {
			..
		}
		| PhysOp::Events(..)
		| PhysOp::Timeout
		| PhysOp::VersionScope
		| PhysOp::Only
		| PhysOp::Explain {
			..
		}
		| PhysOp::Assign(..)
		| PhysOp::Unset {
			..
		}
		| PhysOp::Content {
			..
		}
		| PhysOp::Block
		| PhysOp::Let(..)
		| PhysOp::Sleep
		| PhysOp::If {
			..
		}
		| PhysOp::Loop {
			..
		}
		| PhysOp::Pipeline(..)
		| PhysOp::Choose(..)
		| PhysOp::Prefetch(..)
		| PhysOp::Materialize {
			..
		}
		| PhysOp::LoadUnit(..)
		| PhysOp::Yield
		| PhysOp::Return
		| PhysOp::Break
		| PhysOp::Next => SourceKind::Other,
	}
}

/// [`source_kind`] of the op `op` of kind `kind`, reading the members of a union.
///
/// A union of sources that each split is [`SourceKind::Union`]; one with a member that does not,
/// or that is not a physical op, is [`SourceKind::Other`]. A member is the op that produces the
/// union's operand, or the last op of the pipeline that does.
fn source_kind_of(ir: &Ir, op: OpId, kind: &PhysOp, depth: usize) -> SourceKind {
	if !matches!(kind, PhysOp::Union) {
		return source_kind(kind);
	}
	if depth >= MAX_INPUT_DEPTH {
		return SourceKind::Other;
	}
	let splits = members(ir, op).into_iter().all(|member| {
		member.and_then(|member| phys_of(ir, member).map(|kind| (member, kind))).is_some_and(
			|(member, kind)| {
				source_kind_of(ir, member, kind, depth + 1).split() != SourceSplit::Whole
			},
		)
	});
	if splits {
		SourceKind::Union
	} else {
		SourceKind::Other
	}
}

// ---------------------------------------------------------------------------------------------
// The sink
// ---------------------------------------------------------------------------------------------

/// How a pipeline's rows end, and the op that consumes them.
struct Cut {
	sink: SinkKind,
	consumer: Option<OpId>,
}

/// The cut a pipeline ends in, from the op that reads its result.
///
/// A result nothing reads, and one a statement-level op takes whole (a yield, a return, a `LET`,
/// a bare expression, a condition, a sleep), is the statement's output. A sort, a nearest-neighbour
/// search, a grouping, a write, a split or a binding step, and the two sides of a join, are the
/// sinks the planner cuts a pipeline at. A result read more than once, or by any other op, ends in
/// no sink of the planner's cuts: [`UNPLACED_SINK`].
fn cut_of(ir: &Ir, pipeline_op: OpId) -> Cut {
	let result = ir.op(pipeline_op).and_then(|data| data.results.first().copied());
	let uses = result.map_or(&[][..], |result| ir.uses(result));
	let [only] = uses else {
		return Cut {
			sink: if uses.is_empty() {
				SinkKind::Output
			} else {
				UNPLACED_SINK
			},
			consumer: None,
		};
	};
	let sink = match ir.op(only.user).map(|data| &data.kind) {
		Some(OpKind::Phys(kind)) => sink_of(kind, only.operand),
		Some(OpKind::Expr(ExprOp::Yield)) => Some(SinkKind::Output),
		Some(OpKind::Expr(..) | OpKind::Logical(..)) | None => None,
	};
	match sink {
		Some(sink) => Cut {
			sink,
			consumer: Some(only.user),
		},
		None => Cut {
			sink: UNPLACED_SINK,
			consumer: None,
		},
	}
}

/// The sink the physical op `consumer` makes of the pipeline that is its operand number
/// `operand`, or `None` when the op is none of the planner's cuts.
fn sink_of(consumer: &PhysOp, operand: u16) -> Option<SinkKind> {
	match consumer {
		PhysOp::Sort(attrs) => Some(SinkKind::Sort {
			bounded: attrs.keep.is_some(),
		}),
		PhysOp::KnnTopK(..) => Some(SinkKind::Knn),
		PhysOp::Aggregate(..) => Some(SinkKind::Aggregate),
		PhysOp::Window(..) => Some(SinkKind::Window),
		PhysOp::Write(..) => Some(SinkKind::Write),
		PhysOp::Split {
			..
		}
		| PhysOp::MatchStep(..) => Some(SinkKind::Split),
		PhysOp::Join(..) if operand == 0 => Some(SinkKind::JoinProbe),
		PhysOp::Join(..) => Some(SinkKind::JoinPair),
		PhysOp::Yield
		| PhysOp::Return
		| PhysOp::Let(..)
		| PhysOp::Eval
		| PhysOp::If {
			..
		}
		| PhysOp::VersionScope
		| PhysOp::Only
		| PhysOp::Explain {
			..
		}
		| PhysOp::Sleep => Some(SinkKind::Output),
		PhysOp::TableScan(..)
		| PhysOp::IndexScan(..)
		| PhysOp::IndexUnion(..)
		| PhysOp::Bitmap(..)
		| PhysOp::IndexLookup(..)
		| PhysOp::FullTextScan(..)
		| PhysOp::KeyRangeScan(..)
		| PhysOp::KnnScan(..)
		| PhysOp::Count(..)
		| PhysOp::RecordScan(..)
		| PhysOp::Vacant {
			..
		}
		| PhysOp::Enforced {
			..
		}
		| PhysOp::Values {
			..
		}
		| PhysOp::Subquery
		| PhysOp::Probe {
			..
		}
		| PhysOp::Union
		| PhysOp::Distinct {
			..
		}
		| PhysOp::PermissionFilter(..)
		| PhysOp::Layer(..)
		| PhysOp::Filter
		| PhysOp::Fetch
		| PhysOp::Limit {
			..
		}
		| PhysOp::Project(..)
		| PhysOp::Request {
			..
		}
		| PhysOp::Txn {
			..
		}
		| PhysOp::Stmt {
			..
		}
		| PhysOp::Statement {
			..
		}
		| PhysOp::Match {
			..
		}
		| PhysOp::DdlGuard {
			..
		}
		| PhysOp::Define(..)
		| PhysOp::Remove(..)
		| PhysOp::Alter(..)
		| PhysOp::Rebuild(..)
		| PhysOp::Info {
			..
		}
		| PhysOp::Use {
			..
		}
		| PhysOp::Option {
			..
		}
		| PhysOp::Access(..)
		| PhysOp::Kill
		| PhysOp::Live(..)
		| PhysOp::Subscriber(..)
		| PhysOp::Show(..)
		| PhysOp::WriteStage {
			..
		}
		| PhysOp::Events(..)
		| PhysOp::Timeout
		| PhysOp::Assign(..)
		| PhysOp::Unset {
			..
		}
		| PhysOp::Content {
			..
		}
		| PhysOp::Block
		| PhysOp::Loop {
			..
		}
		| PhysOp::Pipeline(..)
		| PhysOp::Choose(..)
		| PhysOp::Prefetch(..)
		| PhysOp::Materialize {
			..
		}
		| PhysOp::LoadUnit(..)
		| PhysOp::Break
		| PhysOp::Next => None,
	}
}

// ---------------------------------------------------------------------------------------------
// The rows of the chain
// ---------------------------------------------------------------------------------------------

/// What the rows of a pipeline are at one point: whether each is an object, and whether it may be
/// a record id.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Rows {
	objects: bool,
	ids: bool,
}

impl Rows {
	/// Rows nothing is known of: not objects, and possibly ids, which is what makes a body that
	/// reads the record each row names the most likely.
	const UNKNOWN: Self = Self {
		objects: false,
		ids: true,
	};
	/// Rows a binding step produced: objects keyed by binding name.
	const BOUND: Self = Self {
		objects: true,
		ids: false,
	};
}

/// What the chain of a pipeline holds, apart from its source and its sink.
struct ChainFacts {
	enforce: bool,
	limits: usize,
	binds_row: bool,
	fetches_row: bool,
	distincts: usize,
	paging_replayable: bool,
}

/// The facts of the ops `steps` that stream above `source`, in data-flow order, over rows
/// `source_rows`.
fn chain_facts(source: Option<&PhysOp>, source_rows: Rows, steps: &[&PhysOp]) -> ChainFacts {
	ChainFacts {
		enforce: steps.iter().any(|step| matches!(step, PhysOp::Enforced { .. })),
		limits: steps.iter().filter(|step| matches!(step, PhysOp::Limit { .. })).count(),
		binds_row: steps.iter().any(|step| matches!(step, PhysOp::MatchStep(..))),
		fetches_row: fetches_row(source_rows, steps),
		distincts: steps.iter().filter(|step| matches!(step, PhysOp::Distinct { .. })).count(),
		paging_replayable: paging_replayable(source, source_rows, steps),
	}
}

/// Whether the chain reads the record each row names, where a row may be a record id: a `SELECT *`
/// over rows that may be ids, or `*` beside fields over rows that are not objects and may be ids.
/// The read a write makes of its own subject is the sink's, and is not counted.
fn fetches_row(source_rows: Rows, steps: &[&PhysOp]) -> bool {
	let mut rows = source_rows;
	for step in steps {
		if matches!(step, PhysOp::MatchStep(..)) {
			rows = Rows::BOUND;
		} else if let PhysOp::Project(shape) = step {
			let reads = match **shape {
				ProjectionShape::All => rows.ids,
				ProjectionShape::Star {
					..
				} => !rows.objects && rows.ids,
				ProjectionShape::Value
				| ProjectionShape::OmittedValue {
					..
				}
				| ProjectionShape::SortedValue(..)
				| ProjectionShape::Fields {
					..
				} => false,
			};
			if reads {
				return true;
			}
		}
	}
	false
}

/// Whether the paging of the pipeline's one limit is one a merge can replay over rows counted in
/// order: the body runs the same instructions for a row taken whatever the counts were, `START`
/// skips a row to its end before it emits anything, `LIMIT` counts the row straight after, and the
/// check that the limit is spent comes last and is the only way to the pipeline's end.
///
/// A `LIMIT` is replayable only over a source the lowering does not stop at the limit: a scan, a
/// B-tree index, an index union, a bitmap, a count, a sort, a grouping, a probe or a join. It is
/// not over a record read by its id, over rows that are not objects, over a lookup, a full-text
/// scan, a search, a union or a value, after a binding, or over a pipeline whose source lies
/// outside it.
fn paging_replayable(source: Option<&PhysOp>, source_rows: Rows, steps: &[&PhysOp]) -> bool {
	let storage_replays = source.is_some_and(replays_from_storage);
	let is_join = matches!(source, Some(PhysOp::Join(..)));
	let mut rows = source_rows;
	let mut rebound = false;
	let mut replayable = false;
	for step in steps {
		if matches!(step, PhysOp::MatchStep(..)) {
			rebound = true;
			rows = Rows::BOUND;
		} else if let PhysOp::Limit {
			limit,
			start,
		} = step
		{
			replayable = limit.is_some() || start.is_some();
			if limit.is_some() && ((!rows.objects && !is_join) || rebound || !storage_replays) {
				replayable = false;
			}
		}
	}
	replayable
}

/// Whether a limit over the source `source` is counted by the rows the source yields, and so may
/// be replayed by a merge over parts.
fn replays_from_storage(source: &PhysOp) -> bool {
	matches!(
		source,
		PhysOp::TableScan(..)
			| PhysOp::IndexScan(..)
			| PhysOp::IndexUnion(..)
			| PhysOp::Bitmap(..)
			| PhysOp::Count(..)
			| PhysOp::Sort(..)
			| PhysOp::KnnTopK(..)
			| PhysOp::Aggregate(..)
			| PhysOp::Probe { .. }
			| PhysOp::Join(..)
	)
}

/// What the rows the source op `source` hands the chain above it are.
fn rows_of_source(ir: &Ir, source: OpId, depth: usize) -> Rows {
	let Some(kind) = phys_of(ir, source) else {
		return Rows::UNKNOWN;
	};
	if depth >= MAX_INPUT_DEPTH {
		return Rows::UNKNOWN;
	}
	let input = || producer(ir, ir.op(source)?.operands.first().copied()?);
	if matches!(kind, PhysOp::Sort(..) | PhysOp::KnnTopK(..)) {
		let objects = matches!(kind, PhysOp::KnnTopK(..))
			|| input().is_some_and(|input| rows_after(ir, input, depth + 1).objects);
		let ids = input().is_none_or(|input| rows_after(ir, input, depth + 1).ids);
		Rows {
			objects,
			ids,
		}
	} else if matches!(kind, PhysOp::Split { .. }) {
		// A split rewrites the fields it splits on, so its rows are objects when its input's rows
		// are and its input projects no `VALUE`.
		let objects = input().is_some_and(|input| {
			let value_projection = matches!(
				phys_of(ir, top_of(ir, input)),
				Some(PhysOp::Project(shape))
					if matches!(
						**shape,
						ProjectionShape::Value | ProjectionShape::OmittedValue { .. }
					)
			);
			rows_after(ir, input, depth + 1).objects && !value_projection
		});
		Rows {
			objects,
			ids: false,
		}
	} else if matches!(kind, PhysOp::MatchStep(..)) {
		Rows::BOUND
	} else {
		Rows {
			objects: yields_records(ir, source, depth),
			ids: yields_ids(ir, source, depth),
		}
	}
}

/// What the rows of the pipeline whose rows the op `producer` produces are once its chain has run.
fn rows_after(ir: &Ir, producer: OpId, depth: usize) -> Rows {
	let ops = view_ops(ir, producer);
	let Some((first, rest)) = ops.split_first() else {
		return Rows::UNKNOWN;
	};
	let mut rows = match phys_of(ir, *first) {
		Some(kind) if !streams(kind) => rows_of_source(ir, *first, depth),
		Some(_) | None => Rows::UNKNOWN,
	};
	for op in rest {
		if matches!(phys_of(ir, *op), Some(PhysOp::MatchStep(..))) {
			rows = Rows::BOUND;
		}
	}
	rows
}

/// Whether a row the source `op` yields is a record: every source but a value, a statement in
/// `FROM`, a join and a count, and a union of sources that each yield records.
fn yields_records(ir: &Ir, op: OpId, depth: usize) -> bool {
	match phys_of(ir, op) {
		Some(
			PhysOp::Values {
				..
			}
			| PhysOp::Subquery
			| PhysOp::Join(..)
			| PhysOp::Count(..),
		) => false,
		Some(PhysOp::Union) => {
			depth < MAX_INPUT_DEPTH
				&& members(ir, op).into_iter().all(|member| {
					member.is_some_and(|member| yields_records(ir, member, depth + 1))
				})
		}
		Some(_) => true,
		None => false,
	}
}

/// Whether a row the source `op` yields may be a record id: a statement in `FROM` yields its rows
/// as they are.
fn yields_ids(ir: &Ir, op: OpId, depth: usize) -> bool {
	match phys_of(ir, op) {
		Some(PhysOp::Subquery) | None => true,
		Some(PhysOp::Union) => {
			depth >= MAX_INPUT_DEPTH
				|| members(ir, op)
					.into_iter()
					.any(|member| member.is_none_or(|member| yields_ids(ir, member, depth + 1)))
		}
		Some(_) => false,
	}
}

/// The members of the union `op`: for each operand, the op that produces it, or the last op of the
/// pipeline that does. An operand no op produces, a block argument, is a member that is no
/// physical op: `None`. An op that is not held is answered as one such member.
fn members(ir: &Ir, op: OpId) -> Vec<Option<OpId>> {
	let Some(data) = ir.op(op) else {
		return vec![None];
	};
	data.operands
		.iter()
		.map(|operand| producer(ir, *operand).map(|producer| top_of(ir, producer)))
		.collect()
}

// ---------------------------------------------------------------------------------------------
// What the expressions nest, and what writes
// ---------------------------------------------------------------------------------------------

/// What the expression regions of a pipeline hold.
struct Nesting {
	/// The statements written in value position, in the order they are met.
	subqueries: Vec<OpId>,
	/// A graph hop, which is read as a statement of its own, evaluated afresh for each row.
	graph_steps: bool,
	/// A statement in value position that is a block construct.
	host_blocks: bool,
}

/// The expression regions of the ops `ops`, searched for the statements nested in them.
fn scan_expressions(ir: &Ir, ops: &[OpId]) -> Nesting {
	let mut nesting = Nesting {
		subqueries: Vec::new(),
		graph_steps: false,
		host_blocks: false,
	};
	let regions: Vec<RegionId> = ops
		.iter()
		.filter_map(|op| ir.op(*op))
		.flat_map(|data| data.regions.iter().copied())
		.collect();
	for region in regions {
		ir.walk(region, WalkOrder::PreOrder, &mut |op| {
			match ir.op(op).map(|data| &data.kind) {
				Some(OpKind::Expr(ExprOp::Subquery(_))) => {
					nesting.subqueries.push(op);
					nesting.host_blocks |= holds_block_construct(ir, op);
				}
				Some(OpKind::Phys(PhysOp::Loop {
					kind: LoopKind::GraphStep,
					..
				})) => nesting.graph_steps = true,
				Some(_) | None => {}
			}
			Walk::Continue
		});
	}
	nesting
}

/// What the statements nested in a pipeline's expressions make of its use.
///
/// A nested statement that writes is [`NestedUse::Other`]. A graph hop with nothing else nested is
/// [`NestedUse::PerRowReadOnly`]. For a statement that only reads, [`NestedReading::Hedged`] makes
/// it [`NestedUse::OnceOrProbed`], which the policy refuses to fan out. Under
/// [`NestedReading::Strategy`] its strategy decides: `Once`, `Hash` and a `Lookup` that keeps a
/// hashed plan evaluate the statement once or through a probe ([`NestedUse::OnceOrProbed`]);
/// `Apply` and a `Lookup` without a hashed plan evaluate it afresh for each row
/// ([`NestedUse::PerRowReadOnly`]).
///
/// A statement that holds the rows of a `Once` strategy as a membership set
/// ([`nested::held_as_members`]) is [`NestedUse::Other`], as one that writes is; the policy refuses
/// it as it refuses a single value kept.
fn nested_use(
	ir: &Ir,
	facts: &mut FactCache,
	registry: &FunctionRegistry,
	nesting: &Nesting,
	reading: NestedReading,
) -> NestedUse {
	let mut nested = if nesting.graph_steps {
		NestedUse::PerRowReadOnly
	} else {
		NestedUse::None
	};
	for subquery in &nesting.subqueries {
		let contribution = if writes_within(ir, facts, registry, *subquery)
			|| (reading == NestedReading::Strategy
				&& matches!(strategy_of(ir, *subquery), Some(NestedStrategy::Once))
				&& nested::held_as_members(ir, *subquery))
		{
			NestedUse::Other
		} else if reading == NestedReading::Strategy
			&& matches!(
				strategy_of(ir, *subquery),
				Some(
					NestedStrategy::Apply
						| NestedStrategy::Lookup {
							hashed: false,
							..
						}
				)
			) {
			NestedUse::PerRowReadOnly
		} else {
			NestedUse::OnceOrProbed
		};
		nested = nested.max(contribution);
	}
	nested
}

/// The strategy of the statement in value position `subquery`.
fn strategy_of(ir: &Ir, subquery: OpId) -> Option<&NestedStrategy> {
	match &ir.op(subquery)?.kind {
		OpKind::Expr(ExprOp::Subquery(strategy)) => Some(strategy),
		OpKind::Expr(_) | OpKind::Phys(_) | OpKind::Logical(_) => None,
	}
}

/// Whether the statement in value position `subquery` holds, directly, a statement the planner
/// lowers as a block construct: a block, a `LET`, an `IF`, a `FOR`, a `SLEEP`, a `RETURN`, a
/// `THROW`, a `BREAK` or a `CONTINUE`.
fn holds_block_construct(ir: &Ir, subquery: OpId) -> bool {
	/// The ops that stand directly in `region`, looking through a statement's own region.
	fn heads(ir: &Ir, region: RegionId, out: &mut Vec<OpId>) {
		for op in ops_in(ir, region) {
			let Some(data) = ir.op(op) else {
				continue;
			};
			let wrapper =
				matches!(&data.kind, OpKind::Phys(PhysOp::Stmt { .. } | PhysOp::Statement { .. }));
			if wrapper {
				for inner in &data.regions {
					heads(ir, *inner, out);
				}
			} else {
				out.push(op);
			}
		}
	}
	let mut found = Vec::new();
	if let Some(data) = ir.op(subquery) {
		for region in &data.regions {
			heads(ir, *region, &mut found);
		}
	}
	found.into_iter().any(|op| match ir.op(op).map(|data| &data.kind) {
		Some(OpKind::Phys(kind)) => is_block_construct(kind),
		Some(OpKind::Expr(ExprOp::Throw | ExprOp::If)) => true,
		Some(OpKind::Expr(..) | OpKind::Logical(..)) | None => false,
	})
}

/// Whether the statement op `op` is a construct the planner lowers as a block.
fn is_block_construct(op: &PhysOp) -> bool {
	matches!(
		op,
		PhysOp::Block
			| PhysOp::Let(..)
			| PhysOp::If { .. }
			| PhysOp::Sleep
			| PhysOp::Return
			| PhysOp::Break
			| PhysOp::Next
			| PhysOp::Loop {
				kind: LoopKind::For,
				..
			}
	)
}

/// Whether a statement that encloses the pipeline `pipeline_op` writes, in the scope the planner
/// places the pipeline in: the statements that contain it, the nearest first.
fn enclosing_writes(
	ir: &Ir,
	facts: &mut FactCache,
	registry: &FunctionRegistry,
	pipeline_op: OpId,
) -> bool {
	let mut at = pipeline_op;
	while let Some((owner, _)) =
		enclosing_region_of(ir, at, |kind| matches!(kind, RegionKind::Statement(_)))
	{
		if writes_within(ir, facts, registry, owner) {
			return true;
		}
		at = owner;
	}
	false
}

/// Whether the op `op` or an op in its regions writes or locks what it reads.
///
/// The answer is read from the ops themselves. A physical op's effects are those of the logical
/// ops it stands for, which the physical container does not keep, so its effects are not
/// consulted: an op writes when [`nested::statement_writes`] finds a write by shape or an
/// expression that may write, judged under `registry`, or when a physical op changes the
/// session, a grant, a subscription or a binding, or stands in a write's body
/// ([`changes_state_by_shape`]).
fn writes_within(ir: &Ir, facts: &mut FactCache, registry: &FunctionRegistry, op: OpId) -> bool {
	if nested::statement_writes(ir, facts, &structural_cx(registry, ir), op) {
		return true;
	}
	let found = std::cell::Cell::new(false);
	let mut visit = |inner: OpId| {
		if phys_of(ir, inner).is_some_and(changes_state_by_shape) {
			found.set(true);
			Walk::Stop
		} else {
			Walk::Continue
		}
	};
	if visit(op) == Walk::Stop {
		return true;
	}
	if let Some(data) = ir.op(op) {
		for region in &data.regions {
			ir.walk(*region, WalkOrder::PreOrder, &mut visit);
			if found.get() {
				break;
			}
		}
	}
	found.get()
}

/// Whether the op changes state [`writes_by_shape`] does not name: the session (`USE`, `OPTION`,
/// a `LET`), a grant, a subscription (`LIVE`, `KILL`, a subscriber) or the record a write's body
/// stages.
fn changes_state_by_shape(op: &PhysOp) -> bool {
	matches!(
		op,
		PhysOp::Use { .. }
			| PhysOp::Option { .. }
			| PhysOp::Let(..)
			| PhysOp::Access(..)
			| PhysOp::Kill
			| PhysOp::Live(..)
			| PhysOp::Subscriber(..)
			| PhysOp::WriteStage { .. }
			| PhysOp::Events(..)
			| PhysOp::Assign(..)
			| PhysOp::Unset { .. }
			| PhysOp::Content { .. }
	)
}

/// Whether the op writes, or locks what it reads, whatever its footprint states.
pub(super) fn writes_by_shape(op: &PhysOp) -> bool {
	if let PhysOp::MatchStep(attrs) = op {
		return matches!(
			**attrs,
			MatchStepAttrs::Update(..) | MatchStepAttrs::Delete { .. } | MatchStepAttrs::Insert(..)
		) || matches!(
			&**attrs,
			MatchStepAttrs::Bind(shape) if shape.writes()
		);
	}
	matches!(
		op,
		PhysOp::Write(..)
			| PhysOp::Define(..)
			| PhysOp::Remove(..)
			| PhysOp::Alter(..)
			| PhysOp::Rebuild(..)
			| PhysOp::Values {
				read: ValuesRead::Locked,
			}
	) || matches!(op, PhysOp::RecordScan(attrs) if attrs.for_update)
}

// ---------------------------------------------------------------------------------------------
// Reading the container
// ---------------------------------------------------------------------------------------------

/// The physical op `op` is, or `None` for a dangling id and an op of another dialect.
pub(super) fn phys_of(ir: &Ir, op: OpId) -> Option<&PhysOp> {
	match &ir.op(op)?.kind {
		OpKind::Phys(kind) => Some(kind),
		OpKind::Expr(..) | OpKind::Logical(..) => None,
	}
}

/// The op that defines `value`, or `None` for a block argument and a dangling id.
fn producer(ir: &Ir, value: ValueId) -> Option<OpId> {
	match ir.value(value)?.def {
		ValueDef::OpResult {
			op,
			..
		} => Some(op),
		ValueDef::BlockArg {
			..
		} => None,
	}
}

/// The physical ops of the pipeline view that ends in the op `producer`, in data-flow order: the
/// ops of a `phys.pipeline` without its terminator, or the chain [`chain_of`] finds above a bare
/// op.
fn view_ops(ir: &Ir, producer: OpId) -> Vec<OpId> {
	if let Some(PhysOp::Pipeline(..)) = phys_of(ir, producer) {
		return pipeline_entries(ir, producer).into_iter().map(|(op, _)| op).collect();
	}
	chain_of(ir, producer)
}

/// The last op of the pipeline view that ends in `producer`: the op whose result is the rows.
fn top_of(ir: &Ir, producer: OpId) -> OpId {
	view_ops(ir, producer).last().copied().unwrap_or(producer)
}

/// The physical ops a `phys.pipeline` holds, with their kinds, in data-flow order and without the
/// terminator that yields the pipeline's result.
pub(super) fn pipeline_entries(ir: &Ir, pipeline_op: OpId) -> Vec<(OpId, &PhysOp)> {
	let Some(region) = ir.op(pipeline_op).and_then(|data| data.regions.first().copied()) else {
		return Vec::new();
	};
	ops_in(ir, region)
		.filter_map(|op| phys_of(ir, op).map(|kind| (op, kind)))
		.filter(|(_, kind)| !kind.is_terminator())
		.collect()
}

/// The physical facts of one table read for the e-graph: `None` where no index may be chosen for
/// it (an unresolved permission context), which leaves the source without an entry.
///
/// `indexes` are the admissible access candidates of the read. `withheld_order` names the top-level
/// fields a restricted `SELECT` permission governs, so no alternative delivers an order over them;
/// a restricted path that names no top-level field withholds every field. `estimate` is always
/// `None`: the plan carries no estimate of the table's rows.
pub(crate) fn table_facts_of(
	answers: &TableAnswers,
	with: Option<&With>,
	current_snapshot: bool,
) -> Option<PhysTableFacts> {
	let indexes = admissible_access_candidates(answers, with)?;
	let withheld_order = match &answers.restricted {
		RestrictedSelect::Unresolved => return None,
		RestrictedSelect::None => FieldSet::Some(SmallVec::new()),
		RestrictedSelect::Fields(paths) => {
			let mut roots: SmallVec<[Strand; 4]> = SmallVec::new();
			let mut every_field = false;
			for path in paths {
				match path.root() {
					Some(root) if !roots.contains(root) => roots.push(root.clone()),
					Some(_) => {}
					None => every_field = true,
				}
			}
			if every_field {
				FieldSet::All
			} else {
				FieldSet::Some(roots)
			}
		}
	};
	Some(PhysTableFacts {
		table: answers.table.clone(),
		indexes,
		withheld_order,
		hint: with.cloned(),
		current_snapshot,
		estimate: None,
	})
}

/// The physical facts of each of `sources`. Answers an empty list for no sources.
///
/// A source reading the current snapshot has an entry where the physical catalog's answers for its
/// read admit an index candidate. A source at a `VERSION` has none, and neither has a source whose
/// answers are `Unknown` after a catalog change. A source whose answers the memo does not hold
/// answers [`IrError::NotLoaded`], and its read is pending.
///
/// Reads nothing, so it is synchronous: the host wraps the answer in a ready future.
pub(crate) fn table_facts(
	cx: &PhysCx<'_>,
	sources: &[(TableName, Option<With>, SnapshotId)],
) -> Result<Vec<PhysTableFacts>, IrError> {
	let mut facts = Vec::new();
	for (table, with, snapshot) in sources {
		if *snapshot != SnapshotId::new(0) {
			continue;
		}
		let key: SourceKey = (table.clone(), with.clone(), ReadVersion::Current);
		match cx.physical.table_answers(&key) {
			CatalogAnswer::Found(answers) => {
				facts.extend(table_facts_of(&answers, with.as_ref(), true));
			}
			CatalogAnswer::NotLoaded => {
				return Err(IrError::NotLoaded {
					what: "facts::table_facts: the answers of a table read",
				});
			}
			CatalogAnswer::Absent | CatalogAnswer::Unknown => {}
		}
	}
	Ok(facts)
}

#[cfg(test)]
mod tests {
	use std::str::FromStr;
	use std::sync::Arc;

	use smallvec::smallvec;
	use surrealdb_strand::{Strand, TableName};

	use super::*;
	use crate::catalog::{Index, IndexDefinition, IndexId};
	use crate::exec::function::FunctionRegistry;
	use crate::exec::index::access_path::BTreeAccess;
	use crate::exec::ir::core::ir::IrScope;
	use crate::exec::ir::iface::IndexRef;
	use crate::exec::ir::iface::catalog::{EffectCx, PhysicalCatalog, SemanticCatalog};
	use crate::exec::ir::iface::frame::FrameTable;
	use crate::exec::ir::phys::ops::{
		FullTextScanAttrs, IndexLookupAttrs, IndexScanAttrs, IndexUnionAttrs, KeyRangeScanAttrs,
		KeyRangeShape, LookupKeyShape, ReadClauses, RecordScanAttrs, TableScanAttrs,
		key_range_fixtures,
	};
	use crate::exec::vm::payload::Spill;
	use crate::expr::Idiom;
	use crate::kvs::Direction;

	fn table(name: &str) -> TableName {
		TableName::new(Strand::new(name))
	}

	fn index_ref() -> IndexRef {
		IndexRef {
			table: table("t"),
			index: Strand::new("i"),
			position: 0,
			scalar: Box::default(),
		}
	}

	fn table_scan(lightweight: bool) -> PhysOp {
		PhysOp::TableScan(Box::new(TableScanAttrs {
			probes: None,
			bounds: None,
			prefilter: None,
			table: table("t"),
			direction: Direction::Forward,
			topk: None,
			lightweight,
			id_order: true,
			read: ReadClauses::default(),
		}))
	}

	fn index_scan() -> PhysOp {
		PhysOp::IndexScan(Box::new(IndexScanAttrs {
			table: table("t"),
			index: index_ref(),
			access: BTreeAccess::literal_equality(crate::val::Value::from(1i64)),
			direction: Direction::Forward,
			ordered_by: None,
			pinned: None,
			stated: None,
			rechecked: false,
			decode: None,
			read: ReadClauses::default(),
		}))
	}

	fn record_scan() -> PhysOp {
		record_scan_of(false, false)
	}

	fn record_scan_of(for_update: bool, lightweight: bool) -> PhysOp {
		PhysOp::RecordScan(Box::new(RecordScanAttrs {
			table: TableName::new(Strand::new("t")),
			for_update,
			lightweight,
			read: ReadClauses::default(),
		}))
	}

	fn limit(limit: bool, start: bool) -> PhysOp {
		PhysOp::Limit {
			limit: limit.then_some(0),
			start: start.then_some(1),
		}
	}

	#[test]
	fn source_kinds_follow_the_access_path() {
		let lookup = PhysOp::IndexLookup(Box::new(IndexLookupAttrs {
			table: table("t"),
			index: index_ref(),
			field: Strand::new("a"),
			key: LookupKeyShape::Parent,
			read: ReadClauses::default(),
		}));
		let union = PhysOp::IndexUnion(Box::new(IndexUnionAttrs {
			topk: None,
			table: table("t"),
			branches: Vec::new(),
			merge: None,
			ordered_by: None,
			decode: None,
			read: ReadClauses::default(),
		}));
		let full_text = PhysOp::FullTextScan(Box::new(FullTextScanAttrs {
			table: table("t"),
			index: index_ref(),
			query: "x".to_owned(),
			operator: crate::expr::operator::MatchesOperator {
				rf: None,
				operator: crate::expr::operator::BooleanOperator::And,
			},
			column: None,
			decode: None,
			read: ReadClauses::default(),
			topk: None,
		}));
		let key_range = PhysOp::KeyRangeScan(Box::new(KeyRangeScanAttrs {
			prefilter: None,
			table: table("t"),
			read: Default::default(),
			access: KeyRangeShape {
				key: key_range_fixtures::pair(),
				eq: Vec::new(),
				range: None,
				direction: Direction::Forward,
				delivers: crate::exec::index::key_range::DeliveredKeyOrder::None,
				ordered_by: Vec::new(),
			},
			topk: None,
		}));
		let cases = [
			(table_scan(false), SourceKind::Scan),
			(table_scan(true), SourceKind::LightweightEdges),
			(key_range, SourceKind::Other),
			(record_scan(), SourceKind::Record),
			(record_scan_of(false, true), SourceKind::LightweightEdges),
			(record_scan_of(true, true), SourceKind::Record),
			(
				index_scan(),
				SourceKind::Index {
					repeats: true,
				},
			),
			(lookup, SourceKind::Lookup),
			(union, SourceKind::IndexUnion),
			(full_text, SourceKind::FullText),
			(PhysOp::Union, SourceKind::Other),
			(PhysOp::Filter, SourceKind::Other),
			(
				PhysOp::Distinct {
					spill: Spill::Bounded,
				},
				SourceKind::Other,
			),
		];
		for (op, expected) in cases {
			assert_eq!(source_kind(&op), expected, "{:?}", op.tag());
		}
	}

	/// A request container holding one statement in value position with `strategy`, which only
	/// reads; answers the container and the statement.
	fn container_with_subquery(strategy: NestedStrategy) -> (Ir, OpId) {
		use crate::exec::ir::core::location::{LocOrigin, Location};
		use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
		use crate::exec::ir::iface::ids::{FrameId, SnapshotId};

		let mut ir = Ir::new(
			IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		);
		let mut facts = FactCache::new();
		let top = ir.region(ir.top()).expect("the top region exists").blocks[0];
		let mut rw = Rewriter::new(&mut ir, &mut facts);
		let subquery = rw
			.create(
				InsertPoint::End(top),
				OpSpec {
					kind: OpKind::Expr(ExprOp::Subquery(strategy)),
					operands: SmallVec::new(),
					results: smallvec::smallvec![
						crate::exec::ir::iface::types::TypeKnowledge::Unknown
					],
					frame: FrameId::new(0),
					snapshot: SnapshotId::new(0),
					loc: Location {
						stmt: 0,
						span: None,
						origin: LocOrigin::Synthesized,
					},
				},
			)
			.expect("the statement is created");
		rw.create_region(subquery, RegionKind::Subquery, &[]).expect("the region is created");
		(ir, subquery)
	}

	#[test]
	fn a_read_only_statement_is_read_by_its_strategy() {
		let lookup = |hashed| NestedStrategy::Lookup {
			index: Box::new(index_ref()),
			hashed,
		};
		let cases = [
			(NestedStrategy::Apply, NestedUse::PerRowReadOnly),
			(NestedStrategy::Once, NestedUse::OnceOrProbed),
			(
				NestedStrategy::Hash {
					field: Strand::new("a"),
				},
				NestedUse::OnceOrProbed,
			),
			(lookup(false), NestedUse::PerRowReadOnly),
			(lookup(true), NestedUse::OnceOrProbed),
		];
		for (strategy, read) in cases {
			let (ir, subquery) = container_with_subquery(strategy.clone());
			let mut facts = FactCache::new();
			let nesting = Nesting {
				subqueries: vec![subquery],
				graph_steps: false,
				host_blocks: false,
			};
			assert_eq!(
				nested_use(
					&ir,
					&mut facts,
					crate::exec::ir::analysis::cx::builtin_registry(),
					&nesting,
					NestedReading::Hedged
				),
				NestedUse::OnceOrProbed,
				"hedged {strategy:?}"
			);
			assert_eq!(
				nested_use(
					&ir,
					&mut facts,
					crate::exec::ir::analysis::cx::builtin_registry(),
					&nesting,
					NestedReading::Strategy
				),
				read,
				"{strategy:?}"
			);
		}
	}

	#[test]
	fn an_op_that_is_not_held_is_no_pipeline() {
		let ir = Ir::new(
			IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		);
		let mut ids: slotmap::SlotMap<OpId, ()> = slotmap::SlotMap::with_key();
		let dangling = ids.insert(());
		let mut facts = FactCache::new();
		assert!(matches!(
			facts_of(&ir, &mut facts, crate::exec::ir::analysis::cx::builtin_registry(), dangling),
			Err(IrError::Stale("not a pipeline"))
		));
	}

	#[test]
	fn a_pipeline_estimate_is_absent() {
		let chain = chain_facts(Some(&table_scan(false)), Rows::UNKNOWN, &[]);
		let facts =
			assemble(SourceKind::Scan, SinkKind::Output, false, &chain, NestedUse::None, false);
		assert!(facts.estimate.is_none());
		assert!(facts.output);
		assert_eq!(facts.limits, 0);
	}

	#[test]
	fn only_the_statement_output_is_flagged_output() {
		let chain = chain_facts(None, Rows::UNKNOWN, &[]);
		for sink in [
			SinkKind::Sort {
				bounded: true,
			},
			SinkKind::Aggregate,
			SinkKind::Write,
		] {
			let facts = assemble(SourceKind::Scan, sink, false, &chain, NestedUse::None, false);
			assert!(!facts.output);
			assert_eq!(facts.sink, sink);
		}
	}

	#[test]
	fn the_chain_counts_its_limits_distincts_and_enforcement() {
		let enforced = PhysOp::Enforced {
			table: table("t"),
		};
		let distinct = PhysOp::Distinct {
			spill: Spill::Bounded,
		};
		let paging = limit(true, false);
		let chain = chain_facts(
			Some(&table_scan(false)),
			Rows::UNKNOWN,
			&[&enforced, &distinct, &paging, &PhysOp::Filter],
		);
		assert!(chain.enforce);
		assert_eq!(chain.distincts, 1);
		assert_eq!(chain.limits, 1);
		assert!(!chain.binds_row);
	}

	#[test]
	fn paging_replays_over_a_scan_of_objects_and_not_over_a_record_read() {
		let objects = Rows::BOUND;
		let paging = limit(true, true);
		assert!(paging_replayable(Some(&table_scan(false)), objects, &[&paging]));
		assert!(paging_replayable(Some(&index_scan()), objects, &[&paging]));
		assert!(!paging_replayable(Some(&record_scan()), objects, &[&paging]));
		// `START` alone skips rows without counting a limit.
		assert!(paging_replayable(Some(&record_scan()), objects, &[&limit(false, true)]));
		// Rows that are not objects are not counted by the scan.
		assert!(!paging_replayable(Some(&table_scan(false)), Rows::UNKNOWN, &[&paging]));
		// A pipeline with no limit, and one whose source lies outside it, replay nothing.
		assert!(!paging_replayable(Some(&table_scan(false)), objects, &[&PhysOp::Filter]));
		assert!(!paging_replayable(None, objects, &[&paging]));
	}

	#[test]
	fn a_star_projection_over_ids_reads_the_record() {
		let all = PhysOp::Project(Box::new(ProjectionShape::All));
		let value = PhysOp::Project(Box::new(ProjectionShape::Value));
		assert!(fetches_row(Rows::UNKNOWN, &[&all]));
		assert!(!fetches_row(Rows::BOUND, &[&all]));
		assert!(!fetches_row(Rows::UNKNOWN, &[&value]));
		assert!(!fetches_row(Rows::UNKNOWN, &[]));
	}

	/// A semantic catalog that holds no definition.
	struct NoCatalog;

	impl SemanticCatalog for NoCatalog {}

	/// A physical catalog that holds no index and no statistics.
	struct NoPhysical;

	impl PhysicalCatalog for NoPhysical {}

	fn idx(id: u32, name: &str, cols: &[&str]) -> IndexDefinition {
		IndexDefinition {
			index_id: IndexId(id),
			name: Strand::from(name),
			table_name: "t".into(),
			cols: cols.iter().map(|c| Idiom::from_str(c).expect("valid idiom")).collect(),
			index: Index::Idx,
			count_cond: None,
			comment: None,
			prepare_remove: false,
			format_version: 1,
			nulls_not_distinct: false,
		}
	}

	fn answers(indexes: Vec<IndexDefinition>) -> TableAnswers {
		TableAnswers::plain("t".into(), true).with_indexes(indexes)
	}

	fn restricted(path: &str) -> RestrictedSelect {
		RestrictedSelect::Fields(vec![
			crate::expr::logical::FieldPath::from_idiom(&Idiom::from_str(path).expect("idiom"))
				.expect("a field path"),
		])
	}

	#[test]
	fn no_sources_need_no_catalog() {
		let registry = FunctionRegistry::with_builtins();
		let frames = FrameTable::default();
		let cx = PhysCx {
			effects: EffectCx {
				registry: &registry,
				catalog: &NoCatalog,
				frames: &frames,
			},
			physical: &NoPhysical,
			config: crate::exec::ir::iface::catalog::default_config(),
		};
		assert!(table_facts(&cx, &[]).expect("no source is answered").is_empty());
	}

	/// A physical catalog that answers what it is given for each read, and that a read it was
	/// given nothing for is not loaded.
	struct Held(Vec<(SourceKey, CatalogAnswer<Arc<TableAnswers>>)>);

	impl PhysicalCatalog for Held {
		fn table_answers(&self, key: &SourceKey) -> CatalogAnswer<Arc<TableAnswers>> {
			self.0
				.iter()
				.find(|(held, _)| held == key)
				.map_or(CatalogAnswer::NotLoaded, |(_, answer)| answer.clone())
		}
	}

	fn facts_held(
		held: &Held,
		sources: &[(TableName, Option<With>, SnapshotId)],
	) -> Result<Vec<PhysTableFacts>, IrError> {
		let registry = FunctionRegistry::with_builtins();
		let frames = FrameTable::default();
		let cx = PhysCx {
			effects: EffectCx {
				registry: &registry,
				catalog: &NoCatalog,
				frames: &frames,
			},
			physical: held,
			config: crate::exec::ir::iface::catalog::default_config(),
		};
		table_facts(&cx, sources)
	}

	fn current_key(name: &str) -> SourceKey {
		(table(name), None, ReadVersion::Current)
	}

	#[test]
	fn a_current_read_has_the_facts_its_answers_admit() {
		let answers = answers(vec![idx(1, "ix", &["x"])]);
		let held = Held(vec![(current_key("t"), CatalogAnswer::Found(Arc::new(answers)))]);
		let facts = facts_held(&held, &[(table("t"), None, SnapshotId::new(0))])
			.expect("the answers are held");
		let [fact] = facts.as_slice() else {
			panic!("one entry, got {facts:?}");
		};
		assert_eq!(fact.table, table("t"));
		assert_eq!(fact.indexes.len(), 1);
		assert!(fact.current_snapshot);
	}

	#[test]
	fn a_read_the_memo_does_not_hold_is_not_loaded() {
		let held = Held(Vec::new());
		assert!(matches!(
			facts_held(&held, &[(table("t"), None, SnapshotId::new(0))]),
			Err(IrError::NotLoaded { .. })
		));
	}

	#[test]
	fn a_versioned_source_and_an_unknown_answer_have_no_entry() {
		let held = Held(vec![(current_key("t"), CatalogAnswer::Unknown)]);
		let versioned = SnapshotId::new(1);
		assert!(facts_held(&held, &[(table("t"), None, versioned)]).expect("no read").is_empty());
		assert!(
			facts_held(&held, &[(table("t"), None, SnapshotId::new(0))])
				.expect("an unknown answer is no entry")
				.is_empty()
		);
	}

	#[test]
	fn facts_carry_the_admissible_candidates() {
		let table_answers = answers(vec![idx(1, "ix", &["x"]), idx(2, "iy", &["y"])]);
		let facts = table_facts_of(&table_answers, Some(&With::NoIndex), true).expect("an entry");
		assert!(facts.indexes.is_empty());
		assert_eq!(facts.hint, Some(With::NoIndex));
		assert!(facts.current_snapshot);
		assert_eq!(facts.table, table("t"));
		let facts = table_facts_of(&table_answers, None, false).expect("an entry");
		assert_eq!(facts.indexes.len(), 2);
		assert_eq!(facts.indexes[0].index.index, Strand::from("ix"));
		assert!(facts.hint.is_none());
		assert!(!facts.current_snapshot);
		let hint = With::Index(vec!["iy".to_owned()]);
		let facts = table_facts_of(&table_answers, Some(&hint), true).expect("an entry");
		let [only] = facts.indexes.as_slice() else {
			panic!("the hinted index alone, got {:?}", facts.indexes);
		};
		assert_eq!(only.index.index, Strand::from("iy"));
		assert_eq!(only.index.position, 1);
	}

	#[test]
	fn an_unresolved_context_has_no_entry() {
		let mut table_answers = answers(vec![idx(1, "ix", &["x"])]);
		table_answers.restricted = RestrictedSelect::Unresolved;
		assert!(table_facts_of(&table_answers, None, true).is_none());
	}

	#[test]
	fn a_restricted_field_withholds_its_order() {
		let mut table_answers = answers(vec![idx(1, "ix", &["x"]), idx(2, "iy", &["y"])]);
		let open = table_facts_of(&table_answers, None, true).expect("an entry");
		assert_eq!(open.withheld_order, FieldSet::Some(SmallVec::new()));
		table_answers.restricted = restricted("x.inner");
		let table_answers =
			table_answers.with_indexes(vec![idx(1, "ix", &["x"]), idx(2, "iy", &["y"])]);
		let facts = table_facts_of(&table_answers, None, true).expect("an entry");
		assert_eq!(facts.withheld_order, FieldSet::Some(smallvec![Strand::from("x")]));
		// The restricted column's index is not a candidate.
		assert_eq!(facts.indexes.len(), 1);
		assert_eq!(facts.indexes[0].index.index, Strand::from("iy"));
	}

	#[test]
	fn estimate_is_absent() {
		let facts =
			table_facts_of(&answers(vec![idx(1, "ix", &["x"])]), None, true).expect("an entry");
		assert!(facts.estimate.is_none());
	}

	#[test]
	fn a_write_consumer_is_scanned_for_nested_statements() {
		use crate::exec::ir::core::location::{LocOrigin, Location};
		use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
		use crate::exec::ir::iface::ids::{FrameId, SnapshotId};
		use crate::exec::ir::iface::region_kind::ClauseRole;
		use crate::exec::ir::iface::statements::{OutputShape, WriteFlags, WriteForm};
		use crate::exec::ir::iface::types::TypeKnowledge;
		use crate::exec::ir::logical::ops::{
			WriteAttrs, WriteBody, WriteDerived, WriteGates, WriteImage, WriteStored,
		};
		use crate::exec::ir::phys::ops::{
			Batch, PhysWriteAttrs, PipelineAttrs, PipelineId, WriteSubjectsShape,
		};

		let mut ir = Ir::new(
			IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		);
		let mut facts = FactCache::new();
		let loc = Location {
			stmt: 0,
			span: None,
			origin: LocOrigin::Synthesized,
		};
		let top = ir.region(ir.top()).expect("top").blocks[0];
		let mut rw = Rewriter::new(&mut ir, &mut facts);
		let scan = rw
			.create(
				InsertPoint::End(top),
				OpSpec {
					kind: OpKind::Phys(table_scan(false)),
					operands: SmallVec::new(),
					results: smallvec![TypeKnowledge::Unknown],
					frame: FrameId::new(0),
					snapshot: SnapshotId::new(0),
					loc: loc.clone(),
				},
			)
			.expect("scan");
		let scan_rows = rw.ir().op(scan).expect("scan").results[0];
		let pipeline = rw
			.create(
				InsertPoint::End(top),
				OpSpec {
					kind: OpKind::Phys(PhysOp::Pipeline(Box::new(PipelineAttrs {
						id: PipelineId(0),
						edges: SmallVec::new(),
						logical: None,
					}))),
					operands: SmallVec::new(),
					results: smallvec![TypeKnowledge::Unknown],
					frame: FrameId::new(0),
					snapshot: SnapshotId::new(0),
					loc: loc.clone(),
				},
			)
			.expect("pipeline");
		let (_, pipe_block) =
			rw.create_region(pipeline, RegionKind::Pipeline, &[]).expect("pipe region");
		let loop_op = rw
			.create(
				InsertPoint::End(pipe_block),
				OpSpec {
					kind: OpKind::Phys(PhysOp::Loop {
						kind: LoopKind::Write,
						first: false,
						batch: Batch {
							max_iterations: 1,
							max_bytes: 1,
						},
						unroll: 1,
						value: false,
						step: None,
					}),
					operands: smallvec![scan_rows],
					results: smallvec![TypeKnowledge::Unknown],
					frame: FrameId::new(0),
					snapshot: SnapshotId::new(0),
					loc: loc.clone(),
				},
			)
			.expect("loop");
		rw.create_region(loop_op, RegionKind::Loop, &[TypeKnowledge::Unknown]).expect("loop body");
		let loop_out = rw.ir().op(loop_op).expect("loop").results[0];
		rw.create(
			InsertPoint::End(pipe_block),
			OpSpec {
				kind: OpKind::Phys(PhysOp::Yield),
				operands: smallvec![loop_out],
				results: SmallVec::new(),
				frame: FrameId::new(0),
				snapshot: SnapshotId::new(0),
				loc: loc.clone(),
			},
		)
		.expect("yield");
		let pipe_out = rw.ir().op(pipeline).expect("pipeline").results[0];
		let write_op = rw
			.create(
				InsertPoint::End(top),
				OpSpec {
					kind: OpKind::Phys(PhysOp::Write(Box::new(PhysWriteAttrs {
						write: WriteAttrs {
							form: WriteForm::Update,
							only: false,
							output: OutputShape::Fields,
							with: None,
							explain: None,
							flags: WriteFlags::default(),
							body: WriteBody::default(),
							gates: WriteGates::default(),
							image: WriteImage::default(),
							stored: WriteStored::default(),
							derived: WriteDerived::default(),
							into_text: None,
							statement: None,
							linked: None,
						},
						subjects: WriteSubjectsShape::Rows,
					}))),
					operands: smallvec![pipe_out],
					results: smallvec![TypeKnowledge::Unknown],
					frame: FrameId::new(0),
					snapshot: SnapshotId::new(0),
					loc: loc.clone(),
				},
			)
			.expect("write");
		let (_, ret_block) = rw
			.create_region(write_op, RegionKind::Clause(ClauseRole::ReturnClause), &[])
			.expect("return region");
		let subquery = rw
			.create(
				InsertPoint::End(ret_block),
				OpSpec {
					kind: OpKind::Expr(ExprOp::Subquery(NestedStrategy::Apply)),
					operands: SmallVec::new(),
					results: smallvec![TypeKnowledge::Unknown],
					frame: FrameId::new(0),
					snapshot: SnapshotId::new(0),
					loc,
				},
			)
			.expect("subquery");
		rw.create_region(subquery, RegionKind::Subquery, &[]).expect("subquery region");

		let registry = crate::exec::ir::analysis::cx::builtin_registry();
		let got = facts_reading(&ir, &mut facts, registry, pipeline, NestedReading::Strategy)
			.expect("pipeline facts");
		assert_eq!(got.sink, SinkKind::Write);
		assert!(got.writes);
		assert_eq!(got.nested, NestedUse::PerRowReadOnly);
	}
}
