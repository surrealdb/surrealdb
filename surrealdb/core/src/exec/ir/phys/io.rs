//! How a pipeline reads storage beyond what its ops state: the windows its links are read in.
//!
//! A pipeline reads the records its `FETCH` and its dereferences reach one at a time, unless the
//! plan says a run of them may be read at once. [`run`] states that decision for every placed
//! pipeline as its [`PipelineIo`], held beside the pipeline's placement, from the container the
//! physical rewrites left. It changes no op and prints nowhere, and the VM reads no capability
//! and no version to decide a window: it copies what this states.
//!
//! - `deref_windows`: the pipeline's `.*` and field-over-array dereferences read their links in
//!   windows. The datastore's plain reads are untracked, so a window reads no key the run would not
//!   conflict on, and the pipeline reads at the current version. Between two positions of a window
//!   only the position's own fetch runs, whose writes are refused, so no footprint is asked. A
//!   value source (`FROM $ids`, `FROM [t:1, t:2]`) reads the records its array names the same way,
//!   every one of them before the first row runs; the subjects of a write are not read through it.
//! - `fetch_windows`: the same for each collection of links a `FETCH` path reaches. A computed
//!   index step of a path runs host code between positions, so the `FETCH` must write nothing.
//!
//! - `rows_ahead`: a `FETCH` of top-level link fields, or the dereferences `link.field` of a
//!   projection, over a scan reads the links of a batch of source rows before the rows run. A
//!   serial placement reads them for the batches the scan yields, a morsel placement for each
//!   morsel's batch, in a window its state owns. The ops between the scan and the reader pass each
//!   row they take at most once and in order (a `FETCH` after the row itself, a dereferencing
//!   projection after nothing but its paging clause), the fields it keys are stored and unfiltered,
//!   and the statement writes nothing, so no key is read before a write the run would have made
//!   first.
//!
//!   The keys a morsel batch reads are a subset of the reads its body makes: the window of a row
//!   is filled only for rows past the paging clause's skip, which the body runs as though they
//!   were taken, in order, until a row fails before the paging, and a window is filled before its
//!   first row runs, so a stopping failure leaves at most one window of keys past the failing row
//!   read. The window holds bytes only and swallows failures: a failed multi-get leaves every key
//!   of the window absent, and a position the window lacks reads for itself, in its own frame,
//!   where its permissions, computed fields, decoding and errors are those of an unwindowed read.
//!   A dereferencing projection that pages nothing is not placed as batches of rows: it reads
//!   through the window its scan fills for each batch it reads.
//!
//! - `scan_read_ahead`: the scan of a pipeline whose body runs inside the storage cursor reads its
//!   next chunk on a task of its own while the body runs the current one. The datastore says a
//!   chunk read is worth overlapping and that its plain reads are untracked, so the keys the task
//!   reads beyond the ones the run reaches widen no conflict. The pipeline reads its whole range
//!   (no paging, no `ONLY`, no ordered exit, no top-k cut), so on a run that ends without an error
//!   the task reads the keys the serial run reads. The statement writes nothing, so no key is read
//!   ahead of a write the serial run would have made first, and the scan runs once per statement,
//!   never once per row of an enclosing loop or subquery.
//!
//! Every rule is conservative: a pipeline whose shape, version or footprint a rule cannot
//! establish has no window. A window reads at the current version, so a pipeline gets one only
//! where the plan establishes the version it runs at: a statement of a request, in code that runs
//! in the request's own frame, whose ops state their version themselves. A unit compiled on its
//! own, or code a caller's frame runs, may run under a caller's `VERSION` that no op of its plan
//! names.

#![deny(clippy::wildcard_enum_match_arm)]

use smallvec::SmallVec;
use surrealdb_strand::Strand;

use super::ops::{PhysOp, ProjectionShape};
use super::pipeline::{enclosing_statement, streams};
#[cfg(test)]
use super::placement_attr::PlacementAttr;
use super::placement_attr::{PipelineIo, RowsAhead};
use super::{frontier, nested};
use crate::catalog::Permission;
use crate::exec::ir::analysis::effects;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::catalog::{CatalogAnswer, EffectCx, IoCaps, PhysCx};
use crate::exec::ir::iface::frame::Snapshot;
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::{ErrorScope, RegionKind};
use crate::exec::ir::logical::ops::LoopKind;
use crate::exec::ir::op_kind::OpKind;
#[cfg(test)]
use crate::exec::planner::placement::{Combine, Decision, MorselInput, SinkKind};
use crate::expr::logical::EffectSet;
use crate::expr::part::Part;
use crate::val::TableName;

#[cfg(test)]
thread_local! {
	/// Pins the windows of dereferences and `FETCH` on (`Some(true)`, whatever the datastore
	/// offers) or off, for tests that compare the two runs.
	pub(crate) static FORCE_LINK_WINDOWS: std::cell::Cell<Option<bool>> =
		const { std::cell::Cell::new(None) };
	/// Pins the rows-ahead decision off (`Some(false)`), for tests that compare the runs. Pinned on
	/// it is the decision the plan makes.
	pub(crate) static FORCE_ROWS_AHEAD: std::cell::Cell<Option<bool>> =
		const { std::cell::Cell::new(None) };
	/// Pins the scan read-ahead on (`Some(true)`, whatever the datastore offers) or off, for tests
	/// that compare the two runs.
	pub(crate) static FORCE_SCAN_READ_AHEAD: std::cell::Cell<Option<bool>> =
		const { std::cell::Cell::new(None) };
	/// Places every pipeline that scans its whole range as range morsels that concatenate,
	/// whether or not it reads ahead, and whose rows end in the statement's output, so that a test
	/// of the coordinator's front run reaches it whatever the policy places.
	pub(crate) static FORCE_RANGE_MORSELS: std::cell::Cell<bool> =
		const { std::cell::Cell::new(false) };
	/// Places every pipeline as range morsels before its io is decided, for tests of the rules
	/// that depend on the placement and of the runs that read through one.
	pub(crate) static FORCE_MORSEL_PLACEMENT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Places the pipeline of `attr` as range morsels that concatenate, replaying its one paging
/// clause where it has one. The facts the decision is judged by record no write, as a pipeline of
/// a statement that writes nothing states them.
#[cfg(test)]
fn place_as_range_morsels(attr: &mut PlacementAttr) {
	let mut paged = false;
	if let Some(mut facts) = attr.facts().cloned() {
		facts.writes = false;
		paged = facts.limits == 1;
		attr.set_facts(Some(facts));
	}
	attr.set_decision(Some(Decision::Morsels {
		input: MorselInput::Range,
		combine: Combine::Concat,
		paged,
		prunes: false,
	}));
}

/// States the [`PipelineIo`] of every pipeline of `ir`.
pub(crate) fn run(ir: &mut Ir, facts: &mut FactCache, cx: &PhysCx<'_>) -> Result<(), IrError> {
	let untracked = untracked_reads(cx) && cx.physical.at_request_root();
	let scans = scan_read_ahead_offered(cx) && cx.physical.at_request_root();
	// The placed pipelines are the table's keys, so no walk of the container's ops is needed to
	// find them.
	let placed: SmallVec<[OpId; 8]> = ir.placement_table_mut().pipelines().collect();
	let mut decided: SmallVec<[(OpId, PipelineIo); 8]> = SmallVec::new();
	let mut judged: SmallVec<[(OpId, bool); 4]> = SmallVec::new();
	#[cfg(test)]
	let mut morsels: SmallVec<[OpId; 4]> = SmallVec::new();
	for pipeline in placed {
		#[cfg(test)]
		if FORCE_MORSEL_PLACEMENT.with(std::cell::Cell::get) {
			let table = ir.placement_table_mut();
			if let Some(mut attr) = table.get(pipeline).cloned() {
				place_as_range_morsels(&mut attr);
				table.insert(pipeline, attr);
			}
		}
		let mut io = io_of(ir, facts, cx, pipeline, untracked);
		#[cfg(test)]
		let whole_range = FORCE_RANGE_MORSELS.with(std::cell::Cell::get);
		#[cfg(not(test))]
		let whole_range = false;
		let reads_whole =
			(scans || whole_range) && reads_ahead(ir, facts, cx, pipeline, &mut judged);
		io.scan_read_ahead = scans && reads_whole;
		#[cfg(test)]
		if whole_range
			&& reads_whole
			&& ir.placement(pipeline).and_then(|attr| attr.facts()).is_some_and(|facts| {
				facts.sink == SinkKind::Output && facts.limits == 0 && !facts.enforce
			}) {
			morsels.push(pipeline);
		}
		if io != PipelineIo::default() {
			decided.push((pipeline, io));
		}
	}
	for (pipeline, io) in decided {
		ir.placement_table_mut().set_io(pipeline, io);
	}
	#[cfg(test)]
	for pipeline in morsels {
		let table = ir.placement_table_mut();
		if let Some(mut attr) = table.get(pipeline).cloned() {
			// `reads_ahead` established that the statement writes nothing.
			place_as_range_morsels(&mut attr);
			table.insert(pipeline, attr);
		}
	}
	Ok(())
}

/// Whether the datastore offers the read-ahead of a scan, and leaves the conflict set alone for
/// the keys a producer reads beyond the ones the run reaches.
fn scan_read_ahead_offered(cx: &PhysCx<'_>) -> bool {
	#[cfg(test)]
	if let Some(forced) = FORCE_SCAN_READ_AHEAD.with(std::cell::Cell::get) {
		return forced;
	}
	offers_read_ahead(cx.physical.io_caps())
}

/// Whether `caps` offer the read-ahead of a scan with no conflict from the keys it reads beyond
/// the ones the run reaches.
fn offers_read_ahead(caps: IoCaps) -> bool {
	caps.untracked_reads && caps.scan_read_ahead
}

/// Whether a plain read leaves the transaction's conflict set alone.
fn untracked_reads(cx: &PhysCx<'_>) -> bool {
	#[cfg(test)]
	if let Some(forced) = FORCE_LINK_WINDOWS.with(std::cell::Cell::get) {
		return forced;
	}
	cx.physical.io_caps().untracked_reads
}

/// The decision for the pipeline `pipeline`.
fn io_of(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &PhysCx<'_>,
	pipeline: OpId,
	untracked: bool,
) -> PipelineIo {
	if !untracked || !runs_in_request_frame(ir, pipeline) || !reads_current(ir, pipeline) {
		return PipelineIo::default();
	}
	let mut io = PipelineIo {
		deref_windows: true,
		frontier_hops: frontier::served_hops(ir, cx, pipeline),
		..PipelineIo::default()
	};
	let Some(rows) = rows_of(ir, pipeline) else {
		return io;
	};
	io.frontier_rows = frontier::rows(ir, cx, pipeline, &rows);
	let mut fetches =
		rows.passes.iter().copied().filter(|op| matches!(kind_of(ir, *op), Some(PhysOp::Fetch)));
	let Some(first) = fetches.next() else {
		io.rows_ahead = deref_ahead(ir, cx, &rows);
		return io;
	};
	let second = fetches.next();
	io.fetch_windows = std::iter::once(first)
		.chain(second)
		.chain(fetches)
		.all(|fetch| reads_current(ir, fetch) && only_reads(ir, facts, &cx.effects, fetch));
	if io.fetch_windows && second.is_none() {
		io.rows_ahead = rows_ahead(ir, cx, pipeline, &rows, first);
	}
	io
}

/// Whether the scan of the pipeline `pipeline` may read its next chunk on a task of its own.
///
/// The rules of the module hold: the scan is a table scan that stores records (not a lightweight
/// relation) or a record range, with no top-k key to cut it; every op the rows pass takes each
/// row and none pages, ends the pipeline or ends an iteration early; no `ONLY` consumes the rows;
/// the scan reads at the current version; the pipeline stands directly in its statement, which
/// writes nothing, as [`nested::statement_writes`] states it.
fn reads_ahead(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &PhysCx<'_>,
	pipeline: OpId,
	judged: &mut SmallVec<[(OpId, bool); 4]>,
) -> bool {
	let Some(rows) = rows_of(ir, pipeline) else {
		return false;
	};
	let whole = match kind_of(ir, rows.source) {
		Some(PhysOp::TableScan(attrs)) => !attrs.lightweight && attrs.topk.is_none(),
		Some(PhysOp::KeyRangeScan(attrs)) => attrs.topk.is_none(),
		_ => false,
	};
	if !whole || !reads_current(ir, pipeline) || !reads_current(ir, rows.source) {
		return false;
	}
	let ends_early = |op: OpId| {
		matches!(
			kind_of(ir, op),
			Some(
				PhysOp::Limit { .. } | PhysOp::Only | PhysOp::Break | PhysOp::Next | PhysOp::Return
			)
		)
	};
	if rows.passes.iter().any(|op| ends_early(*op) || holds(ir, *op, &ends_early)) {
		return false;
	}
	let Some(statement) = enclosing_statement(ir, pipeline) else {
		return false;
	};
	if !stands_in_statement(ir, pipeline, &ends_early) {
		return false;
	}
	// A statement is asked once, however many of its pipelines read ahead.
	if let Some((_, writes)) = judged.iter().find(|(op, _)| *op == statement) {
		return !writes;
	}
	let writes = nested::statement_writes(ir, facts, &cx.effects, statement);
	judged.push((statement, writes));
	!writes
}

/// Whether an op in a region of `op` satisfies `wanted`.
fn holds(ir: &Ir, op: OpId, wanted: &impl Fn(OpId) -> bool) -> bool {
	let Some(data) = ir.op(op) else {
		return false;
	};
	let mut found = false;
	for region in &data.regions {
		ir.walk(*region, WalkOrder::PreOrder, &mut |inner| {
			if wanted(inner) {
				found = true;
				Walk::Stop
			} else {
				Walk::Continue
			}
		});
		if found {
			break;
		}
	}
	found
}

/// Whether the pipeline `pipeline` runs once for its statement and reads its rows to the end:
/// every op around it, up to the statement of the request that holds it, is that statement, a
/// statement of a block, a block, a pipeline or the statement's timeout, and no op of its own
/// statement's regions ends the rows early (`ends_early`). A loop, a subquery, a clause or a
/// branch stands around a scan that runs once for each of its rows or as often as it is chosen.
///
/// The ops of the statement's own regions are those up to the first statement or block around
/// the pipeline: the `ONLY` that takes the pipeline's rows stands beside it, or beside the timeout
/// that holds it. The other statements of a block run before or after the scan, never while it
/// reads.
fn stands_in_statement(ir: &Ir, pipeline: OpId, ends_early: &impl Fn(OpId) -> bool) -> bool {
	let mut region = ir.enclosing_region(pipeline);
	let mut own = true;
	while let Some(parent) = ir.parent_op(region) {
		if own && ops_in(ir, region).any(ends_early) {
			return false;
		}
		match kind_of(ir, parent) {
			Some(PhysOp::Stmt {
				..
			}) => return true,
			Some(
				PhysOp::Statement {
					..
				}
				| PhysOp::Block,
			) => {
				own = false;
				region = ir.enclosing_region(parent);
			}
			Some(PhysOp::Pipeline(..) | PhysOp::Timeout) => {
				region = ir.enclosing_region(parent);
			}
			_ => return false,
		}
	}
	false
}

/// Whether the paths of the `FETCH` `fetch` run no code that writes or that the plan cannot see.
///
/// The steps of a path (fields, `.*`, first, last, and the index step itself) only name where the
/// walk goes: the records they reach are read by the walk, whose reads of a record run in a
/// frame that refuses a write. A computed index is the other kind of op in a path's region: host
/// code the walk runs between two positions of a window, where a write could land between a
/// record's read and its use. The fetch op itself states no effects, so its paths are read.
fn only_reads(ir: &Ir, facts: &mut FactCache, cx: &EffectCx<'_>, fetch: OpId) -> bool {
	let Some(data) = ir.op(fetch) else {
		return false;
	};
	data.regions.iter().flat_map(|region| ops_in(ir, *region)).all(|op| {
		let is_step = matches!(
			ir.op(op).map(|data| &data.kind),
			Some(OpKind::Expr(
				ExprOp::Field { .. }
					| ExprOp::ComputedRead(..)
					| ExprOp::Access { .. }
					| ExprOp::Each | ExprOp::First
					| ExprOp::Last | ExprOp::Index
					| ExprOp::Yield
			))
		);
		is_step || {
			let set = effects::summary_with(ir, facts, cx, op).set;
			!set.mutates_state() && !set.intersects(EffectSet::OPAQUE_BODY)
		}
	})
}

/// Whether the pipeline `pipeline` runs in the frame of the request it is planned in, so that
/// the version its ops state is the version it runs at.
///
/// Code a caller's frame runs (a function or closure body, a permission or field clause, an
/// event, a stage of a write, a unit of the catalog, a subscriber's frame, code stored and never
/// run here) reads at whatever `VERSION` that frame holds, which no op of the plan names. So does
/// a container whose root is not a request region, whatever planner planned it.
fn runs_in_request_frame(ir: &Ir, pipeline: OpId) -> bool {
	let mut region = ir.enclosing_region(pipeline);
	loop {
		let Some(data) = ir.region(region) else {
			return false;
		};
		let admitted = match data.kind {
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
			| RegionKind::Clause(_)
			| RegionKind::LazyOperand
			| RegionKind::ChooseArm
			| RegionKind::Pipeline
			| RegionKind::Deadline => true,
			RegionKind::ErrorScope(scope) => match scope {
				ErrorScope::ShapeToNone | ErrorScope::MethodBody => true,
				ErrorScope::PermissionCheck => false,
			},
			RegionKind::FunctionBody
			| RegionKind::ClosureBody
			| RegionKind::PermissionFrame
			| RegionKind::FieldFrame(_)
			| RegionKind::EventFrame
			| RegionKind::SubscriberFrame
			| RegionKind::WriteStage
			| RegionKind::CatalogUnit
			| RegionKind::StoredClause(_) => false,
			// A join's lookup arm runs once per left row and may not run at all, so no window
			// rule judges the reads of a pipeline inside it.
			RegionKind::JoinLookupArm => false,
		};
		if !admitted {
			return false;
		}
		match ir.parent_op(region) {
			Some(parent) => region = ir.enclosing_region(parent),
			// Only a request's own container is rooted at a request region. A unit planned apart
			// (a view's recalculation, a cascade) is rooted at the region of the frame it runs in,
			// whichever planner planned it.
			None => return matches!(data.kind, RegionKind::Request),
		}
	}
}

/// Whether the op `op` reads at the current version.
fn reads_current(ir: &Ir, op: OpId) -> bool {
	ir.op(op)
		.is_some_and(|data| matches!(ir.snapshot(data.snapshot), Some(Snapshot::Current) | None))
}

pub(super) fn kind_of(ir: &Ir, op: OpId) -> Option<&PhysOp> {
	match &ir.op(op)?.kind {
		OpKind::Phys(kind) => Some(kind),
		OpKind::Expr(_) | OpKind::Logical(_) => None,
	}
}

/// The ops a pipeline's body runs for each row the source yields, and the source.
pub(super) struct Rows {
	/// The op the rows come from.
	pub(super) source: OpId,
	/// The pipeline itself and every pipeline it continues: each runs some of the ops in
	/// [`Self::passes`].
	pipelines: SmallVec<[OpId; 2]>,
	/// The ops between the source and the end of the pipeline's body, in the order the rows meet
	/// them: the pipelines it continues first, then the streaming ops between them, the body of
	/// each select loop and the ops that follow the loop.
	pub(super) passes: Ops,
}

/// The ops of a pipeline's body, held inline for the bodies of the usual size.
pub(super) type Ops = SmallVec<[OpId; 16]>;

/// The rows of the pipeline `pipeline`, or `None` for a pipeline whose region is not one of the
/// shapes lowering reads.
fn rows_of(ir: &Ir, pipeline: OpId) -> Option<Rows> {
	let mut passes = Ops::new();
	let mut pipelines = SmallVec::new();
	let source = walk(ir, pipeline, &mut passes, &mut pipelines)?;
	Some(Rows {
		source,
		pipelines,
		passes,
	})
}

/// Appends the ops the rows of the pipeline `pipeline` pass to `out`, earliest first, and the
/// pipelines that run them to `pipelines`, and answers the op they come from.
fn walk(
	ir: &Ir,
	pipeline: OpId,
	out: &mut Ops,
	pipelines: &mut SmallVec<[OpId; 2]>,
) -> Option<OpId> {
	pipelines.push(pipeline);
	let region = *ir.op(pipeline)?.regions.first()?;
	let mut chain: Ops = ops_in(ir, region).collect();
	// The region ends in the yield of the rows it delivers.
	chain.pop().filter(|last| matches!(kind_of(ir, *last), Some(PhysOp::Yield)))?;
	let head = *chain.first()?;
	let source = if let Some(PhysOp::Loop {
		kind: LoopKind::Select,
		..
	}) = kind_of(ir, head)
	{
		let data = ir.op(head)?;
		let (input, before) = before_of(ir, *data.operands.first()?)?;
		let source = if matches!(kind_of(ir, input), Some(PhysOp::Pipeline(_))) {
			walk(ir, input, out, pipelines)?
		} else {
			input
		};
		out.extend(before);
		let body = *data.regions.first()?;
		let mut body_ops: Ops = ops_in(ir, body).collect();
		body_ops.pop().filter(|last| matches!(kind_of(ir, *last), Some(PhysOp::Yield)))?;
		out.extend(body_ops);
		source
	} else {
		head
	};
	out.extend(chain.into_iter().skip(1));
	Some(source)
}

/// The op that produces the rows `rows` and the streaming ops that stand between it and the
/// reader, earliest first. The walk stops at the first op that does not stream.
fn before_of(ir: &Ir, rows: ValueId) -> Option<(OpId, Ops)> {
	let mut before = Ops::new();
	let mut at = producer(ir, rows)?;
	loop {
		let data = ir.op(at)?;
		match (&data.kind, data.operands.first()) {
			(OpKind::Phys(kind), Some(input)) if streams(kind) => {
				before.push(at);
				at = producer(ir, *input)?;
			}
			_ => {
				before.reverse();
				return Some((at, before));
			}
		}
	}
}

/// The op that defines `value`, when `value` is an op's result.
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

/// The table `rows` scan, where every pipeline the rows pass may read the links of a batch ahead:
/// a decided placement is all the rule asks, since a serial one reads ahead the rows it runs one
/// after another and a morsel one the rows of each batch it runs, in a window the batch's state
/// owns. The statement writes nothing and runs nothing the plan cannot see, so a key is not read
/// before a write the serial run would have made first. The scan decodes its rows for the body,
/// which a `FETCH` or a dereference makes a host step.
fn scanned_ahead<'a>(ir: &'a Ir, rows: &Rows) -> Option<&'a TableName> {
	for pipeline in &rows.pipelines {
		let placement = ir.placement(*pipeline)?;
		placement.decision()?;
		if placement.statement().refusal.is_some() {
			return None;
		}
	}
	let source = kind_of(ir, rows.source)?;
	if let PhysOp::TableScan(attrs) = source
		&& !attrs.lightweight
	{
		Some(&attrs.table)
	} else if let PhysOp::KeyRangeScan(attrs) = source {
		Some(&attrs.table)
	} else {
		None
	}
}

/// The links of `fetch`, the one `FETCH` of `pipeline`, that its rows' source can be asked for
/// ahead of the rows, or `None` where a rule of the module does not hold.
fn rows_ahead(
	ir: &Ir,
	cx: &PhysCx<'_>,
	pipeline: OpId,
	rows: &Rows,
	fetch: OpId,
) -> Option<RowsAhead> {
	#[cfg(test)]
	if FORCE_ROWS_AHEAD.with(std::cell::Cell::get) == Some(false) {
		return None;
	}
	debug_assert_eq!(rows.pipelines.first(), Some(&pipeline));
	let table = scanned_ahead(ir, rows)?;
	// Every op the rows pass before the fetch takes each row at most once, in order: one paging
	// clause, and the projection that is the row itself. A paging clause after it would bound
	// the rows that reach it, not the keys.
	let at = rows.passes.iter().position(|op| *op == fetch)?;
	let mut limits = 0;
	for op in &rows.passes[..at] {
		let op = kind_of(ir, *op)?;
		if matches!(op, PhysOp::Limit { .. }) {
			limits += 1;
		} else if !matches!(op, PhysOp::Project(shape) if matches!(**shape, ProjectionShape::All)) {
			return None;
		}
	}
	if limits > 1 {
		return None;
	}
	if rows.passes[at + 1..].iter().any(|op| matches!(kind_of(ir, *op), Some(PhysOp::Limit { .. })))
	{
		return None;
	}
	let fields = link_fields(ir, cx, table, fetch)?;
	Some(RowsAhead {
		fields,
		derefs: false,
		// The one clause that pages before the fetch is the first the body holds, and no clause
		// follows the fetch.
		paging: (limits == 1).then_some(0),
	})
}

/// The links of the dereferences `link.field` of the projection of `pipeline`, that its rows'
/// source can be asked for ahead of the rows, or `None` where a rule of the module does not hold.
///
/// The ops the rows pass take each row at most once and in order: one paging clause before a
/// single projection that is the last op to drop nothing, and no `FETCH`, so every row the batch
/// holds past the clause reaches the projection. A link is a stored field the catalog answers
/// the table's definitions leave as stored, read off the row by a region that does nothing else
/// before it reads the field of the record: no branch skips the read and no earlier step of the
/// region can fail.
fn deref_ahead(ir: &Ir, cx: &PhysCx<'_>, rows: &Rows) -> Option<RowsAhead> {
	#[cfg(test)]
	if FORCE_ROWS_AHEAD.with(std::cell::Cell::get) == Some(false) {
		return None;
	}
	let table = scanned_ahead(ir, rows)?;
	let mut limits = 0;
	let mut projection = None;
	for op in &rows.passes {
		let kind = kind_of(ir, *op)?;
		let whole =
			matches!(kind, PhysOp::Project(shape) if matches!(**shape, ProjectionShape::All));
		if matches!(kind, PhysOp::Limit { .. }) && projection.is_none() {
			limits += 1;
		} else if matches!(kind, PhysOp::Project(_)) && !whole && projection.is_none() {
			projection = Some(*op);
		} else if !whole {
			return None;
		}
	}
	if limits > 1 {
		return None;
	}
	let data = ir.op(projection?)?;
	let mut fields: SmallVec<[Strand; 2]> = SmallVec::new();
	for region in &data.regions {
		if let Some(name) = dereferenced(ir, *region)
			&& !fields.contains(&name)
		{
			fields.push(name);
		}
	}
	if fields.is_empty() || !stored_links(cx, table, &fields) {
		return None;
	}
	Some(RowsAhead {
		fields,
		derefs: true,
		paging: (limits == 1).then_some(0),
	})
}

/// The top-level field of the row that the projection region `region` dereferences: `link.name`
/// read off the row, whose steps after the field are field reads and nothing else.
fn dereferenced(ir: &Ir, region: RegionId) -> Option<Strand> {
	let row = *ir.block(*ir.region(region)?.blocks.first()?)?.args.first()?;
	let ops: Vec<OpId> = ops_in(ir, region).collect();
	let (last, steps) = ops.split_last()?;
	let yielded = ir.op(*last)?;
	if !matches!(yielded.kind, OpKind::Expr(ExprOp::Yield)) {
		return None;
	}
	let (first, rest) = steps.split_first()?;
	let first = ir.op(*first)?;
	let link = match (&first.kind, first.operands.as_slice()) {
		(
			OpKind::Expr(ExprOp::Field {
				name,
			}),
			[from],
		) if *from == row => name.clone(),
		_ => return None,
	};
	// Every later step reads the field of what the step before it produced, and the last is
	// what the region yields.
	let mut produced = first.results.as_slice();
	for step in rest {
		let step = ir.op(*step)?;
		if !matches!(step.kind, OpKind::Expr(ExprOp::Access { .. }))
			|| step.operands.as_slice() != produced
		{
			return None;
		}
		produced = step.results.as_slice();
	}
	(!rest.is_empty() && yielded.operands.as_slice() == produced).then_some(link)
}

/// The top-level fields of `table` that the paths of `fetch` read the links of, where every path
/// is such a field or its elements, and the catalog answers that the table's definitions leave
/// those fields as they are stored: none is `COMPUTED` and none has a field permission.
///
/// A field permission is refused whatever the frame, though a frame that waives permissions would
/// read the field as stored: the plan does not hold that fact where it states this.
fn link_fields(
	ir: &Ir,
	cx: &PhysCx<'_>,
	table: &TableName,
	fetch: OpId,
) -> Option<SmallVec<[Strand; 2]>> {
	let data = ir.op(fetch)?;
	let mut fields: SmallVec<[Strand; 2]> = SmallVec::new();
	for region in &data.regions {
		let name = path_of(ir, *region)?;
		if !fields.contains(&name) {
			fields.push(name);
		}
	}
	if fields.is_empty() || !stored_links(cx, table, &fields) {
		return None;
	}
	Some(fields)
}

/// Whether the catalog answers that the definitions of `table` leave each of `fields` as it is
/// stored: none is `COMPUTED` and none has a field permission.
fn stored_links(cx: &PhysCx<'_>, table: &TableName, fields: &[Strand]) -> bool {
	let snapshot = match cx.effects.catalog.table(table) {
		CatalogAnswer::Found(snapshot) => snapshot,
		CatalogAnswer::Absent | CatalogAnswer::NotLoaded | CatalogAnswer::Unknown => return false,
	};
	snapshot.fields.iter().all(|field| {
		let Some(Part::Field(first)) = field.name.0.first() else {
			return true;
		};
		!fields.contains(first)
			|| (field.computed.is_none() && matches!(field.select_permission, Permission::Full))
	})
}

/// The field a `FETCH` path reads, for the paths `f` and `f.*` read off the row the region is
/// evaluated against, and `None` for every other.
fn path_of(ir: &Ir, region: RegionId) -> Option<Strand> {
	let row = *ir.block(*ir.region(region)?.blocks.first()?)?.args.first()?;
	let ops: Vec<OpId> = ops_in(ir, region).collect();
	let (last, steps) = ops.split_last()?;
	let yielded = ir.op(*last)?;
	if !matches!(yielded.kind, OpKind::Expr(ExprOp::Yield)) {
		return None;
	}
	let field_of = |op: OpId| -> Option<Strand> {
		let data = ir.op(op)?;
		match (&data.kind, data.operands.as_slice()) {
			(
				OpKind::Expr(ExprOp::Field {
					name,
				}),
				[from],
			) if *from == row => Some(name.clone()),
			_ => None,
		}
	};
	match (steps, yielded.operands.as_slice()) {
		([field], [value]) if ir.op(*field)?.results.first() == Some(value) => field_of(*field),
		([field, each], [value]) if ir.op(*each)?.results.first() == Some(value) => {
			let each_data = ir.op(*each)?;
			let reads_field = each_data.operands.as_slice() == ir.op(*field)?.results.as_slice();
			if matches!(each_data.kind, OpKind::Expr(ExprOp::Each)) && reads_field {
				field_of(*field)
			} else {
				None
			}
		}
		_ => None,
	}
}

#[cfg(all(test, feature = "kv-mem"))]
mod tests {
	use super::*;
	use crate::dbs::Session;
	use crate::exec::ir::phys::pipeline::PipelineDag;
	use crate::exec::ir::phys::placement_attr::FrontierRows;
	use crate::exec::ir::tests_plan_request::{
		planned_as_unit, planned_request, planned_request_as, planned_unit,
	};
	use crate::exec::ir::unit::UnitKind;
	use crate::exec::operators::test_util::TestDb;

	const SETUP: &str = "DEFINE TABLE person SCHEMALESS; DEFINE TABLE holder SCHEMALESS;";

	/// The decision of each pipeline `sql` plans to, with the windows pinned to `forced`.
	async fn ios(setup: &str, sql: &str, forced: Option<bool>) -> Vec<PipelineIo> {
		FORCE_LINK_WINDOWS.with(|pinned| pinned.set(forced));
		let planned = planned_request(setup, sql).await;
		FORCE_LINK_WINDOWS.with(|pinned| pinned.set(None));
		PipelineDag::of(&planned.ir)
			.nodes
			.iter()
			.map(|(_, pipeline)| {
				planned.ir.placement(*pipeline).expect("a placed pipeline").io().clone()
			})
			.collect()
	}

	/// The decision of each pipeline `sql` plans to as a unit compiled on its own, with the windows
	/// pinned on.
	async fn ios_of_unit(setup: &str, sql: &str) -> Vec<PipelineIo> {
		FORCE_LINK_WINDOWS.with(|pinned| pinned.set(Some(true)));
		let planned = planned_unit(setup, sql).await;
		FORCE_LINK_WINDOWS.with(|pinned| pinned.set(None));
		PipelineDag::of(&planned.ir)
			.nodes
			.iter()
			.map(|(_, pipeline)| {
				planned.ir.placement(*pipeline).expect("a placed pipeline").io().clone()
			})
			.collect()
	}

	/// Whether each pipeline `sql` plans to reads its scan ahead, with the read-ahead pinned to
	/// `forced`.
	async fn read_aheads(setup: &str, sql: &str, forced: Option<bool>) -> Vec<bool> {
		FORCE_SCAN_READ_AHEAD.with(|pinned| pinned.set(forced));
		let planned = planned_request(setup, sql).await;
		FORCE_SCAN_READ_AHEAD.with(|pinned| pinned.set(None));
		PipelineDag::of(&planned.ir)
			.nodes
			.iter()
			.map(|(_, pipeline)| {
				planned.ir.placement(*pipeline).expect("a placed pipeline").io().scan_read_ahead
			})
			.collect()
	}

	#[tokio::test]
	async fn a_scan_of_a_whole_range_reads_its_chunks_ahead() {
		for sql in [
			"SELECT * FROM holder",
			"SELECT * FROM holder WHERE n = 1",
			"SELECT * FROM holder ORDER BY id DESC",
			"SELECT * FROM holder ORDER BY n",
			"SELECT n FROM holder GROUP BY n",
			"SELECT * FROM holder WHERE id > holder:10",
			"BEGIN; SELECT * FROM holder; COMMIT;",
			"RETURN (SELECT * FROM holder)",
			"LET $a = (SELECT * FROM holder); RETURN $a",
		] {
			let found = read_aheads(SETUP, sql, Some(true)).await;
			assert!(found.contains(&true), "{sql}: {found:?}");
		}
	}

	#[tokio::test]
	async fn a_scan_that_may_stop_early_or_run_more_than_once_reads_nothing_ahead() {
		for sql in [
			"SELECT * FROM holder LIMIT 3",
			"SELECT * FROM holder START 2",
			"SELECT * FROM holder LIMIT 3 START 2",
			"SELECT * FROM ONLY holder",
			"SELECT * FROM holder ORDER BY n LIMIT 3",
			"UPDATE holder SET x = 1",
			"DELETE holder",
			"SELECT *, (CREATE person SET x = 1 RETURN VALUE id) AS w FROM holder",
			"FOR $i IN [1, 2] { SELECT * FROM holder; };",
			"SELECT *, (SELECT * FROM person) AS inner FROM holder",
			"SELECT * FROM holder VERSION d'2024-01-01T00:00:00Z'",
		] {
			let found = read_aheads(SETUP, sql, Some(true)).await;
			assert!(found.iter().all(|ahead| !ahead) || sql.contains("inner"), "{sql}: {found:?}");
		}
	}

	#[tokio::test]
	async fn a_scan_under_a_timeout_reads_its_chunks_ahead() {
		let found = read_aheads(SETUP, "SELECT * FROM holder TIMEOUT 5s", Some(true)).await;
		assert_eq!(found, vec![true]);
		for sql in
			["SELECT * FROM ONLY holder TIMEOUT 5s", "SELECT * FROM holder LIMIT 3 TIMEOUT 5s"]
		{
			let found = read_aheads(SETUP, sql, Some(true)).await;
			assert!(found.iter().all(|ahead| !ahead), "{sql}: {found:?}");
		}
	}

	/// A pipeline that stops early behind a breaker or a split stops the scan that feeds it.
	#[tokio::test]
	async fn a_scan_whose_rows_a_later_pipeline_stops_reads_nothing_ahead() {
		for sql in [
			"SELECT * FROM holder SPLIT refs LIMIT 3",
			"SELECT *, n + 1 AS k FROM holder ORDER BY k LIMIT 3",
			"SELECT *, n + 1 AS k FROM holder ORDER BY k LIMIT 0",
			"SELECT * FROM holder ORDER BY rand() LIMIT 3",
			"SELECT * FROM (SELECT * FROM holder) LIMIT 3",
			"SELECT * FROM (SELECT * FROM holder ORDER BY n) LIMIT 3",
		] {
			let found = read_aheads(SETUP, sql, Some(true)).await;
			assert!(found.iter().all(|ahead| !ahead), "{sql}: {found:?}");
		}
	}

	/// The statements of a block run one after another: a write before or after a scan's statement
	/// runs while no chunk of the scan is read.
	#[tokio::test]
	async fn a_scan_beside_a_write_of_its_block_reads_its_chunks_ahead() {
		for sql in [
			"{ CREATE holder:1 SET n = 1; SELECT * FROM holder; }",
			"{ SELECT * FROM holder; CREATE holder:1 SET n = 1; }",
		] {
			let found = read_aheads(SETUP, sql, Some(true)).await;
			assert_eq!(found.iter().filter(|ahead| **ahead).count(), 1, "{sql}: {found:?}");
		}
	}

	#[tokio::test]
	async fn a_scan_nested_in_a_subquery_reads_nothing_ahead_though_its_outer_scan_does() {
		let found =
			read_aheads(SETUP, "SELECT *, (SELECT * FROM person) AS inner FROM holder", Some(true))
				.await;
		assert_eq!(found.len(), 2, "{found:?}");
		assert_eq!(found.iter().filter(|ahead| **ahead).count(), 1, "{found:?}");
	}

	#[test]
	fn the_read_ahead_needs_both_the_chunk_overlap_and_untracked_reads() {
		for (untracked_reads, scan_read_ahead) in
			[(false, false), (false, true), (true, false), (true, true)]
		{
			let caps = IoCaps {
				untracked_reads,
				scan_read_ahead,
				..IoCaps::default()
			};
			assert_eq!(offers_read_ahead(caps), untracked_reads && scan_read_ahead, "{caps:?}");
		}
	}

	#[tokio::test]
	async fn a_datastore_that_does_not_offer_the_read_ahead_gets_none() {
		for forced in [Some(false), None] {
			let found = read_aheads(SETUP, "SELECT * FROM holder", forced).await;
			assert_eq!(found, vec![false], "{forced:?}");
		}
	}

	#[tokio::test]
	async fn a_unit_that_runs_in_a_callers_frame_reads_nothing_ahead() {
		FORCE_SCAN_READ_AHEAD.with(|pinned| pinned.set(Some(true)));
		let planned = planned_unit(SETUP, "SELECT * FROM holder").await;
		FORCE_SCAN_READ_AHEAD.with(|pinned| pinned.set(None));
		let found: Vec<bool> = PipelineDag::of(&planned.ir)
			.nodes
			.iter()
			.map(|(_, pipeline)| {
				planned.ir.placement(*pipeline).expect("a placed pipeline").io().scan_read_ahead
			})
			.collect();
		assert!(!found.is_empty() && found.iter().all(|ahead| !ahead), "{found:?}");
	}

	fn windows(io: &PipelineIo) -> (bool, bool) {
		(io.fetch_windows, io.deref_windows)
	}

	#[tokio::test]
	async fn a_fetch_over_a_scan_reads_its_links_in_windows() {
		let found = ios(SETUP, "SELECT * FROM holder FETCH refs", None).await;
		assert_eq!(found.iter().map(windows).collect::<Vec<_>>(), vec![(true, true)]);
		let found = ios(SETUP, "SELECT * FROM holder FETCH refs, friends.*", None).await;
		assert_eq!(found.iter().map(windows).collect::<Vec<_>>(), vec![(true, true)]);
	}

	#[tokio::test]
	async fn a_fetch_behind_a_limit_is_judged_with_the_pipeline_it_continues() {
		let found = ios(SETUP, "SELECT * FROM holder LIMIT 5 FETCH refs", None).await;
		assert_eq!(found.len(), 2, "the paging and the fetch are two pipelines");
		assert_eq!(windows(&found[1]), (true, true), "{found:?}");
	}

	#[tokio::test]
	async fn a_dereference_without_a_fetch_is_windowed_without_fetch_windows() {
		let found = ios(SETUP, "SELECT refs.*, refs.n FROM holder", None).await;
		assert_eq!(found.iter().map(windows).collect::<Vec<_>>(), vec![(false, true)]);
	}

	#[tokio::test]
	async fn a_datastore_that_tracks_plain_reads_gets_no_window() {
		let found = ios(SETUP, "SELECT * FROM holder FETCH refs", Some(false)).await;
		assert_eq!(found, vec![PipelineIo::default()]);
	}

	/// Whether each pipeline `sql` plans to reads the sources of a hop in windows, with the
	/// datastore's offer pinned to `offered` and the untracked reads to `untracked`.
	async fn hop_windows(
		sql: &str,
		offered: Option<bool>,
		untracked: Option<bool>,
		unit: bool,
	) -> Vec<bool> {
		super::frontier::FORCE_FRONTIER_HOPS.with(|pinned| pinned.set(offered));
		FORCE_LINK_WINDOWS.with(|pinned| pinned.set(untracked));
		let setup = "DEFINE TABLE person SCHEMALESS; DEFINE TABLE knows TYPE RELATION;";
		let planned = match unit {
			true => planned_unit(setup, sql).await,
			false => planned_request(setup, sql).await,
		};
		super::frontier::FORCE_FRONTIER_HOPS.with(|pinned| pinned.set(None));
		FORCE_LINK_WINDOWS.with(|pinned| pinned.set(None));
		PipelineDag::of(&planned.ir)
			.nodes
			.iter()
			.map(|(_, pipeline)| {
				planned.ir.placement(*pipeline).expect("a placed pipeline").io().frontier_hops
					!= Default::default()
			})
			.collect()
	}

	#[tokio::test]
	async fn a_datastore_that_batches_reads_gets_the_hop_windows() {
		for sql in
			["SELECT ->knows->person AS f FROM person", "SELECT <-knows<-person AS f FROM person"]
		{
			let found = hop_windows(sql, Some(true), Some(true), false).await;
			assert!(found.iter().any(|window| *window), "{sql}: {found:?}");
		}
	}

	/// A hop in a branch, a short-circuit operand or an optional chain is lowered into the
	/// pipeline's own code, so the pipeline's windows serve it.
	#[tokio::test]
	async fn a_hop_under_a_conditional_expression_gets_the_hop_windows() {
		for sql in [
			"SELECT IF active THEN ->knows->person->knows->person ELSE [] END AS f FROM person",
			"SELECT active AND ->knows->person AS f FROM person",
			"SELECT active ?? ->knows->person AS f FROM person",
		] {
			let found = hop_windows(sql, Some(true), Some(true), false).await;
			assert!(found.iter().any(|window| *window), "{sql}: {found:?}");
		}
	}

	#[tokio::test]
	async fn a_datastore_that_does_not_batch_reads_gets_no_hop_window() {
		let found =
			hop_windows("SELECT ->knows->person FROM person", Some(false), Some(true), false).await;
		assert!(found.iter().all(|window| !*window), "{found:?}");
	}

	#[tokio::test]
	async fn a_datastore_that_tracks_plain_reads_gets_no_hop_window() {
		let found =
			hop_windows("SELECT ->knows->person FROM person", Some(true), Some(false), false).await;
		assert!(found.iter().all(|window| !*window), "{found:?}");
	}

	#[tokio::test]
	async fn a_unit_that_runs_in_a_callers_frame_gets_no_hop_window() {
		let found =
			hop_windows("SELECT ->knows->person FROM person", Some(true), Some(true), true).await;
		assert!(found.iter().all(|window| !*window), "{found:?}");
	}

	#[tokio::test]
	async fn a_versioned_read_gets_no_hop_window() {
		let found = hop_windows(
			"SELECT ->knows->person FROM person VERSION d'2030-01-01T00:00:00Z'",
			Some(true),
			Some(true),
			false,
		)
		.await;
		assert!(found.iter().all(|window| !*window), "{found:?}");
	}

	/// The rows window each pipeline `sql` plans to states, with the rows window and the link
	/// windows pinned on; `unit` plans `sql` as a unit rather than as a request.
	async fn row_windows(sql: &str, unit: bool) -> Vec<Option<FrontierRows>> {
		super::frontier::FORCE_FRONTIER_ROWS.with(|pinned| pinned.set(Some(true)));
		FORCE_LINK_WINDOWS.with(|pinned| pinned.set(Some(true)));
		let setup = "DEFINE TABLE person SCHEMALESS; DEFINE TABLE knows TYPE RELATION;
			DEFINE TABLE lite TYPE RELATION IN person OUT person LIGHTWEIGHT;";
		let planned = match unit {
			true => planned_unit(setup, sql).await,
			false => planned_request(setup, sql).await,
		};
		super::frontier::FORCE_FRONTIER_ROWS.with(|pinned| pinned.set(None));
		FORCE_LINK_WINDOWS.with(|pinned| pinned.set(None));
		PipelineDag::of(&planned.ir)
			.nodes
			.iter()
			.map(|(_, pipeline)| {
				planned
					.ir
					.placement(*pipeline)
					.expect("a placed pipeline")
					.io()
					.frontier_rows
					.clone()
			})
			.collect()
	}

	/// The paging clause of each rows window the pipelines state, in pipeline order.
	fn named(found: &[Option<FrontierRows>]) -> Vec<Option<usize>> {
		found.iter().flatten().map(|rows| rows.paging).collect()
	}

	#[tokio::test]
	async fn the_lookups_of_a_projection_over_a_scan_read_a_window_of_their_rows() {
		for sql in [
			"SELECT ->knows->person AS f FROM person",
			"SELECT <-knows AS e, ->knows AS g FROM person",
			"SELECT *, ->knows->person AS f FROM person",
			"SELECT VALUE ->knows->person FROM person:1..=60",
			"SELECT <~knows AS e FROM person",
		] {
			let found = row_windows(sql, false).await;
			assert_eq!(named(&found), vec![None], "{sql}: {found:?}");
		}
	}

	#[tokio::test]
	async fn a_paging_clause_before_the_lookups_bounds_the_rows_that_reach_them() {
		for sql in [
			"SELECT ->knows->person AS f FROM person LIMIT 7",
			"SELECT ->knows->person AS f FROM person LIMIT 7 START 2",
		] {
			let found = row_windows(sql, false).await;
			assert_eq!(named(&found), vec![Some(0)], "{sql}: {found:?}");
		}
	}

	/// The length of each chain of lookups each projection of the statements `sql` plans to runs
	/// over its row.
	async fn chain_lengths(sql: &str) -> Vec<usize> {
		let setup = "DEFINE TABLE person SCHEMALESS; DEFINE TABLE knows TYPE RELATION;";
		let planned = planned_request(setup, sql).await;
		let ir = &planned.ir;
		let mut lengths = Vec::new();
		let mut ops = vec![ir.top()];
		while let Some(region) = ops.pop() {
			for op in crate::exec::ir::core::walk::ops_in(ir, region) {
				let data = ir.op(op).expect("an op");
				ops.extend(data.regions.iter().copied());
				lengths.extend(frontier::row_chains(ir, op).iter().map(Vec::len));
			}
		}
		lengths.sort_unstable();
		lengths
	}

	#[tokio::test]
	async fn the_lookups_of_a_projection_chain_by_the_values_they_answer() {
		for (sql, lengths) in [
			("SELECT ->knows AS e FROM person", vec![1]),
			("SELECT ->knows->person AS f FROM person", vec![2]),
			("SELECT ->knows->person->knows->person AS f FROM person", vec![4]),
			("SELECT ->knows AS e, <-knows AS g FROM person", vec![1, 1]),
			("SELECT ->knows->person AS f, <-knows<-person AS g FROM person", vec![2, 2]),
			// A filter of a path is a step of its own: the hops after it start from its answer.
			("SELECT ->knows->person[WHERE n > 1]->knows AS f FROM person", vec![2]),
			// A key range of arrays and a lookup that does not
			// start from the row are not in a chain.
			("SELECT ->knows[WHERE true]->person AS f FROM person", vec![1]),
			("SELECT ->knows:[1]..[5] AS e FROM person", vec![]),
			("SELECT person:3->knows->person AS f FROM person", vec![]),
			// A lookup under a branch does not run on every row.
			("SELECT IF n > 20 { ->knows->person } ELSE { [] } AS f FROM person", vec![]),
		] {
			assert_eq!(chain_lengths(sql).await, lengths, "{sql}");
		}
	}

	#[tokio::test]
	async fn a_copied_unit_keeps_its_windows_on_the_ops_of_its_own_container() {
		super::frontier::FORCE_FRONTIER_HOPS.with(|pinned| pinned.set(Some(true)));
		super::frontier::FORCE_FRONTIER_ROWS.with(|pinned| pinned.set(Some(true)));
		FORCE_LINK_WINDOWS.with(|pinned| pinned.set(Some(true)));
		let setup = "DEFINE TABLE person SCHEMALESS; DEFINE TABLE knows TYPE RELATION;";
		let planned =
			planned_request(setup, "SELECT ->knows->person->knows AS f FROM person").await;
		super::frontier::FORCE_FRONTIER_HOPS.with(|pinned| pinned.set(None));
		super::frontier::FORCE_FRONTIER_ROWS.with(|pinned| pinned.set(None));
		FORCE_LINK_WINDOWS.with(|pinned| pinned.set(None));
		let ir = &planned.ir;
		let is_hop = |ir: &Ir, op: OpId| {
			matches!(
				kind_of(ir, op),
				Some(PhysOp::Loop {
					kind: LoopKind::GraphStep,
					..
				})
			)
		};
		let mut copied = 0;
		for (_, pipeline) in PipelineDag::of(ir).nodes.iter() {
			let io = ir.placement(*pipeline).expect("a placed pipeline").io();
			let Some(rows) = &io.frontier_rows else {
				continue;
			};
			assert!(rows.chains.iter().flatten().all(|hop| is_hop(ir, *hop)));
			// The unit is the region the pipeline stands in, or the nearest one that copies.
			let Some(unit) = crate::exec::ir::core::walk::ancestors(ir, *pipeline)
				.find_map(|(region, _)| ir.copy_region(region).ok())
			else {
				continue;
			};
			let at = unit.map.map_op(*pipeline).expect("the pipeline is copied");
			let carried = unit.ir.placement(at).expect("the placement is carried").io();
			let rows_of_copy = carried.frontier_rows.as_ref().expect("the rows window is carried");
			let expected: Vec<Vec<OpId>> = rows
				.chains
				.iter()
				.map(|chain| chain.iter().filter_map(|hop| unit.map.map_op(*hop)).collect())
				.collect();
			assert_eq!(rows_of_copy.chains, expected);
			assert!(
				rows_of_copy.chains.iter().flatten().all(|hop| is_hop(&unit.ir, *hop)),
				"the chains name ops of the copy"
			);
			assert!(
				rows_of_copy.chains.iter().flatten().all(|hop| carried.frontier_hops.serves(*hop))
			);
			copied += 1;
		}
		assert!(copied > 0, "a pipeline with a rows window was copied");
	}

	#[tokio::test]
	async fn a_hop_over_constant_key_bounds_serves_a_window() {
		assert_eq!(chain_lengths("SELECT ->knows:1..5 AS e FROM person").await, vec![1]);
	}

	#[tokio::test]
	async fn rows_that_may_not_reach_a_lookup_read_no_window() {
		for sql in [
			"SELECT ->knows->person AS f FROM person WHERE n > 20",
			"SELECT IF n > 20 { ->knows->person } ELSE { [] } AS f FROM person",
			"SELECT person:3->knows->person AS f FROM person",
			"SELECT n FROM person",
			"SELECT ->knows->person AS f FROM person ORDER BY n DESC",
			"SELECT ->knows->person AS f FROM person GROUP BY f",
			"SELECT ->knows->person AS f FROM person VERSION d'2030-01-01T00:00:00Z'",
			"SELECT ->knows->person AS f FROM lite",
		] {
			let found = row_windows(sql, false).await;
			assert!(found.iter().all(Option::is_none), "{sql}: {found:?}");
		}
	}

	#[tokio::test]
	async fn a_statement_that_writes_or_a_unit_in_a_callers_frame_reads_no_rows_window() {
		let found = row_windows(
			"SELECT (UPDATE person:1 SET seen = true) AS w, ->knows->person AS f FROM person",
			false,
		)
		.await;
		assert!(found.iter().all(Option::is_none), "{found:?}");
		let found = row_windows("SELECT ->knows->person AS f FROM person", true).await;
		assert!(found.iter().all(Option::is_none), "{found:?}");
	}

	#[tokio::test]
	async fn a_datastore_that_does_not_batch_reads_gets_no_rows_window() {
		super::frontier::FORCE_FRONTIER_HOPS.with(|pinned| pinned.set(Some(false)));
		let planned = planned_request(SETUP, "SELECT ->knows->person AS f FROM person").await;
		super::frontier::FORCE_FRONTIER_HOPS.with(|pinned| pinned.set(None));
		let found: Vec<_> = PipelineDag::of(&planned.ir)
			.nodes
			.iter()
			.map(|(_, pipeline)| {
				planned.ir.placement(*pipeline).unwrap().io().frontier_rows.clone()
			})
			.collect();
		assert!(found.iter().all(Option::is_none), "{found:?}");
	}

	#[tokio::test]
	async fn a_unit_that_runs_in_a_callers_frame_gets_no_window() {
		for sql in [
			"SELECT * FROM holder FETCH refs",
			"SELECT refs.*, refs.n FROM holder",
			"SELECT VALUE refs.n FROM holder",
		] {
			let found = ios_of_unit(SETUP, sql).await;
			assert!(!found.is_empty(), "{sql}");
			assert!(found.iter().all(|io| *io == PipelineIo::default()), "{sql}: {found:?}");
		}
		// The same statements as statements of a request are windowed, where the plan says so.
		let found = ios(SETUP, "SELECT * FROM holder FETCH refs", Some(true)).await;
		assert_eq!(found.iter().map(windows).collect::<Vec<_>>(), vec![(true, true)]);
	}

	#[tokio::test]
	async fn a_versioned_read_gets_no_window() {
		let found = ios(
			SETUP,
			"SELECT * FROM holder FETCH refs VERSION d'2024-01-01T00:00:00Z'",
			Some(true),
		)
		.await;
		assert_eq!(found, vec![PipelineIo::default()]);
	}

	#[tokio::test]
	async fn a_computed_index_that_writes_gets_no_fetch_window() {
		let found = ios(
			SETUP,
			"SELECT * FROM holder FETCH refs[(CREATE person:1 RETURN VALUE 0)[0]]",
			None,
		)
		.await;
		assert!(found.iter().all(|io| !io.fetch_windows), "{found:?}");
		let found = ios(SETUP, "SELECT * FROM holder FETCH refs[0], refs[$]", None).await;
		assert_eq!(found.iter().map(|io| io.fetch_windows).collect::<Vec<_>>(), vec![true]);
	}

	/// The rows-ahead decision of each pipeline `sql` plans to.
	async fn aheads(setup: &str, sql: &str) -> Vec<Option<(Vec<String>, bool)>> {
		ios(setup, sql, None)
			.await
			.into_iter()
			.map(|io| {
				io.rows_ahead.map(|ahead| {
					(
						ahead.fields.iter().map(|field| field.to_string()).collect(),
						ahead.paging.is_some(),
					)
				})
			})
			.collect()
	}

	const LINKS: &str = "DEFINE TABLE user SCHEMALESS; DEFINE TABLE comment SCHEMALESS;
		DEFINE TABLE computed SCHEMALESS; DEFINE FIELD author ON computed COMPUTED id;
		DEFINE TABLE walled SCHEMALESS;
		DEFINE FIELD author ON walled PERMISSIONS FOR select WHERE true;
		DEFINE TABLE derived SCHEMALESS; DEFINE FIELD upper ON derived COMPUTED string::uppercase(text);";

	fn one(fields: &[&str], paged: bool) -> Vec<Option<(Vec<String>, bool)>> {
		vec![Some((fields.iter().map(|field| (*field).to_owned()).collect(), paged))]
	}

	#[tokio::test]
	async fn a_fetch_of_link_fields_over_a_scan_reads_the_links_of_a_batch_ahead() {
		assert_eq!(
			aheads(LINKS, "SELECT * FROM comment FETCH author").await,
			one(&["author"], false)
		);
		assert_eq!(
			aheads(LINKS, "SELECT * FROM comment FETCH author, friends.*").await,
			one(&["author", "friends"], false)
		);
		assert_eq!(
			aheads(LINKS, "SELECT * FROM comment FETCH author, author").await,
			one(&["author"], false)
		);
	}

	#[tokio::test]
	async fn a_pipeline_placed_as_morsels_reads_the_links_of_each_batch_ahead() {
		let sql = "SELECT * FROM comment FETCH author";
		FORCE_MORSEL_PLACEMENT.with(|pinned| pinned.set(true));
		let found = aheads(LINKS, sql).await;
		FORCE_MORSEL_PLACEMENT.with(|pinned| pinned.set(false));
		assert_eq!(found, one(&["author"], false));
	}

	/// The decision of each pipeline `sql` plans to as a container of the kind `unit`, by a
	/// planner of a statement of a request, with the windows pinned on.
	async fn ios_as_unit(setup: &str, sql: &str, unit: UnitKind) -> Vec<PipelineIo> {
		FORCE_LINK_WINDOWS.with(|pinned| pinned.set(Some(true)));
		let planned = planned_as_unit(setup, sql, unit, true).await;
		FORCE_LINK_WINDOWS.with(|pinned| pinned.set(None));
		PipelineDag::of(&planned.ir)
			.nodes
			.iter()
			.map(|(_, pipeline)| {
				planned.ir.placement(*pipeline).expect("a placed pipeline").io().clone()
			})
			.collect()
	}

	/// A view's recalculation and a cascade are planned apart by a planner derived from the
	/// request's own, as stored subquery units: they run in the frame of the write that runs them.
	#[tokio::test]
	async fn a_stored_subquery_unit_planned_by_a_request_planner_gets_no_window() {
		for sql in [
			"SELECT * FROM holder FETCH refs",
			"SELECT refs.*, refs.n FROM holder",
			"SELECT * FROM comment FETCH author",
		] {
			let found = ios_as_unit(
				&format!("{SETUP} {LINKS}"),
				sql,
				UnitKind::Subquery {
					stored: true,
				},
			)
			.await;
			assert!(!found.is_empty(), "{sql}");
			assert!(found.iter().all(|io| *io == PipelineIo::default()), "{sql}: {found:?}");
		}
		// The same planner planning the statement as a request's windows it.
		let found = ios_as_unit(SETUP, "SELECT * FROM holder FETCH refs", UnitKind::Request).await;
		assert_eq!(found.iter().map(windows).collect::<Vec<_>>(), vec![(true, true)]);
	}

	#[tokio::test]
	async fn a_paging_clause_before_the_fetch_bounds_the_rows_that_reach_it() {
		let found = aheads(LINKS, "SELECT * FROM comment LIMIT 5 START 2 FETCH author").await;
		assert_eq!(found.last(), one(&["author"], true).last(), "{found:?}");
	}

	#[tokio::test]
	async fn a_row_that_may_not_reach_the_fetch_reads_no_link_ahead() {
		for sql in [
			"SELECT * FROM comment WHERE x = 1 FETCH author",
			"SELECT * OMIT text FROM comment FETCH author",
			"SELECT * FROM comment ORDER BY text FETCH author",
			"SELECT * FROM comment GROUP BY text FETCH author",
			"SELECT * FROM [comment:1, comment:2] FETCH author",
			"SELECT * FROM comment SPLIT tags FETCH author",
		] {
			let found = aheads(LINKS, sql).await;
			assert!(found.iter().all(Option::is_none), "{sql}: {found:?}");
		}
	}

	#[tokio::test]
	async fn a_link_the_table_derives_or_cuts_is_read_as_the_rows_hold_it() {
		for sql in [
			"SELECT * FROM computed FETCH author",
			"SELECT * FROM walled FETCH author",
			"SELECT * FROM derived FETCH author",
		] {
			let found = aheads(LINKS, sql).await;
			assert!(found.iter().all(Option::is_none), "{sql}: {found:?}");
		}
	}

	#[tokio::test]
	async fn a_path_that_is_not_a_link_field_or_its_elements_reads_no_link_ahead() {
		for sql in [
			"SELECT * FROM comment FETCH author.company",
			"SELECT * FROM comment FETCH author[0]",
			"SELECT * FROM comment FETCH author, author.company",
		] {
			let found = aheads(LINKS, sql).await;
			assert!(found.iter().all(Option::is_none), "{sql}: {found:?}");
		}
	}

	#[tokio::test]
	async fn a_statement_that_writes_reads_no_link_ahead() {
		let found = aheads(
			LINKS,
			"SELECT *, (CREATE audit SET at = time::now() RETURN VALUE id) AS w FROM comment FETCH author",
		)
		.await;
		assert!(found.iter().all(Option::is_none), "{found:?}");
	}

	#[tokio::test]
	async fn no_link_is_read_ahead_where_the_windows_are_refused() {
		let found = ios(
			LINKS,
			"SELECT * FROM comment FETCH author VERSION d'2024-01-01T00:00:00Z'",
			Some(true),
		)
		.await;
		assert!(found.iter().all(|io| io.rows_ahead.is_none()), "{found:?}");
		let found = ios(LINKS, "SELECT * FROM comment FETCH author", Some(false)).await;
		assert!(found.iter().all(|io| io.rows_ahead.is_none()), "{found:?}");
		FORCE_ROWS_AHEAD.with(|pinned| pinned.set(Some(false)));
		let found = ios(LINKS, "SELECT * FROM comment FETCH author", None).await;
		FORCE_ROWS_AHEAD.with(|pinned| pinned.set(None));
		assert!(found.iter().all(|io| io.rows_ahead.is_none()), "{found:?}");
	}

	#[tokio::test]
	async fn a_projection_that_dereferences_link_fields_reads_the_links_of_a_batch_ahead() {
		for (sql, fields) in [
			("SELECT author.name FROM comment", &["author"][..]),
			("SELECT author.name, author.company, text FROM comment", &["author"]),
			("SELECT author.name AS a, friends.name AS f FROM comment", &["author", "friends"]),
			("SELECT VALUE author.name FROM comment", &["author"]),
			("SELECT author.company.name FROM comment", &["author"]),
		] {
			assert_eq!(aheads(LINKS, sql).await, one(fields, false), "{sql}");
		}
		let found = aheads(LINKS, "SELECT author.name FROM comment LIMIT 5 START 2").await;
		assert_eq!(found.last(), one(&["author"], true).last(), "{found:?}");
	}

	#[tokio::test]
	async fn a_projection_that_reads_a_link_for_nothing_but_its_field_reads_no_link_ahead() {
		for sql in [
			"SELECT author FROM comment",
			"SELECT author.* FROM comment",
			"SELECT string::lowercase(author.name) AS n FROM comment",
			"SELECT IF text = 'a' { author.name } ELSE { 1 } AS n FROM comment",
			"SELECT (SELECT VALUE name FROM user) AS n FROM comment",
			"SELECT * FROM comment",
		] {
			let found = aheads(LINKS, sql).await;
			assert!(found.iter().all(Option::is_none), "{sql}: {found:?}");
		}
	}

	#[tokio::test]
	async fn a_row_that_may_not_reach_the_projection_reads_no_link_ahead() {
		for sql in [
			"SELECT author.name FROM comment WHERE x = 1",
			"SELECT author.name FROM comment ORDER BY text",
			"SELECT author.name FROM comment GROUP BY author",
			"SELECT author.name FROM [comment:1, comment:2]",
			"SELECT author.name, tags FROM comment SPLIT tags",
			"SELECT author.name FROM comment FETCH friends",
			"SELECT author.name FROM ONLY comment:1",
		] {
			let found = aheads(LINKS, sql).await;
			assert!(found.iter().all(Option::is_none), "{sql}: {found:?}");
		}
	}

	#[tokio::test]
	async fn a_dereferenced_link_the_table_derives_or_cuts_is_read_as_the_rows_hold_it() {
		for sql in [
			"SELECT author.name FROM computed",
			"SELECT author.name FROM walled",
			"SELECT upper.name FROM derived",
		] {
			let found = aheads(LINKS, sql).await;
			assert!(found.iter().all(Option::is_none), "{sql}: {found:?}");
		}
	}

	#[tokio::test]
	async fn a_statement_that_writes_or_reads_a_version_dereferences_no_link_ahead() {
		let found = aheads(
			LINKS,
			"SELECT author.name, (CREATE audit SET at = time::now() RETURN VALUE id) AS w FROM comment",
		)
		.await;
		assert!(found.iter().all(Option::is_none), "{found:?}");
		let found = ios(
			LINKS,
			"SELECT author.name FROM comment VERSION d'2024-01-01T00:00:00Z'",
			Some(true),
		)
		.await;
		assert!(found.iter().all(|io| io.rows_ahead.is_none()), "{found:?}");
		let found = ios(LINKS, "SELECT author.name FROM comment", Some(false)).await;
		assert!(found.iter().all(|io| io.rows_ahead.is_none()), "{found:?}");
		FORCE_ROWS_AHEAD.with(|pinned| pinned.set(Some(false)));
		let found = ios(LINKS, "SELECT author.name FROM comment", None).await;
		FORCE_ROWS_AHEAD.with(|pinned| pinned.set(None));
		assert!(found.iter().all(|io| io.rows_ahead.is_none()), "{found:?}");
	}

	/// What the principal `session` is told of the rows-ahead decision of `sql`.
	async fn aheads_as(setup: &str, session: &Session, sql: &str) -> Vec<bool> {
		let planned = planned_request_as(setup, session, sql).await;
		PipelineDag::of(&planned.ir)
			.nodes
			.iter()
			.map(|(_, pipeline)| {
				planned
					.ir
					.placement(*pipeline)
					.expect("a placed pipeline")
					.io()
					.rows_ahead
					.is_some()
			})
			.collect()
	}

	#[tokio::test]
	async fn a_table_whose_select_permission_is_conditional_reads_no_link_ahead() {
		let setup = format!(
			"{LINKS} DEFINE TABLE guarded SCHEMALESS PERMISSIONS FOR select WHERE shown = true;
			 DEFINE TABLE open SCHEMALESS PERMISSIONS FULL;"
		);
		let record = Session::for_record(
			"test",
			"test",
			"user",
			crate::types::PublicValue::String("user:bob".to_owned()),
		)
		.with_ns("test")
		.with_db("test");
		let owner = TestDb::owner();
		// The owner's rows pass no permission filter, so the same shape reads its links ahead.
		let sql = "SELECT * FROM guarded FETCH author";
		assert_eq!(aheads_as(&setup, &owner, sql).await, vec![true]);
		let found = aheads_as(&setup, &record, sql).await;
		assert!(
			found.iter().all(|ahead| !ahead),
			"a permission filter before the FETCH: {found:?}"
		);
		// A table the record user reads whole keeps the decision.
		let found = aheads_as(&setup, &record, "SELECT * FROM open FETCH author").await;
		assert_eq!(found, vec![true]);
		let sql = "SELECT author.name FROM guarded";
		assert_eq!(aheads_as(&setup, &owner, sql).await, vec![true]);
		let found = aheads_as(&setup, &record, sql).await;
		assert!(found.iter().all(|ahead| !ahead), "a permission filter before the projection");
		let found = aheads_as(&setup, &record, "SELECT author.name FROM open").await;
		assert_eq!(found, vec![true]);
	}
}
