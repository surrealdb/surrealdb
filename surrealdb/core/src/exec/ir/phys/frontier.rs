//! How a pipeline's graph and reference lookups batch the random key reads they make.
//!
//! A lookup (`Instr::Lookup`) reads, for each source it is handed, the inline cache of its
//! adjacency or references and, where that is not live, the keys of its adjacency. Read one source
//! at a time that is one multi-get of a single key and one cursor per source. The plan says
//! whether a run of sources may be read at once, and the physical rules here state it; the VM
//! copies the decision and reads no capability to make it.
//!
//! - `frontier_hops`: the sources a hop is handed are read in windows, one multi-get of their
//!   inline caches and one multi-range read of the adjacency keys the caches do not answer. The
//!   datastore says that reading many keys or ranges at once costs less than reading them one by
//!   one. A window is filled for the sources of one hop just before the hop reads them, so it holds
//!   keys the hop itself reads and no others; that makes the decision a fact of the datastore, and
//!   (as for every window of [`super::io`]) a pipeline that runs in the request's own frame at the
//!   current version, so that no version a caller holds can differ from the one a window reads at.
//!   [`hop_windows`] states which hops a window can serve.
//! - `frontier_rows`: the first hop of the lookups of a projection is read for a batch of source
//!   rows before the rows run. [`rows`] states the chains of such lookups: a lookup that starts
//!   from the row, and the lookups that each read the answer of the one before it, every one of
//!   them run whenever the projection runs.

use std::ops::Bound;

use super::io::{Rows, kind_of};
use super::ops::PhysOp;
use super::placement_attr::{FrontierHops, FrontierRows};
use crate::exec::ir::core::fxhash::FxHashMap;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::expr::ops::KeyBound;
use crate::exec::ir::iface::catalog::PhysCx;
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::logical::ops::{LookupAttrs, LoopKind, StepAttrs};
use crate::exec::ir::op_kind::OpKind;
use crate::expr::lookup::LookupKind;

#[cfg(test)]
thread_local! {
	/// Pins the hop windows on (`Some(true)`, whatever the datastore offers) or off, for tests
	/// that compare the two runs.
	pub(crate) static FORCE_FRONTIER_HOPS: std::cell::Cell<Option<bool>> =
		const { std::cell::Cell::new(None) };
}

/// Whether the datastore offers reading many keys and ranges at once. Asked only for a pipeline
/// whose plain reads join no conflict set (`io::io_of`), so a window that reads ahead of the
/// positions it serves widens no transaction's conflicts.
pub(super) fn hops_offered(cx: &PhysCx<'_>) -> bool {
	#[cfg(test)]
	if let Some(forced) = FORCE_FRONTIER_HOPS.with(std::cell::Cell::get) {
		return forced;
	}
	cx.physical.io_caps().multi_read
}

#[cfg(test)]
thread_local! {
	/// Pins the rows window on (`Some(true)`, whatever the datastore offers) or off, for tests that
	/// compare the runs.
	pub(crate) static FORCE_FRONTIER_ROWS: std::cell::Cell<Option<bool>> =
		const { std::cell::Cell::new(None) };
}

/// The hops of the pipeline `pipeline` whose sources a window serves: none where the datastore
/// offers no multi-read, otherwise every hop in the pipeline's regions that [`hop_windows`]
/// admits, those of the pipelines nested in them included, since the pipeline's code lowers them.
pub(super) fn served_hops(ir: &Ir, cx: &PhysCx<'_>, pipeline: OpId) -> FrontierHops {
	if !hops_offered(cx) {
		return FrontierHops::default();
	}
	let mut served = Vec::new();
	if let Some(data) = ir.op(pipeline) {
		for region in &data.regions {
			served_in(ir, *region, &mut served);
		}
	}
	FrontierHops::new(served)
}

/// Push onto `served` the hops in `region`, and in the regions of its ops, that a window serves.
/// The regions of an expression (a branch, a short-circuit operand, an optional chain) are walked
/// as the regions of a physical op are: a window reads only the sources of the one hop that
/// reaches it, at the version that hop reads at, so a hop that runs on only some rows is served
/// all the same.
fn served_in(ir: &Ir, region: RegionId, served: &mut Vec<OpId>) {
	for op in ops_in(ir, region) {
		let Some(data) = ir.op(op) else {
			continue;
		};
		if let OpKind::Phys(PhysOp::Loop {
			kind: LoopKind::GraphStep,
			step,
			..
		}) = &data.kind
			&& matches!(step.as_deref(), Some(StepAttrs::Lookup(attrs)) if hop_windows(attrs))
		{
			served.push(op);
		}
		for inner in &data.regions {
			served_in(ir, *inner, served);
		}
	}
}

/// The rows window of the pipeline `pipeline`, or `None` where a rule of the module does not hold.
///
/// The decision is a plan decision about a pipeline that reads rows from a scan of a table or of
/// a record range and whose projection hosts a lookup that starts from the row. The host keys the
/// rows of a batch; the plan states only that the keys it reads are ones the run reads.
pub(super) fn rows(ir: &Ir, cx: &PhysCx<'_>, pipeline: OpId, rows: &Rows) -> Option<FrontierRows> {
	#[cfg(test)]
	let offered = FORCE_FRONTIER_ROWS.with(std::cell::Cell::get).unwrap_or_else(|| hops_offered(cx));
	#[cfg(not(test))]
	let offered = hops_offered(cx);
	if !offered {
		return None;
	}
	let placement = ir.placement(pipeline)?;
	// A decided placement is all the rule asks: serial, or morsels of either input, each batch of
	// source rows reaches the body whole and in order.
	placement.decision()?;
	// The statement writes nothing and runs nothing the plan cannot see.
	if placement.statement().refusal.is_some() {
		return None;
	}
	let source_ok = match kind_of(ir, rows.source)? {
		PhysOp::TableScan(attrs) => !attrs.lightweight && attrs.topk.is_none(),
		PhysOp::KeyRangeScan(attrs) => attrs.topk.is_none(),
		PhysOp::RecordScan(attrs) => !attrs.for_update && !attrs.lightweight,
		PhysOp::IndexScan(_)
		| PhysOp::IndexUnion(_)
		| PhysOp::Bitmap(_)
		| PhysOp::IndexLookup(_)
		| PhysOp::FullTextScan(_)
		| PhysOp::KnnScan(_)
		| PhysOp::KnnTopK(_)
		| PhysOp::Count(_)
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
		| PhysOp::Join(_)
		| PhysOp::Distinct {
			..
		}
		| PhysOp::PermissionFilter(_)
		| PhysOp::Layer(_)
		| PhysOp::Filter
		| PhysOp::Fetch
		| PhysOp::Split {
			..
		}
		| PhysOp::Sort(_)
		| PhysOp::Window(_)
		| PhysOp::Aggregate(_)
		| PhysOp::Limit {
			..
		}
		| PhysOp::Project(_)
		| PhysOp::Timeout
		| PhysOp::VersionScope
		| PhysOp::Only
		| PhysOp::Explain {
			..
		}
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
		| PhysOp::MatchStep(_)
		| PhysOp::DdlGuard {
			..
		}
		| PhysOp::Define(_)
		| PhysOp::Remove(_)
		| PhysOp::Alter(_)
		| PhysOp::Rebuild(_)
		| PhysOp::Info {
			..
		}
		| PhysOp::Use {
			..
		}
		| PhysOp::Option {
			..
		}
		| PhysOp::Access(_)
		| PhysOp::Kill
		| PhysOp::Live(_)
		| PhysOp::Subscriber(_)
		| PhysOp::Show(_)
		| PhysOp::Write(_)
		| PhysOp::WriteStage {
			..
		}
		| PhysOp::Events(_)
		| PhysOp::Assign(_)
		| PhysOp::Unset {
			..
		}
		| PhysOp::Content {
			..
		}
		| PhysOp::Block
		| PhysOp::Let(_)
		| PhysOp::Sleep
		| PhysOp::If {
			..
		}
		| PhysOp::Loop {
			..
		}
		| PhysOp::Pipeline(_)
		| PhysOp::Choose(_)
		| PhysOp::Prefetch(_)
		| PhysOp::Materialize {
			..
		}
		| PhysOp::LoadUnit(_)
		| PhysOp::Yield
		| PhysOp::Return
		| PhysOp::Break
		| PhysOp::Next => false,
	};
	if !source_ok {
		return None;
	}
	// The ops before the projection take each row at most once, in order: one paging clause. The
	// ops after it end nothing early, so every row the batch keys reaches the lookups.
	let mut chains = Vec::new();
	let mut at = None;
	for (place, op) in rows.passes.iter().enumerate() {
		chains = row_chains(ir, *op);
		if !chains.is_empty() {
			at = Some(place);
			break;
		}
	}
	let at = at?;
	let mut limits = 0;
	for op in &rows.passes[..at] {
		if let Some(PhysOp::Limit {
			..
		}) = kind_of(ir, *op)
		{
			limits += 1;
		} else {
			return None;
		}
	}
	if limits > 1 {
		return None;
	}
	let ends_early = |op: &OpId| {
		matches!(
			kind_of(ir, *op),
			Some(
				PhysOp::Limit { .. } | PhysOp::Only | PhysOp::Break | PhysOp::Next | PhysOp::Return
			)
		)
	};
	if rows.passes[at..].iter().any(ends_early) {
		return None;
	}
	Some(FrontierRows {
		chains,
		paging: (limits == 1).then_some(0),
	})
}

/// A graph hop a projection runs whenever it runs: the op that states it, the value it starts
/// from, the value it answers, and whether a window can serve it.
struct Hop {
	op: OpId,
	source: ValueId,
	result: ValueId,
	windows: bool,
}

/// The hops a projection runs whenever it runs, in the order they are read, and the pipelines
/// that answer what a hop answers.
#[derive(Default)]
struct Hops {
	list: Vec<Hop>,
	/// A value a pipeline yields from the answer of a hop, by the value the pipeline answers.
	answers: FxHashMap<ValueId, ValueId>,
}

impl Hops {
	/// The hop answer `value` stands for.
	fn answered(&self, mut value: ValueId) -> ValueId {
		while let Some(next) = self.answers.get(&value) {
			value = *next;
		}
		value
	}
}

/// The chains of hops the projection `op` runs over the row it is evaluated against, each as the
/// graph-step ops that state its hops in the order they are read; empty for an op that is not a
/// projection or that runs no such hop.
///
/// A chain starts at a hop whose source is the row and goes on through the hops that each take the
/// answer of the one before as their source, so the sources of a hop are the records the hop
/// before reaches, through the pipelines that yield it. It ends before a hop no window can serve
/// ([`hop_windows`]). Only hops that run whenever control reaches them are considered: those of
/// the projection's own regions and of the regions of the ops that run their bodies whenever they
/// are reached, with no condition, no closure and no branch between.
pub(super) fn row_chains(ir: &Ir, op: OpId) -> Vec<Vec<OpId>> {
	let Some(PhysOp::Project(_)) = kind_of(ir, op) else {
		return Vec::new();
	};
	let Some(data) = ir.op(op) else {
		return Vec::new();
	};
	let mut found = Hops::default();
	for region in &data.regions {
		hops_in(ir, *region, &mut found);
	}
	let hops = &found.list;
	let mut used = vec![false; hops.len()];
	let mut chains = Vec::new();
	for first in 0..hops.len() {
		if used[first] || !hops[first].windows || !is_row(ir, hops[first].source) {
			continue;
		}
		used[first] = true;
		let mut last = hops[first].result;
		let mut chain = vec![hops[first].op];
		for at in first + 1..hops.len() {
			if !used[at] && hops[at].windows && found.answered(hops[at].source) == last {
				used[at] = true;
				last = hops[at].result;
				chain.push(hops[at].op);
			}
		}
		chains.push(chain);
	}
	chains
}

/// Push onto `found` the graph hops that run whenever control reaches `region`, or a region of an
/// op that does, in the order they are read.
fn hops_in(ir: &Ir, region: RegionId, found: &mut Hops) {
	for op in ops_in(ir, region) {
		let Some(data) = ir.op(op) else {
			continue;
		};
		let OpKind::Phys(kind) = &data.kind else {
			continue;
		};
		match kind {
			PhysOp::Loop {
				kind: LoopKind::GraphStep,
				step,
				..
			} => {
				if let (Some(source), Some(result), Some(StepAttrs::Lookup(attrs))) =
					(data.operands.first(), data.results.first(), step.as_deref())
				{
					found.list.push(Hop {
						op,
						source: *source,
						result: *result,
						windows: hop_windows(attrs),
					});
				}
			}
			PhysOp::Loop {
				kind: LoopKind::Broadcast,
				..
			}
			| PhysOp::Pipeline(..)
			| PhysOp::Block => {
				for inner in &data.regions {
					hops_in(ir, *inner, found);
				}
				if let (PhysOp::Pipeline(..), Some(answer)) = (kind, data.results.first()) {
					let yielded = data
						.regions
						.first()
						.and_then(|region| ops_in(ir, *region).last())
						.filter(|last| matches!(kind_of(ir, *last), Some(PhysOp::Yield)))
						.and_then(|last| ir.op(last)?.operands.first().copied());
					if let Some(yielded) = yielded {
						found.answers.insert(*answer, yielded);
					}
				}
			}
			PhysOp::TableScan(_)
			| PhysOp::IndexScan(_)
			| PhysOp::IndexUnion(_)
			| PhysOp::Bitmap(_)
			| PhysOp::IndexLookup(_)
			| PhysOp::FullTextScan(_)
			| PhysOp::KeyRangeScan(_)
			| PhysOp::KnnScan(_)
			| PhysOp::KnnTopK(_)
			| PhysOp::Count(_)
			| PhysOp::RecordScan(_)
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
			| PhysOp::Join(_)
			| PhysOp::Distinct {
				..
			}
			| PhysOp::PermissionFilter(_)
			| PhysOp::Layer(_)
			| PhysOp::Filter
			| PhysOp::Fetch
			| PhysOp::Split {
				..
			}
			| PhysOp::Sort(_)
			| PhysOp::Window(_)
			| PhysOp::Aggregate(_)
			| PhysOp::Limit {
				..
			}
			| PhysOp::Project(_)
			| PhysOp::Timeout
			| PhysOp::VersionScope
			| PhysOp::Only
			| PhysOp::Explain {
				..
			}
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
			| PhysOp::MatchStep(_)
			| PhysOp::DdlGuard {
				..
			}
			| PhysOp::Define(_)
			| PhysOp::Remove(_)
			| PhysOp::Alter(_)
			| PhysOp::Rebuild(_)
			| PhysOp::Info {
				..
			}
			| PhysOp::Use {
				..
			}
			| PhysOp::Option {
				..
			}
			| PhysOp::Access(_)
			| PhysOp::Kill
			| PhysOp::Live(_)
			| PhysOp::Subscriber(_)
			| PhysOp::Show(_)
			| PhysOp::Write(_)
			| PhysOp::WriteStage {
				..
			}
			| PhysOp::Events(_)
			| PhysOp::Assign(_)
			| PhysOp::Unset {
				..
			}
			| PhysOp::Content {
				..
			}
			| PhysOp::Let(_)
			| PhysOp::Sleep
			| PhysOp::If {
				..
			}
			| PhysOp::Loop {
				..
			}
			| PhysOp::Choose(_)
			| PhysOp::Prefetch(_)
			| PhysOp::Materialize {
				..
			}
			| PhysOp::LoadUnit(_)
			| PhysOp::Yield
			| PhysOp::Return
			| PhysOp::Break
			| PhysOp::Next => {}
		}
	}
}

/// Whether the hop `attrs` states is one a window can serve: it reads the whole adjacency of each
/// source it is handed, or the whole set of records referencing it, over key bounds that are
/// constants. A hop with a `WHERE` runs host code between one source's reads and the next's, and
/// a page reads no more of an adjacency than it reaches, so neither is read in a window.
fn hop_windows(attrs: &LookupAttrs) -> bool {
	if attrs.cond || attrs.limit.is_some() || attrs.start.is_some() {
		return false;
	}
	let constant = |bound: &Bound<KeyBound>| match bound {
		Bound::Unbounded => true,
		Bound::Included(key) | Bound::Excluded(key) => {
			matches!(key, KeyBound::Number(_) | KeyBound::String(_) | KeyBound::Uuid(_))
		}
	};
	attrs.subjects.iter().all(|subject| match (&attrs.kind, &subject.range) {
		(_, None) => true,
		(LookupKind::Graph(_), Some(range)) => constant(&range.start) && constant(&range.end),
		(LookupKind::Reference, Some(range)) => {
			matches!((&range.start, &range.end), (Bound::Unbounded, Bound::Unbounded))
		}
	})
}

/// Whether `value` is the row of a projection: the value its clause is evaluated against, or the
/// one element of a broadcast of such a value, since a row is not a collection.
fn is_row(ir: &Ir, value: ValueId) -> bool {
	let Some(ValueDef::BlockArg {
		block,
		index: 0,
	}) = ir.value(value).map(|value| value.def)
	else {
		return false;
	};
	let Some(region) = ir.block(block).map(|block| block.parent) else {
		return false;
	};
	let Some(owner) = ir.parent_op(region) else {
		return false;
	};
	let Some(data) = ir.op(owner) else {
		return false;
	};
	match kind_of(ir, owner) {
		Some(PhysOp::Project(_)) => {
			ir.region(region).and_then(|region| region.blocks.first()) == Some(&block)
		}
		Some(PhysOp::Loop {
			kind: LoopKind::Broadcast,
			..
		}) => data.operands.first().is_some_and(|source| is_row(ir, *source)),
		_ => false,
	}
}
