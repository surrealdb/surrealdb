//! Where the physical plan decides how a pipeline runs.
//!
//! The logical plan states what may be done with a pipeline ([`Legality`], and the
//! [`StatementLegality`] of its statement); this module is the one place the physical plan chooses.
//! Every decision of the form "run this apart or not", "how many rows to hand over at once", "how
//! many rows may be in flight" is a method of [`Policy`], so that the code that acts on a choice
//! never holds a threshold of its own.
//!
//! A [`Policy`] decides from four inputs:
//!
//! - the pipeline's [`Legality`] and its statement's refusal, which bound every choice: a pipeline
//!   the logical plan does not allow to split is never split, and the physical plan only weakens
//!   what the logical plan states, never strengthens it;
//! - the [`PhysicalFacts`] of the pipeline, derived from the physical plan by the pass that places
//!   it ([`crate::exec::vm::place`]): the access path, whether rows are put through enforcement,
//!   whether the pipeline or a statement around it writes, the sink its rows end in, how its paging
//!   is shaped and what its expressions nest. No fact is read from emitted code;
//! - the [`Signals`] a run observes while it executes: rows merged and reached by paging, the
//!   measured cost of a row, the entries a coordinator has read, what a reader will take before it
//!   may stop, the places the statement holds;
//! - the configuration, [`ExecConfig`].
//!
//! The plan-time question, [`Policy::plan`], answers a [`Decision`]: whether the pipeline runs as
//! morsels, where their rows come from (workers that read their own key ranges,
//! [`MorselInput::Range`], or batches of rows the coordinator reads and hands on,
//! [`MorselInput::Batches`]), how their results combine and whether the merge replays the
//! pipeline's paging. The decision is stored on the pipeline's source node ([`Placement`]) and
//! written only by the placement pass; lowering copies it into the program's [`Parallel`]. A
//! pipeline the logical plan does not list is physical-only ([`PhysicalOrigin`]) and runs
//! serially.
//!
//! The decision does not say how a body executes. Whether a body runs inside the storage cursor
//! is a property of the code lowering emits, and the VM narrows a decision by it when the pipeline
//! runs, never widening one (`morsel::effective_input`).
//!
//! The run-time questions size batches, bound the rows in flight, and say when and how to split
//! what is left of a range ([`Policy::split`]), when a running morsel donates part of its
//! remainder ([`Policy::steal`], [`Policy::resplit`]), when a worker returns its place
//! ([`Policy::yield_place`]) and how many places a statement may hold. Statistics enter the same
//! methods through [`Signals`] and change no signature of the logical layer.
//!
//! The decision cases the policy exists to make:
//!
//! - a selective scan, whose predicate passes few rows, is scan-bound and fans out;
//! - a scan whose predicate passes nearly every row is bound by the bandwidth to the client, does
//!   not fan out, and keeps its operators fused without tracking batches;
//! - a small input runs as one pipeline;
//! - pipelines are split, reordered or elided only where their [`Legality`] allows it.

use std::time::Duration;

use surrealdb_kvs::{Estimate, SplitTarget};

use crate::exec::config::ExecConfig;
use crate::exec::ir::iface::{
	ApplyCut, BarrierCause, Estimate as PlanEstimate, Exit, LoopFacts, LoopMode, LoopRefusal,
};
use crate::exec::ir::phys::ops::Batch;
use crate::exec::ir::phys::write_cut::WriteCut;
use crate::exec::logical::pipelines::{
	AggregateCombine, Arrival, ChainRefusal, Legality, SinkCombine, SourceSplit, StatementLegality,
};

/// The reason a loop that exits in iteration order runs serially.
const ORDERED_EXIT: &str = "the loop exits in iteration order";

/// The time a batch run on the statement's task is sized to.
pub(crate) const INLINE_BUDGET: Duration = Duration::from_millis(1);
/// The time a batch handed to a worker is sized to.
const WORKER_BUDGET: Duration = Duration::from_millis(8);
/// The rows of the first batch, before any cost is measured.
const FIRST_BATCH: usize = 64;
/// The most partitions the groups of morsels are divided into for their merge.
const MAX_PARTITIONS: usize = 64;
/// The groups, between the morsels that wait, from which their merge is divided into partitions.
const MIN_PARTITIONED_GROUPS: usize = 256;
/// The groups that gather before the morsels that wait are merged, when there are two of them.
const PARTIAL_WINDOW_GROUPS: usize = 1024;

/// How a pipeline's rows run.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) enum Parallel {
	/// One after another, on the statement's task.
	#[default]
	Serial,
	/// As morsels, parts of the input run apart and merged in the order of the serial run.
	Morsels(MorselPlan),
}

/// What the physical plan decided for a pipeline that runs as morsels.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MorselPlan {
	/// Where a morsel's rows come from.
	pub(crate) input: MorselInput,
	/// How the morsels' results combine at the sink.
	pub(crate) combine: Combine,
	/// The paging clause the merge replays, when the pipeline pages to the statement's output.
	pub(crate) paged: Option<u16>,
	/// Whether a scan's morsels may reject an entry that the threshold of a top-k pushdown shows
	/// worse than every row the sort keeps, before its body runs. Set only for a body that cannot
	/// raise for any row: the serial scan over stored bytes runs every entry, so an entry the
	/// threshold rejects must have no failure to raise.
	pub(crate) prunes: bool,
	/// In what order the merge takes the morsels' results.
	pub(crate) merge: MergeOrder,
	/// The places, among the calls of the aggregate's functions in the order its fields name
	/// them, of the accumulators whose morsels keep the numbers their rows hand over instead of
	/// folding them, for the merge to fold in the order of the serial run; empty when no morsel
	/// keeps any. Not empty only with [`MergeOrder::Serial`].
	pub(crate) ordered_fold: Vec<usize>,
}

/// In what order the merge of a pipeline's morsels takes their results, from the weakest to the
/// strongest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum MergeOrder {
	/// In the order of the serial run's input: the morsel at the front first, however long it
	/// takes, and every other morsel holds its output until the front has merged.
	Serial,
	/// The sorted runs of a sort that keeps every row are taken as the morsels finish and kept at
	/// their positions in the input, which is where its ties are defined, until the morsels before
	/// them have merged: the sort then takes them in the order of the serial run. A run held in
	/// memory takes no place of the morsels the policy lets be in flight, so a slow morsel at the
	/// front does not stop the morsels after it. Chosen only for a [`Combine::StableMerge`]
	/// pipeline over a key range whose logical plan states [`Arrival::Positioned`].
	Positioned,
	/// The partial groups of an aggregate are taken as the morsels finish, and those of morsels
	/// next to one another in the input are combined, the earlier before the later, before the
	/// first of them is next to the groups the statement holds. Chosen only for a
	/// [`Combine::Aggregate`] pipeline whose logical plan states [`Arrival::Adjacent`] or
	/// [`Arrival::Free`].
	Adjacent,
	/// As the morsels finish: no morsel waits for another to be merged. The rows of the
	/// statement's output reach the reader in no order the plan promises, and the partial counts
	/// of an aggregate add in whatever order they are taken. Chosen only for a pipeline whose
	/// logical plan states [`Arrival::Free`].
	Free,
}

impl MergeOrder {
	/// The arrival the logical plan must state for the merge to take results in this order.
	pub(crate) fn as_arrival(self) -> Arrival {
		match self {
			Self::Serial => Arrival::Serial,
			Self::Positioned => Arrival::Positioned,
			Self::Adjacent => Arrival::Adjacent,
			Self::Free => Arrival::Free,
		}
	}

	/// Whether a merge in this order is one the merge of `combine` has a meaning for: a sort
	/// positions runs, an aggregate combines adjacent partial states, a concatenation of rows is
	/// free where no reader sees their order, and no other combine takes its morsels out of order.
	pub(crate) fn fits(self, combine: Combine) -> bool {
		match self {
			Self::Serial => true,
			Self::Positioned => combine == Combine::StableMerge,
			Self::Adjacent => combine == Combine::Aggregate,
			Self::Free => matches!(combine, Combine::Concat | Combine::Aggregate),
		}
	}
}

/// Where a morsel's rows come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MorselInput {
	/// Workers read their own key ranges through the transaction's read view, in place. Chosen
	/// for a body that runs inside the storage cursor over a table scan or a record range.
	Range,
	/// The coordinator reads the source and hands decoded rows on in batches. Chosen for every
	/// other pipeline that runs as morsels.
	Batches,
}

/// How the results of a pipeline's morsels combine at its sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Combine {
	/// The statement's output: the morsels' rows concatenated, in the order of the serial run or,
	/// where the merge order is [`MergeOrder::Free`], in the order the morsels finish.
	Concat,
	/// A sort that keeps every row: the morsels' entries in order, merged stably.
	StableMerge,
	/// A sort that keeps a bounded number of rows: each morsel prunes as the serial sort does.
	TopK,
	/// Aggregates whose partial states fold, in order, to the serial fold.
	Aggregate,
	/// The statement's output, whose rows pass through one distinct set: each morsel keeps the
	/// first occurrence of each row it runs, and the merge keeps the first of the morsels'.
	Distinct,
	/// A write over rows: each morsel computes its records' images ahead of the write loop's cut,
	/// and the statement's task applies them in the order of the serial run.
	OrderedApply,
}

impl Combine {
	/// The sink of the logical plan that states this combine, for [`Legality::weaker_or_equal`]:
	/// an aggregate names no function, since only its kind is compared.
	pub(crate) fn as_sink(self) -> SinkCombine {
		match self {
			Self::Concat => SinkCombine::Concat,
			Self::StableMerge => SinkCombine::StableMerge,
			Self::TopK => SinkCombine::TopK,
			Self::Aggregate => SinkCombine::Aggregate(Vec::new()),
			Self::Distinct => SinkCombine::Distinct,
			Self::OrderedApply => SinkCombine::OrderedApply,
		}
	}
}

/// What the physical plan decides for a pipeline, before lowering gives it a slot for its paging
/// clause. Stored on the pipeline's source node ([`Placement`]) and copied by lowering into
/// [`Parallel`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Decision {
	/// One after another, on the statement's task.
	Serial,
	/// As morsels.
	Morsels {
		/// Where a morsel's rows come from. The body may narrow it when the pipeline runs; see the
		/// module.
		input: MorselInput,
		/// How the morsels' results combine at the sink.
		combine: Combine,
		/// Whether the pipeline's one paging clause is replayed by the merge. The clause's slot in
		/// the program is lowering's.
		paged: bool,
		/// Whether a scan's morsels may reject entries by a top-k threshold
		/// ([`MorselPlan::prunes`]).
		prunes: bool,
	},
}

impl Decision {
	/// The decision as bytecode, the paging clause being slot `limit` of the program. `None` when
	/// the decision pages and no slot is given. The merge takes the morsels' results in `merge`,
	/// and folds the terms the morsels keep for the accumulators at `ordered_fold` in order.
	pub(crate) fn to_parallel(
		self,
		limit: Option<u16>,
		merge: MergeOrder,
		ordered_fold: Vec<usize>,
	) -> Option<Parallel> {
		match self {
			Self::Serial => Some(Parallel::Serial),
			Self::Morsels {
				input,
				combine,
				paged,
				prunes,
			} => Some(Parallel::Morsels(MorselPlan {
				input,
				combine,
				paged: match (paged, limit) {
					(false, _) => None,
					(true, Some(limit)) => Some(limit),
					(true, None) => return None,
				},
				prunes,
				merge,
				ordered_fold,
			})),
		}
	}
}

/// The access path a pipeline reads, as the physical plan names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SourceKind {
	Scan,
	Record,
	/// A B-tree index scan; `repeats` is whether its access may reach one record through several
	/// entries and so must list each record once.
	Index {
		repeats: bool,
	},
	Lookup,
	IndexUnion,
	Bitmap,
	FullText,
	Knn,
	LightweightEdges,
	/// A union every member of which is a storage source.
	Union,
	/// The ids of a range that a write's one mock names: the coordinator generates them in order
	/// and reads no stored record for them.
	Mock,
	/// Every other source: values, a statement, a sorted or grouped input, a split, a join, a
	/// count, a write's targets, a once-evaluated input.
	Other,
}

impl SourceKind {
	/// How the logical plan may cut this source: a key range the storage layer splits, a
	/// sequence the coordinator hands on in order, or neither. An index scan that may repeat a
	/// record is a sequence: the coordinator lists each record once, in the order of its first
	/// entry, which a cut key range cannot do.
	pub(crate) fn split(self) -> SourceSplit {
		match self {
			Self::Scan
			| Self::Record
			| Self::Index {
				repeats: false,
			} => SourceSplit::Range,
			Self::Index {
				repeats: true,
			}
			| Self::Lookup
			| Self::IndexUnion
			| Self::Bitmap
			| Self::FullText
			| Self::Knn
			| Self::LightweightEdges
			| Self::Union
			| Self::Mock => SourceSplit::Ordinal,
			Self::Other => SourceSplit::Whole,
		}
	}
}

/// What a pipeline's expressions nest, ordered from the most to the least permissive: a
/// pipeline that nests several statements is as restricted as the most restricted of them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum NestedUse {
	/// No statement is written inside an expression of the pipeline.
	#[default]
	None,
	/// Every nested statement only reads and is evaluated afresh for each row.
	PerRowReadOnly,
	/// Every nested statement only reads, and one is evaluated once or through a probe.
	OnceOrProbed,
	/// A nested statement writes, or is a membership test.
	Other,
}

/// How a pipeline's rows end, as the physical plan cuts it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SinkKind {
	/// The statement's output.
	Output,
	/// A sort; `bounded` when it keeps a bounded number of rows.
	Sort {
		bounded: bool,
	},
	/// A brute-force nearest-neighbour search.
	Knn,
	/// A grouping.
	Aggregate,
	/// The write body, which writes each row.
	Write,
	/// A split, or a binding step that expands rows.
	Split,
	/// The probe side of a join.
	JoinProbe,
	/// The pairing side of a join.
	JoinPair,
	/// A window, which reads every row of a partition in partition-then-order sequence.
	Window,
}

/// Physical facts about one pipeline, derived from the placed physical plan ([`PhysicalPlan`]) and
/// never from emitted code.
///
/// [`PhysicalPlan`]: crate::exec::vm::payload::PhysicalPlan
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PhysicalFacts {
	/// The access path the pipeline reads.
	pub(crate) source: SourceKind,
	/// Rows are put through table enforcement first.
	pub(crate) enforce: bool,
	/// The pipeline or its statement writes.
	pub(crate) writes: bool,
	/// The paging clauses of the pipeline.
	pub(crate) limits: usize,
	/// The sink is the statement's output.
	pub(crate) output: bool,
	/// How the pipeline's rows end.
	pub(crate) sink: SinkKind,
	/// A binding step replaces the row.
	pub(crate) binds_row: bool,
	/// The body reads the record each row names, where a row may be a record id.
	pub(crate) fetches_row: bool,
	/// How many distinct sets the rows pass through.
	pub(crate) distincts: usize,
	/// What the pipeline's expressions nest.
	pub(crate) nested: NestedUse,
	/// An expression of the pipeline holds a block construct other than a nested statement.
	pub(crate) host_blocks: bool,
	/// The paging the pipeline holds is one a merge can replay over rows counted in order. The
	/// body runs the same instructions for a row taken whatever the counts were, its `START`
	/// skips a row to the row's end before the row emits anything, its `LIMIT` counts the row
	/// straight after, and the check that the limit is spent comes last, after everything the
	/// row emits, and is the only way to the pipeline's end.
	pub(crate) paging_replayable: bool,
	/// The plan-time estimate of the pipeline's rows. `None`: no plan-time estimate; every
	/// consumer applies its fixed rule.
	pub(crate) estimate: Option<PlanEstimate>,
}

/// What a run observes of a pipeline while it executes.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct Signals {
	/// The rows of a paged pipeline the merge has taken, and how many reached the paging.
	pub(crate) merged: usize,
	pub(crate) reached: usize,
	/// The measured cost of a row, in nanoseconds.
	pub(crate) cost_ns_per_row: Option<f64>,
	/// For a pipeline that pages nothing itself, the rows its reader takes before it may stop.
	pub(crate) scan_hint: Option<u32>,
	/// Whether the pipeline pages its rows to the statement's output.
	pub(crate) paged: bool,
	/// The rows `START` still skips, and the rows `LIMIT` still admits; `None` when no limit
	/// bounds the paging.
	pub(crate) skip: usize,
	pub(crate) remaining: Option<usize>,
	/// The entries the coordinator has read of the range it is running, and the bytes of keys and
	/// values they held.
	pub(crate) visited: u64,
	pub(crate) visited_bytes: u64,
	/// The entries of `visited` the pipeline's filters passed.
	pub(crate) passed: u64,
	/// Whether the transaction gives workers no read view, so that no worker can read a morsel.
	pub(crate) no_view: bool,
	/// What is left of the range being run, when the coordinator has measured it from the
	/// entries it has read; `None` otherwise.
	pub(crate) remaining_estimate: Option<Estimate>,
	/// What the storage's metadata says the range holds as the scan opens, before an entry of it
	/// is read ([`EstimateSource::KvRange`](crate::exec::ir::iface::EstimateSource::KvRange));
	/// `None` where the storage keeps no such metadata. Only [`Policy::split_at_open`] reads it:
	/// once the coordinator has read entries, what it measured answers.
	pub(crate) opened: Option<PlanEstimate>,
	/// The share of the range's rows its pipeline's filters pass (`selectivity_ppm`), known before
	/// the scan opens; `None` where nothing supplies one. Only [`Policy::split_at_open`] reads it.
	pub(crate) selectivity: Option<PlanEstimate>,
	/// The places of the node's worker pool the statement holds.
	pub(crate) places_held: usize,
	/// The morsels of the scan that wait for a worker or the coordinator.
	pub(crate) queued: usize,
	/// The entries a running morsel has read since it was last cut, and every entry it has read
	/// when it never was.
	pub(crate) since_cut: u64,
	/// How many times the morsel has been cut.
	pub(crate) cuts: u32,
}

/// What the policy decides about the remainder of a range the coordinator is running.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Split {
	/// Keep running it.
	Whole,
	/// Cut it into morsels of about `target`.
	Split {
		target: SplitTarget,
	},
}

/// The one place the physical plan chooses how a pipeline runs; see the module.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Policy {
	/// The entries a coordinator reads before it may split its range, and the rows from which a
	/// batch may go to a worker; `usize::MAX` keeps everything on the statement's task.
	split_rows: usize,
	/// The bytes a morsel over a key range aims at.
	target_bytes: usize,
	/// The places one statement may hold.
	places: usize,
	/// The places of the node's pool that workers leave free.
	#[cfg_attr(target_family = "wasm", allow(dead_code))]
	oltp_reserve: usize,
	/// The output a morsel that is not at the front holds before it parks.
	#[cfg_attr(target_family = "wasm", allow(dead_code))]
	window_bytes: usize,
	/// Every running morsel donates its remainder at every chunk boundary that can be cut.
	steal_always: bool,
	/// The quanta a worker holds a place while another statement waits for one.
	#[cfg_attr(target_family = "wasm", allow(dead_code))]
	place_quantum: u32,
	/// `EXPLAIN ANALYZE` renders the morsel counters.
	explain_analyze: bool,
	/// Whether a range is cut only where workers can read the parts through a read view.
	needs_view: bool,
	/// The most rows in a million of a range its filters may pass for the range to be cut.
	split_pass_ppm: u32,
	/// The morsels a host's places run at once for the scan this policy serves; `0` where the
	/// scan's morsels run only on the node.
	host_width: usize,
}

impl Policy {
	/// The policy `config` selects.
	pub(crate) fn from_config(config: &ExecConfig) -> Self {
		Self {
			split_rows: config.morsel_split_rows,
			target_bytes: config.morsel_target_bytes,
			places: config.morsel_places,
			oltp_reserve: config.morsel_oltp_reserve,
			window_bytes: config.morsel_window_bytes,
			steal_always: config.morsel_steal_always,
			place_quantum: config.morsel_place_quantum,
			explain_analyze: config.morsel_explain_analyze,
			needs_view: config.morsel_needs_view,
			split_pass_ppm: config.morsel_split_pass_ppm,
			host_width: 0,
		}
	}

	/// This policy for a scan whose morsels also run at a host's places, `width` at once.
	pub(crate) fn with_host_width(self, width: usize) -> Self {
		Self {
			host_width: width,
			..self
		}
	}

	/// Whether a range is cut only where workers can read its parts through a read view.
	pub(crate) fn needs_view(self) -> bool {
		self.needs_view
	}

	/// Whether the policy ever cuts a range or hands a batch to a worker: `false` for a split
	/// threshold of `usize::MAX`, which keeps every row on the statement's task.
	pub(crate) fn may_split_at_all(self) -> bool {
		self.split_rows != usize::MAX
	}

	/// The decision for a pipeline outside a loop, and for a pipeline that runs serially the
	/// reason.
	pub(crate) fn judge(
		legality: &Legality,
		statement: StatementLegality,
		facts: &PhysicalFacts,
	) -> Result<Decision, &'static str> {
		if let Some(refusal) = statement.refusal {
			return Err(chain_refusal_reason(refusal));
		}
		if facts.enforce {
			return Err("rows are put through enforcement");
		}
		if facts.writes {
			return Err("the pipeline or its statement writes");
		}
		if !legality.may_split() {
			return Err("the logical plan does not allow the pipeline to split");
		}
		// The access path may be one the logical plan cannot see: a union that holds a statement of
		// its own is no key range and no sequence the coordinator hands on.
		if facts.source.split() == SourceSplit::Whole {
			return Err("the access path cannot be cut");
		}
		if !sink_combines(legality) {
			return Err("the sink does not combine");
		}
		// The answer of a split run must equal the serial fold bit for bit, and a float fold's
		// low bits depend on how its terms are grouped: a split run folds them only where the
		// morsels keep the terms for the merge to fold in the order of the serial run.
		if facts.sink == SinkKind::Aggregate && folds_floats(legality) && !legality.ordered_fold {
			return Err("a float fold depends on how its terms are grouped");
		}
		if facts.binds_row {
			return Err("a binding step replaces the row");
		}
		if facts.fetches_row {
			return Err("the body reads the record each row names");
		}
		if !matches!(facts.nested, NestedUse::None | NestedUse::PerRowReadOnly) {
			return Err("a nested statement is evaluated once, probed, or writes");
		}
		if facts.host_blocks {
			return Err("an expression holds a block construct");
		}
		let paged = match facts.limits {
			0 => false,
			1 if facts.output && facts.paging_replayable => true,
			_ => return Err("the paging is not one a merge replays"),
		};
		let picked = match facts.sink {
			SinkKind::Output => match facts.distincts {
				0 => Combine::Concat,
				// One distinct set that the output's rows pass through, with nothing paged after
				// it: the merge keeps the first occurrence of each row, which is all the set
				// decides.
				1 if !paged => Combine::Distinct,
				_ => return Err("rows pass through a distinct set that a merge cannot replay"),
			},
			SinkKind::Sort {
				bounded: true,
			}
			| SinkKind::Knn => Combine::TopK,
			SinkKind::Sort {
				bounded: false,
			} => Combine::StableMerge,
			SinkKind::Aggregate => Combine::Aggregate,
			SinkKind::Write
			| SinkKind::Split
			| SinkKind::JoinProbe
			| SinkKind::JoinPair
			| SinkKind::Window => {
				return Err("the sink holds state a part cannot own");
			}
		};
		// A distinct set of a body that no morsel's merge replays is the serial run's alone.
		if (facts.distincts > 0) != (picked == Combine::Distinct) {
			return Err("rows pass through a distinct set that a merge cannot replay");
		}
		if !legality.weaker_or_equal(&picked.as_sink()) {
			return Err("the physical sink needs a combine the logical plan does not state");
		}
		let input = if matches!(facts.source, SourceKind::Scan | SourceKind::Record) {
			MorselInput::Range
		} else {
			MorselInput::Batches
		};
		// Only the pipeline that feeds a top-k sort has a threshold to reject entries by.
		Ok(Decision::Morsels {
			input,
			combine: picked,
			paged,
			prunes: picked == Combine::TopK && !legality.may_raise,
		})
	}

	/// The order in which the merge of `decision`'s morsels takes their results: the weakest of
	/// what the logical plan states ([`Legality::arrival`]) and what the merge of the decision's
	/// combine can do without a reader seeing a difference. The concatenation of a statement's
	/// output that no paging counts takes results as the morsels finish where the plan states them
	/// free; a sort that keeps every row over a key range keeps the runs it takes at their
	/// positions; an aggregate combines the partial states of morsels that lie next to one another,
	/// or adds counts in any order where the plan states them free. Every other combine merges in
	/// the order of the serial run, which is the order a top-k's ties, a distinct set's first
	/// occurrences and an ordered apply are defined in. A body whose morsels read batches of rows
	/// the coordinator hands on merges batches whole and in order, except for the output and the
	/// counts that take them free.
	pub(crate) fn merge_order(
		legality: &Legality,
		facts: &PhysicalFacts,
		decision: Decision,
	) -> MergeOrder {
		let Decision::Morsels {
			input,
			combine,
			paged: false,
			..
		} = decision
		else {
			return MergeOrder::Serial;
		};
		let aggregates = matches!(legality.sink, SinkCombine::Aggregate(_));
		let picked = match combine {
			Combine::Concat if facts.output && legality.arrival == Arrival::Free => {
				MergeOrder::Free
			}
			Combine::Aggregate if aggregates && legality.arrival == Arrival::Free => {
				MergeOrder::Free
			}
			Combine::Aggregate
				if aggregates
					&& legality.arrival >= Arrival::Adjacent
					&& input == MorselInput::Range =>
			{
				MergeOrder::Adjacent
			}
			Combine::StableMerge
				if legality.sink == SinkCombine::StableMerge
					&& legality.arrival >= Arrival::Positioned
					&& input == MorselInput::Range =>
			{
				MergeOrder::Positioned
			}
			Combine::Concat
			| Combine::StableMerge
			| Combine::TopK
			| Combine::Aggregate
			| Combine::Distinct
			| Combine::OrderedApply => MergeOrder::Serial,
		};
		debug_assert!(picked.fits(combine));
		picked
	}

	/// The places of the accumulators whose terms the morsels of `decision` keep for the merge to
	/// fold in the order of the serial run: for an aggregate of morsels whose merge takes them in
	/// the order of the serial run, and whose logical plan states its float folds may be replayed
	/// ([`Legality::ordered_fold`]), each accumulator that folds inexact floats, by its place
	/// among the aggregate's calls. Empty for any other pipeline.
	pub(crate) fn ordered_fold(
		legality: &Legality,
		decision: Decision,
		merge: MergeOrder,
	) -> Vec<usize> {
		let SinkCombine::Aggregate(combines) = &legality.sink else {
			return Vec::new();
		};
		if !matches!(
			decision,
			Decision::Morsels {
				combine: Combine::Aggregate,
				..
			}
		) || !legality.ordered_fold
			|| merge != MergeOrder::Serial
		{
			return Vec::new();
		}
		combines
			.iter()
			.enumerate()
			.filter(|(_, combine)| **combine == AggregateCombine::InexactFloat)
			.map(|(place, _)| place)
			.collect()
	}

	/// The decision for a pipeline in a loop: [`Decision::Serial`] where the loop refuses.
	/// `write_cut` is the cut of the iterations of a write loop, where it has one.
	#[allow(dead_code)]
	pub(crate) fn plan_loop(
		legality: &Legality,
		statement: StatementLegality,
		loop_facts: &LoopFacts,
		facts: &PhysicalFacts,
		write_cut: Result<&WriteCut, &'static str>,
	) -> Decision {
		Self::judge_loop(legality, statement, loop_facts, facts, write_cut)
			.unwrap_or(Decision::Serial)
	}

	/// [`Self::plan_loop`], and for a pipeline that runs serially the reason.
	///
	/// The loop's verdict narrows the pipeline's own judgement and never widens it. In order: a
	/// loop whose mode is serial answers the reason analysis gave; a loop that may exit in
	/// iteration order runs serially, since a part would run iterations past the exit; a loop whose
	/// apply is ordered is judged by [`Self::judge_write`]. Any other loop is judged as
	/// [`Self::judge`] judges a pipeline outside a loop.
	#[allow(dead_code)]
	pub(crate) fn judge_loop(
		legality: &Legality,
		statement: StatementLegality,
		loop_facts: &LoopFacts,
		facts: &PhysicalFacts,
		write_cut: Result<&WriteCut, &'static str>,
	) -> Result<Decision, &'static str> {
		if let LoopMode::Serial(refusal) = &loop_facts.mode {
			return Err(loop_refusal_reason(*refusal));
		}
		if matches!(loop_facts.exit, Exit::Ordered(_)) {
			return Err(ORDERED_EXIT);
		}
		if loop_facts.apply == ApplyCut::Ordered {
			return Self::judge_write(legality, statement, facts, write_cut);
		}
		Self::judge(legality, statement, facts)
	}

	/// The decision for the pipeline of a write loop whose apply is ordered: the iterations'
	/// compute halves run as morsels over batches of the rows the coordinator reads, and their
	/// effects are applied by the statement's task in iteration order.
	///
	/// In order: the statement refuses nothing but what its own write is; the logical plan states
	/// an ordered apply that may split; the sink is the write; the rows are not put through
	/// enforcement; the source is a table scan or the ids of a range the coordinator generates (a
	/// read of one record has no second batch to hand on, and an index is written by the body's
	/// own maintenance, so its entries may move under the scan); the write does not page in the
	/// loop; what its expressions nest only reads, afresh for each row; its body holds no block
	/// construct; it reads no record each row names; the write has a cut and its compute half
	/// reads no stored data, which the statement's transaction holds while the apply half writes
	/// it.
	pub(crate) fn judge_write(
		legality: &Legality,
		statement: StatementLegality,
		facts: &PhysicalFacts,
		write_cut: Result<&WriteCut, &'static str>,
	) -> Result<Decision, &'static str> {
		if let Some(refusal) = statement.refusal {
			return Err(chain_refusal_reason(refusal));
		}
		if !legality.may_split() || legality.sink != SinkCombine::OrderedApply {
			return Err("the logical plan does not allow the writes to apply in order");
		}
		if facts.sink != SinkKind::Write {
			return Err("the pipeline's sink is not the write");
		}
		if facts.enforce {
			return Err("rows are put through enforcement");
		}
		if facts.source == SourceKind::Record {
			return Err("the write reads one record, which leaves no second batch to hand on");
		}
		if !matches!(facts.source, SourceKind::Scan | SourceKind::Mock) {
			return Err("the write reads its rows through an access path its own writes may move");
		}
		if facts.limits != 0 {
			return Err("the write pages its rows in the loop");
		}
		if !matches!(facts.nested, NestedUse::None | NestedUse::PerRowReadOnly) {
			return Err("a nested statement is evaluated once, probed, or writes");
		}
		if facts.host_blocks {
			return Err("an expression holds a block construct");
		}
		if facts.fetches_row {
			return Err("the body reads the record each row names");
		}
		if facts.binds_row {
			return Err("a binding step replaces the row");
		}
		let cut = write_cut?;
		if cut.compute_reads_data {
			return Err("the compute half reads stored data");
		}
		Ok(Decision::Morsels {
			input: MorselInput::Batches,
			combine: Combine::OrderedApply,
			paged: false,
			prunes: false,
		})
	}

	/// The bounds of one batch of a loop: the iterations a fan-out batch holds and the bytes of
	/// output it holds before it parks, or the positions one execution of the loop's batched read
	/// covers.
	///
	/// The iterations are the most one read of a key range asks for
	/// ([`crate::kvs::NORMAL_BATCH_SIZE`]) and the bytes are the window a morsel that is not at the
	/// front holds, saturated to `u32`. The bounds are the same whether or not `facts` holds a
	/// plan-time estimate of the pipeline's rows.
	#[allow(dead_code)]
	pub(crate) fn loop_batch(self, _loop_facts: &LoopFacts, _facts: &PhysicalFacts) -> Batch {
		Batch {
			max_iterations: crate::kvs::NORMAL_BATCH_SIZE,
			max_bytes: u32::try_from(self.window_bytes).unwrap_or(u32::MAX),
		}
	}

	/// The iterations of a loop's body one execution of its program covers, in `1..=8`. A loop
	/// is not unrolled: the body covers one iteration.
	#[allow(dead_code)]
	pub(crate) fn unroll(self, _loop_facts: &LoopFacts) -> u8 {
		1
	}

	/// Run time: whether the coordinator running a range, having read `signals.visited` entries of
	/// it, has the gates of a split open: it has read as many entries as the split threshold, no
	/// reader that takes few rows is waiting for the first, the paging does not need fewer rows
	/// than that, and the transaction gives workers a read view where the policy needs one.
	/// [`Self::split`] answers the cut, from what [`Signals::remaining_estimate`] says is left.
	///
	/// The rows the filters passed (`signals.passed`) must also be a share of those entries no
	/// larger than the policy's pass threshold: a scan most of whose rows pass is bound by reading
	/// them, which morsels do not speed up, so it stays whole. A pipeline with no filter passes
	/// every row.
	pub(crate) fn may_split(self, signals: &Signals) -> bool {
		signals.visited >= self.split_rows as u64
			&& self.selective(signals.passed, signals.visited)
			&& self.may_split_unread(signals)
	}

	/// Whether `passed` of `of` rows is a share no larger than the pass threshold.
	fn selective(self, passed: u64, of: u64) -> bool {
		u128::from(passed) * 1_000_000 <= u128::from(self.split_pass_ppm) * u128::from(of)
	}

	/// Whether a share known before the scan opens (`signals.selectivity`) is no larger than the
	/// pass threshold; `false` where none is known.
	fn known_selective(self, signals: &Signals) -> bool {
		signals
			.selectivity
			.and_then(|selectivity| selectivity.selectivity_ppm)
			.is_some_and(|ppm| ppm <= self.split_pass_ppm)
	}

	/// The gates of a split other than the entries read: splits are on, no reader that takes few
	/// rows is waiting for the first, the paging does not need fewer rows than the split
	/// threshold, and the transaction gives workers a read view where the policy needs one. A scan
	/// that learns the share of its passing rows only by asking the storage asks only where these
	/// hold.
	pub(crate) fn may_split_unread(self, signals: &Signals) -> bool {
		if self.split_rows == usize::MAX
			|| signals.scan_hint.is_some()
			|| (self.needs_view && signals.no_view)
		{
			return false;
		}
		// A paging that needs few more rows is reached by the coordinator before morsels
		// would repay their cost.
		!(signals.paged
			&& signals
				.remaining
				.is_some_and(|remaining| signals.skip.saturating_add(remaining) < self.split_rows))
	}

	/// Run time: whether the coordinator asks the storage what a range holds as the scan opens,
	/// for [`Self::split_at_open`]: only when a split could follow from the answer, which needs
	/// a known share of passing rows within the pass threshold.
	pub(crate) fn asks_at_open(self, signals: &Signals) -> bool {
		self.known_selective(signals) && self.may_split_unread(signals)
	}

	/// Run time: what to do with a range as its scan opens, before an entry of it is read, from
	/// what the storage's metadata says it holds (`signals.opened`).
	///
	/// A range is cut here only where the share of its rows the filters pass is known before the
	/// scan opens (`signals.selectivity`) and is within the pass threshold: starting morsels costs
	/// latency that only a selective scan repays. It is then cut into as many morsels as its bytes
	/// fill of the target size, no more than the rows the storage counted where it counted them,
	/// and kept whole when that is fewer than two, when the storage gave no measure, or when a gate
	/// of [`Self::may_split`] other than the entries read is shut. A range kept whole here is still
	/// judged by [`Self::split`] once the coordinator has read the split threshold's entries.
	pub(crate) fn split_at_open(self, signals: &Signals) -> Split {
		if !self.asks_at_open(signals) {
			return Split::Whole;
		}
		let Some(bytes) = signals.opened.and_then(|opened| opened.bytes) else {
			return Split::Whole;
		};
		let rows = signals.opened.and_then(|opened| opened.rows).unwrap_or(u64::MAX);
		self.parts_of(bytes, rows, self.target_bytes as u64)
	}

	/// A remainder of `bytes` in `rows` cut into morsels of about `target` bytes: no more parts
	/// than rows or than a split answers, and whole when that is fewer than two.
	fn parts_of(self, bytes: u64, rows: u64, target: u64) -> Split {
		let parts = bytes
			.div_ceil(target.max(1))
			.min(rows)
			.min(u64::from(surrealdb_kvs::split::MAX_SPLIT_PARTS));
		if parts < 2 {
			Split::Whole
		} else {
			Split::Split {
				target: SplitTarget::Parts(parts as u32),
			}
		}
	}

	/// Run time: what to do with the remainder of the range the coordinator is running, once
	/// [`Self::may_split`] holds.
	///
	/// With the entries and bytes the coordinator has measured the remainder to hold
	/// (`signals.remaining_estimate`), the remainder is cut into as many morsels as it fills of
	/// the target size, no more than it has entries, and kept whole when that is fewer than two:
	/// a scan that ends within a morsel of where it stands costs more split than it saves. With no
	/// measure, the datastore sizes the parts from its own estimate, a morsel being no smaller
	/// than an entry.
	pub(crate) fn split(self, signals: &Signals) -> Split {
		if !self.may_split(signals) {
			return Split::Whole;
		}
		let entry = signals.visited_bytes.checked_div(signals.visited).unwrap_or(1).max(1);
		let target = (self.target_bytes as u64).max(entry);
		match signals.remaining_estimate {
			None => Split::Split {
				target: SplitTarget::Bytes(target),
			},
			Some(left) => self.parts_of(left.bytes, left.keys.unwrap_or(u64::MAX), target),
		}
	}

	/// The entries a coordinator must have read before its measure of what is left of a range
	/// is worth more than the datastore's estimate: a few entries say nothing of how the keys
	/// lie.
	pub(crate) const MEASURED_FROM: u64 = 64;

	/// Run time: the entries the coordinator reads in its next chunk of a range, no more than
	/// `most`: no more than it takes to reach the split threshold while the range is whole.
	pub(crate) fn chunk(self, signals: &Signals, most: u32) -> u32 {
		let most = self.owner_chunk(most);
		if self.split_rows == usize::MAX || signals.visited >= self.split_rows as u64 {
			return most;
		}
		let left = u32::try_from(self.split_rows as u64 - signals.visited).unwrap_or(most);
		most.min(left).max(1)
	}

	/// The entries one read of a key range asks for, before the fuel left and the split threshold
	/// bound it.
	pub(crate) fn scan_chunk(self) -> u32 {
		crate::kvs::NORMAL_BATCH_SIZE
	}

	/// The entries a morsel reads between two chunk boundaries, at which it may donate part of what
	/// is left: `most`, or when every boundary donates, as few as the split threshold.
	pub(crate) fn owner_chunk(self, most: u32) -> u32 {
		if self.steal_always && self.split_rows != usize::MAX {
			u32::try_from(self.split_rows).unwrap_or(most).clamp(1, most)
		} else {
			most
		}
	}

	/// Run time: whether a running morsel donates at every chunk boundary at which its range can
	/// be cut, whether or not a worker is idle.
	pub(crate) fn steal_always(self) -> bool {
		self.steal_always && self.split_rows != usize::MAX
	}

	/// Run time: whether `EXPLAIN ANALYZE` renders the statement's morsel counters.
	pub(crate) fn explain_analyze(self) -> bool {
		self.explain_analyze
	}

	/// Run time: whether an idle worker asks the running morsel with the largest remainder, whose
	/// size `signals.remaining_estimate` estimates, to donate part of it. When every morsel
	/// donates at every boundary it always does; otherwise a remainder must be as large as two
	/// morsels, which a cut repays.
	#[cfg_attr(target_family = "wasm", allow(dead_code))]
	pub(crate) fn steal(self, signals: &Signals) -> bool {
		if self.split_rows == usize::MAX || self.places == 0 {
			return false;
		}
		if self.steal_always {
			return true;
		}
		signals.remaining_estimate.is_some_and(|left| left.bytes >= self.target_bytes_twice())
	}

	/// Run time: whether a running morsel whose remainder `signals.remaining_estimate` estimates
	/// cuts it into morsels of the answered size for others to take. It does while no morsel
	/// waits, the remainder is as large as two morsels, and it has read a few chunks since it was
	/// last cut, twice as many for each cut it has made, which bounds the cuts a wrong estimate
	/// asks for.
	pub(crate) fn resplit(self, signals: &Signals) -> Option<SplitTarget> {
		if self.split_rows == usize::MAX
			|| self.places == 0
			|| signals.queued > 0
			|| signals.since_cut < (2 * u64::from(self.scan_chunk())) << signals.cuts.min(8)
		{
			return None;
		}
		let left = signals.remaining_estimate?;
		if left.bytes < self.target_bytes_twice() {
			return None;
		}
		let entry = signals.visited_bytes.checked_div(signals.visited).unwrap_or(1).max(1);
		Some(SplitTarget::Bytes((self.target_bytes as u64).max(entry)))
	}

	/// Run time: whether a worker that has held its place for `held_quanta` quanta of its morsel
	/// returns it, donating what is left of the morsel first, because another statement waits for
	/// a place.
	#[cfg_attr(target_family = "wasm", allow(dead_code))]
	pub(crate) fn yield_place(self, held_quanta: u32, waiting: bool) -> bool {
		waiting && held_quanta >= self.place_quantum.max(1)
	}

	/// The bytes of two morsels.
	fn target_bytes_twice(self) -> u64 {
		(self.target_bytes as u64).saturating_mul(2)
	}

	/// The places one statement may hold; `0` keeps every morsel on the coordinator.
	#[cfg_attr(target_family = "wasm", allow(dead_code))]
	pub(crate) fn place_cap(self) -> usize {
		self.places
	}

	/// The places of the node's pool workers leave free.
	#[cfg_attr(target_family = "wasm", allow(dead_code))]
	pub(crate) fn oltp_reserve(self) -> usize {
		self.oltp_reserve
	}

	/// The quanta a worker holds a place for before it checks whether another
	/// statement waits for one; at least one.
	pub(crate) fn place_quantum(self) -> u32 {
		self.place_quantum.max(1)
	}

	/// The output bytes a morsel that is not at the front holds before it parks.
	#[cfg_attr(target_family = "wasm", allow(dead_code))]
	pub(crate) fn window_bytes(self) -> usize {
		self.window_bytes
	}

	/// Run time: whether the groups of the morsels of a keyed aggregate wait to be merged together
	/// rather than each being folded into the statement's on its own: a target with workers has
	/// places to merge them on.
	#[cfg_attr(target_family = "wasm", allow(dead_code))]
	pub(crate) fn defers_partials(self) -> bool {
		cfg!(not(target_family = "wasm")) && self.may_use_workers()
	}

	/// Run time: whether the `results` of morsels that wait, holding `groups` groups between them,
	/// are merged now: when as many have gathered as may be in flight, or when two or more hold
	/// enough groups for a merge divided into partitions to repay.
	#[cfg_attr(target_family = "wasm", allow(dead_code))]
	pub(crate) fn partials_ready(self, results: usize, groups: usize) -> bool {
		results >= self.read_ahead() || (results >= 2 && groups >= PARTIAL_WINDOW_GROUPS)
	}

	/// Run time: whether the groups of morsels, `groups` of them between the morsels, repay a merge
	/// divided into partitions: a few groups fold sooner than the pieces of a division are handed
	/// out.
	#[cfg_attr(target_family = "wasm", allow(dead_code))]
	pub(crate) fn worth_partitioning(self, groups: usize) -> bool {
		groups >= MIN_PARTITIONED_GROUPS
	}

	/// Run time: the partitions the key space of the groups of morsels is divided into for their
	/// merge, when the statement can run `width` pieces of the merge at once: its own task and the
	/// places it holds.
	#[cfg_attr(target_family = "wasm", allow(dead_code))]
	pub(crate) fn partitions(self, width: usize) -> usize {
		width.clamp(1, MAX_PARTITIONS)
	}

	/// Run time: the rows a batch run in `budget` is sized to, from the cost measured so far.
	pub(crate) fn batch_rows(self, signals: &Signals, budget: Duration) -> usize {
		let most = crate::kvs::NORMAL_BATCH_SIZE as usize;
		match signals.cost_ns_per_row {
			None => FIRST_BATCH.min(most),
			Some(cost) => ((budget.as_nanos() as f64 / cost.max(1.0)) as usize).clamp(1, most),
		}
	}

	/// Run time: the rows a batch destined for a worker holds before it is handed over.
	pub(crate) fn worker_batch_rows(self, signals: &Signals) -> usize {
		self.batch_rows(signals, WORKER_BUDGET).max(self.split_rows)
	}

	/// Run time: whether a batch may be given a place in the worker budget at all. Targets
	/// without worker threads never ask.
	#[cfg_attr(target_family = "wasm", allow(dead_code))]
	pub(crate) fn may_use_workers(self) -> bool {
		self.split_rows != usize::MAX && self.places > 0
	}

	/// Run time: whether a filled batch of `rows` goes to a worker, which it does when it holds
	/// a place; otherwise it runs on the coordinator. Targets without worker threads never ask.
	#[cfg_attr(target_family = "wasm", allow(dead_code))]
	pub(crate) fn hands_to_worker(self, rows: usize, holds_place: bool) -> bool {
		holds_place && rows >= self.split_rows
	}

	/// Run time: how many batches may be in flight ahead of the one the merge waits for: twice the
	/// node's cores, and at least as many as the host's places run at once for the scan.
	pub(crate) fn read_ahead(self) -> usize {
		#[cfg(not(target_family = "wasm"))]
		let node = 2 * node_cores();
		#[cfg(target_family = "wasm")]
		let node = 1;
		node.max(self.host_width)
	}

	/// Run time: the cost of a row, in nanoseconds, once a batch that took `cost` per row has
	/// been merged: an exponentially weighted average over the batches merged so far, `seen`.
	pub(crate) fn smooth_cost(self, seen: Option<f64>, cost: f64) -> f64 {
		match seen {
			Some(seen) => 0.7 * seen + 0.3 * cost,
			None => cost,
		}
	}

	/// Run time: the rows a paged pipeline may have in flight before the merge takes the batch
	/// at the front. For a paged pipeline that is the rows its paging still needs, scaled by the
	/// share of merged rows that reached the paging; for one that pages nothing itself, the
	/// rows its reader takes before it may stop. `None` when every row is needed.
	pub(crate) fn window(self, signals: &Signals) -> Option<usize> {
		if !signals.paged {
			return signals.scan_hint.map(|reads| (reads as usize).max(1));
		}
		let needed = signals.skip.saturating_add(signals.remaining?).max(1);
		if signals.merged == 0 {
			return Some(needed);
		}
		if signals.reached == 0 {
			return None;
		}
		let scaled = needed as u128 * signals.merged as u128 / signals.reached as u128;
		Some(usize::try_from(scaled).unwrap_or(usize::MAX))
	}
}

/// The reason a statement's refusal makes a pipeline run serially.
fn chain_refusal_reason(refusal: ChainRefusal) -> &'static str {
	match refusal {
		ChainRefusal::Writes => "the statement writes data or something outside the database",
		ChainRefusal::AdvancesState => "the statement advances state that outlives it",
		ChainRefusal::Entropy => "the statement draws entropy",
		ChainRefusal::Blocks => "the statement blocks",
		ChainRefusal::Opaque => "the statement runs a body the plan cannot see",
		ChainRefusal::External => "the statement reads outside the transaction",
	}
}

/// The reason a loop whose mode is serial runs serially: one fixed string for each refusal and
/// each barrier cause, and for an effect the string of the chain refusal that names it.
#[deny(clippy::wildcard_enum_match_arm)]
fn loop_refusal_reason(refusal: LoopRefusal) -> &'static str {
	match refusal {
		LoopRefusal::Unsplittable => "the loop's source cannot be split",
		LoopRefusal::Carried => "an iteration carries state into the next",
		LoopRefusal::OrderedExit => ORDERED_EXIT,
		LoopRefusal::Effect(refusal) => chain_refusal_reason(refusal),
		LoopRefusal::Barrier(cause) => match cause {
			BarrierCause::CatalogChange => "the loop body changes the catalog",
			BarrierCause::Use => "the loop body switches namespace or database",
			BarrierCause::SessionWrite => "the loop body writes the session",
			BarrierCause::Sleep => "the loop body sleeps",
			BarrierCause::Opaque => "the loop body has effects the plan cannot see",
			BarrierCause::AsyncEvent => "the loop body queues work an event runs later",
			BarrierCause::PeeledFirst => "the loop body is peeled off as its first iteration",
			BarrierCause::ImplicitTable => "the loop body defines a table implicitly",
			BarrierCause::UnpinnedDynamicTarget => "the loop body writes a target no fact pins",
		},
		LoopRefusal::NoCombine => "the loop's sink has no combine",
		LoopRefusal::SourceWritten => {
			"the loop body, or the code its table stores, may write a record of the table its source reads at another key"
		}
		LoopRefusal::MayRepeatWrite => "two iterations may write one key",
	}
}

/// Whether the sink of `legality` combines the partial results of morsels: not a sink that holds
/// state a part cannot own, and not an aggregate with a function that has no combine over partial
/// states. A float fold's partial states combine, and [`Policy::judge`] declines the split because
/// the answer must equal the serial fold bit for bit, unless the morsels keep the terms for the
/// merge to fold in the order of the serial run ([`Legality::ordered_fold`]). An ordered apply
/// combines, by applying the parts' effects in iteration order.
fn sink_combines(legality: &Legality) -> bool {
	match &legality.sink {
		SinkCombine::NotSplittable => false,
		SinkCombine::Aggregate(combines) => combines.iter().all(|combine| {
			matches!(
				combine,
				AggregateCombine::Exact
					| AggregateCombine::Appending
					| AggregateCombine::InexactFloat
			)
		}),
		SinkCombine::Concat
		| SinkCombine::StableMerge
		| SinkCombine::TopK
		| SinkCombine::Distinct
		| SinkCombine::OrderedApply => true,
	}
}

/// Whether the sink of `legality` folds a float: an aggregate with a function whose partial
/// states combine to a value that depends on how the terms were grouped.
fn folds_floats(legality: &Legality) -> bool {
	matches!(
		&legality.sink,
		SinkCombine::Aggregate(combines) if combines.contains(&AggregateCombine::InexactFloat)
	)
}

/// The node's cores, as [`std::thread::available_parallelism`] answers once for the process: 1
/// when it cannot tell.
///
/// The answer is read once and kept. On Linux each call of `available_parallelism` reads the
/// process's cgroup files (an `open`, a `read` and a `close` apiece), which a scan that asks once
/// per batch pays on every batch of every concurrent scan.
#[cfg(not(target_family = "wasm"))]
pub(crate) fn node_cores() -> usize {
	static CORES: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
	*CORES.get_or_init(|| std::thread::available_parallelism().map_or(1, |cores| cores.get()))
}

#[cfg(test)]
thread_local! {
	/// Whether [`fact_assertions`] holds on this thread.
	pub(crate) static FACT_ASSERTIONS: std::cell::Cell<bool> = const { std::cell::Cell::new(true) };
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::logical::pipelines::{Arrival, ChainLegality, ErrorOrder, Paging};

	/// The legality and facts of a pipeline that scans a table into an aggregate over `combines`.
	fn aggregate(combines: Vec<AggregateCombine>) -> (Legality, PhysicalFacts) {
		let legality = Legality {
			source: SourceSplit::Range,
			chain: ChainLegality::RowLocal,
			sink: SinkCombine::Aggregate(combines),
			paging: Paging::None,
			arrival: Arrival::Serial,
			ordered_fold: false,
			error_order: ErrorOrder::FirstByInputPosition,
			may_raise: false,
		};
		let facts = PhysicalFacts {
			source: SourceKind::Scan,
			enforce: false,
			writes: false,
			limits: 0,
			output: false,
			sink: SinkKind::Aggregate,
			binds_row: false,
			fetches_row: false,
			distincts: 0,
			nested: NestedUse::None,
			host_blocks: false,
			paging_replayable: false,
			estimate: None,
		};
		(legality, facts)
	}

	/// A fold of floats depends on how its terms are grouped, so the policy declines it whatever
	/// the logical plan states of it, while an aggregate whose states combine exactly splits.
	#[test]
	fn a_float_fold_is_declined() {
		let statement = StatementLegality {
			refusal: None,
		};
		let (legality, facts) =
			aggregate(vec![AggregateCombine::Exact, AggregateCombine::Appending]);
		assert!(matches!(
			Policy::judge(&legality, statement, &facts),
			Ok(Decision::Morsels {
				combine: Combine::Aggregate,
				..
			})
		));
		for combines in [
			vec![AggregateCombine::InexactFloat],
			vec![AggregateCombine::Exact, AggregateCombine::InexactFloat],
		] {
			let (legality, facts) = aggregate(combines);
			assert!(legality.may_split());
			assert_eq!(
				Policy::judge(&legality, statement, &facts),
				Err("a float fold depends on how its terms are grouped")
			);
		}
	}

	/// A range is cut into as many morsels as what is left of it fills, once the coordinator has
	/// read the split threshold's entries, and kept whole where it holds fewer than two morsels or
	/// no worker can read it.
	#[test]
	fn a_range_is_cut_by_what_is_left_of_it() {
		let policy = Policy::from_config(&ExecConfig::default());
		let blind = Policy::from_config(&ExecConfig {
			morsel_needs_view: false,
			..ExecConfig::default()
		});
		let target = ExecConfig::default().morsel_target_bytes as u64;
		let read = Signals {
			visited: 256,
			visited_bytes: 256_000,
			passed: 2,
			..Signals::default()
		};
		let left = |bytes: u64, keys: u64| Signals {
			remaining_estimate: Some(Estimate {
				keys: Some(keys),
				bytes,
				exact: false,
			}),
			..read
		};
		assert_eq!(
			policy.split(&left(10 * target, 10_000)),
			Split::Split {
				target: SplitTarget::Parts(10)
			},
			"ten morsels' worth"
		);
		assert_eq!(
			policy.split(&left(100 * target, 5)),
			Split::Split {
				target: SplitTarget::Parts(5)
			},
			"no more morsels than entries"
		);
		assert_eq!(policy.split(&left(target, 1_000)), Split::Whole, "one morsel's worth");
		assert_eq!(
			policy.split(&Signals {
				visited: 255,
				..left(10 * target, 10_000)
			}),
			Split::Whole,
			"fewer entries than the threshold"
		);
		let no_view = Signals {
			no_view: true,
			..left(10 * target, 10_000)
		};
		assert_eq!(policy.split(&no_view), Split::Whole, "no worker can read a part");
		assert!(matches!(blind.split(&no_view), Split::Split { .. }));
		assert_eq!(
			policy.split(&read),
			Split::Split {
				target: SplitTarget::Bytes(target)
			},
			"unmeasured: the datastore sizes the parts"
		);
	}

	/// Past the split threshold, a range is cut only where few of the entries read passed the
	/// filters: a scan passing one row in a hundred splits, one passing nine in ten or every row
	/// (no filter) stays whole, and the threshold is the configured share.
	#[test]
	fn a_range_is_cut_only_where_few_rows_pass() {
		let policy = Policy::from_config(&ExecConfig {
			morsel_needs_view: false,
			..ExecConfig::default()
		});
		let target = ExecConfig::default().morsel_target_bytes as u64;
		let read = |passed: u64| Signals {
			visited: 1_000,
			visited_bytes: 1_000_000,
			passed,
			remaining_estimate: Some(Estimate {
				keys: Some(100_000),
				bytes: 10 * target,
				exact: false,
			}),
			..Signals::default()
		};
		assert!(policy.may_split(&read(10)), "one in a hundred passes");
		assert!(matches!(policy.split(&read(10)), Split::Split { .. }));
		assert!(!policy.may_split(&read(900)), "nine in ten pass");
		assert_eq!(policy.split(&read(900)), Split::Whole);
		assert!(!policy.may_split(&read(1_000)), "no filter passes every row");
		let threshold = u64::from(ExecConfig::default().morsel_split_pass_ppm) / 1_000;
		assert!(policy.may_split(&read(threshold)), "the threshold itself splits");
		assert!(!policy.may_split(&read(threshold + 1)));
		let any = Policy::from_config(&ExecConfig {
			morsel_needs_view: false,
			morsel_split_pass_ppm: 1_000_000,
			..ExecConfig::default()
		});
		assert!(any.may_split(&read(1_000)), "a threshold of a million cuts whatever passes");
	}

	/// As a scan opens, a range the storage measures as many morsels' worth is cut before an entry
	/// of it is read, and a small or unmeasured one is kept whole for the coordinator to judge
	/// once it has read the split threshold's entries.
	#[test]
	fn a_range_the_storage_measures_large_is_cut_as_it_opens() {
		use crate::exec::ir::iface::EstimateSource;
		let policy = Policy::from_config(&ExecConfig::default());
		let target = ExecConfig::default().morsel_target_bytes as u64;
		let one_in_a_hundred = PlanEstimate {
			rows: None,
			bytes: None,
			selectivity_ppm: Some(10_000),
			exact: false,
			source: EstimateSource::KvRange,
		};
		let selective = Some(one_in_a_hundred);
		let opened = |bytes: u64, rows: Option<u64>| Signals {
			opened: Some(PlanEstimate {
				rows,
				bytes: Some(bytes),
				selectivity_ppm: None,
				exact: false,
				source: EstimateSource::KvRange,
			}),
			selectivity: selective,
			..Signals::default()
		};
		assert_eq!(
			policy.split_at_open(&opened(10 * target, None)),
			Split::Split {
				target: SplitTarget::Parts(10)
			},
			"ten morsels' worth of bytes, rows uncounted"
		);
		assert_eq!(
			policy.split_at_open(&opened(10 * target, Some(3))),
			Split::Split {
				target: SplitTarget::Parts(3)
			},
			"no more morsels than counted rows"
		);
		assert_eq!(policy.split_at_open(&opened(target, None)), Split::Whole, "one morsel's worth");
		let unmeasured = Signals {
			selectivity: selective,
			..Signals::default()
		};
		assert_eq!(policy.split_at_open(&unmeasured), Split::Whole, "no measure");
		assert!(policy.asks_at_open(&unmeasured));
		let unknown = Signals {
			selectivity: None,
			..opened(10 * target, None)
		};
		assert!(!policy.asks_at_open(&unknown), "no share of passing rows is known");
		assert_eq!(policy.split_at_open(&unknown), Split::Whole);
		let broad = Signals {
			selectivity: Some(PlanEstimate {
				selectivity_ppm: Some(900_000),
				..one_in_a_hundred
			}),
			..opened(10 * target, None)
		};
		assert!(!policy.asks_at_open(&broad), "most rows pass");
		assert_eq!(policy.split_at_open(&broad), Split::Whole);
		let hinted = Signals {
			scan_hint: Some(1),
			..opened(10 * target, None)
		};
		assert!(!policy.asks_at_open(&hinted), "a reader waits for its first rows");
		assert_eq!(policy.split_at_open(&hinted), Split::Whole);
		let no_view = Signals {
			no_view: true,
			..opened(10 * target, None)
		};
		assert_eq!(policy.split_at_open(&no_view), Split::Whole, "no worker can read a part");
		let serial = Policy::from_config(&ExecConfig {
			morsel_split_rows: usize::MAX,
			..ExecConfig::default()
		});
		assert!(!serial.asks_at_open(&unmeasured), "splits are off");
	}
}
