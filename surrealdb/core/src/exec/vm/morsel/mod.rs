//! A read pipeline's rows run as morsels: parts of its input run on the statement's task or on
//! workers of their own, their results merged in the order of the serial run, or as the morsels
//! finish where nothing reads the rows in order ([`MergeOrder::Free`]).
//!
//! The serial run is what a morsel pipeline means. The pipeline's body only reads, and fills the
//! statement's output, a sort, or an aggregate whose partial states combine exactly
//! ([`Policy::plan`](crate::exec::planner::placement::Policy::plan)). Each morsel runs on a state
//! of its own, so what it produces — rows for the output, entries for a sort, partial groups for an
//! aggregate — depends on its rows alone. Merging those results in the order the source yields the
//! morsels answers what the serial run answers:
//!
//! - the output is the morsels' rows concatenated in order;
//! - a sort that keeps every row receives the morsels' entries in order, each morsel's already
//!   sorted into a run ([`sorted`]), and its sort is stable, so entries that tie keep the order the
//!   serial run gives them; a sort that keeps only the rows its limit can reach prunes each morsel
//!   as the serial run prunes the whole, which keeps every row the serial run keeps, and the
//!   morsels' sorts share one threshold, which only tightens;
//! - an aggregate folds each morsel's partial group into the group it has, in order, so an
//!   accumulator that appends keeps the serial order and a non-aggregate column keeps the value the
//!   earliest morsel evaluated.
//!
//! A morsel whose body is not paged charges its breakers to the query's lease, as the statement
//! does, where the query can spill. A breaker the lease does not cover spills within the morsel,
//! and the morsel hands the merge a [`Partial`](super::partial::Partial) for it in place of its
//! buffer. To absorb it, the merge first writes what the statement holds for that breaker as a run.
//! It then appends the morsel's runs and takes the morsel's held records as the statement's buffer.
//! Every record the statement holds therefore precedes the morsel's, and the statement's runs stay
//! in serial arrival order. A breaker whose state cannot be saved, or `GROUP ALL`, never spills in
//! a morsel.
//!
//! Where the merge takes results in the order of the serial run, a failure is the failure of the
//! earliest morsel that fails, or of the source when no morsel before it failed, raised once every
//! earlier morsel's results are merged, which is where the serial run raises it. The error is the
//! serial run's, never a wrapper. A merge that takes results as morsels finish raises the failure
//! it meets first, as the next section states.
//!
//! # The order results are merged in
//!
//! The merge takes the morsels' results in the order of the serial run, except for the pipeline
//! whose plan merges freely ([`MergeOrder::Free`]): the rows of a statement's result that no
//! `ORDER BY` sorts, no paging counts, no distinct set deduplicates and no operator reads as its
//! input, and that no `LET` binds for the statements after it to read in order. Its rows are
//! unordered, so the merge takes a morsel's rows as soon as the morsel finishes, whatever the
//! morsels before it are doing, and the rows reach the reader in the order the morsels finished. A
//! morsel that takes long therefore holds back no morsel after it: the places it would have kept
//! busy run further morsels, and the output a morsel holds before it is merged stays within the
//! window and the read-ahead that bound every merge. A failure of such a pipeline is the failure of
//! the first morsel the merge meets that has failed, which is one of the failures of the rows that
//! fail; the statement fails and answers no row of those after it. The same holds for an aggregate
//! of one group whose every function is `count`, whose partial counts add to one total in any
//! order. Every other pipeline, a sort, an aggregate, a paged or distinct output, merges in the
//! order of the serial run, which is the order its ties, its appends and its first occurrences are
//! defined in.
//!
//! An aggregate whose partial states combine associatively ([`MergeOrder::Adjacent`]) takes the
//! results of a key range's morsels as the morsels finish, and merges them in the order of the
//! scan. The coordinator holds a finished morsel's results until no morsel lies before them, and
//! then absorbs them as the merge of the serial run does, so an aggregate's appends and first
//! values are the serial run's. While it waits for the morsels before them it combines the results
//! it holds of morsels that lie next to one another, the earlier before the later: the fold of a
//! run of adjacent morsels is the fold of the morsels in order, however the run was put together,
//! and the combined partial states are what the statement then folds in one step. The combining
//! runs on the coordinator, which charges the rows it folds to the thread's fuel. A result the
//! coordinator holds counts against the morsels in flight as a running morsel does, so only
//! combining, which turns several held results into one, frees a place for another morsel.
//!
//! A sort that keeps every row over a key range ([`MergeOrder::Positioned`]) takes the sorted runs
//! of its morsels as the morsels finish, and holds each at the position of its morsel in the scan,
//! which is where the sort's ties are defined: the run's own order key is the tie tag of every
//! entry in it, and an entry's place in its run, which is sorted stably, is its row index among
//! the entries that tie. The coordinator absorbs the held runs into the statement's sort in the
//! order of the scan, once no morsel lies before them, so the sort, which is stable and merges
//! runs a quantum at a time, meets equal keys in the order of the serial run. A run held in
//! memory takes none of the places of the morsels in flight where the morsel that built it charged
//! its entries to the query's lease, which a morsel does only where the query can spill: holding
//! them then takes no memory the statement's sort would not, and the morsels behind a slow one go
//! on. A run of a query with no store to spill to is held uncharged and keeps its place, so the
//! runs held stay within the policy's read-ahead. A spilled run and a failure keep their places
//! and are absorbed or raised only when no morsel lies before them, so the first failure and the
//! order of a sort's spilled runs are the serial run's. Where the morsels read batches of rows the
//! coordinator hands on, whole batches merge in order.
//!
//! The state of a float fold (`math::sum`, `mean`, `stddev`, `variance`) of one group depends on
//! the order its terms are folded in, and the partial states of morsels combine to other bits than
//! the serial fold's. The morsels of such an aggregate keep the terms their rows hand to those
//! folds instead of folding them (`fold`), and the merge, which takes them in the order of the
//! serial run, folds the kept terms into the statement's accumulators as it takes each morsel.
//!
//! A body that pages its rows to the statement's output runs each morsel as though every row were
//! taken, but for the rows `START` is sure to skip — those it still skips when every row in flight
//! ahead of the morsel reaches the paging — and the merge pages them: a morsel records, for each of
//! its rows, whether the row reached the paging, whether the morsel skipped it, what it emitted,
//! and how it failed. The merge then replays the paging over the rows in order, as the serial run
//! counts them:
//!
//! - a row that fails before its paging raises its failure;
//! - a row `START` skips is dropped, and so is any failure its projection raised, which the serial
//!   run never evaluates;
//! - a row taken counts against `LIMIT`, raises its projection's failure or hands on its rows, and
//!   once the limit admits no more the pipeline stops: the morsels after it are dropped, their
//!   workers aborted, and the source is read no further, so a failure past the limit — of a row, a
//!   morsel, or the source — is never raised.
//!
//! A morsel stops at its first row that fails before its paging, since no row after it can be
//! reached. The rows in flight are bounded by the rows still needed: what `START` and `LIMIT` have
//! left, scaled by the share of rows that have reached the paging so far. A small limit runs a
//! morsel about its own size, and the window widens as the merged morsels show a filter passing
//! fewer rows.
//!
//! # Batches input
//!
//! The coordinator — the statement's task — reads the source and gathers decoded rows into
//! batches, each batch one morsel. How wide the pipeline runs is decided per batch, as the batch is
//! filled: a batch runs on the coordinator unless it holds at least
//! [`morsel_split_rows`](crate::exec::config::ExecConfig::morsel_split_rows) rows and the node's
//! pool has a free place ([`places`]), which the batch holds until it is done. A pipeline
//! therefore widens while the node has idle cores and runs serially while other statements hold
//! them. Batches are sized from the cost per row measured so far: one run on the coordinator to
//! about a millisecond, so the task yields its place often; one handed to a worker to several, so
//! a spawn is repaid. A worker runs under the statement's context and frames, and drives what
//! nests beneath its body on its own frame stack. On wasm there are no workers and every batch runs
//! on the coordinator.
//!
//! # A write's records
//!
//! The rows of a write over a table scan or a record range run through the same batches, and the
//! write runs in two halves ([`write_set`]). The stages its plan marks as compute (the permission
//! checks, the condition, the data clauses and the schema) run on a worker over each row of a
//! batch and hand the coordinator a candidate: the record in flight, its write context and where
//! the body stopped. The stages it marks as apply run on the statement's task, one candidate after
//! another in the order the rows were read, so every write, index and reference update, event,
//! notification, change feed entry and output happens in the order of the serial run. A worker
//! writes nothing and reads no stored data: the statement's task reads the record-type marker of
//! each row it hands on, and the compute half takes it with the row; what a record's compute half
//! raises is raised when its candidate is taken, after the records before it
//! have been applied.
//!
//! # Range input
//!
//! A body that runs in the storage cursor over a table scan or a record range
//! ([`MorselInput::Range`]) reads its own key ranges. The coordinator starts as the only morsel,
//! over the whole range, on the statement's state and through the transaction's own cursor,
//! emitting as the serial run does. Once it has visited
//! [`morsel_split_rows`](crate::exec::config::ExecConfig::morsel_split_rows) entries the policy may
//! ask it to cut what is left: the datastore's `split` divides the remainder, the coordinator
//! keeps the first part in scan order and stops at its end, and the rest are queued as morsels
//! ordered by their position in the scan (`OrderKey`). One snapshot serves them all: workers open
//! cursors on the transaction's read view, `Snapshot::open`, and re-aim one cursor at each morsel
//! they take; where the backend gives no view the coordinator runs the queued morsels itself, in
//! order, which answers the same at width one.
//!
//! - A morsel whose pipeline emits rows to the statement's output hands them to the coordinator as
//!   it reads them ([`outbox`]): the rows of each chunk go into a bounded queue of the morsel, and
//!   the coordinator takes them while the morsel runs, from the morsel at the front or, where the
//!   merge is free, from any. A morsel counts the rows it has queued against its window, so one the
//!   coordinator is not taking from parks as it does when it holds its output until it ends.
//! - A worker takes the earliest queued morsel and runs each entry in place on a state of its own,
//!   chunk by chunk under the thread's fuel, like the coordinator's reads. It takes none while as
//!   many morsels are running or finished and unmerged as the policy allows, and stops at a chunk
//!   boundary once the statement does not need it.
//! - The coordinator merges in order. A finished morsel is absorbed as a batch is; a running one is
//!   awaited; a queued one is run on the statement's own state, which makes it the front. A merge
//!   that takes results as morsels finish absorbs any finished morsel first, then runs the earliest
//!   queued one on the statement's own state, and awaits a running one only when none is finished
//!   or queued.
//! - A morsel that is not at the front and holds as much output as the window allows parks at a
//!   chunk boundary: its result so far is finished and the rest of its range is queued at the
//!   position where it stopped, which orders it where the serial run reads it.
//! - A failure is recorded by its morsel; the morsels after it are not needed, and the coordinator
//!   raises it once every morsel before it has merged. A paged body's morsels run each entry as
//!   taken and record how it ended, and the merge replays the paging; once the limit is spent the
//!   queue is dropped and the workers stop, and a worker stops a morsel once as many entries
//!   reached its paging as the statement could take at all.
//! - A running morsel gives part of what it has left to the morsels that wait ([`steal`]): at the
//!   end of a chunk its owner splits the unread remainder, keeps the first part in scan order and
//!   queues the others at their order keys, between its own range and whatever followed it. An idle
//!   worker asks the running morsel with the largest estimated remainder to do so; a morsel with a
//!   large remainder and an empty queue does so unasked; with `morsel_steal_always` every morsel
//!   does so at every chunk boundary. A cursor is never touched by a thread other than its
//!   morsel's: the owner narrows its own cursor, and an owner with no boundary to give declines. A
//!   split that fails finds no boundary, and a cursor that refuses to narrow leaves the range it
//!   has to read ([`RangeCursor::narrow`](surrealdb_kvs::RangeCursor::narrow)), so the owner reads
//!   on and gives nothing away: a failure to cut a range never fails the scan.
//! - The node's places are dispatched to statements ([`places`]): a free place is owed to the
//!   waiting statement holding the fewest, and a worker returns its place at the end of a morsel,
//!   or after `morsel_place_quantum` chunks, while another statement waits, parking first so its
//!   remainder is queued. A run registers once, with the first set that wants a place, and every
//!   set of the run draws on that one registration, so the run never holds more places than the
//!   policy's cap however many scans it runs.
//! - What workers read through the view is counted into the transaction's scan metrics a chunk at a
//!   time, as the reads of the transaction's own cursors are, so the metrics do not depend on how
//!   wide the scan ran.
//! - Every worker is awaited before the scan returns, whatever it returns, so the transaction's
//!   commit or cancel never waits on a cursor longer than one chunk. A scan dropped half way aborts
//!   its workers.
//!
//! # The fan-out lease
//!
//! The bytes a run holds only because it fanned out, a worker's copy of the statement's slots, are
//! a revocable lease. The coordinator asks for a grant of them before it hands a batch
//! or a worker its copy, so a query never fails on a ceiling that only the copies pass. A grant
//! the query's memory cannot admit is refused, which revokes the lease for the rest of the run:
//! the batch runs on the coordinator, as when no place is free, no later batch or morsel goes to a
//! worker, and a Range morsel already on a worker parks at its next chunk boundary, leaving the
//! coordinator to run the rest of its range in order. The answer is the serial run's either way.
//!
//! # Held-value input
//!
//! The rows of a value the VM holds, an array, a set, the rows of a nested statement or an integer
//! range, run through [`Rows`] as a scan's decoded rows do ([`held`]): as `Batches` morsels when
//! the pipeline's decision is `Morsels` with `Batches` input, and one at a time otherwise.
//!
//! What a scan's morsels did is counted in [`MorselMetrics`], whether or not it is rendered.

// A target without worker threads runs every morsel on the coordinator and never reads a view, so
// what only workers use goes unused there.
#![cfg_attr(target_family = "wasm", allow(dead_code))]

mod fold;
mod grouped;
mod held;
mod lease;
mod merge;
mod metrics;
mod outbox;
pub(crate) mod places;
mod remote;
pub(crate) use self::remote::run_leaf;
mod run;
mod set;
mod sorted;
mod steal;
#[cfg(test)]
mod test_hooks;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_lease;
pub(crate) mod wire;
mod write_compute;
mod write_set;

use std::sync::Arc;

#[allow(unused_imports, reason = "request L16-4 calls `run_held_rows` from `run_source`")]
pub(super) use self::held::{HeldRows, run_held_rows};
pub(super) use self::merge::body_breakers;
pub(crate) use self::metrics::MorselMetrics;
pub(super) use self::places::Statement;
use self::set::{MorselSet, RangeSet};
#[cfg(all(test, feature = "kv-mem"))]
pub(super) use self::test_hooks::{
	Counts, batch_partials, batches_of, counts_of, delay_morsels, encoded_ahead, fail_splits,
	forget_ends, metrics_of, seed_opened, txn_reads_on_workers, writes_ended,
};
#[cfg(all(test, feature = "kv-mem", feature = "kv-surrealkv"))]
pub(super) use self::test_hooks::{fail_grants, fail_narrows, waiting_statement};
#[cfg(test)]
pub(crate) use self::test_hooks::{
	forget_datastore, note_encoded_ahead, note_txn_read_on_worker, note_write_ended,
};
pub(super) use self::write_compute::Stop;
use self::write_set::WriteSet;
use super::host::{Ended, Out, flush, run_value_row};
use super::interp::State;
use super::program::{Pipeline, Program};
use crate::exec::planner::placement::{Combine, MergeOrder, MorselInput, MorselPlan, Parallel};
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::ControlFlow;
use crate::val::Value;

/// Run `pipeline`, one of `program`'s, whose body runs in the storage cursor, over every record of
/// `range` as morsels: the coordinator reads the front of the range through the transaction's
/// cursor, as a scan that runs whole does, and the rest is cut into morsels that workers read
/// through the transaction's read view or the coordinator reads itself, in the order of the scan.
/// No more than `first` records are read before the rows are first handed on, where it is given.
/// `true` when the pipeline takes no more rows.
#[expect(
	clippy::too_many_arguments,
	reason = "a scan with the program, pipeline, range, direction, version, hint, state, context and output"
)]
pub(super) async fn scan_range_in_place(
	program: &Arc<Program>,
	pipeline: &Pipeline,
	range: crate::key::RawRange,
	direction: crate::kvs::Direction,
	version: Option<u64>,
	first: Option<u32>,
	layouts: Option<Arc<crate::catalog::TableLayouts>>,
	state: &mut State,
	ctx: &ExecutionContext,
	yielder: &mut Out<'_, '_>,
	flush_at: usize,
) -> FlowResult<bool> {
	let Some(at) = program.pipelines.iter().position(|p| std::ptr::eq(p, pipeline)) else {
		return Err(ControlFlow::Err(anyhow::anyhow!(
			"a scan of a pipeline its program does not hold"
		)));
	};
	// Morsels publish the thresholds of their own sorts into one cell, so a threshold may belong to
	// rows that come after the row a scan is judging: a tie with it is never a reason to reject.
	// A body that may raise runs every entry, as the serial scan does.
	let prunes = matches!(
		pipeline.parallel,
		Parallel::Morsels(MorselPlan {
			prunes: true,
			..
		})
	);
	let probe =
		super::host::armed_probe(pipeline, state).filter(|_| prunes).map(|probe| probe.ties_kept());
	RangeSet::new(program, at, direction, version, probe, layouts, ctx)
		.run(range, first, state, ctx, yielder, flush_at)
		.await
}

/// How the VM runs the morsels the physical plan decided for a pipeline whose body runs in the
/// storage cursor, or does not (`in_place`, the code lowering emitted).
///
/// The physical plan answers [`MorselInput::Range`] for a table scan or a record range and
/// [`MorselInput::Batches`] for every other source it splits. The body narrows that answer, and
/// never widens it:
///
/// | decided | body in the cursor | the VM runs |
/// |---|---|---|
/// | `Range` (a scan or a record range) | yes | `Range`, workers read their key ranges in place |
/// | `Range` (a scan or a record range) | no | `Batches` over decoded rows |
/// | `Batches` (any other source that splits) | either | `Batches`: only a key range is read in a cursor |
/// | serial | either | serial |
///
/// This is the one implementation of the table: the readers of a pipeline's decision and the
/// recorders of the placement of a program call it.
pub(crate) fn effective_mode(decided: MorselInput, in_place: bool) -> MorselInput {
	match (decided, in_place) {
		(MorselInput::Range, true) => MorselInput::Range,
		(MorselInput::Range, false) | (MorselInput::Batches, _) => MorselInput::Batches,
	}
}

/// [`effective_mode`] of `pipeline`: how its morsels run, or `None` when it runs serially.
pub(crate) fn effective_input(pipeline: &Pipeline) -> Option<MorselInput> {
	effective_input_of(pipeline, pipeline.in_place)
}

/// [`effective_input`] of a run of `pipeline` that runs its body on the stored row where
/// `in_place` says so ([`runs_in_place`](super::host::runs_in_place)).
pub(in crate::exec::vm) fn effective_input_of(
	pipeline: &Pipeline,
	in_place: bool,
) -> Option<MorselInput> {
	match &pipeline.parallel {
		Parallel::Serial => None,
		Parallel::Morsels(plan) => Some(effective_mode(plan.input, in_place)),
	}
}

/// The order the plan merges `pipeline`'s morsels in ([`MorselPlan::merge`]); the serial order for
/// a pipeline that runs serially.
fn merge_of(pipeline: &Pipeline) -> MergeOrder {
	match &pipeline.parallel {
		Parallel::Morsels(plan) => plan.merge,
		Parallel::Serial => MergeOrder::Serial,
	}
}

/// Whether `pipeline`'s morsels compute the records of a write and the statement's task applies
/// them in order.
fn applies_in_order(pipeline: &Pipeline) -> bool {
	matches!(
		&pipeline.parallel,
		Parallel::Morsels(MorselPlan {
			combine: Combine::OrderedApply,
			..
		})
	)
}

/// Where a pipeline's rows go as a storage read yields them: run one at a time, or gathered into
/// morsels.
pub(super) enum Rows<'p> {
	Serial,
	Morsels(Box<MorselSet<'p>>),
	/// The records of a write whose compute halves run as morsels.
	Writes(Box<WriteSet<'p>>),
	/// A pipeline planned as an ordered apply whose body holds no cut. Lowering refuses such a
	/// program, so no run holds one; a row, or the end of the source, raises the internal error.
	Unplaced,
}

/// The internal error of a pipeline planned as an ordered apply that holds no write cut.
fn unplaced() -> ControlFlow {
	ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(
		"a pipeline planned as an ordered apply holds no write cut".to_owned(),
	)))
}

impl<'p> Rows<'p> {
	/// The rows of `pipeline`, one of `program`'s: morsels when the physical plan planned it as
	/// them.
	pub(super) fn of(
		program: &'p Arc<Program>,
		pipeline: &Pipeline,
		ctx: &ExecutionContext,
	) -> Self {
		Self::of_run(program, pipeline, pipeline.in_place, ctx)
	}

	/// [`Self::of`] for a run of `pipeline` that runs its body on the stored row where `in_place`
	/// says so ([`runs_in_place`](super::host::runs_in_place)): a run of a pipeline planned to that
	/// reads its rows decoded reads them as morsels of batches where the plan parallelises it.
	pub(super) fn of_run(
		program: &'p Arc<Program>,
		pipeline: &Pipeline,
		in_place: bool,
		ctx: &ExecutionContext,
	) -> Self {
		match program.pipelines.iter().position(|p| std::ptr::eq(p, pipeline)) {
			Some(at) if effective_input_of(pipeline, in_place) == Some(MorselInput::Batches) => {
				if !applies_in_order(pipeline) {
					Rows::Morsels(Box::new(MorselSet::new(program, at, ctx)))
				} else if write_set::holds_cut(pipeline) {
					Rows::Writes(Box::new(WriteSet::new(program, at, ctx)))
				} else {
					Rows::Unplaced
				}
			}
			_ => Rows::Serial,
		}
	}

	/// Take one row the source yielded; `true` when the pipeline takes no more.
	#[expect(
		clippy::too_many_arguments,
		reason = "a row with the program, pipeline, state, context and output it is taken in"
	)]
	pub(super) async fn row(
		&mut self,
		program: &Program,
		pipeline: &Pipeline,
		row: Value,
		state: &mut State,
		ctx: &ExecutionContext,
		yielder: &mut Out<'_, '_>,
		flush_at: usize,
	) -> FlowResult<bool> {
		match self {
			Rows::Serial => {
				let stop = run_value_row(program, pipeline, row, state, ctx).await? == Ended::Stop;
				flush(state, yielder, flush_at, ctx).await?;
				Ok(stop)
			}
			Rows::Morsels(set) => set.push(row, state, ctx, yielder, flush_at).await,
			Rows::Writes(set) => set.push(row, state, ctx, yielder, flush_at).await,
			Rows::Unplaced => Err(unplaced()),
		}
	}

	/// Reads what the rows of a run of the leading rows of `batch`, the rows of a source batch not
	/// yet run, reach, before they run, when the rows run one at a time: the links of a `FETCH`
	/// where the plan states them
	/// ([`Pipeline::rows_ahead`](crate::exec::vm::program::Pipeline::rows_ahead)), and the first
	/// hops of the graph lookups it states
	/// ([`Pipeline::frontier_rows`](crate::exec::vm::program::Pipeline::frontier_rows)). Only the
	/// rows that reach the reader are keyed, which the paging clause before it decides from what it
	/// has left to skip and to admit. Answers how many leading rows of `batch` to run before the
	/// next call, at least one where `batch` holds any: the rows the links' window covers, and the
	/// whole batch where no link is read ahead; the frontier window is filled for those rows. The
	/// caller runs them and asks again for the rest. The rows of a morsel batch are answered whole:
	/// the batch fills its own windows as it runs ([`run::run_batch`]).
	pub(super) async fn ahead(
		&mut self,
		program: &Program,
		pipeline: &Pipeline,
		batch: &[Value],
		state: &mut State,
		ctx: &ExecutionContext,
	) -> FlowResult<usize> {
		if !matches!(&*self, Rows::Serial) {
			return Ok(batch.len());
		}
		// The rows a paging clause before the reader lets through are the ones that reach it.
		let reaching = |paging: Option<u16>, state: &State| match paging {
			Some(slot) => {
				let paging = state.paging[usize::from(slot)];
				let start = paging.skip.min(batch.len());
				let end = paging.remaining.map_or(batch.len(), |remaining| {
					start.saturating_add(remaining).min(batch.len())
				});
				start..end
			}
			None => 0..batch.len(),
		};
		let mut run = batch.len();
		if let Some(spec) = &pipeline.rows_ahead {
			let reaching = reaching(spec.paging, state);
			if !reaching.is_empty() {
				let covered =
					super::host::fill_rows_ahead(pipeline, &batch[reaching.clone()], state, ctx)
						.await?;
				// The rows past the reaching ones do not reach the `FETCH`, so a fill that covers
				// every row that does covers the batch.
				if reaching.start + covered < reaching.end {
					run = reaching.start + covered;
				}
			}
		}
		if let Some(spec) = &pipeline.frontier_rows {
			let reaching = reaching(spec.paging, state);
			let end = reaching.end.min(run);
			let start = reaching.start.min(end);
			super::host::fill_frontier_rows(program, pipeline, &batch[start..end], state, ctx)
				.await;
		}
		Ok(run)
	}

	/// These rows, for a reader that takes no more than `reads` of them before it may stop, where
	/// it says: the morsels have no more than that in flight until they first hand rows on.
	pub(super) fn reading(mut self, reads: Option<u32>) -> Self {
		if let Rows::Morsels(set) = &mut self {
			set.reads = reads;
		}
		self
	}

	/// The most rows the body's own paging clause `paged` still takes from the source: what
	/// `START` still skips plus what `LIMIT` still admits, less the rows the morsels hold that the
	/// merge has not counted; at least one, and `usize::MAX` for a body that pages nothing. A
	/// limit applied past a sort, an aggregate or a join this pipeline feeds is not the body's own
	/// and bounds nothing here: such a pipeline takes every row.
	pub(super) fn wanted(&self, paged: Option<u16>, state: &State) -> usize {
		match self {
			Rows::Serial => paged
				.and_then(|limit| {
					let paging = state.paging[limit as usize];
					paging.remaining.map(|remaining| paging.skip.saturating_add(remaining))
				})
				.map_or(usize::MAX, |wanted| wanted.max(1)),
			Rows::Morsels(set) => set.wanted(state),
			Rows::Writes(_) | Rows::Unplaced => usize::MAX,
		}
	}

	/// The source has no more rows: merge every morsel still out; `true` when the pipeline takes
	/// no more.
	pub(super) async fn finish(
		&mut self,
		state: &mut State,
		ctx: &ExecutionContext,
		yielder: &mut Out<'_, '_>,
		flush_at: usize,
	) -> FlowResult<bool> {
		match self {
			Rows::Serial => {
				if let Some(ahead) = &mut state.link_ahead {
					ahead.clear();
				}
				state.frontier_rows.clear();
				Ok(false)
			}
			Rows::Morsels(set) => set.finish(state, ctx, yielder, flush_at).await,
			Rows::Writes(set) => set.finish(state, ctx, yielder, flush_at).await,
			Rows::Unplaced => Err(unplaced()),
		}
	}

	/// The source failed with `failure` after the rows it yielded: the failure of the earliest
	/// morsel before it that failed, or `failure`. A paged pipeline whose limit is spent before the
	/// failure takes no more rows and answers `true`, as the serial run stops before reading past
	/// its limit.
	pub(super) async fn fail(
		&mut self,
		failure: ControlFlow,
		state: &mut State,
		ctx: &ExecutionContext,
		yielder: &mut Out<'_, '_>,
		flush_at: usize,
	) -> FlowResult<bool> {
		match self {
			Rows::Morsels(set) => set.fail(failure, state, ctx, yielder, flush_at).await,
			Rows::Writes(set) => set.fail(failure, state, ctx, yielder, flush_at).await,
			Rows::Serial => Err(failure),
			Rows::Unplaced => Err(unplaced()),
		}
	}
}
