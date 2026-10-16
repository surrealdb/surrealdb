//! A read pipeline's rows run as morsels: parts of its input run on the statement's task or on
//! workers of their own, their results merged in the order of the serial run.
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
//! A failure is the failure of the earliest morsel that fails, or of the source when no morsel
//! before it failed, raised once every earlier morsel's results are merged, which is where the
//! serial run raises it. The error is the serial run's, never a wrapper.
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
//! - A worker takes the earliest queued morsel and runs each entry in place on a state of its own,
//!   chunk by chunk under the thread's fuel, like the coordinator's reads. It takes none while as
//!   many morsels are running or finished and unmerged as the policy allows, and stops at a chunk
//!   boundary once the statement does not need it.
//! - The coordinator merges in order. A finished morsel is absorbed as a batch is; a running one is
//!   awaited; a queued one is run on the statement's own state, which makes it the front.
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

mod grouped;
mod held;
mod lease;
mod merge;
mod metrics;
pub(crate) mod places;
mod run;
mod set;
mod sorted;
mod steal;
#[cfg(test)]
mod test_hooks;
mod write_apply;
mod write_compute;

use std::sync::Arc;

#[allow(unused_imports, reason = "request L16-4 calls `run_held_rows` from `run_source`")]
pub(super) use self::held::{HeldRows, run_held_rows};
pub(super) use self::merge::body_breakers;
pub(crate) use self::metrics::MorselMetrics;
pub(super) use self::places::Statement;
use self::set::{MorselSet, RangeSet};
pub(super) use self::write_compute::Stop;
use super::host::{Ended, Out, flush, run_value_row};
use super::interp::State;
use super::program::{Pipeline, Program};
use crate::exec::planner::placement::{MorselInput, MorselPlan, Parallel};
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
/// | `Batches` (any other source that splits) | no | `Batches` |
/// | `Batches` (any other source that splits) | yes | serial |
/// | serial | either | serial |
///
/// This is the one implementation of the table: the readers of a pipeline's decision and the
/// recorders of the placement of a program call it.
pub(crate) fn effective_mode(decided: MorselInput, in_place: bool) -> Option<MorselInput> {
	match (decided, in_place) {
		(MorselInput::Range, true) => Some(MorselInput::Range),
		(MorselInput::Range, false) | (MorselInput::Batches, false) => Some(MorselInput::Batches),
		(MorselInput::Batches, true) => None,
	}
}

/// [`effective_mode`] of `pipeline`: how its morsels run, or `None` when it runs serially.
pub(crate) fn effective_input(pipeline: &Pipeline) -> Option<MorselInput> {
	match &pipeline.parallel {
		Parallel::Serial => None,
		Parallel::Morsels(plan) => effective_mode(plan.input, pipeline.in_place),
	}
}

/// Where a pipeline's rows go as a storage read yields them: run one at a time, or gathered into
/// morsels.
pub(super) enum Rows<'p> {
	Serial,
	Morsels(Box<MorselSet<'p>>),
}

impl<'p> Rows<'p> {
	/// The rows of `pipeline`, one of `program`'s: morsels when the physical plan planned it as
	/// them.
	pub(super) fn of(
		program: &'p Arc<Program>,
		pipeline: &Pipeline,
		ctx: &ExecutionContext,
	) -> Self {
		match program.pipelines.iter().position(|p| std::ptr::eq(p, pipeline)) {
			Some(at) if effective_input(pipeline) == Some(MorselInput::Batches) => {
				Rows::Morsels(Box::new(MorselSet::new(program, at, ctx)))
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
		}
	}

	/// Reads the links of a run of the leading rows of `batch`, the rows of a source batch not yet
	/// run, before they run, for a pipeline whose plan states it
	/// ([`Pipeline::rows_ahead`](crate::exec::vm::program::Pipeline::rows_ahead)), when the rows
	/// run one at a time: only the rows that reach the `FETCH` are keyed, which the paging clause
	/// before it decides from what it has left to skip and to admit. Answers how many leading rows
	/// of `batch` to run before the next call, at least one where `batch` holds any; the caller
	/// runs them and asks again for the rest. The rows of a morsel batch read their links as the
	/// `FETCH` reaches them, and are answered whole.
	pub(super) async fn ahead(
		&mut self,
		pipeline: &Pipeline,
		batch: &[Value],
		state: &mut State,
		ctx: &ExecutionContext,
	) -> FlowResult<usize> {
		let (Rows::Serial, Some(spec)) = (&*self, &pipeline.rows_ahead) else {
			return Ok(batch.len());
		};
		let (start, end) = match spec.paging {
			Some(slot) => {
				let paging = state.paging[usize::from(slot)];
				let start = paging.skip.min(batch.len());
				let end = paging.remaining.map_or(batch.len(), |remaining| {
					start.saturating_add(remaining).min(batch.len())
				});
				(start, end)
			}
			None => (0, batch.len()),
		};
		if start == end {
			return Ok(batch.len());
		}
		let covered =
			super::host::fill_rows_ahead(pipeline, &batch[start..end], state, ctx).await?;
		// The rows past `end` do not reach the `FETCH`, so a fill that covers every row that does
		// covers the batch.
		Ok(if start + covered >= end {
			batch.len()
		} else {
			start + covered
		})
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
				Ok(false)
			}
			Rows::Morsels(set) => set.finish(state, ctx, yielder, flush_at).await,
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
			Rows::Serial => Err(failure),
		}
	}
}
