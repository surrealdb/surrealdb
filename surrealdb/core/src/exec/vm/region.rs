//! A read pipeline's rows run as a region: batch by batch, each batch on the
//! statement's task or on a worker of its own, their results merged in batch
//! order.
//!
//! The serial run is what a region means. A region's body only reads, and
//! fills the statement's output, a sort, or an aggregate whose partial states
//! combine exactly ([`body_admits`]). Each batch runs on a state of its own,
//! so what it produces — rows for the output, entries for a sort, partial
//! groups for an aggregate — depends on its rows alone. Merging those results
//! in the order the source yielded the batches answers what the serial run
//! answers:
//!
//! - the output is the batches' rows concatenated in batch order;
//! - a sort receives the batches' entries in batch order, and its sort is stable, so entries that
//!   tie keep the order the serial run gives them; a sort that keeps only the rows its limit can
//!   reach prunes each batch as the serial run prunes the whole, which keeps every row the serial
//!   run keeps;
//! - an aggregate folds each batch's partial group into the group it has, in batch order, so an
//!   accumulator that appends keeps the serial order and a non-aggregate column keeps the value the
//!   earliest batch evaluated.
//!
//! A batch whose body is not paged charges its breakers to the query's lease,
//! as the statement does, where the query can spill. A breaker the lease no
//! longer covers spills within the batch, and the batch hands the merge a
//! [`Partial`] for it in place of its buffer. To absorb it, the merge first
//! writes what the statement holds for that breaker as a run. It then appends
//! the batch's runs and takes the batch's held records as the statement's
//! buffer. Every record the statement holds therefore precedes the batch's,
//! and the statement's runs stay in serial arrival order. A breaker whose
//! state cannot be saved, or `GROUP ALL`, never spills in a batch.
//!
//! A failure is the failure of the earliest batch that fails, or of the
//! source when no batch before it failed, raised once every earlier batch's
//! results are merged, which is where the serial run raises it.
//!
//! A body that pages its rows to the statement's output ([`paged_admits`])
//! runs each batch as though every row were taken, but for the rows `START`
//! is sure to skip — those it still skips when every row in flight ahead of
//! the batch reaches the paging — and the merge pages them: a batch records,
//! for each of its rows, whether the row reached the paging, whether the
//! batch skipped it, what it emitted, and how it failed. The merge then
//! replays the paging over the rows in order, as the serial run counts them:
//!
//! - a row that fails before its paging raises its failure;
//! - a row `START` skips is dropped, and so is any failure its projection raised, which the serial
//!   run never evaluates;
//! - a row taken counts against `LIMIT`, raises its projection's failure or hands on its rows, and
//!   once the limit admits no more the region stops: the batches after it are dropped, their
//!   workers aborted, and the source is read no further, so a failure past the limit — of a row, a
//!   batch, or the source — is never raised.
//!
//! A batch stops at its first row that fails before its paging, since no row
//! after it can be reached. The rows in flight are bounded by the rows still
//! needed: what `START` and `LIMIT` have left, scaled by the share of rows
//! that have reached the paging so far. A small limit runs a batch about its
//! own size, and the window widens as the merged batches show a filter
//! passing fewer rows.
//!
//! How wide a region runs is decided per batch, as the batch is filled. A
//! batch runs on the statement's task unless it holds at least
//! [`region_row_threshold`](crate::exec::config::ExecConfig::region_row_threshold)
//! rows and the node's worker budget has a free place, which the batch holds
//! until it is done. A region therefore widens while the node has idle cores
//! and runs serially while other statements hold them. Batches are sized from
//! the cost per row measured so far: one run on the statement's task is sized
//! to about a millisecond, so the task yields its place often; one handed to
//! a worker to several, so a spawn is repaid.
//!
//! The source is read on the statement's task alone: a transaction serves one
//! read at a time, so a worker is handed decoded rows rather than a range to
//! read. A worker runs under the statement's context and frames, and drives
//! what nests beneath its body on its own frame stack. On wasm there are no
//! workers and every batch runs on the statement's task.

use std::collections::VecDeque;
use std::sync::Arc;

#[cfg(not(target_family = "wasm"))]
use futures::StreamExt;

use super::host::{Ended, Out, flush, run_value_row};
use super::interp::{Groups, Paging, SortRow, State};
use super::partial::Partial;
use super::program::{AggregateSink, BlockInstr, Instr, NestedSpec, Pipeline, Program, Source};
use super::sort::Sorted;
use crate::exec::memory::Reservation;
use crate::exec::memory::runs::BreakerKind;
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::ControlFlow;
use crate::val::Value;

/// Whether a pipeline over `source` may run as a region: a read of storage
/// whose rows come in the order the source defines.
pub(super) fn source_admits(source: &Source) -> bool {
	match source {
		Source::Scan {
			..
		}
		| Source::Record {
			..
		}
		| Source::Index {
			..
		}
		| Source::Lookup {
			..
		}
		| Source::IndexUnion {
			..
		}
		| Source::Bitmap {
			..
		}
		| Source::FullText {
			..
		}
		| Source::Knn {
			..
		}
		| Source::LightweightEdges {
			..
		} => true,
		Source::Union(sources) => sources.iter().all(source_admits),
		Source::Sorted {
			..
		}
		| Source::Groups {
			..
		}
		| Source::Split {
			..
		}
		| Source::Join {
			..
		}
		| Source::Values {
			..
		}
		| Source::Nested {
			..
		}
		| Source::Probe {
			..
		}
		| Source::Count {
			..
		}
		| Source::WriteTargets {
			..
		}
		| Source::WriteInserts {
			..
		}
		| Source::WriteEdges {
			..
		}
		| Source::Once => false,
	}
}

/// Whether a pipeline whose body is `code` may run as a region: it writes
/// nothing, splits nothing, joins nothing, passes each row through a distinct
/// set of the run in no one batch's state, folds only into aggregates of `aggregates` whose
/// partial states combine exactly ([`combines_exactly`]), runs only the
/// statements of `nested` that only read and are evaluated afresh for each
/// row, and pages nothing unless `paged`, when [`paged_admits`] has found its
/// paging one the merge replays.
///
/// A statement kept from its first evaluation is left to the serial run: a
/// batch's state would evaluate it again, and a statement whose value is not
/// determined by what it reads would answer each batch differently.
///
/// The [`Instr::StopPipeline`] every body ends with is reached only by a
/// paging instruction's jump.
pub(super) fn body_admits(
	code: &[Instr],
	aggregates: &[AggregateSink],
	nested: &[NestedSpec],
	paged: bool,
) -> bool {
	code.iter().all(|instr| match instr {
		Instr::Skip {
			..
		}
		| Instr::Take {
			..
		}
		| Instr::JumpIfLimitReached {
			..
		} => paged,
		Instr::SplitPush {
			..
		}
		| Instr::BindRow {
			..
		}
		| Instr::JoinProbe {
			..
		}
		| Instr::JoinPair {
			..
		}
		| Instr::Distinct {
			..
		}
		| Instr::Write(_)
		| Instr::Store(_)
		| Instr::FetchRow {
			..
		} => false,
		Instr::Block(BlockInstr::Nested {
			nested: at,
			..
		}) => nested
			.get(*at as usize)
			.is_some_and(|spec| spec.read_only && !spec.once && !spec.probed),
		Instr::Block(_) => false,
		Instr::GroupKey {
			aggregate,
			..
		}
		| Instr::GroupSelect {
			aggregate,
		}
		| Instr::GroupFirstNeeded {
			aggregate,
			..
		}
		| Instr::GroupFirstSet {
			aggregate,
			..
		}
		| Instr::AccUpdate {
			aggregate,
			..
		} => aggregates.get(*aggregate as usize).is_some_and(combines_exactly),
		_ => true,
	})
}

/// Whether the paging of `code`, a body that hands its rows to the
/// statement's output, is one a region's merge replays: one paging clause,
/// whose `START` skips a row to the row's end before it emits anything, whose
/// `LIMIT` counts the row straight after, and whose check that the limit is
/// spent comes last, after everything the row emits, and is the only way to
/// the pipeline's end.
///
/// Such a body runs the same instructions for a row taken whatever the counts
/// were, and what runs before its paging runs for every row, so a batch may
/// run each row as taken and leave the counting to the merge.
pub(super) fn paged_admits(code: &[Instr]) -> bool {
	let n = code.len();
	if n < 3 || !matches!(code[n - 1], Instr::StopPipeline) || !matches!(code[n - 2], Instr::EndRow)
	{
		return false;
	}
	let (end, halt) = (n - 2, n - 1);
	let mut clause = None;
	let (mut skip, mut take, mut check) = (None, None, None);
	for (at, instr) in code.iter().enumerate() {
		let (limit, slot) = match instr {
			Instr::Skip {
				limit,
				target,
			} => {
				if *target as usize != end {
					return false;
				}
				(limit, &mut skip)
			}
			Instr::Take {
				limit,
			} => (limit, &mut take),
			Instr::JumpIfLimitReached {
				limit,
				done,
			} => {
				if *done as usize != halt {
					return false;
				}
				(limit, &mut check)
			}
			Instr::Emit {
				..
			}
			| Instr::EmitEnd {
				..
			}
			| Instr::BuildObjectEmit {
				..
			} => {
				// A row emits only once it is past its paging.
				if skip.or(take).is_none() {
					return false;
				}
				continue;
			}
			Instr::SortPush {
				..
			}
			| Instr::GroupKey {
				..
			}
			| Instr::GroupSelect {
				..
			}
			| Instr::GroupFirstNeeded {
				..
			}
			| Instr::GroupFirstSet {
				..
			}
			| Instr::AccUpdate {
				..
			} => return false,
			other => {
				if other.clone().labels_mut().iter().any(|label| **label as usize == halt) {
					return false;
				}
				continue;
			}
		};
		if slot.is_some() || clause.is_some_and(|clause| clause != *limit) {
			return false;
		}
		clause = Some(*limit);
		*slot = Some(at);
	}
	match (skip, take, check) {
		(Some(_), None, None) => true,
		(skip, Some(take), Some(check)) => {
			skip.is_none_or(|skip| skip + 1 == take)
				&& check + 1 == end
				&& matches!(code[check - 1], Instr::Emit { .. })
		}
		_ => false,
	}
}

/// The aggregate functions whose partial states, merged in batch order,
/// finalise to what folding every value in order does.
///
/// Excluded are the arithmetic folds over numbers of unknown kind:
/// `math::sum`, `math::mean`, `math::stddev`, `math::variance` and
/// `vector::sum` add floats, and a float sum depends on how its terms are
/// grouped.
const EXACT_AGGREGATES: &[&str] = &[
	"count",
	"math::min",
	"math::max",
	"math::median",
	"time::min",
	"time::max",
	"array::group",
	"array::collect",
	"array::join",
	"array::distinct",
	"array::min",
	"array::max",
];

/// Whether every aggregate function `sink` folds is one of
/// [`EXACT_AGGREGATES`].
fn combines_exactly(sink: &AggregateSink) -> bool {
	sink.fields.iter().all(|field| {
		field.aggregate.as_ref().is_none_or(|info| {
			info.calls.iter().all(|call| EXACT_AGGREGATES.contains(&call.name.name()))
		})
	})
}

/// The node's worker budget: one place per core beyond the one the
/// statement's task runs on, shared by every region the process runs.
#[cfg(not(target_family = "wasm"))]
fn workers() -> &'static Arc<tokio::sync::Semaphore> {
	static WORKERS: std::sync::LazyLock<Arc<tokio::sync::Semaphore>> =
		std::sync::LazyLock::new(|| {
			let cores = std::thread::available_parallelism().map_or(1, |cores| cores.get());
			Arc::new(tokio::sync::Semaphore::new(cores.saturating_sub(1)))
		});
	&WORKERS
}

/// A place in the worker budget, held by a batch until it is done.
#[cfg(not(target_family = "wasm"))]
type Place = tokio::sync::OwnedSemaphorePermit;
#[cfg(target_family = "wasm")]
type Place = std::convert::Infallible;

/// How many batches this process has handed to workers, for tests that must
/// know a region ran wider than serially.
#[cfg(test)]
pub(super) static HANDED_TO_WORKERS: std::sync::atomic::AtomicUsize =
	std::sync::atomic::AtomicUsize::new(0);

/// What one batch produced, on a state of its own.
struct BatchResult {
	output: Vec<Value>,
	sorts: Vec<Vec<(Vec<Value>, super::interp::SortRow)>>,
	/// What each bounded sort counted across the batch's rows.
	arrivals: Vec<super::interp::Arrivals>,
	groups: Vec<Option<Groups>>,
	/// A partial per breaker the batch spilled; that breaker's buffer is
	/// absent from `sorts` and `groups`.
	spilled: Vec<Partial>,
	/// What each of the batch's sorts and groups holds of its query's lease,
	/// by breaker, the records its partials hold included; empty without a
	/// query memory. The merge takes the reservations over for the statement's
	/// own accounts, so a batch waiting to merge still counts against the
	/// lease and the rows it hands over are counted once.
	sort_accounts: Vec<Reservation>,
	group_accounts: Vec<Reservation>,
	/// For a paged body, how each row the batch ran ended, in row order.
	ends: Vec<RowEnd>,
	rows: usize,
	elapsed: std::time::Duration,
}

/// How one row of a paged body's batch ended.
struct RowEnd {
	/// Whether the row reached its paging.
	reached: bool,
	/// Whether the batch skipped the row itself, as one `START` was sure to
	/// skip.
	skipped: bool,
	/// How many rows it handed to the output.
	emitted: usize,
	failure: Option<ControlFlow>,
}

/// A batch whose results the merge has not taken yet.
enum Batch {
	/// Run on the statement's task, or a failure the source raised. Boxed so
	/// a pending batch stays the size of a worker handle; one box per batch.
	Done(Box<FlowResult<BatchResult>>),
	/// Running on a worker, which is aborted if the batch is dropped unmerged.
	#[cfg(not(target_family = "wasm"))]
	Running(Worker),
}

#[cfg(not(target_family = "wasm"))]
struct Worker(Option<tokio::task::JoinHandle<FlowResult<BatchResult>>>);

#[cfg(not(target_family = "wasm"))]
impl Drop for Worker {
	fn drop(&mut self) {
		if let Some(handle) = &self.0 {
			handle.abort();
		}
	}
}

impl Batch {
	/// Whether the batch's results can be taken without waiting.
	fn is_done(&self) -> bool {
		match self {
			Batch::Done(_) => true,
			#[cfg(not(target_family = "wasm"))]
			Batch::Running(worker) => worker.0.as_ref().is_none_or(|handle| handle.is_finished()),
		}
	}

	/// The batch's results, waiting for its worker.
	async fn result(self) -> FlowResult<BatchResult> {
		match self {
			Batch::Done(result) => *result,
			#[cfg(not(target_family = "wasm"))]
			Batch::Running(mut worker) => {
				let handle = worker.0.take().expect("a worker is taken once");
				match handle.await {
					Ok(result) => result,
					Err(e) => Err(ControlFlow::Err(anyhow::anyhow!("a region worker failed: {e}"))),
				}
			}
		}
	}
}

/// The time a batch on the statement's task is sized to.
const INLINE_BUDGET: std::time::Duration = std::time::Duration::from_millis(1);
/// The time a batch handed to a worker is sized to.
const WORKER_BUDGET: std::time::Duration = std::time::Duration::from_millis(8);
/// The rows of the first batch, before any cost is measured.
const FIRST_BATCH: usize = 64;

/// How many batches may be in flight ahead of the one the merge waits for.
fn read_ahead() -> usize {
	#[cfg(not(target_family = "wasm"))]
	{
		2 * std::thread::available_parallelism().map_or(1, |cores| cores.get())
	}
	#[cfg(target_family = "wasm")]
	{
		1
	}
}

/// Where a pipeline's rows go as a storage read yields them: run one at a
/// time, or gathered into a region's batches.
pub(super) enum Rows<'p> {
	Serial,
	Region(Box<Region<'p>>),
}

impl<'p> Rows<'p> {
	/// The rows of `pipeline`, one of `program`'s: a region when its body may
	/// run as one.
	pub(super) fn of(
		program: &'p Arc<Program>,
		pipeline: &Pipeline,
		ctx: &ExecutionContext,
	) -> Self {
		match program.pipelines.iter().position(|p| std::ptr::eq(p, pipeline)) {
			Some(at) if pipeline.region => Rows::Region(Box::new(Region::new(program, at, ctx))),
			_ => Rows::Serial,
		}
	}

	/// Take one row the source yielded; `true` when the pipeline takes no
	/// more.
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
			Rows::Region(region) => region.push(row, state, ctx, yielder, flush_at).await,
		}
	}

	/// These rows, for a reader that takes no more than `reads` of them before
	/// it may stop, where it says: a region has no more than that in flight
	/// until it first hands rows on.
	pub(super) fn reading(mut self, reads: Option<u32>) -> Self {
		if let Rows::Region(region) = &mut self {
			region.reads = reads.map(|reads| reads as usize);
		}
		self
	}

	/// The most rows the body's own paging clause `paged` still takes from
	/// the source: what `START` still skips plus what `LIMIT` still admits,
	/// less the rows a region holds that its merge has not counted; at least
	/// one, and `usize::MAX` for a body that pages nothing. A limit applied
	/// past a sort, an aggregate or a join this pipeline feeds is not the
	/// body's own and bounds nothing here: such a pipeline takes every row.
	pub(super) fn wanted(&self, paged: Option<u16>, state: &State) -> usize {
		match self {
			Rows::Serial => paged
				.and_then(|limit| {
					let paging = state.paging[limit as usize];
					paging.remaining.map(|remaining| paging.skip.saturating_add(remaining))
				})
				.map_or(usize::MAX, |wanted| wanted.max(1)),
			Rows::Region(region) => region.wanted(state),
		}
	}

	/// The source has no more rows: merge every batch still out; `true` when
	/// the pipeline takes no more.
	pub(super) async fn finish(
		&mut self,
		state: &mut State,
		ctx: &ExecutionContext,
		yielder: &mut Out<'_, '_>,
		flush_at: usize,
	) -> FlowResult<bool> {
		match self {
			Rows::Serial => Ok(false),
			Rows::Region(region) => region.finish(state, ctx, yielder, flush_at).await,
		}
	}

	/// The source failed with `failure` after the rows it yielded: the failure
	/// of the earliest batch before it that failed, or `failure`. A paged
	/// region whose limit is spent before the failure takes no more rows and
	/// answers `true`, as the serial run stops before reading past its limit.
	pub(super) async fn fail(
		&mut self,
		failure: ControlFlow,
		state: &mut State,
		ctx: &ExecutionContext,
		yielder: &mut Out<'_, '_>,
		flush_at: usize,
	) -> FlowResult<bool> {
		if let Rows::Region(region) = self {
			if !region.filling.is_empty() {
				region.dispatch(state, ctx, yielder, flush_at).await?;
			}
			if region.stopped {
				return Ok(true);
			}
			region.merging.push_back(Batch::Done(Box::new(Err(failure))));
			while !region.merging.is_empty() {
				region.merge(true, state, yielder, flush_at, ctx).await?;
			}
			if region.stopped {
				return Ok(true);
			}
			return Err(ControlFlow::Err(anyhow::anyhow!("a region's failure was merged")));
		}
		Err(failure)
	}
}

/// The sorts and aggregates a body fills, each once in first-use order, and
/// its paging clause.
pub(super) struct BodyBreakers {
	pub(super) sorts: Vec<u16>,
	pub(super) aggregates: Vec<u16>,
	pub(super) paged: Option<u16>,
}

/// The breakers the body `code` fills.
pub(super) fn body_breakers(code: &[Instr]) -> BodyBreakers {
	let mut sorts = Vec::new();
	let mut aggregates = Vec::new();
	let mut paged = None;
	for instr in code {
		match instr {
			Instr::Skip {
				limit,
				..
			}
			| Instr::Take {
				limit,
			} => paged = Some(*limit),
			Instr::SortPush {
				sort,
				..
			} if !sorts.contains(sort) => sorts.push(*sort),
			Instr::GroupKey {
				aggregate,
				..
			}
			| Instr::GroupSelect {
				aggregate,
			}
			| Instr::GroupFirstNeeded {
				aggregate,
				..
			}
			| Instr::GroupFirstSet {
				aggregate,
				..
			}
			| Instr::AccUpdate {
				aggregate,
				..
			} if !aggregates.contains(aggregate) => aggregates.push(*aggregate),
			_ => {}
		}
	}
	BodyBreakers {
		sorts,
		aggregates,
		paged,
	}
}

/// A region in progress; see the module documentation.
pub(super) struct Region<'p> {
	program: &'p Arc<Program>,
	at: usize,
	threshold: usize,
	/// The rows of the batch being filled.
	filling: Vec<Value>,
	/// The place the batch being filled holds, once it is to go to a worker.
	place: Option<Place>,
	merging: VecDeque<Batch>,
	/// The cost of a row measured so far, in nanoseconds.
	cost: Option<f64>,
	/// The sorts and aggregates the body fills.
	sorts: Vec<u16>,
	aggregates: Vec<u16>,
	/// The paging clause of a paged body, which the merge applies.
	paged: Option<u16>,
	/// Whether the merge has spent the paged body's limit: the pipeline takes
	/// no more rows.
	stopped: bool,
	/// For a body that pages nothing itself, the rows its reader takes before
	/// it may stop, until the region first hands rows on ([`Rows::reading`]).
	reads: Option<usize>,
	/// The rows of the batches the merge has not taken yet.
	in_flight: usize,
	/// The rows of a paged body the merge has taken, and how many of them
	/// reached the paging.
	merged: usize,
	reached: usize,
}

impl<'p> Region<'p> {
	fn new(program: &'p Arc<Program>, at: usize, ctx: &ExecutionContext) -> Self {
		let BodyBreakers {
			sorts,
			aggregates,
			paged,
		} = body_breakers(&program.pipelines[at].code);
		Self {
			program,
			at,
			threshold: ctx.root().ctx.config.exec.region_row_threshold,
			filling: Vec::new(),
			place: None,
			merging: VecDeque::new(),
			cost: None,
			sorts,
			aggregates,
			paged,
			stopped: false,
			reads: None,
			in_flight: 0,
			merged: 0,
			reached: 0,
		}
	}

	/// The rows a paged body may have in flight before the merge takes the
	/// batch at the front: the rows its paging still needs, scaled by the
	/// share of merged rows that reached the paging; for a body that pages
	/// nothing itself, the rows its reader takes before it may stop, until the
	/// region first hands rows on; `None` when it needs every row.
	fn window(&self, state: &State) -> Option<usize> {
		let Some(paged) = self.paged else {
			return self.reads.map(|reads| reads.max(1));
		};
		let paging = state.paging[paged as usize];
		let needed = paging.skip.saturating_add(paging.remaining?).max(1);
		if self.merged == 0 {
			return Some(needed);
		}
		if self.reached == 0 {
			return None;
		}
		let scaled = needed as u128 * self.merged as u128 / self.reached as u128;
		Some(usize::try_from(scaled).unwrap_or(usize::MAX))
	}

	/// The rows the window leaves past those in flight and being filled, at
	/// least one; `usize::MAX` without a window.
	fn wanted(&self, state: &State) -> usize {
		self.window(state).map_or(usize::MAX, |window| {
			window.saturating_sub(self.in_flight + self.filling.len()).max(1)
		})
	}

	/// The rows a batch run in `budget` is sized to, from the cost measured
	/// so far.
	fn rows_for(&self, budget: std::time::Duration) -> usize {
		let most = crate::kvs::NORMAL_BATCH_SIZE as usize;
		match self.cost {
			None => FIRST_BATCH.min(most),
			Some(cost) => ((budget.as_nanos() as f64 / cost.max(1.0)) as usize).clamp(1, most),
		}
	}

	/// Take one row the source yielded; `true` when the pipeline takes no
	/// more.
	async fn push(
		&mut self,
		row: Value,
		state: &mut State,
		ctx: &ExecutionContext,
		yielder: &mut Out<'_, '_>,
		flush_at: usize,
	) -> FlowResult<bool> {
		// Taking the row is a unit of the driver's work, whether it runs
		// the row or hands it to a worker.
		super::fuel::tick(ctx, 1).await?;
		if self.stopped {
			return Ok(true);
		}
		self.filling.push(row);
		if let Some(window) = self.window(state)
			&& self.in_flight + self.filling.len() >= window
		{
			self.dispatch(state, ctx, yielder, flush_at).await?;
			while !self.stopped
				&& !self.merging.is_empty()
				&& self.window(state).is_some_and(|window| self.in_flight >= window)
			{
				self.merge(true, state, yielder, flush_at, ctx).await?;
			}
			return Ok(self.stopped);
		}
		if self.place.is_none() && self.filling.len() >= self.rows_for(INLINE_BUDGET) {
			self.place = self.free_place();
			if self.place.is_none() {
				self.dispatch(state, ctx, yielder, flush_at).await?;
				return Ok(self.stopped);
			}
		}
		if self.place.is_some()
			&& self.filling.len() >= self.rows_for(WORKER_BUDGET).max(self.threshold)
		{
			self.dispatch(state, ctx, yielder, flush_at).await?;
		}
		Ok(self.stopped)
	}

	/// A place in the worker budget for the batch being filled, when the
	/// batch may go to a worker at all.
	#[cfg(not(target_family = "wasm"))]
	fn free_place(&self) -> Option<Place> {
		if self.threshold == usize::MAX {
			return None;
		}
		Arc::clone(workers()).try_acquire_owned().ok()
	}

	#[cfg(target_family = "wasm")]
	fn free_place(&self) -> Option<Place> {
		None
	}

	/// Run the batch being filled: on a worker when it holds a place and
	/// enough rows, and on the statement's task otherwise.
	async fn dispatch(
		&mut self,
		state: &mut State,
		ctx: &ExecutionContext,
		yielder: &mut Out<'_, '_>,
		flush_at: usize,
	) -> FlowResult<()> {
		let rows = std::mem::take(&mut self.filling);
		let place = self.place.take();
		// The rows in flight ahead of this batch are the most that can reach
		// the paging before it, so `START` skips at least the rest.
		let paged = self
			.paged
			.map(|limit| (limit, state.paging[limit as usize].skip.saturating_sub(self.in_flight)));
		self.in_flight += rows.len();
		let under = Under {
			frames: state.frames.clone(),
			slots: state.slots.clone(),
			kept: state.slot_charges(),
			verdicts: state.verdicts.clone(),
			bypassed: state.permissions_bypassed,
			shared: state
				.shared(self.program, ctx.function_registry())
				.map_err(ControlFlow::Err)?,
		};
		// A paged body's batches hold their rows for the merge to count and are
		// never bound.
		let bound = self.paged.is_none();
		let batch = match place {
			#[cfg(not(target_family = "wasm"))]
			Some(place) if rows.len() >= self.threshold => {
				Batch::Running(spawn(self.program, self.at, rows, paged, bound, under, ctx, place))
			}
			_ => Batch::Done(Box::new(
				run_batch(self.program, self.at, rows, paged, bound, under, ctx).await,
			)),
		};
		self.merging.push_back(batch);
		let waiting = self.merging.len() > read_ahead();
		self.merge(waiting, state, yielder, flush_at, ctx).await
	}

	async fn finish(
		&mut self,
		state: &mut State,
		ctx: &ExecutionContext,
		yielder: &mut Out<'_, '_>,
		flush_at: usize,
	) -> FlowResult<bool> {
		if !self.stopped && !self.filling.is_empty() {
			self.dispatch(state, ctx, yielder, flush_at).await?;
		}
		self.place = None;
		while !self.merging.is_empty() {
			self.merge(true, state, yielder, flush_at, ctx).await?;
		}
		Ok(self.stopped)
	}

	/// Take the results of the batches at the front that are done, in order,
	/// waiting for the first when `waiting`. Once a paged body's limit is
	/// spent, every batch still out is dropped, aborting its worker.
	async fn merge(
		&mut self,
		mut waiting: bool,
		state: &mut State,
		yielder: &mut Out<'_, '_>,
		flush_at: usize,
		ctx: &ExecutionContext,
	) -> FlowResult<()> {
		while self.merging.front().is_some_and(|batch| waiting || batch.is_done()) {
			waiting = false;
			let batch = self.merging.pop_front().expect("checked above");
			let result = batch.result().await?;
			// Merging a batch walks its rows, sorted rows and groups once: no
			// more than a batch's worth, spent whole, the driver yielding at
			// the next row it takes.
			super::fuel::charge(super::fuel::units(result.rows));
			self.in_flight -= result.rows;
			if result.rows > 0 {
				let cost = result.elapsed.as_nanos() as f64 / result.rows as f64;
				self.cost = Some(match self.cost {
					Some(seen) => 0.7 * seen + 0.3 * cost,
					None => cost,
				});
			}
			match self.paged {
				Some(limit) => self.page(limit, result, state)?,
				None => self.absorb(result, state, ctx).await?,
			}
			// Rows handed on to a reader that then asks for more: it takes
			// more than it said it might.
			if state.output.len() >= flush_at {
				self.reads = None;
			}
			flush(state, yielder, flush_at, ctx).await?;
			if self.stopped {
				self.merging.clear();
				self.filling.clear();
				self.place = None;
				break;
			}
		}
		Ok(())
	}

	/// Merge one batch of a paged body under paging clause `limit`: its rows
	/// counted in order as the serial run counts them.
	fn page(&mut self, limit: u16, result: BatchResult, state: &mut State) -> FlowResult<()> {
		if !result.spilled.is_empty() {
			return Err(ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(
				"a paged region's batch spilled".to_owned(),
			))));
		}
		let mut output = result.output.into_iter();
		for end in result.ends {
			self.merged += 1;
			if !end.reached {
				if let Some(failure) = end.failure {
					return Err(failure);
				}
				output.by_ref().take(end.emitted).for_each(drop);
				continue;
			}
			self.reached += 1;
			let paging = &mut state.paging[limit as usize];
			if end.skipped || paging.skip > 0 {
				paging.skip = paging.skip.checked_sub(1).ok_or_else(|| {
					ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(
						"a region's batch skipped a row its paging takes".to_owned(),
					)))
				})?;
				output.by_ref().take(end.emitted).for_each(drop);
				continue;
			}
			if let Some(remaining) = &mut paging.remaining {
				*remaining = remaining.saturating_sub(1);
			}
			let spent = paging.remaining == Some(0);
			if let Some(failure) = end.failure {
				return Err(failure);
			}
			state.output.extend(output.by_ref().take(end.emitted));
			if spent {
				self.stopped = true;
				return Ok(());
			}
		}
		Ok(())
	}

	/// Merge one batch's results into the statement's state.
	async fn absorb(
		&self,
		result: BatchResult,
		state: &mut State,
		ctx: &ExecutionContext,
	) -> FlowResult<()> {
		let BatchResult {
			output,
			mut sorts,
			arrivals,
			mut groups,
			mut spilled,
			mut sort_accounts,
			mut group_accounts,
			..
		} = result;
		state.output.extend(output);
		for &sort in &self.sorts {
			let at = sort as usize;
			let partial = spilled
				.iter()
				.position(|p| p.spec.kind == BreakerKind::Sort && p.spec.index == sort)
				.map(|found| spilled.swap_remove(found));
			let Some(partial) = partial else {
				adopt_reservation(&mut state.sort_accounts[at], sort_accounts.get_mut(at));
				let entries = std::mem::take(&mut sorts[at]);
				let arrived = arrivals.get(at).map_or(0, |a| a.arrived);
				state.keep_absorbed(self.program, sort, entries, arrived);
				continue;
			};
			super::host::seal_sort(state, at, ctx).await?;
			adopt_reservation(&mut state.sort_accounts[at], sort_accounts.get_mut(at));
			state.sort_runs[at].get_or_insert_with(Default::default).append(partial.runs)?;
			let rows: Vec<Sorted> =
				partial.held.into_iter().map(|(keys, row)| (keys, SortRow::Value(row))).collect();
			let arrived = rows.len() as u64;
			state.keep_absorbed(self.program, sort, rows, arrived);
		}
		for &aggregate in &self.aggregates {
			let at = aggregate as usize;
			let partial = spilled
				.iter()
				.position(|p| p.spec.kind == BreakerKind::Groups && p.spec.index == aggregate)
				.map(|found| spilled.swap_remove(found));
			let Some(partial) = partial else {
				adopt_reservation(&mut state.group_accounts[at], group_accounts.get_mut(at));
				let part = groups[at].take();
				merge_groups(&mut state.groups[at], part)?;
				continue;
			};
			super::host::seal_groups(state, at, ctx).await?;
			adopt_reservation(&mut state.group_accounts[at], group_accounts.get_mut(at));
			state.group_runs[at].get_or_insert_with(Default::default).append(partial.runs)?;
			let functions = state.aggregates.functions[at].clone();
			let into = state.groups[at].as_mut().ok_or_else(|| {
				ControlFlow::Err(anyhow::anyhow!("a region merged groups of another kind"))
			})?;
			for (key, row) in partial.held {
				into.load_saved(key, row, &functions).map_err(ControlFlow::Err)?;
			}
		}
		// What the statement took from the batch was charged to the
		// reservation it adopted from the batch, so the lease counted those
		// rows once throughout. A charge that outgrew that reservation and
		// the lease leaves the statement holding more than it reserved, and
		// it spills that.
		if state.memory.is_some() {
			super::host::spill_breakers(state, ctx).await?;
		}
		drop((sort_accounts, group_accounts));
		#[cfg(test)]
		if let Some(memory) = state.memory.as_ref()
			&& memory.lease().held() < held_by_breakers(state)
		{
			note_lease_fault(state, LeaseFault::Uncounted);
		}
		Ok(())
	}
}

/// Hand a batch's reservation for a breaker, where it has one, to the
/// statement's account for the same breaker.
fn adopt_reservation(statement: &mut Reservation, batch: Option<&mut Reservation>) {
	if let Some(batch) = batch {
		statement.adopt(batch);
	}
}

/// What the statement's breakers hold in memory, as the lease must cover it:
/// the entries of each sort that charges them one by one, and the open groups
/// of each aggregate that can spill.
#[cfg(test)]
fn held_by_breakers(state: &State) -> usize {
	let sorts: usize = state
		.sorts
		.iter()
		.zip(&state.sort_spill)
		.filter(|(_, spill)| spill.as_ref().is_some_and(|spill| spill.keep.is_none()))
		.flat_map(|(entries, _)| entries)
		.map(super::sort::sorted_bytes)
		.sum();
	let groups: usize =
		state.groups.iter().flatten().filter(|groups| groups.spillable()).map(Groups::bytes).sum();
	sorts + groups
}

/// What the records `partial` holds in memory weigh, as a breaker's account
/// counts them.
fn held_bytes(partial: &Partial) -> usize {
	partial
		.held
		.iter()
		.map(|(keys, row)| {
			crate::exec::memory::size::values_bytes(keys)
				+ crate::exec::memory::size::value_bytes(row)
		})
		.sum()
}

/// Fold a batch's partial groups into the groups the statement has.
fn merge_groups(main: &mut Option<Groups>, part: Option<Groups>) -> FlowResult<()> {
	match (main, part) {
		(Some(main), Some(part)) => main.absorb(part).map_err(ControlFlow::Err),
		(_, None) => Ok(()),
		(None, Some(_)) => {
			Err(ControlFlow::Err(anyhow::anyhow!("a region merged groups of another kind")))
		}
	}
}

/// What a batch runs under besides its rows: the statement's frames, the
/// block names it reads, the verdicts its filters already hold on the
/// conjuncts they judge once — a batch that finds none judges them for
/// itself, deterministic as they are — whether its permissions are bypassed,
/// and what the run shares with its batches.
struct Under {
	frames: Vec<ExecutionContext>,
	slots: Vec<Value>,
	/// The bytes the statement's ledger charges each slot, which the batch's
	/// copy of the slots is charged again.
	kept: Vec<usize>,
	verdicts: Vec<Option<bool>>,
	bypassed: bool,
	shared: super::interp::RunShare,
}

impl Under {
	/// What a batch of `program` runs under, from the statement `state`.
	#[cfg(test)]
	fn of(state: &mut State, program: &Program, ctx: &ExecutionContext) -> FlowResult<Self> {
		Ok(Self {
			frames: state.frames.clone(),
			slots: state.slots.clone(),
			kept: state.slot_charges(),
			verdicts: state.verdicts.clone(),
			bypassed: state.permissions_bypassed,
			shared: state.shared(program, ctx.function_registry()).map_err(ControlFlow::Err)?,
		})
	}
}

/// Run one row of a region's body. A batch's paging never spends its limit,
/// so the body never stops its pipeline; one that did would answer fewer rows
/// than the serial run.
async fn run_row(
	program: &Program,
	pipeline: &Pipeline,
	row: Value,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<()> {
	match run_value_row(program, pipeline, row, state, ctx).await? {
		Ended::Stop => Err(ControlFlow::Err(anyhow::Error::new(
			crate::err::EngineError::Internal("a region's body stopped its pipeline".to_owned()),
		))),
		Ended::Next => Ok(()),
	}
}

/// Run a batch of pipeline `at` on a state of its own, under the statement's
/// frames and the block names it reads. A body `paged` by a clause runs each
/// row as though it were taken, but for the first rows reaching the paging
/// that the clause is sure to skip, and records how each row ended; the batch
/// stops at a row that fails before its paging, which no later row can be
/// reached past. A `bound` batch charges its breakers to the query's lease and
/// spills those it outgrows ([`BatchResult::spilled`]).
async fn run_batch(
	program: &Arc<Program>,
	at: usize,
	rows: Vec<Value>,
	paged: Option<(u16, usize)>,
	bound: bool,
	under: Under,
	ctx: &ExecutionContext,
) -> FlowResult<BatchResult> {
	let started = web_time::Instant::now();
	let mut state = batch_state(program, paged, under, ctx).await?;
	if bound {
		super::host::bound_batch(program, &mut state, ctx);
	}
	let pipeline = &program.pipelines[at];
	let count = rows.len();
	let mut ends = Vec::new();
	for row in rows {
		let Some((limit, _)) = paged else {
			run_row(program, pipeline, row, &mut state, ctx).await?;
			if state.memory.is_some() {
				super::host::spill_breakers(&mut state, ctx).await?;
			}
			continue;
		};
		state.reached_paging = false;
		let before = state.output.len();
		let skip = state.paging[limit as usize].skip;
		let failure = run_row(program, pipeline, row, &mut state, ctx).await.err();
		let reached = state.reached_paging;
		let stops = failure.is_some() && !reached;
		ends.push(RowEnd {
			reached,
			skipped: state.paging[limit as usize].skip < skip,
			emitted: state.output.len() - before,
			failure,
		});
		if state.memory.is_some() {
			super::host::spill_breakers(&mut state, ctx).await?;
		}
		if stops {
			break;
		}
	}
	// A breaker's reservation leaves the state before its buffer becomes a
	// partial, which clears the state's account: the lease keeps counting the
	// buffer until the partial's own records are charged in its place, and
	// the account then returns to the state to travel with the batch to the
	// merge.
	let mut spilled = Vec::new();
	#[cfg(test)]
	let leased = state.memory.as_ref().map(|memory| memory.lease().held());
	for sort in 0..program.sorts.len() {
		if state.sort_runs[sort].is_some() {
			let mut account =
				std::mem::replace(&mut state.sort_accounts[sort], Reservation::unbounded());
			let partial = Partial::of_sort(&mut state, program, sort as u16, ctx).await?;
			account.hold(held_bytes(&partial));
			state.sort_accounts[sort] = account;
			spilled.push(partial);
		}
	}
	for aggregate in 0..program.aggregates.len() {
		if state.group_runs[aggregate].is_some() {
			let mut account =
				std::mem::replace(&mut state.group_accounts[aggregate], Reservation::unbounded());
			let partial = Partial::of_groups(&mut state, program, aggregate as u16)?;
			account.hold(held_bytes(&partial));
			state.group_accounts[aggregate] = account;
			spilled.push(partial);
		}
	}
	// What the batch holds stays reserved from the lease until the merge
	// takes it over for the statement.
	let (sort_accounts, group_accounts) = if state.memory.is_some() {
		(std::mem::take(&mut state.sort_accounts), std::mem::take(&mut state.group_accounts))
	} else {
		Default::default()
	};
	#[cfg(test)]
	if let (Some(leased), Some(memory)) = (leased, state.memory.as_ref())
		&& memory.lease().held() < leased
	{
		note_lease_fault(&state, LeaseFault::Released);
	}
	#[cfg(test)]
	if let Some(store) = state.memory.as_ref().and_then(|memory| memory.store()) {
		let mut counted = BATCH_PARTIALS.lock().unwrap_or_else(|e| e.into_inner());
		for partial in &spilled {
			counted.entry(store.session().clone()).or_default()[partial_slot(partial.spec.kind)] +=
				1;
		}
	}
	Ok(BatchResult {
		output: state.take_output(),
		sorts: std::mem::take(&mut state.sorts),
		arrivals: std::mem::take(&mut state.sort_arrivals),
		groups: std::mem::take(&mut state.groups),
		spilled,
		sort_accounts,
		group_accounts,
		ends,
		rows: count,
		elapsed: started.elapsed(),
	})
}

/// A state for a batch of `program` under `under`: its paging, its fuel and
/// the statement's frames and block names.
async fn batch_state(
	program: &Arc<Program>,
	paged: Option<(u16, usize)>,
	under: Under,
	ctx: &ExecutionContext,
) -> FlowResult<super::interp::PooledState> {
	// Paging that never spends, and skips only the rows sure to be skipped;
	// a statement the body runs starts its own paging afresh.
	let mut paging = vec![
		Paging {
			skip: 0,
			remaining: None,
		};
		program.limits.len()
	];
	if let Some((limit, skip)) = paged {
		paging[limit as usize].skip = skip;
	}
	let mut state = State::sharing(program, paging, under.shared);
	// A worker's task draws on its own thread's fuel, no more than the
	// statement's quantum before its first yield.
	super::fuel::cap(super::fuel::quantum(ctx));
	state.program = Some(Arc::clone(program));
	state.frames = under.frames;
	state.slots = under.slots;
	state.keep_copied(under.kept, ctx).await?;
	state.verdicts = under.verdicts;
	state.permissions_bypassed = under.bypassed;
	Ok(state)
}

/// How many partials region batches have handed their merge, by the session
/// of the store they spilled to and then by breaker (sort, groups), for tests
/// that must know their own batches spilled while others run beside them.
#[cfg(test)]
pub(super) static BATCH_PARTIALS: std::sync::LazyLock<
	std::sync::Mutex<std::collections::HashMap<crate::exec::memory::SessionName, [usize; 2]>>,
> = std::sync::LazyLock::new(Default::default);

/// Where `kind`'s count sits in a [`BATCH_PARTIALS`] entry.
#[cfg(test)]
fn partial_slot(kind: BreakerKind) -> usize {
	usize::from(kind == BreakerKind::Groups)
}

/// How many partials of `kind` (a sort or groups) batches have handed their
/// merge over the store `session` names.
#[cfg(test)]
pub(super) fn batch_partials(
	session: &crate::exec::memory::SessionName,
	kind: BreakerKind,
) -> usize {
	BATCH_PARTIALS
		.lock()
		.unwrap_or_else(|e| e.into_inner())
		.get(session)
		.map_or(0, |counted| counted[partial_slot(kind)])
}

/// A way a region's accounting left the lease counting less than its query
/// holds.
#[cfg(test)]
#[derive(Clone, Copy)]
pub(super) enum LeaseFault {
	/// A batch's lease reading fell while its buffers became partials.
	Released,
	/// The merge finished while the lease counted less than the statement's
	/// sorts and groups hold.
	Uncounted,
}

/// How many times each [`LeaseFault`] (released, uncounted) was seen by the
/// batches and merges over a store's session.
#[cfg(test)]
static LEASE_FAULTS: std::sync::LazyLock<
	std::sync::Mutex<std::collections::HashMap<crate::exec::memory::SessionName, [usize; 2]>>,
> = std::sync::LazyLock::new(Default::default);

#[cfg(test)]
fn note_lease_fault(state: &State, fault: LeaseFault) {
	if let Some(store) = state.memory.as_ref().and_then(|memory| memory.store()) {
		LEASE_FAULTS
			.lock()
			.unwrap_or_else(|e| e.into_inner())
			.entry(store.session().clone())
			.or_default()[fault as usize] += 1;
	}
}

/// How many times `fault` was seen over the store `session` names.
#[cfg(test)]
pub(super) fn lease_faults(session: &crate::exec::memory::SessionName, fault: LeaseFault) -> usize {
	LEASE_FAULTS
		.lock()
		.unwrap_or_else(|e| e.into_inner())
		.get(session)
		.map_or(0, |seen| seen[fault as usize])
}

/// Run `rows` as one bound batch of pipeline `at` of `program` and merge it
/// into `state`, a statement's, as a region does.
#[cfg(test)]
pub(super) async fn merge_batch(
	program: &Arc<Program>,
	at: usize,
	rows: Vec<Value>,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<()> {
	let under = Under::of(state, program, ctx)?;
	let result = run_batch(program, at, rows, None, true, under, ctx).await?;
	Region::new(program, at, ctx).absorb(result, state, ctx).await
}

/// Run `rows` through pipeline `at` of `program` on a state of its own, its
/// breakers bound by `ctx`'s query memory and spilled after each row as a
/// region batch's are: a producer of a simulated fan.
#[cfg(test)]
pub(super) async fn run_morsel(
	program: &Arc<Program>,
	at: usize,
	rows: Vec<Value>,
	ctx: &ExecutionContext,
) -> FlowResult<super::interp::PooledState> {
	let mut statement = super::host::statement_state(program, ctx).map_err(ControlFlow::Err)?;
	let under = Under::of(&mut statement, program, ctx)?;
	let pipeline = &program.pipelines[at];
	if pipeline.enforce.is_some() {
		return Err(ControlFlow::Err(anyhow::anyhow!(
			"Internal error: a morsel of a pipeline that enforces its rows"
		)));
	}
	let mut state = batch_state(program, None, under, ctx).await?;
	super::host::bound_batch(program, &mut state, ctx);
	for row in rows {
		run_row(program, pipeline, row, &mut state, ctx).await?;
		if state.memory.is_some() {
			super::host::spill_breakers(&mut state, ctx).await?;
		}
	}
	Ok(state)
}

/// Run a batch on a worker holding `place` in the budget until it is done.
#[cfg(not(target_family = "wasm"))]
#[expect(
	clippy::too_many_arguments,
	reason = "a batch with the program, rows, paging, binding, state, context and place it runs in"
)]
fn spawn(
	program: &Arc<Program>,
	at: usize,
	rows: Vec<Value>,
	paged: Option<(u16, usize)>,
	bound: bool,
	under: Under,
	ctx: &ExecutionContext,
	place: Place,
) -> Worker {
	#[cfg(test)]
	HANDED_TO_WORKERS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
	let program = Arc::clone(program);
	let ctx = ctx.clone();
	let handle = tokio::spawn(crate::exec::compiles::while_running_task(async move {
		let _place = place;
		let body =
			Box::pin(async move { run_batch(&program, at, rows, paged, bound, under, &ctx).await });
		// What nests beneath the body runs on this worker's own frame stack.
		let mut driven = crate::exec::trampoline::Trampolined::root(futures::stream::once(body));
		match driven.next().await {
			Some(result) => result,
			None => Err(ControlFlow::Err(anyhow::anyhow!("a region worker answered nothing"))),
		}
	}));
	Worker(Some(handle))
}
