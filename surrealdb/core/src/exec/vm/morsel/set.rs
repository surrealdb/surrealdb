//! A pipeline's morsels in progress: queued, running and merged in the serial order.

use std::collections::VecDeque;
use std::sync::Arc;

use super::super::host::{Out, flush};
use super::super::interp::State;
use super::super::program::Program;
use super::fold::Fold;
use super::grouped::Pending;
use super::lease::FanoutLease;
use super::merge::{BodyBreakers, Replay, body_breakers, combinable, dedupe, fold_into};
use super::places::{self, Asking, Place};
#[cfg(not(target_family = "wasm"))]
use super::run::Worker as BatchWorker;
use super::run::{MorselResult, PendingBatch, Under, run_batch};
use crate::catalog::TableLayouts;
#[cfg(not(target_family = "wasm"))]
use crate::exec::memory::size::values_bytes;
use crate::exec::planner::placement::{INLINE_BUDGET, Policy, Signals};
use crate::exec::topk_pushdown::TopKThresholdProbe;
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::ControlFlow;
use crate::val::Value;

/// A batch whose results the merge has not taken.
pub(super) struct Held {
	batch: PendingBatch,
	/// Until when a test holds the batch back from the merge, as though its worker were still
	/// running.
	#[cfg(test)]
	ready: Option<tokio::time::Instant>,
}

impl Held {
	fn new(batch: PendingBatch) -> Self {
		Self {
			batch,
			#[cfg(test)]
			ready: None,
		}
	}

	/// Whether the batch's results can be taken without waiting.
	fn is_done(&self) -> bool {
		#[cfg(test)]
		if self.ready.is_some_and(|at| at > tokio::time::Instant::now()) {
			return false;
		}
		self.batch.is_done()
	}

	/// Whether the batch is running on a worker, which holds a place until it is done.
	fn holds_place(&self) -> bool {
		self.batch.holds_place()
	}

	/// The batch's results, waiting for its worker.
	async fn result(self) -> FlowResult<MorselResult> {
		#[cfg(test)]
		if let Some(at) = self.ready {
			tokio::time::sleep_until(at).await;
		}
		self.batch.result().await
	}
}

/// The morsels of a Batches-input pipeline in progress; see the module documentation.
pub(in crate::exec::vm) struct MorselSet<'p> {
	program: &'p Arc<Program>,
	at: usize,
	policy: Policy,
	/// The rows of the batch being filled.
	pub(super) filling: Vec<Value>,
	/// The place the batch being filled holds, once it is to go to a worker.
	place: Option<Place>,
	pub(super) merging: VecDeque<Held>,
	/// The cost of a row measured so far, in nanoseconds.
	cost: Option<f64>,
	/// The sorts and aggregates the body fills.
	breakers: BodyBreakers,
	/// How the merge has counted a paged body's rows.
	pub(super) replay: Replay,
	/// The groups of the morsels merged but not yet folded into the statement's.
	pending: Pending,
	/// For a body that pages nothing itself, the rows its reader takes before it may stop, until
	/// the pipeline first hands rows on ([`Rows::reading`](super::Rows::reading)).
	pub(super) reads: Option<u32>,
	/// The rows of the batches the merge has not taken yet.
	in_flight: usize,
	/// The run's registration, once this set has asked it for a place.
	asking: Asking,
	/// The bytes the run's workers hold only because it fanned out, granted before each hand-out.
	lease: Arc<FanoutLease>,
	/// Whether the merge takes a batch's results as it finishes, whatever the batches before it
	/// are doing ([`MergeOrder::Free`](crate::exec::planner::placement::MergeOrder::Free)).
	free: bool,
	/// The accumulators whose terms the batches keep for the merge to fold in order.
	fold: Option<Fold>,
}

impl<'p> MorselSet<'p> {
	pub(super) fn new(program: &'p Arc<Program>, at: usize, ctx: &ExecutionContext) -> Self {
		let breakers = body_breakers(&program.pipelines[at].code);
		Self {
			program,
			at,
			policy: Policy::from_config(&ctx.root().ctx.config.exec),
			filling: Vec::new(),
			place: None,
			merging: VecDeque::new(),
			cost: None,
			fold: Fold::of(&program.pipelines[at], &breakers),
			breakers,
			replay: Replay::default(),
			pending: Pending::default(),
			reads: None,
			in_flight: 0,
			asking: Asking::default(),
			lease: FanoutLease::new(ctx.ctx().memory().cloned()),
			free: super::merge_of(&program.pipelines[at]) == MergeOrder::Free,
		}
	}

	/// What the run has observed, for the policy's run-time questions.
	fn signals(&self, state: &State) -> Signals {
		let (skip, remaining) = match self.breakers.paged {
			Some(paged) => {
				let paging = state.paging[paged as usize];
				(paging.skip, paging.remaining)
			}
			None => (0, None),
		};
		Signals {
			merged: self.replay.merged,
			reached: self.replay.reached,
			cost_ns_per_row: self.cost,
			scan_hint: self.reads,
			paged: self.breakers.paged.is_some(),
			skip,
			remaining,
			places_held: self.places_held(),
			..Signals::default()
		}
	}

	/// The rows the window leaves past those in flight and being filled, at least one;
	/// `usize::MAX` without a window.
	pub(super) fn wanted(&self, state: &State) -> usize {
		self.policy.window(&self.signals(state)).map_or(usize::MAX, |window| {
			window.saturating_sub(self.in_flight + self.filling.len()).max(1)
		})
	}

	/// Take one row the source yielded; `true` when the pipeline takes no more.
	pub(super) async fn push(
		&mut self,
		row: Value,
		state: &mut State,
		ctx: &ExecutionContext,
		yielder: &mut Out<'_, '_>,
		flush_at: usize,
	) -> FlowResult<bool> {
		// Taking the row is a unit of the driver's work, whether it runs the row or hands it to a
		// worker.
		super::super::fuel::tick(ctx, 1).await?;
		if self.replay.stopped {
			return Ok(true);
		}
		self.filling.push(row);
		if let Some(window) = self.policy.window(&self.signals(state))
			&& self.in_flight + self.filling.len() >= window
		{
			self.dispatch(state, ctx, yielder, flush_at).await?;
			while !self.replay.stopped
				&& !self.merging.is_empty()
				&& self
					.policy
					.window(&self.signals(state))
					.is_some_and(|window| self.in_flight >= window)
			{
				self.merge(true, state, yielder, flush_at, ctx).await?;
			}
			return Ok(self.replay.stopped);
		}
		if self.place.is_none()
			&& self.filling.len() >= self.policy.batch_rows(&self.signals(state), INLINE_BUDGET)
		{
			self.place = self.free_place(state);
			if self.place.is_none() {
				self.dispatch(state, ctx, yielder, flush_at).await?;
				return Ok(self.replay.stopped);
			}
		}
		if self.place.is_some()
			&& self.filling.len() >= self.policy.worker_batch_rows(&self.signals(state))
		{
			self.dispatch(state, ctx, yielder, flush_at).await?;
		}
		Ok(self.replay.stopped)
	}

	/// A place for the batch being filled, when the batch may go to a worker at all: the policy
	/// allows workers and the run's lease has not been revoked.
	pub(super) fn free_place(&mut self, state: &mut State) -> Option<Place> {
		if !self.policy.may_use_workers() || self.lease.revoked() {
			return None;
		}
		let place = places::of(state).acquire(self.policy);
		self.asking.note(state);
		place
	}

	/// The places this pipeline's batches hold: the one being filled for, and each batch still
	/// running on a worker.
	fn places_held(&self) -> usize {
		usize::from(self.place.is_some())
			+ self.merging.iter().filter(|batch| batch.holds_place()).count()
	}

	/// Run the batch being filled: on a worker when it holds a place and enough rows and the lease
	/// grants the worker's copy of the slots, and on the statement's task otherwise.
	pub(super) async fn dispatch(
		&mut self,
		state: &mut State,
		ctx: &ExecutionContext,
		yielder: &mut Out<'_, '_>,
		flush_at: usize,
	) -> FlowResult<()> {
		let rows = std::mem::take(&mut self.filling);
		let place = self.place.take();
		// The rows in flight ahead of this batch are the most that can reach the paging before it,
		// so `START` skips at least the rest.
		let paged = self
			.breakers
			.paged
			.map(|limit| (limit, state.paging[limit as usize].skip.saturating_sub(self.in_flight)));
		self.in_flight += rows.len();
		let mut under = Under::of(state, self.program, ctx)?;
		under.fold.clone_from(&self.fold);
		// A paged body's batches hold their rows for the merge to count and are never bound.
		let bound = self.breakers.paged.is_none();
		// The grant is asked for with the place in hand, so a refusal gives the place back and the
		// batch runs here, as when no place is free.
		#[cfg(not(target_family = "wasm"))]
		let grant = match place {
			Some(_) if self.policy.hands_to_worker(rows.len(), true) => {
				match self.lease.grant(under.kept.iter().sum(), ctx).await {
					// The rows read ahead of the merge are held only because the batch fanned out,
					// and so are the terms it keeps for the merge to fold.
					Some(copy) => {
						let read = if self.lease.bounded() {
							values_bytes(&rows)
								+ self.fold.as_ref().map_or(0, |fold| fold.block_bytes(rows.len()))
						} else {
							0
						};
						#[cfg(test)]
						super::test_hooks::note_terms_charged(
							ctx,
							self.fold.as_ref().map_or(0, |fold| fold.block_bytes(rows.len())),
						);
						self.lease.grant_rows(read).map(|read| copy.with(read))
					}
					None => None,
				}
			}
			_ => None,
		};
		#[cfg(target_family = "wasm")]
		let grant: Option<super::lease::Grant> = None;
		// A worker's copy of the slots is the grant's; a batch run here charges none, as the serial
		// run holds no copy.
		under.covered = true;
		let batch = match place.zip(grant) {
			#[cfg(not(target_family = "wasm"))]
			Some((place, grant)) => Held::new(PendingBatch::Running(super::run::spawn(
				self.program,
				self.at,
				rows,
				paged,
				bound,
				under,
				ctx,
				place,
				grant,
			))),
			_ => {
				// The terms a batch run here keeps for the merge stay held until the merge has
				// folded them, as the terms of a batch on a worker do, so they are charged the
				// same way. A refused grant, or a revoked lease, still runs the batch with its
				// block open, because the merge folds the terms in order to answer bit for bit
				// as the serial fold does. Those terms are held outside the lease until the
				// merge takes the batch, so they stay within the batches in flight, at most
				// `read_ahead` and the one merging, times the rows of a batch, the kept slots
				// and `size_of::<Number>()`.
				let terms = self.fold.as_ref().and_then(|fold| {
					let bytes = fold.block_bytes(rows.len());
					#[cfg(test)]
					super::test_hooks::note_terms_charged(ctx, bytes);
					self.lease.grant_terms(bytes, ctx)
				});
				let mut done =
					run_batch(self.program, self.at, rows, paged, bound, under, ctx).await;
				if let (Ok(result), Some(fold), Some(terms)) = (&mut done, &self.fold, terms) {
					fold.hold_result(result, Box::new(terms));
				}
				Held::new(PendingBatch::Done(Box::new(done)))
			}
		};
		#[cfg(test)]
		let batch = {
			let mut batch = batch;
			if matches!(batch.batch, PendingBatch::Running(_)) {
				batch.ready = super::test_hooks::claim_delay(ctx)
					.map(|delay| tokio::time::Instant::now() + delay);
			}
			batch
		};
		self.merging.push_back(batch);
		let waiting = self.merging.len() > self.policy.read_ahead();
		self.merge(waiting, state, yielder, flush_at, ctx).await
	}

	pub(super) async fn finish(
		&mut self,
		state: &mut State,
		ctx: &ExecutionContext,
		yielder: &mut Out<'_, '_>,
		flush_at: usize,
	) -> FlowResult<bool> {
		if !self.replay.stopped && !self.filling.is_empty() {
			self.dispatch(state, ctx, yielder, flush_at).await?;
		}
		self.place = None;
		while !self.merging.is_empty() {
			self.merge(true, state, yielder, flush_at, ctx).await?;
		}
		self.pending.flush(self.program, &self.breakers, self.policy, state, ctx).await?;
		Ok(self.replay.stopped)
	}

	/// The next batch the merge takes: the one at the front when it is done or `waiting`, or, for
	/// a merge that takes results as batches finish, the first batch that is done, waiting for the
	/// first to finish when `waiting` and none has.
	async fn next_batch(&mut self, waiting: bool, ctx: &ExecutionContext) -> Option<Held> {
		if !self.free {
			return if self.merging.front().is_some_and(|batch| waiting || batch.is_done()) {
				self.merging.pop_front()
			} else {
				None
			};
		}
		let at = if waiting {
			self.first_done().await?
		} else {
			self.merging.iter().position(Held::is_done)?
		};
		#[cfg(test)]
		if at > 0 {
			super::test_hooks::note_merged_early(ctx);
		}
		#[cfg(not(test))]
		let _ = ctx;
		self.merging.remove(at)
	}

	/// The index of the first batch whose results can be taken without waiting, waiting for the
	/// first of the batches on workers to finish when none can; `None` when no batch is running or
	/// held back. A batch that finished while waited for is turned into its results, so nothing is
	/// polled twice.
	#[cfg(not(target_family = "wasm"))]
	async fn first_done(&mut self) -> Option<usize> {
		use std::task::Poll;
		loop {
			if let Some(at) = self.merging.iter().position(Held::is_done) {
				return Some(at);
			}
			#[cfg(test)]
			let mut held_back = {
				let now = tokio::time::Instant::now();
				self.merging
					.iter()
					.filter_map(|held| held.ready)
					.filter(|at| *at > now)
					.min()
					.map(|at| Box::pin(tokio::time::sleep_until(at)))
			};
			#[cfg(test)]
			let waits = held_back.is_some();
			#[cfg(not(test))]
			let waits = false;
			let running = self
				.merging
				.iter()
				.any(|held| matches!(&held.batch, PendingBatch::Running(BatchWorker(Some(_)))));
			if !running && !waits {
				return None;
			}
			let finished = std::future::poll_fn(|cx| {
				for (at, held) in self.merging.iter_mut().enumerate() {
					if let PendingBatch::Running(BatchWorker(Some(handle))) = &mut held.batch
						&& let Poll::Ready(joined) = std::pin::Pin::new(handle).poll(cx)
					{
						return Poll::Ready(Some((at, joined)));
					}
				}
				#[cfg(test)]
				if let Some(sleep) = held_back.as_mut()
					&& sleep.as_mut().poll(cx).is_ready()
				{
					return Poll::Ready(None);
				}
				Poll::Pending
			})
			.await;
			if let Some((at, joined)) = finished {
				self.merging[at].batch = PendingBatch::Done(Box::new(match joined {
					Ok(result) => result,
					Err(e) => Err(ControlFlow::Err(anyhow::anyhow!("a morsel worker failed: {e}"))),
				}));
			}
		}
	}

	/// The index of the first batch whose results can be taken: with no workers, every batch has
	/// run.
	#[cfg(target_family = "wasm")]
	async fn first_done(&mut self) -> Option<usize> {
		self.merging.iter().position(Held::is_done)
	}

	/// Take the results of the batches that are done, waiting for the first when `waiting`: the
	/// ones at the front, in order, or, for a merge that takes results as batches finish, any.
	/// Once a paged body's limit is spent, every batch still out is dropped, aborting its worker.
	pub(super) async fn merge(
		&mut self,
		mut waiting: bool,
		state: &mut State,
		yielder: &mut Out<'_, '_>,
		flush_at: usize,
		ctx: &ExecutionContext,
	) -> FlowResult<()> {
		while let Some(batch) = self.next_batch(waiting, ctx).await {
			waiting = false;
			let result: MorselResult = batch.result().await?;
			// Merging a batch walks its rows, sorted rows and groups once: no more than a batch's
			// worth, spent whole, the driver yielding at the next row it takes.
			super::super::fuel::charge(super::super::fuel::units(result.rows));
			self.in_flight -= result.rows;
			if result.rows > 0 {
				let cost = result.elapsed.as_nanos() as f64 / result.rows as f64;
				self.cost = Some(self.policy.smooth_cost(self.cost, cost));
			}
			match self.breakers.paged {
				Some(limit) => self.replay.page(limit, result, state)?,
				None => match self.breakers.distinct {
					Some(distinct) => dedupe(distinct, result, state)?,
					None => {
						self.pending
							.take(self.program, &self.breakers, self.policy, result, state, ctx)
							.await?
					}
				},
			}
			// Rows handed on to a reader that then asks for more: it takes more than it said it
			// might.
			if state.output.len() >= flush_at {
				self.reads = None;
			}
			flush(state, yielder, flush_at, ctx).await?;
			if self.replay.stopped {
				self.merging.clear();
				self.filling.clear();
				self.place = None;
				break;
			}
		}
		Ok(())
	}

	/// The source failed with `failure` after the rows it yielded: the failure of the earliest
	/// batch before it that failed, or `failure`. `true` when a paged body's limit is spent before
	/// the failure.
	pub(super) async fn fail(
		&mut self,
		failure: ControlFlow,
		state: &mut State,
		ctx: &ExecutionContext,
		yielder: &mut Out<'_, '_>,
		flush_at: usize,
	) -> FlowResult<bool> {
		if !self.filling.is_empty() {
			self.dispatch(state, ctx, yielder, flush_at).await?;
		}
		if self.replay.stopped {
			return Ok(true);
		}
		self.merging.push_back(Held::new(PendingBatch::Done(Box::new(Err(failure)))));
		while !self.merging.is_empty() {
			self.merge(true, state, yielder, flush_at, ctx).await?;
		}
		if self.replay.stopped {
			return Ok(true);
		}
		Err(ControlFlow::Err(anyhow::anyhow!("a morsel's failure was merged")))
	}
}

// ---------------------------------------------------------------------------
// Range input
// ---------------------------------------------------------------------------

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;

use surrealdb_kvs::Snapshot;

use super::super::host::{admit_row, in_place_row_admitted};
use super::metrics::{MorselMetrics, MorselStat};
use super::outbox::Outbox;
use super::run::{Paged, RangeEnd};
use super::steal::{self, Gate, Origin, Progress, Registered, Slot};
use crate::err::EngineError;
use crate::exec::planner::placement::{Combine, MergeOrder, MorselPlan, Parallel, Split};
use crate::expr::ControlFlowExt;
#[cfg(not(target_family = "wasm"))]
use crate::key::AnyRange;
use crate::key::RawRange;
use crate::kvs::Direction;

/// A Range morsel's position in the serial order of one scan: where its range lies in the scan's
/// direction. Ranges of one scan are disjoint, so the position is unique, and a morsel cut from a
/// range lies between the morsel it was cut from and the next.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct OrderKey {
	/// The member of a union the scan belongs to.
	member: u32,
	/// The range's start key for a forward scan, its end key for a backward one.
	at: Vec<u8>,
	backward: bool,
}

impl OrderKey {
	pub(super) fn of(span: &RawRange, dir: Direction) -> Self {
		let (start, end) = span.bounds();
		match dir {
			Direction::Forward => Self {
				member: 0,
				at: start.to_vec(),
				backward: false,
			},
			Direction::Backward => Self {
				member: 0,
				at: end.to_vec(),
				backward: true,
			},
		}
	}
}

impl Ord for OrderKey {
	fn cmp(&self, other: &Self) -> Ordering {
		self.member.cmp(&other.member).then_with(|| {
			let order = self.at.cmp(&other.at);
			if self.backward {
				order.reverse()
			} else {
				order
			}
		})
	}
}

impl PartialOrd for OrderKey {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

/// A morsel a worker has taken.
enum Started {
	/// Running on a worker; a morsel that streams its rows hands them over through its outbox.
	Running(Option<Arc<Outbox>>),
	/// Finished, waiting for the merge.
	Done(Box<FlowResult<MorselResult>>),
}

/// A morsel waiting for a worker or the coordinator.
pub(super) struct Queued {
	span: RawRange,
	origin: Origin,
}

#[derive(Default)]
pub(super) struct Table {
	/// The morsels waiting for a worker or the coordinator, by order key.
	pub(super) queued: BTreeMap<OrderKey, Queued>,
	/// The morsels a worker has taken and the merge has not, by order key.
	started: BTreeMap<OrderKey, Started>,
	/// The results of morsels the coordinator took out of `started` and holds until the morsels
	/// before them are merged. They count with `started` against the number of morsels the policy
	/// lets be running or finished and unmerged.
	held: usize,
	/// The earliest morsel that failed: no morsel after it is needed.
	failed: Option<OrderKey>,
	/// What the morsels running now publish, for an idle worker to choose whom to ask.
	pub(super) slots: Vec<Arc<Slot>>,
}

/// What a worker finds in the table.
pub(super) enum Claim {
	/// The earliest waiting morsel, now the worker's, with the outbox its rows leave through where
	/// the scan streams them.
	Morsel(OrderKey, RawRange, Option<Arc<Outbox>>),
	/// Morsels wait, but as many are in flight as the policy allows.
	Full,
	/// No morsel waits, or the scan is done.
	Empty,
}

/// What the coordinator takes from the table next.
enum Step {
	/// A morsel is done, and the merge takes it. Boxed: a result is large, and the other steps are
	/// not. `true` when a morsel before it in the scan had not finished, which only a merge that
	/// takes results as morsels finish does.
	Merge(Box<FlowResult<MorselResult>>, bool),
	/// A running morsel has produced rows, taken in the order the morsel produced them: its merge
	/// order is the serial run's when it is at the front, or free.
	Rows(Vec<Value>, Arc<Outbox>),
	/// A morsel is done, ahead of morsels before it in the scan that are not: the coordinator holds
	/// its results until they are next to what the statement has merged. Only a merge that takes
	/// results as morsels finish and absorbs them in order does.
	Collect(OrderKey, Box<FlowResult<MorselResult>>),
	/// The morsel at the front is waiting: run it on the coordinator.
	Run(RawRange),
	/// The morsel at the front is running on a worker.
	Wait,
	/// No morsel is left.
	Empty,
}

/// The morsels of one scan that the coordinator and its workers share.
pub(super) struct Shared {
	table: Mutex<Table>,
	/// Rung when a worker finishes a morsel, for the coordinator waiting on the front.
	wake: tokio::sync::Notify,
	/// Rung when a morsel is queued, a running morsel answers a steal or ends, or the scan stops,
	/// for the idle workers waiting to steal.
	pub(super) arrived: tokio::sync::Notify,
	/// Set once the statement needs no more from its morsels.
	stop: AtomicBool,
	/// Whether the morsels hand their rows to the coordinator as they produce them.
	streams: bool,
	/// Rung when a running morsel puts rows in its outbox, for the coordinator waiting on the
	/// morsels.
	pub(super) fed: tokio::sync::Notify,
	/// What the scan's morsels did.
	metrics: Mutex<MorselMetrics>,
}

impl Shared {
	pub(super) fn new() -> Self {
		Self {
			table: Mutex::new(Table::default()),
			wake: tokio::sync::Notify::new(),
			arrived: tokio::sync::Notify::new(),
			stop: AtomicBool::new(false),
			streams: false,
			fed: tokio::sync::Notify::new(),
			metrics: Mutex::new(MorselMetrics::default()),
		}
	}

	/// Whether the morsels hand their rows to the coordinator as they produce them.
	pub(super) fn streams(&self) -> bool {
		self.streams
	}

	/// This scan with its morsels handing their rows to the coordinator as they produce them.
	pub(super) fn streaming(mut self) -> Self {
		self.streams = true;
		self
	}

	pub(super) fn table(&self) -> std::sync::MutexGuard<'_, Table> {
		self.table.lock().unwrap_or_else(|e| e.into_inner())
	}

	pub(super) fn metrics(&self) -> std::sync::MutexGuard<'_, MorselMetrics> {
		self.metrics.lock().unwrap_or_else(|e| e.into_inner())
	}

	pub(super) fn stopped(&self) -> bool {
		self.stop.load(std::sync::atomic::Ordering::Acquire)
	}

	fn stop(&self) {
		self.stop.store(true, std::sync::atomic::Ordering::Release);
		self.arrived.notify_waiters();
	}

	/// Queue a morsel, counted by how it came to be.
	pub(super) fn queue(&self, key: OrderKey, span: RawRange, origin: Origin) {
		self.table().queued.insert(
			key,
			Queued {
				span,
				origin,
			},
		);
		self.count(origin);
		self.arrived.notify_waiters();
	}

	fn count(&self, origin: Origin) {
		let mut metrics = self.metrics();
		match origin {
			Origin::Split => metrics.split += 1,
			Origin::Stolen => metrics.stolen += 1,
			Origin::Resplit => metrics.resplit += 1,
			Origin::Parked => metrics.parked += 1,
		}
	}

	/// How many morsels wait for a worker or the coordinator.
	pub(super) fn queued_len(&self) -> usize {
		self.table().queued.len()
	}

	/// How many morsels wait for a worker, and how many more may be taken before `most` are running
	/// or finished and waiting for the merge.
	fn open(&self, most: usize) -> (usize, usize) {
		let table = self.table();
		(table.queued.len(), most.saturating_sub(table.started.len() + table.held))
	}

	/// Whether the morsel at `key` is no longer needed: the statement is done, or an earlier
	/// morsel has failed.
	pub(super) fn cancelled(&self, key: &OrderKey) -> bool {
		self.stopped() || self.table().failed.as_ref().is_some_and(|failed| key > failed)
	}

	/// Take the earliest waiting morsel for a worker, unless `most` morsels are already running
	/// or finished and waiting for the merge. Morsels a failure has made unneeded are dropped.
	pub(super) fn claim(&self, most: usize) -> Claim {
		if self.stopped() {
			return Claim::Empty;
		}
		let mut table = self.table();
		let table = &mut *table;
		if let Some(failed) = &table.failed {
			let before = table.queued.len();
			table.queued.retain(|key, _| key <= failed);
			let dropped = (before - table.queued.len()) as u64;
			if dropped > 0 {
				self.metrics().cancelled += dropped;
			}
		}
		if table.queued.is_empty() {
			return Claim::Empty;
		}
		if table.started.len() + table.held >= most {
			return Claim::Full;
		}
		let Some((key, queued)) = table.queued.pop_first() else {
			return Claim::Empty;
		};
		if queued.origin == Origin::Parked {
			self.metrics().resumed += 1;
		}
		let outbox = self.streams.then(|| Arc::new(Outbox::default()));
		table.started.insert(key.clone(), Started::Running(outbox.clone()));
		Claim::Morsel(key, queued.span, outbox)
	}

	/// A worker's morsel at `key` ended with `end`.
	pub(super) fn complete(&self, key: OrderKey, end: FlowResult<RangeEnd>) {
		let mut table = self.table();
		match end {
			Ok(RangeEnd::Done(result)) => {
				table.started.insert(key, Started::Done(Box::new(Ok(result))));
			}
			Ok(RangeEnd::Parked(result, rest)) => {
				let order = OrderKey {
					member: key.member,
					at: match key.backward {
						false => rest.bounds().0.to_vec(),
						true => rest.bounds().1.to_vec(),
					},
					backward: key.backward,
				};
				table.started.insert(key, Started::Done(Box::new(Ok(result))));
				table.queued.insert(
					order,
					Queued {
						span: rest,
						origin: Origin::Parked,
					},
				);
				self.count(Origin::Parked);
			}
			Ok(RangeEnd::Cancelled) => {
				table.started.remove(&key);
				self.metrics().cancelled += 1;
			}
			Err(failure) => {
				if table.failed.as_ref().is_none_or(|failed| key < *failed) {
					table.failed = Some(key.clone());
				}
				table.started.insert(key, Started::Done(Box::new(Err(failure))));
			}
		}
		drop(table);
		self.wake.notify_one();
		self.arrived.notify_waiters();
	}

	/// What the coordinator does next, for a merge that takes results as morsels finish: it takes
	/// the earliest morsel that is done, runs the earliest one that waits when none is, and waits
	/// for a running one when none waits.
	fn step_free(&self) -> Step {
		let mut table = self.table();
		let table = &mut *table;
		let done = table.started.iter().enumerate().find_map(|(at, (key, started))| {
			matches!(started, Started::Done(_)).then(|| (at, key.clone()))
		});
		if let Some((at, key)) = done {
			let early =
				at > 0 || table.queued.first_key_value().is_some_and(|(queued, _)| *queued < key);
			if let Some(Started::Done(result)) = table.started.remove(&key) {
				return Step::Merge(result, early);
			}
		}
		for started in table.started.values() {
			if let Started::Running(Some(outbox)) = started
				&& let Some(rows) = outbox.take()
			{
				return Step::Rows(rows, Arc::clone(outbox));
			}
		}
		if let Some((_, queued)) = table.queued.pop_first() {
			if queued.origin == Origin::Parked {
				self.metrics().resumed += 1;
			}
			return Step::Run(queued.span);
		}
		if table.started.is_empty() {
			Step::Empty
		} else {
			Step::Wait
		}
	}

	/// The earliest order key of a morsel the merge has not taken from the table: queued, running,
	/// or done and not yet taken.
	fn front_key(&self) -> Option<OrderKey> {
		let table = self.table();
		match (table.queued.first_key_value(), table.started.first_key_value()) {
			(None, None) => None,
			(Some((key, _)), None) | (None, Some((key, _))) => Some(key.clone()),
			(Some((queued, _)), Some((started, _))) => Some(queued.min(started).clone()),
		}
	}

	/// Whether a morsel lies between those at `after` and `before` in the scan, queued, running, or
	/// done and not yet taken.
	fn between(&self, after: &OrderKey, before: &OrderKey) -> bool {
		use std::ops::Bound::Excluded;
		let range = (Excluded(after), Excluded(before));
		if after >= before {
			return false;
		}
		let table = self.table();
		table.queued.range::<OrderKey, _>(range).next().is_some()
			|| table.started.range::<OrderKey, _>(range).next().is_some()
	}

	/// What the coordinator does next, for a merge that takes results as morsels finish and
	/// absorbs them in the order of the scan: it takes the earliest morsel that is done when
	/// `collect` allows (the one at the front is merged at once), runs the one that waits at the
	/// front, `ready` being the earliest results it holds, and otherwise waits for a running one.
	fn step_early(&self, collect: bool, ready: Option<&OrderKey>) -> Step {
		let mut table = self.table();
		let table = &mut *table;
		let queued_first = table.queued.first_key_value().map(|(key, _)| key);
		let started_first = table.started.first_key_value().map(|(key, _)| key);
		let done = table
			.started
			.iter()
			.find_map(|(key, started)| matches!(started, Started::Done(_)).then(|| key.clone()));
		if let Some(key) = done {
			let front = queued_first.is_none_or(|queued| key < *queued)
				&& started_first.is_some_and(|first| *first == key)
				&& ready.is_none_or(|ready| key < *ready);
			if (front || collect)
				&& let Some(Started::Done(result)) = table.started.remove(&key)
			{
				return if front {
					Step::Merge(result, false)
				} else {
					// Counted as holding a place until the coordinator has decided, in
					// `RangeSet::collect`, whether it does.
					table.held += 1;
					Step::Collect(key, result)
				};
			}
		}
		let queued_first = table.queued.first_key_value().map(|(key, _)| key.clone());
		let started_first = table.started.first_key_value().map(|(key, _)| key.clone());
		if let Some(queued) = queued_first
			&& started_first.as_ref().is_none_or(|started| queued < *started)
			&& ready.is_none_or(|ready| queued < *ready)
			&& let Some((_, queued)) = table.queued.pop_first()
		{
			if queued.origin == Origin::Parked {
				self.metrics().resumed += 1;
			}
			return Step::Run(queued.span);
		}
		if table.queued.is_empty() && table.started.is_empty() && ready.is_none() {
			Step::Empty
		} else {
			Step::Wait
		}
	}

	/// What the coordinator does next with the morsel at the front.
	fn step(&self) -> Step {
		let mut table = self.table();
		let table = &mut *table;
		let queued_first = table.queued.first_key_value().map(|(key, _)| key);
		let started_first = table.started.first_key_value().map(|(key, _)| key);
		let queued_is_front = match (queued_first, started_first) {
			(None, None) => return Step::Empty,
			(Some(_), None) => true,
			(None, Some(_)) => false,
			(Some(queued), Some(started)) => queued < started,
		};
		if queued_is_front {
			return match table.queued.pop_first() {
				Some((_, queued)) => {
					if queued.origin == Origin::Parked {
						self.metrics().resumed += 1;
					}
					Step::Run(queued.span)
				}
				None => Step::Empty,
			};
		}
		if let Some((_, Started::Running(outbox))) = table.started.first_key_value() {
			return match outbox.as_ref().and_then(|outbox| outbox.take().zip(Some(outbox))) {
				Some((rows, outbox)) => Step::Rows(rows, Arc::clone(outbox)),
				None => Step::Wait,
			};
		}
		match table.started.pop_first() {
			Some((_, Started::Done(result))) => Step::Merge(result, false),
			_ => Step::Empty,
		}
	}

	/// [`Self::step`], waiting while the front morsel runs on a worker. A morsel queued meanwhile,
	/// which a donation does, ends the wait with [`Step::Wait`] so the coordinator can hand it to a
	/// worker.
	async fn next(&self, pace: Pace<'_>) -> Step {
		loop {
			let finished = self.wake.notified();
			let queued = self.arrived.notified();
			let fed = self.fed.notified();
			tokio::pin!(finished, queued, fed);
			finished.as_mut().enable();
			queued.as_mut().enable();
			fed.as_mut().enable();
			match match pace.merge {
				MergeOrder::Free => self.step_free(),
				MergeOrder::Positioned | MergeOrder::Adjacent => {
					self.step_early(pace.collect, pace.ready)
				}
				MergeOrder::Serial => self.step(),
			} {
				Step::Wait => {
					let woken = futures::future::select(finished, queued);
					if let futures::future::Either::Left((futures::future::Either::Right(_), _)) =
						futures::future::select(woken, fed).await
					{
						return Step::Wait;
					}
				}
				step @ (Step::Merge(..)
				| Step::Rows(..)
				| Step::Collect(..)
				| Step::Run(_)
				| Step::Empty) => return step,
			}
		}
	}
}

/// The results of a run of adjacent morsels that the coordinator holds until the morsels before
/// them have been merged, and the key of the last morsel the run holds.
struct Ready {
	result: FlowResult<MorselResult>,
	last: OrderKey,
	/// Whether the results count against the morsels the policy lets be running or finished and
	/// unmerged ([`takes_a_place`]), decided once, when the coordinator takes them.
	placed: bool,
}

/// Whether the results of a morsel that finished ahead of an earlier one, held by a merge in order
/// `merge`, count against the morsels the policy lets be running or finished and unmerged; where
/// they do not, the bytes the coordinator spends holding them are charged to the query's lease.
///
/// Every held result either holds a place or has its whole size charged to the lease, so what the
/// coordinator holds is bounded by the policy's read-ahead and the lease together. A sort's sorted
/// run that lies in memory, holds entries and whose every non-empty sort the morsel charged to the
/// query's lease frees its place once its fixed size ([`held_result_bytes`]) is charged to the
/// account of its first non-empty sort: the lease bounds what the morsels hold of the sort, as it
/// bounds what the statement's own sort would, and the statement's sort holds the same rows once it
/// absorbs them, so the places of the morsels after the run stay free for the morsels that follow.
/// A morsel charges its sort entries only where the query can spill (`host::bound_batch`). Every
/// other result holds its place: a run of a query with no store to spill to, which is held
/// uncharged; an empty run, which charges the lease nothing; a run the lease refuses the fixed size
/// of; a run that was spilled and a failure, which the lease does not bound; and the results of the
/// other merges, which hold output or groups the lease does not bound by itself.
fn takes_a_place(merge: MergeOrder, result: &mut FlowResult<MorselResult>) -> bool {
	if merge != MergeOrder::Positioned {
		return true;
	}
	let Ok(result) = result else {
		return true;
	};
	if !result.spilled.is_empty() || !sorts_are_charged(result) {
		return true;
	}
	let Some(first) = result.sorts.iter().position(|entries| !entries.is_empty()) else {
		return true;
	};
	let bytes = held_result_bytes(result);
	!matches!(result.sort_accounts[first].charge(bytes), crate::exec::memory::Verdict::Hold)
}

/// Whether every sort of `result` that holds entries holds them against the query's lease.
fn sorts_are_charged(result: &MorselResult) -> bool {
	result.sorts.iter().enumerate().all(|(at, entries)| {
		entries.is_empty()
			|| result.sort_accounts.get(at).is_some_and(|account| account.memory().is_some())
	})
}

/// The bytes the coordinator spends holding `result` beyond the sort entries its accounts charge:
/// the entry of `RangeSet::ready` with its share of a map node and the key's bytes, and the
/// buffers of the result's own fields.
fn held_result_bytes(result: &MorselResult) -> usize {
	use std::mem::size_of;
	/// A B-tree node holds up to eleven entries; an entry is charged a whole node's share of
	/// edges and header, rounded up.
	const MAP_NODE_SHARE: usize = 64;
	size_of::<OrderKey>()
		+ size_of::<Ready>()
		+ MAP_NODE_SHARE
		+ result.output.capacity() * size_of::<Value>()
		+ result.sorts.capacity() * size_of::<Vec<(Vec<Value>, super::super::interp::SortRow)>>()
		+ result.slabs.capacity() * size_of::<Vec<u8>>()
		+ result.slabs.iter().map(Vec::capacity).sum::<usize>()
		+ result.arrivals.capacity() * size_of::<super::super::interp::Arrivals>()
		+ result.groups.capacity() * size_of::<Option<super::super::interp::Groups>>()
		+ result.spilled.capacity() * size_of::<super::super::partial::Partial>()
		+ (result.sort_accounts.capacity() + result.group_accounts.capacity())
			* size_of::<crate::exec::memory::Reservation>()
		+ result.ends.capacity() * size_of::<super::run::End>()
}

impl Ready {
	/// The rows the held results stand for: none for a failure.
	fn rows(&self) -> usize {
		self.result.as_ref().map_or(0, |result| result.rows)
	}

	/// Take `later`, the results of the morsels that follow this run's, into this run's: the groups
	/// of each aggregate fold into one another, the earlier before the later, and the run now ends
	/// where `later` did. A run that failed, or holds anything but groups it can fold, is not taken
	/// and is answered back.
	fn absorb(&mut self, later: Ready, breakers: &BodyBreakers) -> Option<Ready> {
		let (Ok(mine), Ok(theirs)) = (&mut self.result, &later.result) else {
			return Some(later);
		};
		if !combinable(mine, breakers) || !combinable(theirs, breakers) {
			return Some(later);
		}
		let Ready {
			result: Ok(mut theirs),
			last,
			..
		} = later
		else {
			unreachable!("checked above");
		};
		match fold_into(mine, &mut theirs, breakers) {
			Ok(()) => self.last = last,
			Err(failure) => self.result = Err(failure),
		}
		None
	}
}

/// How the coordinator takes the results of the morsels of a scan.
#[derive(Clone, Copy)]
struct Pace<'r> {
	merge: MergeOrder,
	/// For a merge that absorbs in the order of the scan, whether it may hold the results of one
	/// more morsel.
	collect: bool,
	/// The earliest results it holds, which are not yet next to what the statement has merged.
	ready: Option<&'r OrderKey>,
}

/// A worker's hold on the morsel it runs: if the worker ends without completing it, whether by
/// failing or by being aborted, the morsel is recorded as failed so the coordinator never waits
/// for it.
struct Running<'s> {
	shared: &'s Shared,
	key: Option<OrderKey>,
}

impl Running<'_> {
	fn complete(mut self, end: FlowResult<RangeEnd>) {
		if let Some(key) = self.key.take() {
			self.shared.complete(key, end);
		}
	}
}

impl Drop for Running<'_> {
	fn drop(&mut self) {
		if let Some(key) = self.key.take() {
			self.shared.complete(
				key,
				Err(ControlFlow::Err(anyhow::Error::new(EngineError::Internal(
					"a morsel worker ended before its morsel".to_owned(),
				)))),
			);
		}
	}
}

/// A scan over a key range run as morsels: the coordinator runs the front of the range on the
/// statement, cuts what is left into morsels as the policy says, and merges them in order as
/// workers finish them or as it runs them itself. Running morsels, the coordinator's among them,
/// give part of what they have left to idle workers ([`steal`]).
pub(in crate::exec::vm) struct RangeSet<'p> {
	program: &'p Arc<Program>,
	at: usize,
	policy: Policy,
	dir: Direction,
	version: Option<u64>,
	breakers: BodyBreakers,
	/// How the merge has counted a paged body's rows.
	replay: Replay,
	/// The top-k pushdown the scan's entries are judged by before they run, armed with the
	/// threshold every morsel's sort publishes into.
	probe: Option<Arc<TopKThresholdProbe>>,
	/// The layouts of the scanned table that a packed record is decoded through.
	layouts: Option<Arc<TableLayouts>>,
	/// The groups of the morsels merged but not yet folded into the statement's.
	pending: Pending,
	shared: Arc<Shared>,
	/// The run's registration, once this set has asked it for a place.
	asking: Asking,
	/// The bytes the scan's workers hold only because it fanned out, granted before each worker is
	/// spawned.
	lease: Arc<FanoutLease>,
	/// The order the merge takes the morsels' results in
	/// ([`MorselPlan::merge`](crate::exec::planner::placement::MorselPlan::merge)).
	merge: MergeOrder,
	/// For a merge that takes results as morsels finish and absorbs them in the order of the scan:
	/// the results of the morsels done, by the first key of the morsels each holds, which wait
	/// until what lies before them has been merged.
	ready: BTreeMap<OrderKey, Ready>,
	/// How many of `ready` hold no place of the morsels in flight ([`Ready::placed`]).
	unplaced: usize,
	/// The accumulators whose terms the morsels keep for the merge to fold in order.
	fold: Option<Fold>,
	/// The transaction's read view, asked for once the range is first split.
	view: Option<Arc<dyn Snapshot>>,
	view_asked: bool,
	/// The host's places the scan's morsels also run at, asked for once the range is first split;
	/// `None` where the host gives none or the pipeline has no unit.
	remote: Option<super::remote::Remote>,
	remote_asked: bool,
	#[cfg(not(target_family = "wasm"))]
	workers: Vec<tokio::task::JoinHandle<()>>,
	/// The scan's workers at the host's places, which a target without a task spawner polls
	/// beside the coordinator ([`super::remote::HostTasks`]).
	#[cfg(target_family = "wasm")]
	hosts: Arc<super::remote::HostTasks>,
}

impl Drop for RangeSet<'_> {
	fn drop(&mut self) {
		self.shared.stop();
		#[cfg(not(target_family = "wasm"))]
		for worker in &self.workers {
			worker.abort();
		}
	}
}

impl<'p> RangeSet<'p> {
	pub(super) fn new(
		program: &'p Arc<Program>,
		at: usize,
		dir: Direction,
		version: Option<u64>,
		probe: Option<Arc<TopKThresholdProbe>>,
		layouts: Option<Arc<TableLayouts>>,
		ctx: &ExecutionContext,
	) -> Self {
		let breakers = body_breakers(&program.pipelines[at].code);
		let streams = matches!(
			&program.pipelines[at].parallel,
			Parallel::Morsels(MorselPlan {
				combine: Combine::Concat,
				paged: None,
				..
			})
		);
		Self {
			program,
			at,
			policy: Policy::from_config(&ctx.root().ctx.config.exec),
			dir,
			version,
			fold: Fold::of(&program.pipelines[at], &breakers),
			breakers,
			replay: Replay::default(),
			probe,
			layouts,
			pending: Pending::default(),
			shared: Arc::new(if streams {
				Shared::new().streaming()
			} else {
				Shared::new()
			}),
			asking: Asking::default(),
			lease: FanoutLease::new(ctx.ctx().memory().cloned()),
			merge: super::merge_of(&program.pipelines[at]),
			ready: BTreeMap::new(),
			unplaced: 0,
			view: None,
			view_asked: false,
			remote: None,
			remote_asked: false,
			#[cfg(not(target_family = "wasm"))]
			workers: Vec::new(),
			#[cfg(target_family = "wasm")]
			hosts: Arc::default(),
		}
	}

	/// Run the pipeline over `range`: `true` when the pipeline takes no more rows.
	///
	/// Every worker the scan spawned has ended when this returns, whatever it returns.
	pub(super) async fn run(
		&mut self,
		range: RawRange,
		hint: Option<u32>,
		state: &mut State,
		ctx: &ExecutionContext,
		yielder: &mut Out<'_, '_>,
		flush_at: usize,
	) -> FlowResult<bool> {
		self.shared.metrics().initial += 1;
		#[cfg(not(target_family = "wasm"))]
		let outcome = self.drive(range, hint, state, ctx, yielder, flush_at).await;
		// The workers at the host's places run while the coordinator does, polled beside it.
		#[cfg(target_family = "wasm")]
		let outcome = {
			let hosts = Arc::clone(&self.hosts);
			let drive = std::pin::pin!(self.drive(range, hint, state, ctx, yielder, flush_at));
			let hosted = std::pin::pin!(hosts.drive());
			match futures::future::select(drive, hosted).await {
				futures::future::Either::Left((outcome, _)) => outcome,
				futures::future::Either::Right(((), drive)) => drive.await,
			}
		};
		self.shutdown().await;
		self.report(ctx);
		outcome
	}

	/// The scan's workers stop at their next chunk, and are awaited, which drops their cursors.
	async fn shutdown(&mut self) {
		self.shared.stop();
		#[cfg(not(target_family = "wasm"))]
		for worker in std::mem::take(&mut self.workers) {
			// A worker that panicked has recorded its morsel as failed; there is nothing more to
			// learn from it.
			let _ = worker.await;
		}
		#[cfg(target_family = "wasm")]
		self.hosts.finish().await;
	}

	/// Hand what the scan's morsels did to the readers of it: the tests, and the analysis of a
	/// statement run under `EXPLAIN ANALYZE` when the policy renders it.
	fn report(&self, ctx: &ExecutionContext) {
		#[cfg(test)]
		super::test_hooks::note_metrics(ctx, &self.shared.metrics());
		if self.policy.explain_analyze()
			&& let Some(sink) = ctx.vm_metrics().filter(|sink| sink.is_enabled())
		{
			sink.add_morsels(&self.shared.metrics());
		}
		#[cfg(not(test))]
		let _ = ctx;
	}

	async fn drive(
		&mut self,
		range: RawRange,
		mut hint: Option<u32>,
		state: &mut State,
		ctx: &ExecutionContext,
		yielder: &mut Out<'_, '_>,
		flush_at: usize,
	) -> FlowResult<bool> {
		if self.front(range, true, &mut hint, state, ctx, yielder, flush_at).await? {
			return Ok(true);
		}
		loop {
			self.top_up(state, ctx).await?;
			let waited = web_time::Instant::now();
			// Results held for the order of the scan are merged once nothing lies before them.
			if let Some(result) = self.next_ready() {
				if self.merged(result, waited, &mut hint, state, ctx, yielder, flush_at).await? {
					return Ok(true);
				}
				continue;
			}
			let pace = Pace {
				merge: self.merge,
				collect: self.ready.len() - self.unplaced < self.policy.read_ahead(),
				ready: self.ready.keys().next(),
			};
			let step = self.shared.next(pace).await;
			match step {
				Step::Empty => {
					self.pending
						.flush(self.program, &self.breakers, self.policy, state, ctx)
						.await?;
					return Ok(false);
				}
				// A morsel was queued while the front one runs: the next turn hands it on.
				Step::Wait => {}
				Step::Rows(mut rows, outbox) => {
					#[cfg(test)]
					super::test_hooks::note_streamed(ctx, rows.len());
					super::super::fuel::tick(ctx, super::super::fuel::units(rows.len())).await?;
					if state.output.is_empty() {
						std::mem::swap(&mut state.output, &mut rows);
					} else {
						state.output.append(&mut rows);
					}
					outbox.recycle(rows);
					hint = hint.filter(|_| state.output_len() < flush_at);
					flush(state, yielder, flush_at, ctx).await?;
				}
				Step::Collect(key, result) => {
					#[cfg(test)]
					super::test_hooks::note_collected_early(ctx);
					self.collect(key, *result, ctx).await?;
				}
				Step::Merge(result, early) => {
					#[cfg(test)]
					if early {
						super::test_hooks::note_merged_early(ctx);
					}
					#[cfg(not(test))]
					let _ = early;
					if self
						.merged(*result, waited, &mut hint, state, ctx, yielder, flush_at)
						.await?
					{
						return Ok(true);
					}
				}
				Step::Run(span) => {
					// What the statement folds itself follows the groups of the morsels before it.
					self.pending
						.flush(self.program, &self.breakers, self.policy, state, ctx)
						.await?;
					if self.front(span, false, &mut hint, state, ctx, yielder, flush_at).await? {
						return Ok(true);
					}
				}
			}
		}
	}

	/// The earliest results the merge holds, when no morsel lies before them in the scan.
	fn next_ready(&mut self) -> Option<FlowResult<MorselResult>> {
		let first = self.ready.keys().next()?;
		if self.shared.front_key().is_some_and(|front| front < *first) {
			return None;
		}
		let (_, first) = self.ready.pop_first()?;
		if !first.placed {
			self.unplaced -= 1;
		}
		self.shared.table().held = self.ready.len() - self.unplaced;
		Some(first.result)
	}

	/// Hold the results of the morsel at `key`, which is done before morsels that precede it in
	/// the scan. A merge that combines the partial states of morsels next to one another does so
	/// with those it holds, the earlier before the later, wherever no morsel lies between them.
	/// The rows of the results folded into one another are charged to the thread's fuel once they
	/// are held, as merging a morsel charges its rows, so the driver yields between one fold and
	/// the next.
	async fn collect(
		&mut self,
		key: OrderKey,
		result: FlowResult<MorselResult>,
		ctx: &ExecutionContext,
	) -> FlowResult<()> {
		use std::ops::Bound::{Excluded, Unbounded};
		let mut held = Ready {
			result,
			last: key.clone(),
			placed: true,
		};
		let mut first = key;
		let mut folded = 0usize;
		if self.merge == MergeOrder::Adjacent {
			let before = self.ready.range(..first.clone()).next_back().map(|(key, _)| key.clone());
			if let Some(before) = before
				&& let Some(left) = self.ready.get_mut(&before)
				&& !self.shared.between(&left.last, &first)
			{
				let rows = held.rows();
				match left.absorb(held, &self.breakers) {
					None => {
						folded += rows;
						#[cfg(test)]
						super::test_hooks::note_combined(ctx);
						first = before;
						held = self.ready.remove(&first).expect("the entry just combined into");
					}
					Some(rejected) => held = rejected,
				}
			}
			loop {
				let after = self
					.ready
					.range((Excluded(first.clone()), Unbounded))
					.next()
					.map(|(key, _)| key.clone());
				let Some(after) = after else {
					break;
				};
				if self.shared.between(&held.last, &after) {
					break;
				}
				let Some(right) = self.ready.remove(&after) else {
					break;
				};
				let rows = right.rows();
				match held.absorb(right, &self.breakers) {
					None => {
						folded += rows;
						#[cfg(test)]
						super::test_hooks::note_combined(ctx);
					}
					Some(rejected) => {
						self.ready.insert(after, rejected);
						break;
					}
				}
			}
		}
		held.placed = takes_a_place(self.merge, &mut held.result);
		if !held.placed {
			self.unplaced += 1;
		}
		self.ready.insert(first, held);
		self.shared.table().held = self.ready.len() - self.unplaced;
		#[cfg(test)]
		super::test_hooks::note_results_held(ctx, self.ready.len());
		super::super::fuel::tick(ctx, super::super::fuel::units(folded)).await
	}

	/// Take the results of a morsel the merge has reached: raise its failure, or absorb it into the
	/// statement's state. `true` when the paging's limit is spent and the pipeline takes no more
	/// rows.
	#[expect(
		clippy::too_many_arguments,
		reason = "a result with when it was waited for, the hint, state, context and output"
	)]
	async fn merged(
		&mut self,
		result: FlowResult<MorselResult>,
		waited: web_time::Instant,
		hint: &mut Option<u32>,
		state: &mut State,
		ctx: &ExecutionContext,
		yielder: &mut Out<'_, '_>,
		flush_at: usize,
	) -> FlowResult<bool> {
		self.shared.metrics().front_wait_ns +=
			u64::try_from(waited.elapsed().as_nanos()).unwrap_or(u64::MAX);
		let ordinal = self.shared.metrics().merged;
		let result = match result {
			Ok(result) => result,
			Err(failure) => {
				tracing::trace_span!("morsel", ordinal, outcome = "failed")
					.in_scope(|| tracing::trace!("morsel failed"));
				return Err(failure);
			}
		};
		tracing::trace_span!("morsel", ordinal, outcome = "merged", rows = result.rows)
			.in_scope(|| tracing::trace!("morsel merged"));
		{
			let keep = cfg!(test) || self.policy.explain_analyze();
			let mut metrics = self.shared.metrics();
			metrics.merged_morsel(
				MorselStat {
					rows: result.rows as u64,
					bytes: result.bytes,
					elapsed_ns: u64::try_from(result.elapsed.as_nanos()).unwrap_or(u64::MAX),
					chunks: result.chunks,
				},
				true,
				keep,
			);
			metrics.held_window(result.held);
		}
		// Merging a morsel walks its rows, sorted rows and groups once: no more than a morsel's
		// worth, spent whole, the driver yielding at the next one it takes.
		super::super::fuel::tick(ctx, super::super::fuel::units(result.rows)).await?;
		match self.breakers.paged {
			Some(limit) => self.replay.page(limit, result, state)?,
			None => match self.breakers.distinct {
				Some(distinct) => dedupe(distinct, result, state)?,
				None => {
					self.pending
						.take(self.program, &self.breakers, self.policy, result, state, ctx)
						.await?;
				}
			},
		}
		*hint = hint.filter(|_| state.output_len() < flush_at);
		flush(state, yielder, flush_at, ctx).await?;
		// Once the paging's limit is spent no morsel after it is read, merged or run.
		Ok(self.replay.stopped)
	}

	/// What the run has observed, for the policy's run-time questions.
	fn signals(&mut self, progress: &Progress, scan_hint: Option<u32>, state: &State) -> Signals {
		let (skip, remaining) = match self.breakers.paged {
			Some(paged) => {
				let paging = state.paging[paged as usize];
				(paging.skip, paging.remaining)
			}
			None => (0, None),
		};
		Signals {
			merged: self.replay.merged,
			reached: self.replay.reached,
			scan_hint,
			paged: self.breakers.paged.is_some(),
			skip,
			remaining,
			visited: progress.visited,
			visited_bytes: progress.bytes,
			passed: progress.passed,
			places_held: self.places_held(),
			since_cut: progress.since_cut,
			cuts: progress.cuts,
			..Signals::default()
		}
	}

	/// The places the scan's workers hold.
	#[cfg(not(target_family = "wasm"))]
	fn places_held(&mut self) -> usize {
		self.workers.retain(|worker| !worker.is_finished());
		self.workers.len()
	}

	#[cfg(target_family = "wasm")]
	fn places_held(&mut self) -> usize {
		0
	}

	/// The transaction's read view, which workers read their morsels through: asked for once, and
	/// `None` where the backend gives none.
	fn read_view(&mut self, ctx: &ExecutionContext) -> Option<Arc<dyn Snapshot>> {
		if !self.view_asked {
			self.view_asked = true;
			self.view = ctx.txn().snapshot();
		}
		self.view.clone()
	}

	/// Hand waiting morsels to workers, as many as there are places for and the lease grants a
	/// worker's copy of the slots for. A refused grant revokes the lease: no worker is spawned from
	/// then on, and the running ones park at their next chunk boundary, leaving the morsels that
	/// wait to the coordinator.
	#[cfg(not(target_family = "wasm"))]
	async fn top_up(&mut self, state: &mut State, ctx: &ExecutionContext) -> FlowResult<()> {
		self.top_up_host(state, ctx);
		if !self.policy.may_use_workers() {
			return Ok(());
		}
		if self.lease.revoked() {
			if let Some(statement) = &state.places {
				statement.stop_waiting();
			}
			return Ok(());
		}
		let (queued, room) = self.shared.open(self.policy.read_ahead());
		let mut want = queued.min(room);
		let held = self.places_held();
		self.shared.metrics().sampled_places(held);
		if want == 0 {
			if let Some(statement) = &state.places {
				statement.stop_waiting();
			}
			return Ok(());
		}
		let Some(view) = self.read_view(ctx) else {
			return Ok(());
		};
		let statement = places::of(state).clone();
		self.asking.note(state);
		// What a worker may take: the paging as the statement stands, which only falls.
		let paged = self.breakers.paged.map(|limit| {
			let paging = state.paging[limit as usize];
			Paged {
				limit,
				needed: paging.remaining.map(|remaining| paging.skip.saturating_add(remaining)),
			}
		});
		// Every worker takes a morsel when it finds one, so no more are wanted than wait or fit.
		while want > 0 {
			let Some(place) = statement.acquire(self.policy) else {
				break;
			};
			let mut under = Under::of(state, self.program, ctx)?;
			under.fold.clone_from(&self.fold);
			// A refused grant gives the place back: the coordinator runs what waits.
			let Some(grant) = self.lease.grant(under.kept.iter().sum(), ctx).await else {
				break;
			};
			under.covered = true;
			#[cfg(test)]
			super::test_hooks::note_handed(ctx);
			let worker = Worker {
				shared: Arc::clone(&self.shared),
				view: Arc::clone(&view),
				program: Arc::clone(self.program),
				at: self.at,
				policy: self.policy,
				dir: self.dir,
				version: self.version,
				under,
				lease: Arc::clone(&self.lease),
				paged,
				probe: self.probe.clone(),
				layouts: self.layouts.clone(),
				statement: statement.clone(),
				ctx: super::run::worker_ctx(ctx),
			};
			self.workers.push(tokio::spawn(crate::exec::compiles::while_running_task(
				async move {
					let _place = place;
					let _grant = grant;
					worker.run().await;
				},
			)));
			want -= 1;
		}
		Ok(())
	}

	#[cfg(target_family = "wasm")]
	async fn top_up(&mut self, state: &mut State, ctx: &ExecutionContext) -> FlowResult<()> {
		self.top_up_host(state, ctx);
		Ok(())
	}

	/// Hand waiting morsels to the host's places, as many as the host takes and fit among the
	/// morsels in flight. The morsels go to the host as they would to the node's workers, which
	/// take the rest; a host place takes no place of the node's pool and no grant of the lease,
	/// since it copies nothing of the statement's state.
	fn top_up_host(&mut self, state: &mut State, ctx: &ExecutionContext) {
		if !self.remote_asked {
			self.remote_asked = true;
			self.remote = super::remote::Remote::of(&self.program.pipelines[self.at], state, ctx);
			if let Some(remote) = &self.remote {
				self.policy = self.policy.with_host_width(remote.width());
			}
		}
		let Some(remote) = self.remote.clone() else {
			return;
		};
		let (queued, room) = self.shared.open(self.policy.read_ahead());
		let want = queued.min(room).min(remote.room());
		if want == 0 {
			return;
		}
		let Some(view) = self.read_view(ctx) else {
			return;
		};
		let paged = self.breakers.paged.map(|limit| {
			let paging = state.paging[limit as usize];
			Paged {
				limit,
				needed: paging.remaining.map(|remaining| paging.skip.saturating_add(remaining)),
			}
		});
		let Some(request) = super::remote::request(
			remote.maps(),
			self.dir,
			self.version,
			paged,
			self.policy.owner_chunk(self.policy.scan_chunk()),
			self.shared.streams(),
			self.layouts.as_ref(),
		) else {
			// The scan's layouts cannot be written for a leaf: its morsels stay on the node.
			self.remote = None;
			return;
		};
		for _ in 0..want {
			remote.started();
			let worker = super::remote::HostWorker {
				remote: remote.clone(),
				shared: Arc::clone(&self.shared),
				view: Arc::clone(&view),
				program: Arc::clone(self.program),
				policy: self.policy,
				request: request.clone(),
				aggregates: Arc::clone(&state.aggregates),
				lease: Arc::clone(&self.lease),
				// The statement's context holds no read buffer, so a host worker reads nothing
				// the coordinator buffered.
				ctx: ctx.clone(),
			};
			let run = crate::exec::compiles::while_running_task(worker.run());
			#[cfg(not(target_family = "wasm"))]
			self.workers.push(tokio::spawn(run));
			#[cfg(target_family = "wasm")]
			self.hosts.push(Box::pin(run));
		}
	}

	/// Run the part of the scan that lies in `span` on the statement's state, reading through the
	/// transaction's own cursor and emitting as the serial run does. When the policy asks, cut
	/// what is left of `span` into morsels and run only the first part; `true` when the pipeline
	/// takes no more rows. The run is a morsel of its own: counted when it ends.
	#[expect(
		clippy::too_many_arguments,
		reason = "a part with its range, whether it may be cut, hint, state, context and output"
	)]
	async fn front(
		&mut self,
		span: RawRange,
		may_cut: bool,
		hint: &mut Option<u32>,
		state: &mut State,
		ctx: &ExecutionContext,
		yielder: &mut Out<'_, '_>,
		flush_at: usize,
	) -> FlowResult<bool> {
		let started = web_time::Instant::now();
		let mut stat = MorselStat::default();
		let outcome =
			self.front_run(span, may_cut, hint, &mut stat, state, ctx, yielder, flush_at).await;
		stat.elapsed_ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
		self.shared.metrics().merged_morsel(
			stat,
			false,
			cfg!(test) || self.policy.explain_analyze(),
		);
		outcome
	}

	#[expect(
		clippy::too_many_arguments,
		reason = "a part with its range, whether it may be cut, hint, figures, state, context and output"
	)]
	async fn front_run(
		&mut self,
		span: RawRange,
		may_cut: bool,
		hint: &mut Option<u32>,
		stat: &mut MorselStat,
		state: &mut State,
		ctx: &ExecutionContext,
		yielder: &mut Out<'_, '_>,
		flush_at: usize,
	) -> FlowResult<bool> {
		let program = Arc::clone(self.program);
		let pipeline = &program.pipelines[self.at];
		let probe = self.probe.clone();
		let layouts = self.layouts.clone();
		let backward = self.dir == Direction::Backward;
		let txn = ctx.txn();
		let reader: Arc<dyn super::super::read_ahead::Reader> = Arc::clone(&txn) as _;
		let shared = Arc::clone(&self.shared);
		let running = Registered::new(&shared);
		// What this part has read, and where it ends: once the range is cut, the first entry at or
		// past the cut (before it, scanning backward) belongs to the next morsel.
		let mut progress = Progress::default();
		let mut own = span;
		// The policy's first split is asked for once: as the scan opens, from what the storage's
		// metadata says the range holds, and otherwise once the coordinator has read enough of it.
		let mut asked = !may_cut;
		if may_cut {
			let gate = Gate {
				shared: &shared,
				slot: running.slot(),
				policy: self.policy,
			};
			let signals = self.signals(&progress, *hint, state);
			if self.cut_at_open(&txn, &mut own, &gate, &signals, ctx).await? {
				asked = true;
				progress.cuts = 1;
				self.top_up(state, ctx).await?;
			}
		}
		let budget = pipeline.read_ahead.then(|| ctx.root().ctx.config.exec.scan_batch_bytes);
		let mut source = super::super::read_ahead::ChunkSource::open(
			&reader,
			own.clone(),
			self.dir,
			self.version,
			budget,
		)
		.await
		.context("Failed to open scan cursor")?;
		// The front run's chunk is no larger than the split threshold, so its producer starts after
		// one such chunk.
		source.starting_after(self.policy.chunk(&Signals::default(), self.policy.scan_chunk()));
		let mut cut: Option<Vec<u8>> = None;
		let mut first = true;
		let mut last: Vec<u8> = Vec::new();
		// The far end of the range, read once: the last key a forward scan reaches, the first a
		// backward one does; `None` inside when the range holds nothing past what was read.
		// Both it and `stored_size`, what the storage's metadata says `own` holds, are read only
		// while the policy's first split is unasked, so `own` has not been cut when they are.
		let mut far: Option<Option<Vec<u8>>> = None;
		let mut stored_size: Option<Option<surrealdb_kvs::Estimate>> = None;
		// The source ends its producer on every exit path, so the loop runs in a block whose
		// result is returned once the source has finished.
		let outcome: FlowResult<bool> = async {
			loop {
				let signals = self.signals(&progress, *hint, state);
				let most = match (first, *hint) {
					(true, Some(reads)) => reads.clamp(1, self.policy.scan_chunk()),
					_ => self.policy.scan_chunk(),
				};
				first = false;
				let chunk = super::super::fuel::batch(self.policy.chunk(&signals, most));
				let quantum = super::super::fuel::quantum(ctx);
				let budget = super::super::fuel::left();
				let mut unspent = budget;
				let mut body_spent = 0;
				let (mut failed, mut stopped, mut ended, mut spent) = (None, false, false, false);
				// The threshold is read once a chunk: a stale one only rejects less.
				let threshold = probe.as_ref().and_then(|probe| probe.snapshot());
				let mut rejected = 0usize;
				let filtered_before = state.rejected;
				let stats = source
					.for_each(chunk, &mut |key, value| {
						unspent = unspent.saturating_sub(1);
						let next = if unspent == 0 {
							spent = true;
							std::ops::ControlFlow::Break(())
						} else {
							std::ops::ControlFlow::Continue(())
						};
						if let Some(cut) = &cut
							&& (if backward {
								key < cut.as_slice()
							} else {
								key >= cut.as_slice()
							}) {
							ended = true;
							return Ok(std::ops::ControlFlow::Break(()));
						}
						last.clear();
						last.extend_from_slice(key);
						if progress.first.is_empty() {
							progress.first.extend_from_slice(key);
						}
						let admitted = admit_row(
							pipeline,
							probe.as_ref().zip(threshold.as_deref()),
							key,
							value,
							layouts.as_deref(),
						);
						if let (Some(probe), Some(threshold)) =
							(probe.as_ref(), threshold.as_deref())
							&& probe.rejects(threshold, &admitted)
						{
							rejected += 1;
							return Ok(next);
						}
						match super::super::fuel::measure(&mut body_spent, quantum, || {
							in_place_row_admitted(
								&program,
								pipeline,
								key,
								value,
								&admitted,
								layouts.as_deref(),
								state,
							)
						}) {
							Ok(false) => Ok(next),
							Ok(true) => {
								stopped = true;
								Ok(std::ops::ControlFlow::Break(()))
							}
							Err(e) => {
								failed = Some(e);
								Ok(std::ops::ControlFlow::Break(()))
							}
						}
					})
					.await
					.context("Failed to scan record")?;
				super::super::fuel::charge((budget - unspent).saturating_add(body_spent));
				if let Some(e) = failed {
					return Err(ControlFlow::Err(e));
				}
				#[cfg(test)]
				super::test_hooks::note_pruned(ctx, rejected);
				let read = stats.rows as usize;
				let bytes = stats.key_bytes + stats.value_bytes;
				progress.visited += stats.rows;
				progress.bytes += bytes;
				// The entries the probe or a filter dropped did not pass; a nested query's filter
				// counts too, which only makes the scan read as more selective.
				let dropped =
					state.rejected.saturating_sub(filtered_before).saturating_add(rejected as u64);
				progress.passed += stats.rows.saturating_sub(dropped);
				progress.since_cut += stats.rows;
				stat.rows += stats.rows;
				stat.bytes += bytes;
				stat.chunks += 1;
				#[cfg(test)]
				{
					super::super::host::RECORDS_SCANNED
						.with(|scanned| scanned.set(scanned.get() + read));
					super::test_hooks::note_scanned(ctx, read);
				}
				// The rows the chunk emitted are held until `flush` hands them on, so a process
				// past its memory threshold fails here rather than scanning on.
				crate::exec::operators::scan::common::ensure_below_memory_threshold()?;
				if state.output_len() >= flush_at {
					*hint = None;
				}
				flush(state, yielder, flush_at, ctx).await?;
				if stopped || ended || (!spent && read < chunk as usize) {
					return Ok(stopped);
				}
				if super::super::fuel::spent() {
					super::super::fuel::yield_point(ctx).await?;
				}
				// Morsels waiting while places are free are handed to workers without waiting for
				// the front morsel to end, so a place owed to this statement is taken.
				if shared.queued_len() > 0 {
					self.top_up(state, ctx).await?;
				}
				let signals = self.signals(&progress, *hint, state);
				let gate = Gate {
					shared: &shared,
					slot: running.slot(),
					policy: self.policy,
				};
				// Where the range is cut next, and why: the policy's first split, or once that is
				// done a morsel's donation.
				let rest = rest_of(&own, self.dir, &last);
				let (target, origin, requested) = if !asked {
					let mut signals = Signals {
						no_view: self.read_view(ctx).is_none(),
						..signals
					};
					if self.policy.may_split(&signals) && progress.visited >= Policy::MEASURED_FROM
					{
						if stored_size.is_none() {
							stored_size = Some(
								txn.approximate(own.clone(), self.version).await.unwrap_or_else(
									|failure| {
										tracing::debug!(%failure, "a scan could not size its range for a measured split");
										None
									},
								),
							);
						}
						if let Some(Some(size)) = stored_size {
							signals.remaining_estimate =
								Some(steal::remaining_from_size(size, progress.visited));
						} else {
							let far = match &far {
								Some(far) => far.clone(),
								None => {
									let found = far_key(&txn, &rest, self.dir, self.version)
										.await
										.context("Failed to read the far end of the scan")?;
									far = Some(found.clone());
									found
								}
							};
							signals.remaining_estimate = Some(match far {
								Some(far) => steal::remaining_to(
									&far,
									self.dir,
									&progress.first,
									&last,
									progress.visited,
									progress.bytes,
								),
								None => surrealdb_kvs::Estimate::default(),
							});
						}
					}
					match self.policy.split(&signals) {
						Split::Split {
							target,
						} => {
							asked = true;
							self.publish(&running, &own, &last, &progress);
							(target, Origin::Split, false)
						}
						Split::Whole => {
							self.publish(&running, &own, &last, &progress);
							continue;
						}
					}
				} else {
					let left = self.publish(&running, &own, &last, &progress);
					let signals = Signals {
						remaining_estimate: Some(left),
						queued: shared.queued_len(),
						..signals
					};
					match gate.target(&signals) {
						Some(cut) => cut,
						None => continue,
					}
				};
				let (lo, hi) = rest.bounds();
				if lo >= hi {
					gate.found_none(requested);
					continue;
				}
				// A split that fails finds no boundary: the range is read whole, which is the
				// answer a range with no boundary gets.
				let split =
					txn.split(rest.clone(), target, self.version).await.map_err(|e| e.to_string());
				#[cfg(test)]
				let split = if super::test_hooks::split_fails(ctx) {
					Err("a split the test fails".to_owned())
				} else {
					split
				};
				let bounds = split.unwrap_or_else(|failure| {
					tracing::debug!(%failure, "a scan could not be split; its range is read whole");
					Vec::new()
				});
				if bounds.is_empty() {
					gate.found_none(requested);
					continue;
				}
				let parts = steal::cut(&rest, self.dir, bounds);
				match self.dir {
					Direction::Forward => {
						source.cut(&parts.edge);
						cut = Some(parts.edge.clone());
						own = own.part(own.bounds().0, &parts.edge);
					}
					Direction::Backward => {
						source.cut(&parts.edge);
						cut = Some(parts.edge.clone());
						own = own.part(&parts.edge, own.bounds().1);
					}
				}
				#[cfg(test)]
				if origin == Origin::Split {
					super::test_hooks::note_split(ctx, parts.donated.len());
				}
				gate.gave(parts.donated, self.dir, origin, requested);
				progress.since_cut = 0;
				progress.cuts += 1;
				self.top_up(state, ctx).await?;
			}
		}
		.await;
		source.finish().await;
		outcome
	}

	/// As the front run opens `own`, before an entry of it is read and with what the run has
	/// observed in `signals`: ask the storage, once, what the range holds, and when the policy cuts
	/// on that answer, queue every part but the first for workers and narrow `own` to the first.
	/// `true` when the range was cut.
	///
	/// A scan whose source holds selectivity probes ([`super::super::program::ScanProbes`]) asks,
	/// once every gate of a split but the share of passing rows is open, what the range and each
	/// probe's index range hold, in one call ([`Self::probe_at_open`]); the share it learns is the
	/// one the policy judges. Any other scan asks the storage's metadata only where a share known
	/// before the scan opens is within the policy's pass threshold. A failed answer, or none,
	/// leaves the range whole, as does a split that finds no boundary; the coordinator's measured
	/// split still judges it once it has read the split threshold's entries.
	async fn cut_at_open(
		&mut self,
		txn: &crate::kvs::Transaction,
		own: &mut RawRange,
		gate: &Gate<'_>,
		signals: &Signals,
		ctx: &ExecutionContext,
	) -> FlowResult<bool> {
		let signals = Signals {
			no_view: self.read_view(ctx).is_none(),
			selectivity: None,
			..*signals
		};
		#[cfg(test)]
		let signals = Signals {
			selectivity: super::test_hooks::seeded_selectivity(ctx),
			..signals
		};
		let probes = if let super::super::program::Source::Scan {
			probes: Some(probes),
			..
		} = &self.program.pipelines[self.at].source
			&& signals.selectivity.is_none()
		{
			Some(Arc::clone(probes))
		} else {
			None
		};
		let (answer, signals) = match probes {
			Some(probes) => {
				if !self.policy.may_split_unread(&signals) {
					return Ok(false);
				}
				#[cfg(test)]
				super::test_hooks::note_asked_at_open(ctx);
				let (answer, selectivity) = self.probe_at_open(txn, own, &probes, ctx).await;
				(
					Ok(answer),
					Signals {
						selectivity,
						..signals
					},
				)
			}
			None => {
				if !self.policy.asks_at_open(&signals) {
					return Ok(false);
				}
				#[cfg(test)]
				super::test_hooks::note_asked_at_open(ctx);
				(txn.approximate(own.clone(), self.version).await, signals)
			}
		};
		#[cfg(test)]
		let answer = match super::test_hooks::seeded(ctx) {
			Some(seeded) => Ok(Some(seeded)),
			None => answer,
		};
		let opened = answer.unwrap_or_else(|failure| {
			tracing::debug!(%failure, "a scan could not size its range as it opened");
			None
		});
		let signals = Signals {
			opened: opened.map(crate::exec::ir::iface::Estimate::kv_range),
			..signals
		};
		let Split::Split {
			target,
		} = self.policy.split_at_open(&signals)
		else {
			return Ok(false);
		};
		let (lo, hi) = own.bounds();
		if lo >= hi {
			return Ok(false);
		}
		let split = txn.split(own.clone(), target, self.version).await.map_err(|e| e.to_string());
		#[cfg(test)]
		let split = if super::test_hooks::split_fails(ctx) {
			Err("a split the test fails".to_owned())
		} else {
			split
		};
		let bounds = split.unwrap_or_else(|failure| {
			tracing::debug!(%failure, "a scan could not be split as it opened; its range is read whole");
			Vec::new()
		});
		if bounds.is_empty() {
			return Ok(false);
		}
		let parts = steal::cut(own, self.dir, bounds);
		*own = match self.dir {
			Direction::Forward => own.part(own.bounds().0, &parts.edge),
			Direction::Backward => own.part(&parts.edge, own.bounds().1),
		};
		#[cfg(test)]
		{
			super::test_hooks::note_split(ctx, parts.donated.len());
			super::test_hooks::note_opened(ctx, parts.donated.len());
		}
		gate.gave(parts.donated, self.dir, Origin::Split, false);
		Ok(true)
	}

	/// What the range `own` holds, and the most rows in a million of it the scan's filters pass,
	/// from one call that asks the storage about `own` and each probe's index range.
	///
	/// The share is the probes' entry counts combined by [`ProbeBound`] over the keys `own`
	/// counted: a ratio of key counts, never of bytes, since an index entry and a record differ in
	/// size. Where every count is exact the share is an upper bound on the rows the filters pass;
	/// where the storage extrapolated a count from the keys it read, the share is an estimate of
	/// that bound, which may misjudge where the range is cut and never which rows it yields. The
	/// share is `None` where the storage counted no keys of `own`, or the counts bound nothing,
	/// and the range is then judged as one whose share is unknown. A probe whose range cannot be
	/// built, or that the storage counted no keys of, counts nothing. A failed call answers
	/// nothing.
	///
	/// [`ProbeBound`]: crate::exec::index::probe::ProbeBound
	async fn probe_at_open(
		&self,
		txn: &crate::kvs::Transaction,
		own: &RawRange,
		probes: &super::super::program::ScanProbes,
		ctx: &ExecutionContext,
	) -> (Option<surrealdb_kvs::Estimate>, Option<crate::exec::ir::iface::Estimate>) {
		let mut ranges = Vec::with_capacity(probes.probes.len() + 1);
		ranges.push(own.clone());
		let mut answered = Vec::with_capacity(probes.probes.len());
		for (index, access) in probes.probes.iter() {
			match super::super::entries::entry_range(ctx, index, access) {
				Ok(range) => {
					answered.push(Some(ranges.len()));
					ranges.push(range);
				}
				Err(_) => answered.push(None),
			}
		}
		let answers = match txn.estimate_many(ranges, self.version).await {
			Ok(answers) => answers,
			Err(failure) => {
				tracing::debug!(%failure, "a scan could not count its probes as it opened");
				return (None, None);
			}
		};
		let table = answers.first().copied();
		let keys: Vec<Option<u64>> = answered
			.iter()
			.map(|at| at.and_then(|at| answers.get(at)).and_then(|answer| answer.keys))
			.collect();
		let selectivity = table
			.and_then(|table| table.keys)
			.and_then(|table_keys| probes.bound.upper_ppm(&keys, table_keys))
			.map(|ppm| crate::exec::ir::iface::Estimate {
				rows: None,
				bytes: None,
				selectivity_ppm: Some(ppm),
				exact: false,
				source: crate::exec::ir::iface::EstimateSource::KvRange,
			});
		#[cfg(test)]
		if selectivity.is_some() {
			super::test_hooks::note_probed(ctx);
		}
		(table, selectivity)
	}

	/// Publish what the coordinator's morsel has left, for an idle worker choosing whom to ask, and
	/// answer it.
	fn publish(
		&self,
		running: &Registered<'_>,
		own: &RawRange,
		last: &[u8],
		progress: &Progress,
	) -> surrealdb_kvs::Estimate {
		let left = steal::remaining(
			own,
			self.dir,
			&progress.first,
			last,
			progress.visited,
			progress.bytes,
		);
		running.slot().publish(left);
		left
	}
}

/// The part of `own` that a scan in `dir` has not read, `last` being the last key it read.
fn rest_of(own: &RawRange, dir: Direction, last: &[u8]) -> RawRange {
	match dir {
		Direction::Forward => {
			let mut start = last.to_vec();
			start.push(0);
			own.part(&start, own.bounds().1)
		}
		Direction::Backward => own.part(own.bounds().0, last),
	}
}

/// The far end of `rest`, the unread part of a scan in `dir`: the last key it holds for a forward
/// scan, the first for a backward one. `None` when it holds none; a failed read is the scan's
/// failure.
async fn far_key(
	txn: &crate::kvs::Transaction,
	rest: &RawRange,
	dir: Direction,
	version: Option<u64>,
) -> anyhow::Result<Option<Vec<u8>>> {
	let (lo, hi) = rest.bounds();
	if lo >= hi {
		return Ok(None);
	}
	let found = match dir {
		Direction::Forward => txn.keysr_raw(rest.clone(), 1, 0, version).await,
		Direction::Backward => txn.keys_raw(rest.clone(), 1, 0, version).await,
	};
	Ok(found?.pop())
}

/// A worker: take the earliest waiting morsel, read its range through `view` and run it, until none
/// waits or the scan is done. A worker with no morsel to take asks the running morsel with the most
/// left to donate part of it; a worker whose statement is no longer the one that should hold its
/// place returns it.
#[cfg(not(target_family = "wasm"))]
struct Worker {
	shared: Arc<Shared>,
	view: Arc<dyn Snapshot>,
	program: Arc<Program>,
	at: usize,
	policy: Policy,
	dir: Direction,
	version: Option<u64>,
	/// What the morsels run under.
	under: Under,
	/// The run's lease, whose revocation parks the worker's morsel at its next chunk boundary.
	lease: Arc<FanoutLease>,
	paged: Option<Paged>,
	probe: Option<Arc<TopKThresholdProbe>>,
	layouts: Option<Arc<TableLayouts>>,
	statement: super::places::Statement,
	ctx: ExecutionContext,
}

#[cfg(not(target_family = "wasm"))]
impl Worker {
	async fn run(self) {
		let mut cursor: Option<Box<dyn surrealdb_kvs::RangeCursor + '_>> = None;
		let mut tried: Vec<Arc<Slot>> = Vec::new();
		loop {
			// A worker with no morsel to take, or no room for one, returns its place; the
			// coordinator hands morsels to new workers as the merge makes room.
			let (key, span, outbox) = match self.shared.claim(self.policy.read_ahead()) {
				Claim::Morsel(key, span, outbox) => (key, span, outbox),
				Claim::Full => break,
				Claim::Empty => {
					if self.shared.steal(self.policy, &self.statement, &mut tried).await {
						continue;
					}
					break;
				}
			};
			tried.clear();
			let guard = Running {
				shared: &self.shared,
				key: Some(key.clone()),
			};
			let cancelled = || self.shared.cancelled(&key);
			#[cfg(test)]
			super::test_hooks::delay_claimed(&self.ctx).await;
			let running = Registered::new(&self.shared);
			let end =
				self.read(&mut cursor, &span, &cancelled, running.slot(), outbox.as_deref()).await;
			drop(running);
			// A cursor that failed, or stopped before its range ended, is not reused.
			if !matches!(end, Ok(RangeEnd::Done(_))) {
				cursor = None;
			}
			let stop = matches!(end, Err(_) | Ok(RangeEnd::Parked(..)));
			guard.complete(end);
			// A place goes back to the pool at the end of every morsel; it is kept for the next
			// one unless another statement waits for it.
			if stop || self.statement.others_wait() {
				break;
			}
		}
	}

	/// One morsel: aim the cursor at `span`, opening it the first time, and run it.
	async fn read<'v>(
		&'v self,
		cursor: &mut Option<Box<dyn surrealdb_kvs::RangeCursor + 'v>>,
		span: &RawRange,
		cancelled: &(dyn Fn() -> bool + Sync),
		slot: &Slot,
		outbox: Option<&Outbox>,
	) -> FlowResult<RangeEnd> {
		let range = span.clone().into_key_range();
		match cursor {
			Some(open) => open.aim(range, self.dir).context("Failed to aim scan cursor")?,
			None => {
				*cursor = Some(
					self.view
						.open(range, self.dir, self.version)
						.await
						.context("Failed to open scan cursor")?,
				)
			}
		}
		let open = cursor.as_mut().expect("opened above");
		super::run::run_range(
			&mut **open,
			super::run::RangeRun {
				span,
				dir: self.dir,
				program: &self.program,
				at: self.at,
				paged: self.paged,
				probe: self.probe.as_deref(),
				layouts: self.layouts.as_deref(),
				chunk: self.policy.owner_chunk(self.policy.scan_chunk()),
				window: self.policy.window_bytes(),
				lease: &self.lease,
				outbox,
				chunks: None,
				donor: Some(steal::Donor {
					gate: Gate {
						shared: &self.shared,
						slot,
						policy: self.policy,
					},
					view: &*self.view,
					version: self.version,
					statement: &self.statement,
					#[cfg(test)]
					ctx: &self.ctx,
				}),
			},
			self.under.clone(),
			&self.ctx,
			cancelled,
		)
		.await
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::memory::runs::{BreakerKind, RunOrder, RunSpec};

	fn span(at: u8) -> RawRange {
		RawRange::of_bytes(&[at], &[at + 1])
	}

	fn finished() -> Box<FlowResult<MorselResult>> {
		Box::new(Ok(MorselResult {
			output: Vec::new(),
			sorts: Vec::new(),
			slabs: Vec::new(),
			arrivals: Vec::new(),
			groups: Vec::new(),
			spilled: Vec::new(),
			sort_accounts: Vec::new(),
			group_accounts: Vec::new(),
			ends: Vec::new(),
			rows: 0,
			elapsed: std::time::Duration::ZERO,
			bytes: 0,
			chunks: 0,
			held: 0,
			scanned: [0; 3],
			decoded: 0,
		}))
	}

	/// The morsels running, finished and waiting for the merge, and the results the coordinator
	/// holds for it, are bounded together: results moved out of the table to be held for the
	/// morsels before them still count, and a worker takes a morsel again once they are merged.
	#[test]
	fn running_and_held_morsels_are_bounded_together() {
		const MOST: usize = 3;
		let shared = Shared::new();
		for at in 0..5 {
			shared.queue(OrderKey::of(&span(at), Direction::Forward), span(at), Origin::Split);
		}
		for at in 0..MOST as u8 {
			let Claim::Morsel(key, ..) = shared.claim(MOST) else {
				panic!("morsel {at} is taken");
			};
			assert_eq!(key, OrderKey::of(&span(at), Direction::Forward));
		}
		assert!(matches!(shared.claim(MOST), Claim::Full));
		assert_eq!(shared.open(MOST), (2, 0));

		// The last morsel finishes ahead of the first: the coordinator holds its results.
		let last = OrderKey::of(&span(2), Direction::Forward);
		shared.table().started.insert(last.clone(), Started::Done(finished()));
		let Step::Collect(key, _) = shared.step_early(true, None) else {
			panic!("the results are held for the morsels before them");
		};
		assert_eq!(key, last);
		assert!(matches!(shared.claim(MOST), Claim::Full), "a held result counts as in flight");
		assert_eq!(shared.open(MOST), (2, 0));

		// The merge takes what it held: its place is free again.
		shared.table().held = 0;
		assert_eq!(shared.open(MOST), (2, 1));
		assert!(matches!(shared.claim(MOST), Claim::Morsel(..)));
		assert!(matches!(shared.claim(MOST), Claim::Full));
	}

	/// A result the coordinator takes ahead of the morsels before it counts as a morsel in flight
	/// until the coordinator has decided whether it holds a place.
	#[test]
	fn a_result_taken_early_counts_as_in_flight() {
		const MOST: usize = 3;
		let shared = Shared::new();
		for at in 0..5 {
			shared.queue(OrderKey::of(&span(at), Direction::Forward), span(at), Origin::Split);
		}
		for _ in 0..MOST {
			assert!(matches!(shared.claim(MOST), Claim::Morsel(..)));
		}
		let last = OrderKey::of(&span(2), Direction::Forward);
		shared.table().started.insert(last, Started::Done(finished()));
		assert!(matches!(shared.step_early(true, None), Step::Collect(..)));
		assert_eq!(shared.open(MOST).1, 0, "the held result counts as in flight");
	}

	/// A sorted run in memory frees the place of its morsel only where it holds entries, the morsel
	/// charged every non-empty sort to the query's lease, and the lease takes the bytes of holding
	/// the result too. An empty run, a run no lease covers, a spilled run and the results of every
	/// other merge hold their places.
	#[test]
	fn a_held_result_holds_a_place_or_charges_its_size() {
		let entries = || {
			vec![vec![(
				vec![crate::val::Value::from(1)],
				crate::exec::vm::interp::SortRow::Value(crate::val::Value::from(1)),
			)]]
		};
		let memory = crate::exec::memory::MemoryHost {
			governor: Arc::new(crate::exec::memory::Unlimited),
			store: None,
			settings: crate::exec::memory::MemorySettings {
				floor: 100,
				fan_in: 2,
				chunk: 64,
				wait: std::time::Duration::ZERO,
				max: usize::MAX,
			},
		}
		.query();
		let charged =
			|sorts: Vec<Vec<(Vec<crate::val::Value>, crate::exec::vm::interp::SortRow)>>| {
				let mut result = finished();
				if let Ok(result) = &mut *result {
					result.sorts = sorts;
					result.sort_accounts = vec![memory.reservation()];
				}
				result
			};

		let mut run = charged(entries());
		assert!(!takes_a_place(MergeOrder::Positioned, &mut run), "a charged run frees it");
		let used = (*run).as_ref().map_or(0, |run| run.sort_accounts[0].used());
		let size = (*run).as_ref().map_or(0, held_result_bytes);
		assert!(size > 0 && used >= size, "the run's size is charged: {used} of {size}");

		let mut empty = charged(vec![Vec::new()]);
		assert!(takes_a_place(MergeOrder::Positioned, &mut empty), "an empty run holds it");

		let mut uncharged = finished();
		if let Ok(result) = &mut *uncharged {
			result.sorts = entries();
		}
		assert!(takes_a_place(MergeOrder::Positioned, &mut uncharged), "no lease: it holds it");

		let mut spilled = charged(entries());
		if let Ok(result) = &mut *spilled {
			result.spilled.push(crate::exec::vm::partial::Partial {
				spec: RunSpec::new(BreakerKind::Sort, 0, 1, RunOrder::Plain),
				runs: Default::default(),
				held: Vec::new(),
				side: Vec::new(),
				counts: Default::default(),
			});
		}
		assert!(takes_a_place(MergeOrder::Positioned, &mut spilled), "a spilled run holds it");

		let mut other = charged(entries());
		assert!(takes_a_place(MergeOrder::Adjacent, &mut other), "other merges hold it");
	}
}
