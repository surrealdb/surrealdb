//! A pipeline's morsels in progress: queued, running and merged in the serial order.

use std::collections::VecDeque;
use std::sync::Arc;

use super::super::host::{Out, flush};
use super::super::interp::State;
use super::super::program::Program;
use super::grouped::Pending;
use super::lease::FanoutLease;
use super::merge::{BodyBreakers, Replay, body_breakers, dedupe};
use super::places::{self, Asking, Place};
use super::run::{MorselResult, PendingBatch, Under, run_batch};
use crate::catalog::TableLayouts;
use crate::exec::planner::placement::{INLINE_BUDGET, Policy, Signals};
use crate::exec::topk_pushdown::TopKThresholdProbe;
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::ControlFlow;
use crate::val::Value;

/// The morsels of a Batches-input pipeline in progress; see the module documentation.
pub(in crate::exec::vm) struct MorselSet<'p> {
	program: &'p Arc<Program>,
	at: usize,
	policy: Policy,
	/// The rows of the batch being filled.
	pub(super) filling: Vec<Value>,
	/// The place the batch being filled holds, once it is to go to a worker.
	place: Option<Place>,
	pub(super) merging: VecDeque<PendingBatch>,
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
}

impl<'p> MorselSet<'p> {
	pub(super) fn new(program: &'p Arc<Program>, at: usize, ctx: &ExecutionContext) -> Self {
		Self {
			program,
			at,
			policy: Policy::from_config(&ctx.root().ctx.config.exec),
			filling: Vec::new(),
			place: None,
			merging: VecDeque::new(),
			cost: None,
			breakers: body_breakers(&program.pipelines[at].code),
			replay: Replay::default(),
			pending: Pending::default(),
			reads: None,
			in_flight: 0,
			asking: Asking::default(),
			lease: FanoutLease::new(ctx.ctx().memory().cloned()),
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
		// A paged body's batches hold their rows for the merge to count and are never bound.
		let bound = self.breakers.paged.is_none();
		// The grant is asked for with the place in hand, so a refusal gives the place back and the
		// batch runs here, as when no place is free.
		#[cfg(not(target_family = "wasm"))]
		let grant = match place {
			Some(_) if self.policy.hands_to_worker(rows.len(), true) => {
				self.lease.grant(under.kept.iter().sum(), ctx).await
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
			Some((place, grant)) => PendingBatch::Running(super::run::spawn(
				self.program,
				self.at,
				rows,
				paged,
				bound,
				under,
				ctx,
				place,
				grant,
			)),
			_ => PendingBatch::Done(Box::new(
				run_batch(self.program, self.at, rows, paged, bound, under, ctx).await,
			)),
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

	/// Take the results of the batches at the front that are done, in order, waiting for the first
	/// when `waiting`. Once a paged body's limit is spent, every batch still out is dropped,
	/// aborting its worker.
	pub(super) async fn merge(
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
		self.merging.push_back(PendingBatch::Done(Box::new(Err(failure))));
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
#[cfg(not(target_family = "wasm"))]
use super::run::Paged;
use super::run::RangeEnd;
use super::steal::{self, Gate, Origin, Progress, Registered, Slot};
use crate::err::EngineError;
use crate::exec::planner::placement::Split;
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
	/// Running on a worker.
	Running,
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
	/// The morsels a worker has taken and the merge has not, by order key: as many as the policy
	/// lets be in flight.
	started: BTreeMap<OrderKey, Started>,
	/// The earliest morsel that failed: no morsel after it is needed.
	failed: Option<OrderKey>,
	/// What the morsels running now publish, for an idle worker to choose whom to ask.
	pub(super) slots: Vec<Arc<Slot>>,
}

/// What a worker finds in the table.
enum Claim {
	/// The earliest waiting morsel, now the worker's.
	Morsel(OrderKey, RawRange),
	/// Morsels wait, but as many are in flight as the policy allows.
	Full,
	/// No morsel waits, or the scan is done.
	Empty,
}

/// What the coordinator takes from the table next.
enum Step {
	/// The morsel at the front is done. Boxed: a result is large, and the other steps are not.
	Merge(Box<FlowResult<MorselResult>>),
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
			metrics: Mutex::new(MorselMetrics::default()),
		}
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
		(table.queued.len(), most.saturating_sub(table.started.len()))
	}

	/// Whether the morsel at `key` is no longer needed: the statement is done, or an earlier
	/// morsel has failed.
	fn cancelled(&self, key: &OrderKey) -> bool {
		self.stopped() || self.table().failed.as_ref().is_some_and(|failed| key > failed)
	}

	/// Take the earliest waiting morsel for a worker, unless `most` morsels are already running
	/// or finished and waiting for the merge. Morsels a failure has made unneeded are dropped.
	fn claim(&self, most: usize) -> Claim {
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
		if table.started.len() >= most {
			return Claim::Full;
		}
		let Some((key, queued)) = table.queued.pop_first() else {
			return Claim::Empty;
		};
		if queued.origin == Origin::Parked {
			self.metrics().resumed += 1;
		}
		table.started.insert(key.clone(), Started::Running);
		Claim::Morsel(key, queued.span)
	}

	/// A worker's morsel at `key` ended with `end`.
	fn complete(&self, key: OrderKey, end: FlowResult<RangeEnd>) {
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
		if matches!(table.started.first_key_value(), Some((_, Started::Running))) {
			return Step::Wait;
		}
		match table.started.pop_first() {
			Some((_, Started::Done(result))) => Step::Merge(result),
			_ => Step::Empty,
		}
	}

	/// [`Self::step`], waiting while the front morsel runs on a worker. A morsel queued meanwhile,
	/// which a donation does, ends the wait with [`Step::Wait`] so the coordinator can hand it to a
	/// worker.
	async fn next(&self) -> Step {
		loop {
			let finished = self.wake.notified();
			let queued = self.arrived.notified();
			tokio::pin!(finished, queued);
			finished.as_mut().enable();
			queued.as_mut().enable();
			match self.step() {
				Step::Wait => {
					if let futures::future::Either::Right(_) =
						futures::future::select(finished, queued).await
					{
						return Step::Wait;
					}
				}
				step => return step,
			}
		}
	}
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
	/// The transaction's read view, asked for once the range is first split.
	view: Option<Arc<dyn Snapshot>>,
	view_asked: bool,
	#[cfg(not(target_family = "wasm"))]
	workers: Vec<tokio::task::JoinHandle<()>>,
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
		Self {
			program,
			at,
			policy: Policy::from_config(&ctx.root().ctx.config.exec),
			dir,
			version,
			breakers: body_breakers(&program.pipelines[at].code),
			replay: Replay::default(),
			probe,
			layouts,
			pending: Pending::default(),
			shared: Arc::new(Shared::new()),
			asking: Asking::default(),
			lease: FanoutLease::new(ctx.ctx().memory().cloned()),
			view: None,
			view_asked: false,
			#[cfg(not(target_family = "wasm"))]
			workers: Vec::new(),
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
		let outcome = self.drive(range, hint, state, ctx, yielder, flush_at).await;
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
			let step = self.shared.next().await;
			match step {
				Step::Empty => {
					self.pending
						.flush(self.program, &self.breakers, self.policy, state, ctx)
						.await?;
					return Ok(false);
				}
				// A morsel was queued while the front one runs: the next turn hands it on.
				Step::Wait => {}
				Step::Merge(result) => {
					self.shared.metrics().front_wait_ns +=
						u64::try_from(waited.elapsed().as_nanos()).unwrap_or(u64::MAX);
					let ordinal = self.shared.metrics().merged;
					let result = match *result {
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
								elapsed_ns: u64::try_from(result.elapsed.as_nanos())
									.unwrap_or(u64::MAX),
								chunks: result.chunks,
							},
							true,
							keep,
						);
						metrics.held_window(result.held);
					}
					// Merging a morsel walks its rows, sorted rows and groups once: no more than a
					// morsel's worth, spent whole, the driver yielding at the next one it takes.
					super::super::fuel::tick(ctx, super::super::fuel::units(result.rows)).await?;
					match self.breakers.paged {
						Some(limit) => self.replay.page(limit, result, state)?,
						None => match self.breakers.distinct {
							Some(distinct) => dedupe(distinct, result, state)?,
							None => {
								self.pending
									.take(
										self.program,
										&self.breakers,
										self.policy,
										result,
										state,
										ctx,
									)
									.await?
							}
						},
					}
					hint = hint.filter(|_| state.output_len() < flush_at);
					flush(state, yielder, flush_at, ctx).await?;
					// Once the paging's limit is spent no morsel after it is read, merged or run.
					if self.replay.stopped {
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

	/// Hand waiting morsels to workers, as many as there are places for and the lease grants a
	/// worker's copy of the slots for. A refused grant revokes the lease: no worker is spawned from
	/// then on, and the running ones park at their next chunk boundary, leaving the morsels that
	/// wait to the coordinator.
	#[cfg(not(target_family = "wasm"))]
	async fn top_up(&mut self, state: &mut State, ctx: &ExecutionContext) -> FlowResult<()> {
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
		if !self.view_asked {
			self.view_asked = true;
			self.view = ctx.txn().snapshot();
		}
		let Some(view) = self.view.clone() else {
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
	async fn top_up(&mut self, _state: &mut State, _ctx: &ExecutionContext) -> FlowResult<()> {
		Ok(())
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
		let mut cursor = txn
			.open_vals_cursor_raw(span.clone(), self.dir, 0, self.version)
			.await
			.context("Failed to open scan cursor")?;
		// What this part has read, and where it ends: once the range is cut, the first entry at or
		// past the cut (before it, scanning backward) belongs to the next morsel.
		let mut progress = Progress::default();
		let mut cut: Option<Vec<u8>> = None;
		let mut asked = !may_cut;
		let mut first = true;
		let mut last: Vec<u8> = Vec::new();
		let mut own = span.clone();
		let shared = Arc::clone(&self.shared);
		let running = Registered::new(&shared);
		loop {
			let signals = self.signals(&progress, *hint, state);
			let most = match (first, *hint) {
				(true, Some(reads)) => reads.clamp(1, self.policy.scan_chunk()),
				_ => self.policy.scan_chunk(),
			};
			first = false;
			let chunk = super::super::fuel::batch(self.policy.chunk(&signals, most));
			let budget = super::super::fuel::left();
			let mut unspent = budget;
			let (mut failed, mut stopped, mut ended, mut spent) = (None, false, false, false);
			// The threshold is read once a chunk: a stale one only rejects less.
			let threshold = probe.as_ref().and_then(|probe| probe.snapshot());
			let mut rejected = 0usize;
			let stats = cursor
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
					if let (Some(probe), Some(threshold)) = (probe.as_ref(), threshold.as_deref())
						&& probe.rejects(threshold, &admitted)
					{
						rejected += 1;
						return Ok(next);
					}
					match in_place_row_admitted(
						&program,
						pipeline,
						key,
						value,
						&admitted,
						layouts.as_deref(),
						state,
					) {
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
			super::super::fuel::charge(budget - unspent);
			if let Some(e) = failed {
				return Err(ControlFlow::Err(e));
			}
			#[cfg(test)]
			super::test_hooks::note_pruned(ctx, rejected);
			#[cfg(not(test))]
			let _ = rejected;
			let read = stats.rows as usize;
			let bytes = stats.key_bytes + stats.value_bytes;
			progress.visited += stats.rows;
			progress.bytes += bytes;
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
			// The rows the chunk emitted are held until `flush` hands them on, so a process past
			// its memory threshold fails here rather than scanning on.
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
			// Morsels waiting while places are free are handed to workers without waiting for the
			// front morsel to end, so a place owed to this statement is taken.
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
			let (target, origin, requested) = if !asked {
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
			let rest = match self.dir {
				Direction::Forward => {
					let mut start = last.clone();
					start.push(0);
					own.part(&start, own.bounds().1)
				}
				Direction::Backward => own.part(own.bounds().0, &last),
			};
			let (lo, hi) = rest.bounds();
			if lo >= hi {
				gate.found_none(requested);
				continue;
			}
			// A split that fails finds no boundary: the range is read whole, which is the answer a
			// range with no boundary gets.
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
					cut = Some(parts.edge.clone());
					own = own.part(own.bounds().0, &parts.edge);
				}
				Direction::Backward => {
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
			let (key, span) = match self.shared.claim(self.policy.read_ahead()) {
				Claim::Morsel(key, span) => (key, span),
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
			let running = Registered::new(&self.shared);
			let end = self.read(&mut cursor, &span, &cancelled, running.slot()).await;
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
				donor: steal::Donor {
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
				},
			},
			self.under.clone(),
			&self.ctx,
			cancelled,
		)
		.await
	}
}
