//! The records of a write that fans out: their compute halves run as morsels, and the statement's
//! task applies them in the order of the serial run.
//!
//! The coordinator, the statement's task, reads the source and gathers the decoded rows into
//! batches. A batch that holds a place of the node's worker pool is handed to a worker, which runs
//! the body of the write over each row as far as its cut: the permission checks, the `WHERE`, the
//! data clauses and the schema, everything the plan marks as compute. The worker writes nothing.
//! It hands back one [`Candidate`] per row: the record in flight, its write context and where it
//! stopped. The coordinator takes the batches in the order it filled them and runs the body again
//! over each candidate, which resumes at the cut: the puts, the index and reference maintenance,
//! the events, the live notifications, the change feed and the output happen there, one record
//! after another, exactly as in the serial run.
//!
//! The statement passes over a record it created once it had begun reading the record's table,
//! as the gate of the serial run does. A worker's write state holds none of the statement's watches
//! on the tables it reads, so the coordinator, which does, judges it: of each row as it hands the
//! row on, so a worker runs nothing of a record passed over and hands back a candidate that skips
//! it, and again of each candidate as it applies it, for a record an earlier record's apply created
//! since. A record fetched by the id the statement names is never passed over, and never reaches a
//! write set: its source runs it whole through the body.
//!
//! A write over the ids of a range reads no source: its rows are the ids the coordinator generates,
//! in order, and the record each begins is the one that id names, with no stored record read for
//! it, so no marker of the record type travels with a row.
//!
//! A batch that holds no place, or whose lease grant is refused, runs the whole body on the
//! coordinator once every batch before it is applied. Nothing is computed ahead of an apply that
//! precedes it. A failure of a record's compute half is raised when its candidate is taken, after
//! every record before it is applied; the failure of the source is raised after every record it
//! yielded. Workers write nothing, so abandoning them on a failure leaves no effect behind.
//!
//! The write set holds the batches in flight and the candidates they return. Both are bounded: the
//! rows of a batch by the policy, the batches by the node's places and the read-ahead, and the
//! bytes by the statement's fan-out lease. The bytes of each candidate are charged to the lease as
//! its worker makes it and are released when the candidate is applied. A batch ends after the
//! candidate at which it holds the policy's window or the lease refuses its bytes, and the rows it
//! did not reach run whole on the statement's task once its candidates are applied. A batch is
//! handed on only while fewer batches than the read-ahead are out and the candidates not yet
//! applied hold less than a window, so the candidates held are bounded by the batches out, each
//! holding at most a window and the candidate that passes it. A worker's state and write state
//! are made when no earlier batch has left one, and are lent to the batches after it.

#[cfg(not(target_family = "wasm"))]
use std::collections::VecDeque;
use std::sync::Arc;
#[cfg(not(target_family = "wasm"))]
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(not(target_family = "wasm"))]
use futures::StreamExt;

use super::super::host::{Out, flush, run_value_row};
#[cfg(not(target_family = "wasm"))]
use super::super::interp::PooledState;
use super::super::interp::State;
#[cfg(not(target_family = "wasm"))]
use super::super::ir_exec::PrefixStop;
use super::super::ir_instr::IrForm;
use super::super::program::{Instr, Pipeline, Program, Source};
#[cfg(not(target_family = "wasm"))]
use super::super::write::WriteState;
#[cfg(not(target_family = "wasm"))]
use super::super::write::encode_ahead;
#[cfg(not(target_family = "wasm"))]
use super::super::write::prefix_state;
use super::lease::FanoutLease;
#[cfg(not(target_family = "wasm"))]
use super::lease::Grant;
use super::places::{self, Asking, Place};
#[cfg(not(target_family = "wasm"))]
use super::run::{Under, batch_state};
#[cfg(not(target_family = "wasm"))]
use super::write_compute::{Candidate, Pos, Stop};
#[cfg(not(target_family = "wasm"))]
use crate::exec::memory::size::{value_bytes, values_bytes};
#[cfg(not(target_family = "wasm"))]
use crate::exec::operators::write::row::WriteRow;
use crate::exec::planner::placement::{INLINE_BUDGET, Policy, Signals};
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::ControlFlow;
use crate::val::Value;

/// Whether `pipeline`'s body opens with the `WriteResume` of a write cut, which a write set runs.
pub(super) fn holds_cut(pipeline: &Pipeline) -> bool {
	matches!(
		pipeline.code.first(),
		Some(Instr::Ir(ir)) if matches!(ir.form(), IrForm::WriteResume(_))
	)
}

/// Whether `pipeline`'s rows are the ids of the targets its write names, each of which begins a
/// record the write creates.
fn names_targets(pipeline: &Pipeline) -> bool {
	matches!(pipeline.source, Source::WriteTargets { .. })
}

/// What `row` is for the body of `pipeline`: the row itself, or, where the rows are the ids of
/// targets, the record that id names put in flight, which no stored record is read for.
fn enter(pipeline: &Pipeline, row: Value, state: &mut State) -> FlowResult<Value> {
	if !names_targets(pipeline) {
		return Ok(row);
	}
	let write = state
		.write
		.as_deref_mut()
		.ok_or_else(|| ControlFlow::Err(anyhow::anyhow!("a write with no write state")))?;
	write.begin_target(row)?;
	Ok(Value::None)
}

/// The rows of a batch with the record-type marker stored with each, which the statement's task
/// read for them, and whether the statement passes over each.
#[cfg(not(target_family = "wasm"))]
struct Rows {
	rows: Vec<Value>,
	markers: Vec<Option<crate::catalog::Metadata>>,
	/// Whether each row is a record the statement created once it had begun reading its table,
	/// which the gate of the serial run passes over ([`passed_over`]).
	passed: Vec<bool>,
}

/// The record-type marker stored with each of `rows`, read through the statement's transaction.
#[cfg(not(target_family = "wasm"))]
async fn stored_markers(
	rows: &[Value],
	ctx: &ExecutionContext,
) -> FlowResult<Vec<Option<crate::catalog::Metadata>>> {
	let mut markers = Vec::with_capacity(rows.len());
	for row in rows {
		let marker = if let Value::RecordId(id) = row.rid() {
			crate::exec::operators::write::row::stored_metadata(ctx, &id).await?
		} else {
			None
		};
		markers.push(marker);
	}
	Ok(markers)
}

/// Whether the statement passes over each of `rows`: a record read from a table that the
/// statement created once it had begun reading that table, as the gate of the serial run judges
/// it. The coordinator judges it, since it holds the statement's watches and the compute half
/// holds none. A row is read only once it is stored and every store is the coordinator's, so the
/// judgement is final for every creation made before the row was read.
#[cfg(not(target_family = "wasm"))]
fn passed_over(rows: &[Value], state: &State) -> FlowResult<Vec<bool>> {
	let Some(write) = state.write.as_deref() else {
		return Err(ControlFlow::Err(anyhow::anyhow!("a write with no write state")));
	};
	rows.iter()
		.map(|row| {
			if let Value::RecordId(id) = row.rid() {
				write.read_after_created(Some(&id))
			} else {
				Ok(false)
			}
		})
		.collect()
}

/// What a worker keeps from one batch to the next: its state, its write state, and the lease's
/// cover for its copy of the slots, which lasts as long as the copy does.
#[cfg(not(target_family = "wasm"))]
struct Lent {
	state: PooledState,
	write: Box<WriteState>,
	copy: Grant,
}

/// What a batch's compute half starts from: a state made from the statement's, or one an earlier
/// batch left.
#[cfg(not(target_family = "wasm"))]
enum Start {
	Fresh {
		under: Box<Under>,
		write: Box<WriteState>,
		copy: Grant,
	},
	Reused(Lent),
}

/// What bounds the candidates a batch holds: the lease they are charged to, the bytes a batch
/// holds before it stops, and the bytes every batch holds that the statement's task has yet to
/// apply.
#[cfg(not(target_family = "wasm"))]
#[derive(Clone)]
struct Bounds {
	lease: Arc<FanoutLease>,
	window: usize,
	in_flight: Arc<AtomicUsize>,
}

/// What a batch's compute half produced: a candidate for each row it ran, up to and including
/// the first row that failed or the row after which the batch held its bound, and the rows it did
/// not reach.
#[cfg(not(target_family = "wasm"))]
struct Computed {
	candidates: Vec<Candidate>,
	/// The rows after the last candidate, which the statement's task runs whole once it has
	/// applied the candidates.
	rest: Vec<Value>,
	elapsed: std::time::Duration,
	/// The bytes of the candidates, counted in [`Bounds::in_flight`] until they are applied.
	charged: usize,
	/// The lease's cover for those bytes, released with the batch.
	held: Option<Grant>,
	/// The worker's state, for the next batch.
	lent: Option<Lent>,
}

/// The bytes a candidate holds: the record in flight with its images and the bytes encoded ahead,
/// and the registers carried to the apply half.
#[cfg(not(target_family = "wasm"))]
fn candidate_bytes(candidate: &Candidate) -> usize {
	let row = candidate.row.as_deref().map_or(0, WriteRow::held_bytes);
	candidate
		.live
		.iter()
		.map(|(_, value)| value_bytes(value))
		.fold(row.saturating_add(std::mem::size_of::<Candidate>()), usize::saturating_add)
}

/// A batch on a worker, aborted if it is dropped before it is taken.
#[cfg(not(target_family = "wasm"))]
struct Worker(Option<tokio::task::JoinHandle<FlowResult<Computed>>>);

#[cfg(not(target_family = "wasm"))]
impl Drop for Worker {
	fn drop(&mut self) {
		if let Some(handle) = &self.0 {
			handle.abort();
		}
	}
}

#[cfg(not(target_family = "wasm"))]
impl Worker {
	/// Whether the batch's candidates can be taken without waiting.
	fn is_done(&self) -> bool {
		self.0.as_ref().is_none_or(|handle| handle.is_finished())
	}

	/// Whether the batch is running on a worker, which holds a place until it is done.
	fn holds_place(&self) -> bool {
		self.0.as_ref().is_some_and(|handle| !handle.is_finished())
	}

	/// The batch's candidates, waiting for its worker.
	async fn result(mut self) -> FlowResult<Computed> {
		let Some(handle) = self.0.take() else {
			return Err(ControlFlow::Err(anyhow::anyhow!("a write batch was taken twice")));
		};
		match handle.await {
			Ok(result) => result,
			Err(e) => Err(ControlFlow::Err(anyhow::anyhow!("a write worker failed: {e}"))),
		}
	}

	/// Stops the batch and waits for the worker to let go of what it holds.
	async fn abandon(mut self) {
		if let Some(handle) = self.0.take() {
			handle.abort();
			let _ = handle.await;
		}
	}
}

/// The compute halves of a write pipeline's records in progress; see the module documentation.
pub(in crate::exec::vm) struct WriteSet<'p> {
	program: &'p Arc<Program>,
	at: usize,
	policy: Policy,
	/// The rows of the batch being filled.
	filling: Vec<Value>,
	/// The place the batch being filled holds, once it is to go to a worker.
	place: Option<Place>,
	/// The position the next row takes in delivery order.
	next: u64,
	/// The position of the next candidate the coordinator applies.
	applied: u64,
	/// The batches on workers, in the order they were filled.
	#[cfg(not(target_family = "wasm"))]
	merging: VecDeque<Worker>,
	/// The cost of a row's compute half measured so far, in nanoseconds.
	cost: Option<f64>,
	/// The run's registration, once this set has asked it for a place.
	asking: Asking,
	/// The bytes the run's workers hold only because it fanned out, granted before each hand-out.
	lease: Arc<FanoutLease>,
	/// The bytes of the candidates the batches hold that are not yet applied.
	#[cfg(not(target_family = "wasm"))]
	in_flight: Arc<AtomicUsize>,
	/// The states of workers whose batches are applied, handed to the next batches.
	#[cfg(not(target_family = "wasm"))]
	idle: Vec<Lent>,
}

impl<'p> WriteSet<'p> {
	pub(super) fn new(program: &'p Arc<Program>, at: usize, ctx: &ExecutionContext) -> Self {
		Self {
			program,
			at,
			policy: Policy::from_config(&ctx.root().ctx.config.exec),
			filling: Vec::new(),
			place: None,
			next: 0,
			applied: 0,
			#[cfg(not(target_family = "wasm"))]
			merging: VecDeque::new(),
			cost: None,
			asking: Asking::default(),
			lease: FanoutLease::new(ctx.ctx().memory().cloned()),
			#[cfg(not(target_family = "wasm"))]
			in_flight: Arc::new(AtomicUsize::new(0)),
			#[cfg(not(target_family = "wasm"))]
			idle: Vec::new(),
		}
	}

	/// What the run has observed, for the policy's run-time questions.
	fn signals(&self) -> Signals {
		Signals {
			cost_ns_per_row: self.cost,
			places_held: self.places_held(),
			..Signals::default()
		}
	}

	/// The places this write's batches hold: the one being filled for, and each batch still
	/// running on a worker.
	fn places_held(&self) -> usize {
		#[cfg(not(target_family = "wasm"))]
		{
			usize::from(self.place.is_some())
				+ self.merging.iter().filter(|batch| batch.holds_place()).count()
		}
		#[cfg(target_family = "wasm")]
		{
			0
		}
	}

	/// Take one row the source yielded; `true` when the pipeline takes no more, which a write
	/// over rows never does.
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
		self.filling.push(row);
		let outcome = self.fill(state, ctx, yielder, flush_at).await;
		self.settle(outcome, false).await
	}

	/// Dispatches the batch being filled where the policy says it is full.
	async fn fill(
		&mut self,
		state: &mut State,
		ctx: &ExecutionContext,
		yielder: &mut Out<'_, '_>,
		flush_at: usize,
	) -> FlowResult<()> {
		if self.place.is_none()
			&& self.filling.len() >= self.policy.batch_rows(&self.signals(), INLINE_BUDGET)
		{
			self.place = self.free_place(state);
			if self.place.is_none() {
				return self.dispatch(state, ctx, yielder, flush_at).await;
			}
		}
		if self.place.is_some()
			&& self.filling.len() >= self.policy.worker_batch_rows(&self.signals())
		{
			self.dispatch(state, ctx, yielder, flush_at).await?;
		}
		Ok(())
	}

	/// A place for the batch being filled, when the batch may go to a worker at all: the policy
	/// allows workers, the run's lease has not been revoked, and a batch has run on the statement's
	/// task. That first batch resolves the table's write context and record format, which the
	/// workers' compute halves then read from the scope the statement shares with them.
	fn free_place(&mut self, state: &mut State) -> Option<Place> {
		if !self.policy.may_use_workers() || self.lease.revoked() || self.next == 0 {
			return None;
		}
		let place = places::of(state).acquire(self.policy);
		self.asking.note(state);
		place
	}

	/// Runs the batch being filled: its compute half on a worker when it holds a place and enough
	/// rows and the lease grants the worker's copy of the slots; otherwise the whole body on the
	/// statement's task, once every batch before it is applied.
	async fn dispatch(
		&mut self,
		state: &mut State,
		ctx: &ExecutionContext,
		yielder: &mut Out<'_, '_>,
		flush_at: usize,
	) -> FlowResult<()> {
		let rows = std::mem::take(&mut self.filling);
		let place = self.place.take();
		#[cfg(not(target_family = "wasm"))]
		let count = rows.len();
		#[cfg(not(target_family = "wasm"))]
		if let Some(place) = place
			&& self.policy.hands_to_worker(count, true)
		{
			// A batch goes out only while fewer than the read-ahead are out and the candidates they
			// hold, not yet applied, are less than a window: the front batch is applied first. A
			// batch applied here also leaves its worker's state for this one.
			while !self.merging.is_empty()
				&& (self.merging.len() >= self.policy.read_ahead()
					|| self.in_flight.load(Ordering::Acquire) >= self.policy.window_bytes())
			{
				self.merge(true, state, yielder, flush_at, ctx).await?;
			}
			// The rows the batch holds are read ahead of the apply, and held only because the
			// statement fanned out.
			let read = if self.lease.bounded() {
				values_bytes(&rows)
			} else {
				0
			};
			let Some(read) = self.lease.grant_rows(read) else {
				// The grant was refused: the place goes back to the pool before the rows run here.
				drop(place);
				return self.run_whole(rows, state, ctx, yielder, flush_at).await;
			};
			let start = match self.idle.pop() {
				Some(lent) => Some(Start::Reused(lent)),
				None => self.fresh(state, ctx).await?,
			};
			let Some(start) = start else {
				drop(place);
				return self.run_whole(rows, state, ctx, yielder, flush_at).await;
			};
			// The compute half reads no stored data: the record-type marker of each row is read
			// here, on the statement's task, which is where the serial run reads it. A read that
			// fails is raised by the whole body at its row.
			let markers = if names_targets(&self.program.pipelines[self.at]) {
				Ok(rows.iter().map(|_| None).collect())
			} else {
				stored_markers(&rows, ctx).await
			};
			let Ok(markers) = markers else {
				if let Start::Reused(lent) = start {
					self.idle.push(lent);
				}
				drop(place);
				return self.run_whole(rows, state, ctx, yielder, flush_at).await;
			};
			let passed = passed_over(&rows, state)?;
			let first = self.next;
			self.next += count as u64;
			let bounds = Bounds {
				lease: Arc::clone(&self.lease),
				window: self.policy.window_bytes(),
				in_flight: Arc::clone(&self.in_flight),
			};
			let batch = spawn(
				self.program,
				self.at,
				Rows {
					rows,
					markers,
					passed,
				},
				first,
				start,
				ctx,
				place,
				read,
				bounds,
			);
			self.merging.push_back(batch);
			// The batches already done are applied; the front one is waited for only when the next
			// batch is to go out.
			return self.merge(false, state, yielder, flush_at, ctx).await;
		}
		#[cfg(target_family = "wasm")]
		let _ = place;
		self.run_whole(rows, state, ctx, yielder, flush_at).await
	}

	/// A state for a worker made from the statement's, with the lease's cover for its copy of the
	/// slots, or `None` where the lease refuses the copy.
	#[cfg(not(target_family = "wasm"))]
	async fn fresh(
		&mut self,
		state: &mut State,
		ctx: &ExecutionContext,
	) -> FlowResult<Option<Start>> {
		let mut under = Under::of(state, self.program, ctx)?;
		let Some(copy) = self.lease.grant(under.kept.iter().sum(), ctx).await else {
			return Ok(None);
		};
		under.covered = true;
		let write = prefix_state(self.program, state, ctx)?;
		#[cfg(test)]
		super::test_hooks::note_worker_state_made(ctx);
		Ok(Some(Start::Fresh {
			under: Box::new(under),
			write,
			copy,
		}))
	}

	/// Runs `rows` through the whole body on the statement's task, after every batch before them.
	async fn run_whole(
		&mut self,
		rows: Vec<Value>,
		state: &mut State,
		ctx: &ExecutionContext,
		yielder: &mut Out<'_, '_>,
		flush_at: usize,
	) -> FlowResult<()> {
		#[cfg(not(target_family = "wasm"))]
		while !self.merging.is_empty() {
			self.merge(true, state, yielder, flush_at, ctx).await?;
		}
		self.next += rows.len() as u64;
		self.applied = self.next;
		let program = &**self.program;
		let pipeline = &program.pipelines[self.at];
		for row in rows {
			let row = enter(pipeline, row, state)?;
			run_value_row(program, pipeline, row, state, ctx).await?;
			flush(state, yielder, flush_at, ctx).await?;
		}
		Ok(())
	}

	/// Takes the candidates of the batches at the front that are done, in order, waiting for the
	/// first when `waiting`, and applies them.
	#[cfg(not(target_family = "wasm"))]
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
			let Some(batch) = self.merging.pop_front() else {
				break;
			};
			let mut computed = batch.result().await?;
			let rows = computed.candidates.len();
			// Applying a batch walks its candidates once: no more than a batch's worth, spent
			// whole, the driver yielding at the next row it takes.
			super::super::fuel::charge(super::super::fuel::units(rows));
			if rows > 0 {
				let cost = computed.elapsed.as_nanos() as f64 / rows as f64;
				self.cost = Some(self.policy.smooth_cost(self.cost, cost));
			}
			if let Some(lent) = computed.lent.take()
				&& self.idle.len() < self.policy.read_ahead()
			{
				self.idle.push(lent);
			}
			let charged = computed.charged;
			let applied = self.apply(computed, state, yielder, flush_at, ctx).await;
			self.in_flight.fetch_sub(charged, Ordering::AcqRel);
			applied?;
		}
		Ok(())
	}

	#[cfg(target_family = "wasm")]
	async fn merge(
		&mut self,
		_waiting: bool,
		_state: &mut State,
		_yielder: &mut Out<'_, '_>,
		_flush_at: usize,
		_ctx: &ExecutionContext,
	) -> FlowResult<()> {
		Ok(())
	}

	/// Applies the candidates of one batch, in order: each resumes its record at the cut, or
	/// raises the failure its compute half raised.
	#[cfg(not(target_family = "wasm"))]
	async fn apply(
		&mut self,
		computed: Computed,
		state: &mut State,
		yielder: &mut Out<'_, '_>,
		flush_at: usize,
		ctx: &ExecutionContext,
	) -> FlowResult<()> {
		let program = &**self.program;
		let pipeline = &program.pipelines[self.at];
		// What the batch holds is released when its candidates are applied.
		let Computed {
			candidates,
			rest,
			held: _held,
			..
		} = computed;
		for mut candidate in candidates {
			// Candidates are taken in the order their rows were delivered in.
			if candidate.pos != Pos(self.applied) {
				return Err(ControlFlow::Err(anyhow::Error::new(
					crate::err::EngineError::Internal(
						"a write candidate was taken out of delivery order".to_owned(),
					),
				)));
			}
			self.applied += 1;
			match candidate.stop {
				Stop::Cut | Stop::Skip => {
					let mut pending = candidate.into_pending().map_err(ControlFlow::Err)?;
					let Some(write) = state.write.as_deref_mut() else {
						return Err(ControlFlow::Err(anyhow::anyhow!(
							"a write candidate was taken with no write state"
						)));
					};
					// A record the statement created once it had begun reading the record's table
					// is passed over, as the serial run's gate passes over it: the creations made
					// before its row was read were judged as it was handed on, and this judges
					// those an earlier record's apply made since. Nothing created since the table
					// was watched costs one load.
					if write.read_after_created(pending.row.id.as_ref())? {
						pending.pass_over();
					}
					write.hold_pending(pending);
					run_value_row(program, pipeline, Value::None, state, ctx).await?;
					flush(state, yielder, flush_at, ctx).await?;
				}
				Stop::OffPath => {
					return Err(ControlFlow::Err(anyhow::Error::new(
						crate::err::EngineError::Internal(
							"a write candidate left the cut path of a body with only one"
								.to_owned(),
						),
					)));
				}
				Stop::Failed(failure) => {
					let Some(write) = state.write.as_deref_mut() else {
						return Err(ControlFlow::Err(anyhow::anyhow!(
							"a write candidate was taken with no write state"
						)));
					};
					// A record the statement created since the row was handed on is passed over,
					// as the serial run's gate passes over it before the stages that raised: the
					// failure is never raised, and the record ends as a skipped one does.
					let Some(id) = candidate.failed_on.take() else {
						return Err(failure);
					};
					if !write.read_after_created(Some(&id))? {
						return Err(failure);
					}
					let record = WriteRow::for_record(Value::RecordId(id))?;
					let mut pending = Candidate::new(
						candidate.pos,
						Some(Box::new(record)),
						None,
						Stop::Skip,
						smallvec::SmallVec::new(),
					)
					.and_then(Candidate::into_pending)
					.map_err(ControlFlow::Err)?;
					pending.pass_over();
					write.hold_pending(pending);
					run_value_row(program, pipeline, Value::None, state, ctx).await?;
					flush(state, yielder, flush_at, ctx).await?;
				}
			}
		}
		// The rows a batch held its bound before reaching run whole, after the records before
		// them.
		for row in rest {
			self.applied += 1;
			let row = enter(pipeline, row, state)?;
			run_value_row(program, pipeline, row, state, ctx).await?;
			flush(state, yielder, flush_at, ctx).await?;
		}
		Ok(())
	}

	/// Hands `outcome` on, after stopping every worker if it is a failure: a worker writes nothing,
	/// so the statement's cancel or commit need only wait for what they hold.
	async fn settle(&mut self, outcome: FlowResult<()>, stop: bool) -> FlowResult<bool> {
		match outcome {
			Ok(()) => Ok(stop),
			Err(failure) => {
				self.abandon().await;
				Err(failure)
			}
		}
	}

	/// Stops every batch still out and waits for its worker to let go of what it holds.
	async fn abandon(&mut self) {
		self.filling.clear();
		self.place = None;
		#[cfg(not(target_family = "wasm"))]
		for batch in self.merging.drain(..) {
			batch.abandon().await;
		}
	}

	/// The source has no more rows: dispatch the batch being filled, and apply every batch still
	/// out; `true` when the pipeline takes no more.
	pub(super) async fn finish(
		&mut self,
		state: &mut State,
		ctx: &ExecutionContext,
		yielder: &mut Out<'_, '_>,
		flush_at: usize,
	) -> FlowResult<bool> {
		let outcome = self.finish_all(state, ctx, yielder, flush_at).await;
		self.place = None;
		self.settle(outcome, false).await
	}

	async fn finish_all(
		&mut self,
		state: &mut State,
		ctx: &ExecutionContext,
		yielder: &mut Out<'_, '_>,
		flush_at: usize,
	) -> FlowResult<()> {
		if !self.filling.is_empty() {
			self.dispatch(state, ctx, yielder, flush_at).await?;
		}
		#[cfg(not(target_family = "wasm"))]
		while !self.merging.is_empty() {
			self.merge(true, state, yielder, flush_at, ctx).await?;
		}
		Ok(())
	}

	/// The source failed with `failure` after the rows it yielded: every record it yielded is
	/// applied first, as the serial run applies them as it reads them, and an apply that fails
	/// first is raised instead. Never answers `true`.
	pub(super) async fn fail(
		&mut self,
		failure: ControlFlow,
		state: &mut State,
		ctx: &ExecutionContext,
		yielder: &mut Out<'_, '_>,
		flush_at: usize,
	) -> FlowResult<bool> {
		self.finish(state, ctx, yielder, flush_at).await?;
		Err(failure)
	}
}

/// Runs the compute half of `rows`, which take positions from `first`, on a worker holding `place`
/// and `read`, the lease's cover for the rows it was handed.
#[cfg(not(target_family = "wasm"))]
#[expect(
	clippy::too_many_arguments,
	reason = "a batch with the program, rows, first position, start, context, place, grant and bounds"
)]
fn spawn(
	program: &Arc<Program>,
	at: usize,
	rows: Rows,
	first: u64,
	start: Start,
	ctx: &ExecutionContext,
	place: Place,
	read: Grant,
	bounds: Bounds,
) -> Worker {
	#[cfg(test)]
	super::test_hooks::note_handed(ctx);
	let program = Arc::clone(program);
	let ctx = super::run::worker_ctx(ctx);
	let work = async move {
		let _place = place;
		let _read = read;
		let body =
			Box::pin(async move { compute(&program, at, rows, first, start, &bounds, &ctx).await });
		// What nests beneath the body runs on this worker's own frame stack.
		let mut driven = crate::exec::trampoline::Trampolined::root(futures::stream::once(body));
		match driven.next().await {
			Some(result) => result,
			None => Err(ControlFlow::Err(anyhow::anyhow!("a write worker answered nothing"))),
		}
	};
	// Everything the worker polls, the frames beneath its body included, is on the worker.
	#[cfg(test)]
	let work = super::test_hooks::on_write_worker(work);
	let handle = tokio::spawn(crate::exec::compiles::while_running_task(work));
	Worker(Some(handle))
}

/// Runs each row of a batch through the body of pipeline `at` as far as its cut, on a state of its
/// own, and answers a candidate for each. The batch stops at the first row that fails, whose
/// candidate carries the failure: no row after it is reached. It stops too after the row at which
/// the candidates it holds pass the window or the lease refuses their bytes, and hands the rows it
/// did not reach back.
#[cfg(not(target_family = "wasm"))]
async fn compute(
	program: &Arc<Program>,
	at: usize,
	batch: Rows,
	first: u64,
	start: Start,
	bounds: &Bounds,
	ctx: &ExecutionContext,
) -> FlowResult<Computed> {
	let started = web_time::Instant::now();
	let (mut state, write, copy) = match start {
		Start::Fresh {
			under,
			write,
			copy,
		} => (batch_state(program, None, *under, ctx).await?, write, copy),
		Start::Reused(Lent {
			state,
			write,
			copy,
		}) => (state, write, copy),
	};
	state.write = Some(write);
	let pipeline = &program.pipelines[at];
	let Rows {
		rows,
		markers,
		passed,
	} = batch;
	let mut candidates = Vec::with_capacity(rows.len());
	let mut charged = 0usize;
	let mut held: Option<Grant> = None;
	#[cfg(test)]
	let total = rows.len();
	let mut remaining = rows.into_iter().zip(markers).zip(passed);
	for ((row, marker), passed) in remaining.by_ref() {
		let pos = Pos(first + candidates.len() as u64);
		// A row the statement passes over runs no stage here: its candidate skips the record
		// as the serial run's gate does, before anything of it is computed or can fail.
		if passed {
			let candidate = match WriteRow::for_record(row) {
				Ok(record) => Candidate::new(
					pos,
					Some(Box::new(record)),
					None,
					Stop::Skip,
					smallvec::SmallVec::new(),
				),
				Err(failure) => Candidate::new(
					pos,
					None,
					None,
					Stop::Failed(failure),
					smallvec::SmallVec::new(),
				),
			}
			.map_err(ControlFlow::Err)?;
			let failed = matches!(candidate.stop, Stop::Failed(_));
			candidates.push(candidate);
			if failed {
				break;
			}
			continue;
		}
		let Some(write) = state.write.as_deref_mut() else {
			return Err(ControlFlow::Err(anyhow::anyhow!("a write worker lost its write state")));
		};
		write.hold_stored_metadata(marker);
		let ran = match enter(pipeline, row, &mut state) {
			Ok(row) => run_value_row(program, pipeline, row, &mut state, ctx).await,
			Err(failure) => Err(failure),
		};
		let Some(write) = state.write.as_deref_mut() else {
			return Err(ControlFlow::Err(anyhow::anyhow!("a write worker lost its write state")));
		};
		let stopped = write.take_prefix_stop();
		let candidate = match (ran, stopped) {
			(
				Ok(_),
				Some(PrefixStop {
					stop,
					live,
					encode,
				}),
			) => {
				let (row, wctx) = match write.take_in_flight() {
					Some((mut row, wctx)) => {
						if encode && let Some(wctx) = &wctx {
							encode_ahead(&mut row, wctx);
						}
						(Some(Box::new(row)), wctx)
					}
					None => (None, None),
				};
				Candidate::new(pos, row, wctx, stop, live).map_err(ControlFlow::Err)?
			}
			(Ok(_), None) => {
				return Err(ControlFlow::Err(anyhow::Error::new(
					crate::err::EngineError::Internal(
						"a write prefix ended its record at no marker".to_owned(),
					),
				)));
			}
			// The record the prefix had taken up travels with the failure, so the coordinator can
			// pass over a record the statement created since the row was handed on.
			(Err(failure), _) => {
				let failed_on = write.take_in_flight().and_then(|(row, _)| row.id);
				Candidate::failed(pos, failure, failed_on)
			}
		};
		let failed = matches!(candidate.stop, Stop::Failed(_));
		// What the candidate holds is charged to the lease as it is made: past the window, or past
		// what the lease holds, the batch ends with it and leaves the rest to the statement's task.
		let bytes = candidate_bytes(&candidate);
		candidates.push(candidate);
		if failed {
			break;
		}
		charged = charged.saturating_add(bytes);
		#[cfg(test)]
		super::test_hooks::note_held(
			ctx,
			bounds.in_flight.fetch_add(bytes, Ordering::AcqRel).saturating_add(bytes),
		);
		#[cfg(not(test))]
		bounds.in_flight.fetch_add(bytes, Ordering::AcqRel);
		let mut full = charged >= bounds.window;
		match bounds.lease.grant_rows(bytes) {
			Some(grant) => {
				held = Some(match held.take() {
					Some(held) => held.with(grant),
					None => grant,
				});
			}
			None => full = true,
		}
		if full {
			#[cfg(test)]
			if candidates.len() < total {
				super::test_hooks::note_cut_short(ctx);
			}
			break;
		}
	}
	let rest = remaining.map(|((row, _), _)| row).collect();
	let write = state
		.write
		.take()
		.ok_or_else(|| ControlFlow::Err(anyhow::anyhow!("a write worker lost its write state")))?;
	Ok(Computed {
		candidates,
		rest,
		elapsed: started.elapsed(),
		charged,
		held,
		lent: Some(Lent {
			state,
			write,
			copy,
		}),
	})
}
