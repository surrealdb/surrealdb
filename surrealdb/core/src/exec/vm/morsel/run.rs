//! Running one morsel: its rows through the pipeline's body on a state of its own.

use std::sync::Arc;

#[cfg(not(target_family = "wasm"))]
use futures::StreamExt;

use super::super::host::{Ended, run_value_row};
use super::super::interp::{Arrivals, Groups, Paging, PooledState, SortRow, State};
use super::super::ir_exec::{MemoCell, install_memos, ir_state, memos_of};
use super::super::partial::Partial;
use super::super::program::{Pipeline, Program};
use super::lease::FanoutLease;
#[cfg(not(target_family = "wasm"))]
use super::lease::Grant;
use super::merge::held_bytes;
#[cfg(not(target_family = "wasm"))]
use super::places::Place;
use crate::exec::memory::Reservation;
use crate::exec::planner::placement::{Combine, MorselPlan, Parallel};
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::ControlFlow;
use crate::val::Value;

/// What one morsel produced, on a state of its own.
pub(super) struct MorselResult {
	pub(super) output: Vec<Value>,
	pub(super) sorts: Vec<Vec<(Vec<Value>, SortRow)>>,
	/// The slab each sort's rows read from, by sort: the stored bytes a row run in place kept for
	/// its sort.
	pub(super) slabs: Vec<Vec<u8>>,
	/// What each bounded sort counted across the morsel's rows.
	pub(super) arrivals: Vec<Arrivals>,
	pub(super) groups: Vec<Option<Groups>>,
	/// A partial per breaker the morsel spilled; that breaker's buffer is absent from `sorts` and
	/// `groups`.
	pub(super) spilled: Vec<Partial>,
	/// What each of the morsel's sorts and groups holds of its query's lease, by breaker, the
	/// records its partials hold included; empty without a query memory. The merge takes the
	/// reservations over for the statement's own accounts, so a morsel waiting to merge still
	/// counts against the lease and the rows it hands over are counted once.
	pub(super) sort_accounts: Vec<Reservation>,
	pub(super) group_accounts: Vec<Reservation>,
	/// For a paged body, how the rows the morsel ran ended, in row order.
	pub(super) ends: Vec<End>,
	pub(super) rows: usize,
	pub(super) elapsed: std::time::Duration,
	/// What a morsel that read a key range through a cursor read and held: the bytes of the keys
	/// and values, the chunks it read them in, and the most output bytes it held. Zero for a
	/// batch.
	pub(super) bytes: u64,
	pub(super) chunks: u64,
	pub(super) held: usize,
}

/// How the rows of a paged body's morsel ended: a row that left something for the merge to replay,
/// or a run of rows that left nothing.
pub(super) enum End {
	/// This many consecutive rows did not reach the paging, did not fail and handed nothing to the
	/// output: the merge counts them and replays nothing else. A morsel's ends are bounded by the
	/// rows that reached the paging or failed, not by the rows it ran.
	Passed(usize),
	/// A row that reached the paging, failed, or emitted.
	Row(RowEnd),
}

impl End {
	/// Record `end` as the next of `ends`: a row that left nothing to replay extends the trailing
	/// run of such rows, any other row is kept as it ended.
	fn record(ends: &mut Vec<End>, end: RowEnd) {
		if !end.reached && end.failure.is_none() && end.emitted == 0 && end.key.is_none() {
			if let Some(End::Passed(rows)) = ends.last_mut() {
				*rows += 1;
			} else {
				ends.push(End::Passed(1));
			}
		} else {
			ends.push(End::Row(end));
		}
	}
}

/// How one row of a paged body's morsel ended.
pub(super) struct RowEnd {
	/// Whether the row reached the paging clause the morsel counts its rows against
	/// ([`State::batch_paging`]): a row a filter refuses never does, whatever clauses of their own
	/// the statements nested in the filter run.
	pub(super) reached: bool,
	/// Whether the morsel skipped the row itself, as one `START` was sure to skip.
	pub(super) skipped: bool,
	/// How many rows it handed to the output.
	pub(super) emitted: usize,
	pub(super) failure: Option<ControlFlow>,
	/// For a body whose rows pass through a distinct set, the row the morsel's own set took, where
	/// this row passed it; the statement's set takes it, or drops what the row did, in the merge.
	pub(super) key: Option<Value>,
}

/// A batch of a Batches-input pipeline whose results the merge has not taken yet.
pub(super) enum PendingBatch {
	/// Run on the statement's task, or a failure the source raised. Boxed so a pending batch stays
	/// the size of a worker handle; one box per batch.
	Done(Box<FlowResult<MorselResult>>),
	/// Running on a worker, which is aborted if the batch is dropped unmerged.
	#[cfg(not(target_family = "wasm"))]
	Running(Worker),
}

#[cfg(not(target_family = "wasm"))]
pub(super) struct Worker(Option<tokio::task::JoinHandle<FlowResult<MorselResult>>>);

#[cfg(not(target_family = "wasm"))]
impl Drop for Worker {
	fn drop(&mut self) {
		if let Some(handle) = &self.0 {
			handle.abort();
		}
	}
}

impl PendingBatch {
	/// Whether the batch's results can be taken without waiting.
	pub(super) fn is_done(&self) -> bool {
		match self {
			PendingBatch::Done(_) => true,
			#[cfg(not(target_family = "wasm"))]
			PendingBatch::Running(worker) => worker.0.as_ref().is_none_or(|handle| handle.is_finished()),
		}
	}

	/// Whether the batch is running on a worker, which holds a place until it is done.
	pub(super) fn holds_place(&self) -> bool {
		match self {
			PendingBatch::Done(_) => false,
			#[cfg(not(target_family = "wasm"))]
			PendingBatch::Running(worker) => worker.0.as_ref().is_some_and(|handle| !handle.is_finished()),
		}
	}

	/// The batch's results, waiting for its worker.
	pub(super) async fn result(self) -> FlowResult<MorselResult> {
		match self {
			PendingBatch::Done(result) => *result,
			#[cfg(not(target_family = "wasm"))]
			PendingBatch::Running(mut worker) => {
				let handle = worker.0.take().expect("a worker is taken once");
				match handle.await {
					Ok(result) => result,
					Err(e) => Err(ControlFlow::Err(anyhow::anyhow!("a morsel worker failed: {e}"))),
				}
			}
		}
	}
}

/// What a morsel runs under besides its rows: the statement's frames, the block names it reads, the
/// verdicts its filters already hold on the conjuncts they judge once — a morsel that finds none
/// judges them for itself, deterministic as they are — whether its permissions are bypassed, and
/// what the run shares with its morsels, the memo cells among it.
#[derive(Clone)]
pub(super) struct Under {
	pub(super) frames: Vec<ExecutionContext>,
	pub(super) slots: Vec<Value>,
	/// The bytes the statement's ledger charges each slot, which the morsel's copy of the slots is
	/// charged again unless `covered`; the fan-out lease's grant is of their sum.
	pub(super) kept: Vec<usize>,
	/// Whether the morsel's copy of the slots is already accounted for: a worker's lease grant
	/// covers it, and a batch the coordinator runs itself makes no copy at all.
	/// When set, `batch_state` charges nothing for the copy.
	pub(super) covered: bool,
	pub(super) verdicts: Vec<Option<bool>>,
	pub(super) bypassed: bool,
	pub(super) shared: super::super::interp::RunShare,
	/// The statement's memo cells, shared with each morsel's state: a value one morsel keeps in a
	/// cell is read by the others, and a morsel that resets a cell empties it for itself alone.
	/// Empty when the statement's state holds no IR run state.
	pub(super) memos: Vec<Arc<MemoCell>>,
}

impl Under {
	/// What a morsel of `program` runs under, from the statement `state`.
	pub(super) fn of(
		state: &mut State,
		program: &Program,
		ctx: &ExecutionContext,
	) -> FlowResult<Self> {
		Ok(Self {
			frames: state.frames.clone(),
			slots: state.slots.clone(),
			kept: state.slot_charges(),
			covered: false,
			verdicts: state.verdicts.clone(),
			bypassed: state.permissions_bypassed,
			shared: state.shared(program, ctx.function_registry()).map_err(ControlFlow::Err)?,
			memos: memos_of(ir_state(state)),
		})
	}
}

/// Whether `pipeline`'s morsels merge by keeping the first occurrence of each row their bodies pass
/// through a distinct set.
fn distinct_rows(pipeline: &Pipeline) -> bool {
	matches!(
		&pipeline.parallel,
		Parallel::Morsels(MorselPlan {
			combine: Combine::Distinct,
			..
		})
	)
}

/// Run one row of a morsel's body. A morsel's paging never spends its limit, so the body never
/// stops its pipeline; one that did would answer fewer rows than the serial run.
pub(super) async fn run_row(
	program: &Program,
	pipeline: &Pipeline,
	row: Value,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<()> {
	match run_value_row(program, pipeline, row, state, ctx).await? {
		Ended::Stop => Err(ControlFlow::Err(anyhow::Error::new(
			crate::err::EngineError::Internal("a morsel's body stopped its pipeline".to_owned()),
		))),
		Ended::Next => Ok(()),
	}
}

/// Run a batch of pipeline `at` on a state of its own, under the statement's frames and the block
/// names it reads. A body `paged` by a clause runs each row as though it were taken, but for the
/// first rows reaching the paging that the clause is sure to skip, and records how each row ended;
/// the batch stops at a row that fails before its paging, which no later row can be reached past.
/// A `bound` batch charges its breakers to the query's lease and spills those it outgrows
/// ([`MorselResult::spilled`]).
pub(super) async fn run_batch(
	program: &Arc<Program>,
	at: usize,
	rows: Vec<Value>,
	paged: Option<(u16, usize)>,
	bound: bool,
	under: Under,
	ctx: &ExecutionContext,
) -> FlowResult<MorselResult> {
	let started = web_time::Instant::now();
	let mut state = batch_state(program, paged, under, ctx).await?;
	if bound {
		super::super::host::bound_batch(program, &mut state, ctx);
	}
	let pipeline = &program.pipelines[at];
	// A batch that pages its rows keys none: which of them reach the lookups is the merge's to say.
	if paged.is_none() {
		super::super::host::fill_frontier_rows(program, pipeline, &rows, &mut state, ctx).await;
	}
	let count = rows.len();
	let mut ends = Vec::new();
	let distinct = distinct_rows(pipeline);
	state.capture_distinct = distinct;
	for row in rows {
		if distinct {
			state.distinct_taken = None;
			let before = state.output.len();
			let failure = run_row(program, pipeline, row, &mut state, ctx).await.err();
			let key = state.distinct_taken.take();
			// A row the morsel's own set took is the merge's to take or drop, with what the row
			// did after the set; a row that failed before the set stops the morsel.
			let stops = failure.is_some() && key.is_none();
			if key.is_some() || failure.is_some() {
				ends.push(End::Row(RowEnd {
					reached: key.is_some(),
					skipped: false,
					emitted: state.output.len() - before,
					failure,
					key,
				}));
			}
			if state.memory.is_some() {
				super::super::host::spill_breakers(&mut state, ctx).await?;
			}
			if stops {
				break;
			}
			continue;
		}
		let Some((limit, _)) = paged else {
			run_row(program, pipeline, row, &mut state, ctx).await?;
			if state.memory.is_some() {
				super::super::host::spill_breakers(&mut state, ctx).await?;
			}
			continue;
		};
		state.reached_paging = false;
		let before = state.output.len();
		let skip = state.paging[limit as usize].skip;
		let failure = run_row(program, pipeline, row, &mut state, ctx).await.err();
		let reached = state.reached_paging;
		let stops = failure.is_some() && !reached;
		End::record(
			&mut ends,
			RowEnd {
				reached,
				skipped: state.paging[limit as usize].skip < skip,
				emitted: state.output.len() - before,
				failure,
				key: None,
			},
		);
		if state.memory.is_some() {
			super::super::host::spill_breakers(&mut state, ctx).await?;
		}
		if stops {
			break;
		}
	}
	finish(program, state, ends, count, started.elapsed(), ctx).await
}

/// Take what a morsel's `state` holds as the morsel's result: a breaker that spilled becomes a
/// partial, and what the morsel holds stays reserved from the lease until the merge takes it over
/// for the statement.
pub(super) async fn finish(
	program: &Arc<Program>,
	mut state: PooledState,
	ends: Vec<End>,
	rows: usize,
	elapsed: std::time::Duration,
	ctx: &ExecutionContext,
) -> FlowResult<MorselResult> {
	// A breaker's reservation leaves the state before its buffer becomes a partial, which clears
	// the state's account: the lease keeps counting the buffer until the partial's own records are
	// charged in its place, and the account then returns to the state to travel with the morsel to
	// the merge.
	let mut spilled = Vec::new();
	#[cfg(test)]
	let leased = state.memory.as_ref().map(|memory| memory.lease().held());
	#[cfg(test)]
	super::test_hooks::note_ends(ctx, ends.len());
	for sort in 0..program.sorts.len() {
		if state.sort_runs[sort].is_none() {
			super::sorted::build_run(&mut state, program, sort, ctx).await?;
		}
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
	let (sort_accounts, group_accounts) = if state.memory.is_some() {
		(std::mem::take(&mut state.sort_accounts), std::mem::take(&mut state.group_accounts))
	} else {
		Default::default()
	};
	#[cfg(test)]
	if let (Some(leased), Some(memory)) = (leased, state.memory.as_ref())
		&& memory.lease().held() < leased
	{
		super::test_hooks::note_lease_fault(&state, super::test_hooks::LeaseFault::Released);
	}
	#[cfg(test)]
	super::test_hooks::note_partials(&state, &spilled);
	Ok(MorselResult {
		output: state.take_output(),
		sorts: std::mem::take(&mut state.sorts),
		slabs: std::mem::take(&mut state.sort_slabs),
		arrivals: std::mem::take(&mut state.sort_arrivals),
		groups: std::mem::take(&mut state.groups),
		spilled,
		sort_accounts,
		group_accounts,
		ends,
		rows,
		elapsed,
		bytes: 0,
		chunks: 0,
		held: 0,
	})
}

/// A state for a morsel of `program` under `under`: its paging, its fuel and the statement's frames
/// and block names. The copy of the slots is charged to the query's lease and insisted on, which
/// can raise the memory threshold error, unless `under` is `covered`: then a grant of the fan-out
/// lease stands for the copy, or the coordinator runs the morsel itself, and nothing is charged.
pub(super) async fn batch_state(
	program: &Arc<Program>,
	paged: Option<(u16, usize)>,
	under: Under,
	ctx: &ExecutionContext,
) -> FlowResult<PooledState> {
	// Paging that never spends, and skips only the rows sure to be skipped; a statement the body
	// runs starts its own paging afresh.
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
	state.batch_paging = paged.map(|(limit, _)| limit);
	// A worker's task draws on its own thread's fuel, no more than the statement's quantum before
	// its first yield.
	super::super::fuel::cap(super::super::fuel::quantum(ctx));
	state.program = Some(Arc::clone(program));
	state.frames = under.frames;
	state.slots = under.slots;
	if !under.covered {
		state.keep_copied(under.kept, ctx).await?;
	}
	state.verdicts = under.verdicts;
	state.permissions_bypassed = under.bypassed;
	install_memos(ir_state(&mut state), under.memos);
	Ok(state)
}

/// The context a morsel's worker runs under: the statement's, which holds no read buffer. A
/// worker never fills or serves from the coordinator's buffer, so what the coordinator buffered
/// is not read from another thread.
///
/// The clone shares the statement's context as it is; that context must hold no read buffer.
/// Otherwise a worker would read from the coordinator's buffer, which it never may.
#[cfg(not(target_family = "wasm"))]
pub(super) fn worker_ctx(ctx: &ExecutionContext) -> ExecutionContext {
	ctx.clone()
}

/// Run a batch on a worker holding `place` and `grant`, the lease's cover for its copy of the
/// slots, until it is done.
#[cfg(not(target_family = "wasm"))]
#[expect(
	clippy::too_many_arguments,
	reason = "a batch with the program, rows, paging, binding, state, context, place and grant it runs in"
)]
pub(super) fn spawn(
	program: &Arc<Program>,
	at: usize,
	rows: Vec<Value>,
	paged: Option<(u16, usize)>,
	bound: bool,
	under: Under,
	ctx: &ExecutionContext,
	place: Place,
	grant: Grant,
) -> Worker {
	#[cfg(test)]
	super::test_hooks::note_handed(ctx);
	let program = Arc::clone(program);
	let ctx = worker_ctx(ctx);
	let handle = tokio::spawn(crate::exec::compiles::while_running_task(async move {
		let _place = place;
		let _grant = grant;
		let body =
			Box::pin(async move { run_batch(&program, at, rows, paged, bound, under, &ctx).await });
		// What nests beneath the body runs on this worker's own frame stack.
		let mut driven = crate::exec::trampoline::Trampolined::root(futures::stream::once(body));
		match driven.next().await {
			Some(result) => result,
			None => Err(ControlFlow::Err(anyhow::anyhow!("a morsel worker answered nothing"))),
		}
	}));
	Worker(Some(handle))
}

/// How a morsel that read a key range through a view ended.
pub(super) enum RangeEnd {
	/// Every entry of its range ran.
	Done(MorselResult),
	/// The morsel held as much output as its window allows and stopped at a chunk boundary: its
	/// result so far, and the part of its range still to read.
	Parked(MorselResult, crate::key::RawRange),
	/// The statement no longer needs the morsel.
	Cancelled,
}

/// The paging of a Range morsel, which a merge replays.
#[derive(Clone, Copy)]
pub(super) struct Paged {
	/// The paging clause.
	pub(super) limit: u16,
	/// The rows that can reach the paging and be taken, at most, as the statement stood when the
	/// worker started: `START` and `LIMIT` together. `None` when `LIMIT` bounds nothing.
	pub(super) needed: Option<usize>,
}

/// What a worker's morsel reads through, and how it ends.
pub(super) struct RangeRun<'a> {
	pub(super) span: &'a crate::key::RawRange,
	pub(super) dir: crate::kvs::Direction,
	pub(super) program: &'a Arc<Program>,
	pub(super) at: usize,
	pub(super) paged: Option<Paged>,
	/// The top-k pushdown the entries are judged by before they run, where the source carries one.
	pub(super) probe: Option<&'a crate::exec::topk_pushdown::TopKThresholdProbe>,
	/// The layouts of the scanned table that a packed record is decoded through.
	pub(super) layouts: Option<&'a crate::catalog::TableLayouts>,
	/// The entries one read asks for, at most.
	pub(super) chunk: u32,
	/// Output bytes the morsel holds before it parks.
	pub(super) window: usize,
	/// The run's fan-out lease: a morsel parks at the first chunk boundary after it is revoked.
	pub(super) lease: &'a FanoutLease,
	/// How the morsel gives part of its range away, or returns its place, at a chunk boundary.
	pub(super) donor: super::steal::Donor<'a>,
}

/// Run the entries of `run.span` that `cursor` reads, as one morsel of pipeline `run.at` of
/// `run.program` on a state of its own, a chunk at a time: each entry runs the body in place, over
/// its stored bytes, and each chunk is charged to the thread's fuel as the coordinator's own reads
/// are.
///
/// A paged body runs each entry as though it were taken, for the merge to count, and records how
/// each ended ([`RowEnd`]); the morsel stops at an entry that fails before its paging, which no
/// later entry can be reached past, and once as many entries have reached the paging as the
/// statement can take, which no later entry can follow.
///
/// The morsel stops at the first chunk boundary at which `cancelled` answers `true`, and parks at
/// the first at which its output weighs as much as the window and entries remain, at which the
/// run's lease has been revoked, or at which the policy has it return its place. At every other
/// chunk boundary the donor may give the tail of the morsel's range away; the cursor and the range
/// the morsel parks from narrow to what it keeps.
pub(super) async fn run_range(
	cursor: &mut dyn surrealdb_kvs::RangeCursor,
	run: RangeRun<'_>,
	under: Under,
	ctx: &ExecutionContext,
	cancelled: &(dyn Fn() -> bool + Sync),
) -> FlowResult<RangeEnd> {
	use crate::expr::ControlFlowExt;

	let RangeRun {
		span,
		dir,
		program,
		at,
		paged,
		probe,
		layouts,
		chunk: most,
		window,
		lease,
		donor,
	} = run;
	let started = web_time::Instant::now();
	let mut state = batch_state(program, paged.map(|paged| (paged.limit, 0)), under, ctx).await?;
	if paged.is_none() {
		super::super::host::bound_batch(program, &mut state, ctx);
	}
	let pipeline = &program.pipelines[at];
	let distinct = distinct_rows(pipeline);
	state.capture_distinct = distinct;
	let mut rows = 0usize;
	let mut ends = Vec::new();
	let mut reached = 0usize;
	// The output and the row ends the morsel holds, in bytes, counted as chunks add to them.
	let (mut held, mut sized, mut counted) = (0usize, 0usize, 0usize);
	let mut parked = None;
	let mut own = span.clone();
	let mut progress = super::steal::Progress::default();
	let mut bytes = 0u64;
	let mut chunks = 0u64;
	let mut held_most = 0usize;
	loop {
		if cancelled() {
			return Ok(RangeEnd::Cancelled);
		}
		let chunk = super::super::fuel::batch(most);
		let budget = super::super::fuel::left();
		let mut unspent = budget;
		let mut failed = None;
		let mut halted = false;
		let mut spent = false;
		// The threshold is read once a chunk: a stale one only rejects less. A paged body counts
		// every entry for the merge, so it is never judged by one.
		let threshold = probe.filter(|_| paged.is_none()).and_then(|probe| probe.snapshot());
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
				if progress.first.is_empty() {
					progress.first.extend_from_slice(key);
				}
				if distinct {
					state.distinct_taken = None;
					let before = state.output.len();
					let failure = match super::super::host::in_place_row(
						program, pipeline, key, value, layouts, &mut state,
					) {
						Ok(false) => None,
						Ok(true) => Some(ControlFlow::Err(stopped_pipeline())),
						Err(e) => Some(ControlFlow::Err(e)),
					};
					let taken = state.distinct_taken.take();
					// A row that failed before the set leaves nothing to take past it.
					halted = failure.is_some() && taken.is_none();
					if taken.is_some() || failure.is_some() {
						ends.push(End::Row(RowEnd {
							reached: taken.is_some(),
							skipped: false,
							emitted: state.output.len() - before,
							failure,
							key: taken,
						}));
					}
					return Ok(if halted {
						std::ops::ControlFlow::Break(())
					} else {
						next
					});
				}
				let Some(paged) = paged else {
					let admitted = super::super::host::admit_row(
						pipeline,
						probe.zip(threshold.as_deref()),
						key,
						value,
						layouts,
					);
					if let (Some(probe), Some(threshold)) = (probe, threshold.as_deref())
						&& probe.rejects(threshold, &admitted)
					{
						rejected += 1;
						return Ok(next);
					}
					return match super::super::host::in_place_row_admitted(
						program, pipeline, key, value, &admitted, layouts, &mut state,
					) {
						Ok(false) => Ok(next),
						Ok(true) => {
							failed = Some(stopped_pipeline());
							Ok(std::ops::ControlFlow::Break(()))
						}
						Err(e) => {
							failed = Some(e);
							Ok(std::ops::ControlFlow::Break(()))
						}
					};
				};
				state.reached_paging = false;
				let before = state.output.len();
				let failure = match super::super::host::in_place_row(
					program, pipeline, key, value, layouts, &mut state,
				) {
					Ok(false) => None,
					Ok(true) => Some(ControlFlow::Err(stopped_pipeline())),
					Err(e) => Some(ControlFlow::Err(e)),
				};
				let reached_paging = state.reached_paging;
				// A failure before the paging leaves nothing to count past it.
				halted = failure.is_some() && !reached_paging;
				End::record(
					&mut ends,
					RowEnd {
						reached: reached_paging,
						skipped: false,
						emitted: state.output.len() - before,
						failure,
						key: None,
					},
				);
				if reached_paging {
					reached += 1;
					halted |= paged.needed.is_some_and(|needed| reached >= needed);
				}
				Ok(if halted {
					std::ops::ControlFlow::Break(())
				} else {
					next
				})
			})
			.await
			.context("Failed to scan record")?;
		super::super::fuel::charge(budget - unspent);
		// The view's cursor is not the transaction's, so the chunk is counted into the
		// transaction's scan metrics here, as its own cursors count theirs.
		ctx.txn().record_view_scan(stats.rows, stats.key_bytes, stats.value_bytes);
		if let Some(e) = failed {
			return Err(ControlFlow::Err(e));
		}
		#[cfg(test)]
		super::test_hooks::note_pruned(ctx, rejected);
		#[cfg(not(test))]
		let _ = rejected;
		rows += stats.rows as usize;
		let chunk_bytes = stats.key_bytes + stats.value_bytes;
		bytes += chunk_bytes;
		chunks += 1;
		progress.visited += stats.rows;
		progress.bytes += chunk_bytes;
		progress.since_cut += stats.rows;
		progress.quanta += 1;
		#[cfg(test)]
		super::test_hooks::note_scanned(ctx, stats.rows as usize);
		crate::exec::operators::scan::common::ensure_below_memory_threshold()?;
		if state.memory.is_some() {
			super::super::host::spill_breakers(&mut state, ctx).await?;
		}
		// A chunk that read fewer entries than it asked for, and was not stopped, has read the
		// range's last.
		if halted || (!spent && stats.rows < u64::from(chunk)) {
			break;
		}
		if super::super::fuel::spent() {
			super::super::fuel::yield_point(ctx).await?;
		}
		for row in &state.output[sized..] {
			held += crate::exec::memory::size::value_bytes(row);
		}
		sized = state.output.len();
		held += (ends.len() - counted) * std::mem::size_of::<End>();
		counted = ends.len();
		held_most = held_most.max(held);
		let yielded = if held >= window || lease.revoked() {
			false
		} else {
			match donor.boundary(cursor, &mut own, dir, &progress).await? {
				super::steal::Boundary::Go => continue,
				super::steal::Boundary::Donated => {
					progress.since_cut = 0;
					progress.cuts += 1;
					continue;
				}
				super::steal::Boundary::Yield => true,
			}
		};
		let resume = cursor.resume();
		#[cfg(test)]
		if resume.is_some() {
			super::test_hooks::note_parked(ctx);
		}
		if yielded && resume.is_some() {
			donor.gate.shared.metrics().yielded += 1;
		}
		parked = resume.map(|resume| {
			let range = surrealdb_kvs::key::AnyRange::into_key_range(own.clone());
			let rest = resume.remainder(&range, dir);
			own.part(rest.start.as_slice(), rest.end.as_slice())
		});
		break;
	}
	let mut result = finish(program, state, ends, rows, started.elapsed(), ctx).await?;
	result.bytes = bytes;
	result.chunks = chunks;
	result.held = held_most;
	Ok(match parked {
		Some(rest) => RangeEnd::Parked(result, rest),
		None => RangeEnd::Done(result),
	})
}

/// The failure of a morsel whose body stops its pipeline, which a morsel's paging never does.
fn stopped_pipeline() -> anyhow::Error {
	anyhow::Error::new(crate::err::EngineError::Internal(
		"a morsel's body stopped its pipeline".to_owned(),
	))
}

#[cfg(test)]
mod tests {
	use super::super::super::ir_exec::IrRunState;
	use super::*;

	/// The cells a run state holds are the cells a worker's state installs, not copies of them,
	/// and a run with no state shares none.
	#[test]
	fn memos_shared_through_under() {
		let mut run = IrRunState::default();
		run.memos.push(Arc::default());
		run.memos.push(Arc::default());

		let shared = memos_of(Some(&mut run));
		assert_eq!(shared.len(), 2);
		assert!(Arc::ptr_eq(&shared[0], &run.memos[0]));

		let mut worker = IrRunState::default();
		install_memos(Some(&mut worker), shared);
		assert_eq!(worker.memos.len(), 2);
		assert!(Arc::ptr_eq(&worker.memos[0], &run.memos[0]));
		assert!(Arc::ptr_eq(&worker.memos[1], &run.memos[1]));

		assert!(memos_of(None).is_empty());
		install_memos(None, run.memos.clone());
	}
}
