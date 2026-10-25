//! Morsels of a scan run at a host's places ([`crate::exec::places`]), and a leaf's run of one.
//!
//! A host place is one more worker of a scan: it takes the earliest queued morsel from the scan's
//! table, as a worker of the node's pool does, and hands the morsel's result to the table where a
//! worker's goes, so the coordinator merges it as it merges any other. The place runs the morsel
//! to the end of its range: it gives none of its range away, never parks, and streams the rows of
//! a scan that streams them, which the coordinator takes through the morsel's outbox. A result
//! the host answers is uncharged to the query's lease, so it holds a place among the morsels in
//! flight until it is merged, and the morsels in flight stay within the policy's read-ahead.
//!
//! A place that fails before the coordinator has taken a frame of the morsel leaves the morsel to
//! the statement: the morsel is queued again at its own position, and the scan hands the host no
//! more morsels. A place that fails after the coordinator has taken rows of the morsel fails the
//! morsel with the place's error, since the rows already taken cannot be taken back.
//!
//! A leaf runs a morsel ([`run_leaf`]) on a state of its own, through a cursor of the reader the
//! host gives it, and answers the morsel's frames in the order the morsel produced them.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use futures::StreamExt;
use surrealdb_kvs::Snapshot;

use super::super::interp::{Paging, ResolvedAggregates, State};
use super::super::program::{Pipeline, Program};
use super::fold::Fold;
use super::lease::FanoutLease;
use super::merge::body_breakers;
use super::run::{MorselResult, Paged, RangeEnd, RangeRun, Under, run_range};
use super::set::{Claim, OrderKey, Shared};
use super::wire::{self, Frame, Request, UnitMaps};
use crate::catalog::TableLayouts;
use crate::exec::ExecutionContext;
use crate::exec::places::{Morsel, MorselFrames, MorselPlaces, MorselUnit};
use crate::exec::planner::placement::Policy;
use crate::expr::ControlFlow;
use crate::key::{AnyRange, RawRange};
use crate::kvs::Direction;
use crate::val::Value;

/// What a scan hands a host's places: the host, the scan's unit and the maps its partials are
/// read back through, how many of its morsels the host runs now, and whether the host has failed
/// the scan.
#[derive(Clone)]
pub(super) struct Remote {
	places: Arc<dyn MorselPlaces>,
	unit: MorselUnit,
	maps: Arc<UnitMaps>,
	running: Arc<AtomicUsize>,
	failed: Arc<AtomicBool>,
}

impl Remote {
	/// The host places pipeline `pipeline`'s morsels may run at under `ctx`: where the host gives
	/// places, the pipeline has a unit, and every aggregate the unit fills can save its state.
	pub(super) fn of(pipeline: &Pipeline, state: &State, ctx: &ExecutionContext) -> Option<Self> {
		let places = ctx.ctx().morsel_places()?;
		let unit = pipeline.unit.as_ref()?;
		if places.width() == 0 {
			return None;
		}
		let maps = wire::unit_maps(unit.bytes()).ok()?;
		for &aggregate in &maps.aggregates {
			let functions = state.aggregates.functions.get(aggregate as usize)?;
			if !super::super::groups::savable(functions) {
				return None;
			}
		}
		Some(Self {
			places: Arc::clone(places),
			unit: unit.clone(),
			maps: Arc::new(maps),
			running: Arc::default(),
			failed: Arc::default(),
		})
	}

	/// The morsels the host runs at once.
	pub(super) fn width(&self) -> usize {
		self.places.width()
	}

	/// The maps the unit's partials are read back through.
	pub(super) fn maps(&self) -> &UnitMaps {
		&self.maps
	}

	/// Count a worker started at the host's places, which ends when its run does.
	pub(super) fn started(&self) {
		self.running.fetch_add(1, Ordering::AcqRel);
	}

	/// How many more of the scan's morsels the host takes now.
	pub(super) fn room(&self) -> usize {
		if self.failed.load(Ordering::Acquire) {
			return 0;
		}
		self.places.width().saturating_sub(self.running.load(Ordering::Acquire))
	}
}

/// One of a scan's workers at a host's places: takes the earliest waiting morsel and runs it at
/// the host, until none waits, the scan is done or the host fails.
pub(super) struct HostWorker {
	pub(super) remote: Remote,
	pub(super) shared: Arc<Shared>,
	pub(super) view: Arc<dyn Snapshot>,
	pub(super) program: Arc<Program>,
	pub(super) policy: Policy,
	/// The morsel request every morsel this worker takes is sent with.
	pub(super) request: Vec<u8>,
	pub(super) aggregates: Arc<ResolvedAggregates>,
	pub(super) lease: Arc<FanoutLease>,
	pub(super) ctx: ExecutionContext,
}

/// The request of a scan's morsels as the statement stands: `None` when the scan's layouts cannot
/// be written for a leaf.
pub(super) fn request(
	maps: &UnitMaps,
	dir: Direction,
	version: Option<u64>,
	paged: Option<Paged>,
	chunk: u32,
	streams: bool,
	layouts: Option<&Arc<TableLayouts>>,
) -> Option<Vec<u8>> {
	let paged = match paged {
		Some(paged) => Some((maps.limit_of(paged.limit)?, paged.needed)),
		None => None,
	};
	wire::encode_request(&Request {
		direction: dir,
		version,
		paged,
		chunk,
		streams,
		layouts: layouts.cloned(),
	})
}

impl HostWorker {
	/// Run morsels until none waits; the worker was counted as started when it was made.
	pub(super) async fn run(self) {
		loop {
			if self.remote.failed.load(Ordering::Acquire) {
				break;
			}
			let (key, span, outbox) = match self.shared.claim(self.policy.read_ahead()) {
				Claim::Morsel(key, span, outbox) => (key, span, outbox),
				Claim::Full | Claim::Empty => break,
			};
			#[cfg(test)]
			super::test_hooks::delay_claimed(&self.ctx).await;
			let end = self.morsel(&key, &span, outbox.as_deref()).await;
			self.shared.complete(key, end);
		}
		self.remote.running.fetch_sub(1, Ordering::AcqRel);
		self.shared.arrived.notify_waiters();
	}

	/// Run the morsel at `key` over `span` at the host, and answer how it ended.
	async fn morsel(
		&self,
		key: &OrderKey,
		span: &RawRange,
		outbox: Option<&super::outbox::Outbox>,
	) -> crate::exec::FlowResult<RangeEnd> {
		let (start, end) = span.bounds();
		let morsel = Morsel {
			start: start.to_vec(),
			end: end.to_vec(),
			bytes: self.request.clone(),
		};
		let mut frames = self.remote.places.run(&self.remote.unit, morsel, &self.view);
		let mut taken = false;
		let mut received = 0u64;
		loop {
			if self.shared.cancelled(key) {
				return Ok(RangeEnd::Cancelled);
			}
			// A morsel's rows wait in its outbox until the coordinator takes them, which it does
			// only once the morsel is at the front where the merge keeps the serial order. While
			// the outbox holds a window of rows the next frame is not read, so the host's stream,
			// not the coordinator, holds what lies beyond the window.
			if let Some(outbox) = outbox {
				self.room_in(outbox, key).await;
			}
			let frame = match frames.next().await {
				Some(Ok(bytes)) => wire::decode_frame(
					&bytes,
					&self.program,
					&self.remote.maps,
					&self.aggregates.functions,
				),
				Some(Err(e)) => Err(e),
				None => Err(anyhow::anyhow!("a host place ended a morsel's frames before its end")),
			};
			match frame {
				Ok(Frame::Rows(rows)) => {
					received = received.saturating_add(rows.len() as u64);
					let Some(outbox) = outbox else {
						return Err(ControlFlow::Err(anyhow::anyhow!(
							"a host place streamed the rows of a morsel that does not stream"
						)));
					};
					taken = true;
					#[cfg(test)]
					super::test_hooks::note_held(&self.ctx, outbox.held());
					let bytes = rows.iter().map(crate::exec::memory::size::value_bytes).sum();
					let grant = self.lease.grant_queued(bytes, &self.ctx);
					outbox.put(rows, bytes, grant);
					self.shared.fed.notify_one();
				}
				Ok(Frame::End {
					result,
					emitted,
				}) => {
					let mut result = match *result {
						Ok(result) => result,
						Err(e) => return Err(ControlFlow::Err(e)),
					};
					let actual = received.saturating_add(result.output.len() as u64);
					if actual != emitted {
						return Err(ControlFlow::Err(anyhow::anyhow!(
							"a host place answered {actual} rows but its morsel emitted {emitted}"
						)));
					}
					if let Some(outbox) = outbox {
						let last = std::mem::take(&mut result.output);
						result.output = outbox.finish(last);
					}
					let [rows, keys, values] = result.scanned;
					self.ctx.txn().record_view_scan(rows, keys, values);
					return Ok(RangeEnd::Done(result));
				}
				Err(e) if !taken => {
					// Nothing of the morsel has reached the statement: it is queued again where it
					// lies, for the statement and its own workers to run.
					tracing::debug!(error = %e, "a host place failed a morsel; the statement runs it");
					self.remote.failed.store(true, Ordering::Release);
					return Ok(RangeEnd::Parked(MorselResult::empty(&self.program), span.clone()));
				}
				Err(e) => return Err(ControlFlow::Err(e)),
			}
		}
	}
}

impl HostWorker {
	/// Wait until `outbox`, the outbox of the morsel at `key`, holds less than the window, or the
	/// morsel is no longer needed.
	async fn room_in(&self, outbox: &super::outbox::Outbox, key: &OrderKey) {
		let window = self.policy.window_bytes();
		loop {
			let mut taken = std::pin::pin!(outbox.taken());
			taken.as_mut().enable();
			let mut stopped = std::pin::pin!(self.shared.arrived.notified());
			stopped.as_mut().enable();
			if outbox.held() < window || self.shared.cancelled(key) {
				return;
			}
			futures::future::select(taken, stopped).await;
		}
	}
}

/// Run `morsel` of the unit `unit` against `reader` under `ctx`, a context of the leaf's own
/// engine, and answer its frames: the rows of each chunk where the scan streams them, then its
/// end. A unit or morsel of another format or build is answered as a failure of the place.
pub(crate) fn run_leaf(
	unit: &[u8],
	morsel: &Morsel,
	reader: Arc<dyn Snapshot>,
	ctx: ExecutionContext,
) -> MorselFrames {
	let decoded = wire::decode_unit(unit).and_then(|unit| {
		let request = wire::decode_request(&morsel.bytes)?;
		Ok((unit, request))
	});
	let (unit, request) = match decoded {
		Ok(decoded) => decoded,
		Err(e) => return Box::pin(futures::stream::once(async move { Err(e) })),
	};
	let span = RawRange::every_key().part(&morsel.start, &morsel.end);
	let (tx, rx) = futures::channel::mpsc::unbounded::<anyhow::Result<Vec<u8>>>();
	let run = async move {
		let program = unit.program;
		let reads_row = program.pipelines[0].reads_row;
		let rows_tx = tx.clone();
		let streamed = std::sync::atomic::AtomicU64::new(0);
		let on_chunk = |rows: Vec<Value>| {
			streamed.fetch_add(rows.len() as u64, Ordering::Relaxed);
			let _ = rows_tx.unbounded_send(wire::rows_frame(&rows));
		};
		let streams = request.streams;
		let end = crate::exec::compiles::while_running_task(leaf_morsel(
			&program,
			&span,
			&request,
			reader,
			streams.then_some(&on_chunk as &(dyn Fn(Vec<Value>) + Sync)),
			&ctx,
		))
		.await;
		let streamed = streamed.load(Ordering::Relaxed);
		let _ = tx.unbounded_send(Ok(wire::end_frame(end, streamed, reads_row)));
		// The leaf's transaction was never read through; it ends with the run.
		let _ = ctx.txn().cancel().await;
	};
	// The run is polled as the frames are taken, and sends its end last, so the frames end when
	// the run does.
	let run = futures::stream::once(run).filter_map(|()| async { None });
	Box::pin(futures::stream::select(rx, run))
}

/// The leaf's run of one morsel over `span`, its result numbered as the unit numbers its breakers.
async fn leaf_morsel(
	program: &Arc<Program>,
	span: &RawRange,
	request: &Request,
	reader: Arc<dyn Snapshot>,
	chunks: Option<&(dyn Fn(Vec<Value>) + Sync)>,
	ctx: &ExecutionContext,
) -> anyhow::Result<MorselResult> {
	let flow = |flow: ControlFlow| match flow {
		ControlFlow::Err(e) => e,
		other @ ControlFlow::Break
		| other @ ControlFlow::Continue
		| other @ ControlFlow::Return(_) => anyhow::anyhow!("a morsel ended with {other:?}"),
	};
	let pipeline = &program.pipelines[0];
	let paging = vec![
		Paging {
			skip: 0,
			remaining: None,
		};
		program.limits.len()
	];
	let mut seed = State::new(program, paging, ctx.function_registry())?;
	let shared = seed.shared(program, ctx.function_registry())?;
	let breakers = body_breakers(&pipeline.code);
	let under = Under {
		frames: Vec::new(),
		slots: Vec::new(),
		kept: Vec::new(),
		covered: true,
		verdicts: Vec::new(),
		bypassed: false,
		shared,
		memos: Vec::new(),
		fold: Fold::of(pipeline, &breakers),
	};
	let mut cursor = reader
		.open(span.clone().into_key_range(), request.direction, request.version)
		.await
		.map_err(|e| anyhow::Error::new(e).context("Failed to open scan cursor"))?;
	let lease = FanoutLease::new(None);
	let end = run_range(
		&mut *cursor,
		RangeRun {
			span,
			dir: request.direction,
			program,
			at: 0,
			paged: request.paged.map(|(limit, needed)| Paged {
				limit,
				needed,
			}),
			probe: None,
			layouts: request.layouts.as_deref(),
			chunk: request.chunk,
			window: usize::MAX,
			lease: &lease,
			outbox: None,
			donor: None,
			chunks,
		},
		under,
		ctx,
		&|| false,
	)
	.await
	.map_err(flow)?;
	match end {
		RangeEnd::Done(result) => Ok(result),
		RangeEnd::Parked(..) | RangeEnd::Cancelled => {
			anyhow::bail!("a leaf's morsel stopped before the end of its range")
		}
	}
}

/// The workers of a scan at a host's places on a target with no task spawner: the scan polls
/// them beside its coordinator ([`Self::drive`]) and, once it is done, until they have ended
/// ([`Self::finish`]).
#[cfg(target_family = "wasm")]
#[derive(Default)]
pub(super) struct HostTasks {
	tasks: std::sync::Mutex<
		futures::stream::FuturesUnordered<
			std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
		>,
	>,
	waker: std::sync::Mutex<Option<std::task::Waker>>,
}

#[cfg(target_family = "wasm")]
impl HostTasks {
	/// Run `task` among the scan's workers.
	pub(super) fn push(
		&self,
		task: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
	) {
		self.tasks.lock().unwrap_or_else(|e| e.into_inner()).push(task);
		if let Some(waker) = self.waker.lock().unwrap_or_else(|e| e.into_inner()).take() {
			waker.wake();
		}
	}

	/// Poll the workers, as they are pushed, for as long as the scan runs: never ready.
	pub(super) async fn drive(&self) {
		std::future::poll_fn(|cx| {
			*self.waker.lock().unwrap_or_else(|e| e.into_inner()) = Some(cx.waker().clone());
			let mut tasks = self.tasks.lock().unwrap_or_else(|e| e.into_inner());
			while let std::task::Poll::Ready(Some(())) = tasks.poll_next_unpin(cx) {}
			std::task::Poll::<()>::Pending
		})
		.await
	}

	/// Poll the workers until every one has ended.
	pub(super) async fn finish(&self) {
		std::future::poll_fn(|cx| {
			let mut tasks = self.tasks.lock().unwrap_or_else(|e| e.into_inner());
			loop {
				match tasks.poll_next_unpin(cx) {
					std::task::Poll::Ready(Some(())) => {}
					std::task::Poll::Ready(None) => return std::task::Poll::Ready(()),
					std::task::Poll::Pending => return std::task::Poll::Pending,
				}
			}
		})
		.await
	}
}
