//! Streaming a query's results out of an embedded engine.
//!
//! [`EmbeddedEngine::execute`](crate::EmbeddedEngine::execute) answers a query
//! with every statement's result at once, which means holding a whole `SELECT`
//! in memory and handing JavaScript nothing until the last row is read.
//! [`EmbeddedEngine::query_stream`](crate::EmbeddedEngine::query_stream)
//! answers with [`QueryStreamFrame`]s instead, in the same sequence and with the
//! same meanings the WebSocket protocol uses — so a client rebuilding a result
//! from frames cannot tell which transport produced them.
//!
//! # The execution is driven by a task, and the consumer holds a channel
//!
//! Everything that has to happen for a stream to be correct happens where it can
//! `await`: the wall-clock guard inside the execution is polled, the session lock
//! can be taken to settle live queries, and a transaction is finalised before the
//! driver returns. A consumer is a [`Receiver`] and owes nothing.
//!
//! The consumer still sets the pace. The channel it reads from holds one frame
//! and the executor's holds [`QUERY_STREAM_BUFFER`], so a reader that stops
//! reading stops the scan rather than filling a buffer nobody drains.
//!
//! Dropping the receiver is how a consumer walks away. The driver's next send
//! fails, which it treats as the cooperative stop — trip the cancel handle, close
//! the items channel — and then it finishes the execution and settles anyway,
//! because an abandoned stream still owns a transaction and may have created
//! subscriptions nothing can reach.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

use futures::Stream;
use surrealdb_core::channel::{Receiver, Sender, bounded};
use surrealdb_core::ctx::CancelHandle;
use surrealdb_core::dbs::{AuthPrincipalSnapshot, QueryStreamJob, Session};
use surrealdb_core::kvs::Datastore;
use surrealdb_core::rpc::{RpcProtocol, live_query_owner};
use surrealdb_rpc::capabilities::MethodTarget;
use surrealdb_rpc::error::method_not_allowed;
use surrealdb_rpc::framing::{live_queries_disowned, stream_stopped};
use surrealdb_rpc::{
	Method, QUERY_STREAM_BUFFER, QueryResult, QueryStreamFrame, QueryStreamItem, QueryType,
	StreamFrames,
};
use surrealdb_types::{Error, HashMap, SerializationError};
use tokio::sync::RwLock;
use tracing::error;
use uuid::Uuid;
use web_time::Instant;

use crate::{EmbeddedEngine, Format, Request, wire};

/// A streaming execution in flight, as [`QueryStreamJob::run`] hands it over.
///
/// [`QueryStreamJob::run`]: surrealdb_core::dbs::QueryStreamJob::run
type QueryStreamRun = Pin<Box<dyn Future<Output = Result<Vec<QueryResult>, Error>> + Send>>;

/// How many frames may wait for the consumer.
///
/// One, so the consumer's read speed is the producer's rate limit: the driver
/// blocks on its next send, stops draining the executor's items, and the executor
/// stops scanning.
const FRAME_BUFFER: usize = 1;

/// Runs the driver on the runtime the stream was opened on.
///
/// Off the browser that is tokio's: a multi-thread runtime natively, or the
/// current-thread runtime a WASI host drives from its event loop.
#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
fn spawn(task: impl Future<Output = ()> + Send + 'static) {
	tokio::spawn(task);
}

/// The browser counterpart, on the JavaScript microtask queue.
#[cfg(all(target_family = "wasm", target_os = "unknown"))]
fn spawn(task: impl Future<Output = ()> + 'static) {
	wasm_bindgen_futures::spawn_local(task);
}

impl EmbeddedEngine {
	/// Runs a query, answering with frames as the executor produces them.
	///
	/// A failure before execution begins — a denied capability, a parse error,
	/// an unknown session or transaction — is reported here and produces no
	/// frames at all, which is the answer [`Self::execute`] would have given the
	/// same request. Everything after that is carried on the frames: a statement
	/// that fails is reported on its own `Finished`, and a failure belonging to
	/// no single statement on the terminal `End`.
	///
	/// The request's `method` is not read. Calling this *is* the routing
	/// decision, and the envelope is shared with [`Self::execute`] so a shim
	/// decodes one shape either way.
	pub async fn query_stream(&self, request: Request) -> Result<QueryFrames, Error> {
		Ok(QueryFrames {
			frames: Box::pin(self.start_stream(request, None).await?),
		})
	}

	/// Runs an encoded query request, answering with encoded frames.
	///
	/// The framed counterpart to [`Self::query_stream`], as
	/// [`Self::execute_encoded`] is to [`Self::execute`] — and like it, a method
	/// error is encoded rather than returned. A failure before execution begins
	/// is the stream's only frame: a terminal `End` carrying it, reporting no
	/// results, which is what an errored `End` already means. Carrying it that
	/// way is what keeps it structured, since the shims can only throw a string.
	///
	/// The `Err` here is reserved for a request that could not be framed at all.
	pub async fn query_stream_encoded(
		&self,
		format: Format,
		request: &[u8],
	) -> anyhow::Result<EncodedQueryFrames> {
		let obj = wire::decode(format, request, self.recursion_limit())?.into_object()?;
		let request = Request::from_object(obj)?;
		let frames = match self.start_stream(request, Some(format)).await {
			Ok(frames) => frames,
			Err(error) => {
				// No stream opened, so the terminal frame is the whole answer. If
				// that frame cannot be encoded there is nothing to answer with, and
				// a stream of no frames at all would report the failure as an empty
				// result -- so it is returned as the framing failure it is.
				let (tx, rx) = bounded(FRAME_BUFFER);
				let end = QueryStreamFrame::End {
					results: 0,
					time: Duration::ZERO,
					error: Some(error),
				};
				let encoded = wire::encode(format, end.into_value())?;
				// The channel is new and holds one frame, so this cannot block.
				let _ = tx.try_send(encoded);
				rx
			}
		};
		Ok(EncodedQueryFrames {
			frames: Box::pin(frames),
		})
	}

	/// Start the execution and its driver, returning the channel the frames
	/// arrive on.
	///
	/// `format` decides what the channel carries: frames as values, or frames
	/// encoded for a shim that speaks bytes. Encoding happens in the driver
	/// because a value the format cannot carry has to fail the statement that
	/// produced it, and only the driver holds the framing state that can.
	async fn start_stream<T: FrameSink>(
		&self,
		request: Request,
		format: Option<Format>,
	) -> Result<Receiver<T>, Error> {
		Ok(self.start_stream_with(request, format, false).await?.0)
	}

	/// [`Self::start_stream`], with the driver either spawned or, when `inline`, handed back for
	/// the consumer to poll alongside the channel ([`FramedQueryFrames::next`] does): the query
	/// then runs only inside the consumer's polls, so whatever the consumer sets up around them
	/// (a host's pin of reads to one log position) reaches it.
	async fn start_stream_with<T: FrameSink>(
		&self,
		request: Request,
		format: Option<Format>,
		inline: bool,
	) -> Result<(Receiver<T>, Option<Pin<Box<dyn Future<Output = ()>>>>), Error> {
		// `query_stream` is its own capability target, and the execution below
		// re-applies the `query` gate inside `RpcProtocol::query_stream` — so
		// denying either method name denies streaming, exactly as it does on the
		// WebSocket transport.
		if !self.kvs.allows_rpc_method(&MethodTarget {
			method: Method::QueryStream,
		}) {
			return Err(method_not_allowed(Method::QueryStream.to_string()));
		}
		let session_id = request.session_id.map(Into::into).unwrap_or(self.id);
		let cancel = CancelHandle::new();
		let (items_tx, items) = bounded(QUERY_STREAM_BUFFER);
		let (job, principal) = RpcProtocol::query_stream(
			self,
			request.txn.map(Into::into),
			session_id,
			request.params,
			Some(cancel.clone()),
			items_tx,
		)
		.await?;
		Ok(self.spawn_driver(job, principal, session_id, cancel, items, format, inline))
	}

	/// Spawn the driver that runs `job`, sending what it reads from `items` as
	/// frames on the returned channel.
	#[allow(clippy::too_many_arguments)]
	fn spawn_driver<T: FrameSink>(
		&self,
		job: QueryStreamJob,
		principal: AuthPrincipalSnapshot,
		session_id: Uuid,
		cancel: CancelHandle,
		items: Receiver<QueryStreamItem>,
		format: Option<Format>,
		inline: bool,
	) -> Spawned<T> {
		let (frames_tx, frames_rx) = bounded(FRAME_BUFFER);
		let driver = Driver {
			frames: StreamFrames::new(),
			items,
			cancel,
			started: Instant::now(),
			format,
			out: frames_tx,
			muted: false,
			kvs: Arc::clone(&self.kvs),
			live_queries: Arc::clone(&self.live_queries),
			sessions: Arc::clone(&self.sessions),
			session_id,
			principal,
		};
		let statements = job.statement_count;
		// The timeout the execution carries only fires while the execution is
		// polled, and the driver polls it for as long as it runs -- including
		// after a consumer has gone, which is what bounds an abandoned stream.
		let drive = driver.drive(statements, job.run);
		if inline {
			let drive: Pin<Box<dyn Future<Output = ()>>> = Box::pin(drive);
			(frames_rx, Some(drive))
		} else {
			spawn(drive);
			(frames_rx, None)
		}
	}
}

/// What a frame channel carries.
///
/// Implemented for the two things a shim can want: the frame itself, or the frame
/// encoded. The driver is written once against this, so the sequence and the
/// rules that produce it cannot differ between them.
pub trait FrameSink: Sized + Send + 'static {
	/// Render a frame, or report why this one cannot be carried.
	fn render(frame: QueryStreamFrame, format: Option<Format>) -> Result<Self, Error>;
}

impl FrameSink for QueryStreamFrame {
	fn render(frame: QueryStreamFrame, _format: Option<Format>) -> Result<Self, Error> {
		Ok(frame)
	}
}

impl FrameSink for Vec<u8> {
	fn render(frame: QueryStreamFrame, format: Option<Format>) -> Result<Self, Error> {
		let format = format.unwrap_or(Format::Cbor);
		wire::encode(format, frame.into_value()).map_err(|error| {
			Error::serialization(error.to_string(), SerializationError::Serialization)
		})
	}
}

/// The frames answering one streaming query.
///
/// Yields [`QueryStreamFrame`]s rather than encoded bytes, so a shim that can
/// hand JavaScript a value directly does not pay for a round trip through a wire
/// format; one that wants bytes uses [`EncodedQueryFrames`].
///
/// Ends after exactly one terminal `End` frame. Dropping it early stops the
/// execution rather than orphaning it — see the module docs.
pub struct QueryFrames {
	/// Pinned on the heap because [`Receiver`] is not `Unpin`, which keeps this
	/// `Unpin` for the shims that drive it from a loop.
	frames: Pin<Box<Receiver<QueryStreamFrame>>>,
}

impl Stream for QueryFrames {
	type Item = QueryStreamFrame;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
		self.frames.as_mut().poll_next(cx)
	}
}

/// [`QueryFrames`] encoded for a shim that carries bytes.
///
/// A value the format cannot carry fails the statement that produced it, exactly
/// as that statement's own error would, and the stream continues — the same rule
/// the WebSocket transport applies. Skipping the frame instead would hand a
/// consumer a sequence with a hole in it, and ending the stream would truncate it
/// with no terminal frame at all.
pub struct EncodedQueryFrames {
	frames: Pin<Box<Receiver<Vec<u8>>>>,
}

impl Stream for EncodedQueryFrames {
	type Item = Vec<u8>;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
		self.frames.as_mut().poll_next(cx)
	}
}

/// The frames a spawned driver sends, and the driver itself when the caller is to run it.
type Spawned<T> = (Receiver<T>, Option<Pin<Box<dyn Future<Output = ()>>>>);

/// One stream's execution, and everything that turns it into frames.
///
/// Owned by a task of its own, which is what lets every rule that needs to
/// `await` be applied where it belongs.
struct Driver<T> {
	frames: StreamFrames,
	items: Receiver<QueryStreamItem>,
	cancel: CancelHandle,
	started: Instant,
	format: Option<Format>,
	out: Sender<T>,
	/// Set once frames can no longer reach the consumer. The execution is still
	/// driven to completion and its live queries still settled; the frames it
	/// produces are discarded.
	muted: bool,
	kvs: Arc<Datastore>,
	live_queries: Arc<HashMap<Uuid, Uuid>>,
	sessions: Arc<HashMap<Uuid, Arc<RwLock<Session>>>>,
	session_id: Uuid,
	/// The principal the execution runs as, and the one its `LIVE SELECT`s
	/// belong to. The session may be acting as someone else by the time they are
	/// registered.
	principal: AuthPrincipalSnapshot,
}

impl<T: FrameSink> Driver<T> {
	/// Drive the execution to completion, framing and sending what it produces.
	///
	/// Returns only once the execution has finished and its live queries are
	/// settled, on every path — including one where the consumer left long ago.
	async fn drive(mut self, statements: usize, run: QueryStreamRun) {
		self.send(QueryStreamFrame::Begin {
			statements,
		})
		.await;

		let mut run = Some(run);
		let mut outcome = None;
		let mut items_done = false;
		loop {
			while let Some(frame) = self.frames.pop() {
				self.send(frame).await;
			}
			// Whatever is still live is awaited, and never anything that is not.
			// A closed channel resolves immediately and forever, so selecting on one
			// is a spin that starves whatever shares the thread — on wasm, the event
			// loop; on a runtime with threads, a worker burning a core where nothing
			// looks.
			match (run.as_mut(), items_done) {
				// Both: the items channel is bounded, so draining it is what lets
				// the execution make progress, and polling the execution is what
				// puts anything in it. Awaiting either alone deadlocks.
				(Some(running), false) => {
					tokio::select! {
						biased;
						item = self.items.recv() => match item {
							Ok(item) => self.frames.absorb(item),
							// The execution dropped its sender, or the stream was
							// stopped and the channel closed under it.
							Err(_) => items_done = true,
						},
						result = running => {
							run = None;
							outcome = Some(result);
						}
					}
				}
				// Nothing more will be produced, so only the execution's own outcome
				// is left — and it is awaited, because it owns the transaction that
				// has to be finalised.
				(Some(running), true) => {
					outcome = Some(running.await);
					run = None;
				}
				// The execution finished; take what it left buffered.
				(None, false) => match self.items.recv().await {
					Ok(item) => self.frames.absorb(item),
					Err(_) => items_done = true,
				},
				(None, true) => break,
			}
		}

		let end = self.settle(outcome).await;
		self.send(end).await;
	}

	/// Send one frame, failing the statement whose value cannot be carried.
	///
	/// A muted stream drops the frame without rendering it and marks nothing
	/// delivered, so the terminal frame's `results` counts only what a consumer
	/// could actually have seen.
	async fn send(&mut self, frame: QueryStreamFrame) {
		if self.muted {
			return;
		}
		let terminal = match &frame {
			QueryStreamFrame::Finished {
				index,
				..
			} => Some(*index),
			QueryStreamFrame::Begin {
				..
			}
			| QueryStreamFrame::Rows {
				..
			}
			| QueryStreamFrame::Value {
				..
			}
			| QueryStreamFrame::End {
				..
			} => None,
		};
		let statement = match &frame {
			QueryStreamFrame::Rows {
				index,
				..
			}
			| QueryStreamFrame::Value {
				index,
				..
			} => Some(*index),
			QueryStreamFrame::Begin {
				..
			}
			| QueryStreamFrame::Finished {
				..
			}
			| QueryStreamFrame::End {
				..
			} => None,
		};
		let rendered = match T::render(frame, self.format) {
			Ok(rendered) => rendered,
			Err(error) => {
				// A value the wire cannot carry fails the statement that produced
				// it, provided that statement can still be failed. Its retraction
				// replaces whatever it had queued, and the loop sends that
				// instead. Once its terminal frame has gone the success cannot be
				// unsaid, so the stream itself is stopped.
				if let Some(index) = statement
					&& self.frames.retract(index, error)
				{
					return;
				}
				self.stop();
				return;
			}
		};
		if self.out.send(rendered).await.is_err() {
			// The consumer has gone. The execution still has to finish.
			self.stop();
			return;
		}
		if let Some(index) = terminal {
			self.frames.mark_delivered(index);
		}
	}

	/// Stop the execution cooperatively and discard what it still produces.
	///
	/// The pair is required: the flag alone cannot unpark an execution parked on
	/// a full channel, and closing the channel alone would let a statement in a
	/// non-emitting phase run to completion.
	fn stop(&mut self) {
		self.cancel.trip();
		self.items.close();
		self.muted = true;
	}

	/// Register the live queries the consumer holds, delete the rest, and report
	/// the outcome as a terminal frame.
	async fn settle(
		&mut self,
		outcome: Option<Result<Vec<QueryResult>, Error>>,
	) -> QueryStreamFrame {
		// The execution's own results are the authority on what it created: the
		// frames only learned of a live query when its `Finished` item was
		// absorbed, and a stopped stream may never have absorbed it.
		let mut produced: Vec<(Option<usize>, Uuid)> =
			self.frames.live_queries().iter().map(|(index, id)| (Some(*index), *id)).collect();
		if let Some(Ok(results)) = &outcome {
			for id in live_query_ids(results) {
				if !produced.iter().any(|(_, known)| *known == id) {
					produced.push((None, id));
				}
			}
		}

		let mut disowned = Vec::new();
		let mut doomed = Vec::new();
		if !produced.is_empty() {
			// The guard is held across every registration, and was taken under the
			// principal the execution ran as. Both matter: a session that merely
			// still exists may have been invalidated, whose `cleanup_lqs` swept a
			// map these ids were not in yet, and releasing the guard between the
			// check and the insert would let the next teardown through the gap.
			let owner = live_query_owner(&self.sessions, self.session_id, &self.principal).await;
			for (index, id) in &produced {
				// An id the consumer never learned is reachable by nothing.
				let told = index.is_some_and(|index| self.frames.was_delivered(index));
				match (&owner, told) {
					(Some(_), true) => {
						self.live_queries.insert(*id, self.session_id);
					}
					_ => {
						if told {
							disowned.push(*id);
						}
						doomed.push(*id);
					}
				}
			}
			// Released before the deletions: nothing below registers anything, and
			// holding a session guard through a datastore write would block the
			// engine's other requests for no gain.
			drop(owner);
			if !doomed.is_empty()
				&& let Err(err) = self.kvs.delete_queries(doomed).await
			{
				error!("Error cleaning up the live queries of a streaming query: {err}");
			}
		}

		// A failure belonging to no single statement outranks the rest: it
		// retracts every statement that never finished. A disowned id outranks the
		// generic stop, because it names a subscription the consumer holds and has
		// to drop. The executor reports an abandoned execution as a success, so
		// the stop is what makes the answer incomplete.
		let error = match outcome {
			Some(Err(error)) => Some(error),
			_ => (!disowned.is_empty())
				.then(|| live_queries_disowned(&disowned))
				.or_else(|| self.cancel.is_cancelled().then(stream_stopped)),
		};
		QueryStreamFrame::End {
			results: self.frames.delivered_count(),
			time: self.started.elapsed(),
			error,
		}
	}
}

/// The live queries a finished execution reports having created.
///
/// A registration inside a transaction block is not real until the block commits,
/// so this is what the executor says survived.
fn live_query_ids(results: &[QueryResult]) -> Vec<Uuid> {
	results
		.iter()
		.filter(|result| matches!(result.query_type, QueryType::Live))
		.filter_map(|result| match &result.result {
			Ok(surrealdb_types::Value::Uuid(id)) => Some(id.into_inner()),
			_ => None,
		})
		.collect()
}

#[cfg(all(test, feature = "kv-mem"))]
mod tests {
	use std::time::Duration;

	use futures::StreamExt;
	use surrealdb_core::kvs::QueryRequest;
	use surrealdb_types::{Array, Object, Value};

	use super::*;
	use crate::Options;

	async fn engine() -> EmbeddedEngine {
		EmbeddedEngine::connect("memory", Options::default()).await.expect("connect")
	}

	/// A buffered `query` request, for the setup a streaming test needs first.
	fn query(sql: &str) -> Request {
		let mut obj = Object::default();
		obj.insert("method".to_owned(), Value::String("query".to_owned()));
		obj.insert(
			"params".to_owned(),
			Value::Array(Array::from(vec![Value::String(sql.to_owned())])),
		);
		Request::from_object(obj).expect("a query request should parse")
	}

	fn request(sql: &str) -> Request {
		let mut obj = Object::default();
		obj.insert("method".to_owned(), Value::String("query_stream".to_owned()));
		obj.insert(
			"params".to_owned(),
			Value::Array(Array::from(vec![Value::String(sql.to_owned())])),
		);
		Request::from_object(obj).expect("a query_stream request should parse")
	}

	// Pins the one-frame buffer, which
	// `a_live_query_is_not_registered_under_a_changed_principal` depends on for
	// its ordering: a consumer that stops reading has to leave the driver parked
	// partway through a statement, before it can settle. A larger buffer would let
	// the driver run to settlement first, turning that test from an ordering
	// assertion into a race that nothing about the frames themselves reveals.
	const _: () = assert!(FRAME_BUFFER == 1);

	async fn frames(engine: &EmbeddedEngine, sql: &str) -> Vec<QueryStreamFrame> {
		engine.query_stream(request(sql)).await.expect("the stream should open").collect().await
	}

	/// Frames of a stream whose explicit transactions are held, driven by the
	/// same driver a `query_stream` request gets. The knob has no request
	/// surface, so the job is built from the datastore directly.
	async fn held_frames(
		engine: &EmbeddedEngine,
		session: &Session,
		sql: &str,
	) -> Vec<QueryStreamFrame> {
		let (items_tx, items) = bounded(QUERY_STREAM_BUFFER);
		let cancel = CancelHandle::new();
		let job = engine
			.kvs
			.run_streaming(
				QueryRequest::new(sql, session)
					.with_cancel(cancel.clone())
					.with_held_transactions(),
				items_tx,
			)
			.expect("the query parses");
		let principal = AuthPrincipalSnapshot::capture(session);
		let (frames, driver) = engine.spawn_driver::<QueryStreamFrame>(
			job, principal, engine.id, cancel, items, None, false,
		);
		assert!(driver.is_none(), "a spawned driver is not handed back");
		frames.collect().await
	}

	/// A database with two `thing` records, and the session that reads it.
	async fn seeded() -> (EmbeddedEngine, Session) {
		let engine = engine().await;
		for sql in [
			"DEFINE NAMESPACE n",
			"USE NS n; DEFINE DATABASE d;",
			"USE NS n DB d; CREATE thing:1; CREATE thing:2;",
		] {
			engine.execute(query(sql)).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
		}
		(engine, Session::owner().with_ns("n").with_db("d"))
	}

	fn finished_error(frames: &[QueryStreamFrame], index: usize) -> Option<bool> {
		frames.iter().find_map(|frame| match frame {
			QueryStreamFrame::Finished {
				index: i,
				error,
				..
			} if *i == index => Some(error.is_some()),
			QueryStreamFrame::Begin {
				..
			}
			| QueryStreamFrame::Rows {
				..
			}
			| QueryStreamFrame::Value {
				..
			}
			| QueryStreamFrame::Finished {
				..
			}
			| QueryStreamFrame::End {
				..
			} => None,
		})
	}

	/// A held block that fails sends the failure alone, and one that commits
	/// sends its rows whole: the consumer is never given rows to take back.
	#[tokio::test]
	async fn a_held_block_emits_no_retraction() {
		let (engine, session) = seeded().await;

		let frames =
			held_frames(&engine, &session, "BEGIN; SELECT * FROM thing; THROW 'nope'; COMMIT;")
				.await;
		assert!(
			!frames.iter().any(|f| matches!(
				f,
				QueryStreamFrame::Rows {
					index: 1,
					..
				}
			)),
			"a failed held block sends no rows: {frames:?}"
		);
		assert_eq!(finished_error(&frames, 1), Some(true), "the SELECT is reported failed");
		assert!(
			matches!(
				frames.last(),
				Some(QueryStreamFrame::End {
					error: None,
					..
				})
			),
			"the stream itself ends cleanly: {frames:?}"
		);

		let frames = held_frames(&engine, &session, "BEGIN; SELECT * FROM thing; COMMIT;").await;
		let rows: usize = frames
			.iter()
			.filter_map(|f| match f {
				QueryStreamFrame::Rows {
					index: 1,
					values,
					..
				} => Some(values.len()),
				QueryStreamFrame::Begin {
					..
				}
				| QueryStreamFrame::Rows {
					..
				}
				| QueryStreamFrame::Value {
					..
				}
				| QueryStreamFrame::Finished {
					..
				}
				| QueryStreamFrame::End {
					..
				} => None,
			})
			.sum();
		assert_eq!(rows, 2, "a committed held block sends its rows: {frames:?}");
		assert_eq!(finished_error(&frames, 1), Some(false), "and finishes them cleanly");
	}

	/// The same failing block, streamed provisionally, is what a held one
	/// spares the consumer: rows first, then the failure that voids them.
	#[tokio::test]
	async fn a_streamed_block_retracts_where_a_held_one_does_not() {
		let engine = engine().await;
		for sql in [
			"DEFINE NAMESPACE n",
			"USE NS n; DEFINE DATABASE d;",
			// More rows than the first frame carries, so some of them have
			// reached the consumer by the time the block fails.
			"USE NS n DB d; FOR $i IN array::range(0, 100) { CREATE type::record('thing', $i) };",
		] {
			engine.execute(query(sql)).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
		}
		// The `use` method, not a `USE` statement: the stream needs the session
		// itself pointed at the database.
		let mut select = Object::default();
		select.insert("method".to_owned(), Value::String("use".to_owned()));
		select.insert(
			"params".to_owned(),
			Value::Array(Array::from(vec![
				Value::String("n".to_owned()),
				Value::String("d".to_owned()),
			])),
		);
		engine
			.execute(Request::from_object(select).expect("a use request should parse"))
			.await
			.expect("use");

		let frames = frames(&engine, "BEGIN; SELECT * FROM thing; THROW 'nope'; COMMIT;").await;
		let rows = frames
			.iter()
			.position(|f| {
				matches!(
					f,
					QueryStreamFrame::Rows {
						index: 1,
						..
					}
				)
			})
			.expect("a streamed block sends rows before it resolves");
		let finished = frames
			.iter()
			.position(|f| {
				matches!(
					f,
					QueryStreamFrame::Finished {
						index: 1,
						error: Some(_),
						..
					}
				)
			})
			.expect("the SELECT is finished with an error");
		assert!(rows < finished, "the rows precede the failure that retracts them");
	}

	/// The frames answer the whole query: one `Begin`, each statement's rows
	/// followed by its own `Finished`, and exactly one terminal `End`.
	#[tokio::test]
	async fn a_query_is_answered_by_a_framed_sequence() {
		let engine = engine().await;
		let frames = frames(&engine, "RETURN [1, 2, 3]; RETURN 'done';").await;

		assert!(
			matches!(
				frames.first(),
				Some(QueryStreamFrame::Begin {
					statements: 2
				})
			),
			"the stream opens by announcing the statement count: {frames:?}",
		);
		assert!(
			matches!(
				frames.last(),
				Some(QueryStreamFrame::End {
					results: 2,
					error: None,
					..
				})
			),
			"the stream ends once, reporting what the consumer holds: {frames:?}",
		);
		let ends = frames.iter().filter(|f| matches!(f, QueryStreamFrame::End { .. })).count();
		assert_eq!(ends, 1, "exactly one frame is terminal");

		// Statement 0 is a list, so it streams as rows; statement 1 is a bare
		// value, so it arrives whole and finishes `single`.
		let rows: Vec<&Value> = frames
			.iter()
			.filter_map(|f| match f {
				QueryStreamFrame::Rows {
					index: 0,
					values,
					..
				} => Some(values),
				QueryStreamFrame::Begin {
					..
				}
				| QueryStreamFrame::Rows {
					..
				}
				| QueryStreamFrame::Value {
					..
				}
				| QueryStreamFrame::Finished {
					..
				}
				| QueryStreamFrame::End {
					..
				} => None,
			})
			.flatten()
			.collect();
		assert_eq!(rows.len(), 3, "every row of the first statement arrives: {frames:?}");
		assert!(frames.iter().any(|f| matches!(
			f,
			QueryStreamFrame::Finished {
				index: 1,
				single: true,
				error: None,
				..
			}
		)));
	}

	/// A statement that fails is reported on its own terminal frame, and the
	/// statements around it are unaffected — the stream itself still completes.
	#[tokio::test]
	async fn a_failed_statement_fails_only_itself() {
		let engine = engine().await;
		let frames = frames(&engine, "RETURN 1; THROW 'boom';").await;

		assert!(
			frames.iter().any(|f| matches!(
				f,
				QueryStreamFrame::Finished {
					index: 0,
					error: None,
					..
				}
			)),
			"the statement before the failure stands: {frames:?}",
		);
		assert!(
			frames.iter().any(|f| matches!(
				f,
				QueryStreamFrame::Finished {
					index: 1,
					error: Some(_),
					..
				}
			)),
			"the failing statement is failed by its own terminal frame: {frames:?}",
		);
		assert!(
			matches!(
				frames.last(),
				Some(QueryStreamFrame::End {
					error: None,
					..
				})
			),
			"a statement failing is not the stream failing: {frames:?}",
		);
	}

	/// A query that never parses has no statement to attribute the failure to,
	/// so the typed caller is told directly rather than through a stream.
	#[tokio::test]
	async fn a_parse_error_is_reported_before_any_frame() {
		let engine = engine().await;
		let opened = engine.query_stream(request("RETURN 1; SELECT * FROM;")).await;
		assert!(opened.is_err(), "a query that cannot parse never opens a stream");
	}

	/// The encoded form has no channel but the frames, so the same failure
	/// arrives as a lone terminal frame carrying it — structured, where a shim
	/// throwing across the FFI could only carry a string.
	#[tokio::test]
	async fn an_encoded_parse_error_arrives_as_the_terminal_frame() {
		let engine = engine().await;
		let mut obj = Object::default();
		obj.insert("method".to_owned(), Value::String("query_stream".to_owned()));
		obj.insert(
			"params".to_owned(),
			Value::Array(Array::from(vec![Value::String("SELECT * FROM;".to_owned())])),
		);
		let encoded = wire::encode(Format::Cbor, Value::Object(obj)).expect("encode the request");

		let frames: Vec<QueryStreamFrame> = engine
			.query_stream_encoded(Format::Cbor, &encoded)
			.await
			.expect("a method error is carried, not returned")
			.map(|bytes| {
				let value = wire::decode(Format::Cbor, &bytes, 32).expect("decode a frame");
				QueryStreamFrame::from_value(value).expect("parse a frame")
			})
			.collect()
			.await;

		assert_eq!(frames.len(), 1, "nothing opened, so only the terminal frame: {frames:?}");
		let QueryStreamFrame::End {
			results,
			error,
			..
		} = &frames[0]
		else {
			panic!("expected a terminal frame, got {frames:?}");
		};
		assert_eq!(*results, 0, "no statement was answered");
		assert!(
			error.as_ref().is_some_and(|e| e.message().contains("Parse error")),
			"the parse error survives whole: {error:?}",
		);
	}

	/// The point of streaming: a statement's results reach the consumer while
	/// the rest of the query is still running.
	///
	/// The first statement's rows must arrive without waiting for the `SLEEP`
	/// that follows them, so the gap between the first frame and the last is
	/// the whole of that sleep. Asserting the *ratio* rather than an absolute
	/// budget keeps this honest on a loaded machine.
	#[tokio::test]
	async fn rows_arrive_before_the_query_finishes() {
		let engine = engine().await;
		let started = Instant::now();
		let mut stream = engine
			.query_stream(request("RETURN [1, 2, 3]; SLEEP 2s;"))
			.await
			.expect("the stream should open");

		let mut first_rows = None;
		let mut frames = Vec::new();
		while let Some(frame) = stream.next().await {
			if first_rows.is_none() && matches!(frame, QueryStreamFrame::Rows { .. }) {
				first_rows = Some(started.elapsed());
			}
			frames.push(frame);
		}
		let total = started.elapsed();
		let first_rows = first_rows.expect("the first statement produces rows");

		assert!(
			total >= Duration::from_secs(2),
			"the whole query really did take the sleep: {total:?}",
		);
		assert!(
			first_rows < Duration::from_secs(1),
			"the first rows waited for the sleep ({first_rows:?} of {total:?}), \
			 so nothing is being streamed",
		);
	}

	/// An abandoned stream does not wedge the engine behind itself.
	///
	/// The budget is deliberately shorter than the abandoned query's own sleep,
	/// which is what gives the assertion teeth: an engine that served the next
	/// request only after the abandoned execution drained would miss it. That
	/// the execution is also *stopped* rather than left to run is not observable
	/// from here — the driver is detached, and this runtime discards detached
	/// tasks when the test ends — so it is
	/// [`a_timeout_stops_a_stream_nobody_is_reading`] that covers the bound.
	#[tokio::test]
	async fn an_abandoned_stream_does_not_wedge_the_engine() {
		let engine = engine().await;
		let mut stream = engine
			.query_stream(request("CREATE ONLY t:1 SET n = 1; SLEEP 5s;"))
			.await
			.expect("the stream should open");

		// Take one frame and walk away while the sleep is still running.
		let first = stream.next().await.expect("a first frame");
		assert!(matches!(first, QueryStreamFrame::Begin { .. }));
		drop(stream);

		let answered = tokio::time::timeout(
			Duration::from_secs(2),
			engine.execute(
				Request::from_object({
					let mut obj = Object::default();
					obj.insert("method".to_owned(), Value::String("query".to_owned()));
					obj.insert(
						"params".to_owned(),
						Value::Array(Array::from(vec![Value::String("RETURN 1".to_owned())])),
					);
					obj
				})
				.expect("a query request should parse"),
			),
		)
		.await;
		assert!(answered.is_ok(), "the engine still answers after a stream was abandoned");
	}

	/// A `LIVE SELECT` in a stream is not registered if the session stopped
	/// acting as the principal that created it.
	///
	/// The ordering is forced by backpressure rather than by timing. The frame
	/// channel holds [`FRAME_BUFFER`] frames, so once the consumer stops reading
	/// after the opening frame the driver parks partway through the statement's
	/// frames and cannot reach settlement. Invalidating there puts the teardown
	/// strictly before the registration — `cleanup_lqs` sweeps a map the id is not
	/// in yet — which is the ordering the race produces, with no sleeping to make
	/// it likely. The id must not land behind it.
	#[tokio::test]
	async fn a_live_query_is_not_registered_under_a_changed_principal() {
		let engine = engine().await;
		for sql in [
			"DEFINE USER tester ON ROOT PASSWORD 'secret' ROLES OWNER",
			"DEFINE NAMESPACE n",
			"USE NS n; DEFINE DATABASE d;",
			"USE NS n DB d; DEFINE TABLE t;",
		] {
			engine.execute(query(sql)).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
		}
		// Signing in is what gives the session a principal to change away from.
		let mut signin = Object::default();
		signin.insert("method".to_owned(), Value::String("signin".to_owned()));
		let mut credentials = Object::default();
		credentials.insert("user".to_owned(), Value::String("tester".to_owned()));
		credentials.insert("pass".to_owned(), Value::String("secret".to_owned()));
		signin.insert(
			"params".to_owned(),
			Value::Array(Array::from(vec![Value::Object(credentials)])),
		);
		engine
			.execute(Request::from_object(signin).expect("a signin request should parse"))
			.await
			.expect("signin");
		// The `use` method, not a `USE` statement: the stream needs the session
		// itself pointed at the database, not one query's copy of it.
		let mut select = Object::default();
		select.insert("method".to_owned(), Value::String("use".to_owned()));
		select.insert(
			"params".to_owned(),
			Value::Array(Array::from(vec![
				Value::String("n".to_owned()),
				Value::String("d".to_owned()),
			])),
		);
		engine
			.execute(Request::from_object(select).expect("a use request should parse"))
			.await
			.expect("use");

		let mut stream = engine
			.query_stream(request("LIVE SELECT * FROM t"))
			.await
			.expect("the stream should open");
		let opened = stream.next().await.expect("an opening frame");
		assert!(matches!(opened, QueryStreamFrame::Begin { .. }));

		// The driver is parked on a frame nobody is reading, so it has not settled
		// and this teardown lands first.
		let mut invalidate = Object::default();
		invalidate.insert("method".to_owned(), Value::String("invalidate".to_owned()));
		engine
			.execute(Request::from_object(invalidate).expect("an invalidate request should parse"))
			.await
			.expect("invalidate");

		let frames: Vec<QueryStreamFrame> = stream.collect().await;
		let live = frames
			.iter()
			.find_map(|frame| match frame {
				QueryStreamFrame::Value {
					value: Value::Uuid(id),
					..
				} => Some(id.into_inner()),
				QueryStreamFrame::Begin {
					..
				}
				| QueryStreamFrame::Rows {
					..
				}
				| QueryStreamFrame::Value {
					..
				}
				| QueryStreamFrame::Finished {
					..
				}
				| QueryStreamFrame::End {
					..
				} => None,
			})
			.expect("the statement still produced a live query id");

		assert!(
			engine.live_queries.get(&live).is_none(),
			"a live query created under a principal the session no longer has must not be \
			 registered: {frames:?}",
		);
		let QueryStreamFrame::End {
			error,
			..
		} = frames.last().expect("a terminal frame")
		else {
			panic!("expected a terminal frame, got {frames:?}");
		};
		assert!(
			error.as_ref().is_some_and(|e| e.message().contains(&live.to_string())),
			"the consumer holds this id and has to be told it is dead: {error:?}",
		);
	}

	/// A configured query timeout bounds a stream whose consumer stops reading
	/// without dropping it.
	///
	/// The guard lives inside the execution and fires only while the execution is
	/// polled — which the driver does for as long as it runs, whether or not
	/// anyone is reading. So the timeout is reported the way the buffered path
	/// reports it, as each statement's own failure, rather than as a stream that
	/// stopped for reasons unexplained.
	#[tokio::test]
	async fn a_timeout_stops_a_stream_nobody_is_reading() {
		let engine = EmbeddedEngine::connect(
			"memory",
			Options {
				query_timeout: Some(1),
				..Default::default()
			},
		)
		.await
		.expect("connect");

		let mut stream = engine
			.query_stream(request("SLEEP 30s; RETURN 1;"))
			.await
			.expect("the stream should open");
		assert!(matches!(stream.next().await, Some(QueryStreamFrame::Begin { .. })));

		// Hold the stream and read nothing until well past the timeout, then
		// resume: the watchdog must already have stopped the execution, so this
		// completes rather than waiting out the sleep.
		tokio::time::sleep(Duration::from_secs(2)).await;
		let frames: Vec<QueryStreamFrame> =
			tokio::time::timeout(Duration::from_secs(10), async { stream.collect().await })
				.await
				.expect("the stopped stream terminates rather than running its sleep out");

		// Reported, and reported precisely: a statement that was cut short says so
		// itself, so a consumer knows which one and why.
		let timed_out = frames.iter().any(|frame| match frame {
			QueryStreamFrame::Finished {
				error: Some(error),
				..
			}
			| QueryStreamFrame::End {
				error: Some(error),
				..
			} => error.message().contains("timeout"),
			QueryStreamFrame::Begin {
				..
			}
			| QueryStreamFrame::Rows {
				..
			}
			| QueryStreamFrame::Value {
				..
			}
			| QueryStreamFrame::Finished {
				..
			}
			| QueryStreamFrame::End {
				..
			} => false,
		});
		assert!(timed_out, "the timeout is reported rather than silently truncating: {frames:?}");
		assert!(
			matches!(frames.last(), Some(QueryStreamFrame::End { .. })),
			"the stream still terminates: {frames:?}",
		);
	}

	/// A value the wire format cannot carry fails its own statement, and the
	/// stream still completes.
	///
	/// CBOR has no representation for a regex, and `RETURN /abc/` is a perfectly
	/// good query — so a shim that speaks CBOR has to be able to report that
	/// statement as failed rather than hand JavaScript a sequence with a hole in
	/// it, or one that stops without a terminal frame.
	#[tokio::test]
	async fn an_unencodable_value_fails_its_statement_not_the_sequence() {
		let engine = engine().await;
		let mut obj = Object::default();
		obj.insert("method".to_owned(), Value::String("query_stream".to_owned()));
		obj.insert(
			"params".to_owned(),
			Value::Array(Array::from(vec![Value::String("RETURN 1; RETURN /abc/;".to_owned())])),
		);
		let encoded = wire::encode(Format::Cbor, Value::Object(obj)).expect("encode the request");

		let frames: Vec<QueryStreamFrame> = engine
			.query_stream_encoded(Format::Cbor, &encoded)
			.await
			.expect("the stream should open")
			.map(|bytes| {
				let value = wire::decode(Format::Cbor, &bytes, 32).expect("decode a frame");
				QueryStreamFrame::from_value(value).expect("parse a frame")
			})
			.collect()
			.await;

		// The statement that could be carried was.
		assert!(
			frames.iter().any(|f| matches!(
				f,
				QueryStreamFrame::Finished {
					index: 0,
					error: None,
					..
				}
			)),
			"the encodable statement is unaffected: {frames:?}",
		);
		// The one that could not is failed, not skipped.
		assert!(
			frames.iter().any(|f| matches!(
				f,
				QueryStreamFrame::Finished {
					index: 1,
					error: Some(_),
					..
				}
			)),
			"the unencodable statement is reported as failed: {frames:?}",
		);
		// And the sequence still terminates, which is what a consumer waits for.
		assert!(
			matches!(frames.last(), Some(QueryStreamFrame::End { .. })),
			"the stream still ends: {frames:?}",
		);
	}

	/// The encoded form carries the same sequence, so a shim that speaks bytes
	/// sees exactly what the typed one does.
	#[tokio::test]
	async fn encoded_frames_round_trip() {
		let engine = engine().await;
		let mut obj = Object::default();
		obj.insert("method".to_owned(), Value::String("query_stream".to_owned()));
		obj.insert(
			"params".to_owned(),
			Value::Array(Array::from(vec![Value::String("RETURN [1, 2]".to_owned())])),
		);
		let encoded = wire::encode(Format::Cbor, Value::Object(obj)).expect("encode the request");

		let frames: Vec<QueryStreamFrame> = engine
			.query_stream_encoded(Format::Cbor, &encoded)
			.await
			.expect("the stream should open")
			.map(|bytes| {
				let value = wire::decode(Format::Cbor, &bytes, 32).expect("decode a frame");
				QueryStreamFrame::from_value(value).expect("parse a frame")
			})
			.collect()
			.await;

		assert!(matches!(
			frames.first(),
			Some(QueryStreamFrame::Begin {
				statements: 1
			})
		));
		assert!(matches!(
			frames.last(),
			Some(QueryStreamFrame::End {
				results: 1,
				error: None,
				..
			})
		));
	}
}

impl EmbeddedEngine {
	/// Runs an encoded `query_stream` request, answering with what a WebSocket
	/// server sends: each frame wrapped in a response envelope carrying the
	/// request's `id` and session, encoded.
	///
	/// No envelope is larger than `max_bytes` when the rows in it allow: a rows
	/// frame whose encoding exceeds it is split into rows frames for the same
	/// statement, which a client concatenates exactly as it would the original.
	/// A single value too large on its own is sent as it is.
	///
	/// A request that cannot be framed at all is answered with one error
	/// envelope and no `id`, as [`Self::execute_framed`] does.
	pub async fn query_stream_framed(
		&self,
		format: Format,
		request: &[u8],
		max_bytes: usize,
	) -> anyhow::Result<FramedQueryFrames> {
		self.query_stream_framed_with(format, request, max_bytes, false).await
	}

	/// [`Self::query_stream_framed`] with the query run inside the calls to
	/// [`FramedQueryFrames::next`] rather than on a task of its own (see
	/// [`Self::start_stream_with`]).
	pub async fn query_stream_framed_inline(
		&self,
		format: Format,
		request: &[u8],
		max_bytes: usize,
	) -> anyhow::Result<FramedQueryFrames> {
		self.query_stream_framed_with(format, request, max_bytes, true).await
	}

	async fn query_stream_framed_with(
		&self,
		format: Format,
		request: &[u8],
		max_bytes: usize,
		inline: bool,
	) -> anyhow::Result<FramedQueryFrames> {
		let request = wire::decode(format, request, self.recursion_limit())
			.and_then(|v| Ok(v.into_object()?))
			.and_then(|obj| Ok(Request::from_object(obj)?));
		let request = match request {
			Ok(request) => request,
			Err(err) => {
				let response =
					crate::DbResponse::failure(None, None, Error::internal(format!("{err:#}")));
				let encoded =
					wire::encode(format, surrealdb_types::SurrealValue::into_value(response))?;
				return Ok(FramedQueryFrames::ready(vec![encoded]));
			}
		};
		let id = request.id.clone();
		let session = request.session_id.map(Into::into);
		let (frames, driver): (Pin<Box<dyn Stream<Item = QueryStreamFrame>>>, _) =
			match self.start_stream_with::<QueryStreamFrame>(request, None, inline).await {
				Ok((rx, driver)) => (Box::pin(rx), driver),
				Err(error) => (
					Box::pin(futures::stream::iter([QueryStreamFrame::End {
						results: 0,
						time: Duration::ZERO,
						error: Some(error),
					}])),
					None,
				),
			};
		Ok(FramedQueryFrames {
			frames,
			driver,
			pending: std::collections::VecDeque::new(),
			last_kind: KIND_OTHER,
			held: None,
			row_bytes: None,
			ended: false,
			id,
			session,
			format,
			max_bytes,
		})
	}
}

/// [`EmbeddedEngine::query_stream_framed`]'s envelopes.
pub struct FramedQueryFrames {
	frames: Pin<Box<dyn Stream<Item = QueryStreamFrame>>>,
	/// The query's driver when it runs inline (see
	/// [`EmbeddedEngine::query_stream_framed_inline`]): polled with every read of `frames`, and
	/// dropped once it has finished.
	driver: Option<Pin<Box<dyn Future<Output = ()>>>>,
	/// Envelopes already encoded and waiting, from a frame that was split.
	/// Encoded envelopes waiting, each with its frame's kind (see
	/// [`FramedQueryFrames::last_kind`]).
	pending: std::collections::VecDeque<(u8, Vec<u8>)>,
	/// The kind of the envelope `next` returned last.
	last_kind: u8,
	/// A frame taken while coalescing that belongs in the next envelope.
	held: Option<QueryStreamFrame>,
	/// Encoded bytes per row, the largest average seen (see `push`).
	row_bytes: Option<usize>,
	/// The frame stream has ended (seen while coalescing).
	ended: bool,
	id: Option<surrealdb_types::Value>,
	session: Option<Uuid>,
	format: Format,
	max_bytes: usize,
}

impl FramedQueryFrames {
	fn ready(envelopes: Vec<Vec<u8>>) -> Self {
		Self {
			frames: Box::pin(futures::stream::empty()),
			driver: None,
			pending: envelopes.into_iter().map(|e| (KIND_OTHER, e)).collect(),
			last_kind: KIND_OTHER,
			held: None,
			row_bytes: None,
			ended: false,
			id: None,
			session: None,
			format: Format::Json,
			max_bytes: usize::MAX,
		}
	}

	fn envelope(&self, frame: QueryStreamFrame) -> anyhow::Result<Vec<u8>> {
		let response = crate::DbResponse::new(
			self.id.clone(),
			self.session,
			Ok(crate::DbResult::Other(frame.into_value())),
		);
		wire::encode(self.format, surrealdb_types::SurrealValue::into_value(response))
	}

	/// Encode `frame` onto the back of `pending`, splitting a rows frame so each
	/// envelope fits `max_bytes`.
	///
	/// Rows are encoded by move: the split is planned from the encoded bytes
	/// per row seen so far, so no row is copied. Only a frame with no estimate
	/// yet, or one predicted near the limit, is encoded from a copy, which is
	/// what lets an envelope that came out too large be split in halves after
	/// all.
	fn push(&mut self, frame: QueryStreamFrame) -> anyhow::Result<()> {
		let (index, mut values, mut trace) = match frame {
			QueryStreamFrame::Rows {
				index,
				values,
				trace,
			} if values.len() > 1 => (index, values, trace),
			frame @ QueryStreamFrame::Begin {
				..
			}
			| frame @ QueryStreamFrame::Rows {
				..
			}
			| frame @ QueryStreamFrame::Value {
				..
			}
			| frame @ QueryStreamFrame::Finished {
				..
			}
			| frame @ QueryStreamFrame::End {
				..
			} => {
				let kind = kind_of(&frame);
				let encoded = self.envelope(frame)?;
				self.pending.push_back((kind, encoded));
				return Ok(());
			}
		};
		match self.row_bytes {
			Some(per_row) => {
				// Rows per envelope at the estimate, with room for rows larger
				// than the average so far.
				let fit = (self.max_bytes / 2 / per_row.max(1)).max(1);
				// The trace rides on the first envelope of the split.
				while values.len() > fit {
					let rest = values.split_off(fit);
					self.push_rows(index, values, trace.take())?;
					values = rest;
				}
				self.push_rows(index, values, trace)
			}
			None => self.push_checked(index, values, trace),
		}
	}

	/// Encode a rows frame by move, falling back to a checked encode when the
	/// estimate says it may not fit.
	fn push_rows(
		&mut self,
		index: usize,
		values: Vec<surrealdb_types::Value>,
		trace: Option<Box<surrealdb_rpc::Trace>>,
	) -> anyhow::Result<()> {
		let n = values.len();
		let predicted = self.row_bytes.unwrap_or(usize::MAX).saturating_mul(n);
		if n > 1 && predicted > self.max_bytes / 2 {
			return self.push_checked(index, values, trace);
		}
		let encoded = self.envelope(QueryStreamFrame::Rows {
			index,
			values,
			trace,
		})?;
		self.learn(encoded.len(), n);
		self.pending.push_back((KIND_ROWS, encoded));
		Ok(())
	}

	/// Encode a rows frame from a copy, splitting it in halves until each
	/// envelope fits. The trace stays with the first half.
	fn push_checked(
		&mut self,
		index: usize,
		mut values: Vec<surrealdb_types::Value>,
		trace: Option<Box<surrealdb_rpc::Trace>>,
	) -> anyhow::Result<()> {
		let n = values.len();
		let encoded = self.envelope(QueryStreamFrame::Rows {
			index,
			values: values.clone(),
			trace: trace.clone(),
		})?;
		self.learn(encoded.len(), n);
		if encoded.len() <= self.max_bytes || n <= 1 {
			self.pending.push_back((KIND_ROWS, encoded));
			return Ok(());
		}
		let tail = values.split_off(n / 2);
		self.push_checked(index, values, trace)?;
		self.push_checked(index, tail, None)
	}

	/// Fold an envelope's size into the bytes-per-row estimate (the largest
	/// average seen, so the estimate errs towards smaller envelopes).
	fn learn(&mut self, bytes: usize, rows: usize) {
		let per_row = bytes.div_ceil(rows.max(1));
		self.row_bytes = Some(self.row_bytes.map_or(per_row, |r| r.max(per_row)));
	}

	/// The next envelope, or `None` once the terminal frame has been sent.
	///
	/// Rows frames of one statement that are already produced are coalesced
	/// into one envelope of up to [`COALESCE_BYTES`]: fewer, larger messages,
	/// with nothing waited for, since only frames ready now are taken.
	pub async fn next(&mut self) -> anyhow::Result<Option<Vec<u8>>> {
		use futures::FutureExt;
		loop {
			if let Some((kind, envelope)) = self.pending.pop_front() {
				self.last_kind = kind;
				return Ok(Some(envelope));
			}
			let mut frame = match self.held.take() {
				Some(frame) => frame,
				None if self.ended => return Ok(None),
				None => match self.next_frame().await {
					Some(frame) => frame,
					None => return Ok(None),
				},
			};
			if let QueryStreamFrame::Rows {
				index,
				values,
				trace,
			} = &mut frame
			{
				let target = COALESCE_BYTES.min(self.max_bytes);
				while let Some(per_row) = self.row_bytes
					&& values.len().saturating_mul(per_row) < target
				{
					match self.next_frame().now_or_never() {
						Some(Some(QueryStreamFrame::Rows {
							index: i,
							values: more,
							trace: more_trace,
						})) if i == *index => {
							values.extend(more);
							match (trace.as_mut(), more_trace) {
								(Some(held), Some(more)) => held.absorb(*more),
								(None, more) => *trace = more,
								(Some(_), None) => {}
							}
						}
						Some(Some(other)) => {
							self.held = Some(other);
							break;
						}
						// Ended: never polled again.
						Some(None) => {
							self.ended = true;
							break;
						}
						None => break,
					}
				}
			}
			self.push(frame)?;
		}
	}
}

/// Keep the trace a frame of statement `index` carried, counting its encoding
/// toward the reply's bytes and in `charged`, by statement.
fn keep_trace(
	traces: &mut std::collections::BTreeMap<usize, surrealdb_rpc::Trace>,
	charged: &mut std::collections::BTreeMap<usize, usize>,
	index: usize,
	trace: Option<Box<surrealdb_rpc::Trace>>,
	format: Format,
	bytes: &mut usize,
) -> anyhow::Result<()> {
	let Some(trace) = trace.filter(|trace| !trace.is_empty()) else {
		return Ok(());
	};
	let size = wire::encode(format, trace.clone().into_value())?.len();
	*bytes += size;
	*charged.entry(index).or_default() += size;
	match traces.entry(index) {
		std::collections::btree_map::Entry::Occupied(mut held) => held.get_mut().absorb(*trace),
		std::collections::btree_map::Entry::Vacant(slot) => {
			slot.insert(*trace);
		}
	}
	Ok(())
}

/// Envelope kinds, by the frame each carries ([`FramedQueryFrames::last_kind`]).
pub const KIND_BEGIN: u8 = 0;
pub const KIND_ROWS: u8 = 1;
pub const KIND_VALUE: u8 = 2;
pub const KIND_FINISHED: u8 = 3;
pub const KIND_END: u8 = 4;
/// An envelope carrying no frame: a request that could not be framed at all.
pub const KIND_OTHER: u8 = 9;

fn kind_of(frame: &QueryStreamFrame) -> u8 {
	match frame {
		QueryStreamFrame::Begin {
			..
		} => KIND_BEGIN,
		QueryStreamFrame::Rows {
			..
		} => KIND_ROWS,
		QueryStreamFrame::Value {
			..
		} => KIND_VALUE,
		QueryStreamFrame::Finished {
			..
		} => KIND_FINISHED,
		QueryStreamFrame::End {
			..
		} => KIND_END,
	}
}

impl FramedQueryFrames {
	/// The kind of the envelope [`Self::next`] returned last (a `KIND_` constant): what a host
	/// splicing several streams into one needs, without decoding the envelope.
	pub fn last_kind(&self) -> u8 {
		self.last_kind
	}

	/// The next frame, polling an inline driver while waiting for it (the driver fills the
	/// channel; the channel holds one frame, so neither progresses without the other).
	async fn next_frame(&mut self) -> Option<QueryStreamFrame> {
		use futures::StreamExt;
		std::future::poll_fn(|cx| {
			if let Some(driver) = self.driver.as_mut()
				&& driver.as_mut().poll(cx).is_ready()
			{
				self.driver = None;
			}
			self.frames.poll_next_unpin(cx)
		})
		.await
	}
}

/// A host's hooks into [`EmbeddedEngine::execute_bounded_with`]: checked and awaited between
/// batches of rows.
#[derive(Default)]
pub struct BoundedHooks<'a> {
	/// Whether to stop: the stream is dropped, which stops the execution.
	pub cancelled: Option<&'a dyn Fn() -> bool>,
	/// A pause the host serves other work in.
	pub pause: Option<&'a dyn Fn() -> Pin<Box<dyn Future<Output = ()>>>>,
	/// Whether the host is out of memory for this query: the stream is dropped, and the reply is
	/// an error carrying `exhausted_message`.
	pub exhausted: Option<&'a dyn Fn() -> bool>,
	pub exhausted_message: Option<&'a dyn Fn() -> String>,
}

/// The size [`FramedQueryFrames::next`] coalesces ready rows frames up to.
const COALESCE_BYTES: usize = 1 << 20;

impl EmbeddedEngine {
	/// Answers an encoded RPC request as [`Self::execute_framed`] does, except
	/// that a `query` runs on the streaming path and its reply is assembled from
	/// the frames while counting their encoded size: past `max_bytes` the stream
	/// is dropped, which stops the execution, and the reply is an error.
	///
	/// So a result too large for one message costs at most `max_bytes` of
	/// memory to refuse, instead of being built whole first — which on a host
	/// with a small memory limit is the difference between an error and a crash.
	pub async fn execute_bounded(
		&self,
		format: Format,
		request: &[u8],
		max_bytes: usize,
	) -> anyhow::Result<Vec<u8>> {
		self.execute_bounded_with(format, request, max_bytes, &BoundedHooks::default()).await
	}

	/// [`Self::execute_bounded`] with a host's hooks between batches of rows: `cancelled`, when it
	/// answers true, drops the stream (which stops the execution) and answers an error; `pause`,
	/// awaited between batches, lets a host on a single thread serve other work in between, so a
	/// long statement does not hold the thread from start to end.
	pub async fn execute_bounded_with(
		&self,
		format: Format,
		request: &[u8],
		max_bytes: usize,
		hooks: &BoundedHooks<'_>,
	) -> anyhow::Result<Vec<u8>> {
		let decoded = wire::decode(format, request, self.recursion_limit())
			.and_then(|v| Ok(v.into_object()?))
			.and_then(|obj| Ok(Request::from_object(obj)?));
		let request = match decoded {
			Ok(request) if request.method == Method::Query => request,
			_ => return self.execute_framed(format, request).await,
		};
		let id = request.id.clone();
		let session = request.session_id.map(Into::into);
		let respond = |result: Result<crate::DbResult, Error>| {
			wire::encode(
				format,
				surrealdb_types::SurrealValue::into_value(crate::DbResponse::new(
					id.clone(),
					session,
					result,
				)),
			)
		};
		let mut frames = match self.query_stream(request).await {
			Ok(frames) => frames,
			Err(error) => return respond(Err(error)),
		};
		use futures::StreamExt;
		// Per statement: its rows so far, kept encoded (a decoded row is many times its encoding,
		// so holding values ran a large result out of memory long before its encoding reached
		// `max_bytes`); spliced into the reply at the end. Formats other than JSON and CBOR keep
		// the values.
		let splice = matches!(format, Format::Json | Format::Cbor);
		// Unique to this reply, so no value a query returns can equal a placeholder.
		let nonce = uuid::Uuid::now_v7().simple().to_string();
		let mut encoded: std::collections::BTreeMap<usize, (usize, Vec<u8>)> = Default::default();
		let mut rows: std::collections::BTreeMap<usize, Vec<surrealdb_types::Value>> =
			Default::default();
		let mut values: std::collections::BTreeMap<usize, surrealdb_types::Value> =
			Default::default();
		let mut finished: std::collections::BTreeMap<usize, QueryResult> = Default::default();
		// Per statement: the trace its frames carried, for a request that asked
		// for one; answered beside its result, as a buffered `query` answers it.
		let mut traces: std::collections::BTreeMap<usize, surrealdb_rpc::Trace> =
			Default::default();
		// The bytes each statement's trace is charged toward `max_bytes`, given
		// back when the statement fails and its trace is retracted.
		let mut trace_bytes: std::collections::BTreeMap<usize, usize> = Default::default();
		let mut bytes = 0usize;
		let mut scratch = Vec::new();
		while let Some(frame) = frames.next().await {
			if hooks.cancelled.is_some_and(|c| c()) {
				drop(frames);
				return respond(Err(Error::internal(
					"The query was cancelled: its connection closed".to_owned(),
				)));
			}
			if hooks.exhausted.is_some_and(|c| c()) {
				drop(frames);
				// The rows held so far go with the stream before the reply is built.
				drop(encoded);
				drop(rows);
				let message = hooks
					.exhausted_message
					.map_or_else(|| "The query ran out of memory".to_owned(), |m| m());
				return respond(Err(Error::internal(message)));
			}
			match frame {
				QueryStreamFrame::Begin {
					..
				} => {}
				QueryStreamFrame::Rows {
					index,
					values: batch,
					trace,
				} => {
					keep_trace(&mut traces, &mut trace_bytes, index, trace, format, &mut bytes)?;
					if splice {
						// Encoded once, here, and the values dropped.
						let (n, out) = encoded.entry(index).or_default();
						for v in &batch {
							if format == Format::Json && *n > 0 {
								out.push(b',');
							}
							let before = out.len();
							wire::encode_into(format, v, out)?;
							bytes += out.len() - before + 1;
							*n += 1;
						}
					} else {
						for v in &batch {
							scratch.clear();
							wire::encode_into(format, v, &mut scratch)?;
							bytes += scratch.len() + 1;
						}
						rows.entry(index).or_default().extend(batch);
					}
					if let Some(pause) = hooks.pause {
						pause().await;
					}
				}
				QueryStreamFrame::Value {
					index,
					value,
					trace,
				} => {
					keep_trace(&mut traces, &mut trace_bytes, index, trace, format, &mut bytes)?;
					scratch.clear();
					wire::encode_into(format, &value, &mut scratch)?;
					bytes += scratch.len();
					values.insert(index, value);
				}
				QueryStreamFrame::Finished {
					index,
					time,
					query_type,
					single,
					error,
					trace,
				} => {
					keep_trace(&mut traces, &mut trace_bytes, index, trace, format, &mut bytes)?;
					// A failed statement's trace is retracted with its rows.
					if error.is_some() {
						traces.remove(&index);
						bytes -= trace_bytes.remove(&index).unwrap_or(0);
					}
					let result = match error {
						Some(error) => Err(error),
						None if single => {
							Ok(values.remove(&index).unwrap_or(surrealdb_types::Value::None))
						}
						// A placeholder the encoded rows replace in the reply's bytes (an empty
						// array for a statement that produced none).
						None if splice => {
							encoded.entry(index).or_default();
							Ok(surrealdb_types::Value::String(rows_placeholder(&nonce, index)))
						}
						None => Ok(surrealdb_types::Value::Array(
							rows.remove(&index).unwrap_or_default().into(),
						)),
					};
					rows.remove(&index);
					finished.insert(
						index,
						QueryResult {
							time,
							result,
							query_type,
						},
					);
				}
				QueryStreamFrame::End {
					error,
					..
				} => {
					if let Some(error) = error {
						return respond(Err(error));
					}
					break;
				}
			}
			if bytes > max_bytes {
				drop(frames);
				return respond(Err(Error::internal(format!(
					"The result is over the {max_bytes}-byte limit for one message: use query_stream, \
					 or LIMIT/START to page it"
				))));
			}
		}
		let over = || {
			Error::internal(format!(
				"The result is over the {max_bytes}-byte limit for one message: use query_stream, \
				 or LIMIT/START to page it"
			))
		};
		let answer = if traces.is_empty() {
			crate::DbResult::Query(finished.into_values().collect())
		} else {
			// The results' wire form, each carrying its statement's trace.
			let results = finished
				.into_iter()
				.map(|(index, result)| {
					let mut value = surrealdb_types::SurrealValue::into_value(result);
					if let (surrealdb_types::Value::Object(object), Some(trace)) =
						(&mut value, traces.remove(&index))
					{
						object.insert("trace", trace.into_value());
					}
					value
				})
				.collect::<Vec<_>>();
			crate::DbResult::Other(surrealdb_types::Value::Array(results.into()))
		};
		let envelope = respond(Ok(answer))?;
		if encoded.is_empty() {
			return if envelope.len() > max_bytes {
				respond(Err(over()))
			} else {
				Ok(envelope)
			};
		}
		// The reply in one allocation: the envelope's bytes with each placeholder replaced by its
		// statement's rows, each statement's encoding dropped once copied.
		let mut cuts = Vec::with_capacity(encoded.len());
		for (index, rows) in encoded {
			let marker = wire::encode(
				format,
				surrealdb_types::Value::String(rows_placeholder(&nonce, index)),
			)?;
			// Absent when the statement failed: its rows are not in the reply.
			if let Some(at) = envelope.windows(marker.len()).position(|w| w == marker.as_slice()) {
				cuts.push((at, marker.len(), rows));
			}
		}
		cuts.sort_by_key(|(at, ..)| *at);
		let size =
			envelope.len() + cuts.iter().map(|(_, _, (_, items))| items.len() + 9).sum::<usize>();
		let mut reply = Vec::with_capacity(size);
		let mut from = 0;
		for (at, len, (n, items)) in cuts {
			reply.extend_from_slice(&envelope[from..at]);
			if format == Format::Json {
				reply.push(b'[');
				reply.extend_from_slice(&items);
				reply.push(b']');
			} else {
				cbor_array_head(&mut reply, n as u64);
				reply.extend_from_slice(&items);
			}
			drop(items);
			from = at + len;
		}
		reply.extend_from_slice(&envelope[from..]);
		if reply.len() > max_bytes {
			return respond(Err(over()));
		}
		Ok(reply)
	}
}

/// The stand-in for statement `index`'s rows while the reply is encoded: a string carrying the
/// reply's `nonce`, which no result holds, replaced by the encoded rows after.
fn rows_placeholder(nonce: &str, index: usize) -> String {
	format!("\u{1}\u{2}surreal-rows:{nonce}:{index}\u{2}\u{1}")
}

/// A CBOR array head for `n` items, in the shortest form.
fn cbor_array_head(out: &mut Vec<u8>, n: u64) {
	match n {
		0..=23 => out.push(0x80 | n as u8),
		24..=0xff => out.extend_from_slice(&[0x98, n as u8]),
		0x100..=0xffff => {
			out.push(0x99);
			out.extend_from_slice(&(n as u16).to_be_bytes());
		}
		0x1_0000..=0xffff_ffff => {
			out.push(0x9a);
			out.extend_from_slice(&(n as u32).to_be_bytes());
		}
		_ => {
			out.push(0x9b);
			out.extend_from_slice(&n.to_be_bytes());
		}
	}
}

/// Where a streamed full scan spends its time, natively, in-memory:
/// `cargo test -p surrealdb-embedded --features kv-mem --release stream_throughput -- --ignored
/// --nocapture`
#[cfg(test)]
mod throughput {
	use std::time::Instant;

	use surrealdb_types::{Array, Object, Value};

	use super::*;
	use crate::{EmbeddedEngine, Options};

	fn req(method: &str, sql: &str) -> Vec<u8> {
		let mut obj = Object::default();
		obj.insert("id".to_owned(), Value::Number(1.into()));
		obj.insert("method".to_owned(), Value::String(method.to_owned()));
		obj.insert(
			"params".to_owned(),
			Value::Array(Array::from(vec![Value::String(sql.to_owned())])),
		);
		wire::encode(Format::Cbor, Value::Object(obj)).unwrap()
	}

	#[tokio::test(flavor = "current_thread")]
	#[ignore]
	async fn stream_throughput() {
		let rows: usize =
			std::env::var("ROWS").ok().and_then(|v| v.parse().ok()).unwrap_or(200_000);
		let e = EmbeddedEngine::connect("memory", Options::default()).await.unwrap();
		let mut sql = String::from("USE NS test DB test; DEFINE TABLE huge SCHEMALESS;");
		e.execute_framed(Format::Cbor, &req("query", &sql)).await.unwrap();
		for lo in (1..=rows).step_by(5000) {
			sql.clear();
			sql.push_str("USE NS test DB test; INSERT INTO huge [");
			for i in lo..(lo + 5000).min(rows + 1) {
				sql.push_str(&format!(
					"{{id:{i},n:{i},g:{},v:{}}},",
					i % 100,
					(i * 7919) % 1000003
				));
			}
			sql.push_str("] RETURN NONE;");
			e.execute_framed(Format::Cbor, &req("query", &sql)).await.unwrap();
		}
		let repeat: usize = std::env::var("REPEAT").ok().and_then(|v| v.parse().ok()).unwrap_or(1);
		let only = std::env::var("ONLY").ok();
		let cases = [
			(
				"VALUE 1 (scan + per-row frame)",
				Format::Cbor,
				"USE NS test DB test; SELECT VALUE 1 FROM huge",
			),
			("SELECT * cbor", Format::Cbor, "USE NS test DB test; SELECT * FROM huge"),
			("SELECT * json", Format::Json, "USE NS test DB test; SELECT * FROM huge"),
		];
		for (label, fmt, q) in cases
			.into_iter()
			.filter(|(l, ..)| only.as_deref().is_none_or(|o| l.contains(o)))
			.flat_map(|c| std::iter::repeat_n(c, repeat))
		{
			let body = if fmt == Format::Json {
				format!(r#"{{"id":1,"method":"query_stream","params":["{q}"]}}"#).into_bytes()
			} else {
				req("query_stream", q)
			};
			let t = Instant::now();
			let mut frames = e.query_stream_framed(fmt, &body, 4 << 20).await.unwrap();
			let (mut n, mut bytes) = (0usize, 0usize);
			while let Some(env) = frames.next().await.unwrap() {
				n += 1;
				bytes += env.len();
			}
			let s = t.elapsed().as_secs_f64();
			eprintln!(
				"{label:32} {n:6} envelopes {:7.1} MB {:6.3} s  {:8.0} rows/s {:6.1} MB/s",
				bytes as f64 / 1e6,
				s,
				rows as f64 / s,
				bytes as f64 / 1e6 / s
			);
		}
	}
}

#[cfg(test)]
mod inline {
	use surrealdb_types::{Array, Object, Value};

	use super::*;
	use crate::{EmbeddedEngine, Options};

	fn req(method: &str, sql: &str) -> Vec<u8> {
		let mut obj = Object::default();
		obj.insert("id".to_owned(), Value::Number(7.into()));
		obj.insert("method".to_owned(), Value::String(method.to_owned()));
		obj.insert(
			"params".to_owned(),
			Value::Array(Array::from(vec![Value::String(sql.to_owned())])),
		);
		wire::encode(Format::Cbor, Value::Object(obj)).unwrap()
	}

	async fn drain(mut f: FramedQueryFrames) -> Vec<Vec<u8>> {
		let mut out = Vec::new();
		while let Some(e) = f.next().await.unwrap() {
			out.push(e);
		}
		out
	}

	/// The inline stream (the query driven inside `next`) answers with exactly the envelopes the
	/// spawned one does, small envelopes forcing many frames through the one-frame channel.
	#[tokio::test(flavor = "current_thread")]
	async fn inline_frames_match_spawned() {
		let e = EmbeddedEngine::connect("memory", Options::default()).await.unwrap();
		let mut sql = String::from("USE NS test DB test; INSERT INTO t [");
		for i in 1..=3000 {
			sql.push_str(&format!("{{id:{i},v:{}}},", i * 3));
		}
		sql.push_str("] RETURN NONE;");
		e.execute_framed(Format::Cbor, &req("query", &sql)).await.unwrap();
		let q = req(
			"query_stream",
			"USE NS test DB test; SELECT * FROM t; SELECT VALUE v FROM t WHERE v > 4000; RETURN 1",
		);
		let spawned = drain(e.query_stream_framed(Format::Cbor, &q, 2048).await.unwrap()).await;
		let inline =
			drain(e.query_stream_framed_inline(Format::Cbor, &q, 2048).await.unwrap()).await;
		assert!(spawned.len() > 10, "many envelopes: {}", spawned.len());
		// Envelope boundaries may differ (frames ready together coalesce), and so do timings:
		// compare each statement's rows in order, and the frame kinds' sequence less repeats.
		assert_eq!(rows_by_statement(&spawned), rows_by_statement(&inline));
		assert_eq!(kinds(&spawned), kinds(&inline));
	}

	/// A bounded reply to a `query` that asks for a trace carries each
	/// statement's trace beside its result, as the unbounded reply does.
	#[tokio::test(flavor = "current_thread")]
	async fn bounded_reply_carries_the_trace() {
		let e = EmbeddedEngine::connect("memory", Options::default()).await.unwrap();
		let setup = "USE NS test DB test; CREATE person:a, person:b; \
		             RELATE person:a->knows:ab->person:b;";
		e.execute_framed(Format::Cbor, &req("query", setup)).await.unwrap();
		let q = "USE NS test DB test; LET $x = 1; SELECT ->knows->person AS f FROM person:a; \
		         THROW 'no'";
		let body = format!(
			r#"{{"id":7,"method":"query","params":["{q}",{{}},{{"trace":["relations","variables"]}}]}}"#
		)
		.into_bytes();
		let bounded = e.execute_bounded(Format::Json, &body, 64 << 20).await.unwrap();
		let plain = e.execute_framed(Format::Json, &body).await.unwrap();
		let results = |bytes: &[u8]| {
			let mut v = wire::decode(Format::Json, bytes, 64).unwrap().into_object().unwrap();
			let Some(Value::Array(results)) = v.remove("result") else {
				panic!("no results: {v:?}")
			};
			results
				.into_iter()
				.map(|r| {
					let mut r = r.into_object().unwrap();
					r.remove("time");
					r
				})
				.collect::<Vec<_>>()
		};
		let (bounded, plain) = (results(&bounded), results(&plain));
		assert_eq!(bounded, plain);
		assert!(bounded[1].contains_key("trace"), "the LET reports what it bound: {bounded:?}");
		assert!(bounded[2].contains_key("trace"), "the lookup reports its edge: {bounded:?}");
		assert!(!bounded[3].contains_key("trace"), "a failed statement carries none");
	}

	/// The limit covers the whole reply, envelope included, and a value that looks like a row
	/// placeholder is a value.
	#[tokio::test(flavor = "current_thread")]
	async fn bounded_reply_enforces_its_limit_and_ignores_lookalike_values() {
		let e = EmbeddedEngine::connect("memory", Options::default()).await.unwrap();
		let body = req("query", "USE NS test DB test; RETURN 1");
		let refused = e.execute_bounded(Format::Cbor, &body, 1).await.unwrap();
		let refused = wire::decode(Format::Cbor, &refused, 64).unwrap().into_object().unwrap();
		assert!(refused.contains_key("error"), "{refused:?}");
		let look = rows_placeholder("x", 1);
		let q =
			format!("USE NS test DB test; CREATE t:1 SET v = 1; RETURN '{look}'; SELECT * FROM t;");
		let reply = e
			.execute_bounded(
				Format::Json,
				format!(r#"{{"id":7,"method":"query","params":["{q}"]}}"#).as_bytes(),
				1 << 20,
			)
			.await
			.unwrap();
		let text = String::from_utf8(reply).unwrap();
		assert!(text.contains("surreal-rows:x:1"), "{text}");
		assert!(text.contains("\"v\":1"), "{text}");
	}

	/// The bounded path keeps rows encoded and splices them into the reply: the reply decodes to
	/// what the unbounded path answers, in both formats, over every shape of result.
	#[tokio::test(flavor = "current_thread")]
	async fn bounded_reply_matches_unbounded() {
		let e = EmbeddedEngine::connect("memory", Options::default()).await.unwrap();
		let mut sql = String::from("USE NS test DB test; INSERT INTO t [");
		for i in 1..=2500 {
			sql.push_str(&format!("{{id:{i},v:{},s:'x{i}'}},", i * 3));
		}
		sql.push_str("] RETURN NONE;");
		e.execute_framed(Format::Cbor, &req("query", &sql)).await.unwrap();
		let q = "USE NS test DB test; SELECT * FROM t; SELECT VALUE v FROM t WHERE v > 7000; \
		         SELECT * FROM t WHERE v < 0; SELECT * FROM ONLY t:5; THROW 'no'; RETURN 'surreal-rows:0'; \
		         SELECT id FROM t LIMIT 3";
		for format in [Format::Cbor, Format::Json] {
			let body = if format == Format::Json {
				format!(r#"{{"id":7,"method":"query","params":["{q}"]}}"#).into_bytes()
			} else {
				req("query", q)
			};
			let bounded = e.execute_bounded(format, &body, 64 << 20).await.unwrap();
			let plain = e.execute_framed(format, &body).await.unwrap();
			let strip = |bytes: &[u8]| {
				let mut v = wire::decode(format, bytes, 64).unwrap().into_object().unwrap();
				let Some(Value::Array(results)) = v.remove("result") else {
					panic!("no results")
				};
				// Timings differ between runs.
				results
					.into_iter()
					.map(|r| {
						let mut r = r.into_object().unwrap();
						r.remove("time");
						r
					})
					.collect::<Vec<_>>()
			};
			let (b, p) = (strip(&bounded), strip(&plain));
			assert_eq!(b.len(), p.len(), "{format:?}: statement count");
			for (i, (x, y)) in b.iter().zip(&p).enumerate() {
				if x != y {
					let brief = |o: &Object| {
						o.iter()
							.map(|(k, v)| {
								let s = format!("{v:?}");
								format!("{k}={}", &s[..s.len().min(160)])
							})
							.collect::<Vec<_>>()
							.join(" ")
					};
					panic!(
						"{format:?} statement {i} differs:\n bounded {}\n plain   {}",
						brief(x),
						brief(y)
					);
				}
			}
		}
	}

	fn frames(envelopes: &[Vec<u8>]) -> Vec<Object> {
		envelopes
			.iter()
			.map(|e| {
				let v = wire::decode(Format::Cbor, e, 64).unwrap().into_object().unwrap();
				v.get("result").cloned().unwrap().into_object().unwrap()
			})
			.collect()
	}

	fn rows_by_statement(envelopes: &[Vec<u8>]) -> Vec<(i64, Vec<Value>)> {
		let mut out: Vec<(i64, Vec<Value>)> = Vec::new();
		for f in frames(envelopes) {
			if f.get("stream") != Some(&Value::String("rows".into())) {
				continue;
			}
			let Some(Value::Number(i)) = f.get("index") else {
				panic!("rows without an index")
			};
			let i = i.to_int().unwrap();
			let Some(Value::Array(vals)) = f.get("values") else {
				panic!("rows without values")
			};
			match out.last_mut() {
				Some((j, v)) if *j == i => v.extend(vals.iter().cloned()),
				_ => out.push((i, vals.iter().cloned().collect())),
			}
		}
		out
	}

	fn kinds(envelopes: &[Vec<u8>]) -> Vec<String> {
		let mut out: Vec<String> = Vec::new();
		for f in frames(envelopes) {
			let Some(Value::String(k)) = f.get("stream") else {
				panic!("a frame without a kind")
			};
			if out.last() != Some(k) {
				out.push(k.clone());
			}
		}
		out
	}
}
