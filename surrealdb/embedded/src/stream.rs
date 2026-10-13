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
use surrealdb_core::dbs::{AuthPrincipalSnapshot, Session};
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
/// Exists only to name one spawn across both targets: wasm has a single event
/// loop and no `Send` to offer, so the driver's future is not `Send` there.
#[cfg(not(target_family = "wasm"))]
fn spawn(task: impl Future<Output = ()> + Send + 'static) {
	tokio::spawn(task);
}

/// The wasm counterpart.
#[cfg(target_family = "wasm")]
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
		let (frames_tx, frames_rx) = bounded(FRAME_BUFFER);
		let (job, principal) = RpcProtocol::query_stream(
			self,
			request.txn.map(Into::into),
			session_id,
			request.params,
			Some(cancel.clone()),
			items_tx,
		)
		.await?;
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
		spawn(driver.drive(statements, job.run));
		Ok(frames_rx)
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
			_ => None,
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
			_ => None,
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
				} => Some(values),
				_ => None,
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
				_ => None,
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
			_ => false,
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
