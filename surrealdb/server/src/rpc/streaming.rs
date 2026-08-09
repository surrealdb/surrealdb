//! Streaming query answers on the WebSocket RPC protocol.
//!
//! A `query_stream` request is answered by a sequence of [`QueryStreamFrame`]
//! responses, every one carrying the request's id and session, instead of a
//! single response holding all the results. The driver here owns the whole
//! exchange: it runs the execution via [`RpcProtocol::query_stream`], frames
//! the items it produces, and writes the frames to the connection's outbound
//! channel as they become available, so the first rows reach the client while
//! the rest of the query is still running.
//!
//! # Lifecycle invariants
//!
//! - The execution future is driven to completion on every path and never dropped mid-flight: it
//!   owns an open transaction, and dropping it would leave that transaction neither committed nor
//!   cancelled. Stopping a stream is always the cooperative pair of tripping its [`CancelHandle`]
//!   *and* closing the items channel — the flag alone cannot unpark an execution blocked on a full
//!   channel, and the close alone would let a statement in a non-emitting phase run to completion.
//! - The execution and the items drain are awaited together: the channel is bounded, so awaiting
//!   either alone deadlocks against the other.
//! - Every frame send is raced against the connection canceller and the query's wall-clock
//!   deadline. The outbound channel is bounded and shared with every other response on the
//!   connection, so a client that stops reading would otherwise park this driver on a send forever
//!   — with the execution unpolled, holding its read snapshot, and the wall-clock guard wrapped
//!   around it never getting the chance to fire.
//! - Rows are provisional until their statement's `Finished` frame: a statement that fails —
//!   including one whose value this server cannot encode for the negotiated format — has its rows
//!   retracted by a `Finished` carrying the error, and nothing further is framed for it.
//!
//! # A stream on a client-managed transaction must not be interleaved with `commit`
//!
//! A `query_stream` given a `txn` runs on the transaction that `begin` handed
//! the client, and requests on one connection are served concurrently. Nothing
//! stops a `commit` for that transaction arriving while the stream is still
//! executing on it: the commit succeeds, and the stream's next operation fails
//! with the transaction already finished. Frames make that reachable
//! deliberately rather than by accident, because the client can see exactly how
//! far the query has got and choose when to commit — so what it commits is a
//! prefix of its own multi-statement query, not the whole of it.
//!
//! A client that streams inside its own transaction must therefore wait for the
//! terminal frame before committing. Bounding this properly needs the
//! transaction map to know an entry is in use, which is not something this
//! transport can decide alone.

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::Message;
use surrealdb_core::channel::{Receiver, bounded};
use surrealdb_core::ctx::CancelHandle;
use surrealdb_core::rpc::RpcProtocol;
use surrealdb_rpc::capabilities::MethodTarget;
use surrealdb_rpc::error::{invalid_params, method_not_allowed, stream_exists, too_many_streams};
use surrealdb_rpc::{
	DbResponse, DbResult, Method, QUERY_STREAM_BUFFER, QueryResult, QueryStreamFrame,
	QueryStreamItem, QueryType,
};
use surrealdb_types::{Array, Error as TypesError, ToSql, Value};
use tokio::sync::mpsc::Sender;
use tokio::time::Instant;
use uuid::Uuid;

use crate::cnf::{WEBSOCKET_MAX_CONCURRENT_STREAMS, WEBSOCKET_STREAM_SEND_TIMEOUT_SECS};
use crate::rpc::format::WsFormat;
use crate::rpc::websocket::Websocket;

/// The first rows frame a statement sends, in records.
///
/// The ramp exists for time-to-first-row: a client waiting on a large `SELECT`
/// sees something after this many records rather than after a full frame. Each
/// subsequent frame doubles up to [`QUERY_BATCH_RECORDS`], so the small frames
/// are confined to the start and a long result still costs one frame per
/// [`QUERY_BATCH_RECORDS`] rows.
const QUERY_FIRST_BATCH_RECORDS: usize = 16;

/// The most records one rows frame carries.
///
/// Bounds the size of a single WebSocket message so a long result interleaves
/// with the connection's other traffic — concurrent responses, live-query
/// notifications, pings all share one outbound channel — instead of occupying
/// it with one enormous frame.
const QUERY_BATCH_RECORDS: usize = 256;

/// How long the terminal `End` frame may wait for the outbound channel before
/// it is given up on.
///
/// Short on purpose: by the time it is sent the stream is over and its driver
/// is the only thing this delays, but a client that merely fell behind deserves
/// to learn its stream ended rather than waiting on a frame that never comes.
const TERMINAL_FRAME_GRACE: Duration = Duration::from_secs(1);

/// The error a terminal `End` carries when live queries the client was already
/// told about could not be kept: their session went away mid-query, so the ids
/// it holds will never fire.
///
/// Those statements finished successfully on the wire, so an errored `End` does
/// not retract them — retraction covers only statements without a `Finished`
/// frame. The ids are therefore named here, which is what lets a client with
/// more than one `LIVE SELECT` in the stream tell which of them is dead.
fn live_queries_disowned(ids: &[Uuid]) -> TypesError {
	let ids = ids.iter().map(|id| id.to_string()).collect::<Vec<_>>().join(", ");
	TypesError::internal(format!(
		"These live queries were discarded because their session ended, and will never \
		 deliver notifications: {ids}"
	))
}

/// The error a terminal `End` carries when the stream was stopped rather than
/// answered: cancelled by its client, torn down with the connection, or
/// abandoned because its frames could no longer be delivered.
///
/// Its presence is what retracts the statements that never finished, so a
/// truncated answer is never mistaken for a whole one.
fn stream_stopped() -> TypesError {
	TypesError::internal("The streaming query was stopped before it completed".to_string())
}

/// A streaming execution in flight, as [`QueryStreamJob::run`] hands it over.
///
/// [`QueryStreamJob::run`]: surrealdb_core::dbs::QueryStreamJob::run
type QueryStreamRun = Pin<Box<dyn Future<Output = Result<Vec<QueryResult>, TypesError>> + Send>>;

/// One in-flight streaming query, as `query_cancel` reaches it.
///
/// Held in the connection's stream registry under the originating request's
/// id, so a cancel can only ever name a stream on its own connection.
pub(crate) struct StreamHandle {
	cancel: CancelHandle,
	items: Receiver<QueryStreamItem>,
}

impl StreamHandle {
	/// Stop the stream's execution: the cooperative cancel pair described in
	/// the module docs. The driver then observes the closed channel, drives
	/// the execution to its (cancelled) completion, and ends the stream.
	pub(crate) fn stop(&self) {
		self.cancel.trip();
		self.items.close();
	}
}

/// Removes a stream's registry entry when the driver returns, on every path.
struct StreamRegistration<'a> {
	rpc: &'a Websocket,
	key: String,
}

impl Drop for StreamRegistration<'_> {
	fn drop(&mut self) {
		self.rpc.streams.remove(&self.key);
	}
}

/// Serve one `query_stream` request end to end.
///
/// Failures before execution begins — a missing request id, a denied
/// capability, a parse error, an unknown transaction — are answered with an
/// ordinary failure response and no frames at all, exactly the answer the
/// buffered `query` method would have given.
pub(crate) async fn process_query_stream(
	rpc: &Arc<Websocket>,
	id: Option<Value>,
	session_id: Uuid,
	client_session: Option<Uuid>,
	txn: Option<Uuid>,
	params: Array,
	chn: Sender<Message>,
) {
	let fmt = rpc.format;
	// Frames are correlated by the request id, so a request without one has
	// nothing for the client to route them to.
	let Some(id) = id else {
		let error = invalid_params("The query_stream method requires a request id");
		crate::rpc::response::send(DbResponse::failure(None, client_session, error), fmt, chn)
			.await;
		return;
	};
	if let Err(error) = preflight(rpc) {
		crate::rpc::response::send(DbResponse::failure(Some(id), client_session, error), fmt, chn)
			.await;
		return;
	}
	// Reserve the registry slot before anything can run, so a concurrent
	// `query_cancel` for this id has a handle to find and a duplicate id is
	// refused before it does any work. The reservation is insert-then-check,
	// like `begin`'s transaction slots: two concurrent inserts cannot both
	// slip under the cap.
	let key = id.to_sql();
	let cancel = CancelHandle::new();
	let (items_tx, items_rx) = bounded(QUERY_STREAM_BUFFER);
	let handle = StreamHandle {
		cancel: cancel.clone(),
		items: items_rx.clone(),
	};
	// An id names one stream: a duplicate is refused, and the stream already
	// running under this id keeps its registration untouched.
	//
	// The reservation's outcome is taken as a `bool` so the map guard is
	// released before anything is awaited. A `DashMap` guard held across an
	// await would be catastrophic here: the guard is a blocking lock over a
	// whole shard, the await below is a send on a channel the client paces,
	// and every other task touching the registry — `query_cancel`, another
	// stream's deregistration, the `len` below — would block its worker
	// thread rather than yield.
	let reserved = match rpc.streams.entry(key.clone()) {
		dashmap::mapref::entry::Entry::Occupied(_) => false,
		dashmap::mapref::entry::Entry::Vacant(slot) => {
			slot.insert(handle);
			true
		}
	};
	if !reserved {
		let error = stream_exists();
		crate::rpc::response::send(DbResponse::failure(Some(id), client_session, error), fmt, chn)
			.await;
		return;
	}
	let registration = StreamRegistration {
		rpc,
		key,
	};
	// A rejected request must hold no reservation while it reports the
	// rejection. Reporting it is an awaited send on a channel the client paces,
	// so on a connection that has stopped reading it parks indefinitely -- and
	// the read loop keeps accepting messages, so every further request would
	// park the same way and hold a reservation of its own. Releasing first
	// bounds the registry by what is actually executing, which is what the cap
	// is for; keeping the reservation until the send completed would let a
	// client grow the registry without bound precisely by refusing to read.
	if rpc.streams.len() > *WEBSOCKET_MAX_CONCURRENT_STREAMS {
		drop(registration);
		let error = too_many_streams();
		crate::rpc::response::send(DbResponse::failure(Some(id), client_session, error), fmt, chn)
			.await;
		return;
	}
	// The deadline for the whole exchange, when the operator configured one.
	// `query_stream` applies the same timeout to the execution, but that guard
	// only fires while the execution is being polled, and a stalled client is
	// exactly what stops it being polled — so the sends are bounded here too.
	let deadline = rpc.kvs().query_timeout().map(|timeout| Instant::now() + timeout);
	let job = match RpcProtocol::query_stream(
		rpc.as_ref(),
		txn,
		session_id,
		params,
		Some(cancel.clone()),
		items_tx,
	)
	.await
	{
		Ok(job) => job,
		Err(error) => {
			// Released before the report, for the reason given at the cap
			// check above: a rejected request holds nothing while it parks.
			drop(registration);
			crate::rpc::response::send(
				DbResponse::failure(Some(id), client_session, error),
				fmt,
				chn,
			)
			.await;
			return;
		}
	};
	let mut driver = StreamDriver {
		rpc,
		id,
		client_session,
		session_id,
		chn,
		cancel,
		items: items_rx,
		deadline,
		muted: false,
		failure: None,
		answered: false,
	};
	driver.serve(job.statement_count, job.run).await;
	drop(registration);
}

/// The gates a request must pass before it may reserve any stream state.
fn preflight(rpc: &Websocket) -> Result<(), TypesError> {
	// `query_stream` is its own capability target, and the execution below
	// re-applies the `query` gate inside `RpcProtocol::query_stream` — so
	// denying either method name denies streaming.
	if !rpc.kvs().allows_rpc_method(&MethodTarget {
		method: Method::QueryStream,
	}) {
		warn!("Capabilities denied RPC method call attempt, target: 'query_stream'");
		return Err(method_not_allowed(Method::QueryStream.to_string()));
	}
	Ok(())
}

/// One streaming exchange being served: the execution, its frames, and the
/// connection state the frames are written through.
struct StreamDriver<'a> {
	rpc: &'a Arc<Websocket>,
	id: Value,
	client_session: Option<Uuid>,
	session_id: Uuid,
	chn: Sender<Message>,
	cancel: CancelHandle,
	items: Receiver<QueryStreamItem>,
	deadline: Option<Instant>,
	/// Set once frames can no longer reach the client — the connection is
	/// closing, the deadline passed while a send was blocked, or the channel
	/// errored. The execution is still driven to completion; its remaining
	/// frames are discarded.
	muted: bool,
	/// Why this stream cannot be answered correctly, when something other than
	/// the execution itself is the reason. Carried on the terminal `End`.
	failure: Option<TypesError>,
	/// Set once the request has been answered outright with a failure, which
	/// happens only when a frame could not be encoded. The stream sends nothing
	/// further for it: one request gets one answer, and a call already settled
	/// by an error must not then receive a success.
	answered: bool,
}

impl StreamDriver<'_> {
	/// Drive the execution while framing and sending everything it produces.
	async fn serve(&mut self, statement_count: usize, run: QueryStreamRun) {
		let started = Instant::now();
		let conn = self.rpc.cancel.token();
		let mut run = Some(run);
		let mut frames = StreamFrames::new();
		let mut outcome = None;
		// The stream opens before anything has been produced, which is what lets
		// a client set up for it while the query is still starting.
		self.send(
			QueryStreamFrame::Begin {
				statements: statement_count,
			},
			&mut frames,
		)
		.await;
		// The connection canceller is observed at most once: it stops the
		// execution, and the loop then finishes through the ordinary
		// channel-closed path.
		let mut conn_observed = false;
		let outcome = loop {
			while let Some(frame) = frames.pop() {
				self.send(frame, &mut frames).await;
			}
			if let Some(outcome) = outcome {
				break outcome;
			}
			let running = run.as_mut().expect("the execution is driven until it completes");
			tokio::select! {
				biased;
				_ = conn.cancelled(), if !conn_observed => {
					conn_observed = true;
					self.stop_sending();
				}
				item = self.items.recv() => match item {
					Ok(item) => frames.absorb(item),
					// The execution dropped its sender, so the only thing
					// left is its own outcome.
					Err(_) => {
						outcome = Some(run.take().expect("still running").await);
					}
				},
				result = running => {
					// The execution finished before its channel drained. Take
					// what is still buffered before terminating.
					run = None;
					while let Ok(item) = self.items.try_recv() {
						frames.absorb(item);
					}
					outcome = Some(result);
				}
			}
		};
		// Every statement frame has now been sent, so what the client holds is
		// settled and the terminal frame can describe it. Building `End` here
		// rather than queueing it alongside the statement frames is what keeps
		// it terminal: a queued `End` would be overtaken by a retraction raised
		// while the queue drained, and would carry a result count and an
		// outcome fixed before those frames were known.
		let error = match &outcome {
			// A stream that was stopped -- cancelled by its client, torn down
			// with the connection, or abandoned because its frames could no
			// longer be sent -- did not deliver every result it would have. The
			// executor reports abandonment as success, so the stop is what
			// makes the answer incomplete, and the terminal frame has to say
			// so: without an error here a client would read a truncated answer
			// as a whole one, and the contract that an errored `End` retracts
			// every unfinished statement would never engage.
			Ok(results) => {
				let disowned = self.settle_live_queries(Some(results), &frames).await;
				// A disowned id outranks the generic stop: it names a
				// subscription the client is holding and has to drop, which is
				// strictly more than "this stream did not finish" tells it.
				self.failure
					.take()
					.or_else(|| (!disowned.is_empty()).then(|| live_queries_disowned(&disowned)))
					.or_else(|| self.cancel.is_cancelled().then(stream_stopped))
			}
			// A failure that belongs to no single statement ends the stream
			// instead of completing it, retracting every statement that has not
			// finished. The execution's own error is the more specific one, so
			// it wins.
			Err(error) => {
				self.settle_live_queries(None, &frames).await;
				Some(error.clone())
			}
		};
		let end = QueryStreamFrame::End {
			// What the client holds, not what was framed: a statement whose
			// terminal frame was queued but never sent is one the client cannot
			// see, and counting it would contradict the count it is told to
			// derive from the `Finished` frames themselves.
			results: frames.delivered.len(),
			time: started.elapsed(),
			error,
		};
		self.send(end, &mut frames).await;
	}

	/// Send one frame, enforcing the module's send invariants.
	///
	/// A frame that cannot be encoded for the negotiated format fails only the
	/// statement whose value it carries, exactly as that statement's own error
	/// would; the stream itself continues. A send that cannot complete —
	/// connection closing, deadline passed, channel gone — stops the execution
	/// and mutes the stream. A muted stream discards its payload frames, but
	/// its terminal `End` still gets one short-grace attempt: a client that
	/// merely fell behind — rather than went away — is told its stream is
	/// over instead of being left waiting on frames that will never come.
	async fn send(&mut self, frame: QueryStreamFrame, frames: &mut StreamFrames) {
		if self.answered {
			return;
		}
		let is_end = matches!(&frame, QueryStreamFrame::End { .. });
		let is_begin = matches!(&frame, QueryStreamFrame::Begin { .. });
		if is_end {
			// The terminal frame is what tells a client its stream is over, and
			// losing it is unrecoverable -- so it gets at least this grace,
			// however little of the exchange's deadline is left. It has to be a
			// floor rather than a replacement: the exchange deadline is armed
			// before the execution starts, while the guard that ends the
			// execution only arms once it is first polled, so a timed-out
			// stream reaches here past that deadline and would get no window at
			// all; but a healthy stream must not be given *less* room for its
			// terminal frame than it had for the rows before it.
			let floor = Instant::now() + TERMINAL_FRAME_GRACE;
			self.deadline = Some(match self.deadline {
				Some(deadline) => deadline.max(floor),
				None => floor,
			});
		} else if self.muted {
			return;
		}
		// The statement a payload frame belongs to, which is what an encode
		// failure fails, and the statement a terminal frame answers, which is
		// what a successful send makes final.
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
		let terminal = match &frame {
			QueryStreamFrame::Finished {
				index,
				..
			} => Some(*index),
			_ => None,
		};
		let response = DbResponse::success(
			Some(self.id.clone()),
			self.client_session,
			DbResult::Other(frame.into_value()),
		);
		let message = match self.rpc.format.res_ws(response) {
			Ok((_len, message)) => message,
			Err(error) => {
				// A value the wire cannot carry fails the statement that
				// produced it, exactly as its own error would -- provided that
				// statement can still be failed. Once its terminal frame has
				// been delivered, its success cannot be unsaid, so the only
				// honest answer left is to fail the whole stream.
				if let Some(index) = statement
					&& frames.retract(index, error.clone())
				{
					return;
				}
				// A frame that brackets or terminates the stream is plain data
				// and cannot fail to encode short of a serializer bug -- but if
				// one did, the request would otherwise be left with no terminal
				// frame at all, and a caller waiting on it would wait forever.
				// The buffered path answers an unencodable response with an
				// encodable failure carrying the same id; so does this.
				//
				// That failure *is* the request's answer, so the stream sends
				// nothing further for it: a client whose call has already been
				// settled by an error must not then be sent a success. It also
				// goes out through this same bounded send rather than the
				// buffered path's unbounded one, which would park the driver
				// here with the execution unpolled -- the state the per-frame
				// timeout exists to prevent.
				self.fail_stream(error.clone());
				self.answered = true;
				let failure =
					DbResponse::failure(Some(self.id.clone()), self.client_session, error);
				if let Ok((_len, message)) = self.rpc.format.res_ws(failure) {
					let conn = self.rpc.cancel.token();
					let send = async {
						tokio::select! {
							biased;
							_ = conn.cancelled() => false,
							sent = self.chn.send(message) => sent.is_ok(),
						}
					};
					let deadline = Instant::now() + TERMINAL_FRAME_GRACE;
					let _ = tokio::time::timeout_at(deadline, send).await;
				}
				return;
			}
		};
		let conn = self.rpc.cancel.token();
		// This stream's own canceller is raced alongside the connection's: a
		// driver parked on a full outbound channel is exactly the state a
		// `query_cancel` has to be able to break, and closing the items
		// channel cannot do it. Without this, a stopped stream would stay
		// parked -- pinning its execution and read snapshot -- until the
		// connection itself went away, and with no configured query timeout
		// there is no deadline to fall back on.
		//
		// The frames that bracket the stream are exempt. A cancelled stream is
		// precisely one that still has to tell its client it ended, and a
		// client that is told a stream ended without ever being told it began
		// cannot reconcile the two -- so `Begin` and `End` are raced only
		// against the connection going away, under the terminal grace above.
		let stream = self.cancel.token();
		let brackets = is_end || is_begin;
		let send = async {
			tokio::select! {
				biased;
				_ = conn.cancelled() => false,
				_ = stream.cancelled(), if !brackets => false,
				sent = self.chn.send(message) => sent.is_ok(),
			}
		};
		// Whichever comes first: the exchange's own deadline, or the per-frame
		// send timeout. The latter is what always applies -- the wall-clock
		// query timeout is off by default, and without a bound here a client
		// that stops reading its socket without closing it parks this send
		// forever, holding an unpolled execution and its transaction open. The
		// ping that would otherwise notice cannot help: it sends on this same
		// full channel, so it never reaches its own failure path.
		let send_timeout = Duration::from_secs(*WEBSOCKET_STREAM_SEND_TIMEOUT_SECS);
		let now = Instant::now();
		// Saturating, so no configured value can turn a deadline into a panic.
		let send_deadline = now.checked_add(send_timeout).unwrap_or(now);
		// The terminal frame's floor wins where it is later, so the frame whose
		// loss cannot be recovered from is never given the smaller window.
		let deadline = match self.deadline {
			Some(deadline) if is_end => deadline.max(send_deadline),
			Some(deadline) => deadline.min(send_deadline),
			None => send_deadline,
		};
		// A send that outlives its deadline counts as failed.
		let sent = tokio::time::timeout_at(deadline, send).await.unwrap_or_default();
		if sent {
			// A delivered terminal frame is an answer the client holds: from
			// here the statement can no longer be retracted.
			if let Some(index) = terminal {
				frames.mark_delivered(index);
			}
		} else {
			self.stop_sending();
		}
	}

	/// Frames can no longer reach the client: stop the execution and drop
	/// everything still queued. The driver keeps draining until the execution
	/// completes, so its transaction is finalised on its normal path. The
	/// tripped handle is also what makes the terminal `End` carry an error, so
	/// a stopped stream is never presented as a complete one.
	fn stop_sending(&mut self) {
		self.cancel.trip();
		self.items.close();
		self.muted = true;
	}

	/// The stream cannot be answered correctly: stop it, and record the reason
	/// so the terminal frame carries it rather than the generic stop error.
	fn fail_stream(&mut self, error: TypesError) {
		self.failure = Some(error);
		self.stop_sending();
	}

	/// Settle the live queries this execution registered.
	///
	/// A `KILL` needs nothing here: its statement's result never carries the id,
	/// so there is nothing to act on, and the subscription it ends unregisters
	/// through the `Action::Killed` notification that reaches the transport.
	///
	/// One policy for every outcome, because the client's view is what decides
	/// it:
	///
	/// - An id the client was told about is registered, so notifications reach it and `kill` and
	///   the connection's disconnect cleanup can find it.
	/// - An id the client never learned is deleted: nothing owns it, no notification could be
	///   delivered for it, and disconnect cleanup cannot find it, since only registered live
	///   queries are reachable there.
	/// - An id the client was told about that nonetheless has to be deleted -- its session was
	///   detached mid-query, so registering it would deliver change data under an authorization
	///   that has been torn down -- is named on the terminal frame. That statement finished
	///   successfully on the wire, and an errored `End` retracts only statements without a
	///   `Finished` frame, so naming the id is what lets the client drop the subscription it holds.
	///
	/// `results` is the execution's own report, authoritative once it has
	/// completed because a registration inside a transaction block is not real
	/// until the block commits. A run that failed as a whole has no results, and
	/// the ids the stream itself carried stand in.
	///
	/// Returns the ids the client holds that were nonetheless discarded.
	async fn settle_live_queries(
		&self,
		results: Option<&[QueryResult]>,
		frames: &StreamFrames,
	) -> Vec<Uuid> {
		let live: Vec<(usize, Uuid)> = match results {
			Some(results) => results
				.iter()
				.enumerate()
				.filter(|(_, r)| matches!(r.query_type, QueryType::Live))
				.filter_map(|(index, r)| match &r.result {
					Ok(Value::Uuid(id)) => Some((index, id.into_inner())),
					_ => None,
				})
				.collect(),
			None => frames.live_queries.clone(),
		};
		if live.is_empty() {
			return Vec::new();
		}
		// One lookup, and the guard dropped before the hooks are called:
		// `handle_live` must not be called with the session lock held, and a
		// write queued between two reads on a write-preferring lock would
		// deadlock both.
		let session = match self.rpc.get_session(&self.session_id).await {
			Ok(lock) => {
				let session = lock.read().await;
				Some((session.ns.clone(), session.db.clone()))
			}
			Err(_) => None,
		};
		let mut orphans = Vec::new();
		let mut disowned = Vec::new();
		for (index, id) in &live {
			let told = frames.delivered.contains(index);
			match &session {
				Some((namespace, database)) if told => {
					self.rpc
						.handle_live(id, self.session_id, namespace.clone(), database.clone())
						.await;
				}
				_ => {
					if told {
						disowned.push(*id);
					}
					orphans.push(*id);
				}
			}
		}
		if !orphans.is_empty()
			&& let Err(err) = self.rpc.kvs().delete_queries(orphans).await
		{
			error!("Error cleaning up the live queries of a streaming query: {err}");
		}
		disowned
	}
}

/// Turns a streaming execution's items into wire frames.
///
/// Holds one statement's rows back only until they fill a frame, so the wire
/// sees them long before the statement — or the query — has finished. Separate
/// from the driver because none of these decisions depend on how the items
/// arrive: a statement's rows are framed the same whether they were produced
/// one batch at a time or all at once.
///
/// Frames are produced one at a time, on demand, and never accumulated: a
/// single item routinely carries a whole statement's result — every sort and
/// aggregate operator emits one batch, as does any statement answered by the
/// legacy evaluator — and pre-framing all of it would hold thousands of row
/// vectors at once and defeat the backpressure the bounded channels provide.
/// Because a statement's rows are taken from the front of a queue rather than
/// drained out of a vector, framing an item costs time linear in its size.
struct StreamFrames {
	/// Everything each statement still has to emit.
	pending: HashMap<usize, PendingStatement>,
	/// The statements with work outstanding, in the order they were first seen,
	/// which is the order their frames go out in.
	order: VecDeque<usize>,
	/// Statements already terminated, so a second attempt is ignored.
	///
	/// Two things can end a statement: its own `Finished` item, and a value
	/// this server could not encode. Whichever happens first wins and the
	/// other is dropped — sending both would put a frame after a terminal one,
	/// which the protocol forbids.
	terminated: HashSet<usize>,
	/// Statements whose terminal frame has actually reached the client. A
	/// statement in here can no longer be retracted: its outcome is something
	/// the client has already been told. It is also what the terminal `End`
	/// counts, since that is the number a client can reconcile against the
	/// `Finished` frames it received.
	delivered: HashSet<usize>,
	/// The live queries this execution produced, each with the statement that
	/// produced it, so settlement can tell an id the client learned from one it
	/// never did.
	live_queries: Vec<(usize, Uuid)>,
}

/// One statement's frames on their way to the wire.
struct PendingStatement {
	/// Rows awaiting framing, taken from the front so that framing an item
	/// costs time linear in its size rather than quadratic.
	values: VecDeque<Value>,
	/// A single value awaiting its own frame. Emitted before anything else the
	/// statement produces.
	single: Option<Value>,
	/// Records to accumulate before emitting a frame, doubling per frame up to
	/// [`QUERY_BATCH_RECORDS`]. Never zero, which is what lets the emission
	/// loop make progress.
	target: usize,
	/// Set when the statement's value is a single value rather than a list.
	///
	/// This decides the `Finished` frame's `single` flag, and it cannot be
	/// inferred from the count: a `SELECT` returning one row is still a
	/// one-element array, where `SELECT ONLY` returning one row is that row.
	is_single: bool,
	/// The uuid a single-value statement produced, kept until the statement
	/// finishes so a `LIVE SELECT`'s id can be read back there — the point at
	/// which the statement's kind is known.
	live_query: Option<surrealdb_types::Uuid>,
	/// The statement's outcome, once known. Its `Finished` frame goes out after
	/// everything else the statement owes.
	closing: Option<(Duration, QueryType, Option<TypesError>)>,
}

impl Default for PendingStatement {
	fn default() -> Self {
		Self {
			values: VecDeque::new(),
			single: None,
			target: QUERY_FIRST_BATCH_RECORDS,
			is_single: false,
			live_query: None,
			closing: None,
		}
	}
}

impl StreamFrames {
	fn new() -> Self {
		Self {
			pending: HashMap::new(),
			order: VecDeque::new(),
			terminated: HashSet::new(),
			delivered: HashSet::new(),
			live_queries: Vec::new(),
		}
	}

	/// The entry for `index`, registering it in emission order the first time.
	fn entry(&mut self, index: usize) -> &mut PendingStatement {
		if !self.pending.contains_key(&index) {
			self.order.push_back(index);
		}
		self.pending.entry(index).or_default()
	}

	/// The next frame, or `None` when nothing is ready.
	///
	/// At most one frame is built per call, so a statement's rows reach the wire
	/// a frame at a time and the caller's send is what paces production.
	fn pop(&mut self) -> Option<QueryStreamFrame> {
		for position in 0..self.order.len() {
			let index = self.order[position];
			let Some(entry) = self.pending.get_mut(&index) else {
				continue;
			};
			// A single value is not a list, so it is not batched: one frame
			// carries it whole.
			if let Some(value) = entry.single.take() {
				return Some(QueryStreamFrame::Value {
					index,
					value,
				});
			}
			// A full frame's worth while the statement is open; whatever is
			// left once its outcome is known.
			let closing = entry.closing.is_some();
			if entry.values.len() >= entry.target || (closing && !entry.values.is_empty()) {
				let take = entry.target.min(entry.values.len());
				let values: Vec<Value> = entry.values.drain(..take).collect();
				// Ramp toward the maximum, so the small frames that make the
				// first row arrive early do not become a per-frame cost on a
				// long result.
				entry.target = (entry.target * 2).min(QUERY_BATCH_RECORDS);
				return Some(QueryStreamFrame::Rows {
					index,
					values,
				});
			}
			if let Some((time, query_type, error)) = entry.closing.take() {
				let single = error.is_none() && entry.is_single;
				self.pending.remove(&index);
				self.order.remove(position);
				return Some(QueryStreamFrame::Finished {
					index,
					time,
					query_type,
					single,
					error,
				});
			}
		}
		None
	}

	/// Fold one item into what its statement owes the wire.
	fn absorb(&mut self, item: QueryStreamItem) {
		// A statement this server already terminated — because a value of its
		// own could not be encoded — takes nothing further. Its rows would
		// otherwise be framed after its terminal frame, which the protocol
		// forbids and a client reading rows as they arrive would act on.
		if self.terminated.contains(&item.index()) {
			return;
		}
		match item {
			QueryStreamItem::Rows {
				index,
				values,
			} => self.entry(index).values.extend(values),
			QueryStreamItem::Value {
				index,
				value,
			} => {
				let entry = self.entry(index);
				entry.is_single = true;
				// Only a `LIVE SELECT`'s id is ever read back from here, so
				// only a uuid is worth keeping: retaining every single
				// statement's value would double the peak memory of the path
				// built to bound it.
				if let Value::Uuid(id) = &value {
					entry.live_query = Some(*id);
				}
				entry.single = Some(value);
			}
			QueryStreamItem::Finished {
				index,
				time,
				query_type,
				error,
			} => {
				// A `LIVE SELECT`'s id rides the stream as this statement's
				// single value, and this is where that statement is known to
				// have produced one.
				if matches!(query_type, QueryType::Live)
					&& error.is_none()
					&& let Some(id) = self.pending.get(&index).and_then(|p| p.live_query)
				{
					self.live_queries.push((index, id.into_inner()));
				}
				self.finished(index, time, query_type, error);
			}
		}
	}

	/// Record a statement's outcome. Its `Finished` frame goes out once the
	/// statement has emitted everything else it owes.
	fn finished(
		&mut self,
		index: usize,
		time: Duration,
		query_type: QueryType,
		error: Option<TypesError>,
	) {
		if !self.terminated.insert(index) {
			// Already terminated; see `terminated`.
			return;
		}
		let failed = error.is_some();
		let entry = self.entry(index);
		// A failed statement's rows are retracted by its error, so nothing
		// residual goes out for it.
		if failed {
			entry.values.clear();
			entry.single = None;
		}
		entry.closing = Some((time, query_type, error));
	}

	/// Fail a statement that has not yet been answered, retracting whatever it
	/// had produced. Returns whether the retraction could be made: a statement
	/// whose terminal frame the client already holds cannot be taken back, and
	/// the caller has to fail the whole stream instead.
	fn retract(&mut self, index: usize, error: TypesError) -> bool {
		if self.delivered.contains(&index) {
			return false;
		}
		// The statement may have been closed while its `Finished` frame was
		// still waiting to go out; that outcome has not been seen, so the
		// retraction takes its place.
		self.terminated.remove(&index);
		self.finished(index, Duration::ZERO, QueryType::Other, Some(error));
		true
	}

	/// Record that a statement's terminal frame reached the client.
	fn mark_delivered(&mut self, index: usize) {
		self.delivered.insert(index);
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_types::Number;

	use super::*;

	fn int(n: i64) -> Value {
		Value::Number(Number::Int(n))
	}

	fn rows(index: usize, n: usize) -> QueryStreamItem {
		QueryStreamItem::Rows {
			index,
			values: (0..n).map(|i| int(i as i64)).collect(),
		}
	}

	fn finished(index: usize) -> QueryStreamItem {
		QueryStreamItem::Finished {
			index,
			time: Duration::from_millis(1),
			query_type: QueryType::Other,
			error: None,
		}
	}

	fn drain(frames: &mut StreamFrames) -> Vec<QueryStreamFrame> {
		std::iter::from_fn(|| frames.pop()).collect()
	}

	/// The first frames are small so the first row arrives early, and they
	/// double toward the cap so a long result is not a per-frame tax.
	#[test]
	fn rows_ramp_from_small_frames_to_the_cap() {
		let mut frames = StreamFrames::new();
		frames.absorb(rows(0, 2048));
		frames.absorb(finished(0));
		let sizes: Vec<usize> = drain(&mut frames)
			.into_iter()
			.filter_map(|f| match f {
				QueryStreamFrame::Rows {
					values,
					..
				} => Some(values.len()),
				_ => None,
			})
			.collect();
		assert_eq!(sizes[..5], [16, 32, 64, 128, 256], "the ramp doubles to the cap");
		assert!(sizes[5..].iter().all(|s| *s <= QUERY_BATCH_RECORDS), "the cap holds");
		assert_eq!(sizes.iter().sum::<usize>(), 2048, "every row goes out exactly once");
	}

	/// A statement's terminal frame is terminal: nothing follows it, and a
	/// second attempt to finish it is dropped.
	#[test]
	fn nothing_follows_a_statements_terminal_frame() {
		let mut frames = StreamFrames::new();
		assert!(frames.retract(0, TypesError::internal("cannot encode".to_string())));
		frames.absorb(rows(0, 100));
		frames.absorb(finished(0));
		let produced = drain(&mut frames);
		assert_eq!(produced.len(), 1, "only the retraction: {produced:?}");
		assert!(matches!(
			produced[0],
			QueryStreamFrame::Finished {
				index: 0,
				error: Some(_),
				..
			}
		));
		assert_eq!(
			frames.terminated.len(),
			1,
			"a statement terminates exactly once in the result count",
		);
	}

	/// A retraction takes back what the client has not seen: rows still
	/// queued for that statement are dropped, and a success finish that was
	/// queued but never delivered is replaced by the failure.
	#[test]
	fn a_retraction_drops_that_statements_queued_frames() {
		let mut frames = StreamFrames::new();
		frames.absorb(rows(0, 16));
		frames.absorb(rows(1, 16));
		frames.absorb(finished(0));
		// Nothing has been sent yet: statement 0's rows and its success finish
		// are both still queued, and the retraction must replace both.
		assert!(frames.retract(0, TypesError::internal("cannot encode".to_string())));
		let produced = drain(&mut frames);
		let for_zero: Vec<&QueryStreamFrame> = produced
			.iter()
			.filter(|f| {
				matches!(
					f,
					QueryStreamFrame::Rows {
						index: 0,
						..
					} | QueryStreamFrame::Finished {
						index: 0,
						..
					}
				)
			})
			.collect();
		assert_eq!(for_zero.len(), 1, "only the retraction survives: {for_zero:?}");
		assert!(matches!(
			for_zero[0],
			QueryStreamFrame::Finished {
				error: Some(_),
				..
			}
		));
		// Statement 1 is untouched.
		assert!(produced.iter().any(|f| matches!(
			f,
			QueryStreamFrame::Rows {
				index: 1,
				..
			}
		)));
	}

	/// An answer the client already holds cannot be taken back: a retraction
	/// against a delivered statement is refused so the caller fails the whole
	/// stream instead of reporting a truncated statement as complete.
	#[test]
	fn a_delivered_statement_cannot_be_retracted() {
		let mut frames = StreamFrames::new();
		frames.absorb(rows(0, 4));
		frames.absorb(finished(0));
		drain(&mut frames);
		frames.mark_delivered(0);
		assert!(
			!frames.retract(0, TypesError::internal("too late".to_string())),
			"a delivered terminal frame cannot be unsaid",
		);
		assert!(drain(&mut frames).is_empty(), "nothing is queued after a refused retraction");
	}

	/// A failed statement's residual rows are retracted, not flushed.
	#[test]
	fn a_failed_statement_retracts_its_residue() {
		let mut frames = StreamFrames::new();
		frames.absorb(rows(0, 10));
		frames.absorb(QueryStreamItem::Finished {
			index: 0,
			time: Duration::ZERO,
			query_type: QueryType::Other,
			error: Some(TypesError::internal("boom".to_string())),
		});
		let produced = drain(&mut frames);
		assert_eq!(produced.len(), 1, "no rows frame precedes the failure: {produced:?}");
		assert!(matches!(
			produced[0],
			QueryStreamFrame::Finished {
				error: Some(_),
				single: false,
				..
			}
		));
	}

	/// A single value goes out whole and its statement finishes `single`, so
	/// the client does not rebuild an array around it; residue below the ramp
	/// threshold goes out with the statement's finish.
	#[test]
	fn single_values_and_small_residues_complete_their_statement() {
		let mut frames = StreamFrames::new();
		frames.absorb(QueryStreamItem::Value {
			index: 0,
			value: int(7),
		});
		frames.absorb(finished(0));
		frames.absorb(rows(1, 3));
		frames.absorb(finished(1));
		let produced = drain(&mut frames);
		assert!(matches!(
			produced[0],
			QueryStreamFrame::Value {
				index: 0,
				..
			}
		));
		assert!(matches!(
			produced[1],
			QueryStreamFrame::Finished {
				index: 0,
				single: true,
				error: None,
				..
			}
		));
		assert!(
			matches!(
				&produced[2],
				QueryStreamFrame::Rows {
					index: 1,
					values,
				} if values.len() == 3
			),
			"a residue below the first ramp target flushes on finish"
		);
		assert!(matches!(
			produced[3],
			QueryStreamFrame::Finished {
				index: 1,
				single: false,
				..
			}
		));
		assert_eq!(frames.terminated.len(), 2, "both statements count toward the result count");
	}

	/// Framing an item costs time linear in its size, and holds one frame at a
	/// time rather than pre-framing the whole result.
	///
	/// A single item routinely carries an entire statement's result — every
	/// sort and aggregate operator emits exactly one batch — so an item of
	/// hundreds of thousands of rows is an ordinary `ORDER BY`, not an
	/// adversarial input. Taking rows out of the front of a vector made that
	/// quadratic and wedged the worker thread doing it, since framing has no
	/// await point.
	///
	/// The measurement covers `absorb` *and* the `pop` loop, because that is
	/// the whole framing cost and either half may be where it lives: the
	/// implementation this replaced did the chunking eagerly inside `absorb`,
	/// so timing only `pop` would have looked linear no matter how bad `absorb`
	/// was. The assertion is on how the cost scales rather than on a wall-clock
	/// budget — doubling the rows should roughly double the work, where
	/// quadratic framing quadruples it — which holds regardless of how fast the
	/// machine is.
	#[test]
	fn framing_a_whole_result_is_linear_and_holds_one_frame() {
		/// Frames one item of `count` rows, timing everything, and returning
		/// how many rows and frames came out.
		fn frame(count: usize) -> (std::time::Duration, usize, usize) {
			let start = std::time::Instant::now();
			let mut frames = StreamFrames::new();
			frames.absorb(rows(0, count));
			frames.absorb(finished(0));
			let mut delivered = 0;
			let mut produced = 0;
			while let Some(frame) = frames.pop() {
				if let QueryStreamFrame::Rows {
					values,
					..
				} = &frame
				{
					assert!(values.len() <= QUERY_BATCH_RECORDS, "no frame exceeds the cap");
					delivered += values.len();
				}
				produced += 1;
			}
			(start.elapsed(), delivered, produced)
		}

		/// The best of several runs, so a page fault or a neighbouring test on
		/// a loaded machine cannot inflate the comparison.
		fn best(count: usize) -> (std::time::Duration, usize, usize) {
			(0..3).map(|_| frame(count)).min_by_key(|(elapsed, ..)| *elapsed).expect("a run")
		}

		// A four-fold spread rather than a doubling, because both shapes share a
		// large per-row cost (cloning each `Value`) that dilutes the difference:
		// linear framing lands near 4x, the quadratic form this replaced lands
		// near 16x, and the threshold between them leaves a wide margin on both
		// sides. A doubling put the two barely either side of the threshold and
		// could report a false result in either direction.
		let (small, delivered, produced) = best(150_000);
		let (large, delivered_large, _) = best(600_000);
		assert_eq!(delivered, 150_000, "every row goes out exactly once");
		assert_eq!(delivered_large, 600_000);
		assert!(produced > 150_000 / QUERY_BATCH_RECORDS, "the result is split across frames");
		let ratio = large.as_secs_f64() / small.as_secs_f64().max(f64::EPSILON);
		assert!(
			ratio < 8.0,
			"quadrupling the rows multiplied framing cost by {ratio:.1}x \
			 ({small:?} -> {large:?}), which is not linear",
		);
	}

	/// A statement's rows keep their order across the frames they are split
	/// into, which is what lets a client concatenate them.
	#[test]
	fn rows_keep_their_order_across_frames() {
		let mut frames = StreamFrames::new();
		frames.absorb(rows(0, 1000));
		frames.absorb(finished(0));
		let mut seen = Vec::new();
		while let Some(frame) = frames.pop() {
			if let QueryStreamFrame::Rows {
				values,
				..
			} = frame
			{
				seen.extend(values);
			}
		}
		let expected: Vec<Value> = (0..1000).map(|i| int(i as i64)).collect();
		assert_eq!(seen, expected, "rows arrive in the order the statement produced them");
	}

	/// Statements are framed independently: each keeps its own ramp, its rows
	/// precede its own terminal frame, and nothing follows it. The order
	/// statements take relative to each other is deliberately not pinned — the
	/// protocol leaves it free, and only each statement's own sequence matters.
	#[test]
	fn statements_are_framed_independently() {
		let mut frames = StreamFrames::new();
		frames.absorb(rows(0, 16));
		frames.absorb(rows(1, 16));
		frames.absorb(finished(1));
		frames.absorb(finished(0));
		let produced = drain(&mut frames);
		for index in [0, 1] {
			let sequence: Vec<&QueryStreamFrame> = produced
				.iter()
				.filter(|f| {
					matches!(f,
						QueryStreamFrame::Rows { index: i, .. }
						| QueryStreamFrame::Finished { index: i, .. } if *i == index)
				})
				.collect();
			assert!(
				matches!(sequence.last(), Some(QueryStreamFrame::Finished { .. })),
				"statement {index} ends with its terminal frame: {sequence:?}",
			);
			assert_eq!(
				sequence.iter().filter(|f| matches!(f, QueryStreamFrame::Finished { .. })).count(),
				1,
				"statement {index} terminates exactly once",
			);
			let rows: usize = sequence
				.iter()
				.filter_map(|f| match f {
					QueryStreamFrame::Rows {
						values,
						..
					} => Some(values.len()),
					_ => None,
				})
				.sum();
			assert_eq!(rows, 16, "statement {index} delivers every row exactly once");
		}
		assert_eq!(frames.terminated.len(), 2, "each statement terminates once");
	}
}
