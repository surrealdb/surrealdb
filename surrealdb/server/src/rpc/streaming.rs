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

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::extract::ws::Message;
use surrealdb_core::channel::{Receiver, bounded};
use surrealdb_core::ctx::CancelHandle;
use surrealdb_core::dbs::AuthPrincipalSnapshot;
use surrealdb_core::rpc::{RpcProtocol, live_query_owner};
use surrealdb_rpc::capabilities::MethodTarget;
use surrealdb_rpc::error::{invalid_params, method_not_allowed, stream_exists, too_many_streams};
use surrealdb_rpc::framing::{live_queries_disowned, stream_stopped};
use surrealdb_rpc::{
	DbResponse, DbResult, Method, QUERY_STREAM_BUFFER, QueryResult, QueryStreamFrame,
	QueryStreamItem, QueryType, StreamFrames,
};
use surrealdb_types::{Array, Error as TypesError, ToSql, Value};
use tokio::sync::mpsc::Sender;
use tokio::time::Instant;
use uuid::Uuid;

use crate::cnf::{WEBSOCKET_MAX_CONCURRENT_STREAMS, WEBSOCKET_STREAM_SEND_TIMEOUT_SECS};
use crate::rpc::format::WsFormat;
use crate::rpc::websocket::Websocket;

/// How long the terminal `End` frame may wait for the outbound channel before
/// it is given up on.
///
/// Short on purpose: by the time it is sent the stream is over and its driver
/// is the only thing this delays, but a client that merely fell behind deserves
/// to learn its stream ended rather than waiting on a frame that never comes.
const TERMINAL_FRAME_GRACE: Duration = Duration::from_secs(1);

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

/// A claim on one of the connection's concurrent-stream slots, held for as
/// long as the stream it admits.
///
/// The claim is what [`WEBSOCKET_MAX_CONCURRENT_STREAMS`] is enforced by, and
/// it is taken *before* the registry grows rather than measured after: a check
/// that consults the registry's length can only notice an overshoot that has
/// already happened, so two requests racing under the last free slot would both
/// enter the registry, both then see it over the cap, and both back out —
/// correct in the end, but with the registry observably above the cap in
/// between. A claim that loses the race never grows anything.
struct StreamSlot<'a> {
	rpc: &'a Websocket,
}

impl<'a> StreamSlot<'a> {
	/// Claim a slot, or `None` when the connection already holds
	/// [`WEBSOCKET_MAX_CONCURRENT_STREAMS`] of them.
	fn claim(rpc: &'a Websocket) -> Option<Self> {
		rpc.stream_slots
			.fetch_update(Ordering::AcqRel, Ordering::Acquire, |held| {
				(held < *WEBSOCKET_MAX_CONCURRENT_STREAMS).then_some(held + 1)
			})
			.ok()
			.map(|_| Self {
				rpc,
			})
	}
}

impl Drop for StreamSlot<'_> {
	fn drop(&mut self) {
		self.rpc.stream_slots.fetch_sub(1, Ordering::AcqRel);
	}
}

/// Removes a stream's registry entry when the driver returns, on every path,
/// and gives back the slot that admitted it.
struct StreamRegistration<'a> {
	rpc: &'a Websocket,
	key: String,
	/// Released after the entry above has been removed — fields drop after the
	/// `Drop` body — so the freed slot is never handed to a request that could
	/// fill it while this one is still registered.
	_slot: StreamSlot<'a>,
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
	// `query_cancel` for this id has a handle to find, and both a duplicate id
	// and an over-cap request are refused before either does any work.
	let key = id.to_sql();
	let cancel = CancelHandle::new();
	let (items_tx, items_rx) = bounded(QUERY_STREAM_BUFFER);
	let handle = StreamHandle {
		cancel: cancel.clone(),
		items: items_rx.clone(),
	};
	// An id names one stream: a duplicate is refused, and the stream already
	// running under this id keeps its registration untouched. Claiming the slot
	// inside the vacant arm is what keeps admission and entry indivisible: the
	// registry only ever grows on a request the cap has already let through.
	// The claim is a wait-free atomic, so the shard guard it runs under is held
	// no longer than the insert it guards.
	//
	// The reservation's outcome is taken as a plain value so the map guard is
	// released before anything is awaited. A `DashMap` guard held across an
	// await would be catastrophic here: the guard is a blocking lock over a
	// whole shard, the await below is a send on a channel the client paces,
	// and every other task touching the registry — `query_cancel`, another
	// stream's deregistration — would block its worker thread rather than
	// yield.
	let reserved = match rpc.streams.entry(key.clone()) {
		dashmap::mapref::entry::Entry::Occupied(_) => Err(stream_exists()),
		dashmap::mapref::entry::Entry::Vacant(entry) => match StreamSlot::claim(rpc) {
			Some(slot) => {
				entry.insert(handle);
				Ok(slot)
			}
			None => Err(too_many_streams()),
		},
	};
	// A rejected request holds no reservation while it reports the rejection.
	// Reporting it is an awaited send on a channel the client paces, so on a
	// connection that has stopped reading it parks indefinitely -- and the read
	// loop keeps accepting messages, so every further request would park the
	// same way. A rejection that kept a slot or a registry entry while parked
	// would let a client grow both without bound precisely by refusing to read,
	// defeating the cap it was being refused by.
	let slot = match reserved {
		Ok(slot) => slot,
		Err(error) => {
			crate::rpc::response::send(
				DbResponse::failure(Some(id), client_session, error),
				fmt,
				chn,
			)
			.await;
			return;
		}
	};
	let registration = StreamRegistration {
		rpc,
		key,
		_slot: slot,
	};
	// The deadline for the whole exchange, when the operator configured one.
	// `query_stream` applies the same timeout to the execution, but that guard
	// only fires while the execution is being polled, and a stalled client is
	// exactly what stops it being polled — so the sends are bounded here too.
	let deadline = rpc.kvs().query_timeout().map(|timeout| Instant::now() + timeout);
	let (job, principal) = match RpcProtocol::query_stream(
		rpc.as_ref(),
		txn,
		session_id,
		params,
		Some(cancel.clone()),
		items_tx,
	)
	.await
	{
		Ok(started) => started,
		Err(error) => {
			// Released before the report, for the reason given at the
			// reservation above: a rejected request holds nothing while it
			// parks.
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
		principal,
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
	/// The principal the execution runs as, and the one its `LIVE SELECT`s
	/// belong to. The session may be acting as someone else by the time they
	/// are registered.
	principal: AuthPrincipalSnapshot,
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
			results: frames.delivered_count(),
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
			None => frames.live_queries().to_vec(),
		};
		if live.is_empty() {
			return Vec::new();
		}
		// The guard is held across every registration below, and the principal it
		// was taken under is the one the execution ran as. Both matter: a session
		// that merely still exists may have been invalidated, whose `cleanup_lqs`
		// swept a map these ids were not in yet, and releasing the guard between
		// the check and the insert would let the next teardown through the gap.
		// `handle_live` does not touch the session lock, which is what makes
		// holding it safe.
		let session =
			live_query_owner(self.rpc.session_map(), self.session_id, &self.principal).await;
		let mut orphans = Vec::new();
		let mut disowned = Vec::new();
		for (index, id) in &live {
			let told = frames.was_delivered(*index);
			match &session {
				Some(owner) if told => {
					self.rpc
						.handle_live(id, self.session_id, owner.ns.clone(), owner.db.clone())
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
		// Released before the deletions: nothing below registers anything, and
		// holding a session guard through a datastore write would block the
		// connection's other requests for no gain.
		drop(session);
		if !orphans.is_empty()
			&& let Err(err) = self.rpc.kvs().delete_queries(orphans).await
		{
			error!("Error cleaning up the live queries of a streaming query: {err}");
		}
		disowned
	}
}
