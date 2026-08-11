//! The NAPI surface over [`surrealdb_embedded::EmbeddedEngine`].
//!
//! Everything here is FFI: turning a JavaScript options object into
//! [`Options`], moving bytes across the boundary, and the lifetime of the
//! addon's handle. The database behaviour lives in `surrealdb-embedded`, shared
//! with `@surrealdb/wasm-native`.

use futures::StreamExt;
use napi::bindgen_prelude::{Error, Uint8Array};
use napi::tokio::sync::{Mutex, RwLock};
use napi_derive::napi;
use serde_json::{Value as JsValue, from_value};
use surrealdb_embedded::{
	EmbeddedEngine, EncodedNotifications, EncodedQueryFrames, Format, Options,
};

use crate::err::err_map;

/// An embedded SurrealDB instance, addressed over the RPC protocol.
///
/// The connection is taken out of the `Option` by [`SurrealNodeEngine::free`],
/// after which every method reports a closed engine rather than panicking —
/// the addon is built with `panic = "abort"`, so a panic here would take the
/// host process down with it.
#[napi]
pub struct SurrealNodeEngine(RwLock<Option<EmbeddedEngine>>);

/// The live-query notifications for a connection, drained one at a time.
///
/// Holds the engine's stream directly rather than forwarding it into a channel
/// of its own, so this connection's notifications sit in exactly one buffer —
/// the datastore's bounded channel — and a consumer that stops calling
/// [`Self::recv`] stops pulling from it rather than accumulating behind it.
///
/// The mutex makes the single-consumer contract explicit: concurrent `recv`
/// calls from JavaScript take their turn instead of interleaving pulls.
#[napi]
pub struct NotificationReceiver {
	stream: Mutex<EncodedNotifications>,
}

#[napi]
impl NotificationReceiver {
	/// The next notification, or `null` once the connection stops producing them.
	#[napi]
	pub async fn recv(&self) -> Result<Option<Uint8Array>, Error> {
		let next = self.stream.lock().await.next().await;
		Ok(next.map(|encoded| encoded.as_slice().into()))
	}
}

/// The frames answering one streaming query, drained one at a time.
///
/// The query is driven by a task of the engine's own, and this is the receiving
/// end of the frames it produces. Reading paces that task rather than driving it:
/// the channel holds one frame, so a reader that stops calling [`Self::next`]
/// stops the scan rather than letting it fill a buffer nobody is draining.
///
/// This outlives the engine that made it deliberately. The driving task holds the
/// datastore, so a `free()` racing an in-flight stream cannot pull the database
/// out from under a running query, and releasing the frames is what tells that
/// task its consumer has gone.
///
/// A consumer that reads to the terminal frame owes nothing further. One that
/// stops early should call [`Self::close`]: JavaScript has no destructors, so the
/// only other signal is the object being collected, which leaves the query parked
/// until then.
#[napi]
pub struct QueryStream {
	/// Taken by [`Self::close`], after which [`Self::next`] reports the stream
	/// ended. The mutex also makes the single-consumer contract explicit:
	/// concurrent `next` calls from JavaScript take their turn instead of
	/// interleaving pulls.
	frames: Mutex<Option<EncodedQueryFrames>>,
}

#[napi]
impl QueryStream {
	/// The next frame, or `null` once the terminal frame has been delivered or
	/// [`Self::close`] has been called.
	#[napi]
	pub async fn next(&self) -> Result<Option<Uint8Array>, Error> {
		let mut lock = self.frames.lock().await;
		let Some(frames) = lock.as_mut() else {
			return Ok(None);
		};
		let next = frames.next().await;
		Ok(next.map(|encoded| encoded.as_slice().into()))
	}

	/// Abandons the query, whether or not it has finished.
	///
	/// Releasing the frames is what the driving task observes: its next send
	/// fails, and it stops the execution and settles the transaction rather than
	/// leaving either in flight. Calling this twice is not an error, and calling
	/// it on a stream that already delivered its terminal frame does nothing.
	///
	/// What the execution had already done stands. The executor runs ahead of the
	/// consumer by a bounded buffer, so a statement can have run before its frames
	/// were ever read — abandoning a query is not a way to undo the part of it that
	/// writes.
	#[napi]
	pub async fn close(&self) {
		let _frames = self.frames.lock().await.take();
	}
}

#[napi]
impl SurrealNodeEngine {
	#[napi]
	pub async fn execute(&self, data: Uint8Array) -> Result<Uint8Array, Error> {
		let lock = self.0.read().await;
		let engine = lock.as_ref().ok_or_else(closed)?;
		// `Uint8Array` derefs to the bytes napi already holds, so the request
		// crosses the boundary without a copy.
		let out = engine.execute_encoded(WIRE_FORMAT, &data).await.map_err(err_map)?;
		Ok(out.as_slice().into())
	}

	/// Runs a query, answering with frames as the executor produces them rather
	/// than with every statement's result at once.
	///
	/// The request is the same envelope [`Self::execute`] takes. A failure
	/// before execution begins — a denied capability, a parse error, an unknown
	/// transaction — is thrown here and produces no frames at all; everything
	/// after that is carried on the frames themselves.
	#[napi]
	pub async fn query_stream(&self, data: Uint8Array) -> Result<QueryStream, Error> {
		let lock = self.0.read().await;
		let engine = lock.as_ref().ok_or_else(closed)?;
		let frames = engine.query_stream_encoded(WIRE_FORMAT, &data).await.map_err(err_map)?;
		Ok(QueryStream {
			frames: Mutex::new(Some(frames)),
		})
	}

	/// The live query notifications for this connection, encoded, drained with
	/// [`NotificationReceiver::recv`].
	#[napi]
	pub async fn notifications(&self) -> Result<NotificationReceiver, Error> {
		let lock = self.0.read().await;
		let engine = lock.as_ref().ok_or_else(closed)?;
		Ok(NotificationReceiver {
			stream: Mutex::new(engine.notifications().encoded(WIRE_FORMAT)),
		})
	}

	#[napi]
	pub async fn connect(
		endpoint: String,
		#[napi(ts_arg_type = "ConnectionOptions")] opts: Option<JsValue>,
	) -> Result<SurrealNodeEngine, Error> {
		let opts: Option<Options> = from_value::<Option<Options>>(JsValue::from(opts))?;
		let engine =
			EmbeddedEngine::connect(&endpoint, opts.unwrap_or_default()).await.map_err(err_map)?;
		Ok(SurrealNodeEngine(RwLock::new(Some(engine))))
	}

	#[napi]
	pub async fn export(&self, config: Option<Uint8Array>) -> Result<String, Error> {
		let lock = self.0.read().await;
		let engine = lock.as_ref().ok_or_else(closed)?;
		engine.export_encoded(WIRE_FORMAT, config.as_deref()).await.map_err(err_map)
	}

	#[napi]
	pub async fn import(&self, input: String) -> Result<(), Error> {
		let lock = self.0.read().await;
		let engine = lock.as_ref().ok_or_else(closed)?;
		engine.import(&input).await.map_err(err_map)
	}

	#[napi]
	pub fn version() -> Result<String, Error> {
		Ok(EmbeddedEngine::version().to_owned())
	}

	#[napi]
	pub async fn free(&self) {
		let _inner_opt = self.0.write().await.take();
	}
}

/// The format this addon speaks across the boundary.
///
/// CBOR because that is the codec the JavaScript SDK's engine interface uses,
/// shared with its WebSocket and HTTP engines. The engine itself is
/// format-agnostic, so this is the only line that decides it.
const WIRE_FORMAT: Format = Format::Cbor;

/// The error reported by every method once [`SurrealNodeEngine::free`] has
/// taken the connection.
fn closed() -> Error {
	Error::new(napi::Status::GenericFailure, "The engine has been closed")
}
