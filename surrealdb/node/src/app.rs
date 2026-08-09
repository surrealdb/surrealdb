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
use surrealdb_embedded::{EmbeddedEngine, EncodedNotifications, Format, Options};

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
