//! The wasm-bindgen surface over [`surrealdb_embedded::EmbeddedEngine`].

use futures::StreamExt;
use js_sys::Uint8Array;
use serde_wasm_bindgen::from_value;
use surrealdb_embedded::{EmbeddedEngine, Format, Options};
use wasm_bindgen::JsValue;
use wasm_bindgen::prelude::wasm_bindgen;
use wasm_streams::ReadableStream;
use wasm_streams::readable::sys;

use crate::err::Error;

mod types;

use self::types::TsConnectionOptions;

/// The format this module speaks across the boundary.
///
/// CBOR because that is the codec the JavaScript SDK's engine interface uses,
/// shared with its WebSocket and HTTP engines. The engine itself is
/// format-agnostic, so this is the only line that decides it.
const WIRE_FORMAT: Format = Format::Cbor;

/// An embedded SurrealDB instance, addressed over the RPC protocol.
///
/// wasm-bindgen generates a `free()` for this, which is what the JavaScript
/// side calls to close the connection; dropping the engine closes the
/// datastore's notification channel, which ends any stream handed out by
/// [`Self::notifications`].
#[wasm_bindgen]
pub struct SurrealWasmEngine(EmbeddedEngine);

#[wasm_bindgen]
impl SurrealWasmEngine {
	/// Every method takes `&self`. With `&mut self` the wasm-bindgen trampoline
	/// demands exclusive access and panics with "Unreachable code" whenever the
	/// engine is still considered borrowed — by the notification stream, or by
	/// an earlier call that has not resolved — before the body even runs.
	pub async fn execute(&self, data: Vec<u8>) -> Result<Vec<u8>, Error> {
		Ok(self.0.execute_encoded(WIRE_FORMAT, &data).await?)
	}

	/// The live-query notifications for this connection, encoded.
	///
	/// A `ReadableStream` rather than a channel of this module's own, so the
	/// notifications sit in exactly one buffer — the datastore's bounded
	/// channel — and a reader that stops pulling stops draining it rather than
	/// accumulating behind it.
	pub fn notifications(&self) -> sys::ReadableStream {
		let stream = self.0.notifications().encoded(WIRE_FORMAT).map(|encoded| {
			let bytes: Uint8Array = encoded.as_slice().into();
			Ok(JsValue::from(bytes))
		});

		ReadableStream::from_stream(stream).into_raw()
	}

	pub async fn connect(
		endpoint: String,
		opts: Option<TsConnectionOptions>,
	) -> Result<SurrealWasmEngine, Error> {
		// An options object that is present but empty deserializes fine;
		// `from_value` on `undefined` does not. That path runs a wasm-bindgen
		// closure that panics with "Unreachable code" under `panic = "abort"`,
		// so absent options never reach serde at all.
		let opts = match opts.map(JsValue::from) {
			Some(value) if !value.is_undefined() && !value.is_null() => {
				from_value::<Options>(value)?
			}
			_ => Options::default(),
		};

		Ok(SurrealWasmEngine(EmbeddedEngine::connect(&endpoint, opts).await?))
	}

	pub async fn export(&self, config: Option<Vec<u8>>) -> Result<String, Error> {
		Ok(self.0.export_encoded(WIRE_FORMAT, config.as_deref()).await?)
	}

	pub async fn import(&self, input: String) -> Result<(), Error> {
		Ok(self.0.import(&input).await?)
	}

	pub fn version() -> String {
		EmbeddedEngine::version().to_owned()
	}
}
