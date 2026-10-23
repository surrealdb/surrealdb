//! The wasm-bindgen surface over [`surrealdb_embedded::EmbeddedEngine`].

use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::StreamExt;
use futures::channel::oneshot;
use futures::lock::Mutex;
use js_sys::Uint8Array;
use serde_wasm_bindgen::from_value;
use surrealdb_embedded::{EmbeddedEngine, Format, Options};
use wasm_bindgen::JsValue;
use wasm_bindgen::prelude::wasm_bindgen;
use wasm_streams::ReadableStream;
use wasm_streams::readable::sys;

use crate::err::Error;

mod types;

/// The calls that use the engine and have not settled. Each holds the shared borrow
/// wasm-bindgen takes for an async method, so the engine is freed only once none is
/// left.
#[derive(Default)]
struct InFlight {
	count: AtomicUsize,
	waiters: StdMutex<Vec<oneshot::Sender<()>>>,
}

/// One call in flight, which leaves the count when it is dropped.
struct Call<'a>(&'a InFlight);

impl InFlight {
	fn enter(&self) -> Call<'_> {
		self.count.fetch_add(1, Ordering::AcqRel);
		Call(self)
	}

	/// Resolves once no call is in flight.
	async fn idle(&self) {
		loop {
			let receiver = {
				let mut waiters = self.waiters.lock().unwrap_or_else(|e| e.into_inner());
				if self.count.load(Ordering::Acquire) == 0 {
					return;
				}
				let (sender, receiver) = oneshot::channel();
				waiters.push(sender);
				receiver
			};
			let _ = receiver.await;
		}
	}
}

impl Drop for Call<'_> {
	fn drop(&mut self) {
		if self.0.count.fetch_sub(1, Ordering::AcqRel) == 1 {
			let mut waiters = self.0.waiters.lock().unwrap_or_else(|e| e.into_inner());
			for sender in waiters.drain(..) {
				let _ = sender.send(());
			}
		}
	}
}

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
/// side calls to dispose of the connection; dropping the engine closes the
/// datastore's notification channel, which ends any stream handed out by
/// [`Self::notifications`].
///
/// Dropping an engine that [`Self::close`] has not closed also closes its
/// datastore, on a task of its own, since a drop cannot wait: the storage lock
/// is released once that task finishes rather than by the time `free()`
/// returns. A caller that needs the lock released at a known point awaits
/// `close()` first.
#[wasm_bindgen]
pub struct SurrealWasmEngine {
	/// Always `Some` until the drop takes it to close the datastore.
	engine: Option<EmbeddedEngine>,
	/// Whether the datastore has been closed by [`Self::close`]. Held while the
	/// datastore shuts down, so a second caller waits for the same shutdown.
	closed: Mutex<bool>,
	/// The calls that use the engine and have not settled
	in_flight: InFlight,
}

impl SurrealWasmEngine {
	fn engine(&self) -> &EmbeddedEngine {
		self.engine.as_ref().expect("the engine is present until it is dropped")
	}
}

impl Drop for SurrealWasmEngine {
	fn drop(&mut self) {
		if *self.closed.get_mut() {
			return;
		}
		if let Some(engine) = self.engine.take() {
			wasm_bindgen_futures::spawn_local(async move {
				engine.shutdown().await;
			});
		}
	}
}

#[wasm_bindgen]
impl SurrealWasmEngine {
	/// Every method takes `&self`. With `&mut self` the wasm-bindgen trampoline
	/// demands exclusive access and panics with "Unreachable code" whenever the
	/// engine is still considered borrowed — by the notification stream, or by
	/// an earlier call that has not resolved — before the body even runs.
	pub async fn execute(&self, data: Vec<u8>) -> Result<Vec<u8>, Error> {
		let _call = self.in_flight.enter();
		Ok(self.engine().execute_encoded(WIRE_FORMAT, &data).await?)
	}

	/// Runs a query, answering with frames as the executor produces them rather
	/// than with every statement's result at once.
	///
	/// The request is the same envelope [`Self::execute`] takes. A failure
	/// before execution begins — a denied capability, a parse error, an unknown
	/// transaction — rejects here and produces no frames at all; everything
	/// after that is carried on the frames themselves.
	///
	/// A `ReadableStream` so the reader sets the pace: the query is driven by a
	/// task of the engine's own, and the frames it produces are buffered one at a
	/// time, so a reader that stops reading stops the scan. Cancelling the stream
	/// is how a consumer walks away.
	pub async fn query_stream(&self, data: Vec<u8>) -> Result<sys::ReadableStream, Error> {
		let _call = self.in_flight.enter();
		let frames = self.engine().query_stream_encoded(WIRE_FORMAT, &data).await?;
		let stream = frames.map(|encoded| {
			let bytes: Uint8Array = encoded.as_slice().into();
			Ok(JsValue::from(bytes))
		});

		Ok(ReadableStream::from_stream(stream).into_raw())
	}

	/// The live-query notifications for this connection, encoded.
	///
	/// A `ReadableStream` rather than a channel of this module's own, so the
	/// notifications sit in exactly one buffer — the datastore's bounded
	/// channel — and a reader that stops pulling stops draining it rather than
	/// accumulating behind it.
	pub fn notifications(&self) -> sys::ReadableStream {
		let stream = self.engine().notifications().encoded(WIRE_FORMAT).map(|encoded| {
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
		let opts = if let Some(value) = opts.map(JsValue::from)
			&& !value.is_undefined()
			&& !value.is_null()
		{
			from_value::<Options>(value)?
		} else {
			Options::default()
		};

		// A database in the origin private file system needs the synchronous
		// file handles that only a dedicated worker has, so the refusal is
		// made here, before the engine touches storage.
		let scheme = endpoint.split_once(':').map_or(endpoint.as_str(), |(scheme, _)| scheme);
		// Schemes are case-insensitive, as the URL parser the endpoint reaches lowercases them
		let scheme = scheme.to_ascii_lowercase();
		if matches!(scheme.as_str(), "surrealkv" | "rocksdb" | "indxdb") && !in_dedicated_worker() {
			return Err(Error::from(anyhow::anyhow!(
				"A `surrealkv://` database in the browser can be opened only from a dedicated \
				 worker: use `connect` from `@surrealdb/wasm-native/worker`, or load this module \
				 inside a dedicated worker yourself and talk to it from the page by message"
			)));
		}

		Ok(SurrealWasmEngine {
			engine: Some(EmbeddedEngine::connect(&endpoint, opts).await?),
			closed: Mutex::new(false),
			in_flight: InFlight::default(),
		})
	}

	/// Closes the datastore: the storage engine is closed and then the files
	/// it holds are released, which frees the database's lock in the origin
	/// private file system so that another tab or worker can open it.
	///
	/// A worker cannot await an asynchronous close from a page-unload
	/// handler, so the application calls this when it knows that it is ending,
	/// for example on a message from the page, and as early as it can: the
	/// more files the database has, the longer it takes. A database that is
	/// not closed is recovered the next time it is opened. Nothing may use
	/// the engine after this resolves, other than `free()`.
	pub async fn close(&self) {
		// Callers arriving during the shutdown wait here and then find it done,
		// so every `close()` resolves only once the storage lock is released.
		let mut closed = self.closed.lock().await;
		if !*closed {
			self.engine().shutdown().await;
			*closed = true;
		}
		// A call that was running when the close began has settled before the engine is freed
		self.in_flight.idle().await;
	}

	/// Whether the browser keeps this origin's storage until the user clears
	/// it, which is what stops it from evicting a `surrealkv://` database when
	/// the disk fills.
	///
	/// It is `false` until the page has asked with `navigator.storage.persist()`,
	/// which a worker cannot call. Always `false` for a build without
	/// `surrealkv://` storage, which keeps nothing.
	pub async fn persisted() -> Result<bool, Error> {
		#[cfg(feature = "kv-surrealkv")]
		{
			Ok(surrealdb_kvs_surrealkv::Datastore::persisted()
				.await
				.map_err(anyhow::Error::from)?)
		}
		#[cfg(not(feature = "kv-surrealkv"))]
		{
			Ok(false)
		}
	}

	pub async fn export(&self, config: Option<Vec<u8>>) -> Result<String, Error> {
		let _call = self.in_flight.enter();
		Ok(self.engine().export_encoded(WIRE_FORMAT, config.as_deref()).await?)
	}

	pub async fn import(&self, input: String) -> Result<(), Error> {
		let _call = self.in_flight.enter();
		Ok(self.engine().import(&input).await?)
	}

	pub fn version() -> String {
		EmbeddedEngine::version().to_owned()
	}
}

/// Whether this code runs in a dedicated worker, the only context that has
/// the synchronous file handles of the origin private file system.
fn in_dedicated_worker() -> bool {
	js_sys::Reflect::has(&js_sys::global(), &JsValue::from_str("DedicatedWorkerGlobalScope"))
		.unwrap_or(false)
}
