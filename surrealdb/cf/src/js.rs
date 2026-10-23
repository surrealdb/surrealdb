//! The `wasm-bindgen` exports: what a JavaScript host (the TypeScript Durable Object) drives the
//! node with. Each is a shim over [`crate::node`], converting the JavaScript types (`Function`,
//! `Array`, numbers for positions) at the edge. Behind the `js` feature, which is on by default.

use surrealdb_embedded::Format;
use wasm_bindgen::prelude::*;

use crate::log::{CommitHook, FrameApplied};
use crate::node::{ConnectionCore, FramesCore, NodeCore, Pause};

fn js_err(e: impl std::fmt::Display) -> JsError {
	JsError::new(&e.to_string())
}

/// An error with its causes, as the engine's own messages carry them.
fn js_err_chain(e: anyhow::Error) -> JsError {
	JsError::new(&format!("{e:#}"))
}

/// A frame's outcome as the number the host reads: the position applied through, -1 on a gap
/// (nothing applied), -2 for a fenced epoch's batch.
fn applied_code(a: FrameApplied) -> f64 {
	match a {
		FrameApplied::Applied(p) => p as f64,
		FrameApplied::Gap => -1.0,
		FrameApplied::Fenced => -2.0,
	}
}

/// A fence cutoff from the host: `Infinity` when the epoch was not fenced.
fn cutoff_of(cutoff: f64) -> u64 {
	if cutoff.is_finite() { cutoff as u64 } else { u64::MAX }
}

/// The heap in use, in bytes: for the host's stats and its own checks.
#[wasm_bindgen]
pub fn heap_live_bytes() -> f64 {
	crate::heap_live() as f64
}

/// Allocations and bytes allocated since the module started: `[count, bytes]`.
#[wasm_bindgen]
pub fn alloc_stats() -> Vec<f64> {
	let (count, bytes) = crate::alloc_counts();
	vec![count as f64, bytes as f64]
}

/// Report a panic to the console before the instance traps, so a host sees
/// the message rather than a bare `unreachable`. Idempotent.
#[wasm_bindgen]
pub fn install_panic_hook() {
	crate::node::install_panic_hook();
}

/// The engine version the `version` RPC method reports.
#[wasm_bindgen]
pub fn version() -> String {
	crate::node::version()
}

/// An allocation-heavy benchmark, identical to the one in cf-experiments/calib/bench-wasm (a tiny
/// module of its own): the two copies compare how Workers runs the same code in a small module and
/// in this large one.
#[wasm_bindgen]
pub fn cpu_bench(n: u32) -> f64 {
	crate::node::cpu_bench(n)
}

/// The write set a pointer commit sends for `packed`, put to R2 as `object`: its keys with empty
/// values, and the unit naming the object (see `log::pointer_write_set`).
#[wasm_bindgen]
pub fn pointer_write_set(packed: &[u8], object: &str) -> Result<Vec<u8>, JsError> {
	crate::log::pointer_write_set(packed, object).map_err(js_err)
}

/// A large commit's object (see `ingest`): the packed write set, then its index, filter and
/// footer. The metadata starts where the write set ends (`packed.length`).
#[wasm_bindgen]
pub fn ingest_object(packed: &[u8]) -> Result<Vec<u8>, JsError> {
	crate::ingest::build(packed).map(|(object, _, _)| object).map_err(|e| js_err(format!("{e:#}")))
}

/// Ingested objects this node holds as layers (not yet in the sealed tier).
#[wasm_bindgen]
impl Node {
	pub fn ingested_layers(&self) -> usize {
		self.0.ingested_layers()
	}
}

/// A connection to a fresh in-memory engine with no log or tier under it: for measuring the
/// engine alone in this module (the same build a node runs), against the native figures.
#[wasm_bindgen]
pub async fn memory_connection() -> Result<Connection, JsError> {
	ConnectionCore::memory().await.map(Connection).map_err(js_err_chain)
}

/// One datastore over the sequenced log.
#[wasm_bindgen]
pub struct Node(NodeCore);

#[wasm_bindgen]
impl Node {
	/// An empty node. `commit` is `(snapshot: number, writes: Uint8Array) =>
	/// Promise<number>` (see [`CommitHook::from_js`]); `host` reaches the bucket the tier is in
	/// (see `edge_tier::store`), under `prefix`. `page_cache` bytes of tree pages and
	/// `index_budget` bytes of segment index pages are held at most.
	#[wasm_bindgen(constructor)]
	pub fn new(
		commit: js_sys::Function,
		host: js_sys::Object,
		prefix: String,
		page_cache: u32,
		index_budget: u32,
	) -> Result<Node, JsError> {
		let tier = edge_tier::read::Tier::new(host, prefix, page_cache as usize, index_budget as usize);
		NodeCore::new(CommitHook::from_js(commit), tier).map(Node).map_err(js_err)
	}

	/// The last log position applied.
	pub fn applied(&self) -> f64 {
		self.0.applied() as f64
	}

	/// Apply the packed write set of log position `pos`. `false` when the
	/// position was already applied.
	pub fn apply(&self, pos: f64, packed: &[u8]) -> Result<bool, JsError> {
		self.0.apply(pos as u64, packed).map_err(js_err)
	}

	/// Apply a whole batch frame in one transaction (see `Replica::apply_frame`). Resolves to
	/// the position applied through, -1 on a gap (nothing applied), -2 for a fenced epoch's batch.
	/// `cutoff`: the first position the frame's epoch was fenced at (`Infinity` when it was not).
	pub fn apply_frame(&self, frame: &[u8], cutoff: f64) -> Result<f64, JsError> {
		self.0.apply_frame(frame, cutoff_of(cutoff)).map(applied_code).map_err(js_err)
	}

	/// The R2 objects a batch frame's pointer records name: the host fetches them (or has them,
	/// for its own commits) and applies the frame with `apply_frame_resolved`.
	pub fn frame_pointers(&self, frame: &[u8]) -> Result<Vec<String>, JsError> {
		NodeCore::frame_pointers(frame).map_err(js_err)
	}

	/// [`Self::apply_frame`] for a frame holding pointer records: `objects[i]` is the object named
	/// `names[i]`, each pointer record's writes read from it.
	pub fn apply_frame_resolved(&self, frame: &[u8], cutoff: f64, names: Vec<String>, objects: js_sys::Array) -> Result<f64, JsError> {
		let objects: Vec<(String, Vec<u8>)> =
			names.into_iter().zip(objects.iter()).map(|(n, o)| (n, js_sys::Uint8Array::new(&o).to_vec())).collect();
		self.0.apply_frame_resolved(frame, cutoff_of(cutoff), &objects).map(applied_code).map_err(js_err)
	}

	/// Keep read snapshots of the last `n` positions applied, so queries can be
	/// run exactly as of any of them (`Connection::execute_at`). 0 keeps none.
	pub fn keep_snapshots(&self, n: usize) {
		self.0.keep_snapshots(n);
	}

	/// [`Self::keep_snapshots`], with the held snapshots pinning at most `bytes` of applied log
	/// together: older ones are discarded past it, the newest always kept.
	pub fn keep_snapshots_within(&self, n: usize, bytes: f64) {
		self.0.keep_snapshots_within(n, bytes as u64);
	}

	/// `[oldest, newest]` positions a pinned query can read now, or
	/// `undefined` when no snapshot is held.
	pub fn snapshot_range(&self) -> Option<Vec<f64>> {
		self.0.snapshot_range().map(|(a, b)| vec![a as f64, b as f64])
	}

	/// Start an empty node from the published tier (the tree and the sealed segments): resolves
	/// to the position it now stands at, from which the host applies the log.
	pub async fn open_tier(&self) -> Result<f64, JsError> {
		self.0.open_tier().await.map(|p| p as f64).map_err(js_err)
	}

	/// Adopt the published manifest if it is newer and this node has applied what it reflects;
	/// resolves to the sealed position read after.
	pub async fn adopt_published(&self) -> Result<f64, JsError> {
		self.0.adopt_published().await.map(|p| p as f64).map_err(js_err)
	}

	/// JSON: the view read (watermark, sealed position), the suffix's size, and the tier's reads.
	pub fn tier_stats(&self) -> String {
		self.0.tier_stats()
	}

	/// Open the datastore over what has been applied so far: initialise its
	/// version and node row if new, and create root user `user` with `pass`
	/// when there are no root users. Writes go through the commit function,
	/// so the host must be able to commit by now.
	pub async fn start(&self, user: String, pass: String) -> Result<(), JsError> {
		self.0.start(&user, &pass).await.map_err(js_err_chain)
	}

	/// A connection signed in as a root owner, in namespace `ns` and database
	/// `db`: for running a fanned-out query's leg on behalf of a root session
	/// on another node.
	pub fn connect_root(&self, ns: String, db: String) -> Result<Connection, JsError> {
		self.0.connect_root(&ns, &db).map(Connection).map_err(js_err)
	}

	/// A connection whose session starts as `session`, which [`Connection::session_json`] of an
	/// earlier connection saved: for a host that lost its connections' memory and found them again.
	pub fn connect_session(&self, session: &[u8]) -> Result<Connection, JsError> {
		self.0.connect_session(session).map(Connection).map_err(js_err)
	}

	/// A new client connection with a session of its own.
	pub fn connect(&self) -> Result<Connection, JsError> {
		self.0.connect().map(Connection).map_err(js_err)
	}
}

/// A streaming query's reply envelopes, pulled one at a time. Pulling is the
/// backpressure: an envelope not pulled is a scan not advanced. Freeing the
/// handle before the end cancels the query.
#[wasm_bindgen]
pub struct Frames(FramesCore);

#[wasm_bindgen]
impl Frames {
	/// The next envelope, or `undefined` once the stream has ended.
	pub async fn next(&self) -> Result<Option<Vec<u8>>, JsError> {
		self.0.next().await.map_err(js_err_chain)
	}

	/// The kind of the envelope `next` returned last: 0 begin, 1 rows, 2 value, 3 finished,
	/// 4 end, 9 none (an envelope for a request that could not be framed).
	pub fn kind(&self) -> u8 {
		self.0.kind()
	}
}

/// One client connection: a session over the node's datastore.
#[wasm_bindgen]
pub struct Connection(ConnectionCore);

#[wasm_bindgen]
impl Connection {
	/// The session as it is now (who it is signed in as, the namespace and database selected, the
	/// variables set), as JSON for [`Node::connect_session`].
	pub async fn session_json(&self) -> Result<Vec<u8>, JsError> {
		self.0.session_json().await.map_err(js_err)
	}

	/// `[ns, db]` when the connection's session is a root owner with both
	/// selected, and `undefined` otherwise.
	pub async fn root_scope(&self) -> Option<Vec<String>> {
		self.0.root_scope().await.map(|(ns, db)| vec![ns, db])
	}

	/// [`Self::execute`], except that a `query` whose reply would pass
	/// `max_bytes` is stopped there and answered with an error, having held at
	/// most about `max_bytes` of results (see `EmbeddedEngine::execute_bounded`).
	///
	/// `cancelled` (a JS function answering a boolean) is asked between batches of rows: true
	/// stops the query, whose reply is then an error. `pause` (a JS function answering a promise)
	/// is awaited between batches: the host yields its thread there, so other clients' messages
	/// are served while a long statement runs.
	pub async fn execute_bounded(
		&self,
		format: String,
		data: Vec<u8>,
		max_bytes: usize,
		cancelled: Option<js_sys::Function>,
		pause: Option<js_sys::Function>,
	) -> Result<Vec<u8>, JsError> {
		let is_cancelled = move || {
			cancelled
				.as_ref()
				.and_then(|f| f.call0(&JsValue::NULL).ok())
				.is_some_and(|v| v.is_truthy())
		};
		let pause_fn = move || -> std::pin::Pin<Box<dyn std::future::Future<Output = ()>>> {
			let promise = pause.as_ref().and_then(|f| f.call0(&JsValue::NULL).ok());
			Box::pin(async move {
				if let Some(p) = promise {
					let _ = wasm_bindgen_futures::JsFuture::from(js_sys::Promise::from(p)).await;
				}
			})
		};
		let pause_ref: Pause<'_> = &pause_fn;
		self.0
			.execute_bounded(Format::from(format.as_str()), &data, max_bytes, Some(&is_cancelled), Some(pause_ref))
			.await
			.map_err(js_err_chain)
	}

	/// [`Self::execute`] with every read reading log position `pos` exactly,
	/// from the snapshot the node holds of it. Fails (in the reply) if that
	/// snapshot is no longer held.
	///
	/// Not the bounded path: that one runs the query on a task of its own,
	/// where the pin, which holds only while this future is polled, would not
	/// reach. Callers bound the result size by what they ask for.
	pub async fn execute_at(&self, format: String, data: Vec<u8>, pos: f64) -> Result<Vec<u8>, JsError> {
		self.0.execute_at(Format::from(format.as_str()), &data, pos as u64).await.map_err(js_err_chain)
	}

	/// Start a `query_stream` request in `format`, answered by envelopes of at
	/// most `max_bytes` each where the rows allow (see
	/// `EmbeddedEngine::query_stream_framed`).
	pub async fn query_stream(
		&self,
		format: String,
		data: Vec<u8>,
		max_bytes: usize,
	) -> Result<Frames, JsError> {
		self.0
			.query_stream(Format::from(format.as_str()), &data, max_bytes)
			.await
			.map(Frames)
			.map_err(js_err_chain)
	}

	/// [`Self::query_stream`] with every read reading log position `pos` exactly, from the
	/// snapshot the node holds of it: the query runs inside the calls to `next`, where the pin
	/// is in force (a fan-out leg streaming its part of a coordinator's snapshot).
	pub async fn query_stream_at(
		&self,
		format: String,
		data: Vec<u8>,
		max_bytes: usize,
		pos: f64,
	) -> Result<Frames, JsError> {
		self.0
			.query_stream_at(Format::from(format.as_str()), &data, max_bytes, pos as u64)
			.await
			.map(Frames)
			.map_err(js_err_chain)
	}

	/// Answer one RPC message in `format` (`"cbor"` or `"json"`) with the
	/// reply a SurrealDB WebSocket server would send.
	pub async fn execute(&self, format: String, data: Vec<u8>) -> Result<Vec<u8>, JsError> {
		self.0.execute(Format::from(format.as_str()), &data).await.map_err(js_err_chain)
	}
}
