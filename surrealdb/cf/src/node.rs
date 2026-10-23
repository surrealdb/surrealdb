//! The Rust API of the node: what a Rust host (a Worker built on workers-rs) drives, and what the
//! `js` exports wrap. Nothing here takes a JavaScript type, and every fallible call answers an
//! [`anyhow::Result`].

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use edge_tier::read::Tier;
use shale::object::ObjectStore;
use surrealdb_cnf::ConfigMap;
use surrealdb_core::kvs::{Datastore, TransactionBuilderFactory, TransactionBuilderParts};
use surrealdb_embedded::{BoundedHooks, EmbeddedEngine, FramedQueryFrames};
pub use surrealdb_embedded::Format;
use tokio_util::sync::CancellationToken;
use wasm_bindgen::prelude::wasm_bindgen;

use crate::log::{self, CommitHook, FrameApplied, LogBackend, Replica};
use crate::{heap_budget, heap_live, over_budget_message};

#[wasm_bindgen]
extern "C" {
	#[wasm_bindgen(js_namespace = console, js_name = error)]
	fn console_error(message: &str);
}

/// Report a panic to the console before the instance traps, so a host sees
/// the message rather than a bare `unreachable`. Idempotent.
pub fn install_panic_hook() {
	std::panic::set_hook(Box::new(|info| console_error(&info.to_string())));
}

/// The engine version the `version` RPC method reports.
pub fn version() -> String {
	EmbeddedEngine::version().to_owned()
}

/// An allocation-heavy benchmark, identical to the one in cf-experiments/calib/bench-wasm (a tiny
/// module of its own): the two copies compare how Workers runs the same code in a small module and
/// in this large one.
pub fn cpu_bench(n: u32) -> f64 {
	use std::collections::BTreeMap;
	let mut sum: u64 = 0;
	for i in 0..n {
		let mut m: BTreeMap<String, String> = BTreeMap::new();
		for f in 0..4u32 {
			m.insert(format!("field{f}"), format!("{}", i.wrapping_mul(7919).wrapping_add(f)));
		}
		for (k, v) in &m {
			sum = sum.wrapping_add(k.len() as u64).wrapping_add(v.as_bytes()[0] as u64);
		}
	}
	sum as f64
}

/// Any error, as an [`anyhow::Error`] carrying its message.
pub(crate) fn any(e: impl std::fmt::Display) -> anyhow::Error {
	anyhow::anyhow!("{e}")
}

/// Hands the datastore its storage: the log backend, already built.
struct Composer(RefCell<Option<LogBackend>>);

// SAFETY: single-threaded wasm host; see `log::AssertSend`.
unsafe impl Send for Composer {}
unsafe impl Sync for Composer {}

impl TransactionBuilderFactory for Composer {
	type RouterState = ();

	async fn new_transaction_builder(
		&self,
		_path: &str,
		_canceller: CancellationToken,
		_config: ConfigMap,
	) -> anyhow::Result<TransactionBuilderParts<()>> {
		let backend =
			self.0.borrow_mut().take().ok_or_else(|| anyhow::anyhow!("backend already taken"))?;
		Ok(TransactionBuilderParts::without_router_state(Box::new(backend)))
	}

	fn path_valid(&self, v: &str) -> anyhow::Result<String> {
		Ok(v.to_owned())
	}
}

/// One datastore over the sequenced log.
pub struct NodeCore {
	replica: Arc<Replica>,
	hook: Arc<CommitHook>,
	ds: RefCell<Option<Arc<Datastore>>>,
}

impl NodeCore {
	/// An empty node over `tier`, whose commits go to `commit`.
	pub fn new(commit: CommitHook, tier: Tier) -> anyhow::Result<Self> {
		let replica = crate::log::now(Replica::new(tier))
			.ok_or_else(|| anyhow::anyhow!("replica creation yielded"))?
			.map_err(any)?;
		Ok(NodeCore {
			replica: Arc::new(replica),
			hook: Arc::new(commit),
			ds: RefCell::new(None),
		})
	}

	/// An empty node whose tier is read through `store`, an object store of the host's own
	/// (the bucket the tier is in), under `prefix`. `page_cache` bytes of tree pages and
	/// `index_budget` bytes of segment index pages are held at most.
	pub fn with_store<S: ObjectStore + 'static>(
		commit: CommitHook,
		store: S,
		prefix: String,
		page_cache: usize,
		index_budget: usize,
	) -> anyhow::Result<Self> {
		Self::new(commit, Tier::with_store(store, prefix, page_cache, index_budget))
	}

	/// The replica under the datastore.
	pub fn replica(&self) -> &Arc<Replica> {
		&self.replica
	}

	/// The last log position applied.
	pub fn applied(&self) -> u64 {
		self.replica.applied()
	}

	/// Apply the packed write set of log position `pos`. `false` when the position was already
	/// applied.
	pub fn apply(&self, pos: u64, packed: &[u8]) -> anyhow::Result<bool> {
		self.replica.apply(pos, packed).map_err(any)
	}

	/// Apply a whole batch frame in one transaction (see `Replica::apply_frame`). `cutoff`: the
	/// first position the frame's epoch was fenced at (`u64::MAX` when it was not).
	pub fn apply_frame(&self, frame: &[u8], cutoff: u64) -> anyhow::Result<FrameApplied> {
		self.replica.apply_frame(frame, cutoff).map_err(any)
	}

	/// The R2 objects a batch frame's pointer records name: the host fetches them (or has them,
	/// for its own commits) and applies the frame with [`Self::apply_frame_resolved`].
	pub fn frame_pointers(frame: &[u8]) -> anyhow::Result<Vec<String>> {
		Replica::frame_pointers(frame).map_err(any)
	}

	/// [`Self::apply_frame`] for a frame holding pointer records: `objects[i]` is the object
	/// named `objects[i].0`, each pointer record's writes read from it.
	pub fn apply_frame_resolved(
		&self,
		frame: &[u8],
		cutoff: u64,
		objects: &[(String, Vec<u8>)],
	) -> anyhow::Result<FrameApplied> {
		self.replica.apply_frame_resolved(frame, cutoff, objects).map_err(any)
	}

	/// [`Self::apply_frame_resolved`], leaving out of memory the section records at the positions in
	/// `skip` (see `Replica::apply_frame_skipping`).
	pub fn apply_frame_skipping(
		&self,
		frame: &[u8],
		cutoff: u64,
		objects: &[(String, Vec<u8>)],
		skip: &std::collections::HashSet<u64>,
	) -> anyhow::Result<FrameApplied> {
		self.replica.apply_frame_skipping(frame, cutoff, objects, skip).map_err(any)
	}

	/// Lay the sections a run's footer lists as one layer, applying nothing (see
	/// `Replica::register_sections`). Each entry is `(position, offset, length, meta)`.
	pub fn register_sections(&self, run_key: &str, entries: &[(u64, u32, u32, &[u8])]) -> anyhow::Result<()> {
		self.replica.register_sections(run_key, entries).map_err(any)
	}

	/// Keep read snapshots of the last `n` positions applied, so queries can be run exactly as of
	/// any of them (`ConnectionCore::execute_at`). 0 keeps none.
	pub fn keep_snapshots(&self, n: usize) {
		self.replica.keep_snapshots(n);
	}

	/// [`Self::keep_snapshots`], with the held snapshots pinning at most `bytes` of applied log
	/// together (see `Replica::keep_snapshots_within`).
	pub fn keep_snapshots_within(&self, n: usize, bytes: u64) {
		self.replica.keep_snapshots_within(n, bytes);
	}

	/// `(oldest, newest)` positions a pinned query can read now, or `None` when no snapshot is
	/// held.
	pub fn snapshot_range(&self) -> Option<(u64, u64)> {
		self.replica.snapshot_range()
	}

	/// Start an empty node from the published tier (the tree and the sealed segments): the
	/// position it now stands at, from which the host applies the log.
	pub async fn open_tier(&self) -> anyhow::Result<u64> {
		self.replica.open_tier().await.map_err(any)
	}

	/// Adopt the published manifest if it is newer and this node has applied what it reflects;
	/// the sealed position read after.
	pub async fn adopt_published(&self) -> anyhow::Result<u64> {
		self.replica.adopt_published().await.map_err(any)
	}

	/// Layers this node holds above the sealed tier: ingested objects, and one per rolled run.
	pub fn ingested_layers(&self) -> usize {
		self.replica.ingested_len()
	}

	/// Commits the layers hold in place (the sections of the rolled runs, and ingested objects).
	pub fn layer_sections(&self) -> usize {
		self.replica.layer_sections()
	}

	/// A run holding sections was rolled: swap the memory of the commits it holds for layers over
	/// it (see `Replica::roll_swap`).
	pub fn roll_swap(&self, run_key: &str, secs: &[(u64, u32, u32)]) -> anyhow::Result<crate::log::Swapped> {
		self.replica.roll_swap(run_key, secs).map_err(any)
	}

	/// Start from a run's footer: register its sections as layers, applying nothing into memory
	/// (see `Replica::layer_run`). Each entry is `(position, offset, length, meta)`.
	pub fn layer_run(&self, run_key: &str, first: u64, last: u64, entries: &[(u64, u32, u32, &[u8])]) -> anyhow::Result<FrameApplied> {
		self.replica.layer_run(run_key, first, last, entries).map_err(any)
	}

	/// Bytes of section commits applied into memory and not yet swapped for layers.
	pub fn live_bytes(&self) -> u64 {
		self.replica.live_bytes()
	}

	/// Section commits applied into memory and not yet swapped.
	pub fn live_sections(&self) -> usize {
		self.replica.live_sections()
	}

	/// What suffix rebuilds did: `(done, refused for heap, swapped commits whose entries wait for the
	/// next rebuild, their bytes, bytes the suffix's store holds)`.
	pub fn rebuild_stats(&self) -> (u64, u64, usize, u64, u64) {
		let (done, refused, pending, bytes) = self.replica.rebuild_stats();
		(done, refused, pending, bytes, self.replica.suffix_bytes())
	}

	/// Bytes the layers' metas take.
	pub fn layer_bytes(&self) -> usize {
		self.replica.layer_bytes()
	}

	/// JSON: the view read (watermark, sealed position), the suffix's size, and the tier's reads.
	pub fn tier_stats(&self) -> String {
		let v = self.replica.view();
		let s = *self.replica.tier.stats.lock().expect("stats");
		format!(
			"{{\"watermark\":{},\"sealedThrough\":{},\"suffixKeys\":{},\"segments\":{},\"indexPagesFetched\":{},\"recordsFetched\":{},\"recordCacheHits\":{},\"footersFetched\":{}}}",
			v.watermark,
			v.sealed_through,
			self.replica.suffix_len().unwrap_or(0),
			s.segments,
			s.index_pages_fetched,
			s.records_fetched,
			s.record_cache_hits,
			s.footers_fetched
		)
	}

	/// Open the datastore over what has been applied so far: initialise its version and node row
	/// if new, and create root user `user` with `pass` when there are no root users (none when
	/// `user` is empty). Writes go through the commit function, so the host must be able to
	/// commit by now.
	pub async fn start(&self, user: &str, pass: &str) -> anyhow::Result<()> {
		let backend = LogBackend {
			replica: Arc::clone(&self.replica),
			hook: Arc::clone(&self.hook),
		};
		let ds = Datastore::builder()
			.with_auth(true)
			.without_maintenance_tasks()
			.build_with_factory_path("cfedge", Composer(RefCell::new(Some(backend))))
			.await
			.map_err(|e| anyhow::anyhow!("{e:#}"))?;
		ds.check_version().await.map_err(|e| anyhow::anyhow!("{e:#}"))?;
		ds.bootstrap().await.map_err(|e| anyhow::anyhow!("{e:#}"))?;
		if !user.is_empty() {
			ds.initialise_credentials(user, pass).await.map_err(|e| anyhow::anyhow!("{e:#}"))?;
		}
		*self.ds.borrow_mut() = Some(ds);
		Ok(())
	}

	/// A connection signed in as a root owner, in namespace `ns` and database `db`: for running
	/// a fanned-out query's leg on behalf of a root session on another node.
	pub fn connect_root(&self, ns: &str, db: &str) -> anyhow::Result<ConnectionCore> {
		let ds = self.ds.borrow().clone().ok_or_else(|| anyhow::anyhow!("node not started"))?;
		let session = surrealdb_core::dbs::Session::owner().with_ns(ns).with_db(db).with_rt(true);
		Ok(ConnectionCore(EmbeddedEngine::attach_as(ds, session)))
	}

	/// A new client connection with a session of its own.
	pub fn connect(&self) -> anyhow::Result<ConnectionCore> {
		let ds = self.ds.borrow().clone().ok_or_else(|| anyhow::anyhow!("node not started"))?;
		Ok(ConnectionCore(EmbeddedEngine::attach(ds)))
	}

	/// A client connection whose session starts as `session`, which an earlier connection's
	/// [`ConnectionCore::session_json`] saved: signed in as the same user, in the same namespace
	/// and database, with the same variables. For a host that lost its connections' memory (a
	/// Durable Object evicted while its WebSockets stayed) and found them again.
	pub fn connect_session(&self, session: &[u8]) -> anyhow::Result<ConnectionCore> {
		let ds = self.ds.borrow().clone().ok_or_else(|| anyhow::anyhow!("node not started"))?;
		let session: surrealdb_core::dbs::Session = serde_json::from_slice(session).map_err(any)?;
		Ok(ConnectionCore(EmbeddedEngine::attach_as(ds, session.with_rt(true))))
	}
}

/// Install the cache signins check passwords through, or remove it with `None`: a verification
/// that succeeded is remembered by the cache, and the same password against the same stored hash
/// is not verified again. See [`PassVerifyCache`]. Per thread, which is all of a single-threaded
/// host. None is installed until a host does.
pub fn set_pass_cache(cache: Option<std::rc::Rc<dyn PassVerifyCache>>) {
	surrealdb_core::iam::verify::set_pass_cache(cache);
}

pub use surrealdb_core::iam::verify::PassVerifyCache;

/// A streaming query's reply envelopes, pulled one at a time. Pulling is the
/// backpressure: an envelope not pulled is a scan not advanced. Dropping the
/// handle before the end cancels the query.
pub struct FramesCore {
	frames: RefCell<Option<FramedQueryFrames>>,
	/// Every read reads this log position (see [`ConnectionCore::query_stream_at`]).
	pin: Option<u64>,
	/// The kind of the envelope `next` returned last.
	kind: std::cell::Cell<u8>,
}

impl FramesCore {
	fn new(frames: FramedQueryFrames, pin: Option<u64>) -> Self {
		FramesCore {
			frames: RefCell::new(Some(frames)),
			pin,
			kind: std::cell::Cell::new(9),
		}
	}

	/// The next envelope, or `None` once the stream has ended.
	pub async fn next(&self) -> anyhow::Result<Option<Vec<u8>>> {
		// Taken out across the await, so a concurrent call sees an ended stream
		// rather than a borrow panic.
		let Some(mut frames) = self.frames.borrow_mut().take() else {
			return Ok(None);
		};
		let next = match self.pin {
			Some(pos) => log::Pinned {
				pos,
				inner: Box::pin(frames.next()),
			}
			.await,
			None => frames.next().await,
		}
		.map_err(|e| anyhow::anyhow!("{e:#}"))?;
		self.kind.set(frames.last_kind());
		if next.is_some() {
			*self.frames.borrow_mut() = Some(frames);
		}
		Ok(next)
	}

	/// The kind of the envelope `next` returned last: 0 begin, 1 rows, 2 value, 3 finished,
	/// 4 end, 9 none (an envelope for a request that could not be framed).
	pub fn kind(&self) -> u8 {
		self.kind.get()
	}
}

/// A pause the host serves other work in, between batches of a bounded query's rows.
pub type Pause<'a> = &'a dyn Fn() -> Pin<Box<dyn Future<Output = ()>>>;

/// One client connection: a session over the node's datastore.
pub struct ConnectionCore(EmbeddedEngine);

impl ConnectionCore {
	/// A connection to a fresh in-memory engine with no log or tier under it: for measuring the
	/// engine alone in this module (the same build a node runs), against the native figures.
	pub async fn memory() -> anyhow::Result<Self> {
		let engine = EmbeddedEngine::connect("memory", surrealdb_embedded::Options::default())
			.await
			.map_err(|e| anyhow::anyhow!("{e:#}"))?;
		Ok(ConnectionCore(engine))
	}

	/// `(ns, db)` when the connection's session is a root owner with both selected, and `None`
	/// otherwise.
	pub async fn root_scope(&self) -> Option<(String, String)> {
		match self.0.root_scope().await {
			Some((Some(ns), Some(db))) => Some((ns, db)),
			_ => None,
		}
	}

	/// [`Self::execute`], except that a `query` whose reply would pass `max_bytes` is stopped
	/// there and answered with an error, having held at most about `max_bytes` of results (see
	/// `EmbeddedEngine::execute_bounded`).
	///
	/// `cancelled` is asked between batches of rows: true stops the query, whose reply is then
	/// an error. `pause` is awaited between batches: the host yields its thread there, so other
	/// clients' messages are served while a long statement runs.
	pub async fn execute_bounded(
		&self,
		format: Format,
		data: &[u8],
		max_bytes: usize,
		cancelled: Option<&dyn Fn() -> bool>,
		pause: Option<Pause<'_>>,
	) -> anyhow::Result<Vec<u8>> {
		let exhausted = || heap_live() > heap_budget();
		let hooks = BoundedHooks {
			cancelled,
			pause,
			exhausted: Some(&exhausted),
			exhausted_message: Some(&over_budget_message),
		};
		self.0
			.execute_bounded_with(format, data, max_bytes, &hooks)
			.await
			.map_err(|e| anyhow::anyhow!("{e:#}"))
	}

	/// [`Self::execute`] with every read reading log position `pos` exactly, from the
	/// snapshot the node holds of it. Fails (in the reply) if that snapshot is no longer held.
	///
	/// Not the bounded path: that one runs the query on a task of its own, where the pin, which
	/// holds only while this future is polled, would not reach. Callers bound the result size by
	/// what they ask for.
	pub async fn execute_at(&self, format: Format, data: &[u8], pos: u64) -> anyhow::Result<Vec<u8>> {
		let fut = self.0.execute_framed(format, data);
		log::Pinned {
			pos,
			inner: Box::pin(fut),
		}
		.await
		.map_err(|e| anyhow::anyhow!("{e:#}"))
	}

	/// Start a `query_stream` request in `format`, answered by envelopes of at most `max_bytes`
	/// each where the rows allow (see `EmbeddedEngine::query_stream_framed`).
	pub async fn query_stream(
		&self,
		format: Format,
		data: &[u8],
		max_bytes: usize,
	) -> anyhow::Result<FramesCore> {
		let frames = self
			.0
			.query_stream_framed(format, data, max_bytes)
			.await
			.map_err(|e| anyhow::anyhow!("{e:#}"))?;
		Ok(FramesCore::new(frames, None))
	}

	/// [`Self::query_stream`] with every read reading log position `pos` exactly, from the
	/// snapshot the node holds of it: the query runs inside the calls to `next`, where the pin
	/// is in force (a fan-out leg streaming its part of a coordinator's snapshot).
	pub async fn query_stream_at(
		&self,
		format: Format,
		data: &[u8],
		max_bytes: usize,
		pos: u64,
	) -> anyhow::Result<FramesCore> {
		let fut = self.0.query_stream_framed_inline(format, data, max_bytes);
		let frames = log::Pinned {
			pos,
			inner: Box::pin(fut),
		}
		.await
		.map_err(|e| anyhow::anyhow!("{e:#}"))?;
		Ok(FramesCore::new(frames, Some(pos)))
	}

	/// The connection's session as it is now (who it is signed in as, the namespace and database
	/// selected, the variables set), serialised as JSON for [`NodeCore::connect_session`].
	pub async fn session_json(&self) -> anyhow::Result<Vec<u8>> {
		let session = self.0.default_session_snapshot().await;
		serde_json::to_vec(&session).map_err(any)
	}

	/// Answer one RPC message in `format` with the reply a SurrealDB WebSocket server would send.
	pub async fn execute(&self, format: Format, data: &[u8]) -> anyhow::Result<Vec<u8>> {
		self.0.execute_framed(format, data).await.map_err(|e| anyhow::anyhow!("{e:#}"))
	}
}
