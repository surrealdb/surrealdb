//! A transaction wrapper that models an object-store LSM: every operation that
//! leaves the transaction's own write buffer is one storage round trip that
//! costs a configurable latency, and the wrapper counts the round trips a
//! statement waits for one after another (its critical path).
//!
//! # Cost model
//!
//! | class         | when it is a round trip                                          |
//! |---------------|------------------------------------------------------------------|
//! | `begin`       | `new_transaction`; counted, never slept                          |
//! | `get`         | `get`/`getu`/`exists` of a key the transaction has not written   |
//! | `getm`        | `getm`, one round trip whatever the key count                    |
//! | `range_first` | first chunk of a cursor, and every single-shot range operation   |
//! | `range_page`  | each later chunk of a cursor that has not reached the range end  |
//! | `check_write` | `put`/`putc`/`delc`/`clrc` of a key the transaction has not written |
//! | `blind_write` | `set`/`del`/`replace`/`clr`, buffered locally; counted, never slept |
//! | `commit`      | `commit` of a writable transaction                               |
//!
//! Savepoints, timestamps and cancel are local and are not counted. A chunk
//! that comes back short without its visitor stopping it has read to the end of
//! the range, as a page read learns, so later chunks of that cursor are free.
//!
//! The **serial round trip** count is the number of times the count of in-flight
//! slept operations goes from zero to one inside the measurement window: an
//! operation that starts while another is in flight overlaps it and adds
//! nothing. Overlap is only visible when the latency is non-zero.
//!
//! Every method of the wrapped transaction is forwarded, including those with
//! trait defaults, so the inner backend's native implementations run.

#![allow(clippy::unwrap_used)]

use std::any::{Any, TypeId};
use std::collections::HashSet;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::future::BoxFut;
use surrealdb_cnf::ConfigMap;
use surrealdb_kvs::api::{KeyVisitor, ScanChunkStats, ValVisitor};
use surrealdb_kvs::snapshot::ViewCapabilities;
use surrealdb_kvs::timestamp::{BoxTimeStamp, BoxTimeStampImpl};
use surrealdb_kvs::{
	Batch, CountCost, Direction, Estimate, GetMultiResult, Isolation, IsolationLevels,
	IsolationLevelsHandle, Key, KeyRange, KeysResult, Metrics, Page, RangeCursor, RangeKeyVisitor,
	Result as KvsResult, ScanCursorKeys, ScanCursorVals, ScanResult, Snapshot, SplitTarget,
	Transactable, TransactionBuilder, TransactionType, Val,
};
use tokio_util::sync::CancellationToken;

use crate::CommunityComposer;
use crate::kvs::{TransactionBuilderFactory, TransactionBuilderParts};

/// The kind of storage operation an access is, per the cost model above.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Class {
	Begin,
	Get,
	Getm,
	RangeFirst,
	RangePage,
	CheckWrite,
	BlindWrite,
	Commit,
}

/// Number of [`Class`] variants.
pub(super) const CLASSES: usize = 8;

impl Class {
	/// Whether an operation of this class waits for the injected latency.
	fn slept(self) -> bool {
		!matches!(self, Class::Begin | Class::BlindWrite)
	}

	pub(super) fn name(self) -> &'static str {
		match self {
			Class::Begin => "begin",
			Class::Get => "get",
			Class::Getm => "getm",
			Class::RangeFirst => "range_first",
			Class::RangePage => "range_page",
			Class::CheckWrite => "check_write",
			Class::BlindWrite => "blind_write",
			Class::Commit => "commit",
		}
	}
}

/// What a key addresses, derived from its sigil layout.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Category {
	Catalog,
	Record,
	Index,
	Graph,
	Ref,
	Other,
}

/// Number of [`Category`] variants.
pub(super) const CATEGORIES: usize = 6;

impl Category {
	pub(super) fn name(self) -> &'static str {
		match self {
			Category::Catalog => "catalog",
			Category::Record => "record",
			Category::Index => "index",
			Category::Graph => "graph",
			Category::Ref => "ref",
			Category::Other => "other",
		}
	}
}

/// The category of the keyspace a key (or the first key of a range) lies in.
///
/// Keys are `/` then `!` for a root definition, or `*` and a 4-byte namespace
/// id, after which `!` is a namespace definition and `*` opens the database
/// level: a 4-byte database id, then `!` for a database definition, `#`/`%`
/// for change feed and live events, `&` for grants, and `*` for a table, whose
/// name is NUL-terminated. The byte after the table name selects the table's
/// keyspace: `*` records, `~`/`^` graph edges, `+` indexes, `&` references,
/// `!` table definitions.
pub(super) fn categorise(key: &[u8]) -> Category {
	let Some(rest) = key.strip_prefix(b"/") else {
		return Category::Other;
	};
	match rest.first() {
		Some(b'!') => return Category::Catalog,
		Some(b'*') => {}
		_ => return Category::Other,
	}
	// `*` + namespace id, then the sigil of the namespace level.
	match rest.get(5) {
		Some(b'!') => return Category::Catalog,
		Some(b'*') => {}
		_ => return Category::Other,
	}
	// `*` + database id, then the sigil of the database level.
	match rest.get(10) {
		Some(b'!') => return Category::Catalog,
		Some(b'*') => {}
		_ => return Category::Other,
	}
	// `*` + NUL-terminated table name, then the sigil of the table level.
	let Some(name) = rest.get(11..) else {
		return Category::Other;
	};
	let Some(end) = name.iter().position(|b| *b == 0) else {
		return Category::Other;
	};
	match name.get(end + 1) {
		Some(b'*') => Category::Record,
		Some(b'~' | b'^') => Category::Graph,
		Some(b'+') => Category::Index,
		Some(b'&') => Category::Ref,
		Some(b'!') => Category::Catalog,
		_ => Category::Other,
	}
}

/// The capability answers the wrapper gives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Profile {
	/// Every capability as the inner backend answers it.
	Forward,
	/// As an object-store LSM declares: `multi_read` is `true`, every other
	/// capability is the inner backend's.
	Lsm,
}

/// One counted operation.
#[derive(Clone, Debug)]
pub(super) struct TraceEntry {
	pub(super) class: Class,
	pub(super) category: Category,
	/// The serial round-trip count when the operation started.
	pub(super) round: u64,
	/// Rows a range operation returned.
	pub(super) rows: Option<u64>,
	/// The key, or first key of the range, as escaped bytes cut to 96 chars.
	pub(super) key: String,
}

/// A copy of the counters at one moment.
#[derive(Clone, Debug, Default)]
pub(super) struct RoundTrips {
	/// Serial round trips.
	pub(super) rounds: u64,
	/// Operations per [`Class`], indexed by `Class as usize`.
	pub(super) per_class: [u64; CLASSES],
	/// Serial round trips per [`Category`] of the operation that opened each,
	/// indexed by `Category as usize`; sums to `rounds`.
	pub(super) per_category: [u64; CATEGORIES],
	/// `snapshot()` calls that returned a view.
	pub(super) views: u64,
}

impl RoundTrips {
	pub(super) fn class(&self, class: Class) -> u64 {
		self.per_class[class as usize]
	}

	pub(super) fn category(&self, category: Category) -> u64 {
		self.per_category[category as usize]
	}
}

/// Counters and settings shared by a datastore's wrapped transactions.
pub(super) struct LatencyStats {
	latency_us: AtomicU64,
	in_flight: AtomicU64,
	rounds: AtomicU64,
	per_class: [AtomicU64; CLASSES],
	per_category: [AtomicU64; CATEGORIES],
	views: AtomicU64,
	/// Commits of writable transactions that waited for the store's own sync.
	synced_commits: AtomicU64,
	enabled: AtomicBool,
	lsm: AtomicBool,
	tracing: AtomicBool,
	/// Whether the wrapped transactions answer that their backend does not detect
	/// write-write overlap on a blind write, whatever it does.
	blind_off: AtomicBool,
	trace: Mutex<Vec<TraceEntry>>,
}

/// Releases an in-flight slot when the operation completes or is dropped.
struct InFlight<'a>(&'a AtomicU64);

impl Drop for InFlight<'_> {
	fn drop(&mut self) {
		self.0.fetch_sub(1, Ordering::SeqCst);
	}
}

impl LatencyStats {
	pub(super) fn new() -> Arc<Self> {
		Arc::new(Self {
			latency_us: AtomicU64::new(0),
			in_flight: AtomicU64::new(0),
			rounds: AtomicU64::new(0),
			per_class: Default::default(),
			per_category: Default::default(),
			views: AtomicU64::new(0),
			synced_commits: AtomicU64::new(0),
			enabled: AtomicBool::new(false),
			lsm: AtomicBool::new(false),
			tracing: AtomicBool::new(false),
			blind_off: AtomicBool::new(false),
			trace: Mutex::new(Vec::new()),
		})
	}

	/// Zero every counter and the trace. Settings are kept.
	pub(super) fn reset(&self) {
		self.in_flight.store(0, Ordering::SeqCst);
		self.rounds.store(0, Ordering::SeqCst);
		self.views.store(0, Ordering::SeqCst);
		self.synced_commits.store(0, Ordering::SeqCst);
		for c in &self.per_class {
			c.store(0, Ordering::SeqCst);
		}
		for c in &self.per_category {
			c.store(0, Ordering::SeqCst);
		}
		self.trace.lock().unwrap().clear();
	}

	pub(super) fn snapshot(&self) -> RoundTrips {
		RoundTrips {
			rounds: self.rounds.load(Ordering::SeqCst),
			per_class: std::array::from_fn(|i| self.per_class[i].load(Ordering::SeqCst)),
			per_category: std::array::from_fn(|i| self.per_category[i].load(Ordering::SeqCst)),
			views: self.views.load(Ordering::SeqCst),
		}
	}

	/// Commits counted as [`Class::Commit`] that waited for the store's own sync, rather than
	/// committing through [`Transactable::commit_unsynced`].
	pub(super) fn synced_commits(&self) -> u64 {
		self.synced_commits.load(Ordering::SeqCst)
	}

	pub(super) fn take_trace(&self) -> Vec<TraceEntry> {
		std::mem::take(&mut *self.trace.lock().unwrap())
	}

	/// Turn the measurement window on or off. Outside it nothing is counted or
	/// slept.
	pub(super) fn set_enabled(&self, on: bool) {
		self.enabled.store(on, Ordering::SeqCst);
	}

	pub(super) fn set_latency(&self, latency: Duration) {
		self.latency_us.store(latency.as_micros() as u64, Ordering::SeqCst);
	}

	pub(super) fn set_profile(&self, profile: Profile) {
		self.lsm.store(profile == Profile::Lsm, Ordering::SeqCst);
	}

	pub(super) fn profile(&self) -> Profile {
		match self.lsm.load(Ordering::SeqCst) {
			true => Profile::Lsm,
			false => Profile::Forward,
		}
	}

	pub(super) fn set_tracing(&self, on: bool) {
		self.tracing.store(on, Ordering::SeqCst);
	}

	/// Make the wrapped transactions answer `false` for
	/// [`Transactable::blind_write_conflicts`], as a backend without the capability does.
	pub(super) fn force_blind_write_conflicts_off(&self, off: bool) {
		self.blind_off.store(off, Ordering::SeqCst);
	}

	fn count_view(&self) {
		if self.enabled.load(Ordering::SeqCst) {
			self.views.fetch_add(1, Ordering::SeqCst);
		}
	}

	/// Run `op` as one counted operation of `class` on `key`.
	///
	/// A slept class waits the injected latency before `op` runs, and holds an
	/// in-flight slot from before the wait until `op` completes. The slot that
	/// takes the in-flight count from zero opens a serial round trip.
	async fn round<T>(
		&self,
		class: Class,
		key: &[u8],
		op: impl Future<Output = KvsResult<T>>,
		rows: impl FnOnce(&T) -> Option<u64>,
	) -> KvsResult<T> {
		if !self.enabled.load(Ordering::SeqCst) {
			return op.await;
		}
		let category = categorise(key);
		self.per_class[class as usize].fetch_add(1, Ordering::SeqCst);
		let slot = class.slept().then(|| {
			if self.in_flight.fetch_add(1, Ordering::SeqCst) == 0 {
				self.rounds.fetch_add(1, Ordering::SeqCst);
				self.per_category[category as usize].fetch_add(1, Ordering::SeqCst);
			}
			InFlight(&self.in_flight)
		});
		let entry = self.tracing.load(Ordering::SeqCst).then(|| {
			let mut trace = self.trace.lock().unwrap();
			trace.push(TraceEntry {
				class,
				category,
				round: self.rounds.load(Ordering::SeqCst),
				rows: None,
				key: key
					.iter()
					.copied()
					.flat_map(std::ascii::escape_default)
					.map(char::from)
					.take(96)
					.collect(),
			});
			trace.len() - 1
		});
		if slot.is_some() {
			let latency = self.latency_us.load(Ordering::SeqCst);
			if latency > 0 {
				tokio::time::sleep(Duration::from_micros(latency)).await;
			}
		}
		let out = op.await;
		drop(slot);
		if let (Some(entry), Ok(value)) = (entry, &out) {
			self.trace.lock().unwrap()[entry].rows = rows(value);
		}
		out
	}
}

/// The class of the next chunk of a cursor, and the cursor is then past its first.
fn chunk_class(first: &mut bool) -> Class {
	match std::mem::replace(first, false) {
		true => Class::RangeFirst,
		false => Class::RangePage,
	}
}

/// A [`TransactionBuilder`] whose transactions count and delay their accesses.
pub(super) struct LatencyBuilder {
	inner: Box<dyn TransactionBuilder>,
	stats: Arc<LatencyStats>,
}

impl LatencyBuilder {
	pub(super) fn new(inner: Box<dyn TransactionBuilder>, stats: Arc<LatencyStats>) -> Self {
		Self {
			inner,
			stats,
		}
	}
}

impl TransactionBuilder for LatencyBuilder {
	fn name(&self) -> &'static str {
		self.inner.name()
	}

	fn new_transaction(
		&self,
		write: TransactionType,
	) -> BoxFut<'_, KvsResult<(Box<dyn Transactable>, bool)>> {
		Box::pin(async move {
			let (tx, local) = self
				.stats
				.round(Class::Begin, b"", self.inner.new_transaction(write), |_| None)
				.await?;
			let tx = LatencyTx::new(tx, Arc::clone(&self.stats));
			Ok((Box::new(tx) as Box<dyn Transactable>, local))
		})
	}

	fn shutdown(&self) -> BoxFut<'_, KvsResult<()>> {
		self.inner.shutdown()
	}

	fn register_metrics(&self) -> Option<Metrics> {
		self.inner.register_metrics()
	}

	fn collect_u64_metric(&self, metric: &str) -> Option<u64> {
		self.inner.collect_u64_metric(metric)
	}

	fn wait_until_serve_ready(&self) -> BoxFut<'_, KvsResult<()>> {
		self.inner.wait_until_serve_ready()
	}

	/// The backend's isolation levels are republished over wrapped
	/// transactions: a `BEGIN` block opens its transaction through them, and
	/// forwarding the backend's own handle would let the block bypass the
	/// wrapper. Every other extension is the inner backend's.
	fn extension(&self, id: TypeId) -> Option<Arc<dyn Any + Send + Sync>> {
		let ext = self.inner.extension(id)?;
		if id != TypeId::of::<IsolationLevelsHandle>() {
			return Some(ext);
		}
		let handle = ext.downcast::<IsolationLevelsHandle>().ok()?;
		let levels: Arc<dyn IsolationLevels> = Arc::new(LatencyIsolation {
			inner: Arc::clone(&handle.0),
			stats: Arc::clone(&self.stats),
		});
		Some(Arc::new(IsolationLevelsHandle(levels)))
	}
}

/// Isolation-level transactions of the backend, wrapped as [`LatencyBuilder`]'s are.
struct LatencyIsolation {
	inner: Arc<dyn IsolationLevels>,
	stats: Arc<LatencyStats>,
}

impl IsolationLevels for LatencyIsolation {
	fn supported(&self) -> &'static [Isolation] {
		self.inner.supported()
	}

	fn open(
		&self,
		write: TransactionType,
		isolation: Isolation,
	) -> BoxFut<'_, KvsResult<(Box<dyn Transactable>, bool)>> {
		Box::pin(async move {
			let (tx, local) = self
				.stats
				.round(Class::Begin, b"", self.inner.open(write, isolation), |_| None)
				.await?;
			let tx = LatencyTx::new(tx, Arc::clone(&self.stats));
			Ok((Box::new(tx) as Box<dyn Transactable>, local))
		})
	}
}

/// A [`TransactionBuilderFactory`] that opens the community backend at the
/// given path and wraps it in a [`LatencyBuilder`] over `stats`.
pub(super) struct LatencyComposer(pub(super) Arc<LatencyStats>);

impl TransactionBuilderFactory for LatencyComposer {
	type RouterState = ();

	async fn new_transaction_builder(
		&self,
		path: &str,
		canceller: CancellationToken,
		config: ConfigMap,
	) -> anyhow::Result<TransactionBuilderParts<Self::RouterState>> {
		let parts = CommunityComposer().new_transaction_builder(path, canceller, config).await?;
		Ok(TransactionBuilderParts::without_router_state(Box::new(LatencyBuilder::new(
			parts.builder,
			Arc::clone(&self.0),
		))))
	}

	fn path_valid(&self, v: &str) -> anyhow::Result<String> {
		CommunityComposer().path_valid(v)
	}
}

/// A transaction that counts and delays the accesses that leave its write buffer.
struct LatencyTx {
	inner: Box<dyn Transactable>,
	stats: Arc<LatencyStats>,
	/// Keys this transaction has written. A read of one is answered from the
	/// transaction's own buffer.
	written: Mutex<HashSet<Vec<u8>>>,
}

impl LatencyTx {
	fn new(inner: Box<dyn Transactable>, stats: Arc<LatencyStats>) -> Self {
		Self {
			inner,
			stats,
			written: Mutex::new(HashSet::new()),
		}
	}

	fn is_written(&self, key: &[u8]) -> bool {
		self.written.lock().unwrap().contains(key)
	}

	fn mark_written(&self, key: Vec<u8>) {
		self.written.lock().unwrap().insert(key);
	}

	/// The class of a write that needs the key's current value.
	fn check_class(&self, key: &[u8]) -> Class {
		match self.is_written(key) {
			true => Class::BlindWrite,
			false => Class::CheckWrite,
		}
	}
}

impl Transactable for LatencyTx {
	fn kind(&self) -> &'static str {
		self.inner.kind()
	}

	fn sole_writer(&self) -> bool {
		self.inner.sole_writer()
	}

	fn validates_reads(&self) -> bool {
		self.inner.validates_reads()
	}

	fn parallel_count(&self) -> bool {
		self.inner.parallel_count()
	}

	fn bounds_write_buffer(&self) -> bool {
		self.inner.bounds_write_buffer()
	}

	fn count_cost(&self) -> CountCost {
		self.inner.count_cost()
	}

	fn closed(&self) -> bool {
		self.inner.closed()
	}

	fn writeable(&self) -> bool {
		self.inner.writeable()
	}

	fn cancel<'a>(&'a self) -> BoxFut<'a, KvsResult<()>> {
		self.inner.cancel()
	}

	/// A read-only transaction has nothing to make durable, so only a
	/// writable one pays a commit round trip.
	fn commit<'a>(&'a self) -> BoxFut<'a, KvsResult<()>> {
		Box::pin(async move {
			match self.inner.writeable() {
				true => {
					if self.stats.enabled.load(Ordering::SeqCst) {
						self.stats.synced_commits.fetch_add(1, Ordering::SeqCst);
					}
					self.stats.round(Class::Commit, b"", self.inner.commit(), |_| None).await
				}
				false => self.inner.commit().await,
			}
		})
	}

	fn commit_unsynced<'a>(&'a self) -> BoxFut<'a, KvsResult<()>> {
		Box::pin(async move {
			match self.inner.writeable() {
				true => {
					self.stats
						.round(Class::Commit, b"", self.inner.commit_unsynced(), |_| None)
						.await
				}
				false => self.inner.commit_unsynced().await,
			}
		})
	}

	fn exists<'a>(&'a self, key: Key<'a>, version: Option<u64>) -> BoxFut<'a, KvsResult<bool>> {
		Box::pin(async move {
			if self.is_written(key.as_slice()) {
				return self.inner.exists(key, version).await;
			}
			let k = key.as_slice().to_vec();
			self.stats.round(Class::Get, &k, self.inner.exists(key, version), |_| None).await
		})
	}

	fn get<'a>(&'a self, key: Key<'a>, version: Option<u64>) -> BoxFut<'a, KvsResult<Option<Val>>> {
		Box::pin(async move {
			if self.is_written(key.as_slice()) {
				return self.inner.get(key, version).await;
			}
			let k = key.as_slice().to_vec();
			self.stats.round(Class::Get, &k, self.inner.get(key, version), |_| None).await
		})
	}

	fn getu<'a>(&'a self, key: Key<'a>) -> BoxFut<'a, KvsResult<Option<Val>>> {
		Box::pin(async move {
			if self.is_written(key.as_slice()) {
				return self.inner.getu(key).await;
			}
			let k = key.as_slice().to_vec();
			self.stats.round(Class::Get, &k, self.inner.getu(key), |_| None).await
		})
	}

	fn shared_locked_reads(&self) -> bool {
		self.inner.shared_locked_reads()
	}

	fn set<'a>(&'a self, key: Key<'a>, val: Val) -> BoxFut<'a, KvsResult<()>> {
		Box::pin(async move {
			let k = key.as_slice().to_vec();
			let out =
				self.stats.round(Class::BlindWrite, &k, self.inner.set(key, val), |_| None).await;
			if out.is_ok() {
				self.mark_written(k);
			}
			out
		})
	}

	fn set_many<'a>(&'a self, entries: Vec<(Vec<u8>, Val)>) -> BoxFut<'a, KvsResult<()>> {
		Box::pin(async move {
			let keys: Vec<Vec<u8>> = entries.iter().map(|(k, _)| k.clone()).collect();
			let first = keys.first().cloned().unwrap_or_default();
			let out = self
				.stats
				.round(Class::BlindWrite, &first, self.inner.set_many(entries), |_| None)
				.await;
			if out.is_ok() {
				for k in keys {
					self.mark_written(k);
				}
			}
			out
		})
	}

	fn put<'a>(&'a self, key: Key<'a>, val: Val) -> BoxFut<'a, KvsResult<()>> {
		Box::pin(async move {
			let k = key.as_slice().to_vec();
			let class = self.check_class(&k);
			let out = self.stats.round(class, &k, self.inner.put(key, val), |_| None).await;
			if out.is_ok() {
				self.mark_written(k);
			}
			out
		})
	}

	fn putc<'a>(&'a self, key: Key<'a>, val: Val, chk: Option<Val>) -> BoxFut<'a, KvsResult<()>> {
		Box::pin(async move {
			let k = key.as_slice().to_vec();
			let class = self.check_class(&k);
			let out = self.stats.round(class, &k, self.inner.putc(key, val, chk), |_| None).await;
			if out.is_ok() {
				self.mark_written(k);
			}
			out
		})
	}

	fn del<'a>(&'a self, key: Key<'a>) -> BoxFut<'a, KvsResult<()>> {
		Box::pin(async move {
			let k = key.as_slice().to_vec();
			let out = self.stats.round(Class::BlindWrite, &k, self.inner.del(key), |_| None).await;
			if out.is_ok() {
				self.mark_written(k);
			}
			out
		})
	}

	fn delc<'a>(&'a self, key: Key<'a>, chk: Option<&'a [u8]>) -> BoxFut<'a, KvsResult<()>> {
		Box::pin(async move {
			let k = key.as_slice().to_vec();
			let class = self.check_class(&k);
			let out = self.stats.round(class, &k, self.inner.delc(key, chk), |_| None).await;
			if out.is_ok() {
				self.mark_written(k);
			}
			out
		})
	}

	fn keys<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<KeysResult>> {
		Box::pin(async move {
			let k = rng.start.as_slice().to_vec();
			let op = self.inner.keys(rng, limit, skip, version);
			self.stats.round(Class::RangeFirst, &k, op, |r| Some(r.keys.len() as u64)).await
		})
	}

	fn keysr<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<KeysResult>> {
		Box::pin(async move {
			let k = rng.start.as_slice().to_vec();
			let op = self.inner.keysr(rng, limit, skip, version);
			self.stats.round(Class::RangeFirst, &k, op, |r| Some(r.keys.len() as u64)).await
		})
	}

	fn scan<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<ScanResult>> {
		Box::pin(async move {
			let k = rng.start.as_slice().to_vec();
			let op = self.inner.scan(rng, limit, skip, version);
			self.stats.round(Class::RangeFirst, &k, op, |r| Some(r.values.len() as u64)).await
		})
	}

	fn scanr<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<ScanResult>> {
		Box::pin(async move {
			let k = rng.start.as_slice().to_vec();
			let op = self.inner.scanr(rng, limit, skip, version);
			self.stats.round(Class::RangeFirst, &k, op, |r| Some(r.values.len() as u64)).await
		})
	}

	fn open_keys_cursor<'a>(
		&'a self,
		rng: KeyRange<'a>,
		dir: Direction,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<Box<dyn ScanCursorKeys + 'a>>> {
		Box::pin(async move {
			let key = rng.start.as_slice().to_vec();
			let inner = self.inner.open_keys_cursor(rng, dir, skip, version).await?;
			Ok(Box::new(LatencyKeys {
				inner,
				stats: Arc::clone(&self.stats),
				first: true,
				done: false,
				key,
			}) as Box<dyn ScanCursorKeys + 'a>)
		})
	}

	fn visit_key_ranges<'a>(
		&'a self,
		ranges: &'a [KeyRange<'a>],
		limit: u32,
		version: Option<u64>,
		f: &'a mut dyn RangeKeyVisitor,
	) -> BoxFut<'a, KvsResult<ScanChunkStats>> {
		Box::pin(async move {
			let key = ranges.first().map(|r| r.start.as_slice().to_vec()).unwrap_or_default();
			let op = self.inner.visit_key_ranges(ranges, limit, version, f);
			match ranges.is_empty() {
				true => op.await,
				false => self.stats.round(Class::RangeFirst, &key, op, |s| Some(s.rows)).await,
			}
		})
	}

	fn open_vals_cursor<'a>(
		&'a self,
		rng: KeyRange<'a>,
		dir: Direction,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<Box<dyn ScanCursorVals + 'a>>> {
		Box::pin(async move {
			let key = rng.start.as_slice().to_vec();
			let inner = self.inner.open_vals_cursor(rng, dir, skip, version).await?;
			Ok(Box::new(LatencyVals {
				inner,
				stats: Arc::clone(&self.stats),
				first: true,
				done: false,
				key,
			}) as Box<dyn ScanCursorVals + 'a>)
		})
	}

	fn replace<'a>(&'a self, key: Key<'a>, val: Val) -> BoxFut<'a, KvsResult<()>> {
		Box::pin(async move {
			let k = key.as_slice().to_vec();
			let out = self
				.stats
				.round(Class::BlindWrite, &k, self.inner.replace(key, val), |_| None)
				.await;
			if out.is_ok() {
				self.mark_written(k);
			}
			out
		})
	}

	fn clr<'a>(&'a self, key: Key<'a>) -> BoxFut<'a, KvsResult<()>> {
		Box::pin(async move {
			let k = key.as_slice().to_vec();
			let out = self.stats.round(Class::BlindWrite, &k, self.inner.clr(key), |_| None).await;
			if out.is_ok() {
				self.mark_written(k);
			}
			out
		})
	}

	fn clrc<'a>(&'a self, key: Key<'a>, chk: Option<&'a [u8]>) -> BoxFut<'a, KvsResult<()>> {
		Box::pin(async move {
			let k = key.as_slice().to_vec();
			let class = self.check_class(&k);
			let out = self.stats.round(class, &k, self.inner.clrc(key, chk), |_| None).await;
			if out.is_ok() {
				self.mark_written(k);
			}
			out
		})
	}

	fn getm<'a>(
		&'a self,
		keys: &'a [Key<'a>],
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<GetMultiResult>> {
		Box::pin(async move {
			let all_written = {
				let written = self.written.lock().unwrap();
				keys.iter().all(|k| written.contains(k.as_slice()))
			};
			if all_written {
				return self.inner.getm(keys, version).await;
			}
			let key = keys.first().map(|k| k.as_slice().to_vec()).unwrap_or_default();
			let op = self.inner.getm(keys, version);
			self.stats.round(Class::Getm, &key, op, |r| Some(r.values.len() as u64)).await
		})
	}

	fn assert_absent<'a>(&'a self, keys: &'a [Key<'a>]) -> BoxFut<'a, KvsResult<Option<usize>>> {
		Box::pin(async move {
			let all_written = {
				let written = self.written.lock().unwrap();
				keys.iter().all(|k| written.contains(k.as_slice()))
			};
			if all_written {
				return self.inner.assert_absent(keys).await;
			}
			let key = keys.first().map(|k| k.as_slice().to_vec()).unwrap_or_default();
			let op = self.inner.assert_absent(keys);
			self.stats.round(Class::Getm, &key, op, |_| Some(keys.len() as u64)).await
		})
	}

	fn getr<'a>(
		&'a self,
		rng: KeyRange<'a>,
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<ScanResult>> {
		Box::pin(async move {
			let k = rng.start.as_slice().to_vec();
			let op = self.inner.getr(rng, version);
			self.stats.round(Class::RangeFirst, &k, op, |r| Some(r.values.len() as u64)).await
		})
	}

	fn delr<'a>(&'a self, rng: KeyRange<'a>) -> BoxFut<'a, KvsResult<()>> {
		Box::pin(async move {
			let k = rng.start.as_slice().to_vec();
			self.stats.round(Class::RangeFirst, &k, self.inner.delr(rng), |_| None).await
		})
	}

	fn clrr<'a>(&'a self, rng: KeyRange<'a>) -> BoxFut<'a, KvsResult<()>> {
		Box::pin(async move {
			let k = rng.start.as_slice().to_vec();
			self.stats.round(Class::RangeFirst, &k, self.inner.clrr(rng), |_| None).await
		})
	}

	fn count<'a>(
		&'a self,
		rng: KeyRange<'a>,
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<usize>> {
		Box::pin(async move {
			let k = rng.start.as_slice().to_vec();
			let op = self.inner.count(rng, version);
			self.stats.round(Class::RangeFirst, &k, op, |n| Some(*n as u64)).await
		})
	}

	fn count_bounded<'a>(
		&'a self,
		rng: KeyRange<'a>,
		version: Option<u64>,
		budget: u32,
	) -> BoxFut<'a, KvsResult<(usize, Option<Key<'static>>)>> {
		Box::pin(async move {
			let k = rng.start.as_slice().to_vec();
			let op = self.inner.count_bounded(rng, version, budget);
			self.stats.round(Class::RangeFirst, &k, op, |(n, _)| Some(*n as u64)).await
		})
	}

	fn snapshot(&self) -> Option<Arc<dyn Snapshot>> {
		let view = self.inner.snapshot()?;
		self.stats.count_view();
		Some(Arc::new(LatencyView {
			inner: view,
			stats: Arc::clone(&self.stats),
		}))
	}

	fn view_capabilities(&self) -> ViewCapabilities {
		self.inner.view_capabilities()
	}

	fn plain_reads_tracked(&self) -> bool {
		self.inner.plain_reads_tracked()
	}

	fn blind_write_conflicts(&self) -> bool {
		self.inner.blind_write_conflicts() && !self.stats.blind_off.load(Ordering::SeqCst)
	}

	fn scan_read_ahead(&self) -> bool {
		self.inner.scan_read_ahead()
	}

	fn multi_read(&self) -> bool {
		match self.stats.profile() {
			Profile::Forward => self.inner.multi_read(),
			Profile::Lsm => true,
		}
	}

	fn estimate<'a>(
		&'a self,
		rng: KeyRange<'a>,
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<Estimate>> {
		Box::pin(async move {
			let k = rng.start.as_slice().to_vec();
			let op = self.inner.estimate(rng, version);
			self.stats.round(Class::RangeFirst, &k, op, |e| e.keys).await
		})
	}

	fn estimate_many<'a>(
		&'a self,
		ranges: Vec<KeyRange<'a>>,
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<Vec<Estimate>>> {
		Box::pin(async move {
			let k = ranges.first().map(|rng| rng.start.as_slice().to_vec()).unwrap_or_default();
			let op = self.inner.estimate_many(ranges, version);
			self.stats.round(Class::RangeFirst, &k, op, |e| Some(e.len() as u64)).await
		})
	}

	fn approximate<'a>(
		&'a self,
		rng: KeyRange<'a>,
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<Option<Estimate>>> {
		Box::pin(async move {
			let k = rng.start.as_slice().to_vec();
			let op = self.inner.approximate(rng, version);
			self.stats.round(Class::RangeFirst, &k, op, |e| e.and_then(|e| e.keys)).await
		})
	}

	fn split<'a>(
		&'a self,
		rng: KeyRange<'a>,
		target: SplitTarget,
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<Vec<Key<'static>>>> {
		Box::pin(async move {
			let k = rng.start.as_slice().to_vec();
			let op = self.inner.split(rng, target, version);
			self.stats.round(Class::RangeFirst, &k, op, |v| Some(v.len() as u64)).await
		})
	}

	fn batch_keys<'a>(
		&'a self,
		rng: KeyRange<'a>,
		batch: u32,
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<Batch<Vec<u8>>>> {
		Box::pin(async move {
			let k = rng.start.as_slice().to_vec();
			let op = self.inner.batch_keys(rng, batch, version);
			self.stats.round(Class::RangeFirst, &k, op, |b| Some(b.result.len() as u64)).await
		})
	}

	fn batch_keys_vals<'a>(
		&'a self,
		rng: KeyRange<'a>,
		batch: u32,
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<Batch<(Vec<u8>, Vec<u8>)>>> {
		Box::pin(async move {
			let k = rng.start.as_slice().to_vec();
			let op = self.inner.batch_keys_vals(rng, batch, version);
			self.stats.round(Class::RangeFirst, &k, op, |b| Some(b.result.len() as u64)).await
		})
	}

	fn new_save_point(&self) -> BoxFut<'_, KvsResult<()>> {
		self.inner.new_save_point()
	}

	fn release_last_save_point(&self) -> BoxFut<'_, KvsResult<()>> {
		self.inner.release_last_save_point()
	}

	fn rollback_to_save_point(&self) -> BoxFut<'_, KvsResult<()>> {
		self.inner.rollback_to_save_point()
	}

	fn timestamp(&self) -> BoxFut<'_, KvsResult<BoxTimeStamp>> {
		self.inner.timestamp()
	}

	fn safe_timestamp(&self) -> BoxFut<'_, KvsResult<BoxTimeStamp>> {
		self.inner.safe_timestamp()
	}

	fn timestamp_impl(&self) -> BoxTimeStampImpl {
		self.inner.timestamp_impl()
	}

	fn version_impl(&self) -> BoxTimeStampImpl {
		self.inner.version_impl()
	}

	fn compact<'a>(&'a self, range: Option<KeyRange<'a>>) -> BoxFut<'a, KvsResult<()>> {
		self.inner.compact(range)
	}
}

/// A keys cursor whose first chunk is a `range_first` and later chunks are
/// `range_page`.
struct LatencyKeys<'a> {
	inner: Box<dyn ScanCursorKeys + 'a>,
	stats: Arc<LatencyStats>,
	first: bool,
	/// The last chunk came back short without the visitor stopping it: the
	/// read reached the end of the range, so a later chunk reads nothing.
	done: bool,
	key: Vec<u8>,
}

impl ScanCursorKeys for LatencyKeys<'_> {
	fn next_batch<'s>(
		&'s mut self,
		limit: u32,
	) -> BoxFut<'s, KvsResult<surrealdb_kvs::api::KeysBatch<'s>>> {
		Box::pin(async move {
			let Self {
				inner,
				stats,
				first,
				done,
				key,
			} = self;
			if limit == 0 || *done {
				return inner.next_batch(limit).await;
			}
			let class = chunk_class(first);
			let batch =
				stats.round(class, key, inner.next_batch(limit), |b| Some(b.len() as u64)).await?;
			*done = batch.len() < limit as usize;
			Ok(batch)
		})
	}

	fn for_each<'s>(
		&'s mut self,
		limit: u32,
		f: &'s mut dyn KeyVisitor,
	) -> BoxFut<'s, KvsResult<ScanChunkStats>> {
		Box::pin(async move {
			let Self {
				inner,
				stats,
				first,
				done,
				key,
			} = self;
			if limit == 0 || *done {
				return inner.for_each(limit, f).await;
			}
			let class = chunk_class(first);
			let mut stopped = false;
			let mut visit = |k: &[u8]| {
				let flow = f(k)?;
				stopped |= flow.is_break();
				Ok(flow)
			};
			let stats = stats
				.round(class, key, inner.for_each(limit, &mut visit), |s| Some(s.rows))
				.await?;
			*done = !stopped && stats.rows < u64::from(limit);
			Ok(stats)
		})
	}
}

/// A key+value cursor, counted as [`LatencyKeys`] is.
struct LatencyVals<'a> {
	inner: Box<dyn ScanCursorVals + 'a>,
	stats: Arc<LatencyStats>,
	first: bool,
	/// The last chunk came back short without the visitor stopping it: the
	/// read reached the end of the range, so a later chunk reads nothing.
	done: bool,
	key: Vec<u8>,
}

impl ScanCursorVals for LatencyVals<'_> {
	fn next_batch<'s>(
		&'s mut self,
		limit: u32,
	) -> BoxFut<'s, KvsResult<surrealdb_kvs::api::ValsBatch<'s>>> {
		Box::pin(async move {
			let Self {
				inner,
				stats,
				first,
				done,
				key,
			} = self;
			if limit == 0 || *done {
				return inner.next_batch(limit).await;
			}
			let class = chunk_class(first);
			let batch =
				stats.round(class, key, inner.next_batch(limit), |b| Some(b.len() as u64)).await?;
			*done = batch.len() < limit as usize;
			Ok(batch)
		})
	}

	fn for_each<'s>(
		&'s mut self,
		limit: u32,
		f: &'s mut dyn ValVisitor,
	) -> BoxFut<'s, KvsResult<ScanChunkStats>> {
		Box::pin(async move {
			let Self {
				inner,
				stats,
				first,
				done,
				key,
			} = self;
			if limit == 0 || *done {
				return inner.for_each(limit, f).await;
			}
			let class = chunk_class(first);
			let mut stopped = false;
			let mut visit = |k: &[u8], v: &[u8]| {
				let flow = f(k, v)?;
				stopped |= flow.is_break();
				Ok(flow)
			};
			let stats = stats
				.round(class, key, inner.for_each(limit, &mut visit), |s| Some(s.rows))
				.await?;
			*done = !stopped && stats.rows < u64::from(limit);
			Ok(stats)
		})
	}
}

/// A view whose cursors and batched reads are counted. Opening a cursor is free.
struct LatencyView {
	inner: Arc<dyn Snapshot>,
	stats: Arc<LatencyStats>,
}

impl Snapshot for LatencyView {
	fn held_bytes(&self) -> u64 {
		self.inner.held_bytes()
	}

	fn open<'a>(
		&'a self,
		rng: KeyRange<'a>,
		dir: Direction,
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<Box<dyn RangeCursor + 'a>>> {
		Box::pin(async move {
			let key = rng.start.as_slice().to_vec();
			let inner = self.inner.open(rng, dir, version).await?;
			Ok(Box::new(LatencyRange {
				inner,
				stats: Arc::clone(&self.stats),
				first: true,
				done: false,
				key,
			}) as Box<dyn RangeCursor + 'a>)
		})
	}

	fn get_many<'a>(
		&'a self,
		keys: &'a [Key<'a>],
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<GetMultiResult>> {
		Box::pin(async move {
			let key = keys.first().map(|k| k.as_slice().to_vec()).unwrap_or_default();
			let op = self.inner.get_many(keys, version);
			self.stats.round(Class::Getm, &key, op, |r| Some(r.values.len() as u64)).await
		})
	}

	fn estimate<'a>(
		&'a self,
		rng: KeyRange<'a>,
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<Estimate>> {
		Box::pin(async move {
			let k = rng.start.as_slice().to_vec();
			let op = self.inner.estimate(rng, version);
			self.stats.round(Class::RangeFirst, &k, op, |e| e.keys).await
		})
	}

	fn split<'a>(
		&'a self,
		rng: KeyRange<'a>,
		target: SplitTarget,
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<Vec<Key<'static>>>> {
		Box::pin(async move {
			let k = rng.start.as_slice().to_vec();
			let op = self.inner.split(rng, target, version);
			self.stats.round(Class::RangeFirst, &k, op, |v| Some(v.len() as u64)).await
		})
	}

	fn count_bounded<'a>(
		&'a self,
		rng: KeyRange<'a>,
		version: Option<u64>,
		budget: u32,
	) -> BoxFut<'a, KvsResult<(usize, Option<Key<'static>>)>> {
		Box::pin(async move {
			let k = rng.start.as_slice().to_vec();
			let op = self.inner.count_bounded(rng, version, budget);
			self.stats.round(Class::RangeFirst, &k, op, |(n, _)| Some(*n as u64)).await
		})
	}
}

/// A view cursor; `aim` re-arms its first-chunk accounting.
struct LatencyRange<'a> {
	inner: Box<dyn RangeCursor + 'a>,
	stats: Arc<LatencyStats>,
	first: bool,
	/// The last chunk came back short without the visitor stopping it: the
	/// read reached the end of the range, so a later chunk reads nothing.
	done: bool,
	key: Vec<u8>,
}

impl RangeCursor for LatencyRange<'_> {
	fn aim(&mut self, rng: KeyRange<'_>, dir: Direction) -> KvsResult<()> {
		self.inner.aim(rng.as_borrowed(), dir)?;
		self.first = true;
		self.done = false;
		self.key = rng.start.as_slice().to_vec();
		Ok(())
	}

	fn narrow(&mut self, end: Key<'_>) -> KvsResult<()> {
		self.inner.narrow(end)
	}

	fn for_each<'s>(
		&'s mut self,
		limit: u32,
		f: &'s mut dyn ValVisitor,
	) -> BoxFut<'s, KvsResult<ScanChunkStats>> {
		Box::pin(async move {
			let Self {
				inner,
				stats,
				first,
				done,
				key,
			} = self;
			if limit == 0 || *done {
				return inner.for_each(limit, f).await;
			}
			let class = chunk_class(first);
			let mut stopped = false;
			let mut visit = |k: &[u8], v: &[u8]| {
				let flow = f(k, v)?;
				stopped |= flow.is_break();
				Ok(flow)
			};
			let stats = stats
				.round(class, key, inner.for_each(limit, &mut visit), |s| Some(s.rows))
				.await?;
			*done = !stopped && stats.rows < u64::from(limit);
			Ok(stats)
		})
	}

	fn fill<'s>(
		&'s mut self,
		page: &'s mut Page,
		limit: u32,
	) -> BoxFut<'s, KvsResult<ScanChunkStats>> {
		Box::pin(async move {
			let Self {
				inner,
				stats,
				first,
				done,
				key,
			} = self;
			if limit == 0 {
				return inner.fill(page, 0).await;
			}
			if *done {
				return inner.fill(page, limit).await;
			}
			let class = chunk_class(first);
			let stats = stats.round(class, key, inner.fill(page, limit), |s| Some(s.rows)).await?;
			*done = inner.resume().is_none();
			Ok(stats)
		})
	}

	fn resume(&self) -> Option<surrealdb_kvs::Resume> {
		self.inner.resume()
	}
}
