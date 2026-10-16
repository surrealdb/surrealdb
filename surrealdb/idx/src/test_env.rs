//! A transaction source for the index engines' own tests.
//!
//! The engines take their environment as [`IndexEnv`]; the only production
//! implementation is the query context, which a `Datastore` builds. Reaching
//! for a `Datastore` to get a transaction would put these white-box tests above
//! the thing they test, so they build the environment from a
//! [`TransactionFactory`] instead — which is what a datastore uses underneath
//! anyway — plus the process-local pieces `IndexEnv` exposes.
//!
//! The defaults match what a datastore hands the engines: [`IdxConfig`]'s
//! defaults are the same values the datastore reads from configuration, and the
//! two ANN cache sizes come from that same config, so an engine under test
//! allocates exactly as it does in production.
//!
//! Places come from a [`CountingPlaces`] pool of the store's own rather than
//! from the node's, so a test reads exactly what its run took.

use std::any::{Any, TypeId};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{Result, bail};
use common::EngineError;
use parking_lot::Mutex;
use surrealdb_cnf::ConfigMap;
use surrealdb_datastore::sequences::Sequences;
use surrealdb_datastore::triggers::CommitTriggers;
use surrealdb_datastore::{Transaction, TransactionFactory};
use surrealdb_kvs::api::BoxFut;
use surrealdb_kvs::{Metrics, Transactable, TransactionBuilder, TransactionType};
use uuid::Uuid;

use crate::catalog::providers::{BoxProviderFut, CancellationProbe};
use crate::config::IdxConfig;
use crate::env::{BoxEnvFut, IndexEnv};
use crate::places::{Place, PlaceSource, Ticket};
use crate::trees::store::IndexStores;

/// The places a test store's pool has: enough that the staged batches the
/// tests build on several tasks are granted a place per task.
const TEST_PLACES: usize = 16;

/// The quantum of a test store's pool, in jobs.
const TEST_QUANTUM: u32 = 8;

/// A pool of places with one registration, whose books a test reads.
///
/// Its asks are granted while a place is free and refused otherwise; a refusal
/// marks the registration waiting, as the node's pool does. Whether another
/// registration waits is the test's to say: from the start
/// ([`Self::set_others_waiting`]), or from a given check on
/// ([`Self::others_wait_from`]), which makes workers yield mid-run.
pub(crate) struct CountingPlaces {
	books: Mutex<CountingBooks>,
	quantum: u32,
	/// The checks so far that answered that another registration waits.
	yields: tokio::sync::watch::Sender<usize>,
}

#[derive(Default)]
struct CountingBooks {
	free: usize,
	held: usize,
	peak: usize,
	taken: usize,
	refused: usize,
	waiting: bool,
	/// Checks of whether another registration waits, and how many answered
	/// that one does.
	checks: usize,
	yields: usize,
	/// The check from which another registration waits; `None` while none
	/// does.
	others_from: Option<usize>,
}

impl CountingPlaces {
	/// A pool of `places` places whose workers check for a waiting
	/// registration every `quantum` jobs.
	pub(crate) fn new(places: usize, quantum: u32) -> Arc<Self> {
		Arc::new(Self {
			books: Mutex::new(CountingBooks {
				free: places,
				..CountingBooks::default()
			}),
			quantum,
			yields: tokio::sync::watch::Sender::new(0),
		})
	}

	/// The pool's one registration.
	pub(crate) fn ticket(self: &Arc<Self>) -> Ticket {
		Ticket::new(CountingTicket(Arc::clone(self)))
	}

	/// Whether another registration waits, from now on.
	pub(crate) fn set_others_waiting(&self, waiting: bool) {
		self.books.lock().others_from = waiting.then_some(0);
	}

	/// Another registration waits from the `check`-th check on, counting from
	/// zero.
	pub(crate) fn others_wait_from(&self, check: usize) {
		self.books.lock().others_from = Some(check);
	}

	/// The places held now.
	pub(crate) fn held(&self) -> usize {
		self.books.lock().held
	}

	/// The most places held at once.
	pub(crate) fn peak(&self) -> usize {
		self.books.lock().peak
	}

	/// The places taken so far.
	pub(crate) fn taken(&self) -> usize {
		self.books.lock().taken
	}

	/// The asks refused so far.
	pub(crate) fn refused(&self) -> usize {
		self.books.lock().refused
	}

	/// Whether the registration is marked waiting.
	pub(crate) fn waiting(&self) -> bool {
		self.books.lock().waiting
	}

	/// The checks so far that answered that another registration waits.
	pub(crate) fn yields(&self) -> usize {
		self.books.lock().yields
	}

	/// Returns once `n` checks have answered that another registration waits.
	pub(crate) async fn wait_for_yields(&self, n: usize) {
		let mut yields = self.yields.subscribe();
		yields.wait_for(|yields| *yields >= n).await.expect("the pool holds the sender");
	}
}

/// A pool of places shared by several registrations, which grants, refuses
/// and owes places by the node pool's rules (`exec/vm/morsel/places.rs` in
/// `surrealdb-core`), with no reserve and no cap: a place is granted only
/// when the free places outnumber the waiting registrations owed one first,
/// those that hold fewer or hold as many and registered earlier. A test reads
/// each registration's books.
pub(crate) struct SharedPlaces {
	books: Mutex<SharedBooks>,
	quantum: u32,
}

struct SharedBooks {
	free: usize,
	/// By registration order: places held, places taken, and whether waiting.
	entries: Vec<SharedEntry>,
	/// The registration each place was granted to, in grant order.
	granted: Vec<usize>,
	/// Whether each grant, in grant order, left its registration marked
	/// waiting when it released the books.
	granted_waiting: Vec<bool>,
}

#[derive(Clone, Copy, Default)]
struct SharedEntry {
	held: usize,
	taken: usize,
	waiting: bool,
}

impl SharedPlaces {
	/// A pool of `places` places whose workers check for a waiting
	/// registration every `quantum` jobs.
	pub(crate) fn new(places: usize, quantum: u32) -> Arc<Self> {
		Arc::new(Self {
			books: Mutex::new(SharedBooks {
				free: places,
				entries: Vec::new(),
				granted: Vec::new(),
				granted_waiting: Vec::new(),
			}),
			quantum,
		})
	}

	/// Registers with the pool, after every registration made before.
	pub(crate) fn register(self: &Arc<Self>) -> (usize, Ticket) {
		let mut books = self.books.lock();
		let id = books.entries.len();
		books.entries.push(SharedEntry::default());
		(
			id,
			Ticket::new(SharedTicket {
				pool: Arc::clone(self),
				id,
			}),
		)
	}

	/// The places registration `id` holds.
	pub(crate) fn held(&self, id: usize) -> usize {
		self.books.lock().entries[id].held
	}

	/// The places registration `id` has taken so far.
	pub(crate) fn taken(&self, id: usize) -> usize {
		self.books.lock().entries[id].taken
	}

	/// Whether registration `id` is marked waiting.
	pub(crate) fn waiting(&self, id: usize) -> bool {
		self.books.lock().entries[id].waiting
	}

	/// The registration each place was granted to, in grant order.
	pub(crate) fn granted(&self) -> Vec<usize> {
		self.books.lock().granted.clone()
	}

	/// Whether each grant, in grant order, left its registration marked
	/// waiting as it released the books.
	pub(crate) fn granted_waiting(&self) -> Vec<bool> {
		self.books.lock().granted_waiting.clone()
	}
}

/// A registration of a [`SharedPlaces`] pool.
struct SharedTicket {
	pool: Arc<SharedPlaces>,
	id: usize,
}

/// A place of a [`SharedPlaces`] pool, held for registration `id`.
struct SharedPlace(Arc<SharedPlaces>, usize);

impl Drop for SharedPlace {
	fn drop(&mut self) {
		let mut books = self.0.books.lock();
		books.free += 1;
		books.entries[self.1].held -= 1;
	}
}

impl PlaceSource for SharedTicket {
	fn acquire(&self, keep_waiting: bool) -> Option<Place> {
		let mut books = self.pool.books.lock();
		let me = self.id;
		let held = books.entries[me].held;
		let owed_before = books
			.entries
			.iter()
			.enumerate()
			.filter(|(id, entry)| {
				*id != me
					&& entry.waiting
					&& (entry.held < held || (entry.held == held && *id < me))
			})
			.count();
		if books.free <= owed_before {
			books.entries[me].waiting = true;
			return None;
		}
		books.free -= 1;
		books.granted.push(me);
		let entry = &mut books.entries[me];
		entry.waiting = keep_waiting;
		entry.held += 1;
		entry.taken += 1;
		let waiting = entry.waiting;
		books.granted_waiting.push(waiting);
		Some(Place::new(SharedPlace(Arc::clone(&self.pool), me)))
	}

	fn others_wait(&self) -> bool {
		let books = self.pool.books.lock();
		let held = books.entries[self.id].held;
		books
			.entries
			.iter()
			.enumerate()
			.any(|(id, entry)| id != self.id && entry.waiting && entry.held <= held)
	}

	fn stop_waiting(&self) {
		self.pool.books.lock().entries[self.id].waiting = false;
	}

	fn quantum(&self) -> u32 {
		self.pool.quantum
	}
}

/// The registration of a [`CountingPlaces`] pool.
struct CountingTicket(Arc<CountingPlaces>);

/// A place of a [`CountingPlaces`] pool.
struct CountingPlace(Arc<CountingPlaces>);

impl Drop for CountingPlace {
	fn drop(&mut self) {
		let mut books = self.0.books.lock();
		books.free += 1;
		books.held -= 1;
	}
}

impl PlaceSource for CountingTicket {
	fn acquire(&self, keep_waiting: bool) -> Option<Place> {
		let mut books = self.0.books.lock();
		if books.free == 0 {
			books.refused += 1;
			books.waiting = true;
			return None;
		}
		books.free -= 1;
		books.held += 1;
		books.taken += 1;
		books.peak = books.peak.max(books.held);
		books.waiting = keep_waiting;
		Some(Place::new(CountingPlace(Arc::clone(&self.0))))
	}

	fn others_wait(&self) -> bool {
		let mut books = self.0.books.lock();
		let check = books.checks;
		books.checks += 1;
		let waits = books.others_from.is_some_and(|from| check >= from);
		if waits {
			books.yields += 1;
			self.0.yields.send_replace(books.yields);
		}
		waits
	}

	fn stop_waiting(&self) {
		self.0.books.lock().waiting = false;
	}

	fn quantum(&self) -> u32 {
		self.0.quantum
	}
}

/// The process-local state an index operation runs against, minus the
/// transaction: one of these stands in for a `Datastore` across a test.
#[derive(Clone)]
pub(crate) struct TestIndexStore {
	tf: TransactionFactory,
	sequences: Sequences,
	stores: IndexStores,
	node_id: Uuid,
	config: Arc<IdxConfig>,
	/// The read-only transactions opened on the backend, by this store and every node over it.
	reads_opened: Arc<AtomicUsize>,
	/// The places every environment of this store takes its workers from.
	places: Arc<CountingPlaces>,
}

/// A backend that counts the read-only transactions opened on it and otherwise answers as the
/// backend it wraps.
struct CountingBuilder {
	inner: Box<dyn TransactionBuilder>,
	reads_opened: Arc<AtomicUsize>,
}

impl TransactionBuilder for CountingBuilder {
	fn name(&self) -> &'static str {
		self.inner.name()
	}

	fn new_transaction(
		&self,
		write: TransactionType,
	) -> BoxFut<'_, surrealdb_kvs::Result<(Box<dyn Transactable>, bool)>> {
		if matches!(write, TransactionType::Read) {
			self.reads_opened.fetch_add(1, Ordering::Relaxed);
		}
		self.inner.new_transaction(write)
	}

	fn shutdown(&self) -> BoxFut<'_, surrealdb_kvs::Result<()>> {
		self.inner.shutdown()
	}

	fn register_metrics(&self) -> Option<Metrics> {
		self.inner.register_metrics()
	}

	fn collect_u64_metric(&self, metric: &str) -> Option<u64> {
		self.inner.collect_u64_metric(metric)
	}

	fn wait_until_serve_ready(&self) -> BoxFut<'_, surrealdb_kvs::Result<()>> {
		self.inner.wait_until_serve_ready()
	}

	fn extension(&self, id: TypeId) -> Option<Arc<dyn Any + Send + Sync>> {
		self.inner.extension(id)
	}
}

impl TestIndexStore {
	/// Opens an in-memory store. Panics on failure: a test that cannot get a
	/// backend has nothing left to assert.
	pub(crate) async fn new() -> Self {
		Self::with_path("mem://").await
	}

	/// Opens an in-memory store with a modified engine configuration. The ANN
	/// caches are sized from the modified configuration.
	pub(crate) async fn with_idx_config(configure: impl FnOnce(&mut IdxConfig)) -> Self {
		let mut config = IdxConfig::default();
		configure(&mut config);
		Self::open("mem://", config).await
	}

	/// The engine configuration this store hands out.
	pub(crate) fn config(&self) -> &Arc<IdxConfig> {
		&self.config
	}

	/// The same store — same backend, same caches — under a different
	/// engine configuration, for tests that flip a knob mid-flight.
	pub(crate) fn rewire_config(mut store: Self, config: IdxConfig) -> Self {
		store.config = Arc::new(config);
		store
	}

	/// Opens a store on `path`, for the tests pinned to a specific backend.
	pub(crate) async fn with_path(path: &str) -> Self {
		Self::open(path, IdxConfig::default()).await
	}

	async fn open(path: &str, config: IdxConfig) -> Self {
		let builder = surrealdb_kvs_any::Backends::community()
			.new_transaction_builder(path, Default::default(), ConfigMap::default())
			.await
			.unwrap();
		let reads_opened = Arc::new(AtomicUsize::new(0));
		let builder = Box::new(CountingBuilder {
			inner: builder,
			reads_opened: Arc::clone(&reads_opened),
		});
		let tf = TransactionFactory::new(
			Arc::new(CommitTriggers::new()),
			builder,
			Arc::new(Default::default()),
		);
		let node_id = Uuid::new_v4();
		let stores = IndexStores::new(
			config.hnsw_cache_size,
			config.diskann_cache_size,
			config.graph_resolve_cache_size,
			config.segment_dictionary_path.clone(),
		);
		Self {
			sequences: Sequences::new(tf.clone(), node_id),
			tf,
			stores,
			node_id,
			config: Arc::new(config),
			reads_opened,
			places: CountingPlaces::new(TEST_PLACES, TEST_QUANTUM),
		}
	}

	/// How many read-only transactions have been opened on the backend so far, by this store and
	/// by every node over it.
	pub(crate) fn reads_opened(&self) -> usize {
		self.reads_opened.load(Ordering::Relaxed)
	}

	/// Another datastore over this store's backend, as a second node of a
	/// cluster sees it: its own node id, index stores and commit triggers, and
	/// the DiskANN writer-lease protocol a store shared between datastores needs.
	pub(crate) fn shared_node(&self) -> Self {
		let triggers = Arc::new(CommitTriggers::new());
		triggers.assume_shared_store();
		self.node_with(triggers)
	}

	/// Another datastore over this store's backend that covers DiskANN guards
	/// as on a store validating every read
	/// (`surrealdb_kvs::api::Transactable::validates_reads`).
	pub(crate) fn validating_node(&self) -> Self {
		let triggers = Arc::new(CommitTriggers::new());
		triggers.assume_validated_reads();
		self.node_with(triggers)
	}

	/// Another datastore over this store's backend, with `triggers` and its own
	/// node id and index stores.
	fn node_with(&self, triggers: Arc<CommitTriggers>) -> Self {
		let tf = self.tf.with_triggers(triggers);
		let node_id = Uuid::new_v4();
		let config = &self.config;
		let stores = IndexStores::new(
			config.hnsw_cache_size,
			config.diskann_cache_size,
			config.graph_resolve_cache_size,
			config.segment_dictionary_path.clone(),
		);
		Self {
			sequences: Sequences::new(tf.clone(), node_id),
			tf,
			stores,
			node_id,
			config: Arc::clone(config),
			reads_opened: Arc::clone(&self.reads_opened),
			places: CountingPlaces::new(TEST_PLACES, TEST_QUANTUM),
		}
	}

	/// Have this datastore's next commit run `hook` between covering its DiskANN
	/// guards and its storage commit.
	pub(crate) fn before_next_commit(&self, hook: surrealdb_datastore::triggers::BeforeCommitHook) {
		self.tf.triggers().set_before_commit_hook(hook);
	}

	/// How many DiskANN guards this datastore's compaction has repaired.
	pub(crate) fn guard_repairs(&self) -> usize {
		self.tf.triggers().diskann_guard_repairs()
	}

	/// This datastore's node id.
	pub(crate) fn node_id(&self) -> Uuid {
		self.node_id
	}

	/// Release every DiskANN writer lease this datastore holds, as its idle
	/// release would once nothing has committed under them for long enough.
	pub(crate) async fn release_writer_leases(&self) -> Result<usize> {
		self.release_idle_writer_leases(Duration::ZERO).await
	}

	/// Release the DiskANN writer leases nothing has relied on for `idle`, as the
	/// idle release does.
	pub(crate) async fn release_idle_writer_leases(&self, idle: Duration) -> Result<usize> {
		self.tf.release_idle_diskann_writer_leases(&self.sequences, idle).await
	}

	/// The process-local index registry, shared by every environment this
	/// store hands out — the caches must survive across transactions for the
	/// tests that assert on cache behaviour.
	pub(crate) fn index_stores(&self) -> &IndexStores {
		&self.stores
	}

	/// Opens a bare transaction, for the assertions that read keys directly
	/// rather than driving an engine.
	pub(crate) async fn transaction(&self, tt: TransactionType) -> Result<Transaction> {
		self.tf.transaction(tt, self.sequences.clone()).await
	}

	/// Opens a transaction and wraps it in the environment an engine takes.
	pub(crate) async fn env(&self, tt: TransactionType) -> TestIndexEnv {
		TestIndexEnv {
			store: self.clone(),
			tx: Arc::new(self.transaction(tt).await.unwrap()),
		}
	}
}

/// One index operation's environment: a [`TestIndexStore`] plus the
/// transaction the operation reads and writes through.
#[derive(Clone)]
pub(crate) struct TestIndexEnv {
	store: TestIndexStore,
	tx: Arc<Transaction>,
}

impl TestIndexEnv {
	/// The operation's transaction, for the assertions that read keys directly.
	///
	/// Inherent as well as on the trait so a test can reach the transaction
	/// without importing [`IndexEnv`] just to name the method.
	pub(crate) fn tx(&self) -> Arc<Transaction> {
		Arc::clone(&self.tx)
	}
}

impl CancellationProbe for TestIndexEnv {
	/// Nothing cancels a test operation, so the deadline never fires.
	fn expect_not_timedout(&self) -> BoxProviderFut<'_, Result<()>> {
		Box::pin(async { Ok(()) })
	}
}

impl IndexEnv for TestIndexEnv {
	fn tx(&self) -> Arc<Transaction> {
		Arc::clone(&self.tx)
	}

	/// Never done: no test operation carries a deadline or a cancellation
	/// flag. The periodic yield is kept, because a scan loop that never awaits
	/// otherwise would hold its worker for the whole scan.
	fn is_done(&self, count: Option<usize>) -> BoxEnvFut<'_> {
		Box::pin(async move {
			if count.is_none_or(|count| count % 32 == 0) {
				yield_now!();
			}
			Ok(false)
		})
	}

	fn index_stores(&self) -> &IndexStores {
		&self.store.stores
	}

	fn node_id(&self) -> Uuid {
		self.store.node_id
	}

	fn sequences(&self) -> Result<&Sequences> {
		Ok(&self.store.sequences)
	}

	fn config(&self) -> &IdxConfig {
		&self.store.config
	}

	fn places(&self) -> Ticket {
		self.store.places.ticket()
	}
}

/// A [`TestIndexEnv`] whose cancellation checkpoint reports done on the `at`-th
/// counted call and every call after it, for the tests that pin what a scan has
/// accounted for when the query stops it.
///
/// A scan loop checks cancellation once per entry, so the call index *is* the
/// entry index: reporting done on call `n` stands for a query stopped by the
/// time the scan reached its `n`th entry. Which entry that call belongs to is
/// what a reordering of the loop body changes, and what the ordering tests read
/// back off the engine's own accounting.
///
/// Built with [`Self::new`] the stop is a cancellation, which
/// [`CancellationProbe::expect_not_timedout`] does not report. Built with
/// [`Self::timing_out`] it is a deadline: once the checkpoint has fired, that
/// probe reports the query timed out, as the query context does past its
/// deadline.
///
/// Only counted calls are the ones a scan loop makes (`Some(count)`); a
/// single-operation check (`None`) passes through untouched, so an unrelated
/// probe cannot shift the schedule.
pub(crate) struct CancelAtCheckpoint {
	inner: TestIndexEnv,
	at: usize,
	calls: AtomicUsize,
	timeout: Option<Duration>,
}

impl CancelAtCheckpoint {
	pub(crate) fn new(inner: TestIndexEnv, at: usize) -> Self {
		Self {
			inner,
			at,
			calls: AtomicUsize::new(0),
			timeout: None,
		}
	}

	/// A checkpoint that stands for a deadline of `timeout` expiring by the
	/// `at`-th counted call.
	pub(crate) fn timing_out(inner: TestIndexEnv, at: usize, timeout: Duration) -> Self {
		Self {
			timeout: Some(timeout),
			..Self::new(inner, at)
		}
	}

	fn fired(&self) -> bool {
		self.calls.load(Ordering::Relaxed) >= self.at
	}

	/// The operation's transaction, for the assertions that read keys directly.
	pub(crate) fn tx(&self) -> Arc<Transaction> {
		self.inner.tx()
	}
}

impl CancellationProbe for CancelAtCheckpoint {
	fn expect_not_timedout(&self) -> BoxProviderFut<'_, Result<()>> {
		Box::pin(async move {
			if let Some(timeout) = self.timeout
				&& self.fired()
			{
				bail!(EngineError::QueryTimedout(timeout));
			}
			self.inner.expect_not_timedout().await
		})
	}
}

impl IndexEnv for CancelAtCheckpoint {
	fn tx(&self) -> Arc<Transaction> {
		IndexEnv::tx(&self.inner)
	}

	fn is_done(&self, count: Option<usize>) -> BoxEnvFut<'_> {
		Box::pin(async move {
			if count.is_none() {
				return self.inner.is_done(count).await;
			}
			let call = self.calls.fetch_add(1, Ordering::Relaxed) + 1;
			if call >= self.at {
				return Ok(true);
			}
			self.inner.is_done(count).await
		})
	}

	fn index_stores(&self) -> &IndexStores {
		self.inner.index_stores()
	}

	fn node_id(&self) -> Uuid {
		self.inner.node_id()
	}

	fn sequences(&self) -> Result<&Sequences> {
		self.inner.sequences()
	}

	fn config(&self) -> &IdxConfig {
		self.inner.config()
	}

	fn places(&self) -> Ticket {
		self.inner.places()
	}
}
