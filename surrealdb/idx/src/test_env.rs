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

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use surrealdb_cnf::ConfigMap;
use surrealdb_datastore::sequences::Sequences;
use surrealdb_datastore::triggers::CommitTriggers;
use surrealdb_datastore::{Transaction, TransactionFactory};
use surrealdb_kvs::TransactionType;
use uuid::Uuid;

use crate::catalog::providers::{BoxProviderFut, CancellationProbe};
use crate::config::IdxConfig;
use crate::env::{BoxEnvFut, IndexEnv};
use crate::trees::store::IndexStores;

/// The process-local state an index operation runs against, minus the
/// transaction: one of these stands in for a `Datastore` across a test.
#[derive(Clone)]
pub(crate) struct TestIndexStore {
	tf: TransactionFactory,
	sequences: Sequences,
	stores: IndexStores,
	node_id: Uuid,
	config: Arc<IdxConfig>,
}

impl TestIndexStore {
	/// Opens an in-memory store. Panics on failure: a test that cannot get a
	/// backend has nothing left to assert.
	pub(crate) async fn new() -> Self {
		Self::with_path("mem://").await
	}

	/// Opens an in-memory store with a modified engine configuration.
	pub(crate) async fn with_idx_config(configure: impl FnOnce(&mut IdxConfig)) -> Self {
		let mut store = Self::with_path("mem://").await;
		let mut config = (*store.config).clone();
		configure(&mut config);
		store.config = Arc::new(config);
		store
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
		let builder = surrealdb_kvs_any::Backends::community()
			.new_transaction_builder(path, Default::default(), ConfigMap::default())
			.await
			.unwrap();
		let tf = TransactionFactory::new(
			Arc::new(CommitTriggers::new()),
			builder,
			Arc::new(Default::default()),
		);
		let node_id = Uuid::new_v4();
		let config = IdxConfig::default();
		let stores = IndexStores::new(
			config.hnsw_cache_size,
			config.diskann_cache_size,
			config.graph_resolve_cache_size,
		);
		Self {
			sequences: Sequences::new(tf.clone(), node_id),
			tf,
			stores,
			node_id,
			config: Arc::new(config),
		}
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
}

/// A [`TestIndexEnv`] whose cancellation checkpoint reports done on the `at`-th
/// counted call, for the tests that pin what a scan has accounted for when a
/// deadline stops it.
///
/// A scan loop checks cancellation once per entry, so the call index *is* the
/// entry index: reporting done on call `n` stands for a deadline that had
/// expired by the time the scan reached its `n`th entry. Which entry that call
/// belongs to is what a reordering of the loop body changes, and what the
/// ordering tests read back off the engine's own accounting.
///
/// Only counted calls are the ones a scan loop makes (`Some(count)`); a
/// single-operation check (`None`) passes through untouched, so an unrelated
/// probe cannot shift the schedule.
pub(crate) struct CancelAtCheckpoint {
	inner: TestIndexEnv,
	at: usize,
	calls: AtomicUsize,
}

impl CancelAtCheckpoint {
	pub(crate) fn new(inner: TestIndexEnv, at: usize) -> Self {
		Self {
			inner,
			at,
			calls: AtomicUsize::new(0),
		}
	}

	/// The operation's transaction, for the assertions that read keys directly.
	pub(crate) fn tx(&self) -> Arc<Transaction> {
		self.inner.tx()
	}
}

impl CancellationProbe for CancelAtCheckpoint {
	fn expect_not_timedout(&self) -> BoxProviderFut<'_, Result<()>> {
		self.inner.expect_not_timedout()
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
}
