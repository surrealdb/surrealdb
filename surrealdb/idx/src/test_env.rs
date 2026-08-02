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

use anyhow::Result;
use surrealdb_cnf::ConfigMap;
use surrealdb_datastore::sequences::Sequences;
use surrealdb_datastore::{Transaction, TransactionFactory};
use surrealdb_kvs::TransactionType;
use tokio::sync::Notify;
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

	/// Opens a store on `path`, for the tests pinned to a specific backend.
	pub(crate) async fn with_path(path: &str) -> Self {
		let builder = surrealdb_kvs_any::Backends::community()
			.new_transaction_builder(path, Default::default(), ConfigMap::default())
			.await
			.unwrap();
		let tf =
			TransactionFactory::new(Arc::new(Notify::new()), builder, Arc::new(Default::default()));
		let node_id = Uuid::new_v4();
		let config = IdxConfig::default();
		let stores = IndexStores::new(config.hnsw_cache_size, config.diskann_cache_size);
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
