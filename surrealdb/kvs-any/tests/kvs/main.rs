//! KV-store backend tests, run against every enabled `kv-*` backend through
//! the `surrealdb-kvs-any` facade.
#![allow(clippy::unwrap_used)]
#![cfg(any(
	feature = "kv-mem",
	feature = "kv-rocksdb",
	feature = "kv-tikv",
	feature = "kv-surrealkv",
))]

use std::future::Future;

use common::config::ConfigMap;
use surrealdb_kvs::{Result, Transactable, TransactionBuilder, TransactionType};
use tokio_util::sync::CancellationToken;

macro_rules! include_tests {
	($new_ds:ident => $($name:ident),* $(,)?) => {
		$(
			super::$name::define_tests!($new_ds);
		)*
	};
}

#[cfg(feature = "kv-rocksdb")]
mod metrics;
#[cfg(feature = "kv-rocksdb")]
mod rocksdb_ds;

mod multireader;
mod multiwriter_different_keys;
mod multiwriter_same_keys_allow;
mod multiwriter_same_keys_conflict;
mod multiwriter_same_keys_putc;
mod raw;
mod snapshot;

/// Mirrors the locking half of `TransactionBuilder::new_transaction`'s `lock`
/// flag. See [`TransactionType`].
#[derive(Clone, Copy, Debug)]
pub enum LockType {
	Optimistic,
	Pessimistic,
}

/// A backend datastore under test, wrapping the boxed [`TransactionBuilder`]
/// produced by [`surrealdb_kvs_any::new_transaction_builder`].
pub struct TestDs(Box<dyn TransactionBuilder>);

impl TestDs {
	/// Construct the backend selected by the given connection path with an
	/// empty configuration.
	async fn new(path: &str) -> Self {
		Self::new_with_config(path, ConfigMap::empty()).await.unwrap()
	}

	/// Construct the backend selected by the given connection path.
	async fn new_with_config(path: &str, config: ConfigMap) -> Result<Self> {
		let builder =
			surrealdb_kvs_any::new_transaction_builder(path, CancellationToken::new(), config)
				.await?;
		Ok(Self(builder))
	}

	/// Start a new transaction on the underlying backend.
	async fn transaction(
		&self,
		write: TransactionType,
		lock: LockType,
	) -> Result<Box<dyn Transactable>> {
		let lock = matches!(lock, LockType::Pessimistic);
		let (tx, _) = self.0.new_transaction(write, lock).await?;
		Ok(tx)
	}

	/// Register the backend's metrics, if it exposes any.
	#[cfg(feature = "kv-rocksdb")]
	fn register_metrics(&self) -> Option<surrealdb_kvs::Metrics> {
		self.0.register_metrics()
	}

	/// Collect a single named `u64` metric from the backend, if supported.
	#[cfg(feature = "kv-rocksdb")]
	fn collect_u64_metric(&self, metric: &str) -> Option<u64> {
		self.0.collect_u64_metric(metric)
	}
}

trait CreateDs {
	async fn create_ds(&self) -> TestDs;
}

impl<F, Fut> CreateDs for F
where
	F: Fn() -> Fut,
	Fut: Future<Output = TestDs>,
{
	async fn create_ds(&self) -> TestDs {
		(self)().await
	}
}

#[cfg(feature = "kv-mem")]
mod mem {
	use super::TestDs;

	async fn new_ds() -> TestDs {
		// Setup the in-memory datastore
		TestDs::new("memory").await
	}

	include_tests!(new_ds =>
		raw,
		snapshot,
		multireader,
		multiwriter_different_keys,
		multiwriter_same_keys_conflict,
		multiwriter_same_keys_putc,
	);
}

#[cfg(feature = "kv-rocksdb")]
mod rocksdb {
	use temp_dir::TempDir;

	use super::TestDs;

	async fn new_ds() -> TestDs {
		// Setup the temporary data storage path
		let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
		// Setup the RocksDB datastore
		TestDs::new(&format!("rocksdb:{path}")).await
	}

	include_tests!(new_ds =>
		raw,
		snapshot,
		multireader,
		multiwriter_different_keys,
		multiwriter_same_keys_conflict,
		multiwriter_same_keys_putc,
		metrics
	);
}

#[cfg(feature = "kv-surrealkv")]
mod surrealkv {
	use temp_dir::TempDir;

	use super::TestDs;

	async fn new_ds() -> TestDs {
		// Setup the temporary data storage path
		let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
		// Setup the SurrealKV datastore
		TestDs::new(&format!("surrealkv:{path}")).await
	}

	include_tests!(new_ds =>
		raw,
		snapshot,
		multireader,
		multiwriter_different_keys,
		multiwriter_same_keys_conflict,
		multiwriter_same_keys_putc,
	);
}

#[cfg(feature = "kv-tikv")]
mod tikv {
	use surrealdb_kvs::TransactionType;

	use super::{LockType, TestDs};

	async fn new_ds() -> TestDs {
		// Setup the TiKV datastore from the cluster connection string
		let ds = TestDs::new("tikv:127.0.0.1:2379").await;
		// Clear any previous test entries
		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
		tx.delr((vec![0u8]..vec![0xffu8]).into()).await.unwrap();
		tx.commit().await.unwrap();
		// Return the datastore
		ds
	}

	include_tests!(new_ds =>
		raw,
		snapshot,
		multireader,
		multiwriter_different_keys,
		multiwriter_same_keys_allow,
		multiwriter_same_keys_putc,
	);
}
