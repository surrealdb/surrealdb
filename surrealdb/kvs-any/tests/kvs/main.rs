//! Runs the shared KV-store behaviour suite (`surrealdb-kvs-test-suite`)
//! against every first-party backend enabled by a `kv-*` feature, going
//! through the `surrealdb-kvs-any` connection-string entry point so the
//! parser is covered on every run.
#![allow(clippy::unwrap_used)]

use std::process::ExitCode;

use surrealdb_cnf::ConfigMap;
#[allow(unused_imports)]
use surrealdb_kvs_test::TestBackend;
use surrealdb_kvs_test::TestDs;
use tokio_util::sync::CancellationToken;

/// Construct the backend selected by the given connection path.
#[allow(dead_code)]
async fn ds_from_path(path: &str) -> TestDs {
	let builder = surrealdb_kvs_any::Backends::community()
		.new_transaction_builder(path, CancellationToken::new(), ConfigMap::empty())
		.await
		.unwrap();
	TestDs::from_builder(builder)
}

/// Construct an on-disk backend in a fresh temporary directory, kept alive
/// for the lifetime of the datastore.
#[cfg(any(feature = "kv-rocksdb", feature = "kv-surrealkv"))]
async fn ds_on_disk(scheme: &str) -> TestDs {
	let dir = temp_dir::TempDir::new().unwrap();
	let path = dir.path().to_string_lossy().to_string();
	let builder = surrealdb_kvs_any::Backends::community()
		.new_transaction_builder(
			&format!("{scheme}:{path}"),
			CancellationToken::new(),
			ConfigMap::empty(),
		)
		.await
		.unwrap();
	TestDs::from_builder_with_guard(builder, dir)
}

fn main() -> ExitCode {
	#[allow(unused_mut)]
	let mut backends = Vec::new();

	// The memory backend no longer supports versioning (removed in the
	// surrealmx 0.23 upgrade), so there is no `mem_versioned` backend.
	#[cfg(feature = "kv-mem")]
	backends.push(TestBackend::new("mem", || ds_from_path("memory")));

	#[cfg(feature = "kv-rocksdb")]
	backends.push(TestBackend::new("rocksdb", || ds_on_disk("rocksdb")));

	#[cfg(feature = "kv-surrealkv")]
	backends.push(TestBackend::new("surrealkv", || ds_on_disk("surrealkv")));

	// The TiKV backend aliases one shared external cluster, so its tests run
	// serially and each test starts by wiping the keyspace.
	#[cfg(feature = "kv-tikv")]
	backends.push(
		TestBackend::new("tikv", || async {
			let ds = ds_from_path("tikv:127.0.0.1:2379").await;
			// Clear any previous test entries
			let tx = ds.transaction(surrealdb_kvs::TransactionType::Write).await.unwrap();
			tx.delr((vec![0u8]..vec![0xffu8]).into()).await.unwrap();
			tx.commit().await.unwrap();
			ds
		})
		.serial(),
	);

	surrealdb_kvs_test::run(backends)
}
