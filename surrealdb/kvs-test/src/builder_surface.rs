//! Contracts on the `TransactionBuilder` surface itself: shutdown,
//! compaction support, metrics registration, and the transaction "local"
//! flag.

use surrealdb_kvs::Error;
use surrealdb_kvs::TransactionType::*;

use crate::{TestBackend, kvs_test};

/// `shutdown()` on a fresh datastore succeeds.
async fn shutdown_ok(b: &TestBackend) {
	let ds = b.create_ds().await;

	let tx = ds.transaction(Write).await.unwrap();
	tx.set(b"test".into(), b"value".to_vec()).await.unwrap();
	tx.commit().await.unwrap();

	ds.builder().shutdown().await.unwrap();
}
kvs_test!(shutdown_ok);

/// Backends with a compaction primitive accept the hint.
async fn compact_supported(b: &TestBackend) {
	let ds = b.create_ds().await;

	let tx = ds.transaction(Write).await.unwrap();
	tx.set(b"test".into(), b"value".to_vec()).await.unwrap();
	tx.commit().await.unwrap();

	let tx = ds.transaction(Read).await.unwrap();
	tx.compact(None).await.unwrap();
	tx.cancel().await.unwrap();
}

kvs_test!(compact_supported, only = [rocksdb]);

/// Backends without a compaction primitive report
/// [`Error::CompactionNotSupported`].
async fn compact_unsupported(b: &TestBackend) {
	let ds = b.create_ds().await;

	let tx = ds.transaction(Read).await.unwrap();

	assert!(matches!(tx.compact(None).await, Err(Error::CompactionNotSupported)));

	tx.cancel().await.unwrap();
}

kvs_test!(compact_unsupported, except = [rocksdb]);

/// A backend that registers metrics must expose every declared metric
/// through `collect_u64_metric`.
async fn metrics_collectable(b: &TestBackend) {
	let ds = b.create_ds().await;
	let metrics = ds.builder().register_metrics().expect("expected registered metrics");
	assert!(!metrics.name.is_empty());
	assert!(!metrics.u64_metrics.is_empty());
	for metric in &metrics.u64_metrics {
		assert!(
			ds.builder().collect_u64_metric(metric.name).is_some(),
			"declared metric {} must be collectable",
			metric.name
		);
	}
}

kvs_test!(metrics_collectable, only = [rocksdb, surrealds]);

/// Backends without metrics return `None` from both metric hooks.
async fn metrics_none(b: &TestBackend) {
	let ds = b.create_ds().await;
	assert!(ds.builder().register_metrics().is_none());
	assert!(ds.builder().collect_u64_metric("anything").is_none());
}

kvs_test!(metrics_none, except = [rocksdb, surrealds]);

/// Process-local backends report their transactions as local.
///
/// The enterprise distributed store also reports `local = true`: the flag
/// gates in-process optimisations that its coordinator-side transactions
/// support, not physical locality.
async fn transactions_local(b: &TestBackend) {
	let ds = b.create_ds().await;
	let (tx, local) = ds.transaction_with_locality(Write).await.unwrap();
	assert!(local, "process-local backends must report local transactions");
	tx.cancel().await.unwrap();
}

kvs_test!(transactions_local, except = [tikv]);

/// Backends backed by external resources report their transactions as
/// non-local.
async fn transactions_remote(b: &TestBackend) {
	let ds = b.create_ds().await;
	let (tx, local) = ds.transaction_with_locality(Write).await.unwrap();
	assert!(!local, "externally backed backends must report non-local transactions");
	tx.cancel().await.unwrap();
}

kvs_test!(transactions_remote, only = [tikv]);
