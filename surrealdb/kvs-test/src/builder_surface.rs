//! Contracts on the `TransactionBuilder` surface itself: shutdown,
//! compaction support, out-of-transaction range destruction, metrics
//! registration, and the transaction "local" flag.

use std::any::TypeId;

use surrealdb_kvs::TransactionType::*;
use surrealdb_kvs::{DestroyRangeHandle, Error, KeyRange};

use crate::{TestBackend, TestDs, kvs_test};

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

/// Keys the range-destroy tests below seed inside [`destroy_test_range`].
const DESTROY_SEED_KEYS: u8 = 5;

/// The half-open range the range-destroy tests below seed and destroy.
fn destroy_test_range() -> KeyRange<'static> {
	KeyRange::from(b"destroy/".to_vec()..b"destroy0".to_vec())
}

/// Seed [`DESTROY_SEED_KEYS`] keys inside [`destroy_test_range`], plus one
/// immediately after it.
async fn seed_destroy_range(ds: &TestDs) {
	let tx = ds.transaction(Write).await.unwrap();
	for i in 1..=DESTROY_SEED_KEYS {
		tx.set(format!("destroy/{i}").as_bytes().into(), b"value".to_vec()).await.unwrap();
	}
	// A neighbour past the exclusive end, so a destroy that overshoots its
	// range is visible as a count mismatch rather than passing unnoticed.
	tx.set(b"destroy0keep".into(), b"value".to_vec()).await.unwrap();
	tx.commit().await.unwrap();
}

/// Number of keys currently inside [`destroy_test_range`].
async fn count_destroy_range(ds: &TestDs) -> usize {
	let tx = ds.transaction(Read).await.unwrap();
	let count = tx.count(destroy_test_range(), None).await.unwrap();
	tx.cancel().await.unwrap();
	count
}

/// A backend publishing a [`DestroyRangeHandle`] empties exactly the range it is
/// handed.
///
/// The handle is the only way to destroy a range outside a transaction, so its
/// presence is a claim that the keys really go: a handle that reported success
/// over a range still holding its keys is what a caller with no fallback would
/// accept as a completed destruction. The exclusive end is asserted too, since
/// overshooting destroys data no caller asked about.
async fn destroy_range_empties_the_range(b: &TestBackend) {
	let ds = b.create_ds().await;
	let seeded = usize::from(DESTROY_SEED_KEYS);
	seed_destroy_range(&ds).await;
	assert_eq!(count_destroy_range(&ds).await, seeded);

	let ext = ds
		.builder()
		.extension(TypeId::of::<DestroyRangeHandle>())
		.expect("this backend must publish a range-destroy capability");
	let handle = ext
		.downcast::<DestroyRangeHandle>()
		.expect("the DestroyRangeHandle TypeId must resolve to a DestroyRangeHandle");
	handle.0.destroy_range(destroy_test_range()).await.unwrap();
	assert_eq!(
		count_destroy_range(&ds).await,
		0,
		"a published range-destroy capability must empty the range",
	);
	let tx = ds.transaction(Read).await.unwrap();
	assert!(
		tx.exists(b"destroy0keep".into(), None).await.unwrap(),
		"a destroy must stop at the exclusive end of its range",
	);
	tx.cancel().await.unwrap();
}

kvs_test!(destroy_range_empties_the_range, only = [tikv]);

/// Backends without an out-of-transaction range destroy publish no capability
/// handle at all, and leave the range alone.
///
/// Absence is what the engine's transactional fallback keys off, and what makes
/// the datastore-level entry point able to answer
/// [`Error::RangeDestroyNotSupported`] instead of an `Ok` that deleted nothing.
async fn destroy_range_unsupported(b: &TestBackend) {
	let ds = b.create_ds().await;
	let seeded = usize::from(DESTROY_SEED_KEYS);
	seed_destroy_range(&ds).await;
	assert!(
		ds.builder().extension(TypeId::of::<DestroyRangeHandle>()).is_none(),
		"this backend must publish no range-destroy capability",
	);
	assert_eq!(
		count_destroy_range(&ds).await,
		seeded,
		"a backend publishing no capability must leave the range untouched",
	);
}

kvs_test!(destroy_range_unsupported, except = [tikv]);

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

kvs_test!(metrics_collectable, only = [rocksdb, surrealds, surrealos, surrealos_s3]);

/// Backends without metrics return `None` from both metric hooks.
async fn metrics_none(b: &TestBackend) {
	let ds = b.create_ds().await;
	assert!(ds.builder().register_metrics().is_none());
	assert!(ds.builder().collect_u64_metric("anything").is_none());
}

kvs_test!(metrics_none, except = [rocksdb, surrealds, surrealos, surrealos_s3]);

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
