use surrealdb_kvs::TransactionType::*;
use uuid::Uuid;

use crate::CommunityComposer;
use crate::idx::planner::ScanDirection;
use crate::key::KeyRange;
use crate::kvs::Datastore;
use crate::kvs::LockType::*;

#[cfg(feature = "kv-mem")]
#[tokio::test]
async fn mem_cursor_for_each_metrics_match_next_batch() {
	let node_id = Uuid::parse_str("af71d2c0-5e6f-4a1b-8c2d-3e4f5a6b7c8d").unwrap();

	// Setup the in-memory datastore
	let ds = Datastore::builder()
		.with_id(node_id)
		.build_with_factory_path("memory", CommunityComposer())
		.await
		.unwrap();

	cursor_for_each_metrics_match_next_batch(ds).await;
}

#[cfg(feature = "kv-rocksdb")]
#[tokio::test]
async fn rocksdb_cursor_for_each_metrics_match_next_batch() {
	use temp_dir::TempDir;

	let node_id = Uuid::parse_str("af71d2c0-5e6f-4a1b-8c2d-3e4f5a6b7c8d").unwrap();

	// Setup the temporary data storage path
	let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
	let path = format!("rocksdb:{path}");

	// Setup the in-memory datastore
	let ds = Datastore::builder()
		.with_id(node_id)
		.build_with_factory_path(&path, CommunityComposer())
		.await
		.unwrap();

	cursor_for_each_metrics_match_next_batch(ds).await;
}

#[cfg(feature = "kv-surrealkv")]
#[tokio::test]
async fn surrealkv_cursor_for_each_metrics_match_next_batch() {
	use temp_dir::TempDir;

	let node_id = Uuid::parse_str("af71d2c0-5e6f-4a1b-8c2d-3e4f5a6b7c8d").unwrap();

	// Setup the temporary data storage path
	let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
	let path = format!("surrealkv:{path}");

	// Setup the in-memory datastore
	let ds = Datastore::builder()
		.with_id(node_id)
		.build_with_factory_path(&path, CommunityComposer())
		.await
		.unwrap();

	cursor_for_each_metrics_match_next_batch(ds).await;
}

#[cfg(feature = "kv-tikv")]
#[tokio::test]
async fn tikv_cursor_for_each_metrics_match_next_batch() {
	let node_id = Uuid::parse_str("af71d2c0-5e6f-4a1b-8c2d-3e4f5a6b7c8d").unwrap();

	// Setup the cluster connection string
	let path = "tikv:127.0.0.1:2379";

	// Setup the in-memory datastore
	let ds = Datastore::builder()
		.with_id(node_id)
		.build_with_factory_path(path, CommunityComposer())
		.await
		.unwrap();

	cursor_for_each_metrics_match_next_batch(ds).await;
}

/// `for_each` must record the same scan metrics (keys + byte counters) as
/// draining `next_batch` — including rows the visitor ignores, since the cursor
/// reads them from storage either way. Guards EXPLAIN ANALYZE / observability.
pub async fn cursor_for_each_metrics_match_next_batch(ds: Datastore) {
	let pairs: Vec<(Vec<u8>, Vec<u8>)> = vec![
		(b"a".to_vec(), b"vvv0".to_vec()),
		(b"a\x00".to_vec(), b"vv1".to_vec()),
		(b"ab".to_vec(), b"value-2".to_vec()),
		(b"b".to_vec(), b"v3".to_vec()),
		(b"c".to_vec(), b"value-four".to_vec()),
	];
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	for (k, v) in &pairs {
		tx.set(k.into(), v).await.unwrap();
	}
	tx.commit().await.unwrap();
	let rng = KeyRange::from(b"a"..b"d");

	// Drain via next_batch and snapshot the transaction's scan metrics.
	let tx1 = ds.transaction(Read, Optimistic).await.unwrap();
	{
		let mut c =
			tx1.open_vals_cursor(rng.as_borrowed(), ScanDirection::Forward, 0, None).await.unwrap();
		loop {
			let b = c.next_batch(2).await.unwrap();
			if b.is_empty() {
				break;
			}
		}
	}
	let m1 = tx1.metrics_snapshot_for_test();
	tx1.cancel().await.unwrap();

	// Drain via for_each (visitor ignores every row) and snapshot.
	let tx2 = ds.transaction(Read, Optimistic).await.unwrap();
	{
		let mut c = tx2.open_vals_cursor(rng, ScanDirection::Forward, 0, None).await.unwrap();
		loop {
			let s =
				c.for_each(2, &mut |_k, _v| Ok(std::ops::ControlFlow::Continue(()))).await.unwrap();
			if s.rows == 0 {
				break;
			}
		}
	}
	let m2 = tx2.metrics_snapshot_for_test();
	tx2.cancel().await.unwrap();

	assert_eq!(m1.keys_read, pairs.len() as u64, "next_batch keys_read baseline drifted");
	assert_eq!(m2.keys_read, m1.keys_read, "for_each keys_read != next_batch");
	assert_eq!(m2.key_bytes_read, m1.key_bytes_read, "for_each key_bytes_read != next_batch");
	assert_eq!(m2.value_bytes_read, m1.value_bytes_read, "for_each value_bytes_read != next_batch");
}
