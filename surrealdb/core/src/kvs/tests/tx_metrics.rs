use std::sync::Arc;

use surrealdb_kvs::TransactionType::*;
use uuid::Uuid;

use crate::CommunityComposer;
#[cfg(feature = "kv-mem")]
use crate::key::KVKey;
use crate::key::RawRange;
use crate::kvs::{Datastore, Direction};

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

/// `for_each` must record the same scan metrics (keys + byte counters) as
/// draining `next_batch` — including rows the visitor ignores, since the cursor
/// reads them from storage either way. Guards EXPLAIN ANALYZE / observability.
pub async fn cursor_for_each_metrics_match_next_batch(ds: Arc<Datastore>) {
	let pairs: Vec<(Vec<u8>, Vec<u8>)> = vec![
		(b"a".to_vec(), b"vvv0".to_vec()),
		(b"a\x00".to_vec(), b"vv1".to_vec()),
		(b"ab".to_vec(), b"value-2".to_vec()),
		(b"b".to_vec(), b"v3".to_vec()),
		(b"c".to_vec(), b"value-four".to_vec()),
	];
	let tx = ds.transaction(Write).await.unwrap();
	for (k, v) in &pairs {
		tx.set(k.into(), v).await.unwrap();
	}
	tx.commit().await.unwrap();
	let rng = RawRange::of_bytes(b"a", b"d");

	// Drain via next_batch and snapshot the transaction's scan metrics.
	let tx1 = ds.transaction(Read).await.unwrap();
	{
		let mut c =
			tx1.open_vals_cursor_raw(rng.clone(), Direction::Forward, 0, None).await.unwrap();
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
	let tx2 = ds.transaction(Read).await.unwrap();
	{
		let mut c = tx2.open_vals_cursor_raw(rng, Direction::Forward, 0, None).await.unwrap();
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

/// A key that names no catalog definition, so a write to it is only a write.
#[cfg(feature = "kv-mem")]
#[derive(Debug)]
struct ProbeKey(&'static [u8]);

#[cfg(feature = "kv-mem")]
impl KVKey for ProbeKey {
	type Value = Vec<u8>;

	fn encode_buffer(&self, buffer: &mut Vec<u8>) -> anyhow::Result<()> {
		buffer.extend_from_slice(self.0);
		Ok(())
	}

	fn value_context(&self) {}
}

/// A typed call and the raw call over the same encoded key and value must
/// record the same metrics, step by step, so that the typed path routes through
/// the same accounting as the raw one.
#[cfg(feature = "kv-mem")]
#[tokio::test]
async fn typed_and_raw_paths_record_identical_metrics() {
	use surrealdb_observe::TransactionMetricsSnapshot as Snap;

	let ds =
		Datastore::builder().without_maintenance_tasks().build_with_path("memory").await.unwrap();
	let a = ProbeKey(b"/!metrics-probe-a");
	let b = ProbeKey(b"/!metrics-probe-b");
	let missing = ProbeKey(b"/!metrics-probe-c");
	let value = b"probe-value".to_vec();

	// Each call's increments over the counters before it.
	let delta = |after: &Snap, before: &Snap| {
		(
			after.ops_get - before.ops_get,
			after.keys_read - before.keys_read,
			after.key_bytes_read - before.key_bytes_read,
			after.value_bytes_read - before.value_bytes_read,
			after.ops_set - before.ops_set,
			after.keys_written - before.keys_written,
			after.key_bytes_written - before.key_bytes_written,
			after.value_bytes_written - before.value_bytes_written,
			after.ops_del - before.ops_del,
		)
	};

	let tx = ds.transaction(Write).await.unwrap();

	let m0 = tx.metrics_snapshot_for_test();
	tx.set_key(&a, &value).await.unwrap();
	let m1 = tx.metrics_snapshot_for_test();
	tx.set(b.encode_key().unwrap(), value.clone()).await.unwrap();
	let m2 = tx.metrics_snapshot_for_test();
	let typed_set = delta(&m1, &m0);
	assert_eq!(typed_set, delta(&m2, &m1), "set_key and set differ");
	assert_eq!(typed_set.4, 1, "a set is counted once");

	let got = tx.get_key(&a, None).await.unwrap();
	let m3 = tx.metrics_snapshot_for_test();
	let raw = tx.get(b.encode_key().unwrap(), None).await.unwrap();
	let m4 = tx.metrics_snapshot_for_test();
	assert_eq!(got.as_deref(), Some(value.as_slice()));
	assert_eq!(raw.as_deref(), Some(value.as_slice()));
	let typed_hit = delta(&m3, &m2);
	assert_eq!(typed_hit, delta(&m4, &m3), "get_key and get differ on a hit");
	assert_eq!(typed_hit.1, 1, "a hit finds one key");
	assert_eq!(typed_hit.3, value.len() as u64, "a hit reads the value");

	assert!(tx.get_key(&missing, None).await.unwrap().is_none());
	let m5 = tx.metrics_snapshot_for_test();
	assert!(tx.get(missing.encode_key().unwrap(), None).await.unwrap().is_none());
	let m6 = tx.metrics_snapshot_for_test();
	let typed_miss = delta(&m5, &m4);
	assert_eq!(typed_miss, delta(&m6, &m5), "get_key and get differ on a miss");
	assert_eq!(typed_miss.1, 0, "a miss finds no key");
	assert_eq!(typed_miss.3, 0, "a miss reads no value");

	tx.del_key(&a).await.unwrap();
	let m7 = tx.metrics_snapshot_for_test();
	tx.del(b.encode_key().unwrap()).await.unwrap();
	let m8 = tx.metrics_snapshot_for_test();
	let typed_del = delta(&m7, &m6);
	assert_eq!(typed_del, delta(&m8, &m7), "del_key and del differ");
	assert_eq!(typed_del.8, 1, "a delete is counted once");
	tx.cancel().await.unwrap();

	// A locked read registers the key for commit-time conflict detection, which a
	// read-only transaction never reaches, so it is refused.
	let tx = ds.transaction(Read).await.unwrap();
	let err = tx.getu_key(&a).await.unwrap_err();
	assert!(
		matches!(
			err.downcast_ref::<surrealdb_kvs::Error>(),
			Some(surrealdb_kvs::Error::TransactionReadonly)
		),
		"getu_key on a read-only transaction answered {err:?}"
	);
	tx.cancel().await.unwrap();
}
