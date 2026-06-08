use uuid::Uuid;

use super::CreateDs;
use crate::idx::planner::ScanDirection;
use crate::kvs::LockType::*;
use crate::kvs::TransactionType::*;
use crate::kvs::api::ScanLimit;

pub async fn initialise(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("d09445ed-520b-438c-b275-0f3c768bdb8d").unwrap();
	let (ds, _) = new_ds.create_ds(node_id).await;
	// Create a writeable transaction
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.put(&"test", &"ok".as_bytes().to_vec()).await.unwrap();
	tx.commit().await.unwrap();
}

pub async fn exists(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("463a5008-ee1d-43db-9662-5e752b6ea3f9").unwrap();
	let (ds, _) = new_ds.create_ds(node_id).await;
	// Create a writeable transaction
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.put(&"test", &"ok".as_bytes().to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.exists(&"test", None).await.unwrap();
	assert!(val);
	let val = tx.exists(&"none", None).await.unwrap();
	assert!(!val);
	tx.cancel().await.unwrap();
}

pub async fn get(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("477e2895-8c98-4606-a827-0add82eb466b").unwrap();
	let (ds, _) = new_ds.create_ds(node_id).await;
	// Create a writeable transaction
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.put(&"test", &"ok".as_bytes().to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.get(&"test", None).await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"ok")));
	let val = tx.get(&"none", None).await.unwrap();
	assert!(val.as_deref().is_none());
	tx.cancel().await.unwrap();
}

pub async fn set(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("32b80d8b-dd16-4f6f-a687-1192f6cfc6f1").unwrap();
	let (ds, _) = new_ds.create_ds(node_id).await;
	// Create a writeable transaction
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.set(&"test", &"one".as_bytes().to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.get(&"test", None).await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"one")));
	tx.cancel().await.unwrap();
	// Create a writeable transaction
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.set(&"test", &"two".as_bytes().to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.get(&"test", None).await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"two")));
	tx.cancel().await.unwrap();
}

pub async fn put(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("80149655-db34-451c-8711-6fa662a44b70").unwrap();
	let (ds, _) = new_ds.create_ds(node_id).await;
	// Create a writeable transaction
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.put(&"test", &"one".as_bytes().to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.get(&"test", None).await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"one")));
	tx.cancel().await.unwrap();
	// Create a writeable transaction
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	assert!(tx.put(&"test", &"two".as_bytes().to_vec()).await.is_err());
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.get(&"test", None).await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"one")));
	tx.cancel().await.unwrap();
}

pub async fn putc(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("705bb520-bc2b-4d52-8e64-d1214397e408").unwrap();
	let (ds, _) = new_ds.create_ds(node_id).await;
	// Create a writeable transaction
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.put(&"test", &"one".as_bytes().to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.get(&"test", None).await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"one")));
	tx.cancel().await.unwrap();
	// Create a writeable transaction
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.putc(&"test", &"two".as_bytes().to_vec(), Some(&"one".as_bytes().to_vec())).await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.get(&"test", None).await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"two")));
	tx.cancel().await.unwrap();
	// Create a writeable transaction
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	assert!(
		tx.putc(&"test", &"tre".as_bytes().to_vec(), Some(&"one".as_bytes().to_vec()))
			.await
			.is_err()
	);
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.get(&"test", None).await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"two")));
	tx.cancel().await.unwrap();
}

pub async fn del(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("e0acb360-9187-401f-8192-f870b09e2c9e").unwrap();
	let (ds, _) = new_ds.create_ds(node_id).await;
	// Create a writeable transaction
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.put(&"test", &"one".as_bytes().to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	// Create a writeable transaction
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.del(&"test").await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.get(&"test", None).await.unwrap();
	assert!(val.as_deref().is_none());
	tx.cancel().await.unwrap();
}

pub async fn delc(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("0985488e-cf2f-417a-bd10-7f4aa9c99c15").unwrap();
	let (ds, _) = new_ds.create_ds(node_id).await;
	// Create a writeable transaction
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.put(&"test", &"one".as_bytes().to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	// Create a writeable transaction
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	assert!(tx.delc(&"test", Some(&"two".as_bytes().to_vec())).await.is_err());
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.get(&"test", None).await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"one")));
	tx.cancel().await.unwrap();
	// Create a writeable transaction
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.delc(&"test", Some(&"one".as_bytes().to_vec())).await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.get(&"test", None).await.unwrap();
	assert!(val.as_deref().is_none());
	tx.cancel().await.unwrap();
}

pub async fn keys(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("83b81cc2-9609-4533-bede-c170ab9f7bbe").unwrap();
	let (ds, _) = new_ds.create_ds(node_id).await;
	// Create a writeable transaction
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.put(&"test1", &"1".as_bytes().to_vec()).await.unwrap();
	tx.put(&"test2", &"2".as_bytes().to_vec()).await.unwrap();
	tx.put(&"test3", &"3".as_bytes().to_vec()).await.unwrap();
	tx.put(&"test4", &"4".as_bytes().to_vec()).await.unwrap();
	tx.put(&"test5", &"5".as_bytes().to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.keys("test1".."test9", u32::MAX, 0, None).await.unwrap();
	assert_eq!(val.len(), 5);
	assert_eq!(val[0], b"test1");
	assert_eq!(val[1], b"test2");
	assert_eq!(val[2], b"test3");
	assert_eq!(val[3], b"test4");
	assert_eq!(val[4], b"test5");
	tx.cancel().await.unwrap();
	// Create a readonly transaction
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.keys("test2".."test4", u32::MAX, 0, None).await.unwrap();
	assert_eq!(val.len(), 2);
	assert_eq!(val[0], b"test2");
	assert_eq!(val[1], b"test3");
	tx.cancel().await.unwrap();
	// Create a readonly transaction
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.keys("test1".."test9", 2, 0, None).await.unwrap();
	assert_eq!(val.len(), 2);
	assert_eq!(val[0], b"test1");
	assert_eq!(val[1], b"test2");
	tx.cancel().await.unwrap();
}

pub async fn keysr(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("83b81cc2-9609-4533-bede-c170ab9f7bbe").unwrap();
	let (ds, _) = new_ds.create_ds(node_id).await;
	// Create a writeable transaction
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.put(&"test1", &"1".as_bytes().to_vec()).await.unwrap();
	tx.put(&"test2", &"2".as_bytes().to_vec()).await.unwrap();
	tx.put(&"test3", &"3".as_bytes().to_vec()).await.unwrap();
	tx.put(&"test4", &"4".as_bytes().to_vec()).await.unwrap();
	tx.put(&"test5", &"5".as_bytes().to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.keysr("test1".."test9", u32::MAX, 0, None).await.unwrap();
	assert_eq!(val.len(), 5);
	assert_eq!(val[0], b"test5");
	assert_eq!(val[1], b"test4");
	assert_eq!(val[2], b"test3");
	assert_eq!(val[3], b"test2");
	assert_eq!(val[4], b"test1");
	tx.cancel().await.unwrap();
	// Create a readonly transaction
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.keysr("test2".."test4", u32::MAX, 0, None).await.unwrap();
	assert_eq!(val.len(), 2);
	assert_eq!(val[0], b"test3");
	assert_eq!(val[1], b"test2");
	tx.cancel().await.unwrap();
	// Create a readonly transaction
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.keysr("test1".."test9", 2, 0, None).await.unwrap();
	assert_eq!(val.len(), 2);
	assert_eq!(val[0], b"test5");
	assert_eq!(val[1], b"test4");
	tx.cancel().await.unwrap();
}

pub async fn scan(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("83b81cc2-9609-4533-bede-c170ab9f7bbe").unwrap();
	let (ds, _) = new_ds.create_ds(node_id).await;
	// Create a writeable transaction
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.put(&"test1", &"1".as_bytes().to_vec()).await.unwrap();
	tx.put(&"test2", &"2".as_bytes().to_vec()).await.unwrap();
	tx.put(&"test3", &"3".as_bytes().to_vec()).await.unwrap();
	tx.put(&"test4", &"4".as_bytes().to_vec()).await.unwrap();
	tx.put(&"test5", &"5".as_bytes().to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.scan("test1".."test9", u32::MAX, 0, None).await.unwrap();
	assert_eq!(val.len(), 5);
	assert_eq!(val[0].0, b"test1");
	assert_eq!(val[0].1, b"1");
	assert_eq!(val[1].0, b"test2");
	assert_eq!(val[1].1, b"2");
	assert_eq!(val[2].0, b"test3");
	assert_eq!(val[2].1, b"3");
	assert_eq!(val[3].0, b"test4");
	assert_eq!(val[3].1, b"4");
	assert_eq!(val[4].0, b"test5");
	assert_eq!(val[4].1, b"5");
	tx.cancel().await.unwrap();
	// Create a readonly transaction
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.scan("test2".."test4", u32::MAX, 0, None).await.unwrap();
	assert_eq!(val.len(), 2);
	assert_eq!(val[0].0, b"test2");
	assert_eq!(val[0].1, b"2");
	assert_eq!(val[1].0, b"test3");
	assert_eq!(val[1].1, b"3");
	tx.cancel().await.unwrap();
	// Create a readonly transaction
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.scan("test1".."test9", 2, 0, None).await.unwrap();
	assert_eq!(val.len(), 2);
	assert_eq!(val[0].0, b"test1");
	assert_eq!(val[0].1, b"1");
	assert_eq!(val[1].0, b"test2");
	assert_eq!(val[1].1, b"2");
	tx.cancel().await.unwrap();
}

pub async fn scanr(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("83b81cc2-9609-4533-bede-c170ab9f7bbe").unwrap();
	let (ds, _) = new_ds.create_ds(node_id).await;
	// Create a writeable transaction
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.put(&"test1", &"1".as_bytes().to_vec()).await.unwrap();
	tx.put(&"test2", &"2".as_bytes().to_vec()).await.unwrap();
	tx.put(&"test3", &"3".as_bytes().to_vec()).await.unwrap();
	tx.put(&"test4", &"4".as_bytes().to_vec()).await.unwrap();
	tx.put(&"test5", &"5".as_bytes().to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.scanr("test1".."test9", u32::MAX, 0, None).await.unwrap();
	assert_eq!(val.len(), 5);
	assert_eq!(val[0].0, b"test5");
	assert_eq!(val[0].1, b"5");
	assert_eq!(val[1].0, b"test4");
	assert_eq!(val[1].1, b"4");
	assert_eq!(val[2].0, b"test3");
	assert_eq!(val[2].1, b"3");
	assert_eq!(val[3].0, b"test2");
	assert_eq!(val[3].1, b"2");
	assert_eq!(val[4].0, b"test1");
	assert_eq!(val[4].1, b"1");
	tx.cancel().await.unwrap();
	// Create a readonly transaction
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.scanr("test2".."test4", u32::MAX, 0, None).await.unwrap();
	assert_eq!(val.len(), 2);
	assert_eq!(val[0].0, b"test3");
	assert_eq!(val[0].1, b"3");
	assert_eq!(val[1].0, b"test2");
	assert_eq!(val[1].1, b"2");
	tx.cancel().await.unwrap();
	// Create a readonly transaction
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.scanr("test1".."test9", 2, 0, None).await.unwrap();
	assert_eq!(val.len(), 2);
	assert_eq!(val[0].0, b"test5");
	assert_eq!(val[0].1, b"5");
	assert_eq!(val[1].0, b"test4");
	assert_eq!(val[1].1, b"4");
	tx.cancel().await.unwrap();
}

pub async fn skip(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("83b81cc2-9609-4533-bede-c170ab9f7bbe").unwrap();
	let (ds, _) = new_ds.create_ds(node_id).await;
	// Create a writeable transaction
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.put(&"test1", &"1".as_bytes().to_vec()).await.unwrap();
	tx.put(&"test2", &"2".as_bytes().to_vec()).await.unwrap();
	tx.put(&"test3", &"3".as_bytes().to_vec()).await.unwrap();
	tx.put(&"test4", &"4".as_bytes().to_vec()).await.unwrap();
	tx.put(&"test5", &"5".as_bytes().to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	// Test keys with skip 2
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.keys("test1".."test9", u32::MAX, 2, None).await.unwrap();
	assert_eq!(val.len(), 3);
	assert_eq!(val[0], b"test3");
	assert_eq!(val[1], b"test4");
	assert_eq!(val[2], b"test5");
	tx.cancel().await.unwrap();
	// Test keys with skip and limit
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.keys("test1".."test9", 2, 2, None).await.unwrap();
	assert_eq!(val.len(), 2);
	assert_eq!(val[0], b"test3");
	assert_eq!(val[1], b"test4");
	tx.cancel().await.unwrap();
	// Test keys with skip past all entries
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.keys("test1".."test9", u32::MAX, 10, None).await.unwrap();
	assert_eq!(val.len(), 0);
	tx.cancel().await.unwrap();
	// Test keysr with skip 2
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.keysr("test1".."test9", u32::MAX, 2, None).await.unwrap();
	assert_eq!(val.len(), 3);
	assert_eq!(val[0], b"test3");
	assert_eq!(val[1], b"test2");
	assert_eq!(val[2], b"test1");
	tx.cancel().await.unwrap();
	// Test scan with skip 2
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.scan("test1".."test9", u32::MAX, 2, None).await.unwrap();
	assert_eq!(val.len(), 3);
	assert_eq!(val[0].0, b"test3");
	assert_eq!(val[0].1, b"3");
	assert_eq!(val[1].0, b"test4");
	assert_eq!(val[1].1, b"4");
	assert_eq!(val[2].0, b"test5");
	assert_eq!(val[2].1, b"5");
	tx.cancel().await.unwrap();
	// Test scanr with skip 2
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.scanr("test1".."test9", u32::MAX, 2, None).await.unwrap();
	assert_eq!(val.len(), 3);
	assert_eq!(val[0].0, b"test3");
	assert_eq!(val[0].1, b"3");
	assert_eq!(val[1].0, b"test2");
	assert_eq!(val[1].1, b"2");
	assert_eq!(val[2].0, b"test1");
	assert_eq!(val[2].1, b"1");
	tx.cancel().await.unwrap();
	// Test skip 0 returns all entries (no skip)
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.keys("test1".."test9", u32::MAX, 0, None).await.unwrap();
	assert_eq!(val.len(), 5);
	tx.cancel().await.unwrap();
}

pub async fn batch(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("6572a13c-a7a0-4e19-be62-18acb4e854f5").unwrap();
	let (ds, _) = new_ds.create_ds(node_id).await;
	// Create a writeable transaction
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.put(&"test1", &"1".as_bytes().to_vec()).await.unwrap();
	tx.put(&"test2", &"2".as_bytes().to_vec()).await.unwrap();
	tx.put(&"test3", &"3".as_bytes().to_vec()).await.unwrap();
	tx.put(&"test4", &"4".as_bytes().to_vec()).await.unwrap();
	tx.put(&"test5", &"5".as_bytes().to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let rng = "test1".."test9";
	let res = tx.batch_keys_vals(rng, u32::MAX, None).await.unwrap();
	let val = res.result;
	assert_eq!(val.len(), 5);
	assert_eq!(val[0].0, b"test1");
	assert_eq!(val[0].1, b"1");
	assert_eq!(val[1].0, b"test2");
	assert_eq!(val[1].1, b"2");
	assert_eq!(val[2].0, b"test3");
	assert_eq!(val[2].1, b"3");
	assert_eq!(val[3].0, b"test4");
	assert_eq!(val[3].1, b"4");
	assert_eq!(val[4].0, b"test5");
	assert_eq!(val[4].1, b"5");
	tx.cancel().await.unwrap();
	// Create a readonly transaction
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let rng = "test2".."test4";
	let res = tx.batch_keys_vals(rng, u32::MAX, None).await.unwrap();
	let val = res.result;
	assert_eq!(val.len(), 2);
	assert_eq!(val[0].0, b"test2");
	assert_eq!(val[0].1, b"2");
	assert_eq!(val[1].0, b"test3");
	assert_eq!(val[1].1, b"3");
	tx.cancel().await.unwrap();
	// Create a readonly transaction
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let rng = "test2".."test4";
	let res = tx.batch_keys_vals(rng, u32::MAX, None).await.unwrap();
	let val = res.result;
	assert_eq!(val.len(), 2);
	assert_eq!(val[0].0, b"test2");
	assert_eq!(val[0].1, b"2");
	assert_eq!(val[1].0, b"test3");
	assert_eq!(val[1].1, b"3");
	tx.cancel().await.unwrap();
}

/// Regression test for the default cursor's forward successor logic.
///
/// `DefaultKeysCursor::next_batch` (used by mem/surrealkv/tikv/indxdb)
/// previously did `rng.start = last; rng.start.push(0xff)` to resume past
/// the batch's last key. That jumps from `last` to `last\xff`, skipping
/// every key in between — including `last\0` if it exists. Under the
/// migrated scan operators which consume the cursor batch-by-batch, this
/// silently dropped rows at batch boundaries.
///
/// The fix appends `\x00` to `last` — the minimal key strictly greater
/// than `last` — so no key in `(last, ...]` is skipped. This test
/// inserts prefix-sharing keys (`a`, `a\0`, `a\x01`, `ab`, `b`) and
/// pumps a cursor with `Count(1)` so every adjacent pair is a batch
/// boundary; without the fix the cursor would return only `a` and `b`.
pub async fn cursor_keys_resume_past_prefix(new_ds: impl CreateDs) {
	let node_id = Uuid::parse_str("9b4d3e72-1f2a-4f8a-9e8d-3c1d6a7e1f01").unwrap();
	let (ds, _) = new_ds.create_ds(node_id).await;
	let keys: Vec<Vec<u8>> =
		vec![b"a".to_vec(), b"a\x00".to_vec(), b"a\x01".to_vec(), b"ab".to_vec(), b"b".to_vec()];
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	for k in &keys {
		tx.set(k, &b"v".to_vec()).await.unwrap();
	}
	tx.commit().await.unwrap();

	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	// `Count(1)` forces a fresh range advance after every key — every
	// adjacent pair becomes a boundary at which the broken successor
	// logic would skip.
	let mut cursor = tx
		.open_keys_cursor(b"a".to_vec()..b"c".to_vec(), ScanDirection::Forward, 0, None)
		.await
		.unwrap();
	let mut collected: Vec<Vec<u8>> = Vec::new();
	loop {
		let batch = cursor.next_batch(ScanLimit::Count(1)).await.unwrap();
		if batch.is_empty() {
			break;
		}
		for k in &batch {
			collected.push(k.to_vec());
		}
	}
	// Drop the cursor before cancelling: backends that drain live cursors
	// during cancel (rocksdb) wait forever for `cursors_alive == 0`.
	drop(cursor);
	tx.cancel().await.unwrap();

	assert_eq!(
		collected, keys,
		"default cursor skipped prefix-shared keys at batch boundaries; \
		successor logic must append `\\x00` to `last`, not `\\xff`"
	);
}

/// Same as [`cursor_keys_resume_past_prefix`] for the vals cursor —
/// `DefaultValsCursor` had the same `push(0xff)` bug.
pub async fn cursor_vals_resume_past_prefix(new_ds: impl CreateDs) {
	let node_id = Uuid::parse_str("9b4d3e72-1f2a-4f8a-9e8d-3c1d6a7e1f02").unwrap();
	let (ds, _) = new_ds.create_ds(node_id).await;
	let pairs: Vec<(Vec<u8>, Vec<u8>)> = vec![
		(b"a".to_vec(), b"v0".to_vec()),
		(b"a\x00".to_vec(), b"v1".to_vec()),
		(b"a\x01".to_vec(), b"v2".to_vec()),
		(b"ab".to_vec(), b"v3".to_vec()),
		(b"b".to_vec(), b"v4".to_vec()),
	];
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	for (k, v) in &pairs {
		tx.set(k, v).await.unwrap();
	}
	tx.commit().await.unwrap();

	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let mut cursor = tx
		.open_vals_cursor(b"a".to_vec()..b"c".to_vec(), ScanDirection::Forward, 0, None)
		.await
		.unwrap();
	let mut collected: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
	loop {
		let batch = cursor.next_batch(ScanLimit::Count(1)).await.unwrap();
		if batch.is_empty() {
			break;
		}
		for (k, v) in &batch {
			collected.push((k.to_vec(), v.to_vec()));
		}
	}
	// Drop the cursor before cancelling: backends that drain live cursors
	// during cancel (rocksdb) wait forever for `cursors_alive == 0`.
	drop(cursor);
	tx.cancel().await.unwrap();

	assert_eq!(
		collected, pairs,
		"default vals cursor skipped prefix-shared keys at batch boundaries"
	);
}

macro_rules! define_tests {
	($new_ds:ident) => {
		#[tokio::test]
		#[serial_test::serial]
		async fn initialise() {
			super::raw::initialise($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn exists() {
			super::raw::exists($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn get() {
			super::raw::get($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn set() {
			super::raw::set($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn put() {
			super::raw::put($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn putc() {
			super::raw::putc($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn del() {
			super::raw::del($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn delc() {
			super::raw::delc($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn keys() {
			super::raw::keys($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn keysr() {
			super::raw::keysr($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn scan() {
			super::raw::scan($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn scanr() {
			super::raw::scanr($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn skip() {
			super::raw::skip($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn batch() {
			super::raw::batch($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn cursor_keys_resume_past_prefix() {
			super::raw::cursor_keys_resume_past_prefix($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn cursor_vals_resume_past_prefix() {
			super::raw::cursor_vals_resume_past_prefix($new_ds).await;
		}
	};
}
pub(crate) use define_tests;
