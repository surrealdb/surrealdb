use uuid::Uuid;

use super::CreateDs;
use crate::idx::planner::ScanDirection;
use crate::kvs::LockType::*;
use crate::kvs::TransactionType::*;

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

pub async fn put_after_delete(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("025a7ced-a8ac-40c9-bd15-3400b714dacc").unwrap();
	let (ds, _) = new_ds.create_ds(node_id).await;
	// Create a writeable transaction
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.put(&"test", &"one".as_bytes().to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	// Create a writeable transaction
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.del(&"test").await.unwrap();
	tx.put(&"test", &"two".as_bytes().to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.get(&"test", None).await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"two")));
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
		let batch = cursor.next_batch(1).await.unwrap();
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
		let batch = cursor.next_batch(1).await.unwrap();
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

/// `for_each` must yield the exact same `(key, value)` sequence as draining
/// `next_batch` — across batch boundaries (including prefix-shared keys) and
/// for every backend (resume-by-bound default cursors and the stateful rocksdb
/// cursor). Also checks `stats.rows` equals the number of rows visited.
pub async fn cursor_for_each_vals_matches_next_batch(new_ds: impl CreateDs) {
	let node_id = Uuid::parse_str("7c1e9a44-2b3c-4d5e-8f90-1a2b3c4d5e6f").unwrap();
	let (ds, _) = new_ds.create_ds(node_id).await;
	let pairs: Vec<(Vec<u8>, Vec<u8>)> = vec![
		(b"a".to_vec(), b"v0".to_vec()),
		(b"a\x00".to_vec(), b"v1".to_vec()),
		(b"a\x01".to_vec(), b"v2".to_vec()),
		(b"ab".to_vec(), b"v3".to_vec()),
		(b"b".to_vec(), b"v4".to_vec()),
		(b"bc".to_vec(), b"v5".to_vec()),
		(b"c".to_vec(), b"v6".to_vec()),
	];
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	for (k, v) in &pairs {
		tx.set(k, v).await.unwrap();
	}
	tx.commit().await.unwrap();

	let rng = b"a".to_vec()..b"d".to_vec();
	let tx = ds.transaction(Read, Optimistic).await.unwrap();

	// Drain via `next_batch` (chunk size 2 to exercise boundary handling).
	let mut c1 = tx.open_vals_cursor(rng.clone(), ScanDirection::Forward, 0, None).await.unwrap();
	let mut via_batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
	loop {
		let batch = c1.next_batch(2).await.unwrap();
		if batch.is_empty() {
			break;
		}
		for (k, v) in &batch {
			via_batch.push((k.to_vec(), v.to_vec()));
		}
	}
	drop(c1);

	// Drain via `for_each` (same chunk size).
	let mut c2 = tx.open_vals_cursor(rng, ScanDirection::Forward, 0, None).await.unwrap();
	let mut via_visit: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
	let mut total_rows = 0u64;
	loop {
		let stats = c2
			.for_each(2, &mut |k, v| {
				via_visit.push((k.to_vec(), v.to_vec()));
				Ok(std::ops::ControlFlow::Continue(()))
			})
			.await
			.unwrap();
		total_rows += stats.rows;
		if stats.rows == 0 {
			break;
		}
	}
	drop(c2);
	tx.cancel().await.unwrap();

	assert_eq!(via_batch, pairs, "next_batch baseline drifted");
	assert_eq!(via_visit, via_batch, "for_each yielded a different sequence than next_batch");
	assert_eq!(total_rows as usize, pairs.len(), "for_each stats.rows mismatch");
}

/// Keys-cursor analogue of [`cursor_for_each_vals_matches_next_batch`].
pub async fn cursor_for_each_keys_matches_next_batch(new_ds: impl CreateDs) {
	let node_id = Uuid::parse_str("8d2f0b55-3c4d-5e6f-9a01-2b3c4d5e6f70").unwrap();
	let (ds, _) = new_ds.create_ds(node_id).await;
	let pairs: Vec<(Vec<u8>, Vec<u8>)> = vec![
		(b"k".to_vec(), b"v0".to_vec()),
		(b"k\x00".to_vec(), b"v1".to_vec()),
		(b"kk".to_vec(), b"v2".to_vec()),
		(b"l".to_vec(), b"v3".to_vec()),
		(b"m".to_vec(), b"v4".to_vec()),
	];
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	for (k, v) in &pairs {
		tx.set(k, v).await.unwrap();
	}
	tx.commit().await.unwrap();

	let rng = b"k".to_vec()..b"n".to_vec();
	let tx = ds.transaction(Read, Optimistic).await.unwrap();

	let mut c1 = tx.open_keys_cursor(rng.clone(), ScanDirection::Forward, 0, None).await.unwrap();
	let mut via_batch: Vec<Vec<u8>> = Vec::new();
	loop {
		let batch = c1.next_batch(2).await.unwrap();
		if batch.is_empty() {
			break;
		}
		for k in &batch {
			via_batch.push(k.to_vec());
		}
	}
	drop(c1);

	let mut c2 = tx.open_keys_cursor(rng, ScanDirection::Forward, 0, None).await.unwrap();
	let mut via_visit: Vec<Vec<u8>> = Vec::new();
	loop {
		let stats = c2
			.for_each(2, &mut |k| {
				via_visit.push(k.to_vec());
				Ok(std::ops::ControlFlow::Continue(()))
			})
			.await
			.unwrap();
		if stats.rows == 0 {
			break;
		}
	}
	drop(c2);
	tx.cancel().await.unwrap();

	let expected: Vec<Vec<u8>> = pairs.iter().map(|(k, _)| k.clone()).collect();
	assert_eq!(via_batch, expected, "next_batch keys baseline drifted");
	assert_eq!(via_visit, via_batch, "keys for_each yielded a different sequence than next_batch");
}

/// Backward-scan analogue of [`cursor_for_each_vals_matches_next_batch`] —
/// exercises each engine's backward resume logic (e.g. SurrealKV's
/// `end = next·0x00` and the default cursor's `end = last`).
pub async fn cursor_for_each_vals_matches_next_batch_reverse(new_ds: impl CreateDs) {
	let node_id = Uuid::parse_str("9e3a1c66-4d5e-6f70-ab12-3c4d5e6f7081").unwrap();
	let (ds, _) = new_ds.create_ds(node_id).await;
	let pairs: Vec<(Vec<u8>, Vec<u8>)> = vec![
		(b"a".to_vec(), b"v0".to_vec()),
		(b"a\x00".to_vec(), b"v1".to_vec()),
		(b"a\x01".to_vec(), b"v2".to_vec()),
		(b"ab".to_vec(), b"v3".to_vec()),
		(b"b".to_vec(), b"v4".to_vec()),
		(b"bc".to_vec(), b"v5".to_vec()),
		(b"c".to_vec(), b"v6".to_vec()),
	];
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	for (k, v) in &pairs {
		tx.set(k, v).await.unwrap();
	}
	tx.commit().await.unwrap();

	let rng = b"a".to_vec()..b"d".to_vec();
	let tx = ds.transaction(Read, Optimistic).await.unwrap();

	let mut c1 = tx.open_vals_cursor(rng.clone(), ScanDirection::Backward, 0, None).await.unwrap();
	let mut via_batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
	loop {
		let batch = c1.next_batch(2).await.unwrap();
		if batch.is_empty() {
			break;
		}
		for (k, v) in &batch {
			via_batch.push((k.to_vec(), v.to_vec()));
		}
	}
	drop(c1);

	let mut c2 = tx.open_vals_cursor(rng, ScanDirection::Backward, 0, None).await.unwrap();
	let mut via_visit: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
	loop {
		let stats = c2
			.for_each(2, &mut |k, v| {
				via_visit.push((k.to_vec(), v.to_vec()));
				Ok(std::ops::ControlFlow::Continue(()))
			})
			.await
			.unwrap();
		if stats.rows == 0 {
			break;
		}
	}
	drop(c2);
	tx.cancel().await.unwrap();

	let mut expected = pairs;
	expected.reverse();
	assert_eq!(via_batch, expected, "backward next_batch baseline drifted");
	assert_eq!(via_visit, via_batch, "backward for_each diverged from next_batch");
}

/// `for_each` must record the same scan metrics (keys + byte counters) as
/// draining `next_batch` — including rows the visitor ignores, since the cursor
/// reads them from storage either way. Guards EXPLAIN ANALYZE / observability.
pub async fn cursor_for_each_metrics_match_next_batch(new_ds: impl CreateDs) {
	let node_id = Uuid::parse_str("af71d2c0-5e6f-4a1b-8c2d-3e4f5a6b7c8d").unwrap();
	let (ds, _) = new_ds.create_ds(node_id).await;
	let pairs: Vec<(Vec<u8>, Vec<u8>)> = vec![
		(b"a".to_vec(), b"vvv0".to_vec()),
		(b"a\x00".to_vec(), b"vv1".to_vec()),
		(b"ab".to_vec(), b"value-2".to_vec()),
		(b"b".to_vec(), b"v3".to_vec()),
		(b"c".to_vec(), b"value-four".to_vec()),
	];
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	for (k, v) in &pairs {
		tx.set(k, v).await.unwrap();
	}
	tx.commit().await.unwrap();
	let rng = b"a".to_vec()..b"d".to_vec();

	// Drain via next_batch and snapshot the transaction's scan metrics.
	let tx1 = ds.transaction(Read, Optimistic).await.unwrap();
	{
		let mut c =
			tx1.open_vals_cursor(rng.clone(), ScanDirection::Forward, 0, None).await.unwrap();
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

/// A visitor that `Break`s after every row must still see each row exactly once,
/// in order — exercises the `Break`-then-resume interaction (the broken row is
/// counted and consumed; the cursor resumes strictly after it) on every engine.
pub async fn cursor_for_each_break_resumes(new_ds: impl CreateDs) {
	let node_id = Uuid::parse_str("c2d3e4f5-7081-4923-ab4c-5d6e7f809112").unwrap();
	let (ds, _) = new_ds.create_ds(node_id).await;
	let pairs: Vec<(Vec<u8>, Vec<u8>)> = vec![
		(b"a".to_vec(), b"v0".to_vec()),
		(b"a\x00".to_vec(), b"v1".to_vec()),
		(b"a\x01".to_vec(), b"v2".to_vec()),
		(b"ab".to_vec(), b"v3".to_vec()),
		(b"b".to_vec(), b"v4".to_vec()),
		(b"c".to_vec(), b"v5".to_vec()),
	];
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	for (k, v) in &pairs {
		tx.set(k, v).await.unwrap();
	}
	tx.commit().await.unwrap();
	let rng = b"a".to_vec()..b"d".to_vec();
	let tx = ds.transaction(Read, Optimistic).await.unwrap();

	let mut c = tx.open_vals_cursor(rng, ScanDirection::Forward, 0, None).await.unwrap();
	let mut collected: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
	loop {
		let mut visited = 0u64;
		// A generous limit so the chunk would hold every row — `Break` is what
		// stops it after the first.
		let s = c
			.for_each(100, &mut |k, v| {
				collected.push((k.to_vec(), v.to_vec()));
				visited += 1;
				Ok(std::ops::ControlFlow::Break(()))
			})
			.await
			.unwrap();
		if s.rows == 0 {
			break;
		}
		assert_eq!(visited, 1, "Break should stop the chunk after the first visited row");
		assert_eq!(s.rows, 1, "stats.rows should count the single visited (broken) row");
	}
	drop(c);
	tx.cancel().await.unwrap();

	assert_eq!(collected, pairs, "Break+resume skipped or duplicated rows");
}

/// `for_each` must drain a byte-identical `(key, value)` sequence — and report
/// the same total stats — as draining `next_batch`, for a spread of chunk
/// sizes, across every engine (resume-by-bound default cursors + the stateful
/// rocksdb cursor). For limits smaller than the row count, both paths must
/// also split into more than one chunk — so a backend that ignored the limit
/// and returned the whole range in one page would fail here rather than
/// trivially match itself.
pub async fn cursor_for_each_vals_limit_counts_match_next_batch(new_ds: impl CreateDs) {
	let node_id = Uuid::parse_str("d3e4f5a6-8192-4a34-bc5d-6e7f80911223").unwrap();
	let (ds, _) = new_ds.create_ds(node_id).await;
	// Varied key + value lengths so the byte totals are non-uniform.
	let pairs: Vec<(Vec<u8>, Vec<u8>)> = vec![
		(b"a".to_vec(), b"v0".to_vec()),
		(b"a\x00".to_vec(), b"value-one".to_vec()),
		(b"a\x01".to_vec(), b"vv2".to_vec()),
		(b"ab".to_vec(), b"value-three-longer".to_vec()),
		(b"b".to_vec(), b"v4".to_vec()),
		(b"bc".to_vec(), b"value-5".to_vec()),
		(b"c".to_vec(), b"v6".to_vec()),
	];
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	for (k, v) in &pairs {
		tx.set(k, v).await.unwrap();
	}
	tx.commit().await.unwrap();
	let rng = b"a".to_vec()..b"d".to_vec();

	let expected_key_bytes: u64 = pairs.iter().map(|(k, _)| k.len() as u64).sum();
	let expected_value_bytes: u64 = pairs.iter().map(|(_, v)| v.len() as u64).sum();

	// Single-row chunks, uneven split, near-full, and a single oversized chunk.
	for limit in [1u32, 3, 5, 1000] {
		let tx = ds.transaction(Read, Optimistic).await.unwrap();

		// Drain via next_batch, counting non-empty pages.
		let mut c1 =
			tx.open_vals_cursor(rng.clone(), ScanDirection::Forward, 0, None).await.unwrap();
		let mut via_batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
		let mut batch_chunks = 0u64;
		loop {
			let batch = c1.next_batch(limit).await.unwrap();
			if batch.is_empty() {
				break;
			}
			assert!(batch.len() <= limit as usize, "next_batch overshot limit {limit}");
			batch_chunks += 1;
			for (k, v) in &batch {
				via_batch.push((k.to_vec(), v.to_vec()));
			}
		}
		drop(c1);

		// Drain via for_each (same limit), summing the reported per-chunk stats
		// and counting non-empty chunks.
		let mut c2 =
			tx.open_vals_cursor(rng.clone(), ScanDirection::Forward, 0, None).await.unwrap();
		let mut via_visit: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
		let (mut rows, mut key_bytes, mut value_bytes) = (0u64, 0u64, 0u64);
		let mut visit_chunks = 0u64;
		loop {
			let s = c2
				.for_each(limit, &mut |k, v| {
					via_visit.push((k.to_vec(), v.to_vec()));
					Ok(std::ops::ControlFlow::Continue(()))
				})
				.await
				.unwrap();
			assert!(s.rows <= limit as u64, "for_each overshot limit {limit}");
			rows += s.rows;
			key_bytes += s.key_bytes;
			value_bytes += s.value_bytes;
			if s.rows == 0 {
				break;
			}
			visit_chunks += 1;
		}
		drop(c2);
		tx.cancel().await.unwrap();

		assert_eq!(via_batch, pairs, "next_batch did not fully drain under limit {limit}");
		assert_eq!(via_visit, via_batch, "for_each diverged from next_batch under limit {limit}");
		assert_eq!(rows as usize, pairs.len(), "for_each stats.rows mismatch under limit {limit}");
		assert_eq!(key_bytes, expected_key_bytes, "for_each key_bytes mismatch under {limit}");
		assert_eq!(
			value_bytes, expected_value_bytes,
			"for_each value_bytes mismatch under {limit}"
		);

		// Limits smaller than the row count must chunk on every engine.
		if (limit as usize) < pairs.len() {
			assert!(batch_chunks > 1, "next_batch did not chunk under limit {limit}");
			assert!(visit_chunks > 1, "for_each did not chunk under limit {limit}");
		}
	}
}

/// Keys analogue of [`cursor_for_each_break_resumes`]: a visitor that `Break`s
/// after every key must still see each key exactly once, in order, and the
/// per-chunk `stats.key_bytes` must sum to the keyspace total. Exercises the
/// keys Break-then-resume path (production-reachable from `reference.rs`) on
/// every engine.
pub async fn cursor_for_each_keys_break_resumes(new_ds: impl CreateDs) {
	let node_id = Uuid::parse_str("e4f5a6b7-9203-4a45-bc6d-7e8f90112334").unwrap();
	let (ds, _) = new_ds.create_ds(node_id).await;
	let pairs: Vec<(Vec<u8>, Vec<u8>)> = vec![
		(b"a".to_vec(), b"v0".to_vec()),
		(b"a\x00".to_vec(), b"v1".to_vec()),
		(b"ab".to_vec(), b"v2".to_vec()),
		(b"b".to_vec(), b"v3".to_vec()),
		(b"c".to_vec(), b"v4".to_vec()),
	];
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	for (k, v) in &pairs {
		tx.set(k, v).await.unwrap();
	}
	tx.commit().await.unwrap();
	let rng = b"a".to_vec()..b"d".to_vec();
	let tx = ds.transaction(Read, Optimistic).await.unwrap();

	let mut c = tx.open_keys_cursor(rng, ScanDirection::Forward, 0, None).await.unwrap();
	let mut collected: Vec<Vec<u8>> = Vec::new();
	let mut total_key_bytes = 0u64;
	loop {
		let s = c
			.for_each(100, &mut |k| {
				collected.push(k.to_vec());
				Ok(std::ops::ControlFlow::Break(()))
			})
			.await
			.unwrap();
		total_key_bytes += s.key_bytes;
		if s.rows == 0 {
			break;
		}
		assert_eq!(s.rows, 1, "Break should stop the keys chunk after the first visited key");
	}
	drop(c);
	tx.cancel().await.unwrap();

	let expected: Vec<Vec<u8>> = pairs.iter().map(|(k, _)| k.clone()).collect();
	let expected_bytes: u64 = expected.iter().map(|k| k.len() as u64).sum();
	assert_eq!(collected, expected, "keys Break+resume skipped or duplicated keys");
	assert_eq!(total_key_bytes, expected_bytes, "keys for_each key_bytes mismatch");
}

/// Backward-scan keys analogue of [`cursor_for_each_keys_matches_next_batch`].
pub async fn cursor_for_each_keys_matches_next_batch_reverse(new_ds: impl CreateDs) {
	let node_id = Uuid::parse_str("f5a6b7c8-a314-4b56-9d7e-8f9011223445").unwrap();
	let (ds, _) = new_ds.create_ds(node_id).await;
	let pairs: Vec<(Vec<u8>, Vec<u8>)> = vec![
		(b"k".to_vec(), b"v0".to_vec()),
		(b"k\x00".to_vec(), b"v1".to_vec()),
		(b"kk".to_vec(), b"v2".to_vec()),
		(b"l".to_vec(), b"v3".to_vec()),
		(b"m".to_vec(), b"v4".to_vec()),
	];
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	for (k, v) in &pairs {
		tx.set(k, v).await.unwrap();
	}
	tx.commit().await.unwrap();
	let rng = b"k".to_vec()..b"n".to_vec();
	let tx = ds.transaction(Read, Optimistic).await.unwrap();

	let mut c1 = tx.open_keys_cursor(rng.clone(), ScanDirection::Backward, 0, None).await.unwrap();
	let mut via_batch: Vec<Vec<u8>> = Vec::new();
	loop {
		let batch = c1.next_batch(2).await.unwrap();
		if batch.is_empty() {
			break;
		}
		for k in &batch {
			via_batch.push(k.to_vec());
		}
	}
	drop(c1);

	let mut c2 = tx.open_keys_cursor(rng, ScanDirection::Backward, 0, None).await.unwrap();
	let mut via_visit: Vec<Vec<u8>> = Vec::new();
	loop {
		let stats = c2
			.for_each(2, &mut |k| {
				via_visit.push(k.to_vec());
				Ok(std::ops::ControlFlow::Continue(()))
			})
			.await
			.unwrap();
		if stats.rows == 0 {
			break;
		}
	}
	drop(c2);
	tx.cancel().await.unwrap();

	let mut expected: Vec<Vec<u8>> = pairs.iter().map(|(k, _)| k.clone()).collect();
	expected.reverse();
	assert_eq!(via_batch, expected, "backward next_batch keys baseline drifted");
	assert_eq!(via_visit, via_batch, "backward keys for_each diverged from next_batch");
}

/// `for_each` must apply the leading `skip` exactly like `next_batch` in BOTH
/// directions — backward skip exercises the reverse arm of the shared
/// rocksdb `seek_and_skip` helper and the resume-by-bound cursors' skip burn,
/// for the vals and keys cursors alike (production-reachable via
/// `ORDER BY id DESC` + `START`).
pub async fn cursor_for_each_respects_skip_both_directions(new_ds: impl CreateDs) {
	let node_id = Uuid::parse_str("b1c2d3e4-6f70-4812-9a3b-4c5d6e7f8091").unwrap();
	let (ds, _) = new_ds.create_ds(node_id).await;
	let pairs: Vec<(Vec<u8>, Vec<u8>)> = vec![
		(b"a".to_vec(), b"v0".to_vec()),
		(b"a\x00".to_vec(), b"v1".to_vec()),
		(b"ab".to_vec(), b"v2".to_vec()),
		(b"b".to_vec(), b"v3".to_vec()),
		(b"c".to_vec(), b"v4".to_vec()),
	];
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	for (k, v) in &pairs {
		tx.set(k, v).await.unwrap();
	}
	tx.commit().await.unwrap();
	let rng = b"a".to_vec()..b"d".to_vec();

	for dir in [ScanDirection::Forward, ScanDirection::Backward] {
		let tx = ds.transaction(Read, Optimistic).await.unwrap();

		// Vals: next_batch baseline with skip=2.
		let mut c1 = tx.open_vals_cursor(rng.clone(), dir, 2, None).await.unwrap();
		let mut via_batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
		loop {
			let batch = c1.next_batch(2).await.unwrap();
			if batch.is_empty() {
				break;
			}
			for (k, v) in &batch {
				via_batch.push((k.to_vec(), v.to_vec()));
			}
		}
		drop(c1);

		// Vals: for_each with the same skip.
		let mut c2 = tx.open_vals_cursor(rng.clone(), dir, 2, None).await.unwrap();
		let mut via_visit: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
		loop {
			let s = c2
				.for_each(2, &mut |k, v| {
					via_visit.push((k.to_vec(), v.to_vec()));
					Ok(std::ops::ControlFlow::Continue(()))
				})
				.await
				.unwrap();
			if s.rows == 0 {
				break;
			}
		}
		drop(c2);

		// Keys: for_each with the same skip must match the vals key sequence.
		let mut c3 = tx.open_keys_cursor(rng.clone(), dir, 2, None).await.unwrap();
		let mut keys_visit: Vec<Vec<u8>> = Vec::new();
		loop {
			let s = c3
				.for_each(2, &mut |k| {
					keys_visit.push(k.to_vec());
					Ok(std::ops::ControlFlow::Continue(()))
				})
				.await
				.unwrap();
			if s.rows == 0 {
				break;
			}
		}
		drop(c3);
		tx.cancel().await.unwrap();

		// Build the expected sequence: full ordering for `dir`, minus the
		// first two rows.
		let mut expected = pairs.clone();
		if matches!(dir, ScanDirection::Backward) {
			expected.reverse();
		}
		let expected: Vec<(Vec<u8>, Vec<u8>)> = expected[2..].to_vec();
		assert_eq!(via_batch, expected, "next_batch skip mismatch ({dir:?})");
		assert_eq!(via_visit, via_batch, "vals for_each skip diverged ({dir:?})");
		let expected_keys: Vec<Vec<u8>> = expected.iter().map(|(k, _)| k.clone()).collect();
		assert_eq!(keys_visit, expected_keys, "keys for_each skip diverged ({dir:?})");
	}
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
		async fn put_after_delete() {
			super::raw::put_after_delete($new_ds).await;
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

		#[tokio::test]
		#[serial_test::serial]
		async fn cursor_for_each_vals_matches_next_batch() {
			super::raw::cursor_for_each_vals_matches_next_batch($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn cursor_for_each_keys_matches_next_batch() {
			super::raw::cursor_for_each_keys_matches_next_batch($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn cursor_for_each_vals_matches_next_batch_reverse() {
			super::raw::cursor_for_each_vals_matches_next_batch_reverse($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn cursor_for_each_metrics_match_next_batch() {
			super::raw::cursor_for_each_metrics_match_next_batch($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn cursor_for_each_respects_skip_both_directions() {
			super::raw::cursor_for_each_respects_skip_both_directions($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn cursor_for_each_break_resumes() {
			super::raw::cursor_for_each_break_resumes($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn cursor_for_each_vals_limit_counts_match_next_batch() {
			super::raw::cursor_for_each_vals_limit_counts_match_next_batch($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn cursor_for_each_keys_break_resumes() {
			super::raw::cursor_for_each_keys_break_resumes($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn cursor_for_each_keys_matches_next_batch_reverse() {
			super::raw::cursor_for_each_keys_matches_next_batch_reverse($new_ds).await;
		}
	};
}
pub(crate) use define_tests;

/// Versioned scans through the cursor API: a cursor opened at a historical
/// version must see exactly the rows visible at that timestamp — identically
/// via `next_batch` and `for_each`, in both directions. Covers the versioned
/// forward/backward iterator arms (e.g. `MemValsCursor::build_iter`), which no
/// other cursor test reaches. Mem-only: it needs a datastore built with
/// versioning enabled, which the shared per-engine harness doesn't provide.
#[cfg(feature = "kv-mem")]
#[tokio::test]
#[serial_test::serial]
async fn cursor_versioned_for_each_matches_next_batch() {
	use crate::kvs::Datastore;

	let ds = Datastore::builder().build_with_path("memory?versioned=true").await.unwrap();

	// Batch A: the historical view.
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	let historical: Vec<(Vec<u8>, Vec<u8>)> = vec![
		(b"a".to_vec(), b"v0".to_vec()),
		(b"a\x00".to_vec(), b"v1".to_vec()),
		(b"b".to_vec(), b"v2".to_vec()),
	];
	for (k, v) in &historical {
		tx.set(k, v).await.unwrap();
	}
	tx.commit().await.unwrap();

	// Capture a version timestamp strictly between the two commits. Versions
	// are wall-clock nanoseconds (SurrealQL `VERSION` semantics); the sleeps
	// guard against clock granularity ties on either side.
	tokio::time::sleep(std::time::Duration::from_millis(20)).await;
	let version = web_time::SystemTime::now()
		.duration_since(web_time::SystemTime::UNIX_EPOCH)
		.unwrap()
		.as_nanos() as u64;
	tokio::time::sleep(std::time::Duration::from_millis(20)).await;

	// Batch B: overwrite one row and add another — the current view.
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.set(&b"a".to_vec(), &b"v0-new".to_vec()).await.unwrap();
	tx.set(&b"c".to_vec(), &b"v3".to_vec()).await.unwrap();
	tx.commit().await.unwrap();

	let rng = b"a".to_vec()..b"d".to_vec();
	for dir in [ScanDirection::Forward, ScanDirection::Backward] {
		let tx = ds.transaction(Read, Optimistic).await.unwrap();

		// Historical view via next_batch.
		let mut c1 = tx.open_vals_cursor(rng.clone(), dir, 0, Some(version)).await.unwrap();
		let mut via_batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
		loop {
			let batch = c1.next_batch(2).await.unwrap();
			if batch.is_empty() {
				break;
			}
			for (k, v) in &batch {
				via_batch.push((k.to_vec(), v.to_vec()));
			}
		}
		drop(c1);

		// Historical view via for_each.
		let mut c2 = tx.open_vals_cursor(rng.clone(), dir, 0, Some(version)).await.unwrap();
		let mut via_visit: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
		loop {
			let s = c2
				.for_each(2, &mut |k, v| {
					via_visit.push((k.to_vec(), v.to_vec()));
					Ok(std::ops::ControlFlow::Continue(()))
				})
				.await
				.unwrap();
			if s.rows == 0 {
				break;
			}
		}
		drop(c2);
		tx.cancel().await.unwrap();

		let mut expected = historical.clone();
		if matches!(dir, ScanDirection::Backward) {
			expected.reverse();
		}
		assert_eq!(via_batch, expected, "versioned next_batch view mismatch ({dir:?})");
		assert_eq!(via_visit, via_batch, "versioned for_each diverged from next_batch ({dir:?})");
	}

	// Sanity: the current view (no version) must reflect batch B.
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let mut c = tx.open_vals_cursor(rng, ScanDirection::Forward, 0, None).await.unwrap();
	let mut current: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
	loop {
		let s = c
			.for_each(10, &mut |k, v| {
				current.push((k.to_vec(), v.to_vec()));
				Ok(std::ops::ControlFlow::Continue(()))
			})
			.await
			.unwrap();
		if s.rows == 0 {
			break;
		}
	}
	drop(c);
	tx.cancel().await.unwrap();
	let expected_current: Vec<(Vec<u8>, Vec<u8>)> = vec![
		(b"a".to_vec(), b"v0-new".to_vec()),
		(b"a\x00".to_vec(), b"v1".to_vec()),
		(b"b".to_vec(), b"v2".to_vec()),
		(b"c".to_vec(), b"v3".to_vec()),
	];
	assert_eq!(current, expected_current, "current view should reflect the second commit");
}
