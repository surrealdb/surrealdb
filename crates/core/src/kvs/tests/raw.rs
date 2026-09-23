use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use crate::key::root::rc::{ReclaimKey, ReclaimKind, ReclaimState};
use crate::{
	dbs::node::Timestamp,
	kvs::{
		clock::{FakeClock, SizedClock},
		KeyEncode,
		LockType::*,
		TransactionType::*,
	},
};

use super::CreateDs;

pub async fn initialise(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("d09445ed-520b-438c-b275-0f3c768bdb8d").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.put("test", "ok", None).await.unwrap();
	tx.commit().await.unwrap();
}

pub async fn exists(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("463a5008-ee1d-43db-9662-5e752b6ea3f9").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.put("test", "ok", None).await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx.exists("test", None).await.unwrap();
	assert!(val);
	let val = tx.exists("none", None).await.unwrap();
	assert!(!val);
	tx.cancel().await.unwrap();
}

pub async fn get(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("477e2895-8c98-4606-a827-0add82eb466b").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.put("test", "ok", None).await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx.get("test", None).await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"ok")));
	let val = tx.get("none", None).await.unwrap();
	assert!(val.as_deref().is_none());
	tx.cancel().await.unwrap();
}

pub async fn set(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("32b80d8b-dd16-4f6f-a687-1192f6cfc6f1").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.set("test", "one", None).await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx.get("test", None).await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"one")));
	tx.cancel().await.unwrap();
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.set("test", "two", None).await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx.get("test", None).await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"two")));
	tx.cancel().await.unwrap();
}

pub async fn put(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("80149655-db34-451c-8711-6fa662a44b70").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.put("test", "one", None).await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx.get("test", None).await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"one")));
	tx.cancel().await.unwrap();
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	assert!(tx.put("test", "two", None).await.is_err());
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx.get("test", None).await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"one")));
	tx.cancel().await.unwrap();
}

pub async fn putc(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("705bb520-bc2b-4d52-8e64-d1214397e408").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.put("test", "one", None).await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx.get("test", None).await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"one")));
	tx.cancel().await.unwrap();
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.putc("test", "two", Some("one")).await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx.get("test", None).await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"two")));
	tx.cancel().await.unwrap();
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	assert!(tx.putc("test", "tre", Some("one")).await.is_err());
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx.get("test", None).await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"two")));
	tx.cancel().await.unwrap();
}

pub async fn del(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("e0acb360-9187-401f-8192-f870b09e2c9e").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.put("test", "one", None).await.unwrap();
	tx.commit().await.unwrap();
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.del("test").await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx.get("test", None).await.unwrap();
	assert!(val.as_deref().is_none());
	tx.cancel().await.unwrap();
}

pub async fn delc(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("0985488e-cf2f-417a-bd10-7f4aa9c99c15").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.put("test", "one", None).await.unwrap();
	tx.commit().await.unwrap();
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	assert!(tx.delc("test", Some("two")).await.is_err());
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx.get("test", None).await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"one")));
	tx.cancel().await.unwrap();
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.delc("test", Some("one")).await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx.get("test", None).await.unwrap();
	assert!(val.as_deref().is_none());
	tx.cancel().await.unwrap();
}

pub async fn keys(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("83b81cc2-9609-4533-bede-c170ab9f7bbe").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.put("test1", "1", None).await.unwrap();
	tx.put("test2", "2", None).await.unwrap();
	tx.put("test3", "3", None).await.unwrap();
	tx.put("test4", "4", None).await.unwrap();
	tx.put("test5", "5", None).await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx.keys("test1".."test9", u32::MAX, None).await.unwrap();
	assert_eq!(val.len(), 5);
	assert_eq!(val[0], b"test1");
	assert_eq!(val[1], b"test2");
	assert_eq!(val[2], b"test3");
	assert_eq!(val[3], b"test4");
	assert_eq!(val[4], b"test5");
	tx.cancel().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx.keys("test2".."test4", u32::MAX, None).await.unwrap();
	assert_eq!(val.len(), 2);
	assert_eq!(val[0], b"test2");
	assert_eq!(val[1], b"test3");
	tx.cancel().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx.keys("test1".."test9", 2, None).await.unwrap();
	assert_eq!(val.len(), 2);
	assert_eq!(val[0], b"test1");
	assert_eq!(val[1], b"test2");
	tx.cancel().await.unwrap();
}

pub async fn delp_bounded(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("5a7cbf9f-8b2e-4dbb-9f2a-5f0f6a2b0c11").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// The prefix the pages walk, and one key that must survive it
	let rng = || b"test".to_vec()..b"test\xff".to_vec();
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.put("test1", "1", None).await.unwrap();
	tx.put("test2", "2", None).await.unwrap();
	tx.put("test3", "3", None).await.unwrap();
	tx.put("test4", "4", None).await.unwrap();
	tx.put("test5", "5", None).await.unwrap();
	tx.put("zzzz", "z", None).await.unwrap();
	tx.commit().await.unwrap();
	// A page deletes at most `limit` keys and reports the last one
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	let (cursor, count, drained) = tx.delp_bounded(rng(), None, 2, false).await.unwrap();
	assert_eq!((count, drained), (2, false));
	assert_eq!(cursor.as_deref(), Some(b"test2".as_slice()));
	tx.commit().await.unwrap();
	// Only that page was deleted
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx.keys(rng(), u32::MAX, None).await.unwrap();
	assert_eq!(val, vec![b"test3".to_vec(), b"test4".to_vec(), b"test5".to_vec()]);
	tx.cancel().await.unwrap();
	// The next page resumes strictly after the committed cursor
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	let (cursor, count, drained) =
		tx.delp_bounded(rng(), cursor.as_deref(), 2, false).await.unwrap();
	assert_eq!((count, drained), (2, false));
	assert_eq!(cursor.as_deref(), Some(b"test4".as_slice()));
	tx.commit().await.unwrap();
	// A short page means the range is drained
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	let (cursor, count, drained) =
		tx.delp_bounded(rng(), cursor.as_deref(), 10, false).await.unwrap();
	assert_eq!((count, drained), (1, true));
	assert_eq!(cursor.as_deref(), Some(b"test5".as_slice()));
	tx.commit().await.unwrap();
	// A drained range reports no progress
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	let (cursor, count, drained) =
		tx.delp_bounded(rng(), cursor.as_deref(), 10, false).await.unwrap();
	assert_eq!((count, drained), (0, true));
	assert_eq!(cursor, None);
	tx.commit().await.unwrap();
	// Keys outside the prefix were never touched
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	assert!(tx.keys(rng(), u32::MAX, None).await.unwrap().is_empty());
	assert!(tx.exists("zzzz", None).await.unwrap());
	tx.cancel().await.unwrap();
}

pub async fn delp_bounded_ignores_a_cursor_outside_the_range(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("1c1b9d0e-2f3a-4a7d-9a1e-6b0d2e4f8a22").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.put("test1", "1", None).await.unwrap();
	tx.put("test2", "2", None).await.unwrap();
	tx.put("test3", "3", None).await.unwrap();
	tx.commit().await.unwrap();
	// A cursor below the range start is ignored, so the scan cannot reach keys
	// outside the prefix the caller asked for
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	let rng = b"test2".to_vec()..b"test\xff".to_vec();
	let (cursor, count, drained) = tx.delp_bounded(rng, Some(b"test1"), 10, false).await.unwrap();
	assert_eq!((count, drained), (2, true));
	assert_eq!(cursor.as_deref(), Some(b"test3".as_slice()));
	tx.commit().await.unwrap();
	// The key below the range start survived
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	assert!(tx.exists("test1", None).await.unwrap());
	assert!(!tx.exists("test2", None).await.unwrap());
	assert!(!tx.exists("test3", None).await.unwrap());
	tx.cancel().await.unwrap();
}
#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
pub async fn keysr(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("83b81cc2-9609-4533-bede-c170ab9f7bbe").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.put("test1", "1", None).await.unwrap();
	tx.put("test2", "2", None).await.unwrap();
	tx.put("test3", "3", None).await.unwrap();
	tx.put("test4", "4", None).await.unwrap();
	tx.put("test5", "5", None).await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx.keysr("test1".."test9", u32::MAX, None).await.unwrap();
	assert_eq!(val.len(), 5);
	assert_eq!(val[0], b"test5");
	assert_eq!(val[1], b"test4");
	assert_eq!(val[2], b"test3");
	assert_eq!(val[3], b"test2");
	assert_eq!(val[4], b"test1");
	tx.cancel().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx.keysr("test2".."test4", u32::MAX, None).await.unwrap();
	assert_eq!(val.len(), 2);
	assert_eq!(val[0], b"test3");
	assert_eq!(val[1], b"test2");
	tx.cancel().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx.keysr("test1".."test9", 2, None).await.unwrap();
	assert_eq!(val.len(), 2);
	assert_eq!(val[0], b"test5");
	assert_eq!(val[1], b"test4");
	tx.cancel().await.unwrap();
}

pub async fn scan(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("83b81cc2-9609-4533-bede-c170ab9f7bbe").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.put("test1", "1", None).await.unwrap();
	tx.put("test2", "2", None).await.unwrap();
	tx.put("test3", "3", None).await.unwrap();
	tx.put("test4", "4", None).await.unwrap();
	tx.put("test5", "5", None).await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx.scan("test1".."test9", u32::MAX, None).await.unwrap();
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
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx.scan("test2".."test4", u32::MAX, None).await.unwrap();
	assert_eq!(val.len(), 2);
	assert_eq!(val[0].0, b"test2");
	assert_eq!(val[0].1, b"2");
	assert_eq!(val[1].0, b"test3");
	assert_eq!(val[1].1, b"3");
	tx.cancel().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx.scan("test1".."test9", 2, None).await.unwrap();
	assert_eq!(val.len(), 2);
	assert_eq!(val[0].0, b"test1");
	assert_eq!(val[0].1, b"1");
	assert_eq!(val[1].0, b"test2");
	assert_eq!(val[1].1, b"2");
	tx.cancel().await.unwrap();
}

#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
pub async fn scanr(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("83b81cc2-9609-4533-bede-c170ab9f7bbe").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.put("test1", "1", None).await.unwrap();
	tx.put("test2", "2", None).await.unwrap();
	tx.put("test3", "3", None).await.unwrap();
	tx.put("test4", "4", None).await.unwrap();
	tx.put("test5", "5", None).await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx.scanr("test1".."test9", u32::MAX, None).await.unwrap();
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
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx.scanr("test2".."test4", u32::MAX, None).await.unwrap();
	assert_eq!(val.len(), 2);
	assert_eq!(val[0].0, b"test3");
	assert_eq!(val[0].1, b"3");
	assert_eq!(val[1].0, b"test2");
	assert_eq!(val[1].1, b"2");
	tx.cancel().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx.scanr("test1".."test9", 2, None).await.unwrap();
	assert_eq!(val.len(), 2);
	assert_eq!(val[0].0, b"test5");
	assert_eq!(val[0].1, b"5");
	assert_eq!(val[1].0, b"test4");
	assert_eq!(val[1].1, b"4");
	tx.cancel().await.unwrap();
}

pub async fn batch(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("6572a13c-a7a0-4e19-be62-18acb4e854f5").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.put("test1", "1", None).await.unwrap();
	tx.put("test2", "2", None).await.unwrap();
	tx.put("test3", "3", None).await.unwrap();
	tx.put("test4", "4", None).await.unwrap();
	tx.put("test5", "5", None).await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let rng = "test1".as_bytes().."test9".as_bytes();
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
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let rng = "test2".as_bytes().."test4".as_bytes();
	let res = tx.batch_keys_vals(rng, u32::MAX, None).await.unwrap();
	let val = res.result;
	assert_eq!(val.len(), 2);
	assert_eq!(val[0].0, b"test2");
	assert_eq!(val[0].1, b"2");
	assert_eq!(val[1].0, b"test3");
	assert_eq!(val[1].1, b"3");
	tx.cancel().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let rng = "test2".as_bytes().."test4".as_bytes();
	let res = tx.batch_keys_vals(rng, u32::MAX, None).await.unwrap();
	let val = res.result;
	assert_eq!(val.len(), 2);
	assert_eq!(val[0].0, b"test2");
	assert_eq!(val[0].1, b"2");
	assert_eq!(val[1].0, b"test3");
	assert_eq!(val[1].1, b"3");
	tx.cancel().await.unwrap();
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

		#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
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

		#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
		#[tokio::test]
		#[serial_test::serial]
		async fn scanr() {
			super::raw::scanr($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn batch() {
			super::raw::batch($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn delp_bounded() {
			super::raw::delp_bounded($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn delp_bounded_ignores_a_cursor_outside_the_range() {
			super::raw::delp_bounded_ignores_a_cursor_outside_the_range($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn del_db_deferred() {
			super::raw::del_db_deferred($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn del_ns_deferred() {
			super::raw::del_ns_deferred($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn del_db_deferred_rolls_back_as_a_unit() {
			super::raw::del_db_deferred_rolls_back_as_a_unit($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn reclaim_tombstones_serves_the_grace() {
			super::raw::reclaim_tombstones_serves_the_grace($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn reclaim_tombstones_pages_a_large_prefix() {
			super::raw::reclaim_tombstones_pages_a_large_prefix($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn reclaim_tombstones_resumes_from_the_committed_cursor() {
			super::raw::reclaim_tombstones_resumes_from_the_committed_cursor($new_ds).await;
		}
	};
}
pub async fn del_db_deferred(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("6d2f4b31-70a1-4f0c-9f4b-3d8e1c0a7b33").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A catalog definition, and one key inside the database data prefix
	let cat = crate::key::namespace::db::new("ns", "db").encode().unwrap();
	let mut data = crate::key::database::all::new("ns", "db").encode().unwrap();
	data.extend_from_slice(b"!tbtest\0");
	// Create a writeable transaction
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.set(cat.clone(), vec![1u8], None).await.unwrap();
	tx.set(data.clone(), vec![9u8], None).await.unwrap();
	tx.commit().await.unwrap();
	// Remove the database
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.del_db_deferred("ns", "db").await.unwrap();
	tx.commit().await.unwrap();
	// The catalog definition is gone, but the data prefix is untouched
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	assert!(tx.get(cat, None).await.unwrap().is_none());
	assert!(tx.get(data.clone(), None).await.unwrap().is_some());
	// Exactly one reclaim job names the removed database
	let (beg, end) = ReclaimKey::range();
	let res = tx.scan(beg..end, u32::MAX, None).await.unwrap();
	assert_eq!(res.len(), 1);
	let key = ReclaimKey::decode_key(&res[0].0).unwrap();
	assert_eq!(key.kind, ReclaimKind::Database);
	assert_eq!(key.ns, "ns");
	assert_eq!(key.db.as_deref(), Some("db"));
	let state: ReclaimState = revision::from_slice(&res[0].1).unwrap();
	assert_eq!(state, ReclaimState::enqueued());
	// The job names the whole database data prefix
	let rng = key.prefix_range().unwrap();
	assert!(rng.start <= data && data < rng.end);
	tx.cancel().await.unwrap();
}

pub async fn del_ns_deferred(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("2e8a0c74-51d6-4a3b-8c9e-7f1b6d2a4e44").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A catalog definition, and one key inside the namespace data prefix
	let cat = crate::key::root::ns::new("ns").encode().unwrap();
	let mut data = crate::key::namespace::all::new("ns").encode().unwrap();
	data.extend_from_slice(b"!dbtest\0");
	// Create a writeable transaction
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.set(cat.clone(), vec![1u8], None).await.unwrap();
	tx.set(data.clone(), vec![9u8], None).await.unwrap();
	tx.commit().await.unwrap();
	// Remove the namespace
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.del_ns_deferred("ns").await.unwrap();
	tx.commit().await.unwrap();
	// The catalog definition is gone, but the data prefix is untouched
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	assert!(tx.get(cat, None).await.unwrap().is_none());
	assert!(tx.get(data.clone(), None).await.unwrap().is_some());
	// Exactly one reclaim job names the removed namespace
	let (beg, end) = ReclaimKey::range();
	let res = tx.scan(beg..end, u32::MAX, None).await.unwrap();
	assert_eq!(res.len(), 1);
	let key = ReclaimKey::decode_key(&res[0].0).unwrap();
	assert_eq!(key.kind, ReclaimKind::Namespace);
	assert_eq!(key.ns, "ns");
	assert_eq!(key.db, None);
	// The job names the whole namespace data prefix
	let rng = key.prefix_range().unwrap();
	assert!(rng.start <= data && data < rng.end);
	tx.cancel().await.unwrap();
}

pub async fn del_db_deferred_rolls_back_as_a_unit(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("9f0c3b18-4d2a-4e6f-b1a7-0c5e8d3f2a55").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A catalog definition to remove
	let cat = crate::key::namespace::db::new("ns", "db").encode().unwrap();
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.set(cat.clone(), vec![1u8], None).await.unwrap();
	tx.commit().await.unwrap();
	// Remove the database, then abandon the transaction
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.del_db_deferred("ns", "db").await.unwrap();
	tx.cancel().await.unwrap();
	// Neither the removal nor the reclaim job survives
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	assert!(tx.get(cat, None).await.unwrap().is_some());
	let (beg, end) = ReclaimKey::range();
	assert!(tx.scan(beg..end, u32::MAX, None).await.unwrap().is_empty());
	tx.cancel().await.unwrap();
}

pub async fn reclaim_tombstones_serves_the_grace(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("3b6e1f2c-8a4d-4c1e-9b7f-2d5a0e6c8b77").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// One key inside the database data prefix, and one outside it
	let mut data = crate::key::database::all::new("ns", "db").encode().unwrap();
	data.extend_from_slice(b"!tbtest\0");
	let other = crate::key::database::all::new("ns", "keep").encode().unwrap();
	// Create a writeable transaction
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	let cat = crate::key::namespace::db::new("ns", "db").encode().unwrap();
	tx.set(cat, vec![1u8], None).await.unwrap();
	tx.set(data.clone(), vec![9u8], None).await.unwrap();
	tx.set(other.clone(), vec![9u8], None).await.unwrap();
	tx.commit().await.unwrap();
	// Remove the database, enqueueing its data prefix for reclaim
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.del_db_deferred("ns", "db").await.unwrap();
	tx.commit().await.unwrap();
	// The pass which first sees a job only stamps it
	let res = ds.reclaim_tombstones(Duration::from_secs(60)).await.unwrap();
	assert_eq!(res, (1, 0));
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	assert!(tx.get(data.clone(), None).await.unwrap().is_some());
	let (beg, end) = ReclaimKey::range();
	let job = tx.scan(beg..end, u32::MAX, None).await.unwrap();
	assert_eq!(job.len(), 1);
	let state: ReclaimState = revision::from_slice(&job[0].1).unwrap();
	assert!(state.is_observed());
	tx.cancel().await.unwrap();
	// A pass which meets the job again does not restamp it: the stamp is what
	// orders the queue, so a prefix served over several passes keeps its place
	let res = ds.reclaim_tombstones(Duration::from_secs(60)).await;
	assert_eq!(res.unwrap(), (1, 0));
	// It destroys the prefix and retires the job, keeping the stamp it was given
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	assert!(tx.get(data, None).await.unwrap().is_none());
	assert!(tx.get(other, None).await.unwrap().is_some());
	let (beg, end) = ReclaimKey::range();
	assert!(tx.scan(beg..end, u32::MAX, None).await.unwrap().is_empty());
	tx.cancel().await.unwrap();
}

pub async fn reclaim_tombstones_pages_a_large_prefix(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("7c4a2e90-1b6d-4f83-a2c5-9e0b7d3f1a88").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// More keys than one page can delete, so the pass must commit several
	let prefix = crate::key::database::all::new("ns", "db").encode().unwrap();
	let key = |n: u32| {
		let mut k = prefix.clone();
		k.extend_from_slice(&n.to_be_bytes());
		k
	};
	let count = 2_500;
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	for n in 0..count {
		tx.set(key(n), vec![9u8], None).await.unwrap();
	}
	tx.commit().await.unwrap();
	// Remove the database, then stamp the job so that it is eligible
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.del_db_deferred("ns", "db").await.unwrap();
	tx.commit().await.unwrap();
	let res = ds.reclaim_tombstones(Duration::from_secs(60)).await.unwrap();
	assert_eq!(res, (1, 0));
	// The whole prefix is destroyed within one pass, and the job is removed
	let res = ds.reclaim_tombstones(Duration::from_secs(60)).await.unwrap();
	assert_eq!(res, (1, 0));
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let mut end = prefix.clone();
	end.push(0xff);
	assert!(tx.keys(prefix..end, u32::MAX, None).await.unwrap().is_empty());
	let (beg, end) = ReclaimKey::range();
	assert!(tx.scan(beg..end, u32::MAX, None).await.unwrap().is_empty());
	tx.cancel().await.unwrap();
}

pub async fn reclaim_tombstones_resumes_from_the_committed_cursor(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("0a9d5c33-6e2b-4718-8f4a-1b7c3d9e0f66").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// Three keys inside the database data prefix
	let key = |n: &str| {
		let mut k = crate::key::database::all::new("ns", "db").encode().unwrap();
		k.extend_from_slice(n.as_bytes());
		k
	};
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.set(key("a"), vec![1u8], None).await.unwrap();
	tx.set(key("b"), vec![1u8], None).await.unwrap();
	tx.set(key("c"), vec![1u8], None).await.unwrap();
	// A job which has already been observed, and whose committed cursor
	// accounts for the first of those keys
	let job = ReclaimKey::database(Cow::Borrowed("ns"), Cow::Borrowed("db"), Uuid::now_v7());
	let state = ReclaimState {
		observed_ms: 1,
		cursor: Some(key("a")),
	};
	tx.set(&job, revision::to_vec(&state).unwrap(), None).await.unwrap();
	tx.commit().await.unwrap();
	// The pass resumes strictly after the cursor rather than restarting the
	// prefix, so the keys above it go first. Draining that window does not
	// retire the job: what is behind the cursor has not been looked at, and a
	// transaction older than the removal can commit there without conflicting
	// with it. The job is retired only once a sweep from the start of the
	// prefix finds nothing, which takes the key at the cursor with it.
	let res = ds.reclaim_tombstones(Duration::from_secs(60)).await.unwrap();
	assert_eq!(res, (1, 0));
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	assert!(tx.get(key("a"), None).await.unwrap().is_none());
	assert!(tx.get(key("b"), None).await.unwrap().is_none());
	assert!(tx.get(key("c"), None).await.unwrap().is_none());
	let (beg, end) = ReclaimKey::range();
	assert!(tx.scan(beg..end, u32::MAX, None).await.unwrap().is_empty());
	tx.cancel().await.unwrap();
}

pub(crate) use define_tests;
