use std::sync::Arc;

use uuid::Uuid;

use crate::{
	dbs::node::Timestamp,
	kvs::{
		clock::{FakeClock, SizedClock},
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

		#[tokio::test]
		#[serial_test::serial]
		async fn scan() {
			super::raw::scan($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn batch() {
			super::raw::batch($new_ds).await;
		}
	};
}
pub(crate) use define_tests;
