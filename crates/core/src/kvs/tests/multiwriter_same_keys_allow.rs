#![cfg(any(feature = "kv-tikv", feature = "kv-fdb"))]

use super::CreateDs;
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

pub async fn multiwriter_same_keys_allow(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("a19cf00d-f95b-42c6-95e5-7b310162d570").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// Insert an initial key
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.set("test", "some text", None).await.unwrap();
	tx.commit().await.unwrap();
	// Create a writeable transaction
	let mut tx1 = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx1.set("test", "other text 1", None).await.unwrap();
	// Create a writeable transaction
	let mut tx2 = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx2.set("test", "other text 2", None).await.unwrap();
	// Create a writeable transaction
	let mut tx3 = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx3.set("test", "other text 3", None).await.unwrap();
	// Cancel both writeable transactions
	tx1.commit().await.unwrap();
	tx2.commit().await.unwrap();
	tx3.commit().await.unwrap();
	// Check that the key was updated ok
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx.get("test", None).await.unwrap().unwrap();
	assert_eq!(val, b"other text 3");
	tx.cancel().await.unwrap();
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.set("test", "original text", None).await.unwrap();
	tx.commit().await.unwrap();
	// Check that the key was updated ok
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx.get("test", None).await.unwrap().unwrap();
	assert_eq!(val, b"original text");
	tx.cancel().await.unwrap();
}

macro_rules! define_tests {
	($new_ds:ident) => {
		#[tokio::test]
		#[serial_test::serial]
		async fn multiwriter_same_keys_allow() {
			super::multiwriter_same_keys_allow::multiwriter_same_keys_allow($new_ds).await;
		}
	};
}
pub(crate) use define_tests;
