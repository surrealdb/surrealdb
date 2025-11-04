use std::sync::Arc;

use uuid::Uuid;

use super::CreateDs;
use crate::dbs::node::Timestamp;
use crate::kvs::LockType::*;
use crate::kvs::TransactionType::*;
use crate::kvs::clock::{FakeClock, SizedClock};

pub async fn multiwriter_different_keys(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("7f0153b0-79cf-4922-85ef-61e390970514").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// Insert an initial key
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.set(&"test", &"some text".as_bytes().to_vec(), None).await.unwrap();
	tx.commit().await.unwrap();
	// Create a writeable transaction
	let mut tx1 = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx1.set(&"test1", &"other text 1".as_bytes().to_vec(), None).await.unwrap();
	// Create a writeable transaction
	let mut tx2 = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx2.set(&"test2", &"other text 2".as_bytes().to_vec(), None).await.unwrap();
	// Create a writeable transaction
	let mut tx3 = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx3.set(&"test3", &"other text 3".as_bytes().to_vec(), None).await.unwrap();
	// Cancel both writeable transactions
	tx1.commit().await.unwrap();
	tx2.commit().await.unwrap();
	tx3.commit().await.unwrap();
	// Check that the key was updated ok
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx.get(&"test", None).await.unwrap().unwrap();
	assert_eq!(val, b"some text");
	let val = tx.get(&"test1", None).await.unwrap().unwrap();
	assert_eq!(val, b"other text 1");
	let val = tx.get(&"test2", None).await.unwrap().unwrap();
	assert_eq!(val, b"other text 2");
	let val = tx.get(&"test3", None).await.unwrap().unwrap();
	assert_eq!(val, b"other text 3");
	tx.cancel().await.unwrap();
}

macro_rules! define_tests {
	($new_ds:ident) => {
		#[tokio::test]
		#[serial_test::serial]
		async fn multiwriter_different_keys() {
			super::multiwriter_different_keys::multiwriter_different_keys($new_ds).await;
		}
	};
}
pub(crate) use define_tests;
