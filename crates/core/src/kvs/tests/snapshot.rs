use std::sync::Arc;

use uuid::Uuid;

use super::CreateDs;
use crate::dbs::node::Timestamp;
use crate::kvs::LockType::*;
use crate::kvs::TransactionType::*;
use crate::kvs::clock::{FakeClock, SizedClock};

pub async fn snapshot(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("056804f2-b379-4397-9ceb-af8ebd527beb").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// Insert an initial key
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.set(&"test", &"some text".as_bytes().to_vec(), None).await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx1 = ds.transaction(Read, Optimistic).await.unwrap().inner();
	// Check that the key was inserted ok
	let val = tx1.get(&"test", None).await.unwrap().unwrap();
	assert_eq!(val, b"some text");
	// Create a new writeable transaction
	let mut txw = ds.transaction(Write, Optimistic).await.unwrap().inner();
	// Update the test key content
	txw.set(&"test", &"other text".as_bytes().to_vec(), None).await.unwrap();
	// Create a readonly transaction
	let mut tx2 = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx2.get(&"test", None).await.unwrap().unwrap();
	assert_eq!(val, b"some text");
	// Create a readonly transaction
	let mut tx3 = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx3.get(&"test", None).await.unwrap().unwrap();
	assert_eq!(val, b"some text");
	// Update the test key content
	txw.set(&"test", &"extra text".as_bytes().to_vec(), None).await.unwrap();
	// Check the key from the original transaction
	let val = tx1.get(&"test", None).await.unwrap().unwrap();
	assert_eq!(val, b"some text");
	// Cancel both readonly transactions
	tx1.cancel().await.unwrap();
	tx2.cancel().await.unwrap();
	tx3.cancel().await.unwrap();
	// Commit the writable transaction
	txw.commit().await.unwrap();
	// Check that the key was updated ok
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx.get(&"test", None).await.unwrap().unwrap();
	assert_eq!(val, b"extra text");
	tx.cancel().await.unwrap();
}

macro_rules! define_tests {
	($new_ds:ident) => {
		#[tokio::test]
		#[serial_test::serial]
		async fn snapshot() {
			super::snapshot::snapshot($new_ds).await;
		}
	};
}
pub(crate) use define_tests;
