use std::sync::Arc;

use uuid::Uuid;

use super::CreateDs;
use crate::dbs::node::Timestamp;
use crate::kvs::LockType::*;
use crate::kvs::TransactionType::*;
use crate::kvs::clock::{FakeClock, SizedClock};

pub async fn multireader(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("b7afc077-2123-476f-bee0-43d7504f1e0a").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// Insert an initial key
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.set(&"test", &"some text".as_bytes().to_vec(), None).await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx1 = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx1.get(&"test", None).await.unwrap().unwrap();
	assert_eq!(val, b"some text");
	// Create a readonly transaction
	let mut tx2 = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx2.get(&"test", None).await.unwrap().unwrap();
	assert_eq!(val, b"some text");
	// Create a readonly transaction
	let mut tx3 = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx3.get(&"test", None).await.unwrap().unwrap();
	assert_eq!(val, b"some text");
	// Cancel both readonly transactions
	tx1.cancel().await.unwrap();
	tx2.cancel().await.unwrap();
	tx3.cancel().await.unwrap();
}

macro_rules! define_tests {
	($new_ds:ident) => {
		#[tokio::test]
		#[serial_test::serial]
		async fn multireader() {
			super::multireader::multireader($new_ds).await;
		}
	};
}
pub(crate) use define_tests;
