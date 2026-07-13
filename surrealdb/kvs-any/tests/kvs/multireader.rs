use surrealdb_kvs::TransactionType::*;

use super::CreateDs;

pub async fn multireader(new_ds: impl CreateDs) {
	// Create a new datastore
	let ds = new_ds.create_ds().await;
	// Insert an initial key
	let tx = ds.transaction(Write).await.unwrap();
	tx.set("test".as_bytes().into(), "some text".as_bytes().to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let tx1 = ds.transaction(Read).await.unwrap();
	let val = tx1.get(b"test".into(), None).await.unwrap().unwrap();
	assert_eq!(val, b"some text");
	// Create a readonly transaction
	let tx2 = ds.transaction(Read).await.unwrap();
	let val = tx2.get(b"test".into(), None).await.unwrap().unwrap();
	assert_eq!(val, b"some text");
	// Create a readonly transaction
	let tx3 = ds.transaction(Read).await.unwrap();
	let val = tx3.get(b"test".into(), None).await.unwrap().unwrap();
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
