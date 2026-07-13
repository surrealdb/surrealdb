use super::CreateDs;
use super::TransactionType::*;

pub async fn snapshot(new_ds: impl CreateDs) {
	// Create a new datastore
	let ds = new_ds.create_ds().await;
	// Insert an initial key
	let tx = ds.transaction(Write).await.unwrap();
	tx.set("test".as_bytes().into(), "some text".as_bytes().to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let tx1 = ds.transaction(Read).await.unwrap();
	// Check that the key was inserted ok
	let val = tx1.get("test".as_bytes().into(), None).await.unwrap().unwrap();
	assert_eq!(val, b"some text");
	// Create a new writeable transaction
	let txw = ds.transaction(Write).await.unwrap();
	// Update the test key content
	txw.set("test".as_bytes().into(), "other text".as_bytes().to_vec()).await.unwrap();
	// Create a readonly transaction
	let tx2 = ds.transaction(Read).await.unwrap();
	let val = tx2.get("test".as_bytes().into(), None).await.unwrap().unwrap();
	assert_eq!(val, b"some text");
	// Create a readonly transaction
	let tx3 = ds.transaction(Read).await.unwrap();
	let val = tx3.get("test".as_bytes().into(), None).await.unwrap().unwrap();
	assert_eq!(val, b"some text");
	// Update the test key content
	txw.set("test".as_bytes().into(), "extra text".as_bytes().to_vec()).await.unwrap();
	// Check the key from the original transaction
	let val = tx1.get("test".as_bytes().into(), None).await.unwrap().unwrap();
	assert_eq!(val, b"some text");
	// Cancel both readonly transactions
	tx1.cancel().await.unwrap();
	tx2.cancel().await.unwrap();
	tx3.cancel().await.unwrap();
	// Commit the writable transaction
	txw.commit().await.unwrap();
	// Check that the key was updated ok
	let tx = ds.transaction(Read).await.unwrap();
	let val = tx.get("test".as_bytes().into(), None).await.unwrap().unwrap();
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
