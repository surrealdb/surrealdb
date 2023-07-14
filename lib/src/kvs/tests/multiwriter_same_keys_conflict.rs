#[tokio::test]
#[serial]
async fn multiwriter_same_keys_conflict() {
	// Create a new datastore
	let ds = new_ds().await;
	// Insert an initial key
	let mut tx = ds.transaction(true, false).await.unwrap();
	tx.set("test", "some text").await.unwrap();
	tx.commit().await.unwrap();
	// Create a writeable transaction
	let mut tx1 = ds.transaction(true, false).await.unwrap();
	tx1.set("test", "other text 1").await.unwrap();
	// Create a writeable transaction
	let mut tx2 = ds.transaction(true, false).await.unwrap();
	tx2.set("test", "other text 2").await.unwrap();
	// Create a writeable transaction
	let mut tx3 = ds.transaction(true, false).await.unwrap();
	tx3.set("test", "other text 3").await.unwrap();
	// Cancel both writeable transactions
	assert!(tx1.commit().await.is_ok());
	assert!(tx2.commit().await.is_err());
	assert!(tx3.commit().await.is_err());
	// Check that the key was updated ok
	let mut tx = ds.transaction(false, false).await.unwrap();
	let val = tx.get("test").await.unwrap().unwrap();
	assert_eq!(val, b"other text 1");
	tx.cancel().await.unwrap();
	// Create a writeable transaction
	let mut tx = ds.transaction(true, false).await.unwrap();
	tx.set("test", "original text").await.unwrap();
	tx.commit().await.unwrap();
	// Check that the key was updated ok
	let mut tx = ds.transaction(false, false).await.unwrap();
	let val = tx.get("test").await.unwrap().unwrap();
	assert_eq!(val, b"original text");
	tx.cancel().await.unwrap();
}
