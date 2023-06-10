#[tokio::test]
#[serial]
async fn multiwriter_same_key() {
	// Create a new datastore
	let ds = new_ds().await;
	// Insert an initial key
	let mut tx = ds.transaction(true, false).await.unwrap();
	tx.set("test", "some text").await.unwrap();
	tx.commit().await.unwrap();
	// Create a writeable transaction
	let mut tx1 = ds.transaction(true, false).await.unwrap();
	tx1.set("test", "other text").await.unwrap();
	// Create a writeable transaction
	let mut tx2 = ds.transaction(true, false).await.unwrap();
	tx2.set("test", "other text").await.unwrap();
	// Create a writeable transaction
	let mut tx3 = ds.transaction(true, false).await.unwrap();
	tx3.set("test", "other text").await.unwrap();
	// Cancel both writeable transactions
	assert!(tx1.commit().await.is_ok());
	assert!(tx2.commit().await.is_err());
	assert!(tx3.commit().await.is_err());
	// Check that the key was updated ok
	let mut tx = ds.transaction(false, false).await.unwrap();
	let val = tx.get("test").await.unwrap().unwrap();
	assert_eq!(std::str::from_utf8(&val).unwrap(), "other text");
	tx.cancel().await.unwrap();
	// Create a writeable transaction
	let mut tx = ds.transaction(true, false).await.unwrap();
	tx.set("test", "original text").await.unwrap();
	tx.commit().await.unwrap();
	// Check that the key was updated ok
	let mut tx = ds.transaction(false, false).await.unwrap();
	let val = tx.get("test").await.unwrap().unwrap();
	assert_eq!(std::str::from_utf8(&val).unwrap(), "original text");
	tx.cancel().await.unwrap();
}

#[tokio::test]
#[serial]
async fn multiwriter_different_keys() {
	// Create a new datastore
	let ds = new_ds().await;
	// Insert an initial key
	let mut tx = ds.transaction(true, false).await.unwrap();
	tx.set("test", "some text").await.unwrap();
	tx.commit().await.unwrap();
	// Create a writeable transaction
	let mut tx1 = ds.transaction(true, false).await.unwrap();
	tx1.set("test1", "other text 1").await.unwrap();
	// Create a writeable transaction
	let mut tx2 = ds.transaction(true, false).await.unwrap();
	tx2.set("test2", "other text 2").await.unwrap();
	// Create a writeable transaction
	let mut tx3 = ds.transaction(true, false).await.unwrap();
	tx3.set("test3", "other text 3").await.unwrap();
	// Cancel both writeable transactions
	tx1.commit().await.unwrap();
	tx2.commit().await.unwrap();
	tx3.commit().await.unwrap();
	// Check that the key was updated ok
	let mut tx = ds.transaction(false, false).await.unwrap();
	let val = tx.get("test").await.unwrap().unwrap();
	assert_eq!(std::str::from_utf8(&val).unwrap(), "some text");
	let val = tx.get("test1").await.unwrap().unwrap();
	assert_eq!(std::str::from_utf8(&val).unwrap(), "other text 1");
	let val = tx.get("test2").await.unwrap().unwrap();
	assert_eq!(std::str::from_utf8(&val).unwrap(), "other text 2");
	let val = tx.get("test3").await.unwrap().unwrap();
	assert_eq!(std::str::from_utf8(&val).unwrap(), "other text 3");
	tx.cancel().await.unwrap();
}
