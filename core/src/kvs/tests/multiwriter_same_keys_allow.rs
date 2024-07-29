#[tokio::test]
#[serial]
async fn multiwriter_same_keys_allow() {
	// Create a new datastore
	let node_id = Uuid::parse_str("a19cf00d-f95b-42c6-95e5-7b310162d570").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds(node_id, clock).await;
	// Insert an initial key
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.set("test", "some text").await.unwrap();
	tx.commit().await.unwrap();
	// Create a writeable transaction
	let mut tx1 = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx1.set("test", "other text 1").await.unwrap();
	// Create a writeable transaction
	let mut tx2 = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx2.set("test", "other text 2").await.unwrap();
	// Create a writeable transaction
	let mut tx3 = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx3.set("test", "other text 3").await.unwrap();
	// Cancel both writeable transactions
	assert!(tx1.commit().await.is_ok());
	assert!(tx2.commit().await.is_ok());
	assert!(tx3.commit().await.is_ok());
	// Check that the key was updated ok
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx.get("test").await.unwrap().unwrap();
	assert_eq!(val, b"other text 3");
	tx.cancel().await.unwrap();
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.set("test", "original text").await.unwrap();
	tx.commit().await.unwrap();
	// Check that the key was updated ok
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let val = tx.get("test").await.unwrap().unwrap();
	assert_eq!(val, b"original text");
	tx.cancel().await.unwrap();
}
