#[tokio::test]
#[serial]
async fn multireader() {
	// Create a new datastore
	let node_id = Uuid::from_str("b7afc077-2123-476f-bee0-43d7504f1e0a").unwrap();
	let clock = Arc::new(RwLock::new(SizedClock::Fake(FakeClock::new(Timestamp::default()))));
	let (ds, _) = new_ds(node_id, clock).await;

	// Insert an initial key
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.set("test", "some text").await.unwrap();
	tx.commit().await.unwrap();

	// Create a readonly transaction
	let mut tx1 = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx1
		.get("test")
		.await
		.map_err(|e| format!("Failed to get value in tx1: {}", e))
		.unwrap()
		.ok_or(format!("value from tx1 was None"))
		.unwrap();
	assert_eq!(val, b"some text");

	// Create a readonly transaction
	let mut tx2 = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx2
		.get("test")
		.await
		.map_err(|e| format!("Failed to get value in tx2: {}", e))
		.unwrap()
		.ok_or(format!("value from tx2 was None"))
		.unwrap();
	assert_eq!(val, b"some text");

	// Create a readonly transaction
	let mut tx3 = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx3
		.get("test")
		.await
		.map_err(|e| format!("Failed to get value in tx3: {}", e))
		.unwrap()
		.ok_or(format!("value from tx3 was None"))
		.unwrap();
	assert_eq!(val, b"some text");

	// Cancel both readonly transactions
	tx1.cancel().await.map_err(|err| format!("Failed the first transaction: {}", err)).unwrap();
	tx2.cancel().await.map_err(|err| format!("Failed the second transaction: {}", err)).unwrap();
	tx3.cancel().await.map_err(|err| format!("Failed the third transaction: {}", err)).unwrap();
}
