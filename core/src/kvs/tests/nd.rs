#[tokio::test]
#[serial]
async fn write_scan_nd() {
	let nd = uuid::Uuid::parse_str("6a6a4e59-3e86-431d-884f-8f433781e4e9").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let test = init(nd, clock).await.unwrap();

	// Add 2 nodes
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();
	tx.set_nd(Uuid::parse_str("83d9b3c0-f3c4-45be-9ef9-9d48502fecb1").unwrap()).await.unwrap();
	tx.set_nd(Uuid::parse_str("cbefc4fe-8ba0-4898-ab69-782e3ebc06f9").unwrap()).await.unwrap();
	tx.commit().await.unwrap();

	// Scan in batches of 1
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();
	let res_many_batches = tx.scan_nd(1).await.unwrap();
	tx.cancel().await.unwrap();

	// Scan in batches of 100k
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();
	let res_single_batch = tx.scan_nd(100_000).await.unwrap();
	tx.cancel().await.unwrap();

	// Assert equal
	assert_eq!(res_many_batches, res_single_batch);
	assert_eq!(res_many_batches.len(), 2);
	assert_eq!(res_single_batch.len(), 2);
}
