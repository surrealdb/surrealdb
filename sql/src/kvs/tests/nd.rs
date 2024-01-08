#[tokio::test]
#[serial]
async fn write_scan_nd() {
	let nd = uuid::Uuid::parse_str("6a6a4e59-3e86-431d-884f-8f433781e4e9").unwrap();
	let clock = Arc::new(RwLock::new(SizedClock::Fake(FakeClock::new(Timestamp::default()))));
	let test = init(nd, clock).await.unwrap();

	// Add 2 nodes
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();
	tx.set_nd(Uuid::parse_str("83d9b3c0-f3c4-45be-9ef9-9d48502fecb1").unwrap()).await.unwrap();
	tx.set_nd(Uuid::parse_str("cbefc4fe-8ba0-4898-ab69-782e3ebc06f9").unwrap()).await.unwrap();
	tx.commit().await.unwrap();

	// Scan limit 1000
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();
	let vals_lim = tx.scan_nd(1000).await.unwrap();
	tx.cancel().await.unwrap();

	// Scan limit 0
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();
	let vals_no_lim = tx.scan_nd(NO_LIMIT).await.unwrap();
	tx.cancel().await.unwrap();

	// Assert equal
	assert_eq!(vals_lim, vals_no_lim);
	assert_eq!(vals_lim.len(), 2);
	assert_eq!(vals_no_lim.len(), 2);
}
