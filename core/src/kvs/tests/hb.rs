#[tokio::test]
#[serial]
async fn write_scan_hb() {
	let nd = uuid::Uuid::parse_str("e80540d4-2869-4bf3-ae27-790a538c53f3").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let test = init(nd, clock).await.unwrap();

	// Add 2 nodes
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();
	let t1 = tx.clock().await;
	let t2 = Timestamp {
		value: t1.value + 1,
	};
	let t3 = Timestamp {
		value: t2.value + 1,
	};
	tx.set_hb(t1, Uuid::parse_str("6d1210a0-9224-4813-8090-ded787d51894").unwrap()).await.unwrap();
	tx.set_hb(t2, Uuid::parse_str("b80ff454-c3e7-46a9-a0b0-7b40e9a62626").unwrap()).await.unwrap();
	tx.commit().await.unwrap();

	// Scan in batches of 1
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();
	let vals_lim = tx.scan_hb(&t3, 1).await.unwrap();
	tx.cancel().await.unwrap();

	// Scan in batches of 100k
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();
	let vals_no_lim = tx.scan_hb(&t3, 100_000).await.unwrap();
	tx.cancel().await.unwrap();

	// Assert equal
	assert_eq!(vals_lim, vals_no_lim);
	assert_eq!(vals_lim.len(), 2);
	assert_eq!(vals_no_lim.len(), 2);
}
