#[tokio::test]
#[serial]
async fn sequences() {
	// Create a new datastore
	let node_id = Uuid::parse_str("b7afc077-2123-476f-bee0-43d7504f1e0a").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds(node_id, clock).await;
	// Test separate sequences
	let mut txn = ds.transaction(Write, Optimistic).await.unwrap().inner();
	let nsid = txn.get_next_ns_id().await.unwrap();
	txn.complete_changes(false).await.unwrap();
	txn.commit().await.unwrap();
	assert_eq!(nsid, 0);
	// Test separate sequences
	let mut txn = ds.transaction(Write, Optimistic).await.unwrap();
	let dbid = txn.get_next_db_id(nsid).await.unwrap();
	txn.complete_changes(false).await.unwrap();
	txn.commit().await.unwrap();
	assert_eq!(dbid, 0);
	// Test separate sequences
	let mut txn = ds.transaction(Write, Optimistic).await.unwrap();
	let tbid1 = txn.get_next_tb_id(nsid, dbid).await.unwrap();
	txn.complete_changes(false).await.unwrap();
	txn.commit().await.unwrap();
	assert_eq!(tbid1, 0);
	// Test separate sequences
	let mut txn = ds.transaction(Write, Optimistic).await.unwrap();
	let tbid2 = txn.get_next_tb_id(nsid, dbid).await.unwrap();
	txn.complete_changes(false).await.unwrap();
	txn.commit().await.unwrap();
	assert_eq!(tbid2, 1);
	// Test separate sequences
	let mut txn = ds.transaction(Write, Optimistic).await.unwrap();
	txn.remove_tb_id(nsid, dbid, tbid1).await.unwrap();
	txn.complete_changes(false).await.unwrap();
	txn.commit().await.unwrap();
	// Test separate sequences
	let mut txn = ds.transaction(Write, Optimistic).await.unwrap();
	txn.remove_db_id(nsid, dbid).await.unwrap();
	txn.complete_changes(false).await.unwrap();
	txn.commit().await.unwrap();
	// Test separate sequences
	let mut txn = ds.transaction(Write, Optimistic).await.unwrap();
	txn.remove_ns_id(nsid).await.unwrap();
	txn.complete_changes(false).await.unwrap();
	txn.commit().await.unwrap();
}
