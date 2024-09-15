// Timestamp to versionstamp tests
// This translation mechanism is currently used by the garbage collector to determine which change feed entries to delete.
//
// FAQ:
// Q: What’s the difference between database TS and database VS?
// A: Timestamps are basically seconds since the unix epoch.
//    Versionstamps can be anything that is provided by our TSO.
// Q: Why do we need to translate timestamps to versionstamps?
// A: The garbage collector needs to know which change feed entries to delete.
//    However our SQL syntax `DEFINE DATABASE foo CHANGEFEED 1h` let the user specify the expiration in a duration, not a delta in the versionstamp.
//    We need to translate the timestamp to the versionstamp due to that; `now - 1h` to a key suffixed by the versionstamp.
#[tokio::test]
#[serial]
async fn timestamp_to_versionstamp() {
	// Create a new datastore
	let node_id = Uuid::parse_str("A905CA25-56ED-49FB-B759-696AEA87C342").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds(node_id, clock).await;
	// Give the current versionstamp a timestamp of 0
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.set_timestamp_for_versionstamp(0, "myns", "mydb").await.unwrap();
	tx.commit().await.unwrap();
	// Get the versionstamp for timestamp 0
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	let vs1 = tx.get_versionstamp_from_timestamp(0, "myns", "mydb").await.unwrap().unwrap();
	tx.commit().await.unwrap();
	// Give the current versionstamp a timestamp of 1
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.set_timestamp_for_versionstamp(1, "myns", "mydb").await.unwrap();
	tx.commit().await.unwrap();
	// Get the versionstamp for timestamp 1
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	let vs2 = tx.get_versionstamp_from_timestamp(1, "myns", "mydb").await.unwrap().unwrap();
	tx.commit().await.unwrap();
	// Give the current versionstamp a timestamp of 2
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.set_timestamp_for_versionstamp(2, "myns", "mydb").await.unwrap();
	tx.commit().await.unwrap();
	// Get the versionstamp for timestamp 2
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	let vs3 = tx.get_versionstamp_from_timestamp(2, "myns", "mydb").await.unwrap().unwrap();
	tx.commit().await.unwrap();
	assert!(vs1 < vs2);
	assert!(vs2 < vs3);
}

#[tokio::test]
#[serial]
async fn writing_ts_again_results_in_following_ts() {
	// Create a new datastore
	let node_id = Uuid::parse_str("A905CA25-56ED-49FB-B759-696AEA87C342").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds(node_id, clock).await;

	// Declare ns/db
	ds.execute("USE NS myns; USE DB mydb; CREATE record", &Session::owner(), None).await.unwrap();

	// Give the current versionstamp a timestamp of 0
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.set_timestamp_for_versionstamp(0, "myns", "mydb").await.unwrap();
	tx.commit().await.unwrap();

	// Get the versionstamp for timestamp 0
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	let vs1 = tx.get_versionstamp_from_timestamp(0, "myns", "mydb").await.unwrap().unwrap();
	tx.commit().await.unwrap();

	// Give the current versionstamp a timestamp of 1
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.set_timestamp_for_versionstamp(1, "myns", "mydb").await.unwrap();
	tx.commit().await.unwrap();

	// Get the versionstamp for timestamp 1
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	let vs2 = tx.get_versionstamp_from_timestamp(1, "myns", "mydb").await.unwrap().unwrap();
	tx.commit().await.unwrap();

	assert!(vs1 < vs2);

	// Scan range
	let start = crate::key::database::ts::new("myns", "mydb", 0);
	let end = crate::key::database::ts::new("myns", "mydb", u64::MAX);
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	let scanned = tx.scan(start.clone()..end.clone(), u32::MAX, None).await.unwrap();
	tx.commit().await.unwrap();
	assert_eq!(scanned.len(), 2);
	assert_eq!(scanned[0].0, crate::key::database::ts::new("myns", "mydb", 0).encode().unwrap());
	assert_eq!(scanned[1].0, crate::key::database::ts::new("myns", "mydb", 1).encode().unwrap());

	// Repeating tick
	ds.tick_at(1).await.unwrap();

	// Validate
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	let scanned = tx.scan(start..end, u32::MAX, None).await.unwrap();
	tx.commit().await.unwrap();
	assert_eq!(scanned.len(), 3);
	assert_eq!(scanned[0].0, crate::key::database::ts::new("myns", "mydb", 0).encode().unwrap());
	assert_eq!(scanned[1].0, crate::key::database::ts::new("myns", "mydb", 1).encode().unwrap());
	assert_eq!(scanned[2].0, crate::key::database::ts::new("myns", "mydb", 2).encode().unwrap());
}
