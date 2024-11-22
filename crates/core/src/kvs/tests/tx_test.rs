use crate::key::debug::sprint;
use crate::sql::Strand;

#[tokio::test]
#[serial]
async fn delr_range_correct() {
	let node_id = uuid::uuid!("d0f1a200-e24e-44fe-98c1-2271a5781da7");
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let test = init(node_id, clock).await.unwrap();

	// Create some data
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();
	tx.putc(b"hugh\x00\x10", Value::Strand(Strand::from("0010")), None).await.unwrap();
	tx.put(b"hugh\x00\x10\x10", Value::Strand(Strand::from("001010"))).await.unwrap();
	tx.putc(b"hugh\x00\x20", Value::Strand(Strand::from("0020")), None).await.unwrap();
	tx.commit().await.unwrap();

	// Check we have all data
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();
	let vals = tx.scan(b"hugh\x00".to_vec()..b"hugh\xff".to_vec(), 100).await.unwrap();
	assert_eq!(vals.len(), 3);
	tx.cancel().await.unwrap();

	// Delete first range, inclusive of next key, without deleting next key
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();
	tx.delr(b"hugh\x00".to_vec()..b"hugh\x00\x10\x10".to_vec(), 100).await.unwrap();
	tx.commit().await.unwrap();

	// Scan results
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();
	let vals = tx.scan(b"hugh\x00"..b"hugh\xff", 100).await.unwrap();
	assert_eq!(vals.len(), 2);
	tx.cancel().await.unwrap();

	// Delete second range, beyond next key but beyond limit
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();
	tx.delr(b"hugh\x00\x20".to_vec()..b"hugh\xff".to_vec(), 1).await.unwrap();
	tx.commit().await.unwrap();

	// Scan results
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();
	let vals = tx.scan(b"hugh\x00"..b"hugh\xff", 100).await.unwrap();
	assert_eq!(vals.len(), 1);
	tx.cancel().await.unwrap();
}

#[tokio::test]
#[serial]
async fn set_versionstamp_is_incremental() {
	let node_id = uuid::uuid!("3988b179-6212-4a45-a496-4d9ee4cbd639");
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let test = init(node_id, clock).await.unwrap();

	// Create the first timestamped key
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();
	tx.set_versionstamped_key(b"ts_key", b"prefix", b"suffix", Value::from("'value'"))
		.await
		.unwrap();
	tx.commit().await.unwrap();

	// Create the second timestamped key
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();
	tx.set_versionstamped_key(b"ts_key", b"prefix", b"suffix", Value::from("'value'"))
		.await
		.unwrap();
	tx.commit().await.unwrap();

	// Scan the keys and validate versionstamps match expected
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();
	let found = tx.scan(b"prefix".to_vec()..b"prefix\xff".to_vec(), 1000).await.unwrap();
	tx.cancel().await.unwrap();
	assert_eq!(found.len(), 2);
	let expected_keys = [
		b"prefix\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00suffix",
		b"prefix\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00suffix",
	];
	assert_eq!(found[0].0, expected_keys[0], "key was {}", sprint(&found[0].0));
	assert_eq!(found[1].0, expected_keys[1], "key was {}", sprint(&found[1].0));
}
