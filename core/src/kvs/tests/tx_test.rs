use crate::key::debug::sprint_key;
use crate::key::error::KeyCategory;
use crate::kvs::lq_structs::{KillEntry, LqEntry, TrackedResult};
use crate::sql::Strand;

#[tokio::test]
#[serial]
async fn live_queries_sent_to_tx_are_received() {
	let node_id = uuid::uuid!("d0f1a200-e24e-44fe-98c1-2271a5781da7");
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let test = init(node_id, clock).await.unwrap();
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();

	// Create live query data
	let lq_entry = LqEntry {
		live_id: sql::Uuid::new_v4(),
		ns: "namespace".to_string(),
		db: "database".to_string(),
		stm: LiveStatement {
			id: sql::Uuid::new_v4(),
			node: sql::Uuid::from(node_id),
			expr: Default::default(),
			what: Default::default(),
			cond: None,
			fetch: None,
			archived: None,
			session: Some(Value::None),
			auth: None,
			session_id: Default::default(),
		},
	};
	tx.pre_commit_register_async_event(TrackedResult::LiveQuery(lq_entry.clone())).unwrap();

	tx.commit().await.unwrap();

	// Verify data
	let live_queries = tx.consume_pending_live_queries();
	assert_eq!(live_queries.len(), 1);
	assert_eq!(live_queries[0], TrackedResult::LiveQuery(lq_entry));
}
#[tokio::test]
#[serial]
async fn kill_queries_sent_to_tx_are_received() {
	let node_id = uuid::uuid!("1cae3d33-64e6-4867-bf17-d095c1b842d7");
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let test = init(node_id, clock).await.unwrap();
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();

	let kill_entry = KillEntry {
		live_id: uuid::uuid!("f396c0cb-01ca-4213-a72d-b0240f6d00b2").into(),
		ns: "some_ns".to_string(),
		db: "some_db".to_string(),
	};

	// Create live query data
	tx.pre_commit_register_async_event(TrackedResult::KillQuery(kill_entry.clone())).unwrap();

	tx.commit().await.unwrap();

	// Verify data
	let live_queries = tx.consume_pending_live_queries();
	assert_eq!(live_queries.len(), 1);
	assert_eq!(live_queries[0], TrackedResult::KillQuery(kill_entry));
}

#[tokio::test]
#[serial]
async fn delr_range_correct() {
	let node_id = uuid::uuid!("d0f1a200-e24e-44fe-98c1-2271a5781da7");
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let test = init(node_id, clock).await.unwrap();

	// Create some data
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();
	tx.putc(b"hugh\x00\x10", Value::Strand(Strand::from("0010")), None).await.unwrap();
	tx.put(KeyCategory::ChangeFeed, b"hugh\x00\x10\x10", Value::Strand(Strand::from("001010")))
		.await
		.unwrap();
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
	assert_eq!(found[0].0, expected_keys[0], "key was {}", sprint_key(&found[0].0));
	assert_eq!(found[1].0, expected_keys[1], "key was {}", sprint_key(&found[1].0));
}
