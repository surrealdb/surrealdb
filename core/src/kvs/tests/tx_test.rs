use crate::kvs::lq_structs::{KillEntry, LqEntry, TrackedResult};

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
