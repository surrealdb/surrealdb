use crate::kvs::lq_structs::LqEntry;

#[tokio::test]
#[serial]
async fn live_queries_sent_to_tx_are_received() {
	panic!("BLAAA");
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
	tx.prepare_lq(lq_entry.clone()).unwrap();

	tx.commit().await.unwrap();

	// Verify data
	let live_queries = tx.consume_pending_live_queries();
	assert_eq!(live_queries.len(), 1);
	assert_eq!(live_queries[0], lq_entry);

	panic!("just in case")
}
