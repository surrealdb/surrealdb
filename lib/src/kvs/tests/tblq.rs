#[tokio::test]
#[serial]
async fn write_scan_tblq() {
	let node_id = uuid::uuid!("0bee25e0-34d7-448c-abc0-48cdf3db3a53");
	let live_ids = [
		uuid::Uuid::nil(),
		uuid::uuid!("b5aab54e-d1ef-4a14-b537-9206dcde2209"),
		uuid::Uuid::new_v4(),
		uuid::Uuid::max(),
	];

	for live_id in live_ids {
		let clock = Arc::new(RwLock::new(SizedClock::Fake(FakeClock::new(Timestamp::default()))));
		let test = init(node_id, clock).await.unwrap();

		// Write some data
		let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();
		let ns = "namespace";
		let db = "database";
		let tb = "table";
		let live_id = sql::Uuid::from(live_id);
		let live_stm = LiveStatement {
			id: live_id.clone(),
			node: sql::Uuid::from(node_id),
			expr: Default::default(),
			what: Default::default(),
			cond: None,
			fetch: None,
			archived: None,
			session: Some(Value::None),
			auth: None,
		};
		tx.putc_tblq(ns, db, tb, live_stm, None).await.unwrap();
		tx.commit().await.unwrap();

		// Verify scan
		let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();
		let res = tx.scan_tblq(ns, db, tb, 100).await.unwrap();
		let no_limit = tx.scan_tblq(ns, db, tb, NO_LIMIT).await.unwrap();
		tx.commit().await.unwrap();
		assert_eq!(
			res,
			vec![LqValue {
				nd: sql::Uuid::from(node_id),
				ns: ns.to_string(),
				db: db.to_string(),
				tb: tb.to_string(),
				lq: live_id
			}]
		);
		assert_eq!(res, no_limit);
	}
}
