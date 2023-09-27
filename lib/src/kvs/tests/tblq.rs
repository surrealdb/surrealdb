#[tokio::test]
#[serial]
async fn write_scan_tblq() {
	let node_id = uuid::Uuid::parse_str("0bee25e0-34d7-448c-abc0-48cdf3db3a53").unwrap();
	let test = init(node_id).await.unwrap();

	// Write some data
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();
	let ns = "namespace";
	let db = "database";
	let tb = "table";
	let live_id =
		sql::Uuid::from(uuid::Uuid::parse_str("b5aab54e-d1ef-4a14-b537-9206dcde2209").unwrap());
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
	tx.commit().await.unwrap();
}
