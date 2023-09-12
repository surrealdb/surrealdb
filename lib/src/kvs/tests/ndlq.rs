use crate::kvs::LqValue;

#[tokio::test]
#[serial]
async fn write_scan_ndlq() {
	let nd = uuid::Uuid::parse_str("7a17446f-721f-4855-8fc7-81086752ca44").unwrap();
	let test = init(nd).await.unwrap();

	// Write some data
	let mut tx = test.db.transaction(true, false).await.unwrap();
	let ns = "namespace";
	let db = "database";
	let tb = "table";
	let lq =
		sql::Uuid::from(uuid::Uuid::parse_str("4c3dca4b-ec08-4e3e-b23a-6b03b5cdc3fc").unwrap());
	let live_stm = LiveStatement {
		id: lq.clone(),
		node: sql::Uuid::from(nd),
		expr: Default::default(),
		what: Default::default(),
		cond: None,
		fetch: None,
		archived: None,
		auth: None,
	};
	tx.putc_tblq(ns, db, tb, live_stm, None).await.unwrap();
	tx.putc_ndlq(nd, lq.clone().0, ns, db, tb, None).await.unwrap();
	tx.commit().await.unwrap();

	// Verify scan
	let mut tx = test.db.transaction(true, false).await.unwrap();
	let res = tx.scan_tblq(ns, db, tb, 100).await.unwrap();
	assert_eq!(
		res,
		vec![LqValue {
			nd: sql::Uuid::from(nd),
			ns: ns.to_string(),
			db: db.to_string(),
			tb: tb.to_string(),
			lq
		}]
	);
	tx.commit().await.unwrap();
}
