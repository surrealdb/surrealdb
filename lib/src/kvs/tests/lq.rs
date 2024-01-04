use sql::uuid::Uuid;
use std::str::FromStr;

#[tokio::test]
#[serial]
async fn scan_node_lq() {
	let node_id = Uuid::from_str("63bb5c1a-b14e-4075-a7f8-680267fbe136").unwrap();
	let clock = Arc::new(RwLock::new(SizedClock::Fake(FakeClock::new(Timestamp::default()))));
	let test = init(node_id, clock).await.unwrap();
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();
	let namespace = "test_namespace";
	let database = "test_database";
	let live_query_id = Uuid::from_str("d9024cf8-c547-41f2-90a5-3d5d139ddbc5").unwrap();
	let key = crate::key::node::lq::new(*node_id, *live_query_id, namespace, database);
	trace!(
		"Inserting key: {}",
		key.encode()
			.unwrap()
			.iter()
			.flat_map(|byte| std::ascii::escape_default(*byte))
			.map(|byte| byte as char)
			.collect::<String>()
	);
	tx.putc(key, "value", None).await.unwrap();
	tx.commit().await.unwrap();
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();

	let res = tx.scan_ndlq(&node_id, 100).await.unwrap();
	assert_eq!(res.len(), 1);
	for val in res {
		assert_eq!(val.nd, node_id);
		assert_eq!(val.ns, namespace);
		assert_eq!(val.db, database);
		assert_eq!(val.lq, live_query_id);
	}

	tx.commit().await.unwrap();
}
