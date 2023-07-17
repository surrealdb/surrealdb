use uuid::Uuid;

#[tokio::test]
#[serial]
async fn scan_node_lq() {
	let node_id = Uuid::parse_str("63bb5c1a-b14e-4075-a7f8-680267fbe136").unwrap();
	let test = init(node_id).await.unwrap();
	let mut tx = test.db.transaction(true, true).await.unwrap();
	let namespace = "test_namespace";
	let database = "test_database";
	let live_query_id = Uuid::from_bytes([
		0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E,
		0x1F,
	]);
	let key = crate::key::lq::new(node_id, namespace, database, live_query_id);
	trace!(
		"Inserting key: {}",
		key.encode()
			.unwrap()
			.iter()
			.flat_map(|byte| std::ascii::escape_default(byte.clone()))
			.map(|byte| byte as char)
			.collect::<String>()
	);
	let _ = tx.putc(key, "value", None).await.unwrap();
	tx.commit().await.unwrap();
	let mut tx = test.db.transaction(true, true).await.unwrap();

	let res = tx.scan_lq(&node_id, 100).await.unwrap();
	assert_eq!(res.len(), 1);
	for val in res {
		assert_eq!(val.cl, node_id);
		assert_eq!(val.ns, namespace);
		assert_eq!(val.db, database);
		assert_eq!(val.lq, live_query_id);
	}

	tx.commit().await.unwrap();
}
