use crate::sql::statements::live::live;

#[tokio::test]
#[serial]
async fn archive_lv_for_node_archives() {
	let test = init().await.unwrap();
	let mut tx = test.db.transaction(true, true).await.unwrap();
	let namespace = "test_namespace";
	let database = "test_database";
	let table = "test_table";
	let node_id = crate::sql::uuid::Uuid::from(Uuid::from_bytes([
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E,
		0x0F,
	]));
	tx.set_nd(node_id.0).await.unwrap();

	let lv_id = crate::sql::uuid::Uuid::from(Uuid::from_bytes([
		0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E,
		0x1F,
	]));

	let key = crate::key::node::lq::new(node_id.0.clone(), lv_id.0.clone(), namespace, database);
	tx.putc(key, table, None).await.unwrap();

	let (_, mut stm) = live(format!("LIVE SELECT * FROM {}", table).as_str()).unwrap();
	stm.id = lv_id.clone();
	tx.putc_lv(namespace, database, table, stm, None).await.unwrap();

	let this_node_id = crate::sql::uuid::Uuid::from(Uuid::from_bytes([
		0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2A, 0x2B, 0x2C, 0x2D, 0x2E,
		0x2F,
	]));
	// We commit after setup because otherwise in memory does not have read your own writes
	// i.e. setup data is part of same transaction as required implementation checks
	tx.commit().await.unwrap();

	let mut tx = test.db.transaction(true, false).await.unwrap();
	let results =
		test.db.archive_lv_for_node(&mut tx, &node_id, this_node_id.clone()).await.unwrap();
	assert_eq!(results.len(), 1);
	assert_eq!(results[0].nd, node_id);
	assert_eq!(results[0].ns, namespace);
	assert_eq!(results[0].db, database);
	assert_eq!(results[0].tb, table);
	assert_eq!(results[0].lq, lv_id);
	tx.commit().await.unwrap();

	let mut tx = test.db.transaction(true, false).await.unwrap();
	let lv = tx.all_lv(namespace, database, table).await.unwrap();
	assert_eq!(lv.len(), 1, "{:?}", lv);
	assert_eq!(lv[0].archived, Some(this_node_id));
	tx.commit().await.unwrap();
}
