#[tokio::test]
#[serial]
async fn archive_lv_for_node_archives() {
	let node_id = Uuid::from_str("9ab2d498-757f-48cc-8c07-a7d337997445").unwrap();
	let clock = Arc::new(RwLock::new(SizedClock::Fake(FakeClock::new(Timestamp::default()))));
	let test = init(node_id, clock).await.unwrap();
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();
	let namespace = "test_namespace";
	let database = "test_database";
	let table = "test_table";
	tx.set_nd(*node_id).await.unwrap();

	let lv_id = Uuid::from_str("1108f946-6930-4d29-a339-4d1f9d088cc0").unwrap();

	let key = crate::key::node::lq::new(*node_id, lv_id.0, namespace, database);
	tx.putc(key, table, None).await.unwrap();

	let mut stm = LiveStatement::from_source_parts(Fields::all(), Table(table.into()), None, None);
	stm.id = lv_id;
	tx.putc_tblq(namespace, database, table, stm, None).await.unwrap();

	let this_node_id = crate::sql::uuid::Uuid::from(uuid::Uuid::from_bytes([
		0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2A, 0x2B, 0x2C, 0x2D, 0x2E,
		0x2F,
	]));
	// We commit after setup because otherwise in memory does not have read your own writes
	// i.e. setup data is part of same transaction as required implementation checks
	tx.commit().await.unwrap();

	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();
	let results = test.db.archive_lv_for_node(&mut tx, &node_id, this_node_id).await.unwrap();
	assert_eq!(results.len(), 1);
	tx.commit().await.unwrap();
	let (lq, opt_err) = &results[0];
	match opt_err {
		None => {
			//expected
		}
		Some(err) => {
			panic!("Unexpected error: {:?}", err);
		}
	}
	assert_eq!(lq.nd, node_id);
	assert_eq!(lq.ns, namespace);
	assert_eq!(lq.db, database);
	assert_eq!(lq.tb, table);
	assert_eq!(lq.lq, lv_id);

	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();
	let lv = tx.all_tb_lives(namespace, database, table).await.unwrap();
	assert_eq!(lv.len(), 1, "{:?}", lv);
	assert_eq!(lv[0].archived, Some(this_node_id));
	tx.commit().await.unwrap();
}
