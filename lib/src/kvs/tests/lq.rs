use crate::dbs::Notification;
use crate::sql::statements::CreateStatement;
use crate::sql::table::table;
use uuid::Uuid;

#[tokio::test]
#[serial]
async fn scan_node_lq() {
	let node_id = Uuid::parse_str("63bb5c1a-b14e-4075-a7f8-680267fbe136").unwrap();
	let test = init(node_id).await.unwrap();
	let mut tx = test.db.transaction(true, false).await.unwrap();
	let namespace = "test_namespace";
	let database = "test_database";
	let live_query_id = Uuid::from_bytes([
		0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E,
		0x1F,
	]);
	let key = crate::key::node::lq::new(node_id, live_query_id, namespace, database);
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
	let mut tx = test.db.transaction(true, false).await.unwrap();

	let res = tx.scan_ndlq(&node_id, 100).await.unwrap();
	assert_eq!(res.len(), 1);
	for val in res {
		assert_eq!(val.nd.0, node_id.clone());
		assert_eq!(val.ns, namespace);
		assert_eq!(val.db, database);
		assert_eq!(val.lq.0, live_query_id.clone());
	}

	tx.commit().await.unwrap();
}

#[tokio::test]
#[serial]
async fn live_creates_remote_notification() {
	// Setup
	let remote_node = Uuid::parse_str("30a9bea3-8430-42db-9524-3d4d5c41e3ea").unwrap();
	let local_node = Uuid::parse_str("4aa13527-538c-40da-b903-2402f57c4e74").unwrap();
	let context = context::Context::background();
	let options = Options::new().with_auth(Arc::new(Auth::for_root(Role::Owner)));
	let namespace = "test_namespace";
	let database = "test_database";
	let table = "f3d4a40b50ba4221ab02fa406edb58cc";
	let live_query_id = Uuid::parse_str("fddc6025-39c0-4ee4-9b4c-d51102fd0efe").unwrap();

	// Init as local node, so we do not receive the notification
	let test = init(local_node).await.unwrap();

	// Bootstrap the remote node, so both nodes are alive
	let mut tx = test.db.transaction(true, false).await.unwrap();
	test.bootstrap_at_time(sql::uuid::Uuid::from(local_node), tx.clock()).await.unwrap();
	tx.commit().await.unwrap();

	// Register a live query on the remote node
	let tx = test.db.transaction(true, false).await.unwrap();
	let tx = Arc::new(Mutex::new(tx));
	let live_stm = LiveStatement {
		id: sql::uuid::Uuid::from(live_query_id),
		node: sql::uuid::Uuid::from(remote_node),
		expr: Fields(vec![sql::Field::All], false),
		what: Value::Table(sql::table::Table(table.to_owned())),
		cond: None,
		fetch: None,
		archived: None,
	};
	let _ = live_stm.compute(&context, &options, &tx, None).await.unwrap();
	tx.lock().await.commit().await.unwrap();

	// Write locally to cause a remote notification
	let tx = test.db.transaction(true, false).await.unwrap();
	let tx = Arc::new(Mutex::new(tx));
	let create_stm = CreateStatement {
		what: Default::default(),
		data: None,
		output: None,
		timeout: None,
		parallel: false,
	};
	let _value = create_stm.compute(&context, &options, &tx, None).await.unwrap();
	tx.lock().await.commit().await.unwrap();

	// Verify local node did not get notification
	assert!(test.db.notifications().unwrap().try_recv().is_err());

	// Verify there is a remote node notification entry
	let prefix = crate::key::table::nt::prefix(
		namespace,
		database,
		table,
		sql::uuid::Uuid::from(live_query_id),
	);
	let suffix = crate::key::table::nt::suffix(
		namespace,
		database,
		table,
		sql::uuid::Uuid::from(live_query_id),
	);
	let res: Vec<crate::key::table::nt::Nt> =
		tx.lock().await.scan::<crate::key::table::nt::Nt>(prefix..suffix, 100).await.unwrap();
	tx.commit().await.unwrap();
	let mut tx = test.db.transaction(true, false).await.unwrap();

	let res = tx.scan_ndlq(&remote_node, 100).await.unwrap();
	assert_eq!(res.len(), 1);
	for val in res {
		assert_eq!(val.nd.0, remote_node.clone());
		assert_eq!(val.ns, namespace);
		assert_eq!(val.db, database);
		assert_eq!(val.lq.0, live_query_id.clone());
	}

	tx.commit().await.unwrap();
}
