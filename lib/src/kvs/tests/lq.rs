use crate::sql::statements::CreateStatement;
use crate::sql::{Data, Id, Object, Strand, Thing, Values};
use std::collections::BTreeMap;
use uuid::Uuid;

#[tokio::test]
#[serial]
async fn scan_node_lq() {
	let node_id = Uuid::parse_str("63bb5c1a-b14e-4075-a7f8-680267fbe136").unwrap();
	let clock = Arc::new(RwLock::new(SizedClock::Fake(FakeClock::new(Timestamp::default()))));
	let test = init(node_id, clock).await.unwrap();
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
async fn live_creates_remote_notification_for_create() {
	println!("STARTED");

	// Setup
	let remote_node = Uuid::parse_str("30a9bea3-8430-42db-9524-3d4d5c41e3ea").unwrap();
	let local_node = Uuid::parse_str("4aa13527-538c-40da-b903-2402f57c4e74").unwrap();
	let namespace = Arc::new("test_namespace".to_string());
	let database = Arc::new("test_database".to_string());
	let table = "f3d4a40b50ba4221ab02fa406edb58cc";
	let live_query_id = Uuid::parse_str("fddc6025-39c0-4ee4-9b4c-d51102fd0efe").unwrap();
	let ctx = context::Context::background();
	let ses = Session::owner().with_ns(namespace.as_str()).with_db(database.as_str());
	let local_options = Options::new()
		.with_auth(Arc::new(Auth::for_root(Role::Owner)))
		.with_id(local_node)
		.with_live(true)
		.with_ns(ses.ns())
		.with_db(ses.db());
	let remote_options = local_options.clone().with_id(remote_node);
	let t1 = Timestamp {
		value: 0x0102030405060708u64,
	};

	// Init as local node, so we do not receive the notification
	println!("First init");
	let clock = Arc::new(RwLock::new(SizedClock::Fake(FakeClock::new(t1.clone()))));
	let mut test = init(local_node, clock).await.unwrap();

	// Bootstrap the remote node, so both nodes are alive
	println!("Second init");
	test.db = test.db.with_node_id(sql::uuid::Uuid::from(local_node)).with_notifications();
	test.db.bootstrap().await.unwrap();
	println!("Init complete");

	println!("Before starting live query statement");
	// Register a live query on the remote node
	let tx = test.db.transaction(true, false).await.unwrap().enclose();
	let live_value =
		compute_live(&ctx, &remote_options, tx.clone(), live_query_id, remote_node, table).await;
	tx.lock().await.commit().await.unwrap();
	assert_eq!(live_value, Value::Uuid(sql::uuid::Uuid::from(live_query_id)));
	println!("Created live query");

	// Write locally to cause a remote notification
	let tx = test.db.transaction(true, false).await.unwrap().enclose();
	let _value = compute_create(&ctx, &local_options, tx.clone(), table).await;
	tx.lock().await.commit().await.unwrap();
	println!("Created entry");

	// Verify local node did not get notification
	assert!(test
		.db
		.notifications()
		.ok_or(Error::Unreachable("The notifications should always exist".to_string()))
		.unwrap()
		.try_recv()
		.is_err());

	// Verify there is a remote node notification entry
	let tx = test.db.transaction(true, false).await.unwrap().enclose();
	let res = tx
		.lock()
		.await
		.scan_nt(
			namespace.as_str(),
			database.as_str(),
			table,
			sql::uuid::Uuid::from(live_query_id),
			1000,
		)
		.await
		.unwrap();
	tx.lock().await.commit().await.unwrap();
	println!("Did the scan");

	assert_eq!(res.len(), 1);
	// TODO verify key and value
	let mut tx = test.db.transaction(true, false).await.unwrap();

	let res = tx.scan_ndlq(&remote_node, 100).await.unwrap();
	tx.commit().await.unwrap();
	assert_eq!(res.len(), 1);
	let val = res.get(0).unwrap();
	// for val in res {
	assert_eq!(val.nd.0, remote_node.clone());
	assert_eq!(val.ns, namespace.as_str());
	assert_eq!(val.db, database.as_str());
	assert_eq!(val.lq.0, live_query_id.clone());
	// }
	println!("Finished test");
}
#[tokio::test]
#[serial]
async fn live_creates_remote_notification_for_update() {}

#[tokio::test]
#[serial]
async fn live_creates_remote_notification_for_delete() {}

async fn compute_live<'a>(
	ctx: &'a context::Context<'a>,
	opt: &'a Options,
	tx: Arc<Mutex<Transaction>>,
	live_query_id: Uuid,
	node_id: Uuid,
	table: &'a str,
) -> Value {
	let live_stm = LiveStatement {
		id: sql::uuid::Uuid::from(live_query_id),
		node: sql::uuid::Uuid::from(node_id),
		expr: Fields(vec![sql::Field::All], false),
		what: Value::Table(sql::table::Table::from(table.to_owned())),
		cond: None,
		fetch: None,
		archived: None,
		auth: None,
	};
	live_stm.compute(ctx, opt, &tx, None).await.unwrap()
}

async fn compute_create<'a>(
	ctx: &'a context::Context<'a>,
	opt: &'a Options,
	tx: Arc<Mutex<Transaction>>,
	table: &'a str,
) -> Value {
	let mut map: BTreeMap<String, Value> = BTreeMap::new();
	map.insert("name".to_string(), Value::Strand(Strand::from("a name")));
	let obj_val = Value::Object(Object::from(map));
	let data = Data::ContentExpression(obj_val);
	let thing = Thing::from((table.to_string(), Id::rand()));
	let create_stm = CreateStatement {
		only: false,
		what: Values(vec![Value::Thing(thing)]),
		data: Some(data),
		output: None,
		timeout: None,
		parallel: false,
	};
	create_stm.compute(ctx, opt, &tx, None).await.unwrap()
}
