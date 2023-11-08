use crate::dbs::{Action, Notification};
use crate::sql::statements::{CreateStatement, DeleteStatement, UpdateStatement};
use crate::sql::Data::ContentExpression;
use crate::sql::{Array, Data, Id, Object, Strand, Thing, Values};
use sql::uuid::Uuid;
use std::collections::BTreeMap;
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

#[tokio::test]
#[serial]
async fn live_creates_remote_notification_for_create() {
	// Setup
	let remote_node = Uuid::from_str("30a9bea3-8430-42db-9524-3d4d5c41e3ea").unwrap();
	let local_node = Uuid::from_str("4aa13527-538c-40da-b903-2402f57c4e74").unwrap();
	let namespace = Arc::new("test_namespace".to_string());
	let database = Arc::new("test_database".to_string());
	let table = "f3d4a40b50ba4221ab02fa406edb58cc";
	let live_query_id = Uuid::from_str("fddc6025-39c0-4ee4-9b4c-d51102fd0efe").unwrap();
	let ses = Session::owner().with_ns(namespace.as_str()).with_db(database.as_str());
	let ctx = ses.context(context::Context::background());

	// Init as local node, so we do not receive the notification
	let t1 = Timestamp {
		value: 0x0102030405060708u64,
	};
	let clock = Arc::new(RwLock::new(SizedClock::Fake(FakeClock::new(t1.clone()))));
	let mut test = init(local_node, clock).await.unwrap();

	// Bootstrap the remote node, so both nodes are alive
	test.db = test.db.with_node_id(remote_node).with_notifications();
	test.db.bootstrap().await.unwrap();

	let send = test.db.live_sender().unwrap();
	let local_options =
		Options::new_from_sess(&ses, &local_node, false, true).new_with_sender(send);
	let remote_options = local_options.clone().with_id(*remote_node);

	// Register a live query on the remote node
	let tx = test.db.transaction(Write, Optimistic).await.unwrap().enclose();
	let live_value =
		compute_live(&ctx, &remote_options, tx.clone(), live_query_id, remote_node, table).await;
	tx.lock().await.commit().await.unwrap();
	assert_eq!(live_value, Value::Uuid(live_query_id));

	// Write locally to cause a remote notification
	let tx = test.db.transaction(Write, Optimistic).await.unwrap().enclose();
	let create_value = compute_create(&ctx, &local_options, tx.clone(), table, None).await;
	tx.lock().await.commit().await.unwrap();
	let create_value = match create_value {
		Value::Array(arr) => {
			assert_eq!(arr.len(), 1);
			match arr.get(0).unwrap().clone() {
				Value::Object(o) => o,
				_ => {
					panic!("Expected an object");
				}
			}
		}
		_ => panic!("Expected a uuid"),
	};
	println!("Created entry");

	// Verify local node did not get notification
	assert!(test.db.notifications().unwrap().try_recv().is_err());

	// Verify there is a remote node notification entry
	let tx = test.db.transaction(Write, Optimistic).await.unwrap().enclose();
	let mut res = tx
		.lock()
		.await
		.scan_tbnt(namespace.as_str(), database.as_str(), table, live_query_id, 1000)
		.await
		.unwrap();
	tx.lock().await.commit().await.unwrap();

	// Validate there is a remote notification
	assert_eq!(res.len(), 1);
	let not = res.get_mut(0).unwrap();
	// Notification ID is random, so we set it to a known value
	assert!(!not.notification_id.is_nil());
	not.notification_id = Default::default();
	let expected_remote_notification = Notification {
		live_id: live_query_id,
		node_id: remote_node,
		notification_id: Default::default(),
		action: Action::Create,
		result: Value::Object(create_value),
		timestamp: t1,
	};
	assert_eq!(not, &expected_remote_notification);
}

#[tokio::test]
#[serial]
async fn live_query_reads_local_notifications_before_broadcast() {
	// Setup
	let remote_node = Uuid::from_str("315565b0-8a2b-4340-a60e-428b219b464a").unwrap();
	let local_node = Uuid::from_str("e3bf1ab6-2ccd-4883-adb7-ef1d28a7f72b").unwrap();
	let namespace = Arc::new("test_namespace".to_string());
	let database = Arc::new("test_database".to_string());
	let table = "6caaf95a53124920b093152048b5a06d";
	let live_query_id = Uuid::from_str("0bc4bfc2-4001-40ac-9dc2-6728c974cd68").unwrap();
	let ses = Session::owner().with_ns(namespace.as_str()).with_db(database.as_str());
	let ctx = ses.context(context::Context::background());
	let local_options = Options::new()
		.with_auth(Arc::new(Auth::for_root(Role::Owner)))
		.with_id(*local_node)
		.with_live(true)
		.with_ns(ses.ns())
		.with_db(ses.db());
	let t1 = Timestamp {
		value: 0x0102030405060708u64,
	};

	// Init as local node, so we do not receive the notification
	let clock = Arc::new(RwLock::new(SizedClock::Fake(FakeClock::new(t1.clone()))));
	let mut test = init(local_node, clock).await.unwrap();

	// Bootstrap the remote node, so both nodes are alive
	test.db = test.db.with_node_id(remote_node).with_notifications();
	test.db.bootstrap().await.unwrap();
	let sender = test.db.live_sender().unwrap();
	let local_options = local_options.new_with_sender(sender);
	let remote_options = local_options.clone().with_id(*remote_node);

	// Create the table before starting live query
	let tx = test.db.transaction(Write, Optimistic).await.unwrap().enclose();
	let _ = compute_create(
		&ctx,
		&local_options,
		tx.clone(),
		table,
		Some(Thing::from((table, Id::String("table_create".to_string())))),
	)
	.await;
	tx.lock().await.commit().await.unwrap();
	println!("Created table");

	// Register a live query on the local node
	let tx = test.db.transaction(Write, Optimistic).await.unwrap().enclose();
	let live_value =
		compute_live(&ctx, &local_options, tx.clone(), live_query_id, local_node, table).await;
	tx.lock().await.commit().await.unwrap();
	assert_eq!(live_value, Value::Uuid(live_query_id));
	println!("Created local live query, now creating entries");

	// Write remotely to cause a local stored notification
	let tx = test.db.transaction(Write, Optimistic).await.unwrap().enclose();
	let first_create = compute_create(
		&ctx,
		&remote_options,
		tx.clone(),
		table,
		Some(Thing::from((table, Id::String("first_remote_create".to_string())))),
	)
	.await;
	tx.lock().await.commit().await.unwrap();
	println!("Created first entry - remote, now checking notifications queue before second entry");

	// Verify local node did not get notification
	assert!(test.db.notifications().unwrap().try_recv().is_err());

	// Create a local notification to cause scanning of the lq notifications
	test.db = test.db.with_node_id(local_node);
	test.db.bootstrap().await.unwrap();
	let tx = test.db.transaction(Write, Optimistic).await.unwrap().enclose();
	let local_create_value = compute_create(
		&ctx,
		&local_options,
		tx.clone(),
		table,
		Some(Thing::from((table, Id::String("second_local_create".to_string())))),
	)
	.await;
	tx.lock().await.commit().await.unwrap();
	println!("Created second entry - local. Now checking notifications channel");

	// Validate the remote notification occurs before the local one
	let nots = test.db.notifications().unwrap();

	let mut first_not = nots.try_recv().unwrap();
	// We cannot determine live query ID
	assert!(!first_not.live_id.is_nil());
	first_not.live_id = Default::default();
	// We cannot determine notification ID
	assert!(!first_not.notification_id.is_nil());
	first_not.notification_id = Default::default();
	// We cannot determine the timestamp
	assert_ne!(first_not.timestamp, Default::default());
	first_not.timestamp = Default::default();

	let mut second_not = nots.try_recv().unwrap();
	// We cannot determine live query ID
	assert!(!second_not.live_id.is_nil());
	second_not.live_id = Default::default();
	// We cannot determine notification ID
	assert!(!second_not.notification_id.is_nil());
	second_not.notification_id = Default::default();
	// We cannot determine the timestamp
	assert_ne!(second_not.timestamp, Default::default());
	second_not.timestamp = Default::default();

	let expected = vec![
		Notification {
			live_id: Default::default(),
			node_id: local_node,
			notification_id: Default::default(),
			action: Action::Create,
			result: safe_pop(first_create),
			timestamp: Default::default(),
		},
		Notification {
			live_id: Default::default(),
			node_id: local_node,
			notification_id: Default::default(),
			action: Action::Create,
			result: safe_pop(local_create_value),
			timestamp: Default::default(),
		},
	];
	let actual = vec![first_not, second_not];
	assert_eq!(expected, actual);

	// Then no more notifications
	assert!(nots.try_recv().is_err());
}

#[tokio::test]
#[serial]
async fn live_creates_remote_notification_for_update() {
	// Setup
	let remote_node = Uuid::from_str("c529eedc-2f41-4825-a41e-906bb1791a7d").unwrap();
	let local_node = Uuid::from_str("6e0bfb9a-3e60-4b64-b0f4-97b7a7566001").unwrap();
	let namespace = Arc::new("test_namespace".to_string());
	let database = Arc::new("test_database".to_string());
	let table = "862dc7a9-285b-4e25-988f-cf21c83127a3";
	let live_query_id = Uuid::from_str("6d7ccea8-5120-4cb0-9225-62e339ecd832").unwrap();
	let ses = Session::owner().with_ns(namespace.as_str()).with_db(database.as_str());
	let ctx = ses.context(context::Context::background());
	let t1 = Timestamp {
		value: 0x0102030405060708u64,
	};

	// Init as local node, so we do not receive the notification
	let clock = Arc::new(RwLock::new(SizedClock::Fake(FakeClock::new(t1.clone()))));
	let mut test = init(local_node, clock).await.unwrap();

	// Bootstrap the remote node, so both nodes are alive
	test.db = test.db.with_node_id(remote_node).with_notifications();
	test.db.bootstrap().await.unwrap();
	let send = test.db.live_sender().unwrap();
	let local_options = Options::new()
		.with_auth(Arc::new(Auth::for_root(Role::Owner)))
		.with_id(*local_node)
		.with_live(true)
		.new_with_sender(send)
		.with_ns(ses.ns())
		.with_db(ses.db());
	let remote_options = local_options.clone().with_id(*remote_node);

	// Create the record we will update
	let tx = test.db.transaction(Write, Optimistic).await.unwrap().enclose();
	let create_value = compute_create(&ctx, &local_options, tx.clone(), table, None).await;
	tx.lock().await.commit().await.unwrap();
	let create_value = match create_value {
		Value::Array(arr) => {
			assert_eq!(arr.len(), 1);
			match arr.get(0).unwrap().clone() {
				Value::Object(o) => o,
				_ => {
					panic!("Expected an object");
				}
			}
		}
		_ => panic!("Expected a uuid"),
	};

	// Register a live query on the remote node
	let tx = test.db.transaction(Write, Optimistic).await.unwrap().enclose();
	let live_value =
		compute_live(&ctx, &remote_options, tx.clone(), live_query_id, remote_node, table).await;
	tx.lock().await.commit().await.unwrap();
	assert_eq!(live_value, Value::Uuid(live_query_id));

	// Update to cause a remote notification
	let thing = match create_value.get("id").unwrap().clone() {
		Value::Thing(thing) => thing,
		_ => panic!("Expected ID to be a thing"),
	};
	let tx = test.db.transaction(Write, Optimistic).await.unwrap().enclose();
	let update_value = compute_update(
		&ctx,
		&local_options,
		tx.clone(),
		thing.clone(),
		"some_field".to_string(),
		Value::Strand(Strand::from("Some Value")),
	)
	.await;
	tx.lock().await.commit().await.unwrap();

	// Verify local node did not get notification
	assert!(test.db.notifications().unwrap().try_recv().is_err());

	// Verify there is a remote node notification entry
	let tx = test.db.transaction(Write, Optimistic).await.unwrap().enclose();
	let mut res = tx
		.lock()
		.await
		.scan_tbnt(namespace.as_str(), database.as_str(), table, live_query_id, 1000)
		.await
		.unwrap();
	tx.lock().await.commit().await.unwrap();

	// Validate there is a remote notification
	assert_eq!(res.len(), 1);
	let not = res.get_mut(0).unwrap();
	// Notification ID is random, so we set it to a known value
	assert!(!not.notification_id.is_nil());
	not.notification_id = Default::default();
	// TODO bug that update notifs are array
	let expected_remote_notification = Notification {
		live_id: live_query_id,
		node_id: remote_node,
		notification_id: Default::default(),
		action: Action::Update,
		result: update_value,
		timestamp: t1,
	};
	assert_eq!(not, &expected_remote_notification);
}

#[tokio::test]
#[serial]
async fn live_creates_remote_notification_for_delete() {
	// Setup
	let remote_node = Uuid::from_str("50b98717-71aa-491e-a96a-51c4e1e249c6").unwrap();
	let local_node = Uuid::from_str("25750b67-65df-4a0a-b4f8-bd5dd0418730").unwrap();
	let namespace = Arc::new("test_namespace".to_string());
	let database = Arc::new("test_database".to_string());
	let table = "9ebc8a9a-46d7-4751-9077-ee1842684d12";
	let live_query_id = Uuid::from_str("1ef4da92-344c-4ce3-b9cf-7cc572956e3f").unwrap();
	let ses = Session::owner().with_ns(namespace.as_str()).with_db(database.as_str());
	let ctx = ses.context(context::Context::background());
	// Init as local node, so we do not receive the notification
	let t1 = Timestamp {
		value: 0x0102030405060708u64,
	};
	let clock = Arc::new(RwLock::new(SizedClock::Fake(FakeClock::new(t1.clone()))));
	let mut test = init(local_node, clock).await.unwrap();

	// Bootstrap the remote node, so both nodes are alive
	test.db = test.db.with_node_id(remote_node).with_notifications();
	test.db.bootstrap().await.unwrap();

	let send = test.db.live_sender().unwrap();
	let local_options = Options::new()
		.with_auth(Arc::new(Auth::for_root(Role::Owner)))
		.with_id(*local_node)
		.with_live(true)
		.new_with_sender(send)
		.with_ns(ses.ns())
		.with_db(ses.db());
	let remote_options = local_options.clone().with_id(*remote_node);

	// Create a record that we intend to delete for a notification
	let tx = test.db.transaction(Write, Optimistic).await.unwrap().enclose();
	let create_value = compute_create(&ctx, &local_options, tx.clone(), table, None).await;
	tx.lock().await.commit().await.unwrap();
	let create_value = match create_value {
		Value::Array(arr) => {
			assert_eq!(arr.len(), 1);
			match arr.get(0).unwrap().clone() {
				Value::Object(o) => o,
				_ => {
					panic!("Expected an object");
				}
			}
		}
		_ => panic!("Expected a uuid"),
	};
	println!("Created entry");

	// Register a live query on the remote node
	let tx = test.db.transaction(Write, Optimistic).await.unwrap().enclose();
	let live_value =
		compute_live(&ctx, &remote_options, tx.clone(), live_query_id, remote_node, table).await;
	tx.lock().await.commit().await.unwrap();
	assert_eq!(live_value, Value::Uuid(live_query_id));
	println!("Created live query in test");

	// Write locally to cause a remote notification
	let thing = match create_value.get("id").unwrap().clone() {
		Value::Thing(thing) => thing,
		_ => panic!("Expected ID to be a thing"),
	};
	let tx = test.db.transaction(Write, Optimistic).await.unwrap().enclose();
	let delete_value = compute_delete(&ctx, &local_options, tx.clone(), thing.clone()).await;
	tx.lock().await.commit().await.unwrap();
	// Delete returns empty
	assert_eq!(Value::Array(Array::new()), delete_value);

	// Verify local node did not get notification
	assert!(test.db.notifications().unwrap().try_recv().is_err());

	// Verify there is a remote node notification entry
	let tx = test.db.transaction(Write, Optimistic).await.unwrap().enclose();
	let mut res = tx
		.lock()
		.await
		.scan_tbnt(namespace.as_str(), database.as_str(), table, live_query_id, 1000)
		.await
		.unwrap();
	tx.lock().await.commit().await.unwrap();

	// Validate there is a remote notification
	assert_eq!(res.len(), 1);
	let not = res.get_mut(0).unwrap();
	// Notification ID is random, so we set it to a known value
	assert!(!not.notification_id.is_nil());
	not.notification_id = Default::default();
	// The notification value for delete is just the ID of the record
	let expected_result = Value::Object(Object(map! {
		"id".to_string() => Value::Thing(thing),
		"name".to_string() => Value::Strand(Strand::from("a name")),
	}));
	let expected_remote_notification = Notification {
		live_id: live_query_id,
		node_id: remote_node,
		notification_id: Default::default(),
		action: Action::Delete,
		result: expected_result,
		timestamp: t1,
	};
	assert_eq!(not, &expected_remote_notification);
}

async fn compute_live<'a>(
	ctx: &'a context::Context<'a>,
	opt: &'a Options,
	tx: Arc<Mutex<Transaction>>,
	live_query_id: Uuid,
	node_id: Uuid,
	table: &'a str,
) -> Value {
	let live_stm = LiveStatement {
		id: live_query_id,
		node: node_id,
		expr: Fields(vec![sql::Field::All], false),
		what: Value::Table(sql::table::Table::from(table.to_owned())),
		cond: None,
		fetch: None,
		archived: None,
		session: Some(Value::None),
		auth: None,
	};
	live_stm.compute(ctx, opt, &tx, None).await.unwrap()
}

async fn compute_create<'a>(
	ctx: &'a context::Context<'a>,
	opt: &'a Options,
	tx: Arc<Mutex<Transaction>>,
	table: &'a str,
	what: Option<Thing>,
) -> Value {
	let mut map: BTreeMap<String, Value> = BTreeMap::new();
	map.insert("name".to_string(), Value::Strand(Strand::from("a name")));
	let obj_val = Value::Object(Object::from(map));
	let data = Data::ContentExpression(obj_val.clone());
	let thing = what.unwrap_or_else(|| Thing::from((table.to_string(), Id::rand())));
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

async fn compute_delete<'a>(
	ctx: &'a context::Context<'a>,
	opt: &'a Options,
	tx: Arc<Mutex<Transaction>>,
	what: Thing,
) -> Value {
	let delete_stm = DeleteStatement {
		only: false,
		what: Values(vec![Value::Thing(what)]),
		cond: None,
		output: None,
		timeout: None,
		parallel: false,
	};
	// delete returns an empty array
	delete_stm.compute(ctx, opt, &tx, None).await.unwrap()
}

async fn compute_update<'a>(
	ctx: &'a context::Context<'a>,
	opt: &'a Options,
	tx: Arc<Mutex<Transaction>>,
	what: Thing,
	field: String,
	value: Value,
) -> Value {
	let mut map = BTreeMap::new();
	map.insert(field, value);
	let obj = Object::from(map);
	let data = ContentExpression(Value::Object(obj));
	let update_stm = UpdateStatement {
		only: false,
		what: Values(vec![Value::Thing(what)]),
		data: Some(data),
		cond: None,
		output: None,
		timeout: None,
		parallel: false,
	};
	let array_result = update_stm.compute(ctx, opt, &tx, None).await.unwrap();
	// TODO update(1) returns array, create(1) returns object
	match array_result {
		Value::Array(arr) => {
			assert_eq!(arr.len(), 1);
			arr.get(0).unwrap().clone()
		}
		_ => panic!("Expected an array"),
	}
}

fn safe_pop(v: Value) -> Value {
	match v {
		Value::Array(mut arr) => {
			assert_eq!(arr.len(), 1);
			arr.pop().unwrap()
		}
		o => o,
	}
}
