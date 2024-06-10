use std::collections::BTreeSet;
use std::sync::Arc;

use crate::ctx::context;

use crate::dbs::{Options, Session};
use crate::iam::{Auth, Role};
use crate::kvs::lq_structs::{LqValue, UnreachableLqType};
use crate::kvs::{LockType::*, TransactionType::*};
use crate::sql;
use crate::sql::statements::LiveStatement;
use crate::sql::Value::Table;
use crate::sql::{Fields, Value};
use test_log::test;
use uuid;

#[tokio::test]
#[serial]
async fn expired_nodes_are_garbage_collected() {
	let old_node = Uuid::parse_str("2ea6d33f-4c0a-417a-ab04-1fa9869f9a65").unwrap();
	let new_node = Uuid::parse_str("fbfb3487-71fe-4749-b3aa-1cc0a5380cdd").unwrap();
	let old_time = Timestamp {
		value: 123000,
	};
	let fake_clock = FakeClock::new(old_time);
	let fake_clock = Arc::new(SizedClock::Fake(fake_clock));
	let mut test = init(new_node, fake_clock.clone()).await.unwrap();

	// Set up the first node at an early timestamp
	test.db = test.db.with_node_id(sql::Uuid::from(old_node));
	test.db.bootstrap().await.unwrap();

	// Throw in some stray nodes and heartbeats
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();
	let corrupt_node_1 = Uuid::parse_str("5a65fe57-7ac3-4b13-a31f-6376d3b484c8").unwrap();
	let corrupt_node_2 = Uuid::parse_str("eb94a0b4-70ea-482f-a7dd-dc02132be846").unwrap();
	tx.set_nd(corrupt_node_1).await.unwrap();
	tx.set_hb(old_time, corrupt_node_2).await.unwrap();
	tx.commit().await.unwrap();

	// Set up second node at a later timestamp
	let new_time = Timestamp {
		value: 567000,
	};
	set_fake_clock(fake_clock.clone(), new_time).await;
	test.db = test.db.with_node_id(sql::Uuid::from(new_node));
	test.db.bootstrap().await.unwrap();

	// Now scan the heartbeats to validate there is only one node left
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();
	let scanned = tx.scan_hb(&new_time, 100).await.unwrap();
	assert_eq!(scanned.len(), 1);
	for hb in scanned.iter() {
		assert_eq!(&hb.nd, &new_node);
	}

	// And scan the nodes to verify its just the latest also
	let scanned = tx.scan_nd(100).await.unwrap();
	assert_eq!(scanned.len(), 1);
	for cl in scanned.iter() {
		assert_eq!(&cl.name, &new_node.to_string());
	}

	tx.commit().await.unwrap();
}

#[tokio::test]
#[serial]
async fn expired_nodes_get_live_queries_archived() {
	let old_node = Uuid::parse_str("c756ed5a-3b19-4303-bce2-5e0edf72e66b").unwrap();
	let old_time = Timestamp {
		value: 123000,
	};
	let fake_clock = FakeClock::new(old_time);
	let fake_clock = Arc::new(SizedClock::Fake(fake_clock));
	let mut test = init(old_node, fake_clock.clone()).await.unwrap();

	// Set up the first node at an early timestamp
	test.db = test.db.with_node_id(sql::Uuid::from(old_node)).with_notifications();
	test.db.bootstrap().await.unwrap();

	// Set up live query
	let ses = Session::owner()
		.with_ns(test.test_str("testns").as_str())
		.with_db(test.test_str("testdb").as_str());
	let table = "my_table";
	let lq = LiveStatement {
		id: sql::Uuid(Uuid::parse_str("da60fa34-902d-4110-b810-7d435267a9f8").unwrap()),
		node: crate::sql::uuid::Uuid::from(old_node),
		expr: Fields(vec![sql::Field::All], false),
		what: Table(sql::Table::from(table)),
		cond: None,
		fetch: None,
		archived: Some(crate::sql::uuid::Uuid::from(old_node)),
		session: Some(Value::None),
		auth: Some(Auth::for_root(Role::Owner)),
	};
	let ctx = context::Context::background();
	let (sender, _) = channel::unbounded();
	let opt = Options::new()
		.with_ns(ses.ns())
		.with_db(ses.db())
		.with_auth(Arc::new(Default::default()))
		.with_live(true)
		.with_id(old_node);
	let opt = Options::new_with_sender(&opt, sender);
	let tx = test.db.transaction(Write, Optimistic).await.unwrap();
	let ctx = ctx.with_transaction(tx.enclose());
	let res = {
		let mut stack = reblessive::tree::TreeStack::new();
		stack.enter(|stk| lq.compute(stk, &ctx, &opt, None)).finish().await.unwrap()
	};
	match res {
		Value::Uuid(_) => {}
		_ => {
			panic!("Not a uuid: {:?}", res);
		}
	}
	ctx.tx_lock().await.commit().await.unwrap();

	// Set up second node at a later timestamp
	let new_node = Uuid::parse_str("04da7d4c-0086-4358-8318-49f0bb168fa7").unwrap();
	let new_time = Timestamp {
		value: 456000,
	};
	set_fake_clock(fake_clock.clone(), new_time).await;
	test.db = test.db.with_node_id(sql::Uuid::from(new_node));
	test.db.bootstrap().await.unwrap();

	// Now validate lq was removed
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();
	let scanned = tx
		.all_tb_lives(ses.ns().unwrap().as_ref(), ses.db().unwrap().as_ref(), table)
		.await
		.unwrap();
	assert_eq!(scanned.len(), 0);
	tx.commit().await.unwrap();
}

#[test(tokio::test)]
#[serial]
async fn single_live_queries_are_garbage_collected() {
	// Test parameters
	let mut stack = reblessive::tree::TreeStack::new();
	let ctx = context::Context::background();
	let node_id = Uuid::parse_str("b1a08614-a826-4581-938d-bea17f00e253").unwrap();
	let time = Timestamp {
		value: 123000,
	};
	let fake_clock = FakeClock::new(time);
	let fake_clock = Arc::new(SizedClock::Fake(fake_clock));
	let mut test = init(node_id, fake_clock).await.unwrap();
	let namespace = "test_namespace";
	let database = "test_db";
	let table = "test_table";
	let options = Options::default()
		.with_required(
			node_id,
			Some(Arc::from(namespace)),
			Some(Arc::from(database)),
			Arc::new(Auth::for_root(Role::Owner)),
		)
		.with_live(true);

	// We do standard cluster init
	trace!("Bootstrapping node {}", node_id);
	test.db = test.db.with_node_id(crate::sql::uuid::Uuid::from(node_id));
	test.db.bootstrap().await.unwrap();

	// We set up 2 live queries, one of which we want to garbage collect
	trace!("Setting up live queries");
	let tx = test.db.transaction(Write, Optimistic).await.unwrap();
	let ctx = ctx.with_transaction(tx.enclose());
	let live_query_to_delete = Uuid::parse_str("8aed07c4-9683-480e-b1e4-f0db8b331530").unwrap();
	let live_st = LiveStatement {
		id: sql::Uuid(live_query_to_delete),
		node: sql::uuid::Uuid::from(node_id),
		expr: Fields(vec![sql::Field::All], false),
		what: Table(sql::Table::from(table)),
		cond: None,
		fetch: None,
		archived: None,
		session: Some(Value::None),
		auth: Some(Auth::for_root(Role::Owner)),
	};
	stack
		.enter(|stk| live_st.compute(stk, &ctx, &options, None))
		.finish()
		.await
		.map_err(|e| format!("Error computing live statement: {:?} {:?}", live_st, e))
		.unwrap();
	let live_query_to_keep = Uuid::parse_str("adea762a-17db-4810-a4a2-c54babfdaf23").unwrap();
	let live_st = LiveStatement {
		id: sql::Uuid(live_query_to_keep),
		node: sql::Uuid::from(node_id),
		expr: Fields(vec![sql::Field::All], false),
		what: Table(sql::Table::from(table)),
		cond: None,
		fetch: None,
		archived: None,
		session: Some(Value::None),
		auth: Some(Auth::for_root(Role::Owner)),
	};
	stack
		.enter(|stk| live_st.compute(stk, &ctx, &options, None))
		.finish()
		.await
		.map_err(|e| format!("Error computing live statement: {:?} {:?}", live_st, e))
		.unwrap();
	ctx.tx_lock().await.commit().await.unwrap();

	// Subject: Perform the action we are testing
	trace!("Garbage collecting dead sessions");
	test.db.garbage_collect_dead_session(&[live_query_to_delete]).await.unwrap();

	// Validate
	trace!("Validating live queries");
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();
	let scanned = tx.all_tb_lives(namespace, database, table).await.unwrap();
	assert_eq!(scanned.len(), 1, "The scanned values are {:?}", scanned);
	assert_eq!(&scanned[0].id.0, &live_query_to_keep);
	let scanned = tx.all_lq(&node_id).await.unwrap();
	assert_eq!(scanned.len(), 1);
	assert_eq!(&scanned[0].lq, &sql::Uuid::from(live_query_to_keep));
	tx.commit().await.unwrap();
}

#[test(tokio::test)]
#[serial]
async fn bootstrap_does_not_error_on_missing_live_queries() {
	// Test parameters
	let mut stack = reblessive::tree::TreeStack::new();
	let ctx = context::Context::background();
	let old_node_id = Uuid::parse_str("5f644f02-7c1a-4f8b-babd-bd9e92c1836a").unwrap();
	let t1 = Timestamp {
		value: 123_000,
	};
	let t2 = Timestamp {
		value: 456_000,
	};
	let fake_clock = FakeClock::new(t1);
	let fake_clock = Arc::new(SizedClock::Fake(fake_clock));
	let test = init(old_node_id, fake_clock.clone()).await.unwrap();
	let namespace = "test_namespace_0A8BD08BE4F2457BB9F145557EF19605";
	let database_owned = format!("test_db_{:?}", test.kvs);
	let database = database_owned.as_str();
	let table = "test_table";
	let options = Options::default()
		.with_required(
			old_node_id,
			Some(Arc::from(namespace)),
			Some(Arc::from(database)),
			Arc::new(Auth::for_root(Role::Owner)),
		)
		.with_live(true);

	// We do standard cluster init
	trace!("Bootstrapping node {}", old_node_id);
	test.db.bootstrap().await.unwrap();

	// We set up 2 live queries, one of which we want to garbage collect
	trace!("Setting up live queries");
	let tx = test.db.transaction(Write, Optimistic).await.unwrap();
	let ctx = ctx.with_transaction(tx.enclose());
	let live_query_to_corrupt = Uuid::parse_str("d4cee7ce-5c78-4a30-9fa9-2444d58029f6").unwrap();
	let live_st = LiveStatement {
		id: sql::Uuid(live_query_to_corrupt),
		node: sql::uuid::Uuid::from(old_node_id),
		expr: Fields(vec![sql::Field::All], false),
		what: Table(sql::Table::from(table)),
		cond: None,
		fetch: None,
		archived: None,
		session: Some(Value::None),
		auth: Some(Auth::for_root(Role::Owner)),
	};
	stack
		.enter(|stk| live_st.compute(stk, &ctx, &options, None))
		.finish()
		.await
		.map_err(|e| format!("Error computing live statement: {:?} {:?}", live_st, e))
		.unwrap();

	// Now we corrupt the live query entry by leaving the node entry in but removing the table entry
	let key = crate::key::table::lq::new(namespace, database, table, live_query_to_corrupt);
	ctx.tx_lock().await.del(key).await.unwrap();
	ctx.tx_lock().await.commit().await.unwrap();

	// Subject: Perform the action we are testing
	trace!("Bootstrapping");
	let new_node_id = Uuid::parse_str("53f7355d-5be1-4a94-9803-5192b59c5244").unwrap();

	// There should not be an error
	set_fake_clock(fake_clock.clone(), t2).await;
	let second_node = test.db.with_node_id(crate::sql::uuid::Uuid::from(new_node_id));
	match second_node.bootstrap().await {
		Ok(_) => {
			// The behaviour has now changed to remove all broken entries without raising errors
		}
		Err(e) => {
			panic!("Bootstrapping should not generate errors: {:?}", e)
		}
	}

	// Verify node live query was deleted
	let mut tx = second_node.transaction(Write, Optimistic).await.unwrap();
	let found = tx
		.scan_ndlq(&old_node_id, 100)
		.await
		.map_err(|e| format!("Error scanning ndlq: {:?}", e))
		.unwrap();
	assert_eq!(0, found.len(), "Found: {:?}", found);
	let found = tx
		.scan_ndlq(&new_node_id, 100)
		.await
		.map_err(|e| format!("Error scanning ndlq: {:?}", e))
		.unwrap();
	assert_eq!(0, found.len(), "Found: {:?}", found);

	// Verify table live query does not exist
	let found = tx
		.scan_tblq(namespace, database, table, 100)
		.await
		.map_err(|e| format!("Error scanning tblq: {:?}", e))
		.unwrap();
	assert_eq!(0, found.len(), "Found: {:?}", found);
	tx.cancel().await.unwrap();
}

#[test(tokio::test)]
async fn test_asymmetric_difference() {
	let nd1 = Uuid::parse_str("7da0b3bb-1811-4c0e-8d8d-5fc08b8200a5").unwrap();
	let nd2 = Uuid::parse_str("8fd394df-7f96-4395-9c9a-3abf1e2386ea").unwrap();
	let nd3 = Uuid::parse_str("aa53cb74-1d6b-44df-b063-c495e240ae9e").unwrap();
	let ns1 = "namespace_one";
	let ns2 = "namespace_two";
	let ns3 = "namespace_three";
	let db1 = "database_one";
	let db2 = "database_two";
	let db3 = "database_three";
	let tb1 = "table_one";
	let tb2 = "table_two";
	let tb3 = "table_three";
	let lq1 = Uuid::parse_str("95f0e060-d301-4dfc-9d35-f150e802873b").unwrap();
	let lq2 = Uuid::parse_str("acf60c04-5819-4a23-9874-aeb0ae1be425").unwrap();
	let lq3 = Uuid::parse_str("5d591ae7-db79-4e4f-aa02-a83a4a25ce3f").unwrap();
	let left_set = BTreeSet::from_iter(vec![
		UnreachableLqType::Nd(LqValue {
			nd: nd1.into(),
			ns: ns1.to_string(),
			db: db1.to_string(),
			tb: tb1.to_string(),
			lq: lq1.into(),
		}),
		UnreachableLqType::Nd(LqValue {
			nd: nd2.into(),
			ns: ns2.to_string(),
			db: db2.to_string(),
			tb: tb2.to_string(),
			lq: lq2.into(),
		}),
	]);

	let right_set = BTreeSet::from_iter(vec![
		UnreachableLqType::Tb(LqValue {
			nd: nd2.into(),
			ns: ns2.to_string(),
			db: db2.to_string(),
			tb: tb2.to_string(),
			lq: lq2.into(),
		}),
		UnreachableLqType::Tb(LqValue {
			nd: nd3.into(),
			ns: ns3.to_string(),
			db: db3.to_string(),
			tb: tb3.to_string(),
			lq: lq3.into(),
		}),
	]);

	let diff = left_set.symmetric_difference(&right_set);
	// TODO but also poorman's count
	let mut count = 0;
	for _ in diff {
		count += 1;
	}
	assert_ne!(count, 0);
}

async fn set_fake_clock(fake_clock: Arc<SizedClock>, time: Timestamp) {
	let clock = match &*fake_clock {
		SizedClock::Fake(f) => f,
		_ => panic!("Clock is not fake"),
	};
	clock.set(time).await;
}
