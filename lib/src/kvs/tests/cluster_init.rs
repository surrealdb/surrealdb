use futures::lock::Mutex;
use std::sync::Arc;

use crate::ctx::context;

use crate::dbs::{Options, Session};
use crate::iam::{Auth, Role};
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
	let test = init(new_node).await.unwrap();

	// Set up the first node at an early timestamp
	let old_time = Timestamp {
		value: 123,
	};
	test.bootstrap_at_time(sql::Uuid::from(old_node), old_time.clone()).await.unwrap();

	// Set up second node at a later timestamp
	let new_time = Timestamp {
		value: 567,
	};
	test.bootstrap_at_time(sql::Uuid::from(new_node), new_time.clone()).await.unwrap();

	// Now scan the heartbeats to validate there is only one node left
	let mut tx = test.db.transaction(true, false).await.unwrap();
	let scanned = tx.scan_hb(&new_time, 100).await.unwrap();
	assert_eq!(scanned.len(), 1);
	for hb in scanned.iter() {
		assert_eq!(&hb.nd, &new_node);
	}

	// And scan the nodes to verify its just the latest also
	let scanned = tx.scan_cl(100).await.unwrap();
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
	let test = init(old_node).await.unwrap();

	// Set up the first node at an early timestamp
	let old_time = Timestamp {
		value: 123,
	};
	test.bootstrap_at_time(sql::Uuid::from(old_node), old_time.clone()).await.unwrap();

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
	let tx = Arc::new(Mutex::new(test.db.transaction(true, false).await.unwrap()));
	let res = lq.compute(&ctx, &opt, &tx, None).await.unwrap();
	match res {
		Value::Uuid(_) => {}
		_ => {
			panic!("Not a uuid: {:?}", res);
		}
	}
	tx.lock().await.commit().await.unwrap();

	// Set up second node at a later timestamp
	let new_node = Uuid::parse_str("04da7d4c-0086-4358-8318-49f0bb168fa7").unwrap();
	let new_time = Timestamp {
		value: 456,
	}; // TODO These timestsamps are incorrect and should really be derived; Also check timestamp errors
	test.bootstrap_at_time(sql::Uuid::from(new_node), new_time.clone()).await.unwrap();

	// Now validate lq was removed
	let mut tx = test.db.transaction(true, false).await.unwrap();
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
	let ctx = context::Context::background();
	let node_id = Uuid::parse_str("b1a08614-a826-4581-938d-bea17f00e253").unwrap();
	let test = init(node_id).await.unwrap();
	let time = Timestamp {
		value: 123,
	};
	let namespace = "test_namespace";
	let database = "test_db";
	let table = "test_table";
	let options = Options::default()
		.with_required(
			node_id.clone(),
			Some(Arc::from(namespace)),
			Some(Arc::from(database)),
			Arc::new(Auth::for_root(Role::Owner)),
		)
		.with_live(true);

	// We do standard cluster init
	trace!("Bootstrapping node {}", node_id);
	test.bootstrap_at_time(crate::sql::uuid::Uuid::from(node_id), time).await.unwrap();

	// We set up 2 live queries, one of which we want to garbage collect
	trace!("Setting up live queries");
	let tx = Arc::new(Mutex::new(test.db.transaction(true, false).await.unwrap()));
	let live_query_to_delete = Uuid::parse_str("8aed07c4-9683-480e-b1e4-f0db8b331530").unwrap();
	let live_st = LiveStatement {
		id: sql::Uuid(live_query_to_delete),
		node: sql::uuid::Uuid::from(node_id),
		expr: Fields(vec![sql::Field::All], false),
		what: Table(sql::Table::from(table)),
		cond: None,
		fetch: None,
		archived: None,
		auth: Some(Auth::for_root(Role::Owner)),
	};
	live_st
		.compute(&ctx, &options, &tx, None)
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
		auth: Some(Auth::for_root(Role::Owner)),
	};
	live_st
		.compute(&ctx, &options, &tx, None)
		.await
		.map_err(|e| format!("Error computing live statement: {:?} {:?}", live_st, e))
		.unwrap();
	tx.lock().await.commit().await.unwrap();

	// Subject: Perform the action we are testing
	trace!("Garbage collecting dead sessions");
	test.db.garbage_collect_dead_session(&[live_query_to_delete]).await.unwrap();

	// Validate
	trace!("Validating live queries");
	let mut tx = test.db.transaction(true, false).await.unwrap();
	let scanned = tx.all_tb_lives(namespace, database, table).await.unwrap();
	assert_eq!(scanned.len(), 1, "The scanned values are {:?}", scanned);
	assert_eq!(&scanned[0].id.0, &live_query_to_keep);
	let scanned = tx.all_lq(&node_id).await.unwrap();
	assert_eq!(scanned.len(), 1);
	assert_eq!(&scanned[0].lq, &sql::Uuid::from(live_query_to_keep));
	tx.commit().await.unwrap();
}
