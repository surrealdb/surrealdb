use futures::lock::Mutex;
use std::sync::Arc;

use crate::ctx::context;

use crate::dbs::{Options, Session};
use crate::sql;
use crate::sql::statements::LiveStatement;
use crate::sql::Value::Table;
use crate::sql::{Fields, Value};
use uuid;

#[tokio::test]
#[serial]
async fn expired_nodes_are_garbage_collected() {
	let test = init().await.unwrap();

	// Set up the first node at an early timestamp
	let old_node =
		crate::sql::uuid::Uuid::try_from("BA01030F-0D86-493D-A6E8-EF27287FC5DC").unwrap();
	let old_time = Timestamp {
		value: 123,
	};
	test.bootstrap_at_time(old_node, old_time.clone()).await.unwrap();

	// Set up second node at a later timestamp
	let new_node =
		crate::sql::uuid::Uuid::try_from("EF34C843-FB06-4E94-B779-434B900878C0").unwrap();
	let new_time = Timestamp {
		value: 567,
	};
	test.bootstrap_at_time(new_node.clone(), new_time.clone()).await.unwrap();

	// Now scan the heartbeats to validate there is only one node left
	let mut tx = test.db.transaction(true, false).await.unwrap();
	let scanned = tx.scan_hb(&new_time, 100).await.unwrap();
	assert_eq!(scanned.len(), 1);
	for hb in scanned.iter() {
		assert_eq!(&hb.nd, &new_node.0);
	}

	// And scan the nodes to verify its just the latest also
	let scanned = tx.scan_cl(100).await.unwrap();
	assert_eq!(scanned.len(), 1);
	for cl in scanned.iter() {
		assert_eq!(&cl.name, &new_node.0.to_string());
	}

	tx.commit().await.unwrap();
}

#[tokio::test]
#[serial]
async fn expired_nodes_get_live_queries_archived() {
	let test = init().await.unwrap();

	// Set up the first node at an early timestamp
	let old_node = crate::sql::uuid::Uuid::from(
		uuid::Uuid::parse_str("19AC759A-16D8-4DC3-BFC2-E9B24294C8EB").unwrap(),
	);
	let old_time = Timestamp {
		value: 123,
	};
	test.bootstrap_at_time(old_node.clone(), old_time.clone()).await.unwrap();

	// Set up live query
	let ses = Session::owner()
		.with_ns(test.test_str("testns").as_str())
		.with_db(test.test_str("testdb").as_str());
	let table = "my_table";
	let lq = LiveStatement {
		id: sql::Uuid(uuid::Uuid::parse_str("748DD7B1-99CA-4C66-A6E2-C9558596AD48").unwrap()),
		node: crate::sql::uuid::Uuid::from(
			Uuid::parse_str("4C21BA69-8468-4E8C-8405-9EBB1C62D7AC").unwrap(),
		),
		expr: Fields(vec![sql::Field::All], false),
		what: Table(sql::Table::from(table)),
		cond: None,
		fetch: None,
		archived: Some(crate::sql::uuid::Uuid::from(old_node.0)),
	};
	let ctx = context::Context::background();
	let (sender, _) = channel::unbounded();
	let opt = Options::new()
		.with_ns(ses.ns())
		.with_db(ses.db())
		.with_auth(Arc::new(Default::default()))
		.with_live(true)
		.with_id(old_node.0);
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
	let new_node = crate::sql::uuid::Uuid::from(
		uuid::Uuid::parse_str("2F95505B-4F0F-47BE-BC50-417D6E3F249A").unwrap(),
	);
	let new_time = Timestamp {
		value: 456,
	}; // TODO These timestsamps are incorrect and should really be derived; Also check timestamp errors
	test.bootstrap_at_time(new_node, new_time.clone()).await.unwrap();

	// Now validate lq was removed
	let mut tx = test.db.transaction(true, false).await.unwrap();
	let scanned =
		tx.all_lv(ses.ns().unwrap().as_ref(), ses.db().unwrap().as_ref(), table).await.unwrap();
	assert_eq!(scanned.len(), 0);
	tx.commit().await.unwrap();
}
