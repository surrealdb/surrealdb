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
#[rustfmt::skip]
#[serial]
async fn expired_nodes_are_garbage_collected() {
    let test = match init().await {
        Ok(test) => test,
        Err(e) => panic!("{}", e),
    };

    // Set up the first node at an early timestamp
    let old_node = uuid::Uuid::new_v4();
    let old_time = Timestamp { value: 123 };
    test.bootstrap_at_time(&old_node, old_time.clone()).await.unwrap();

    // Set up second node at a later timestamp
    let new_node = uuid::Uuid::new_v4();
    let new_time = Timestamp { value: 456 };
    test.bootstrap_at_time(&new_node, new_time.clone()).await.unwrap();

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
#[rustfmt::skip]
#[serial]
async fn expired_nodes_get_live_queries_archived() {
    let test = match init().await {
        Ok(test) => test,
        Err(e) => panic!("{}", e),
    };

    // Set up the first node at an early timestamp
    let old_node = uuid::Uuid::from_fields(0, 1, 2, &[3, 4, 5, 6, 7, 8, 9, 10]);
    let old_time = Timestamp { value: 123 };
    test.bootstrap_at_time(&old_node, old_time.clone()).await.unwrap();
    
    // Set up live query
    let ses = Session::for_kv().
        with_ns(test.test_str("testns").as_str()).
        with_db(test.test_str("testdb").as_str());
    let table = "my_table";
    let lq = LiveStatement {
        id: sql::Uuid(uuid::Uuid::new_v4()),
        node: Uuid::new_v4(),
        expr: Fields(vec!(sql::Field::All), false),
        what: Table(sql::Table::from(table)),
        cond: None,
        fetch: None,
        archived: Some(old_node),
    };
    let ctx = context::Context::background();
    let (sender, _) = channel::unbounded();
    let opt = Options::new().
        with_ns(ses.ns()).
        with_db(ses.db()).
        with_auth(Arc::new(Default::default())).
        with_live(true).
        with_id(old_node.clone());
    let opt = Options::new_with_sender(&opt, sender);
    let tx = Arc::new(Mutex::new(test.db.transaction(true, false).await.unwrap()));
    let res = lq.compute(&ctx, &opt, &tx, None).await.unwrap();
    match res {
        Value::Uuid(_) => {},
        _ => {panic!("Not a uuid: {:?}", res);},
    }
    tx.lock().await.commit().await.unwrap();

    // Set up second node at a later timestamp
    let new_node = uuid::Uuid::from_fields(16, 17, 18, &[19, 20, 21, 22, 23, 24, 25, 26]);
    let new_time = Timestamp { value: 456 }; // TODO These timestsamps are incorrect and should really be derived; Also check timestamp errors
    test.bootstrap_at_time(&new_node, new_time.clone()).await.unwrap();

    // Now validate lq was removed
    let mut tx = test.db.transaction(true, false).await.unwrap();
    let scanned = tx.all_lv(ses.ns().unwrap().as_ref(), ses.db().unwrap().as_ref(), table).await.unwrap();
    assert_eq!(scanned.len(), 0);
    tx.commit().await.unwrap();
}
