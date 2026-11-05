//! A write whose own expressions change its table's schema keeps the definitions it resolved
//! when it started for every row it writes, whatever other transactions commit meanwhile.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use surrealdb_types::Value;

use crate::dbs::Session;
use crate::kvs::Datastore;

async fn ds() -> Arc<Datastore> {
	Datastore::builder().build_with_path("memory").await.unwrap()
}

async fn run(ds: &Datastore, ses: &Session, sql: &str) -> Vec<Value> {
	ds.execute(sql, ses, None)
		.await
		.unwrap()
		.into_iter()
		.map(|response| response.result.unwrap())
		.collect()
}

/// A field the write defines on its own table applies to none of its rows, also when an
/// unrelated catalog commit lands between two of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_catalog_commit_elsewhere_does_not_rebind_a_running_write() {
	let ds = ds().await;
	let ses = Session::owner().with_ns("test").with_db("test");
	run(&ds, &Session::owner(), "DEFINE NAMESPACE test; USE NS test; DEFINE DATABASE test").await;
	run(&ds, &ses, "CREATE t:1, t:2, t:3").await;
	// Each row sleeps once it has defined the field, so the commit below lands while the
	// write is between its rows.
	let other = {
		let ds = Arc::clone(&ds);
		tokio::spawn(async move {
			for i in 0..12 {
				tokio::time::sleep(Duration::from_millis(25)).await;
				run(
					&ds,
					&Session::owner(),
					&format!("DEFINE NAMESPACE e{i}; USE NS e{i}; DEFINE DATABASE d; USE DB d; CREATE x:1; REMOVE NAMESPACE e{i}"),
				)
				.await;
			}
		})
	};
	let rows = run(
		&ds,
		&ses,
		"UPDATE t SET r = (DEFINE FIELD OVERWRITE f ON t VALUE 42), s = sleep(100ms) RETURN NONE; \
		 SELECT VALUE f FROM t ORDER BY id",
	)
	.await;
	other.await.unwrap();
	assert_eq!(rows[1], Value::Array(vec![Value::None, Value::None, Value::None].into()));
}
