//! A blocking index statement on a store other datastores write to returns with its index
//! materialised, as it does where its datastore is the sole writer.
//!
//! There a write into a vector index covers its shard's pending-state guard with the node's
//! writer lease rather than arming it, and no node steps a guard under a lease. The statement's
//! wait releases the node's leases on the index once the backlog is folded, so `INFO FOR INDEX`
//! reports `compacting: false` straight after it rather than once the leases go idle.

use std::sync::Arc;

use surrealdb_types::Value;

use super::fulltext_nodes_test::{SharedBackend, node};
use crate::dbs::Session;
use crate::kvs::Datastore;

async fn run(ds: &Datastore, ses: &Session, sql: &str) -> Vec<Value> {
	ds.execute(sql, ses, None)
		.await
		.expect("the query runs")
		.into_iter()
		.map(|response| response.result.expect("the statement succeeds"))
		.collect()
}

/// `compacting` of index `ix` on `t`, as `INFO FOR INDEX` reports it.
async fn compacting(ds: &Arc<Datastore>, ses: &Session, ix: &str) -> Value {
	run(ds, ses, &format!("RETURN (INFO FOR INDEX {ix} ON t).building.compacting")).await.remove(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_blocking_vector_build_returns_materialised_on_a_shared_store() {
	let backend = SharedBackend::memory().await;
	let ds = node(&backend, false).await;
	run(&ds, &Session::owner(), "DEFINE NAMESPACE a; USE NS a; DEFINE DATABASE b").await;
	let ses = Session::owner().with_ns("a").with_db("b");
	run(
		&ds,
		&ses,
		"CREATE t:1 SET vec = [1.0, 0.0]; CREATE t:2 SET vec = [0.0, 1.0]; \
		 CREATE t:3 SET vec = [1.0, 1.0];",
	)
	.await;
	run(&ds, &ses, "DEFINE INDEX hx ON t FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32")
		.await;
	assert_eq!(compacting(&ds, &ses, "hx").await, Value::Bool(false), "after DEFINE");
	run(&ds, &ses, "REBUILD INDEX hx ON t").await;
	assert_eq!(compacting(&ds, &ses, "hx").await, Value::Bool(false), "after REBUILD");
	let ids = run(&ds, &ses, "SELECT VALUE id FROM t WHERE vec <|2,10|> [1.0, 0.0] ORDER BY id")
		.await
		.remove(0);
	assert_eq!(ids, run(&ds, &ses, "RETURN [t:1, t:3]").await.remove(0));
}
