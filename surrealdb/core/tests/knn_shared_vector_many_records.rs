#![recursion_limit = "256"]
//! A vector shared by more records than a vector index's inline document-set
//! variants hold: every record must stay reachable through KNN once the index
//! is compacted, and removed ones must stop being returned.

use std::sync::Arc;
use std::time::Duration;

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_types::{ToSql, Value};
use tokio_util::sync::CancellationToken;

async fn run(ds: &Datastore, session: &Session, sql: &str) -> anyhow::Result<Value> {
	let mut results = ds.execute(sql, session, None).await?;
	Ok(results.pop().expect("one statement result").result?)
}

async fn compact(ds: &Arc<Datastore>) -> anyhow::Result<()> {
	Datastore::index_compaction(Arc::clone(ds), Duration::from_secs(1), CancellationToken::new())
		.await?;
	Ok(())
}

/// The ids of the records within 50 neighbours of the shared vector, sorted.
async fn neighbours(ds: &Datastore, session: &Session) -> anyhow::Result<String> {
	Ok(run(ds, session, "SELECT VALUE id FROM pts WHERE point <|50,100|> [1f, 1f] ORDER BY id;")
		.await?
		.to_sql())
}

async fn every_record_stays_reachable(index: &str) -> anyhow::Result<()> {
	let ds = Arc::new(Datastore::new("memory").await?);
	let session = Session::owner().with_ns("test").with_db("test");
	run(&ds, &session, "DEFINE NAMESPACE test; USE NS test; DEFINE DATABASE test;").await?;
	run(&ds, &session, &format!("DEFINE INDEX ix ON pts FIELDS point {index};")).await?;
	// Compacted one record at a time, so the shared vector's document set is
	// written back after every addition.
	for i in 1..=12 {
		run(&ds, &session, &format!("CREATE pts:{i} SET point = [1f, 1f];")).await?;
		compact(&ds).await?;
	}
	let all = (1..=12).map(|i| format!("pts:{i}")).collect::<Vec<_>>().join(", ");
	assert_eq!(neighbours(&ds, &session).await?, format!("[{all}]"), "{index}");
	for i in 1..=10 {
		run(&ds, &session, &format!("DELETE pts:{i};")).await?;
		compact(&ds).await?;
	}
	assert_eq!(neighbours(&ds, &session).await?, "[pts:11, pts:12]", "{index}");
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn hnsw_vector_shared_by_many_records() -> anyhow::Result<()> {
	every_record_stays_reachable("HNSW DIMENSION 2").await
}

#[cfg(diskann)]
#[tokio::test(flavor = "multi_thread")]
async fn diskann_vector_shared_by_many_records() -> anyhow::Result<()> {
	every_record_stays_reachable("DISKANN DIMENSION 2 TYPE F32").await
}
