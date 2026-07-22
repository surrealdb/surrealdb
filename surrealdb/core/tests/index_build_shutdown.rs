//! A real engine shutdown/restart cycle mid concurrent index build must
//! leave the build resumable.
//!
//! Regression test for a cloud incident: a SIGTERM restart during two
//! in-flight `FULLTEXT ... CONCURRENTLY` builds used to record a durable
//! `Error("... commit coordinator is shut down")` on both builds. After the
//! restart nothing ever resumed them (`resume_stalled` skips `Error`), and
//! every write to the table failed on admission with the stale reason.
//!
//! With the fix, `Datastore::shutdown` stops builder tasks before the
//! storage engine goes away (and shutdown-class commit failures are no
//! longer recorded as durable errors), so after reopening the builds are
//! still `indexing` — adoptable by the periodic resume scan — and writes to
//! the table succeed.
#![recursion_limit = "256"]
#![cfg(feature = "kv-surrealkv")]

mod helpers;

use std::time::Duration;

use anyhow::{Result, bail};
use helpers::new_ns_db;
use surrealdb_core::dbs::Session;
use surrealdb_core::dbs::capabilities::Capabilities;
use surrealdb_core::kvs::Datastore;
use surrealdb_types::Value;

async fn open_ds(path: &str) -> Result<Datastore> {
	Datastore::builder()
		.with_capabilities(Capabilities::all())
		.build_with_path(&format!("surrealkv://{path}"))
		.await
}

fn session() -> Session {
	Session::owner().with_ns("test").with_db("test")
}

/// Extract `(status, error, initial)` from an `INFO FOR INDEX` response.
fn building_fields(value: &Value) -> (Option<String>, Option<String>, Option<i64>) {
	let Value::Object(obj) = value else {
		return (None, None, None);
	};
	let Some(Value::Object(building)) = obj.get("building") else {
		return (None, None, None);
	};
	let status = match building.get("status") {
		Some(Value::String(s)) => Some(s.clone()),
		_ => None,
	};
	let error = match building.get("error") {
		Some(Value::String(s)) => Some(s.clone()),
		_ => None,
	};
	let initial = match building.get("initial") {
		Some(Value::Number(n)) => n.to_int(),
		_ => None,
	};
	(status, error, initial)
}

async fn index_info(
	ds: &Datastore,
	index: &str,
) -> Result<(Option<String>, Option<String>, Option<i64>)> {
	let mut res = ds.execute(&format!("INFO FOR INDEX {index} ON doc"), &session(), None).await?;
	let value = res.remove(0).result?;
	Ok(building_fields(&value))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_mid_concurrent_ft_build_stays_resumable() -> Result<()> {
	let dir = std::env::temp_dir()
		.join(format!("skv-shutdown-resume-{}", std::process::id()))
		.to_string_lossy()
		.into_owned();
	// Phase 1: seed 2500 records and start two concurrent FULLTEXT builds,
	// then shut the datastore down after the first batch (250 records) has
	// committed — the exact shape of the original incident.
	{
		let ds = open_ds(&dir).await?;
		new_ns_db(&ds, "test", "test").await?;
		let sess = session();
		ds.execute(
			"CREATE |doc:1..2500| SET text = string::repeat('lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua ', 3) + <string>id, extra = string::repeat('ut enim ad minim veniam quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat ', 3) + <string>id RETURN NONE",
			&sess,
			None,
		)
		.await?;
		ds.execute("DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase", &sess, None)
			.await?;
		ds.execute(
			"DEFINE INDEX ft1 ON doc FIELDS text FULLTEXT ANALYZER simple BM25(1.2,0.75) CONCURRENTLY",
			&sess,
			None,
		)
		.await?;
		ds.execute(
			"DEFINE INDEX ft2 ON doc FIELDS extra FULLTEXT ANALYZER simple BM25(1.2,0.75) CONCURRENTLY",
			&sess,
			None,
		)
		.await?;
		let deadline = std::time::Instant::now() + Duration::from_secs(60);
		loop {
			let (status, _, initial) = index_info(&ds, "ft1").await?;
			if let Some(i) = initial
				&& i >= 250
			{
				break;
			}
			if status.as_deref() == Some("ready") {
				bail!("build finished before shutdown; increase the dataset size");
			}
			if std::time::Instant::now() > deadline {
				bail!("build never committed its first batch");
			}
			tokio::time::sleep(Duration::from_millis(2)).await;
		}
		ds.shutdown().await?;
		drop(ds);
	}
	// Phase 2: reopen. Neither build may be in a durable error state, and
	// writes to the table must be admitted again.
	let ds = open_ds(&dir).await?;
	for index in ["ft1", "ft2"] {
		let (status, error, initial) = index_info(&ds, index).await?;
		assert_ne!(
			status.as_deref(),
			Some("error"),
			"{index}: a shutdown mid-build must not leave a durable error \
			 (error: {error:?}, initial: {initial:?})"
		);
		assert_eq!(error, None, "{index}: no durable error reason should survive the restart");
	}
	ds.execute(
		"CREATE doc:99999 SET text = 'hello world', extra = 'goodbye' RETURN NONE",
		&session(),
		None,
	)
	.await?
	.remove(0)
	.result?;
	ds.shutdown().await?;
	// Clean up the on-disk store.
	let _ = std::fs::remove_dir_all(&dir);
	Ok(())
}
