#![recursion_limit = "256"]
//! An import whose `DEFINE MODEL` does not commit leaves no `.surml` artifact
//! behind, and an import of content a model already references keeps it.
//!
//! The object store is a process-wide static read from the environment on first
//! use, so this binary pins it to memory before anything touches it and keeps
//! to a single test.

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::obs;

async fn run(ds: &Datastore, session: &Session, sql: &str) -> anyhow::Result<()> {
	for response in ds.execute(sql, session, None).await? {
		response.result?;
	}
	Ok(())
}

#[tokio::test]
async fn a_failed_import_leaves_no_artifact() -> anyhow::Result<()> {
	// SAFETY: the only test of this binary, set before any other thread reads it.
	unsafe {
		std::env::set_var("SURREAL_OBJECT_STORE", "memory://");
		std::env::set_var("SURREAL_CACHE_STORE", "memory://");
	}
	let ds = Datastore::builder().without_maintenance_tasks().build_with_path("memory").await?;
	let setup = Session::owner();
	run(&ds, &setup, "DEFINE NAMESPACE test; USE NS test; DEFINE DATABASE test;").await?;
	let session = Session::owner().with_ns("test").with_db("test");

	let first = b"first model".to_vec();
	let first_path = format!("ml/test/test/m-1.0.0-{}.surml", obs::hash(&first));
	ds.put_ml_model(&session, "m", "1.0.0", "a model", first.clone()).await?;
	assert!(obs::get(&first_path).await.is_ok(), "the import stores the artifact");

	// The name and version are taken, so this definition is refused.
	let second = b"second model".to_vec();
	let second_path = format!("ml/test/test/m-1.0.0-{}.surml", obs::hash(&second));
	let outcome = ds.put_ml_model(&session, "m", "1.0.0", "a model", second).await;
	assert!(outcome.is_err(), "an import that is not defined reports it: {outcome:?}");
	assert!(obs::get(&second_path).await.is_err(), "a failed import leaves no artifact");
	assert!(obs::get(&first_path).await.is_ok(), "the defined model keeps its artifact");

	// Importing the same content again is refused too, and must not delete the
	// artifact the definition still points at.
	let again = ds.put_ml_model(&session, "m", "1.0.0", "a model", first).await;
	assert!(again.is_err(), "the model already exists: {again:?}");
	assert!(obs::get(&first_path).await.is_ok(), "a refused re-import keeps the live artifact");
	Ok(())
}
