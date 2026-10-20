#![allow(clippy::unwrap_used)]
//! `Surreal::shutdown` resolves once the embedded datastore has released its
//! storage, so the same path can be opened again straight away.

#[cfg(feature = "kv-mem")]
#[tokio::test]
async fn requests_fail_after_shutdown() {
	use surrealdb::Surreal;
	use surrealdb::engine::local::Mem;

	let db = Surreal::new::<Mem>(()).await.unwrap();
	db.use_ns("main").use_db("main").await.unwrap();
	let other = db.clone();
	db.shutdown().await.unwrap();
	assert!(other.query("RETURN 1").await.is_err(), "a handle sharing the connection must fail");
	// Shutting down twice is not an error.
	db.shutdown().await.unwrap();
}

#[cfg(feature = "kv-surrealkv")]
#[tokio::test]
async fn storage_can_be_reopened_immediately_after_shutdown() {
	use surrealdb::Surreal;
	use surrealdb::engine::any::Any;

	let dir = temp_dir::TempDir::new().unwrap();
	let endpoint = format!("surrealkv://{}", dir.path().join("db").display());
	for _ in 0..5 {
		let db = Surreal::<Any>::init();
		db.connect(&endpoint).await.unwrap();
		db.use_ns("main").use_db("main").await.unwrap();
		db.query("CREATE t:1").await.unwrap();
		db.shutdown().await.unwrap();
		drop(db);
	}
}
