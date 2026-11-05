#![allow(clippy::unwrap_used)]
//! A `CREATE` that takes its content from bound parameters persists the record
//! on an embedded engine, including after the schema was defined across several
//! `query` calls and `use_ns`/`use_db` were repeated between them. A statement
//! the schema rejects reports its error in the response rather than reporting
//! success without writing.
#![cfg(any(feature = "kv-mem", feature = "kv-surrealkv"))]

use surrealdb::Surreal;
use surrealdb::engine::local::Db;
#[cfg(feature = "kv-mem")]
use surrealdb::engine::local::Mem;
#[cfg(feature = "kv-surrealkv")]
use surrealdb::engine::local::SurrealKv;
use surrealdb::types::Value;

async fn bound_create_is_persisted(db: Surreal<Db>) {
	db.use_ns("test").use_db("test").await.unwrap();

	db.query(
		r#"
		DEFINE TABLE sync_queue SCHEMAFULL;
		DEFINE FIELD entity_type ON TABLE sync_queue TYPE string;
		DEFINE FIELD entity_id ON TABLE sync_queue TYPE string;
		DEFINE FIELD payload ON TABLE sync_queue TYPE object FLEXIBLE;
		DEFINE FIELD created_at ON TABLE sync_queue TYPE datetime;
		DEFINE INDEX idx_entity ON TABLE sync_queue COLUMNS entity_type, entity_id;
	"#,
	)
	.await
	.unwrap()
	.check()
	.unwrap();
	db.query("DEFINE TABLE other_table SCHEMALESS;").await.unwrap().check().unwrap();

	db.use_ns("test").use_db("test").await.unwrap();
	db.query(
		r#"
		CREATE sync_queue CONTENT {
			entity_type: $entity_type,
			entity_id: $entity_id,
			payload: $payload,
			created_at: time::now()
		}
	"#,
	)
	.bind(("entity_type", "test"))
	.bind(("entity_id", "test123"))
	.bind(("payload", serde_json::json!({"key": "value"})))
	.await
	.unwrap()
	.check()
	.unwrap();

	db.use_ns("test").use_db("test").await.unwrap();
	let mut resp = db.query("SELECT * FROM sync_queue").await.unwrap();
	let records: Vec<Value> = resp.take(0).unwrap();
	assert_eq!(records.len(), 1, "the bound CREATE must be visible: {records:?}");

	let mut resp = db
		.query("SELECT * FROM sync_queue WHERE entity_type = $t AND entity_id = $i")
		.bind(("t", "test"))
		.bind(("i", "test123"))
		.await
		.unwrap();
	let indexed: Vec<Value> = resp.take(0).unwrap();
	assert_eq!(indexed.len(), 1, "the index lookup must find the record: {indexed:?}");
}

async fn rejected_bound_create_reports_its_error(db: Surreal<Db>) {
	db.use_ns("test").use_db("test").await.unwrap();
	db.query("DEFINE TABLE strict SCHEMAFULL; DEFINE FIELD payload ON strict TYPE object;")
		.await
		.unwrap()
		.check()
		.unwrap();

	let mut resp = db
		.query("CREATE strict CONTENT { payload: $payload }")
		.bind(("payload", serde_json::json!({"key": "value"})))
		.await
		.expect("the request itself succeeds; statement errors live in the response");
	let errors = resp.take_errors();
	assert_eq!(errors.len(), 1, "the schema rejection must be reported: {errors:?}");

	let mut resp = db.query("SELECT * FROM strict").await.unwrap();
	let rows: Vec<Value> = resp.take(0).unwrap();
	assert!(rows.is_empty());
}

#[cfg(feature = "kv-mem")]
#[tokio::test]
async fn mem_rejected_bound_create_reports_its_error() {
	let db = Surreal::new::<Mem>(()).await.unwrap();
	rejected_bound_create_reports_its_error(db).await;
}

#[cfg(feature = "kv-mem")]
#[tokio::test]
async fn mem_bound_create_is_persisted() {
	let db = Surreal::new::<Mem>(()).await.unwrap();
	bound_create_is_persisted(db).await;
}

#[cfg(feature = "kv-surrealkv")]
#[tokio::test]
async fn surrealkv_bound_create_is_persisted_across_reopen() {
	struct Dir(std::path::PathBuf);
	impl Drop for Dir {
		fn drop(&mut self) {
			std::fs::remove_dir_all(&self.0).ok();
		}
	}
	let dir = Dir(std::env::temp_dir().join(format!("surreal-bound-params-{}", ulid::Ulid::new())));
	let db = Surreal::new::<SurrealKv>(dir.0.clone()).await.unwrap();
	bound_create_is_persisted(db.clone()).await;
	db.shutdown().await.unwrap();
	drop(db);

	let db = Surreal::new::<SurrealKv>(dir.0.clone()).await.unwrap();
	db.use_ns("test").use_db("test").await.unwrap();
	let mut resp = db.query("SELECT * FROM sync_queue").await.unwrap();
	let records: Vec<Value> = resp.take(0).unwrap();
	assert_eq!(records.len(), 1, "the record must survive a reopen: {records:?}");
	db.shutdown().await.unwrap();
}
