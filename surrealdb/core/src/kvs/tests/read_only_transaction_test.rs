//! `BEGIN READ ONLY [MAX STALENESS <duration> [STRICT]]` against a datastore
//! whose backend has no stale tier: every bound is met by the current state.

use std::sync::Arc;

use surrealdb_types::{ToSql, Value, Variables};

use crate::catalog::providers::CatalogProvider;
use crate::dbs::Session;
use crate::kvs::Datastore;

async fn memory_ds() -> (Arc<Datastore>, Session) {
	let ds = Datastore::new("memory").await.unwrap();
	{
		let tx = ds.transaction(surrealdb_kvs::TransactionType::Write).await.unwrap();
		tx.ensure_ns_db(None, "test", "test").await.unwrap();
		tx.commit().await.unwrap();
	}
	let ses = Session::owner().with_ns("test").with_db("test");
	for result in ds
		.execute("DEFINE TABLE item SCHEMALESS; CREATE item:one SET value = 1;", &ses, None)
		.await
		.unwrap()
	{
		result.result.unwrap();
	}
	(ds, ses)
}

fn failed(results: &[crate::dbs::QueryResult]) -> bool {
	results.iter().any(|r| r.result.is_err())
}

#[tokio::test]
async fn a_read_only_block_reads_the_current_state() {
	let (ds, ses) = memory_ds().await;
	for sql in [
		"BEGIN READ ONLY; SELECT * FROM item; COMMIT;",
		"BEGIN TRANSACTION READ ONLY MAX STALENESS 5s; SELECT * FROM item; COMMIT;",
		"BEGIN READ ONLY MAX STALENESS 5s STRICT; SELECT * FROM item; COMMIT;",
		"BEGIN READ ONLY MAX STALENESS 0s STRICT; SELECT * FROM item; COMMIT;",
	] {
		let results = ds.execute(sql, &ses, None).await.unwrap();
		assert!(!failed(&results), "{sql}: {results:?}");
		let rows = results[1].result.as_ref().unwrap();
		assert!(matches!(rows, Value::Array(a) if a.len() == 1), "{sql}: {rows:?}");
	}
}

#[tokio::test]
async fn a_read_only_block_rejects_writes() {
	let (ds, ses) = memory_ds().await;
	for sql in [
		"BEGIN READ ONLY; CREATE item:two SET value = 2; COMMIT;",
		"BEGIN READ ONLY MAX STALENESS 5s; UPDATE item:one SET value = 9; COMMIT;",
		"BEGIN READ ONLY; DEFINE TABLE other; COMMIT;",
	] {
		let results = ds.execute(sql, &ses, None).await.unwrap();
		assert!(failed(&results), "{sql}: {results:?}");
	}
	let after = ds.execute("SELECT VALUE value FROM item;", &ses, None).await.unwrap();
	let rows = after.last().unwrap().result.as_ref().unwrap();
	assert_eq!(rows.to_sql(), "[1]");
}

#[tokio::test]
async fn a_plain_begin_still_writes() {
	let (ds, ses) = memory_ds().await;
	let results = ds
		.execute("BEGIN; CREATE item:two SET value = 2; COMMIT; SELECT * FROM item;", &ses, None)
		.await
		.unwrap();
	assert!(!failed(&results), "{results:?}");
}

#[tokio::test]
async fn max_staleness_takes_a_duration_parameter() {
	let (ds, ses) = memory_ds().await;
	let sql = "BEGIN READ ONLY MAX STALENESS $s; SELECT * FROM item; COMMIT;";

	let mut vars = Variables::new();
	vars.insert("s", surrealdb_types::Duration::from_secs(5));
	let results = ds.execute(sql, &ses, Some(vars)).await.unwrap();
	assert!(!failed(&results), "{results:?}");

	let mut vars = Variables::new();
	vars.insert("s", 5_i64);
	let results = ds.execute(sql, &ses, Some(vars)).await.unwrap();
	assert!(failed(&results), "{results:?}");
	let message = results.iter().find_map(|r| r.result.as_ref().err()).unwrap().to_string();
	assert!(message.contains("not a duration"), "{message}");

	let results = ds.execute(sql, &ses, None).await.unwrap();
	let message = results.iter().find_map(|r| r.result.as_ref().err()).unwrap().to_string();
	assert!(message.contains("not defined"), "{message}");
}

#[tokio::test]
async fn a_bound_above_the_maximum_fails_the_block() {
	let (ds, ses) = memory_ds().await;
	let results = ds
		.execute("BEGIN READ ONLY MAX STALENESS 400d; SELECT * FROM item; COMMIT;", &ses, None)
		.await
		.unwrap();
	assert!(failed(&results), "{results:?}");
	let message = results.iter().find_map(|r| r.result.as_ref().err()).unwrap().to_string();
	assert!(message.contains("above the maximum"), "{message}");
}

#[tokio::test]
async fn staleness_words_stay_usable_as_names() {
	let (ds, ses) = memory_ds().await;
	let results = ds
		.execute(
			"DEFINE TABLE read SCHEMALESS; CREATE read:max SET staleness = 1, only = 2; \
			 BEGIN; SELECT staleness FROM read; COMMIT;",
			&ses,
			None,
		)
		.await
		.unwrap();
	assert!(!failed(&results), "{results:?}");
}
