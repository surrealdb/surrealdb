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

#[tokio::test]
async fn a_read_only_begin_answers_with_the_snapshot_it_reads() {
	let (ds, ses) = memory_ds().await;
	let results = ds
		.execute("BEGIN READ ONLY MAX STALENESS 5s; SELECT * FROM item; COMMIT;", &ses, None)
		.await
		.unwrap();
	let Value::Object(snapshot) = results[0].result.as_ref().unwrap() else {
		panic!("the BEGIN result describes the snapshot: {:?}", results[0]);
	};
	assert_eq!(snapshot.get("position"), Some(&Value::None));
	assert_eq!(snapshot.get("stale"), Some(&Value::Bool(false)));
	assert!(matches!(snapshot.get("age"), Some(Value::Duration(d)) if d.is_zero()));

	// A plain BEGIN still answers NONE.
	let results = ds.execute("BEGIN; RETURN 1; COMMIT;", &ses, None).await.unwrap();
	assert_eq!(results[0].result.as_ref().unwrap(), &Value::None);
}

#[tokio::test]
async fn a_read_only_request_is_the_query_wrapped_in_a_read_only_block() {
	use crate::kvs::{QueryRequest, ReadOnlyOptions};

	let (ds, ses) = memory_ds().await;
	let options = ReadOnlyOptions {
		max_staleness: std::time::Duration::from_secs(5),
		strict: true,
	};
	let sugar = ds
		.run(QueryRequest::new("SELECT * FROM item;", &ses).with_read_only(Some(options)))
		.await
		.unwrap();
	let wrapped = ds
		.execute("BEGIN READ ONLY MAX STALENESS 5s STRICT; SELECT * FROM item; COMMIT;", &ses, None)
		.await
		.unwrap();
	let shape = |r: &[crate::dbs::QueryResult]| {
		r.iter().map(|r| format!("{:?}", r.result)).collect::<Vec<_>>()
	};
	assert_eq!(shape(&sugar), shape(&wrapped));
	assert_eq!(sugar.len(), 3);

	let write = ds
		.run(QueryRequest::new("CREATE item:two;", &ses).with_read_only(Some(options)))
		.await
		.unwrap();
	assert!(failed(&write), "{write:?}");

	// A transaction-control statement of its own would end the wrapping block
	// and let the statements after it run outside it.
	for sql in [
		"BEGIN; RETURN 1; COMMIT;",
		"RETURN 1; COMMIT; CREATE item:three;",
		"RETURN 1; CANCEL; CREATE item:three;",
	] {
		let refused = ds
			.run(QueryRequest::new(sql, &ses).with_read_only(Some(options)))
			.await
			.expect_err(sql);
		assert!(refused.message().contains("cannot contain"), "{sql}: {refused:?}");
	}
	let after = ds.execute("SELECT VALUE id FROM item;", &ses, None).await.unwrap();
	assert_eq!(after.last().unwrap().result.as_ref().unwrap().to_sql(), "[item:one]");
}
