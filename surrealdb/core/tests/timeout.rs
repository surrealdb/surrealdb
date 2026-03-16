mod helpers;
use std::time::{Duration, Instant};

use anyhow::Result;
use helpers::{Test, new_ds};
use surrealdb_core::dbs::Session;
#[allow(unused_imports)]
use surrealdb_core::kvs::Datastore;

#[tokio::test]
async fn statement_timeouts() -> Result<()> {
	let sql = "
		CREATE ONLY person:ok CONTENT { test: true };
		CREATE person:test CONTENT { test: true } TIMEOUT 0s;
		UPSERT person:test CONTENT { test: true } TIMEOUT 0s;
		UPDATE person:test CONTENT { test: true } TIMEOUT 0s;
		INSERT { id: person:test, test: true } TIMEOUT 0s;
		RELATE person:test->know->person:ok TIMEOUT 0s;
		LET $temp = SELECT * FROM person TIMEOUT 0s;
		SELECT * FROM person TIMEOUT 0s;
		DELETE person:test TIMEOUT 0s;
	";
	let error = "The query was not executed because it exceeded the timeout: 0ns";
	Test::new(sql)
		.await?
		.expect_val("{ id: person:ok, test: true }")?
		.expect_error(error)?
		.expect_error(error)?
		.expect_error(error)?
		.expect_error(error)?
		.expect_error(error)?
		.expect_error(error)?
		.expect_error(error)?
		.expect_error(error)?;

	Ok(())
}

#[tokio::test]
async fn query_timeout() -> Result<()> {
	let sql = "
		FOR $i in 0..1000000000{
			FOR $i in 0..1000000000{
				FOR $i in 0..1000000000{
				}
			}
		}
	";
	let ds = new_ds("test", "test").await?.with_query_timeout(Some(Duration::from_millis(500)));
	let session = Session::owner();
	let before = Instant::now();
	let mut res = ds.execute(sql, &session, None).await.unwrap();
	if before.elapsed() > Duration::from_millis(7050) {
		panic!("Query did not properly timeout");
	}
	res.pop().unwrap().result.unwrap_err();

	Ok(())
}

#[tokio::test]
async fn transaction_timeout() -> Result<()> {
	let ds =
		new_ds("test", "test").await?.with_transaction_timeout(Some(Duration::from_millis(500)));
	let session = Session::owner().with_ns("test").with_db("test");

	let before = Instant::now();
	let mut res = ds.execute("SLEEP 10s", &session, None).await.unwrap();
	let elapsed = before.elapsed();
	assert!(
		elapsed < Duration::from_secs(5),
		"Transaction timeout was not enforced, took {elapsed:?}"
	);

	let result = res.pop().unwrap().result;
	let err = result.unwrap_err().to_string();
	assert!(err.contains("exceeded the timeout"), "Expected transaction timeout error, got: {err}");

	Ok(())
}

#[tokio::test]
async fn transaction_timeout_begin_commit() -> Result<()> {
	let ds =
		new_ds("test", "test").await?.with_transaction_timeout(Some(Duration::from_millis(500)));
	let session = Session::owner().with_ns("test").with_db("test");

	let before = Instant::now();
	let res = ds
		.execute("BEGIN; CREATE person:1 SET name = 'a'; SLEEP 10s; COMMIT;", &session, None)
		.await
		.unwrap();
	let elapsed = before.elapsed();
	assert!(
		elapsed < Duration::from_secs(5),
		"Transaction timeout was not enforced in BEGIN block, took {elapsed:?}"
	);

	let has_timeout_err = res.iter().any(|r| {
		r.result.as_ref().is_err_and(|e| {
			e.to_string().contains("exceeded the timeout") || e.to_string().contains("timed out")
		})
	});
	assert!(has_timeout_err, "Expected transaction timeout error in BEGIN block results: {res:?}");

	Ok(())
}
