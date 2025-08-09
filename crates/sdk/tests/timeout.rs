mod helpers;
use std::time::{Duration, Instant};

use helpers::{Test, new_ds};
use surrealdb::Result;
use surrealdb_core::dbs::Session;

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
	let error = "The query was not executed because it exceeded the timeout";
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
	let ds = new_ds().await?.with_query_timeout(Some(Duration::from_millis(500)));
	let session = Session::owner();
	let before = Instant::now();
	let mut res = ds.execute(sql, &session, None).await.unwrap();
	if before.elapsed() > Duration::from_millis(7050) {
		panic!("Query did not properly timeout");
	}
	res.pop().unwrap().result.unwrap_err();

	Ok(())
}
