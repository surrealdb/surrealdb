mod helpers;
mod parse;
use helpers::Test;
use surrealdb::err::Error;

#[tokio::test]
async fn statement_timeouts() -> Result<(), Error> {
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
