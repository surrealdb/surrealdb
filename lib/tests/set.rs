mod parse;
use parse::Parse;
mod helpers;
use helpers::{new_ds, Test};
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::sql::Value;

#[tokio::test]
async fn define_global_param() -> Result<(), Error> {
	let sql = "
        LET $foo: int = 42;
        RETURN $foo;
        LET $bar: int = 'hello';
        RETURN $bar;
	";
	let error = "Found 'hello' for param $bar, but expected a int";
	Test::new(sql)
		.await?
		.expect_val("None")?
		.expect_val("42")?
		.expect_error(error)?
		.expect_error("None")?;

	Ok(())
}
