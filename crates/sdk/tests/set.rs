mod helpers;
mod parse;
use helpers::Test;
use surrealdb::err::Error;

#[tokio::test]
async fn typed_set() -> Result<(), Error> {
	let sql = "
        LET $foo: int = 42;
        RETURN $foo;
        LET $bar: int = 'hello';
        RETURN $bar;
	";
	let error =
		"Tried to set `$bar`, but couldn't coerce value: Expected `int` but found `'hello'`";
	Test::new(sql)
		.await?
		.expect_val("None")?
		.expect_val("42")?
		.expect_error(error)?
		.expect_val("None")?;

	Ok(())
}
