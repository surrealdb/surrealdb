mod helpers;
use helpers::Test;
use surrealdb::err::Error;

#[tokio::test]
async fn expr_modulo() -> Result<(), Error> {
	Test::new("8 % 3").await?.expect_val("2")?;
	Ok(())
}

#[tokio::test]
async fn expr_value_in_range() -> Result<(), Error> {
	Test::new(
		"
    	    1 in 1..2;
    	    'a' in 'a'..'b';
    		0 in 1..2;
    	",
	)
	.await?
	.expect_val("true")?
	.expect_val("true")?
	.expect_val("false")?;
	Ok(())
}

#[tokio::test]
async fn expr_object_contains_key() -> Result<(), Error> {
	Test::new("
		'a' IN { a: 1 };
		'b' IN { a: 1 };
	").await?
		.expect_val("true")?
		.expect_val("false")?;
	Ok(())
}