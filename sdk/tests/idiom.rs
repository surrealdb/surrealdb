mod helpers;
mod parse;
use helpers::Test;
use surrealdb::err::Error;

#[tokio::test]
async fn idiom_chain_part_optional() -> Result<(), Error> {
	let sql = r#"
		{}.prop.is_bool();
		{}.prop?.is_bool();
	"#;
	Test::new(sql).await?.expect_val("false")?.expect_val("None")?;
	Ok(())
}

#[tokio::test]
async fn idiom_index_expression() -> Result<(), Error> {
	let sql = r#"
		[1,2,3,4][1 + 1];
	"#;
	Test::new(sql).await?.expect_val("3")?;
	Ok(())
}

#[tokio::test]
async fn idiom_index_call() -> Result<(), Error> {
	let sql = r#"
		DEFINE FUNCTION fn::foo() {
			return 1 + 1;
		}
		[1,2,3,4][fn::foo()];
	"#;
	Test::new(sql).await?.expect_val("3")?;
	Ok(())
}
