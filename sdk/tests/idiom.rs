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
		};
		RETURN [1,2,3,4][fn::foo()];
	"#;
	Test::new(sql).await?.expect_val("None")?.expect_val("3")?;
	Ok(())
}

#[tokio::test]
async fn idiom_index_range() -> Result<(), Error> {
	let sql = r#"
		[1,2,3,4][1..2];
		[1,2,3,4][1..=2];
		[1,2,3,4][1>..=2];
		[1,2,3,4][1>..];
		[1,2,3,4][1..];
		[1,2,3,4][..2];
		[1,2,3,4][..=2];
	"#;
	Test::new(sql)
		.await?
		.expect_val("[2]")?
		.expect_val("[2,3]")?
		.expect_val("[3]")?
		.expect_val("[3,4]")?
		.expect_val("[2,3,4]")?
		.expect_val("[1,2]")?
		.expect_val("[1,2,3]")?;
	Ok(())
}

#[tokio::test]
async fn idiom_array_nested_prop_continues_as_array() -> Result<(), Error> {
	let sql = r#"
    	[{x:2}].x[0];
    	[{x:2}].x.at(0);
	"#;
	Test::new(sql).await?.expect_val("2")?.expect_val("2")?;
	Ok(())
}

#[tokio::test]
async fn idiom_select_all_from_nested_array_prop() -> Result<(), Error> {
	let sql = r#"
    	CREATE a:1, a:2;
        RELATE a:1->edge:1->a:2;
        a:1->edge.out;
        a:1->edge.out.*;
	"#;
	Test::new(sql)
		.await?
		.expect_val("[{id: a:1}, {id: a:2}]")?
		.expect_val("[{id: edge:1, in: a:1, out: a:2}]")?
		.expect_val("[a:2]")?
		.expect_val("[{id: a:2}]")?;
	Ok(())
}

#[tokio::test]
async fn idiom_graph_with_filter_should_be_flattened() -> Result<(), Error> {
	let sql = r#"
    	CREATE person:1, person:2, person:3;
		RELATE person:1->likes:1->person:2;
		RELATE person:2->likes:2->person:3;
		person:1->likes->person->likes->person;
		person:1->likes->person[?true]->likes->person;
		person:1->likes->person[?true];
		[person:1][?true]->likes->person;
		[person:1]->likes->person[?true]->likes->person;
		SELECT ->likes[?true]->person as likes FROM person;
	"#;
	Test::new(sql)
		.await?
		.expect_val("[{id: person:1}, {id: person:2}, {id: person:3}]")?
		.expect_val("[{id: likes:1, in: person:1, out: person:2}]")?
		.expect_val("[{id: likes:2, in: person:2, out: person:3}]")?
		.expect_val("[person:3]")?
		.expect_val("[person:3]")?
		.expect_val("[person:2]")?
		.expect_val("[[person:2]]")?
		.expect_val("[[person:3]]")?
		.expect_val("[
			{likes: [person:2]},
			{likes: [person:3]},
			{likes: []},
		]")?;
	Ok(())
}
