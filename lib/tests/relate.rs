mod parse;

use parse::Parse;

mod helpers;
use crate::helpers::Test;

use helpers::new_ds;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::sql::Value;

#[tokio::test]
async fn relate_with_parameters() -> Result<(), Error> {
	let sql = "
		LET $tobie = person:tobie;
		LET $jaime = person:jaime;
		RELATE $tobie->knows->$jaime SET id = knows:test, brother = true;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: knows:test,
				in: person:tobie,
				out: person:jaime,
				brother: true,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn relate_and_overwrite() -> Result<(), Error> {
	let sql = "
		LET $tobie = person:tobie;
		LET $jaime = person:jaime;
		RELATE $tobie->knows->$jaime CONTENT { id: knows:test, brother: true };
		UPDATE knows:test CONTENT { test: true };
		SELECT * FROM knows:test;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: knows:test,
				in: person:tobie,
				out: person:jaime,
				brother: true,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: knows:test,
				in: person:tobie,
				out: person:jaime,
				test: true,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: knows:test,
				in: person:tobie,
				out: person:jaime,
				test: true,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn relate_with_param_or_subquery() -> Result<(), Error> {
	let sql = r#"
		LET $tobie = person:tobie;
		LET $jaime = person:jaime;
        LET $relation = type::table("knows");
		RELATE $tobie->$relation->$jaime;
		RELATE $tobie->(type::table("knows"))->$jaime;
        LET $relation = type::thing("knows:foo");
		RELATE $tobie->$relation->$jaime;
		RELATE $tobie->(type::thing("knows:bar"))->$jaime;
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 8);
	//
	for _ in 0..3 {
		let tmp = res.remove(0).result?;
		let val = Value::None;
		assert_eq!(tmp, val);
	}
	//
	for _ in 0..2 {
		let tmp = res.remove(0).result?;
		let Value::Array(v) = tmp else {
			panic!("response should be array:{tmp:?}")
		};
		assert_eq!(v.len(), 1);
		let tmp = v.into_iter().next().unwrap();
		let Value::Object(o) = tmp else {
			panic!("should be object {tmp:?}")
		};
		assert_eq!(o.get("in").unwrap(), &Value::parse("person:tobie"));
		assert_eq!(o.get("out").unwrap(), &Value::parse("person:jaime"));
		let id = o.get("id").unwrap();

		let Value::Thing(t) = id else {
			panic!("should be thing {id:?}")
		};
		assert_eq!(t.tb, "knows");
	}
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: knows:foo,
				in: person:tobie,
				out: person:jaime,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: knows:bar,
				in: person:tobie,
				out: person:jaime,
			}
		]",
	);
	//
	assert_eq!(tmp, val);
	Ok(())
}

#[tokio::test]
async fn relate_with_complex_table() -> Result<(), Error> {
	let sql = "
		CREATE a:1, a:2;
		RELATE a:1->`-`:`-`->a:2;
		select ->`-` as rel from a:1;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: a:1 }, { id: a:2 }]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: `-`:`-`, in: a:1, out: a:2 }]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ rel: [`-`:`-`] }]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn schemafull_relate() -> Result<(), Error> {
	let sql = r#"
	INSERT INTO person [
		{ id: 1 },
		{ id: 2 }
	];

	DEFINE TABLE likes TYPE RELATION FROM person TO person;
	DEFINE FIELD reason ON likes TYPE string;

	RELATE person:1 -> likes -> person:2 CONTENT {id: 1, reason: "nice smile"};
	RELATE person:2 -> likes -> person:1 CONTENT {id: 2, reason: true};
	RELATE dog:1 -> likes -> person:2 CONTENT {id: 3, reason: "nice smell"};
	"#;

	let mut t = Test::new(sql).await?;

	t.expect_val(
		"[
			{id: person:1},
			{id: person:2}
        ]",
	)?;

	t.skip_ok(2)?;

	t.expect_val(
		"[
			{
				id: likes:1,
				in: person:1,
				out: person:2,
				reason: 'nice smile'
			}
        ]",
	)?;

	// reason is bool not string
	t.expect_error_func(|e| matches!(e, Error::FieldCheck { .. }))?;

	// dog:1 is not a person
	t.expect_error_func(|e| matches!(e, Error::FieldCheck { .. }))?;

	Ok(())
}
