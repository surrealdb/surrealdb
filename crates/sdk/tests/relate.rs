mod helpers;
use helpers::new_ds;
use surrealdb::Result;
use surrealdb_core::dbs::Session;
use surrealdb_core::err::Error;
use surrealdb_core::syn;
use surrealdb_core::val::Value;

use crate::helpers::Test;

#[tokio::test]
async fn relate_with_parameters() -> Result<()> {
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
	let val = syn::value(
		"[
			{
				id: knows:test,
				in: person:tobie,
				out: person:jaime,
				brother: true,
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn relate_and_overwrite() -> Result<()> {
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
	let val = syn::value(
		"[
			{
				id: knows:test,
				in: person:tobie,
				out: person:jaime,
				brother: true,
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: knows:test,
				in: person:tobie,
				out: person:jaime,
				test: true,
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: knows:test,
				in: person:tobie,
				out: person:jaime,
				test: true,
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn relate_with_param_or_subquery() -> Result<()> {
	let sql = r#"
		USE NS test DB test;
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
	assert_eq!(res.len(), 9);

	// USE NS test DB test;
	let tmp = res.remove(0).result;
	tmp.unwrap();
	// LET $tobie = person:tobie;
	let tmp = res.remove(0).result;
	assert_eq!(tmp.unwrap(), Value::None);
	// LET $jaime = person:jaime;
	let tmp = res.remove(0).result;
	assert_eq!(tmp.unwrap(), Value::None);
	// LET $relation = type::table("knows");
	let tmp = res.remove(0).result;
	assert_eq!(tmp.unwrap(), Value::None);
	// RELATE $tobie->$relation->$jaime;
	// RELATE $tobie->(type::table("knows"))->$jaime;
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
		assert_eq!(o.get("in").unwrap(), &syn::value("person:tobie").unwrap());
		assert_eq!(o.get("out").unwrap(), &syn::value("person:jaime").unwrap());
		let id = o.get("id").unwrap();

		let Value::RecordId(t) = id else {
			panic!("should be thing {id:?}")
		};
		assert_eq!(t.table, "knows");
	}
	// LET $relation = type::thing("knows:foo");
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	// RELATE $tobie->$relation->$jaime;
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: knows:foo,
				in: person:tobie,
				out: person:jaime,
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	// LET $relation = type::thing("knows:bar");
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: knows:bar,
				in: person:tobie,
				out: person:jaime,
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn relate_with_complex_table() -> Result<()> {
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
	let val = syn::value("[{ id: a:1 }, { id: a:2 }]").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value("[{ id: `-`:`-`, in: a:1, out: a:2 }]").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value("[{ rel: [`-`:`-`] }]").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn schemafull_relate() -> Result<()> {
	let sql = r#"
	USE NS test DB test;
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
	// USE NS test DB test;
	let tmp = t.next()?.result;
	tmp.unwrap();
	//
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
	t.expect_error_func(|e| matches!(e.downcast_ref(), Some(Error::FieldCoerce { .. })))?;

	// dog:1 is not a person
	t.expect_error_func(|e| matches!(e.downcast_ref(), Some(Error::FieldCoerce { .. })))?;

	Ok(())
}

#[tokio::test]
async fn relate_enforced() -> Result<()> {
	let sql = "
	    DEFINE TABLE edge TYPE RELATION ENFORCED;
		RELATE a:1->edge:1->a:2;
		CREATE a:1, a:2;
		RELATE a:1->edge:1->a:2;
		INFO FOR DB;
	";

	let mut t = Test::new(sql).await?;
	//
	t.skip_ok(1)?;
	//
	t.expect_error_func(|e| matches!(e.downcast_ref(), Some(Error::IdNotFound { .. })))?;
	//
	t.expect_val("[{ id: a:1 }, { id: a:2 }]")?;
	//
	t.expect_val("[{ id: edge:1, in: a:1, out: a:2 }]")?;
	//
	let info = syn::value(
		"{
	accesses: {},
	analyzers: {},
	apis: {},
	buckets: {},
	configs: {},
	functions: {},
	models: {},
	params: {},
	sequences: {},
	tables: {
		a: 'DEFINE TABLE a TYPE ANY SCHEMALESS PERMISSIONS NONE',
		edge: 'DEFINE TABLE edge TYPE RELATION ENFORCED SCHEMALESS PERMISSIONS NONE'
	},
	users: {}
	}",
	)
	.unwrap();
	t.expect_value(info)?;
	Ok(())
}
