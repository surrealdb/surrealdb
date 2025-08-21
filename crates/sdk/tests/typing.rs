mod helpers;
use helpers::new_ds;
use surrealdb::Result;
use surrealdb_core::dbs::Session;
use surrealdb_core::syn;

use crate::helpers::Test;

#[tokio::test]
async fn strict_typing_inline() -> Result<()> {
	let sql = "
		UPSERT person:test SET age = <int> NONE;
		UPSERT person:test SET age = <int> '18';
		UPSERT person:test SET enabled = <bool | int> NONE;
		UPSERT person:test SET enabled = <bool | int> true;
		UPSERT person:test SET name = <string> 'Tobie Morgan Hitchcock';
		UPSERT person:test SET scores = <set<float>> [1,1,2,2,3,3,4,4,5,5];
		UPSERT person:test SET scores = <array<float>> [1,1,2,2,3,3,4,4,5,5];
		UPSERT person:test SET scores = <set<float, 5>> [1,1,2,2,3,3,4,4,5,5];
		UPSERT person:test SET scores = <array<float, 5>> [1,1,2,2,3,3,4,4,5,5];
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 9);
	//
	let tmp = res.remove(0).result;
	assert_eq!(tmp.unwrap_err().to_string(), "Expected `int` but found a `NONE`");
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: person:test,
				age: 18,
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert_eq!(tmp.unwrap_err().to_string(), "Expected `bool | int` but found a `NONE`");
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: person:test,
				age: 18,
				enabled: true,
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
				id: person:test,
				age: 18,
				enabled: true,
				name: 'Tobie Morgan Hitchcock',
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
				id: person:test,
				age: 18,
				enabled: true,
				name: 'Tobie Morgan Hitchcock',
				scores: [1.0, 2.0, 3.0, 4.0, 5.0],
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
				id: person:test,
				age: 18,
				enabled: true,
				name: 'Tobie Morgan Hitchcock',
				scores: [1.0, 1.0, 2.0, 2.0, 3.0, 3.0, 4.0, 4.0, 5.0, 5.0],
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
				id: person:test,
				age: 18,
				enabled: true,
				name: 'Tobie Morgan Hitchcock',
				scores: [1.0, 2.0, 3.0, 4.0, 5.0],
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert_eq!(
		tmp.unwrap_err().to_string(),
		"Expected `array<float,5>` buf found an collection of length `10`"
	);
	//
	Ok(())
}

#[tokio::test]
async fn strict_typing_defined() -> Result<()> {
	let sql = "
		DEFINE FIELD age ON person TYPE int;
		DEFINE FIELD enabled ON person TYPE bool | int;
		DEFINE FIELD name ON person TYPE string;
		DEFINE FIELD scores ON person TYPE set<float, 5>;
		UPSERT person:test SET age = NONE, enabled = NONE, name = NONE, scored = [1,1,2,2,3,3,4,4,5,5];
		UPSERT person:test SET age = 18, enabled = NONE, name = NONE, scored = [1,1,2,2,3,3,4,4,5,5];
		UPSERT person:test SET age = 18, enabled = true, name = NONE, scored = [1,1,2,2,3,3,4,4,5,5];
		UPSERT person:test SET age = 18, enabled = true, name = 'Tobie Morgan Hitchcock', scores = [1,1,2,2,3,3,4,4,5,5];
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 8);
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result;
	assert_eq!(
		tmp.unwrap_err().to_string(),
		"Couldn't coerce value for field `age` of `person:test`: Expected `int` but found `NONE`"
	);
	//
	let tmp = res.remove(0).result;
	assert_eq!(
		tmp.unwrap_err().to_string(),
		"Couldn't coerce value for field `enabled` of `person:test`: Expected `bool | int` but found `NONE`"
	);
	//
	let tmp = res.remove(0).result;

	assert_eq!(
		tmp.unwrap_err().to_string(),
		"Couldn't coerce value for field `name` of `person:test`: Expected `string` but found `NONE`"
	);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: person:test,
				age: 18,
				enabled: true,
				name: 'Tobie Morgan Hitchcock',
				scores: [1.0, 2.0, 3.0, 4.0, 5.0],
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn strict_typing_none_null() -> Result<()> {
	let sql = "
		DEFINE TABLE person SCHEMAFULL;
		DEFINE FIELD name ON TABLE person TYPE option<string>;
		UPSERT person:test SET name = 'Tobie';
		UPSERT person:test SET name = NULL;
		UPSERT person:test SET name = NONE;
		--
		REMOVE TABLE person;
		DEFINE TABLE person SCHEMAFULL;
		DEFINE FIELD name ON TABLE person TYPE option<string | null>;
		UPSERT person:test SET name = 'Tobie';
		UPSERT person:test SET name = NULL;
		UPSERT person:test SET name = NONE;
		--
		REMOVE TABLE person;
		DEFINE TABLE person SCHEMAFULL;
		DEFINE FIELD name ON TABLE person TYPE string | null;
		UPSERT person:test SET name = 'Tobie';
		UPSERT person:test SET name = NULL;
		UPSERT person:test SET name = NONE;
	";
	let mut t = Test::new(sql).await?;
	//
	t.skip_ok(2)?;
	t.expect_val(
		"[
			{
				id: person:test,
				name: 'Tobie',
			}
		]",
	)?;
	t.expect_error(
		"Couldn't coerce value for field `name` of `person:test`: Expected `string` but found `NULL`",
	)?;
	t.expect_val(
		"[
			{
				id: person:test,
			}
		]",
	)?;
	//
	t.skip_ok(3)?;
	t.expect_val(
		"[
			{
				id: person:test,
				name: 'Tobie',
			}
		]",
	)?;
	t.expect_val(
		"[
			{
				id: person:test,
				name: NULL,
			}
		]",
	)?;
	t.expect_val(
		"[
			{
				id: person:test,
			}
		]",
	)?;
	//
	t.skip_ok(3)?;
	t.expect_val(
		"[
			{
				id: person:test,
				name: 'Tobie',
			}
		]",
	)?;
	t.expect_val(
		"[
			{
				id: person:test,
				name: NULL,
			}
		]",
	)?;
	t.expect_error(
		"Couldn't coerce value for field `name` of `person:test`: Expected `string | null` but found `NONE`"
	)?;
	//
	Ok(())
}

#[tokio::test]
async fn literal_typing() -> Result<()> {
	let sql = "
		DEFINE TABLE test SCHEMAFULL;
		DEFINE FIELD obj ON test TYPE {
		    a: int,
		    b: option<string>
		};

		CREATE ONLY test:1 SET obj = { a: 1 };
		CREATE ONLY test:2 SET obj = { a: 2, b: 'foo' };
		CREATE ONLY test:3 SET obj = { a: 3, b: 'bar', c: 'forbidden' };
	";
	let mut t = Test::new(sql).await?;
	//
	t.skip_ok(2)?;
	t.expect_val(
		"{
			id: test:1,
			obj: { a: 1 },
		}",
	)?;
	t.expect_val(
		"{
			id: test:2,
			obj: { a: 2, b: 'foo' },
		}",
	)?;
	t.expect_error(
		"Couldn't coerce value for field `obj` of `test:3`: Expected `{ a: int, b: option<string> }` but found `{ a: 3, b: 'bar', c: 'forbidden' }`"
	)?;
	//
	Ok(())
}

#[tokio::test]
async fn strict_typing_optional_object() -> Result<()> {
	let sql = "
        DEFINE TABLE test SCHEMAFULL;
        DEFINE FIELD obj ON test TYPE option<object>;
        DEFINE FIELD obj.a ON test TYPE string;

        CREATE ONLY test:1;
        CREATE ONLY test:2 SET obj = {};
        CREATE ONLY test:3 SET obj = { a: 'abc' };
	";
	let mut t = Test::new(sql).await?;
	//
	t.skip_ok(3)?;
	//
	t.expect_val(
		"{
            id: test:1,
        }",
	)?;
	//
	t.expect_error(
		"Couldn't coerce value for field `obj.a` of `test:2`: Expected `string` but found `NONE`",
	)?;
	//
	t.expect_val(
		"{
            id: test:3,
            obj: {
                a: 'abc',
            },
        }",
	)?;
	//
	Ok(())
}
