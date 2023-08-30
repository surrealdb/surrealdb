mod parse;
use parse::Parse;
mod helpers;
use helpers::new_ds;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::sql::Value;

#[tokio::test]
async fn strict_typing_inline() -> Result<(), Error> {
	let sql = "
		UPDATE person:test SET age = <int> NONE;
		UPDATE person:test SET age = <int> '18';
		UPDATE person:test SET enabled = <bool | int> NONE;
		UPDATE person:test SET enabled = <bool | int> true;
		UPDATE person:test SET name = <string> 'Tobie Morgan Hitchcock';
		UPDATE person:test SET scores = <set<float>> [1,1,2,2,3,3,4,4,5,5];
		UPDATE person:test SET scores = <array<float>> [1,1,2,2,3,3,4,4,5,5];
		UPDATE person:test SET scores = <set<float, 5>> [1,1,2,2,3,3,4,4,5,5];
		UPDATE person:test SET scores = <array<float, 5>> [1,1,2,2,3,3,4,4,5,5];
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 9);
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == "Expected a int but cannot convert NONE into a int"
	));
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				age: 18,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == "Expected a bool | int but cannot convert NONE into a bool | int"
	));
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				age: 18,
				enabled: true,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				age: 18,
				enabled: true,
				name: 'Tobie Morgan Hitchcock',
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				age: 18,
				enabled: true,
				name: 'Tobie Morgan Hitchcock',
				scores: [1.0, 2.0, 3.0, 4.0, 5.0],
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				age: 18,
				enabled: true,
				name: 'Tobie Morgan Hitchcock',
				scores: [1.0, 1.0, 2.0, 2.0, 3.0, 3.0, 4.0, 4.0, 5.0, 5.0],
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				age: 18,
				enabled: true,
				name: 'Tobie Morgan Hitchcock',
				scores: [1.0, 2.0, 3.0, 4.0, 5.0],
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == "Expected a array<float, 5> but the array had 10 items"
	));
	//
	Ok(())
}

#[tokio::test]
async fn strict_typing_defined() -> Result<(), Error> {
	let sql = "
		DEFINE FIELD age ON person TYPE int;
		DEFINE FIELD enabled ON person TYPE bool | int;
		DEFINE FIELD name ON person TYPE string;
		DEFINE FIELD scores ON person TYPE set<float, 5>;
		UPDATE person:test SET age = NONE, enabled = NONE, name = NONE, scored = [1,1,2,2,3,3,4,4,5,5];
		UPDATE person:test SET age = 18, enabled = NONE, name = NONE, scored = [1,1,2,2,3,3,4,4,5,5];
		UPDATE person:test SET age = 18, enabled = true, name = NONE, scored = [1,1,2,2,3,3,4,4,5,5];
		UPDATE person:test SET age = 18, enabled = true, name = 'Tobie Morgan Hitchcock', scores = [1,1,2,2,3,3,4,4,5,5];
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 8);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == "Found NONE for field `age`, with record `person:test`, but expected a int"
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == "Found NONE for field `enabled`, with record `person:test`, but expected a bool | int"
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == "Found NONE for field `name`, with record `person:test`, but expected a string"
	));
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				age: 18,
				enabled: true,
				name: 'Tobie Morgan Hitchcock',
				scores: [1.0, 2.0, 3.0, 4.0, 5.0],
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn strict_typing_none_null() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE person SCHEMAFULL;
		DEFINE FIELD name ON TABLE person TYPE option<string>;
		UPDATE person:test SET name = 'Tobie';
		UPDATE person:test SET name = NULL;
		UPDATE person:test SET name = NONE;
		--
		DEFINE TABLE person SCHEMAFULL;
		DEFINE FIELD name ON TABLE person TYPE option<string | null>;
		UPDATE person:test SET name = 'Tobie';
		UPDATE person:test SET name = NULL;
		UPDATE person:test SET name = NONE;
		--
		DEFINE TABLE person SCHEMAFULL;
		DEFINE FIELD name ON TABLE person TYPE string | null;
		UPDATE person:test SET name = 'Tobie';
		UPDATE person:test SET name = NULL;
		UPDATE person:test SET name = NONE;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 15);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				name: 'Tobie',
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == "Found NULL for field `name`, with record `person:test`, but expected a option<string>"
	));
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				name: 'Tobie',
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				name: NULL,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				name: 'Tobie',
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				name: NULL,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == "Found NONE for field `name`, with record `person:test`, but expected a string | null"
	));
	//
	Ok(())
}
