mod parse;
use parse::Parse;
use surrealdb::sql::Value;
use surrealdb::Datastore;
use surrealdb::Error;
use surrealdb::Session;

#[tokio::test]
async fn field_definition_value_assert_failure() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE person SCHEMAFULL;
		DEFINE FIELD age ON person TYPE number ASSERT $value > 0;
		DEFINE FIELD email ON person TYPE string ASSERT is::email($value);
		DEFINE FIELD name ON person TYPE string VALUE $value OR 'No name';
		CREATE person:test SET email = 'info@surrealdb.com', other = 'ignore';
		CREATE person:test SET email = 'info@surrealdb.com', other = 'ignore', age = NONE;
		CREATE person:test SET email = 'info@surrealdb.com', other = 'ignore', age = NULL;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 7);
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
		Some(e) if e.to_string() == "Found NONE for field `age`, with record `person:test`, but field must conform to: $value > 0"
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == "Found NONE for field `age`, with record `person:test`, but field must conform to: $value > 0"
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == "Found NULL for field `age`, with record `person:test`, but field must conform to: $value > 0"
	));
	//
	Ok(())
}

#[tokio::test]
async fn field_definition_value_assert_success() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE person SCHEMAFULL;
		DEFINE FIELD age ON person TYPE number ASSERT $value > 0;
		DEFINE FIELD email ON person TYPE string ASSERT is::email($value);
		DEFINE FIELD name ON person TYPE string VALUE $value OR 'No name';
		CREATE person:test SET email = 'info@surrealdb.com', other = 'ignore', age = 22;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 5);
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
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				email: 'info@surrealdb.com',
				age: 22,
				name: 'No name',
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}
