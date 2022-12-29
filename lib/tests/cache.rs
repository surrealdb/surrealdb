mod parse;
use parse::Parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;
use surrealdb::sql::Value;

#[tokio::test]
async fn clear_transaction_cache_table() -> Result<(), Error> {
	let sql = "
		BEGIN;
		CREATE person:one CONTENT { x: 0 };
		SELECT * FROM person;
		DEFINE TABLE other AS SELECT * FROM person;
		COMMIT;
		SELECT * FROM other;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:one,
				x: 0
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:one,
				x: 0
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: other:one,
				x: 0
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn clear_transaction_cache_field() -> Result<(), Error> {
	let sql = "
		DEFINE FIELD test ON person TYPE string VALUE 'test';
		BEGIN;
		UPDATE person:one CONTENT { x: 0 };
		SELECT * FROM person;
		REMOVE FIELD test ON person;
		UPDATE person:two CONTENT { x: 0 };
		SELECT * FROM person;
		COMMIT;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 6);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:one,
				test: 'test',
				x: 0
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:one,
				test: 'test',
				x: 0
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:two,
				x: 0
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:one,
				test: 'test',
				x: 0
			},
			{
				id: person:two,
				x: 0
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}
