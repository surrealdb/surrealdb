mod parse;
use parse::Parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;
use surrealdb::sql::Value;

#[tokio::test]
async fn field_definition_value_assert_failure() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE person SCHEMAFULL;
		DEFINE FIELD age ON person TYPE number ASSERT $value > 0;
		DEFINE FIELD email ON person TYPE string ASSERT is::email($value);
		DEFINE FIELD name ON person TYPE option<string> VALUE $value OR 'No name';
		CREATE person:test SET email = 'info@surrealdb.com', other = 'ignore';
		CREATE person:test SET email = 'info@surrealdb.com', other = 'ignore', age = NONE;
		CREATE person:test SET email = 'info@surrealdb.com', other = 'ignore', age = NULL;
		CREATE person:test SET email = 'info@surrealdb.com', other = 'ignore', age = 0;
		CREATE person:test SET email = 'info@surrealdb.com', other = 'ignore', age = 13;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 9);
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
		Some(e) if e.to_string() == "Found NONE for field `age`, with record `person:test`, but expected a number"
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == "Found NONE for field `age`, with record `person:test`, but expected a number"
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == "Found NULL for field `age`, with record `person:test`, but expected a number"
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == "Found 0 for field `age`, with record `person:test`, but field must conform to: $value > 0"
	));
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				age: 13,
				email: 'info@surrealdb.com',
				id: person:test,
				name: 'No name',
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn field_definition_value_assert_success() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE person SCHEMAFULL;
		DEFINE FIELD age ON person TYPE number ASSERT $value > 0;
		DEFINE FIELD email ON person TYPE string ASSERT is::email($value);
		DEFINE FIELD name ON person TYPE option<string> VALUE $value OR 'No name';
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

#[tokio::test]
async fn field_definition_empty_nested_objects() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE person SCHEMAFULL;
		DEFINE FIELD settings on person TYPE object;
		UPDATE person:test CONTENT {
		    settings: {
		        nested: {
		            object: {
						thing: 'test'
					}
		        }
		    }
		};
		SELECT * FROM person;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 4);
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
				settings: {},
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
				settings: {},
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn field_definition_empty_nested_arrays() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE person SCHEMAFULL;
		DEFINE FIELD settings on person TYPE object;
		UPDATE person:test CONTENT {
		    settings: {
		        nested: [
					1,
					2,
					3,
					4,
					5
				]
		    }
		};
		SELECT * FROM person;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 4);
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
				settings: {},
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
				settings: {},
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn field_definition_empty_nested_flexible() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE person SCHEMAFULL;
		DEFINE FIELD settings on person FLEXIBLE TYPE object;
		UPDATE person:test CONTENT {
		    settings: {
				nested: {
		            object: {
						thing: 'test'
					}
		        }
		    }
		};
		SELECT * FROM person;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 4);
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
				settings: {
					nested: {
			            object: {
							thing: 'test'
						}
			        }
				},
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
				settings: {
					nested: {
			            object: {
							thing: 'test'
						}
			        }
				},
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}
