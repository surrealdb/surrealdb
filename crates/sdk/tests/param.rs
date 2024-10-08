mod parse;
use parse::Parse;
mod helpers;
use helpers::new_ds;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::sql::Value;

#[tokio::test]
async fn define_global_param() -> Result<(), Error> {
	let sql = "
		DEFINE PARAM $test VALUE 12345;
		INFO FOR DB;
		SELECT * FROM $test;
		LET $test = 56789;
		SELECT * FROM $test;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			accesses: {},
			analyzers: {},
			configs: {},
			functions: {},
			models: {},
			params: { test: 'DEFINE PARAM $test VALUE 12345 PERMISSIONS FULL' },
			tables: {},
			users: {},
		}",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[12345]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[56789]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn define_protected_param() -> Result<(), Error> {
	let sql = "
		LET $test = { some: 'thing', other: true };
		SELECT * FROM $test WHERE some = 'thing';
		LET $auth = { ID: admin:tester };
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				other: true,
				some: 'thing'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == "'auth' is a protected variable and cannot be set"
	));
	//
	Ok(())
}

#[tokio::test]
async fn parameter_outside_database() -> Result<(), Error> {
	let sql = "RETURN $does_not_exist;";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);

	match res.remove(0).result {
		Err(Error::DbEmpty) => (),
		_ => panic!("Query should have failed with error: Specify a database to use"),
	}

	Ok(())
}
