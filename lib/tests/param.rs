mod parse;
use parse::Parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 5);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			dl: {},
			dt: {},
			pa: { test: 'DEFINE PARAM $test VALUE 12345' },
			sc: {},
			tb: {},
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
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
		Some(e) if e.to_string() == r#"Found 'auth' but it is not possible to set a variable with this name"#
	));
	//
	Ok(())
}
