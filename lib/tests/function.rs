mod parse;
use parse::Parse;
use surrealdb::sql::Value;
use surrealdb::Datastore;
use surrealdb::Error;
use surrealdb::Session;

#[tokio::test]
async fn function_array_distinct() -> Result<(), Error> {
	let sql = r#"
		RETURN array::distinct([1, 3, 2, 1, 3, 3, 4]);
		RETURN array::distinct([]);
		RETURN array::distinct("something");
		RETURN array::distinct(["something"]);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[1, 3, 2, 4]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("['something']");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_slice() -> Result<(), Error> {
	let sql = r#"
		RETURN string::slice("the quick brown fox jumps over the lazy dog.");
		RETURN string::slice("the quick brown fox jumps over the lazy dog.", 16);
		RETURN string::slice("the quick brown fox jumps over the lazy dog.", 0, 60);
		RETURN string::slice("the quick brown fox jumps over the lazy dog.", 0, -1);
		RETURN string::slice("the quick brown fox jumps over the lazy dog.", 16, -1);
		RETURN string::slice("the quick brown fox jumps over the lazy dog.", -9, -1);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 6);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'the quick brown fox jumps over the lazy dog.'");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'fox jumps over the lazy dog.'");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'the quick brown fox jumps over the lazy dog.'");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'the quick brown fox jumps over the lazy dog'");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'fox jumps over the lazy dog'");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'lazy dog'");
	assert_eq!(tmp, val);
	//
	Ok(())
}
