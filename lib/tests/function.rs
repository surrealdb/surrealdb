mod parse;
use parse::Parse;
use surrealdb::sql::Value;
use surrealdb::Datastore;
use surrealdb::Error;
use surrealdb::Session;

#[tokio::test]
async fn array_distinct() -> Result<(), Error> {
	let sql = r#"
		SELECT * FROM array::distinct([1, 3, 2, 1, 3, 3, 4]);
		SELECT * FROM array::distinct([]);
		SELECT * FROM array::distinct("something");
		SELECT * FROM array::distinct(["something"]);
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
	let val = Value::parse("[NONE]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("['something']");
	assert_eq!(tmp, val);
	//
	Ok(())
}
