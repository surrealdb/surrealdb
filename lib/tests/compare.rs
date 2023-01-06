mod parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;
use surrealdb::sql::Value;

#[tokio::test]
async fn compare_empty() -> Result<(), Error> {
	let sql = r#"
		RETURN NONE = NONE;
		RETURN NULL = NULL;
		RETURN NONE = NULL;
		RETURN [] = [];
		RETURN {} = {};
		RETURN [] = {};
		RETURN 0 = 0;
		RETURN 0 = 0.0;
		RETURN 0 = 0.1;
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 9);
	//
	let tmp = res.remove(0).result?;
	let val = Value::True;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::True;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::False;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::True;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::True;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::False;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::True;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::True;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::False;
	assert_eq!(tmp, val);
	//
	Ok(())
}
