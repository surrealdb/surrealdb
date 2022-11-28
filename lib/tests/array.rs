mod parse;
use parse::Parse;
use surrealdb::sql;
use surrealdb::sql::Value;
use surrealdb::Datastore;
use surrealdb::Error;
use surrealdb::Session;

#[tokio::test]
async fn flatten() -> Result<(), Error> {
	let sql = "
    SELECT * FROM array::flatten([[1, 2], [3, 4]]);
    SELECT * FROM array::flatten([]);
    SELECT * FROM array::flatten([[1,2], [3, 4], 'SurrealDB', [5, 6, [7, 8]]]);
    SELECT * FROM [array::flatten(1)];
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[1, 2, 3, 4]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[1, 2, 3, 4, 'SurrealDB', 5, 6, [7, 8]]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::Array(sql::Array(vec![Value::None]));
	assert_eq!(tmp, val);
	//
	Ok(())
}
