mod parse;
use parse::Parse;
use surrealdb::sql::Value;
use surrealdb::Datastore;
use surrealdb::Error;
use surrealdb::Session;

#[tokio::test]
async fn future_function_simple() -> Result<(), Error> {
	let sql = "
		UPDATE person:test SET can_drive = <future> { birthday && time::now() > birthday + 18y };
		UPDATE person:test SET birthday = <datetime> '2007-06-22';
		UPDATE person:test SET birthday = <datetime> '2001-06-22';
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: person:test, can_drive: NONE }]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val =
		Value::parse("[{ id: person:test, birthday: '2007-06-22T00:00:00Z', can_drive: false }]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val =
		Value::parse("[{ id: person:test, birthday: '2001-06-22T00:00:00Z', can_drive: true }]");
	assert_eq!(tmp, val);
	//
	Ok(())
}
