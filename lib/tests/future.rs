mod parse;
use parse::Parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;
use surrealdb::sql::Value;

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

#[tokio::test]
async fn future_function_arguments() -> Result<(), Error> {
	let sql = "
		UPDATE future:test SET
			a = 'test@surrealdb.com',
			b = <future> { 'test@surrealdb.com' },
			x = 'a-' + parse::email::user(a),
			y = 'b-' + parse::email::user(b)
		;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				a: 'test@surrealdb.com',
				b: 'test@surrealdb.com',
				id: 'future:test',
				x: 'a-test',
				y: 'b-test',
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}
