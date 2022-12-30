mod parse;
use parse::Parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;
use surrealdb::sql::Value;

#[tokio::test]
async fn create_with_id() -> Result<(), Error> {
	let sql = "
		CREATE person:test SET name = 'Tester';
		CREATE person SET id = person:tobie, name = 'Tobie';
		CREATE person CONTENT { id: person:jaime, name: 'Jaime' };
		CREATE user CONTENT { id: 1, name: 'Robert' };
		CREATE city CONTENT { id: 'london', name: 'London' };
		CREATE city CONTENT { id: '8e60244d-95f6-4f95-9e30-09a98977efb0', name: 'London' };
		CREATE temperature CONTENT { id: ['London', '2022-09-30T20:25:01.406828Z'], name: 'London' };
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 7);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				name: 'Tester'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:tobie,
				name: 'Tobie'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:jaime,
				name: 'Jaime'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: user:1,
				name: 'Robert'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: city:london,
				name: 'London'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: city:⟨8e60244d-95f6-4f95-9e30-09a98977efb0⟩,
				name: 'London'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: temperature:['London', '2022-09-30T20:25:01.406828Z'],
				name: 'London'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}
