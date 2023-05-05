mod parse;
use parse::Parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;
use surrealdb::sql::Value;

#[tokio::test]
async fn select_field_value() -> Result<(), Error> {
	let sql = "
		CREATE person:tobie SET name = 'Tobie';
		CREATE person:jaime SET name = 'Jaime';
		SELECT VALUE name FROM person;
		SELECT name FROM person;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 4);
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
			'Jaime',
			'Tobie',
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				name: 'Jaime'
			},
			{
				name: 'Tobie'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn select_where_or() -> Result<(), Error> {
	let sql = "
		CREATE person:tobie SET name = 'Tobie';
		DEFINE INDEX person_name ON TABLE person COLUMNS name;
		CREATE activity:piano SET name = 'Piano';
		SELECT name FROM person,activity WHERE name = 'Tobie' OR name = 'Piano';";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 4);
	//
	let _ = res.remove(0).result?;
	let _ = res.remove(0).result?;
	let _ = res.remove(0).result?;
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				name: 'Tobie'
			},
			{
				name: 'Piano'
			}
		]",
	);
	assert_eq!(tmp, val);
	Ok(())
}
