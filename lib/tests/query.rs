mod parse;
use parse::Parse;
mod helpers;
use helpers::new_ds;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::sql::Value;

#[tokio::test]
async fn query_basic() -> Result<(), Error> {
	let sql = "
		LET $test = 'Tobie';
		SELECT * FROM $test;
		RETURN $test;
		$test;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("['Tobie']");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("Tobie");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("Tobie");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn query_basic_with_modification() -> Result<(), Error> {
	let sql = "
		LET $test = 33693;
		SELECT * FROM $test + 11369;
		RETURN $test + 11369;
		$test + 11369;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[45062]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(45062);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(45062);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn query_root_function() -> Result<(), Error> {
	let sql = "
		LET $test = 'This is a test';
		string::uppercase($test);
		string::lowercase($test);
		string::slug($test);
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("THIS IS A TEST");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("this is a test");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("this-is-a-test");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn query_root_record() -> Result<(), Error> {
	let sql = "
		UPSERT person:tobie SET name = 'Tobie';
		UPSERT person:jaime SET name = 'Jaime';
		RELATE person:tobie->knows->person:jaime SET id = 'test', brother = true;
		<future> { person:tobie->knows->person.name };
		person:tobie->knows->person.name;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
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
				id: knows:test,
				in: person:tobie,
				out: person:jaime,
				brother: true,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("['Jaime']");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("['Jaime']");
	assert_eq!(tmp, val);
	//
	Ok(())
}
