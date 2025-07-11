mod parse;
use parse::Parse;
mod helpers;
use helpers::new_ds;
use surrealdb::Result;
use surrealdb::dbs::Session;
use surrealdb::expr::Value;
use surrealdb::sql::SqlValue;

#[tokio::test]
async fn query_basic() -> Result<()> {
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
	let tmp = res.remove(0).take_first()?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).values?;
	let val = SqlValue::parse("['Tobie']").into_vec();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).take_first()?;
	let val = SqlValue::from("Tobie");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).take_first()?;
	let val = SqlValue::from("Tobie");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn query_basic_with_modification() -> Result<()> {
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
	let tmp = res.remove(0).take_first()?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).values?;
	let val = SqlValue::parse("[45062]").into_vec();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).take_first()?;
	let val = Value::from(45062);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).take_first()?;
	let val = Value::from(45062);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn query_root_function() -> Result<()> {
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
	let tmp = res.remove(0).take_first()?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).take_first()?;
	let val = Value::from("THIS IS A TEST");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).take_first()?;
	let val = Value::from("this is a test");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).take_first()?;
	let val = Value::from("this-is-a-test");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn query_root_record() -> Result<()> {
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
	let tmp = res.remove(0).values?;
	let val = SqlValue::parse(
		"[
			{
				id: person:tobie,
				name: 'Tobie'
			}
		]",
	)
	.into_vec();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).values?;
	let val = SqlValue::parse(
		"[
			{
				id: person:jaime,
				name: 'Jaime'
			}
		]",
	)
	.into_vec();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).values?;
	let val = SqlValue::parse(
		"[
			{
				id: knows:test,
				in: person:tobie,
				out: person:jaime,
				brother: true,
			}
		]",
	)
	.into_vec();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).values?;
	let val = SqlValue::parse("['Jaime']").into_vec();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).values?;
	let val = SqlValue::parse("['Jaime']").into_vec();
	assert_eq!(tmp, val);
	//
	Ok(())
}
