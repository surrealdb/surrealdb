mod parse;
use parse::Parse;
mod helpers;
use helpers::new_ds;
use surrealdb::Result;
use surrealdb::dbs::Session;
use surrealdb::expr::Value;
use surrealdb::sql::SqlValue;

#[tokio::test]
async fn subquery_select() -> Result<()> {
	let sql = "
		-- Create a record
		CREATE person:test SET name = 'Tobie', age = 21;
		-- Select all records, returning an array
		SELECT age >= 18 as adult FROM person;
		-- Select a specific record, still returning an array
		SELECT age >= 18 as adult FROM person:test;
		-- Select all records in a subquery, returning an array
		RETURN (SELECT age >= 18 AS adult FROM person);
		-- Select a specific record in a subquery, returning an object
		RETURN (SELECT age >= 18 AS adult FROM person:test);
		-- Using an outer SELECT, select all records in a subquery, returning an array
		SELECT * FROM (SELECT age >= 18 AS adult FROM person) WHERE adult = true;
		-- Using an outer SELECT, select a specific record in a subquery, returning an array
		SELECT * FROM (SELECT age >= 18 AS adult FROM person:test) WHERE adult = true;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 7);
	//
	let tmp = res.remove(0).result?;
	let val = SqlValue::parse(
		"[
			{
				age: 21,
				id: person:test,
				name: 'Tobie'
			}
		]",
	)
	.into();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = SqlValue::parse(
		"[
			{
				adult: true
			}
		]",
	)
	.into();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = SqlValue::parse(
		"[
			{
				adult: true
			}
		]",
	)
	.into();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = SqlValue::parse(
		"[
			{
				adult: true
			}
		]",
	)
	.into();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = SqlValue::parse(
		"[
			{
				adult: true
			}
		]",
	)
	.into();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = SqlValue::parse(
		"[
			{
				adult: true
			}
		]",
	)
	.into();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = SqlValue::parse(
		"[
			{
				adult: true
			}
		]",
	)
	.into();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn subquery_ifelse_set() -> Result<()> {
	let sql = "
		-- Check if the record exists
		LET $record = (SELECT *, count() AS count FROM person:test);
		-- Return the specified record
		RETURN $record;
		-- Update the record field if it exists
		IF $record.count THEN
			(UPSERT person:test SET sport +?= 'football' RETURN sport)
		ELSE
			(UPSERT person:test SET sport = ['basketball'] RETURN sport)
		END;
		-- Check if the record exists
		LET $record = SELECT *, count() AS count FROM person:test;
		-- Return the specified record
		RETURN $record;
		-- Update the record field if it exists
		IF $record.count THEN
			UPSERT person:test SET sport +?= 'football' RETURN sport
		ELSE
			UPSERT person:test SET sport = ['basketball'] RETURN sport
		END;
		-- Check if the record exists
		LET $record = SELECT *, count() AS count FROM person:test;
		-- Return the specified record
		RETURN $record;
		-- Update the record field if it exists
		IF $record.count THEN
			UPSERT person:test SET sport +?= 'football' RETURN sport;
		ELSE
			UPSERT person:test SET sport = ['basketball'] RETURN sport;
		END;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 9);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = SqlValue::parse("[]").into();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = SqlValue::parse(
		"[
			{
				sport: [
					'basketball',
				]
			}
		]",
	)
	.into();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = SqlValue::parse(
		"[
			{
				count: 1,
				id: person:test,
				sport: [
					'basketball',
				]
			}
		]",
	)
	.into();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = SqlValue::parse(
		"[
			{
				sport: [
					'basketball',
					'football',
				]
			}
		]",
	)
	.into();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = SqlValue::parse(
		"[
			{
				count: 1,
				id: person:test,
				sport: [
					'basketball',
					'football',
				]
			}
		]",
	)
	.into();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = SqlValue::parse(
		"[
			{
				sport: [
					'basketball',
					'football',
				]
			}
		]",
	)
	.into();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn subquery_ifelse_array() -> Result<()> {
	let sql = "
		-- Check if the record exists
		LET $record = (SELECT *, count() AS count FROM person:test);
		-- Return the specified record
		RETURN $record;
		-- Update the record field if it exists
		IF $record.count THEN
			(UPSERT person:test SET sport += 'football' RETURN sport)
		ELSE
			(UPSERT person:test SET sport = ['basketball'] RETURN sport)
		END;
		-- Check if the record exists
		LET $record = SELECT *, count() AS count FROM person:test;
		-- Return the specified record
		RETURN $record;
		-- Update the record field if it exists
		IF $record.count THEN
			UPSERT person:test SET sport += 'football' RETURN sport
		ELSE
			UPSERT person:test SET sport = ['basketball'] RETURN sport
		END;
		-- Check if the record exists
		LET $record = SELECT *, count() AS count FROM person:test;
		-- Return the specified record
		RETURN $record;
		-- Update the record field if it exists
		IF $record.count THEN
			UPSERT person:test SET sport += 'football' RETURN sport;
		ELSE
			UPSERT person:test SET sport = ['basketball'] RETURN sport;
		END;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 9);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = SqlValue::parse("[]").into();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = SqlValue::parse(
		"[
			{
				sport: [
					'basketball',
				]
			}
		]",
	)
	.into();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = SqlValue::parse(
		"[
			{
				count: 1,
				id: person:test,
				sport: [
					'basketball',
				]
			}
		]",
	)
	.into();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = SqlValue::parse(
		"[
			{
				sport: [
					'basketball',
					'football',
				]
			}
		]",
	)
	.into();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = SqlValue::parse(
		"[
			{
				count: 1,
				id: person:test,
				sport: [
					'basketball',
					'football',
				]
			}
		]",
	)
	.into();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = SqlValue::parse(
		"[
			{
				sport: [
					'basketball',
					'football',
					'football',
				]
			}
		]",
	)
	.into();
	assert_eq!(tmp, val);
	//
	Ok(())
}
