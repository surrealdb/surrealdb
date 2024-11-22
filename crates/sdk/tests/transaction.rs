mod parse;
use parse::Parse;
mod helpers;
use helpers::new_ds;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::sql::Value;

#[tokio::test]
async fn transaction_basic() -> Result<(), Error> {
	let sql = "
		BEGIN;
		CREATE person:tobie;
		CREATE person:jaime;
		COMMIT;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:tobie,
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
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn transaction_with_return() -> Result<(), Error> {
	let sql = "
		BEGIN;
		CREATE person:tobie;
		CREATE person:jaime;
		RETURN { tobie: person:tobie, jaime: person:jaime };
		COMMIT;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			tobie: person:tobie,
			jaime: person:jaime,
		}",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn transaction_with_failure() -> Result<(), Error> {
	let sql = "
		BEGIN;
		CREATE person:tobie;
		CREATE person:jaime;
		CREATE person:tobie;
		COMMIT;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"The query was not executed due to a failed transaction"#
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"The query was not executed due to a failed transaction"#
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Database record `person:tobie` already exists"#
	));
	//
	Ok(())
}

#[tokio::test]
async fn transaction_with_failure_and_return() -> Result<(), Error> {
	let sql = "
		BEGIN;
		CREATE person:tobie;
		CREATE person:jaime;
		CREATE person:tobie;
		RETURN { tobie: person:tobie, jaime: person:jaime };
		COMMIT;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result;
	assert_eq!(
		tmp.err().map(|x| x.to_string()),
		Some(r#"The query was not executed due to a failed transaction"#.to_string())
	);
	let tmp = res.remove(0).result;
	assert_eq!(
		tmp.err().map(|x| x.to_string()),
		Some(r#"The query was not executed due to a failed transaction"#.to_string())
	);
	let tmp = res.remove(0).result;
	assert_eq!(
		tmp.err().map(|x| x.to_string()),
		Some(r#"Database record `person:tobie` already exists"#.to_string())
	);
	let tmp = res.remove(0).result;
	assert_eq!(
		tmp.err().map(|x| x.to_string()),
		Some(r#"The query was not executed due to a failed transaction"#.to_string())
	);
	//
	Ok(())
}

#[tokio::test]
async fn transaction_with_throw() -> Result<(), Error> {
	let sql = "
		BEGIN;
		CREATE person:tobie;
		CREATE person:jaime;
		THROW 'there was an error';
		COMMIT;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"The query was not executed due to a failed transaction"#
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"The query was not executed due to a failed transaction"#
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"An error occurred: there was an error"#
	));
	//
	Ok(())
}

#[tokio::test]
async fn transaction_with_throw_and_return() -> Result<(), Error> {
	let sql = "
		BEGIN;
		CREATE person:tobie;
		CREATE person:jaime;
		THROW 'there was an error';
		RETURN { tobie: person:tobie, jaime: person:jaime };
		COMMIT;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"The query was not executed due to a failed transaction"#
	));
	//
	Ok(())
}
