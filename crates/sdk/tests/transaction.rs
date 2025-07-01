mod parse;
use parse::Parse;
mod helpers;
use helpers::new_ds;
use surrealdb::Result;
use surrealdb::dbs::Session;
use surrealdb::sql::SqlValue;

#[tokio::test]
async fn transaction_basic() -> Result<()> {
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
	let tmp = res.remove(0).values?;
	let val = SqlValue::parse(
		"[
			{
				id: person:tobie,
			}
		]",
	)
	.into();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).values?;
	let val = SqlValue::parse(
		"[
			{
				id: person:jaime,
			}
		]",
	)
	.into();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn transaction_with_return() -> Result<()> {
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
	let tmp = res.remove(0).values?;
	let val = SqlValue::parse(
		"{
			tobie: person:tobie,
			jaime: person:jaime,
		}",
	)
	.into();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn transaction_with_failure() -> Result<()> {
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
	let tmp = res.remove(0).values;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"The query was not executed due to a failed transaction"#
	));
	//
	let tmp = res.remove(0).values;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"The query was not executed due to a failed transaction"#
	));
	//
	let tmp = res.remove(0).values;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Database record `person:tobie` already exists"#
	));
	//
	Ok(())
}

#[tokio::test]
async fn transaction_with_failure_and_return() -> Result<()> {
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
	let tmp = res.remove(0).values;
	assert_eq!(
		tmp.err().map(|x| x.to_string()),
		Some(r#"The query was not executed due to a failed transaction"#.to_string())
	);
	let tmp = res.remove(0).values;
	assert_eq!(
		tmp.err().map(|x| x.to_string()),
		Some(r#"The query was not executed due to a failed transaction"#.to_string())
	);
	let tmp = res.remove(0).values;
	assert_eq!(
		tmp.err().map(|x| x.to_string()),
		Some(r#"Database record `person:tobie` already exists"#.to_string())
	);
	let tmp = res.remove(0).values;
	assert_eq!(
		tmp.err().map(|x| x.to_string()),
		Some(r#"The query was not executed due to a failed transaction"#.to_string())
	);
	//
	Ok(())
}

#[tokio::test]
async fn transaction_with_throw() -> Result<()> {
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
	let tmp = res.remove(0).values;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"The query was not executed due to a failed transaction"#
	));
	//
	let tmp = res.remove(0).values;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"The query was not executed due to a failed transaction"#
	));
	//
	let tmp = res.remove(0).values;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"An error occurred: there was an error"#
	));
	//
	Ok(())
}

#[tokio::test]
async fn transaction_with_throw_and_return() -> Result<()> {
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
	let tmp = res.remove(0).values;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"The query was not executed due to a failed transaction"#
	));
	//
	Ok(())
}
