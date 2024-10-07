mod helpers;
use helpers::new_ds;
use surrealdb::dbs::Session;
use surrealdb::err::Error;

#[tokio::test]
async fn throw_basic() -> Result<(), Error> {
	let sql = "
		THROW 'there was an error';
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
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
async fn throw_param() -> Result<(), Error> {
	let sql = "
		LET $err = 'there was an error';
		THROW $err;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
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
async fn throw_value() -> Result<(), Error> {
	let sql = "
		LET $err = string::concat('found unexpected value: ', {
			test: true
		});
		THROW $err;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"An error occurred: found unexpected value: { test: true }"#
	));

	Ok(())
}
