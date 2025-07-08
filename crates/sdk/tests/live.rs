mod helpers;
mod parse;
use anyhow::Result;
use helpers::new_ds;
use helpers::skip_ok;
use parse::Parse;
use surrealdb::dbs::Session;
use surrealdb::expr::Thing;
use surrealdb::sql::SqlValue;

#[tokio::test]
async fn live_permissions() -> Result<()> {
	let dbs = new_ds().await?.with_auth_enabled(true).with_notifications();

	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
	let sql = "
			DEFINE TABLE test SCHEMAFULL PERMISSIONS
				FOR create WHERE { THROW 'create' }
				FOR select WHERE { THROW 'select' }
				FOR update WHERE { THROW 'update' }
				FOR delete WHERE { THROW 'delete' };
			CREATE test:1;
		";
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	skip_ok(res, 1)?;
	//
	let tmp = res.remove(0).result?;
	let val = SqlValue::parse(
		"[
			{
				id: test:1,
			},
		]",
	)
	.into();
	assert_eq!(tmp, val);
	//
	let ses = Session::for_record("test", "test", "test", Thing::from(("user", "test")).into())
		.with_rt(true);
	let sql = "
		LIVE SELECT * FROM test;
		CREATE test:2;
	";
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	skip_ok(res, 1)?;
	//
	let tmp = res.remove(0).result.unwrap_err().to_string();
	let val = "An error occurred: create".to_string();
	assert_eq!(tmp, val);
	//
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
	let sql = "CREATE test:3;";
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = SqlValue::parse(
		"[
			{
				id: test:3,
			},
		]",
	)
	.into();
	assert_eq!(tmp, val);
	//
	Ok(())
}
