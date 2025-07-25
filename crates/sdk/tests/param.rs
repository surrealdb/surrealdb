mod helpers;
use helpers::new_ds;
use surrealdb::Result;
use surrealdb::dbs::Session;
use surrealdb_core::syn;

#[tokio::test]
async fn define_global_param() -> Result<()> {
	let sql = "
		DEFINE PARAM $test VALUE 12345;
		INFO FOR DB;
		SELECT * FROM $test;
		LET $test = 56789;
		SELECT * FROM $test;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"{
			accesses: {},
			analyzers: {},
			apis: {},
			buckets: {},
			configs: {},
			functions: {},
			models: {},
			params: { test: 'DEFINE PARAM $test VALUE 12345 PERMISSIONS FULL' },
			sequences: {},
			tables: {},
			users: {},
		}",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value("[12345]").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result?;
	let val = syn::value("[56789]").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn define_protected_param() -> Result<()> {
	let sql = "
		LET $test = { some: 'thing', other: true };
		SELECT * FROM $test WHERE some = 'thing';
		LET $auth = { ID: admin:tester };
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				other: true,
				some: 'thing'
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == "'auth' is a protected variable and cannot be set"
	));
	//
	Ok(())
}
