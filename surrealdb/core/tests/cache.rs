mod helpers;
use anyhow::Result;
use helpers::new_ds;
use surrealdb_core::dbs::Session;
use surrealdb_core::syn;
use surrealdb_types::Value;

#[tokio::test]
async fn clear_transaction_cache_table() -> Result<()> {
	let sql = "
		USE NS test DB test;
		BEGIN;
		CREATE person:one CONTENT { x: 0 };
		SELECT * FROM person;
		DEFINE TABLE other AS SELECT * FROM person;
		COMMIT;
		SELECT * FROM other;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 7);

	// USE NS test DB test;
	let tmp = res.remove(0).result;
	tmp.unwrap();
	// BEGIN;
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: person:one,
				x: 0
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: person:one,
				x: 0
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	// COMMIT;
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: other:one,
				x: 0
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn clear_transaction_cache_field() -> Result<()> {
	let sql = "
		DEFINE FIELD test ON person TYPE option<string> VALUE 'test';
		BEGIN;
		UPSERT person:one CONTENT { x: 0 };
		SELECT * FROM person;
		REMOVE FIELD test ON person;
		UPSERT person:two CONTENT { x: 0 };
		SELECT * FROM person;
		COMMIT;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 8);
	// DEFINE FIELD ...
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok(), "{:?}", tmp.err());
	// BEGIN;
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	// UPSERT person:one CONTENT { x: 0 };
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: person:one,
				test: 'test',
				x: 0
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	// SELECT * FROM person;
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: person:one,
				test: 'test',
				x: 0
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	// REMOVE FIELD test ON person;
	let tmp = res.remove(0).result;
	tmp.unwrap();
	// UPSERT person:two CONTENT { x: 0 };
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: person:two,
				x: 0
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	// SELECT * FROM person;
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: person:one,
				test: 'test',
				x: 0
			},
			{
				id: person:two,
				x: 0
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	// COMMIT;
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	Ok(())
}
