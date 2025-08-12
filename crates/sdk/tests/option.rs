mod helpers;
use helpers::new_ds;
use surrealdb::Result;
use surrealdb_core::dbs::Session;
use surrealdb_core::syn;

#[tokio::test]
async fn option_import_indexes_should_be_populated() -> Result<()> {
	let sql = "
		OPTION IMPORT;
		DEFINE INDEX field_num ON test FIELDS num;
		CREATE ONLY test:1 SET num = 123;
		SELECT * FROM test WHERE num = 123;
		SELECT * FROM test WHERE num = 123 EXPLAIN;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	//
	// OPTION IMPORT does not count as a result
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok(), "{:?}", tmp.err());
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"{
			id: test:1,
			num: 123
		}",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: test:1,
				num: 123
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
				detail: {
					plan: {
						index: 'field_num',
						operator: '=',
						value: 123
					},
					table: 'test'
				},
				operation: 'Iterate Index'
			},
			{
				detail: {
					type: 'Memory'
				},
				operation: 'Collector'
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}
