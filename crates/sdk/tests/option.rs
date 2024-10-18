mod parse;
use parse::Parse;
mod helpers;
use helpers::new_ds;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::sql::Value;

#[tokio::test]
async fn option_import_indexes_should_be_populated() -> Result<(), Error> {
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
	let val = Value::parse(
		"{
			id: test:1,
			num: 123
		}",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: test:1,
				num: 123
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
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
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}
