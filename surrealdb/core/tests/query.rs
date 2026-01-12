mod helpers;
use anyhow::Result;
use helpers::new_ds;
use surrealdb_core::dbs::Session;
use surrealdb_core::syn;
use surrealdb_types::Value;

#[tokio::test]
async fn query_basic() -> Result<()> {
	let sql = "
		LET $test = 'Tobie';
		SELECT * FROM $test;
		RETURN $test;
		$test;
	";
	let dbs = new_ds("test", "test").await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value("['Tobie']").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from_t("Tobie".to_owned());
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from_t("Tobie".to_owned());
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
	let dbs = new_ds("test", "test").await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value("[45062]").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from_int(45062);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from_int(45062);
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
	let dbs = new_ds("test", "test").await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from_t("THIS IS A TEST".to_owned());
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from_t("this is a test".to_owned());
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from_t("this-is-a-test".to_owned());
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
		person:tobie->knows->person.name;
	";
	let dbs = new_ds("test", "test").await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: person:tobie,
				name: 'Tobie'
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
				id: person:jaime,
				name: 'Jaime'
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
				id: knows:test,
				in: person:tobie,
				out: person:jaime,
				brother: true,
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value("['Jaime']").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}
