mod parse;
use parse::Parse;
mod helpers;
use helpers::new_ds;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::sql::Value;

#[tokio::test]
async fn cast_string_to_record() -> Result<(), Error> {
	let sql = r#"
		<record> <string> a:1
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("a:1");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn cast_to_record_table() -> Result<(), Error> {
	let sql = r#"
        <record<a>> a:1;
        <record<a>> "a:1";
        <record<b>> a:1;
        <record<b>> "a:1";
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("a:1");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("a:1");
	assert_eq!(tmp, val);
	//
	match res.remove(0).result {
		Err(Error::ConvertTo {
			from,
			into,
		}) if into == "record<b>" && from == Value::parse("a:1") => (),
		_ => panic!("Casting should have failed with error: Expected a record<b> but cannot convert a:1 into a record<b>"),
	}
	//
	match res.remove(0).result {
		Err(Error::ConvertTo {
			from,
			into,
		}) if into == "record<b>" && from == Value::parse("'a:1'") => (),
		_ => panic!("Casting should have failed with error: Expected a record<b> but cannot convert 'a:1' into a record<b>"),
	}
	//
	Ok(())
}

#[tokio::test]
async fn cast_range_to_array() -> Result<(), Error> {
	let sql = r#"
    	<array> 1..5;
    	<array> 1>..5;
    	<array> 1..=5;
    	<array> 1>..=5;
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[1, 2, 3, 4]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[2, 3, 4]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[1, 2, 3, 4, 5]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[2, 3, 4, 5]");
	assert_eq!(tmp, val);
	//
	Ok(())
}
