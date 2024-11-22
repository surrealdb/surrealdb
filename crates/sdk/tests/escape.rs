mod parse;
use parse::Parse;
mod helpers;
use helpers::new_ds;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::sql::Value;

#[tokio::test]
async fn complex_ids() -> Result<(), Error> {
	let sql = r#"
		CREATE person:100 SET test = 'One';
		CREATE person:00100;
		CREATE r'person:100';
		CREATE r"person:100";
		CREATE person:⟨100⟩ SET test = 'Two';
		CREATE person:`100`;
		SELECT * FROM person;
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 7);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:100,
				test: 'One'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Database record `person:100` already exists"#
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Database record `person:100` already exists"#
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Database record `person:100` already exists"#
	));
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:⟨100⟩,
				test: 'Two'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Database record `person:⟨100⟩` already exists"#
	));
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:100,
				test: 'One'
			},
			{
				id: person:⟨100⟩,
				test: 'Two'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn complex_strings() -> Result<(), Error> {
	let sql = r#"
		RETURN 'String with no complex characters';
		RETURN 'String with some "double quoted" characters';
		RETURN 'String with some \'escaped single quoted\' characters';
		RETURN "String with some \"escaped double quoted\" characters";
		RETURN "String with some 'single' and \"double\" quoted characters";
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(r#"'String with no complex characters'"#);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(r#"'String with some "double quoted" characters'"#);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(r#""String with some 'escaped single quoted' characters""#);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(r#"'String with some "escaped double quoted" characters'"#);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(r#""String with some 'single' and \"double\" quoted characters""#);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn object_keys() -> Result<(), Error> {
	let sql = r#"
		RETURN object::from_entries([ ["3ds", 1 ] ])
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?.to_string();
	assert_eq!(tmp, "{ \"3ds\": 1 }");
	Ok(())
}
