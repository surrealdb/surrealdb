mod parse;
use parse::Parse;
use surrealdb::sql::Value;
use surrealdb::Datastore;
use surrealdb::Error;
use surrealdb::Session;

#[tokio::test]
async fn complex_ids() -> Result<(), Error> {
	let sql = r#"
		CREATE person:100 SET test = 'One';
		CREATE person:00100;
		CREATE 'person:100';
		CREATE "person:100";
		CREATE person:⟨100⟩ SET test = 'Two';
		CREATE person:`100`;
		SELECT * FROM person;
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
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
