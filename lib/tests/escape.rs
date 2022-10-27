mod parse;
use parse::Parse;
use surrealdb::sql::Value;
use surrealdb::Datastore;
use surrealdb::Error;
use surrealdb::Session;

#[tokio::test]
async fn complex_string() -> Result<(), Error> {
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
