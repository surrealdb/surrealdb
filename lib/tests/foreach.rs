mod parse;
use parse::Parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;
use surrealdb::sql::Value;

#[tokio::test]
async fn foreach() -> Result<(), Error> {
	let sql = "
		FOR $test in [1, 2, 3] {
			IF $test == 2 {
				BREAK;
			};
			UPDATE type::thing('person', $test) SET test = $test;
		};
		SELECT * FROM person;
		FOR $test in [4, 5, 6] {
			IF $test >= 5 {
				CONTINUE;
			};
			UPDATE type::thing('person', $test) SET test = $test;
		};
		SELECT * FROM person;
		FOR $test in [7, 8, 9] {
			IF $test > 8 {
				THROW 'This is an error';
			};
			UPDATE type::thing('person', $test) SET test = $test;
		};
		SELECT * FROM person;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 6);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:1,
				test: 1,
			},
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:1,
				test: 1,
			},
			{
				id: person:4,
				test: 4,
			},
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_err());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:1,
				test: 1,
			},
			{
				id: person:4,
				test: 4,
			},
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}
