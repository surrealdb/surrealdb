mod parse;
use parse::Parse;
mod helpers;
use helpers::new_ds;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::sql::Value;

#[tokio::test]
async fn foreach_simple() -> Result<(), Error> {
	let sql = "
		FOR $test IN [1, 2, 3] {
			IF $test == 2 {
				BREAK;
			};
			UPSERT type::thing('person', $test) SET test = $test;
		};
		SELECT * FROM person;
		FOR $test IN [4, 5, 6] {
			IF $test == 5 {
				CONTINUE;
			};
			UPSERT type::thing('person', $test) SET test = $test;
		};
		SELECT * FROM person;
		FOR $test IN <future> { [7, 8, 9] } {
			IF $test > 8 {
				THROW 'This is an error';
			};
			UPSERT type::thing('person', $test) SET test = $test;
		};
		SELECT * FROM person;
	";
	let dbs = new_ds().await?;
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
			{
				id: person:6,
				test: 6,
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
			{
				id: person:6,
				test: 6,
			},
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}
