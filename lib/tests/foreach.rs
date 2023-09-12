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
			UPDATE type::thing('person', $test) SET test = $test;
		};
		SELECT * FROM person;
		FOR $test IN [4, 5, 6] {
			IF $test == 5 {
				CONTINUE;
			};
			UPDATE type::thing('person', $test) SET test = $test;
		};
		SELECT * FROM person;
		FOR $test IN <future> { [7, 8, 9] } {
			IF $test > 8 {
				THROW 'This is an error';
			};
			UPDATE type::thing('person', $test) SET test = $test;
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

#[tokio::test]
async fn foreach_nested() -> Result<(), Error> {
	let sql = "
		FOR $i IN [1,2,3,4,5] {
			FOR $j IN [6,7,8,9,0] {
				CREATE type::thing('person', [$i, $j]);
			}
		};
		SELECT * FROM person;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:[1, 0]
			},
			{
				id: person:[1, 6]
			},
			{
				id: person:[1, 7]
			},
			{
				id: person:[1, 8]
			},
			{
				id: person:[1, 9]
			},
			{
				id: person:[2, 0]
			},
			{
				id: person:[2, 6]
			},
			{
				id: person:[2, 7]
			},
			{
				id: person:[2, 8]
			},
			{
				id: person:[2, 9]
			},
			{
				id: person:[3, 0]
			},
			{
				id: person:[3, 6]
			},
			{
				id: person:[3, 7]
			},
			{
				id: person:[3, 8]
			},
			{
				id: person:[3, 9]
			},
			{
				id: person:[4, 0]
			},
			{
				id: person:[4, 6]
			},
			{
				id: person:[4, 7]
			},
			{
				id: person:[4, 8]
			},
			{
				id: person:[4, 9]
			},
			{
				id: person:[5, 0]
			},
			{
				id: person:[5, 6]
			},
			{
				id: person:[5, 7]
			},
			{
				id: person:[5, 8]
			},
			{
				id: person:[5, 9]
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}
