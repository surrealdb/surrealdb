mod parse;
use parse::Parse;
mod helpers;
use helpers::new_ds;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::sql::Value;

#[tokio::test]
async fn define_foreign_table() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE person SCHEMALESS;
		DEFINE TABLE person_by_age AS
			SELECT
				count(),
				age,
				math::sum(age) AS total,
				math::mean(score) AS average
			FROM person
			GROUP BY age
		;
		INFO FOR TABLE person;
		UPDATE person:one SET age = 39, score = 70;
		SELECT * FROM person_by_age;
		UPDATE person:two SET age = 39, score = 80;
		SELECT * FROM person_by_age;
		UPDATE person:two SET age = 39, score = 90;
		SELECT * FROM person_by_age;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 9);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			events: {},
			fields: {},
			tables: { person_by_age: 'DEFINE TABLE person_by_age SCHEMALESS AS SELECT count(), age, math::sum(age) AS total, math::mean(score) AS average FROM person GROUP BY age' },
			indexes: {},
			lives: {},
		}",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: person:one, age: 39, score: 70 }]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				age: 39,
				average: 70,
				count: 1,
				id: person_by_age:[39],
				total: 39
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: person:two, age: 39, score: 80 }]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				age: 39,
				average: 75,
				count: 2,
				id: person_by_age:[39],
				total: 78
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: person:two, age: 39, score: 90 }]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				age: 39,
				average: 80,
				count: 2,
				id: person_by_age:[39],
				total: 78
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}
