mod parse;
use parse::Parse;
mod helpers;
use crate::helpers::skip_ok;
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
				math::mean(score) AS average,
				math::max(score) AS max,
				math::min(score) AS min
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
			tables: { person_by_age: 'DEFINE TABLE person_by_age TYPE ANY SCHEMALESS AS SELECT count(), age, math::sum(age) AS total, math::mean(score) AS average, math::max(score) AS max, math::min(score) AS min FROM person GROUP BY age PERMISSIONS NONE' },
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
				max: 70,
				min: 70,
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
				max: 80,
				min: 70,
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
				max: 90,
				min: 70,
				total: 78
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn define_foreign_table_no_doubles() -> Result<(), Error> {
	// From: https://github.com/surrealdb/surrealdb/issues/3556
	let sql = "
		CREATE happy:1 SET year=2024, month=1, day=1;
		CREATE happy:2 SET year=2024, month=1, day=1;
		CREATE happy:3 SET year=2024, month=1, day=1;
		DEFINE TABLE monthly AS SELECT count() as activeRounds, year, month FROM happy GROUP BY year, month;
		DEFINE TABLE daily AS SELECT count() as activeRounds, year, month, day FROM happy GROUP BY year, month, day;
		SELECT * FROM monthly;
		SELECT * FROM daily;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 7);
	//
	skip_ok(res, 5)?;
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: monthly:[2024, 1],
				activeRounds: 3,
				year: 2024,
				month: 1,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: daily:[2024, 1, 1],
				activeRounds: 3,
				year: 2024,
				month: 1,
				day: 1,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn define_foreign_table_with_non_truthy_condition() -> Result<(), Error> {
	// From https://github.com/surrealdb/surrealdb/issues/3968
	// From https://github.com/surrealdb/surrealdb/issues/3966
	let sql = "
		UPDATE wallet:1 CONTENT { value: 10, day: 1 };
		UPDATE wallet:4 CONTENT {
    		day: 1,
		};
		UPDATE wallet:5 CONTENT { value: 30, day: 1 };
		DEFINE TABLE IF NOT EXISTS wallet_mean AS
				SELECT math::mean(value) as mean, day FROM wallet WHERE value IS NOT NONE GROUP BY day;
		UPDATE wallet:10 CONTENT { value: 20, day: 1 };
		SELECT * FROM wallet_mean;
		UPDATE wallet:5 CONTENT {
    		day: 1,
		};
		SELECT * FROM wallet_mean;
		DELETE wallet:10;
		SELECT * FROM wallet_mean;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 10);
	//
	skip_ok(res, 5)?;
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
				{
					day: 1,
					id: wallet_mean:[
						1
					],
					mean: 20
				}
			]",
	);
	assert_eq!(format!("{tmp:#}"), format!("{val:#}"));
	//
	skip_ok(res, 1)?;
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
				{
					day: 1,
					id: wallet_mean:[
						1
					],
					mean: 15
				}
			]",
	);
	assert_eq!(format!("{tmp:#}"), format!("{val:#}"));
	//
	skip_ok(res, 1)?;
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
				{
					day: 1,
					id: wallet_mean:[
						1
					],
					mean: 10
				}
			]",
	);
	assert_eq!(format!("{tmp:#}"), format!("{val:#}"));
	Ok(())
}
