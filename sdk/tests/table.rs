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
		UPSERT person:one SET age = 39, score = 72;
		SELECT * FROM person_by_age;
		UPSERT person:two SET age = 39, score = 83;
		SELECT * FROM person_by_age;
		UPSERT person:two SET age = 39, score = 91;
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
	let val = Value::parse(
		"[
			{
				age: 39,
				id: person:one,
				score: 72,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				age: 39,
				average: 72,
				count: 1,
				id: person_by_age:[39],
				max: 72,
				min: 72,
				total: 39
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				age: 39,
				id: person:two,
				score: 83,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				age: 39,
				average: 77.5,
				count: 2,
				id: person_by_age:[39],
				max: 83,
				min: 72,
				total: 78
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				age: 39,
				id: person:two,
				score: 91,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				age: 39,
				average: 81.5,
				count: 2,
				id: person_by_age:[39],
				max: 91,
				min: 72,
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

async fn define_foreign_table_group(cond: bool, agr: &str) -> Result<(), Error> {
	let cond = if cond {
		"WHERE value >= 5"
	} else {
		""
	};
	let sql = format!(
		"
		UPDATE wallet:1 CONTENT {{ value: 20.0, day: 1 }} RETURN NONE;
		UPDATE wallet:2 CONTENT {{ value: 5.0, day: 1 }} RETURN NONE;
		// 0
		DEFINE TABLE wallet_agr AS SELECT {agr} as agr, day FROM wallet {cond} GROUP BY day;
		SELECT {agr} as agr, day FROM wallet {cond} GROUP BY day;
		SELECT agr, day FROM wallet_agr;
		// 1
		UPDATE wallet:1 CONTENT {{ value: 10.0, day: 1 }} RETURN NONE;
		SELECT {agr} as agr, day FROM wallet {cond} GROUP BY day;
		SELECT agr, day FROM wallet_agr;
		// 2
		UPDATE wallet:2 CONTENT {{ value: 15.0, day: 1 }} RETURN NONE;
		SELECT {agr} as agr, day FROM wallet {cond} GROUP BY day;
		SELECT agr, day FROM wallet_agr;
		// 3
		UPDATE wallet:3 CONTENT {{ value: 10.0, day: 2 }} RETURN NONE;
		SELECT {agr} as agr, day FROM wallet {cond} GROUP BY day;
		SELECT agr, day FROM wallet_agr;
		// 4
		UPDATE wallet:4 CONTENT {{ value: 5.0, day: 2 }} RETURN NONE;
		SELECT {agr} as agr, day FROM wallet {cond} GROUP BY day;
		SELECT agr, day FROM wallet_agr;
		// 5
		UPDATE wallet:2 SET value = 3.0 RETURN NONE;
		SELECT {agr} as agr, day FROM wallet {cond} GROUP BY day;
		SELECT agr, day FROM wallet_agr;
		// 6
		UPDATE wallet:4 SET day = 3 RETURN NONE;
		SELECT {agr} as agr, day FROM wallet {cond} GROUP BY day;
		SELECT agr, day FROM wallet_agr;
		// 7
		DELETE wallet:2;
		SELECT {agr} as agr, day FROM wallet {cond} GROUP BY day;
		SELECT agr, day FROM wallet_agr;
		// 8
		DELETE wallet:3;
		SELECT {agr} as agr, day FROM wallet {cond} GROUP BY day;
		SELECT agr, day FROM wallet_agr;
	"
	);
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None).await?;
	assert_eq!(res.len(), 29);
	//
	skip_ok(res, 2)?;
	//
	for i in 0..9 {
		// Skip the UPDATE or DELETE statement
		skip_ok(res, 1)?;
		// Get the computed result
		let comp = res.remove(0).result?;
		// Get the projected result
		let proj = res.remove(0).result?;
		// Check they are similar
		assert_eq!(format!("{proj:#}"), format!("{comp:#}"), "#{i}");
	}
	//
	Ok(())
}

#[tokio::test]
async fn define_foreign_table_with_cond_group_mean() -> Result<(), Error> {
	define_foreign_table_group(true, "math::mean(value)").await
}

#[tokio::test]
async fn define_foreign_table_with_with_cond_group_count() -> Result<(), Error> {
	define_foreign_table_group(true, "count()").await
}

#[tokio::test]
async fn define_foreign_table_with_cond_group_min() -> Result<(), Error> {
	define_foreign_table_group(true, "math::min(value)").await
}

#[tokio::test]
async fn define_foreign_table_with_cond_group_max() -> Result<(), Error> {
	define_foreign_table_group(true, "math::max(value)").await
}

#[tokio::test]
async fn define_foreign_table_with_cond_group_sum() -> Result<(), Error> {
	define_foreign_table_group(true, "math::sum(value)").await
}

#[tokio::test]
async fn define_foreign_table_with_no_cond_and_group_mean() -> Result<(), Error> {
	define_foreign_table_group(false, "math::mean(value)").await
}

#[tokio::test]
async fn define_foreign_table_with_no_cond_and_group_count() -> Result<(), Error> {
	define_foreign_table_group(false, "count()").await
}

#[tokio::test]
async fn define_foreign_table_with_no_cond_and_group_min() -> Result<(), Error> {
	define_foreign_table_group(false, "math::min(value)").await
}

#[tokio::test]
async fn define_foreign_table_with_no_cond_and_group_max() -> Result<(), Error> {
	define_foreign_table_group(false, "math::max(value)").await
}

#[tokio::test]
async fn define_foreign_table_with_no_cond_and_group_sum() -> Result<(), Error> {
	define_foreign_table_group(false, "math::sum(value)").await
}
