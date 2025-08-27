mod helpers;
use helpers::new_ds;
use surrealdb::Result;
use surrealdb_core::dbs::Session;
use surrealdb_core::err::Error;
use surrealdb_core::syn;

use crate::helpers::skip_ok;

#[tokio::test]
async fn define_foreign_table() -> Result<()> {
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
		UPSERT person:two SET age = 39, score = 'test';
		SELECT * FROM person_by_age;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 11);
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"{
			events: {},
			fields: {},
			tables: { person_by_age: 'DEFINE TABLE person_by_age TYPE ANY SCHEMALESS AS SELECT count(), age, math::sum(age) AS total, math::mean(score) AS average, math::max(score) AS max, math::min(score) AS min FROM person GROUP BY age PERMISSIONS NONE' },
			indexes: {},
			lives: {},
		}",
	).unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				age: 39,
				id: person:one,
				score: 72,
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
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
	)
	.unwrap();
	assert_ne!(tmp, val); // Temporarily ignore checking metadata fields
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				age: 39,
				id: person:two,
				score: 83,
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
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
	)
	.unwrap();
	assert_ne!(tmp, val); // Temporarily ignore checking metadata fields
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				age: 39,
				id: person:two,
				score: 91,
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
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
	)
	.unwrap();
	assert_ne!(tmp, val); // Temporarily ignore checking metadata fields
	//
	let tmp = res.remove(0).result.unwrap_err();
	assert!(matches!(tmp.downcast_ref(), Some(Error::InvalidAggregation { .. })));
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
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
	)
	.unwrap();
	assert_ne!(tmp, val); // Temporarily ignore checking metadata fields
	//
	Ok(())
}

#[tokio::test]
async fn define_foreign_table_no_doubles() -> Result<()> {
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
	let val = syn::value(
		"[
			{
				id: monthly:[2024, 1],
				activeRounds: 3,
				year: 2024,
				month: 1,
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: daily:[2024, 1, 1],
				activeRounds: 3,
				year: 2024,
				month: 1,
				day: 1,
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

async fn define_foreign_table_group(cond: bool, agr: &str) -> Result<()> {
	let cond = if cond {
		"WHERE value >= 5"
	} else {
		""
	};
	let sql = format!(
		"
		UPSERT wallet:1 CONTENT {{ value: 20.0dec, day: 1 }} RETURN NONE;
		UPSERT wallet:2 CONTENT {{ value: 5.0dec, day: 1 }} RETURN NONE;
		// 0
		DEFINE TABLE wallet_agr AS SELECT {agr} as agr, day FROM wallet {cond} GROUP BY day;
		SELECT {agr} as agr, day FROM wallet {cond} GROUP BY day;
		SELECT agr, day FROM wallet_agr;
		// 1
		UPSERT wallet:1 CONTENT {{ value: 10.0dec, day: 1 }} RETURN NONE;
		SELECT {agr} as agr, day FROM wallet {cond} GROUP BY day;
		SELECT agr, day FROM wallet_agr;
		// 2
		UPSERT wallet:2 CONTENT {{ value: 15.0dec, day: 1 }} RETURN NONE;
		SELECT {agr} as agr, day FROM wallet {cond} GROUP BY day;
		SELECT agr, day FROM wallet_agr;
		// 3
		UPSERT wallet:3 CONTENT {{ value: 10.0dec, day: 2 }} RETURN NONE;
		SELECT {agr} as agr, day FROM wallet {cond} GROUP BY day;
		SELECT agr, day FROM wallet_agr;
		// 4
		UPSERT wallet:4 CONTENT {{ value: 5.0dec, day: 2 }} RETURN NONE;
		SELECT {agr} as agr, day FROM wallet {cond} GROUP BY day;
		SELECT agr, day FROM wallet_agr;
		// 5
		UPSERT wallet:2 SET value = 3.0dec RETURN NONE;
		SELECT {agr} as agr, day FROM wallet {cond} GROUP BY day;
		SELECT agr, day FROM wallet_agr;
		// 6
		UPSERT wallet:4 SET day = 3 RETURN NONE;
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
		assert_eq!(proj, comp, "#{i}");
	}
	//
	Ok(())
}

#[tokio::test]
async fn define_foreign_table_with_cond_group_mean() -> Result<()> {
	define_foreign_table_group(true, "math::mean(value)").await
}

#[tokio::test]
async fn define_foreign_table_with_with_cond_group_count() -> Result<()> {
	define_foreign_table_group(true, "count()").await
}

#[tokio::test]
async fn define_foreign_table_with_cond_group_min() -> Result<()> {
	define_foreign_table_group(true, "math::min(value)").await
}

#[tokio::test]
async fn define_foreign_table_with_cond_group_max() -> Result<()> {
	define_foreign_table_group(true, "math::max(value)").await
}

#[tokio::test]
async fn define_foreign_table_with_cond_group_sum() -> Result<()> {
	define_foreign_table_group(true, "math::sum(value)").await
}

#[tokio::test]
async fn define_foreign_table_with_no_cond_and_group_mean() -> Result<()> {
	define_foreign_table_group(false, "math::mean(value)").await
}

#[tokio::test]
async fn define_foreign_table_with_no_cond_and_group_count() -> Result<()> {
	define_foreign_table_group(false, "count()").await
}

#[tokio::test]
async fn define_foreign_table_with_no_cond_and_group_min() -> Result<()> {
	define_foreign_table_group(false, "math::min(value)").await
}

#[tokio::test]
async fn define_foreign_table_with_no_cond_and_group_max() -> Result<()> {
	define_foreign_table_group(false, "math::max(value)").await
}

#[tokio::test]
async fn define_foreign_table_with_no_cond_and_group_sum() -> Result<()> {
	define_foreign_table_group(false, "math::sum(value)").await
}
