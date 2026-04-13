mod helpers;
use anyhow::Result;
use helpers::new_ds;
use surrealdb_core::dbs::Session;

use crate::helpers::skip_ok;

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
	let (_, dbs) = new_ds("test", "test", false).await?;
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
