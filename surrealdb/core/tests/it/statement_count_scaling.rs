//! The time a request takes grows linearly with its statement count, however
//! many variables it holds: a statement never pays for variables it does not
//! read.
//!
//! These compare wall-clock times, so they are ignored by default and run by
//! hand (`-- --ignored statement_count_scaling`). The deterministic guards are
//! the unit tests of the shared values (`ctx::context`) and of the bound
//! context (`exec::vm::ir_compile`).

use std::time::{Duration, Instant};

use anyhow::Result;
use surrealdb_core::dbs::Session;
use surrealdb_types::Variables as PublicVariables;

use crate::helpers::new_ds;

/// How many times slower the largest request may run than the smallest,
/// relative to its size: linear growth gives 1, quadratic growth gives the
/// size ratio.
const SIZE_RATIO: u32 = 4;
const LIMIT: f64 = 2.5;

async fn timed(sql: String, vars: Option<PublicVariables>) -> Result<Duration> {
	let (_, dbs) = new_ds("test", "test", true).await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = dbs.execute("DEFINE FUNCTION fn::id($v: int) { RETURN $v }", &ses, None).await?;
	for r in res {
		r.result?;
	}
	let start = Instant::now();
	for r in dbs.execute(&sql, &ses, vars).await? {
		r.result?;
	}
	Ok(start.elapsed())
}

fn bound(n: usize) -> (String, Option<PublicVariables>) {
	let mut sql = String::from("BEGIN;\n");
	let mut vars = PublicVariables::new();
	for i in 0..n {
		sql.push_str(&format!("RETURN fn::id($id{i} + $v{i});\n"));
		vars.insert(format!("id{i}"), i as i64);
		vars.insert(format!("v{i}"), i as i64);
	}
	sql.push_str("COMMIT;");
	(sql, Some(vars))
}

/// Bound variables read by `LET`s, computed and constant, between the statements.
fn lets_over_bound(n: usize) -> (String, Option<PublicVariables>) {
	let mut sql = String::from("BEGIN;\n");
	let mut vars = PublicVariables::new();
	for i in 0..n {
		sql.push_str(&format!("LET $r{i} = fn::id($id{i} + $v{i});\n"));
		sql.push_str(&format!("LET $k{i} = {i};\n"));
		sql.push_str(&format!("RETURN fn::id($k{i});\n"));
		vars.insert(format!("id{i}"), i as i64);
		vars.insert(format!("v{i}"), i as i64);
	}
	sql.push_str("COMMIT;");
	(sql, Some(vars))
}

async fn assert_linear(make: fn(usize) -> (String, Option<PublicVariables>)) -> Result<()> {
	let small = 1000;
	let large = small * SIZE_RATIO as usize;
	// The best of three runs of each size keeps a scheduling hiccup out of the ratio.
	let mut a = Duration::MAX;
	let mut b = Duration::MAX;
	for _ in 0..3 {
		let (sql, vars) = make(small);
		a = a.min(timed(sql, vars).await?);
		let (sql, vars) = make(large);
		b = b.min(timed(sql, vars).await?);
	}
	let growth = b.as_secs_f64() / a.as_secs_f64() / f64::from(SIZE_RATIO);
	eprintln!("{small}: {a:?}, {large}: {b:?}, growth over linear: {growth:.2}");
	assert!(growth < LIMIT, "{small} statements took {a:?} but {large} took {b:?}");
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "compares wall-clock times; run by hand"]
async fn statements_over_bound_variables_scale_linearly() -> Result<()> {
	assert_linear(bound).await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "compares wall-clock times; run by hand"]
async fn lets_over_bound_variables_scale_linearly() -> Result<()> {
	assert_linear(lets_over_bound).await
}
