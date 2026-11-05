use anyhow::Result;
use surrealdb_core::dbs::Session;
use surrealdb_core::syn;

use crate::helpers::new_ds;

#[tokio::test]
async fn define_global_param() -> Result<()> {
	let sql = "
		DEFINE PARAM $test VALUE 12345;
		INFO FOR DB;
		SELECT * FROM $test;
		LET $test = 56789;
		SELECT * FROM $test;
	";
	let (_, dbs) = new_ds("test", "test", true).await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"{
			accesses: {},
			analyzers: {},
			apis: {},
			buckets: {},
			compatibility: 4,
			configs: {},
			extensions: {},
			functions: {},
			models: {},
			params: { test: 'DEFINE PARAM $test VALUE 12345 PERMISSIONS FULL' },
			sequences: {},
			tables: {},
			types: {},
			users: {},
		}",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value("[12345]").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result?;
	let val = syn::value("[56789]").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn define_protected_param() -> Result<()> {
	let sql = "
		USE NS test DB test;
		LET $test = { some: 'thing', other: true };
		SELECT * FROM $test WHERE some = 'thing';
		LET $auth = { ID: admin:tester };
	";
	let (_, dbs) = new_ds("test", "test", true).await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	// USE NS test DB test;
	let tmp = res.remove(0).result;
	tmp.unwrap();
	// LET $test = { some: 'thing', other: true };
	let tmp = res.remove(0).result;
	tmp.unwrap();
	// SELECT * FROM $test WHERE some = 'thing';
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				other: true,
				some: 'thing'
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	// LET $auth = { ID: admin:tester };
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == "'auth' is a protected variable and cannot be set"
	));
	//
	Ok(())
}

/// A mutation whose `WHERE` bound is a client bind parameter reads the index
/// a literal bound reads, and reaches the records a forced table scan reaches.
#[tokio::test]
async fn a_mutation_bound_by_a_client_parameter_reads_the_index() -> Result<()> {
	use surrealdb_types::{Value, Variables};

	let (_, ds) = new_ds("test", "test", true).await?;
	let session = Session::owner().with_ns("test").with_db("test");
	let run = |sql: &'static str, cut: Option<&'static str>| {
		let ds = &ds;
		let session = &session;
		async move {
			let vars = cut.map(|cut| {
				let mut vars = Variables::new();
				vars.insert("cut", Value::Datetime(cut.parse().expect("datetime")));
				vars
			});
			let mut responses = ds.execute(sql, session, vars).await.expect("executes");
			responses.pop().expect("a response").result.unwrap_or_else(|e| panic!("`{sql}`: {e}"))
		}
	};
	run(
		"DEFINE TABLE plain SCHEMALESS;
		DEFINE FIELD time ON plain TYPE datetime;
		DEFINE INDEX by_time ON plain FIELDS time;
		CREATE plain:1 SET time = d'2025-01-01T00:00:00Z';
		CREATE plain:2 SET time = d'2025-03-01T00:00:00Z';
		CREATE plain:3 SET time = d'2025-09-01T00:00:00Z';",
		None,
	)
	.await;
	let cut = Some("2025-06-01T00:00:00Z");
	for sql in [
		"EXPLAIN DELETE plain WHERE time < $cut",
		"EXPLAIN UPDATE plain SET x = 1 WHERE time < $cut",
		"EXPLAIN UPSERT plain SET x = 1 WHERE time < $cut",
	] {
		let Value::String(plan) = run(sql, cut).await else {
			panic!("`{sql}` renders no text");
		};
		assert!(plan.contains("IndexScan"), "`{sql}` renders\n{plan}");
		assert!(plan.contains("index: by_time"), "`{sql}` renders\n{plan}");
		assert!(!plan.contains("TableScan"), "`{sql}` renders\n{plan}");
	}
	let indexed = run("UPDATE plain SET hit = true WHERE time < $cut RETURN VALUE id", cut).await;
	let scanned =
		run("UPDATE plain WITH NOINDEX SET hit = true WHERE time < $cut RETURN VALUE id", cut)
			.await;
	assert_eq!(indexed, scanned);
	let deleted = run("DELETE plain WHERE time < $cut RETURN VALUE $before.id", cut).await;
	assert_eq!(deleted, scanned);
	let left = run("SELECT VALUE record::id(id) FROM plain", None).await;
	assert_eq!(left, Value::from_t(vec![3_i64]));
	Ok(())
}
