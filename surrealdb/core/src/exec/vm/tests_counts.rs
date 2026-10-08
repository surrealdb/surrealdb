//! A count source holds plain data and the host reads it through
//! [`super::services::Services`]. Each count answers its table's rows, for the
//! owner and for record users under every kind of table permission; the
//! table's permission is applied once, by the program; a principal of another
//! tenant is refused; a count sees the writes of its own transaction; and
//! running the program compiles nothing.

use std::sync::Arc;

use surrealdb_types::ToSql;

use super::golden::Golden;
use super::program::{CountSource, CountVia, Program, Source};
use super::tests::{bind, outcome, planner};
use crate::exec::ExecutionContext;
use crate::exec::operators::test_util::TestDb;
use crate::val::Value;

/// The tables a count is made against: one without an index, one with an
/// unconditional and a guarded `COUNT` index and two B-tree indexes, one
/// whose permission is conditional on the record's owner, and one denied.
fn tables() -> String {
	let mut setup = String::new();
	for (table, permission, indexes) in [
		("plain", "FULL", ""),
		(
			"counted",
			"FULL",
			"DEFINE INDEX {t}_all ON {t} COUNT;
			 DEFINE INDEX {t}_big ON {t} COUNT WHERE n > 1;
			 DEFINE INDEX {t}_n ON {t} FIELDS n;
			 DEFINE INDEX {t}_m ON {t} FIELDS m;",
		),
		(
			"mine",
			"FOR select WHERE owner = $auth OR owner = $auth.id",
			"DEFINE INDEX {t}_all ON {t} COUNT;
			 DEFINE INDEX {t}_n ON {t} FIELDS n;
			 DEFINE INDEX {t}_m ON {t} FIELDS m;",
		),
		("locked", "FOR select NONE", "DEFINE INDEX {t}_n ON {t} FIELDS n;"),
	] {
		setup.push_str(&format!(
			"DEFINE TABLE {table} SCHEMAFULL PERMISSIONS {permission};
			 DEFINE FIELD n ON {table} TYPE int;
			 DEFINE FIELD m ON {table} TYPE int;
			 DEFINE FIELD owner ON {table} TYPE record<user>;
			 {indexes}
			 FOR $i IN 1..=24 {{
				 CREATE type::record('{table}', $i) SET
					 owner = IF $i % 2 = 0 {{ user:alice }} ELSE {{ user:bob }},
					 n = $i % 4,
					 m = $i % 5;
			 }};\n",
			indexes = indexes.replace("{t}", table),
		));
	}
	setup
}

/// Statements counting table `t`, each answered by a different structure.
const COUNTS: &[&str] = &[
	"SELECT count() FROM t GROUP ALL",
	"SELECT VALUE count() FROM t GROUP ALL",
	"SELECT count() AS c FROM t GROUP ALL",
	"SELECT count() FROM t GROUP ALL LIMIT 0",
	"SELECT count() FROM t GROUP ALL START 1",
	"SELECT count() FROM t:3 GROUP ALL",
	"SELECT count() FROM t:99 GROUP ALL",
	"SELECT count() FROM t:2..=9 GROUP ALL",
	"SELECT count() FROM t:2..9 GROUP ALL",
	"SELECT count() FROM t WHERE n > 1 GROUP ALL",
	"SELECT count() FROM t WHERE n = 1 GROUP ALL",
	"SELECT count() FROM t WHERE n > 0 AND n < 3 GROUP ALL",
	"SELECT count() FROM t WHERE n = 1 OR m = 2 GROUP ALL",
	"SELECT count() FROM t WHERE n = 1 AND m = 2 GROUP ALL",
	"SELECT count() FROM t WHERE n % 2 = 0 GROUP ALL",
	"SELECT VALUE count() FROM t WHERE n % 2 = 0 GROUP ALL",
];

fn session(user: &str) -> crate::dbs::Session {
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", user));
	crate::dbs::Session::for_record("test", "test", "user", rid)
}

/// A record user whose tenant is not the database the data is in.
fn foreign() -> crate::dbs::Session {
	crate::dbs::Session {
		au: Arc::new(crate::iam::Auth::for_record(
			"user:tobie".to_owned(),
			"other",
			"test",
			"user",
		)),
		..session("tobie")
	}
}

/// Run `sql` on the VM over `ctx`, answering its rows and how many
/// compilations running the plan made; `None` where the VM declines the
/// statement.
async fn run(ctx: &ExecutionContext, sql: &str) -> Option<(Result<Vec<Value>, String>, usize)> {
	let bound = bind(ctx, sql).await;
	let op = super::plan(&planner(ctx), &bound).await?;
	let before = crate::exec::compiles::count();
	let rows = outcome(&op, ctx).await;
	Some((rows, crate::exec::compiles::count() - before))
}

/// The program `sql` lowers to for `ctx`.
async fn program_of(ctx: &ExecutionContext, sql: &str) -> Program {
	let bound = bind(ctx, sql).await;
	let planner = planner(ctx);
	let physical =
		super::optimize::optimize(super::map::map(&planner, &bound).await.expect("statement maps"));
	super::lower::lower(&planner, &physical).await.expect("program lowers")
}

/// How `program` counts: the structure each count source reads, and whether
/// an ordinary pipeline stands beside it.
fn counted_by(program: &Program) -> Vec<String> {
	program
		.pipelines
		.iter()
		.filter_map(|pipeline| match &pipeline.source {
			Source::Count {
				count,
				..
			} => Some(match count {
				CountSource::All {
					deltas,
					ordinary,
					..
				} => format!(
					"keys{}{}",
					if deltas.is_some() {
						"+deltas"
					} else {
						""
					},
					if ordinary.is_some() {
						"+ordinary"
					} else {
						""
					}
				),
				CountSource::Indexed {
					via,
					ordinary,
					..
				} => format!(
					"{}{}",
					match via {
						CountVia::Deltas {
							..
						} => "deltas",
						CountVia::BTreeKeys {
							..
						} => "btree",
						CountVia::Bitmap(_) => "bitmap",
					},
					if ordinary.is_some() {
						"+ordinary"
					} else {
						""
					}
				),
			}),
			_ => None,
		})
		.collect()
}

fn queries() -> Vec<String> {
	let mut out = Vec::new();
	for table in ["plain", "counted", "mine", "locked"] {
		out.extend(COUNTS.iter().map(|sql| {
			sql.replace(" t ", &format!(" {table} ")).replace(" t:", &format!(" {table}:"))
		}));
	}
	out.sort();
	out.dedup();
	out
}

/// Every count answers its rows, for the owner and for two record users, and
/// compiles nothing while it runs: the table's permission is the program's,
/// whatever it is.
#[tokio::test]
async fn counts_answer_their_rows_and_compile_nothing() {
	let mut golden = Golden::new("counts_answer_their_rows_and_compile_nothing");
	let db = TestDb::new_with_auth(&tables()).await;
	let mut declined = Vec::new();
	let mut compiled = Vec::new();
	let mut nonzero = 0;
	for (who, session) in
		[("owner", TestDb::owner()), ("alice", session("alice")), ("bob", session("bob"))]
	{
		let ctx = db.exec_ctx_as(&session, crate::kvs::TransactionType::Read).await;
		for sql in queries() {
			let Some((vm, compiles)) = run(&ctx, &sql).await else {
				declined.push(format!("`{sql}` for {who}"));
				continue;
			};
			golden.rows(format!("{who}: {sql}"), &vm);
			if matches!(&vm, Ok(rows) if rows.iter().any(|row| row.to_sql() != "{ count: 0 }" && row.to_sql() != "0"))
			{
				nonzero += 1;
			}
			if compiles > 0 {
				compiled.push(format!("`{sql}` for {who}: {compiles} compilations"));
			}
		}
	}
	assert!(compiled.is_empty(), "{compiled:#?}");
	assert!(declined.is_empty(), "the VM declined {declined:#?}");
	assert!(nonzero > 60, "the counts read too little: {nonzero}");
	golden.check();
}

/// A count of a table that is not defined fails.
#[tokio::test]
async fn a_count_of_a_missing_table_fails() {
	let mut golden = Golden::new("a_count_of_a_missing_table_fails");
	let db = TestDb::new_with_auth(&tables()).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	for sql in [
		"SELECT count() FROM missing GROUP ALL",
		"SELECT count() FROM missing:1 GROUP ALL",
		"SELECT count() FROM missing WHERE n = 1 GROUP ALL",
	] {
		let (vm, _) = run(&ctx, sql).await.expect("the VM covers it");
		assert!(vm.is_err(), "`{sql}`: {vm:?}");
		golden.rows(sql, &vm);
	}
	golden.check();
}

/// The structure a count reads is the one the catalog offers when the program
/// is lowered, and an ordinary pipeline stands beside it only where the
/// shortcut cannot answer alone.
#[tokio::test]
async fn a_count_reads_the_structure_the_catalog_offers() {
	let db = TestDb::new_with_auth(&tables()).await;
	let owner = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	let alice = db.exec_ctx_as(&session("alice"), crate::kvs::TransactionType::Read).await;
	for (ctx, sql, expected) in [
		(&owner, "SELECT count() FROM plain GROUP ALL", "keys"),
		(&owner, "SELECT count() FROM counted GROUP ALL", "keys+deltas"),
		(&owner, "SELECT count() FROM counted:2..=9 GROUP ALL", "keys+deltas"),
		(&owner, "SELECT count() FROM counted WHERE n > 1 GROUP ALL", "deltas"),
		(&owner, "SELECT count() FROM counted WHERE n = 1 GROUP ALL", "btree"),
		(&owner, "SELECT count() FROM counted WHERE n = 1 AND m = 2 GROUP ALL", "bitmap"),
		// A disjunction is counted by the ordinary pipeline's own aggregate.
		(&owner, "SELECT count() FROM counted WHERE n = 1 OR m = 2 GROUP ALL", ""),
		// A predicate no index covers is counted by the ordinary pipeline's own
		// aggregate.
		(&owner, "SELECT count() FROM plain WHERE n % 2 = 0 GROUP ALL", ""),
		(&alice, "SELECT count() FROM mine GROUP ALL", "keys+deltas+ordinary"),
		(&alice, "SELECT count() FROM mine WHERE n = 1 GROUP ALL", "btree+ordinary"),
		(&alice, "SELECT count() FROM locked GROUP ALL", "keys"),
		(&alice, "SELECT count() FROM locked WHERE n = 1 GROUP ALL", "btree"),
	] {
		let program = program_of(ctx, sql).await;
		let expected: Vec<String> =
			Some(expected.to_owned()).filter(|e| !e.is_empty()).into_iter().collect();
		assert_eq!(counted_by(&program), expected, "`{sql}`");
	}
}

/// A denied table counts none and a count that reports one row still reports
/// it; a conditional permission counts the records it admits.
#[tokio::test]
async fn a_checked_count_follows_the_tables_permission() {
	let db = TestDb::new_with_auth(&tables()).await;
	let alice = db.exec_ctx_as(&session("alice"), crate::kvs::TransactionType::Read).await;
	let count = |rows: Result<Vec<Value>, String>| {
		let rows = rows.expect("counts");
		assert_eq!(rows.len(), 1, "{rows:?}");
		rows[0].to_sql()
	};
	for sql in [
		"SELECT count() FROM locked GROUP ALL",
		"SELECT count() FROM locked WHERE n = 1 GROUP ALL",
		"SELECT count() FROM locked:3 GROUP ALL",
	] {
		let (rows, _) = run(&alice, sql).await.expect("covered");
		assert_eq!(count(rows), "{ count: 0 }", "`{sql}`");
	}
	let (rows, _) =
		run(&alice, "SELECT VALUE count() FROM locked GROUP ALL").await.expect("covered");
	assert_eq!(count(rows), "0");
	// Alice owns the even rows.
	let (rows, _) = run(&alice, "SELECT count() FROM mine GROUP ALL").await.expect("covered");
	assert_eq!(count(rows), "{ count: 12 }");
	let (rows, _) =
		run(&alice, "SELECT count() FROM mine WHERE n = 2 GROUP ALL").await.expect("covered");
	assert_eq!(count(rows), "{ count: 6 }");
	let owner = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	let (rows, _) = run(&owner, "SELECT count() FROM mine GROUP ALL").await.expect("covered");
	assert_eq!(count(rows), "{ count: 24 }");
}

/// A principal of another tenant is refused before the count reads anything.
#[tokio::test]
async fn a_principal_of_another_tenant_is_refused_by_every_count() {
	let db = TestDb::new_with_auth(&tables()).await;
	let owner = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	let ctx = db.exec_ctx_as(&foreign(), crate::kvs::TransactionType::Read).await;
	for sql in [
		"SELECT count() FROM plain GROUP ALL",
		"SELECT count() FROM counted GROUP ALL",
		"SELECT count() FROM counted WHERE n > 1 GROUP ALL",
		"SELECT count() FROM counted WHERE n = 1 GROUP ALL",
		"SELECT count() FROM counted WHERE n = 1 OR m = 2 GROUP ALL",
		"SELECT count() FROM plain WHERE n % 2 = 0 GROUP ALL",
		"SELECT count() FROM mine GROUP ALL",
	] {
		let bound = bind(&owner, sql).await;
		let vm = super::plan(&planner(&owner), &bound).await.expect("the VM covers it");
		let vm = outcome(&vm, &ctx).await;
		assert!(vm.is_err(), "`{sql}` was counted: {vm:?}");
	}
}

/// A count sees the records its own transaction wrote and deleted, whichever
/// structure answers it.
#[tokio::test]
async fn a_count_reads_its_own_transactions_writes() {
	let db = TestDb::new_with_auth(&tables()).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	let value = |rows: Result<Vec<Value>, String>| rows.expect("runs")[0].to_sql();
	let counts = [
		("SELECT VALUE count() FROM counted GROUP ALL", ["24", "25", "24"]),
		("SELECT VALUE count() FROM counted WHERE n > 1 GROUP ALL", ["12", "13", "12"]),
		("SELECT VALUE count() FROM counted WHERE n = 1 GROUP ALL", ["6", "6", "6"]),
		("SELECT VALUE count() FROM counted WHERE n = 1 OR m = 2 GROUP ALL", ["10", "10", "10"]),
		("SELECT VALUE count() FROM plain WHERE n % 2 = 0 GROUP ALL", ["12", "12", "12"]),
	];
	for (at, write) in [
		None,
		Some("CREATE counted:100 SET n = 3, m = 4, owner = user:bob"),
		Some("DELETE counted:100"),
	]
	.into_iter()
	.enumerate()
	{
		if let Some(write) = write {
			let (rows, _) = run(&ctx, write).await.expect("covered");
			rows.expect("writes");
		}
		for (sql, expected) in &counts {
			let (rows, _) = run(&ctx, sql).await.expect("covered");
			assert_eq!(value(rows), expected[at], "`{sql}` after step {at}");
		}
	}
}
