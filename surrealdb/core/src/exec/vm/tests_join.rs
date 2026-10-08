//! A statement joined to the row it is evaluated for by an equality on one
//! field reads through a probe, and answers its rows in order and its errors,
//! for the owner and for a record user whose reads are checked.

use surrealdb_types::ToSql;

use super::golden::Golden;
use super::program::Program;
use super::tests::{bind, outcome, planner, render_written, without_generated_ids};
use crate::exec::ExecutionContext;
use crate::exec::operators::test_util::TestDb;
use crate::val::Value;

const SETUP: &str = "
	DEFINE TABLE a PERMISSIONS FULL;
	DEFINE TABLE b PERMISSIONS FOR select WHERE hidden != true;
	DEFINE FIELD guarded ON b PERMISSIONS FOR select WHERE $auth.id = user:x;
	DEFINE FIELD twice ON b COMPUTED x * 2;
	DEFINE TABLE empty PERMISSIONS FULL;
	DEFINE TABLE log PERMISSIONS FULL;
	CREATE a:1 SET k = 1, ref = b:1;
	CREATE a:2 SET k = 2.0, ref = b:4;
	CREATE a:3 SET k = 'one';
	CREATE a:4 SET k = NONE;
	CREATE a:5 SET k = NULL;
	CREATE a:6 SET k = 3dec;
	CREATE a:7 SET k = [1, 'x'];
	CREATE a:8 SET k = 'abc';
	CREATE a:9 SET k = 0.1;
	CREATE a:10 SET k = -0.0;
	CREATE a:11 SET k = 9;
	CREATE b:1 SET k = 1, x = 10, s = '1', guarded = 'g1';
	CREATE b:2 SET k = 1.0, x = 20, s = 'two';
	CREATE b:3 SET k = 2, x = 30, s = '3';
	CREATE b:4 SET k = 2dec, x = 40, s = '4', hidden = true;
	CREATE b:5 SET k = 'one', x = 50, s = '5';
	CREATE b:6 SET x = 60, s = '6';
	CREATE b:7 SET k = NULL, x = 70, s = '7';
	CREATE b:8 SET k = 3.0, x = 80, s = '8';
	CREATE b:9 SET k = [1, 'x'], x = 90, s = '9';
	CREATE b:10 SET k = [1.0, 'x'], x = 100, s = '10';
	CREATE b:11 SET k = /b/, x = 110, s = '11';
	CREATE b:12 SET k = 0.1dec, x = 120, s = '12';
	CREATE b:13 SET k = 0, x = 130, s = '13';
	CREATE b:14 SET k = 1, x = 5, s = 'fourteen';
";

/// Each statement, and whether each statement it nests in scalar position
/// reads through a probe, for the owner and for the record user.
const STATEMENTS: &[(&str, &[bool], &[bool])] = &[
	("SELECT id, (SELECT VALUE x FROM b WHERE k = $parent.k) AS xs FROM a", &[true], &[true]),
	("SELECT id, (SELECT VALUE x FROM b WHERE $parent.k = k) AS xs FROM a", &[true], &[true]),
	(
		"SELECT id, (SELECT VALUE id FROM b WHERE k = $parent.k AND x > 10) AS xs FROM a",
		&[true],
		&[true],
	),
	(
		"SELECT id, (SELECT VALUE x FROM b WHERE k = $parent.k ORDER BY x DESC LIMIT 1) AS top FROM a",
		&[true],
		&[true],
	),
	(
		"SELECT id, (SELECT VALUE x FROM b WHERE k = $parent.k LIMIT 1 START 1) AS second FROM a",
		&[true],
		&[true],
	),
	(
		"SELECT id, (SELECT count() AS c, k FROM b WHERE k = $parent.k GROUP BY k) AS g FROM a",
		&[true],
		&[true],
	),
	(
		"SELECT id, (SELECT VALUE x FROM ONLY b WHERE k = $parent.k LIMIT 1) AS one FROM a",
		&[true],
		&[true],
	),
	(
		"SELECT VALUE id FROM a WHERE (SELECT VALUE x FROM b WHERE k = $parent.k) != []",
		&[true],
		&[true],
	),
	("SELECT id, (SELECT VALUE x FROM b WHERE k = $parent.ref.k) AS xs FROM a", &[true], &[true]),
	// The row is a record id, whose field the predicate reads by fetching it.
	(
		"SELECT VALUE (SELECT VALUE x FROM b WHERE k = $parent.k) FROM (SELECT VALUE id FROM a)",
		&[true],
		&[true],
	),
	("SELECT id, (SELECT VALUE twice FROM b WHERE k = $parent.k) AS t FROM a", &[true], &[true]),
	("SELECT id, (SELECT VALUE x FROM b WHERE twice = $parent.k) AS t FROM a", &[true], &[true]),
	("SELECT id, (SELECT * FROM b WHERE k = $parent.k) AS rows FROM a", &[true], &[false]),
	("SELECT id, (SELECT VALUE x FROM empty WHERE k = $parent.k) AS xs FROM a", &[true], &[true]),
	(
		"SELECT id, (SELECT VALUE x FROM b WHERE k = $parent.k LIMIT 0) AS xs FROM a",
		&[true],
		&[true],
	),
	(
		"SELECT id, (SELECT VALUE x FROM b WHERE k = $parent.k) AS xs FROM a ORDER BY id LIMIT 3",
		&[true],
		&[true],
	),
	// Not joined by one equality first: applied per row.
	(
		"SELECT id, (SELECT VALUE x FROM b WHERE x > 10 AND k = $parent.k) AS xs FROM a",
		&[false],
		&[false],
	),
	(
		"SELECT id, (SELECT VALUE x FROM b WHERE k = $parent.k OR x = 10) AS xs FROM a",
		&[false],
		&[false],
	),
	(
		"SELECT id, (SELECT VALUE x FROM b WHERE k = $parent.k AND x != $parent.k) AS xs FROM a",
		&[false],
		&[false],
	),
	(
		"SELECT id, (SELECT VALUE [x, $parent.id] FROM b WHERE k = $parent.k) AS xs FROM a",
		&[false],
		&[false],
	),
	("SELECT id, (SELECT VALUE x FROM b WHERE k == $parent.k) AS xs FROM a", &[false], &[false]),
];

/// Whether each statement a program nests in scalar position reads through
/// a probe.
fn probed(program: &Program) -> Vec<bool> {
	program.nested.iter().map(|spec| spec.probed).collect()
}

async fn lowered(ctx: &ExecutionContext, sql: &str) -> Program {
	let bound = bind(ctx, sql).await;
	let physical = super::optimize::optimize(
		super::map::map(&planner(ctx), &bound).await.unwrap_or_else(|e| panic!("`{sql}`: {e:?}")),
	);
	super::lower::lower(&planner(ctx), &physical).await.unwrap_or_else(|e| panic!("`{sql}`: {e:?}"))
}

fn reader() -> crate::dbs::Session {
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	crate::dbs::Session::for_record("test", "test", "user", rid)
}

#[tokio::test]
async fn join_hashed_reads_answer_their_rows() {
	let mut golden = Golden::new("join_hashed_reads_answer_their_rows");
	let db = TestDb::new_with_auth(SETUP).await;
	let mut shapes = Vec::new();
	for (who, session) in [("owner", TestDb::owner()), ("record user", reader())] {
		let ctx = db.exec_ctx_as(&session, crate::kvs::TransactionType::Read).await;
		for (sql, owner, record_user) in STATEMENTS {
			let bound = bind(&ctx, sql).await;
			let vm = super::plan(&planner(&ctx), &bound)
				.await
				.unwrap_or_else(|| panic!("the VM declined `{sql}` for the {who}"));
			golden.rows(format!("{who}: {sql}"), &outcome(&vm, &ctx).await);
			let program = lowered(&ctx, sql).await;
			let expected = if who == "owner" {
				owner
			} else {
				record_user
			};
			if probed(&program) != *expected {
				shapes.push(format!("`{sql}` for the {who}: {:?}", probed(&program)));
			}
			assert!(
				program.subqueries.is_empty(),
				"`{sql}` for the {who} runs a statement as a program of its own"
			);
		}
	}
	assert!(shapes.is_empty(), "programs of another shape: {shapes:#?}");
	golden.check();
}

/// An error the statement's body raises for a row is raised for a row its key
/// reaches, and not for one no key reaches.
#[tokio::test]
async fn join_an_error_is_raised_only_for_a_row_a_key_reaches() {
	let mut golden = Golden::new("join_an_error_is_raised_only_for_a_row_a_key_reaches");
	let db = TestDb::new_with_auth(SETUP).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	for sql in [
		// `b:2` holds a string no int cast takes, and only key 1 reaches it.
		"SELECT id, (SELECT VALUE <int> s FROM b WHERE k = $parent.k) AS n FROM a WHERE k = 2.0",
		"SELECT id, (SELECT VALUE <int> s FROM b WHERE k = $parent.k) AS n FROM a WHERE k = 1",
		"SELECT id, (SELECT VALUE x FROM b WHERE k = $parent.k AND <int> s > 0) AS n FROM a WHERE k = 'one'",
		"SELECT id, (SELECT VALUE x FROM b WHERE k = $parent.k AND <int> s > 0) AS n FROM a",
		"SELECT id, (SELECT VALUE x FROM b WHERE k = $parent.k AND <int> s > 0) AS n FROM a WHERE k = 99",
	] {
		let bound = bind(&ctx, sql).await;
		let vm = super::plan(&planner(&ctx), &bound)
			.await
			.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
		golden.rows(sql, &outcome(&vm, &ctx).await);
		assert_eq!(probed(&lowered(&ctx, sql).await), vec![true], "`{sql}`");
	}
	golden.check();
}

/// Run `sql` on a fresh database as the owner and answer its rows and every
/// record of `log`.
async fn written(sql: &str) -> (Result<Vec<Value>, String>, Vec<String>) {
	let db = TestDb::new_with_auth(SETUP).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	let bound = bind(&ctx, sql).await;
	let op = super::plan(&planner(&ctx), &bound)
		.await
		.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
	let rows =
		outcome(&op, &ctx).await.map(|rows| rows.into_iter().map(without_generated_ids).collect());
	let txn = ctx.txn();
	if rows.is_ok() {
		txn.commit().await.expect("commit");
	} else {
		txn.cancel().await.expect("cancel");
	}
	let read = db.exec_ctx().await;
	let op = crate::exec::operators::test_util::plan_statement("SELECT * FROM log", &read).await;
	let mut state: Vec<String> = outcome(&op, &read)
		.await
		.expect("the state reads")
		.into_iter()
		.map(|row| without_generated_ids(row).to_sql())
		.collect();
	state.sort();
	(rows, state)
}

/// A probe's rows are read again at each reach of the statement it is
/// written in, so a reach reads what an earlier one wrote; a statement that
/// writes is applied per row.
#[tokio::test]
async fn join_a_probe_reads_what_an_earlier_reach_wrote() {
	let mut golden = Golden::new("join_a_probe_reads_what_an_earlier_reach_wrote");
	for sql in [
		"FOR $i IN [1, 2, 3] { CREATE type::record('b', 100 + $i) SET k = 1, x = $i; CREATE log SET xs = (SELECT (SELECT VALUE x FROM b WHERE k = $parent.k) AS xs FROM ONLY a:1).xs }",
		"FOR $i IN [1, 2] { CREATE log SET n = $i; CREATE log SET seen = (SELECT VALUE (SELECT VALUE n FROM log WHERE n = $parent.n) FROM [{ n: $i }]) }",
		"UPDATE a SET xs = (SELECT VALUE x FROM b WHERE k = $parent.k)",
		"UPDATE a SET xs = (SELECT VALUE x FROM b WHERE k = $parent.k) RETURN xs",
	] {
		let (rows, state) = written(sql).await;
		golden.record(sql, render_written(&rows, &state));
	}
	golden.check();
}

/// A probe reads its rows once for every row of the statement it is written
/// in, and again at each reach of that statement.
#[tokio::test]
async fn join_a_probe_reads_its_rows_once_per_reach() {
	let db = TestDb::new_with_auth(SETUP).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	for (sql, reads) in [
		("SELECT id, (SELECT VALUE x FROM b WHERE k = $parent.k) AS xs FROM a", 1),
		("SELECT id, (SELECT VALUE x FROM b WHERE k = $parent.k) AS xs FROM a WHERE k = 'none'", 0),
		(
			"FOR $i IN [1, 2, 3] { LET $r = SELECT (SELECT VALUE x FROM b WHERE k = $parent.k) AS xs FROM a }",
			3,
		),
	] {
		let bound = bind(&ctx, sql).await;
		let vm = super::plan(&planner(&ctx), &bound)
			.await
			.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
		let before = super::nested::PROBES_BUILT.with(std::cell::Cell::get);
		outcome(&vm, &ctx).await.unwrap_or_else(|e| panic!("`{sql}`: {e}"));
		let after = super::nested::PROBES_BUILT.with(std::cell::Cell::get);
		assert_eq!(after - before, reads, "`{sql}`");
	}
}

/// How long a correlated read takes joined through a probe and applied per
/// row: `outer` rows of `a`, each reading the
/// ten rows of `b`'s 10,000 its key names. The per-row form is the same
/// statement with its join written so that it is no conjunct of the form a
/// probe takes. Run with `cargo test --release -p surrealdb-core --lib
/// join_throughput -- --ignored --nocapture`; `JOIN_OUTER` sets the outer
/// rows (10,000 by default).
#[tokio::test]
#[ignore = "a measurement, not a check"]
async fn join_throughput() {
	let outer: usize =
		std::env::var("JOIN_OUTER").ok().and_then(|v| v.parse().ok()).unwrap_or(10_000);
	let setup = format!(
		"
		DEFINE TABLE a;
		DEFINE TABLE b;
		FOR $i IN 0..{outer} {{ CREATE type::record('a', $i) SET k = $i % 1000 }};
		FOR $i IN 0..10000 {{ CREATE type::record('b', $i) SET k = $i % 1000, x = $i }};
	"
	);
	let db = TestDb::new(&setup).await;
	let ctx = db.exec_ctx().await;
	let hashed = "SELECT id, (SELECT VALUE x FROM b WHERE k = $parent.k) AS xs FROM a";
	let applied = "SELECT id, (SELECT VALUE x FROM b WHERE (k = $parent.k) = true) AS xs FROM a";
	let semi = "SELECT VALUE id FROM a WHERE (SELECT VALUE x FROM b WHERE k = $parent.k AND x > 9990) != []";
	let semi_applied = "SELECT VALUE id FROM a WHERE (SELECT VALUE x FROM b WHERE (k = $parent.k) = true AND x > 9990) != []";
	for (label, sql) in
		[("hash", hashed), ("apply", applied), ("semi hash", semi), ("semi apply", semi_applied)]
	{
		let bound = bind(&ctx, sql).await;
		let op = super::plan(&planner(&ctx), &bound).await.expect("the VM runs it");
		let mut best = std::time::Duration::MAX;
		let mut rows = 0;
		for _ in 0..if label.contains("hash") {
			5
		} else {
			1
		} {
			let started = web_time::Instant::now();
			rows = outcome(&op, &ctx).await.expect("runs").len();
			best = best.min(started.elapsed());
		}
		eprintln!("{outer} × 10000 {label}: {best:?} ({rows} rows)");
	}
}

/// A membership whose subquery is correlated on a further key reads that key
/// off its probe row once per probe row, whether the probe yields objects or
/// bare record ids: `coin` is drawn afresh each time it is read, and with one
/// subquery row of each answer, a key drawn once always finds a row, so every
/// probe row is kept. Read again for each subquery row, a probe row would be
/// kept only when one of its two draws matched its own, and the count would
/// fall short of the whole table with near certainty over forty rows.
#[tokio::test]
async fn a_key_read_off_a_probe_of_record_ids_is_read_once_per_probe_row() {
	let db = TestDb::new(
		"
		DEFINE TABLE o SCHEMALESS;
		DEFINE TABLE r SCHEMALESS;
		FOR $i IN 0..40 { CREATE type::record('o:' + <string> $i) RETURN NONE };
		CREATE r:f SET k = false, v = 1 RETURN NONE;
		CREATE r:t SET k = true, v = 1 RETURN NONE;
		DEFINE FIELD coin ON o COMPUTED rand::bool();
	",
	)
	.await;
	let ctx = db.exec_ctx().await;
	let member = "(SELECT VALUE v FROM r WHERE k = $parent.coin)";
	let probes = [
		("record ids", "(SELECT VALUE id FROM o)"),
		("filtered record ids", "(SELECT VALUE id FROM o WHERE id != o:99)"),
		("objects", "(SELECT * FROM o)"),
		("a table", "o"),
	];
	let mut checked = 0;
	for (label, probe) in probes {
		for (form, kept) in [
			(format!("1 IN {member}"), 40),
			(format!("{member} CONTAINS 1"), 40),
			(format!("1 NOT IN {member}"), 0),
			(format!("2 NOT IN {member}"), 40),
		] {
			let sql = format!(
				"SELECT VALUE count() FROM ONLY (SELECT * FROM {probe} WHERE {form}) GROUP ALL"
			);
			let bound = bind(&ctx, &sql).await;
			let vm = super::plan(&planner(&ctx), &bound)
				.await
				.unwrap_or_else(|| panic!("the VM runs `{sql}` ({label})"));
			for _ in 0..3 {
				let rows = outcome(&vm, &ctx).await.unwrap_or_else(|e| panic!("`{sql}`: {e}"));
				// A statement that keeps no row groups nothing.
				let count = rows.first().cloned().unwrap_or(Value::None);
				let count = if kept == 0 && count == Value::None {
					Value::from(0)
				} else {
					count
				};
				assert_eq!(count, Value::from(kept), "`{sql}` ({label})");
				checked += 1;
			}
		}
	}
	assert_eq!(checked, 48);
}
