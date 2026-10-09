//! Values, parameters, mocks and statements in `FROM`, and statements in
//! scalar position, answer their values, for the owner and for a record user
//! whose reads are checked: a value source is a source of the program, and a
//! nested statement a group of it, run once or per row as the plan's strategy
//! says.

use surrealdb_types::ToSql;

use super::golden::Golden;
use super::program::{Program, Source};
use super::tests::{bind, outcome, planner, render_written, without_generated_ids};
use crate::exec::ExecutionContext;
use crate::exec::operators::test_util::TestDb;
use crate::val::Value;

const SETUP: &str = "
	DEFINE TABLE item PERMISSIONS FOR select WHERE hidden != true;
	DEFINE FIELD secret ON item PERMISSIONS FOR select WHERE $auth.id = user:x;
	DEFINE FIELD twice ON item COMPUTED n * 2;
	DEFINE TABLE open PERMISSIONS FULL;
	DEFINE TABLE closed PERMISSIONS NONE;
	DEFINE TABLE log PERMISSIONS FULL;
	DEFINE TABLE seen PERMISSIONS FULL;
	CREATE item:1 SET n = 1, secret = 's1';
	CREATE item:2 SET n = 2, secret = 's2';
	CREATE item:3 SET n = 3, hidden = true;
	CREATE open:1 SET n = 10, of = item:1;
	CREATE open:2 SET n = 20, of = item:3;
	CREATE open:3 SET n = 2, of = item:9;
	CREATE closed:1 SET n = 100;
";

/// How a statement's program reads a value or a statement in `FROM`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Read {
	/// A value source.
	Values,
	/// A statement in `FROM`, a group of the program.
	Nested,
}

/// Each statement, how its program reads its sources, and the strategy of
/// each statement it nests in scalar position (`true` for once).
const STATEMENTS: &[(&str, &[Read], &[bool])] = &[
	("SELECT * FROM [item:1, item:3, closed:1, 5, NONE, NULL]", &[Read::Values], &[]),
	("SELECT * FROM [[item:1, item:2], 3, [4]]", &[Read::Values], &[]),
	("SELECT VALUE secret FROM [item:1, item:2]", &[Read::Values], &[]),
	("SELECT n, twice FROM [item:2, { n: 5 }] WHERE n > 1", &[Read::Values], &[]),
	("SELECT * FROM { a: 1 }", &[Read::Values], &[]),
	("SELECT * FROM [item:1, item:2] LIMIT 1 START 1", &[Read::Values], &[]),
	("SELECT * FROM [item:1, item:2] LIMIT 0", &[Read::Values], &[]),
	("SELECT count() FROM [item:1, item:2, item:3] GROUP ALL", &[Read::Values], &[]),
	("SELECT * FROM |item:1..3|", &[Read::Values], &[]),
	("SELECT * FROM array::shuffle([item:3])", &[Read::Values], &[]),
	("SELECT * FROM array::at(array::shuffle([item:1]), 0)", &[Read::Values], &[]),
	("SELECT * FROM array::at(array::shuffle([item:1]), 0) LIMIT 0", &[Read::Values], &[]),
	("SELECT * FROM array::at(array::shuffle([item:3]), 0)", &[Read::Values], &[]),
	("SELECT * FROM array::at(array::shuffle([closed:1]), 0)", &[Read::Values], &[]),
	("SELECT * FROM array::at(array::shuffle([item:1..=2]), 0)", &[Read::Values], &[]),
	("SELECT * FROM type::table(array::at(array::shuffle(['item']), 0))", &[Read::Values], &[]),
	(
		"SELECT n FROM type::table(array::at(array::shuffle(['item']), 0)) LIMIT 1",
		&[Read::Values],
		&[],
	),
	("SELECT * FROM type::table(array::at(array::shuffle(['closed']), 0))", &[Read::Values], &[]),
	("SELECT * FROM type::table(array::at(array::shuffle(['nope']), 0))", &[Read::Values], &[]),
	(
		"SELECT * FROM type::table(array::at(array::shuffle(['nope']), 0)) LIMIT 0",
		&[Read::Values],
		&[],
	),
	("SELECT * FROM array::shuffle([5])", &[Read::Values], &[]),
	("SELECT * FROM [1, 2], open", &[Read::Values], &[]),
	(
		"SELECT * FROM open:1, [item:2], array::shuffle([open:2])",
		&[Read::Values, Read::Values],
		&[],
	),
	("SELECT * FROM (SELECT * FROM item WHERE n > 1)", &[Read::Nested], &[]),
	("SELECT n FROM (SELECT * FROM item ORDER BY n DESC LIMIT 2)", &[Read::Nested], &[]),
	("SELECT * FROM (SELECT VALUE n FROM item)", &[Read::Nested], &[]),
	("SELECT * FROM (SELECT VALUE id FROM item)", &[Read::Nested], &[]),
	("SELECT * FROM (SELECT VALUE of FROM open) LIMIT 2", &[Read::Nested], &[]),
	("SELECT VALUE $this FROM (SELECT VALUE id FROM item)", &[Read::Nested], &[]),
	("SELECT * FROM (SELECT * FROM ONLY item:1)", &[Read::Nested], &[]),
	("SELECT * FROM (SELECT * FROM item) LIMIT 0", &[Read::Nested], &[]),
	(
		"SELECT * FROM (SELECT * FROM (SELECT * FROM item)) LIMIT 1",
		&[Read::Nested, Read::Nested],
		&[],
	),
	("SELECT * FROM (SELECT * FROM item), [7]", &[Read::Nested, Read::Values], &[]),
	("{ LET $t = type::table('item'); RETURN SELECT * FROM $t }", &[Read::Values], &[]),
	("{ LET $r = item:2; RETURN SELECT n, twice FROM $r }", &[Read::Values], &[]),
	("{ LET $a = [item:1, 7]; RETURN SELECT * FROM $a WHERE n > 0 }", &[Read::Values], &[]),
	("SELECT n, (SELECT VALUE count() FROM item GROUP ALL)[0] AS total FROM open", &[], &[true]),
	(
		"SELECT * FROM open WHERE n > (SELECT VALUE math::mean(n) FROM open GROUP ALL)[0]",
		&[],
		&[true],
	),
	("SELECT n, (SELECT * FROM closed) AS c FROM open", &[], &[true]),
	("SELECT n, (SELECT VALUE n FROM item WHERE n < $parent.n) AS below FROM open", &[], &[false]),
	("SELECT (SELECT VALUE twice FROM ONLY $parent.of) AS t FROM open", &[Read::Values], &[false]),
	(
		"SELECT n FROM open WHERE (SELECT VALUE hidden FROM ONLY $parent.of) != true",
		&[Read::Values],
		&[false],
	),
	("SELECT (SELECT VALUE x FROM [{ x: $parent.n }]) AS x FROM open", &[Read::Values], &[false]),
	(
		"SELECT (SELECT VALUE count() FROM item GROUP ALL) AS a, (SELECT VALUE n FROM item WHERE n = $parent.n) AS b FROM open",
		&[],
		&[true, false],
	),
	("SELECT VALUE (SELECT VALUE n FROM item LIMIT 1) FROM open LIMIT 0", &[], &[true]),
	// An aggregate's argument is evaluated by the aggregate, and a statement
	// in it runs as a program of its own.
	("SELECT math::sum((SELECT VALUE n FROM item)) AS s FROM open GROUP ALL", &[], &[true]),
	(
		"SELECT * FROM (SELECT (SELECT VALUE n FROM item WHERE n < $parent.n) AS b FROM open)",
		&[Read::Nested],
		&[false],
	),
];

/// The value and statement sources a program's pipelines read, in pipeline
/// order, a union's in its written order.
fn reads(program: &Program) -> Vec<Read> {
	fn of(source: &Source, out: &mut Vec<Read>) {
		match source {
			Source::Values {
				..
			} => out.push(Read::Values),
			Source::Nested {
				..
			} => out.push(Read::Nested),
			Source::Union(sources) => sources.iter().for_each(|source| of(source, out)),
			_ => {}
		}
	}
	let mut out = Vec::new();
	for pipeline in &program.pipelines {
		of(&pipeline.source, &mut out);
	}
	out
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
async fn subq_sources_and_nested_statements_answer_their_values() {
	let mut golden = Golden::new("subq_sources_and_nested_statements_answer_their_values");
	let db = TestDb::new_with_auth(SETUP).await;
	let mut shapes = Vec::new();
	for (who, session) in [("owner", TestDb::owner()), ("record user", reader())] {
		let ctx = db.exec_ctx_as(&session, crate::kvs::TransactionType::Read).await;
		for (sql, sources, nested) in STATEMENTS {
			let program = lowered(&ctx, sql).await;
			let bound = bind(&ctx, sql).await;
			let vm = super::plan(&planner(&ctx), &bound)
				.await
				.unwrap_or_else(|| panic!("the VM declined `{sql}` for the {who}"));
			golden.rows(format!("{who}: {sql}"), &outcome(&vm, &ctx).await);
			let read = reads(&program);
			let strategies: Vec<bool> = program.nested.iter().map(|spec| spec.once).collect();
			if read != *sources || strategies != *nested {
				shapes.push(format!("`{sql}` for the {who}: {read:?} {strategies:?}"));
			}
			if !nested.is_empty() {
				assert!(
					program.subqueries.is_empty(),
					"`{sql}` for the {who} runs a statement as a program of its own"
				);
			}
			// Every group a nested statement takes is the program's own.
			for spec in &program.nested {
				assert!((spec.group as usize) < program.groups.len());
			}
		}
	}
	assert!(shapes.is_empty(), "programs of another shape: {shapes:#?}");
	golden.check();
}

/// Run `sql` on a fresh database as the owner and answer its rows and every
/// record of `log`, `seen` and `open`.
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
	let mut state = Vec::new();
	for table in ["log", "seen", "open"] {
		let op = crate::exec::operators::test_util::plan_statement(
			&format!("SELECT * FROM {table}"),
			&read,
		)
		.await;
		let mut rows: Vec<String> = outcome(&op, &read)
			.await
			.expect("the state reads")
			.into_iter()
			.map(|row| format!("{table}: {}", without_generated_ids(row).to_sql()))
			.collect();
		rows.sort();
		state.extend(rows);
	}
	(rows, state)
}

/// A nested statement in a write, or reading what an earlier row or reach
/// wrote, answers the rows the write leaves: it is run per row where the
/// statement writes, and a statement run once runs again at each reach of the
/// statement it is written in.
#[tokio::test]
async fn subq_a_nested_statement_reads_what_the_statement_wrote() {
	let mut golden = Golden::new("subq_a_nested_statement_reads_what_the_statement_wrote");
	for sql in [
		"UPDATE open SET m = (SELECT VALUE count() FROM item GROUP ALL)[0]",
		"UPDATE open SET m = (SELECT VALUE n FROM item WHERE n < $parent.n)",
		"FOR $i IN [1, 2, 3] { CREATE log SET c = (SELECT VALUE count() FROM log GROUP ALL)[0] }",
		"FOR $i IN [1, 2] { CREATE log; LET $x = SELECT (SELECT VALUE count() FROM log GROUP ALL)[0] AS c FROM ONLY open:1; CREATE seen SET c = $x.c }",
		"UPDATE open SET m = (CREATE log SET of = $parent.id RETURN VALUE of)",
		"UPDATE (SELECT * FROM open WHERE n > 5) SET m = 1",
		"UPDATE (SELECT VALUE id FROM open ORDER BY n DESC LIMIT 1) SET m = 2",
		"DELETE [open:1, open:3]",
		"{ LET $o = open:2; RETURN UPDATE $o SET m = 3 }",
	] {
		let (rows, state) = written(sql).await;
		golden.record(sql, render_written(&rows, &state));
	}
	golden.check();
}

/// A statement run once is run by the first row that reaches it and by no
/// other, and by none when no row reaches it: its value is the same for every
/// row, and a statement over no rows never raises what it would.
#[tokio::test]
async fn subq_a_statement_run_once_runs_where_the_first_row_reaches_it() {
	let mut golden = Golden::new("subq_a_statement_run_once_runs_where_the_first_row_reaches_it");
	let db = TestDb::new_with_auth(SETUP).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	for sql in [
		"SELECT n, (SELECT VALUE <int> secret FROM item) AS x FROM open WHERE n > 100",
		"SELECT n, (SELECT VALUE <int> secret FROM item) AS x FROM open",
	] {
		let bound = bind(&ctx, sql).await;
		let vm = super::plan(&planner(&ctx), &bound)
			.await
			.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
		golden.rows(sql, &outcome(&vm, &ctx).await);
		let program = lowered(&ctx, sql).await;
		assert_eq!(program.nested.iter().map(|spec| spec.once).collect::<Vec<_>>(), vec![true]);
	}
	golden.check();
}

/// A statement reading a field its caller's guard judges through `$auth` is
/// applied per row for a checked caller — the guard's read of `$auth.id` is
/// a read that may fetch a record, whose computed fields are bodies the plan
/// cannot see — and run once for the owner, whose reads no guard judges.
#[tokio::test]
async fn subq_a_guarded_read_runs_per_row_for_a_checked_caller() {
	let mut golden = Golden::new("subq_a_guarded_read_runs_per_row_for_a_checked_caller");
	let db = TestDb::new_with_auth(SETUP).await;
	let sql = "SELECT n, (SELECT VALUE secret FROM item) AS s FROM open";
	for (who, session, once) in [("owner", TestDb::owner(), true), ("record user", reader(), false)]
	{
		let ctx = db.exec_ctx_as(&session, crate::kvs::TransactionType::Read).await;
		let bound = bind(&ctx, sql).await;
		let vm = super::plan(&planner(&ctx), &bound)
			.await
			.unwrap_or_else(|| panic!("the VM declined `{sql}` for the {who}"));
		golden.rows(format!("{who}: {sql}"), &outcome(&vm, &ctx).await);
		let program = lowered(&ctx, sql).await;
		assert_eq!(
			program.nested.iter().map(|spec| spec.once).collect::<Vec<_>>(),
			vec![once],
			"for the {who}"
		);
	}
	golden.check();
}

/// What the statements of [`UNIT_STATEMENTS`] read beyond [`SETUP`]: a
/// computed field and a table permission each holding a statement, a cycle of
/// record links the permission follows, and an event whose clauses are
/// statements.
const UNIT_EXTRA: &str = "
	DEFINE TABLE cs PERMISSIONS FULL;
	DEFINE FIELD total ON cs COMPUTED (SELECT VALUE count() FROM item GROUP ALL)[0];
	CREATE cs:1;
	DEFINE TABLE node SCHEMAFULL PERMISSIONS
		FOR select WHERE (SELECT VALUE label FROM node WHERE id = $this.next LIMIT 1) != NONE
		FOR create, update, delete FULL;
	DEFINE FIELD label ON node TYPE string;
	DEFINE FIELD next ON node TYPE option<record<node>>;
	CREATE node:a SET label = 'alpha', next = node:b;
	CREATE node:b SET label = 'beta', next = node:c;
	CREATE node:c SET label = 'gamma', next = node:a;
	DEFINE EVENT audit ON open WHEN (RETURN $event = 'UPDATE') THEN (CREATE log SET of = $value.id, m = $after.m);
";

/// Statements whose statements in scalar position the program runs as units
/// of its own: in a clause the host evaluates, in a write's `RETURN`, where
/// the group of a nested statement cannot be run, and as a statement that is a
/// block, a `RETURN` or `INFO`.
const UNIT_STATEMENTS: &[&str] = &[
	// Paging clauses.
	"SELECT * FROM open ORDER BY n LIMIT (SELECT VALUE count() FROM item GROUP ALL)[0]",
	"SELECT * FROM open ORDER BY n LIMIT 1 START (RETURN 1)",
	// A statement that is not a relational plan, over the row it reads.
	"SELECT (RETURN n + 1) AS m FROM open",
	"SELECT ({ LET $a = n; RETURN $a }) AS m FROM open",
	"SELECT (INFO FOR DB).tables AS t FROM open LIMIT 1",
	// A computed field's body.
	"SELECT * FROM cs",
];

/// Statements that write, run on a fresh database; the unit a subquery in
/// them is runs with the row it is evaluated for as `$parent` and `$this`.
const UNIT_WRITES: &[&str] = &[
	"UPDATE open SET m = 1 RETURN (SELECT VALUE count() FROM item GROUP ALL)[0] AS c",
	"UPDATE open SET m = 1 RETURN n, (SELECT VALUE n FROM item WHERE n < $parent.n) AS below",
	"UPDATE open SET m = 1 RETURN n, (SELECT VALUE twice FROM ONLY $parent.of) AS t",
	"UPDATE open SET m = (CREATE log SET of = $parent.id RETURN VALUE of)",
	"UPDATE open SET m = 1",
	"UPDATE open SET m = 1 RETURN n, (SELECT VALUE m FROM ONLY $parent) AS written",
];

fn units_are_programs(program: &Program) -> bool {
	program
		.subqueries
		.iter()
		.all(|spec| program.units.get(spec.unit.0 as usize).is_some_and(|unit| unit.unit.is_some()))
}

/// A statement in scalar position that the group of a nested statement does
/// not run is a unit of the program, plain data and no operator, and answers
/// its value, for the owner and for a record user whose reads are checked.
#[tokio::test]
async fn subq_a_statement_run_as_a_unit_answers_its_value() {
	let mut golden = Golden::new("subq_a_statement_run_as_a_unit_answers_its_value");
	let db = TestDb::new_with_auth(&format!("{SETUP}{UNIT_EXTRA}")).await;
	let mut held = 0;
	for (who, session) in [("owner", TestDb::owner()), ("record user", reader())] {
		let ctx = db.exec_ctx_as(&session, crate::kvs::TransactionType::Read).await;
		for sql in UNIT_STATEMENTS {
			let program = lowered(&ctx, sql).await;
			assert!(
				units_are_programs(&program),
				"`{sql}` for the {who} holds a subquery that is no unit"
			);
			held += program.subqueries.len();
			let bound = bind(&ctx, sql).await;
			let vm = super::plan(&planner(&ctx), &bound)
				.await
				.unwrap_or_else(|| panic!("the VM declined `{sql}` for the {who}"));
			golden.rows(format!("{who}: {sql}"), &outcome(&vm, &ctx).await);
		}
	}
	assert!(held >= UNIT_STATEMENTS.len(), "the statements ran no unit between them");
	golden.check();
}

/// Run `sql` on a fresh [`SETUP`] with [`UNIT_EXTRA`] as the owner and answer
/// its rows, the records of `log` and `open` it leaves, and the units its
/// program holds.
async fn written_over_units(sql: &str) -> (Result<Vec<Value>, String>, Vec<String>, usize) {
	let db = TestDb::new_with_auth(&format!("{SETUP}{UNIT_EXTRA}")).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	let bound = bind(&ctx, sql).await;
	let physical = super::optimize::optimize(
		super::map::map(&planner(&ctx), &bound).await.unwrap_or_else(|e| panic!("`{sql}`: {e:?}")),
	);
	let program = super::lower::lower(&planner(&ctx), &physical)
		.await
		.unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
	assert!(units_are_programs(&program), "`{sql}` holds a subquery that is no unit");
	let units = program.subqueries.len();
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
	let mut state = Vec::new();
	for table in ["log", "open"] {
		let op = crate::exec::operators::test_util::plan_statement(
			&format!("SELECT * FROM {table}"),
			&read,
		)
		.await;
		let mut rows: Vec<String> = outcome(&op, &read)
			.await
			.expect("the state reads")
			.into_iter()
			.map(|row| format!("{table}: {}", without_generated_ids(row).to_sql()))
			.collect();
		rows.sort();
		state.extend(rows);
	}
	(rows, state, units)
}

/// A subquery in a write — its `RETURN`, its data, and the clauses of an event
/// it fires — is run per row as a unit with the row bound as `$parent` and
/// `$this`, and leaves the records it writes.
#[tokio::test]
async fn subq_a_subquery_in_a_write_runs_as_a_unit() {
	let mut golden = Golden::new("subq_a_subquery_in_a_write_runs_as_a_unit");
	let mut held = 0;
	for sql in UNIT_WRITES {
		let (rows, state, units) = written_over_units(sql).await;
		golden.record(sql, render_written(&rows, &state));
		held += units;
	}
	assert!(held >= 4, "the writes ran no unit between them: {held}");
	golden.check();
}

/// A permission predicate holding a statement that follows a cycle of record
/// links is not read again by the statement: the predicate's own reads skip
/// the permission inside its frame, so the statement answers its rows rather
/// than recursing.
#[tokio::test]
async fn subq_a_permission_predicate_over_a_cycle_of_links_does_not_recurse() {
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("node", "a"));
	let session = crate::dbs::Session::for_record("test", "test", "user", rid);
	let mut golden =
		Golden::new("subq_a_permission_predicate_over_a_cycle_of_links_does_not_recurse");
	let db = TestDb::new_with_auth(&format!("{SETUP}{UNIT_EXTRA}")).await;
	for sql in ["SELECT * FROM node ORDER BY id", "SELECT * FROM node:b"] {
		let responses = db.datastore().execute(sql, &session, None).await.expect("runs");
		let answered: Vec<String> =
			responses.into_iter().map(|r| format!("{:?}", r.result)).collect();
		assert!(answered.iter().all(|answer| answer.contains("label")), "{answered:?}");
		golden.record(sql, answered.join("\n"));
	}
	golden.check();
}

/// A statement the VM does not cover makes the statement holding it declined
/// whole: no subquery is planned apart from the program.
#[tokio::test]
async fn subq_an_uncovered_subquery_declines_the_statement_that_holds_it() {
	let db = TestDb::new_with_auth(SETUP).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	for sql in ["SELECT n, (SELECT * FROM item FOR UPDATE) AS rows FROM open"] {
		let bound = bind(&ctx, sql).await;
		assert!(
			super::plan(&planner(&ctx), &bound).await.is_none(),
			"`{sql}` is run by the VM with a statement planned apart inside it"
		);
	}
}

/// A plain `EXPLAIN` inside a statement is answered by the program with the
/// rendering of the plan it explains, which it holds as a constant: the access
/// path of the statement it explains.
#[tokio::test]
async fn subq_a_nested_explain_is_a_constant_of_the_program() {
	let mut golden = Golden::new("subq_a_nested_explain_is_a_constant_of_the_program");
	let db = TestDb::new_with_auth(SETUP).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	for (sql, names) in [
		(
			"SELECT n, (EXPLAIN SELECT * FROM item) AS plan FROM open",
			&["TableScan", "table: item"][..],
		),
		(
			"{ LET $plan = (EXPLAIN SELECT * FROM item WHERE n > 1); RETURN $plan }",
			&["TableScan", "table: item", "n > 1"][..],
		),
		(
			"RETURN [(EXPLAIN FORMAT JSON SELECT * FROM item), \
			 (EXPLAIN SELECT count() FROM item GROUP ALL)]",
			&["TableScan", "CountScan"][..],
		),
	] {
		let bound = bind(&ctx, sql).await;
		let vm = super::plan(&planner(&ctx), &bound)
			.await
			.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
		// A block answers through its `RETURN`, which the harness prints as an
		// error carrying the value's own printing.
		let answer = outcome(&vm, &ctx).await;
		golden.rows(sql, &answer);
		let answer = format!("{answer:?}");
		for name in names {
			assert!(answer.contains(name), "`{sql}` answers {answer}");
		}
	}
	golden.check();
}

/// Whether `program` tests membership of a statement's rows with an index.
fn tests_membership(program: &Program) -> bool {
	program.pipelines.iter().flat_map(|pipeline| &pipeline.code).any(|instr| {
		matches!(instr, super::program::Instr::Block(super::program::BlockInstr::Member { .. }))
	})
}

/// `x IN` and `x NOT IN` a statement run once are answered from a membership
/// index of its rows: a statement over rows the size of the table it is
/// tested against does not take a pass over the rows for each probe.
#[tokio::test]
async fn in_subquery_50k_by_50k() {
	let setup = "
		DEFINE TABLE t SCHEMALESS;
		DEFINE TABLE u SCHEMALESS;
		FOR $i IN 0..50000 { CREATE type::record('t', $i) SET k = $i };
		FOR $i IN 0..50000 { CREATE type::record('u', $i) SET k = $i };
	";
	let db = TestDb::new(setup).await;
	let ctx = db.exec_ctx().await;
	for (sql, count) in [
		(
			"SELECT count() FROM t WHERE k IN (SELECT VALUE k FROM u WHERE k % 2 = 0) GROUP ALL",
			25000,
		),
		(
			"SELECT count() FROM t WHERE k NOT IN (SELECT VALUE k FROM u WHERE k % 2 = 0) GROUP ALL",
			25000,
		),
		("SELECT count() FROM t WHERE k IN (SELECT VALUE k FROM u WHERE k < 10) GROUP ALL", 10),
	] {
		let program = lowered(&ctx, sql).await;
		assert!(tests_membership(&program), "`{sql}` keeps no membership index");
		let bound = bind(&ctx, sql).await;
		let vm = super::plan(&planner(&ctx), &bound).await.expect("plans");
		let started = web_time::Instant::now();
		let rows = outcome(&vm, &ctx).await.expect("answers");
		let took = started.elapsed();
		assert_eq!(rows.len(), 1, "`{sql}` answers {rows:?}");
		assert_eq!(rows[0].to_sql(), format!("{{ count: {count} }}"), "`{sql}`");
		// The structural assertion is the real check; this bound only catches a
		// pass over the rows for each probe, which takes minutes.
		assert!(took < std::time::Duration::from_secs(30), "`{sql}` took {took:?}");
	}
}

/// The values the membership tests probe with and keep, written as the
/// statements write them: every kind of number, the two nothings, strings, a
/// regex that matches some of them, a geometry, and collections and record
/// ids holding numbers.
const MEMBERS: &[&str] = &[
	"1",
	"1.0",
	"1dec",
	"-0.0",
	"0",
	"<float> 'NaN'",
	"NONE",
	"NULL",
	"'abc'",
	"'zzz'",
	"<regex> '^a'",
	"(1.0, 2.0)",
	"[1, 2.0]",
	"[1.0, 2]",
	"{ a: 1dec }",
	"{ a: 1 }",
	"t:1",
	"t:2",
];

/// The values among [`MEMBERS`] the kept side holds, a subset of them with
/// others that equal some by a different type and a regex.
const KEPT: &[&str] = &[
	"1dec",
	"-0.0",
	"<float> 'NaN'",
	"NONE",
	"NULL",
	"'abc'",
	"<regex> '^a'",
	"(1.0, 2.0)",
	"[1, 2.0]",
	"{ a: 1dec }",
	"t:1",
];

/// A membership test answers what `IN` and `NOT IN` over the array of the
/// rows answers, probe by probe, for numbers that equal across types, `NONE`
/// and `NULL`, a string a regex row matches, a regex probe, a geometry, and
/// nested values and record ids.
#[tokio::test]
async fn in_subquery_membership_matches_equal() {
	let mut setup = String::from("DEFINE TABLE s SCHEMALESS; DEFINE TABLE p SCHEMALESS;\n");
	for (at, kept) in KEPT.iter().enumerate() {
		setup.push_str(&format!("CREATE s:{at} SET v = {kept};\n"));
	}
	for (at, probe) in MEMBERS.iter().enumerate() {
		setup.push_str(&format!("CREATE p:{at} SET v = {probe};\n"));
	}
	let db = TestDb::new(&setup).await;
	let ctx = db.exec_ctx().await;
	for (member, test) in [
		(
			"SELECT VALUE id FROM p WHERE v IN (SELECT VALUE v FROM s) ORDER BY id",
			"SELECT VALUE id FROM p WHERE v IN $kept ORDER BY id",
		),
		(
			"SELECT VALUE id FROM p WHERE v NOT IN (SELECT VALUE v FROM s) ORDER BY id",
			"SELECT VALUE id FROM p WHERE v NOT IN $kept ORDER BY id",
		),
	] {
		let program = lowered(&ctx, member).await;
		assert!(tests_membership(&program), "`{member}` keeps no membership index");
		let by_index = db.datastore().execute(member, &TestDb::owner(), None).await.expect("runs");
		let by_array = db
			.datastore()
			.execute(
				&format!("LET $kept = (SELECT VALUE v FROM s); {test}"),
				&TestDb::owner(),
				None,
			)
			.await
			.expect("runs");
		let indexed = by_index[0].result.as_ref().map(|v| v.to_sql()).map_err(|e| e.to_string());
		let linear = by_array[1].result.as_ref().map(|v| v.to_sql()).map_err(|e| e.to_string());
		assert!(indexed.is_ok(), "`{member}` answers {indexed:?}");
		assert_eq!(indexed, linear, "`{member}` against the array of the rows");
		let ids = indexed.expect("answers");
		// Both sides of the test are exercised: some probe is a member, and some
		// is not.
		let members = ids.matches("p:").count();
		assert!(members > 0 && members < MEMBERS.len(), "`{member}` answers {ids}");
	}
}

/// What a membership index answers for each probe is what `contains` answers
/// of the array of its rows, for every pair of values of a corpus.
#[tokio::test]
async fn members_answer_what_contains_answers() {
	let db = TestDb::new("").await;
	let ctx = db.exec_ctx().await;
	let corpus: Vec<Value> = {
		let sql = format!("RETURN [{}]", MEMBERS.join(", "));
		let bound = bind(&ctx, &sql).await;
		let vm = super::plan(&planner(&ctx), &bound).await.expect("plans");
		match outcome(&vm, &ctx).await.expect("answers").into_iter().next() {
			Some(Value::Array(values)) => values.0,
			other => panic!("the corpus answers {other:?}"),
		}
	};
	let kept = [&corpus[..], &corpus[2..9], &corpus[..1]].concat();
	for rows in [corpus.clone(), kept, Vec::new()] {
		let members = super::membership::Members::keep(rows.clone(), &ctx).await.expect("kept");
		let array = Value::Array(crate::val::Array(rows));
		for probe in &corpus {
			assert_eq!(members.contains(probe), array.contains(probe), "{probe:?}");
		}
	}
}
