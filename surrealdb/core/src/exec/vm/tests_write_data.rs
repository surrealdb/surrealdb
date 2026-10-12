//! Tests of the data stages run as instructions: each statement runs on the VM
//! for the owner and for a checked record user, and its answers and written
//! records are the recorded ones.

use surrealdb_types::ToSql;

use super::golden::{Golden, mask_generated_keys as mask, render_outcome};
use super::tests::*;
use crate::exec::operators::test_util::TestDb;

/// Writes to the viewed table whose conditions and data clauses read computed
/// fields, guarded fields, and nothing at all, under every clause form.
pub(super) const DATA_WRITES: &[&str] = &[
	"UPDATE doc SET n += 1 WHERE twice > 2",
	"UPDATE doc SET n = n + 1, m = n WHERE veiled",
	"UPDATE doc SET copy = secret",
	"UPDATE doc SET copy = echoed, twin = twice",
	"UPDATE doc SET n = NONE",
	"UPDATE doc SET tags += 'a', tags += 'b'",
	"UPDATE doc SET x = $this.n, y = $parent",
	"UPDATE doc CONTENT { n: 10 }",
	"UPDATE doc MERGE { extra: twice, peek: secret }",
	"UPDATE doc REPLACE { n: 4 }",
	"UPDATE doc UNSET veiled, open",
	"UPDATE doc PATCH [{ op: 'replace', path: '/n', value: 9 }]",
	"UPDATE doc SET n = 1 WHERE (SELECT VALUE n FROM ONLY $this) > 1",
	"DELETE doc WHERE n > 1",
	"DELETE doc WHERE secret",
	"UPSERT doc:1 SET n = 7 WHERE n = 1",
	"UPSERT doc:5 SET n = 7",
	"UPSERT doc SET n = 8 WHERE n = 99",
	"CREATE doc:4 SET n = secret, m = twice",
	"CREATE doc:4 CONTENT { n: 3, secret: 'z' }",
	"CREATE doc SET n = 1",
	"INSERT INTO doc { id: doc:8, n: 8 }",
	"INSERT INTO doc [{ id: doc:1, n: 5 }, { id: doc:9, n: 9 }]",
	"UPDATE doc SET n = <int> 'x'",
	// A patch's pointers are judged against what the caller may read.
	"UPDATE doc PATCH [{ op: 'copy', from: '/secret', path: '/leak' }]",
	"UPDATE doc PATCH [{ op: 'move', from: '/secret', path: '/moved' }]",
	"UPDATE doc PATCH [{ op: 'test', path: '/secret', value: 's1' }]",
	"UPDATE doc PATCH [{ op: 'copy', from: '/n', path: '/m' }, { op: 'test', path: '/m', value: 1 }]",
	"UPDATE doc PATCH [{ op: 'add', path: '/list', value: [1] }, { op: 'copy', from: '/list/0', path: '/first' }]",
	"UPDATE doc PATCH [{ op: 'copy', from: '/missing', path: '/x' }]",
	"UPDATE doc PATCH [{ op: 'nonsense' }]",
	// `ON DUPLICATE KEY UPDATE` reads the row it attempted as `$input`.
	"INSERT INTO doc { id: doc:1, n: 5 } ON DUPLICATE KEY UPDATE n += $input.n, seen = $input",
	"INSERT INTO doc { id: doc:1, n: 5 } ON DUPLICATE KEY UPDATE copy = secret, got = $input.n",
	"INSERT INTO doc [{ id: doc:2, n: 1 }, { id: doc:6, n: 6 }] ON DUPLICATE KEY UPDATE n = $input.n * 100",
	"INSERT INTO doc { id: doc:1, n: 5 } ON DUPLICATE KEY UPDATE nested = (SELECT VALUE $input FROM ONLY 1)",
	// Places that embed expressions are evaluated where they are walked, with no
	// record in scope, and a filter judges one element at a time.
	"UPDATE doc SET a = [1, 2, 3], a[1 + 1] = 9",
	"UPDATE doc SET a = [1, 2, 3], a[1 + 1] += 9",
	"UPDATE doc SET a = [{ k: 1, v: 1 }, { k: 2, v: 2 }], a[WHERE k > 1].v = 7",
	"UPDATE doc SET a = [{ k: 1, v: 1 }, { k: 2, v: 2 }], a[WHERE k > 1].v += 7",
	"UPDATE doc SET a = [{ b: 1 }, { b: 2 }], a.*.b = 5",
	"UPDATE doc SET a = [{ b: 1 }, { b: 2 }], a[*].b += 5",
	"UPDATE doc SET a = { x: { y: 1 } }, a['x'].y += 2, a.x['z'] = 3",
	"UPDATE doc SET a = [1, 2, 3, 4], a[WHERE $this > 2] -= 1",
	"UPDATE doc SET a = [1, 2, 3, 4], a[WHERE $this > 1][0] = 9",
	"UPDATE doc SET a = [1, 2, 3, 4], a[WHERE $this > 1][0] = NONE",
	"UPDATE doc SET a = [3, 4, 5], a -= 4, a += 1, a +?= 3",
	"UPDATE doc SET a = [[1], [2]], a[0][0] += 1, a[$][0] = 7",
	"UPDATE doc SET a[1 + 1] = 1, b.c['d'] = 2",
	"UPDATE doc SET a = [1, 2], a[<int> 'x'] = 3",
	"UPDATE doc SET a[<int> 'x'] = 3, b = <int> 'y'",
	"UPDATE doc SET a = [1, 2, 3], a[1 + 0] = NONE",
	"UPDATE doc UNSET veiled, tags[0], tags[WHERE $this = 1]",
	"UPSERT doc:50 SET a = [{ k: 1 }], a[WHERE k = 1].v = 3",
	"UPDATE doc SET a = [{ k: 1 }] RETURN a, $before.n",
	"CREATE doc:60 SET a.b[WHERE x = 1].c = 2",
	"INSERT INTO doc { id: doc:1, n: 5 } ON DUPLICATE KEY UPDATE a = [1, 2], a[0 + 1] = $input.n",
	// An `INSERT`'s rows, in each spelling, and where they land.
	"INSERT INTO doc (id, n, tags) VALUES (doc:20, 1, [1]), (doc:21, 2 + 3, [2])",
	"INSERT INTO doc (id, a.b, a.c[0]) VALUES (doc:22, 1, 2)",
	"INSERT INTO doc (id, a[1 + 1]) VALUES (doc:24, 5)",
	"INSERT INTO doc (id, n) VALUES (doc:25, 1), (doc:25, 2)",
	"INSERT INTO doc (id, a[<int> 'x']) VALUES (doc:26, 5)",
	"INSERT INTO $session { id: doc:1 }",
	"INSERT INTO doc 5",
	"INSERT INTO other (SELECT n + 10 AS n FROM doc)",
	"INSERT [{ id: doc:31, n: 3 }, { id: other:31, n: 4 }]",
	"INSERT INTO doc { id: doc:32, n: 3 + 4 }",
	"INSERT RELATION INTO doc { in: doc:1, out: doc:2, id: doc:33 }",
	"INSERT RELATION INTO doc [{ in: doc:1, out: doc:2 }, { in: doc:2, out: doc:1 }]",
	"INSERT RELATION INTO doc { in: doc:1 }",
	// Targets and endpoints that are computed.
	"RELATE (SELECT VALUE id FROM doc:1)->doc->doc:2 SET n = 1",
	"RELATE doc:1->(type::record('doc', 'e1'))->(SELECT VALUE id FROM doc:2) SET a = [1], a[0] += 1",
	"CREATE |doc:3| SET n = 1",
	"CREATE |doc:5..7| SET n = 2",
	"UPDATE |doc:1..3| SET n = 9",
	// The special fields a clause names are held to the record's.
	"CREATE doc:4 CONTENT { id: doc:5 }",
	"CREATE doc:4 CONTENT { id: doc:4, n: 1 }",
	"CREATE doc:4 SET id = 4",
	"CREATE doc CONTENT { id: doc:12, n: 1 }",
	"UPDATE doc:1 CONTENT { id: doc:2 }",
	"RELATE doc:1->doc->doc:2 SET n = 1",
	"RELATE doc:1->doc->doc:2 CONTENT { in: doc:9 }",
	"RELATE doc:1->doc:77->doc:2 CONTENT { id: doc:77, out: doc:2 }",
];

/// The data stages as instructions answer and write their records, for the
/// owner and for a record user whose reads are checked, and every write runs
/// them as data instructions.
#[tokio::test]
async fn the_data_stages_write_their_records() {
	let mut golden = Golden::new("the_data_stages_write_their_records");
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	let reader = crate::dbs::Session::for_record("test", "test", "user", rid);
	for (who, session) in [("owner", TestDb::owner()), ("record user", reader)] {
		for sql in DATA_WRITES {
			let (rows, state) = written_as(sql, &session).await;
			golden.record(format!("{who}: {sql}"), render_masked(&rows, &state));
			if who != "owner"
				&& let Ok(rows) = &rows
			{
				let shown = mask(&render_outcome(&Ok(rows.clone())));
				assert!(!shown.contains("s1"), "`{sql}` shows the {who} a guarded field: {shown}");
			}
			let db = TestDb::new_with_auth(VIEWED_SCHEMA).await;
			let ctx = db.exec_ctx_as(&session, crate::kvs::TransactionType::Write).await;
			let bound = bind(&ctx, sql).await;
			let physical = super::optimize::optimize(
				super::map::map(&planner(&ctx), &bound).await.expect("maps"),
			);
			let program = super::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
			assert!(
				super::tests::write_families(
					&super::tests::with_stages(&planner(&ctx), &program).await
				)
				.contains(&"data"),
				"`{sql}` for the {who} runs no data instruction"
			);
			ctx.txn().cancel().await.expect("cancel");
		}
	}
	golden.check();
}

/// A reader is refused a patch pointer into a field it may not select, and
/// the owner is not.
#[tokio::test]
async fn a_patch_pointer_into_a_guarded_field_is_refused_to_the_reader() {
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	let reader = crate::dbs::Session::for_record("test", "test", "user", rid);
	let sql = "UPDATE doc:1 PATCH [{ op: 'copy', from: '/secret', path: '/leak' }]";
	let (rows, _) = written_as(sql, &reader).await;
	let refused = rows.expect_err("the reader is refused");
	assert!(refused.contains("secret"), "the refusal names the pointer: {refused}");
	let (rows, state) = written_as(sql, &TestDb::owner()).await;
	assert!(rows.is_ok(), "the owner may copy the field: {rows:?}");
	assert!(state.iter().any(|row| row.contains("leak")), "the copy landed: {state:?}");
}

/// Tables whose `id` field declares a kind or a `DEFAULT`.
pub(super) const ID_SCHEMA: &str = "
	DEFINE TABLE fixed PERMISSIONS FULL;
	DEFINE FIELD id ON fixed TYPE int DEFAULT 42;
	DEFINE TABLE derived PERMISSIONS FULL;
	DEFINE FIELD id ON derived DEFAULT n * 10;
	DEFINE TABLE typed PERMISSIONS FULL;
	DEFINE FIELD id ON typed TYPE int;
	DEFINE TABLE keyed PERMISSIONS FULL;
	DEFINE FIELD id ON keyed TYPE string;
	CREATE derived:1 SET n = 1;
";

/// Writes whose id comes from each source: the record, the clause, the
/// `DEFAULT`, a synthesised key, or none that the declared kind admits.
pub(super) const ID_WRITES: &[(&str, &str)] = &[
	("fixed", "CREATE fixed SET a = 1"),
	("fixed", "CREATE fixed SET a = 1; CREATE fixed SET a = 2"),
	("fixed", "CREATE fixed CONTENT { id: 7, a: 1 }"),
	("fixed", "CREATE fixed CONTENT { id: 'x' }"),
	("derived", "CREATE derived SET n = 3"),
	("derived", "INSERT INTO derived { n: 4 }"),
	("derived", "CREATE derived SET n = 'a'"),
	("derived", "CREATE derived:9 SET n = 1"),
	("typed", "CREATE typed SET a = 1"),
	("typed", "CREATE typed:5 SET a = 1"),
	("typed", "CREATE typed:five SET a = 1"),
	("keyed", "CREATE keyed:1 SET a = 1"),
	("keyed", "CREATE keyed SET a = 1"),
];

/// Run `sql` against a fresh [`ID_SCHEMA`] as the owner and answer its rows and
/// the records of `table` after.
async fn written_in(
	table: &str,
	sql: &str,
) -> (Result<Vec<crate::val::Value>, String>, Vec<String>) {
	let db = TestDb::new_with_auth(ID_SCHEMA).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	let mut rows = Ok(Vec::new());
	for statement in sql.split("; ") {
		let bound = bind(&ctx, statement).await;
		let op = super::plan(&planner(&ctx), &bound)
			.await
			.unwrap_or_else(|| panic!("the VM declined `{statement}`"));
		rows = outcome(&op, &ctx).await;
		if rows.is_err() {
			break;
		}
	}
	let txn = ctx.txn();
	if rows.is_ok() {
		txn.commit().await.expect("commit");
	} else {
		txn.cancel().await.expect("cancel");
	}
	let read = db.exec_ctx().await;
	let op =
		crate::exec::operators::test_util::plan_statement(&format!("SELECT * FROM {table}"), &read)
			.await;
	let mut state: Vec<String> = outcome(&op, &read)
		.await
		.expect("the state reads")
		.into_iter()
		.map(|row| row.to_sql())
		.collect();
	state.sort();
	(rows, state)
}

/// A record's id is settled from the record, the clause or the `DEFAULT`, with
/// the id derivation run as instructions.
#[tokio::test]
async fn an_id_is_derived_from_the_record_the_clause_or_the_default() {
	let mut golden = Golden::new("an_id_is_derived_from_the_record_the_clause_or_the_default");
	for (table, sql) in ID_WRITES {
		let vm = written_in(table, sql).await;
		golden.record(*sql, render_masked(&vm.0, &vm.1));
		// The `DEFAULT` is what names these records.
		let expected = match *sql {
			"CREATE fixed SET a = 1" => Some(42),
			"CREATE derived SET n = 3" => Some(30),
			"INSERT INTO derived { n: 4 }" => Some(40),
			_ => None,
		};
		if let Some(key) = expected {
			let shown = vm.1.join("\n");
			let id = format!("{table}:{key}");
			assert!(shown.contains(&id), "`{sql}` wrote {shown}");
		}
		let db = TestDb::new_with_auth(ID_SCHEMA).await;
		let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
		let last = sql.rsplit("; ").next().expect("a statement");
		let bound = bind(&ctx, last).await;
		let physical =
			super::optimize::optimize(super::map::map(&planner(&ctx), &bound).await.expect("maps"));
		let program = super::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
		assert!(
			super::tests::write_families(
				&super::tests::with_stages(&planner(&ctx), &program).await
			)
			.contains(&"data"),
			"`{sql}` runs no data instruction"
		);
		ctx.txn().cancel().await.expect("cancel");
	}
	golden.check();
}

/// A run's rows and records as a golden body, with every generated record key
/// read as one placeholder; the records are sorted after the placeholders are
/// set, so the order does not depend on the keys drawn.
fn render_masked(rows: &Result<Vec<crate::val::Value>, String>, state: &[String]) -> String {
	let mut state: Vec<String> = state.iter().map(|line| mask(line)).collect();
	state.sort();
	format!("{}\nrecords:\n{}", mask(&render_outcome(rows)), state.join("\n"))
}

/// Writes whose condition, data clauses, places and `RETURN` all lower to
/// instructions, over a table with no schema of its own, each beside the same
/// write with none of its clauses.
const CLAUSE_WRITES: &[(&str, &str)] = &[
	("UPDATE plain SET n += 1, m = n * 2 WHERE n > 1 RETURN n, m", "UPDATE plain"),
	("UPDATE plain SET a = [1, 2, 3], a[1 + 1] = 9", "UPDATE plain"),
	("UPDATE plain UNSET n, a", "UPDATE plain"),
	("UPDATE plain REPLACE { n: 4 }", "UPDATE plain"),
	("CREATE plain:1 CONTENT { n: 1 } RETURN AFTER", "CREATE plain:1"),
	("UPSERT plain:2 MERGE { n: 2 }", "UPSERT plain:2"),
	(
		"INSERT INTO plain { id: plain:3, n: 3 } ON DUPLICATE KEY UPDATE n += $input.n",
		"INSERT INTO plain { id: plain:3, n: 3 }",
	),
	("DELETE plain WHERE n = 1 RETURN BEFORE", "DELETE plain"),
];

/// How many binds, plans and expression compiles the planner makes while
/// `sql` is mapped and lowered, asserting it lowers. The node's units are not
/// consulted, so every statement lowers what it reaches, as the first of its
/// kind on a node does.
async fn lowering_compiles(db: &TestDb, sql: &str) -> usize {
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	let bound = bind(&ctx, sql).await;
	crate::exec::planner::COMPILES.with(|compiles| compiles.set(0));
	super::units::OFF.with(|off| off.set(true));
	let physical =
		super::optimize::optimize(super::map::map(&planner(&ctx), &bound).await.expect("maps"));
	super::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
	super::units::OFF.with(|off| off.set(false));
	let count = crate::exec::planner::COMPILES.with(std::cell::Cell::get);
	ctx.txn().cancel().await.expect("cancel");
	count
}

/// A write's clauses are lowered once, to the instructions that evaluate
/// them: lowering a write with clauses makes no planner compilation the same
/// write without them does not. A write without clauses still binds the
/// statements of its cascade units as they are lowered.
#[tokio::test]
async fn a_write_lowers_its_clauses_without_compiling_them() {
	let db = TestDb::new("DEFINE TABLE plain PERMISSIONS FULL").await;
	let mut compiled = Vec::new();
	for (sql, bare) in CLAUSE_WRITES {
		let with = lowering_compiles(&db, sql).await;
		let without = lowering_compiles(&db, bare).await;
		if with != without {
			compiled.push((*sql, with, without));
		}
	}
	assert!(
		compiled.is_empty(),
		"lowering a write compiled its clauses (statement, with, without): {compiled:?}"
	);
}

/// Tables whose `id` names its components, beside tables whose does not: a
/// string and an int component, a `DEFAULT` that completes a key, a `DEFAULT`
/// that yields nothing, a table with no `id` field of its own and one whose
/// `id` is a tuple type with no names.
pub(super) const KEYED_SCHEMA: &str = "
	DEFINE TABLE reading PERMISSIONS FULL;
	DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
	DEFINE TABLE counter PERMISSIONS FULL;
	DEFINE FIELD id ON counter TYPE [site: string, n: int];
	DEFINE TABLE defaulted PERMISSIONS FULL;
	DEFINE FIELD id ON defaulted TYPE [city: string, time: datetime] DEFAULT ['London', d'2024-01-01T00:00:00Z'];
	DEFINE TABLE nothing PERMISSIONS FULL;
	DEFINE FIELD id ON nothing TYPE [city: string, time: datetime] DEFAULT NONE;
	DEFINE TABLE plain PERMISSIONS FULL;
	DEFINE TABLE tupled PERMISSIONS FULL;
	DEFINE FIELD id ON tupled TYPE [string, datetime];
";

/// The statements a table of [`KEYED_SCHEMA`] is written by, in the forms the
/// census lowers: one of each order that settles a record's id and its fields.
const KEYED_STATEMENTS: &[&str] = &[
	"CREATE {} SET a = 1",
	"CREATE {}:1 SET a = 1",
	"CREATE {} CONTENT { a: 1 }",
	"UPDATE {} SET a = 1",
	"UPDATE {}:1 SET a = 1",
	"UPSERT {} SET a = 1",
	"UPSERT {}:1 SET a = 1",
	"INSERT INTO {} { a: 1 }",
	"INSERT INTO {} { a: 1 } ON DUPLICATE KEY UPDATE a = 2",
];

/// Whether a run of `program` judges the fields that name the components of a
/// record id: whether any of its pipelines, host pipelines or units holds
/// [`DataInstr::SettleKeyFields`].
fn settles_keys(program: &super::program::Program) -> bool {
	use super::program::{Instr, WriteInstr};
	use super::write::DataInstr;
	program
		.pipelines
		.iter()
		.chain(&program.host_code)
		.flat_map(|pipeline| pipeline.code.iter())
		.any(|instr| matches!(instr, Instr::Write(WriteInstr::Data(DataInstr::SettleKeyFields))))
		|| program.units.iter().any(|unit| settles_keys(unit))
}

/// The program `sql` lowers to as the owner writes it, and the same with the
/// stage bundles a write that reaches any table loads by name appended.
async fn lowered_for_census(
	db: &TestDb,
	sql: &str,
) -> (super::program::Program, super::program::Program) {
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	let bound = bind(&ctx, sql).await;
	let physical =
		super::optimize::optimize(super::map::map(&planner(&ctx), &bound).await.expect("maps"));
	let program = super::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
	let staged = with_stages(&planner(&ctx), &program).await;
	ctx.txn().cancel().await.expect("cancel");
	(program, staged)
}

/// A write to a table whose `id` names its components runs the instruction that
/// judges them, and a write to any other table runs none: an unkeyed table pays
/// nothing for the feature, whether its `id` is undeclared or is a tuple type
/// with no names. A delete has no data stage that settles fields, keyed or not.
#[tokio::test]
async fn only_a_write_to_a_keyed_table_settles_key_fields() {
	let db = TestDb::new_with_auth(KEYED_SCHEMA).await;
	for statement in KEYED_STATEMENTS {
		for (table, keyed) in [
			("reading", true),
			("counter", true),
			("defaulted", true),
			("plain", false),
			("tupled", false),
		] {
			let sql = statement.replace("{}", table);
			let (program, staged) = lowered_for_census(&db, &sql).await;
			if keyed {
				assert!(settles_keys(&staged), "`{sql}` does not settle key fields");
			} else {
				assert!(!settles_keys(&program), "`{sql}` settles key fields");
			}
		}
	}
	for table in ["reading", "plain"] {
		let sql = format!("DELETE {table}");
		let (program, staged) = lowered_for_census(&db, &sql).await;
		assert!(!settles_keys(&program), "`{sql}` settles key fields");
		assert!(!settles_keys(&staged), "`{sql}` settles key fields once its stages are loaded");
	}
}

/// What a keyed write is expected to leave.
enum Left {
	/// The write answers and the table holds this many records.
	Records(usize),
	/// The write is refused with a message holding the text. Nothing it did is
	/// kept, so the table is as it was.
	Refused(&'static str),
}

/// The records of `table` as stored, each with its id, after `sql` runs as the
/// owner against a fresh [`KEYED_SCHEMA`]: the statements are separated by `; `,
/// the run stops at the first that fails, and what it left is committed only
/// when none did. Answers the last outcome and the records.
async fn written_keyed(
	table: &str,
	sql: &str,
) -> (Result<Vec<crate::val::Value>, String>, Vec<(crate::val::RecordId, crate::val::Value)>) {
	use crate::catalog::providers::TableProvider;
	let db = TestDb::new_with_auth(KEYED_SCHEMA).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	let mut outcome_of_last = Ok(Vec::new());
	for statement in sql.split("; ") {
		let bound = bind(&ctx, statement).await;
		let op = super::plan(&planner(&ctx), &bound)
			.await
			.unwrap_or_else(|| panic!("the VM declined `{statement}`"));
		outcome_of_last = outcome(&op, &ctx).await;
		if outcome_of_last.is_err() {
			break;
		}
	}
	let txn = ctx.txn();
	if outcome_of_last.is_ok() {
		txn.commit().await.expect("commit");
	} else {
		txn.cancel().await.expect("cancel");
	}
	let read = db.exec_ctx().await;
	let op = crate::exec::operators::test_util::plan_statement(
		&format!("SELECT VALUE id FROM {table}"),
		&read,
	)
	.await;
	let ids = outcome(&op, &read).await.expect("the ids read");
	let db_ctx = read.database().expect("a database");
	let (ns, dbid) = (db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id);
	let txn = read.txn();
	let mut stored = Vec::new();
	for id in ids {
		let crate::val::Value::RecordId(rid) = id else {
			panic!("an id that is no record id: {id:?}");
		};
		let record = txn.get_record(ns, dbid, &rid.table, &rid.key, None).await.expect("reads");
		stored.push((rid, record.data.clone()));
	}
	(outcome_of_last, stored)
}

/// The names of the components of each keyed table of [`KEYED_SCHEMA`], which no
/// record of it ever stores.
fn component_names(table: &str) -> &'static [&'static str] {
	match table {
		"reading" | "defaulted" | "nothing" => &["city", "time"],
		"counter" => &["site", "n"],
		_ => &[],
	}
}

const SEED: &str = "CREATE reading:['London', d'2024-01-01T00:00:00Z'] SET temp = 1";

/// Writes to the keyed tables, each with what it leaves. A record is stored
/// without the fields that name the components of its id, whichever way they
/// were written, and a refused write leaves the table as it was.
fn keyed_writes() -> Vec<(&'static str, String, Left)> {
	let london = "d'2024-01-01T00:00:00Z'";
	let seeded = |sql: &str| format!("{SEED}; {sql}");
	vec![
		(
			"reading",
			format!("CREATE reading SET city = 'London', time = {london}, temp = 1"),
			Left::Records(1),
		),
		(
			"reading",
			format!("CREATE reading CONTENT {{ city: 'London', time: {london}, temp: 1 }}"),
			Left::Records(1),
		),
		(
			"reading",
			format!("INSERT INTO reading {{ city: 'London', time: {london}, temp: 1 }}"),
			Left::Records(1),
		),
		(
			"reading",
			format!(
				"INSERT INTO reading [{{ city: 'London', time: {london} }}, {{ city: 'Paris', time: {london} }}]"
			),
			Left::Records(2),
		),
		(
			"reading",
			format!("INSERT INTO reading (city, time, temp) VALUES ('Rome', {london}, 1)"),
			Left::Records(1),
		),
		("reading", format!("CREATE reading:['London', {london}] SET temp = 1"), Left::Records(1)),
		(
			"reading",
			format!("CREATE reading:['London', {london}] SET city = 'London', time = {london}"),
			Left::Records(1),
		),
		(
			"reading",
			format!("CREATE reading:['London', {london}] SET city = 'Paris'"),
			Left::Refused("part of the record id of `reading`"),
		),
		(
			"reading",
			"CREATE reading SET city = 'London'".to_owned(),
			Left::Refused("record id component `time`"),
		),
		(
			"reading",
			"CREATE reading SET temp = 1".to_owned(),
			Left::Refused("record id component `city`"),
		),
		(
			"reading",
			"INSERT IGNORE INTO reading { temp: 1 }".to_owned(),
			Left::Refused("record id component `city`"),
		),
		(
			"reading",
			"CREATE reading:['London'] SET temp = 1".to_owned(),
			Left::Refused("1 value was given"),
		),
		(
			"reading",
			format!("CREATE reading:['London', {london}, 3] SET temp = 1"),
			Left::Refused("3 values were given"),
		),
		(
			"reading",
			"CREATE reading:['London', 5] SET temp = 1".to_owned(),
			Left::Refused("record id component `time`"),
		),
		(
			"reading",
			"CREATE reading SET city = 'London', time = '2024-01-01T00:00:00Z'".to_owned(),
			Left::Refused("record id component `time`"),
		),
		(
			"reading",
			format!(
				"UPSERT reading SET city = 'London', time = {london}, temp = 1; UPSERT reading SET city = 'London', time = {london}, temp = 2"
			),
			Left::Records(1),
		),
		("reading", seeded("UPDATE reading SET temp = 2"), Left::Records(1)),
		("reading", seeded("UPDATE reading SET city = 'London'"), Left::Records(1)),
		(
			"reading",
			seeded("UPDATE reading SET city = 'Paris'"),
			Left::Refused("Found 'Paris' for the `city` field"),
		),
		(
			"reading",
			seeded("UPDATE reading UNSET city"),
			Left::Refused("Found NONE for the `city` field"),
		),
		(
			"reading",
			seeded("UPDATE reading SET city = NONE"),
			Left::Refused("Found NONE for the `city` field"),
		),
		(
			"reading",
			seeded("UPDATE reading SET city += 'x'"),
			Left::Refused("Found 'Londonx' for the `city` field"),
		),
		("reading", seeded("UPDATE reading SET city.x = 1"), Left::Refused("for the `city` field")),
		(
			"reading",
			seeded("UPDATE reading MERGE { city: 'Paris' }"),
			Left::Refused("Found 'Paris' for the `city` field"),
		),
		("reading", seeded("UPDATE reading MERGE { city: 'London', temp: 3 }"), Left::Records(1)),
		(
			"reading",
			seeded("UPDATE reading PATCH [{ op: 'replace', path: '/city', value: 'Paris' }]"),
			Left::Refused("Found 'Paris' for the `city` field"),
		),
		(
			"reading",
			seeded("UPDATE reading PATCH [{ op: 'remove', path: '/city' }]"),
			Left::Refused("Found NONE for the `city` field"),
		),
		(
			"reading",
			seeded(&format!(
				"UPDATE reading CONTENT (SELECT * FROM ONLY reading:['London', {london}])"
			)),
			Left::Records(1),
		),
		(
			"reading",
			seeded(&format!("UPDATE reading REPLACE {{ city: 'Paris', time: {london} }}")),
			Left::Refused("Found 'Paris' for the `city` field"),
		),
		(
			"reading",
			seeded(&format!(
				"INSERT INTO reading {{ city: 'London', time: {london}, temp: 2 }} ON DUPLICATE KEY UPDATE temp = $input.temp"
			)),
			Left::Records(1),
		),
		(
			"reading",
			seeded(&format!(
				"INSERT INTO reading {{ city: 'London', time: {london}, temp: 2 }} ON DUPLICATE KEY UPDATE city = 'Paris'"
			)),
			Left::Refused("Found 'Paris' for the `city` field"),
		),
		("counter", "CREATE counter SET site = 'a', n = 5.0".to_owned(), Left::Records(1)),
		("counter", "CREATE counter SET site = 'a', n = 5dec".to_owned(), Left::Records(1)),
		(
			"counter",
			"CREATE counter SET site = 'a', n = 5.5".to_owned(),
			Left::Refused("record id component `n`"),
		),
		(
			"counter",
			"CREATE counter SET site = 'a', n = 1e30".to_owned(),
			Left::Refused("record id component `n`"),
		),
		("counter", "CREATE counter:['a', 5] SET n = 5.0".to_owned(), Left::Records(1)),
		(
			"counter",
			"CREATE counter:['a', 5] SET n = 6".to_owned(),
			Left::Refused("Found 6 for the `n` field"),
		),
		("defaulted", "CREATE defaulted SET temp = 1".to_owned(), Left::Records(1)),
		(
			"defaulted",
			"CREATE defaulted SET city = 'Paris', time = d'2025-01-01T00:00:00Z'".to_owned(),
			Left::Records(1),
		),
		(
			"defaulted",
			"CREATE defaulted SET city = 'Paris'".to_owned(),
			Left::Refused("Found 'Paris' for the `city` field"),
		),
		(
			"nothing",
			"CREATE nothing SET temp = 1".to_owned(),
			Left::Refused("record id component `city`"),
		),
		// A `DEFAULT` yielding nothing names the component the record lacks.
		(
			"nothing",
			"CREATE nothing SET city = 'London'".to_owned(),
			Left::Refused("record id component `time`"),
		),
		// An edge written into a table that is not a relation table takes its
		// key from its components as any other record does.
		(
			"reading",
			format!(
				"CREATE person:a; CREATE person:b; RELATE person:a->reading->person:b SET city = 'London', time = {london}, temp = 1"
			),
			Left::Records(1),
		),
		(
			"reading",
			"CREATE person:a; CREATE person:b; RELATE person:a->reading->person:b SET city = 'London', temp = 1".to_owned(),
			Left::Refused("record id component `time`"),
		),
	]
}

/// A keyed write builds its key from the components it is given, holds every
/// element to its component, stores none of the fields that name them, and
/// judges what a statement restates against the key.
#[tokio::test]
async fn keyed_writes_settle_their_components_as_instructions() {
	for (table, sql, left) in keyed_writes() {
		let (rows, stored) = written_keyed(table, &sql).await;
		match left {
			Left::Records(count) => {
				assert!(rows.is_ok(), "`{sql}` was refused: {rows:?}");
				assert_eq!(stored.len(), count, "`{sql}` left {stored:?}");
				for (rid, body) in &stored {
					let crate::val::Value::Object(body) = body else {
						panic!("`{sql}` stored the non-object {body:?} at {}", rid.to_sql());
					};
					for name in component_names(table) {
						assert!(
							body.get(name).is_none(),
							"`{sql}` stored `{name}` in {}: {}",
							rid.to_sql(),
							body.to_sql()
						);
					}
				}
			}
			Left::Refused(text) => {
				let message = rows.expect_err(&format!("`{sql}` was not refused"));
				assert!(message.contains(text), "`{sql}` was refused with `{message}`");
				assert!(stored.is_empty(), "`{sql}` left {stored:?}");
			}
		}
	}
}

/// An int component of a key is an int, however the number was written, and a
/// key that was built from the components is the key an explicit id names.
#[tokio::test]
async fn an_int_component_of_a_written_key_is_an_int() {
	use crate::val::{Number, RecordIdKey, Value};
	for sql in [
		"CREATE counter SET site = 'a', n = 5.0",
		"CREATE counter SET site = 'a', n = 5dec",
		"CREATE counter SET site = 'a', n = 5",
		"CREATE counter:['a', 5.0] SET v = 1",
		"INSERT INTO counter { id: counter:['a', 5dec] }",
	] {
		let (rows, stored) = written_keyed("counter", sql).await;
		assert!(rows.is_ok(), "`{sql}` was refused: {rows:?}");
		let [(rid, _)] = stored.as_slice() else {
			panic!("`{sql}` left {stored:?}");
		};
		let RecordIdKey::Array(elements) = &rid.key else {
			panic!("`{sql}` made the key {}", rid.to_sql());
		};
		assert!(
			matches!(elements.0.as_slice(), [Value::String(_), Value::Number(Number::Int(5))]),
			"`{sql}` made the key {}",
			rid.to_sql()
		);
	}
}

/// A table that names no components stores the fields a statement writes,
/// whatever they are called: the names of another table's components are
/// ordinary fields here.
#[tokio::test]
async fn an_unkeyed_table_stores_the_fields_it_is_given() {
	for table in ["plain", "tupled"] {
		let sql = format!("CREATE {table}:['London', d'2024-01-01T00:00:00Z'] SET city = 'London'");
		let (rows, stored) = written_keyed(table, &sql).await;
		assert!(rows.is_ok(), "`{sql}` was refused: {rows:?}");
		let [(_, body)] = stored.as_slice() else {
			panic!("`{sql}` left {stored:?}");
		};
		let crate::val::Value::Object(body) = body else {
			panic!("`{sql}` stored {body:?}");
		};
		assert!(body.get("city").is_some(), "`{sql}` stored {}", body.to_sql());
	}
}
