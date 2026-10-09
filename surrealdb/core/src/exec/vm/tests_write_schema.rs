//! Tests of the schema stages run as instructions: each statement runs on the
//! VM, for the owner and for a checked record user, and its answers and written
//! records are the recorded ones.

use surrealdb_types::ToSql;

use super::golden::{Golden, mask_generated_keys};
use super::tests::{bind, outcome, planner, render_written};
use crate::exec::operators::test_util::TestDb;
use crate::val::Value;

/// Tables whose fields declare every clause the schema stages apply, and one
/// whose computed fields an event observes.
pub(super) const FIELD_SCHEMA: &str = "
	DEFINE TABLE f SCHEMAFULL PERMISSIONS FULL;
	DEFINE FIELD n ON f TYPE int;
	DEFINE FIELD d ON f TYPE string DEFAULT 'dflt';
	DEFINE FIELD always ON f TYPE int DEFAULT ALWAYS 7;
	DEFINE FIELD v ON f VALUE string::uppercase($value ?? 'x');
	DEFINE FIELD sv ON f VALUE 'static';
	DEFINE FIELD b ON f VALUE $before ?? 'fresh';
	DEFINE FIELD i ON f VALUE $input ?? 'none';
	DEFINE FIELD a ON f TYPE option<int> ASSERT $value > 0;
	DEFINE FIELD pair ON f TYPE option<int> ASSERT $value != n;
	DEFINE FIELD ro ON f TYPE option<string> READONLY;
	DEFINE FIELD obj ON f TYPE option<object>;
	DEFINE FIELD obj.x ON f TYPE int DEFAULT 1;
	DEFINE FIELD arr ON f TYPE array<object> DEFAULT [];
	DEFINE FIELD arr[*].k ON f TYPE string;
	DEFINE FIELD flex ON f TYPE option<object> FLEXIBLE;
	DEFINE FIELD locked ON f TYPE option<string> PERMISSIONS FOR update NONE;
	DEFINE FIELD sealed ON f TYPE option<string> PERMISSIONS FOR create NONE;
	DEFINE FIELD s ON f TYPE option<set<int>>;
	DEFINE FIELD twice ON f COMPUTED n * 2;
	CREATE f:1 SET n = 1, ro = 'r', a = 5, locked = 'l';
	DEFINE TABLE e PERMISSIONS FULL;
	DEFINE FIELD n ON e TYPE int;
	DEFINE FIELD twice ON e COMPUTED n * 2;
	DEFINE FIELD more ON e COMPUTED twice + 1;
	DEFINE EVENT log ON e THEN { CREATE elog SET after = $after.more, before = $before.more, event = $event };
	CREATE e:1 SET n = 1;
	DEFINE TABLE loose PERMISSIONS FULL;
	DEFINE FIELD n ON loose TYPE int DEFAULT 3;
";

/// The tables whose records a statement is judged by.
const FIELD_TABLES: &[&str] = &["f", "e", "elog", "loose"];

/// Writes that reach every clause of [`FIELD_SCHEMA`].
pub(super) const FIELD_WRITES: &[&str] = &[
	"CREATE f:10 SET n = 10, ro = 'r', a = 5, locked = 'l', sealed = 's'",
	"CREATE f:11 SET n = '2'",
	"CREATE f:12 SET n = 12, a = -1",
	"CREATE f:13 SET n = 13, a = 13, pair = 13",
	"CREATE f:14 SET n = 14, bogus = 1",
	"CREATE f:15 SET n = 15, obj = { x: 2 }, arr = [{ k: 'a' }, { k: 'b' }], flex = { any: 1, deep: { x: 1 } }, s = [1, 1, 2]",
	"CREATE f:16 CONTENT { n: 16, v: 'lower' }",
	"CREATE f:17 SET n = 17, twice = 99",
	"CREATE f:18 SET n = 18, arr = [{ k: 1 }]",
	"CREATE f:19 SET n = 19, obj = {}",
	"CREATE f:20 SET n = 20, flex = { x: 1 }, obj = { x: 1, y: 2 }",
	"UPDATE f:1 SET ro = 'changed'",
	"UPDATE f:1 SET n = 10",
	"UPDATE f:1 CONTENT { n: 11 }",
	"UPDATE f:1 CONTENT { n: 11, arr: [] }",
	"UPDATE f:1 SET locked = 'new', sealed = 'x'",
	"UPDATE f:1 SET a = NONE",
	"UPDATE f:1 SET obj = NONE",
	"UPDATE f:1 SET v = 'again', d = NONE",
	"UPDATE f:1 MERGE { n: 30, d: 'kept' }",
	"UPSERT f:9 SET n = 9",
	"INSERT INTO f { id: f:8, n: 8 }",
	"INSERT INTO f { id: f:1, n: 8 } ON DUPLICATE KEY UPDATE n += 1",
	"UPDATE f SET n += 1",
	"DELETE f:1",
	"CREATE e:2 SET n = 2",
	"UPDATE e:1 SET n = 5",
	"DELETE e:1",
	"CREATE loose:1",
	"CREATE loose:2 SET n = 1, extra = NONE, nested = { gone: NONE, kept: 1 }",
];

/// Run `sql` against a fresh [`FIELD_SCHEMA`] as `session` and answer its rows
/// and the records of [`FIELD_TABLES`] after.
async fn written_with(
	sql: &str,
	session: &crate::dbs::Session,
) -> (Result<Vec<Value>, String>, Vec<String>) {
	written_on(FIELD_SCHEMA, FIELD_TABLES, sql, session).await
}

/// Run `sql` against a fresh `schema` as `session` and answer its rows and the
/// records of `tables` after.
async fn written_on(
	schema: &str,
	tables: &[&str],
	sql: &str,
	session: &crate::dbs::Session,
) -> (Result<Vec<Value>, String>, Vec<String>) {
	let db = fixture(schema).await;
	let ctx = db.exec_ctx_as(session, crate::kvs::TransactionType::Write).await;
	let bound = bind(&ctx, sql).await;
	let op = super::plan(&planner(&ctx), &bound)
		.await
		.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
	let rows = outcome(&op, &ctx).await;
	let txn = ctx.txn();
	if rows.is_ok() {
		txn.commit().await.expect("commit");
	} else {
		txn.cancel().await.expect("cancel");
	}
	let read = db.exec_ctx().await;
	let mut state = Vec::new();
	for table in tables {
		let op = crate::exec::operators::test_util::plan_statement(
			&format!("SELECT * FROM {table}"),
			&read,
		)
		.await;
		let mut rows: Vec<String> = outcome(&op, &read)
			.await
			.expect("the state reads")
			.into_iter()
			.map(|row| format!("{table}: {}", mask_generated_keys(&row.to_sql())))
			.collect();
		rows.sort();
		state.extend(rows);
	}
	(rows, state)
}

/// A fresh database over `schema`. A table `cyc` has its fields' producing
/// clauses closed into a cycle in the catalog itself, as a catalog written
/// before `DEFINE FIELD` refused cycles holds one.
async fn fixture(schema: &str) -> TestDb {
	use crate::catalog::providers::TableProvider;
	let db = TestDb::new_with_auth(schema).await;
	if !schema.contains("DEFINE TABLE cyc") {
		return db;
	}
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	let db_ctx = ctx.database().expect("a database context");
	let (ns, dbid) = (db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id);
	let table = crate::val::TableName::from("cyc");
	let txn = ctx.txn();
	let mut field = (*txn
		.get_tb_field(ns, dbid, &table, "b", None)
		.await
		.expect("the field reads")
		.expect("the field is defined"))
	.clone();
	let value: crate::expr::Expr = crate::exec::operators::test_util::parse_sql_expr("a");
	field.value_body = Some(crate::catalog::LogicalBody::of(&value));
	field.value = Some(value);
	txn.put_tb_field(ns, dbid, &table, &field).await.expect("the field is stored");
	txn.commit().await.expect("commit");
	db
}

/// Whether a statement's program runs its schema stages by instructions.
async fn schema_by_instructions(sql: &str, session: &crate::dbs::Session) -> bool {
	schema_by_instructions_on(FIELD_SCHEMA, sql, session).await
}

/// Whether `sql`'s program runs its schema stages by instructions over a
/// fresh `schema`.
async fn schema_by_instructions_on(schema: &str, sql: &str, session: &crate::dbs::Session) -> bool {
	let db = fixture(schema).await;
	let ctx = db.exec_ctx_as(session, crate::kvs::TransactionType::Write).await;
	let bound = bind(&ctx, sql).await;
	let physical =
		super::optimize::optimize(super::map::map(&planner(&ctx), &bound).await.expect("maps"));
	let program = super::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
	ctx.txn().cancel().await.expect("cancel");
	super::tests::write_families(&super::tests::with_stages(&planner(&ctx), &program).await)
		.contains(&"schema")
}

/// A write applies a table's field schema — every clause, to its owner and to
/// a record user whose field permissions are enforced — with the schema stages
/// run as instructions.
#[tokio::test]
async fn a_write_applies_the_field_schema() {
	let mut golden = Golden::new("a_write_applies_the_field_schema");
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	let reader = crate::dbs::Session::for_record("test", "test", "user", rid);
	for (who, session) in [("owner", TestDb::owner()), ("record user", reader)] {
		for sql in FIELD_WRITES {
			let (rows, state) = written_with(sql, &session).await;
			golden.record(format!("{who}: {sql}"), render_written(&rows, &state));
			assert!(
				schema_by_instructions(sql, &session).await,
				"`{sql}` for the {who} runs its schema by no instruction"
			);
		}
	}
	golden.check();
}

/// The field permissions a record user is held to revert what it may not
/// change, where the owner's change stands.
#[tokio::test]
async fn a_refused_field_change_is_reverted_for_a_checked_caller_alone() {
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	let reader = crate::dbs::Session::for_record("test", "test", "user", rid);
	let sql = "UPDATE f:1 SET locked = 'new' RETURN VALUE locked";
	let (owner, _) = written_with(sql, &TestDb::owner()).await;
	let (user, _) = written_with(sql, &reader).await;
	assert_eq!(owner, Ok(vec![Value::from("new")]));
	assert_eq!(user, Ok(vec![Value::from("l")]));
}

/// Tables whose fields guard their changes with `PERMISSIONS FOR create` and
/// `update` predicates, read a `COMPUTED` field from another field's clause,
/// and — for `cyc`, once [`fixture`] closes it — produce each other's values in
/// a cycle.
pub(super) const GUARDED_SCHEMA: &str = "
	CREATE user:reader, user:other;
	DEFINE TABLE g PERMISSIONS FULL;
	DEFINE FIELD n ON g TYPE option<int>;
	DEFINE FIELD up ON g TYPE option<int> PERMISSIONS FOR update WHERE $value > $before;
	DEFINE FIELD mk ON g TYPE option<string>
		PERMISSIONS FOR create WHERE $value = 'ok' FOR update WHERE $auth.id = user:reader;
	DEFINE FIELD own ON g TYPE option<string> PERMISSIONS FOR update WHERE $input != NONE AND n > 1;
	DEFINE FIELD opt ON g TYPE option<object>;
	DEFINE FIELD opt.k ON g TYPE option<int> PERMISSIONS FOR create, update WHERE $value != 13;
	DEFINE FIELD twice ON g COMPUTED n * 2;
	DEFINE FIELD reads ON g VALUE twice + 1;
	DEFINE FIELD asks ON g TYPE option<int> ASSERT $value = NONE OR $value < twice;
	CREATE g:1 SET n = 1, up = 5, mk = 'ok', own = 'o', opt = { k: 1 };
	DEFINE TABLE cyc PERMISSIONS FULL;
	DEFINE FIELD a ON cyc VALUE b;
	DEFINE FIELD b ON cyc VALUE 1;
";

/// The tables whose records a statement over [`GUARDED_SCHEMA`] is judged by.
const GUARDED_TABLES: &[&str] = &["g", "cyc"];

/// Writes that reach every guard, derivation and cycle of [`GUARDED_SCHEMA`].
pub(super) const GUARDED_WRITES: &[&str] = &[
	"UPDATE g:1 SET up = 9",
	"UPDATE g:1 SET up = 2",
	"UPDATE g SET up += 1",
	"CREATE g:2 SET n = 2, mk = 'ok'",
	"CREATE g:3 SET n = 3, mk = 'no'",
	"UPDATE g:1 SET mk = 'changed'",
	"UPDATE g:1 SET own = 'x'",
	"UPDATE g:1 SET n = 2, own = 'y'",
	"UPDATE g:1 MERGE { opt: { k: 13 } }",
	"UPDATE g:1 MERGE { opt: { k: 14 } }",
	"UPDATE g:1 SET opt = NONE",
	"CREATE g:4 SET n = 3, asks = 5",
	"CREATE g:5 SET n = 3, asks = 1, opt = { k: 13 }",
	"UPDATE g:1 SET n = 4",
	"INSERT INTO g { id: g:7, n: 7, mk: 'no' }",
	"UPSERT g:1 SET up = 1, mk = 'ok'",
	"CREATE cyc:1 SET a = 1",
	"UPDATE cyc SET b = 2",
];

/// Principals the guards judge differently: exempt ones, whose field
/// permissions are not enforced, and checked ones, whose are.
fn guarded_principals() -> Vec<(&'static str, crate::dbs::Session)> {
	use crate::dbs::Session;
	use crate::iam::{Level, Role};
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	let other =
		crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "other"));
	vec![
		("owner", TestDb::owner()),
		("record user", Session::for_record("test", "test", "user", rid)),
		("other record user", Session::for_record("test", "test", "user", other)),
		(
			"database editor",
			Session::for_level(Level::Database("test".into(), "test".into()), Role::Editor),
		),
		(
			"database viewer",
			Session::for_level(Level::Database("test".into(), "test".into()), Role::Viewer),
		),
	]
}

/// A field's write permission predicates, a computed field another field's
/// clause reads, and a cycle among producing clauses are applied for every kind
/// of principal, with the schema stages run as instructions.
#[tokio::test]
async fn field_write_guards_and_derivations_apply() {
	let mut golden = Golden::new("field_write_guards_and_derivations_apply");
	for (who, session) in guarded_principals() {
		for sql in GUARDED_WRITES {
			let (rows, state) = written_on(GUARDED_SCHEMA, GUARDED_TABLES, sql, &session).await;
			golden.record(format!("{who}: {sql}"), render_written(&rows, &state));
			assert!(
				schema_by_instructions_on(GUARDED_SCHEMA, sql, &session).await,
				"`{sql}` for the {who} runs its schema by no instruction"
			);
		}
	}
	golden.check();
}

/// A guard admits the changes it is written to admit and reverts the rest for
/// a checked caller, and a cycle refuses the record.
#[tokio::test]
async fn field_write_guards_admit_and_revert_what_they_are_written_to() {
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	let reader = crate::dbs::Session::for_record("test", "test", "user", rid);
	let run = async |sql: &str, session: &crate::dbs::Session| {
		written_on(GUARDED_SCHEMA, GUARDED_TABLES, sql, session).await.0
	};
	assert_eq!(
		run("UPDATE g:1 SET up = 9 RETURN VALUE up", &reader).await,
		Ok(vec![Value::from(9)])
	);
	assert_eq!(
		run("UPDATE g:1 SET up = 2 RETURN VALUE up", &reader).await,
		Ok(vec![Value::from(5)])
	);
	assert_eq!(
		run("UPDATE g:1 SET up = 2 RETURN VALUE up", &TestDb::owner()).await,
		Ok(vec![Value::from(2)])
	);
	assert_eq!(
		run("CREATE g:3 SET n = 3, mk = 'no' RETURN VALUE mk", &reader).await,
		Ok(vec![Value::None])
	);
	assert_eq!(
		run("CREATE g:4 SET n = 3, reads = 0 RETURN VALUE reads", &reader).await,
		Ok(vec![Value::from(7)])
	);
	let refused = run("CREATE cyc:1 SET a = 1", &TestDb::owner()).await;
	assert!(
		refused.as_ref().is_err_and(|e| e.contains("cycle") || e.contains("Cycle")),
		"a cycle refuses the record: {refused:?}"
	);
}

/// Clauses of a field that hold a statement or a call of a
/// user-defined function, and the table they guard. The computed field reads
/// another table: one reading its own declines the statement (see
/// `tests_plain_data`).
const STATEMENT_CLAUSES: &str = "
	DEFINE FUNCTION fn::open() { RETURN true; } PERMISSIONS FULL;
	DEFINE TABLE src PERMISSIONS FULL;
	CREATE src:1;
	DEFINE TABLE p PERMISSIONS FULL;
	DEFINE FIELD x ON p PERMISSIONS FOR update WHERE (SELECT VALUE 1 FROM ONLY p:1) = 1;
	DEFINE FIELD y ON p TYPE option<int> PERMISSIONS FOR create, update WHERE fn::open();
	DEFINE FIELD z ON p TYPE option<int> DEFAULT (SELECT VALUE 7 FROM ONLY p:1)
		ASSERT $value = NONE OR (SELECT VALUE true FROM ONLY p:1) = true;
	DEFINE FIELD c ON p COMPUTED (SELECT VALUE 2 FROM ONLY src:1) * 3;
	DEFINE FIELD d ON p COMPUTED c + 1;
	CREATE p:1 SET x = 1, y = 1;
";

/// Writes that reach the clauses of [`STATEMENT_CLAUSES`].
const STATEMENT_WRITES: &[&str] = &[
	"UPDATE p:1 SET x = 2",
	"UPDATE p:1 SET y = 3 RETURN AFTER",
	"CREATE p:2 SET x = 2, y = 2, z = 4 RETURN AFTER",
	"CREATE p:3 RETURN AFTER",
	"UPSERT p:1 SET z = 9 RETURN AFTER",
	"UPDATE p SET x = 5 RETURN d",
	"DELETE p:1 RETURN BEFORE",
];

/// A field whose clauses hold a statement or a call of a user-defined
/// function is applied as instructions, for the owner and for a record user.
#[tokio::test]
async fn a_field_clause_holding_a_statement_lowers_to_instructions() {
	let mut golden = Golden::new("a_field_clause_holding_a_statement_lowers_to_instructions");
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	let reader = crate::dbs::Session::for_record("test", "test", "user", rid);
	for (who, session) in [("owner", TestDb::owner()), ("record user", reader)] {
		for sql in STATEMENT_WRITES {
			let (rows, state) = written_on(STATEMENT_CLAUSES, &["p"], sql, &session).await;
			golden.record(format!("{who}: {sql}"), render_written(&rows, &state));
			assert!(
				schema_by_instructions_on(STATEMENT_CLAUSES, sql, &session).await,
				"`{sql}` for the {who} runs its schema by no instruction"
			);
		}
	}
	golden.check();
}
