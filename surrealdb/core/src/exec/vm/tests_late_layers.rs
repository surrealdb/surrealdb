//! A late-bound statement reads the table it opens as the catalog stands when
//! it runs: its computed fields, field and table permissions, and the
//! `VALUE`, `ASSERT` and `DEFAULT` clauses of its writes are loaded when it
//! opens its source, never fixed when it was planned.
//!
//! Each test plans a statement late-bound in one transaction, lets a schema
//! statement commit after it, and runs the plan in a later transaction. It
//! must answer what the statement answers planned where it runs.

use std::sync::Arc;

use crate::dbs::Session;
use crate::exec::operators::test_util::{TestDb, try_collect};
use crate::exec::planner::Planner;
use crate::kvs::TransactionType;
use crate::val::Value;

fn record_user(user: &str) -> Session {
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", user));
	Session::for_record("test", "test", "user", rid)
}

const SETUP: &str = "
	DEFINE ACCESS user ON DATABASE TYPE RECORD;
	DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
	DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
	CREATE user:alice;
	CREATE t:1 SET a = 1, s = 'secret';
	CREATE t:2 SET a = -1, s = 'other';
";

/// `sql` planned late-bound as `session` before `change` commits, and run
/// after it.
async fn planned_before(db: &TestDb, session: &Session, sql: &str, change: &str) -> String {
	let plan_ctx = db.exec_ctx_as(session, TransactionType::Read).await;
	let ast = crate::syn::parse(sql).expect("parses");
	let mut exprs = ast.expressions;
	let surrealdb_expr::parser::TopLevelExpr::Expr(expr) = exprs.remove(0) else {
		panic!("a statement");
	};
	let op = crate::expr::logical::lower(expr).expect("lowers");
	let planner = Planner::with_txn(
		plan_ctx.ctx(),
		plan_ctx.function_registry(),
		plan_ctx.txn(),
		Some("test".to_owned()),
		Some("test".to_owned()),
	)
	.with_auth(Arc::clone(&plan_ctx.root().auth))
	.late_binding();
	let bound = Box::pin(planner.bind(op)).await.expect("binds");
	let plan =
		Box::pin(crate::exec::vm::plan_checked(&planner, &bound, None)).await.expect("plans");
	plan_ctx.txn().cancel().await.expect("the planning transaction ends");
	db.run(change).await;
	let run_ctx = db.exec_ctx_as(session, TransactionType::Write).await;
	let answer = match try_collect(&plan, &run_ctx).await {
		Ok(values) => format!("{:?}", Value::from(values)),
		Err(crate::expr::ControlFlow::Err(e)) => format!("error: {e:#}"),
		Err(crate::expr::ControlFlow::Return(value)) => format!("{value:?}"),
		Err(other) => format!("control flow: {other:?}"),
	};
	run_ctx.txn().cancel().await.expect("the run's transaction ends");
	answer
}

/// `sql` run as `session` once `change` has committed.
async fn planned_where_it_runs(db: &TestDb, session: &Session, sql: &str, change: &str) -> String {
	db.run(change).await;
	let mut results = db.datastore().execute(sql, session, None).await.expect("runs");
	match results.pop().expect("a result").result {
		Ok(value) => format!("{:?}", Value::from(value)),
		Err(e) => format!("error: {}", e.message()),
	}
}

async fn same_either_way(session: Session, sql: &str, change: &str) {
	let late = planned_before(&TestDb::new_with_auth(SETUP).await, &session, sql, change).await;
	let fresh =
		planned_where_it_runs(&TestDb::new_with_auth(SETUP).await, &session, sql, change).await;
	assert_eq!(late, fresh, "`{sql}` planned before `{change}` answered differently");
}

#[tokio::test]
async fn a_computed_field_defined_after_planning_is_derived() {
	for session in [TestDb::owner(), record_user("alice")] {
		same_either_way(session, "SELECT * FROM t", "DEFINE FIELD c ON t COMPUTED a + 1").await;
	}
}

#[tokio::test]
async fn a_removed_computed_field_is_no_longer_derived() {
	same_either_way(
		record_user("alice"),
		"SELECT * FROM t",
		"DEFINE FIELD c ON t COMPUTED a + 1; REMOVE FIELD c ON t",
	)
	.await;
}

#[tokio::test]
async fn a_field_permission_defined_after_planning_applies() {
	same_either_way(
		record_user("alice"),
		"SELECT * FROM t",
		"DEFINE FIELD s ON t PERMISSIONS FOR select NONE",
	)
	.await;
	same_either_way(
		record_user("alice"),
		"SELECT s FROM t",
		"DEFINE FIELD s ON t PERMISSIONS FOR select WHERE a > 0",
	)
	.await;
}

#[tokio::test]
async fn a_table_permission_changed_after_planning_applies() {
	same_either_way(
		record_user("alice"),
		"SELECT * FROM t",
		"DEFINE TABLE OVERWRITE t SCHEMALESS PERMISSIONS FOR select WHERE a > 0",
	)
	.await;
	same_either_way(
		record_user("alice"),
		"SELECT * FROM t:1",
		"DEFINE TABLE OVERWRITE t SCHEMALESS PERMISSIONS NONE",
	)
	.await;
}

#[tokio::test]
async fn a_permission_reading_a_computed_field_defined_after_planning_derives_it() {
	same_either_way(
		record_user("alice"),
		"SELECT * FROM t",
		"DEFINE FIELD pos ON t COMPUTED a > 0 PERMISSIONS FOR select NONE; \
		 DEFINE FIELD s ON t PERMISSIONS FOR select WHERE pos = true",
	)
	.await;
	same_either_way(
		record_user("alice"),
		"SELECT * FROM t",
		"DEFINE FIELD pos ON t COMPUTED a > 0; \
		 DEFINE TABLE OVERWRITE t SCHEMALESS PERMISSIONS FOR select WHERE pos = true",
	)
	.await;
}

#[tokio::test]
async fn a_write_meets_the_field_clauses_defined_after_planning() {
	for session in [TestDb::owner(), record_user("alice")] {
		same_either_way(session.clone(), "UPDATE t:1 SET a = 2", "DEFINE FIELD b ON t VALUE 7")
			.await;
		same_either_way(
			session.clone(),
			"UPDATE t:1 SET b = 3",
			"DEFINE FIELD b ON t ASSERT $value > 100",
		)
		.await;
		same_either_way(session.clone(), "CREATE t:3 SET a = 3", "DEFINE FIELD d ON t DEFAULT 'x'")
			.await;
		same_either_way(
			session,
			"UPDATE t:1 SET s = 'new'",
			"DEFINE FIELD s ON t PERMISSIONS FOR update NONE",
		)
		.await;
	}
}

#[tokio::test]
async fn an_update_reads_a_computed_field_defined_after_planning() {
	same_either_way(
		record_user("alice"),
		"UPDATE t:1 SET a = 5 RETURN AFTER",
		"DEFINE FIELD c ON t COMPUTED a * 10",
	)
	.await;
}

#[tokio::test]
async fn every_read_shape_meets_the_layers_defined_after_planning() {
	let change = "DEFINE FIELD c ON t COMPUTED a + 1; \
		DEFINE FIELD s ON t PERMISSIONS FOR select WHERE c > 1; \
		DEFINE TABLE OVERWRITE t SCHEMALESS PERMISSIONS FOR select WHERE a != 7 FOR create, update, delete FULL";
	for session in [TestDb::owner(), record_user("alice")] {
		for sql in [
			"SELECT c FROM t",
			"SELECT s, c FROM t",
			"SELECT * FROM t WHERE c = 2",
			"SELECT * FROM t ORDER BY c DESC",
			"SELECT * FROM t:1",
			"SELECT * FROM t:1..=2",
			"SELECT VALUE c FROM t",
			"SELECT count() FROM t GROUP ALL",
			"SELECT c, count() FROM t GROUP BY c",
			"SELECT * OMIT a FROM t",
			"SELECT * FROM t LIMIT 1",
			"RETURN t:1.c",
			"RETURN t:1.*",
			"RETURN (SELECT * FROM t)",
			"SELECT * FROM [t:1, t:2]",
			"DELETE t:1 RETURN BEFORE",
			"UPDATE t SET a = a + 1 RETURN AFTER",
			"UPSERT t:9 SET a = 2 RETURN AFTER",
		] {
			same_either_way(session.clone(), sql, change).await;
		}
	}
}

#[tokio::test]
async fn an_index_defined_after_planning_answers_as_the_scan_would() {
	let change = "DEFINE FIELD c ON t COMPUTED a + 1; DEFINE INDEX ia ON t FIELDS a";
	for session in [TestDb::owner(), record_user("alice")] {
		for sql in ["SELECT * FROM t WHERE a = 1", "SELECT c FROM t WHERE a > 0 ORDER BY a"] {
			same_either_way(session.clone(), sql, change).await;
		}
	}
}

#[tokio::test]
async fn a_relation_and_a_fetch_meet_the_layers_defined_after_planning() {
	let setup_change = "RELATE t:1->r:1->t:2; \
		DEFINE FIELD c ON t COMPUTED a + 1; \
		DEFINE FIELD s ON t PERMISSIONS FOR select NONE";
	for session in [TestDb::owner(), record_user("alice")] {
		for sql in ["SELECT ->r->t.* AS out FROM t:1", "SELECT * FROM r FETCH in, out"] {
			same_either_way(session.clone(), sql, setup_change).await;
		}
	}
}

#[tokio::test]
async fn a_table_defined_after_planning_applies_its_permissions() {
	for change in [
		"DEFINE TABLE n SCHEMALESS PERMISSIONS NONE; CREATE n:1 SET a = 1",
		"DEFINE TABLE n SCHEMALESS PERMISSIONS FOR select WHERE a > 1; CREATE n:1 SET a = 1",
		"CREATE n:1 SET a = 1",
		"DEFINE TABLE n SCHEMALESS PERMISSIONS FULL; DEFINE FIELD a ON n PERMISSIONS FOR select NONE; \
		 DEFINE FIELD c ON n COMPUTED 3; CREATE n:1 SET a = 1",
	] {
		for sql in ["SELECT * FROM n", "SELECT * FROM n:1", "RETURN n:1.*", "SELECT a FROM n"] {
			same_either_way(TestDb::owner(), sql, change).await;
			same_either_way(record_user("alice"), sql, change).await;
		}
	}
}

#[tokio::test]
async fn a_table_removed_after_planning_is_read_as_absent() {
	for sql in ["SELECT * FROM t", "SELECT * FROM t:1", "UPDATE t:1 SET a = 3"] {
		same_either_way(TestDb::owner(), sql, "REMOVE TABLE t").await;
		same_either_way(record_user("alice"), sql, "REMOVE TABLE t").await;
	}
}

#[tokio::test]
async fn an_index_removed_after_planning_is_not_read() {
	let setup_change = "DEFINE INDEX ia ON t FIELDS a";
	for sql in ["SELECT * FROM t WHERE a = 1", "SELECT * FROM t WHERE a > 0 ORDER BY a"] {
		let session = record_user("alice");
		let db = TestDb::new_with_auth(SETUP).await;
		db.run(setup_change).await;
		let late = planned_before(&db, &session, sql, "REMOVE INDEX ia ON t").await;
		let db = TestDb::new_with_auth(SETUP).await;
		db.run(setup_change).await;
		let fresh = planned_where_it_runs(&db, &session, sql, "REMOVE INDEX ia ON t").await;
		assert_eq!(late, fresh, "`{sql}` planned before the index was removed");
		let db = TestDb::new_with_auth(SETUP).await;
		db.run(setup_change).await;
		let late = planned_before(
			&db,
			&session,
			sql,
			"REMOVE INDEX ia ON t; DEFINE INDEX ia ON t FIELDS s",
		)
		.await;
		assert_eq!(late, fresh, "`{sql}` planned before the index was redefined");
	}
}

#[tokio::test]
async fn a_count_and_a_traversal_meet_permissions_changed_after_planning() {
	for change in [
		"DEFINE TABLE OVERWRITE t SCHEMALESS PERMISSIONS NONE",
		"DEFINE TABLE OVERWRITE t SCHEMALESS PERMISSIONS FOR select WHERE a > 0",
		"DEFINE TABLE OVERWRITE r SCHEMALESS PERMISSIONS NONE",
		"DEFINE TABLE OVERWRITE r TYPE RELATION SCHEMALESS PERMISSIONS FOR select WHERE false",
	] {
		for sql in [
			"SELECT count() FROM t GROUP ALL",
			"RETURN count(SELECT * FROM t)",
			"SELECT ->r->t AS out FROM t:1",
			"SELECT ->r.* AS out FROM t:1",
			"RETURN t:1->r->t",
			"SELECT * FROM r",
		] {
			let session = record_user("alice");
			let db = TestDb::new_with_auth(SETUP).await;
			db.run("RELATE t:1->r:1->t:2").await;
			let late = planned_before(&db, &session, sql, change).await;
			let db = TestDb::new_with_auth(SETUP).await;
			db.run("RELATE t:1->r:1->t:2").await;
			let fresh = planned_where_it_runs(&db, &session, sql, change).await;
			assert_eq!(late, fresh, "`{sql}` planned before `{change}` answered differently");
		}
	}
}

#[tokio::test]
async fn a_write_meets_table_permissions_changed_after_planning() {
	for change in [
		"DEFINE TABLE OVERWRITE t SCHEMALESS PERMISSIONS FOR select FULL FOR update WHERE a > 0",
		"DEFINE TABLE OVERWRITE t SCHEMALESS PERMISSIONS FOR select WHERE a > 0 FOR create, update, delete FULL",
		"DEFINE TABLE OVERWRITE t SCHEMALESS PERMISSIONS FOR select, update FULL FOR delete NONE",
		"DEFINE TABLE n SCHEMALESS PERMISSIONS FOR select FULL FOR create NONE",
		"DEFINE TABLE n SCHEMALESS PERMISSIONS FULL; DEFINE FIELD c ON n COMPUTED 1; DEFINE FIELD v ON n VALUE 'v'",
	] {
		for sql in [
			"UPDATE t SET b = 1",
			"UPDATE t:2 SET b = 1",
			"DELETE t RETURN BEFORE",
			"CREATE n:1 SET a = 1",
			"UPSERT n:1 SET a = 1",
			"INSERT INTO n { id: n:2, a: 2 }",
		] {
			same_either_way(record_user("alice"), sql, change).await;
		}
	}
}

/// A computed field whose body reads, under a `LIMIT`, a table the caller may
/// not select: a permission reading it derives it unchecked, and the caller's
/// own read of it derives it as the caller, whose read of that table is empty.
const DENIED_READ: &str = "
	DEFINE ACCESS user ON DATABASE TYPE RECORD;
	DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
	CREATE user:alice;
	DEFINE TABLE flag SCHEMALESS PERMISSIONS NONE;
	DEFINE TABLE doc SCHEMALESS PERMISSIONS FOR select WHERE flagged != true, FOR create, update FULL;
	DEFINE FIELD flagged ON doc COMPUTED (SELECT VALUE id FROM ONLY flag WHERE target = $parent.id LIMIT 1) != NONE;
	INSERT INTO doc [{ id: doc:pub, status: 'public' }, { id: doc:sec, status: 'secret' }];
	CREATE flag:1 SET target = doc:sec;
	DEFINE TABLE holder SCHEMALESS PERMISSIONS FULL;
	CREATE holder:1 SET d = doc:sec, e = doc:pub;
";

#[tokio::test]
async fn a_permission_derives_a_field_reading_a_denied_table_unchecked() {
	for (sql, expected) in [
		("SELECT VALUE id FROM doc", "[doc:pub]"),
		("SELECT * FROM doc", "[{ flagged: false, id: doc:pub, status: 'public' }]"),
		("RETURN doc:sec.*", "NONE"),
		("RETURN doc:pub.*", "{ flagged: false, id: doc:pub, status: 'public' }"),
		(
			"SELECT * FROM holder FETCH d, e",
			"[{ d: NONE, e: { flagged: false, id: doc:pub, status: 'public' }, id: holder:1 }]",
		),
		("SELECT d.flagged AS f FROM holder", "[{ f: NONE }]"),
	] {
		let session = record_user("alice");
		let db = TestDb::new_with_auth(DENIED_READ).await;
		let fresh = planned_where_it_runs(&db, &session, sql, "RETURN 1").await;
		let parsed = Value::from(crate::syn::value(expected).expect("parses"));
		assert_eq!(fresh, format!("{:?}", parsed), "`{sql}` planned where it runs");
		let db = TestDb::new_with_auth(DENIED_READ).await;
		let late = planned_before(&db, &session, sql, "RETURN 1").await;
		assert_eq!(late, fresh, "`{sql}` planned late-bound");
	}
}

/// A table whose `SELECT` permission reads a computed field through a
/// function, and a function whose body holds a schema statement, so that the
/// statements of a block after a call of it are served late-bound.
const THROUGH_A_FUNCTION: &str = "
	DEFINE ACCESS user ON DATABASE TYPE RECORD;
	DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
	CREATE user:alice;
	DEFINE FUNCTION fn::hidden() { RETURN hidden; };
	DEFINE FUNCTION fn::late($when: string) { IF $when = 'never' { DEFINE TABLE late_never }; RETURN 0; } PERMISSIONS FULL;
	DEFINE TABLE doc SCHEMALESS PERMISSIONS FOR select WHERE fn::hidden() != true, FOR create, update FULL;
	DEFINE FIELD hidden ON doc COMPUTED status = 'secret';
	CREATE doc:pub SET status = 'public';
	CREATE doc:sec SET status = 'secret';
";

#[tokio::test]
async fn a_statement_after_a_possible_catalog_change_applies_the_permission() {
	for (sql, expected) in [
		("SELECT VALUE id FROM doc:sec WHERE fn::late('now') = 0", "[]"),
		("SELECT VALUE id FROM doc:sec", "[]"),
		("SELECT VALUE id FROM doc ORDER BY id", "[doc:pub]"),
		("SELECT VALUE id FROM doc WHERE id = doc:sec", "[]"),
		("SELECT VALUE id FROM doc:pub..=sec", "[doc:pub]"),
		("SELECT count() FROM doc GROUP ALL", "[{ count: 1 }]"),
	] {
		let session = record_user("alice");
		let db = TestDb::new_with_auth(THROUGH_A_FUNCTION).await;
		let block = format!("{{ LET $late = fn::late('now'); RETURN {sql}; }};");
		let parsed = format!("{:?}", Value::from(crate::syn::value(expected).expect("parses")));
		let request = format!("{block}{block}{sql};");
		let results = db.datastore().execute(&request, &session, None).await.expect("runs");
		for (at, result) in results.into_iter().enumerate() {
			let answer = match result.result {
				Ok(value) => format!("{:?}", Value::from(value)),
				Err(e) => format!("error: {}", e.message()),
			};
			assert_eq!(answer, parsed, "`{sql}` at statement {at}");
		}
	}
}
