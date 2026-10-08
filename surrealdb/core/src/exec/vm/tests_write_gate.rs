//! Tests of the gate stages run as instructions: each statement runs on the VM
//! for the owner and for a checked record user, and its answers and written
//! records are the recorded ones.

#![allow(unused_imports)]

use std::sync::Arc;

use super::golden::Golden;
use super::tests::*;
use crate::dbs::Session;
use crate::exec::operators::test_util::TestDb;
use crate::exec::operators::write::stage::{RecordImage, StageId};
use crate::iam::{Level, Role};
use crate::val::Value;

/// Tables under each kind of table permission: `FULL`, `NONE`, a predicate
/// per clause, one reading `COMPUTED` fields, and write guards that write.
const GATED_SCHEMA: &str = "
	DEFINE TABLE open PERMISSIONS FULL;
	DEFINE TABLE shut PERMISSIONS NONE;
	DEFINE TABLE cond PERMISSIONS
		FOR select WHERE visible = true
		FOR create WHERE n < 10
		FOR update WHERE n < 10 AND $before.n != 5
		FOR delete WHERE locked != true;
	DEFINE TABLE comp PERMISSIONS FOR select, create, update, delete WHERE blocked != true;
	DEFINE FIELD blocked ON comp COMPUTED n > 5;
	DEFINE FIELD twice ON comp COMPUTED blocked OR n * 2 > 12;
	DEFINE TABLE selfref PERMISSIONS
		FOR select FULL
		FOR create, update, delete WHERE id.n != 3;
	DEFINE FIELD mirror ON selfref COMPUTED id.n;
	DEFINE TABLE counted PERMISSIONS
		FOR select FULL
		FOR create, update, delete WHERE (CREATE log SET at = 1) != NONE AND n < 10;
	DEFINE TABLE stmt PERMISSIONS
		FOR select WHERE (SELECT VALUE n FROM open:1)[0] = 1
		FOR create, update, delete WHERE n IN (SELECT VALUE n FROM open);
	DEFINE TABLE log PERMISSIONS FULL;
	DEFINE TABLE owned PERMISSIONS
		FOR select, create, update, delete WHERE owner = $auth.id;
	CREATE open:1 SET n = 1;
	CREATE shut:1 SET n = 1;
	CREATE cond:1 SET n = 1, visible = true, locked = false;
	CREATE cond:2 SET n = 5, visible = false, locked = true;
	CREATE comp:1 SET n = 1;
	CREATE comp:2 SET n = 7;
	CREATE selfref:1 SET n = 1;
	CREATE selfref:3 SET n = 3;
	CREATE counted:1 SET n = 1;
	CREATE counted:2 SET n = 20;
	CREATE stmt:1 SET n = 1;
	CREATE stmt:2 SET n = 2;
	CREATE owned:1 SET owner = user:reader;
	CREATE owned:2 SET owner = user:other;
";

/// The tables [`GATED_SCHEMA`] defines, read back after each statement.
const GATED_TABLES: &[&str] =
	&["open", "shut", "cond", "comp", "selfref", "counted", "stmt", "log", "owned", "nowhere"];

/// Every write form against every gated table.
fn gated_writes() -> Vec<String> {
	let mut out = Vec::new();
	for table in ["open", "shut", "cond", "comp", "selfref", "counted", "stmt"] {
		for sql in [
			"CREATE {t}:9 SET n = 3",
			"CREATE {t}:8 SET n = 20",
			"CREATE {t}:7 SET n = 7, visible = true RETURN AFTER",
			"UPDATE {t} SET n += 1",
			"UPDATE {t} SET n = 50 RETURN BEFORE",
			"UPDATE {t}:1 SET n = 6 RETURN DIFF",
			"UPSERT {t}:1 SET n = 2",
			"UPSERT {t}:6 SET n = 2",
			"DELETE {t}:1",
			"DELETE {t} RETURN BEFORE",
			"INSERT INTO {t} { id: {t}:5, n: 1 }",
		] {
			out.push(sql.replace("{t}", table));
		}
	}
	out.extend(
		[
			"CREATE owned:3 SET owner = user:reader",
			"CREATE owned:4 SET owner = user:other",
			"UPDATE owned SET n = 1",
			"UPDATE owned SET owner = user:other",
			"DELETE owned RETURN BEFORE",
			"CREATE comp:3 SET n = 6",
			"UPDATE comp:1 SET n = 9",
			"UPDATE comp:2 SET n = 1",
			// A table the catalog does not hold is defined by the write, with
			// every permission `NONE`: the owner writes it and a record user
			// is refused.
			"CREATE nowhere:1 SET n = 1 RETURN AFTER",
			"UPSERT nowhere:2 SET n = 3 RETURN AFTER",
			"INSERT INTO nowhere { id: nowhere:3, n: 4 }",
			"CREATE type::record('nowhere', 4) SET n = 5 RETURN AFTER",
			"RELATE open:1->nowhere:6->shut:1 SET n = 6",
			"UPDATE nowhere SET n = 7",
			"DELETE nowhere:1",
		]
		.map(str::to_owned),
	);
	out
}

/// `value` with the generated key of a `log` record — which a write guard
/// writes, under an id drawn per run — read as one placeholder.
fn shown(value: Value) -> Value {
	match value {
		Value::RecordId(mut rid) if rid.table.as_str() == "log" => {
			rid.key = crate::val::RecordIdKey::String("generated".into());
			Value::RecordId(rid)
		}
		Value::Array(array) => {
			Value::Array(array.into_iter().map(shown).collect::<Vec<_>>().into())
		}
		Value::Object(object) => Value::Object(
			object
				.into_iter()
				.map(|(k, v)| (k, shown(v)))
				.collect::<std::collections::BTreeMap<_, _>>()
				.into(),
		),
		other => other,
	}
}

/// Run `sql` against a fresh [`GATED_SCHEMA`] as `session` and answer its rows
/// and every table's records after.
async fn gated(sql: &str, session: &Session) -> (Result<Vec<Value>, String>, Vec<String>) {
	use surrealdb_types::ToSql;
	let db = TestDb::new_with_auth(GATED_SCHEMA).await;
	let ctx = db.exec_ctx_as(session, crate::kvs::TransactionType::Write).await;
	let bound = bind(&ctx, sql).await;
	let op = super::plan(&planner(&ctx), &bound)
		.await
		.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
	let rows = outcome(&op, &ctx).await.map(|rows| rows.into_iter().map(shown).collect());
	let txn = ctx.txn();
	if rows.is_ok() {
		txn.commit().await.expect("commit");
	} else {
		txn.cancel().await.expect("cancel");
	}
	let read = db.exec_ctx().await;
	let mut state = Vec::new();
	for table in GATED_TABLES {
		let op = crate::exec::operators::test_util::plan_statement(
			&format!("SELECT * FROM {table}"),
			&read,
		)
		.await;
		// A table no write defined is not there to read.
		let mut rows: Vec<String> = match outcome(&op, &read).await {
			Ok(rows) => {
				rows.into_iter().map(|row| format!("{table}: {}", shown(row).to_sql())).collect()
			}
			Err(error) => vec![format!("{table}: {error}")],
		};
		rows.sort();
		state.extend(rows);
	}
	(rows, state)
}
/// The principals each statement runs as: exempt ones, whose writes no gate
/// judges, and checked ones, whose every write is judged.
fn principals() -> Vec<(&'static str, Session)> {
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	vec![
		("owner", TestDb::owner()),
		("record user", Session::for_record("test", "test", "user", rid)),
		(
			"database editor",
			Session::for_level(Level::Database("test".into(), "test".into()), Role::Editor),
		),
		(
			"database viewer",
			Session::for_level(Level::Database("test".into(), "test".into()), Role::Viewer),
		),
		(
			"namespace viewer",
			Session::for_level(Level::Namespace("test".into()), Role::Viewer).with_db("test"),
		),
	]
}

/// Each table permission admits and refuses a write, and withholds its
/// result — for exempt and checked principals alike — with every gate stage
/// run as instructions.
#[tokio::test]
async fn a_write_is_gated_by_the_table_permissions() {
	let mut golden = Golden::new("a_write_is_gated_by_the_table_permissions");
	for (who, session) in principals() {
		for sql in gated_writes() {
			let (rows, state) = gated(&sql, &session).await;
			golden.record(format!("{who}: {sql}"), render_written(&rows, &state));
			let db = TestDb::new_with_auth(GATED_SCHEMA).await;
			let ctx = db.exec_ctx_as(&session, crate::kvs::TransactionType::Write).await;
			let bound = bind(&ctx, &sql).await;
			let physical = super::optimize::optimize(
				super::map::map(&planner(&ctx), &bound).await.expect("maps"),
			);
			let program = super::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
			assert!(
				super::tests::write_families(
					&super::tests::with_stages(&planner(&ctx), &program).await
				)
				.contains(&"gate"),
				"`{sql}` for the {who} runs no gate instruction"
			);
			ctx.txn().cancel().await.expect("cancel");
		}
	}
	golden.check();
}

/// A record user is refused what its table permissions deny.
#[tokio::test]
async fn a_checked_caller_is_refused_what_the_permissions_deny() {
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	let reader = Session::for_record("test", "test", "user", rid);
	for (sql, forbidden) in [
		// `NONE` everywhere: nothing written, nothing shown.
		("CREATE shut:9 SET n = 3", "shut:9"),
		("UPDATE shut SET n = 99", "n: 99 "),
		// A create predicate refuses the record the data clause built.
		("CREATE cond:8 SET n = 20", "cond:8"),
		// A write guard reading a `COMPUTED` field reads its derived value.
		("CREATE comp:3 SET n = 6", "comp:3"),
		("UPDATE comp:2 SET n = 0", "id: comp:2, n: 0,"),
		// Another principal's record.
		("UPDATE owned:2 SET n = 1", "id: owned:2, n: 1, "),
		("DELETE owned:2", "__deleted__"),
	] {
		// The owner is exempt, so each refusal is the permission's and not
		// the statement's.
		let (rows, state) = gated(sql, &TestDb::owner()).await;
		let written = format!("{rows:?} {}", state.join(" | "));
		if forbidden == "__deleted__" {
			assert!(!written.contains("owned: { id: owned:2"), "`{sql}` for the owner: {written}");
		} else {
			assert!(written.contains(forbidden), "`{sql}` for the owner: {written}");
		}
		let (rows, state) = gated(sql, &reader).await;
		let written = format!("{rows:?} {}", state.join(" | "));
		if forbidden == "__deleted__" {
			assert!(
				written.contains("owned: { id: owned:2"),
				"`{sql}` deleted a record it may not: {written}"
			);
		} else {
			assert!(!written.contains(forbidden), "`{sql}` let through {forbidden}: {written}");
		}
	}
}

/// A write guard with a side effect is judged once per gate that reads it: the
/// records it writes are counted in the recorded state.
#[tokio::test]
async fn a_write_guard_is_judged_once_per_gate_that_reads_it() {
	let mut golden = Golden::new("a_write_guard_is_judged_once_per_gate_that_reads_it");
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	let reader = Session::for_record("test", "test", "user", rid);
	for sql in [
		"UPDATE counted SET n += 1",
		"CREATE counted:5 SET n = 1",
		"UPSERT counted:1 SET n = 3",
		"DELETE counted",
	] {
		let (rows, state) = gated(sql, &reader).await;
		let logged = state.iter().filter(|row| row.starts_with("log:")).count();
		assert!(logged > 0, "`{sql}` never judged its guard: {state:?}");
		golden.record(sql, render_written(&rows, &state));
	}
	golden.check();
}
