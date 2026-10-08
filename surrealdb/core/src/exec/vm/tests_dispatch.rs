//! Reads of tables and records whose identity only the data knows hold no
//! compiled expression: a program is lowered with the unit that reads each
//! table it can reach ([`super::dispatch`]), and the run picks the unit by the
//! table the data names. Each read answers its rows, for the owner and for
//! record users under every kind of table permission, with computed fields and
//! field permissions applied once; a principal of another tenant is refused; a
//! database of many tables is read and written on the VM; and running the
//! program compiles nothing.

use std::sync::Arc;

use super::golden::Golden;
use super::tests::{bind, outcome, planner};
use crate::exec::ExecutionContext;
use crate::exec::operators::test_util::TestDb;
use crate::val::Value;

/// Three tables whose permissions are full, conditional on the record's owner
/// and denied. Each holds a field a checked caller may not read, a computed
/// field, a link to a record of another table and a list of links.
fn tables() -> String {
	let mut setup = String::new();
	for (table, permission) in [
		("open", "FULL"),
		("mine", "FOR select WHERE owner = $auth OR owner = $auth.id"),
		("locked", "FOR select NONE"),
	] {
		setup.push_str(&format!(
			"DEFINE TABLE {table} SCHEMALESS PERMISSIONS {permission};
			 DEFINE FIELD secret ON {table} PERMISSIONS FOR select NONE;
			 DEFINE FIELD label ON {table} COMPUTED 'row ' + <string> n;
			 FOR $i IN 1..=6 {{
				 CREATE type::record('{table}', $i) SET
					 owner = IF $i % 2 = 0 {{ user:alice }} ELSE {{ user:bob }},
					 n = $i,
					 secret = 's' + <string> $i,
					 link = type::record('mine', $i % 3 + 1),
					 links = [type::record('open', 1), type::record('locked', 1), type::record('mine', 2)];
			 }};\n"
		));
	}
	setup.push_str(
		"DEFINE TABLE pal TYPE RELATION IN open OUT open LIGHTWEIGHT PERMISSIONS FULL;
		 RELATE open:1->pal->open:2;
		 RELATE open:2->pal->open:3;
		 DEFINE TABLE sealed_pal TYPE RELATION IN open OUT open LIGHTWEIGHT
			 PERMISSIONS FOR select NONE;
		 RELATE open:1->sealed_pal->open:3;",
	);
	setup
}

/// Statements that read a table or a record the data names.
const READS: &[&str] = &[
	"SELECT * FROM type::table(rand::enum('open')) ORDER BY id",
	"SELECT * FROM type::table(rand::enum('mine')) ORDER BY id",
	"SELECT * FROM type::table(rand::enum('locked')) ORDER BY id",
	"SELECT id, label FROM type::table(rand::enum('mine')) ORDER BY id",
	"SELECT * FROM type::table(rand::enum('open')) ORDER BY id LIMIT 2 START 1",
	"SELECT * FROM type::table(rand::enum('mine')) LIMIT 0",
	"SELECT * FROM type::table(rand::enum('missing'))",
	"SELECT * FROM type::table(rand::enum('missing')) LIMIT 0",
	"SELECT in, out FROM type::table(rand::enum('pal')) ORDER BY in",
	"SELECT in, out FROM type::table(rand::enum('sealed_pal'))",
	"SELECT * FROM type::record(rand::enum('open:1'))",
	"SELECT * FROM type::record(rand::enum('mine:2'))",
	"SELECT * FROM type::record(rand::enum('mine:3'))",
	"SELECT * FROM type::record(rand::enum('locked:1'))",
	"SELECT * FROM type::record(rand::enum('open:99'))",
	"SELECT * FROM type::record(rand::enum('missing:1'))",
	"SELECT * FROM type::record(rand::enum('open:1')) LIMIT 0",
	"SELECT * FROM type::record(rand::enum('missing:1')) LIMIT 0",
	"{ LET $f = |$t| (SELECT * FROM type::table($t) ORDER BY id); RETURN [$f('open'), $f('mine'), $f('locked')] }",
	"{ LET $f = |$id| (SELECT secret, label FROM ONLY $id); RETURN [$f(open:1), $f(mine:2), $f(mine:3), $f(locked:1)] }",
	"{ LET $f = |$ids| (SELECT id, label FROM $ids ORDER BY id); RETURN $f([open:1, mine:2, locked:3, mine:3]) }",
	"{ LET $ids = [open:2, mine:2, mine:3, locked:1]; RETURN (SELECT id, secret, label FROM $ids ORDER BY id) }",
	"SELECT id, label FROM [open:1, mine:2, mine:3, locked:1] ORDER BY id",
	"SELECT id, label FROM open:1",
	"SELECT * FROM type::record(rand::enum('open:1'))",
];

/// Statements that follow a link to a record the data names.
const LINKS: &[&str] = &[
	"SELECT id, link.secret, link.label FROM open ORDER BY id",
	"SELECT id, link.* FROM mine ORDER BY id",
	"SELECT id, links.*.label FROM open ORDER BY id",
	"SELECT id, link.{secret, label} FROM open ORDER BY id",
	"SELECT id, links[*].secret FROM mine ORDER BY id",
	"SELECT * FROM open ORDER BY id FETCH link",
	"SELECT * FROM mine ORDER BY id FETCH links",
	"SELECT * FROM locked ORDER BY id FETCH link, links",
	"SELECT * FROM open ORDER BY id FETCH links[0], links[$]",
	"SELECT * FROM open ORDER BY id LIMIT 1 FETCH link.link",
	"SELECT * FROM open ORDER BY id FETCH links[1 + 1]",
	"SELECT * FROM open ORDER BY id FETCH links[0 - 1], links[9]",
	"{ LET $i = 1; RETURN SELECT * FROM mine ORDER BY id FETCH links[$i] }",
	"{ LET $k = 'link'; RETURN SELECT * FROM mine ORDER BY id FETCH link, links[$k] }",
	"SELECT VALUE link FROM open ORDER BY id",
	"SELECT link.link.label FROM open ORDER BY id",
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

/// Every read answers its rows, for the owner and for two record users, and
/// compiles nothing while it runs outside a fault: the first to follow a
/// record link into a table the context has not met faults for that table's
/// definitions.
#[tokio::test]
async fn dispatched_reads_answer_their_rows_and_compile_nothing() {
	let mut golden = Golden::new("dispatched_reads_answer_their_rows_and_compile_nothing");
	let db = TestDb::new_with_auth(&tables()).await;
	let mut declined = Vec::new();
	let mut compiled = Vec::new();
	let mut cold = Vec::new();
	let mut nonempty = 0;
	for (who, session) in
		[("owner", TestDb::owner()), ("alice", session("alice")), ("bob", session("bob"))]
	{
		let ctx = db.exec_ctx_as(&session, crate::kvs::TransactionType::Read).await;
		for sql in READS.iter().chain(LINKS) {
			let Some((vm, first)) = run(&ctx, sql).await else {
				declined.push(format!("`{sql}` for {who}"));
				continue;
			};
			golden.rows(format!("{who}: {sql}"), &vm);
			if matches!(&vm, Ok(rows) if !rows.is_empty()) {
				nonempty += 1;
			}
			if first > 0 && !cold.contains(sql) {
				cold.push(*sql);
			}
			let (again, compiles) = run(&ctx, sql).await.expect("planned the first time");
			assert_eq!(again, vm, "`{sql}` for {who} answers otherwise the second time");
			if compiles > 0 {
				compiled.push(format!("`{sql}` for {who}: {compiles} compilations"));
			}
		}
	}
	assert!(compiled.is_empty(), "{compiled:#?}");
	assert!(cold.is_empty(), "the statements that compile on their first run: {cold:#?}");
	assert!(declined.is_empty(), "the VM declined {declined:#?}");
	assert!(nonempty > 50, "the statements read too little: {nonempty}");
	golden.check();
}

/// A record user sees the rows the table's permission admits and the fields
/// its field permissions leave, and the computed field is derived once.
#[tokio::test]
async fn a_dispatched_read_applies_the_tables_layers_once() {
	let db = TestDb::new_with_auth(&tables()).await;
	let alice = db.exec_ctx_as(&session("alice"), crate::kvs::TransactionType::Read).await;
	let read = async |sql: &str| {
		let (rows, _) = run(&alice, sql).await.expect("covered");
		rows.expect("reads")
	};
	// Alice owns the even rows of `mine`.
	let rows = read("SELECT * FROM type::table(rand::enum('mine')) ORDER BY id").await;
	assert_eq!(rows.len(), 3);
	for row in &rows {
		let Value::Object(object) = row else {
			panic!("{row:?}");
		};
		assert!(object.get("secret").is_none(), "{object:?}");
		assert!(object.get("label").is_some(), "{object:?}");
	}
	assert!(read("SELECT * FROM type::table(rand::enum('locked'))").await.is_empty());
	assert!(read("SELECT * FROM type::record(rand::enum('locked:1'))").await.is_empty());
	assert_eq!(read("SELECT * FROM type::record(rand::enum('mine:2'))").await.len(), 1);
	assert!(read("SELECT * FROM type::record(rand::enum('mine:3'))").await.is_empty());
	assert_eq!(read("SELECT * FROM type::record(rand::enum('open:3'))").await.len(), 1);
}

/// A principal of another tenant is refused by every dispatched read.
#[tokio::test]
async fn a_principal_of_another_tenant_is_refused_by_every_dispatched_read() {
	let db = TestDb::new_with_auth(&tables()).await;
	let owner = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	let ctx = db.exec_ctx_as(&foreign(), crate::kvs::TransactionType::Read).await;
	for sql in [
		"SELECT * FROM type::table(rand::enum('open'))",
		"SELECT * FROM type::record(rand::enum('open:1'))",
		"SELECT id, link.secret FROM open",
		"SELECT * FROM open FETCH link",
		"SELECT * FROM [open:1, mine:2]",
	] {
		let bound = bind(&owner, sql).await;
		let vm = super::plan(&planner(&owner), &bound).await.expect("the VM covers it");
		let vm = outcome(&vm, &ctx).await;
		assert!(vm.is_err(), "`{sql}` was read: {vm:?}");
	}
}

/// A database of many tables is read and written on the VM: a read of a
/// table the data names is lowered with no unit of any table and links the
/// tables its run meets, a read of a record id written in the text holds the
/// units of its tables, and a write that may reach any table, or a delete with
/// its edge cascade, holds the stage set of the database. Each answers its
/// rows and compiles nothing outside the faults that link its tables.
#[tokio::test]
async fn a_database_of_many_tables_is_read_and_written_on_the_vm() {
	let mut golden = Golden::new("a_database_of_many_tables_is_read_and_written_on_the_vm");
	let mut setup = tables();
	for i in 0..100 {
		setup.push_str(&format!(
			"DEFINE TABLE extra_{i} SCHEMALESS PERMISSIONS FULL;
			 DEFINE FIELD label ON extra_{i} COMPUTED 'extra ' + <string> n;\n"
		));
	}
	setup.push_str(
		"DEFINE TABLE likes TYPE RELATION;
		 CREATE extra_7:1 SET n = 7;
		 RELATE open:1->likes->mine:2; RELATE extra_7:1->likes->open:1;
		 RELATE mine:1->likes->extra_7:1;",
	);
	let db = TestDb::new_with_auth(&setup).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	for sql in [
		"SELECT * FROM type::table(rand::enum('open')) ORDER BY id",
		"SELECT * FROM type::table(rand::enum('extra_7')) ORDER BY id",
		"SELECT * FROM open ORDER BY id FETCH link",
		"SELECT id, label FROM [open:1, mine:2] ORDER BY id",
	] {
		let (vm, compiles) = run(&ctx, sql).await.expect("the VM covers it");
		golden.rows(sql, &vm);
		assert_eq!(compiles, 0, "`{sql}` compiled while it ran");
	}
	// Each write runs in a transaction of its own, which is not committed.
	for (sql, shows) in [
		("UPDATE type::table('open') SET n = n + 1 RETURN AFTER", "row 2"),
		("UPDATE type::table('extra_7') SET n = n + 1 RETURN AFTER", "extra 8"),
		("DELETE open:1 RETURN BEFORE", "row 1"),
		("DELETE type::record('extra_7', 1) RETURN BEFORE", "extra 7"),
		("{ DELETE open:1; RETURN SELECT in, out FROM likes ORDER BY in }", "mine"),
		("{ DELETE type::record('extra_7', 1); RETURN SELECT in, out FROM likes }", "mine"),
	] {
		let writer = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
		let (vm, compiles) = run(&writer, sql).await.expect("the VM covers it");
		golden.rows(sql, &vm);
		assert!(format!("{vm:?}").contains(shows), "`{sql}`: {vm:?}");
		assert_eq!(compiles, 0, "`{sql}` compiled while it ran");
	}
	golden.check();
}

/// The program `sql` lowers to for `ctx`.
async fn program_of(ctx: &ExecutionContext, sql: &str) -> super::program::Program {
	let bound = bind(ctx, sql).await;
	let planner = planner(ctx);
	let physical =
		super::optimize::optimize(super::map::map(&planner, &bound).await.expect("statement maps"));
	super::lower::lower(&planner, &physical).await.expect("program lowers")
}

/// A program that reads a record or a table the data names holds a dispatch
/// naming each table its sites name, whose read units the compile cache was
/// asked for when the query arrived, and holds none of those units itself:
/// its run loads them by name. One that may read any table names none, and
/// its run loads those it meets. A program that reads nothing by id holds no
/// dispatch.
#[tokio::test]
async fn a_program_names_the_tables_its_sites_name() {
	let db = TestDb::new_with_auth(&tables()).await;
	let owner = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	let alice = db.exec_ctx_as(&session("alice"), crate::kvs::TransactionType::Read).await;
	for (ctx, sql, tables, units) in [
		(&owner, "SELECT * FROM type::table(rand::enum('open'))", Some(0), 0),
		(&alice, "SELECT * FROM type::table(rand::enum('open'))", Some(0), 0),
		(&owner, "SELECT * FROM [open:1, mine:2]", Some(2), 0),
		(&alice, "SELECT * FROM [open:1, mine:2]", Some(2), 0),
		(&owner, "SELECT * FROM open FETCH link", Some(0), 0),
		(&owner, "SELECT * FROM open:1", None, 0),
		(&owner, "SELECT * FROM open WHERE n = 1", None, 0),
	] {
		let program = program_of(ctx, sql).await;
		let held = program.dispatch.as_ref().map(|dispatch| dispatch.tables.len());
		assert_eq!(held, tables, "`{sql}`");
		assert_eq!(program.units.len(), units, "`{sql}`");
	}
}

/// How long planning a statement that may read any table, and a write with
/// its cascades, takes as the database grows by tables none of the statements
/// names: bind and plan, best of 41, each in a transaction of its own; and the
/// first plan after a table the statement does not name is redefined, the
/// median of 9. Run with
/// `cargo test --release -p surrealdb-core --lib planning_against_table_count
/// -- --ignored --nocapture`.
#[tokio::test]
#[ignore = "a measurement, not a check"]
async fn planning_against_table_count() {
	for extra in [0, 20, 60] {
		let mut setup = "
			DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
			DEFINE TABLE post SCHEMALESS PERMISSIONS FULL;
			FOR $i IN 1..=10 {
				CREATE type::record('person', $i) SET name = <string> $i;
				CREATE type::record('post', $i) SET author = type::record('person', $i), n = $i;
			};
			DEFINE TABLE aside SCHEMALESS PERMISSIONS FULL;
		"
		.to_owned();
		for i in 0..extra {
			setup.push_str(&format!("DEFINE TABLE extra_{i} SCHEMALESS PERMISSIONS FULL;\n"));
		}
		let db = TestDb::new_with_auth(&setup).await;
		eprintln!("extra tables {extra}:");
		let mut redefined = 0;
		for (sql, kind) in [
			("SELECT * FROM post FETCH author", crate::kvs::TransactionType::Read),
			("SELECT author.* FROM post", crate::kvs::TransactionType::Read),
			("SELECT author.{ name } FROM post", crate::kvs::TransactionType::Read),
			("SELECT VALUE author.name FROM post", crate::kvs::TransactionType::Read),
			("DELETE post:1", crate::kvs::TransactionType::Write),
			("UPDATE type::table('post') SET n = 2", crate::kvs::TransactionType::Write),
		] {
			let mut line = format!("  {sql}:");
			let mut best = std::time::Duration::MAX;
			for _ in 0..41 {
				let ctx = db.exec_ctx_as(&TestDb::owner(), kind).await;
				let started = web_time::Instant::now();
				let bound = bind(&ctx, sql).await;
				super::plan(&planner(&ctx), &bound).await.expect("the VM plans it");
				best = best.min(started.elapsed());
				ctx.txn().cancel().await.expect("cancel");
			}
			line.push_str(&format!(" plan {}us", best.as_micros()));
			let mut firsts = Vec::new();
			for _ in 0..9 {
				db.run(&format!(
					"DEFINE TABLE OVERWRITE aside SCHEMALESS PERMISSIONS FULL COMMENT '{redefined}';"
				))
				.await;
				redefined += 1;
				let ctx = db.exec_ctx_as(&TestDb::owner(), kind).await;
				let started = web_time::Instant::now();
				let bound = bind(&ctx, sql).await;
				super::plan(&planner(&ctx), &bound).await.expect("the VM plans it");
				firsts.push(started.elapsed());
				ctx.txn().cancel().await.expect("cancel");
			}
			firsts.sort();
			line.push_str(&format!(" first {}us", firsts[4].as_micros()));
			eprintln!("{line}");
		}
	}
}

/// A run loads the unit of each table it meets from the compile cache, and
/// compiles one the cache holds none of in one fault, once however many of
/// its records it reads; a later run loads what the first compiled, with no
/// fault.
#[tokio::test]
async fn a_run_links_each_table_it_meets_once() {
	let mut golden = Golden::new("a_run_links_each_table_it_meets_once");
	let db = TestDb::new_with_auth(&tables()).await;
	// The links of `open` reach `mine` alone, its lists every table. Alice's
	// reads of `mine` judge its permission, which reads `$auth.id` where she
	// does not own the record: a read of `user`, which is not defined, that
	// waives permission checks.
	for (who, session, link) in
		[("owner", TestDb::owner(), (1, 1)), ("alice", session("alice"), (2, 1))]
	{
		let ctx = db.exec_ctx_as(&session, crate::kvs::TransactionType::Read).await;
		for (sql, (faulted, lowered)) in [
			("SELECT * FROM open ORDER BY id FETCH link", link),
			("SELECT * FROM open ORDER BY id FETCH links", (2, 2)),
			("SELECT * FROM open ORDER BY id FETCH links", (0, 0)),
		] {
			let faults = super::fault::READ_FAULTS.with(std::cell::Cell::get);
			let (compiled, _) = super::units::CATALOG_UNITS.with(std::cell::Cell::get);
			let (vm, compiles) = run(&ctx, sql).await.expect("the VM covers it");
			let faults = super::fault::READ_FAULTS.with(std::cell::Cell::get) - faults;
			let compiled = super::units::CATALOG_UNITS.with(std::cell::Cell::get).0 - compiled;
			golden.rows(format!("{who}: {sql}"), &vm);
			assert_eq!(compiles, 0, "`{sql}` for {who} compiled while it ran");
			assert_eq!((faults, compiled), (faulted, lowered), "`{sql}` for {who}");
		}
	}
	golden.check();
}

/// A delete's repair reads each referencing record through the unit of the
/// table the reference key names, lowered with the repair: it links nothing
/// while it runs, however many records the delete repairs.
#[tokio::test]
async fn a_repair_reads_its_records_through_a_unit_lowered_with_it() {
	let setup = "
		DEFINE TABLE p SCHEMALESS PERMISSIONS FULL;
		FOR $i IN 0..50 { CREATE type::record('p', $i) };
		DEFINE FIELD owner ON c TYPE option<record<p>> REFERENCE ON DELETE UNSET;
		FOR $i IN 0..50 { CREATE type::record('c', $i) SET owner = type::record('p', $i) };
	";
	let db = TestDb::new_with_auth(setup).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	let faults = super::fault::READ_FAULTS.with(std::cell::Cell::get);
	let (_, compiles) = run(&ctx, "DELETE p").await.expect("the VM covers it");
	let faults = super::fault::READ_FAULTS.with(std::cell::Cell::get) - faults;
	assert_eq!((faults, compiles), (0, 0), "(read faults, compiles outside a fault)");
	let (rows, _) = run(&ctx, "SELECT * FROM c ORDER BY id").await.expect("reads");
	let mut golden = Golden::new("a_repair_reads_its_records_through_a_unit_lowered_with_it");
	golden.rows("SELECT * FROM c ORDER BY id", &rows);
	golden.check();
}

/// The reads a run links are the run's, whichever of its programs makes
/// them: two programs started in one run, beneath no program that reads by
/// dispatch — as a statement run once per record is started beneath a write
/// — link a table both read once, and the second finds the first's link.
#[tokio::test]
async fn the_programs_of_a_run_share_the_reads_it_linked() {
	let db = TestDb::new_with_auth("DEFINE TABLE x SCHEMALESS PERMISSIONS FULL; CREATE x:1;").await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	let program = program_of(&ctx, "SELECT * FROM type::table(rand::enum('card'))").await;
	let run = super::closure::with_units(&program, &ctx).expect("a run's table");
	let rid = crate::val::RecordId::new(surrealdb_strand::TableName::new("x".to_owned()), 1i64);
	let faults = super::fault::READ_FAULTS.with(std::cell::Cell::get);
	for _ in 0..3 {
		let running = super::dispatch::with_dispatch(&program, &run).expect("a dispatch");
		let read =
			crate::exec::operators::fetch::fetch_record(&running, &rid).await.expect("reads");
		assert_eq!(
			read,
			crate::exec::operators::fetch::fetch_record(&ctx, &rid).await.expect("reads")
		);
	}
	assert_eq!(super::fault::READ_FAULTS.with(std::cell::Cell::get) - faults, 1);
}

/// A read a run linked stands only while the catalog it was linked against
/// does: a field the run's transaction defines after the run linked the read
/// of its table is computed by the next read the run makes of the table.
#[tokio::test]
async fn a_linked_read_stands_only_while_the_runs_catalog_does() {
	let db = TestDb::new_with_auth("DEFINE TABLE x SCHEMALESS PERMISSIONS FULL; CREATE x:1;").await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	let program = program_of(&ctx, "SELECT * FROM type::table(rand::enum('card'))").await;
	let running = super::dispatch::with_dispatch(&program, &ctx).expect("a dispatch");
	let rid = crate::val::RecordId::new(surrealdb_strand::TableName::new("x".to_owned()), 1i64);
	let first = crate::exec::operators::fetch::fetch_record(&running, &rid).await.expect("reads");
	assert!(!format!("{first:?}").contains("late"), "{first:?}");
	run(&ctx, "DEFINE FIELD label ON x COMPUTED 'late'")
		.await
		.expect("the VM covers it")
		.0
		.expect("defines");
	let faults = super::fault::READ_FAULTS.with(std::cell::Cell::get);
	let second = crate::exec::operators::fetch::fetch_record(&running, &rid).await.expect("reads");
	assert_eq!(super::fault::READ_FAULTS.with(std::cell::Cell::get) - faults, 1, "relinked");
	assert!(format!("{second:?}").contains("late"), "{second:?}");
}

/// A read that waives the permission checks of the frame it is made in — a
/// write's gate, a field it computes — links only the unit it reads with:
/// whether the frame's own unit would serve the read is judged without
/// linking or lowering it, and the record is read without checks.
#[tokio::test]
async fn a_read_that_waives_checks_links_only_the_unit_it_reads_with() {
	let db = TestDb::new_with_auth(
		"DEFINE TABLE member SCHEMALESS PERMISSIONS FOR select WHERE false;
		 DEFINE FIELD badge ON member COMPUTED 'member ' + name;
		 DEFINE TABLE card SCHEMALESS PERMISSIONS FULL;
		 CREATE member:1 SET name = 'a';",
	)
	.await;
	let alice = db.exec_ctx_as(&session("alice"), crate::kvs::TransactionType::Read).await;
	assert!(!alice.root().skip_fetch_perms, "her reads are checked");
	// A program that may read any table, run by her, reads through the units
	// its run links.
	let program = program_of(&alice, "SELECT * FROM type::table(rand::enum('card'))").await;
	let running = super::dispatch::with_dispatch(&program, &alice).expect("a dispatch");
	let rid =
		crate::val::RecordId::new(surrealdb_strand::TableName::new("member".to_owned()), 1i64);
	let faults = super::fault::READ_FAULTS.with(std::cell::Cell::get);
	let (compiled, _) = super::units::CATALOG_UNITS.with(std::cell::Cell::get);
	let vm = crate::exec::operators::fetch::fetch_record_no_perms(&running, &rid).await;
	let faults = super::fault::READ_FAULTS.with(std::cell::Cell::get) - faults;
	let compiled = super::units::CATALOG_UNITS.with(std::cell::Cell::get).0 - compiled;
	assert!(format!("{vm:?}").contains("member a"), "{vm:?}");
	assert_eq!((faults, compiled), (1, 1), "a unit no read uses was linked");
}

/// How long planning a statement that dispatches over tables takes as the
/// database holds more tables: the first plan, which lowers what the node
/// keeps, and the best of several, each in a transaction of its own.
#[tokio::test]
#[ignore = "a measurement, not a check"]
async fn dispatch_planning_by_table_count() {
	for extra in [0usize, 2, 20, 60, 62, 100] {
		let mut setup = String::from(
			"DEFINE TABLE person SCHEMALESS; DEFINE TABLE post SCHEMALESS;
			 DEFINE TABLE typed SCHEMAFULL; DEFINE FIELD author ON typed TYPE record<person>;
			 CREATE person:1 SET name = 'a'; CREATE post:1 SET author = person:1, n = 1;
			 CREATE typed:1 SET author = person:1;",
		);
		for i in 0..extra {
			setup.push_str(&format!(
				"DEFINE TABLE t{i} SCHEMAFULL PERMISSIONS FOR select WHERE true;
				 DEFINE FIELD a ON t{i} TYPE int;
				 DEFINE FIELD b ON t{i} TYPE string COMPUTED 'x' + <string> a;"
			));
		}
		let db = TestDb::new(&setup).await;
		for sql in [
			"SELECT * FROM post WHERE n = 1",
			"SELECT * FROM post FETCH author",
			"SELECT author.name FROM post",
			"SELECT * FROM typed FETCH author",
			"DELETE post:1",
			"CREATE type::record('post', 9) SET n = 2",
			"UPDATE type::table('post') SET n = 2",
		] {
			let write = !sql.starts_with("SELECT");
			let mut best_vm = std::time::Duration::MAX;
			let mut declined = false;
			let mut first = None;
			for _ in 0..7 {
				let ctx = if write {
					db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await
				} else {
					db.exec_ctx().await
				};
				let bound = bind(&ctx, sql).await;
				let started = web_time::Instant::now();
				let plan = super::plan(&planner(&ctx), &bound).await;
				best_vm = best_vm.min(started.elapsed());
				first.get_or_insert(started.elapsed());
				declined = plan.is_none();
			}
			let first = first.unwrap_or_default();
			eprintln!(
				"PLANCOST extra={extra} `{sql}` best={best_vm:?} first={first:?} declined={declined}"
			);
		}
	}
}

/// A read of a table the data names, of a table defined after the statement
/// was planned, which the run links the unit of, takes the statement's
/// `START` and `LIMIT` into its read of storage as a read of a table named in
/// the text does: it reads no more records than the paging admits before the
/// statement has its rows, and a predicate that keeps fewer rows reads on
/// until it has them.
#[tokio::test]
async fn a_linked_table_read_reads_no_more_records_than_its_paging_admits() {
	let mut golden =
		Golden::new("a_linked_table_read_reads_no_more_records_than_its_paging_admits");
	let db = TestDb::new_with_auth("DEFINE TABLE other SCHEMALESS PERMISSIONS FULL;").await;
	let planned = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	let statements = [
		("SELECT * FROM type::table(rand::enum('fresh')) LIMIT 10", 10, Some(10)),
		("SELECT * FROM type::table(rand::enum('fresh')) LIMIT 10 START 5", 10, Some(15)),
		("SELECT n FROM type::table(rand::enum('fresh')) WHERE n > 2990 LIMIT 5", 5, None),
		("SELECT * FROM type::table(rand::enum('fresh')) LIMIT 0", 0, Some(0)),
	];
	let mut plans = Vec::new();
	for (sql, _, _) in statements {
		let bound = bind(&planned, sql).await;
		let vm = super::plan(&planner(&planned), &bound).await.expect("the VM covers it");
		plans.push(vm);
	}
	db.run(
		"DEFINE TABLE fresh SCHEMALESS PERMISSIONS FULL;
		 DEFINE FIELD label ON fresh COMPUTED 'n' + <string> n;
		 FOR $i IN 1..=3000 { CREATE type::record('fresh', $i) SET n = $i };",
	)
	.await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	let scanned = || super::host::RECORDS_SCANNED.with(std::cell::Cell::get);
	// A table named in the text reads no more than one batch of storage.
	let before = scanned();
	let (named, _) = run(&ctx, "SELECT * FROM fresh LIMIT 10 START 5").await.expect("covered");
	assert_eq!(named.expect("reads").len(), 10);
	let batch = scanned() - before;
	assert!(batch < 3000, "a named table read {batch} records");
	for ((sql, rows, admits), vm) in statements.into_iter().zip(&plans) {
		let faults = super::fault::READ_FAULTS.with(std::cell::Cell::get);
		let before = scanned();
		let answer = outcome(vm, &ctx).await.expect("reads");
		let read = scanned() - before;
		let faults = super::fault::READ_FAULTS.with(std::cell::Cell::get) - faults;
		assert!(faults <= 1, "`{sql}` faulted {faults} times");
		golden.rows(sql, &Ok(answer.clone()));
		assert_eq!(answer.len(), rows, "`{sql}`");
		match admits {
			Some(admits) => assert!(read <= admits.min(batch), "`{sql}` read {read} records"),
			None => assert!(read >= 2995, "`{sql}` stopped reading early: {read}"),
		}
	}
	golden.check();
}

/// A table whose select permission holds a statement that writes data is read
/// by a unit of its own: the statement is a block statement of the unit, whose
/// pipeline is lowered before the read's, so the read is the one pipeline no
/// block statement owns. A record user's read of such a record fails with the
/// refusal the statement raises: a scan names the check that failed, a record
/// read by dereferencing it — linked in a fault where the data names it —
/// fails its table permission under the context a record read by id is
/// checked under, which the refusal's own message shows through, and a field
/// permission is named either way.
#[tokio::test]
async fn a_table_whose_permission_holds_a_write_is_read_by_a_unit_of_its_own() {
	let setup = "
		DEFINE TABLE d SCHEMALESS PERMISSIONS FOR select WHERE (CREATE x:1) != NONE, FOR create FULL;
		CREATE d:1;
		DEFINE TABLE g SCHEMALESS PERMISSIONS FULL;
		DEFINE FIELD f ON g TYPE option<int> PERMISSIONS FOR select WHERE (DELETE x) != NONE;
		CREATE g:1 SET f = 1;
	";
	let refusal = "A PERMISSIONS clause cannot contain a statement that modifies data";
	let db = TestDb::new_with_auth(setup).await;
	let alice = db.exec_ctx_as(&session("alice"), crate::kvs::TransactionType::Read).await;
	// The deref runs first, so no unit of `d` is cached and the read faults.
	for (sql, error, faulted) in [
		(
			"RETURN type::record(rand::enum('d:1')).*",
			format!("Permission check failed: {refusal}"),
			Some(1),
		),
		("SELECT * FROM d", format!("Failed to check permission: {refusal}"), Some(0)),
		("RETURN g:1.*", format!("Failed to check field permission: {refusal}"), None),
	] {
		let faults = super::fault::READ_FAULTS.with(std::cell::Cell::get);
		let (rows, _) = run(&alice, sql).await.expect("the VM covers it");
		let faults = super::fault::READ_FAULTS.with(std::cell::Cell::get) - faults;
		assert_eq!(rows, Err(error), "`{sql}`");
		if let Some(faulted) = faulted {
			assert_eq!(faults, faulted, "`{sql}` read faults");
		}
	}
	let table = surrealdb_strand::TableName::new("d".to_owned());
	let program =
		super::dispatch::table_unit(&planner(&alice), &table).await.expect("the read lowers");
	assert!(!program.groups.is_empty(), "the permission's statement is a block statement");
	let own = program.own_pipeline().expect("one pipeline no block statement owns");
	assert!(own.group.is_none());
	assert!(
		!std::ptr::eq(own, &program.pipelines[0]),
		"the block statement's pipeline is lowered before the read's"
	);
}
