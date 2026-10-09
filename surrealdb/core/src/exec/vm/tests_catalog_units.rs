//! What a plan lowers from the catalog alone — the read of each table a
//! dispatch runs, which a run links when it meets a table its program holds
//! no unit for, a delete's edge cascade and the reference fields its repairs
//! are lowered from — is kept by the node and linked by later plans and runs
//! while everything it was lowered against holds ([`super::units`]). Each
//! test changes the schema between two runs of the same statement and checks
//! the second run answers the changed schema's rows, which a unit kept past
//! the change would not; and that a run with nothing changed lowers nothing
//! anew.

use surrealdb_types::ToSql;

use super::golden::{Golden, render_outcome};
use super::tests::{bind, outcome, planner};
use crate::exec::ExecutionContext;
use crate::exec::operators::test_util::TestDb;
use crate::val::Value;

/// How many catalog units this thread has lowered and linked so far.
fn catalog_units() -> (usize, usize) {
	super::units::CATALOG_UNITS.with(std::cell::Cell::get)
}

/// Run `sql` on the VM over a fresh transaction of `session`, answering its
/// rows; `None` where the VM declines it.
async fn read(db: &TestDb, session: &crate::dbs::Session, sql: &str) -> Option<Value> {
	let ctx = db.exec_ctx_as(session, crate::kvs::TransactionType::Read).await;
	let rows = run(&ctx, sql).await?;
	ctx.txn().cancel().await.expect("cancel");
	Some(Value::from(rows.unwrap_or_else(|e| vec![Value::from(e)])))
}

async fn run(ctx: &ExecutionContext, sql: &str) -> Option<Result<Vec<Value>, String>> {
	let bound = bind(ctx, sql).await;
	let op = super::plan(&planner(ctx), &bound).await?;
	Some(outcome(&op, ctx).await)
}

/// `sql` on the VM for `session`, which must cover it; the answer, and the
/// catalog units the VM's plan lowered.
async fn answered(db: &TestDb, session: &crate::dbs::Session, sql: &str) -> (Value, usize) {
	let (lowered, _) = catalog_units();
	let vm = read(db, session, sql).await.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
	let lowered = catalog_units().0 - lowered;
	(vm, lowered)
}

/// A run's answers, one per line.
fn render_answers(answers: &[Answer]) -> String {
	answers.iter().map(|answer| answer.to_sql().replace('\n', "\\n")).collect::<Vec<_>>().join("\n")
}

fn record_user(user: &str) -> crate::dbs::Session {
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", user));
	crate::dbs::Session::for_record("test", "test", "user", rid)
}

const READ_SETUP: &str = "
	DEFINE TABLE open SCHEMALESS PERMISSIONS FULL;
	DEFINE FIELD label ON open COMPUTED 'row ' + <string> n;
	DEFINE TABLE mine SCHEMALESS PERMISSIONS FOR select WHERE owner = $auth;
	CREATE open:1 SET n = 1, link = mine:1;
	CREATE open:2 SET n = 2, link = mine:2;
	CREATE mine:1 SET owner = user:alice, n = 1;
	CREATE mine:2 SET owner = user:bob, n = 2;
";

/// A table's read unit is linked while the table stands as it was lowered
/// against, and lowered again once a field of it changes; the plan after the
/// change answers the changed table's rows.
#[tokio::test]
async fn a_kept_read_stands_until_its_table_changes() {
	let mut golden = Golden::new("a_kept_read_stands_until_its_table_changes");
	let db = TestDb::new_with_auth(READ_SETUP).await;
	let alice = record_user("alice");
	let sql = "SELECT * FROM open ORDER BY id FETCH link";
	let (first, _) = answered(&db, &alice, sql).await;
	let (again, lowered) = answered(&db, &alice, sql).await;
	assert_eq!(first, again);
	assert_eq!(lowered, 0, "a plan with nothing changed lowered {lowered} catalog units");
	db.run("DEFINE FIELD OVERWRITE secret ON mine COMPUTED 'hidden ' + <string> n PERMISSIONS FOR select NONE;
		DEFINE FIELD OVERWRITE shown ON mine COMPUTED n * 10;")
		.await;
	let (changed, lowered) = answered(&db, &alice, sql).await;
	assert_ne!(first, changed, "the new fields of mine are not read");
	assert!(lowered > 0, "the read of mine was linked past a change to its fields");
	golden.record("before the change", first.to_sql());
	golden.record("after the change", changed.to_sql());
	golden.check();
}

/// The stored `!cv` value of `table` in `test`/`test`, raw.
async fn stored_version(
	db: &TestDb,
	table: &str,
) -> (surrealdb_kvs::Key<'static>, Option<Vec<u8>>) {
	use surrealdb_kvs::key::KVKey;

	use crate::catalog::providers::DatabaseProvider;
	let txn = db.datastore().transaction(crate::kvs::TransactionType::Read).await.expect("txn");
	let def = txn.get_db_by_name("test", "test", None).await.expect("read").expect("defined");
	let table = crate::val::TableName::new(table);
	let key = surrealdb_datastore::key::schema::CatalogVersionKey {
		ns: def.namespace_id,
		db: def.database_id,
		tb: std::borrow::Cow::Borrowed(&table),
	}
	.encode_key()
	.expect("encodes");
	let stored = txn.get(key.clone(), None).await.expect("read");
	txn.cancel().await.expect("cancel");
	(key, stored)
}

/// A build that predates catalog versions shares the store in a rolling
/// upgrade and changes a table's fields without moving its version. A read
/// unit kept under that version is not linked past the change: the version
/// no longer matches the definition it was stamped beside, and the plan
/// after the change answers the changed table's rows.
#[tokio::test]
async fn a_kept_read_does_not_stand_past_a_change_an_older_build_made() {
	let db = TestDb::new_with_auth(READ_SETUP).await;
	let alice = record_user("alice");
	let sql = "SELECT * FROM open ORDER BY id FETCH link";
	let (first, _) = answered(&db, &alice, sql).await;
	let (again, lowered) = answered(&db, &alice, sql).await;
	assert_eq!(first, again);
	assert_eq!(lowered, 0, "a plan with nothing changed lowered {lowered} catalog units");
	let (key, before) = stored_version(&db, "mine").await;
	let before = before.expect("this build stamped mine");
	db.run("DEFINE FIELD OVERWRITE secret ON mine COMPUTED 'hidden ' + <string> n PERMISSIONS FOR select NONE;
		DEFINE FIELD OVERWRITE shown ON mine COMPUTED n * 10;")
		.await;
	// Put back the version as it stood: the older build wrote the fields and
	// the table's definition, and left `!cv` alone.
	let txn = db.datastore().transaction(crate::kvs::TransactionType::Write).await.expect("txn");
	txn.set(key, before).await.expect("write");
	txn.commit().await.expect("commit");
	let (changed, _) = answered(&db, &alice, sql).await;
	assert_ne!(first, changed, "the read of mine was linked past a change to its fields");
	let (again, _) = answered(&db, &alice, sql).await;
	assert_eq!(changed, again);
}

/// How many reads of a table this thread's runs faulted for.
fn read_faults() -> usize {
	super::fault::READ_FAULTS.with(std::cell::Cell::get)
}

/// A read of a table the data names links the unit of that table by the
/// table's own catalog, in one fault per run: the first run's fault lowers it
/// and the node keeps it, a later run's links it however many tables are
/// defined meanwhile, and a change to the table has the next run's lower it
/// anew, with its permission and computed fields applied.
#[tokio::test]
async fn a_linked_read_stands_until_its_own_table_changes() {
	let mut golden = Golden::new("a_linked_read_stands_until_its_own_table_changes");
	let db = TestDb::new_with_auth(READ_SETUP).await;
	let alice = record_user("alice");
	let sql = "SELECT * FROM type::table(rand::enum('fresh')) ORDER BY id";
	// A table not defined is read by no unit: the fault finds none to link.
	let faults = read_faults();
	let (missing, lowered) = answered(&db, &alice, sql).await;
	assert!(format!("{missing:?}").contains("does not exist"), "{missing:?}");
	assert_eq!((lowered, read_faults() - faults), (0, 1));
	db.run(
		"DEFINE TABLE fresh SCHEMALESS PERMISSIONS FOR select WHERE n > 1;
		 DEFINE FIELD label ON fresh COMPUTED 'fresh ' + <string> n;
		 CREATE fresh:1 SET n = 1; CREATE fresh:2 SET n = 2; CREATE fresh:3 SET n = 3;",
	)
	.await;
	let faults = read_faults();
	let (rows, lowered) = answered(&db, &alice, sql).await;
	assert_eq!((lowered, read_faults() - faults), (1, 1), "the run did not fault for fresh");
	let Value::Array(rows) = &rows else {
		panic!("{rows:?}");
	};
	assert_eq!(rows.len(), 2, "{rows:?}");
	assert!(format!("{rows:?}").contains("fresh 2"), "{rows:?}");
	// Other tables defined between two runs leave the read standing.
	db.run("DEFINE TABLE unrelated SCHEMALESS; DEFINE TABLE another SCHEMALESS;").await;
	let (_, linked) = catalog_units();
	let faults = read_faults();
	let (again, lowered) = answered(&db, &alice, sql).await;
	assert_eq!(Value::Array(rows.clone()), again);
	assert_eq!((lowered, read_faults() - faults), (0, 0), "an unrelated table moved the read");
	assert_eq!(catalog_units().1, linked + 1, "the run did not load the cache's unit");
	// A change to the table itself has the next run lower it anew.
	db.run("DEFINE FIELD OVERWRITE label ON fresh COMPUTED 'renewed ' + <string> n;").await;
	let faults = read_faults();
	let (changed, lowered) = answered(&db, &alice, sql).await;
	assert_eq!((lowered, read_faults() - faults), (1, 1), "the read was linked past a change");
	assert!(format!("{changed:?}").contains("renewed 2"), "{changed:?}");
	golden.record("missing", missing.to_sql());
	golden.record("defined", Value::Array(rows.clone()).to_sql());
	golden.record("redefined", changed.to_sql());
	golden.check();
}

/// A permission is definer code, which reads none of the variables a session
/// sets: the read it gates answers alike for every session of the principal,
/// whatever the variable holds, by the database's parameter of the name.
#[tokio::test]
async fn a_permission_reads_none_of_the_sessions_variables() {
	let mut golden = Golden::new("a_permission_reads_none_of_the_sessions_variables");
	let db = TestDb::new_with_auth(
		"DEFINE TABLE gated SCHEMALESS PERMISSIONS FOR select WHERE $flag = true;
		 DEFINE PARAM $flag VALUE false PERMISSIONS FULL;
		 CREATE gated:1 SET n = 1;",
	)
	.await;
	let with = |flag: bool| {
		let mut session = record_user("alice");
		session.variables.insert("flag", flag);
		session
	};
	let sql = "SELECT * FROM type::table(rand::enum('gated'))";
	let (shown, _) = answered(&db, &with(true), sql).await;
	assert_eq!(shown.to_sql(), "[]", "a session variable reached the permission");
	let (hidden, _) = answered(&db, &with(false), sql).await;
	assert_eq!(shown, hidden, "a session variable decided the permission");
	db.run("DEFINE PARAM OVERWRITE $flag VALUE true PERMISSIONS FULL;").await;
	let (granted, _) = answered(&db, &with(false), sql).await;
	assert_eq!(granted.to_sql(), "[{ id: gated:1, n: 1 }]", "the database's parameter did not");
	golden.record("flag true", shown.to_sql());
	golden.record("flag false", hidden.to_sql());
	golden.record("parameter true", granted.to_sql());
	golden.check();
}

/// A read whose condition reads a value of the session binds it into the
/// read's code: the read is linked only by a plan whose session holds the same
/// value, not merely an equal one: `1`, `1.0f` and `1dec`, `0.0f` and
/// `-0.0f`, and arrays and objects of them compare equal, but a read that
/// bound one answers as that one. Each run answers on a node that lowered the
/// read for every value before it as on a node that lowered none.
#[tokio::test]
async fn a_read_is_linked_only_where_the_values_it_binds_are_the_same() {
	let setup = "DEFINE TABLE gated SCHEMALESS PERMISSIONS FULL;
		 CREATE gated:1 SET n = 1;";
	let with = |flag: &str| {
		let mut session = record_user("alice");
		session.variables.insert("flag", crate::syn::value(flag).expect("parses"));
		session
	};
	let sql = "SELECT VALUE n FROM type::table(rand::enum('gated'))
		WHERE <string> $flag IN ['1', '-0f', '[1]', '{ a: 1 }']";
	let warm = TestDb::new_with_auth(setup).await;
	let flags = ["1", "1.0f", "1dec", "-0.0f", "0.0f", "[1]", "[1.0f]", "{ a: 1 }", "{ a: 1.0f }"];
	for flag in flags.iter().chain(flags.iter()) {
		let cold = TestDb::new_with_auth(setup).await;
		let (expected, _) = answered(&cold, &with(flag), sql).await;
		let (answer, _) = answered(&warm, &with(flag), sql).await;
		assert_eq!(answer, expected, "$flag = {flag}");
	}
	assert_eq!(answered(&warm, &with("1"), sql).await.0.to_sql(), "[1]");
	assert_eq!(answered(&warm, &with("1.0f"), sql).await.0.to_sql(), "[]");
	assert_eq!(answered(&warm, &with("-0.0f"), sql).await.0.to_sql(), "[1]");
	assert_eq!(answered(&warm, &with("0.0f"), sql).await.0.to_sql(), "[]");
}

/// A `FETCH` of a field whose kind names the tables of its records holds the
/// units of those tables alone, however many tables the database holds; a
/// field of no kind may name any table and holds none, its run linking the
/// tables it meets. A record outside the tables the kind names, which data
/// written before the kind changed may hold, is read by the unit its run
/// links for its table, faulted for once however many records name it, and
/// nothing is compiled outside that fault.
#[tokio::test]
async fn a_fetch_holds_the_units_of_the_tables_its_field_kinds_name() {
	let mut golden = Golden::new("a_fetch_holds_the_units_of_the_tables_its_field_kinds_name");
	let mut setup = String::from(
		"DEFINE TABLE person SCHEMALESS; DEFINE TABLE other SCHEMALESS;
		 DEFINE FIELD label ON other COMPUTED 'other ' + name;
		 DEFINE TABLE post SCHEMAFULL;
		 DEFINE FIELD author ON post TYPE option<record<other>>;
		 DEFINE FIELD loose ON post TYPE any;
		 DEFINE FIELD many ON post TYPE option<array<record<person>>>;
		 CREATE person:1 SET name = 'p'; CREATE other:1 SET name = 'o';
		 CREATE post:1 SET author = other:1, loose = person:1, many = [person:1];
		 CREATE post:2 SET author = other:1, loose = other:1, many = [person:1];
		 DEFINE FIELD OVERWRITE author ON post TYPE option<record<person>>;",
	);
	for i in 0..10 {
		setup.push_str(&format!("DEFINE TABLE t{i} SCHEMALESS;"));
	}
	let db = TestDb::new_with_auth(&setup).await;
	let owner = TestDb::owner();
	let ctx = db.exec_ctx_as(&owner, crate::kvs::TransactionType::Read).await;
	for (sql, held) in [
		("SELECT * FROM post FETCH author", 1),
		("SELECT * FROM post FETCH many", 1),
		("SELECT * FROM post FETCH author, many", 1),
		("SELECT * FROM post FETCH loose", 0),
		("SELECT author AS loose FROM post FETCH loose", 0),
	] {
		let bound = bind(&ctx, sql).await;
		let planner = planner(&ctx);
		let physical = super::optimize::optimize(
			super::map::map(&planner, &bound).await.expect("statement maps"),
		);
		let program = super::lower::lower(&planner, &physical).await.expect("program lowers");
		let tables = program.dispatch.as_ref().map_or(0, |dispatch| dispatch.tables.len());
		assert_eq!(tables, held, "`{sql}`");
	}
	// `other` lies outside the kind of `author`: both posts name `other:1`,
	// read by the unit of `other` the run links in one fault, its computed
	// field derived; the first run's fault compiles it, and a later run loads
	// it from the compile cache without a fault. `loose` may name any table,
	// so that run loads `person` and `other`, both kept.
	for (sql, faulted, lowered) in [
		("SELECT * FROM post ORDER BY id FETCH author", 1, 1),
		("SELECT * FROM post ORDER BY id FETCH author", 0, 0),
		("SELECT * FROM post ORDER BY id FETCH many, loose", 0, 0),
	] {
		let faults = read_faults();
		let compiles = crate::exec::compiles::count();
		let (rows, compiled) = answered(&db, &owner, sql).await;
		assert!(format!("{rows:?}").contains("name"), "`{sql}` fetched nothing: {rows:?}");
		assert!(format!("{rows:?}").contains("other o"), "`{sql}`: {rows:?}");
		assert_eq!((read_faults() - faults, compiled), (faulted, lowered), "`{sql}`");
		assert_eq!(crate::exec::compiles::count(), compiles, "`{sql}` compiled while it ran");
		golden.record(sql, rows.to_sql());
	}
	golden.check();
}

/// A statement's answer, as a client receives it.
type Answer = crate::types::PublicValue;

/// What `sql` leaves in the tables `tables` lists, run on the VM over a
/// database set up with `setup` and changed by `changes` in order between the
/// statements of `steps`.
async fn wrote(setup: &str, steps: &[(&str, &str)], tables: &str) -> Vec<Answer> {
	let db = TestDb::new(setup).await;
	let owner = TestDb::owner();
	let mut out = Vec::new();
	for (change, sql) in steps {
		db.run(change).await;
		let lowered = catalog_units().0;
		let responses = db.datastore().execute(sql, &owner, None).await.expect("runs");
		for response in responses {
			out.push(response.result.unwrap_or_else(|e| Answer::String(e.to_string())));
		}
		out.push(Answer::Bool(catalog_units().0 > lowered));
	}
	let responses = db.datastore().execute(tables, &owner, None).await.expect("reads");
	for response in responses {
		out.push(response.result.expect("reads"));
	}
	out
}

/// A delete's edge cascade is kept by the node and lowered again once an edge
/// table changes or a table is defined: an event defined on an edge table
/// between two deletes fires for the second, and the edges of a table defined
/// between them are deleted with it.
#[tokio::test]
async fn a_kept_edge_cascade_stands_until_an_edge_table_changes() {
	let mut golden = Golden::new("a_kept_edge_cascade_stands_until_an_edge_table_changes");
	let setup = "
		DEFINE TABLE person SCHEMALESS; DEFINE TABLE likes TYPE RELATION; DEFINE TABLE log SCHEMALESS;
		FOR $i IN 1..=6 { CREATE type::record('person', $i) };
		RELATE person:1->likes->person:2; RELATE person:3->likes->person:4;
		RELATE person:5->likes->person:6;
	";
	let steps: &[(&str, &str)] = &[
		("RETURN NONE", "DELETE person:1"),
		("RETURN NONE", "DELETE person:6"),
		(
			"DEFINE EVENT gone ON likes WHEN $event = 'DELETE' THEN (CREATE log SET edge = [record::tb($before.id), $before.in, $before.out])",
			"DELETE person:3",
		),
		(
			"DEFINE TABLE follows TYPE RELATION;
			 DEFINE EVENT gone ON follows WHEN $event = 'DELETE' THEN (CREATE log SET edge = [record::tb($before.id), $before.in, $before.out]);
			 RELATE person:2->follows->person:4;",
			"DELETE person:2",
		),
	];
	let tables = "SELECT VALUE edge FROM log ORDER BY edge; SELECT in, out FROM likes ORDER BY in;
		SELECT in, out FROM follows; SELECT VALUE id FROM person ORDER BY id;";
	let vm = wrote(setup, steps, tables).await;
	let answers = |out: &[Answer]| -> Vec<Answer> {
		out.iter()
			.enumerate()
			.filter(|(at, _)| at % 2 == 0 || *at >= 8)
			.map(|(_, v)| v.clone())
			.collect()
	};
	golden.record("answers", render_answers(&answers(&vm)));
	// The first delete lowers the cascade, the second links it, and each
	// change has the next delete lower it anew.
	let lowered: Vec<&Answer> = vm.iter().take(8).skip(1).step_by(2).collect();
	assert_eq!(
		lowered,
		[&Answer::Bool(true), &Answer::Bool(false), &Answer::Bool(true), &Answer::Bool(true)],
		"{vm:#?}"
	);
	assert!(format!("{:?}", vm[8]).contains("likes"), "no event fired: {vm:#?}");
	assert!(format!("{:?}", vm[8]).contains("follows"), "no event fired: {vm:#?}");
	golden.check();
}

/// A delete's edge cascade loads the stages of the edge tables it writes by
/// name: they stand across a schema change to another database, and across a
/// table of its own database defined, which leave the edge table as it was.
#[tokio::test]
async fn a_kept_edge_cascade_stands_across_a_change_to_another_database() {
	let mut golden = Golden::new("a_kept_edge_cascade_stands_across_a_change_to_another_database");
	let setup = "
		DEFINE TABLE person SCHEMALESS; DEFINE TABLE likes TYPE RELATION;
		FOR $i IN 1..=6 { CREATE type::record('person', $i) };
		RELATE person:1->likes->person:2; RELATE person:3->likes->person:4;
		RELATE person:5->likes->person:6;
	";
	let steps: &[(&str, &str)] = &[
		("RETURN NONE", "DELETE person:1"),
		(
			"DEFINE DATABASE elsewhere; USE DB elsewhere; DEFINE TABLE far SCHEMALESS;",
			"DELETE person:3",
		),
		("DEFINE TABLE near SCHEMALESS", "DELETE person:5"),
	];
	let tables = "SELECT in, out FROM likes ORDER BY in; SELECT VALUE id FROM person ORDER BY id;";
	let vm = wrote(setup, steps, tables).await;
	let answers = |out: &[Answer]| -> Vec<Answer> {
		out.iter()
			.enumerate()
			.filter(|(at, _)| at % 2 == 0 || *at >= 6)
			.map(|(_, v)| v.clone())
			.collect()
	};
	golden.record("answers", render_answers(&answers(&vm)));
	let lowered: Vec<&Answer> = vm.iter().take(6).skip(1).step_by(2).collect();
	assert_eq!(
		lowered,
		[&Answer::Bool(true), &Answer::Bool(false), &Answer::Bool(false)],
		"{vm:#?}"
	);
	golden.check();
}

/// The reference fields a delete's repairs are lowered from are kept by the
/// node and found again once a field changes: a repair redefined between two
/// deletes is the one the second runs.
#[tokio::test]
async fn kept_reference_fields_stand_until_a_field_changes() {
	let mut golden = Golden::new("kept_reference_fields_stand_until_a_field_changes");
	let setup = "
		DEFINE TABLE author SCHEMALESS; DEFINE TABLE book SCHEMALESS;
		DEFINE FIELD writer ON book TYPE option<record<author>> REFERENCE ON DELETE UNSET;
		CREATE author:1; CREATE author:2; CREATE author:3;
		CREATE book:1 SET writer = author:1; CREATE book:2 SET writer = author:2;
		CREATE book:3 SET writer = author:3;
	";
	let steps: &[(&str, &str)] = &[
		("RETURN NONE", "DELETE author:1"),
		(
			"DEFINE FIELD OVERWRITE writer ON book TYPE option<record<author>> REFERENCE ON DELETE CASCADE",
			"DELETE author:2",
		),
		(
			"DEFINE TABLE shelf SCHEMALESS;
			 DEFINE FIELD holds ON shelf TYPE option<record<author>> REFERENCE ON DELETE REJECT;
			 CREATE shelf:1 SET holds = author:3;",
			"DELETE author:3",
		),
	];
	let tables =
		"SELECT id, writer FROM book ORDER BY id; SELECT VALUE id FROM author ORDER BY id;";
	let vm = wrote(setup, steps, tables).await;
	let answers = |out: &[Answer]| -> Vec<Answer> {
		out.iter()
			.enumerate()
			.filter(|(at, _)| at % 2 == 0 || *at >= 6)
			.map(|(_, v)| v.clone())
			.collect()
	};
	golden.record("answers", render_answers(&answers(&vm)));
	assert!(matches!(&vm[4], Answer::String(e) if e.contains("REJECT")), "{vm:#?}");
	let books = format!("{:?}", vm[6]);
	assert_eq!(books.matches("Table(\"book\")").count(), 2, "the cascade was not run: {vm:#?}");
	// A delete of an author writes no edge table, so its edge cascade
	// compiles no stages, before a change or after it; its repairs are its
	// plan's own.
	let lowered: Vec<&Answer> = vm.iter().take(6).skip(1).step_by(2).collect();
	assert_eq!(lowered[1..], [&Answer::Bool(false), &Answer::Bool(false)], "{vm:#?}");
	golden.check();
}

/// A plan that lowers the repairs of every table of its database — a delete
/// that may reach any table, with its edge cascade — reads the database's
/// `REFERENCE` fields from the catalog once, and a later plan not at all
/// until a field changes; each table's repairs are those of the fields that
/// can name its records, so each delete leaves the records its repairs give.
#[tokio::test]
async fn reference_fields_are_read_once_for_every_table_a_plan_repairs() {
	let mut golden = Golden::new("reference_fields_are_read_once_for_every_table_a_plan_repairs");
	let mut setup = String::from(
		"DEFINE TABLE author SCHEMALESS; DEFINE TABLE editor SCHEMALESS;
		 DEFINE TABLE book SCHEMALESS; DEFINE TABLE likes TYPE RELATION;
		 DEFINE FIELD writer ON book TYPE option<record<author>> REFERENCE ON DELETE CASCADE;
		 DEFINE FIELD editor ON book TYPE option<record<editor>> REFERENCE ON DELETE UNSET;
		 CREATE author:1; CREATE author:2; CREATE editor:1;
		 CREATE book:1 SET writer = author:1;
		 CREATE book:2 SET writer = author:2, editor = editor:1;
		 RELATE author:2->likes->editor:1;",
	);
	for i in 0..20 {
		setup.push_str(&format!("DEFINE TABLE extra_{i} SCHEMALESS;"));
	}
	let db = TestDb::new(&setup).await;
	let found = || super::units::REFERENCING_FOUND.with(std::cell::Cell::get);
	let delete = |table: &str, id: i64| {
		format!(
			"{{ DELETE type::record('{table}', {id});
			   RETURN [(SELECT id, writer, editor FROM book ORDER BY id),
			           (SELECT VALUE id FROM likes)] }}"
		)
	};
	for (change, sql, reads) in [
		("RETURN NONE", delete("editor", 1), 1),
		("RETURN NONE", delete("author", 1), 0),
		("RETURN NONE", delete("author", 2), 0),
		(
			"DEFINE FIELD fan ON extra_3 TYPE option<record<author>> REFERENCE ON DELETE REJECT;
			 CREATE extra_3:1 SET fan = author:2;",
			delete("author", 2),
			1,
		),
	] {
		db.run(change).await;
		let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
		let before = found();
		let vm = run(&ctx, &sql).await.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
		let read = found() - before;
		ctx.txn().cancel().await.expect("cancel");
		golden.record(
			format!("after `{change}`: {sql}"),
			super::golden::mask_generated_keys(&render_outcome(&vm)),
		);
		assert_eq!(read, reads, "`{sql}` after `{change}` read the fields {read} times");
	}
	golden.check();
}

/// The stages a write that may reach any table applies to the records of a
/// table are loaded by the table's name from the compile cache: a later run
/// of the same statement loads them while the table stands as it was, across
/// a change to another table or another database, and compiles them anew
/// once the table itself changes, the new definitions applied.
#[tokio::test]
async fn a_kept_stage_set_stands_until_a_table_of_its_database_changes() {
	let mut golden = Golden::new("a_kept_stage_set_stands_until_a_table_of_its_database_changes");
	let setup = "
		DEFINE TABLE a SCHEMALESS; DEFINE TABLE b SCHEMALESS; DEFINE TABLE log SCHEMALESS;
		CREATE a:1 SET n = 1; CREATE b:1 SET n = 1;
	";
	let write = "UPDATE type::table('a') SET n += 1";
	let steps: &[(&str, &str)] = &[
		("RETURN NONE", write),
		("RETURN NONE", write),
		("DEFINE EVENT bump ON b WHEN $event = 'UPDATE' THEN (CREATE log SET t = 'b')", write),
		(
			"DEFINE EVENT bump ON a WHEN $event = 'UPDATE' THEN (CREATE log SET t = 'a', n = $after.n)",
			write,
		),
		("DEFINE DATABASE elsewhere; USE DB elsewhere; DEFINE TABLE far SCHEMALESS;", write),
		("DEFINE TABLE c SCHEMALESS", write),
		("DEFINE FIELD OVERWRITE n ON a TYPE int ASSERT $value < 6", write),
		("RETURN NONE", write),
	];
	let tables = "SELECT t, n FROM log ORDER BY n; SELECT VALUE n FROM a;";
	let vm = wrote(setup, steps, tables).await;
	let answers = |out: &[Answer]| -> Vec<Answer> {
		out.iter()
			.enumerate()
			.filter(|(at, _)| at % 2 == 0 || *at >= 16)
			.map(|(_, v)| v.clone())
			.collect()
	};
	golden.record("answers", render_answers(&answers(&vm)));
	let lowered: Vec<&Answer> = vm.iter().take(16).skip(1).step_by(2).collect();
	let (yes, no) = (&Answer::Bool(true), &Answer::Bool(false));
	assert_eq!(lowered, [yes, no, no, yes, no, no, yes, no], "{vm:#?}");
	// The event defined on `a` fired for each later write the assertion
	// defined on its field let through, and the assertion refused the rest.
	assert_eq!(format!("{:?}", vm[16]).matches("\"a\"").count(), 3, "{vm:#?}");
	for refused in [&vm[12], &vm[14]] {
		assert!(matches!(refused, Answer::String(e) if e.contains("must conform")), "{vm:#?}");
	}
	golden.check();
}

/// The stages a write that may reach any table loads by name read a name the
/// write reads inside what their lowering binds as a statement of its own — a
/// graph lookup's condition — when they run, never folding the value one run
/// held: a later run with another value loads the same stages and answers
/// its own rows.
#[tokio::test]
async fn a_kept_stage_set_is_linked_only_where_the_values_it_binds_hold() {
	let mut golden = Golden::new("a_kept_stage_set_is_linked_only_where_the_values_it_binds_hold");
	let db = TestDb::new(
		"DEFINE TABLE a SCHEMALESS; DEFINE TABLE b SCHEMALESS; DEFINE TABLE likes TYPE RELATION;
		 CREATE a:1; CREATE b:1 SET w = 1; CREATE b:2 SET w = 2;
		 RELATE a:1->likes->b:1 SET w = 1; RELATE a:1->likes->b:2 SET w = 2;",
	)
	.await;
	let with = |w: i64| {
		let mut session = TestDb::owner();
		session.variables.insert("w", w);
		session
	};
	let sql = "UPDATE type::table('a') SET seen = ->likes[WHERE w = $w]->b RETURN AFTER";
	let write = async |w: i64| {
		let ctx = db.exec_ctx_as(&with(w), crate::kvs::TransactionType::Write).await;
		let rows = run(&ctx, sql).await.expect("covered");
		ctx.txn().cancel().await.expect("cancel");
		rows
	};
	let mut seen = Vec::new();
	for (at, (w, lowers)) in [(1, true), (2, false), (1, false), (2, false)].into_iter().enumerate()
	{
		let (before, _) = catalog_units();
		let vm = write(w).await;
		let lowered = catalog_units().0 > before;
		assert_eq!(lowered, lowers, "w = {w}");
		golden.rows(format!("w = {w}, run {at}"), &vm);
		seen.push(vm);
	}
	assert_ne!(seen[0], seen[1], "the lookup's condition did not decide the write");
	golden.check();
}

/// A listing kept for a unit and found to stand without being read again,
/// under the catalog epoch it was read under, is noted in the open recordings
/// as a read of the listing: what links the unit depends on the listing as
/// though it had read it.
#[tokio::test]
async fn a_listing_that_stands_unread_is_noted_as_read() {
	use crate::catalog::providers::{DatabaseProvider as _, NamespaceProvider as _};
	let db = TestDb::new("DEFINE TABLE a SCHEMALESS; DEFINE TABLE b SCHEMALESS;").await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	let txn = ctx.txn();
	let ns = txn.expect_ns_by_name("test").await.expect("namespace").namespace_id;
	let dbid = txn.expect_db_by_name("test", "test").await.expect("database").database_id;
	let recording = txn.record_catalog_reads();
	txn.all_tb_each(ns, dbid).await.expect("lists");
	let reads = recording.finish();
	let deps = crate::exec::catalog_deps::CatalogDeps::resolve(&txn, &reads).await.expect("deps");
	let later = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	let txn = later.txn();
	assert!(txn.catalog_epoch().is_some(), "a memory store names its catalog");
	let recording = txn.record_catalog_reads();
	assert!(deps.hold(&txn).await, "the listing changed");
	let reads = recording.finish();
	assert!(reads.listings.contains(&(ns, dbid)), "{reads:?}");
}

/// What `steps` answer, run on the VM over a database set up with `setup`,
/// each change made before its statements run, then what `tables` reads; and
/// the tables whose members of a stage set each statement's plan lowered,
/// sorted and each once.
async fn changed(
	setup: &str,
	steps: &[(&str, &[String])],
	tables: &str,
) -> (Vec<Answer>, Vec<Vec<String>>) {
	let db = TestDb::new(setup).await;
	let owner = TestDb::owner();
	let mut out = Vec::new();
	let mut members = Vec::new();
	for (change, statements) in steps {
		db.run(change).await;
		for sql in statements.iter() {
			super::lower::SET_MEMBERS_LOWERED.with(|lowered| lowered.borrow_mut().clear());
			let responses = db.datastore().execute(sql, &owner, None).await.expect("runs");
			for response in responses {
				out.push(response.result.unwrap_or_else(|e| Answer::String(e.to_string())));
			}
			let mut lowered: Vec<String> = super::lower::SET_MEMBERS_LOWERED
				.with(|lowered| lowered.borrow().iter().map(|t| t.as_str().to_owned()).collect());
			lowered.sort();
			lowered.dedup();
			members.push(lowered);
		}
	}
	let responses = db.datastore().execute(tables, &owner, None).await.expect("reads");
	for response in responses {
		out.push(response.result.expect("reads"));
	}
	(out, members)
}

/// The stages a write that may reach any table applies to a table — and
/// those of a delete's edge cascade — are compiled for the tables its run
/// writes records of, by name, and kept by table: a schema change between
/// two runs of the same statement has the next compile again the stages of
/// the tables it changed, and of those whose definitions read them, and no
/// other. Each kind of change — a table defined, altered or removed, a field,
/// an event, a view, a `REFERENCE` field and an index — is made to a table
/// the statements write and to one they do not, and each statement after it
/// answers its rows.
#[tokio::test]
async fn a_schema_change_lowers_again_only_the_members_of_the_tables_it_changed() {
	let mut golden =
		Golden::new("a_schema_change_lowers_again_only_the_members_of_the_tables_it_changed");
	let mut setup = String::from(
		"DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
		 DEFINE TABLE post SCHEMALESS PERMISSIONS FULL;
		 DEFINE TABLE likes TYPE RELATION PERMISSIONS FULL;
		 DEFINE TABLE other SCHEMALESS PERMISSIONS FULL;
		 DEFINE TABLE spare SCHEMALESS;
		 DEFINE TABLE log SCHEMALESS;
		 DEFINE FIELD author ON post TYPE option<record<person>> REFERENCE ON DELETE UNSET;
		 FOR $i IN 1..=20 {
			 CREATE type::record('person', $i) SET n = $i;
			 CREATE type::record('post', $i) SET author = type::record('person', $i), n = $i;
			 CREATE type::record('other', $i) SET n = $i;
		 };
		 FOR $i IN 1..20 {
			 RELATE (type::record('person', $i))->likes->(type::record('person', $i + 1));
		 };",
	);
	for i in 0..10 {
		setup.push_str(&format!("DEFINE TABLE extra_{i} SCHEMALESS;"));
	}
	let only =
		|tables: &[&str]| -> Vec<String> { tables.iter().map(|t| (*t).to_owned()).collect() };
	// The change, and the tables whose stages each of the two statements
	// compiles after it: the update writes records of `post` alone, and the
	// delete's edge cascade records of `likes` alone, each compiled once and
	// again only after a change to its own table or to what its stages read.
	let changes: Vec<(&str, Vec<String>, Vec<String>)> = vec![
		("RETURN NONE", only(&["post"]), only(&["likes"])),
		("RETURN NONE", only(&[]), only(&[])),
		(
			"DEFINE TABLE OVERWRITE other SCHEMALESS PERMISSIONS FULL COMMENT 'c'",
			only(&[]),
			only(&[]),
		),
		("ALTER TABLE other COMMENT 'd'", only(&[]), only(&[])),
		("REMOVE TABLE spare", only(&[]), only(&[])),
		("DEFINE FIELD x ON other TYPE option<int>", only(&[]), only(&[])),
		(
			"DEFINE FIELD OVERWRITE n ON post TYPE int ASSERT $value < 30",
			only(&["post"]),
			only(&[]),
		),
		(
			"DEFINE EVENT ev ON other THEN (CREATE log SET t = 'other')",
			only(&[]),
			only(&[]),
		),
		(
			"DEFINE EVENT ev ON post WHEN $event = 'UPDATE' THEN (CREATE log SET t = 'post', n = $after.n)",
			only(&["post"]),
			only(&[]),
		),
		(
			"DEFINE TABLE other_view AS SELECT count() AS c FROM other GROUP ALL",
			only(&[]),
			only(&[]),
		),
		(
			"DEFINE TABLE post_view AS SELECT count() AS c, math::sum(n) AS s FROM post GROUP ALL",
			only(&["post"]),
			only(&[]),
		),
		("DEFINE INDEX ix ON other FIELDS n", only(&[]), only(&[])),
		("DEFINE INDEX ix ON post FIELDS n", only(&["post"]), only(&[])),
		// A reference that can name no record of a table the statements
		// write reaches the repairs of a table the catalog does not hold.
		(
			"DEFINE FIELD far ON other TYPE option<record<nowhere>> REFERENCE ON DELETE IGNORE",
			only(&[]),
			only(&[]),
		),
		// One that can name a person reaches the repairs of a person's delete,
		// which the second statement's own stages make, and rejects it.
		(
			"DEFINE FIELD fan ON other TYPE option<record<person>> REFERENCE ON DELETE REJECT;
			 UPDATE other:1 SET fan = person:15;",
			only(&[]),
			only(&[]),
		),
		("ALTER TABLE post COMMENT 'p'", only(&["post"]), only(&[])),
		("REMOVE EVENT ev ON post", only(&["post"]), only(&[])),
		// `other` now holds a reference to a person, whose repair runs its
		// stages.
		("REMOVE FIELD x ON other", only(&[]), only(&[])),
		(
			"DEFINE FIELD OVERWRITE n ON post TYPE int",
			only(&["post"]),
			only(&[]),
		),
		// A function an event calls is read by the members whose stages run
		// the event, and by no other.
		(
			"DEFINE FUNCTION fn::tag() { RETURN 'v1' };
			 DEFINE EVENT tagged ON post WHEN $event = 'UPDATE' THEN (CREATE log SET t = fn::tag(), n = $after.n);",
			only(&["post"]),
			only(&[]),
		),
		(
			"DEFINE FUNCTION OVERWRITE fn::tag() { RETURN 'v2' }",
			only(&["post"]),
			only(&[]),
		),
		// The edges of a person are gone with the table that held them.
		("REMOVE TABLE likes", only(&[]), only(&[])),
		("RETURN NONE", only(&[]), only(&[])),
	];
	// Each step also reads the edges the deletes left.
	let statements: Vec<[String; 3]> = (0..changes.len())
		.map(|at| {
			[
				"UPDATE type::table('post') SET n += 1 RETURN VALUE n".to_owned(),
				format!("DELETE person:{} RETURN BEFORE", at + 1),
				"SELECT in, out FROM likes ORDER BY in".to_owned(),
			]
		})
		.collect();
	let steps: Vec<(&str, &[String])> = changes
		.iter()
		.zip(&statements)
		.map(|((change, _, _), statements)| (*change, &statements[..]))
		.collect();
	let tables = "SELECT t, n FROM log ORDER BY n, t; SELECT id, author FROM post ORDER BY id;
		SELECT c, s FROM post_view; SELECT c FROM other_view; SELECT VALUE id FROM person ORDER BY id;";
	let (vm, members) = changed(&setup, &steps, tables).await;
	golden.record("answers", render_answers(&vm));
	let shown = format!("{vm:?}");
	assert!(shown.contains("must conform"), "the assertion on post did not refuse: {shown}");
	assert!(shown.contains("REJECT"), "the rejecting reference did not refuse: {shown}");
	assert!(shown.contains("\"post\""), "the event on post did not fire: {shown}");
	assert!(shown.contains("\"v2\""), "the redefined function was not called: {shown}");
	for (at, (change, update, delete)) in changes.iter().enumerate() {
		for (expected, lowered) in [(update, &members[3 * at]), (delete, &members[3 * at + 1])] {
			assert_eq!(lowered, expected, "after `{change}`");
		}
	}
	golden.check();
}

/// A table's read unit is compiled once per class of principal: every record
/// user links the one the first lowered, each reading only the rows its own
/// `$auth` admits, and an owner, of another class, lowers its own. A read
/// nested in a loop that binds names around it links the same unit.
#[tokio::test]
async fn a_read_unit_is_shared_by_every_principal_of_a_class() {
	let db = TestDb::new_with_auth(
		"DEFINE TABLE owned SCHEMALESS PERMISSIONS FOR select WHERE owner = $auth.id;
		 DEFINE FIELD shown ON owned COMPUTED 'by ' + <string> owner;
		 CREATE owned:1 SET owner = user:alice;
		 CREATE owned:2 SET owner = user:bob;
		 CREATE user:alice, user:bob, user:carol;",
	)
	.await;
	let sql = "SELECT VALUE shown FROM type::table(rand::enum('owned'))";
	let (alice, lowered) = answered(&db, &record_user("alice"), sql).await;
	assert!(lowered > 0, "the first record user lowered no unit");
	assert_eq!(alice, Value::from(vec![Value::from("by user:alice")]));
	let (bob, lowered) = answered(&db, &record_user("bob"), sql).await;
	assert_eq!(lowered, 0, "a record user of the same class lowered a unit of its own");
	assert_eq!(bob, Value::from(vec![Value::from("by user:bob")]));
	let looped = "FOR $t IN ['owned'] { RETURN SELECT VALUE shown FROM type::table($t) }";
	let (_, lowered) = answered(&db, &record_user("carol"), looped).await;
	assert_eq!(lowered, 0, "a read nested in a loop lowered a unit of its own");
	let (owner, lowered) = answered(&db, &TestDb::owner(), sql).await;
	assert!(lowered > 0, "the owner linked a record user's unit");
	let Value::Array(rows) = owner else {
		panic!("{owner:?}");
	};
	assert_eq!(rows.len(), 2);
}

/// A table memo serves the descriptor it loaded again while the transaction
/// is the same and stamps no catalog version, loads it afresh once a schema
/// statement of the transaction stamps one, and never holds one read through
/// a speculation.
#[tokio::test]
async fn a_table_memo_serves_a_descriptor_until_a_version_is_stamped() {
	use crate::exec::vm::descriptor::TableMemo;
	let db = TestDb::new("DEFINE TABLE m SCHEMALESS;").await;
	let table = crate::val::TableName::from("m".to_owned());
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	let mut memo = TableMemo::default();

	let first = memo.current(&ctx, &table).await.expect("the table loads");
	let second = memo.current(&ctx, &table).await.expect("the table loads");
	assert!(std::sync::Arc::ptr_eq(&first, &second), "the second load read the catalog again");
	assert!(!first.defined().expect("m is defined").schemafull);
	assert_eq!(memo.len(), 1);

	run(&ctx, "DEFINE TABLE OVERWRITE m SCHEMAFULL")
		.await
		.expect("the VM runs it")
		.expect("the table is redefined");
	let third = memo.current(&ctx, &table).await.expect("the table loads");
	assert!(!std::sync::Arc::ptr_eq(&first, &third), "a stamped version left the memo serving");
	assert!(third.defined().expect("m is defined").schemafull, "the new definition was not read");
	let fourth = memo.current(&ctx, &table).await.expect("the table loads");
	assert!(std::sync::Arc::ptr_eq(&third, &fourth));

	let other = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	let elsewhere = memo.current(&other, &table).await.expect("the table loads");
	assert!(!std::sync::Arc::ptr_eq(&fourth, &elsewhere), "another transaction was served");
	other.txn().cancel().await.expect("cancel");

	let mut speculative = TableMemo::default();
	let speculation = ctx
		.txn()
		.speculate(crate::kvs::speculation::Overlay::default())
		.expect("the transaction speculates");
	let mut view = crate::ctx::Context::new_child(ctx.ctx());
	view.set_transaction(std::sync::Arc::clone(speculation.txn()));
	let view = ctx.clone().with_new_ctx(view.freeze());
	let a = speculative.current(&view, &table).await.expect("the table loads");
	let b = speculative.current(&view, &table).await.expect("the table loads");
	assert!(!std::sync::Arc::ptr_eq(&a, &b), "a speculation was served from the memo");
	assert_eq!(speculative.len(), 0, "a speculation left an entry");
	drop(speculation);
	ctx.txn().cancel().await.expect("cancel");
}
