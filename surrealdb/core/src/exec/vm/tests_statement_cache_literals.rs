//! Statements kept in the cache of compiled top-level statements for the literals they free
//! ([`super::serve::free_literals`]). Each test runs requests through a datastore of its own,
//! and compares every answer with one of a datastore that keeps no statement.

use surrealdb_cnf::ConfigMap;
use surrealdb_types::{RecordId, ToSql, Variables};

use crate::dbs::Session;
use crate::exec::operators::test_util::TestDb;
use crate::kvs::{Datastore, StatementCacheStats};

const TABLES: &str = "
	DEFINE ACCESS user ON DATABASE TYPE RECORD;
	DEFINE TABLE pk SCHEMAFULL PERMISSIONS FULL;
	DEFINE FIELD name ON pk TYPE string;
	DEFINE FIELD age ON pk TYPE int;
	DEFINE FIELD secret ON pk TYPE string PERMISSIONS FOR select NONE;
	DEFINE FIELD owner ON pk TYPE record<user>;
	DEFINE INDEX pk_age ON pk FIELDS age;
	DEFINE TABLE ls SCHEMALESS PERMISSIONS FOR select WHERE owner = $auth;
	CREATE pk:1 SET name = 'p1', age = 30, secret = 's1', owner = user:alice;
	CREATE pk:2 SET name = 'p2', age = 31, secret = 's2', owner = user:bob;
	CREATE pk:3 SET name = 'p3', age = 30, secret = 's3', owner = user:bob;
	CREATE pk:x SET name = 'px', age = 9, secret = 'sx', owner = user:alice;
	CREATE ls:1 SET name = 'l1', owner = user:alice;
	CREATE ls:2 SET name = 'l2', owner = user:bob;
	CREATE ls:[1, 'a'] SET name = 'la', owner = user:alice;
	CREATE ls:{ k: 1 } SET name = 'lo', owner = user:alice;
	CREATE ls:u'0192e6a4-8b3c-7c11-9a2b-3c4d5e6f7a8b' SET name = 'lu', owner = user:alice;
";

/// A datastore that keeps compiled statements and one that keeps none, over the same data.
struct Pair {
	cached: TestDb,
	plain: TestDb,
}

impl Pair {
	async fn new(setup: &str) -> Self {
		Self {
			cached: TestDb::new_with_auth(setup).await,
			plain: TestDb::new_with_auth_and_config(
				setup,
				ConfigMap::empty().with_key_value("statement_cache_size", "0"),
			)
			.await,
		}
	}

	fn stats(&self) -> StatementCacheStats {
		self.cached.datastore().statement_cache_stats()
	}

	/// Runs `sql` on both, as `session`, asserting they answer alike, and answers it.
	async fn same(&self, session: &Session, sql: &str) -> Vec<String> {
		self.same_with(session, sql, None).await
	}

	/// [`Self::same`] with `vars` bound.
	async fn same_with(
		&self,
		session: &Session,
		sql: &str,
		vars: Option<Variables>,
	) -> Vec<String> {
		let got = answers(self.cached.datastore(), session, sql, vars.clone()).await;
		let want = answers(self.plain.datastore(), session, sql, vars).await;
		assert_eq!(got, want, "{sql}");
		got
	}

	async fn run(&self, sql: &str) {
		self.cached.run(sql).await;
		self.plain.run(sql).await;
	}
}

/// The answer of each statement of `sql`, as SurrealQL, or its error.
async fn answers(
	ds: &Datastore,
	session: &Session,
	sql: &str,
	vars: Option<Variables>,
) -> Vec<String> {
	ds.execute(sql, session, vars)
		.await
		.expect("the request runs")
		.into_iter()
		.map(|response| match response.result {
			Ok(value) => value.to_sql(),
			Err(e) => format!("error: {}", e.message()),
		})
		.collect()
}

fn since(before: StatementCacheStats, after: StatementCacheStats) -> StatementCacheStats {
	StatementCacheStats {
		hits: after.hits - before.hits,
		misses: after.misses - before.misses,
		invalidations: after.invalidations - before.invalidations,
	}
}

fn stats(hits: u64, misses: u64, invalidations: u64) -> StatementCacheStats {
	StatementCacheStats {
		hits,
		misses,
		invalidations,
	}
}

fn record_user(user: &str) -> Session {
	let rid = surrealdb_types::Value::RecordId(RecordId::new("user", user));
	Session::for_record("test", "test", "user", rid)
}

/// A statement whose one source is a record id written in place is kept for the table it names:
/// another record of that table is served the kept compilation and answered with its own row, and
/// a record of another table is compiled afresh.
#[tokio::test]
async fn a_literal_record_source_is_kept_for_its_table() {
	let db = Pair::new(TABLES).await;
	let owner = TestDb::owner();
	for shape in [
		"SELECT * FROM {}",
		"SELECT name FROM ONLY {}",
		"SELECT VALUE name FROM {} LIMIT 1",
		"SELECT name, secret FROM {} FETCH owner",
	] {
		let before = db.stats();
		let mut seen = Vec::new();
		for (id, served) in [
			("pk:1", stats(0, 1, 0)),
			("pk:2", stats(1, 0, 0)),
			("ls:1", stats(0, 1, 0)),
			("pk:x", stats(1, 0, 0)),
			("pk:9", stats(1, 0, 0)),
			("ls:2", stats(1, 0, 0)),
			("nosuch:1", stats(0, 1, 0)),
		] {
			let at = db.stats();
			seen.push(db.same(&owner, &shape.replace("{}", id)).await);
			assert_eq!(since(at, db.stats()), served, "{shape} with {id}");
		}
		assert_ne!(seen[0], seen[1], "each record answers its own row");
		assert_eq!(since(before, db.stats()), stats(4, 3, 0), "{shape}");
	}
}

/// The key of `type::record` written in place is freed as a variable in its place is. An array of
/// record ids written as the source is a list of values, compiled with its request.
#[tokio::test]
async fn literal_record_keys_are_kept_for_their_tables() {
	let db = Pair::new(TABLES).await;
	let owner = TestDb::owner();
	let before = db.stats();
	for sql in [
		"SELECT VALUE name FROM [pk:1, ls:1]",
		"SELECT VALUE name FROM [ls:2, pk:3, pk:9]",
		"SELECT VALUE name FROM [pk:2]",
		"SELECT name FROM type::record('pk', 1)",
		"SELECT name FROM type::record('pk', 3)",
		"SELECT name FROM type::record('pk', 'x')",
		"SELECT name FROM type::record('pk', 1) WHERE age = 30",
		"SELECT name FROM type::record('pk', 3) WHERE age = 30",
	] {
		db.same(&owner, sql).await;
	}
	assert_eq!(since(before, db.stats()), stats(2, 1, 0));
}

/// Two statements of one request that differ only in their record are each answered with their
/// own: a placeholder is bound for its statement alone.
#[tokio::test]
async fn statements_of_one_request_each_read_their_own_literal() {
	let db = Pair::new(TABLES).await;
	let owner = TestDb::owner();
	let got = db
		.same(&owner, "SELECT VALUE name FROM pk:1; SELECT VALUE name FROM pk:2; LET $a = 1; SELECT VALUE name FROM pk:3")
		.await;
	assert_eq!(got, vec!["['p1']", "['p2']", "NONE", "['p3']"]);
}

/// A literal that is not the record a statement reads only as its source stays in the statement
/// as written: a range, a filter's operand (which may choose an index), an ordering, a `LIMIT`,
/// a literal inside a closure or a subquery. Each is compiled for its own value or with its
/// request, and answers as main does.
#[tokio::test]
async fn other_literals_are_kept_exactly_or_compiled_with_their_request() {
	let db = Pair::new(TABLES).await;
	let owner = TestDb::owner();
	let before = db.stats();
	for sql in [
		"SELECT VALUE name FROM pk:1..2",
		"SELECT VALUE name FROM pk:1..=3",
		"SELECT VALUE name FROM pk WHERE age = 30 ORDER BY name",
		"SELECT VALUE name FROM pk WHERE age = 31 ORDER BY name",
		"SELECT VALUE name FROM pk WHERE name = 'p2'",
		"SELECT * FROM pk:1 WHERE age = 30",
		"SELECT * FROM pk:2 WHERE age = 30",
		"SELECT name, (SELECT VALUE name FROM pk:2) AS other FROM pk:1",
	] {
		for _ in 0..2 {
			db.same(&owner, sql).await;
		}
	}
	assert_eq!(since(before, db.stats()), stats(0, 0, 0));
	// The source is freed; the other literals stay as written and key the compilation.
	let before = db.stats();
	for (sql, served) in [
		("SELECT VALUE name FROM pk:1 LIMIT 1", stats(0, 1, 0)),
		("SELECT VALUE name FROM pk:2 LIMIT 1", stats(1, 0, 0)),
		("SELECT VALUE name FROM pk:2 LIMIT 0", stats(0, 1, 0)),
		("SELECT VALUE name FROM pk:1 START 1", stats(0, 1, 0)),
		("SELECT name, (|$x| $x + 1)(2) AS y FROM pk:1", stats(0, 1, 0)),
		("SELECT name, (|$x| $x + 1)(2) AS y FROM pk:2", stats(1, 0, 0)),
		("SELECT name, (|$x| $x + 1)(5) AS y FROM pk:2", stats(0, 1, 0)),
		("SELECT name, 'a' AS tag FROM pk:3", stats(0, 1, 0)),
		("SELECT name, 'b' AS tag FROM pk:3", stats(0, 1, 0)),
	] {
		let at = db.stats();
		db.same(&owner, sql).await;
		assert_eq!(since(at, db.stats()), served, "{sql}");
	}
	assert_eq!(since(before, db.stats()), stats(2, 7, 0));
}

/// A record user is shown what the table each record names allows, whether the compilation is
/// kept or not: a field a permission hides, the rows a table permission refuses.
#[tokio::test]
async fn a_kept_literal_source_enforces_the_permissions_of_its_table() {
	let db = Pair::new(TABLES).await;
	let alice = record_user("alice");
	for shape in ["SELECT * FROM {}", "SELECT * FROM ONLY {}", "SELECT VALUE secret FROM {}"] {
		for _ in 0..2 {
			for id in ["pk:1", "ls:1", "ls:2", "pk:2", "pk:x"] {
				db.same(&alice, &shape.replace("{}", id)).await;
			}
		}
	}
}

/// A catalog change between two requests is read by the statement kept for a literal source:
/// a field defined afterwards is read, a table permission changed is enforced.
#[tokio::test]
async fn a_catalog_change_is_read_by_a_kept_literal_source() {
	let db = Pair::new(TABLES).await;
	let (owner, alice) = (TestDb::owner(), record_user("alice"));
	let read = |n: i64| format!("SELECT VALUE twice FROM ONLY ls:{n}");
	assert_eq!(db.same(&owner, &read(1)).await, vec!["NONE"]);
	assert_eq!(db.same(&owner, &read(2)).await, vec!["NONE"]);
	db.run("DEFINE FIELD twice ON ls COMPUTED string::concat(name, name)").await;
	assert_eq!(db.same(&owner, &read(1)).await, vec!["'l1l1'"]);
	assert_eq!(db.same(&owner, &read(2)).await, vec!["'l2l2'"]);
	db.run("REMOVE FIELD twice ON ls").await;
	assert_eq!(db.same(&owner, &read(1)).await, vec!["NONE"]);

	let read = |n: i64| format!("SELECT VALUE name FROM ls:{n}");
	db.same(&alice, &read(1)).await;
	db.same(&alice, &read(2)).await;
	db.run("DEFINE TABLE OVERWRITE ls SCHEMALESS PERMISSIONS FULL").await;
	assert_eq!(db.same(&alice, &read(2)).await, vec!["['l2']"]);
	db.run("DEFINE TABLE OVERWRITE ls SCHEMALESS PERMISSIONS NONE").await;
	assert_eq!(db.same(&alice, &read(1)).await, vec!["[]"]);
	db.run("REMOVE TABLE ls").await;
	db.same(&alice, &read(1)).await;
}

/// What a request shows of a statement whose literal a kept compilation frees is the literal as
/// written: `EXPLAIN` and `EXPLAIN ANALYZE` (compiled with their request) print it, and an error
/// raised over the source reads as main's.
#[tokio::test]
async fn a_freed_literal_is_shown_as_written() {
	let db = Pair::new(TABLES).await;
	let owner = TestDb::owner();
	for n in [1, 2, 1] {
		db.same(&owner, &format!("SELECT * FROM pk:{n}")).await;
	}
	for sql in [
		"EXPLAIN SELECT * FROM pk:2",
		"SELECT * FROM pk:2 EXPLAIN",
		"SELECT * FROM ONLY [pk:1, pk:2]",
		"SELECT * FROM ONLY [pk:1, pk:2]",
		"SELECT * FROM ONLY nosuch:1",
		"SELECT * FROM nosuch:1",
	] {
		let got = db.same(&owner, sql).await;
		assert!(!got.iter().any(|answer| answer.contains("literal ")), "{sql}: {got:?}");
	}
	let explained = db.same(&owner, "EXPLAIN SELECT * FROM pk:2").await;
	assert!(explained[0].contains("pk:2"), "{explained:?}");
	// Timings differ from run to run, so the analysed plan is read, not compared.
	let analysed =
		answers(db.cached.datastore(), &owner, "EXPLAIN ANALYZE SELECT * FROM pk:2", None).await;
	assert!(analysed[0].contains("record_id: pk:2"), "{analysed:?}");
	assert!(!analysed[0].contains("literal "), "{analysed:?}");
}

/// An array of record ids with duplicates and records of several tables answers as main does,
/// and is kept for the tables it names.
#[tokio::test]
async fn a_literal_id_list_with_duplicates_and_mixed_tables_answers_as_written() {
	let db = Pair::new(TABLES).await;
	let owner = TestDb::owner();
	for sql in [
		"SELECT VALUE name FROM [pk:1, pk:1, ls:1, pk:2]",
		"SELECT VALUE name FROM [ls:2, pk:3, pk:3, ls:2]",
		"SELECT VALUE name FROM [pk:9, nosuch:1]",
		"SELECT VALUE name FROM [nosuch:2, pk:1]",
	] {
		for _ in 0..2 {
			db.same(&owner, sql).await;
		}
	}
}

/// A placeholder is seen by its statement alone: a client variable of the same name is read by
/// a statement that names it, and is not changed by a statement whose literal is freed.
#[tokio::test]
async fn a_placeholder_neither_shadows_nor_is_shadowed_by_a_request_variable() {
	let db = Pair::new(TABLES).await;
	let owner = TestDb::owner();
	let mut vars = Variables::new();
	vars.insert("literal 0", RecordId::new("ls", 1));
	vars.insert("\0literal 0", RecordId::new("ls", 2));
	for sql in [
		"SELECT VALUE name FROM pk:1",
		"SELECT VALUE name FROM pk:2; RETURN $`literal 0`",
		"SELECT VALUE name FROM $`literal 0`",
		"SELECT name, $`literal 0` AS v FROM pk:3",
		"SELECT name, $`\\0literal 0` AS v FROM pk:3",
		"LET $`literal 0` = pk:2; SELECT VALUE name FROM pk:1; SELECT VALUE name FROM $`literal 0`",
	] {
		for _ in 0..2 {
			db.same_with(&owner, sql, Some(vars.clone())).await;
		}
	}
}

/// Record ids whose keys are arrays, objects and uuids are freed for their table like any other,
/// each answered with its own row.
#[tokio::test]
async fn literal_records_of_every_key_kind_answer_their_own_rows() {
	let db = Pair::new(TABLES).await;
	let (owner, alice) = (TestDb::owner(), record_user("alice"));
	let before = db.stats();
	for id in [
		"ls:[1, 'a']",
		"ls:{ k: 1 }",
		"ls:u'0192e6a4-8b3c-7c11-9a2b-3c4d5e6f7a8b'",
		"ls:[2, 'a']",
		"ls:{ k: 2 }",
		"ls:1",
	] {
		db.same(&owner, &format!("SELECT VALUE name FROM {id}")).await;
		db.same(&alice, &format!("SELECT VALUE name FROM {id}")).await;
	}
	assert_eq!(since(before, db.stats()), stats(10, 2, 0));
}

/// A key `type::record` cannot make a record of raises as main raises it, kept or not, and a
/// record user is shown what the table allows.
#[tokio::test]
async fn a_freed_record_key_raises_and_is_permitted_as_written() {
	let db = Pair::new(TABLES).await;
	let (owner, alice) = (TestDb::owner(), record_user("alice"));
	for sql in [
		"SELECT name FROM type::record('pk', 1)",
		"SELECT name FROM type::record('pk', NONE)",
		"SELECT name FROM type::record('pk', NULL)",
		"SELECT name FROM type::record('pk', 1.5)",
		"SELECT name FROM type::record('pk', {})",
		"SELECT name FROM type::record('pk', 2)",
		"SELECT VALUE name FROM type::record('ls', 2)",
		"SELECT VALUE name FROM type::record('ls', 1)",
	] {
		for _ in 0..2 {
			db.same(&owner, sql).await;
			db.same(&alice, sql).await;
		}
	}
}

/// A statement inside a transaction, a statement whose rows a `LET` binds, and several sources
/// each answer as main does.
#[tokio::test]
async fn literal_sources_in_transactions_lets_and_lists_answer_as_written() {
	let db = Pair::new(TABLES).await;
	let owner = TestDb::owner();
	for sql in [
		"BEGIN; SELECT VALUE name FROM pk:1; SELECT VALUE name FROM pk:2; COMMIT",
		"BEGIN; SELECT VALUE name FROM pk:2; CANCEL",
		"LET $x = SELECT VALUE name FROM pk:1; RETURN $x",
		"LET $x = SELECT VALUE name FROM pk:3; SELECT VALUE name FROM pk:2; RETURN $x",
		"SELECT VALUE name FROM pk:1, pk:2",
		"SELECT VALUE name FROM pk:2, ls:1",
		"BEGIN; CREATE pk:7 SET name = 'p7', age = 1, owner = user:bob; SELECT VALUE name FROM pk:7; CANCEL",
		"SELECT VALUE name FROM pk:7",
	] {
		for _ in 0..2 {
			db.same(&owner, sql).await;
		}
	}
}
