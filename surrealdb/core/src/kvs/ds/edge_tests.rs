//! The edge entry against the full one: the same statements give the same values and errors, the
//! statements the entry does not serve are refused before any runs, and stored code that holds
//! one is refused where it stands.

use std::str::FromStr;
use std::sync::Arc;

use surrealdb_kvs::key::KVKey;

use crate::dbs::Session;
use crate::dbs::capabilities::{Capabilities, FuncTarget, Targets};
use crate::key::schema::{MigrationKey, StorageVersionKey};
use crate::kvs::version::StorageVersion;
use crate::kvs::{Builder, Datastore, DatastoreError, TransactionType, migration};

fn session() -> Session {
	Session::owner().with_ns("test").with_db("test")
}

/// The full datastore and the edge one.
async fn pair() -> (Arc<Datastore>, Arc<Datastore>) {
	let a = Datastore::new("memory").await.expect("the full datastore opens");
	let b = Builder::for_edge().build_with_path("memory").await.expect("the edge datastore opens");
	for ds in [&a, &b] {
		full(ds, &Session::owner(), "DEFINE NAMESPACE test; USE NS test; DEFINE DATABASE test;")
			.await;
	}
	(a, b)
}

/// The statement outcomes of a call, with the time left out.
type Outcomes = Vec<Result<String, String>>;

fn outcomes(results: Vec<crate::dbs::QueryResult>) -> Outcomes {
	results
		.into_iter()
		.map(|result| match result.result {
			Ok(value) => Ok(format!("{value:?}")),
			Err(error) => Err(error.to_string()),
		})
		.collect()
}

async fn full(ds: &Datastore, sess: &Session, sql: &str) -> Outcomes {
	outcomes(ds.execute(sql, sess, None).await.expect("the call runs"))
}

async fn edge(ds: &Datastore, sess: &Session, sql: &str) -> Outcomes {
	outcomes(ds.execute_edge(sql, sess, None).await.expect("the call runs"))
}

/// Sets the same schema up on both datastores through `execute`.
async fn schema(a: &Datastore, b: &Datastore, sql: &str) {
	for ds in [a, b] {
		for outcome in full(ds, &session(), sql).await {
			outcome.expect("the schema statement runs");
		}
	}
}

/// Runs `sql` on both and asserts that the outcomes are the same.
async fn parity(a: &Datastore, b: &Datastore, sess: &Session, sql: &str) {
	let on_full = full(a, sess, sql).await;
	let on_edge = edge(b, sess, sql).await;
	assert_eq!(on_full, on_edge, "{sql}");
}

#[tokio::test]
async fn data_statements_give_the_values_execute_gives() {
	let (a, b) = pair().await;
	schema(
		&a,
		&b,
		"DEFINE TABLE person SCHEMALESS;
		 DEFINE TABLE knows TYPE RELATION;
		 DEFINE TABLE indexed;
		 DEFINE INDEX by_n ON indexed FIELDS n;
		 DEFINE TABLE logged;
		 DEFINE TABLE evented;
		 DEFINE EVENT noted ON evented WHEN true THEN (CREATE logged SET src = $after.id);
		 DEFINE TABLE feed CHANGEFEED 1h;
		 DEFINE TABLE totals AS SELECT count() AS c FROM person GROUP ALL;
		 DEFINE FUNCTION fn::double($x: int) { RETURN $x * 2; };",
	)
	.await;
	let s = session();
	for sql in [
		"CREATE person:1 SET name = 'a', age = 10; CREATE person:2 CONTENT { name: 'b', age: 20 };",
		"CREATE person:3 SET name = 'c', age = 30; CREATE person:4 SET name = 'd', age = 30;",
		"INSERT INTO person { id: 5, name: 'e', age: 40 };",
		"INSERT INTO person [{ id: 6, name: 'f', age: 50 }, { id: 7, name: 'g', age: 60 }];",
		"INSERT INTO person { id: 5, name: 'e2', age: 41 } ON DUPLICATE KEY UPDATE name = 'dup';",
		"UPDATE person:1 SET age += 1; UPSERT person:8 SET name = 'h';",
		"DELETE person:8; RELATE person:1->knows:1->person:2 SET since = 1;",
		"SELECT * FROM person WHERE age > 20 ORDER BY age DESC LIMIT 3;",
		"SELECT age, count() AS n, math::sum(age) AS total FROM person GROUP BY age ORDER BY age;",
		"SELECT ->knows->person.name AS friends FROM person:1;",
		"SELECT *, ->knows->person AS out FROM person:1 FETCH out;",
		"LET $x = 1; RETURN $x + 1;",
		"RETURN IF 1 = 1 { 'yes' } ELSE { 'no' };",
		"FOR $i IN [1, 2, 3] { CREATE person SET name = 'loop', age = $i, id = rand::uuid::v4(); }; SELECT count() FROM person GROUP ALL;",
		"RETURN (SELECT VALUE name FROM person WHERE age = 10);",
		"RETURN type::is_datetime(time::now());",
		"RETURN [string::uppercase('a'), array::len([1, 2]), crypto::sha256('x')];",
		"RETURN fn::double(21);",
		"CREATE indexed:1 SET n = 1; CREATE indexed:2 SET n = 2;",
		"SELECT VALUE id FROM indexed WHERE n = 2;",
		"CREATE evented:1; SELECT VALUE src FROM logged;",
		"CREATE feed:1 SET v = 1; UPDATE feed:1 SET v = 2;",
		"SELECT * FROM totals;",
	] {
		parity(&a, &b, &s, sql).await;
	}
	// The change feed the edge writes is the one `execute` writes.
	let feed = "SHOW CHANGES FOR TABLE feed SINCE 0";
	let on_full = full(&a, &s, feed).await;
	let on_edge = full(&b, &s, feed).await;
	assert_eq!(on_full.len(), on_edge.len());
	assert!(on_edge[0].as_ref().is_ok_and(|feed| feed.contains("update")), "{on_edge:?}");
}

#[tokio::test]
async fn a_statement_that_is_not_served_fails_the_call_and_runs_nothing() {
	let (a, b) = pair().await;
	let s = session();
	for statement in [
		"DEFINE TABLE t",
		"REMOVE TABLE t",
		"ALTER TABLE t COMMENT 'x'",
		"REBUILD INDEX i ON t",
		"INFO FOR DB",
		"LIVE SELECT * FROM t",
		"KILL u'00000000-0000-0000-0000-000000000000'",
		"USE NS a DB b",
		"OPTION FORCE = true",
		"ACCESS a ON DATABASE GRANT FOR USER u",
		"SHOW CHANGES FOR TABLE t SINCE 0",
		"BEGIN",
		"COMMIT",
		"CANCEL",
		"SLEEP 1ms",
		"EXPLAIN SELECT * FROM t",
	] {
		let sql = format!("CREATE probe:1; {statement};");
		let refused = b.execute_edge(&sql, &s, None).await;
		let error = refused.expect_err(&format!("{statement} is served"));
		assert!(error.to_string().contains("does not serve"), "{statement}: {error}");
		let on_full = full(&a, &s, "SELECT * FROM probe").await;
		let seen = edge(&b, &s, "SELECT * FROM probe").await;
		assert_eq!(seen, on_full, "{statement} ran its neighbour");
	}
}

#[tokio::test]
async fn a_nested_statement_that_is_not_served_is_refused() {
	let (a, b) = pair().await;
	let s = session();
	for sql in [
		"CREATE probe:1; IF true { DEFINE TABLE x };",
		"CREATE probe:1; FOR $i IN [1] { REMOVE TABLE t };",
		"CREATE probe:1; { DEFINE TABLE x };",
		"CREATE probe:1; SELECT * FROM (INFO FOR DB);",
	] {
		let refused = b.execute_edge(sql, &s, None).await;
		let error = refused.expect_err(sql);
		assert!(error.to_string().contains("does not serve"), "{sql}: {error}");
	}
	let rows = edge(&b, &s, "SELECT * FROM probe").await;
	assert_eq!(rows, full(&a, &s, "SELECT * FROM probe").await);
}

const NOT_SERVED: &str = "is not served by this entry";

#[tokio::test]
async fn stored_code_that_holds_an_unserved_statement_is_refused_at_run_time() {
	let (_, b) = pair().await;
	let s = session();
	full(&b, &s, "DEFINE FUNCTION fn::mk() { DEFINE TABLE made; RETURN 1 };").await;
	for sql in [
		"RETURN fn::mk()",
		"LET $f = || { DEFINE TABLE y }; $f()",
		"LET $f = |$p| { DEFINE TABLE y }; $f(1)",
	] {
		let outcome = edge(&b, &s, sql).await;
		let refused =
			outcome.iter().any(|outcome| outcome.as_ref().is_err_and(|e| e.contains(NOT_SERVED)));
		assert!(refused, "{sql}: {outcome:?}");
	}
	let info = full(&b, &s, "INFO FOR DB").await;
	let info = info[0].as_ref().expect("the info reads");
	assert!(info.contains("\"tables\": Object(Object({}))"), "no table was made: {info}");
}

#[tokio::test]
async fn stored_code_that_reaches_an_explain_is_refused_clearly() {
	let (_, b) = pair().await;
	let s = session();
	full(&b, &s, "CREATE t:1 SET v = 1;").await;
	full(&b, &s, "DEFINE FUNCTION fn::plain() { RETURN (SELECT * FROM t EXPLAIN) };").await;
	full(&b, &s, "DEFINE FUNCTION fn::analysed() { RETURN (SELECT * FROM t EXPLAIN FULL) };").await;
	for sql in [
		"RETURN fn::plain()",
		"RETURN fn::analysed()",
		"LET $f = || { SELECT * FROM t EXPLAIN }; $f()",
	] {
		let outcome = edge(&b, &s, sql).await;
		let refused = outcome.iter().any(|outcome| {
			outcome.as_ref().is_err_and(|error| {
				error.contains("an EXPLAIN statement is not served by this entry")
			})
		});
		assert!(refused, "{sql}: {outcome:?}");
	}
	// The same code runs on the full entry, so the refusal is the entry's and not the code's.
	let on_full = full(&b, &s, "RETURN fn::plain()").await;
	assert!(on_full[0].is_ok(), "{on_full:?}");
}

/// A write on the edge entry meets the catalog kinds a full-text index and a live query keep, and
/// the edge entry's codec serves them.
#[tokio::test]
async fn writes_on_tables_with_a_full_text_index_and_a_live_query_run_on_the_edge() {
	let (_, b) = pair().await;
	let s = session();
	full(
		&b,
		&s,
		"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
		 DEFINE TABLE docs;
		 DEFINE INDEX docs_text ON docs FIELDS body FULLTEXT ANALYZER simple BM25;
		 DEFINE TABLE watched;
		 DEFINE FIELD n ON watched TYPE int;",
	)
	.await;
	let live = full(&b, &s.clone().with_rt(true), "LIVE SELECT * FROM watched").await;
	assert!(live[0].is_ok(), "{live:?}");
	for sql in [
		"CREATE docs:1 SET body = 'Hello edge World'",
		"UPDATE docs:1 SET body = 'Goodbye edge'",
		"CREATE watched:1 SET n = 1",
		"UPDATE watched:1 SET n = 2",
		"DELETE watched:1",
		"DELETE docs:1",
	] {
		let outcome = edge(&b, &s, sql).await;
		assert!(outcome.iter().all(Result::is_ok), "{sql}: {outcome:?}");
	}
	let rows = edge(&b, &s, "SELECT * FROM docs WHERE body @@ 'edge'").await;
	assert!(rows.iter().all(Result::is_ok), "{rows:?}");
}

#[tokio::test]
async fn a_function_cached_on_the_full_path_is_still_refused_on_the_edge() {
	let (_, b) = pair().await;
	let s = session();
	full(&b, &s, "DEFINE FUNCTION fn::mk() { DEFINE TABLE made; RETURN 1 };").await;
	let on_full = full(&b, &s, "RETURN fn::mk()").await;
	assert!(on_full[0].is_ok(), "{on_full:?}");
	let outcome = edge(&b, &s, "RETURN fn::mk()").await;
	let message = outcome[0].as_ref().expect_err("the edge refuses it");
	assert!(message.contains(NOT_SERVED), "{message}");
}

#[tokio::test]
async fn permissions_are_enforced_as_execute_enforces_them() {
	let (a, b) = pair().await;
	schema(
		&a,
		&b,
		"DEFINE ACCESS user ON DATABASE TYPE RECORD DURATION FOR SESSION 1h;
		 DEFINE TABLE doc SCHEMAFULL PERMISSIONS FOR select, update WHERE owner = $auth.id FOR create FULL FOR delete WHERE owner = $auth.id;
		 DEFINE FIELD owner ON doc TYPE record PERMISSIONS FULL;
		 DEFINE FIELD secret ON doc TYPE string PERMISSIONS FOR select WHERE owner = $auth.id;
		 DEFINE FIELD note ON doc TYPE option<string> PERMISSIONS FULL;
		 CREATE doc:1 SET owner = user:a, secret = 's1', note = 'n1';
		 CREATE doc:2 SET owner = user:b, secret = 's2', note = 'n2';",
	)
	.await;
	let record = Session::for_record(
		"test",
		"test",
		"user",
		crate::types::PublicValue::from_t(surrealdb_types::RecordId::new("user", "a")),
	);
	for sql in [
		"SELECT * FROM doc ORDER BY id",
		"UPDATE doc SET note = 'changed'",
		"DELETE doc:2",
		"DELETE doc:1",
		"SELECT * FROM doc",
		"CREATE doc:3 SET owner = user:a, secret = 'x'",
	] {
		parity(&a, &b, &record, sql).await;
	}
}

#[tokio::test]
async fn the_session_checks_are_those_of_execute() {
	let a = Builder::new().with_auth(true).build_with_path("memory").await.expect("opens");
	let b = Builder::for_edge().with_auth(true).build_with_path("memory").await.expect("opens");
	let anonymous = Session::default().with_ns("test").with_db("test");
	let on_full = a.execute("RETURN 1", &anonymous, None).await;
	let on_edge = b.execute_edge("RETURN 1", &anonymous, None).await;
	assert_eq!(
		on_full.as_ref().err().map(|error| error.to_string()),
		on_edge.as_ref().err().map(|error| error.to_string())
	);
	assert!(on_edge.is_err());
	let mut expired = session();
	expired.exp = Some(1);
	let on_full = a.execute("RETURN 1", &expired, None).await;
	let on_edge = b.execute_edge("RETURN 1", &expired, None).await;
	assert_eq!(
		on_full.as_ref().err().map(|error| error.to_string()),
		on_edge.as_ref().err().map(|error| error.to_string())
	);
	assert!(on_edge.is_err());
}

#[tokio::test]
async fn a_function_outside_the_edge_set_is_unknown() {
	let (a, b) = pair().await;
	let s = session();
	// The parser rejects a path it has no function for, before any datastore sees it.
	let parsed =
		a.execute("RETURN nosuch::fn()", &s, None).await.expect_err("the parser refuses it");
	assert!(parsed.to_string().contains("Invalid function/constant path"), "{parsed}");
	// A path the parser knows but the edge registry does not hold is the binder's unknown-function
	// error, whichever family the name belongs to.
	let mut shapes = Vec::new();
	for name in [
		"http::get('x')",
		"eval::surql('1')",
		"sequence::nextval('s')",
		"sleep(1ms)",
		"crypto::argon2::generate('x')",
	] {
		let outcome = edge(&b, &s, &format!("RETURN {name}")).await;
		let message = outcome[0].as_ref().expect_err("the function is unknown").clone();
		let function = name.split('(').next().expect("a name");
		shapes.push(message.replace(function, "NAME"));
		// The full datastore serves the same names, so the refusal is the registry's.
		let served = full(&a, &s, &format!("RETURN {name}")).await;
		assert_ne!(served[0].as_ref().err(), Some(&message), "{name} is served on the edge");
	}
	assert!(shapes.windows(2).all(|pair| pair[0] == pair[1]), "{shapes:?}");
}

#[tokio::test]
async fn a_denied_function_gives_the_same_error() {
	let caps = Capabilities::all().without_functions(Targets::Some(
		[FuncTarget::from_str("string::len").expect("a target")].into(),
	));
	let a = Builder::new()
		.with_capabilities(caps.clone())
		.build_with_path("memory")
		.await
		.expect("opens");
	let b =
		Builder::for_edge().with_capabilities(caps).build_with_path("memory").await.expect("opens");
	parity(&a, &b, &session(), "RETURN string::len('abc')").await;
	parity(&a, &b, &session(), "RETURN string::uppercase('abc')").await;
}

#[tokio::test]
async fn a_top_level_let_binds_for_the_statements_after_it() {
	let (a, b) = pair().await;
	let s = session();
	let outcome = edge(&b, &s, "LET $x = 1; RETURN $x + 1").await;
	assert_eq!(outcome, full(&a, &s, "LET $x = 1; RETURN $x + 1").await);
	assert_eq!(outcome.last().expect("a result").as_deref(), Ok("Number(Int(2))"), "{outcome:?}");
	parity(&a, &b, &s, "LET $auth = 1").await;
	let refused = edge(&b, &s, "LET $auth = 1").await;
	assert!(refused[0].is_err(), "{refused:?}");
}

#[tokio::test]
async fn each_statement_runs_in_a_transaction_of_its_own() {
	let (a, b) = pair().await;
	let s = session();
	let sql = "CREATE a:1; CREATE a:1; CREATE b:1;";
	let on_edge = edge(&b, &s, sql).await;
	assert!(on_edge[0].is_ok() && on_edge[1].is_err() && on_edge[2].is_ok(), "{on_edge:?}");
	let on_full = full(&a, &s, sql).await;
	assert_eq!(
		on_full.iter().map(Result::is_ok).collect::<Vec<_>>(),
		on_edge.iter().map(Result::is_ok).collect::<Vec<_>>()
	);
	for record in ["a:1", "b:1"] {
		let kept = edge(&b, &s, &format!("SELECT VALUE id FROM {record}")).await;
		assert_eq!(kept, full(&a, &s, &format!("SELECT VALUE id FROM {record}")).await);
		assert!(kept[0].as_ref().is_ok_and(|kept| kept.contains(&record[..1])), "{kept:?}");
	}
}

#[tokio::test]
async fn timeouts_behave_as_they_do_on_execute() {
	let (a, b) = pair().await;
	let s = session();
	parity(&a, &b, &s, "SELECT * FROM [1, 2, 3] TIMEOUT 1s").await;
	let slow = "FOR $i IN 0..2000000 { LET $x = $i * 2; }; RETURN 1;";
	let a = Builder::new()
		.with_query_timeout(Some(std::time::Duration::from_millis(1)))
		.build_with_path("memory")
		.await
		.expect("opens");
	let b = Builder::for_edge()
		.with_query_timeout(Some(std::time::Duration::from_millis(1)))
		.build_with_path("memory")
		.await
		.expect("opens");
	let on_full = full(&a, &s, slow).await;
	let on_edge = edge(&b, &s, slow).await;
	assert!(on_full[0].is_err(), "{on_full:?}");
	assert_eq!(on_full[0], on_edge[0]);
	let (a, b) = pair().await;
	let clause =
		"LET $n = 3000000; SELECT VALUE math::sum(array::range(0, $n)) FROM [1] TIMEOUT 1ms;";
	let on_full = full(&a, &s, clause).await;
	let on_edge = edge(&b, &s, clause).await;
	assert_eq!(on_full, on_edge);
}

/// The error the attach check of an edge datastore gives for its own store, as the builder's hook
/// would see it.
async fn attach_error(ds: &Datastore) -> anyhow::Error {
	let verdict = migration::ledger_verdict(ds, migration::LEDGER).await.expect("the ledger reads");
	ds.attach_edge(verdict).await.expect_err("the attach is refused")
}

#[tokio::test]
async fn a_new_store_is_baselined_and_stamped_by_the_edge_attach() {
	let b = Builder::for_edge().build_with_path("memory").await.expect("the edge datastore opens");
	let stamp = b.storage_version().await.unwrap().expect("a new store is stamped");
	assert_eq!(stamp.triple(), StorageVersion::current().triple());
	let applied: Vec<u32> =
		b.applied_migration_ids().await.unwrap().into_iter().map(|(id, _)| id).collect();
	for row in migration::LEDGER.iter().filter(|row| !row.on_demand) {
		assert!(applied.contains(&row.id), "the baseline lacks `{}`", row.name);
	}
}

/// An edge datastore never runs `check_version`, so its attach is what releases the jobs a host
/// drives: without it the host's every maintenance slice is refused.
#[tokio::test]
async fn an_attached_edge_store_takes_host_driven_maintenance() {
	let b = Builder::for_edge().build_with_path("memory").await.expect("the edge datastore opens");
	assert!(b.is_migrated());
	b.run_maintenance_slice().await.expect("the host drives a slice");
}

#[tokio::test]
async fn a_store_with_a_pending_migration_is_refused_by_the_edge_attach() {
	let b = Builder::for_edge().build_with_path("memory").await.expect("the edge datastore opens");
	let txn = b.transaction(TransactionType::Write).await.unwrap();
	txn.del_key(&MigrationKey::new(1)).await.unwrap();
	txn.commit().await.unwrap();

	let err = attach_error(&b).await;
	assert!(matches!(
		err.downcast_ref::<DatastoreError>(),
		Some(DatastoreError::NotAttachable { .. })
	));
	assert!(err.to_string().contains("copy sequence definitions out of the table band"), "{err}");
}

#[tokio::test]
async fn a_store_at_another_release_is_refused_by_the_edge_attach() {
	let b = Builder::for_edge().build_with_path("memory").await.expect("the edge datastore opens");
	let txn = b.transaction(TransactionType::Write).await.unwrap();
	txn.set_key(&StorageVersionKey {}, &StorageVersion::parse("3.3.0").unwrap()).await.unwrap();
	txn.commit().await.unwrap();

	let err = attach_error(&b).await;
	assert!(matches!(
		err.downcast_ref::<DatastoreError>(),
		Some(DatastoreError::NotAttachable { .. })
	));
	assert!(err.to_string().contains("version stamp"), "{err}");
}

#[tokio::test]
async fn an_unknown_incompatible_ledger_entry_is_refused_by_the_edge_attach() {
	let b = Builder::for_edge().build_with_path("memory").await.expect("the edge datastore opens");
	let txn = b.transaction(TransactionType::Write).await.unwrap();
	txn.set(MigrationKey::new(9999).encode_key().unwrap(), vec![0xff, 0xff, 0xff]).await.unwrap();
	txn.commit().await.unwrap();

	let err = attach_error(&b).await;
	assert!(
		matches!(
			err.downcast_ref::<DatastoreError>(),
			Some(DatastoreError::MigratedBeyondStorageVersion { .. })
		),
		"{err}"
	);
	assert!(err.to_string().contains("id 9999"), "{err}");
}

/// A unique index is enforced only if the write path maintains it, and a read cannot tell: an
/// unmaintained index falls back to a scan and returns the same rows.
#[tokio::test]
async fn a_write_on_the_edge_maintains_an_index() {
	let b = Builder::for_edge().build_with_path("memory").await.expect("the edge datastore opens");
	full(&b, &Session::owner(), "DEFINE NAMESPACE test; USE NS test; DEFINE DATABASE test;").await;
	for outcome in full(
		&b,
		&session(),
		"DEFINE TABLE t SCHEMALESS; DEFINE INDEX uniq ON t FIELDS name UNIQUE;",
	)
	.await
	{
		outcome.expect("the schema statement runs");
	}

	let outcome =
		edge(&b, &session(), "CREATE t:1 SET name = 'a'; CREATE t:2 SET name = 'a';").await;
	assert!(outcome[0].is_ok(), "{outcome:?}");
	assert!(outcome[1].as_ref().is_err_and(|error| error.contains("uniq")), "{outcome:?}");

	let outcome =
		edge(&b, &session(), "UPDATE t:1 SET name = 'b'; CREATE t:3 SET name = 'a';").await;
	assert!(outcome.iter().all(|outcome| outcome.is_ok()), "{outcome:?}");
}
