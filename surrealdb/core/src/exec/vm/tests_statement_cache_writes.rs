//! Writes kept in the cache of compiled top-level statements.
//!
//! A write whose request variables are its target, named by a variable whose value names one
//! table, and values its clauses read is compiled once per statement text, frame and class of
//! target: requests that bind other records of that table and other values to its clauses are
//! served the one kept. Each test runs the same requests against a datastore that keeps
//! statements and one that keeps none, and the two answer alike, the state they leave included.

use surrealdb_cnf::ConfigMap;
use surrealdb_types::{Array, Object, RecordId, Table, ToSql, Value, Variables};

use crate::dbs::Session;
use crate::exec::operators::test_util::TestDb;
use crate::kvs::{Datastore, StatementCacheStats};

/// Who runs a request.
#[derive(Clone, Copy)]
enum Who {
	Owner,
	Alice,
	Bob,
}

/// One request: who runs it, its text, and the variables it binds.
struct Request {
	who: Who,
	sql: String,
	vars: Variables,
}

fn owner(sql: &str, vars: &[(&str, Value)]) -> Request {
	request(Who::Owner, sql, vars)
}

fn alice(sql: &str, vars: &[(&str, Value)]) -> Request {
	request(Who::Alice, sql, vars)
}

fn bob(sql: &str, vars: &[(&str, Value)]) -> Request {
	request(Who::Bob, sql, vars)
}

fn request(who: Who, sql: &str, vars: &[(&str, Value)]) -> Request {
	let mut bound = Variables::new();
	for (name, value) in vars {
		bound.insert(*name, value.clone());
	}
	Request {
		who,
		sql: sql.to_owned(),
		vars: bound,
	}
}

/// The request variables of one request of a form, by its index.
type VarsOf = fn(i64) -> Vec<(&'static str, Value)>;

fn record(table: &str, key: i64) -> Value {
	Value::RecordId(RecordId::new(table, key))
}

fn records(table: &str, keys: &[i64]) -> Value {
	Value::Array(Array::from(keys.iter().map(|key| record(table, *key)).collect::<Vec<_>>()))
}

fn table(name: &str) -> Value {
	Value::Table(Table::new(name))
}

fn object(fields: &[(&str, Value)]) -> Value {
	let mut object = Object::new();
	for (name, value) in fields {
		object.insert((*name).to_owned(), value.clone());
	}
	Value::Object(object)
}

fn int(n: i64) -> Value {
	Value::Number(surrealdb_types::Number::Int(n))
}

fn text(s: &str) -> Value {
	Value::String(s.to_owned())
}

fn session(who: Who) -> Session {
	match who {
		Who::Owner => TestDb::owner(),
		Who::Alice => Session::for_record(
			"test",
			"test",
			"user",
			Value::RecordId(RecordId::new("user", "alice")),
		),
		Who::Bob => Session::for_record(
			"test",
			"test",
			"user",
			Value::RecordId(RecordId::new("user", "bob")),
		),
	}
}

/// The answer of every statement of `request`, as SurrealQL, or its error.
async fn answers(ds: &Datastore, request: &Request) -> Vec<String> {
	ds.execute(&request.sql, &session(request.who), Some(request.vars.clone()))
		.await
		.expect("the request runs")
		.into_iter()
		.map(|response| match response.result {
			Ok(value) => unstamped(&value.to_sql()),
			Err(e) => format!("error: {}", e.message()),
		})
		.collect()
}

/// `answer` with each change-feed versionstamp written as `_`: a stamp counts the commits of the
/// process, not of one datastore.
fn unstamped(answer: &str) -> String {
	const STAMP: &str = "versionstamp: ";
	let mut out = String::with_capacity(answer.len());
	let mut rest = answer;
	while let Some(at) = rest.find(STAMP) {
		out.push_str(&rest[..at + STAMP.len()]);
		out.push('_');
		rest = rest[at + STAMP.len()..].trim_start_matches(|c: char| c.is_ascii_digit());
	}
	out.push_str(rest);
	out
}

/// The answers of `requests` in order against a datastore set up by `setup` and built with
/// `config`, each line naming the request it answers, and what the statement cache counted.
async fn run(
	setup: &str,
	requests: &[Request],
	config: ConfigMap,
) -> (Vec<String>, StatementCacheStats) {
	let db = TestDb::new_with_auth_and_config(setup, config).await;
	let ds = db.datastore();
	let before = ds.statement_cache_stats();
	let mut lines = Vec::new();
	for (at, request) in requests.iter().enumerate() {
		for answer in answers(ds, request).await {
			lines.push(format!("{at}: {} => {answer}", request.sql));
		}
	}
	let after = ds.statement_cache_stats();
	(
		lines,
		StatementCacheStats {
			hits: after.hits - before.hits,
			misses: after.misses - before.misses,
			invalidations: after.invalidations - before.invalidations,
		},
	)
}

/// Runs `requests` three ways — every write's stages inline with nothing kept, kept with each
/// linked bundle compiled for its statement alone (no compile cache), and kept with the bundles
/// shared — asserts every answer is the same, and answers what the statement cache counted with
/// both caches on.
async fn same_on_and_off(setup: &str, requests: &[Request]) -> StatementCacheStats {
	// What main answers: every write's stages its own ops, nothing kept.
	crate::exec::vm::units::INLINE_STAGES.with(|inline| inline.set(true));
	let (inline, _) = run(
		setup,
		requests,
		ConfigMap::empty()
			.with_key_value("statement_cache_size", "0")
			.with_key_value("compiled_unit_cache_size", "0"),
	)
	.await;
	crate::exec::vm::units::INLINE_STAGES.with(|inline| inline.set(false));
	crate::exec::vm::units::INLINED_AGAIN.with(|again| again.set(0));
	let mut on = Vec::new();
	let mut kept = StatementCacheStats::default();
	let kept_units = catalog_units(async {
		(on, kept) = run(setup, requests, ConfigMap::empty()).await;
	})
	.await;
	let mut linked = Vec::new();
	let linked_units = catalog_units(async {
		(linked, _) = run(
			setup,
			requests,
			ConfigMap::empty().with_key_value("compiled_unit_cache_size", "0"),
		)
		.await;
	})
	.await;
	// Each run links the stages of some write, no write is planned inline after a failed link,
	// and the kept run serves writes from the statement cache.
	for (name, (compiled, taken)) in [("kept", kept_units), ("linked", linked_units)] {
		assert!(compiled + taken > 0, "{name}: no write linked its stages");
	}
	assert_eq!(crate::exec::vm::units::INLINED_AGAIN.with(std::cell::Cell::get), 0);
	assert!(kept.hits > 0, "no write was served from the statement cache: {kept:?}");
	for (name, answers) in [("kept", &on), ("linked", &linked)] {
		assert_eq!(answers.len(), inline.len(), "{name}: {answers:#?}\n{inline:#?}");
		for (answer, main) in answers.iter().zip(&inline) {
			assert_eq!(answer, main, "{name}");
		}
	}
	kept
}

const SETUP: &str = "
	DEFINE ACCESS user ON DATABASE TYPE RECORD;
	DEFINE TABLE ls SCHEMALESS;
	DEFINE TABLE pk SCHEMAFULL;
	DEFINE FIELD name ON pk TYPE string DEFAULT 'none';
	DEFINE FIELD n ON pk TYPE int DEFAULT 0 ASSERT $value >= 0;
	DEFINE TABLE likes TYPE RELATION;
	CREATE ls:1 SET n = 1; CREATE ls:2 SET n = 2; CREATE ls:3 SET n = 3;
	CREATE pk:1 SET name = 'a', n = 1; CREATE pk:2 SET name = 'b', n = 2;
";

/// Every write form the cache keeps, bound to several records and values, answers as it does
/// compiled with its request, and the second request of each statement is served the compilation
/// the first kept.
#[tokio::test]
async fn kept_writes_answer_as_writes_compiled_with_their_request() {
	let mut requests = Vec::new();
	for key in [1, 2, 3, 1] {
		requests.push(owner("UPDATE $id SET n += 1", &[("id", record("ls", key))]));
		requests.push(owner(
			"UPDATE $id SET n = $v RETURN n, $v AS v",
			&[("id", record("pk", 1 + key % 2)), ("v", int(key * 10))],
		));
		requests.push(owner(
			"UPDATE $_record_id MERGE $_data",
			&[("_record_id", record("ls", key)), ("_data", object(&[("m", int(key))]))],
		));
		requests.push(owner(
			"UPSERT $id SET n = $v",
			&[("id", record("ls", 10 + key)), ("v", int(key))],
		));
		requests.push(owner(
			"CREATE $_record_id CONTENT $_content",
			&[
				("_record_id", record("pk", 100 + key)),
				("_content", object(&[("name", text("made")), ("n", int(key))])),
			],
		));
		requests.push(owner(
			"CREATE $_table CONTENT $_content RETURN NONE",
			&[("_table", table("ls")), ("_content", object(&[("n", int(key))]))],
		));
		requests.push(owner(
			"RELATE $a->likes->$b SET w = $w RETURN in, out, w",
			&[("a", record("ls", key)), ("b", record("pk", 1)), ("w", int(key))],
		));
		requests.push(owner(
			"UPDATE $ids SET n += 1 RETURN VALUE n",
			&[("ids", records("ls", &[1, 2, 3]))],
		));
		requests.push(owner("DELETE $id RETURN BEFORE", &[("id", record("ls", 10 + key))]));
	}
	// A value an ASSERT refuses, and a record that already exists, fail as they do compiled with
	// the request.
	requests.push(owner("UPDATE $id SET n = $v", &[("id", record("pk", 1)), ("v", int(-1))]));
	requests.push(owner("UPDATE $id SET n = $v", &[("id", record("pk", 1)), ("v", text("x"))]));
	requests.push(owner(
		"CREATE $_record_id CONTENT $_content",
		&[("_record_id", record("pk", 101)), ("_content", object(&[("name", text("again"))]))],
	));
	// The values a data clause reads key nothing: values of every kind, and ones the stages
	// refuse, are served the one compilation and answer as a compilation that folded them.
	for content in [
		object(&[("name", text("x")), ("n", int(1))]),
		object(&[("name", int(7))]),
		Value::Array(Array::from(vec![int(1)])),
		Value::None,
		text("not an object"),
	] {
		requests.push(owner("UPSERT $id CONTENT $c", &[("id", record("pk", 1)), ("c", content)]));
	}
	for data in [object(&[("m", object(&[("deep", int(1))]))]), object(&[]), Value::None, int(3)] {
		requests
			.push(owner("UPDATE $id MERGE $d RETURN m", &[("id", record("ls", 2)), ("d", data)]));
	}
	for v in [int(1), text("1"), Value::None, Value::Bool(true)] {
		requests
			.push(owner("UPDATE $id SET n = $v RETURN n", &[("id", record("pk", 2)), ("v", v)]));
	}
	// Another class of target is compiled afresh: a table, another table's record.
	requests.push(owner("UPDATE $id SET n += 1 RETURN NONE", &[("id", table("ls"))]));
	requests.push(owner("UPDATE $id SET n += 1", &[("id", record("pk", 2))]));
	// Records written at random ids are compared without them.
	requests.push(owner(
		"SELECT * FROM pk ORDER BY id; SELECT VALUE [n, m] FROM ls ORDER BY n, m; SELECT in, out, w FROM likes ORDER BY in, w",
		&[],
	));
	let kept = same_on_and_off(SETUP, &requests).await;
	assert!(kept.hits >= 20, "{kept:?}");
}

/// A schema statement between two identical kept writes changes what the second coerces,
/// derives, fires, indexes and enforces, as it does for writes compiled with their request.
#[tokio::test]
async fn a_schema_change_between_two_kept_writes_changes_what_the_second_runs() {
	let write = |key: i64, v: Value| {
		owner("UPDATE $id SET a = $v, b = 1 RETURN AFTER", &[("id", record("t", key)), ("v", v)])
	};
	let requests = vec![
		owner("DEFINE TABLE t SCHEMALESS; DEFINE TABLE log SCHEMALESS", &[]),
		write(1, text("1")),
		write(2, text("1")),
		owner("DEFINE FIELD a ON t TYPE int", &[]),
		write(1, text("1")),
		write(2, text("x")),
		owner("DEFINE FIELD b ON t VALUE $value + 10", &[]),
		write(1, text("2")),
		owner("DEFINE FIELD c ON t TYPE string DEFAULT 'filled'", &[]),
		write(2, text("3")),
		owner("DEFINE EVENT seen ON t THEN { CREATE log SET of = $after.id, a = $after.a }", &[]),
		write(1, text("4")),
		write(2, text("5")),
		owner("REMOVE EVENT seen ON t", &[]),
		write(1, text("6")),
		owner("DEFINE INDEX ua ON t FIELDS a UNIQUE", &[]),
		write(1, text("7")),
		write(2, text("7")),
		owner("REMOVE INDEX ua ON t", &[]),
		write(2, text("7")),
		owner("BEGIN; DEFINE FIELD OVERWRITE a ON t TYPE string; CANCEL", &[]),
		write(1, text("8")),
		owner("SELECT * FROM t ORDER BY id; SELECT a FROM log ORDER BY a", &[]),
	];
	let kept = same_on_and_off("", &requests).await;
	assert!(kept.hits >= 1, "{kept:?}");
}

/// A record user's kept write is judged against the table's permissions as they stand when each
/// request runs: a redefined `PERMISSIONS` clause, and a field's, take effect on the next one.
#[tokio::test]
async fn a_kept_write_of_a_record_user_takes_the_permissions_redefined_between_requests() {
	let setup = "
		DEFINE ACCESS user ON DATABASE TYPE RECORD;
		DEFINE TABLE doc SCHEMALESS PERMISSIONS FULL;
		CREATE doc:1 SET owner = user:alice, n = 0;
		CREATE doc:2 SET owner = user:bob, n = 0;
	";
	let bump = |key: i64| {
		alice(
			"UPDATE $id SET n += 1, note = $note",
			&[("id", record("doc", key)), ("note", text("by alice"))],
		)
	};
	let requests = vec![
		bump(1),
		bump(2),
		owner(
			"DEFINE TABLE OVERWRITE doc SCHEMALESS PERMISSIONS FOR select FULL FOR update WHERE owner = $auth",
			&[],
		),
		bump(1),
		bump(2),
		owner("DEFINE FIELD note ON doc PERMISSIONS FOR update NONE", &[]),
		bump(1),
		owner("DEFINE TABLE OVERWRITE doc SCHEMALESS PERMISSIONS NONE", &[]),
		bump(1),
		alice("DELETE $id", &[("id", record("doc", 1))]),
		owner("DEFINE TABLE OVERWRITE doc SCHEMALESS PERMISSIONS FULL", &[]),
		bump(2),
		alice("CREATE $id SET owner = $auth", &[("id", record("doc", 3))]),
		owner("SELECT * FROM doc ORDER BY id", &[]),
	];
	same_on_and_off(setup, &requests).await;
}

/// The cache counts a kept write: the first request of a statement compiles it, and the next ones
/// bound to other records of the table are served it.
#[tokio::test]
async fn a_kept_write_is_compiled_once_for_its_class() {
	let db = TestDb::new(SETUP).await;
	let ds = db.datastore();
	let base = ds.statement_cache_stats();
	for key in [1, 2, 3] {
		let got = answers(
			ds,
			&owner(
				"UPDATE $id SET n = $v RETURN VALUE n",
				&[("id", record("ls", key)), ("v", int(key * 2))],
			),
		)
		.await;
		assert_eq!(got, vec![format!("[{}]", key * 2)]);
	}
	let after = ds.statement_cache_stats();
	assert_eq!((after.misses - base.misses, after.hits - base.hits), (1, 2));
}

/// A kept write under `TIMEOUT` is compiled once for its class, as it is without one. A variable
/// its timeout reads is folded into the plan with the timeout, so each value of it is compiled
/// with its request rather than served a plan holding another request's timeout.
#[tokio::test]
async fn a_kept_write_with_a_timeout_is_compiled_once_for_its_class() {
	let db = TestDb::new(SETUP).await;
	let ds = db.datastore();
	let base = ds.statement_cache_stats();
	for key in [1, 2, 3] {
		let got = answers(
			ds,
			&owner(
				"UPDATE $id SET n = $v RETURN VALUE n TIMEOUT 5s",
				&[("id", record("ls", key)), ("v", int(key * 3))],
			),
		)
		.await;
		assert_eq!(got, vec![format!("[{}]", key * 3)]);
	}
	let after = ds.statement_cache_stats();
	assert_eq!((after.misses - base.misses, after.hits - base.hits), (1, 2));
	let base = ds.statement_cache_stats();
	for key in [1, 2, 3] {
		let timeout = Value::Duration(std::time::Duration::from_secs(5 + key as u64).into());
		let got = answers(
			ds,
			&owner(
				"UPDATE $id SET n = $v RETURN VALUE n TIMEOUT $t",
				&[("id", record("ls", key)), ("v", int(key * 5)), ("t", timeout)],
			),
		)
		.await;
		assert_eq!(got, vec![format!("[{}]", key * 5)]);
	}
	let after = ds.statement_cache_stats();
	assert_eq!(after.hits - base.hits, 0, "no request is served another's timeout");
}

/// Each form of a kept write is compiled once for its class and served after that.
#[tokio::test]
async fn each_kept_write_form_is_compiled_once() {
	let db = TestDb::new(SETUP).await;
	let ds = db.datastore();
	let forms: [(&str, VarsOf); 5] = [
		("CREATE $id SET n = 1", |key| vec![("id", record("ls", 100 + key))]),
		("UPSERT $id SET n = 1", |key| vec![("id", record("ls", 200 + key))]),
		("UPDATE $id SET n = 1", |key| vec![("id", record("ls", 1 + key % 3))]),
		("DELETE $id", |key| vec![("id", record("ls", 200 + key))]),
		("RELATE $a->likes->$b", |key| {
			vec![("a", record("ls", 1 + key % 3)), ("b", record("pk", 1))]
		}),
	];
	for (sql, vars) in forms {
		let base = ds.statement_cache_stats();
		for key in 0..3 {
			let bound = vars(key);
			let bound: Vec<(&str, Value)> = bound.into_iter().collect();
			answers(ds, &owner(sql, &bound)).await;
		}
		let after = ds.statement_cache_stats();
		assert_eq!(
			(
				after.misses - base.misses,
				after.hits - base.hits,
				after.invalidations - base.invalidations
			),
			(1, 2, 0),
			"{sql}"
		);
	}
}

/// How many bundles of linked stages this thread compiled and took from the compile cache while
/// `run` ran.
async fn catalog_units<F: std::future::Future<Output = ()>>(run: F) -> (usize, usize) {
	crate::exec::vm::units::LINKED.with(|units| units.set((0, 0)));
	run.await;
	crate::exec::vm::units::LINKED.with(std::cell::Cell::get)
}

/// Kept statements of one shape share the bundle of their table's stages whatever variables
/// name their targets: the first compiles it and the next link it, until a definition of the
/// table changes or a statement of another shape asks for its own. A statement the cache does
/// not keep links nothing.
#[tokio::test]
async fn statements_of_one_shape_link_one_bundle_of_their_table() {
	let db = TestDb::new(SETUP).await;
	let ds = db.datastore();
	let run = |sql: &'static str, vars: &[(&'static str, Value)]| {
		let request = owner(sql, vars);
		async move {
			let got = answers(ds, &request).await;
			assert!(got.iter().all(|answer| !answer.starts_with("error")), "{sql}: {got:?}");
		}
	};
	let units = catalog_units(async {
		run("UPDATE $id SET n = 5", &[("id", record("ls", 1))]).await;
		run("UPDATE $id SET n = 5", &[("id", record("ls", 2))]).await;
		run("UPDATE $other SET n = 5", &[("other", record("ls", 3))]).await;
	})
	.await;
	assert_eq!(units, (1, 1), "one bundle, compiled once and linked by the other statement");
	let units = catalog_units(async {
		run("DEFINE FIELD m ON ls VALUE $value OR 0", &[]).await;
		run("UPDATE $id SET n = 5", &[("id", record("ls", 1))]).await;
		run("UPDATE $other SET n = 5", &[("other", record("ls", 2))]).await;
		run("UPDATE $id SET n = 6", &[("id", record("ls", 1))]).await;
	})
	.await;
	assert_eq!(units, (2, 1), "a new definition, and a statement of another shape, compile anew");
	let units = catalog_units(async {
		run("UPDATE ls:3 SET n = 7", &[]).await;
	})
	.await;
	assert_eq!(units, (0, 0), "a statement planned for its request alone links nothing");
	let got = answers(ds, &owner("SELECT VALUE [n, m] FROM ls ORDER BY id", &[])).await;
	assert_eq!(got, vec!["[[6, 0], [5, 0], [7, 0]]".to_owned()]);
}

/// A live query started or killed between two identical writes is notified by the writes that run
/// while it is live and by no other, whether the write is kept for its class of target or links
/// its table's stages: the notifications are those of writes whose stages are their own ops.
#[tokio::test]
async fn a_live_query_between_two_identical_writes_sees_the_second() {
	let run = async |config: ConfigMap| {
		let (send, recv) = crate::channel::bounded(1_000);
		let builder = crate::kvs::Datastore::builder().with_notify(send);
		let db = TestDb::new_on(builder, SETUP, config).await;
		let ds = db.datastore();
		let session = TestDb::owner().with_rt(true);
		let writes = async || {
			for n in [1, 2] {
				let mut vars = Variables::new();
				vars.insert("id", record("ls", n));
				for (sql, vars) in
					[("UPDATE $id SET n += 1", Some(vars)), ("UPDATE ls:3 SET n += 1", None)]
				{
					let responses = ds.execute(sql, &session, vars).await.expect("the write runs");
					assert!(responses.iter().all(|response| response.result.is_ok()), "{sql}");
				}
			}
		};
		let received = async || {
			let mut seen = Vec::new();
			while let Ok(Ok(notification)) =
				tokio::time::timeout(std::time::Duration::from_millis(300), recv.recv()).await
			{
				seen.push(format!("{} {}", notification.action, notification.record.to_sql()));
			}
			seen
		};
		let mut phases = Vec::new();
		writes().await;
		phases.push(received().await);
		let live = ds.execute("LIVE SELECT * FROM ls", &session, None).await.expect("starts");
		let id = live[0].result.as_ref().expect("a live query id").clone();
		writes().await;
		phases.push(received().await);
		let killed =
			ds.execute(&format!("KILL {}", id.to_sql()), &session, None).await.expect("kills");
		assert!(killed[0].result.is_ok(), "{:?}", killed[0].result);
		writes().await;
		phases.push(received().await);
		phases
	};
	let nothing_kept = || {
		ConfigMap::empty()
			.with_key_value("statement_cache_size", "0")
			.with_key_value("compiled_unit_cache_size", "0")
	};
	crate::exec::vm::units::INLINE_STAGES.with(|inline| inline.set(true));
	let inline = run(nothing_kept()).await;
	crate::exec::vm::units::INLINE_STAGES.with(|inline| inline.set(false));
	let mut kept = Vec::new();
	let (compiled, taken) = catalog_units(async {
		kept = run(ConfigMap::empty()).await;
	})
	.await;
	assert!(compiled + taken > 0, "no write linked its stages");
	let linked = run(ConfigMap::empty().with_key_value("compiled_unit_cache_size", "0")).await;
	assert_eq!(kept, inline);
	assert_eq!(linked, inline);
	assert!(kept[0].is_empty(), "no live query yet: {kept:?}");
	assert_eq!(
		kept[1].iter().filter(|seen| seen.starts_with("UPDATE")).count(),
		4,
		"every write while the query is live notifies it: {kept:?}"
	);
	assert!(
		!kept[2].iter().any(|seen| seen.starts_with("UPDATE")),
		"a killed query is notified of no write: {kept:?}"
	);
}

/// What a table's stages read beyond its own definition — the `REFERENCE` fields of other tables,
/// a function a clause calls, a parameter a clause reads, a change feed — changed between two
/// identical writes changes what the second runs, kept or linked, as it does inline.
#[tokio::test]
async fn a_definition_the_stages_read_beyond_their_table_changes_between_two_writes() {
	let setup = "
		DEFINE TABLE t SCHEMALESS;
		DEFINE TABLE u SCHEMALESS;
		DEFINE FUNCTION fn::bump($v: int) { RETURN $v + 1 };
		DEFINE PARAM $step VALUE 10;
		DEFINE FIELD f ON t VALUE fn::bump($value OR 0);
		DEFINE FIELD g ON t VALUE ($value OR 0) + $step;
		CREATE t:1, t:2, t:3, t:4, t:5, t:6;
		CREATE u:1 SET r = t:1; CREATE u:2 SET r = t:2; CREATE u:3 SET r = t:3;
	";
	let update = |key: i64| owner("UPDATE $id SET f = 1, g = 1", &[("id", record("t", key))]);
	let delete = |key: i64| owner("DELETE $id", &[("id", record("t", key))]);
	let requests = vec![
		update(1),
		owner("DEFINE FUNCTION OVERWRITE fn::bump($v: int) { RETURN $v + 100 }", &[]),
		update(2),
		owner("DEFINE PARAM OVERWRITE $step VALUE 1000", &[]),
		update(3),
		owner("UPDATE t:4 SET f = 1, g = 1", &[]),
		delete(1),
		owner("DEFINE FIELD r ON u TYPE option<record<t>> REFERENCE ON DELETE UNSET", &[]),
		delete(2),
		owner("DELETE t:5", &[]),
		owner(
			"DEFINE FIELD OVERWRITE r ON u TYPE option<record<t>> REFERENCE ON DELETE CASCADE",
			&[],
		),
		delete(3),
		owner("DEFINE TABLE OVERWRITE t SCHEMALESS CHANGEFEED 1h", &[]),
		update(6),
		owner("UPDATE t:6 SET f = 2, g = 2", &[]),
		owner("SHOW CHANGES FOR TABLE t SINCE 0 LIMIT 10", &[]),
		owner("SELECT * FROM t ORDER BY id; SELECT * FROM u ORDER BY id", &[]),
	];
	same_on_and_off(setup, &requests).await;
}

/// Inside one transaction a definition ahead of a kept write in the same request changes what it
/// runs, as it does for the write compiled with the request.
#[tokio::test]
async fn a_definition_ahead_of_a_kept_write_in_one_transaction_is_read_by_it() {
	let setup = "DEFINE TABLE t SCHEMALESS; CREATE t:1, t:2;";
	let requests = vec![
		owner("UPDATE $id SET a = '1'", &[("id", record("t", 1))]),
		owner("UPDATE $id SET a = '1'", &[("id", record("t", 2))]),
		owner(
			"BEGIN; DEFINE FIELD a ON t TYPE int; UPDATE $id SET a = '1'; COMMIT",
			&[("id", record("t", 2))],
		),
		owner(
			"BEGIN; DEFINE FIELD OVERWRITE a ON t TYPE string; UPDATE $id SET a = '1'; COMMIT",
			&[("id", record("t", 2))],
		),
		owner("UPDATE $id SET a = '1'", &[("id", record("t", 1))]),
		owner("SELECT * FROM t ORDER BY id", &[]),
	];
	same_on_and_off(setup, &requests).await;
}

/// Two record users of one access share a kept write and its linked stages, and each is judged by
/// the table's and the fields' permissions as themself: one is refused what the other may write.
#[tokio::test]
async fn record_users_sharing_a_kept_write_are_each_judged_as_themselves() {
	let setup = "
		DEFINE ACCESS user ON DATABASE TYPE RECORD;
		DEFINE TABLE doc SCHEMALESS
			PERMISSIONS FOR select FULL FOR create, update, delete WHERE owner = $auth;
		DEFINE FIELD note ON doc PERMISSIONS FOR update WHERE $auth = user:alice;
		CREATE doc:1 SET owner = user:alice; CREATE doc:2 SET owner = user:bob;
	";
	let write = |key: i64| [("id", record("doc", key)), ("note", text("hi"))];
	let requests = vec![
		alice("UPDATE $id SET n += 1, note = $note", &write(1)),
		bob("UPDATE $id SET n += 1, note = $note", &write(1)),
		bob("UPDATE $id SET n += 1, note = $note", &write(2)),
		alice("UPDATE $id SET n += 1, note = $note", &write(2)),
		alice("UPDATE doc:1 SET n += 1", &[]),
		bob("UPDATE doc:1 SET n += 1", &[]),
		bob("DELETE $id", &[("id", record("doc", 1))]),
		alice("CREATE $id SET owner = $auth", &[("id", record("doc", 3))]),
		bob("CREATE $id SET owner = user:alice", &[("id", record("doc", 4))]),
		owner("SELECT * FROM doc ORDER BY id", &[]),
	];
	same_on_and_off(setup, &requests).await;
}

/// A record id of another table among a write's data fails as the inline write fails it.
#[tokio::test]
async fn a_record_id_of_another_table_in_the_data_fails_as_inline() {
	let setup = "DEFINE TABLE t SCHEMALESS; DEFINE TABLE u SCHEMALESS; DEFINE TABLE e TYPE RELATION; CREATE t:1, t:2;";
	let requests = vec![
		owner("CREATE t CONTENT { id: u:1 }", &[]),
		owner(
			"CREATE $t CONTENT $c",
			&[("t", table("t")), ("c", object(&[("id", record("u", 2))]))],
		),
		owner("UPDATE t:1 SET id = u:1", &[]),
		owner("UPDATE $id SET id = u:1", &[("id", record("t", 2))]),
		owner("UPDATE $id SET id = u:1", &[("id", record("t", 1))]),
		owner("UPSERT t:3 CONTENT { id: u:3 }", &[]),
		owner("RELATE t:1->e->t:2 CONTENT { id: u:4 }", &[]),
		owner("RELATE t:1->e->t:2 CONTENT { id: e:5 }", &[]),
		owner("SELECT * FROM t, u, e ORDER BY id", &[]),
	];
	same_on_and_off(setup, &requests).await;
}

/// A view fed by the table, defined and redefined between identical writes, holds what the
/// writes after each definition feed it.
#[tokio::test]
async fn a_view_defined_between_two_writes_is_fed_by_the_second() {
	let setup =
		"DEFINE TABLE t SCHEMALESS; CREATE t:1 SET g = 'a', n = 1; CREATE t:2 SET g = 'b', n = 1;";
	let bump = |key: i64| owner("UPDATE $id SET n += 1", &[("id", record("t", key))]);
	let requests = vec![
		bump(1),
		bump(1),
		owner("DEFINE TABLE v AS SELECT g, math::sum(n) AS total FROM t GROUP BY g", &[]),
		bump(1),
		bump(2),
		owner("SELECT * FROM v ORDER BY g", &[]),
		owner(
			"DEFINE TABLE OVERWRITE v AS SELECT g, count() AS rows FROM t WHERE n > 2 GROUP BY g",
			&[],
		),
		bump(2),
		bump(2),
		owner("SELECT * FROM v ORDER BY g", &[]),
	];
	same_on_and_off(setup, &requests).await;
}

/// The function a field's `VALUE` calls runs under its own `PERMISSIONS` for each record user
/// that shares a kept write, and a field's `SELECT` permission cuts what `RETURN AFTER` shows each.
#[tokio::test]
async fn record_users_sharing_a_kept_write_meet_function_and_field_permissions_as_themselves() {
	let setup = "
		DEFINE ACCESS user ON DATABASE TYPE RECORD;
		DEFINE FUNCTION fn::stamp($v: any) { RETURN $v + 1 } PERMISSIONS WHERE $auth = user:alice;
		DEFINE TABLE doc SCHEMALESS PERMISSIONS FULL;
		DEFINE FIELD stamped ON doc VALUE IF $auth = user:alice { fn::stamp($value OR 0) } ELSE { $value };
		DEFINE FIELD secret ON doc PERMISSIONS FOR select WHERE $auth = user:alice;
		CREATE doc:1 SET secret = 's'; CREATE doc:2 SET secret = 's';
	";
	let write = |key: i64| [("id", record("doc", key)), ("v", int(key))];
	let requests = vec![
		alice("UPDATE $id SET stamped = $v RETURN AFTER", &write(1)),
		bob("UPDATE $id SET stamped = $v RETURN AFTER", &write(2)),
		alice("UPDATE $id SET stamped = $v RETURN AFTER", &write(2)),
		bob("UPDATE $id SET stamped = $v RETURN AFTER", &write(1)),
		bob("CREATE $id SET secret = 'b' RETURN AFTER", &[("id", record("doc", 3))]),
		alice("CREATE $id SET secret = 'a' RETURN AFTER", &[("id", record("doc", 4))]),
		owner("SELECT * FROM doc ORDER BY id", &[]),
	];
	same_on_and_off(setup, &requests).await;
}

/// An id a write's data names, read when the write runs, is judged against the target as the
/// inline write judges it: the same table's id is taken, another key of the record is refused.
#[tokio::test]
async fn an_id_in_a_kept_write_s_data_is_judged_against_its_target() {
	let setup = "DEFINE TABLE t SCHEMALESS; CREATE t:1;";
	let requests = vec![
		owner(
			"CREATE $t CONTENT $c",
			&[("t", table("t")), ("c", object(&[("id", record("t", 7))]))],
		),
		owner(
			"CREATE $t CONTENT $c",
			&[("t", table("t")), ("c", object(&[("id", record("t", 8))]))],
		),
		owner(
			"UPSERT $id CONTENT $c",
			&[("id", record("t", 1)), ("c", object(&[("id", record("t", 2))]))],
		),
		owner(
			"UPSERT $id CONTENT $c",
			&[("id", record("t", 1)), ("c", object(&[("id", record("t", 1))]))],
		),
		owner(
			"UPSERT $id CONTENT $c",
			&[("id", record("t", 3)), ("c", object(&[("id", record("t", 3))]))],
		),
		owner("SELECT * FROM t ORDER BY id", &[]),
	];
	same_on_and_off(setup, &requests).await;
}
