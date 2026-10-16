//! Reads of record links in windows against reads one at a time.
//!
//! Each test runs the same statements on two fresh datastores, one with the windows of
//! dereferences and `FETCH` pinned off and one pinned on, and requires the answers, errors and
//! their order to be the same: a window changes only where the bytes of a record come from. A
//! pinned-on run counts the windows it filled, and a pinned-off run fills none.

use surrealdb_cnf::ConfigMap;
use surrealdb_kvs::consts::NORMAL_BATCH_SIZE;
use surrealdb_types::ToSql;

use crate::dbs::Session;
use crate::exec::ir::phys::io::{FORCE_LINK_WINDOWS, FORCE_ROWS_AHEAD};
use crate::exec::operators::link_window::tests::{aheads, eaches, fills, reset_fills};
use crate::exec::operators::test_util::TestDb;

/// What a run answered, and how many windows it filled.
struct Run {
	/// One answer per statement of the run: the value as text, or `error: ` and the failure.
	answers: Vec<String>,
	/// The windows filled while the statements ran, and the positions they held.
	filled: (usize, usize),
	/// The rows windows filled while the statements ran, and the distinct keys they held.
	ahead: (usize, usize),
	/// The collections of links the statements fetched by overlapping reads.
	each: usize,
}

/// How a run is set up.
#[derive(Default)]
struct Setup<'a> {
	config: &'a [(&'a str, &'a str)],
	/// Run on a datastore that enforces permissions, as this session.
	record_user: Option<Session>,
	/// Let the plan read the links of a batch of rows before the rows run. Off, the collection
	/// windows are the only windows a run fills.
	rows_ahead: bool,
	/// Run on a SurrealKV datastore that keeps versions, which answers reads at a version; the
	/// in-memory one refuses them.
	#[cfg(feature = "kv-surrealkv")]
	versioned: bool,
}

/// Runs `setup` and then `sql` on a fresh datastore with the windows pinned to `windows`.
async fn run(windows: bool, how: &Setup<'_>, setup: &str, sql: &str) -> Run {
	FORCE_LINK_WINDOWS.with(|forced| forced.set(Some(windows)));
	FORCE_ROWS_AHEAD.with(|forced| forced.set(Some(windows && how.rows_ahead)));
	let mut config = ConfigMap::empty();
	for (key, value) in how.config {
		config = config.with_key_value(*key, *value);
	}
	#[cfg(feature = "kv-surrealkv")]
	let dir = temp_dir::TempDir::new().expect("a directory");
	let db = match how.record_user {
		Some(_) => TestDb::new_with_auth_and_config(setup, config).await,
		#[cfg(feature = "kv-surrealkv")]
		None if how.versioned => {
			let path = format!("surrealkv://{}?versioned=true&retention=1h", dir.path().display());
			let ds = crate::kvs::Datastore::builder()
				.without_maintenance_tasks()
				.with_capabilities(crate::dbs::Capabilities::all())
				.with_config(config)
				.build_with_path(&path)
				.await
				.expect("a versioned SurrealKV datastore");
			ds.execute(
				"DEFINE NAMESPACE test; USE NS test; DEFINE DATABASE test",
				&TestDb::owner(),
				None,
			)
			.await
			.expect("defines the database");
			let db = TestDb::over(ds);
			db.run(setup).await;
			db
		}
		None => TestDb::new_with_config(setup, config).await,
	};
	let session = how.record_user.clone().unwrap_or_else(TestDb::owner);
	reset_fills();
	let responses = db.datastore().execute(sql, &session, None).await.expect("the request runs");
	let answers = responses
		.into_iter()
		.map(|response| match response.result {
			Ok(value) => value.to_sql(),
			Err(error) => format!("error: {error}"),
		})
		.collect();
	let filled = fills();
	let ahead = aheads();
	let each = eaches();
	FORCE_LINK_WINDOWS.with(|forced| forced.set(None));
	FORCE_ROWS_AHEAD.with(|forced| forced.set(None));
	Run {
		answers,
		filled,
		ahead,
		each,
	}
}

/// Runs the statements with the windows off and on, requires the same answers, and answers the
/// run with windows. `windowed` is whether the pinned-on run must have filled a window.
async fn same(how: &Setup<'_>, setup: &str, sql: &str, windowed: bool) -> Run {
	let off = run(false, how, setup, sql).await;
	let on = run(true, how, setup, sql).await;
	assert_eq!(off.filled.0 + off.ahead.0, 0, "a pinned-off run fills no window");
	assert_eq!(on.answers, off.answers, "the answers of {sql}");
	if windowed {
		assert!(on.filled.0 + on.ahead.0 > 0, "the pinned-on run of {sql} filled a window");
	}
	on
}

/// Runs `setup` on a fresh datastore with the windows pinned to `windows`, and then each of
/// `requests` in turn on one write transaction the caller holds, so a request reads what the ones
/// before it wrote. Each request is planned with the statement it holds first, as the statements of
/// a request that follow a write are planned when they are reached.
async fn run_in_transaction(windows: bool, how: &Setup<'_>, setup: &str, requests: &[&str]) -> Run {
	FORCE_LINK_WINDOWS.with(|forced| forced.set(Some(windows)));
	FORCE_ROWS_AHEAD.with(|forced| forced.set(Some(windows && how.rows_ahead)));
	let mut config = ConfigMap::empty();
	for (key, value) in how.config {
		config = config.with_key_value(*key, *value);
	}
	let db = TestDb::new_with_config(setup, config).await;
	let txn = std::sync::Arc::new(
		db.datastore()
			.transaction(crate::kvs::TransactionType::Write)
			.await
			.expect("a transaction"),
	);
	let session = TestDb::owner();
	reset_fills();
	let mut answers = Vec::new();
	for sql in requests {
		let request = crate::kvs::request::QueryRequest::new(*sql, &session)
			.with_transaction(std::sync::Arc::clone(&txn));
		let responses = db.datastore().run(request).await.expect("the request runs");
		answers.extend(responses.into_iter().map(|response| match response.result {
			Ok(value) => value.to_sql(),
			Err(error) => format!("error: {error}"),
		}));
	}
	let filled = fills();
	let ahead = aheads();
	let each = eaches();
	txn.commit().await.expect("the transaction commits");
	FORCE_LINK_WINDOWS.with(|forced| forced.set(None));
	FORCE_ROWS_AHEAD.with(|forced| forced.set(None));
	Run {
		answers,
		filled,
		ahead,
		each,
	}
}

/// [`same`] for `requests` run on one transaction ([`run_in_transaction`]).
async fn same_in_transaction(
	how: &Setup<'_>,
	setup: &str,
	requests: &[&str],
	windowed: bool,
) -> Run {
	let off = run_in_transaction(false, how, setup, requests).await;
	let on = run_in_transaction(true, how, setup, requests).await;
	assert_eq!(off.filled.0 + off.ahead.0, 0, "a pinned-off run fills no window");
	assert_eq!(on.answers, off.answers, "the answers of {requests:?}");
	if windowed {
		assert!(on.filled.0 + on.ahead.0 > 0, "the pinned-on run of {requests:?} filled a window");
	}
	on
}

/// `n` `person` records, `person:0` to `person:n-1`, each holding its number.
fn people(n: usize) -> String {
	format!(
		"FOR $i IN 0..{n} {{ CREATE type::record('person', $i) SET n = $i, name = <string>$i; }};"
	)
}

/// One record per window: a window reads at least one record, and the byte budget bounds the rest.
const SMALL_WINDOWS: &[(&str, &str)] = &[("scan_batch_bytes", "1KiB")];

#[tokio::test]
async fn a_fetch_of_many_links_reads_them_in_windows_and_keeps_their_order() {
	let setup = format!(
		"{} CREATE holder:1 SET refs = array::reverse((SELECT VALUE id FROM person));",
		people(1200)
	);
	// The default budget reaches the row cap of a window within the 1 200 links.
	let on = same(&Setup::default(), &setup, "SELECT * FROM holder FETCH refs;", true).await;
	assert_eq!(on.filled.1, 1200, "every link is a position of a window");
	assert!(on.filled.0 >= 5, "the windows grow from a probe: {:?}", on.filled);
	assert!(on.answers[0].contains("n: 1199"), "the first link is the last person");
	// A budget of one record's bytes reads a window of one link, then grows.
	let small = Setup {
		config: SMALL_WINDOWS,
		..Setup::default()
	};
	let on = same(&small, &setup, "SELECT * FROM holder FETCH refs;", true).await;
	assert!(on.filled.0 > 20, "many small windows: {:?}", on.filled);
}

#[tokio::test]
async fn a_missing_link_reads_none_and_an_undefined_table_fails_at_its_position() {
	let setup = "CREATE person:1 SET n = 1; CREATE person:2 SET n = 2; CREATE person:4 SET n = 4;
		DEFINE TABLE other SCHEMALESS;
		CREATE holder:1 SET refs = [person:1, person:3, person:2, person:9, person:4];
		CREATE holder:2 SET refs = [person:1, person:2, nosuchtable:1, person:4];
		CREATE holder:3 SET refs = [person:1, person:2, other:7];";
	let how = Setup::default();
	let on = same(&how, setup, "SELECT * FROM holder:1 FETCH refs;", true).await;
	assert!(on.answers[0].contains("NONE"), "{}", on.answers[0]);
	let on = same(&how, setup, "SELECT * FROM holder:2 FETCH refs;", true).await;
	assert!(on.answers[0].starts_with("error:"), "an undefined table fails the fetch");
	same(&how, setup, "SELECT * FROM holder:3 FETCH refs;", true).await;
	same(&how, setup, "SELECT refs.* FROM holder:2;", true).await;
	same(&how, setup, "SELECT refs.n FROM holder:2;", true).await;
}

#[tokio::test]
async fn repeated_and_mixed_elements_read_as_they_are_read_one_at_a_time() {
	let setup = "CREATE person:1 SET n = 1; CREATE person:2 SET n = 2; CREATE person:3 SET n = 3;
		CREATE holder:1 SET refs = [person:1, 'text', person:1, [person:2, person:3, person:2], \
			{ a: person:2 }, 5, person:2, NONE, person:3];
		CREATE holder:2 SET refs = <set>[person:1, person:2, person:3, 4];
		CREATE holder:3 SET obj = { a: person:1, b: person:2 }, refs = [person:1, person:2];";
	let how = Setup::default();
	same(&how, setup, "SELECT * FROM holder:1 FETCH refs;", true).await;
	same(&how, setup, "SELECT * FROM holder:1 FETCH refs.*;", true).await;
	same(&how, setup, "SELECT refs.* FROM holder:1;", true).await;
	same(&how, setup, "SELECT refs.* FROM holder:2;", true).await;
	same(&how, setup, "SELECT refs.n FROM holder:2;", true).await;
	same(&how, setup, "SELECT * FROM holder:3 FETCH obj, refs;", true).await;
	same(&how, setup, "SELECT obj.*, refs.n FROM holder:3;", true).await;
	// Below two links there is nothing to read at once.
	let setup = "CREATE person:1 SET n = 1; CREATE holder:1 SET refs = [person:1, 5];";
	same(&how, setup, "SELECT * FROM holder:1 FETCH refs;", false).await;
	same(&how, setup, "SELECT refs.n FROM holder:1;", false).await;
}

#[tokio::test]
async fn a_hidden_target_reads_as_the_caller_may_see_it() {
	let setup = "DEFINE TABLE person SCHEMALESS PERMISSIONS FOR select WHERE n > 1;
		DEFINE FIELD secret ON person PERMISSIONS FOR select WHERE n > 2;
		DEFINE TABLE walled SCHEMALESS PERMISSIONS FOR select NONE;
		DEFINE TABLE holder SCHEMALESS PERMISSIONS FULL;
		CREATE person:1 SET n = 1, secret = 'a'; CREATE person:2 SET n = 2, secret = 'b';
		CREATE person:3 SET n = 3, secret = 'c'; CREATE walled:1 SET n = 9;
		CREATE holder:1 SET refs = [person:1, person:2, person:3, walled:1, person:3];";
	let how = Setup {
		config: SMALL_WINDOWS,
		record_user: Some(Session::for_record(
			"test",
			"test",
			"user",
			crate::types::PublicValue::String("user:tobie".to_owned()),
		)),
		..Setup::default()
	};
	let on = same(&how, setup, "SELECT * FROM holder FETCH refs;", true).await;
	assert!(!on.answers[0].contains("walled"), "{}", on.answers[0]);
	same(&how, setup, "SELECT refs.* FROM holder;", true).await;
	same(&how, setup, "SELECT refs.secret FROM holder;", true).await;
}

#[tokio::test]
async fn computed_fields_of_the_targets_are_derived_at_their_positions() {
	let setup = "DEFINE TABLE person SCHEMALESS;
		DEFINE FIELD double ON person COMPUTED n * 2;
		DEFINE TABLE node SCHEMALESS;
		DEFINE FIELD kin_n ON node COMPUTED kin.n;
		CREATE person:1 SET n = 1; CREATE person:2 SET n = 2; CREATE person:3 SET n = 3;
		CREATE node:1 SET n = 10, kin = [node:1, node:2, person:1];
		CREATE node:2 SET n = 20, kin = [];
		CREATE holder:1 SET refs = [person:1, person:2, person:3, person:1];";
	let how = Setup::default();
	same(&how, setup, "SELECT * FROM holder FETCH refs;", true).await;
	same(&how, setup, "SELECT refs.double FROM holder;", true).await;
	same(&how, setup, "SELECT refs.* FROM holder;", true).await;
	// A computed field that reads its own record through a link gets the row as it is computed,
	// and the window leaves that link out.
	same(&how, setup, "SELECT kin_n FROM node:1;", false).await;
	same(&how, setup, "SELECT * FROM node;", false).await;
}

#[tokio::test]
async fn lightweight_relation_records_read_through_their_adjacency() {
	let setup = "DEFINE TABLE person SCHEMALESS;
		DEFINE TABLE knows TYPE RELATION IN person OUT person LIGHTWEIGHT;
		CREATE person:1, person:2, person:3;
		LET $a = (RELATE person:1->knows->person:2 RETURN VALUE id);
		LET $b = (RELATE person:2->knows->person:3 RETURN VALUE id);
		CREATE holder:1 SET refs = [$a[0], person:1, $b[0], person:2];";
	same(&Setup::default(), setup, "SELECT * FROM holder FETCH refs;", true).await;
}

#[tokio::test]
async fn the_first_failing_position_fails_the_fetch_as_it_does_one_at_a_time() {
	// A computed field that throws, and a stored record that does not decode. With at most three
	// links the reads are one at a time without windows, so the order of the errors is fixed.
	let setup = "DEFINE TABLE throws SCHEMALESS;
		DEFINE FIELD boom ON throws COMPUTED { THROW 'computed'; };
		CREATE throws:1 RETURN NONE; CREATE person:1 SET n = 1; CREATE person:2 SET n = 2;
		CREATE holder:1 SET refs = [person:1, throws:1, person:2];";
	same(&Setup::default(), setup, "SELECT * FROM holder FETCH refs;", true).await;
	let setup = "DEFINE TABLE throws SCHEMALESS;
		DEFINE FIELD boom ON throws COMPUTED { THROW 'computed'; };
		CREATE throws:1 RETURN NONE; CREATE person:1 SET n = 1;
		CREATE holder:1 SET refs = [throws:1, person:1, person:1];";
	let on = same(&Setup::default(), setup, "SELECT * FROM holder FETCH refs;", true).await;
	assert!(on.answers[0].contains("computed"), "{}", on.answers[0]);
}

#[tokio::test]
async fn a_stored_record_that_does_not_decode_fails_with_the_context_of_its_own_read() {
	let setup = "CREATE person:1 SET n = 1; CREATE person:2 SET n = 2; CREATE person:3 SET n = 3;
		CREATE holder:1 SET refs = [person:1, person:2, person:3];";
	for windows in [false, true] {
		FORCE_LINK_WINDOWS.with(|forced| forced.set(Some(windows)));
		FORCE_ROWS_AHEAD.with(|forced| forced.set(Some(false)));
		let db = TestDb::new_with_config(setup, ConfigMap::empty()).await;
		corrupt(&db, "person", 1).await;
		reset_fills();
		let answers = db
			.datastore()
			.execute("SELECT * FROM holder FETCH refs;", &TestDb::owner(), None)
			.await
			.expect("the request runs");
		FORCE_LINK_WINDOWS.with(|forced| forced.set(None));
		FORCE_ROWS_AHEAD.with(|forced| forced.set(None));
		let error = answers[0].result.as_ref().expect_err("the corrupt record fails the fetch");
		let text = format!("{error:#}");
		assert!(text.contains("Failed to fetch record"), "windows {windows}: {text}");
		assert_eq!(fills().0 > 0, windows, "windows {windows}");
	}
}

/// Overwrites the stored bytes of the `nth` record of `table`, in key order, with bytes no record
/// decodes from.
async fn corrupt(db: &TestDb, table: &str, nth: usize) {
	let mut needle = table.as_bytes().to_vec();
	needle.extend_from_slice(&[0, b'*']);
	let write = db
		.datastore()
		.transaction(crate::kvs::TransactionType::Write)
		.await
		.expect("a write transaction opens");
	let all = write
		.getr_raw(surrealdb_kvs::key::RawRange::every_key(), None)
		.await
		.expect("the keyspace reads");
	let (key, _) = all
		.into_iter()
		.filter(|(key, _)| key.windows(needle.len()).any(|w| w == needle.as_slice()))
		.nth(nth)
		.expect("the record is stored");
	write
		.set(surrealdb_kvs::Key::from(key), vec![0xFF, 0x7F, 0x01, 0x02, 0x03])
		.await
		.expect("set");
	write.commit().await.expect("the corruption commits");
}

#[tokio::test]
async fn a_pipeline_that_reads_at_a_version_reads_no_window() {
	let setup = "CREATE person:1 SET n = 1; CREATE person:2 SET n = 2; CREATE person:3 SET n = 3;
		CREATE holder:1 SET refs = [person:1, person:2, person:3];";
	let how = Setup::default();
	let off =
		run(false, &how, setup, "SELECT * FROM holder FETCH refs VERSION d'2999-01-01T00:00:00Z';")
			.await;
	let on =
		run(true, &how, setup, "SELECT * FROM holder FETCH refs VERSION d'2999-01-01T00:00:00Z';")
			.await;
	assert_eq!(on.answers, off.answers);
	assert_eq!(on.filled.0, 0, "a versioned read fills no window");
}

#[tokio::test]
async fn a_function_body_called_under_a_version_reads_its_links_at_that_version() {
	// The body is compiled as a unit of its own and runs under the `VERSION` of the call that
	// enters it, which no op of its plan names: its links are read one at a time, at the version.
	let setup = "CREATE person:1 SET n = 1; CREATE person:2 SET n = 2; CREATE person:3 SET n = 3;
		CREATE holder:1 SET refs = [person:1, person:2, person:3];
		DEFINE FUNCTION fn::f() { RETURN SELECT * FROM holder FETCH refs; };
		DEFINE FUNCTION fn::g() { RETURN SELECT VALUE refs.n FROM holder; };";
	let how = Setup::default();
	for sql in [
		"SELECT VALUE fn::f() FROM holder VERSION d'2999-01-01T00:00:00Z';",
		"SELECT VALUE fn::g() FROM holder VERSION d'2999-01-01T00:00:00Z';",
		"RETURN fn::f(); RETURN fn::g();",
	] {
		let off = run(false, &how, setup, sql).await;
		let on = run(true, &how, setup, sql).await;
		assert_eq!(on.answers, off.answers, "{sql}");
		assert_eq!(on.filled.0 + on.ahead.0, 0, "{sql}: a unit of its own fills no window");
	}
}

/// The events a write fires, and the read unit of a table with a computed field, are planned
/// apart from the statement that reaches them. Run in the frame of a request's own statement
/// they read no window: the region guard of `runs_in_request_frame` refuses them.
const UNITS_SETUP: &str =
	"CREATE person:1 SET n = 1; CREATE person:2 SET n = 2; CREATE person:3 SET n = 3;
		CREATE holder:1 SET refs = [person:1, person:2, person:3];
		DEFINE TABLE log SCHEMALESS;
		DEFINE EVENT seen ON TABLE log WHEN $event = 'CREATE' THEN {
			SELECT * FROM holder FETCH refs;
		};
		DEFINE TABLE host SCHEMALESS;
		DEFINE FIELD c ON host COMPUTED (SELECT * FROM holder FETCH refs);
		CREATE host:1;
		DEFINE FUNCTION fn::mk() { CREATE log:1; RETURN 1; };
		DEFINE FUNCTION fn::read() { RETURN SELECT c FROM host; };";

#[tokio::test]
async fn units_planned_apart_from_their_caller_read_no_window() {
	let how = Setup::default();
	for sql in ["CREATE log:2;", "RETURN fn::mk();", "SELECT c FROM host;"] {
		let off = run(false, &how, UNITS_SETUP, sql).await;
		let on = run(true, &how, UNITS_SETUP, sql).await;
		assert_eq!(on.answers, off.answers, "{sql}");
		assert_eq!(on.filled.0 + on.ahead.0, 0, "{sql}: a unit planned apart fills no window");
	}
}

/// The same units reached through a function called under the `VERSION` of a statement that no op
/// of their plan names. A write and a computed field read at a version are not reachable from the
/// statement itself, so a function body is the way in; the datastore answers reads at a version,
/// so each answer is a value.
#[cfg(feature = "kv-surrealkv")]
#[tokio::test]
async fn units_planned_apart_from_their_caller_read_no_window_under_its_version() {
	let how = Setup {
		versioned: true,
		..Setup::default()
	};
	for sql in [
		"SLEEP 50ms; LET $ts = time::now(); SELECT VALUE fn::mk() FROM holder VERSION $ts;",
		"SLEEP 50ms; LET $ts = time::now(); SELECT VALUE (SELECT VALUE fn::mk() FROM holder) FROM holder VERSION $ts;",
		"SLEEP 50ms; LET $ts = time::now(); SELECT VALUE fn::read() FROM holder VERSION $ts;",
	] {
		let off = run(false, &how, UNITS_SETUP, sql).await;
		let on = run(true, &how, UNITS_SETUP, sql).await;
		assert_eq!(on.answers, off.answers, "{sql}");
		assert!(
			on.answers.iter().all(|answer| !answer.starts_with("error: ")),
			"{sql}: {:?}",
			on.answers
		);
		assert_eq!(on.filled.0 + on.ahead.0, 0, "{sql}: a unit planned apart fills no window");
	}
}

#[tokio::test]
async fn a_collection_of_links_of_a_pipeline_without_windows_is_fetched_by_overlapping_reads() {
	let setup = "CREATE person:1 SET n = 1; CREATE person:2 SET n = 2; CREATE person:3 SET n = 3;
		CREATE holder:1 SET refs = [person:1, person:2, person:3];";
	let how = Setup::default();
	let sql = "SELECT * FROM holder FETCH refs;";
	let off = run(false, &how, setup, sql).await;
	let on = run(true, &how, setup, sql).await;
	assert_eq!(on.answers, off.answers);
	assert_eq!(off.each, 1, "without windows the collection is fetched as a whole");
	assert_eq!(on.each, 0, "with windows no collection is");
	assert!(on.filled.0 > 0);
}

#[tokio::test]
async fn a_transaction_reads_its_own_writes_through_a_window() {
	let setup = "DEFINE TABLE holder SCHEMALESS;";
	let requests = [
		"CREATE person:1 SET n = 1; CREATE person:2 SET n = 2; CREATE person:3 SET n = 3;",
		"CREATE holder:1 SET refs = [person:1, person:2, person:3];",
		"UPDATE person:2 SET n = 20;",
		"SELECT * FROM holder FETCH refs;",
		"SELECT refs.n FROM holder;",
	];
	let on = same_in_transaction(&Setup::default(), setup, &requests, true).await;
	assert!(on.answers.iter().any(|a| a.contains("n: 20")), "{:?}", on.answers);
	assert!(on.filled.0 >= 2, "both reads fill a window: {:?}", on.filled);
}

/// People stored with a string `age`, then a step that migrates `age` to an int, which no stored
/// row has passed: every read brings a row through the step.
const MIGRATED: &str = "
	DEFINE TABLE person SCHEMAFULL;
	DEFINE FIELD name ON person TYPE string;
	DEFINE FIELD age ON person TYPE string;
	CREATE person:1 SET name = 'Ann', age = '41';
	CREATE person:2 SET name = 'Bob', age = '37';
	CREATE person:3 SET name = 'Cy', age = 'old';
	DEFINE TABLE holder SCHEMALESS;
	CREATE holder:1 SET refs = [person:3, person:1, person:2], author = person:1;
	CREATE holder:2 SET refs = [person:2, person:3], author = person:2;
	ALTER FIELD age ON person TYPE int MIGRATE EXISTING ROWS AS age_to_int USING <int>$value FALLBACK 0;
";

#[tokio::test]
async fn a_window_brings_the_records_it_holds_through_the_migration_steps_they_have_not_passed() {
	let on = same(&Setup::default(), MIGRATED, "SELECT * FROM holder FETCH refs;", true).await;
	assert!(on.answers[0].contains("age: 0"), "{:?}", on.answers);
	assert!(on.answers[0].contains("age: 41"), "{:?}", on.answers);
	assert!(!on.answers[0].contains("'41'"), "{:?}", on.answers);
	let on = same(&Setup::default(), MIGRATED, "SELECT VALUE refs.age FROM holder;", true).await;
	assert_eq!(on.answers, vec!["[[0, 41, 37], [37, 0]]".to_owned()]);
	let ahead = Setup {
		rows_ahead: true,
		..Setup::default()
	};
	let on = same(&ahead, MIGRATED, "SELECT * FROM holder FETCH author;", false).await;
	assert!(on.ahead.0 > 0, "the rows window was filled: {:?}", on.ahead);
	assert!(
		on.answers[0].contains("age: 41") && on.answers[0].contains("age: 37"),
		"{:?}",
		on.answers
	);
}

#[tokio::test]
async fn a_window_brings_a_cached_record_through_the_steps_its_stored_bytes_have_not_passed() {
	let requests = [
		"SELECT * FROM person:1; SELECT * FROM person:3;",
		"SELECT * FROM holder FETCH refs;",
		"SELECT VALUE refs.age FROM holder;",
	];
	let on = same_in_transaction(&Setup::default(), MIGRATED, &requests, true).await;
	assert_eq!(on.answers[3], "[[0, 41, 37], [37, 0]]", "{:?}", on.answers);
	assert!(
		!on.answers[2].contains("'41'") && !on.answers[2].contains("'old'"),
		"{:?}",
		on.answers
	);
}

#[tokio::test]
async fn a_dereference_in_a_function_body_is_never_windowed() {
	let setup = "CREATE person:1 SET n = 1; CREATE person:2 SET n = 2; CREATE person:3 SET n = 3;
		DEFINE FUNCTION fn::all($refs: array) { RETURN $refs.*; };
		DEFINE FUNCTION fn::names($refs: array) { RETURN $refs.n; };";
	let sql = "RETURN fn::all([person:1, person:2, person:3]);
		RETURN fn::names([person:1, person:2, person:3]);";
	let on = same(&Setup::default(), setup, sql, false).await;
	assert_eq!(on.filled.0, 0, "no phys decision reaches a function body: {:?}", on.filled);
	assert!(on.answers[0].contains("n: 3"), "{:?}", on.answers);
}

#[tokio::test]
async fn explain_analyze_of_a_windowed_fetch_shows_what_the_unwindowed_run_shows() {
	let setup = format!(
		"{} CREATE holder:1 SET refs = array::reverse((SELECT VALUE id FROM person));",
		people(100)
	);
	let mask = |answer: &str| -> String {
		// Timings of a run differ from run to run; every other counter must not.
		let mut out = String::new();
		let mut chars = answer.chars().peekable();
		while let Some(c) = chars.next() {
			out.push(c);
			if out.ends_with("elapsed: ") || out.ends_with("time: ") {
				while chars.peek().is_some_and(|c| !matches!(c, ',' | '}' | ' ' | ']')) {
					chars.next();
				}
				out.push('#');
			}
		}
		out
	};
	// A collection read in windows, and the links of a batch read before its rows.
	for (setup, sql, rows_ahead) in [
		(setup.clone(), "EXPLAIN ANALYZE SELECT * FROM holder FETCH refs;", false),
		(comments(80), "EXPLAIN ANALYZE SELECT * FROM comment FETCH author, friends;", true),
	] {
		let how = Setup {
			rows_ahead,
			..Setup::default()
		};
		let off = run(false, &how, &setup, sql).await;
		let on = run(true, &how, &setup, sql).await;
		assert!(on.filled.0 + on.ahead.0 > 0, "the analysed fetch fills a window");
		assert_eq!(
			on.answers.iter().map(|a| mask(a)).collect::<Vec<_>>(),
			off.answers.iter().map(|a| mask(a)).collect::<Vec<_>>(),
			"{:?}",
			on.answers
		);
	}
}

/// `n` comments in key order, each linking its author and two friends among 300 users, some of whom
/// do not exist or are not given.
fn comments(n: usize) -> String {
	format!(
		"FOR $i IN 0..300 {{ CREATE type::record('user', $i) SET name = <string>$i; }};
		 FOR $i IN 0..{n} {{
			CREATE type::record('comment', $i) SET text = <string>$i,
				author = IF $i % 11 = 0 {{ NONE }} ELSE IF $i % 13 = 0 {{ type::record('user', 9999) }} ELSE {{ type::record('user', $i % 300) }},
				friends = [type::record('user', $i % 300), type::record('user', ($i + 1) % 300)];
		 }};"
	)
}

/// One task runs every row of a statement, in order.
const SERIAL: &[(&str, &str)] = &[("morsel_split_rows", "18446744073709551615")];
/// Morsel knobs that split a statement's rows into batches of a few rows where the policy places
/// the statement as morsels; a statement it keeps serial runs as it does without them.
const MORSELS: &[(&str, &str)] =
	&[("morsel_split_rows", "1"), ("morsel_target_bytes", "4000"), ("morsel_oltp_reserve", "0")];

#[tokio::test]
async fn a_fetch_of_one_link_per_row_reads_the_links_of_each_batch_before_its_rows() {
	let setup = comments(1200);
	for config in [SERIAL, MORSELS] {
		let how = Setup {
			config,
			rows_ahead: true,
			..Setup::default()
		};
		for (sql, filled) in [
			("SELECT * FROM comment FETCH author;", true),
			("SELECT * FROM comment FETCH author, friends;", true),
			("SELECT * FROM comment FETCH friends.*;", true),
			("SELECT * FROM comment LIMIT 700 START 100 FETCH author;", true),
			("SELECT * FROM comment LIMIT 5 FETCH author;", true),
			("SELECT * FROM comment LIMIT 0 FETCH author;", false),
			("SELECT * FROM comment START 1100 FETCH author;", true),
			("SELECT * FROM comment START 5000 FETCH author;", false),
		] {
			let on = same(&how, &setup, sql, filled).await;
			if filled {
				assert!(on.ahead.0 > 0, "{sql}: no rows window was filled");
			}
		}
	}
}

/// What a read transaction counts running `sql` on a datastore `setup` built, with the windows
/// pinned to `windows`, on a statement kept to one task.
async fn reads_of(
	setup: &str,
	windows: bool,
	sql: &str,
) -> surrealdb_observe::TransactionMetricsSnapshot {
	FORCE_LINK_WINDOWS.with(|forced| forced.set(Some(windows)));
	FORCE_ROWS_AHEAD.with(|forced| forced.set(Some(windows)));
	let db = TestDb::new_with_config(
		setup,
		ConfigMap::empty().with_key_value("morsel_split_rows", "18446744073709551615"),
	)
	.await;
	let txn = std::sync::Arc::new(
		db.datastore().transaction(crate::kvs::TransactionType::Read).await.expect("a transaction"),
	);
	let session = TestDb::owner();
	let request = crate::kvs::request::QueryRequest::new(sql, &session)
		.with_transaction(std::sync::Arc::clone(&txn));
	db.datastore().run(request).await.expect("the request runs");
	FORCE_LINK_WINDOWS.with(|forced| forced.set(None));
	FORCE_ROWS_AHEAD.with(|forced| forced.set(None));
	let reads = txn.metrics_snapshot_for_test();
	txn.cancel().await.expect("the transaction ends");
	reads
}

/// The key reads a read transaction counts running `sql` over `rows` comments that each link a
/// user, with the windows pinned to `windows`, on a statement kept to one task.
async fn gets_of(rows: usize, windows: bool, sql: &'static str) -> u32 {
	let setup = format!(
		"FOR $i IN 0..{rows} {{ CREATE type::record('user', $i) SET name = <string>$i RETURN NONE; }};
		 FOR $i IN 0..{rows} {{ CREATE type::record('comment', $i) SET author = type::record('user', $i) RETURN NONE; }};"
	);
	reads_of(&setup, windows, sql).await.ops_get
}

#[tokio::test]
async fn the_links_of_a_batch_are_read_by_a_get_for_the_batch_and_not_for_a_link() {
	const FETCH: &str = "SELECT * FROM comment FETCH author;";
	let batch = NORMAL_BATCH_SIZE as usize;
	let rows = 20 * batch;
	let scan = gets_of(rows, true, "SELECT * FROM comment;").await;
	let without = gets_of(rows, false, FETCH).await - scan;
	let with = gets_of(rows, true, FETCH).await - scan;
	assert!(without >= rows as u32, "without windows a link is a read: {without} reads");
	// Every key of a batch is read ahead of its rows, a window at a time. A window grows from its
	// probe of 32 keys by doubling at each fill until it holds a batch, and a batch is then one
	// read, or two where the scan's own batches do not line up with the windows.
	let growth = batch.next_power_of_two().ilog2() as usize - 4;
	let bound = (3 * (rows / batch) + growth) as u32;
	assert!(
		with <= bound,
		"the links of a batch are one read: {with} reads for {} batches, against {without} for a \
		 read a link",
		rows / batch
	);
	assert!(with * 4 < without, "{with} reads against {without}");
}

#[tokio::test]
async fn a_batch_of_links_is_read_from_the_transactions_own_writes() {
	let setup = "DEFINE TABLE user SCHEMALESS; DEFINE TABLE comment SCHEMALESS;";
	let requests = [
		"FOR $i IN 0..60 { CREATE type::record('user', $i) SET name = <string>$i RETURN NONE; };",
		"FOR $i IN 0..60 { CREATE type::record('comment', $i) SET author = type::record('user', $i) RETURN NONE; };",
		"UPDATE user:7 SET name = 'seven';",
		"SELECT * FROM comment FETCH author;",
		"DELETE user:8;",
		"SELECT * FROM comment WHERE id = comment:8 FETCH author;",
		"SELECT * FROM comment FETCH author;",
	];
	let how = Setup {
		rows_ahead: true,
		..Setup::default()
	};
	let on = same_in_transaction(&how, setup, &requests, true).await;
	assert!(on.answers.iter().any(|answer| answer.contains("seven")), "{:?}", on.answers);
	assert!(on.ahead.0 > 0);
}

#[tokio::test]
async fn rows_ahead_is_a_decision_of_its_own_beside_the_collection_windows() {
	let setup = format!("{} CREATE holder:1 SET refs = [user:1, user:2, user:3];", comments(40));
	let off =
		run(true, &Setup::default(), &setup, "SELECT * FROM comment FETCH author, friends;").await;
	let how = Setup {
		rows_ahead: true,
		..Setup::default()
	};
	let on = run(true, &how, &setup, "SELECT * FROM comment FETCH author, friends;").await;
	assert_eq!(off.answers, on.answers);
	assert_eq!(off.ahead.0, 0, "no rows window");
	assert!(on.ahead.0 > 0, "a rows window");
}

#[tokio::test]
async fn a_computed_field_that_writes_is_refused_at_the_position_of_its_record() {
	// The body of the function is stored apart from the field that calls it, so it can be
	// redefined to write once the field is defined.
	let setup = "DEFINE FUNCTION fn::mark() { RETURN 1; };
		DEFINE TABLE person SCHEMALESS;
		DEFINE TABLE marked SCHEMALESS;
		DEFINE FIELD flag ON marked COMPUTED fn::mark();
		DEFINE FUNCTION OVERWRITE fn::mark() { CREATE audit SET at = time::now(); RETURN 1; };
		CREATE person:1 SET n = 1; CREATE person:2 SET n = 2;
		CREATE marked:1 RETURN NONE;
		CREATE holder:1 SET refs = [person:1, marked:1, person:2];
		CREATE holder:2 SET refs = [person:1, person:2];";
	let on = same(&Setup::default(), setup, "SELECT * FROM holder:1 FETCH refs;", true).await;
	assert!(on.answers[0].starts_with("error:"), "{:?}", on.answers);
	let on = same(&Setup::default(), setup, "SELECT refs.flag FROM holder:1;", true).await;
	assert!(on.answers[0].starts_with("error:"), "{:?}", on.answers);
	// Nothing was written by either run: the table the function would have created is not defined.
	let on = same(
		&Setup::default(),
		setup,
		"SELECT * FROM holder:2 FETCH refs; SELECT * FROM audit;",
		true,
	)
	.await;
	assert!(on.answers[1].contains("'audit' does not exist"), "{:?}", on.answers);
}

#[tokio::test]
async fn a_throw_at_one_position_and_a_record_that_does_not_decode_at_another_fail_in_position_order()
 {
	// At most three links, so the reads one at a time are sequential and their first error is the
	// lowest position's.
	let setup = "DEFINE TABLE throws SCHEMALESS;
		DEFINE FIELD boom ON throws COMPUTED { THROW 'computed'; };
		CREATE throws:1 RETURN NONE; CREATE person:1 SET n = 1; CREATE person:2 SET n = 2;
		CREATE person:3 SET n = 3;
		CREATE holder:1 SET refs = [person:1, throws:1, person:3];
		CREATE holder:2 SET refs = [person:1, person:3, throws:1];";
	for (holder, first) in [("holder:1", "computed"), ("holder:2", "Failed to fetch record")] {
		let mut texts = Vec::new();
		for windows in [false, true] {
			FORCE_LINK_WINDOWS.with(|forced| forced.set(Some(windows)));
			FORCE_ROWS_AHEAD.with(|forced| forced.set(Some(false)));
			let db = TestDb::new_with_config(setup, ConfigMap::empty()).await;
			// person:3 is the third record of its table in key order.
			corrupt(&db, "person", 2).await;
			let answers = db
				.datastore()
				.execute(&format!("SELECT * FROM {holder} FETCH refs;"), &TestDb::owner(), None)
				.await
				.expect("the request runs");
			FORCE_LINK_WINDOWS.with(|forced| forced.set(None));
			FORCE_ROWS_AHEAD.with(|forced| forced.set(None));
			let error = answers[0].result.as_ref().expect_err("a position fails the fetch");
			texts.push(format!("{error:#}"));
		}
		assert_eq!(texts[0], texts[1], "{holder}");
		assert!(texts[1].contains(first), "{holder}: {}", texts[1]);
	}
}

#[tokio::test]
async fn the_links_of_a_batch_are_not_read_by_a_fetch_that_runs_after_a_write() {
	let setup = "DEFINE TABLE user SCHEMALESS; DEFINE TABLE comment SCHEMALESS;
		CREATE user:0 SET name = 'x'; CREATE user:1 SET name = 'y';
		CREATE comment:0 SET author = user:0, text = 'first'; CREATE comment:1 SET author = user:1;";
	// The first read stops after one row, which leaves its batch's links behind; a write follows,
	// and the next reads of the same record must see it.
	let requests = [
		"SELECT * FROM comment LIMIT 1 FETCH author;",
		"UPDATE user:0 SET name = 'changed';",
		"SELECT * FROM [comment:0] FETCH author;",
		"SELECT * FROM comment LIMIT 1 FETCH author;",
	];
	let how = Setup {
		rows_ahead: true,
		..Setup::default()
	};
	let on = same_in_transaction(&how, setup, &requests, true).await;
	assert!(on.answers[0].contains("name: 'x'"), "{:?}", on.answers);
	assert!(on.answers[2].contains("name: 'changed'"), "{:?}", on.answers);
	assert!(on.answers[3].contains("name: 'changed'"), "{:?}", on.answers);
	assert!(on.ahead.0 >= 2, "the scans read their links ahead: {:?}", on.ahead);
}

#[tokio::test]
async fn a_batch_whose_links_pass_one_window_is_read_a_window_at_a_time() {
	// Each comment links four users no other comment links, so a window of `NORMAL_BATCH_SIZE`
	// keys holds the links of a quarter of a batch of rows.
	let rows = 2 * NORMAL_BATCH_SIZE as usize;
	let setup = format!(
		"FOR $i IN 0..{users} {{ CREATE type::record('user', $i) SET name = <string>$i RETURN NONE; }};
		 FOR $i IN 0..{rows} {{ CREATE type::record('comment', $i) SET author = type::record('user', $i * 4),
			friends = [type::record('user', $i * 4 + 1), type::record('user', $i * 4 + 2), type::record('user', $i * 4 + 3)]
			RETURN NONE; }};",
		users = 4 * rows
	);
	const FETCH: &str = "SELECT * FROM comment FETCH author, friends;";
	let scan = reads_of(&setup, true, "SELECT * FROM comment;").await.ops_get;
	let without = reads_of(&setup, false, FETCH).await.ops_get - scan;
	let with = reads_of(&setup, true, FETCH).await.ops_get - scan;
	assert!(without >= rows as u32, "without windows a row reads for itself: {without} reads");
	// Every key is read ahead of its row, a window at a time: the reads are the windows, which
	// grow from their probe to the most keys a window holds.
	let keys = 4 * rows;
	let bound = (keys / NORMAL_BATCH_SIZE as usize + 16) as u32;
	assert!(with <= bound, "{with} reads for {keys} keys, against {without} without windows");
}

#[tokio::test]
async fn a_record_a_window_decodes_serves_the_next_dereference_from_the_cache() {
	let setup = format!(
		"{} FOR $i IN 0..40 {{ CREATE type::record('holder', $i) SET refs = [type::record('person', $i), type::record('person', $i + 1), type::record('person', $i + 2)] RETURN NONE; }};",
		people(50)
	);
	const SQL: &str = "SELECT refs.n, refs.name, refs.id FROM holder;";
	let scan = reads_of(&setup, true, "SELECT * FROM holder;").await;
	let without = reads_of(&setup, false, SQL).await;
	let with = reads_of(&setup, true, SQL).await;
	let read =
		|reads: &surrealdb_observe::TransactionMetricsSnapshot| reads.keys_read - scan.keys_read;
	// Each person is read once from storage whichever projection reads it first; the others take
	// it from the transaction's cache.
	assert_eq!(read(&with), read(&without), "with {with:?}, without {without:?}");
	assert!(read(&with) <= 50, "{with:?}");
	same(&Setup::default(), &setup, SQL, true).await;
}
