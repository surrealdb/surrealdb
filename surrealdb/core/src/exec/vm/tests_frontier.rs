//! Graph lookups that read the sources of a hop in windows against lookups that read them one at
//! a time.
//!
//! Each test runs the same statements on two fresh datastores, one with the hop windows pinned off
//! and one pinned on, and requires the answers, errors and their order to be the same: a window
//! changes only where the bytes of an adjacency come from. A pinned-on run counts the windows it
//! filled and the reads they served, and a pinned-off run fills none.

use surrealdb_cnf::ConfigMap;
use surrealdb_types::ToSql;

use super::graph::frontier::counters;
use crate::dbs::Session;
use crate::exec::config::ExecConfig;
use crate::exec::ir::phys::frontier::{FORCE_FRONTIER_HOPS, FORCE_FRONTIER_ROWS};
use crate::exec::operators::test_util::TestDb;

/// What a run answered, and what its windows did.
struct Run {
	/// One answer per statement of the run: the value as text, or `error: ` and the failure.
	answers: Vec<String>,
	/// The windows filled while the statements ran, and the ranges and probes they held.
	filled: (usize, usize),
	/// The ranges and probes the reads took from a window.
	served: (usize, usize),
	/// The batches of rows whose window held something.
	rows: usize,
	/// The levels of a path the rows windows were filled for.
	levels: usize,
	/// The lookups that ended with a hop window of their own still held.
	left: usize,
	/// The scans that ended with the window of their batch of rows still held.
	ended_holding: usize,
	/// The most ranges one group of a fill built.
	peak_group: usize,
	/// The key ranges the positions built for themselves.
	built: usize,
	/// The most keys one multi-range read of a fill visited.
	call_keys: usize,
	/// The most ranges one multi-range read of a fill sought.
	call_ranges: usize,
	/// The batches the statement ran through the morsel host, in the order they began: the rows
	/// the batch's paging skips first, where it is paged, and the rows it holds.
	#[cfg_attr(not(feature = "kv-surrealkv"), allow(dead_code))]
	batches: Vec<(Option<usize>, usize)>,
}

/// Which windows a run is pinned to.
#[derive(Clone, Copy)]
struct Pins {
	/// The windows of the sources of a hop.
	hops: bool,
	/// The window of a batch of rows.
	rows: bool,
}

impl Pins {
	const OFF: Pins = Pins {
		hops: false,
		rows: false,
	};
	const ALL: Pins = Pins {
		hops: true,
		rows: true,
	};
	const HOPS: Pins = Pins {
		hops: true,
		rows: false,
	};
	const ROWS: Pins = Pins {
		hops: false,
		rows: true,
	};

	fn set(self) {
		FORCE_FRONTIER_HOPS.with(|forced| forced.set(Some(self.hops)));
		FORCE_FRONTIER_ROWS.with(|forced| forced.set(Some(self.rows)));
	}

	fn clear() {
		FORCE_FRONTIER_HOPS.with(|forced| forced.set(None));
		FORCE_FRONTIER_ROWS.with(|forced| forced.set(None));
	}
}

/// The datastore a run reads.
#[derive(Clone, Copy)]
enum Backend {
	Memory,
	#[cfg(feature = "kv-surrealkv")]
	SurrealKv,
}

/// A fresh datastore on `backend`, with `setup` run.
async fn database(backend: Backend, config: ConfigMap, setup: &str) -> (TestDb, Option<Directory>) {
	match backend {
		Backend::Memory => (TestDb::new_with_config(setup, config).await, None),
		#[cfg(feature = "kv-surrealkv")]
		Backend::SurrealKv => {
			let dir = temp_dir::TempDir::new().unwrap();
			let db = TestDb::new_on_surrealkv(setup, dir.path(), config).await;
			(db, Some(Directory(dir)))
		}
	}
}

/// A directory the datastore under test stands in, removed when the run is done.
#[cfg(feature = "kv-surrealkv")]
struct Directory(#[allow(dead_code)] temp_dir::TempDir);
#[cfg(not(feature = "kv-surrealkv"))]
struct Directory;

/// Runs `setup` and then `sql` on a fresh datastore with the hop windows pinned to `windowed`.
async fn run(windowed: bool, config: &[(&str, &str)], setup: &str, sql: &str) -> Run {
	let pins = if windowed {
		Pins::ALL
	} else {
		Pins::OFF
	};
	run_on(Backend::Memory, pins, config, setup, sql).await
}

/// [`run`] on the datastore `backend` names.
async fn run_on(
	backend: Backend,
	pins: Pins,
	config: &[(&str, &str)],
	setup: &str,
	sql: &str,
) -> Run {
	run_as(&TestDb::owner(), backend, pins, config, setup, sql).await
}

/// The units of fuel a statement run with `config` starts on: the `vm_quantum` it names, or the
/// datastore's default.
fn quantum_of(config: &[(&str, &str)]) -> u32 {
	config.iter().find(|(key, _)| *key == "vm_quantum").map_or_else(
		|| ExecConfig::default().vm_quantum,
		|(_, value)| value.parse().expect("`vm_quantum` is a number of units"),
	)
}

/// [`run_on`] as the session `session`.
async fn run_as(
	session: &Session,
	backend: Backend,
	pins: Pins,
	config: &[(&str, &str)],
	setup: &str,
	sql: &str,
) -> Run {
	pins.set();
	let mut map = ConfigMap::empty();
	for (key, value) in config {
		map = map.with_key_value(*key, *value);
	}
	let mut exec = crate::exec::config::ExecConfig::default();
	surrealdb_cnf::Config::parse(&mut exec, &map);
	let (db, _directory) = database(backend, map, setup).await;
	// A window is filled no wider than the fuel the thread's meter holds, and the setup leaves
	// the meter at whatever phase its own work ended in. The statement starts on the whole
	// quantum the run is configured with, so a window is as wide as the knobs the test names make
	// it and not as the setup's cost left it.
	super::fuel::refill(quantum_of(config));
	counters::reset();
	let ran = super::morsel::batches_of(&db.exec_ctx().await).len();
	// The thread's meter is what the work before the run left of it, and a fill reads no more
	// keys than the meter holds, cutting the range it stops in. Each run starts on a full quantum
	// of its own, as a statement first polled after a yield point does, so what a window holds
	// does not depend on what the thread ran before.
	super::fuel::refill(exec.vm_quantum);
	let responses = db.datastore().execute(sql, session, None).await.expect("the request runs");
	let answers = responses
		.into_iter()
		.map(|response| match response.result {
			Ok(value) => value.to_sql(),
			Err(error) => format!("error: {error}"),
		})
		.collect();
	let run = Run {
		answers,
		filled: counters::filled(),
		served: counters::served(),
		rows: counters::rows_filled(),
		levels: counters::levels(),
		left: counters::left(),
		ended_holding: counters::ended_holding(),
		peak_group: counters::peak_group(),
		built: counters::built(),
		call_keys: counters::call_keys(),
		call_ranges: counters::call_ranges(),
		batches: super::morsel::batches_of(&db.exec_ctx().await).split_off(ran),
	};
	assert_eq!(run.left, 0, "a hop window outlived its hop");
	Pins::clear();
	run
}

/// Runs the statements with the windows off and on, requires the same answers, and answers the
/// run with windows. `windowed` is whether the pinned-on run must have served a read from a window.
async fn same(config: &[(&str, &str)], setup: &str, sql: &str, windowed: bool) -> Run {
	let off = run(false, config, setup, sql).await;
	let on = run(true, config, setup, sql).await;
	assert_eq!(off.filled, (0, 0), "a pinned-off run fills no window");
	assert_eq!(on.answers, off.answers, "the answers of {sql}");
	if windowed {
		assert!(on.filled.0 > 0, "the pinned-on run of {sql} filled a window");
		assert!(on.served.0 + on.served.1 > 0, "the pinned-on run of {sql} served a read");
	}
	on
}

/// `n` people, each knowing up to four others, and some liking one. The edges have keys of their
/// own, so two datastores built from this answer alike.
fn people(n: usize, table: &str) -> String {
	format!(
		"DEFINE TABLE person SCHEMALESS {table}; DEFINE TABLE knows TYPE RELATION; DEFINE TABLE likes TYPE RELATION;
		 FOR $i IN 1..={n} {{ CREATE type::record('person', $i) SET n = $i; }};
		 FOR $i IN 1..={n} {{
			FOR $k IN 0..($i % 5) {{
				LET $edge = type::record('knows', $i * 10 + $k);
				RELATE (type::record('person', $i))->$edge->(type::record('person', ($i * 3 + $k) % {n} + 1));
			}};
			IF $i % 3 = 0 {{
				LET $like = type::record('likes', $i);
				RELATE (type::record('person', $i))->$like->(type::record('person', $i % {n} + 1));
			}};
		 }};"
	)
}

/// Timings of a run differ from run to run; every other counter must not.
fn mask(answer: &str) -> String {
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
}

fn no_errors(on: &Run) {
	assert!(on.answers.iter().all(|answer| !answer.starts_with("error:")), "{:?}", on.answers);
}

#[tokio::test]
async fn the_hops_of_a_path_read_their_sources_in_windows() {
	let setup = people(40, "");
	for sql in [
		"SELECT ->knows->person AS f FROM person",
		"SELECT <-knows<-person AS f FROM person",
		"SELECT <->knows<->person AS f FROM person",
		"SELECT ->(knows, likes)->person AS f FROM person",
		"SELECT ->knows->person->knows->person AS f FROM person",
		"SELECT ->knows->person->likes->person AS f FROM person",
		"SELECT VALUE ->knows->person FROM person:1..=40",
		"SELECT ->knows->person.n AS ns FROM person",
		"SELECT ->knows->person[WHERE n > 10] AS f FROM person",
	] {
		let on = same(&[], &setup, sql, true).await;
		no_errors(&on);
	}
}

#[tokio::test]
async fn a_path_in_a_value_statement_answers_as_it_does_without_windows() {
	let setup = people(40, "");
	let on = same(&[], &setup, "RETURN person:3->knows->person->knows->person", false).await;
	no_errors(&on);
}

#[tokio::test]
async fn a_hop_with_one_source_reads_it_as_it_always_did() {
	let setup = people(40, "");
	let sql = "SELECT ->knows AS e FROM person";
	let off = run_on(Backend::Memory, Pins::OFF, &[], &setup, sql).await;
	let on = run_on(Backend::Memory, Pins::HOPS, &[], &setup, sql).await;
	no_errors(&on);
	assert_eq!(on.answers, off.answers);
	assert_eq!(on.filled, (0, 0), "{:?}", on.filled);
}

#[tokio::test]
async fn a_range_with_more_keys_than_a_fill_reads_goes_on_from_a_cursor() {
	// A hub of 1200 edges stands among the people the second hop reaches, so a window of several
	// sources holds the first 500 keys of the hub's range and the cursor reads the rest. From the
	// hub itself, the 1200 people it reaches are the origins of the next lookup.
	let setup = format!(
		"{} FOR $i IN 1..=1200 {{ CREATE type::record('person', 1000 + $i) SET n = $i; }};
		 FOR $i IN 1..=1200 {{
			LET $edge = type::record('knows', 100000 + $i);
			RELATE person:1->$edge->(type::record('person', 1000 + $i));
		 }};
		 RELATE person:2->knows:99999->person:1;",
		people(40, "")
	);
	for sql in [
		"SELECT VALUE ->knows->person->knows->person FROM person:2",
		"SELECT VALUE array::len(->knows->person->knows->person) FROM person:2, person:7, person:2",
		"SELECT VALUE array::len(->knows->person->knows->person) FROM person:1",
	] {
		let on = same(&[], &setup, sql, true).await;
		no_errors(&on);
	}
}

#[tokio::test]
async fn a_fill_takes_no_more_keys_from_storage_than_the_fuel_left() {
	// A hub of 1200 edges stands among the people the second hop reaches, whose range a fill of
	// several sources reads 500 keys of at once under the default quantum.
	let setup = format!(
		"{} FOR $i IN 1..=1200 {{ CREATE type::record('person', 1000 + $i) SET n = $i; }};
		 FOR $i IN 1..=1200 {{
			LET $edge = type::record('knows', 100000 + $i);
			RELATE person:1->$edge->(type::record('person', 1000 + $i));
		 }};
		 RELATE person:2->knows:99999->person:1;",
		people(40, "")
	);
	let sql =
		"SELECT VALUE array::len(->knows->person->knows->person) FROM person:2, person:7, person:2";
	let wide = same(&[], &setup, sql, true).await;
	no_errors(&wide);
	assert!(
		wide.call_keys > 100,
		"a read of the default quantum visits many keys: {}",
		wide.call_keys
	);
	let narrow = same(&[("vm_quantum", "100")], &setup, sql, true).await;
	no_errors(&narrow);
	assert_eq!(narrow.answers, wide.answers);
	assert!(
		(1..=100).contains(&narrow.call_keys),
		"no read visits more keys than the quantum holds: {}",
		narrow.call_keys
	);
}

#[tokio::test]
async fn a_fill_builds_the_ranges_of_a_group_of_sources_at_a_time() {
	use super::graph::frontier::SOURCES_PER_FILL;
	let n = 2 * SOURCES_PER_FILL + 100;
	let setup = people(n, "");
	// The origins of one hop are an array of more than two groups, each with a range to read. A
	// path of several hops from the array is a broadcast over its elements, each a lookup of one
	// origin, so the array is handed whole to a lookup of one hop.
	for sql in [
		"SELECT VALUE (SELECT VALUE id FROM person)->knows FROM person:1..=2;",
		"SELECT VALUE array::len((SELECT VALUE id FROM person)<-knows) FROM person:1..=2;",
	] {
		let on = same(&[], &setup, sql, true).await;
		no_errors(&on);
		assert!(on.peak_group > 0, "a range was built");
		assert!(
			on.peak_group <= SOURCES_PER_FILL,
			"{} ranges were built for one group of {SOURCES_PER_FILL} sources",
			on.peak_group
		);
	}
}

#[tokio::test]
async fn a_window_that_runs_out_of_budget_leaves_the_rest_to_the_reads() {
	let setup = people(60, "");
	for budget in ["2KiB", "4KiB"] {
		let on = same(
			&[("scan_batch_bytes", budget)],
			&setup,
			"SELECT ->knows->person->knows->person AS f FROM person",
			true,
		)
		.await;
		no_errors(&on);
	}
}

#[tokio::test]
async fn the_inline_caches_a_window_holds_count_toward_its_budget() {
	// Every source of the window is a person, probed once whatever the levels that reach it, and
	// a narrow window holds the probes of fewer than half of them.
	let setup = people(200, "INLINE EDGES 16");
	let sql = "SELECT ->knows->person AS f FROM person";
	let wide = run_on(Backend::Memory, Pins::ROWS, SERIAL, &setup, sql).await;
	let narrow = run_on(
		Backend::Memory,
		Pins::ROWS,
		&[("morsel_split_rows", "18446744073709551615"), ("scan_batch_bytes", "2KiB")],
		&setup,
		sql,
	)
	.await;
	no_errors(&wide);
	assert_eq!(narrow.answers, wide.answers);
	assert!(wide.filled.1 >= 200, "a wide window holds a probe of each source: {:?}", wide.filled);
	assert!(
		narrow.filled.1 * 2 < wide.filled.1,
		"the probes of a narrow window are charged to it: {:?} against {:?}",
		narrow.filled,
		wide.filled
	);
}

#[tokio::test]
async fn a_read_a_window_holds_a_range_for_builds_no_range() {
	let setup = people(60, "");
	// Each read of a source builds the range it reads without a window; with the window of the
	// rows, the fill built them and the reads are handed them.
	for sql in [
		"SELECT ->knows AS e FROM person",
		"SELECT ->knows AS e, ->likes AS l FROM person",
		"SELECT <-knows AS e, ->knows AS g FROM person",
	] {
		let off = run_on(Backend::Memory, Pins::OFF, UNSPENT, &setup, sql).await;
		let rows = run_on(Backend::Memory, Pins::ROWS, UNSPENT, &setup, sql).await;
		no_errors(&rows);
		assert_eq!(rows.answers, off.answers, "{sql}");
		assert!(off.built >= 60, "{sql}: the reads of a run without windows build {}", off.built);
		assert_eq!(rows.built, 0, "{sql}: no read of the run with the window builds a range");
		assert!(rows.served.0 >= 60, "{sql}: the reads were served: {:?}", rows.served);
	}
	// Two hops over the same persons read one range, which the window names for both.
	let sql = "SELECT ->knows->person->knows->person AS f FROM person";
	let off = run_on(Backend::Memory, Pins::OFF, UNSPENT, &setup, sql).await;
	let rows = run_on(Backend::Memory, Pins::ROWS, UNSPENT, &setup, sql).await;
	no_errors(&rows);
	assert_eq!(rows.answers, off.answers, "{sql}");
	assert!(off.built >= 60, "{sql}: the reads of a run without windows build {}", off.built);
	assert_eq!(rows.built, 0, "{sql}: no read of the run with the window builds a range");
	// The sources of a later hop are read from the window of the hop, whose reads build none of
	// the ranges it holds. An owner reads `->knows->person` as one hop, so the path takes it twice:
	// the people the first reaches are the sources of the second.
	let sql = "SELECT ->knows->person->knows->person AS f FROM person";
	let off = run_on(Backend::Memory, Pins::OFF, UNSPENT, &setup, sql).await;
	let hops = run_on(Backend::Memory, Pins::HOPS, UNSPENT, &setup, sql).await;
	assert_eq!(hops.answers, off.answers);
	assert!(hops.built < off.built, "{} against {}", hops.built, off.built);
}

/// A window that an earlier chain filled to its budget reads nothing more for a later chain, but
/// names the ranges it holds for the later chain's reads of them, which are handed them.
#[tokio::test]
async fn a_full_window_names_the_ranges_it_holds_for_a_later_chain() {
	let setup = people(60, "");
	let narrow = [("morsel_split_rows", "18446744073709551615"), ("scan_batch_bytes", "2KiB")];
	let one =
		run_on(Backend::Memory, Pins::ROWS, &narrow, &setup, "SELECT ->knows AS e FROM person")
			.await;
	let two = run_on(
		Backend::Memory,
		Pins::ROWS,
		&narrow,
		&setup,
		"SELECT ->knows AS e, ->knows.id AS g FROM person",
	)
	.await;
	let off = run_on(
		Backend::Memory,
		Pins::OFF,
		&narrow,
		&setup,
		"SELECT ->knows AS e, ->knows.id AS g FROM person",
	)
	.await;
	no_errors(&two);
	assert_eq!(two.answers, off.answers);
	assert!(one.built > 0, "the window held only some of the ranges: {}", one.built);
	assert!(one.built < off.built / 2, "and held some: {} of {}", one.built, off.built / 2);
	assert_eq!(
		two.built,
		2 * one.built,
		"each chain builds only the ranges the window does not hold"
	);
}

#[tokio::test]
async fn a_hop_window_reads_nothing_the_window_of_the_rows_holds() {
	// The rows window holds both levels of the path, so the window of the second hop has nothing
	// left to read.
	let setup = people(60, "");
	let sql = "SELECT ->knows->person AS f FROM person";
	let rows = run_on(Backend::Memory, Pins::ROWS, UNSPENT, &setup, sql).await;
	let all = run_on(Backend::Memory, Pins::ALL, UNSPENT, &setup, sql).await;
	no_errors(&all);
	assert_eq!(all.answers, rows.answers);
	assert_eq!(all.filled, rows.filled, "both windows hold what the rows window alone holds");
	assert_eq!(all.built, 0);
}

#[tokio::test]
async fn a_lookup_with_a_where_answers_as_it_does_without_windows() {
	let setup =
		format!("{} DEFINE FUNCTION fn::touch($p: record) {{ RETURN $p.n > 2; }};", people(40, ""));
	for sql in [
		"SELECT ->knows->person[WHERE n > 5]->knows->person AS f FROM person",
		"SELECT ->knows->person[WHERE fn::touch(id)]->knows->person AS f FROM person",
		"SELECT ->knows[WHERE true]->person->knows->person AS f FROM person",
	] {
		let off = run_on(Backend::Memory, Pins::OFF, &[], &setup, sql).await;
		let on = run_on(Backend::Memory, Pins::HOPS, &[], &setup, sql).await;
		no_errors(&on);
		assert_eq!(on.answers, off.answers, "{sql}");
	}
}

#[tokio::test]
async fn a_where_that_calls_a_function_that_writes_leaves_the_windows_with_what_they_read() {
	let setup = format!(
		"{} DEFINE FUNCTION fn::mark($p: record) {{ UPDATE $p SET marked = true; CREATE type::record('person', 5000 + $p.n); RELATE $p->knows:7777->person:1; RETURN true; }};",
		people(20, "")
	);
	let sql = "SELECT ->knows->person[WHERE fn::mark(id)]->knows->person AS f FROM person; SELECT ->knows->person AS f FROM person;";
	let off = run_on(Backend::Memory, Pins::OFF, &[], &setup, sql).await;
	for pins in [Pins::HOPS, Pins::ROWS, Pins::ALL] {
		let on = run_on(Backend::Memory, pins, &[], &setup, sql).await;
		assert_eq!(on.answers, off.answers, "the answers with the windows pinned");
	}
}

#[tokio::test]
async fn a_window_filled_by_several_reads_serves_every_source() {
	counters::CALL_BYTES.with(|pinned| pinned.set(Some(1)));
	let setup = people(60, "");
	for sql in [
		"SELECT ->knows->person->knows->person AS f FROM person",
		"SELECT <-knows<-person<-knows<-person AS f FROM person",
	] {
		let on = same(&[], &setup, sql, true).await;
		no_errors(&on);
	}
	counters::CALL_BYTES.with(|pinned| pinned.set(None));
}

#[tokio::test]
async fn the_inline_adjacency_caches_of_the_sources_are_probed_in_one_read() {
	let setup = people(40, "INLINE EDGES 16");
	let on =
		same(&[], &setup, "SELECT ->knows->person->knows->person AS f FROM person", true).await;
	no_errors(&on);
	assert!(on.served.1 > 0, "a cache probe was served from a window: {:?}", on.served);
}

/// Runs `setup` on a fresh datastore with the hop windows pinned to `windowed`, and then each of
/// `requests` in turn on one write transaction the caller holds, so a request reads what the ones
/// before it wrote.
async fn run_in_transaction(backend: Backend, pins: Pins, setup: &str, requests: &[&str]) -> Run {
	pins.set();
	let (db, _directory) = database(backend, ConfigMap::empty(), setup).await;
	let txn = std::sync::Arc::new(
		db.datastore()
			.transaction(crate::kvs::TransactionType::Write)
			.await
			.expect("a transaction"),
	);
	let session = TestDb::owner();
	counters::reset();
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
	let run = Run {
		answers,
		filled: counters::filled(),
		served: counters::served(),
		rows: counters::rows_filled(),
		levels: counters::levels(),
		left: counters::left(),
		ended_holding: counters::ended_holding(),
		peak_group: counters::peak_group(),
		built: counters::built(),
		call_keys: counters::call_keys(),
		call_ranges: counters::call_ranges(),
		batches: Vec::new(),
	};
	assert_eq!(run.left, 0, "a hop window outlived its hop");
	txn.commit().await.expect("the transaction commits");
	Pins::clear();
	run
}

#[tokio::test]
async fn a_window_reads_what_the_transaction_wrote() {
	let setup = people(20, "");
	let requests = [
		"RELATE person:1->knows:900->person:20; RELATE person:20->knows:901->person:2;",
		"SELECT VALUE ->knows->person->knows->person FROM person:1;",
		"DELETE person:3; DELETE knows:901;",
		"SELECT ->knows->person->knows->person AS f FROM person:1..=10;",
	];
	for backend in backends() {
		let off = run_in_transaction(backend, Pins::OFF, &setup, &requests).await;
		let on = run_in_transaction(backend, Pins::ALL, &setup, &requests).await;
		assert_eq!(off.filled, (0, 0));
		assert_eq!(on.answers, off.answers);
		no_errors(&on);
		assert!(on.served.0 > 0, "{:?}", on.served);
	}
}

/// The datastores the tests that read what a transaction wrote run on.
fn backends() -> Vec<Backend> {
	vec![
		Backend::Memory,
		#[cfg(feature = "kv-surrealkv")]
		Backend::SurrealKv,
	]
}

#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread")]
async fn surrealkv_reads_the_ranges_of_a_window_with_one_iterator() {
	let setup = format!(
		"{} FOR $i IN 1..=1200 {{ CREATE type::record('person', 1000 + $i) SET n = $i; }};
		 FOR $i IN 1..=1200 {{
			LET $edge = type::record('knows', 100000 + $i);
			RELATE person:1->$edge->(type::record('person', 1000 + $i));
		 }};
		 RELATE person:2->knows:99999->person:1;",
		people(40, "")
	);
	for sql in [
		"SELECT ->knows->person->knows->person AS f FROM person",
		"SELECT VALUE ->knows->person->knows->person FROM person:2",
		"SELECT <-knows<-person<-knows<-person AS f FROM person",
		"EXPLAIN ANALYZE SELECT ->knows->person->knows->person AS f FROM person",
	] {
		// Rows that no `ORDER BY` orders come back in the order of the scan only where the scan
		// runs serially.
		let off = run_on(Backend::SurrealKv, Pins::OFF, SERIAL, &setup, sql).await;
		let on = run_on(Backend::SurrealKv, Pins::ALL, SERIAL, &setup, sql).await;
		assert_eq!(off.filled, (0, 0));
		assert!(on.served.0 > 0, "{sql}: {:?}", on.served);
		if sql.starts_with("EXPLAIN") {
			assert_eq!(mask(&on.answers[0]), mask(&off.answers[0]));
		} else {
			assert_eq!(on.answers, off.answers, "{sql}");
		}
		no_errors(&on);
	}
}

/// `n` users, who know some of each other, and `m` comments, each referencing one user (and some
/// also another through `editor`); the edges and the comments have keys of their own.
fn commenters(n: usize, m: usize, table: &str) -> String {
	format!(
		"DEFINE TABLE user SCHEMALESS {table}; DEFINE TABLE knows TYPE RELATION; DEFINE TABLE comment SCHEMALESS;
		 DEFINE FIELD author ON comment TYPE record<user> REFERENCE;
		 DEFINE FIELD editor ON comment TYPE option<record<user>> REFERENCE;
		 FOR $i IN 1..={n} {{ CREATE type::record('user', $i) SET n = $i; }};
		 FOR $i IN 1..={n} {{
			FOR $k IN 0..($i % 4) {{
				LET $edge = type::record('knows', $i * 10 + $k);
				RELATE (type::record('user', $i))->$edge->(type::record('user', ($i * 3 + $k) % {n} + 1));
			}};
		 }};
		 FOR $i IN 1..={m} {{
			CREATE type::record('comment', $i) SET
				author = type::record('user', ($i * 7) % {n} + 1),
				editor = IF $i % 4 = 0 {{ type::record('user', ($i * 5) % {n} + 1) }} ELSE {{ NONE }};
		 }};"
	)
}

#[tokio::test]
async fn the_references_of_a_set_of_records_are_read_in_windows() {
	for table in ["", "INLINE REFERENCES 8"] {
		let setup = commenters(30, 90, table);
		for sql in [
			"SELECT ->knows->user<~comment AS back FROM user",
			"SELECT VALUE ->knows->user<~comment FROM user",
			"SELECT ->knows->user<~comment->knows->user AS back FROM user",
			"SELECT ->knows->user<~(comment FIELD editor) AS back FROM user",
		] {
			let on = same(&[], &setup, sql, true).await;
			no_errors(&on);
		}
	}
}

#[tokio::test]
async fn the_references_of_more_than_a_group_of_records_are_read_a_group_at_a_time() {
	use super::graph::frontier::SOURCES_PER_FILL;
	let n = 2 * SOURCES_PER_FILL + 100;
	for table in ["", "INLINE REFERENCES 8"] {
		let setup = commenters(n, 300, table);
		for (sql, subjects) in [
			("SELECT VALUE (SELECT VALUE id FROM user)<~comment FROM user:1..=2;", 1),
			(
				"SELECT VALUE (SELECT VALUE id FROM user)<~(comment FIELD editor, comment FIELD author) FROM user:1..=2;",
				2,
			),
		] {
			let on = same(&[], &setup, sql, true).await;
			no_errors(&on);
			assert!(
				on.peak_group <= subjects * SOURCES_PER_FILL,
				"{} ranges were built for one group of {SOURCES_PER_FILL} sources of {subjects} subjects",
				on.peak_group
			);
		}
	}
}

/// The fuel `sql` spends on a datastore `setup` built, read one at a time, on a quantum no
/// statement here spends.
async fn fuel_of(setup: &str, sql: &str) -> u32 {
	const QUANTUM: u32 = 1 << 30;
	Pins::OFF.set();
	let quantum = QUANTUM.to_string();
	let db = TestDb::new_with_config(
		setup,
		ConfigMap::empty()
			.with_key_value("vm_quantum", &quantum)
			.with_key_value("morsel_split_rows", "18446744073709551615"),
	)
	.await;
	super::fuel::refill(QUANTUM);
	let responses =
		db.datastore().execute(sql, &TestDb::owner(), None).await.expect("the request runs");
	let left = super::fuel::left();
	Pins::clear();
	assert!(responses.into_iter().all(|response| response.result.is_ok()));
	QUANTUM - left
}

#[tokio::test]
async fn the_entries_of_an_inline_reference_cache_are_charged_as_an_adjacency_caches_are() {
	let setup =
		"DEFINE TABLE user SCHEMALESS INLINE REFERENCES 16; DEFINE TABLE comment SCHEMALESS;
		 DEFINE FIELD author ON comment TYPE record<user> REFERENCE;
		 CREATE user:1, user:2;
		 FOR $i IN 1..=12 { CREATE type::record('comment', $i) SET author = user:1; };";
	let many = fuel_of(setup, "SELECT VALUE <~comment FROM ONLY user:1").await;
	let none = fuel_of(setup, "SELECT VALUE <~comment FROM ONLY user:2").await;
	assert!(
		many >= none + 8,
		"twelve cached references are charged a unit each: {many} against {none}"
	);
}

#[tokio::test]
async fn lightweight_edges_read_as_they_always_did() {
	let setup = "DEFINE TABLE person SCHEMALESS; DEFINE TABLE knows TYPE RELATION IN person OUT person LIGHTWEIGHT;
		FOR $i IN 1..=30 { CREATE type::record('person', $i) SET n = $i; };
		FOR $i IN 1..=30 {
			FOR $k IN 0..($i % 5) {
				RELATE (type::record('person', $i))->knows->(type::record('person', ($i * 3 + $k) % 30 + 1));
			};
		};";
	for sql in [
		"SELECT ->knows->person AS f FROM person",
		"SELECT ->knows->person->knows->person AS f FROM person",
		"SELECT <-knows<-person AS f FROM person",
	] {
		let on = same(&[], setup, sql, false).await;
		no_errors(&on);
	}
}

/// Runs `setup` on a fresh in-memory datastore, folds the adjacency of every second person into
/// packed blocks, and then `sql`, with the hop windows pinned to `windowed`.
async fn run_folded(windowed: bool, setup: &str, sql: &str) -> Run {
	use std::sync::Arc;

	use surrealdb_kvs::TransactionType::Write;

	use crate::catalog::providers::DatabaseProvider;
	use crate::expr::Dir;
	use crate::idx::adjacency::fold_scope;
	use crate::val::RecordId;

	let ds = crate::kvs::Datastore::builder()
		.without_maintenance_tasks()
		.build_with_path("memory")
		.await
		.unwrap();
	let root = crate::dbs::Session::owner();
	for response in ds
		.execute("DEFINE NAMESPACE test; USE NS test; DEFINE DATABASE test;", &root, None)
		.await
		.unwrap()
	{
		response.result.unwrap();
	}
	let session = root.with_ns("test").with_db("test");
	for response in ds.execute(setup, &session, None).await.unwrap() {
		response.result.unwrap();
	}
	for n in (2..=40).step_by(2) {
		let rid = RecordId::new("person".into(), n as i64);
		for dir in [Dir::In, Dir::Out] {
			loop {
				let txn = Arc::new(ds.transaction(Write).await.unwrap());
				let db = txn.get_db_by_name("test", "test", None).await.unwrap().unwrap();
				let mut env = ds.setup_ctx().unwrap();
				env.set_transaction(Arc::clone(&txn));
				let env = env.freeze();
				let outcome = fold_scope(&env, db.namespace_id, db.database_id, &rid, dir, 1024)
					.await
					.unwrap();
				txn.commit().await.unwrap();
				if !outcome.has_more {
					break;
				}
			}
		}
	}
	let pins = if windowed {
		Pins::ALL
	} else {
		Pins::OFF
	};
	pins.set();
	counters::reset();
	let answers = ds
		.execute(sql, &session, None)
		.await
		.unwrap()
		.into_iter()
		.map(|response| match response.result {
			Ok(value) => value.to_sql(),
			Err(error) => format!("error: {error}"),
		})
		.collect();
	let run = Run {
		answers,
		filled: counters::filled(),
		served: counters::served(),
		rows: counters::rows_filled(),
		levels: counters::levels(),
		left: counters::left(),
		ended_holding: counters::ended_holding(),
		peak_group: counters::peak_group(),
		built: counters::built(),
		call_keys: counters::call_keys(),
		call_ranges: counters::call_ranges(),
		batches: Vec::new(),
	};
	assert_eq!(run.left, 0, "a hop window outlived its hop");
	Pins::clear();
	run
}

#[tokio::test]
async fn folded_and_unfolded_adjacencies_in_one_window_read_as_they_always_did() {
	let setup = people(40, "");
	// The edges are the unfolded sources: a hop from an edge back to a person, or both ways, reads
	// the edge's adjacency. An edge and the person beyond it are one hop from the person, whose
	// table is folded and whose keys no window reads, so such a path answers alike and is served
	// nothing.
	for (sql, served) in [
		("SELECT ->knows<-person->knows<-person AS f FROM person", true),
		("SELECT <-knows->person<-knows->person AS f FROM person", true),
		("SELECT <->knows<->person AS f FROM person", true),
		("SELECT ->knows->person->knows->person AS f FROM person", false),
		("SELECT <-knows<-person<-knows<-person AS f FROM person", false),
	] {
		let off = run_folded(false, &setup, sql).await;
		let on = run_folded(true, &setup, sql).await;
		no_errors(&on);
		assert_eq!(on.answers, off.answers, "{sql}");
		if served {
			assert!(
				on.served.0 > 0,
				"{sql}: the unfolded sources are served from a window: {:?}",
				on.served
			);
		}
	}
}

#[tokio::test]
async fn explain_analyze_of_a_windowed_lookup_shows_what_the_unwindowed_run_shows() {
	for (setup, sql) in [
		(people(40, ""), "EXPLAIN ANALYZE SELECT ->knows->person->knows->person AS f FROM person;"),
		(
			people(40, "INLINE EDGES 16"),
			"EXPLAIN ANALYZE SELECT ->knows->person->knows->person AS f FROM person;",
		),
		(
			commenters(30, 90, ""),
			"EXPLAIN ANALYZE SELECT ->knows->user<~comment AS back FROM user;",
		),
		(
			commenters(30, 90, "INLINE REFERENCES 8"),
			"EXPLAIN ANALYZE SELECT ->knows->user<~comment AS back FROM user;",
		),
	] {
		let off = run(false, &[], &setup, sql).await;
		let on = run(true, &[], &setup, sql).await;
		assert!(on.served.0 + on.served.1 > 0, "the analysed lookup served a read from a window");
		assert_eq!(
			on.answers.iter().map(|a| mask(a)).collect::<Vec<_>>(),
			off.answers.iter().map(|a| mask(a)).collect::<Vec<_>>(),
			"{:?}",
			on.answers
		);
	}
}

/// Morsel knobs that split a statement's rows into batches of a few rows where the policy places
/// the statement as morsels; a statement it keeps serial runs as it does without them.
const MORSELS: &[(&str, &str)] = &[
	("morsel_split_rows", "1"),
	("morsel_split_pass_ppm", "1000000"),
	("morsel_target_bytes", "4000"),
	("morsel_oltp_reserve", "0"),
];
/// One task runs every row of a statement, in order.
const SERIAL: &[(&str, &str)] = &[("morsel_split_rows", "18446744073709551615")];
/// [`SERIAL`] on a quantum no statement here spends. A window is cut where the meter runs out, so
/// a test that requires a window to hold every range it was asked for cannot let the fuel bound it.
const UNSPENT: &[(&str, &str)] =
	&[("morsel_split_rows", "18446744073709551615"), ("vm_quantum", "1073741824")];

/// Runs `sql` with no window, with only the window of the batch of rows, and with every window,
/// requires the same answers of the three, and answers the run with the rows window only.
async fn same_with_rows(config: &[(&str, &str)], setup: &str, sql: &str, windowed: bool) -> Run {
	let off = run_on(Backend::Memory, Pins::OFF, config, setup, sql).await;
	let rows = run_on(Backend::Memory, Pins::ROWS, config, setup, sql).await;
	let all = run_on(Backend::Memory, Pins::ALL, config, setup, sql).await;
	assert_eq!(off.filled, (0, 0), "a pinned-off run fills no window");
	assert_eq!(off.rows, 0);
	assert_eq!(rows.answers, off.answers, "the answers of {sql} with the rows window");
	assert_eq!(all.answers, off.answers, "the answers of {sql} with every window");
	if windowed {
		assert!(rows.rows > 0, "the rows window of {sql} was filled");
		assert!(rows.served.0 + rows.served.1 > 0, "{sql} read from the rows window");
	} else {
		assert_eq!(rows.rows, 0, "{sql} fills no rows window");
	}
	rows
}

#[tokio::test]
async fn the_rows_of_a_batch_read_the_first_hop_of_their_lookups_in_one_window() {
	let setup = people(60, "");
	for sql in [
		"SELECT ->knows->person AS f FROM person",
		"SELECT ->knows AS e, <-knows AS back FROM person",
		"SELECT * , ->knows->person AS f FROM person",
		"SELECT VALUE ->knows->person FROM person:1..=60",
		"SELECT ->knows->person AS f FROM person LIMIT 7",
		"SELECT ->knows->person AS f FROM person LIMIT 5 START 3",
		"SELECT ->knows->person AS f FROM person START 55",
		"SELECT ->knows->person->knows->person AS f FROM person",
		"SELECT <->knows AS e FROM person",
	] {
		for config in [SERIAL, MORSELS] {
			let on = same_with_rows(config, &setup, sql, true).await;
			no_errors(&on);
		}
	}
}

#[tokio::test]
async fn the_rows_window_holds_each_level_of_a_path_for_the_whole_batch() {
	let setup = people(60, "");
	// One batch, four lookups, each an edge and the person beyond it: the window holds the first
	// hop of every lookup of the path.
	let sql =
		"SELECT ->knows->person->knows->person->knows->person->knows->person AS f FROM person";
	let on = same_with_rows(SERIAL, &setup, sql, true).await;
	no_errors(&on);
	assert!(on.levels >= 4, "{} levels", on.levels);
	// Two paths, each of two lookups: each path is a chain of its own.
	let sql = "SELECT ->knows->person->knows->person AS f, <-knows<-person<-knows<-person AS g FROM person";
	let on = same_with_rows(SERIAL, &setup, sql, true).await;
	assert!(on.levels >= 4, "{} levels", on.levels);
	// Few levels of a deep path where the window runs out of budget.
	let on = same_with_rows(
		&[("morsel_split_rows", "18446744073709551615"), ("scan_batch_bytes", "4KiB")],
		&setup,
		"SELECT ->knows->person->knows->person->knows->person AS f FROM person",
		true,
	)
	.await;
	no_errors(&on);
}

#[tokio::test]
async fn a_scan_that_ends_at_its_limit_empties_the_window_of_its_batch() {
	let setup = people(60, "");
	// The limit is spent inside the batch the window was filled for, so the scan ends while the
	// window holds the keys of rows that never ran.
	let sql = "SELECT ->knows->person AS f FROM person LIMIT 7";
	let on = same_with_rows(SERIAL, &setup, sql, true).await;
	no_errors(&on);
	assert!(on.ended_holding > 0, "the scan ended with its window held");
	let on = same_with_rows(MORSELS, &setup, sql, true).await;
	no_errors(&on);
}

#[tokio::test]
async fn the_rows_that_reach_no_lookup_key_nothing() {
	let setup = people(60, "");
	for sql in [
		// Rows a filter takes may not reach the projection.
		"SELECT ->knows->person AS f FROM person WHERE n > 20",
		// A lookup that runs only on some rows.
		"SELECT IF n > 20 { ->knows->person } ELSE { [] } AS f FROM person",
		// A lookup that starts from another value than the row.
		"SELECT person:3->knows->person AS f FROM person",
		"SELECT ->knows->person AS f FROM person ORDER BY n DESC",
		"SELECT n FROM person",
	] {
		for config in [SERIAL, MORSELS] {
			let on = same_with_rows(config, &setup, sql, false).await;
			no_errors(&on);
		}
	}
}

#[tokio::test]
async fn the_rows_of_a_batch_read_their_inline_caches_and_references_in_one_window() {
	let on = same_with_rows(
		SERIAL,
		&people(60, "INLINE EDGES 16"),
		"SELECT ->knows->person AS f FROM person",
		true,
	)
	.await;
	assert!(on.served.1 > 0, "a cache probe was served from the rows window: {:?}", on.served);
	for table in ["", "INLINE REFERENCES 8"] {
		for config in [SERIAL, MORSELS] {
			let on = same_with_rows(
				config,
				&commenters(30, 90, table),
				"SELECT <~comment AS back FROM user",
				true,
			)
			.await;
			no_errors(&on);
		}
	}
}

#[tokio::test]
async fn a_rows_window_reads_what_the_transaction_wrote() {
	let setup = people(20, "");
	let requests = [
		"RELATE person:1->knows:900->person:20; RELATE person:20->knows:901->person:2;",
		"SELECT ->knows->person AS f FROM person;",
		"DELETE person:3; DELETE knows:901;",
		"SELECT ->knows->person AS f FROM person LIMIT 12;",
	];
	for backend in backends() {
		let off = run_in_transaction(backend, Pins::OFF, &setup, &requests).await;
		let on = run_in_transaction(backend, Pins::ROWS, &setup, &requests).await;
		assert_eq!(off.filled, (0, 0));
		assert_eq!(on.answers, off.answers);
		no_errors(&on);
		assert!(on.rows > 0 && on.served.0 > 0, "{:?} {}", on.served, on.rows);
	}
}

#[tokio::test]
async fn a_statement_that_writes_keys_no_rows() {
	let setup = people(20, "");
	let on = same_with_rows(
		SERIAL,
		&setup,
		"SELECT ->knows->person AS f FROM person; UPDATE person SET seen = true;",
		true,
	)
	.await;
	no_errors(&on);
	// The write is a statement of its own; a statement that holds the write refuses the window.
	let sql = "SELECT (UPDATE person:1 SET seen = true) AS w, ->knows->person AS f FROM person";
	let on = same_with_rows(SERIAL, &setup, sql, false).await;
	no_errors(&on);
}

#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread")]
async fn surrealkv_reads_the_first_hop_of_a_batch_of_rows_with_one_iterator() {
	let setup = people(300, "");
	for sql in [
		"SELECT ->knows->person AS f FROM person",
		"SELECT ->knows->person AS f FROM person LIMIT 40 START 7",
		"SELECT <-knows AS e FROM person",
		"EXPLAIN ANALYZE SELECT ->knows->person AS f FROM person",
	] {
		let off = run_on(Backend::SurrealKv, Pins::OFF, SERIAL, &setup, sql).await;
		let on = run_on(Backend::SurrealKv, Pins::ALL, SERIAL, &setup, sql).await;
		assert_eq!(off.filled, (0, 0));
		assert!(on.rows > 0 && on.served.0 > 0, "{sql}: {:?} {}", on.served, on.rows);
		if sql.starts_with("EXPLAIN") {
			assert_eq!(mask(&on.answers[0]), mask(&off.answers[0]));
		} else {
			assert_eq!(on.answers, off.answers, "{sql}");
		}
		no_errors(&on);
	}
}

/// Adds a key to the `->knows` adjacency of `person:rid` that lies inside its range and decodes
/// as no graph key.
async fn corrupt_adjacency(db: &TestDb, rid: i64) {
	corrupt_adjacency_of(db, crate::val::RecordId::new("person".into(), rid), "knows", &[0xFF; 3])
		.await;
}

/// Adds a key to the adjacency of `source` toward `edge` that lies inside its range and decodes as
/// no graph key; `tail` is the bytes of the key after the range's start.
async fn corrupt_adjacency_of(db: &TestDb, source: crate::val::RecordId, edge: &str, tail: &[u8]) {
	use std::ops::Bound;

	use crate::catalog::providers::DatabaseProvider;
	use crate::exec::operators::scan::graph_keys::graph_ranges;
	use crate::expr::Dir;

	let write = db
		.datastore()
		.transaction(crate::kvs::TransactionType::Write)
		.await
		.expect("a write transaction opens");
	let def = write
		.get_db_by_name("test", "test", None)
		.await
		.expect("the database reads")
		.expect("the database exists");
	let ranges = graph_ranges(
		def.namespace_id,
		def.database_id,
		&source,
		Dir::Out,
		&[(edge.into(), Bound::Unbounded, Bound::Unbounded)],
	)
	.expect("the range builds");
	let (start, _) = ranges[0].range.bounds();
	let mut key = start.to_vec();
	key.extend_from_slice(tail);
	write.set(surrealdb_kvs::Key::from(key), vec![0u8]).await.expect("the key is written");
	write.commit().await.expect("the corruption commits");
}

/// Runs `sql` over a database whose `->knows` adjacency of `person:2` holds a key that decodes as
/// no graph key, with the windows pinned to `pins`.
async fn run_corrupt(pins: Pins, setup: &str, sql: &str) -> Run {
	pins.set();
	let (db, _directory) = database(Backend::Memory, ConfigMap::empty(), setup).await;
	corrupt_adjacency(&db, 2).await;
	counters::reset();
	let session = TestDb::owner();
	let responses = db.datastore().execute(sql, &session, None).await.expect("the request runs");
	let answers = responses
		.into_iter()
		.map(|response| match response.result {
			Ok(value) => value.to_sql(),
			Err(error) => format!("error: {error:#}"),
		})
		.collect();
	let run = Run {
		answers,
		filled: counters::filled(),
		served: counters::served(),
		rows: counters::rows_filled(),
		levels: counters::levels(),
		left: counters::left(),
		ended_holding: counters::ended_holding(),
		peak_group: counters::peak_group(),
		built: counters::built(),
		call_keys: counters::call_keys(),
		call_ranges: counters::call_ranges(),
		batches: Vec::new(),
	};
	Pins::clear();
	assert_eq!(run.left, 0, "a hop window outlived its hop");
	run
}

#[tokio::test]
async fn a_key_that_does_not_decode_fails_a_run_with_windows_as_it_fails_one_without() {
	let setup = people(40, "");
	for sql in [
		"SELECT ->knows->person AS f FROM person",
		"SELECT ->knows->person->knows->person AS f FROM person",
		"SELECT VALUE ->knows FROM person:1..=10",
	] {
		let off = run_corrupt(Pins::OFF, &setup, sql).await;
		assert!(off.answers[0].starts_with("error: "), "{sql}: {:?}", off.answers);
		for pins in [Pins::HOPS, Pins::ROWS, Pins::ALL] {
			let on = run_corrupt(pins, &setup, sql).await;
			assert_eq!(on.answers, off.answers, "{sql}");
		}
	}
}

/// A statement that holds a nested statement, read at a version or not, reads no window of its
/// rows, and answers as it does without windows: the version a nested read names is never served
/// the current keys a window holds.
#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread")]
async fn a_nested_statement_in_a_projection_reads_no_window_of_the_rows() {
	let setup = people(30, "");
	for nested in [
		"(SELECT VALUE ->knows FROM ONLY person:1 VERSION $t1)",
		"(SELECT VALUE ->knows->person FROM person:1..=6 VERSION $t1)",
		"(SELECT VALUE ->knows FROM ONLY person:1)",
	] {
		// The edge the second statement adds is newer than `$t1`.
		let sql = format!(
			"LET $t1 = time::now(); RELATE person:1->knows:9001->person:7;
			 SELECT id, {nested} AS then, ->knows AS e FROM person;"
		);
		for config in [SERIAL, MORSELS] {
			let off = run_on(Backend::SurrealKv, Pins::OFF, config, &setup, &sql).await;
			let on = run_on(Backend::SurrealKv, Pins::ALL, config, &setup, &sql).await;
			assert_eq!(on.rows, 0, "the rows of {nested} fill no window");
			assert_eq!(on.answers, off.answers, "{nested}");
		}
	}
}

#[tokio::test]
async fn a_path_under_a_condition_reads_its_hops_in_windows() {
	let setup = people(40, "");
	for sql in [
		"SELECT IF n > 10 THEN ->knows->person->knows->person ELSE [] END AS f FROM person",
		"SELECT n > 10 AND ->knows->person->knows->person AS f FROM person",
	] {
		let off = run_on(Backend::Memory, Pins::OFF, &[], &setup, sql).await;
		let on = run_on(Backend::Memory, Pins::HOPS, &[], &setup, sql).await;
		no_errors(&on);
		assert_eq!(on.answers, off.answers, "{sql}");
		assert!(on.filled.0 > 0, "the hops under the condition filled a window: {sql}");
		assert!(on.served.0 + on.served.1 > 0, "and a read took from one: {sql}");
	}
}

#[tokio::test]
async fn a_lookup_under_a_condition_is_never_keyed_for_the_batch() {
	let setup = people(60, "");
	for config in [SERIAL, MORSELS] {
		// The first lookup runs on every row and is keyed, one level for each batch; the one under
		// the branch is not.
		let sql = "SELECT ->knows AS e, IF n > 20 { ->knows->person } ELSE { [] } AS f FROM person";
		let on = same_with_rows(config, &setup, sql, true).await;
		no_errors(&on);
		assert_eq!(on.levels, on.rows, "one level of a path is keyed for each batch: {sql}");
		// A block holds no chain, whatever its branch reads.
		let sql =
			"SELECT { LET $k = ->knows; IF n > 20 { $k->person } ELSE { [] } } AS f FROM person";
		let on = same_with_rows(config, &setup, sql, false).await;
		no_errors(&on);
	}
}

#[tokio::test]
async fn a_fill_seeks_no_more_ranges_than_the_fuel_left() {
	use super::graph::frontier::SOURCES_PER_FILL;
	// A hub of twice a group of edges: the lookup after the hub's has the people the hub reaches
	// as an array of origins, whose ranges one call of a fill seeks together under the default
	// quantum.
	let setup = format!(
		"DEFINE TABLE person SCHEMALESS; DEFINE TABLE knows TYPE RELATION;
		 FOR $i IN 1..={n} {{ CREATE type::record('person', $i); }};
		 FOR $i IN 2..={n} {{
			LET $edge = type::record('knows', $i);
			RELATE person:1->$edge->(type::record('person', $i));
		 }};",
		n = 2 * SOURCES_PER_FILL
	);
	let sql = "SELECT VALUE array::len(->knows->person->knows->person) FROM person:1;";
	let wide = same(&[], &setup, sql, true).await;
	no_errors(&wide);
	assert!(
		wide.call_ranges > 100,
		"a read of the default quantum seeks many ranges: {}",
		wide.call_ranges
	);
	let narrow = same(&[("vm_quantum", "100")], &setup, sql, true).await;
	no_errors(&narrow);
	assert_eq!(narrow.answers, wide.answers);
	assert!(
		(1..=100).contains(&narrow.call_ranges),
		"no read seeks more ranges than the quantum holds: {}",
		narrow.call_ranges
	);
}

#[tokio::test]
async fn each_array_of_origins_fills_a_window_whatever_the_arrays_before_it_held() {
	use super::graph::frontier::SOURCES_PER_FILL;
	let n = 2 * SOURCES_PER_FILL + 100;
	let setup = people(n, "");
	// Every person's edges are an array of origins to the lookup that follows, and a small budget
	// is filled by a few of them: a window that one array left standing would leave the arrays
	// after it to read for themselves, so that only the first filled.
	let sql = "SELECT VALUE array::len((SELECT VALUE id FROM person)->knows->person->knows->person) FROM person:1;";
	let on = same(&[("scan_batch_bytes", "8KiB")], &setup, sql, true).await;
	no_errors(&on);
	let reaching_two = (1..=n).filter(|i| i % 5 >= 2).count();
	assert!(
		on.filled.0 >= reaching_two,
		"{} fills for {reaching_two} arrays of two or more origins",
		on.filled.0
	);
	assert!(on.peak_group <= SOURCES_PER_FILL, "{}", on.peak_group);
}

#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread")]
async fn a_record_user_reads_through_windows_what_the_reads_one_at_a_time_give() {
	let setup = "
		DEFINE ACCESS user ON DATABASE TYPE RECORD;
		DEFINE TABLE person SCHEMALESS INLINE EDGES 4 INLINE REFERENCES 4
			PERMISSIONS FOR select WHERE hidden != true;
		DEFINE FIELD secret ON person PERMISSIONS FOR select WHERE id = $auth.id;
		DEFINE TABLE knows SCHEMALESS TYPE RELATION FROM person TO person
			PERMISSIONS FOR select WHERE public = true;
		DEFINE TABLE comment SCHEMALESS PERMISSIONS FOR select WHERE public = true;
		DEFINE FIELD author ON comment TYPE record<person> REFERENCE;
		FOR $i IN 1..=40 {
			CREATE type::record('person', $i) SET n = $i, secret = 's' + <string> $i, hidden = ($i % 5 = 0);
		};
		FOR $i IN 1..=40 {
			FOR $k IN 0..($i % 4 + 1) {
				LET $edge = type::record('knows', $i * 10 + $k);
				RELATE (type::record('person', $i))->$edge->(type::record('person', ($i * 5 + $k) % 40 + 1))
					SET public = (($i + $k) % 2 = 0);
			};
		};
		FOR $i IN 1..=40 {
			FOR $k IN 0..($i % 3 + 1) {
				CREATE type::record('comment', $i * 10 + $k)
					SET author = type::record('person', $i), public = (($i + $k) % 2 = 1);
			};
		};";
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("person", 1));
	let session = Session::for_record("test", "test", "user", rid);
	let mut served = 0;
	for sql in [
		"SELECT id, secret, ->knows->person AS f FROM person",
		"SELECT id, ->knows->person->knows->person AS fof FROM person",
		"SELECT id, <-knows<-person AS b FROM person",
		"SELECT id, <~comment AS c FROM person",
		"SELECT id, ->knows->person<~comment AS reached FROM person",
		"SELECT id, ->knows->person.secret AS secrets FROM person",
		"SELECT VALUE (SELECT VALUE id FROM person)->knows->person FROM ONLY person:1",
		"SELECT id, ->knows->person AS f FROM person LIMIT 6 START 3",
	] {
		let off = run_as(&session, Backend::SurrealKv, Pins::OFF, &[], setup, sql).await;
		let on = run_as(&session, Backend::SurrealKv, Pins::ALL, &[], setup, sql).await;
		no_errors(&on);
		assert_eq!(on.answers, off.answers, "{sql}");
		assert_eq!(off.filled, (0, 0));
		served += on.served.0 + on.served.1;
	}
	assert!(served > 0, "no read of a record user's was served from a window");
}

/// What a lookup of two hops built by hand answers: the value as text, or `error: ` and the
/// failure, with the windows it filled and the lookups that ended holding one.
struct Hops {
	answer: String,
	filled: usize,
	left: usize,
}

/// Runs the lookup `->knows->person` from `person:4` as one lookup of two hops, read in windows
/// where `window`, over a database where `corrupt` adds a key that decodes as no graph key to the
/// adjacency of that edge toward `person`. No statement lowers a lookup of several hops, so the
/// spec is built here and `lookup` is called with it.
async fn two_hops(window: bool, corrupt: Option<i64>) -> Hops {
	use std::sync::Arc;

	use crate::exec::vm::fold_host::tests::program_with;
	use crate::exec::vm::interp::State;
	use crate::exec::vm::program::{EdgeSpec, HopRead, LookupHop, LookupSpec};
	use crate::expr::{ControlFlow, Dir};
	use crate::val::{RecordId, TableName, Value};

	let (db, _directory) = database(Backend::Memory, ConfigMap::empty(), &people(40, "")).await;
	if let Some(edge) = corrupt {
		corrupt_adjacency_of(&db, RecordId::new("knows".into(), edge), "person", &[0xFF; 3]).await;
	}
	let hop = |table: &str| {
		let table = TableName::from(table);
		let prefixes = vec![storekey::encode_vec(&table).expect("a table name encodes")];
		LookupHop {
			read: Arc::new(HopRead::graph(
				vec![Dir::Out],
				vec![EdgeSpec::whole(table)],
				prefixes,
				None,
				Vec::new(),
			)),
			full: false,
			tables: Vec::new(),
			cond: None,
			key_cond: None,
		}
	};
	let spec = LookupSpec {
		hops: vec![hop("knows"), hop("person")],
		extract_id: false,
		only: false,
		only_path: false,
		checked: true,
		page: None,
		window,
		trace: crate::expr::logical::LookupTrace::None,
	};
	let program = program_with(Vec::new(), Vec::new(), Vec::new(), 0);
	let ctx = db.exec_ctx().await;
	let mut state = State::new(&program, Vec::new(), ctx.function_registry()).expect("a state");
	let origin = Value::RecordId(RecordId::new("person".into(), 4));
	counters::reset();
	let answered = super::graph::lookup(&program, &spec, origin, None, &mut state, &ctx).await;
	let answer = match answered {
		Ok(value) => value.to_sql(),
		Err(ControlFlow::Err(error)) => format!("error: {error:#}"),
		Err(other) => format!("error: {other}"),
	};
	Hops {
		answer,
		filled: counters::filled().0,
		left: counters::left(),
	}
}

/// Runs `person:4->knows->person` as one lookup of two hops whose page needs its first `needs`
/// records, over 600 edges `knows:0` to `knows:599` from `person:4`, each toward `person:<i>`.
/// The adjacency of `knows:<corrupt>` toward `person` holds a key that decodes as no graph key.
async fn paged_two_hops(window: bool, needs: usize, corrupt: i64) -> String {
	use std::sync::Arc;

	use crate::exec::vm::fold_host::tests::program_with;
	use crate::exec::vm::interp::State;
	use crate::exec::vm::program::{EdgeSpec, HopRead, LookupHop, LookupPage, LookupSpec};
	use crate::expr::{ControlFlow, Dir};
	use crate::val::{RecordId, TableName, Value};

	let setup = "FOR $i IN 0..600 { INSERT RELATION INTO knows { id: $i, in: person:4, out: type::record('person', $i) }; };";
	let (db, _directory) = database(Backend::Memory, ConfigMap::empty(), setup).await;
	corrupt_adjacency_of(&db, RecordId::new("knows".into(), corrupt), "person", &[0xFF; 3]).await;
	let hop = |table: &str| {
		let table = TableName::from(table);
		let prefixes = vec![storekey::encode_vec(&table).expect("a table name encodes")];
		LookupHop {
			read: Arc::new(HopRead::graph(
				vec![Dir::Out],
				vec![EdgeSpec::whole(table)],
				prefixes,
				None,
				Vec::new(),
			)),
			full: false,
			tables: Vec::new(),
			cond: None,
			key_cond: None,
		}
	};
	let spec = LookupSpec {
		hops: vec![hop("knows"), hop("person")],
		extract_id: false,
		only: false,
		only_path: false,
		checked: true,
		page: Some(LookupPage {
			start: 0,
			limit: Some(needs),
		}),
		window,
		trace: crate::expr::logical::LookupTrace::None,
	};
	let program = program_with(Vec::new(), Vec::new(), Vec::new(), 0);
	let ctx = db.exec_ctx().await;
	let mut state = State::new(&program, Vec::new(), ctx.function_registry()).expect("a state");
	let origin = Value::RecordId(RecordId::new("person".into(), 4));
	match super::graph::lookup(&program, &spec, origin, None, &mut state, &ctx).await {
		Ok(value) => value.to_sql(),
		Err(ControlFlow::Err(error)) => format!("error: {error:#}"),
		Err(other) => format!("error: {other}"),
	}
}

#[tokio::test]
async fn a_paged_hop_over_more_sources_than_a_fill_takes_reads_no_source_past_its_page() {
	use super::graph::frontier::SOURCES_PER_FILL;
	// The sources span two groups.
	const { assert!(SOURCES_PER_FILL < 502) };
	// The page needs 502 records: the first group of 500 edges answers 500, and the second group
	// reads `knows:500` and `knows:501` only, never the corrupt adjacency of `knows:550`.
	for window in [false, true] {
		let answer = paged_two_hops(window, 502, 550).await;
		assert!(!answer.starts_with("error:"), "window {window}: {answer}");
		assert!(answer.contains("person:501"), "window {window}: {answer}");
		assert!(!answer.contains("person:502"), "window {window}: {answer}");
	}
	// A page that reaches the corrupt adjacency fails as the read of that adjacency fails.
	for window in [false, true] {
		let answer = paged_two_hops(window, 560, 550).await;
		assert!(answer.starts_with("error:"), "window {window}: {answer}");
	}
}

#[tokio::test]
async fn a_lookup_of_two_hops_reads_its_second_hop_in_a_window_and_drops_it() {
	let off = two_hops(false, None).await;
	assert!(!off.answer.starts_with("error:"), "{}", off.answer);
	assert_eq!(off.filled, 0);
	let on = two_hops(true, None).await;
	assert_eq!(on.answer, off.answer);
	assert!(on.filled > 0, "the second hop reads the edges the first reached in a window");
	assert_eq!(on.left, 0, "the hop window outlived its lookup");

	// The same lookup as a statement answers what the lookup built by hand answers.
	let (db, _directory) = database(Backend::Memory, ConfigMap::empty(), &people(40, "")).await;
	let session = TestDb::owner();
	let sql = "RETURN person:4->knows->person";
	let responses = db.datastore().execute(sql, &session, None).await.expect("the request runs");
	let statement = responses[0].result.as_ref().expect("the statement answers").to_sql();
	assert_eq!(statement, off.answer);
}

#[tokio::test]
async fn a_lookup_of_two_hops_whose_second_hop_fails_drops_its_window_as_one_without_does() {
	// The edges of `person:4` are `knows:40` to `knows:43`; the read of the adjacency of the
	// third fails.
	let off = two_hops(false, Some(42)).await;
	assert!(off.answer.starts_with("error: "), "{}", off.answer);
	let on = two_hops(true, Some(42)).await;
	assert_eq!(on.answer, off.answer);
	assert!(on.filled > 0, "the window was filled before the read that failed");
	assert_eq!(on.left, 0, "the hop window outlived its failed lookup");
}

#[cfg(feature = "kv-surrealkv")]
#[tokio::test]
async fn a_paged_morsel_batch_keys_the_lookups_of_the_rows_past_its_skip_only() {
	// SurrealKV places a small paged range as morsels, and a batch that pages runs each row as
	// though it were taken: the rows its clause skips are never keyed.
	let setup = people(60, "");
	for sql in [
		"SELECT ->knows->person AS f FROM person START 7 LIMIT 20",
		"SELECT ->knows->person AS f FROM person START 3 LIMIT 40",
	] {
		// The ranges one source of the lookups takes, from a run of the whole table.
		let base = run_on(
			Backend::SurrealKv,
			Pins::ROWS,
			SERIAL,
			&setup,
			"SELECT ->knows->person AS f FROM person",
		)
		.await;
		assert!(base.filled.1 > 0 && base.filled.1 % 60 == 0, "{:?}", base.filled);
		let per_source = base.filled.1 / 60;
		let off = run_on(Backend::SurrealKv, Pins::OFF, SERIAL, &setup, sql).await;
		let on = run_on(Backend::SurrealKv, Pins::ROWS, MORSELS, &setup, sql).await;
		no_errors(&on);
		assert_eq!(on.answers, off.answers, "the answers of {sql}");
		assert!(on.rows > 0, "{sql}: no batch of rows filled its window: {:?}", on.batches);
		assert!(
			on.batches.iter().any(|(skip, rows)| skip.is_some_and(|skip| skip > 0 && skip < *rows)),
			"{sql}: no batch holds rows past its skip: {:?}",
			on.batches
		);
		// The rows a batch's skip covers key no origin: every range filled is one of a row past it.
		let reached: usize = on
			.batches
			.iter()
			.map(|(skip, rows)| rows - skip.map_or(0, |skip| skip.min(*rows)))
			.sum();
		assert_eq!(
			on.filled.1,
			per_source * reached,
			"{sql}: {:?} ranges for {reached} rows past the skip of {:?}",
			on.filled,
			on.batches
		);
	}
}
