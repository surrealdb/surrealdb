//! Scans that read their next chunk on a task of their own against scans that do not.
//!
//! Each test runs the same statements on two fresh datastores, one with the read-ahead pinned off
//! and one pinned on, and requires the answers, the errors and the entries read to be the same:
//! the read-ahead changes only which task reads a chunk. A pinned-on run counts the producers it
//! started, and a pinned-off run starts none. Every producer that started has ended when the
//! statement has, and was awaited by its source's `finish` rather than dropped.

use std::sync::Arc;

use surrealdb_cnf::ConfigMap;
use surrealdb_types::ToSql;

use super::host::RECORDS_SCANNED;
use super::read_ahead::probe;
use crate::exec::ir::phys::io::{FORCE_RANGE_MORSELS, FORCE_SCAN_READ_AHEAD};
use crate::exec::operators::test_util::TestDb;

/// What a run answered and how its scans read.
struct Run {
	/// One answer per statement: the value as text, or `error: ` and the failure.
	answers: Vec<String>,
	/// The records the scans of the statements read.
	scanned: usize,
	/// The producers the statements started and the ones that ended.
	producers: (usize, usize),
	/// The producers a source awaited to their end, as against the ones it dropped.
	joined: usize,
	/// The edges the scans cut their ranges at, and the ones cut while a producer read ahead.
	cuts: usize,
	cuts_ahead: usize,
}

/// The scan runs whole on the statement's task.
const SERIAL: (&str, &str) = ("morsel_split_rows", "18446744073709551615");
/// The scan is cut into morsels early, so the front of the range runs as the coordinator's morsel.
const MORSELS: (&str, &str) = ("morsel_split_rows", "64");
/// A page of a few dozen records, so that many pages and byte boundaries are crossed.
const SMALL_PAGES: (&str, &str) = ("scan_batch_bytes", "4KiB");
/// A morsel of a few dozen records, so that the coordinator cuts its range several times.
const SMALL_MORSELS: (&str, &str) = ("morsel_target_bytes", "8KiB");
/// Every morsel donates the rest of its range at each chunk boundary, so a cut is published while
/// the next chunk is already being read.
const DONATING: (&str, &str) = ("morsel_steal_always", "true");
/// The settings `pairs` names. A range is cut where workers get no read view, so the front run
/// over the in-memory datastore, built to give none, is cut as it is over one with views, and
/// whatever share of its rows passes.
fn config(pairs: &[(&str, &str)]) -> ConfigMap {
	pairs.iter().fold(
		ConfigMap::empty()
			.with_key_value("morsel_needs_view", "false")
			.with_key_value("morsel_split_pass_ppm", "1000000"),
		|config, (key, value)| config.with_key_value(*key, *value),
	)
}

/// The datastore a test runs on when it is not in memory: a directory of SurrealKV.
#[derive(Clone, Copy)]
enum Disk<'a> {
	#[cfg_attr(not(feature = "kv-surrealkv"), allow(dead_code))]
	SurrealKv(&'a std::path::Path),
}

/// A fresh datastore holding `setup`: in memory, giving workers no view so its morsels run on the
/// coordinator as front runs, or on the disk `disk` names.
async fn open(setup: &str, disk: Option<Disk<'_>>, pairs: &[(&str, &str)]) -> TestDb {
	match disk {
		None => TestDb::new_viewless(setup, config(pairs)).await,
		#[cfg(feature = "kv-surrealkv")]
		Some(Disk::SurrealKv(dir)) => {
			let path = format!("surrealkv://{}", dir.display());
			let ds = crate::kvs::Datastore::builder()
				.with_capabilities(crate::dbs::Capabilities::all())
				.with_config(config(pairs))
				.build_with_path(&path)
				.await
				.expect("a surrealkv datastore");
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
		#[cfg(not(feature = "kv-surrealkv"))]
		Some(Disk::SurrealKv(_)) => {
			unreachable!("a SurrealKV datastore needs the `kv-surrealkv` feature")
		}
	}
}

/// Runs `setup` and then `sql` on a fresh datastore with the read-ahead pinned to `ahead`.
async fn run(ahead: bool, pairs: &[(&str, &str)], setup: &str, sql: &str) -> Run {
	run_on(Some(ahead), None, pairs, setup, sql).await
}

/// As [`run`] on the datastore `disk` names, with the read-ahead as the datastore offers it when
/// `forced` is `None`.
async fn run_on(
	forced: Option<bool>,
	disk: Option<Disk<'_>>,
	pairs: &[(&str, &str)],
	setup: &str,
	sql: &str,
) -> Run {
	FORCE_SCAN_READ_AHEAD.with(|pinned| pinned.set(forced));
	let db = open(setup, disk, pairs).await;
	let session = TestDb::owner();
	probe::reset();
	RECORDS_SCANNED.with(|scanned| scanned.set(0));
	// Where a scan's chunks end depends on the fuel the thread has left, so every run starts with
	// the same.
	super::fuel::refill(4096);
	let responses = db.datastore().execute(sql, &session, None).await.expect("the request runs");
	let answers = responses
		.into_iter()
		.map(|response| match response.result {
			Ok(value) => value.to_sql(),
			Err(error) => format!("error: {error}"),
		})
		.collect();
	let scanned = RECORDS_SCANNED.with(std::cell::Cell::get);
	let producers = (probe::started(), probe::finished());
	let (joined, cuts, cuts_ahead) = (probe::joined(), probe::cuts(), probe::cuts_ahead());
	FORCE_SCAN_READ_AHEAD.with(|pinned| pinned.set(None));
	Run {
		answers,
		scanned,
		producers,
		joined,
		cuts,
		cuts_ahead,
	}
}

/// Runs the statements without and with the read-ahead, requires the same answers, and answers the
/// run with it. `reads_ahead` is whether it must have started a producer; a scan that reads ahead
/// runs in the storage cursor, which reads the same records however its chunks are read. A scan
/// that decodes its rows is read through a buffer whose reach is a matter of timing, so only the
/// answers of those are compared.
async fn same(pairs: &[(&str, &str)], setup: &str, sql: &str, reads_ahead: bool) -> Run {
	let off = run(false, pairs, setup, sql).await;
	let on = run(true, pairs, setup, sql).await;
	assert_eq!(off.producers, (0, 0), "a pinned-off run starts no producer: {sql}");
	assert_eq!(on.answers, off.answers, "the answers of {sql}");
	// A scan that fails reads as far as the chunks that ended before the failure, and where those
	// end depends on the fuel the thread has left.
	if reads_ahead && !on.answers.iter().any(|answer| answer.starts_with("error:")) {
		assert_eq!(on.scanned, off.scanned, "the records read by {sql}");
	}
	assert_eq!(on.producers.0, on.producers.1, "every producer ended: {sql}");
	assert_eq!(on.joined, on.producers.0, "every producer was awaited by `finish`: {sql}");
	assert_eq!(on.producers.0 > 0, reads_ahead, "producers started by {sql}: {:?}", on.producers);
	on
}

/// `n` records of table `t`: `t:0` to `t:n-1`.
fn rows(n: usize) -> String {
	format!(
		"FOR $i IN 0..{n} {{ CREATE type::record('t', $i) SET n = $i, g = $i % 7, name = <string>$i; }};"
	)
}

#[tokio::test]
async fn a_scan_reads_ahead_and_answers_as_it_does_not() {
	let setup = rows(3000);
	let queries: &[(&str, bool)] = &[
		("SELECT * FROM t;", true),
		("SELECT * FROM t WHERE n % 3 = 0;", true),
		("SELECT * FROM t ORDER BY id DESC;", true),
		// The steps of a group and of a sort run on the row, so their bodies run in the storage
		// cursor too.
		("SELECT g, count() AS c FROM t GROUP BY g ORDER BY g;", true),
		("SELECT VALUE n FROM t ORDER BY n DESC;", true),
		("SELECT * FROM t WHERE id > t:100 AND id < t:2900;", true),
		("SELECT count() FROM t GROUP ALL;", false),
		("SELECT g, count() FROM t GROUP BY g;", true),
		("SELECT * FROM t WHERE g = 3 AND n > 100;", true),
	];
	for placement in [SERIAL, MORSELS] {
		for pages in [None, Some(SMALL_PAGES)] {
			let mut pairs = vec![placement];
			pairs.extend(pages);
			for (sql, reads_ahead) in queries {
				same(&pairs, &setup, sql, *reads_ahead).await;
			}
		}
	}
}

#[tokio::test]
async fn a_scan_over_rows_of_two_layouts_answers_as_it_does_not_read_ahead() {
	// The even ids are written before a field is added and the odd ones after it, so the packed
	// layouts alternate record by record across every page and chunk boundary.
	let setup = "DEFINE TABLE p SCHEMAFULL;
		DEFINE FIELD name ON p TYPE string;
		DEFINE FIELD age ON p TYPE int;
		FOR $i IN 0..1200 { CREATE type::record('p', $i * 2) SET name = <string>$i, age = $i; };
		DEFINE FIELD email ON p TYPE option<string>;
		FOR $i IN 0..1200 {
			CREATE type::record('p', $i * 2 + 1) SET name = <string>$i, age = $i, email = 'e';
		};";
	for placement in [SERIAL, MORSELS] {
		for pages in [None, Some(SMALL_PAGES)] {
			let mut pairs = vec![placement];
			pairs.extend(pages);
			let on = same(&pairs, setup, "SELECT VALUE age FROM p WHERE email = 'e';", true).await;
			assert!(on.answers[0].starts_with("[0, 1, 2,"), "{:?}", on.answers[0]);
			same(&pairs, setup, "SELECT * FROM p WHERE age % 400 = 0;", true).await;
			same(&pairs, setup, "SELECT * FROM p ORDER BY id DESC;", true).await;
		}
	}
}

#[tokio::test]
async fn a_range_of_one_chunk_starts_no_producer() {
	let setup = rows(100);
	// The coordinator of morsels reads its first chunk up to the split threshold, so a threshold
	// above the range keeps the range in one chunk.
	for placement in [SERIAL, ("morsel_split_rows", "500")] {
		same(&[placement], &setup, "SELECT * FROM t;", false).await;
	}
}

#[tokio::test]
async fn a_scan_that_may_stop_early_or_write_starts_no_producer() {
	let setup = rows(2000);
	for sql in [
		"SELECT * FROM t LIMIT 700;",
		"SELECT * FROM t START 600;",
		"SELECT * FROM ONLY t LIMIT 1;",
		"SELECT * FROM t ORDER BY n LIMIT 10;",
		"UPDATE t SET n += 1;",
		"SELECT * FROM t VERSION d'2999-01-01T00:00:00Z';",
	] {
		same(&[SERIAL], &setup, sql, false).await;
	}
}

#[tokio::test]
async fn a_body_that_fails_ends_its_producer_before_the_statement_answers() {
	let setup = rows(3000);
	// Adding one overflows from the record `t:1700` on.
	let sql = "SELECT * FROM t WHERE 9223372036854775807 + (n - 1699) > 0;";
	for placement in [SERIAL, MORSELS] {
		let on = same(&[placement, SMALL_PAGES], &setup, sql, true).await;
		assert!(on.answers[0].starts_with("error:"), "{:?}", on.answers);
	}
}

/// Runs `requests` in turn on one write transaction the caller holds, so a request reads what the
/// ones before it wrote, with the read-ahead as `forced` pins it. A statement that follows a write
/// of its own request is planned when it is reached, so each request here plans its statements
/// before any write of its own. The answers end with what the committed state holds.
async fn run_in_transaction(
	forced: Option<bool>,
	disk: Option<Disk<'_>>,
	setup: &str,
	requests: &[&str],
) -> Run {
	FORCE_SCAN_READ_AHEAD.with(|pinned| pinned.set(forced));
	let db = open(setup, disk, &[SERIAL]).await;
	let txn = Arc::new(
		db.datastore()
			.transaction(crate::kvs::TransactionType::Write)
			.await
			.expect("a transaction"),
	);
	let session = TestDb::owner();
	probe::reset();
	RECORDS_SCANNED.with(|scanned| scanned.set(0));
	let mut answers = Vec::new();
	for sql in requests {
		let request = crate::kvs::request::QueryRequest::new(*sql, &session)
			.with_transaction(Arc::clone(&txn));
		let responses = db.datastore().run(request).await.expect("the request runs");
		answers.extend(responses.into_iter().map(|response| match response.result {
			Ok(value) => value.to_sql(),
			Err(error) => format!("error: {error}"),
		}));
	}
	let scanned = RECORDS_SCANNED.with(std::cell::Cell::get);
	let producers = (probe::started(), probe::finished());
	let (joined, cuts, cuts_ahead) = (probe::joined(), probe::cuts(), probe::cuts_ahead());
	txn.commit().await.expect("the transaction commits");
	FORCE_SCAN_READ_AHEAD.with(|pinned| pinned.set(None));
	answers.extend(committed(&db).await);
	Run {
		answers,
		scanned,
		producers,
		joined,
		cuts,
		cuts_ahead,
	}
}

/// What the committed state of table `w` holds, read after the transaction ended.
async fn committed(db: &TestDb) -> Vec<String> {
	db.datastore()
		.execute("SELECT count() AS c, math::sum(n) AS s FROM w GROUP ALL;", &TestDb::owner(), None)
		.await
		.expect("the request runs")
		.into_iter()
		.map(|response| response.result.expect("it answers").to_sql())
		.collect()
}

const WRITTEN: [&str; 4] = [
	"FOR $i IN 0..2000 { CREATE type::record('w', $i) SET n = $i; };",
	"SELECT * FROM w;",
	"UPDATE w SET n += 1;",
	"SELECT VALUE n FROM w;",
];

/// A transaction's scans read what the transaction wrote, whether or not they read ahead, and the
/// committed state is the same.
async fn reads_what_the_transaction_wrote(disk: Option<Disk<'_>>, off_disk: Option<Disk<'_>>) {
	let setup = "DEFINE TABLE w SCHEMALESS;";
	let off = run_in_transaction(Some(false), off_disk, setup, &WRITTEN).await;
	let on = run_in_transaction(Some(true), disk, setup, &WRITTEN).await;
	assert_eq!(on.answers, off.answers);
	assert!(on.answers[1].contains("n: 1999"), "the writes of the transaction are read");
	assert!(on.answers[3].contains("2000"), "and so are the writes after them");
	assert!(on.answers[4].contains("c: 2000"), "and they are committed: {:?}", on.answers);
	assert_eq!(on.scanned, off.scanned);
	// The two SELECTs read ahead; the UPDATE, which writes, does not.
	assert_eq!(on.producers, (2, 2));
	assert_eq!(off.producers, (0, 0));
}

#[tokio::test]
async fn a_scan_in_a_transaction_reads_what_the_transaction_wrote() {
	reads_what_the_transaction_wrote(None, None).await;
}

/// A scan the statement abandons mid-run ends its producer, and the transaction that held it
/// writes and commits.
async fn dropped_mid_run(disk: Option<Disk<'_>>) {
	FORCE_SCAN_READ_AHEAD.with(|forced| forced.set(Some(true)));
	let db = open(&rows(3000), disk, &[SERIAL, SMALL_PAGES]).await;
	let txn = Arc::new(
		db.datastore()
			.transaction(crate::kvs::TransactionType::Write)
			.await
			.expect("a transaction"),
	);
	let session = TestDb::owner();
	probe::reset();
	let reached = Arc::new(tokio::sync::Notify::new());
	probe::stall_after(2, Arc::clone(&reached));
	let request = crate::kvs::request::QueryRequest::new("SELECT * FROM t;", &session)
		.with_transaction(Arc::clone(&txn));
	let ran = db.datastore().run(request);
	tokio::select! {
		_ = ran => panic!("the scan is held where the test cancels it"),
		_ = reached.notified() => {}
	}
	assert_eq!(probe::started(), 1);
	assert_eq!(probe::finished(), 1, "the dropped scan ended its producer");
	assert_eq!(probe::joined(), 0, "a dropped scan aborts its producer and awaits nothing");
	FORCE_SCAN_READ_AHEAD.with(|forced| forced.set(None));
	let request = crate::kvs::request::QueryRequest::new(
		"CREATE t:9999 SET n = 9999; SELECT count() FROM t GROUP ALL;",
		&session,
	)
	.with_transaction(Arc::clone(&txn));
	let responses = db.datastore().run(request).await.expect("the request runs");
	let answers: Vec<String> = responses
		.into_iter()
		.map(|response| response.result.expect("it answers").to_sql())
		.collect();
	assert!(answers[1].contains("3001"), "{answers:?}");
	txn.commit().await.expect("the transaction commits");
	let held = db
		.datastore()
		.execute("SELECT count() AS c FROM t GROUP ALL;", &session, None)
		.await
		.expect("the request runs");
	assert_eq!(held[0].result.clone().expect("it answers").to_sql(), "[{ c: 3001 }]");
}

#[tokio::test]
async fn a_scan_dropped_mid_run_leaves_the_transaction_usable() {
	dropped_mid_run(None).await;
}

/// The datastore's own answer, with nothing pinned, reads ahead on SurrealKV, and the answers are
/// those of a run that does not.
#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread")]
async fn surrealkv_offers_the_read_ahead_to_the_plan() {
	let setup = rows(3000);
	for placement in [SERIAL, MORSELS] {
		let pairs = [placement, SMALL_PAGES];
		for sql in [
			"SELECT * FROM t;",
			"SELECT * FROM t WHERE n % 3 = 0;",
			"SELECT * FROM t ORDER BY id DESC;",
		] {
			let (dir, off_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
			let on = run_on(None, Some(Disk::SurrealKv(dir.path())), &pairs, &setup, sql).await;
			let off =
				run_on(Some(false), Some(Disk::SurrealKv(off_dir.path())), &pairs, &setup, sql)
					.await;
			assert!(on.producers.0 > 0, "SurrealKV reads {sql} ahead: {:?}", on.producers);
			assert_eq!(on.producers.0, on.producers.1);
			assert_eq!(on.joined, on.producers.0);
			assert_eq!(off.producers, (0, 0));
			assert_eq!(on.answers, off.answers, "the answers of {sql}");
			assert_eq!(on.scanned, off.scanned, "the records read by {sql}");
		}
	}
}

#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread")]
async fn surrealkv_scans_in_a_transaction_read_what_the_transaction_wrote() {
	let (on, off) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
	reads_what_the_transaction_wrote(
		Some(Disk::SurrealKv(on.path())),
		Some(Disk::SurrealKv(off.path())),
	)
	.await;
}

#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread")]
async fn surrealkv_scan_dropped_mid_run_leaves_the_transaction_usable() {
	let dir = tempfile::tempdir().unwrap();
	dropped_mid_run(Some(Disk::SurrealKv(dir.path()))).await;
}

/// `text` with the time each operator took left out.
fn untimed(text: &str) -> String {
	let mut out = String::new();
	let mut rest = text;
	while let Some(at) = rest.find("elapsed: ") {
		out.push_str(&rest[..at]);
		out.push_str("elapsed: _");
		rest = &rest[at..];
		rest = &rest[rest.find('}').unwrap_or(rest.len())..];
	}
	out.push_str(rest);
	out
}

#[tokio::test]
async fn explain_analyze_counts_the_entries_the_scan_visited() {
	for placement in [SERIAL, MORSELS] {
		let sql = "EXPLAIN ANALYZE SELECT * FROM t;";
		let off = run(false, &[placement, SMALL_PAGES], &rows(1500), sql).await;
		let on = run(true, &[placement, SMALL_PAGES], &rows(1500), sql).await;
		assert!(on.producers.0 > 0, "the scan reads ahead: {:?}", on.producers);
		assert_eq!(
			on.answers.iter().map(|answer| untimed(answer)).collect::<Vec<_>>(),
			off.answers.iter().map(|answer| untimed(answer)).collect::<Vec<_>>()
		);
	}
}

/// As [`run_on`], with a scan of a whole range placed as range morsels whatever the policy
/// places, and the read-ahead pinned to `ahead`.
async fn run_morsels(
	ahead: bool,
	disk: Option<Disk<'_>>,
	pairs: &[(&str, &str)],
	setup: &str,
	sql: &str,
) -> Run {
	FORCE_RANGE_MORSELS.with(|forced| forced.set(true));
	let run = run_on(Some(ahead), disk, pairs, setup, sql).await;
	FORCE_RANGE_MORSELS.with(|forced| forced.set(false));
	run
}

/// The coordinator's front run cuts its range while its producer reads ahead, and no entry is run
/// twice or lost: the part it gives away is run by another morsel, so an entry past the cut that
/// the front morsel also ran would answer twice. The front run visits the entry at the edge, as
/// the visitor over a cursor does, and ends there, so it counts the entries a front run over a
/// cursor counts, and a cut costs at most one entry visited by two morsels. The placement is
/// pinned ([`FORCE_RANGE_MORSELS`]), so the test reaches the front run whatever the policy places.
/// The backward cut is covered at the source, over real cursors, in the tests of `read_ahead`: the
/// backward scan of `ORDER BY id DESC` reads a physical source, which loop legality states as read
/// whole, so the pinned placement of a backward scan is refused and no front run is backward.
#[tokio::test]
async fn a_front_run_cut_while_reading_ahead_runs_no_entry_twice() {
	let setup = rows(3000);
	let pairs = [MORSELS, SMALL_PAGES, SMALL_MORSELS, DONATING];
	for sql in ["SELECT * FROM t;", "SELECT * FROM t WHERE n % 5 < 4;"] {
		let whole = run(false, &[SERIAL], &setup, sql).await;
		let off = run_morsels(false, None, &pairs, &setup, sql).await;
		let on = run_morsels(true, None, &pairs, &setup, sql).await;
		assert!(on.cuts > 0, "the front run cuts its range: {sql}");
		assert!(on.cuts_ahead > 0, "and cuts it while its producer reads ahead: {sql}");
		assert!(on.producers.0 > 0, "and the scan reads ahead: {sql}");
		assert_eq!(off.producers, (0, 0), "{sql}");
		assert_eq!(on.answers, whole.answers, "the answers of {sql}");
		assert_eq!(off.answers, whole.answers, "the answers of {sql}");
		assert_eq!(whole.scanned, 3000, "{sql}");
		assert!(
			(3000..=3000 + off.cuts).contains(&off.scanned),
			"no entry is lost, and no more than the one at each edge is read twice: {sql}: {} cuts, {} read",
			off.cuts,
			off.scanned
		);
		assert_eq!(on.cuts, off.cuts, "the range is cut where it is cut without the read-ahead");
		assert_eq!(on.scanned, off.scanned, "the entries the front run visited in {sql}");
		assert_eq!(on.joined, on.producers.0, "every producer was awaited: {sql}");
	}
}

/// As above where workers run the morsels the front run gives away, so a cut is published while the
/// coordinator reads on and the part past it is read elsewhere at the same time.
#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread")]
async fn surrealkv_front_run_cut_while_reading_ahead_runs_no_entry_twice() {
	let setup = rows(3000);
	let pairs = [MORSELS, SMALL_PAGES, SMALL_MORSELS, DONATING];
	for sql in ["SELECT * FROM t;", "SELECT * FROM t WHERE n % 5 < 4;"] {
		let (dir, off_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
		let on = run_morsels(true, Some(Disk::SurrealKv(dir.path())), &pairs, &setup, sql).await;
		let off =
			run_morsels(false, Some(Disk::SurrealKv(off_dir.path())), &pairs, &setup, sql).await;
		assert!(on.cuts_ahead > 0, "the front run cuts its range while it reads ahead: {sql}");
		assert!(on.producers.0 > 0, "and the scan reads ahead: {sql}");
		// Without `ORDER BY` the rows of a statement whose morsels run on workers arrive in no
		// specified order, so the answers are compared as multisets.
		assert_eq!(rows_of(&on.answers), rows_of(&off.answers), "the answers of {sql}");
		assert_eq!(on.joined, on.producers.0, "every producer was awaited: {sql}");
	}
}

/// The rows of each answer, an array of flat objects, in sorted order.
#[cfg(feature = "kv-surrealkv")]
fn rows_of(answers: &[String]) -> Vec<Vec<String>> {
	answers
		.iter()
		.map(|answer| {
			let inner = answer.trim().trim_start_matches("[{").trim_end_matches("}]");
			let mut rows: Vec<String> = inner.split("}, {").map(str::to_owned).collect();
			rows.sort();
			rows
		})
		.collect()
}

/// `EXPLAIN ANALYZE` of a front run that is cut while it reads ahead states what one that reads
/// through a cursor states: the counters count the entries the front run visited, not the pages
/// the producer read.
#[tokio::test]
async fn explain_analyze_of_a_front_run_that_is_cut_counts_the_entries_it_visited() {
	let pairs = [MORSELS, SMALL_PAGES, SMALL_MORSELS, DONATING, ("morsel_explain_analyze", "true")];
	let sql = "EXPLAIN ANALYZE SELECT * FROM t;";
	let off = run_morsels(false, None, &pairs, &rows(3000), sql).await;
	let on = run_morsels(true, None, &pairs, &rows(3000), sql).await;
	assert!(on.cuts_ahead > 0, "the front run is cut while it reads ahead: {:?}", on.cuts_ahead);
	assert!(on.answers[0].contains("Morsels"), "the morsels are rendered: {:?}", on.answers);
	assert_eq!(
		on.answers.iter().map(|answer| untimed(answer)).collect::<Vec<_>>(),
		off.answers.iter().map(|answer| untimed(answer)).collect::<Vec<_>>()
	);
}
