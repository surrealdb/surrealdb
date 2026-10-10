//! Pre-emption is a property of the VM: whatever a statement's shape, a
//! program yields to the host's scheduler once per quantum of work, and a
//! deadline stops a statement that does no I/O within a quantum of passing.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::tests::{bind, outcome, planner};
use crate::exec::operators::test_util::TestDb;
use crate::exec::scheduler::{Scheduler, YieldFuture};

/// The quantum the shapes run under.
const QUANTUM: usize = 16;

/// The rows, elements or depths each shape walks.
const ROWS: usize = 2_000;

/// The yields a run may fall short of one per quantum by: the quantum the
/// thread held when the statement started, and the rounding at its end.
const SLACK: usize = 2;

/// A scheduler that counts the yields it is asked for and yields to tokio.
#[derive(Default)]
struct Counting(AtomicUsize);

impl Scheduler for Counting {
	fn yield_now(&self) -> YieldFuture {
		self.0.fetch_add(1, Ordering::Relaxed);
		Box::pin(tokio::task::yield_now())
	}
}

fn config(region: bool) -> surrealdb_cnf::ConfigMap {
	let config =
		surrealdb_cnf::ConfigMap::empty().with_key_value("vm_quantum", QUANTUM.to_string());
	if region {
		config.with_key_value("region_row_threshold", "1")
	} else {
		config.with_key_value("region_row_threshold", usize::MAX.to_string())
	}
}

/// The yields `sql` asks the scheduler for, run once over `db`, and its rows.
async fn yields(db: &TestDb, sql: &str) -> (usize, usize) {
	let counting = Arc::new(Counting::default());
	let ctx = db.exec_ctx_scheduled(Arc::clone(&counting) as Arc<dyn Scheduler>).await;
	let bound = bind(&ctx, sql).await;
	let vm = super::plan(&planner(&ctx), &bound).await.unwrap_or_else(|| panic!("plans `{sql}`"));
	let rows = outcome(&vm, &ctx).await.unwrap_or_else(|e| panic!("`{sql}` fails: {e}"));
	(counting.0.load(Ordering::Relaxed), rows.len())
}

/// Assert `sql` yields at least once per quantum of the `units` it walks. A
/// shape that walks its rows more than once names more units than rows, so
/// that a walk which spent no fuel would fall short.
async fn assert_yields(db: &TestDb, sql: &str, units: usize) {
	let (yields, rows) = yields(db, sql).await;
	let least = units / QUANTUM - SLACK;
	assert!(
		yields >= least,
		"`{sql}` yielded {yields} times over {units} units, {rows} rows; at least {least} expected"
	);
}

fn rows_setup() -> String {
	format!(
		"DEFINE TABLE t; FOR $i IN 0..{ROWS} {{ CREATE type::record('t', $i) SET n = $i, m = $i % 10, s = <string> $i }};"
	)
}

/// Each loop kind of a statement yields once per quantum of its rows,
/// iterations or depths.
#[tokio::test]
async fn every_loop_kind_yields_once_per_quantum() {
	let setup = format!(
		"{rows}
		FOR $i IN 0..{ROWS} {{ CREATE type::record('c', $i) SET next = type::record('c', $i + 1) }};
		CREATE v:hub;
		FOR $i IN 0..{ROWS} {{ RELATE v:hub->e->(type::record('v', $i)) }};",
		rows = rows_setup()
	);
	let db = TestDb::new_with_config(&setup, config(false)).await;
	for (sql, units) in [
		// A scan whose body runs in place, inside the storage cursor.
		("SELECT * FROM t", ROWS),
		// A scan whose body the host runs, a row at a time.
		("SELECT n, s.len() AS l FROM t", ROWS),
		// A scan that refuses every record before decoding it.
		("SELECT * FROM t WHERE n < 0", ROWS),
		// A `FOR` over a range in a block, doing no I/O.
		(&format!("{{ FOR $i IN 0..{ROWS} {{ LET $x = $i + 1; }}; RETURN 1 }}"), ROWS),
		// A `FOR` over an array in a block.
		(
			&format!("{{ FOR $x IN array::range(0, {ROWS}) {{ LET $y = $x * 2; }}; RETURN 1 }}"),
			ROWS,
		),
		// An array filtered by a loop the interpreter runs within one row.
		(&format!("RETURN array::range(0, {ROWS})[WHERE $this % 2 = 0].len()"), ROWS),
		// A closure called once per element by a builtin.
		(&format!("RETURN array::range(0, {ROWS}).map(|$v| $v + 1).len()"), ROWS),
		// An unlimited sort: the rows scanned, then sorted in runs and merged
		// (or shuffled), then decoded and run, each a unit.
		("SELECT * FROM t ORDER BY n DESC", 3 * ROWS),
		("SELECT * FROM t ORDER BY RAND()", 7 * ROWS / 2),
		// A high-cardinality aggregate: the rows scanned, then each group
		// finished and run.
		("SELECT n, count() AS c FROM t GROUP BY n", 5 * ROWS / 2),
		// A deep recursion.
		("RETURN c:0.{..250+collect}.next", 250),
		// A dense graph walk.
		("SELECT VALUE ->e->v FROM ONLY v:hub", ROWS),
	] {
		assert_yields(&db, sql, units).await;
	}
}

/// A B-tree scan spends a unit per entry it reads, so entries that name no
/// row it hands on — the repeats of a record that holds an array, listed
/// once per element — still bring it to a yield once per quantum.
#[tokio::test]
async fn an_index_scan_yields_over_entries_it_collapses() {
	let setup = format!(
		"DEFINE TABLE a; DEFINE INDEX ix ON a FIELDS v; CREATE a:1 SET v = array::range(0, {ROWS});"
	);
	let db = TestDb::new_with_config(&setup, config(false)).await;
	let sql = "SELECT * FROM a WITH INDEX ix WHERE v >= 0";
	let (_, rows) = yields(&db, sql).await;
	assert_eq!(rows, 1, "`{sql}` lists its one record once");
	assert_yields(&db, sql, ROWS).await;
}

/// The elements of the arrays the builtin shapes walk.
const ELEMENTS: usize = 4_000;

/// A builtin that walks a large collection, or answers one from a small
/// input, yields mid-walk once per quantum of the elements it walks; a count
/// over many keys reads them in steps with a yield between; and a single
/// bulk instruction over a huge array yields mid-copy.
#[tokio::test]
async fn every_walk_yields_once_per_quantum() {
	let db = TestDb::new_with_config(&rows_setup(), config(false)).await;
	// The length is drawn when the statement runs, so that no call is folded
	// into a constant when it is planned.
	let n = format!("rand::int({ELEMENTS}, {ELEMENTS})");
	let n = n.as_str();
	let n_ = ELEMENTS;
	for (sql, units) in [
		// A small input answering a large output: the range generated.
		(format!("RETURN array::range(0, {n}).len()"), n_),
		(format!("RETURN string::len(string::repeat('abcdefgh', {n}))"), n_ * 8 / 64),
		// A sort of a large array: generated, shuffled, then sorted in runs
		// and merged.
		(format!("RETURN array::sort(array::shuffle(array::range(0, {n}))).len()"), 3 * n_),
		(format!("RETURN array::sort::desc(array::range(0, {n})).len()"), 2 * n_),
		// A join of a large array into one string.
		(format!("RETURN string::len(array::join(array::range(0, {n}), ','))"), 2 * n_),
		// Aggregates over a large array, one of them sorting.
		(format!("RETURN math::sum(array::range(0, {n}))"), 2 * n_),
		(format!("RETURN math::median(array::shuffle(array::range(0, {n})))"), 3 * n_),
		// Set and object families.
		(format!("RETURN array::distinct(array::range(0, {n})).len()"), 2 * n_),
		(
			format!(
				"RETURN object::keys(object::from_entries(array::range(0, {n}).map(|$v| [<string> $v, $v]))).len()"
			),
			3 * n_,
		),
		// A count over many keys.
		("SELECT count() FROM t GROUP ALL".to_owned(), ROWS),
		// A top-k with a limit far beyond the quantum.
		("SELECT * FROM t ORDER BY n DESC LIMIT 1000".to_owned(), ROWS),
	] {
		assert_yields(&db, &sql, units).await;
	}
	// One `Each` over a huge array copies it a stretch at a time: the array
	// is generated in ten ranges (a range is capped in size) and flattened,
	// then copied, each yielding once per quantum.
	let huge = 100_000;
	let part = format!("array::range(0, rand::int({0}, {0}))", huge / 10);
	let parts = vec![part; 10].join(", ");
	assert_yields(
		&db,
		&format!("{{ LET $a = array::flatten([{parts}]); RETURN array::len($a.*) }}"),
		3 * huge,
	)
	.await;
}

/// A sort that keeps a bounded number of rows never sorts more than a
/// quantum of them at once: one whose limit fits the quantum prunes within
/// it, and one with a larger limit merges runs of no more than a quantum,
/// answering the same rows either way.
#[tokio::test]
async fn a_top_k_sort_works_within_the_quantum() {
	for (quantum, limit) in [(64, 3), (64, 40), (16, 200), (64, 1000), (16, 1000)] {
		let config = surrealdb_cnf::ConfigMap::empty()
			.with_key_value("vm_quantum", quantum.to_string())
			.with_key_value("region_row_threshold", usize::MAX.to_string());
		let db = TestDb::new_with_config(&rows_setup(), config).await;
		super::interp::LARGEST_PRUNE.with(|largest| largest.set(0));
		let ctx = db.exec_ctx().await;
		let sql = format!("SELECT VALUE n FROM t ORDER BY m DESC, n LIMIT {limit}");
		let bound = bind(&ctx, &sql).await;
		let vm =
			super::plan(&planner(&ctx), &bound).await.unwrap_or_else(|| panic!("plans `{sql}`"));
		let rows = outcome(&vm, &ctx).await.unwrap_or_else(|e| panic!("`{sql}` fails: {e}"));
		let largest = super::interp::LARGEST_PRUNE.with(std::cell::Cell::get);
		assert!(
			largest <= quantum,
			"`{sql}` pruned {largest} rows at once under a quantum of {quantum}"
		);
		// `m = 9` holds the rows whose `n` ends in 9, in order of `n`.
		let expected: Vec<crate::val::Value> = (0..ROWS as i64)
			.filter(|n| n % 10 == 9)
			.take(limit)
			.map(crate::val::Value::from)
			.collect();
		let got: Vec<crate::val::Value> = rows.into_iter().take(expected.len()).collect();
		assert_eq!(got, expected, "`{sql}`");
	}
}

/// A top-k sort answers the rows, in the order, that one stable sort of
/// every row answers — ties by arrival — whether it prunes or merges, under
/// any quantum, and whether its rows arrive one at a time or in the batches
/// of a region.
#[tokio::test]
async fn a_top_k_sort_keeps_the_stable_order_of_one_sort() {
	const N: i64 = 3_000;
	let setup = format!(
		"DEFINE TABLE t; FOR $i IN 0..{N} {{ CREATE type::record('t', $i) SET n = $i, k = ($i * 7919) % 97 }};"
	);
	for region in [false, true] {
		for quantum in [4, 16, 64, 1024] {
			let config = surrealdb_cnf::ConfigMap::empty()
				.with_key_value("vm_quantum", quantum.to_string())
				.with_key_value(
					"region_row_threshold",
					if region {
						"1".to_owned()
					} else {
						usize::MAX.to_string()
					},
				);
			let db = TestDb::new_with_config(&setup, config).await;
			let ctx = db.exec_ctx().await;
			for limit in [1, 3, 50, 700, 2_000, 2_999, 5_000] {
				for descending in [false, true] {
					let sql = format!(
						"SELECT VALUE n FROM t ORDER BY k {} LIMIT {limit}",
						if descending {
							"DESC"
						} else {
							"ASC"
						}
					);
					let bound = bind(&ctx, &sql).await;
					let vm = super::plan(&planner(&ctx), &bound)
						.await
						.unwrap_or_else(|| panic!("plans `{sql}`"));
					let rows =
						outcome(&vm, &ctx).await.unwrap_or_else(|e| panic!("`{sql}` fails: {e}"));
					let mut expected: Vec<i64> = (0..N).collect();
					expected.sort_by_key(|n| {
						let k = (n * 7919) % 97;
						if descending {
							-k
						} else {
							k
						}
					});
					let expected: Vec<crate::val::Value> =
						expected.into_iter().take(limit).map(crate::val::Value::from).collect();
					assert_eq!(rows, expected, "`{sql}` under quantum {quantum}, region {region}");
				}
			}
		}
	}
}

/// The rows the large top-k and count shapes walk.
const MANY: usize = 24_000;

/// A top-k sort with a limit far beyond the quantum yields once per quantum
/// of the rows it scans and hands on, and answers its rows in stable order;
/// a count over many keys yields once per quantum of the keys it reads, a
/// read at a time on a backend that counts on the calling thread.
#[tokio::test]
async fn a_large_top_k_and_count_yield_once_per_quantum() {
	let setup = format!(
		"DEFINE TABLE t; FOR $i IN 0..{MANY} {{ CREATE type::record('t', $i) SET n = $i, m = $i % 10 }};"
	);
	let db = TestDb::new_with_config(&setup, config(false)).await;
	for limit in [5_000, 20_000] {
		let sql = format!("SELECT VALUE n FROM t ORDER BY m DESC, n LIMIT {limit}");
		let counting = Arc::new(Counting::default());
		let ctx = db.exec_ctx_scheduled(Arc::clone(&counting) as Arc<dyn Scheduler>).await;
		let bound = bind(&ctx, &sql).await;
		let vm =
			super::plan(&planner(&ctx), &bound).await.unwrap_or_else(|| panic!("plans `{sql}`"));
		let rows = outcome(&vm, &ctx).await.unwrap_or_else(|e| panic!("`{sql}` fails: {e}"));
		let yields = counting.0.load(Ordering::Relaxed);
		let least = (MANY + limit) / QUANTUM - SLACK;
		assert!(yields >= least, "`{sql}` yielded {yields} times; at least {least} expected");
		// The rows by `m` descending, each `m`'s rows in order of `n`.
		let expected: Vec<crate::val::Value> = (0..10)
			.rev()
			.flat_map(|m| (0..MANY as i64).filter(move |n| n % 10 == m))
			.take(limit)
			.map(crate::val::Value::from)
			.collect();
		assert_eq!(rows, expected, "`{sql}`");
	}
	let (yields, rows) = yields(&db, "SELECT count() FROM t GROUP ALL").await;
	let least = MANY / QUANTUM - SLACK;
	assert_eq!(rows, 1);
	assert!(
		yields >= least,
		"a count of {MANY} keys yielded {yields} times; at least {least} expected"
	);
}

/// The rest of a key range counted in parts answers what one count of it
/// answers, wherever the parts fall.
#[tokio::test]
async fn a_count_in_parts_answers_one_count() {
	let setup = "DEFINE TABLE t; FOR $i IN 0..3000 { CREATE type::record('t', $i) }; \
		FOR $i IN 0..500 { CREATE type::record('t', <string> $i) };";
	let db = TestDb::new_with_config(setup, config(false)).await;
	let ctx = db.exec_ctx().await;
	let range = super::services::Services::table_range(
		&super::services::EngineServices::new(&ctx),
		&"t".into(),
	)
	.expect("range");
	let whole = ctx.txn().count(range.clone(), None).await.expect("count");
	assert_eq!(whole, 3_500);
	let (start, _) = range.bounds();
	let all = super::count::count_rest(&ctx, &range, start.to_vec(), None).await.expect("parts");
	// From a key below every key of the table, every key is counted.
	assert_eq!(all, whole);
	// From one record's key, it and the keys beyond it.
	let keys = ctx.txn().keys_raw(range.clone(), 1_001, 0, None).await.expect("keys");
	let rest =
		super::count::count_rest(&ctx, &range, keys[1_000].clone(), None).await.expect("parts");
	assert_eq!(rest, whole - 1_000);
}

/// On a backend that counts in parallel, a count over many keys answers
/// what one count of the range answers, and yields between the parts it
/// counts the range in.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread")]
async fn a_count_on_a_parallel_backend_yields_between_parts() {
	let dir = tempfile::tempdir().expect("temporary directory");
	let setup = format!("CREATE |t:0..{MANY}| RETURN NONE;");
	let db = TestDb::new_on_rocksdb(&setup, dir.path(), config(false)).await;
	assert!(db.exec_ctx().await.txn().parallel_count());
	let (yields, rows) = yields(&db, "SELECT count() FROM t GROUP ALL").await;
	assert_eq!(rows, 1);
	assert!(yields >= 8, "a count of {MANY} keys in parts yielded {yields} times");
	let ctx = db.exec_ctx().await;
	let rows = outcome(
		&super::plan(&planner(&ctx), &bind(&ctx, "SELECT VALUE count() FROM t GROUP ALL").await)
			.await
			.expect("plans"),
		&ctx,
	)
	.await
	.expect("counts");
	assert_eq!(rows, vec![crate::val::Value::from(MANY as i64)]);
}

/// Time a count of a large RocksDB table three ways: through a keys cursor
/// a batch at a time, by one call to the backend's count, and as the VM
/// counts. Run in release with the `kv-rocksdb` feature:
/// `cargo test --release -p surrealdb-core --features kv-rocksdb --lib
/// exec::vm::tests_preempt::count_throughput -- --ignored --nocapture`.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement, not a check"]
async fn count_throughput() {
	const ROWS: usize = 2_000_000;
	let dir = tempfile::tempdir().expect("temporary directory");
	let setup = format!("CREATE |t:0..{ROWS}| RETURN NONE;");
	let db = TestDb::new_on_rocksdb(&setup, dir.path(), surrealdb_cnf::ConfigMap::empty()).await;
	for _ in 0..5 {
		let ctx = db.exec_ctx().await;
		let range = super::services::Services::table_range(
			&super::services::EngineServices::new(&ctx),
			&"t".into(),
		)
		.expect("range");
		let txn = ctx.txn();
		let started = web_time::Instant::now();
		let mut cursor = txn
			.open_keys_cursor_raw(range.clone(), crate::kvs::Direction::Forward, 0, None)
			.await
			.expect("cursor");
		let mut serial = 0;
		loop {
			let stats = cursor
				.for_each(crate::kvs::NORMAL_BATCH_SIZE, &mut |_| {
					Ok(std::ops::ControlFlow::Continue(()))
				})
				.await
				.expect("reads");
			if stats.rows == 0 {
				break;
			}
			serial += stats.rows as usize;
		}
		drop(cursor);
		let cursor_time = started.elapsed();
		let started = web_time::Instant::now();
		let whole = txn.count(range.clone(), None).await.expect("count");
		let whole_time = started.elapsed();
		let started = web_time::Instant::now();
		let parts = super::count::count_keys(&ctx, range, None).await.expect("count");
		let parts_time = started.elapsed();
		assert_eq!((serial, whole), (parts, parts));
		println!(
			"{parts} keys: cursor {cursor_time:?}, one count {whole_time:?}, in parts {parts_time:?}"
		);
	}
}

/// A `FOR` loop checks its deadline at its yield points, not on each
/// iteration: the clock is read about once a quantum.
#[tokio::test]
async fn a_for_loop_reads_the_clock_once_a_quantum() {
	const LARGE: usize = 1024;
	let db = TestDb::new_with_config(
		"",
		surrealdb_cnf::ConfigMap::empty().with_key_value("vm_quantum", LARGE.to_string()),
	)
	.await;
	let session = TestDb::owner();
	super::fuel::INTERRUPT_CHECKS.with(|checks| checks.set(0));
	let responses = db
		.datastore()
		.execute(
			&format!(
				"SELECT VALUE {{ FOR $i IN 0..{ROWS} {{ LET $x = $i + 1; }}; RETURN 1 }} FROM [1] TIMEOUT 1m"
			),
			&session,
			None,
		)
		.await
		.expect("the query runs");
	responses.into_iter().next().expect("one response").result.expect("the loop runs to its end");
	let checks = super::fuel::INTERRUPT_CHECKS.with(std::cell::Cell::get);
	assert!(checks > 0, "the deadline was never checked");
	// An iteration spends a few units, so a quantum holds a few hundred.
	assert!(
		checks <= ROWS / 10,
		"the deadline was checked {checks} times over {ROWS} iterations under a quantum of {LARGE}"
	);
}

/// A region's workers each draw on their own thread's fuel and yield
/// through the statement's scheduler, as its own task does.
#[tokio::test(flavor = "multi_thread")]
async fn a_region_yields_once_per_quantum() {
	let db = TestDb::new_with_config(&rows_setup(), config(true)).await;
	let handed = super::region::HANDED_TO_WORKERS.load(Ordering::Relaxed);
	for sql in ["SELECT n, $this.s AS s FROM t", "SELECT m, count() AS c FROM t GROUP BY m"] {
		assert_yields(&db, sql, ROWS).await;
	}
	assert!(
		super::region::HANDED_TO_WORKERS.load(Ordering::Relaxed) > handed,
		"no batch ran on a worker"
	);
}

/// A body run inside the storage cursor holds no backward jump, so the
/// interpreter never yields inside the cursor.
#[tokio::test]
async fn a_body_run_in_place_holds_no_backward_jump() {
	let db = TestDb::new(&rows_setup()).await;
	let ctx = db.exec_ctx().await;
	for sql in [
		"SELECT * FROM t",
		"SELECT n FROM t WHERE m = 3",
		"SELECT * FROM t ORDER BY n LIMIT 3",
		"SELECT m, count() FROM t GROUP BY m",
	] {
		let bound = bind(&ctx, sql).await;
		let physical =
			super::optimize::optimize(super::map::map(&planner(&ctx), &bound).await.expect("maps"));
		let program = super::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
		for pipeline in program.pipelines.iter().filter(|p| p.in_place) {
			assert!(!super::lower::jumps_back(&pipeline.code), "`{sql}` runs a loop in place");
		}
	}
}

/// A statement that does no I/O is stopped by its `TIMEOUT` within a quantum
/// of its deadline, rather than running to its end.
#[tokio::test]
async fn a_cpu_bound_statement_stops_at_its_deadline() {
	let db = TestDb::new("").await;
	let session = TestDb::owner();
	let started = web_time::Instant::now();
	let responses = db
		.datastore()
		.execute(
			"SELECT VALUE { FOR $i IN 0..1000000000 { LET $x = $i + 1; }; RETURN 1 } FROM [1] TIMEOUT 100ms",
			&session,
			None,
		)
		.await
		.expect("the query runs");
	let elapsed = started.elapsed();
	let error = responses
		.into_iter()
		.next()
		.expect("one response")
		.result
		.expect_err("the statement times out");
	assert!(error.to_string().contains("exceeded the timeout"), "{error}");
	// A billion iterations take minutes; a quantum takes microseconds, so
	// the margin is the scheduler's, not the statement's.
	assert!(elapsed < std::time::Duration::from_secs(1), "stopped {elapsed:?} after starting");
}
