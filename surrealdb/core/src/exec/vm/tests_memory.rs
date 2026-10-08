//! Bounded memory: a statement's breakers spill once their query's lease no
//! longer covers them, and answer what the same statement answers with
//! memory to spare — under a small budget, under a grant lowered while the
//! statement runs, under a merge of more runs than its read buffers, and
//! beside other queries sharing the node.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use super::tests::{bind, outcome, planner};
use crate::exec::memory::{FairGovernor, Lease, MemTempStore, MemoryGovernor, TempStore};
use crate::exec::operators::test_util::TestDb;
use crate::exec::scheduler::{Scheduler, YieldFuture};
use crate::val::Value;

/// Rows of `t`: a key of few values with many ties, a string key, and a
/// payload, so the table outgrows a small budget many times over.
const SETUP: &str = "
	DEFINE TABLE t;
	FOR $i IN 0..3000 {
		CREATE type::record('t', $i) SET
			n = $i,
			k = $i % 7,
			g = <string> ($i % 50),
			s = <string> ($i % 13),
			pad = string::repeat('x', 64 + $i % 17)
	};
	DEFINE TABLE u;
	FOR $i IN 0..400 { CREATE type::record('u', $i) SET m = $i % 50, w = <string> $i };
";

/// The statements each test runs: sorts that keep every row, ties included.
const SORTS: &[&str] = &[
	"SELECT * FROM t ORDER BY k",
	"SELECT n, k FROM t ORDER BY k DESC, s",
	"SELECT VALUE n FROM t ORDER BY s COLLATE, k",
	"SELECT n FROM t WHERE n % 3 = 0 ORDER BY k",
	"SELECT n, pad FROM t ORDER BY pad, n DESC",
	"SELECT n FROM t ORDER BY k LIMIT 5 START 2000",
];

/// Aggregates grouped by keys of many values, so their groups outgrow a
/// small budget, with every kind of accumulator state and first values.
const GROUPS: &[&str] = &[
	"SELECT g, count() AS c, math::sum(n) AS s FROM t GROUP BY g",
	"SELECT n % 500 AS b, count() AS c, math::mean(n) AS m, math::min(n) AS lo, math::max(n) AS hi FROM t GROUP BY b",
	"SELECT n % 300 AS b, math::stddev(n) AS d, math::variance(n) AS v, math::median(n) AS md FROM t GROUP BY b",
	"SELECT n % 400 AS b, array::group(k) AS ks, array::distinct(s) AS ss, array::min(pad) AS p FROM t GROUP BY b",
	"SELECT n % 250 AS b, k, s, pad FROM t GROUP BY b",
	"SELECT k, s, count() AS c, array::join(g, ',') AS j FROM t GROUP BY k, s",
	"SELECT n % 600 AS b, count() AS c FROM t GROUP BY b ORDER BY c DESC, b LIMIT 7",
];

/// A configuration for a small budget: parts of 1 KiB, a merge of `fan_in`
/// runs at once, and a floor and chunk of a few KiB.
fn small(fan_in: usize) -> surrealdb_cnf::ConfigMap {
	surrealdb_cnf::ConfigMap::empty()
		.with_key_value("spill_part_size", "1024")
		.with_key_value("spill_merge_fan_in", fan_in.to_string())
		.with_key_value("query_memory_minimum", "4096")
		.with_key_value("memory_reserve_chunk", "4096")
		.with_key_value("region_row_threshold", usize::MAX.to_string())
}

/// A governor that grants every request while a database is set up, and
/// defers to `inner` once the tests run on it: a setup's writes are not what
/// a test bounds.
struct SetUpFreely<G> {
	setting_up: std::sync::atomic::AtomicBool,
	inner: Arc<G>,
}

impl<G: MemoryGovernor> MemoryGovernor for SetUpFreely<G> {
	fn opened(&self, lease: &Arc<Lease>) {
		self.inner.opened(lease);
	}

	fn grow(&self, lease: &Lease, bytes: usize) -> bool {
		if self.setting_up.load(Ordering::Relaxed) {
			lease.set_grant(lease.grant().saturating_add(bytes));
			return true;
		}
		self.inner.grow(lease, bytes)
	}

	fn closed(&self, lease: &Lease) {
		self.inner.closed(lease);
	}
}

/// A database set up with `setup`, then bounded by `governor` and spilling
/// to `store`.
async fn bounded_on(
	governor: Arc<impl MemoryGovernor>,
	store: Arc<impl TempStore>,
	setup: &str,
	config: surrealdb_cnf::ConfigMap,
) -> TestDb {
	let freely = Arc::new(SetUpFreely {
		setting_up: std::sync::atomic::AtomicBool::new(true),
		inner: governor,
	});
	let builder = crate::kvs::Datastore::builder()
		.with_memory_governor(Arc::clone(&freely) as Arc<dyn MemoryGovernor>)
		.with_temp_store(store);
	let db = TestDb::new_on(builder, setup, config).await;
	freely.setting_up.store(false, Ordering::Relaxed);
	db
}

/// A database of [`SETUP`] over `governor` and `store`.
async fn db_on(
	governor: Arc<impl MemoryGovernor>,
	store: Arc<impl TempStore>,
	config: surrealdb_cnf::ConfigMap,
) -> TestDb {
	bounded_on(governor, store, SETUP, config).await
}

/// The rows `sql` answers over `db`, under `scheduler` where given.
async fn answer(
	db: &TestDb,
	sql: &str,
	scheduler: Option<Arc<dyn Scheduler>>,
) -> Result<Vec<Value>, String> {
	let ctx = match scheduler {
		Some(scheduler) => db.exec_ctx_scheduled(scheduler).await,
		None => db.exec_ctx().await,
	};
	let bound = bind(&ctx, sql).await;
	let vm = super::plan(&planner(&ctx), &bound).await.unwrap_or_else(|| panic!("plans `{sql}`"));
	outcome(&vm, &ctx).await
}

/// The rows each of `statements` answers with memory to spare.
async fn unbounded(statements: &[&str]) -> Vec<Result<Vec<Value>, String>> {
	let db = TestDb::new_with_config(SETUP, small(8)).await;
	let mut answers = Vec::new();
	for sql in statements {
		let rows = answer(&db, sql, None).await;
		assert!(rows.as_ref().is_ok_and(|rows| !rows.is_empty()), "`{sql}` answers {rows:?}");
		answers.push(rows);
	}
	answers
}

/// A governor that keeps what the node's leases held at most, and every
/// lease it opened, for a test to lower.
struct Watching {
	inner: FairGovernor,
	peak: AtomicUsize,
	leases: Mutex<Vec<Weak<Lease>>>,
	/// Set once the leases are lowered: no lease grows past its minimum
	/// again, as on a node that stays under pressure.
	pressed: std::sync::atomic::AtomicBool,
}

impl Watching {
	fn new(limit: usize) -> Arc<Self> {
		Arc::new(Self {
			inner: FairGovernor::new(limit),
			peak: AtomicUsize::new(0),
			leases: Mutex::new(Vec::new()),
			pressed: Default::default(),
		})
	}

	fn held(&self) -> usize {
		let leases = self.leases.lock().expect("leases");
		let open: Vec<_> = leases.iter().filter_map(Weak::upgrade).collect();
		drop(leases);
		open.iter().map(|lease| lease.held()).sum()
	}

	fn note(&self) {
		self.peak.fetch_max(self.held(), Ordering::Relaxed);
	}

	/// Lower every open lease to its minimum, as a node under pressure does.
	fn lower_all(&self) {
		let leases = self.leases.lock().expect("leases");
		let open: Vec<_> = leases.iter().filter_map(Weak::upgrade).collect();
		drop(leases);
		self.pressed.store(true, Ordering::Relaxed);
		for lease in &open {
			lease.set_grant(0);
		}
	}
}

impl MemoryGovernor for Watching {
	fn opened(&self, lease: &Arc<Lease>) {
		self.leases.lock().expect("leases").push(Arc::downgrade(lease));
		self.inner.opened(lease);
	}

	fn grow(&self, lease: &Lease, bytes: usize) -> bool {
		self.note();
		if self.pressed.load(Ordering::Relaxed) {
			return false;
		}
		let grown = self.inner.grow(lease, bytes);
		self.note();
		grown
	}

	fn closed(&self, lease: &Lease) {
		self.inner.closed(lease);
	}
}

/// Every sort run under a budget a fraction of the table's size spills, and
/// answers what it answers with memory to spare; what the query holds stays
/// within the node's limit and one batch over it.
#[tokio::test]
async fn sorts_under_a_small_budget_spill_and_answer_as_unbounded() {
	let expected = unbounded(SORTS).await;
	let limit = 64 << 10;
	let governor = Watching::new(limit);
	let store = Arc::new(MemTempStore::new(1024, None));
	let db = db_on(Arc::clone(&governor), Arc::clone(&store), small(8)).await;
	let before = store.runs_written();
	for (sql, expected) in SORTS.iter().zip(&expected) {
		assert_eq!(&answer(&db, sql, None).await, expected, "`{sql}` under a small budget");
	}
	assert!(store.runs_written() > before, "no sort spilled");
	assert_eq!(store.runs(), 0, "a spilled run outlived its query");
	let peak = governor.peak.load(Ordering::Relaxed);
	assert!(peak <= limit, "the node's leases held {peak} bytes over a limit of {limit}");
}

/// The rows `sql` answers over `db` as a request whose rows leave encoded in
/// `format`: each statement's encoded rows, or its value or error.
async fn encoded(db: &TestDb, sql: &str, format: surrealdb_rpc::RowFormat) -> Vec<String> {
	use surrealdb_rpc::EncodedPayload;
	let results = crate::rpc::format::encoded::execute_encoded(
		db.datastore(),
		sql,
		&TestDb::owner(),
		None,
		format,
	)
	.await
	.unwrap_or_else(|e| panic!("`{sql}` runs: {e}"));
	results
		.into_iter()
		.map(|result| match result.result {
			Ok(EncodedPayload::Rows(rows)) => format!("rows {:?}", rows.bytes()),
			Ok(EncodedPayload::Value(value)) => format!("value {value:?}"),
			Err(error) => format!("error {error}"),
		})
		.collect()
}

/// A sort whose rows leave encoded, those read in place held as their stored
/// bytes, spills under a small budget and encodes the bytes it encodes with
/// memory to spare, in CBOR and JSON.
#[tokio::test]
async fn sorts_with_encoded_rows_spill_and_encode_as_unbounded() {
	use surrealdb_rpc::RowFormat;
	let free = TestDb::new_with_config(SETUP, small(8)).await;
	let store = Arc::new(MemTempStore::new(1024, None));
	let db = db_on(Watching::new(64 << 10), Arc::clone(&store), small(8)).await;
	for statements in [SORTS, &GROUPS[..2]] {
		let before = store.runs_written();
		for sql in statements {
			for format in [RowFormat::Cbor, RowFormat::Json] {
				let expected = encoded(&free, sql, format).await;
				assert!(expected.iter().all(|r| r.starts_with("rows")), "`{sql}`: {expected:?}");
				assert_eq!(encoded(&db, sql, format).await, expected, "`{sql}` ({format:?})");
			}
		}
		assert!(store.runs_written() > before, "none of {statements:?} spilled");
		assert_eq!(store.runs(), 0, "a spilled run outlived its query");
	}
}

/// Every aggregate grouped by a key, run under a small budget and under
/// regions of tiny batches, spills its groups and answers as unbounded.
#[tokio::test]
async fn groups_under_a_small_budget_spill_and_answer_as_unbounded() {
	let expected = unbounded(GROUPS).await;
	for regions in [false, true] {
		let governor = Watching::new(48 << 10);
		let store = Arc::new(MemTempStore::new(1024, None));
		let mut config = small(4);
		if regions {
			config = config.with_key_value("region_row_threshold", "1");
		}
		let db = db_on(governor, Arc::clone(&store), config).await;
		let before = store.runs_written();
		for (sql, expected) in GROUPS.iter().zip(&expected) {
			assert_eq!(&answer(&db, sql, None).await, expected, "`{sql}` (regions: {regions})");
		}
		assert!(store.runs_written() > before, "no aggregate spilled (regions: {regions})");
		assert_eq!(store.runs(), 0, "a spilled run outlived its query");
	}
}

/// A merge of more runs than its query's read buffers takes several passes,
/// and answers as one merge does.
#[tokio::test]
async fn a_merge_of_more_runs_than_its_read_buffers_takes_several_passes() {
	let statements = &SORTS[..3];
	let expected = unbounded(statements).await;
	let governor = Arc::new(FairGovernor::new(32 << 10));
	let store = Arc::new(MemTempStore::new(1024, None));
	let db = db_on(governor, store, small(2)).await;
	let before = super::spill::MERGE_PASSES.load(Ordering::Relaxed);
	for (sql, expected) in statements.iter().zip(&expected) {
		assert_eq!(&answer(&db, sql, None).await, expected, "`{sql}` merged in passes");
	}
	assert!(super::spill::MERGE_PASSES.load(Ordering::Relaxed) > before, "no merge took a pass");
}

/// A query at its minimum grant, which is no more than its spill headroom
/// and a small floor, still spills and answers: the headroom was reserved
/// when its lease opened.
#[tokio::test]
async fn a_query_at_its_minimum_grant_still_spills() {
	let statements = &SORTS[..2];
	let expected = unbounded(statements).await;
	// A limit below any lease's minimum: every grant stays at it.
	let governor = Arc::new(FairGovernor::new(1));
	let store = Arc::new(MemTempStore::new(1024, None));
	let db = db_on(governor, Arc::clone(&store), small(4)).await;
	let before = store.runs_written();
	for (sql, expected) in statements.iter().zip(&expected) {
		assert_eq!(&answer(&db, sql, None).await, expected, "`{sql}` at its minimum");
	}
	assert!(store.runs_written() > before, "no sort spilled");
}

/// A scheduler that lowers every lease of its governor to its minimum at its
/// `at`th yield.
struct Lowering {
	governor: Arc<Watching>,
	yields: AtomicUsize,
	at: usize,
}

impl Scheduler for Lowering {
	fn yield_now(&self) -> YieldFuture {
		if self.yields.fetch_add(1, Ordering::Relaxed) + 1 == self.at {
			self.governor.lower_all();
		}
		Box::pin(tokio::task::yield_now())
	}
}

/// A grant lowered while a sort runs, observed at a quantum boundary, makes
/// the sort spill what it holds, and the sort still answers as unbounded.
#[tokio::test]
async fn a_grant_lowered_mid_sort_spills_and_answers_as_unbounded() {
	let statements = &SORTS[..2];
	let expected = unbounded(statements).await;
	// A limit the table fits in, so nothing spills until the grant is lowered.
	let governor = Watching::new(1 << 30);
	let store = Arc::new(MemTempStore::new(1024, None));
	let config = small(8).with_key_value("vm_quantum", "64");
	let db = db_on(Arc::clone(&governor), Arc::clone(&store), config).await;
	for (sql, expected) in statements.iter().zip(&expected) {
		governor.pressed.store(false, Ordering::Relaxed);
		let before = store.runs_written();
		let lowering = Arc::new(Lowering {
			governor: Arc::clone(&governor),
			yields: AtomicUsize::new(0),
			at: 20,
		});
		let got = answer(&db, sql, Some(lowering as Arc<dyn Scheduler>)).await;
		assert_eq!(&got, expected, "`{sql}` with its grant lowered");
		assert!(store.runs_written() > before, "`{sql}` did not spill once lowered");
	}
}

/// Queries running at once under one governor share its limit: the grants'
/// sum never passes it, and each answers as unbounded.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_queries_stay_within_the_node_limit() {
	let statements = &SORTS[..3];
	let expected = Arc::new(unbounded(statements).await);
	let limit = 256 << 10;
	let governor = Watching::new(limit);
	let store = Arc::new(MemTempStore::new(1024, None));
	let db = Arc::new(db_on(Arc::clone(&governor), store, small(4)).await);
	let mut tasks = Vec::new();
	for task in 0..8 {
		let db = Arc::clone(&db);
		let expected = Arc::clone(&expected);
		let sql = statements[task % statements.len()];
		tasks.push(tokio::spawn(async move {
			let got = answer(&db, sql, None).await;
			assert_eq!(got, expected[task % expected.len()], "`{sql}` beside other queries");
		}));
	}
	for task in tasks {
		task.await.expect("a query task");
	}
	assert!(governor.inner.granted() <= limit);
	let peak = governor.peak.load(Ordering::Relaxed);
	// What the leases hold may pass the limit by what lowered leases hold
	// until their next flush.
	assert!(peak <= 2 * limit, "the node's leases held {peak} bytes over a limit of {limit}");
}

/// A query that spills more than its store's quota fails with an error that
/// names the quota, rather than spilling on.
#[tokio::test]
async fn a_spill_past_the_store_quota_fails_the_statement() {
	let governor = Arc::new(FairGovernor::new(1));
	let store = Arc::new(MemTempStore::new(1024, Some(4096)));
	let db = db_on(governor, Arc::clone(&store), small(4)).await;
	let error = answer(&db, SORTS[0], None).await.expect_err("a sort past its quota answered");
	assert!(error.contains("quota of 4096 bytes"), "{error}");
	assert_eq!(store.runs(), 0, "a spilled run outlived its failed query");
}

/// A subquery's rows are gathered into one value, which cannot be spilled: a
/// gather its lease no longer covers, with the lease at its minimum, fails
/// with the memory threshold's error rather than growing, and the same
/// statement with memory to spare answers.
#[tokio::test]
async fn a_gather_past_a_lease_at_its_minimum_fails_with_the_memory_error() {
	let sql = "RETURN array::len((SELECT * FROM t))";
	let expected = unbounded(&[sql]).await;
	assert_eq!(expected[0], Ok(vec![Value::from(3000)]));
	let governor = Arc::new(FairGovernor::new(1));
	let store = Arc::new(MemTempStore::new(1024, None));
	let db = db_on(governor, store, small(4)).await;
	let error = answer(&db, sql, None).await.expect_err("a gather past its lease answered");
	assert!(error.contains("memory threshold"), "{error}");
}

/// `ORDER BY RAND()` with no limit spills under a small budget: each row
/// takes a random key as it is spilled or merged, and the merge hands on
/// every row once, in an order that is not the table's.
#[tokio::test]
async fn a_random_order_spills_and_hands_on_every_row_once() {
	let sql = "SELECT VALUE n FROM t ORDER BY RAND()";
	let governor = Arc::new(FairGovernor::new(32 << 10));
	let store = Arc::new(MemTempStore::new(1024, None));
	let db = db_on(governor, Arc::clone(&store), small(4)).await;
	let before = store.runs_written();
	let rows = answer(&db, sql, None).await.expect("a random order answers");
	assert!(store.runs_written() > before, "a random order did not spill");
	let mut sorted = rows.clone();
	sorted.sort();
	assert_eq!(sorted, (0..3000).map(Value::from).collect::<Vec<_>>());
	assert_ne!(rows, sorted, "the rows came in the table's order");
}

/// People who know one another, so a GQL match reaches each person many
/// times: `knows` edges from every person to five others, with ids that
/// order the same in every datastore.
#[cfg(feature = "gql")]
const PEOPLE: &str = "
	FOR $i IN 0..600 {
		CREATE type::record('person', $i) SET name = string::concat('p', <string> $i, string::repeat('-', 48))
	};
	FOR $i IN 0..600 {
		FOR $j IN 1..6 {
			INSERT RELATION INTO knows {
				id: [$i, $j],
				in: type::record('person', $i),
				out: type::record('person', ($i * 7 + $j * 13) % 600)
			} RETURN NONE
		}
	};
";

/// The rows a GQL statement answers over `db`.
#[cfg(feature = "gql")]
async fn gql(db: &TestDb, text: &str) -> Vec<Result<String, String>> {
	use surrealdb_types::ToSql;

	use crate::kvs::request::{Dialect, QueryRequest, QuerySource};
	db.datastore()
		.run(QueryRequest::new(QuerySource::in_dialect(text, Dialect::Gql), &TestDb::owner()))
		.await
		.expect("the query runs")
		.into_iter()
		.map(|result| result.result.map(|value| value.to_sql()).map_err(|e| e.to_string()))
		.collect()
}

/// A GQL `RETURN DISTINCT` over more rows than a small budget holds spills
/// its seen rows and defers the rest, and answers the rows in the order the
/// in-memory set answers them, a limit included.
#[cfg(feature = "gql")]
#[tokio::test]
async fn a_gql_distinct_spills_and_keeps_its_order() {
	let statements = [
		"MATCH (a:person)-[:knows]->(b:person) RETURN DISTINCT b.name AS name",
		"MATCH (a:person)-[:knows]->(b:person) RETURN DISTINCT b.name AS name, a.name AS src",
		"MATCH (a:person)-[:knows]->(b:person) RETURN DISTINCT b.name AS name LIMIT 450",
	];
	let free = TestDb::new_with_config(PEOPLE, small(4)).await;
	let store = Arc::new(MemTempStore::new(1024, None));
	let governor = Arc::new(FairGovernor::new(16 << 10));
	let bounded = bounded_on(governor, Arc::clone(&store), PEOPLE, small(4)).await;
	for text in statements {
		let expected = gql(&free, text).await;
		assert!(expected.iter().all(Result::is_ok), "`{text}` answers {expected:?}");
		let before = store.runs_written();
		assert_eq!(gql(&bounded, text).await, expected, "`{text}` under a small budget");
		assert!(store.runs_written() > before, "`{text}` did not spill");
	}
	// A merge reads no more runs at once than the fan-in the headroom holds.
	assert!(store.most_read_at_once() <= 4, "read {} runs at once", store.most_read_at_once());
	assert_eq!(store.runs(), 0, "a spilled run outlived its query");
}

/// A GQL hash join whose build side outgrows a small budget spills it,
/// defers its probe rows, and answers the rows in the order the in-memory
/// join answers them, a residual and a limit included.
#[cfg(feature = "gql")]
#[tokio::test]
async fn a_gql_join_spills_its_build_and_keeps_its_order() {
	let statements = [
		"MATCH (a:person)-[:knows]->(b:person), (c:person)-[:knows]->(b:person) RETURN a.name AS a, c.name AS c",
		"MATCH (a:person)-[:knows]->(b:person), (c:person)-[:knows]->(b:person) WHERE a.name <> c.name RETURN a.name AS a, b.name AS b LIMIT 900",
	];
	let free = TestDb::new_with_config(PEOPLE, small(4)).await;
	let store = Arc::new(MemTempStore::new(1024, None));
	let governor = Arc::new(FairGovernor::new(16 << 10));
	let bounded = bounded_on(governor, Arc::clone(&store), PEOPLE, small(4)).await;
	for text in statements {
		let expected = gql(&free, text).await;
		assert!(expected.iter().all(Result::is_ok), "`{text}` answers {expected:?}");
		let before = store.runs_written();
		let got = gql(&bounded, text).await;
		assert!(got == expected, "`{text}` under a small budget answered differently");
		assert!(store.runs_written() > before, "`{text}` did not spill");
	}
	// A merge reads no more runs at once than the fan-in the headroom holds.
	assert!(store.most_read_at_once() <= 4, "read {} runs at once", store.most_read_at_once());
	assert_eq!(store.runs(), 0, "a spilled run outlived its query");
}

/// A transaction's change-feed buffer holds every change until it commits
/// and cannot be spilled: a write whose buffered changes outgrow a lease at
/// its minimum fails with the memory threshold's error, and the same write
/// with memory to spare succeeds.
#[tokio::test]
async fn a_change_feed_past_a_lease_at_its_minimum_fails_with_the_memory_error() {
	let setup = "DEFINE TABLE w CHANGEFEED 1h;";
	let sql = "FOR $i IN 0..400 { CREATE w SET pad = string::repeat('x', 1000) }; RETURN count(SELECT * FROM w)";
	let free = TestDb::new_with_config(setup, small(4)).await;
	let answers = free.datastore().execute(sql, &TestDb::owner(), None).await.expect("runs");
	assert!(answers.into_iter().all(|r| r.result.is_ok()), "the unbounded write failed");
	let governor = Arc::new(FairGovernor::new(1));
	let store = Arc::new(MemTempStore::new(1024, None));
	let bounded = bounded_on(governor, store, setup, small(4)).await;
	let answers = bounded.datastore().execute(sql, &TestDb::owner(), None).await.expect("runs");
	let error = answers[0].result.as_ref().expect_err("a change feed past its lease was buffered");
	assert!(error.to_string().contains("memory threshold"), "{error}");
}

/// The rows a statement in `dialect` answers over `db`.
#[cfg(feature = "postgres")]
async fn in_dialect(
	db: &TestDb,
	dialect: crate::kvs::request::Dialect,
	text: &str,
) -> Vec<Result<String, String>> {
	use surrealdb_types::ToSql;

	use crate::kvs::request::{QueryRequest, QuerySource};
	db.datastore()
		.run(QueryRequest::new(QuerySource::in_dialect(text, dialect), &TestDb::owner()))
		.await
		.expect("the query runs")
		.into_iter()
		.map(|result| result.result.map(|value| value.to_sql()).map_err(|e| e.to_string()))
		.collect()
}

/// A Postgres-dialect join whose build side outgrows a small budget spills
/// it, defers its probe rows, and answers in the order the in-memory join
/// answers, a left join, a residual and a limit included.
#[cfg(feature = "postgres")]
#[tokio::test]
async fn a_sql_join_spills_its_build_and_keeps_its_order() {
	use crate::kvs::request::Dialect;
	let statements = [
		"SELECT u.w, t.n, t.g FROM u JOIN t ON u.m = t.k",
		"SELECT u.w, t.n FROM u LEFT JOIN t ON u.m = t.n WHERE u.m < 40",
		"SELECT u.w, t.pad FROM u JOIN t ON u.m = t.k AND t.n > u.m LIMIT 700",
	];
	let free = TestDb::new_with_config(SETUP, small(4)).await;
	let store = Arc::new(MemTempStore::new(1024, None));
	let governor = Arc::new(FairGovernor::new(16 << 10));
	let bounded = bounded_on(governor, Arc::clone(&store), SETUP, small(4)).await;
	for text in statements {
		let expected = in_dialect(&free, Dialect::PgSql, text).await;
		assert!(expected.iter().all(Result::is_ok), "`{text}` answers {expected:?}");
		let before = store.runs_written();
		let got = in_dialect(&bounded, Dialect::PgSql, text).await;
		assert!(got == expected, "`{text}` under a small budget answered differently");
		assert!(store.runs_written() > before, "`{text}` did not spill");
	}
	assert!(store.most_read_at_once() <= 4, "read {} runs at once", store.most_read_at_once());
	assert_eq!(store.runs(), 0, "a spilled run outlived its query");
}

/// A transaction's write buffer holds what it writes until it commits and
/// cannot be spilled: a write past a lease at its minimum fails with the
/// memory threshold's error, and the same write with memory to spare
/// succeeds.
#[tokio::test]
async fn a_write_buffer_past_a_lease_at_its_minimum_fails_with_the_memory_error() {
	let sql = "FOR $i IN 0..400 { CREATE w SET pad = string::repeat('x', 1000) }; RETURN count(SELECT * FROM w)";
	let free = TestDb::new_with_config("", small(4)).await;
	let answers = free.datastore().execute(sql, &TestDb::owner(), None).await.expect("runs");
	assert!(answers.into_iter().all(|r| r.result.is_ok()), "the unbounded write failed");
	let governor = Arc::new(FairGovernor::new(1));
	let store = Arc::new(MemTempStore::new(1024, None));
	let bounded = bounded_on(governor, store, "", small(4)).await;
	let answers = bounded.datastore().execute(sql, &TestDb::owner(), None).await.expect("runs");
	let error = answers[0].result.as_ref().expect_err("a write buffer past its lease was kept");
	assert!(error.to_string().contains("memory threshold"), "{error}");
}
