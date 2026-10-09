//! Bounded memory: a statement's breakers spill once their query's lease no
//! longer covers them, and answer what the same statement answers with
//! memory to spare — under a small budget, under a grant lowered while the
//! statement runs, under a merge of more runs than its read buffers, and
//! beside other queries sharing the node.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use super::tests::{bind, outcome, planner};
use crate::exec::memory::runs::BreakerKind;
use crate::exec::memory::{
	Admit, BoxFut, FairGovernor, Lease, MemTempStore, MemoryGovernor, TempStore,
};
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
pub(super) struct SetUpFreely<G> {
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

	fn admit(&self, lease: &Lease, bytes: usize, may_wait: bool) -> Admit {
		if self.setting_up.load(Ordering::Relaxed) {
			lease.reserve_pinned(bytes);
			return Admit::Granted;
		}
		self.inner.admit(lease, bytes, may_wait)
	}

	fn changed(&self, epoch: u64) -> BoxFut<'_, ()> {
		self.inner.changed(epoch)
	}

	fn released(&self, lease: &Lease) {
		self.inner.released(lease);
	}

	fn overcommitted(&self, lease: &Lease, bytes: usize) {
		MemoryGovernor::overcommitted(&*self.inner, lease, bytes);
	}

	fn unwait(&self, lease: &Lease) {
		self.inner.unwait(lease);
	}

	fn limit(&self) -> Option<usize> {
		MemoryGovernor::limit(&*self.inner)
	}
}

/// A database set up with `setup`, then bounded by `governor` and spilling
/// to `store`.
pub(super) async fn bounded_on(
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

/// A database set up with `setup`, then bounded by `governor`, whose queries
/// have no store to spill to.
async fn bounded_without_store(
	governor: Arc<impl MemoryGovernor>,
	setup: &str,
	config: surrealdb_cnf::ConfigMap,
) -> TestDb {
	let freely = Arc::new(SetUpFreely {
		setting_up: std::sync::atomic::AtomicBool::new(true),
		inner: governor,
	});
	let builder = crate::kvs::Datastore::builder()
		.with_memory_governor(Arc::clone(&freely) as Arc<dyn MemoryGovernor>)
		.without_temp_store();
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
pub(super) async fn answer(
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
	/// The most the open leases' grants summed to at one of the governor's
	/// requests, read under the governor's lock.
	peak_granted: AtomicUsize,
	/// The most pinned bytes the node's leases held at one of the governor's
	/// requests.
	peak_pinned: AtomicUsize,
	/// Whether a lease was ever seen granted less than its minimum.
	below_minimum: std::sync::atomic::AtomicBool,
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
			peak_granted: AtomicUsize::new(0),
			peak_pinned: AtomicUsize::new(0),
			below_minimum: Default::default(),
			leases: Mutex::new(Vec::new()),
			pressed: Default::default(),
		})
	}

	/// The leases open now.
	fn open(&self) -> Vec<Arc<Lease>> {
		let leases = self.leases.lock().expect("leases");
		let open: Vec<_> = leases.iter().filter_map(Weak::upgrade).collect();
		drop(leases);
		open
	}

	fn held(&self) -> usize {
		self.open().iter().map(|lease| lease.held()).sum()
	}

	/// The most pinned bytes any one open lease holds now.
	fn most_pinned(&self) -> usize {
		self.open().iter().map(|lease| lease.pinned()).max().unwrap_or(0)
	}

	fn note(&self) {
		let open = self.open();
		self.peak.fetch_max(open.iter().map(|lease| lease.held()).sum(), Ordering::Relaxed);
		self.peak_granted.fetch_max(self.inner.granted(), Ordering::Relaxed);
		self.peak_pinned
			.fetch_max(open.iter().map(|lease| lease.pinned()).sum(), Ordering::Relaxed);
		if open.iter().any(|lease| lease.grant() < lease.minimum()) {
			self.below_minimum.store(true, Ordering::Relaxed);
		}
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

	fn admit(&self, lease: &Lease, bytes: usize, may_wait: bool) -> Admit {
		self.note();
		let admitted = match self.pressed.load(Ordering::Relaxed) {
			true => {
				lease.force_pinned(bytes);
				MemoryGovernor::overcommitted(&self.inner, lease, lease.overcommit());
				Admit::Overcommit
			}
			false => self.inner.admit(lease, bytes, may_wait),
		};
		self.note();
		admitted
	}

	fn changed(&self, epoch: u64) -> BoxFut<'_, ()> {
		self.inner.changed(epoch)
	}

	fn released(&self, lease: &Lease) {
		self.inner.released(lease);
	}

	fn overcommitted(&self, lease: &Lease, bytes: usize) {
		MemoryGovernor::overcommitted(&self.inner, lease, bytes);
	}

	fn limit(&self) -> Option<usize> {
		MemoryGovernor::limit(&self.inner)
	}

	fn unwait(&self, lease: &Lease) {
		self.inner.unwait(lease);
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
/// sum never passes it, and each answers as unbounded. What the leases hold is
/// held to no fixed multiple of the limit: a lease reserves only within its
/// grant, but the governor lowers a grant below what the lease already holds as
/// leases open, and the lease keeps those bytes until its breaker's next flush,
/// so with many leases lowered in turn what they hold together passes the limit
/// by the sum of what each held when it was lowered. Only the grants, which the
/// governor sets under its lock, are held to the limit.
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
	assert_eq!(governor.inner.granted(), 0, "a lease outlived its query");
	let granted = governor.peak_granted.load(Ordering::Relaxed);
	assert!(granted > 0, "the governor was never asked");
	assert!(granted <= limit, "the leases were granted {granted} bytes over a limit of {limit}");
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
/// gather past the node's limit is granted past it and recorded as an
/// overcommit, answers as the same statement with memory to spare, and is
/// given back when the query ends.
#[tokio::test]
async fn a_gather_past_the_node_limit_is_overcommitted_and_answers() {
	let sql = "RETURN array::len((SELECT * FROM t))";
	let expected = unbounded(&[sql]).await;
	assert_eq!(expected[0], Ok(vec![Value::from(3000)]));
	let governor = Arc::new(FairGovernor::new(1));
	let store = Arc::new(MemTempStore::new(1024, None));
	let db = db_on(Arc::clone(&governor), store, small(4)).await;
	assert_eq!(answer(&db, sql, None).await, expected[0]);
	assert!(governor.overcommits() > 0, "the gather was not overcommitted");
	assert_eq!(governor.overcommitted(), 0, "an overcommit outlived its query");
}

/// The statement the concurrency tests run: one gather of every row of `t`.
const GATHER: &str = "RETURN array::len((SELECT * FROM t))";

/// What `GATHER` holds pinned while it runs: the weight of the rows it gathers.
async fn gather_bytes() -> usize {
	let rows = unbounded(&["SELECT * FROM t"]).await.remove(0).expect("the rows answer");
	crate::exec::memory::size::values_bytes(&rows)
}

/// A lower bound on what the leases of a node hold while `GATHER` runs alone:
/// what they held at the governor's last request.
async fn gather_peak() -> usize {
	let governor = Watching::new(1 << 30);
	let store = Arc::new(MemTempStore::new(1024, None));
	let db = db_on(Arc::clone(&governor), store, small(4)).await;
	assert_eq!(answer(&db, GATHER, None).await, Ok(vec![Value::from(3000)]));
	governor.peak.load(Ordering::Relaxed)
}

/// Two queries that each pin about 0.6 of the node's limit through a gather,
/// and fit together in no way, both answer: the older goes past the limit once
/// nothing that is running can give memory back, and the other borrows what it
/// returns. Neither fails, since each is well within its own ceiling.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_queries_each_needing_0_6_of_the_limit_both_answer() {
	let limit = gather_bytes().await * 5 / 3;
	let wait = 5000;
	let governor = Arc::new(FairGovernor::new(limit));
	let store = Arc::new(MemTempStore::new(1024, None));
	let config = small(4).with_key_value("memory_wait", wait.to_string());
	let db = Arc::new(db_on(Arc::clone(&governor), store, config).await);
	let started = std::time::Instant::now();
	let mut tasks = Vec::new();
	for _ in 0..2 {
		let db = Arc::clone(&db);
		tasks.push(tokio::spawn(async move { answer(&db, GATHER, None).await }));
	}
	for task in tasks {
		assert_eq!(task.await.expect("a query task"), Ok(vec![Value::from(3000)]));
	}
	assert!(
		started.elapsed()
			< std::time::Duration::from_millis(wait) + std::time::Duration::from_secs(10),
		"the queries took {:?}: a wait outlived its budget",
		started.elapsed()
	);
	assert!(governor.waits() + governor.overcommits() >= 1, "the queries never met");
	assert_eq!(governor.overcommitted(), 0, "an overcommit outlived its query");
}

/// A query that needs more than its fair share, while another lease holds
/// memory that is not spillable and does not give it back, waits for it rather
/// than being granted past the limit, and completes once the holder closes and
/// its memory returns to the node. The holder gives back down to its fair share
/// as the query grows within its own, which is the one run of overcommit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_query_waits_for_a_holder_to_give_memory_back() {
	// The gather needs two thirds of the limit, more than its fair share, and
	// what the holder keeps leaves it too little: past its fair share it can
	// only wait.
	let limit = gather_bytes().await * 3 / 2;
	let governor = Arc::new(FairGovernor::new(limit));
	let store = Arc::new(MemTempStore::new(1024, None));
	let config = small(4).with_key_value("memory_wait", "30000");
	let db = Arc::new(db_on(Arc::clone(&governor), store, config).await);
	let holder = Lease::open(&(Arc::clone(&governor) as Arc<dyn MemoryGovernor>), 1024, 0);
	let held = limit * 3 / 5;
	assert!(holder.reserve_pinned(held));
	let query = {
		let db = Arc::clone(&db);
		tokio::spawn(async move { answer(&db, GATHER, None).await })
	};
	for _ in 0..30_000 {
		if governor.waits() > 0 {
			break;
		}
		tokio::time::sleep(std::time::Duration::from_millis(1)).await;
	}
	assert!(governor.waits() > 0, "the query did not wait");
	assert!(!query.is_finished(), "the query answered while the holder held its memory");
	drop(holder);
	assert_eq!(query.await.expect("the query task"), Ok(vec![Value::from(3000)]));
	assert!(governor.overcommits() <= 1, "the query was granted past the limit");
	assert_eq!(governor.overcommitted(), 0, "an overcommit outlived its query");
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

/// A GQL `RETURN DISTINCT` with no store to spill to keeps its seen rows in
/// memory, accounted for: it is granted the memory past the node's limit,
/// recorded as an overcommit, and answers as the same statement with memory to
/// spare.
#[cfg(feature = "gql")]
#[tokio::test]
async fn a_distinct_with_no_store_is_accounted() {
	let text = "MATCH (a:person)-[:knows]->(b:person) RETURN DISTINCT b.name AS name";
	let free = TestDb::new_with_config(PEOPLE, small(4)).await;
	let expected = gql(&free, text).await;
	assert!(expected.iter().all(Result::is_ok), "`{text}` answers {expected:?}");
	let governor = Arc::new(FairGovernor::new(1));
	let bounded = bounded_without_store(Arc::clone(&governor), PEOPLE, small(4)).await;
	assert_eq!(gql(&bounded, text).await, expected);
	assert!(governor.overcommits() > 0, "the distinct was not accounted");
	assert_eq!(governor.overcommitted(), 0, "an overcommit outlived its query");
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

/// The statements of `sql` run over `db`, each answered as a value.
async fn run_all(db: &TestDb, sql: &str) -> Vec<Result<surrealdb_types::Value, String>> {
	db.datastore()
		.execute(sql, &TestDb::owner(), None)
		.await
		.expect("runs")
		.into_iter()
		.map(|result| result.result.map_err(|e| e.to_string()))
		.collect()
}

/// A transaction's change-feed buffer holds every change until it commits and
/// cannot be spilled: a write whose buffered changes outgrow the node's limit
/// is granted the memory past it, recorded as an overcommit, and commits with
/// every change in the feed, as the same write with memory to spare does.
#[tokio::test]
async fn a_change_feed_past_the_node_limit_is_overcommitted_and_commits() {
	let setup = "DEFINE TABLE w CHANGEFEED 1h;";
	let sql = "FOR $i IN 0..400 { CREATE w SET pad = string::repeat('x', 1000) }; RETURN count(SELECT * FROM w)";
	let changes = "SHOW CHANGES FOR TABLE w SINCE 0";
	let free = TestDb::new_with_config(setup, small(4)).await;
	let expected = run_all(&free, sql).await;
	assert!(expected.iter().all(Result::is_ok), "the unbounded write failed: {expected:?}");
	let expected_changes = run_all(&free, changes).await;
	let governor = Arc::new(FairGovernor::new(1));
	let store = Arc::new(MemTempStore::new(1024, None));
	let bounded = bounded_on(Arc::clone(&governor), store, setup, small(4)).await;
	let got = run_all(&bounded, sql).await;
	assert!(got.iter().all(Result::is_ok), "the bounded write failed: {got:?}");
	assert_eq!(got, expected);
	assert!(governor.overcommits() > 0, "the change feed was not overcommitted");
	assert_eq!(governor.overcommitted(), 0, "an overcommit outlived its query");
	let entries = |changes: &[Result<surrealdb_types::Value, String>]| match changes {
		[Ok(surrealdb_types::Value::Array(entries))] => entries.len(),
		other => panic!("changes answered {other:?}"),
	};
	let got_changes = run_all(&bounded, changes).await;
	assert!(entries(&expected_changes) > 0, "the feed holds no change");
	assert_eq!(entries(&got_changes), entries(&expected_changes));
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

/// A transaction's write buffer holds what it writes until it commits, and a
/// backend that does not bound it is counted against the query's memory: a
/// write past the node's limit is granted the memory past it, recorded as an
/// overcommit, and commits as the same write with memory to spare does.
#[tokio::test]
async fn a_write_buffer_past_the_node_limit_is_overcommitted_and_commits() {
	let sql = "FOR $i IN 0..400 { CREATE w SET pad = string::repeat('x', 1000) }; RETURN count(SELECT * FROM w)";
	let free = TestDb::new_with_config("", small(4)).await;
	let expected = run_all(&free, sql).await;
	assert!(expected.iter().all(Result::is_ok), "the unbounded write failed: {expected:?}");
	let governor = Arc::new(FairGovernor::new(1));
	let store = Arc::new(MemTempStore::new(1024, None));
	let bounded = bounded_on(Arc::clone(&governor), store, "", small(4)).await;
	let got = run_all(&bounded, sql).await;
	assert_eq!(got, expected);
	assert!(governor.overcommits() > 0, "the write buffer was not overcommitted");
	assert_eq!(governor.overcommitted(), 0, "an overcommit outlived its query");
}

/// A sort or an aggregate with no store to spill to holds what it holds: it
/// is granted the memory past the node's limit, recorded as an overcommit,
/// and answers as the same statement with memory to spare.
#[tokio::test]
async fn a_single_query_over_the_node_limit_answers() {
	let statements = [SORTS[0], SORTS[4], GROUPS[0], GROUPS[1]];
	let expected = unbounded(&statements).await;
	let governor = Arc::new(FairGovernor::new(1));
	let db = bounded_without_store(Arc::clone(&governor), SETUP, small(4)).await;
	for (sql, expected) in statements.iter().zip(&expected) {
		let before = governor.overcommits();
		assert_eq!(&answer(&db, sql, None).await, expected, "`{sql}` with no store");
		assert!(governor.overcommits() > before, "`{sql}` was not overcommitted");
	}
	assert_eq!(governor.overcommitted(), 0, "an overcommit outlived its query");
}

/// A setup that seeds no rows, for tests whose queries bind their own values: a
/// setup's writes run under the datastore's configuration, ceiling included.
const NO_ROWS: &str = "DEFINE TABLE t;";

/// Whether `error` is the memory threshold error a query raises past its
/// ceiling.
fn is_ceiling(error: &str) -> bool {
	error.contains("memory threshold")
}

/// A query that binds values past its ceiling fails with the memory threshold
/// error, from the statement that bound the value that took it past, and a
/// query run to completion while it is held, which holds nothing of the kind,
/// answers as unbounded does. The error is the ceiling's: the process memory
/// threshold is not set.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_query_binding_values_past_its_ceiling_fails_only_that_query() {
	assert_eq!(*surrealdb_cnf::MEMORY_THRESHOLD, 0, "the process guard is set");
	let ceiling: usize = 512 << 10;
	// Three values of two fifths of the ceiling each: two fit, three do not.
	let elements = ceiling * 2 / 5 / std::mem::size_of::<Value>();
	let setup = "DEFINE TABLE t; FOR $i IN 0..1000 { CREATE type::record('t', $i) SET n = $i };";
	let count = "SELECT count() FROM t GROUP ALL";
	let grow = format!(
		"RETURN {{ LET $a = array::range(0, {elements}); LET $b = array::range(0, {elements}); \
		 LET $c = array::range(0, {elements}); \
		 RETURN array::len($a) + array::len($b) + array::len($c) }}"
	);
	let config = small(4)
		.with_key_value("query_memory_max", ceiling.to_string())
		.with_key_value("vm_quantum", "1")
		.with_key_value("memory_wait", "5000");
	let free = TestDb::new_with_config(setup, config.clone()).await;
	let expected = answer(&free, count, None).await;
	assert!(expected.is_ok(), "the count answers {expected:?}");
	let governor = Watching::new(64 << 20);
	let store = Arc::new(MemTempStore::new(1024, None));
	let db = Arc::new(bounded_on(Arc::clone(&governor), store, setup, config).await);
	let pausing = Arc::new(Pausing {
		governor: Arc::clone(&governor),
		threshold: ceiling * 3 / 10,
		paused: Default::default(),
		reached: Default::default(),
		resume: Default::default(),
	});
	let holder = {
		let (db, pausing) = (Arc::clone(&db), Arc::clone(&pausing));
		tokio::spawn(async move { answer(&db, &grow, Some(pausing as Arc<dyn Scheduler>)).await })
	};
	pausing.reached.notified().await;
	assert_eq!(answer(&db, count, None).await, expected, "the query run beside the held one");
	pausing.resume.notify_one();
	let error = holder.await.expect("the held query").expect_err("values past the ceiling bound");
	assert!(is_ceiling(&error), "{error}");
	assert_eq!(governor.inner.overcommitted(), 0, "an overcommit outlived its query");
	assert_eq!(governor.inner.granted(), 0, "a lease outlived its query");
}

/// A loop body's bindings last one iteration: a body that binds a value within
/// the query's ceiling runs to the end however many iterations it takes, and a
/// body that binds values past the ceiling fails the query.
#[tokio::test]
async fn a_loop_bodys_bindings_last_one_iteration() {
	assert_eq!(*surrealdb_cnf::MEMORY_THRESHOLD, 0, "the process guard is set");
	let ceiling: usize = 512 << 10;
	// Two fifths of the ceiling per value: one fits, three do not.
	let elements = ceiling * 2 / 5 / std::mem::size_of::<Value>();
	let config = small(4).with_key_value("query_memory_max", ceiling.to_string());
	let governor = Watching::new(64 << 20);
	let store = Arc::new(MemTempStore::new(1024, None));
	let db = bounded_on(Arc::clone(&governor), store, NO_ROWS, config).await;
	let bind = format!("LET $a = array::range(0, {elements});");
	let within = format!("RETURN {{ FOR $i IN 0..8 {{ {bind} }}; RETURN 1 }}");
	let past = format!("RETURN {{ FOR $i IN 0..8 {{ {bind} {bind} {bind} }}; RETURN 1 }}");
	assert_eq!(answer(&db, &within, None).await, Ok(vec![Value::from(1)]));
	let error = answer(&db, &past, None).await.expect_err("values past the ceiling bound");
	assert!(is_ceiling(&error), "{error}");
	assert_eq!(
		answer(&db, &within, None).await,
		Ok(vec![Value::from(1)]),
		"the loop run after the failed one"
	);
	assert_eq!(governor.inner.overcommitted(), 0, "an overcommit outlived its query");
	assert_eq!(governor.inner.granted(), 0, "a lease outlived its query");
}

/// Each binding of a name in a block is charged, the copies its frame holds
/// as well as the slot's: a name bound many times in a block passes a ceiling
/// that the same name bound twice does not.
#[tokio::test]
async fn every_binding_of_a_name_in_a_block_is_charged() {
	assert_eq!(*surrealdb_cnf::MEMORY_THRESHOLD, 0, "the process guard is set");
	let ceiling: usize = 512 << 10;
	// A tenth of the ceiling per value: with the slot's, the frames' copies of
	// twelve bindings weigh 1.3 times the ceiling, and of two, 0.3 times it.
	let elements = ceiling / 10 / std::mem::size_of::<Value>();
	let config = small(4).with_key_value("query_memory_max", ceiling.to_string());
	let governor = Watching::new(64 << 20);
	let store = Arc::new(MemTempStore::new(1024, None));
	let db = bounded_on(Arc::clone(&governor), store, NO_ROWS, config).await;
	let bind = format!("LET $a = array::range(0, {elements});");
	let twice = format!("RETURN {{ {bind} {bind} RETURN array::len($a) }}");
	let many = format!("RETURN {{ {} RETURN array::len($a) }}", bind.repeat(12));
	assert_eq!(answer(&db, &twice, None).await, Ok(vec![Value::from(elements)]));
	let error = answer(&db, &many, None).await.expect_err("twelve copies bound");
	assert!(is_ceiling(&error), "{error}");
	assert_eq!(governor.inner.granted(), 0, "a lease outlived its query");
}

/// A loop whose body binds values past the query's ceiling fails the query
/// that holds them, from the binding that took it past, and only that query:
/// one that runs while it is held answers as unbounded does. A binding in a
/// loop body lasts its iteration, so what a loop fails by is what one
/// iteration binds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_loop_past_the_query_ceiling_fails_only_that_query() {
	assert_eq!(*surrealdb_cnf::MEMORY_THRESHOLD, 0, "the process guard is set");
	let ceiling: usize = 4 << 20;
	let setup = "DEFINE TABLE t; FOR $i IN 0..1000 { CREATE type::record('t', $i) SET n = $i };";
	let count = "SELECT count() FROM t GROUP ALL";
	// Each binding weighs about a MiB: four fit the ceiling, and a fifth in the
	// same iteration takes the loop past it.
	let grow = "RETURN { FOR $i IN 0..8 { LET $a = array::range(0, 18000); \
		LET $b = array::range(0, 18000); LET $c = array::range(0, 18000); \
		LET $d = array::range(0, 18000); LET $e = array::range(0, 18000); }; RETURN 1 }";
	let config = small(4)
		.with_key_value("query_memory_max", ceiling.to_string())
		.with_key_value("vm_quantum", "1")
		.with_key_value("memory_wait", "5000");
	let free =
		TestDb::new_with_config(setup, config.clone().with_key_value("query_memory_max", "1GiB"))
			.await;
	let expected = answer(&free, count, None).await;
	assert!(expected.is_ok(), "the count answers {expected:?}");
	assert_eq!(answer(&free, grow, None).await, Ok(vec![Value::from(1)]), "the loop unbounded");
	let governor = Watching::new(64 << 20);
	let store = Arc::new(MemTempStore::new(1024, None));
	let db = Arc::new(bounded_on(Arc::clone(&governor), store, setup, config).await);
	let pausing = Arc::new(Pausing {
		governor: Arc::clone(&governor),
		threshold: ceiling / 2,
		paused: Default::default(),
		reached: Default::default(),
		resume: Default::default(),
	});
	let holder = {
		let (db, pausing) = (Arc::clone(&db), Arc::clone(&pausing));
		tokio::spawn(async move { answer(&db, grow, Some(pausing as Arc<dyn Scheduler>)).await })
	};
	pausing.reached.notified().await;
	assert_eq!(answer(&db, count, None).await, expected, "the query run beside the held one");
	pausing.resume.notify_one();
	let error = holder.await.expect("the held query").expect_err("a loop past the ceiling");
	assert!(is_ceiling(&error), "{error}");
	assert_eq!(governor.inner.overcommitted(), 0, "an overcommit outlived its query");
	assert_eq!(governor.inner.granted(), 0, "a lease outlived its query");
}

/// A `FOR` loop holds the array it walks until it ends, so an array past the
/// query's ceiling fails the statement that starts the loop, and one within it
/// does not.
#[tokio::test]
async fn a_for_loop_over_an_array_past_the_ceiling_fails() {
	assert_eq!(*surrealdb_cnf::MEMORY_THRESHOLD, 0, "the process guard is set");
	let ceiling: usize = 512 << 10;
	let config = small(4).with_key_value("query_memory_max", ceiling.to_string());
	let governor = Watching::new(64 << 20);
	let store = Arc::new(MemTempStore::new(1024, None));
	let db = bounded_on(Arc::clone(&governor), store, NO_ROWS, config).await;
	let walk = |elements: usize| {
		format!("RETURN {{ FOR $i IN array::range(0, {elements}) {{ }}; RETURN 1 }}")
	};
	let fits = ceiling / 4 / std::mem::size_of::<Value>();
	assert_eq!(answer(&db, &walk(fits), None).await, Ok(vec![Value::from(1)]));
	let past = ceiling * 3 / 2 / std::mem::size_of::<Value>();
	let error = answer(&db, &walk(past), None).await.expect_err("an array past the ceiling");
	assert!(is_ceiling(&error), "{error}");
	assert_eq!(governor.inner.granted(), 0, "a lease outlived its query");
}

/// The rows of `t` that `ORDER BY` of any size spills, in statements that each
/// write at most a quarter of a MiB, so that no setup transaction's write
/// buffer reaches a ceiling of one MiB.
fn heavy_rows() -> String {
	(0..42)
		.map(|chunk| {
			format!(
				"FOR $i IN {}..{} {{ CREATE type::record('t', $i) SET n = $i, v = (100000 - $i) % 977, pad = string::repeat('x', 1000) }};",
				chunk * 200,
				(chunk + 1) * 200
			)
		})
		.collect::<Vec<_>>()
		.join("\n")
}

/// A sort is spillable, so it holds to the node's limit whatever the ceiling,
/// and answers as unbounded does; a gather of the same rows is not, and fails
/// at the ceiling.
#[tokio::test]
async fn a_large_order_by_under_a_tiny_limit_spills_and_answers() {
	let setup = format!("DEFINE TABLE t;\n{}", heavy_rows());
	let sql = "SELECT * FROM t ORDER BY v";
	let gather = "RETURN array::len((SELECT * FROM t ORDER BY v))";
	let free = TestDb::new_with_config(&setup, small(4)).await;
	let expected = answer(&free, sql, None).await;
	assert!(expected.as_ref().is_ok_and(|rows| rows.len() == 8400), "the sort answers");
	let governor = Arc::new(FairGovernor::new(1 << 20));
	let store = Arc::new(MemTempStore::new(1024, None));
	let config = small(4).with_key_value("query_memory_max", "1MiB");
	let db = bounded_on(Arc::clone(&governor), Arc::clone(&store), &setup, config).await;
	let before = store.runs_written();
	assert_eq!(answer(&db, sql, None).await, expected, "`{sql}` under a tiny limit");
	assert!(store.runs_written() > before, "the sort did not spill");
	let error = answer(&db, gather, None).await.expect_err("a gather past the ceiling answered");
	assert!(is_ceiling(&error), "{error}");
	assert_eq!(store.runs(), 0, "a spilled run outlived its query");
}

/// A scheduler that holds its query at the first yield where a lease of its
/// governor has pinned `threshold` bytes, until resumed.
struct Pausing {
	governor: Arc<Watching>,
	threshold: usize,
	paused: std::sync::atomic::AtomicBool,
	reached: Arc<tokio::sync::Notify>,
	resume: Arc<tokio::sync::Notify>,
}

impl Scheduler for Pausing {
	fn yield_now(&self) -> YieldFuture {
		if self.governor.most_pinned() >= self.threshold
			&& !self.paused.swap(true, Ordering::SeqCst)
		{
			let (reached, resume) = (Arc::clone(&self.reached), Arc::clone(&self.resume));
			return Box::pin(async move {
				reached.notify_one();
				resume.notified().await;
			});
		}
		Box::pin(tokio::task::yield_now())
	}
}

/// A query that has pinned about the whole of the node's limit gives memory
/// back to a query that opens while it is held: the newcomer is granted its
/// minimum at once and completes, the holder's pinned bytes above its new grant
/// are its own overcommit, and it answers when it resumes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_growing_query_yields_to_a_newcomers_minimum() {
	let limit = gather_peak().await / 2;
	let governor = Watching::new(limit);
	let store = Arc::new(MemTempStore::new(1024, None));
	let config = small(4).with_key_value("vm_quantum", "64");
	let db = Arc::new(db_on(Arc::clone(&governor), store, config).await);
	let expected = unbounded(&[SORTS[0]]).await;
	let pausing = Arc::new(Pausing {
		governor: Arc::clone(&governor),
		threshold: limit * 9 / 10,
		paused: Default::default(),
		reached: Default::default(),
		resume: Default::default(),
	});
	let holder = {
		let (db, pausing) = (Arc::clone(&db), Arc::clone(&pausing));
		tokio::spawn(async move { answer(&db, GATHER, Some(pausing as Arc<dyn Scheduler>)).await })
	};
	pausing.reached.notified().await;
	assert_eq!(answer(&db, SORTS[0], None).await, expected[0], "the newcomer under a held node");
	let held = governor.open();
	let holding = held.iter().max_by_key(|lease| lease.pinned()).expect("the holder's lease");
	assert!(holding.overcommit() > 0, "the holder was not shrunk to its fair share");
	assert_eq!(governor.inner.overcommitted(), holding.pinned() - holding.usable());
	assert!(governor.inner.granted() <= limit.max(held.len() * holding.minimum()));
	assert!(
		!governor.below_minimum.load(Ordering::Relaxed),
		"a lease was granted below its minimum"
	);
	drop(held);
	pausing.resume.notify_one();
	assert_eq!(holder.await.expect("the holder"), Ok(vec![Value::from(3000)]));
	assert_eq!(governor.inner.overcommitted(), 0, "an overcommit outlived its query");
}

/// A governor that records the order in which its leases ask for memory and
/// give it back, and what the lease held pinned after each.
struct Journal {
	inner: FairGovernor,
	events: Mutex<Vec<(&'static str, usize)>>,
}

impl MemoryGovernor for Journal {
	fn opened(&self, lease: &Arc<Lease>) {
		self.inner.opened(lease);
	}

	fn grow(&self, lease: &Lease, bytes: usize) -> bool {
		self.events.lock().expect("events").push(("ask", lease.pinned()));
		self.inner.grow(lease, bytes)
	}

	fn closed(&self, lease: &Lease) {
		self.inner.closed(lease);
	}

	fn admit(&self, lease: &Lease, bytes: usize, may_wait: bool) -> Admit {
		self.events.lock().expect("events").push(("ask", lease.pinned()));
		self.inner.admit(lease, bytes, may_wait)
	}

	fn changed(&self, epoch: u64) -> BoxFut<'_, ()> {
		self.inner.changed(epoch)
	}

	fn released(&self, lease: &Lease) {
		self.events.lock().expect("events").push(("release", lease.pinned()));
		self.inner.released(lease);
	}

	fn overcommitted(&self, lease: &Lease, bytes: usize) {
		MemoryGovernor::overcommitted(&self.inner, lease, bytes);
	}

	fn unwait(&self, lease: &Lease) {
		self.inner.unwait(lease);
	}

	fn limit(&self) -> Option<usize> {
		MemoryGovernor::limit(&self.inner)
	}
}

/// The values a program builds and binds are charged to its query's lease at
/// what they weigh, a name that is bound again gives back the whole chunks its
/// old value held, and what remains is given back when the query ends.
#[tokio::test]
async fn program_values_are_charged_and_released() {
	// Within what `array::range` builds in one call, which the process bounds by
	// `SURREAL_GENERATION_ALLOCATION_LIMIT` (1 MiB by default).
	const ELEMENTS: i64 = 18_000;
	let config = small(4);
	let governor = Watching::new(1 << 30);
	let store = Arc::new(MemTempStore::new(1024, None));
	let db = db_on(Arc::clone(&governor), store, config.clone()).await;
	let value = Value::Array(crate::val::Array((0..ELEMENTS).map(Value::from).collect()));
	let weight = crate::exec::memory::size::value_bytes(&value);
	// The third name's request is the one that samples the first two held.
	let sql = format!(
		"RETURN {{ LET $a = array::range(0, {ELEMENTS}); LET $b = $a; LET $c = $b; \
		RETURN array::len($c) }}"
	);
	assert_eq!(answer(&db, &sql, None).await, Ok(vec![Value::from(ELEMENTS)]));
	let peak = governor.peak_pinned.load(Ordering::Relaxed);
	assert!(peak + 4096 >= 2 * weight, "the program's values weighed {peak} against {weight}");
	assert_eq!(governor.held(), 0, "a program's values outlived its query");

	// A slot bound again to a smaller value gives back what its large one held,
	// as whole chunks, while the query still runs.
	let journal = Arc::new(Journal {
		inner: FairGovernor::new(1 << 30),
		events: Mutex::new(Vec::new()),
	});
	let store = Arc::new(MemTempStore::new(1024, None));
	let db = db_on(Arc::clone(&journal), store, config).await;
	journal.events.lock().expect("events").clear();
	let sql = format!(
		"RETURN {{ FOR $i IN [1, 2] {{ \
		LET $a = IF $i = 1 {{ array::range(0, {ELEMENTS}) }} ELSE {{ array::range(0, 1000) }}; \
		LET $n = array::len($a); }}; RETURN 1 }}"
	);
	assert_eq!(answer(&db, &sql, None).await, Ok(vec![Value::from(1)]));
	let events = journal.events.lock().expect("events");
	let held: Vec<usize> =
		events.iter().filter(|(kind, _)| *kind == "release").map(|(_, pinned)| *pinned).collect();
	assert!(
		held.iter().any(|pinned| *pinned > 0 && *pinned < weight / 4),
		"no release left only the smaller value's chunks held: {held:?}"
	);
}

/// A `LET`'s gathered rows are bound by taking over the account that charged
/// them: the bytes stay reserved as the value changes hands, so no other query
/// can take them in between, and what the query holds does not fall until its
/// state ends.
#[tokio::test]
async fn a_gathered_let_transfers_its_charge() {
	let sql = "RETURN { LET $x = (SELECT * FROM t); RETURN array::len($x) }";
	let journal = Arc::new(Journal {
		inner: FairGovernor::new(1 << 30),
		events: Mutex::new(Vec::new()),
	});
	let store = Arc::new(MemTempStore::new(1024, None));
	let db = db_on(Arc::clone(&journal), store, small(4)).await;
	journal.events.lock().expect("events").clear();
	assert_eq!(answer(&db, sql, None).await, Ok(vec![Value::from(3000)]));
	let events = journal.events.lock().expect("events");
	let peak = events.iter().map(|(_, pinned)| *pinned).max().unwrap_or(0);
	assert!(peak > 0, "the gather held nothing pinned");
	let releases: Vec<usize> =
		events.iter().filter(|(kind, _)| *kind == "release").map(|(_, pinned)| *pinned).collect();
	let (last, earlier) = releases.split_last().expect("the query released");
	assert_eq!(*last, 0, "the query left bytes held: {events:?}");
	assert!(
		earlier.iter().all(|pinned| *pinned * 2 >= peak),
		"the gathered bytes were given back before the bind: {events:?}"
	);
}

/// A sort that keeps a bounded number of rows spills once what it holds
/// passes a chunk of its lease, whether it prunes or, under a small quantum,
/// merges; every pass and run is cut to the rows it keeps; and it answers as
/// unbounded.
#[tokio::test]
async fn a_limited_sort_spills_and_answers_as_unbounded() {
	let statements = [
		"SELECT n FROM t ORDER BY k LIMIT 5 START 2000",
		"SELECT n, pad FROM t ORDER BY pad DESC, n LIMIT 1500",
		"SELECT VALUE n FROM t ORDER BY k DESC, n LIMIT 2500",
		"SELECT n FROM t WHERE n % 2 = 0 ORDER BY s COLLATE, k LIMIT 1200",
	];
	let keeps = [2005, 1500, 2500, 1200];
	let expected = unbounded(&statements).await;
	for fan_in in [2usize, 8] {
		for quantum in [None, Some("64")] {
			let mut config = small(fan_in);
			if let Some(quantum) = quantum {
				config = config.with_key_value("vm_quantum", quantum);
			}
			let governor = Watching::new(32 << 10);
			let store = Arc::new(MemTempStore::new(1024, None));
			let db = db_on(governor, Arc::clone(&store), config).await;
			let before = store.runs_written();
			let passes = super::spill::MERGE_PASSES.load(Ordering::Relaxed);
			for ((sql, expected), keep) in statements.iter().zip(&expected).zip(keeps) {
				super::spill::LARGEST_PASS.with(|largest| largest.set(0));
				assert_eq!(
					&answer(&db, sql, None).await,
					expected,
					"`{sql}` (fan-in {fan_in}, quantum {quantum:?})"
				);
				if fan_in == 2 {
					let largest = super::spill::LARGEST_PASS.with(|largest| largest.get());
					assert!(largest <= keep as u64, "`{sql}` wrote a pass of {largest} records");
				}
			}
			assert!(store.runs_written() > before, "no sort spilled");
			assert_eq!(store.runs(), 0, "a spilled run outlived its query");
			assert!(
				store.most_read_at_once() <= fan_in,
				"read {} runs at once",
				store.most_read_at_once()
			);
			if fan_in == 2 {
				assert!(
					super::spill::MERGE_PASSES.load(Ordering::Relaxed) > passes,
					"no merge took a pass"
				);
			}
		}
	}
}

/// A random order that keeps a bounded number of rows spills, and hands on
/// distinct rows.
#[tokio::test]
async fn a_limited_random_order_spills_and_hands_on_distinct_rows() {
	let governor = Arc::new(FairGovernor::new(32 << 10));
	let store = Arc::new(MemTempStore::new(1024, None));
	let db = db_on(governor, Arc::clone(&store), small(4)).await;
	let before = store.runs_written();
	let rows = answer(&db, "SELECT VALUE n FROM t ORDER BY RAND() LIMIT 2000", None)
		.await
		.expect("answers");
	assert!(store.runs_written() > before, "the sort did not spill");
	assert_eq!(rows.len(), 2000);
	let distinct: std::collections::BTreeSet<_> = rows.iter().cloned().collect();
	assert_eq!(distinct.len(), 2000, "a row was handed on twice");
	let every: std::collections::BTreeSet<Value> = (0..3000i64).map(Value::from).collect();
	assert!(distinct.is_subset(&every), "a row was not in the table");
	assert_eq!(store.runs(), 0, "a spilled run outlived its query");
}

/// A sort that keeps few rows holds them within one chunk and spills nothing.
#[tokio::test]
async fn a_small_limited_sort_takes_no_account() {
	let sql = "SELECT n FROM t ORDER BY k LIMIT 10";
	let expected = unbounded(&[sql]).await;
	let governor = Watching::new(32 << 10);
	let store = Arc::new(MemTempStore::new(1024, None));
	let db = db_on(governor, Arc::clone(&store), small(8)).await;
	let before = store.runs_written();
	assert_eq!(answer(&db, sql, None).await, expected[0]);
	assert_eq!(store.runs_written(), before, "a sort within a chunk spilled");
}

/// A sort that keeps a bounded number of rows, in a query with no temporary
/// store, takes no account: it prunes in memory and answers as unbounded
/// rather than failing for memory.
#[tokio::test]
async fn a_limited_sort_without_a_temporary_store_prunes_in_memory() {
	let sql = "SELECT n, pad FROM t ORDER BY k, n LIMIT 2000";
	let expected = unbounded(&[sql]).await;
	let governor = Watching::new(24 << 10);
	let freely = Arc::new(SetUpFreely {
		setting_up: std::sync::atomic::AtomicBool::new(true),
		inner: governor,
	});
	// A directory that cannot be made leaves the datastore with no store.
	let builder = crate::kvs::Datastore::builder()
		.with_memory_governor(Arc::clone(&freely) as Arc<dyn MemoryGovernor>)
		.with_temporary_directory(Some(std::path::PathBuf::from("/dev/null/spill")));
	let config = small(8).with_key_value("memory_reserve_chunk", "1024");
	let db = TestDb::new_on(builder, SETUP, config).await;
	freely.setting_up.store(false, Ordering::Relaxed);
	assert_eq!(answer(&db, sql, None).await, expected[0]);
}

/// A sort that keeps more rows than a chunk holds, in order, drops the rows
/// its spilled runs show cannot be kept, and answers as unbounded.
#[tokio::test]
async fn a_spilled_floor_drops_rows_that_cannot_be_kept() {
	let statements =
		["SELECT n FROM t ORDER BY n LIMIT 100", "SELECT n FROM t ORDER BY n DESC LIMIT 100"];
	let expected = unbounded(&statements).await;
	let governor = Watching::new(24 << 10);
	let store = Arc::new(MemTempStore::new(1024, None));
	let config = small(8).with_key_value("memory_reserve_chunk", "1024");
	let db = db_on(governor, Arc::clone(&store), config).await;
	for (sql, expected) in statements.iter().zip(&expected) {
		let before = store.runs_written();
		let floors = super::spill::FLOORS_SET.with(|floors| floors.get());
		assert_eq!(&answer(&db, sql, None).await, expected, "`{sql}`");
		assert!(store.runs_written() > before, "`{sql}` did not spill");
		assert!(
			super::spill::FLOORS_SET.with(|floors| floors.get()) > floors,
			"`{sql}` set no spilled floor"
		);
	}
	assert_eq!(store.runs(), 0, "a spilled run outlived its query");
}

/// A region whose batches outgrow a small budget spills inside its batches,
/// and its merge answers what the serial run answers: sorts, whole and under a
/// limit, aggregates whose states combine exactly, and `GROUP ALL`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn b_a_region_whose_batches_spill_answers_as_serial() {
	// A `$this` read keeps each body off the storage cursor's in-place path,
	// so each runs as a region.
	let statements = [
		"SELECT n, $this.k AS kk FROM t ORDER BY kk",
		"SELECT n, $this.s AS ss FROM t ORDER BY ss COLLATE, n DESC",
		"SELECT n, $this.k AS kk FROM t ORDER BY kk DESC LIMIT 1500",
		"SELECT g, count() AS c, array::group(k) AS ks, math::max(n) AS mx, \
		 array::join(s, ',') AS j, math::median(n) AS md FROM t WHERE $this.n >= 0 GROUP BY g",
		"SELECT n % 400 AS b, pad AS first, array::distinct(s) AS ds, math::min(n) AS mn \
		 FROM t WHERE $this.n >= 0 GROUP BY b",
		"SELECT count() AS c, array::group(k) AS ks FROM t WHERE $this.n >= 0 GROUP ALL",
	];
	let expected = unbounded(&statements).await;
	let store = Arc::new(MemTempStore::new(1024, None));
	let config = small(4).with_key_value("region_row_threshold", "1");
	let db = db_on(Watching::new(64 << 10), Arc::clone(&store), config).await;
	let ctx = db.exec_ctx().await;
	for sql in statements {
		let program = super::tests_sources::program_of(&ctx, sql).await;
		assert!(program.pipelines.iter().any(|p| p.region), "`{sql}` is no region");
	}
	let handed = super::region::HANDED_TO_WORKERS.load(Ordering::Relaxed);
	let before = store.runs_written();
	let partials = |kind| super::region::batch_partials(store.session(), kind);
	for (sql, expected) in statements.iter().zip(&expected) {
		let (sorts, groups) = (partials(BreakerKind::Sort), partials(BreakerKind::Groups));
		assert_eq!(&answer(&db, sql, None).await, expected, "`{sql}` as a spilling region");
		// `GROUP ALL` holds one group and never spills in a batch.
		let (sorted, grouped) = (sql.contains("ORDER BY"), sql.contains("GROUP BY"));
		assert_eq!(
			partials(BreakerKind::Sort) > sorts,
			sorted,
			"`{sql}`: its sorts did not spill in a batch as expected"
		);
		assert_eq!(
			partials(BreakerKind::Groups) > groups,
			grouped,
			"`{sql}`: its groups did not spill in a batch as expected"
		);
	}
	assert!(
		super::region::HANDED_TO_WORKERS.load(Ordering::Relaxed) > handed,
		"no batch ran on a worker"
	);
	assert!(store.runs_written() > before, "nothing spilled");
	assert_eq!(store.runs(), 0, "a spilled run outlived its query");
	assert!(store.most_read_at_once() <= 4, "read {} runs at once", store.most_read_at_once());
}
