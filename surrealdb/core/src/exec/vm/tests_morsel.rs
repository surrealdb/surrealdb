//! Morsels answer what the serial run answers, at every width: a scan split into entry-sized
//! morsels, read by workers through the transaction's read view or by the coordinator alone, on a
//! backend that gives a view and on one that does not. The reference of every case is the same
//! statement over a datastore whose scans never split (`whole`), which runs the morsel code at
//! width one.
//!
//! Statements run through the datastore's request path. A case that needs the statement's own
//! transaction (to cancel it, to read its scan metrics) plans the statement with
//! [`plan_checked`] and runs the operator over a context of its own. The plan of a statement is
//! read from the program the production compile gives it.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use super::compile::compile;
use super::golden::Golden;
use super::morsel::{
	Counts, counts_of, delay_morsels, effective_input, fail_splits, forget_ends, metrics_of,
};
#[cfg(feature = "kv-rocksdb")]
use super::morsel::{fail_grants, fail_narrows, waiting_statement};
use super::plan_checked;
use super::program::Program;
use crate::exec::memory::{Admit, BoxFut, Lease, MemoryGovernor, TempStore};
use crate::exec::operators::test_util::{TestDb, try_collect};
use crate::exec::planner::Planner;
use crate::exec::planner::placement::{Combine, MergeOrder, MorselInput, MorselPlan, Parallel};
use crate::exec::{ExecOperator, ExecutionContext};
use crate::expr::logical::{LogicalBreak, LogicalDistinct, LogicalOp};
use crate::val::Value;

/// Workers draw on one pool the whole process shares, so the tests that count on holding places
/// take turns.
static TURNS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Rows of the table the tests read.
const ROWS: usize = 3_000;

/// A table of [`ROWS`] rows whose `s` field is a string, with a numeric `n` and a float `f`.
fn setup() -> String {
	format!(
		"DEFINE TABLE t;
		 FOR $i IN 0..{ROWS} {{
			CREATE type::record('t', $i) SET n = $i, s = <string> $i, f = $i * 0.5
		 }};"
	)
}

/// Settings that cut every scan into entry-sized morsels and let workers take every place.
pub(super) fn tiny() -> surrealdb_cnf::ConfigMap {
	surrealdb_cnf::ConfigMap::empty()
		.with_key_value("morsel_split_rows", "1")
		.with_key_value("morsel_needs_view", "false")
		.with_key_value("morsel_target_bytes", "1")
		.with_key_value("morsel_oltp_reserve", "0")
}

/// The default settings, except that a range is cut where workers get no read view, so a datastore
/// without views (mem) plans the morsels a datastore with views plans.
pub(super) fn viewless() -> surrealdb_cnf::ConfigMap {
	surrealdb_cnf::ConfigMap::empty().with_key_value("morsel_needs_view", "false")
}

/// Settings that never split.
pub(super) fn whole() -> surrealdb_cnf::ConfigMap {
	surrealdb_cnf::ConfigMap::empty().with_key_value("morsel_split_rows", usize::MAX.to_string())
}

/// The one statement `sql` holds, parsed and lowered.
pub(super) fn parse(sql: &str) -> LogicalOp {
	let ast = crate::syn::parse(sql).expect("statement parses");
	let surrealdb_expr::parser::TopLevelExpr::Expr(expr) =
		ast.expressions.into_iter().next().expect("one statement")
	else {
		panic!("not an expression statement: {sql}");
	};
	crate::expr::logical::lower(expr).expect("statement lowers")
}

/// A planner for a statement of `ctx`, as the executor plans one it is about to run: speculating,
/// so a block's schema statements are modelled.
pub(super) fn planner(ctx: &ExecutionContext) -> Planner<'_> {
	let txn = ctx.try_txn().expect("a transaction");
	// A read-only transaction records no value shape, so a statement planned in one trusts the
	// shapes the indexes have recorded, as a statement that cannot write does.
	let reads_only = !txn.writeable();
	let mut planner = Planner::with_txn(
		ctx.ctx(),
		ctx.function_registry(),
		txn,
		Some("test".to_owned()),
		Some("test".to_owned()),
	)
	.with_auth(Arc::clone(&ctx.root().auth))
	.trusting_index_shapes(reads_only);
	planner.speculate_with(ctx.options().map(|options| Arc::new(options.clone())));
	planner
}

/// The statement `sql` holds, bound by the planner of `ctx`.
pub(super) async fn bind(ctx: &ExecutionContext, sql: &str) -> LogicalOp {
	planner(ctx).bind(parse(sql)).await.expect("statement binds")
}

/// The operator the VM plans for `bound`.
async fn operator_of(ctx: &ExecutionContext, bound: &LogicalOp) -> Arc<dyn ExecOperator> {
	Box::pin(plan_checked(&planner(ctx), bound, None)).await.expect("plans")
}

/// The operator the VM plans for the statement `sql`.
pub(super) async fn statement(ctx: &ExecutionContext, sql: &str) -> Arc<dyn ExecOperator> {
	let bound = bind(ctx, sql).await;
	operator_of(ctx, &bound).await
}

/// What `op` answers over `ctx`, as the executor ends a statement: a `RETURN` with its value, and
/// a `BREAK` or `CONTINUE` that no loop caught with the error it raises.
pub(super) async fn outcome(
	op: &Arc<dyn ExecOperator>,
	ctx: &ExecutionContext,
) -> Result<Vec<Value>, String> {
	use crate::expr::ControlFlow;
	match try_collect(op, ctx).await {
		Ok(rows) => Ok(rows),
		Err(ControlFlow::Return(value)) => Ok(vec![value]),
		Err(ControlFlow::Err(e)) => Err(format!("{e:#}")),
		Err(ControlFlow::Break | ControlFlow::Continue) => {
			Err(crate::exec::Error::InvalidControlFlow.to_string())
		}
	}
}

/// The program the bound plan `bound` compiles to over `ctx`.
async fn compiled_plan(ctx: &ExecutionContext, bound: &LogicalOp) -> Program {
	Box::pin(compile(&planner(ctx), bound)).await.expect("compiles").program
}

/// The program `sql` compiles to over `ctx`.
pub(super) async fn lowered(ctx: &ExecutionContext, sql: &str) -> Arc<Program> {
	let bound = bind(ctx, sql).await;
	Arc::new(compiled_plan(ctx, &bound).await)
}

/// Whether any pipeline of `program` reads its key range in morsels: its body runs in the storage
/// cursor ([`effective_input`]).
fn reads_ranges(program: &Program) -> bool {
	program.pipelines.iter().any(|pipeline| effective_input(pipeline) == Some(MorselInput::Range))
}

/// Whether any pipeline of `program` runs as morsels, over a key range or over batches.
pub(super) fn fans_out(program: &Program) -> bool {
	program.pipelines.iter().any(|pipeline| effective_input(pipeline).is_some())
}

/// The statements of `statements` that read a key range in morsels over `db`, and the others.
async fn split_by_ranges<'s>(db: &TestDb, statements: &[&'s str]) -> (Vec<&'s str>, Vec<&'s str>) {
	let ctx = db.exec_ctx().await;
	let mut ranges = Vec::new();
	let mut others = Vec::new();
	for sql in statements {
		if reads_ranges(&*lowered(&ctx, sql).await) {
			ranges.push(*sql);
		} else {
			others.push(*sql);
		}
	}
	(ranges, others)
}

/// What `sql` answers over `db`, as a request to its datastore.
pub(super) async fn answered(db: &TestDb, sql: &str) -> Result<Vec<Value>, String> {
	let mut results =
		db.datastore().execute(sql, &TestDb::owner(), None).await.expect("the request runs");
	match results.pop().expect("a result").result {
		Ok(value) => Ok(match Value::from(value) {
			Value::Array(rows) => rows.into_iter().collect(),
			other => vec![other],
		}),
		Err(e) => Err(e.message().to_owned()),
	}
}

/// What `sql` answers over `db`, and whether it reads a key range in morsels.
async fn answer(db: &TestDb, sql: &str) -> (Result<Vec<Value>, String>, bool) {
	let program = lowered(&db.exec_ctx().await, sql).await;
	(answered(db, sql).await, reads_ranges(&program))
}

/// What a statement answers: its rows, or the message of the error it fails with.
pub(super) type Answer = Result<Vec<Value>, String>;

/// What a statement's rows owe the serial run's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Rows {
	/// `ORDER BY` fixes the order: the rows are the serial run's, in order.
	Ordered,
	/// A paging clause over rows no `ORDER BY` orders. A paged pipeline replays its rows in the
	/// serial order, so the rows are the serial run's, in order.
	Paged,
	/// Nothing fixes the order, and nothing pages: the rows are the serial run's as a multiset.
	Free,
}

/// Whether the relational operators of the plan of the statement `sql` sort their rows, and
/// whether they page them: a sort or a limit nested in an expression of the statement does not
/// order or page its rows.
fn sorts_and_pages(sql: &str) -> (bool, bool) {
	fn walk(op: &LogicalOp, sorted: &mut bool, paged: &mut bool) {
		match op {
			LogicalOp::Sort(_) => *sorted = true,
			LogicalOp::Limit(_) => *paged = true,
			_ => {}
		}
		for child in op.children() {
			walk(child, sorted, paged);
		}
	}
	let (mut sorted, mut paged) = (false, false);
	walk(&parse(sql), &mut sorted, &mut paged);
	(sorted, paged)
}

/// What the rows of the statement `sql` owe the serial run's.
fn rows_of(sql: &str) -> Rows {
	match sorts_and_pages(sql) {
		(true, _) => Rows::Ordered,
		(false, true) => Rows::Paged,
		(false, false) => Rows::Free,
	}
}

/// The rows of `rows` as text, sorted: two lists of rows hold the same multiset when their texts
/// are equal. A value's rendering tells `1` from `1.0`, which compare equal and differ.
pub(super) fn multiset(rows: &[Value]) -> Vec<String> {
	let mut rendered: Vec<String> = rows.iter().map(|row| format!("{row:?}")).collect();
	rendered.sort();
	rendered
}

/// A rendering of `value` short enough to read in a failure message.
fn shown(value: &impl std::fmt::Debug) -> String {
	let text = format!("{value:?}");
	match text.char_indices().nth(600) {
		Some((end, _)) => format!("{}…", &text[..end]),
		None => text,
	}
}

/// Whether `got`, the answer a morsel run gives the statement `sql`, is one the serial run's
/// `expected` allows: the same rows, in order where the statement orders or pages them and as a
/// multiset where it does not, or, for a statement that fails, a failure whose message is
/// `expected`'s or one of `errors`, the failures of the other rows that may fail it. Values nested
/// in a row are compared exactly.
fn equivalent(sql: &str, got: &Answer, expected: &Answer, errors: &[String]) -> Result<(), String> {
	match (got, expected) {
		(Ok(got), Ok(expected)) => {
			let same = match rows_of(sql) {
				Rows::Ordered | Rows::Paged => got == expected,
				Rows::Free => multiset(got) == multiset(expected),
			};
			if same {
				Ok(())
			} else {
				Err(format!("rows {} against {}", shown(got), shown(expected)))
			}
		}
		(Err(got), Err(expected)) => {
			if got == expected || errors.contains(got) {
				Ok(())
			} else {
				Err(format!("fails with `{got}`, not with `{expected}` or one of {errors:?}"))
			}
		}
		(Ok(got), Err(expected)) => {
			Err(format!("answers {} where the serial run fails with `{expected}`", shown(got)))
		}
		(Err(got), Ok(expected)) => {
			Err(format!("fails with `{got}` where the serial run answers {}", shown(expected)))
		}
	}
}

/// Assert that `got` is an answer to `sql` that `expected` allows, [`equivalent`].
#[track_caller]
pub(super) fn assert_equivalent(sql: &str, got: &Answer, expected: &Answer) {
	assert_equivalent_among(sql, got, expected, &[]);
}

/// [`assert_equivalent`], where a failure may be any of `errors` as well as `expected`'s.
#[track_caller]
fn assert_equivalent_among(sql: &str, got: &Answer, expected: &Answer, errors: &[String]) {
	if let Err(why) = equivalent(sql, got, expected, errors) {
		panic!("`{sql}`: {why}");
	}
}

/// A governor that grants every request while a database is set up, and defers to `inner` once
/// the tests run on it: a setup's writes are not what a test bounds.
pub(super) struct SetUpFreely<G> {
	setting_up: AtomicBool,
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

/// A datastore builder whose queries are bounded by `governor` and spill to `store`, and the
/// governor's gate, which is open until the database is set up.
fn bounded_builder(
	governor: Arc<impl MemoryGovernor>,
	store: Arc<impl TempStore>,
) -> (crate::kvs::ds::Builder, Arc<SetUpFreely<impl MemoryGovernor>>) {
	let freely = Arc::new(SetUpFreely {
		setting_up: AtomicBool::new(true),
		inner: governor,
	});
	let builder = crate::kvs::Datastore::builder()
		.with_memory_governor(Arc::clone(&freely) as Arc<dyn MemoryGovernor>)
		.with_temp_store(store);
	(builder, freely)
}

/// A database set up with `setup`, then bounded by `governor` and spilling to `store`.
pub(super) async fn bounded_on(
	governor: Arc<impl MemoryGovernor>,
	store: Arc<impl TempStore>,
	setup: &str,
	config: surrealdb_cnf::ConfigMap,
) -> TestDb {
	let (builder, freely) = bounded_builder(governor, store);
	let db = TestDb::new_on(builder, setup, config).await;
	freely.setting_up.store(false, Ordering::Relaxed);
	db
}

/// [`bounded_on`] over a RocksDB datastore in `dir`, whose read-only transactions give the
/// morsels of a scan a read view.
#[cfg(feature = "kv-rocksdb")]
pub(super) async fn bounded_on_rocksdb(
	governor: Arc<impl MemoryGovernor>,
	store: Arc<impl TempStore>,
	setup: &str,
	dir: &std::path::Path,
	config: surrealdb_cnf::ConfigMap,
) -> TestDb {
	let (builder, freely) = bounded_builder(governor, store);
	let db = TestDb::new_on_rocksdb_over(builder, setup, dir, config).await;
	freely.setting_up.store(false, Ordering::Relaxed);
	db
}

/// Run each of `statements` over `serial` and `wide` and assert the answers equal, those that
/// read ranges in morsels on `wide` among them. Answers what `wide`'s morsels did.
async fn same_answers(serial: &TestDb, wide: &TestDb, statements: &[&str]) -> Counts {
	same_answers_in(serial, wide, statements, true).await
}

/// [`same_answers`], where a statement reading no key range in morsels is a failure only when
/// `ranges_wanted`: the bodies of the others run as batches.
async fn same_answers_in(
	serial: &TestDb,
	wide: &TestDb,
	statements: &[&str],
	ranges_wanted: bool,
) -> Counts {
	forget_ends(&wide.exec_ctx().await);
	let before = counts_of(&wide.exec_ctx().await);
	let mut ranges = 0;
	let wanted = workers_wanted(wide).await;
	// Other tests of the process may hold the node's places for a while.
	for attempt in 0..5 {
		let handed = counts_of(&wide.exec_ctx().await).handed;
		ranges = run_all(serial, wide, statements).await;
		if attempt == 4 || !wanted || counts_of(&wide.exec_ctx().await).handed > handed {
			break;
		}
	}
	assert!(!ranges_wanted || ranges > 0, "no statement read a key range in morsels");
	let after = counts_of(&wide.exec_ctx().await);
	Counts {
		handed: after.handed - before.handed,
		split: after.split - before.split,
		stolen: after.stolen - before.stolen,
		resplit: after.resplit - before.resplit,
		scanned: after.scanned - before.scanned,
		chunks: after.chunks - before.chunks,
		parked: after.parked - before.parked,
		parked_after_refusal: after.parked_after_refusal - before.parked_after_refusal,
		pruned: after.pruned - before.pruned,
		runs: after.runs - before.runs,
		partitioned: after.partitioned - before.partitioned,
		partitions: after.partitions - before.partitions,
		most_ends: after.most_ends,
		refused: after.refused - before.refused,
		states: after.states - before.states,
		cut_short: after.cut_short - before.cut_short,
		most_held: after.most_held,
		merged_early: after.merged_early - before.merged_early,
		terms_charged: after.terms_charged - before.terms_charged,
		output_charged: after.output_charged - before.output_charged,
		output_refused: after.output_refused - before.output_refused,
		streamed: after.streamed - before.streamed,
		collected_early: after.collected_early - before.collected_early,
		most_results_held: after.most_results_held,
		combined: after.combined - before.combined,
	}
}

/// Whether `wide` may hand morsels to workers at all: it may take a place, and its transactions
/// give a read view.
async fn workers_wanted(wide: &TestDb) -> bool {
	places_given(wide) && wide.exec_ctx().await.txn().snapshot().is_some()
}

/// How the program of a statement runs its pipelines in morsels.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FanOut {
	/// No pipeline runs as morsels.
	No,
	/// A pipeline reads its key range in morsels.
	Range,
	/// A pipeline runs morsels over batches of rows the coordinator read, and none reads a range.
	Batches,
}

/// How the program `sql` compiles to over `db` runs in morsels.
async fn fan_out_of(db: &TestDb, sql: &str) -> FanOut {
	let program = lowered(&db.exec_ctx().await, sql).await;
	if reads_ranges(&program) {
		FanOut::Range
	} else if fans_out(&program) {
		FanOut::Batches
	} else {
		FanOut::No
	}
}

/// Whether `db` lets workers take morsels at all.
fn places_given(db: &TestDb) -> bool {
	db.datastore()
		.morsel_knobs()
		.into_iter()
		.any(|(key, value)| key == "morsel_places" && value != "0")
}

/// Whether the statement `sql` writes nothing, by its text: running it again changes nothing.
fn reads_only(sql: &str) -> bool {
	let sql = sql.to_uppercase();
	["CREATE", "UPDATE", "UPSERT", "INSERT", "DELETE", "RELATE", "DEFINE", "REMOVE", "ALTER", "LET"]
		.iter()
		.all(|word| !sql.contains(word))
}

/// Run the statement `sql` over `wide`, assert its answer is one `expected` allows, and, for a
/// statement that plans morsels and answers rows, that its morsels ran: a key range was split, or
/// a batch was handed to a worker. A statement that fails, pages or answers no row may stop before
/// its first morsel is out, and answers without the proof. Another test of the process may hold the
/// node's places, so a statement that only reads runs again, up to five times, until its morsels
/// have run; one that writes runs once. Answers how the statement fans out.
pub(super) async fn answered_fanning_out(wide: &TestDb, sql: &str, expected: &Answer) -> FanOut {
	match try_answered_fanning_out(wide, sql, expected, true).await {
		Ok(fan_out) => fan_out,
		Err(why) => panic!("`{sql}`: {why}"),
	}
}

/// [`answered_fanning_out`], answering what it asserts as an error, and asserting that morsels ran
/// only where `proof`: a statement whose pipeline reads a handful of rows, which fit one batch,
/// cannot show it.
pub(super) async fn try_answered_fanning_out(
	wide: &TestDb,
	sql: &str,
	expected: &Answer,
	proof: bool,
) -> Result<FanOut, String> {
	let fan_out = fan_out_of(wide, sql).await;
	let proved = proof
		&& fan_out != FanOut::No
		&& expected.as_ref().is_ok_and(|rows| !rows.is_empty())
		&& !sorts_and_pages(sql).1;
	let attempts = if reads_only(sql) {
		5
	} else {
		1
	};
	for attempt in 0..attempts {
		let before = counts_of(&wide.exec_ctx().await);
		let got = answered(wide, sql).await;
		equivalent(sql, &got, expected, &[])?;
		let after = counts_of(&wide.exec_ctx().await);
		let ran = match fan_out {
			FanOut::No => true,
			FanOut::Range => after.split > before.split,
			FanOut::Batches => after.handed > before.handed || !places_given(wide),
		};
		if !proved || ran {
			return Ok(fan_out);
		}
		if attempt + 1 == attempts {
			return Err(format!("plans {fan_out:?} morsels and none ran: {before:?}, {after:?}"));
		}
	}
	unreachable!("the last attempt answers")
}

/// The comparison the equivalence tests judge a morsel run by: rows in any order where nothing
/// orders or pages them, in order where something does, nested values exactly, and a failure that
/// is the serial run's or one of the failures the statement may raise.
#[test]
fn equivalence_is_a_multiset_only_where_no_clause_orders_or_pages_the_rows() {
	let rows =
		|numbers: &[i64]| -> Answer { Ok(numbers.iter().copied().map(Value::from).collect()) };
	let fails = |message: &str| -> Answer { Err(message.to_owned()) };
	let free = "SELECT n FROM t";
	let ordered = "SELECT n FROM t ORDER BY n";
	let paged = "SELECT n FROM t LIMIT 3";
	let nested_order =
		"SELECT n, (SELECT VALUE v FROM [1, 2] ORDER BY v DESC LIMIT 1) AS top FROM t";
	assert_eq!(
		[free, ordered, paged, nested_order].map(rows_of),
		[Rows::Free, Rows::Ordered, Rows::Paged, Rows::Free]
	);
	let ok =
		|sql: &str, got: &Answer, expected: &Answer| equivalent(sql, got, expected, &[]).is_ok();
	// Without a clause that orders or pages them, the rows are the serial run's in any order.
	assert!(ok(free, &rows(&[3, 1, 2]), &rows(&[1, 2, 3])));
	assert!(ok(nested_order, &rows(&[3, 1, 2]), &rows(&[1, 2, 3])));
	assert!(!ok(free, &rows(&[1, 2, 2]), &rows(&[1, 2, 3])), "a different multiset");
	assert!(!ok(free, &rows(&[1, 2]), &rows(&[1, 2, 3])), "a row missing");
	// `1` and `1.0` compare equal and are different values.
	let mixed = |first: Value, second: Value| -> Answer { Ok(vec![first, second]) };
	assert!(!ok(
		free,
		&mixed(Value::from(1), Value::from(2)),
		&mixed(Value::from(1.0), Value::from(2))
	));
	// Where a clause orders or pages them, the order is the serial run's.
	assert!(ok(ordered, &rows(&[1, 2, 3]), &rows(&[1, 2, 3])));
	assert!(!ok(ordered, &rows(&[3, 1, 2]), &rows(&[1, 2, 3])));
	assert!(!ok(paged, &rows(&[3, 1, 2]), &rows(&[1, 2, 3])));
	// A failure is the serial run's, or one of the failures the statement may raise, and a failing
	// run never answers rows, nor the other way about.
	let failures = ["x".to_owned(), "y".to_owned()];
	assert!(equivalent(free, &fails("y"), &fails("x"), &failures).is_ok());
	assert!(equivalent(free, &fails("x"), &fails("x"), &[]).is_ok());
	assert!(equivalent(free, &fails("z"), &fails("x"), &failures).is_err());
	assert!(equivalent(free, &fails("y"), &fails("x"), &[]).is_err());
	assert!(!ok(free, &fails("x"), &rows(&[1])));
	assert!(!ok(free, &rows(&[1]), &fails("x")));
}

/// Run each statement over both, and assert only that the answers equal as the contract of the
/// statement says: for a test that holds the morsels back.
async fn run_all_unproved(serial: &TestDb, wide: &TestDb, statements: &[&str]) {
	for sql in statements {
		let (expected, _) = answer(serial, sql).await;
		assert_equivalent(sql, &answered(wide, sql).await, &expected);
	}
}

/// Run each statement over both, assert the answers equal as the contract of the statement says,
/// and count the statements that read a key range in morsels on `wide`. Each statement that plans
/// morsels and runs to its end is shown to have run them.
async fn run_all(serial: &TestDb, wide: &TestDb, statements: &[&str]) -> usize {
	let mut ranges = 0;
	for sql in statements {
		let (expected, _) = answer(serial, sql).await;
		ranges += usize::from(answered_fanning_out(wide, sql, &expected).await == FanOut::Range);
	}
	ranges
}

const SCANS: &[&str] = &[
	"SELECT * FROM t",
	"SELECT count() FROM t GROUP ALL",
	"SELECT math::max(n), array::group(n) FROM t GROUP ALL",
	"SELECT * FROM t WHERE n % 7 = 0",
	"SELECT n, s FROM t ORDER BY id DESC",
	"SELECT * FROM t:100..2900",
	"SELECT n FROM t WHERE n % 5 = 1 ORDER BY n DESC",
];

/// A scan on RocksDB, whose read-only transactions give a view, tiles into morsels that workers
/// read, and answers what the whole scan answers, forward and backward.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread")]
async fn range_morsels_tile_an_in_place_scan() {
	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(&setup(), serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_rocksdb(&setup(), wide_dir.path(), tiny()).await;
	let counts = same_answers(&serial, &wide, SCANS).await;
	assert!(counts.split > 1, "{counts:?}");
	assert!(counts.handed > 0, "{counts:?}");
	assert!(counts.scanned >= ROWS, "{counts:?}");
}

/// The backward scan of a table reads its key range in morsels too.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread")]
async fn a_backward_scan_reads_its_key_range_in_morsels() {
	let _turn = TURNS.lock().await;
	let dir = tempfile::tempdir().expect("temporary directory");
	let wide = TestDb::new_on_rocksdb(&setup(), dir.path(), tiny()).await;
	assert!(answer(&wide, "SELECT n, s FROM t ORDER BY id DESC").await.1);
}

/// Without a view (kv-mem gives none) the coordinator reads every morsel itself, in order, and
/// answers the same.
#[tokio::test]
async fn range_morsels_run_on_the_coordinator_without_a_view() {
	let serial = TestDb::new_with_config(&setup(), whole()).await;
	let wide = TestDb::new_with_config(&setup(), tiny()).await;
	assert!(wide.exec_ctx().await.txn().snapshot().is_none());
	// A body that runs over decoded rows is read by the coordinator and handed on in batches,
	// which needs no view. A range morsel is read by whoever runs it, and with no view that is the
	// coordinator alone.
	let (ranges, others) = split_by_ranges(&wide, SCANS).await;
	let counts = same_answers(&serial, &wide, &ranges).await;
	assert!(counts.split > 1, "{counts:?}");
	assert_eq!(counts.handed, 0, "{counts:?}");
	same_answers_in(&serial, &wide, &others, false).await;
}

/// A plain `SELECT`, whose pipeline holds no loop op, is placed by the datastore's morsel
/// settings when it runs as a request: on RocksDB with morsels on, its key range is split and
/// workers read the parts, and the rows are the serial run's.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread")]
async fn a_plain_select_fans_out_under_the_datastores_morsel_settings() {
	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(&setup(), serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_rocksdb(&setup(), wide_dir.path(), tiny()).await;
	let sql = "SELECT * FROM t";
	assert_eq!(fan_out_of(&serial, sql).await, FanOut::No);
	let expected = answered(&serial, sql).await;
	assert_eq!(answered_fanning_out(&wide, sql, &expected).await, FanOut::Range);
	let mut handed = false;
	for _ in 0..5 {
		let before = counts_of(&wide.exec_ctx().await);
		assert_equivalent(sql, &answered(&wide, sql).await, &expected);
		let after = counts_of(&wide.exec_ctx().await);
		assert!(after.split > before.split + 1, "{before:?}, {after:?}");
		if after.handed > before.handed {
			handed = true;
			break;
		}
	}
	assert!(handed, "no morsel of `{sql}` was handed to a worker");
}

/// Where the settings cut a range only for workers that read through a view, a plain `SELECT` over
/// a datastore whose transactions give none (kv-mem) is placed serially; with that requirement off
/// the same settings place it as range morsels, which the request's run splits.
#[tokio::test]
async fn a_plain_select_reads_ranges_without_a_view_only_where_the_settings_allow_it() {
	let needs_view = surrealdb_cnf::ConfigMap::empty()
		.with_key_value("morsel_split_rows", "1")
		.with_key_value("morsel_target_bytes", "1")
		.with_key_value("morsel_oltp_reserve", "0");
	let sql = "SELECT * FROM t";
	let held = TestDb::new_with_config(&setup(), needs_view).await;
	assert!(held.exec_ctx().await.txn().snapshot().is_none());
	assert_eq!(fan_out_of(&held, sql).await, FanOut::No);
	let cut = TestDb::new_with_config(&setup(), tiny()).await;
	assert_eq!(fan_out_of(&cut, sql).await, FanOut::Range);
	let expected = answered(&held, sql).await;
	let before = counts_of(&cut.exec_ctx().await);
	assert_equivalent(sql, &answered(&cut, sql).await, &expected);
	let after = counts_of(&cut.exec_ctx().await);
	assert!(after.split > before.split + 1, "{before:?}, {after:?}");
}

/// A statement that may take no place answers as one that takes every place.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread")]
async fn coordinator_only_answers_as_wide() {
	let (serial_dir, narrow_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(&setup(), serial_dir.path(), whole()).await;
	let narrow = TestDb::new_on_rocksdb(
		&setup(),
		narrow_dir.path(),
		tiny().with_key_value("morsel_places", "0"),
	)
	.await;
	let counts = same_answers(&serial, &narrow, SCANS).await;
	assert!(counts.split > 1, "{counts:?}");
	assert_eq!(counts.handed, 0, "{counts:?}");
}

/// Morsels whose windows hold a byte park at every chunk, leaving the rest of their range to
/// another morsel, and the answers stay those of the whole scan.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread")]
async fn range_morsels_park_when_their_window_is_full() {
	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(&setup(), serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_rocksdb(
		&setup(),
		wide_dir.path(),
		// Morsels of a few hundred entries read in chunks of at most a quantum.
		tiny()
			.with_key_value("morsel_target_bytes", "20000")
			.with_key_value("morsel_window_bytes", "1")
			.with_key_value("vm_quantum", "64"),
	)
	.await;
	let counts = same_answers(&serial, &wide, SCANS).await;
	assert!(counts.handed > 0, "{counts:?}");
	assert!(counts.parked > 0, "{counts:?}");
}

/// The failures of the two rows [`failing_setup`] makes fail.
fn cast_failures() -> Vec<String> {
	["x", "y"].map(|input| format!("Could not cast into `int` using input `'{input}'`")).to_vec()
}

/// A body that fails on two rows, which tiny morsels put in different parts, fails with the
/// failure of one of them: which of the rows' failures a parallel scan raises is not specified.
async fn a_failing_scan_fails(serial: &TestDb, wide: &TestDb) {
	let mut morsels = 0;
	for sql in [
		"SELECT <int> s AS i FROM t",
		"SELECT <int> s AS i FROM t ORDER BY id DESC",
		"SELECT n, <int> s AS i FROM t WHERE n % 2 = 0",
	] {
		let (expected, _) = answer(serial, sql).await;
		let got = answered(wide, sql).await;
		assert!(expected.is_err(), "`{sql}` fails serially");
		assert_equivalent_among(sql, &got, &expected, &cast_failures());
		morsels += usize::from(fans_out(&*lowered(&wide.exec_ctx().await, sql).await));
	}
	assert!(morsels > 0, "no failing statement runs as morsels");
}

fn failing_setup() -> String {
	format!("{} UPDATE t:700 SET s = 'x'; UPDATE t:2200 SET s = 'y';", setup())
}

#[tokio::test]
async fn a_failing_scan_raises_one_of_its_failures_on_the_coordinator() {
	let serial = TestDb::new_with_config(&failing_setup(), whole()).await;
	let wide = TestDb::new_with_config(&failing_setup(), tiny()).await;
	a_failing_scan_fails(&serial, &wide).await;
}

#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread")]
async fn a_failing_scan_raises_one_of_its_failures() {
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(&failing_setup(), serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_rocksdb(&failing_setup(), wide_dir.path(), tiny()).await;
	a_failing_scan_fails(&serial, &wide).await;
}

/// A scan that never splits raises the first failure in the order of the scan, and a row past a
/// spent limit or skipped by `START` is never judged.
#[tokio::test]
async fn a_scan_at_width_one_raises_its_first_failure_and_judges_nothing_past_its_limit() {
	let db = TestDb::new_with_config(&failing_setup(), whole()).await;
	let fails = |input: &str| Err(format!("Could not cast into `int` using input `'{input}'`"));
	let rows = |count: i64| Ok(vec![Value::from(count)]);
	let statements: [(&str, Result<Vec<Value>, String>); 9] = [
		("SELECT <int> s AS i FROM t", fails("x")),
		("SELECT <int> s AS i FROM t ORDER BY id DESC", fails("y")),
		("SELECT n, <int> s AS i FROM t WHERE n % 2 = 0", fails("x")),
		("array::len(SELECT n, <int> s AS i FROM t LIMIT 600)", rows(600)),
		("array::len(SELECT n, <int> s AS i FROM t LIMIT 800)", fails("x")),
		("array::len(SELECT n, <int> s AS i FROM t LIMIT 5 START 701)", rows(5)),
		("array::len(SELECT n, <int> s AS i FROM t LIMIT 5 START 698)", fails("x")),
		(
			"array::len(SELECT n, <int> s AS i FROM t WHERE n % 100 = 7 OR n > 2450 LIMIT 30)",
			rows(30),
		),
		(
			"array::len(SELECT n, <int> s AS i FROM t WHERE n % 100 = 7 OR n > 2450 LIMIT 80)",
			rows(80),
		),
	];
	for (sql, expected) in statements {
		assert_eq!(answered(&db, sql).await, expected, "`{sql}`");
	}
}

/// A statement that fails while workers hold cursors on the transaction's view returns only once
/// they have dropped them: the transaction cancels at once.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread")]
async fn workers_are_awaited_before_the_statement_returns() {
	let dir = tempfile::tempdir().expect("temporary directory");
	let wide = TestDb::new_on_rocksdb(&failing_setup(), dir.path(), tiny()).await;
	for sql in ["SELECT <int> s AS i FROM t", "SELECT * FROM t"] {
		let ctx = wide.exec_ctx().await;
		let vm = statement(&ctx, sql).await;
		let answered = outcome(&vm, &ctx).await;
		assert_eq!(answered.is_err(), sql.contains("<int>"), "`{sql}`");
		drop(vm);
		tokio::time::timeout(std::time::Duration::from_secs(20), ctx.txn().cancel())
			.await
			.unwrap_or_else(|_| panic!("the transaction waits for a worker after `{sql}`"))
			.expect("cancels");
	}
}

/// A statement dropped half way, as a client that goes away drops it, leaves no cursor behind.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_statement_leaves_no_cursor_behind() {
	let dir = tempfile::tempdir().expect("temporary directory");
	let wide = TestDb::new_on_rocksdb(&setup(), dir.path(), tiny()).await;
	let ctx = wide.exec_ctx().await;
	let vm = statement(&ctx, "SELECT * FROM t").await;
	let started =
		tokio::time::timeout(std::time::Duration::from_millis(5), outcome(&vm, &ctx)).await;
	drop(started);
	drop(vm);
	tokio::time::timeout(std::time::Duration::from_secs(20), ctx.txn().cancel())
		.await
		.expect("the transaction waits for a worker")
		.expect("cancels");
}

/// What `sql` lowers to over a table, for the pipelines' plans.
async fn plans_of(sql: &str) -> Vec<Parallel> {
	let db = TestDb::new_with_config(&format!("{} DEFINE SEQUENCE s;", setup()), viewless()).await;
	let ctx = db.exec_ctx().await;
	lowered(&ctx, sql).await.pipelines.iter().map(|pipeline| pipeline.parallel.clone()).collect()
}

/// A body that reads the clock runs as morsels, each row reading it when it runs; one that draws
/// randomness or advances a sequence stays serial.
#[tokio::test]
async fn a_clock_body_runs_as_morsels() {
	let morsels =
		|plans: &[Parallel]| plans.iter().any(|plan| matches!(plan, Parallel::Morsels(_)));
	assert!(morsels(&plans_of("SELECT time::now() AS t FROM t").await));
	assert!(!morsels(&plans_of("SELECT rand() FROM t").await));
	assert!(!morsels(&plans_of("SELECT sequence::nextval('s') FROM t").await));
}

/// A sort key that calls a function, a computed grouping key and a grouping argument that calls one
/// hold a region or a step of the host, so the body is not in the storage cursor and its morsels
/// are batches of decoded rows; a sort and a grouping over fields read their key ranges.
#[tokio::test]
async fn a_body_that_calls_a_function_plans_batches() {
	let db = TestDb::new_with_config(&setup(), viewless()).await;
	let ctx = db.exec_ctx().await;
	for (sql, input) in [
		("SELECT s, n FROM t ORDER BY s, n", MorselInput::Range),
		("SELECT string::len(s) AS l, n FROM t ORDER BY l, n", MorselInput::Batches),
		("SELECT s, count() AS c FROM t GROUP BY s", MorselInput::Range),
		("SELECT n % 100 AS b, count() AS c FROM t GROUP BY b", MorselInput::Batches),
		("SELECT math::abs(n % 100) AS b, count() AS c FROM t GROUP BY b", MorselInput::Batches),
	] {
		let program = lowered(&ctx, sql).await;
		let inputs: Vec<_> = program.pipelines.iter().filter_map(effective_input).collect();
		assert_eq!(inputs.first(), Some(&input), "`{sql}`");
	}
}

/// A float fold of one group runs as morsels that keep their terms for the merge, which folds them
/// in the order of the serial run; a fold the merge cannot replay, and a fold of groups of a key,
/// stay serial.
#[tokio::test]
async fn float_folds_plan_morsels_that_keep_their_terms() {
	let folds = |plans: &[Parallel]| -> Vec<(bool, MergeOrder)> {
		plans
			.iter()
			.filter_map(|plan| match plan {
				Parallel::Morsels(MorselPlan {
					combine: Combine::Aggregate,
					ordered_fold,
					merge,
					..
				}) => Some((!ordered_fold.is_empty(), *merge)),
				Parallel::Morsels(_) | Parallel::Serial => None,
			})
			.collect()
	};
	for sql in [
		"SELECT math::sum(f) FROM t GROUP ALL",
		"SELECT math::mean(f), count() FROM t GROUP ALL",
		"SELECT math::stddev(f) AS d, math::variance(f) AS v, math::max(n) AS hi FROM t GROUP ALL",
		"SELECT math::sum(f) AS s, array::group(n) AS ns FROM t WHERE n % 3 = 0 GROUP ALL",
	] {
		assert_eq!(folds(&plans_of(sql).await), [(true, MergeOrder::Serial)], "`{sql}`");
	}
	// An aggregate with no float fold keeps nothing, and combines the partial states of morsels
	// next to one another.
	let plans = plans_of("SELECT math::max(n) FROM t GROUP ALL").await;
	assert_eq!(folds(&plans), [(false, MergeOrder::Adjacent)], "{plans:?}");
	for sql in [
		"SELECT vector::sum([f]) FROM t GROUP ALL",
		"SELECT s, math::sum(f) FROM t GROUP BY s",
		"SELECT n % 2 AS p, math::sum(f) AS s, math::median(f) AS m FROM t GROUP BY p",
	] {
		let plans = plans_of(sql).await;
		assert!(plans.iter().all(|plan| *plan == Parallel::Serial), "`{sql}`: {plans:?}");
	}
}

/// The places the plan names for a float fold are the places of the calls that fold floats among
/// the calls of the aggregate the body fills: a morsel keeps the terms of those accumulators, and
/// no other.
#[tokio::test]
async fn the_places_a_plan_names_are_the_places_of_the_float_calls_of_the_aggregate() {
	let db = TestDb::new_with_config(&format!("{} DEFINE SEQUENCE s;", setup()), viewless()).await;
	let ctx = db.exec_ctx().await;
	for sql in [
		"SELECT math::sum(f) FROM t GROUP ALL",
		"SELECT count() AS c, math::sum(f) AS s, array::group(n) AS ns, math::mean(f) AS m FROM t GROUP ALL",
		"SELECT math::max(n) AS hi, math::sum(f) AS a, math::sum(f) AS b, math::stddev(f) + 1 AS d, count() AS c FROM t GROUP ALL",
		"SELECT array::group(n) AS ns, 3 AS three, math::variance(f) AS v, math::min(n) AS lo FROM t WHERE n > 5 GROUP ALL",
		"SELECT math::sum(f) / count() AS avg, math::mean(f) AS m, count() AS c FROM t GROUP ALL",
	] {
		let program = lowered(&ctx, sql).await;
		let named: Vec<&Vec<usize>> = program
			.pipelines
			.iter()
			.filter_map(|pipeline| match &pipeline.parallel {
				Parallel::Morsels(MorselPlan {
					ordered_fold,
					..
				}) if !ordered_fold.is_empty() => Some(ordered_fold),
				Parallel::Morsels(_) | Parallel::Serial => None,
			})
			.collect();
		assert_eq!(named.len(), 1, "`{sql}`: {named:?}");
		let [sink] = program.aggregates.as_slice() else {
			panic!("`{sql}` fills {} aggregates", program.aggregates.len());
		};
		let calls: Vec<&str> = sink
			.fields
			.iter()
			.flat_map(|field| field.aggregate.iter())
			.flat_map(|aggregate| aggregate.calls.iter())
			.map(|call| call.name.name())
			.collect();
		let floats: Vec<usize> = calls
			.iter()
			.enumerate()
			.filter(|(_, name)| {
				matches!(**name, "math::sum" | "math::mean" | "math::stddev" | "math::variance")
			})
			.map(|(place, _)| place)
			.collect();
		assert_eq!(*named[0], floats, "`{sql}`: {calls:?}");
	}
}

/// A table whose floats lose different bits when they are folded in a different order, with
/// values the folds skip or refuse among them: `f` of a falling series and a tenth, `x` of floats,
/// integers, strings and absent values, and `w` of strings only.
fn float_setup() -> String {
	format!(
		"DEFINE TABLE t;
		 FOR $i IN 0..{ROWS} {{
			CREATE type::record('t', $i) SET
				n = $i,
				f = (1e16 / ($i + 1)) + ($i * 0.1),
				x = IF $i % 29 = 0 {{ 'text' }}
					ELSE IF $i % 23 = 0 {{ NONE }}
					ELSE IF $i % 19 = 0 {{ NULL }}
					ELSE IF $i % 7 = 0 {{ $i }}
					ELSE {{ (1e15 / ($i + 1)) + ($i * 0.37) }},
				w = <string> $i
		 }};"
	)
}

/// Float folds of one group whose morsels keep their terms.
#[cfg(feature = "kv-rocksdb")]
const FLOAT_FOLDS: &[&str] = &[
	"SELECT math::sum(f) AS s, math::mean(f) AS m, math::stddev(f) AS d, math::variance(f) AS v FROM t GROUP ALL",
	"SELECT math::sum(x) AS s, math::mean(x) AS m, math::stddev(x) AS d, math::variance(x) AS v, count() AS c FROM t GROUP ALL",
	"SELECT math::sum(f) AS s, math::max(n) AS hi, array::group(n) AS ns FROM t WHERE n % 3 = 0 GROUP ALL",
	"SELECT math::sum(f) AS s, math::mean(x) AS m FROM t WHERE n > 2900 GROUP ALL",
	"SELECT math::sum(w) AS s FROM t GROUP ALL",
	"SELECT math::variance(w) AS v, math::sum(f) AS s FROM t WHERE n < 5 GROUP ALL",
];

/// The same over bodies that call a function on their rows, which run over decoded rows.
const FLOAT_FOLD_BATCHES: &[&str] = &[
	"SELECT math::sum(f) AS s, math::mean(f) AS m, math::stddev(f) AS d, math::variance(f) AS v FROM t WHERE math::abs(n) >= 0 GROUP ALL",
	"SELECT math::sum(x) AS s, math::mean(x) AS m, math::stddev(x) AS d, math::variance(x) AS v, count() AS c FROM t WHERE math::abs(n) >= 0 GROUP ALL",
	"SELECT math::sum(math::abs(f)) AS s, math::sum(w) AS bad FROM t WHERE n > 2900 GROUP ALL",
];

/// The fold of every term, in the order of the serial run, answers what the serial fold answers
/// bit for bit, whatever the widths and the windows: the failures a fold with nothing to fold
/// raises are the serial run's too.
async fn float_folds_answer_the_serial_fold(
	serial: &TestDb,
	wide: &TestDb,
	statements: &[&str],
	ranges: bool,
) -> Counts {
	plans_combine(wide, statements, Combine::Aggregate).await;
	for sql in statements {
		let ctx = wide.exec_ctx().await;
		let program = lowered(&ctx, sql).await;
		assert!(
			program.pipelines.iter().any(|pipeline| matches!(
				&pipeline.parallel,
				Parallel::Morsels(MorselPlan { ordered_fold, .. }) if !ordered_fold.is_empty()
			)),
			"`{sql}` keeps no terms"
		);
	}
	let (expected, _) = answer(serial, statements[0]).await;
	assert!(expected.is_ok(), "{expected:?}");
	let counts = same_answers_in(serial, wide, statements, ranges).await;
	if ranges {
		assert!(counts.split > 1, "{counts:?}");
	}
	counts
}

/// The terms of [`float_setup`] tell the order they are folded in: the serial fold of the same
/// terms in the opposite order answers other bits, so a fold that answered the serial fold's
/// proves the terms were folded in order.
#[tokio::test]
async fn the_terms_of_the_float_setup_tell_the_order_they_are_folded_in() {
	let db = TestDb::new_with_config(&float_setup(), whole()).await;
	for field in ["f", "x"] {
		let forward = answered(
			&db,
			&format!("SELECT math::sum({field}) AS s, math::stddev({field}) AS d FROM t GROUP ALL"),
		)
		.await;
		let backward = answered(
			&db,
			&format!(
				"SELECT math::sum({field}) AS s, math::stddev({field}) AS d \
				 FROM (SELECT {field} FROM t ORDER BY id DESC) GROUP ALL"
			),
		)
		.await;
		assert!(forward.is_ok() && backward.is_ok(), "{forward:?} {backward:?}");
		assert_ne!(forward, backward, "`{field}`");
	}
}

/// Windows that park a morsel after each chunk, a few morsels per statement, and a window nothing
/// reaches.
fn window_settings() -> [surrealdb_cnf::ConfigMap; 3] {
	[
		stealing().with_key_value("morsel_window_bytes", "1"),
		few_hundred()
			.with_key_value("vm_quantum", "64")
			.with_key_value("morsel_window_bytes", "4096"),
		tiny().with_key_value("morsel_window_bytes", (1usize << 30).to_string()),
	]
}

#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn float_folds_over_key_ranges_answer_the_serial_fold() {
	let _turn = TURNS.lock().await;
	let serial_dir = tempfile::tempdir().expect("temporary directory");
	let serial = TestDb::new_on_rocksdb(&float_setup(), serial_dir.path(), whole()).await;
	let mut parked = Vec::new();
	for config in window_settings() {
		let dir = tempfile::tempdir().expect("temporary directory");
		let wide = TestDb::new_on_rocksdb(&float_setup(), dir.path(), config).await;
		float_folds_answer_the_serial_fold(&serial, &wide, FLOAT_FOLDS, true).await;
		let before = counts_of(&wide.exec_ctx().await);
		let counts = float_folds_answer_the_serial_fold(&serial, &wide, FLOAT_FOLDS, true).await;
		let after = counts_of(&wide.exec_ctx().await);
		assert!(after.handed > before.handed || !places_given(&wide), "no morsel ran on a worker");
		// What the morsels keep for the merge is charged to the lease as they open its blocks.
		assert!(
			after.terms_charged > before.terms_charged || !places_given(&wide),
			"the terms the morsels kept were charged to no lease"
		);
		parked.push(counts.parked);
	}
	// A morsel that holds as many kept terms as its window allows parks, and the merge folds the
	// terms of what it ran before those of the rest of its range: the two small windows park.
	assert!(parked[..2].iter().all(|parked| *parked > 0), "{parked:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn float_folds_over_batches_answer_the_serial_fold() {
	let _turn = TURNS.lock().await;
	let serial = TestDb::new_with_config(&float_setup(), whole()).await;
	for config in window_settings() {
		let wide = TestDb::new_with_config(&float_setup(), config).await;
		float_folds_answer_the_serial_fold(&serial, &wide, FLOAT_FOLD_BATCHES, false).await;
		let before = counts_of(&wide.exec_ctx().await);
		let (handed, charged) = (before.handed, before.terms_charged);
		float_folds_answer_the_serial_fold(&serial, &wide, FLOAT_FOLD_BATCHES, false).await;
		let after = counts_of(&wide.exec_ctx().await);
		assert!(after.handed > handed || !places_given(&wide), "no batch ran on a worker");
		assert!(
			after.terms_charged > charged || !places_given(&wide),
			"the terms the batches kept were charged to no lease"
		);
	}
}

/// The terms a batch keeps for the merge are charged to the lease whether the batch runs on a
/// worker or on the coordinator, which takes every batch when it holds no place.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_terms_of_batches_the_coordinator_runs_are_charged_too() {
	let _turn = TURNS.lock().await;
	let serial = TestDb::new_with_config(&float_setup(), whole()).await;
	let narrow =
		TestDb::new_with_config(&float_setup(), tiny().with_key_value("morsel_places", "0")).await;
	let before = counts_of(&narrow.exec_ctx().await);
	float_folds_answer_the_serial_fold(&serial, &narrow, FLOAT_FOLD_BATCHES, false).await;
	let after = counts_of(&narrow.exec_ctx().await);
	assert_eq!(after.handed, before.handed, "a batch ran on a worker");
	assert!(after.terms_charged > before.terms_charged, "the terms the batches kept were charged");
}

/// A grant of the lease that a morsel asks for the blocks of its terms and the query's memory
/// refuses revokes the lease as any refused grant does: the morsel parks after its chunk, the
/// coordinator runs what is left of its range, and the fold is the serial fold, bit for bit.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_float_fold_whose_terms_are_refused_a_grant_answers_the_serial_fold() {
	let _turn = TURNS.lock().await;
	let serial_dir = tempfile::tempdir().expect("temporary directory");
	let serial = TestDb::new_on_rocksdb(&float_setup(), serial_dir.path(), whole()).await;
	let dir = tempfile::tempdir().expect("temporary directory");
	let wide = TestDb::new_on_rocksdb(&float_setup(), dir.path(), tiny()).await;
	// The first grant, a worker's copy of the slots, is made; the grants for its terms are refused.
	let failing = fail_grants(&wide.exec_ctx().await, 1);
	for _ in 0..5 {
		for sql in FLOAT_FOLDS {
			let expected = answered(&serial, sql).await;
			assert_equivalent(sql, &answered(&wide, sql).await, &expected);
		}
		if failing.refused() > 0 {
			break;
		}
	}
	assert!(failing.refused() > 0, "no grant was refused");
}

/// With no place to take, the coordinator runs every morsel itself, and the fold is the same.
#[tokio::test]
async fn float_folds_answer_the_serial_fold_on_the_coordinator() {
	let serial = TestDb::new_with_config(&float_setup(), whole()).await;
	let wide =
		TestDb::new_with_config(&float_setup(), stealing().with_key_value("morsel_places", "0"))
			.await;
	float_folds_answer_the_serial_fold(&serial, &wide, FLOAT_FOLD_BATCHES, false).await;
}

/// A table whose `m` multiplies without overflow but for the rows `n * m` fails on: rows 2 and
/// 2500.
fn overflow_setup() -> String {
	format!(
		"{} UPDATE t SET m = 1; UPDATE t:2 SET m = 9223372036854775807; \
		 UPDATE t:2500 SET m = 9223372036854775807;",
		setup()
	)
}

const PAGED: &[&str] = &[
	"SELECT n * m AS x FROM t LIMIT 3",
	"SELECT n * m AS x FROM t LIMIT 1 START 10",
	"SELECT n * m AS x FROM t START 10 LIMIT 3",
	"SELECT n * m AS x FROM t START 5 LIMIT 2490",
	"SELECT n * m AS x FROM t START 5 LIMIT 2600",
	"SELECT n * m AS x FROM t LIMIT 3000",
	"SELECT n * m AS x FROM t START 2490 LIMIT 20",
	"SELECT n * m AS x FROM t WHERE n % 100 = 7 LIMIT 5",
	"SELECT n, s FROM t WHERE n > 1500 LIMIT 3",
	"SELECT n, s FROM t ORDER BY id DESC START 7 LIMIT 11",
	"SELECT n, s FROM t START 2900",
];

/// A row past a spent limit or skipped by `START` never raises its failure, a row the limit
/// reaches does, and the answer is the serial run's at every width: a paged pipeline replays its
/// rows in the serial order.
async fn paged_answers(serial: &TestDb, wide: &TestDb) {
	let mut errors = 0;
	let mut ranges = 0;
	for sql in PAGED {
		let (expected, _) = answer(serial, sql).await;
		let (got, range) = answer(wide, sql).await;
		assert_equivalent(sql, &got, &expected);
		errors += usize::from(expected.is_err());
		ranges += usize::from(range);
	}
	assert!(errors > 0 && errors < PAGED.len(), "{errors} of the statements fail");
	assert!(ranges > 0, "no paged statement read a key range in morsels");
}

#[tokio::test]
async fn a_failure_past_a_spent_limit_is_never_raised_on_the_coordinator() {
	let serial = TestDb::new_with_config(&overflow_setup(), whole()).await;
	let wide = TestDb::new_with_config(&overflow_setup(), tiny()).await;
	paged_answers(&serial, &wide).await;
}

#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread")]
async fn a_failure_past_a_spent_limit_is_never_raised() {
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(&overflow_setup(), serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_rocksdb(&overflow_setup(), wide_dir.path(), tiny()).await;
	paged_answers(&serial, &wide).await;
}

/// A spent limit cancels the morsels still out and reads no further: a limit that the first rows
/// of a scan spend reads about a window of the table, not the table.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread")]
async fn a_spent_limit_reads_no_further() {
	let _turn = TURNS.lock().await;
	let dir = tempfile::tempdir().expect("temporary directory");
	let wide = TestDb::new_on_rocksdb(&setup(), dir.path(), tiny()).await;
	for (sql, needs) in [
		("SELECT * FROM t LIMIT 1", 1),
		("SELECT * FROM t WHERE n >= 100 LIMIT 1", 101),
		("SELECT * FROM t WHERE n >= 100 LIMIT 5", 105),
	] {
		let before = counts_of(&wide.exec_ctx().await);
		let (answered, _) = answer(&wide, sql).await;
		assert!(answered.is_ok(), "`{sql}`");
		let read = counts_of(&wide.exec_ctx().await).scanned - before.scanned;
		// The entries the limit needs, and the morsels in flight beyond them.
		let window = needs + 8 * std::thread::available_parallelism().map_or(1, |n| n.get());
		assert!(read <= window.max(ROWS / 4), "`{sql}` read {read} entries");
		assert!(read < ROWS, "`{sql}` read the table");
	}
}

/// A reader that takes one row of a table's scan is answered by the coordinator, which cuts no
/// range for it. The policy's unit test (`split_waits_for_the_threshold_and_the_first_rows`) holds
/// the rule for a reader whose first rows do not come at once.
#[tokio::test]
async fn a_scan_hint_does_not_split_before_the_first_flush() {
	let wide = TestDb::new_with_config(&setup(), tiny()).await;
	let before = counts_of(&wide.exec_ctx().await);
	let (answered, _) = answer(&wide, "SELECT VALUE n FROM type::table('t') LIMIT 1").await;
	assert_eq!(answered, Ok(vec![Value::from(0)]));
	let after = counts_of(&wide.exec_ctx().await);
	assert_eq!(after.split, before.split, "{after:?}");
	// A reader that holds a hinted scan in a projection is answered by the coordinator as well.
	let sql = "SELECT (SELECT VALUE n FROM t LIMIT 1) AS one FROM ONLY 1";
	let serial = TestDb::new_with_config(&setup(), whole()).await;
	let (expected, _) = answer(&serial, sql).await;
	let (answered, _) = answer(&wide, sql).await;
	assert_eq!(answered, expected, "`{sql}`");
	assert!(answered.is_ok(), "`{sql}`: {answered:?}");
	let last = counts_of(&wide.exec_ctx().await);
	assert_eq!(last.split, after.split, "{last:?}");
}

/// The counters `EXPLAIN ANALYZE` renders count every row any morsel read or produced, so its text
/// does not depend on how the scan was cut.
async fn analyzed(db: &TestDb, sql: &str) -> String {
	let ctx = db.exec_ctx().await;
	let vm = statement(&ctx, &format!("EXPLAIN ANALYZE {sql}")).await;
	match outcome(&vm, &ctx).await.expect("answers").as_slice() {
		[Value::String(text)] => without_times(text),
		other => panic!("`{sql}` answers {other:?}"),
	}
}

/// `text` without the times it reports, which no two runs share.
fn without_times(text: &str) -> String {
	let mut out = String::new();
	let mut rest = text;
	while let Some(at) = rest.find("elapsed: ") {
		let (before, after) = rest.split_at(at);
		out.push_str(before);
		out.push_str("elapsed: _");
		rest = after.find('}').map_or("", |end| &after[end..]);
	}
	out.push_str(rest);
	out
}

async fn analyses_match(serial: &TestDb, wide: &TestDb) {
	for sql in [
		"SELECT * FROM t WHERE n > 10",
		"SELECT count() FROM t GROUP ALL",
		"SELECT * FROM t LIMIT 5",
		"SELECT n FROM t ORDER BY n LIMIT 5",
		"SELECT n FROM t WHERE n > 10 ORDER BY f DESC LIMIT 5 START 2",
	] {
		let expected = analyzed(serial, sql).await;
		assert_eq!(analyzed(wide, sql).await, expected, "`{sql}`");
	}
}

#[tokio::test]
async fn explain_analyze_row_counts_are_width_invariant_on_the_coordinator() {
	let serial = TestDb::new_with_config(&setup(), whole()).await;
	let wide = TestDb::new_with_config(&setup(), tiny()).await;
	analyses_match(&serial, &wide).await;
}

#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread")]
async fn explain_analyze_row_counts_are_width_invariant() {
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(&setup(), serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_rocksdb(&setup(), wide_dir.path(), tiny()).await;
	analyses_match(&serial, &wide).await;
}

/// A body in the storage cursor that pages to the output reads its key range in morsels, its
/// paging replayed by the merge, as one that does not page does.
#[tokio::test]
async fn a_paged_scan_in_the_cursor_reads_ranges() {
	let range = |plans: &[Parallel], paged: bool| {
		plans.iter().any(|plan| match plan {
			Parallel::Morsels(MorselPlan {
				input: MorselInput::Range,
				paged: at,
				..
			}) => at.is_some() == paged,
			_ => false,
		})
	};
	assert!(range(&plans_of("SELECT n, s FROM t WHERE n > 1500 LIMIT 3").await, true));
	assert!(range(&plans_of("SELECT n, s FROM t START 7").await, true));
	assert!(range(&plans_of("SELECT n, s FROM t").await, false));
}

/// Rows of the table whose selective `START` the bounded-ends tests read.
const SPARSE_ROWS: usize = 20_000;

fn sparse_setup() -> String {
	format!(
		"DEFINE TABLE t;
		 FOR $i IN 0..{SPARSE_ROWS} {{
			CREATE type::record('t', $i) SET n = $i
		 }};"
	)
}

/// Morsels that span thousands of entries and park at every chunk that holds anything.
fn sparse() -> surrealdb_cnf::ConfigMap {
	tiny().with_key_value("morsel_target_bytes", "20000").with_key_value("morsel_window_bytes", "1")
}

/// A paged scan of which one entry in a hundred reaches the paging keeps one row end per row that
/// does, not one per entry it ran: the morsels' ends stay bounded by what the merge must replay,
/// and the answer is the serial run's.
async fn row_ends_stay_bounded(serial: &TestDb, wide: &TestDb) {
	let sql = "SELECT n FROM t WHERE n % 100 = 7 START 10";
	let program = lowered(&wide.exec_ctx().await, sql).await;
	assert!(
		program.pipelines.iter().any(|pipeline| pipeline.in_place
			&& matches!(
				pipeline.parallel,
				Parallel::Morsels(MorselPlan {
					input: MorselInput::Range,
					paged: Some(_),
					..
				})
			)),
		"the statement is not a paged Range morsel in the cursor: {:?}",
		program.pipelines.iter().map(|pipeline| &pipeline.parallel).collect::<Vec<_>>()
	);
	let counts = same_answers(serial, wide, &[sql]).await;
	assert!(counts.scanned >= SPARSE_ROWS, "{counts:?}");
	assert!(counts.split > 0, "{counts:?}");
	assert!(counts.most_ends < 64, "{counts:?}");
	// A worker's morsel returns row ends for the merge to replay; the coordinator's own replay
	// none.
	assert!(counts.handed == 0 || counts.most_ends > 0, "{counts:?}");
}

#[tokio::test]
async fn a_selective_start_keeps_its_row_ends_bounded_on_the_coordinator() {
	let serial = TestDb::new_with_config(&sparse_setup(), whole()).await;
	let wide = TestDb::new_with_config(&sparse_setup(), sparse()).await;
	row_ends_stay_bounded(&serial, &wide).await;
}

#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread")]
async fn a_selective_start_keeps_its_row_ends_bounded() {
	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(&sparse_setup(), serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_rocksdb(&sparse_setup(), wide_dir.path(), sparse()).await;
	row_ends_stay_bounded(&serial, &wide).await;
}

// ---------------------------------------------------------------------------
// Failed splits and narrowings, the statement's places, worker reads in the metrics
// ---------------------------------------------------------------------------

/// Statements whose scans are cut and donated from when nothing fails.
const CUT_SCANS: &[&str] = &[
	"SELECT * FROM t",
	"SELECT * FROM t:100..2900",
	"SELECT n, s FROM t ORDER BY id DESC",
	"SELECT n FROM t WHERE n % 7 = 0",
];

/// A split that fails finds no boundary: every range is read whole, the answers are the serial
/// run's rows, and no error reaches the statement.
async fn failed_splits_read_whole(serial: &TestDb, wide: &TestDb) {
	// A body that runs over decoded rows is handed on in batches whatever a split does.
	let statements: Vec<&str> = CUT_SCANS.iter().chain(STEALS).copied().collect();
	let (ranges, others) = split_by_ranges(wide, &statements).await;
	run_all(serial, wide, &others).await;
	let before = counts_of(&wide.exec_ctx().await);
	let failing = fail_splits(&wide.exec_ctx().await);
	run_all_unproved(serial, wide, &ranges).await;
	assert!(failing.refused() > 0, "no split was asked for");
	drop(failing);
	let after = counts_of(&wide.exec_ctx().await);
	assert_eq!(after.split, before.split, "{after:?}");
	assert_eq!(after.stolen, before.stolen, "{after:?}");
	assert_eq!(after.resplit, before.resplit, "{after:?}");
	assert_eq!(after.handed, before.handed, "{after:?}");
}

#[tokio::test]
async fn a_failed_split_runs_the_range_whole_on_the_coordinator() {
	let serial = TestDb::new_with_config(&skewed_setup(), whole()).await;
	let wide = TestDb::new_with_config(&skewed_setup(), stealing()).await;
	failed_splits_read_whole(&serial, &wide).await;
}

#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_split_runs_the_range_whole() {
	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(&skewed_setup(), serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_rocksdb(&skewed_setup(), wide_dir.path(), stealing()).await;
	failed_splits_read_whole(&serial, &wide).await;
}

/// A cursor that refuses to narrow keeps the range it has: the morsel that was to give part of it
/// away reads all of it, and the answers are the serial run's rows.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_narrow_keeps_the_morsel() {
	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(&skewed_setup(), serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_rocksdb(&skewed_setup(), wide_dir.path(), stealing()).await;
	let failing = fail_narrows(&wide.exec_ctx().await);
	// Other tests of the process may hold the node's places while workers should donate.
	for _ in 0..5 {
		run_all(&serial, &wide, CUT_SCANS).await;
		run_all(&serial, &wide, STEALS).await;
		if failing.refused() > 0 {
			break;
		}
	}
	assert!(failing.refused() > 0, "no worker's cursor was asked to narrow");
}

/// What the transaction `sql` runs on counted as read keys, and what the morsels of its datastore
/// read and handed to workers meanwhile.
#[cfg(feature = "kv-rocksdb")]
async fn keys_read_by(db: &TestDb, sql: &str) -> (u64, Counts) {
	let ctx = db.exec_ctx().await;
	let vm = statement(&ctx, sql).await;
	let (before, counted) = (ctx.txn().metrics_snapshot_for_test().keys_read, counts_of(&ctx));
	let answered = outcome(&vm, &ctx).await.expect("answers");
	assert_eq!(answered.len(), ROWS);
	let after = counts_of(&ctx);
	let read = ctx.txn().metrics_snapshot_for_test().keys_read - before;
	(
		read,
		Counts {
			handed: after.handed - counted.handed,
			scanned: after.scanned - counted.scanned,
			..Counts::default()
		},
	)
}

/// What workers read through the view is counted in the transaction's scan metrics, so every entry
/// a morsel read, on a worker or the coordinator, is a key the transaction read. The serial run
/// reads each entry once. A morsel that ends at a cut reads the entry at the cut before it finds it
/// is past its range, and the morsel that follows reads it again, so the wide run reads no fewer
/// keys than the serial one and may read a few more.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread")]
async fn worker_reads_count_in_the_transactions_scan_metrics() {
	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(&setup(), serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_rocksdb(&setup(), wide_dir.path(), tiny()).await;
	let sql = "SELECT * FROM t";
	let (expected, _) = keys_read_by(&serial, sql).await;
	assert!(expected >= ROWS as u64, "{expected}");
	// Other tests of the process may hold the node's places while workers should read.
	let mut handed = 0;
	for _ in 0..5 {
		let (read, counts) = keys_read_by(&wide, sql).await;
		assert_eq!(read, counts.scanned as u64, "keys read against entries the morsels read");
		assert!(read >= expected, "{read} keys read, {expected} by the serial run");
		handed += counts.handed;
		if handed > 0 {
			break;
		}
	}
	assert!(handed > 0, "no morsel went to a worker");
}

/// A statement that times out while its morsels run answers the serial run's error and leaves no
/// worker holding a cursor on the transaction.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread")]
async fn a_timed_out_statement_releases_its_workers() {
	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(&setup(), serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_rocksdb(&setup(), wide_dir.path(), tiny()).await;
	let sql = "SELECT * FROM t WHERE string::len(string::repeat(s, 20000)) > 0 TIMEOUT 5ms";
	let (expected, _) = answer(&serial, sql).await;
	let failure = expected.as_ref().expect_err("the serial run times out");
	assert!(failure.contains("timeout"), "{failure}");
	let ctx = wide.exec_ctx().await;
	let vm = statement(&ctx, sql).await;
	let answered = outcome(&vm, &ctx).await;
	assert_eq!(answered, expected);
	drop(vm);
	tokio::time::timeout(std::time::Duration::from_secs(20), ctx.txn().cancel())
		.await
		.expect("the transaction waits for a worker after the timeout")
		.expect("cancels");
}

// ---------------------------------------------------------------------------
// Stealing, re-splitting and the morsel counters
// ---------------------------------------------------------------------------

/// Rows of the skewed table's first band.
const BAND: usize = 5_000;

/// A table whose ids sit in two bands far apart in key space: nearly every row lies in the first,
/// so a cut that divides the key space evenly leaves one morsel holding the table.
fn skewed_setup() -> String {
	format!(
		"DEFINE TABLE t;
		 FOR $i IN 0..{BAND} {{
			CREATE type::record('t', $i) SET n = $i, s = <string> $i, f = $i * 0.5
		 }};
		 FOR $i IN 0..100 {{
			CREATE type::record('t', 1000000000 + $i) SET n = 10000 + $i, s = <string> $i, f = 0.5
		 }};"
	)
}

/// Settings under which every running morsel donates at every chunk boundary.
fn stealing() -> surrealdb_cnf::ConfigMap {
	tiny().with_key_value("morsel_steal_always", "true")
}

const STEALS: &[&str] = &[
	"SELECT * FROM t",
	"SELECT * FROM t ORDER BY id",
	"SELECT count(), math::max(n), math::min(n) FROM t GROUP ALL",
	"SELECT array::group(n) FROM t GROUP ALL",
	"SELECT n FROM t WHERE n % 7 = 0",
	"SELECT n FROM t START 100 LIMIT 50",
	"SELECT n FROM t WHERE n > 2500 LIMIT 5",
];

const BACKWARD_STEALS: &[&str] = &[
	"SELECT n, s FROM t ORDER BY id DESC",
	"SELECT n, s FROM t ORDER BY id DESC START 7 LIMIT 11",
	"SELECT array::group(n) FROM t GROUP ALL",
	"SELECT n FROM t WHERE n % 5 = 1 ORDER BY id DESC",
];

/// Every donation of a running morsel keeps the answers of the serial run, in the statements that
/// show it: unordered and ordered output, exact and appending aggregates, and paging. Rows keep the
/// serial order where the statement orders or pages them.
async fn steals_keep_the_order(serial: &TestDb, wide: &TestDb, statements: &[&str]) {
	let before = metrics_of(&wide.exec_ctx().await);
	let counts = same_answers(serial, wide, statements).await;
	let after = metrics_of(&wide.exec_ctx().await);
	assert!(counts.stolen > 0, "{counts:?}");
	assert!(after.stolen > before.stolen, "{after:?}");
}

#[tokio::test]
async fn a_steal_never_reorders_output_on_the_coordinator() {
	let serial = TestDb::new_with_config(&skewed_setup(), whole()).await;
	let wide = TestDb::new_with_config(&skewed_setup(), stealing()).await;
	steals_keep_the_order(&serial, &wide, STEALS).await;
}

/// Workers read through the view and are asked, and the coordinator's own morsel donates too.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread")]
async fn a_steal_never_reorders_output() {
	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(&skewed_setup(), serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_rocksdb(&skewed_setup(), wide_dir.path(), stealing()).await;
	steals_keep_the_order(&serial, &wide, STEALS).await;
	assert!(counts_of(&wide.exec_ctx().await).handed > 0);
}

#[tokio::test]
async fn a_backward_steal_keeps_the_lower_part_on_the_coordinator() {
	let serial = TestDb::new_with_config(&skewed_setup(), whole()).await;
	let wide = TestDb::new_with_config(&skewed_setup(), stealing()).await;
	steals_keep_the_order(&serial, &wide, BACKWARD_STEALS).await;
}

/// A backward scan keeps the part it reads first, and donates the part below it, which the merge
/// takes after.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread")]
async fn a_backward_steal_keeps_the_lower_part() {
	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(&skewed_setup(), serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_rocksdb(&skewed_setup(), wide_dir.path(), stealing()).await;
	steals_keep_the_order(&serial, &wide, BACKWARD_STEALS).await;
	assert!(answer(&wide, "SELECT n, s FROM t ORDER BY id DESC").await.1);
}

/// Settings under which a morsel cuts itself when it will outlast its siblings: no morsel donates
/// unasked, the first cut is after one entry, and a morsel is a few kilobytes.
fn resplitting() -> surrealdb_cnf::ConfigMap {
	tiny().with_key_value("morsel_target_bytes", "8000")
}

/// With no view the coordinator runs every morsel, so no worker is idle to ask it for part of its
/// range and its queue never drains for it to cut itself: it donates nothing, and answers the same.
#[tokio::test]
async fn a_coordinator_alone_has_nobody_to_take_what_it_gives() {
	let serial = TestDb::new_with_config(&skewed_setup(), whole()).await;
	let wide = TestDb::new_with_config(&skewed_setup(), resplitting()).await;
	let counts = same_answers(&serial, &wide, STEALS).await;
	assert_eq!((counts.stolen, counts.resplit), (0, 0), "{counts:?}");
}

/// The first cut puts nearly the whole table in one morsel; once the queue of the others is
/// drained that morsel cuts itself, and the answers are the whole scan's.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread")]
async fn resplit_divides_a_hot_morsel() {
	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(&skewed_setup(), serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_rocksdb(&skewed_setup(), wide_dir.path(), resplitting()).await;
	let mut resplit = 0;
	// Other tests of the process may hold the node's places while the queue should drain.
	for _ in 0..25 {
		let counts = same_answers(&serial, &wide, STEALS).await;
		resplit += counts.resplit;
		if resplit > 0 {
			break;
		}
	}
	assert!(resplit > 0, "no morsel cut itself");
	assert!(metrics_of(&wide.exec_ctx().await).resplit > 0);
}

/// A worker that has drained the cheap morsels asks the hot one to donate, without being forced
/// to: the hot morsel's remainder is as large as two morsels, and it gives part of it away.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread")]
async fn an_idle_worker_asks_the_hot_morsel_for_part_of_its_range() {
	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(&skewed_setup(), serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_rocksdb(&skewed_setup(), wide_dir.path(), resplitting()).await;
	let mut shared = 0;
	// Other tests of the process may hold the node's places while workers should be idle.
	for _ in 0..5 {
		let counts = same_answers(&serial, &wide, STEALS).await;
		shared += counts.stolen + counts.resplit;
		if shared > 0 {
			break;
		}
	}
	assert!(shared > 0, "the hot morsel gave nothing away");
	let scans = metrics_of(&wide.exec_ctx().await);
	assert_eq!(scans.stolen as usize, counts_of(&wide.exec_ctx().await).stolen);
}

/// The text `EXPLAIN ANALYZE` renders when no morsel counters are asked for is the same at every
/// width, forced donation included.
async fn analyses_match_under_stealing(serial: &TestDb, wide: &TestDb) {
	analyses_match(serial, wide).await;
	assert!(!analyzed(wide, "SELECT * FROM t WHERE n > 10").await.contains("Morsels"));
}

#[tokio::test]
async fn explain_analyze_is_unchanged_by_default_on_the_coordinator() {
	let serial = TestDb::new_with_config(&setup(), whole()).await;
	let wide = TestDb::new_with_config(&setup(), stealing()).await;
	analyses_match_under_stealing(&serial, &wide).await;
}

#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread")]
async fn explain_analyze_is_unchanged_by_default() {
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(&setup(), serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_rocksdb(&setup(), wide_dir.path(), stealing()).await;
	analyses_match_under_stealing(&serial, &wide).await;
}

/// What a text of morsel counters may hold: no key bytes, which are not printable ASCII text.
fn names_no_keys(block: &str) {
	assert!(block.chars().all(|c| c == '\n' || c == ' ' || c.is_ascii_graphic()), "{block}");
	assert!(!block.contains("/*") && !block.contains("\\x"), "{block}");
}

#[tokio::test]
async fn explain_analyze_reports_morsels_when_asked() {
	let wide = TestDb::new_with_config(
		&setup(),
		stealing().with_key_value("morsel_explain_analyze", "true"),
	)
	.await;
	let ctx = wide.exec_ctx().await;
	let vm = statement(&ctx, "EXPLAIN ANALYZE SELECT * FROM t WHERE n > 10").await;
	let text = match outcome(&vm, &ctx).await.expect("answers").as_slice() {
		[Value::String(text)] => text.to_string(),
		other => panic!("answers {other:?}"),
	};
	let (plan, block) = text.split_once("Morsels:").expect("the morsel block is rendered");
	assert!(plan.contains("Total rows: 2989"), "{text}");
	assert!(block.contains("initial: 1"), "{text}");
	assert!(block.contains("#0: rows:"), "{text}");
	names_no_keys(block);
	// The JSON rendering carries the same figures under one key.
	let vm = statement(&ctx, "EXPLAIN ANALYZE FORMAT JSON SELECT * FROM t WHERE n > 10").await;
	let rendered = outcome(&vm, &ctx).await.expect("answers");
	let json = format!("{rendered:?}");
	assert!(json.contains("morsels") && json.contains("ordinal"), "{json}");
}

/// The rows `EXPLAIN ANALYZE` reports workers read in the `Morsels:` block of `sql`, or `None`
/// where the statement renders no block.
#[cfg(feature = "kv-rocksdb")]
async fn worker_rows_of(db: &TestDb, sql: &str) -> Option<u64> {
	let ctx = db.exec_ctx().await;
	let vm = statement(&ctx, &format!("EXPLAIN ANALYZE {sql}")).await;
	let text = match outcome(&vm, &ctx).await.expect("answers").as_slice() {
		[Value::String(text)] => text.to_string(),
		other => panic!("`{sql}` answers {other:?}"),
	};
	let (_, block) = text.split_once("Morsels:")?;
	let (_, workers) = block.split_once("workers: rows: ")?;
	let digits: String = workers.chars().take_while(char::is_ascii_digit).collect();
	Some(digits.parse().unwrap_or_else(|_| panic!("`{sql}` renders {block}")))
}

/// Every read shape the planner fans out runs on workers on a backend that gives them a read view.
/// A shape whose body runs in the storage cursor reads its key ranges as morsels and reports a
/// `Morsels:` block in which workers read rows: a projection, a sort, a top-k, a grouping, a paged
/// scan, a backward scan and a record range among them. A shape whose body does not run in the
/// cursor hands decoded batches to workers, which the hand-off counter shows.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread")]
async fn read_shapes_run_on_workers_on_a_backend_with_a_view() {
	let _turn = TURNS.lock().await;
	let dir = tempfile::tempdir().expect("temporary directory");
	let db = TestDb::new_on_rocksdb(
		&merge_setup(),
		dir.path(),
		stealing().with_key_value("morsel_explain_analyze", "true"),
	)
	.await;
	for sql in [
		"SELECT * FROM t",
		"SELECT * FROM t WHERE n > 5",
		"SELECT * FROM t:100..2500",
		"SELECT n FROM t",
		"SELECT id, n FROM t",
		"SELECT n FROM t WHERE n % 7 = 0",
		"SELECT n FROM t:100..2500",
		"SELECT n, k FROM t ORDER BY k",
		"SELECT n, k FROM t ORDER BY k LIMIT 10",
		"SELECT n, k FROM t ORDER BY k LIMIT 10 START 5",
		"SELECT n FROM t ORDER BY id DESC",
		"SELECT k, count() FROM t GROUP BY k",
		"SELECT count() FROM t WHERE n > 5 GROUP ALL",
		"SELECT math::max(n) FROM t GROUP ALL",
		"SELECT * FROM t LIMIT 10 START 5",
		"SELECT n FROM t WHERE n > 3 LIMIT 100",
	] {
		let rows = worker_rows_of(&db, sql).await;
		assert!(rows.is_some_and(|rows| rows > 0), "`{sql}`: workers read {rows:?} rows");
	}
	for sql in [
		"SELECT math::abs(n) FROM t WHERE n % 7 = 0",
		"SELECT n % 10 AS k, count() FROM t GROUP BY k",
		"SELECT * FROM t ORDER BY s",
		"SELECT * FROM t ORDER BY s LIMIT 10",
	] {
		let handed = counts_of(&db.exec_ctx().await).handed;
		answered(&db, sql).await.expect("answers");
		assert!(
			counts_of(&db.exec_ctx().await).handed > handed,
			"`{sql}`: no batch ran on a worker"
		);
	}
}

/// A worker that has held its place for a quantum while another statement waits for one stops at
/// its chunk boundary and queues what is left of its morsel, and the answers are the whole scan's.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread")]
async fn a_worker_yields_its_place_while_another_statement_waits() {
	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(&setup(), serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_rocksdb(
		&setup(),
		wide_dir.path(),
		// Morsels of a few hundred entries, read in chunks of a quantum of fuel.
		tiny()
			.with_key_value("morsel_target_bytes", "20000")
			.with_key_value("morsel_place_quantum", "1")
			.with_key_value("vm_quantum", "64"),
	)
	.await;
	let waiter = waiting_statement();
	let mut yielded = 0;
	for _ in 0..5 {
		same_answers(&serial, &wide, SCANS).await;
		yielded = metrics_of(&wide.exec_ctx().await).yielded;
		if yielded > 0 {
			break;
		}
	}
	drop(waiter);
	assert!(yielded > 0, "no worker returned its place");
}

// ---------------------------------------------------------------------------
// Sorted runs, a shared top-k threshold, partitioned groups and merge-side distinct
// ---------------------------------------------------------------------------

/// Rows of the table the merge tests read.
const MERGE_ROWS: usize = 5_000;

/// A table whose keys tie many times over: `b` takes two values, `k` seven, `m` thirteen, `g` five
/// and `h` two thousand, beside the unique `n`, a string and a float.
fn merge_setup() -> String {
	format!(
		"DEFINE TABLE t;
		 FOR $i IN 0..{MERGE_ROWS} {{
			CREATE type::record('t', $i) SET
				n = $i, k = $i % 7, m = $i % 13, g = $i % 5, h = $i % 2000, b = $i % 2,
				s = <string> $i, f = $i * 0.5
		 }};"
	)
}

/// How the pipelines of `sql` combine their morsels over `db`.
async fn combines_of(db: &TestDb, sql: &str) -> Vec<Combine> {
	let ctx = db.exec_ctx().await;
	lowered(&ctx, sql)
		.await
		.pipelines
		.iter()
		.filter_map(|pipeline| match &pipeline.parallel {
			Parallel::Morsels(plan) => Some(plan.combine),
			Parallel::Serial => None,
		})
		.collect()
}

/// Assert that every statement plans a pipeline combined as `combine` over `db`.
async fn plans_combine(db: &TestDb, statements: &[&str], combine: Combine) {
	for sql in statements {
		let combines = combines_of(db, sql).await;
		assert!(combines.contains(&combine), "`{sql}` plans {combines:?}, not {combine:?}");
	}
}

/// Sorts that keep every row, whose keys tie many times.
const SORT_RUNS: &[&str] = &[
	"SELECT n, k FROM t ORDER BY k",
	"SELECT n, k, m FROM t ORDER BY k DESC, n",
	"SELECT n, m FROM t ORDER BY m, k DESC",
	"SELECT n FROM t WHERE n % 3 = 0 ORDER BY k",
];

/// Morsels sort their own entries into runs, which the statement merges in the order the serial
/// run reads the morsels: equal keys keep their arrival order. `workers_run` is whether `wide`'s
/// backend always hands morsels to workers; otherwise runs are counted only where a worker is
/// wanted.
async fn runs_merge_in_serial_order(serial: &TestDb, wide: &TestDb, workers_run: bool) {
	plans_combine(wide, SORT_RUNS, Combine::StableMerge).await;
	// A sort's body runs in the storage cursor: each morsel is a key range, and one a worker takes
	// sorts a run of its own, where the coordinator runs the morsels on the statement's own state.
	let counts = same_answers(serial, wide, SORT_RUNS).await;
	assert!(counts.split > 1, "{counts:?}");
	if workers_run || workers_wanted(wide).await {
		assert!(counts.runs > 1, "{counts:?}");
	}
}

#[tokio::test]
async fn sorted_runs_merge_to_the_serial_order_on_the_coordinator() {
	let serial = TestDb::new_with_config(&merge_setup(), whole()).await;
	let wide = TestDb::new_with_config(&merge_setup(), stealing()).await;
	runs_merge_in_serial_order(&serial, &wide, false).await;
}

#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread")]
async fn sorted_runs_merge_to_the_serial_order() {
	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(&merge_setup(), serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_rocksdb(&merge_setup(), wide_dir.path(), stealing()).await;
	runs_merge_in_serial_order(&serial, &wide, true).await;
}

/// Bounded sorts under a limit and a start, whose scans carry a top-k pushdown; the last two tie on
/// most of their rows.
const TOP_K: &[&str] = &[
	"SELECT n, k FROM t ORDER BY k LIMIT 10 START 5",
	"SELECT n, k FROM t ORDER BY k DESC LIMIT 10 START 5",
	"SELECT n, k FROM t ORDER BY k, n DESC LIMIT 10 START 5",
	"SELECT n FROM t ORDER BY b LIMIT 10 START 5",
	"SELECT n FROM t ORDER BY b DESC LIMIT 10 START 5",
];

/// A bounded sort over a scan filtered by a comparison of a stored field: no row raises, so the
/// scan carries the sort's threshold like an unfiltered one.
const TOP_K_FILTERED: &[&str] =
	&["SELECT n FROM t WHERE n >= 1000 ORDER BY b DESC LIMIT 10 START 5"];

/// Settings under which morsels hold a few dozen entries, enough to fill a bounded sort's heap.
fn few_dozen() -> surrealdb_cnf::ConfigMap {
	tiny().with_key_value("morsel_target_bytes", "4000")
}

/// Every morsel of a scan with a top-k pushdown publishes the threshold of its own sort into the
/// scan's cell, which only tightens, and the scan rejects the entries strictly worse than it, ties
/// kept: the answers are the serial run's, and entries were rejected.
async fn one_threshold_serves_every_morsel(serial: &TestDb, wide: &TestDb) {
	plans_combine(wide, TOP_K, Combine::TopK).await;
	let ctx = wide.exec_ctx().await;
	for sql in TOP_K {
		let program = lowered(&ctx, sql).await;
		assert!(
			program.pipelines.iter().any(|pipeline| pipeline.source.topk().is_some()),
			"`{sql}` carries no top-k pushdown"
		);
		assert!(reads_ranges(&program), "`{sql}` does not read its key range in morsels");
	}
	let counts = same_answers(serial, wide, TOP_K).await;
	assert!(counts.pruned > 0, "{counts:?}");
	assert!(counts.split > 1, "{counts:?}");
	for sql in TOP_K_FILTERED {
		assert_eq!(prunes_topk_scan(wide, sql).await, (true, true), "`{sql}`");
	}
	same_answers(serial, wide, TOP_K_FILTERED).await;
}

#[tokio::test]
async fn top_k_prunes_with_one_shared_threshold_on_the_coordinator() {
	let serial = TestDb::new_with_config(&merge_setup(), whole()).await;
	let wide = TestDb::new_with_config(&merge_setup(), few_dozen()).await;
	one_threshold_serves_every_morsel(&serial, &wide).await;
}

#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread")]
async fn top_k_prunes_with_one_shared_threshold() {
	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(&merge_setup(), serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_rocksdb(&merge_setup(), wide_dir.path(), few_dozen()).await;
	one_threshold_serves_every_morsel(&serial, &wide).await;
}

/// What `sql` answers over `db` when every pipeline of its program runs serially: the scan inside
/// the storage cursor runs every entry and judges none by a top-k threshold.
async fn serial_answer(db: &TestDb, sql: &str) -> Result<Vec<Value>, String> {
	let ctx = db.exec_ctx().await;
	let bound = bind(&ctx, sql).await;
	let mut program = compiled_plan(&ctx, &bound).await;
	for pipeline in &mut program.pipelines {
		pipeline.parallel = Parallel::Serial;
	}
	let operator: Arc<dyn crate::exec::ExecOperator> =
		Arc::new(super::host::VmOperator::new(program));
	outcome(&operator, &ctx).await
}

/// Whether the morsels of the scan with a top-k pushdown that `sql` lowers to reject entries by
/// the threshold, and whether that scan reads its key range in morsels.
async fn prunes_topk_scan(db: &TestDb, sql: &str) -> (bool, bool) {
	let ctx = db.exec_ctx().await;
	let program = lowered(&ctx, sql).await;
	let scans: Vec<_> =
		program.pipelines.iter().filter(|pipeline| pipeline.source.topk().is_some()).collect();
	assert_eq!(scans.len(), 1, "`{sql}` carries one top-k pushdown");
	match &scans[0].parallel {
		Parallel::Morsels(MorselPlan {
			input: MorselInput::Range,
			prunes,
			..
		}) => (*prunes, true),
		_ => (false, false),
	}
}

/// Whether a scan of the program `sql` lowers to carries a top-k pushdown.
async fn carries_topk(db: &TestDb, sql: &str) -> bool {
	let ctx = db.exec_ctx().await;
	lowered(&ctx, sql).await.pipelines.iter().any(|pipeline| pipeline.source.topk().is_some())
}

/// Top-k scans whose bodies hold a failure on a row a threshold rejects: `t:2200` holds a string
/// `<int>` does not cast, and ranks after the first rows of `ORDER BY k`.
const TOP_K_THAT_FAIL: &[&str] = &[
	"SELECT n, k FROM t WHERE n = 700 OR <int> s >= 0 ORDER BY k LIMIT 3",
	"SELECT n, k FROM t WHERE <int> s >= 0 ORDER BY k DESC LIMIT 10 START 5",
];

/// A top-k scan whose body may raise runs every entry, as the serial scan does, whatever width
/// it runs at: the failure of a row the threshold would reject is raised, and the answers of the
/// scans that cannot raise are the serial scan's own.
async fn a_top_k_scan_is_the_serial_scan(wide: &TestDb) {
	for sql in TOP_K_THAT_FAIL {
		let expected = serial_answer(wide, sql).await;
		let error = expected.clone().expect_err("the serial scan reads the row that fails");
		assert!(error.contains("Could not cast"), "`{sql}`: {error}");
		assert!(!carries_topk(wide, sql).await, "`{sql}` is judged by a threshold");
		assert_equivalent(sql, &answer(wide, sql).await.0, &expected);
	}
	for sql in TOP_K.iter().chain(TOP_K_FILTERED) {
		assert_equivalent(sql, &answer(wide, sql).await.0, &serial_answer(wide, sql).await);
	}
	// A body that holds nothing that can raise is judged by the threshold.
	assert_eq!(
		prunes_topk_scan(wide, "SELECT n, k FROM t ORDER BY k LIMIT 10 START 5").await,
		(true, true)
	);
	assert!(
		!carries_topk(wide, "SELECT n, k FROM t WHERE n % 3 = 0 ORDER BY k LIMIT 10").await,
		"arithmetic may raise"
	);
}

#[tokio::test]
async fn top_k_failures_are_the_serial_scans_on_the_coordinator() {
	let coordinator = tiny().with_key_value("morsel_places", "0");
	for config in [coordinator, few_dozen(), stealing()] {
		let wide = TestDb::new_with_config(&failing_merge_setup(), config).await;
		a_top_k_scan_is_the_serial_scan(&wide).await;
	}
}

#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread")]
async fn top_k_failures_are_the_serial_scans() {
	let _turn = TURNS.lock().await;
	for config in [few_dozen(), stealing()] {
		let dir = tempfile::tempdir().expect("temporary directory");
		let wide = TestDb::new_on_rocksdb(&failing_merge_setup(), dir.path(), config).await;
		a_top_k_scan_is_the_serial_scan(&wide).await;
	}
}

/// Top-k scans whose bodies hold arithmetic that overflows, or arithmetic over a value of the wrong
/// type, on a row a threshold rejects: `t:2200` holds the largest integer and a field that is no
/// number.
const TOP_K_THAT_RAISE: &[(&str, &str)] = &[
	("SELECT n, k FROM t WHERE (big ?? 0) + 1 >= 0 ORDER BY k LIMIT 3", "cannot perform addition"),
	("SELECT n, k FROM t WHERE v * 2 >= 0 ORDER BY k LIMIT 3", "cannot perform multiplication"),
];

/// A top-k scan whose body may raise an arithmetic overflow or a type error is not judged by the
/// threshold: it runs every entry, as the serial scan does, and raises the serial scan's error.
async fn a_raising_top_k_scan_is_the_serial_scan(wide: &TestDb) {
	for (sql, what) in TOP_K_THAT_RAISE {
		let expected = serial_answer(wide, sql).await;
		let error = expected.clone().expect_err("the serial scan reads the row that raises");
		assert!(error.to_lowercase().contains(what), "`{sql}`: {error}");
		assert!(!carries_topk(wide, sql).await, "`{sql}` is judged by a threshold");
		assert_equivalent(sql, &answer(wide, sql).await.0, &expected);
	}
}

fn raising_merge_setup() -> String {
	format!(
		"{} UPDATE t SET v = n; UPDATE t:2200 SET v = 'oops', big = 9223372036854775807;",
		merge_setup()
	)
}

#[tokio::test]
async fn top_k_overflows_and_type_errors_are_the_serial_scans_on_the_coordinator() {
	for config in [whole(), few_dozen(), stealing()] {
		let wide = TestDb::new_with_config(&raising_merge_setup(), config).await;
		a_raising_top_k_scan_is_the_serial_scan(&wide).await;
	}
}

#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread")]
async fn top_k_overflows_and_type_errors_are_the_serial_scans() {
	let _turn = TURNS.lock().await;
	for config in [few_dozen(), stealing()] {
		let dir = tempfile::tempdir().expect("temporary directory");
		let wide = TestDb::new_on_rocksdb(&raising_merge_setup(), dir.path(), config).await;
		a_raising_top_k_scan_is_the_serial_scan(&wide).await;
	}
}

/// Settings for a small memory budget: parts of a KiB, a merge of four runs at once, a floor and a
/// chunk of a few KiB, and morsels of a few hundred entries, which outgrow the budget each.
fn spilling() -> surrealdb_cnf::ConfigMap {
	surrealdb_cnf::ConfigMap::empty()
		.with_key_value("spill_part_size", "1024")
		.with_key_value("spill_merge_fan_in", "4")
		.with_key_value("query_memory_minimum", "4096")
		.with_key_value("memory_reserve_chunk", "4096")
		.with_key_value("morsel_split_rows", "1")
		.with_key_value("morsel_needs_view", "false")
		.with_key_value("morsel_target_bytes", "30000")
		.with_key_value("morsel_oltp_reserve", "0")
}

/// What each of `statements` answers with memory to spare, serially.
async fn answered_serially(statements: &[&str]) -> Vec<Result<Vec<Value>, String>> {
	let db = TestDb::new_with_config(&merge_setup(), whole()).await;
	let mut answers = Vec::new();
	for sql in statements {
		answers.push(answer(&db, sql).await.0);
	}
	answers
}

/// Run `statements` over `db`, bounded to spill to `store`, and assert each answers as `expected`,
/// that morsels handed the merge partials of the breaker `kind` they spilled, and that nothing a
/// query spilled outlives it.
async fn spilled_morsels_answer_as_unbounded(
	db: &TestDb,
	store: &std::sync::Arc<crate::exec::memory::MemTempStore>,
	statements: &[&str],
	expected: &[Result<Vec<Value>, String>],
	kind: crate::exec::memory::runs::BreakerKind,
) {
	use crate::exec::memory::TempStore as _;
	let written = store.runs_written();
	let partials = super::morsel::batch_partials(store.session(), kind);
	for (sql, expected) in statements.iter().zip(expected) {
		assert_equivalent(sql, &answered(db, sql).await, expected);
	}
	assert!(store.runs_written() > written, "nothing spilled");
	assert!(
		super::morsel::batch_partials(store.session(), kind) > partials,
		"no morsel handed the merge a partial"
	);
	assert_eq!(store.runs(), 0, "a spilled run outlived its query");
	assert!(store.most_read_at_once() <= 4, "read {} runs at once", store.most_read_at_once());
}

/// Morsels whose sorts outgrow a small budget spill runs of their own, and the merge of those with
/// the runs of morsels that did not answers what the in-memory merge answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spilled_morsel_runs_merge_as_in_memory_for_batches() {
	use crate::exec::memory::runs::BreakerKind;
	use crate::exec::memory::{FairGovernor, MemTempStore};
	// A sort key that calls a function runs over decoded rows, so each morsel is a batch of rows
	// the coordinator read.
	let statements = [
		"SELECT n, math::abs(k) AS kk FROM t ORDER BY kk",
		"SELECT n, math::abs(b) AS bb, m AS mm FROM t ORDER BY bb DESC, mm",
	];
	let expected = answered_serially(&statements).await;
	let store = std::sync::Arc::new(MemTempStore::new(1024, None));
	let governor = std::sync::Arc::new(FairGovernor::new(48 << 10));
	let db = bounded_on(governor, std::sync::Arc::clone(&store), &merge_setup(), spilling()).await;
	plans_combine(&db, &statements, Combine::StableMerge).await;
	spilled_morsels_answer_as_unbounded(&db, &store, &statements, &expected, BreakerKind::Sort)
		.await;
}

#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spilled_morsel_runs_merge_as_in_memory() {
	use crate::exec::memory::runs::BreakerKind;
	use crate::exec::memory::{FairGovernor, MemTempStore};
	let _turn = TURNS.lock().await;
	let expected = answered_serially(SORT_RUNS).await;
	let dir = tempfile::tempdir().expect("temporary directory");
	let store = std::sync::Arc::new(MemTempStore::new(1024, None));
	let governor = std::sync::Arc::new(FairGovernor::new(48 << 10));
	let db = bounded_on_rocksdb(
		governor,
		std::sync::Arc::clone(&store),
		&merge_setup(),
		dir.path(),
		spilling(),
	)
	.await;
	plans_combine(&db, SORT_RUNS, Combine::StableMerge).await;
	spilled_morsels_answer_as_unbounded(&db, &store, SORT_RUNS, &expected, BreakerKind::Sort).await;
}

/// A governor that grants no growth: every charge past a lease's minimum finds it short.
#[cfg(feature = "kv-rocksdb")]
struct Refusing;

#[cfg(feature = "kv-rocksdb")]
impl MemoryGovernor for Refusing {
	fn opened(&self, _lease: &Arc<Lease>) {}

	fn grow(&self, _lease: &Lease, _bytes: usize) -> bool {
		false
	}

	fn closed(&self, _lease: &Lease) {}
}

/// A sort over a datastore with no temporary store to spill to, whose governor grants no growth,
/// reads a range morsel's entries in large chunks: no breaker of a morsel is bound to the query's
/// memory where the query cannot spill, so no charge marks a spill due, and the chunk of
/// `run_range` ends at a due spill only where the query can write a run. A chunk that ended at
/// every refused charge would read tens of entries at most. The statement answers what it
/// answers serially.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_range_morsel_of_a_query_that_cannot_spill_reads_many_entries_in_a_chunk() {
	let _turn = TURNS.lock().await;
	let sql = "SELECT n, k FROM t ORDER BY k";
	let expected = answered_serially(&[sql]).await;
	let dir = tempfile::tempdir().expect("temporary directory");
	let freely = Arc::new(SetUpFreely {
		setting_up: AtomicBool::new(true),
		inner: Arc::new(Refusing),
	});
	let builder = crate::kvs::Datastore::builder()
		.with_memory_governor(Arc::clone(&freely) as Arc<dyn MemoryGovernor>)
		.without_temp_store();
	let db = TestDb::new_on_rocksdb_over(builder, &merge_setup(), dir.path(), spilling()).await;
	freely.setting_up.store(false, Ordering::Relaxed);
	let before = counts_of(&db.exec_ctx().await);
	assert_equivalent(sql, &answered(&db, sql).await, &expected[0]);
	let after = counts_of(&db.exec_ctx().await);
	let (scanned, chunks) = (after.scanned - before.scanned, after.chunks - before.chunks);
	assert!(chunks > 0, "no range morsel ran");
	assert!(chunks * 100 < scanned, "{scanned} entries were read in {chunks} chunks");
}

/// Aggregates grouped by keys of few values and of many, with exact states, appending states and
/// first values.
const GROUP_PARTITIONS: &[&str] = &[
	"SELECT g, count() AS c, math::max(n) AS hi, array::group(n) AS ns FROM t GROUP BY g",
	"SELECT h, count() AS c, math::max(n) AS hi, array::group(k) AS ks FROM t GROUP BY h",
	"SELECT g, k, count() AS c, math::min(n) AS lo FROM t GROUP BY g, k",
	"SELECT h, s, m, count() AS c FROM t GROUP BY h",
];

/// The same shapes over bodies that call a function on their rows, which run over decoded rows.
const GROUP_BATCHES: &[&str] = &[
	"SELECT g, count() AS c, math::max(n) AS hi, array::group(n) AS ns FROM t WHERE math::abs(n) >= 0 GROUP BY g",
	"SELECT h, count() AS c, math::max(n) AS hi, array::group(k) AS ks FROM t WHERE math::abs(n) >= 0 GROUP BY h",
	"SELECT h, s, m, count() AS c FROM t WHERE math::abs(n) >= 0 GROUP BY h",
];

/// Settings under which morsels hold a few hundred entries, so that the groups of those that wait
/// repay a merge by partitions.
fn few_hundred() -> surrealdb_cnf::ConfigMap {
	tiny().with_key_value("morsel_target_bytes", "20000")
}

/// The groups of morsels merge by partitions of the key space, and answer, in group-key order,
/// what the serial fold answers; a statement that can take places merges in more than one.
async fn groups_merge_by_partitions(
	serial: &TestDb,
	wide: &TestDb,
	statements: &[&str],
	ranges: bool,
	widely: bool,
) {
	plans_combine(wide, statements, Combine::Aggregate).await;
	let mut counts = Counts::default();
	for _ in 0..5 {
		counts = same_answers_in(serial, wide, statements, ranges).await;
		if !widely || counts.partitioned > 0 {
			break;
		}
	}
	if widely {
		assert!(counts.partitioned > 0, "{counts:?}");
		assert!(counts.partitions > counts.partitioned, "{counts:?}");
	} else {
		assert_eq!(counts.partitioned, 0, "{counts:?}");
	}
}

/// With no place to take, the coordinator folds the groups itself, in order.
#[tokio::test]
async fn partitioned_groups_answer_in_group_key_order_on_the_coordinator() {
	let serial = TestDb::new_with_config(&merge_setup(), whole()).await;
	let wide =
		TestDb::new_with_config(&merge_setup(), stealing().with_key_value("morsel_places", "0"))
			.await;
	groups_merge_by_partitions(&serial, &wide, GROUP_PARTITIONS, false, false).await;
	groups_merge_by_partitions(&serial, &wide, GROUP_BATCHES, false, false).await;
}

/// Batches handed to workers hold groups that wait for one another, and are merged by partitions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partitioned_groups_answer_in_group_key_order_over_batches() {
	let _turn = TURNS.lock().await;
	let serial = TestDb::new_with_config(&merge_setup(), whole()).await;
	let wide = TestDb::new_with_config(&merge_setup(), few_hundred()).await;
	groups_merge_by_partitions(&serial, &wide, GROUP_BATCHES, false, true).await;
}

#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partitioned_groups_answer_in_group_key_order() {
	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(&merge_setup(), serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_rocksdb(&merge_setup(), wide_dir.path(), few_hundred()).await;
	groups_merge_by_partitions(&serial, &wide, GROUP_PARTITIONS, false, true).await;
}

/// A float fold depends on how its values are grouped, so a keyed aggregate of one folds serially.
#[tokio::test]
async fn float_group_by_stays_serial() {
	let db = TestDb::new_with_config(&merge_setup(), viewless()).await;
	for sql in [
		"SELECT g, math::sum(f) AS total FROM t GROUP BY g",
		"SELECT g, count() AS c, math::mean(f) AS mean FROM t GROUP BY g",
	] {
		assert!(combines_of(&db, sql).await.is_empty(), "`{sql}` plans morsels");
	}
	let exact = combines_of(&db, "SELECT g, math::max(n) AS hi FROM t GROUP BY g").await;
	assert_eq!(exact, [Combine::Aggregate]);
}

/// Groups that spilled from morsels, whether or not the others did, merge by key as the
/// in-memory merge does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spilled_partials_merge_by_key_over_batches() {
	use crate::exec::memory::runs::BreakerKind;
	use crate::exec::memory::{FairGovernor, MemTempStore};
	let statements = [
		"SELECT h, count() AS c, array::group(k) AS ks, math::max(n) AS hi FROM t WHERE math::abs(n) >= 0 GROUP BY h",
		"SELECT h, s, m, count() AS c FROM t WHERE math::abs(n) >= 0 GROUP BY h",
	];
	let expected = answered_serially(&statements).await;
	let store = std::sync::Arc::new(MemTempStore::new(1024, None));
	let governor = std::sync::Arc::new(FairGovernor::new(48 << 10));
	let db = bounded_on(governor, std::sync::Arc::clone(&store), &merge_setup(), spilling()).await;
	plans_combine(&db, &statements, Combine::Aggregate).await;
	spilled_morsels_answer_as_unbounded(&db, &store, &statements, &expected, BreakerKind::Groups)
		.await;
}

#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spilled_partials_merge_by_key() {
	use crate::exec::memory::runs::BreakerKind;
	use crate::exec::memory::{FairGovernor, MemTempStore};
	let _turn = TURNS.lock().await;
	let expected = answered_serially(GROUP_PARTITIONS).await;
	let dir = tempfile::tempdir().expect("temporary directory");
	let store = std::sync::Arc::new(MemTempStore::new(1024, None));
	let governor = std::sync::Arc::new(FairGovernor::new(48 << 10));
	let db = bounded_on_rocksdb(
		governor,
		std::sync::Arc::clone(&store),
		&merge_setup(),
		dir.path(),
		spilling(),
	)
	.await;
	plans_combine(&db, GROUP_PARTITIONS, Combine::Aggregate).await;
	spilled_morsels_answer_as_unbounded(
		&db,
		&store,
		GROUP_PARTITIONS,
		&expected,
		BreakerKind::Groups,
	)
	.await;
}

/// Run `statements` over `db`, bounded to spill to `store`, with the first morsels its workers
/// claim delayed so that those after them finish first, and assert each answers as `expected`, that
/// a morsel was held for the morsels before it while morsels spilled partials of the breaker
/// `kind`, and that nothing a query spilled outlives it.
#[cfg(feature = "kv-rocksdb")]
async fn spilled_partials_wait_for_the_morsels_before_them(
	db: &TestDb,
	store: &std::sync::Arc<crate::exec::memory::MemTempStore>,
	statements: &[&str],
	expected: &[Result<Vec<Value>, String>],
	kind: crate::exec::memory::runs::BreakerKind,
) {
	use crate::exec::memory::TempStore as _;
	let ctx = db.exec_ctx().await;
	let written = store.runs_written();
	let partials = super::morsel::batch_partials(store.session(), kind);
	let before = counts_of(&ctx);
	for (sql, expected) in statements.iter().zip(expected) {
		for attempt in 0..5 {
			let held = counts_of(&ctx).collected_early;
			delay_morsels(&ctx, &REVERSED);
			assert_equivalent(sql, &answered(db, sql).await, expected);
			if counts_of(&ctx).collected_early > held || attempt == 4 {
				break;
			}
		}
		delay_morsels(&ctx, &[]);
	}
	let after = counts_of(&ctx);
	assert!(after.handed > before.handed, "no morsel went to a worker");
	assert!(after.collected_early > before.collected_early, "no finished morsel was held");
	assert_eq!(
		after.merged_early, before.merged_early,
		"a morsel was merged ahead of an earlier one"
	);
	assert!(store.runs_written() > written, "nothing spilled");
	assert!(
		super::morsel::batch_partials(store.session(), kind) > partials,
		"no morsel handed the merge a partial"
	);
	assert_eq!(store.runs(), 0, "a spilled run outlived its query");
}

/// The runs a sort's morsels spill and the groups an aggregate's morsels spill, which finish ahead
/// of the morsels before them, are absorbed when the morsels before them have been: the rows and
/// the groups are the in-memory merge's, ties and appends included.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spilled_partials_held_for_earlier_morsels_merge_as_in_memory() {
	use crate::exec::memory::runs::BreakerKind;
	use crate::exec::memory::{FairGovernor, MemTempStore};
	let _turn = TURNS.lock().await;
	for (statements, kind, merge) in [
		(SORT_RUNS, BreakerKind::Sort, MergeOrder::Positioned),
		(GROUP_PARTITIONS, BreakerKind::Groups, MergeOrder::Adjacent),
	] {
		let expected = answered_serially(statements).await;
		let dir = tempfile::tempdir().expect("temporary directory");
		let store = std::sync::Arc::new(MemTempStore::new(1024, None));
		let governor = std::sync::Arc::new(FairGovernor::new(48 << 10));
		let db = bounded_on_rocksdb(
			governor,
			std::sync::Arc::clone(&store),
			&merge_setup(),
			dir.path(),
			spilling(),
		)
		.await;
		for sql in statements {
			assert_eq!(merges_of(&db, sql).await, [merge], "`{sql}`");
		}
		spilled_partials_wait_for_the_morsels_before_them(&db, &store, statements, &expected, kind)
			.await;
	}
}

/// A statement whose rows fail, where a sort that keeps every row or an aggregate that combines
/// associatively merges its morsels as they finish: the failing morsels finish ahead of the
/// morsels before them and are held, and the statement fails with the serial run's failure, the
/// earliest, which it raises once the morsels before the failure have merged. Key ranges' morsels
/// are held when they finish early (`holds`); batches merge whole, in order.
async fn a_failing_morsel_held_for_the_morsels_before_it_fails_the_statement(
	serial: &TestDb,
	wide: &TestDb,
	statements: &[(&str, MergeOrder)],
	holds: bool,
) {
	let handed = counts_of(&wide.exec_ctx().await).handed;
	let held = counts_of(&wide.exec_ctx().await).collected_early;
	for (sql, merge) in statements {
		assert_eq!(merges_of(wide, sql).await, [*merge], "`{sql}`");
		let (expected, _) = answer(serial, sql).await;
		assert!(expected.is_err(), "`{sql}` fails serially");
		for attempt in 0..5 {
			let before = counts_of(&wide.exec_ctx().await).collected_early;
			for delays in [REVERSED, [300, 0, 0, 0], [0, 200, 0, 100]] {
				delay_morsels(&wide.exec_ctx().await, &delays);
				assert_equivalent(sql, &answered(wide, sql).await, &expected);
			}
			if counts_of(&wide.exec_ctx().await).collected_early > before || attempt == 4 {
				break;
			}
		}
		delay_morsels(&wide.exec_ctx().await, &[]);
	}
	let after = counts_of(&wide.exec_ctx().await);
	assert!(after.handed > handed, "no morsel went to a worker");
	assert!(!holds || after.collected_early > held, "no finished morsel was held");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_batch_held_for_the_batches_before_it_fails_the_statement() {
	let _turn = TURNS.lock().await;
	let serial = TestDb::new_with_config(&failing_setup(), whole()).await;
	let wide = TestDb::new_with_config(&failing_setup(), tiny()).await;
	a_failing_morsel_held_for_the_morsels_before_it_fails_the_statement(
		&serial,
		&wide,
		&[
			(
				"SELECT n, <int> s AS i FROM t WHERE math::abs(n) >= 0 ORDER BY n",
				MergeOrder::Positioned,
			),
			(
				"SELECT count() AS c, array::group(<int> s) AS ns FROM t WHERE math::abs(n) >= 0 GROUP ALL",
				MergeOrder::Adjacent,
			),
		],
		false,
	)
	.await;
}

#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_morsel_held_for_the_morsels_before_it_fails_the_statement_over_ranges() {
	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(&failing_setup(), serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_rocksdb(&failing_setup(), wide_dir.path(), tiny()).await;
	a_failing_morsel_held_for_the_morsels_before_it_fails_the_statement(
		&serial,
		&wide,
		&[
			("SELECT n, <int> s AS i FROM t ORDER BY n", MergeOrder::Positioned),
			(
				"SELECT count() AS c, array::group(n) AS ns FROM t WHERE <int> s >= 0 GROUP ALL",
				MergeOrder::Adjacent,
			),
			(
				"SELECT count() AS c, array::group(<int> s) AS ns FROM t GROUP ALL",
				MergeOrder::Adjacent,
			),
		],
		true,
	)
	.await;
}

/// `sql` as the plan of a statement whose rows pass through a distinct set before they are
/// projected: a node of the plan that no SurrealQL statement writes, and the SQL front ends do.
async fn distinct_plan(ctx: &ExecutionContext, sql: &str) -> LogicalOp {
	let mut bound = bind(ctx, sql).await;
	let rows = match &mut bound {
		LogicalOp::Project(project) => &mut *project.input,
		other => other,
	};
	let input = std::mem::replace(rows, LogicalOp::Break(LogicalBreak));
	*rows = LogicalOp::Distinct(LogicalDistinct {
		input: Box::new(input),
	});
	bound
}

/// What the distinct plan of `sql` answers over `db`, and whether its pipelines run as morsels
/// that keep the first occurrence of each row.
async fn distinct_answer(db: &TestDb, sql: &str) -> (Result<Vec<Value>, String>, bool) {
	let ctx = db.exec_ctx().await;
	let bound = distinct_plan(&ctx, sql).await;
	let program = compiled_plan(&ctx, &bound).await;
	let merges = program.pipelines.iter().any(|pipeline| {
		matches!(
			&pipeline.parallel,
			Parallel::Morsels(MorselPlan {
				combine: Combine::Distinct,
				..
			})
		)
	});
	let vm = operator_of(&ctx, &bound).await;
	(outcome(&vm, &ctx).await, merges)
}

/// Rows read twice, which the distinct set keeps once, and rows read once.
const DISTINCTS: &[&str] = &[
	"SELECT n, k FROM t, t",
	"SELECT n FROM t, t WHERE n % 3 = 0",
	"SELECT n FROM t",
	"SELECT n, s FROM t WHERE n % 5 = 1",
];

/// Statements that fail on one row, which a duplicate repeats.
const FAILING_DISTINCTS: &[&str] = &["SELECT <int> s AS i FROM t, t", "SELECT <int> s AS i FROM t"];

fn failing_merge_setup() -> String {
	format!("{} UPDATE t:700 SET s = 'x'; UPDATE t:2200 SET s = 'y';", merge_setup())
}

/// The rows of a body that passes them through one distinct set are kept once, at the first place
/// the serial run reads them, whichever morsel reads that place first, so the rows come in the
/// serial order; a failure is one of `errors`, the failures of the rows that may fail the
/// statement, or the serial run's.
async fn distinct_keeps_first_occurrences(
	serial: &TestDb,
	wide: &TestDb,
	statements: &[&str],
	errors: &[String],
) {
	let mut merged = 0;
	for sql in statements {
		let (expected, _) = distinct_answer(serial, sql).await;
		let (got, merges) = distinct_answer(wide, sql).await;
		match (&got, &expected) {
			(Ok(_), Ok(_)) => assert_eq!(got, expected, "`{sql}`"),
			_ => assert_equivalent_among(sql, &got, &expected, errors),
		}
		merged += usize::from(merges);
	}
	assert!(merged > 0, "no distinct set ran as morsels");
}

#[tokio::test]
async fn merge_side_distinct_keeps_first_occurrences_on_the_coordinator() {
	let serial = TestDb::new_with_config(&failing_merge_setup(), whole()).await;
	let wide = TestDb::new_with_config(&failing_merge_setup(), stealing()).await;
	distinct_keeps_first_occurrences(&serial, &wide, DISTINCTS, &[]).await;
	distinct_keeps_first_occurrences(&serial, &wide, FAILING_DISTINCTS, &cast_failures()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_side_distinct_keeps_first_occurrences_over_batches() {
	let _turn = TURNS.lock().await;
	let serial = TestDb::new_with_config(&failing_merge_setup(), whole()).await;
	let wide = TestDb::new_with_config(&failing_merge_setup(), tiny()).await;
	distinct_keeps_first_occurrences(&serial, &wide, DISTINCTS, &[]).await;
	distinct_keeps_first_occurrences(&serial, &wide, FAILING_DISTINCTS, &cast_failures()).await;
}

#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_side_distinct_keeps_first_occurrences() {
	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(&failing_merge_setup(), serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_rocksdb(&failing_merge_setup(), wide_dir.path(), stealing()).await;
	distinct_keeps_first_occurrences(&serial, &wide, DISTINCTS, &[]).await;
	distinct_keeps_first_occurrences(&serial, &wide, FAILING_DISTINCTS, &cast_failures()).await;
}

/// A set that outgrows its budget fails the row that outgrows it, as the serial run's does, though
/// every morsel's own set holds far fewer rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_distinct_set_past_its_budget_fails_at_the_serial_row() {
	let _turn = TURNS.lock().await;
	let budget =
		|config: surrealdb_cnf::ConfigMap| config.with_key_value("gql_max_join_build_rows", "1000");
	let serial = TestDb::new_with_config(&merge_setup(), budget(whole())).await;
	let wide = TestDb::new_with_config(&merge_setup(), budget(tiny())).await;
	for sql in ["SELECT n FROM t, t", "SELECT n FROM t WHERE n % 2 = 0"] {
		let (expected, _) = distinct_answer(&serial, sql).await;
		let error = expected.clone().expect_err("the serial set outgrows its budget");
		assert!(error.contains("1000 distinct rows"), "{error}");
		assert_eq!(distinct_answer(&wide, sql).await.0, expected, "`{sql}`");
	}
}

// ---------------------------------------------------------------------------
// The corpora that pin morsels to the serial run's answers
// ---------------------------------------------------------------------------

/// The statements the corpus runs: a table read through its layer, statements nested per row, an
/// index, a record range and a union of indexes, and sorts and aggregates over keys most rows tie
/// on.
const CORPUS: &[&str] = &[
	"SELECT n, twice FROM t",
	// A statement nested per row that only reads runs over each
	// batch's state; one kept from its first evaluation runs serially.
	"SELECT n, (SELECT VALUE twice FROM ONLY $parent.id) AS again FROM t WHERE n % 7 = 0",
	"SELECT n, (SELECT VALUE twice FROM ONLY $parent.id) AS again FROM t LIMIT 40 START 3",
	"SELECT n, (SELECT VALUE count() FROM t GROUP ALL) AS all FROM t",
	"SELECT VALUE $this.n + twice FROM t",
	"SELECT n, <int> s AS i FROM t",
	"SELECT n, $this.s.len() AS l, <int> $this.s AS i FROM t",
	// An index, a record range and a union of indexes, in their order.
	"SELECT n, $this.s AS s FROM t WHERE n > 1200",
	"SELECT n, $this.s AS s FROM t WHERE n > 1200 ORDER BY n DESC",
	"SELECT n, $this.s AS s FROM t:100..2900",
	"SELECT n, $this.s AS s FROM t WHERE n < 50 OR m = 3",
	"SELECT n, <int> $this.s AS i FROM t WHERE n > 2000",
	// Sorts, whole and under a limit, over keys most rows tie on.
	"SELECT n, $this.m AS k FROM t ORDER BY k",
	"SELECT n, $this.m AS k FROM t ORDER BY k DESC LIMIT 7",
	"SELECT n, $this.m AS k FROM t ORDER BY k, twice DESC LIMIT 25 START 3",
	// Aggregates whose partial states combine exactly; reading `twice`
	// decodes the rows, through its table's layer.
	"SELECT m, count() AS c, array::group(twice % 3) AS g, math::max(n) AS mx, \
				 array::join(s, ',') AS all, math::median(twice) AS md FROM t GROUP BY m",
	"SELECT count() AS c, array::distinct(m) AS d, math::min(twice) AS mn FROM t GROUP ALL",
	"SELECT m, s AS first, math::max(twice) AS top FROM t GROUP BY m",
	// A float sum depends on how it is grouped, so it folds serially.
	"SELECT m, math::sum(twice) AS total FROM t GROUP BY m",
];

/// The table the corpus reads.
const CORPUS_SETUP: &str = "
	DEFINE TABLE t;
	DEFINE FIELD n ON t TYPE int;
	DEFINE FIELD twice ON t COMPUTED n * 2;
	DEFINE INDEX by_n ON t FIELDS n;
	DEFINE INDEX by_m ON t FIELDS m;
	FOR $i IN 0..3000 { CREATE type::record('t', $i) SET n = $i, m = $i % 10, s = <string> $i };
	UPDATE t:2500 SET s = 'x';
";

/// Settings that cut every scan into entry-sized morsels.
fn region_config() -> surrealdb_cnf::ConfigMap {
	surrealdb_cnf::ConfigMap::empty()
		.with_key_value("morsel_split_rows", "1")
		.with_key_value("morsel_needs_view", "false")
}

/// A table read as a region answers what the serial run answers — its rows, in order where the
/// statement orders them and as a multiset where it does not, and the failure of one of its
/// failing batches — whichever of its batches the workers took. The serial answers are the
/// golden's.
#[tokio::test]
async fn morsels_answer_what_the_serial_run_answers() {
	let mut golden = Golden::new("morsels_answer_what_the_serial_run_answers");
	let serial = TestDb::new_with_config(CORPUS_SETUP, whole()).await;
	let region = TestDb::new_with_config(CORPUS_SETUP, region_config()).await;
	for sql in CORPUS {
		let answer = answered(&serial, sql).await;
		golden.rows(sql, &answer);
		assert_equivalent(sql, &answered(&region, sql).await, &answer);
	}
	golden.check();
}

/// Each statement of the corpus that plans morsels and runs to its end runs them: a key range of
/// it is split, or a batch of it is handed to a worker. The answers are the serial run's.
#[tokio::test]
async fn the_corpus_runs_morsels_on_workers() {
	let serial = TestDb::new_with_config(CORPUS_SETUP, whole()).await;
	let region = TestDb::new_with_config(CORPUS_SETUP, region_config()).await;
	let mut fanned_out = 0;
	for sql in CORPUS {
		let answer = answered(&serial, sql).await;
		fanned_out += usize::from(answered_fanning_out(&region, sql, &answer).await != FanOut::No);
	}
	assert!(fanned_out > 0, "no statement of the corpus plans morsels");
}

/// The table the paged corpus reads.
const PAGED_SETUP: &str = "
	DEFINE TABLE t;
	DEFINE INDEX by_n ON t FIELDS n;
	FOR $i IN 0..3000 { CREATE type::record('t', $i) SET n = $i, s = <string> $i };
	UPDATE t:2500 SET s = 'x';
";

/// The paged statements, each with the rows it answers or that it fails.
fn paged_cases() -> Vec<(String, Result<usize, ()>)> {
	// Rows 0, 100, …, 2400 and then every row past 2450 pass the filter, so
	// the merged batches show a filter passing few rows and the window widens
	// past the rows the limit leaves; row 2500 fails `<int>`.
	let sparse = "(n % 100 = 0 OR n > 2450)";
	// Per-row statements, each answering `n + 1` for row `n`, that page the
	// two values they read from `$parent` with a `LIMIT` and with a `START`.
	let nested_top = "(SELECT VALUE v FROM [{ v: $parent.n }, { v: $parent.n + 1 }] \
		ORDER BY v DESC LIMIT 1)[0]";
	let nested_skipped = "(SELECT VALUE v FROM [{ v: $parent.n }, { v: $parent.n + 1 }] \
		ORDER BY v LIMIT 1 START 1)[0]";
	vec![
		("SELECT n, $this.s AS s FROM t LIMIT 1000".to_owned(), Ok(1000)),
		("SELECT n, $this.s AS s FROM t LIMIT 1".to_owned(), Ok(1)),
		("SELECT n, $this.s AS s FROM t LIMIT 10 START 5".to_owned(), Ok(10)),
		("SELECT n, $this.s AS s FROM t START 2990".to_owned(), Ok(10)),
		("SELECT n, $this.s AS s FROM t LIMIT 0".to_owned(), Ok(0)),
		("SELECT n, $this.s AS s FROM t WHERE n % 97 = 0 LIMIT 20".to_owned(), Ok(20)),
		("SELECT n, $this.s AS s FROM t WHERE n > 1200 LIMIT 50 START 3".to_owned(), Ok(50)),
		// A projection that fails: before the limit, at a row taken, and at a
		// row skipped or past the limit, which the serial run never projects.
		("SELECT n, <int> $this.s AS i FROM t LIMIT 2400".to_owned(), Ok(2400)),
		("SELECT n, <int> $this.s AS i FROM t LIMIT 2600".to_owned(), Err(())),
		("SELECT n, <int> $this.s AS i FROM t LIMIT 10 START 2495".to_owned(), Err(())),
		("SELECT n, <int> $this.s AS i FROM t LIMIT 5 START 2501".to_owned(), Ok(5)),
		("SELECT n, <int> $this.s AS i FROM t LIMIT 0".to_owned(), Ok(0)),
		(format!("SELECT n, <int> $this.s AS i FROM t WHERE {sparse} LIMIT 30"), Ok(30)),
		(format!("SELECT n, <int> $this.s AS i FROM t WHERE {sparse} LIMIT 80"), Err(())),
		// `START` past rows a batch runs ahead: skipped up to row 2500, and
		// taking it first or last.
		(format!("SELECT n, <int> $this.s AS i FROM t WHERE {sparse} LIMIT 5 START 75"), Ok(5)),
		(format!("SELECT n, <int> $this.s AS i FROM t WHERE {sparse} LIMIT 5 START 74"), Err(())),
		(format!("SELECT n, <int> $this.s AS i FROM t WHERE {sparse} LIMIT 5 START 70"), Err(())),
		(format!("SELECT n, $this.s AS s FROM t WHERE {sparse} LIMIT 400 START 26"), Ok(400)),
		// A filter that fails: the serial run judges every row it reads,
		// skipped or taken, and none past the limit.
		(
			format!("SELECT n, $this.s AS s FROM t WHERE {sparse} AND <int> $this.s >= 0 LIMIT 30"),
			Ok(30),
		),
		(
			format!("SELECT n, $this.s AS s FROM t WHERE {sparse} AND <int> $this.s >= 0 LIMIT 80"),
			Err(()),
		),
		(
			"SELECT n, $this.s AS s FROM t WHERE <int> $this.s >= 0 LIMIT 5 START 2600".to_owned(),
			Err(()),
		),
		("SELECT n, $this.s AS s FROM t WHERE <int> $this.s >= 0 LIMIT 0".to_owned(), Ok(0)),
		// A statement nested per row with a paging clause of its own: a row
		// reaches the body's paging only by passing the filter, whatever the
		// nested statement's own `LIMIT` and `START` take of the rows beneath
		// it. Row `n` passes where `n + 1` is 1 modulo 97.
		(format!("SELECT n FROM t WHERE {nested_top} % 97 = 1 LIMIT 5"), Ok(5)),
		(format!("SELECT n FROM t WHERE {nested_top} % 97 = 1 LIMIT 3 START 1"), Ok(3)),
		(format!("SELECT n FROM t WHERE {nested_skipped} % 97 = 1 LIMIT 5"), Ok(5)),
		(
			format!(
				"SELECT n FROM t WHERE {nested_top} % 97 = 1 AND {nested_skipped} >= 0 LIMIT 4 START 2"
			),
			Ok(4),
		),
	]
}

/// A paged body read as a region answers what the VM's serial run answers:
/// the rows `START` and `LIMIT` leave, the failure of a row the serial run
/// reaches, and no failure of a row past the limit, even one a batch ran
/// ahead of the merge.
#[tokio::test(flavor = "multi_thread")]
async fn paged_morsels_answer_what_the_serial_run_answers() {
	let serial = TestDb::new_with_config(
		PAGED_SETUP,
		surrealdb_cnf::ConfigMap::empty()
			.with_key_value("morsel_split_rows", usize::MAX.to_string()),
	)
	.await;
	let region = TestDb::new_with_config(PAGED_SETUP, region_config()).await;
	for (sql, expected) in &paged_cases() {
		let answer = answered(&serial, sql).await;
		assert_eq!(
			answer.as_ref().map(Vec::len).map_err(|_| ()),
			*expected,
			"`{sql}` run serially: {answer:?}"
		);
		assert_equivalent(sql, &answered(&region, sql).await, &answer);
	}
}

/// The paged statements plan their morsels, and some batch of them runs on a worker.
#[tokio::test(flavor = "multi_thread")]
async fn paged_statements_run_morsels_on_workers() {
	let region = TestDb::new_with_config(PAGED_SETUP, region_config()).await;
	let handed = counts_of(&region.exec_ctx().await).handed;
	let ctx = region.exec_ctx().await;
	for (sql, _) in &paged_cases() {
		// A filter that reads `$this` or `$parent` is a body the logical plan cannot see, which it
		// keeps serial.
		let opaque = sql.contains("$parent") || sql.contains("<int> $this.s >= 0 LIMIT");
		assert_eq!(fans_out(&*lowered(&ctx, sql).await), !opaque, "`{sql}`");
		answered(&region, sql).await.ok();
	}
	assert!(counts_of(&region.exec_ctx().await).handed > handed, "no paged batch ran on a worker");
}

// ---------------------------------------------------------------------------
// The order the merge takes morsels in
// ---------------------------------------------------------------------------

/// The merge order of each pipeline of `sql` over `db` that runs as morsels.
async fn merges_of(db: &TestDb, sql: &str) -> Vec<MergeOrder> {
	lowered(&db.exec_ctx().await, sql)
		.await
		.pipelines
		.iter()
		.filter_map(|pipeline| match &pipeline.parallel {
			Parallel::Morsels(plan) => Some(plan.merge),
			Parallel::Serial => None,
		})
		.collect()
}

/// The merge order of each pipeline of `sql` over `db` that runs as morsels, planned as the value a
/// top-level `LET` binds.
async fn binding_merges_of(db: &TestDb, sql: &str) -> Vec<MergeOrder> {
	let ctx = db.exec_ctx().await;
	let bound = bind(&ctx, sql).await;
	let program =
		Box::pin(compile(&planner(&ctx).binding_rows(), &bound)).await.expect("compiles").program;
	program
		.pipelines
		.iter()
		.filter_map(|pipeline| match &pipeline.parallel {
			Parallel::Morsels(plan) => Some(plan.merge),
			Parallel::Serial => None,
		})
		.collect()
}

/// The rows a `LET` binds are read by the statements after it in the order they were bound, so
/// the plan of the value merges them in the order of the serial run: the plan says so, and the
/// merge does what the plan says. The counts of one group are a total whoever reads them, and
/// stay free.
#[tokio::test]
async fn the_plan_of_a_value_a_let_binds_merges_its_rows_in_serial_order() {
	let db = TestDb::new_with_config(&merge_setup(), tiny()).await;
	for sql in
		["SELECT n FROM t", "SELECT n, s FROM t WHERE n % 3 = 0", "SELECT * FROM t:100..2900"]
	{
		assert_eq!(merges_of(&db, sql).await, [MergeOrder::Free], "`{sql}` as a result");
		assert_eq!(binding_merges_of(&db, sql).await, [MergeOrder::Serial], "`{sql}` bound");
	}
	let counts = "SELECT count() AS c FROM t WHERE n % 3 = 0 GROUP ALL";
	assert_eq!(binding_merges_of(&db, counts).await, [MergeOrder::Free]);
}

/// A statement's rows are free to arrive as the morsels finish only where nothing reads them in
/// order: the rows of a statement's result that no `ORDER BY` sorts, no paging counts and no
/// distinct set keeps the first of, and the counts of one group. The planned merge order says so,
/// and the plan never takes results as they finish for the rest.
#[tokio::test]
async fn only_the_unordered_output_of_a_statement_merges_as_morsels_finish() {
	let db = TestDb::new_with_config(&merge_setup(), tiny()).await;
	let free = [
		"SELECT n FROM t",
		"SELECT n, s FROM t WHERE n % 3 = 0",
		"SELECT n FROM t WHERE math::abs(n) >= 0",
		"SELECT * FROM t:100..2900",
		// The counts of one group add up in any order.
		"SELECT count() AS c FROM t WHERE n % 3 = 0 GROUP ALL",
		"SELECT count(n % 2 = 0) AS c, count() AS d FROM t WHERE n > 100 GROUP ALL",
	];
	for sql in free {
		assert_eq!(merges_of(&db, sql).await, [MergeOrder::Free], "`{sql}`");
	}
	// The input of a sort that keeps every row, and the partial states of an aggregate that
	// combine associatively, are taken as the morsels finish and merged in the order of the scan.
	for sql in ["SELECT n FROM t ORDER BY n", "SELECT n, k FROM t ORDER BY k"] {
		assert_eq!(merges_of(&db, sql).await, [MergeOrder::Positioned], "`{sql}`");
	}
	for sql in [
		"SELECT k, count() FROM t GROUP BY k",
		"SELECT count(), array::group(n) FROM t GROUP ALL",
		"SELECT count(), math::max(n) FROM t GROUP ALL",
		"SELECT g, count() FROM t WHERE n > 100 GROUP BY g",
	] {
		assert_eq!(merges_of(&db, sql).await, [MergeOrder::Adjacent], "`{sql}`");
	}
	// Whatever pages, bounds a sort, folds a float or deduplicates the rows, and whatever another
	// operator or expression reads in order, merges in the order of the serial run.
	let serial = [
		"SELECT n FROM t ORDER BY id",
		"SELECT n FROM t ORDER BY id DESC",
		"SELECT n FROM t ORDER BY n LIMIT 5",
		"SELECT n FROM t LIMIT 5",
		"SELECT n FROM t START 100",
		"SELECT count(), math::sum(f) FROM t GROUP ALL",
		"SELECT n FROM (SELECT n FROM t)",
		"SELECT n FROM (SELECT n, k FROM t) ORDER BY k",
		"SELECT n, (SELECT VALUE s FROM t WHERE n = 1) AS s FROM t",
	];
	for sql in serial {
		let merges = merges_of(&db, sql).await;
		assert!(merges.iter().all(|merge| *merge == MergeOrder::Serial), "`{sql}`: {merges:?}");
	}
	for sql in DISTINCTS {
		let ctx = db.exec_ctx().await;
		let bound = distinct_plan(&ctx, sql).await;
		let program = compiled_plan(&ctx, &bound).await;
		assert!(
			program.pipelines.iter().all(|pipeline| !matches!(
				&pipeline.parallel,
				Parallel::Morsels(MorselPlan {
					merge: MergeOrder::Free,
					..
				})
			)),
			"`{sql}` under a distinct set"
		);
	}
}

/// Delays that finish the first morsels a worker claims last: the morsels after them are done
/// first.
const REVERSED: [u64; 4] = [400, 300, 200, 100];

/// Statements whose rows no clause orders, over `t`.
#[cfg(feature = "kv-rocksdb")]
const UNORDERED: &[&str] = &[
	"SELECT n, s FROM t WHERE n % 3 = 0",
	"SELECT * FROM t",
	"SELECT n FROM t WHERE n > 100 AND math::abs(n) >= 0",
];

/// Run each of `statements` over `serial` and `wide`, with the first morsels `wide`'s workers claim
/// delayed so that they finish in reverse, and answer how many morsels the merge took before an
/// earlier one had finished, and how many went to workers. Each answer is one the serial run's
/// allows. A statement is run again, up to five times, until some morsel went to a worker: another
/// test of the process may hold the node's places.
async fn reversed_completions(
	serial: &TestDb,
	wide: &TestDb,
	statements: &[&str],
) -> (usize, usize) {
	let (mut early, mut handed) = (0, 0);
	for sql in statements {
		let (expected, _) = answer(serial, sql).await;
		for attempt in 0..5 {
			let ctx = wide.exec_ctx().await;
			let before = counts_of(&ctx);
			delay_morsels(&ctx, &REVERSED);
			let got = answered(wide, sql).await;
			assert_equivalent(sql, &got, &expected);
			let after = counts_of(&ctx);
			early += after.merged_early - before.merged_early;
			handed += after.handed - before.handed;
			if after.handed > before.handed || attempt == 4 {
				break;
			}
		}
		delay_morsels(&wide.exec_ctx().await, &[]);
	}
	(early, handed)
}

/// A key range read by workers whose first morsels finish last is merged as the morsels finish,
/// and answers what the serial run answers, in any order where no clause orders the rows.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_morsel_does_not_hold_back_the_morsels_after_it() {
	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(&setup(), serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_rocksdb(&setup(), wide_dir.path(), tiny()).await;
	for sql in UNORDERED {
		assert!(
			merges_of(&wide, sql).await.contains(&MergeOrder::Free),
			"`{sql}` does not merge as morsels finish"
		);
	}
	let (early, handed) = reversed_completions(&serial, &wide, UNORDERED).await;
	assert!(handed > 0, "no morsel went to a worker");
	assert!(early > 0, "no morsel was merged before an earlier one finished");
}

/// The same over batches the coordinator read, which workers run on any backend.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_batch_does_not_hold_back_the_batches_after_it() {
	let _turn = TURNS.lock().await;
	let serial = TestDb::new_with_config(&setup(), whole()).await;
	let wide = TestDb::new_with_config(&setup(), tiny()).await;
	let statements = [
		"SELECT n FROM t WHERE math::abs(n) >= 0",
		"SELECT n, s FROM t WHERE math::abs(n) % 3 = 0",
	];
	for sql in statements {
		assert_eq!(merges_of(&wide, sql).await, [MergeOrder::Free], "`{sql}`");
	}
	let (early, handed) = reversed_completions(&serial, &wide, &statements).await;
	assert!(handed > 0, "no batch went to a worker");
	assert!(early > 0, "no batch was merged before an earlier one finished");
}

/// Run each of `statements`, which end in the `RETURN` of a name a `LET` binds, over `serial` and
/// `wide` with the first morsels `wide`'s workers claim delayed so that they finish in reverse,
/// and assert that the rows are the serial run's in order and that no morsel was merged before an
/// earlier one had finished. Answers how many morsels went to workers.
async fn bound_rows_keep_the_order(serial: &TestDb, wide: &TestDb, statements: &[&str]) -> usize {
	let mut handed = 0;
	for sql in statements {
		let expected = answered(serial, sql).await.expect("the serial run answers");
		assert!(expected.len() > 1, "`{sql}` binds no rows");
		for attempt in 0..5 {
			let ctx = wide.exec_ctx().await;
			let before = counts_of(&ctx);
			delay_morsels(&ctx, &REVERSED);
			let got = answered(wide, sql).await.expect("the wide run answers");
			assert_eq!(got, expected, "`{sql}`");
			let after = counts_of(&ctx);
			assert_eq!(
				after.merged_early, before.merged_early,
				"`{sql}` merged ahead of an earlier morsel"
			);
			handed += after.handed - before.handed;
			if after.handed > before.handed || attempt == 4 {
				break;
			}
		}
		delay_morsels(&wide.exec_ctx().await, &[]);
	}
	handed
}

/// The rows a `LET` binds are read by the statements after it in the order they were bound, so
/// their merge takes the morsels in the order of the serial run however late the first ones are,
/// where the same statement written as a result merges as the morsels finish.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_rows_a_let_binds_keep_the_serial_order_whatever_order_morsels_finish_in() {
	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(&setup(), serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_rocksdb(&setup(), wide_dir.path(), tiny()).await;
	let statements = [
		"LET $s = SELECT n, s FROM t WHERE n % 3 = 0; RETURN $s",
		"LET $s = SELECT * FROM t; RETURN $s",
		"LET $s = { SELECT n FROM t WHERE n > 100 AND math::abs(n) >= 0 }; RETURN $s",
		// A statement after one that changes the catalog is planned when it is reached, which is
		// another path to a plan than the request's own.
		"DEFINE TABLE u SCHEMALESS; LET $s = SELECT n FROM t WHERE n % 3 = 0; RETURN $s",
		"DEFINE TABLE u SCHEMALESS; LET $s = SELECT n FROM t WHERE n % 3 = 0; LET $c = $s; RETURN $c",
	];
	assert!(
		bound_rows_keep_the_order(&serial, &wide, &statements).await > 0,
		"no morsel went to a worker"
	);
}

/// The same over batches the coordinator read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_rows_a_let_binds_keep_the_serial_order_whatever_order_batches_finish_in() {
	let _turn = TURNS.lock().await;
	let serial = TestDb::new_with_config(&setup(), whole()).await;
	let wide = TestDb::new_with_config(&setup(), tiny()).await;
	let statements = [
		"LET $s = SELECT n FROM t WHERE math::abs(n) >= 0; RETURN $s",
		"LET $s = SELECT n, s FROM t WHERE math::abs(n) % 3 = 0; RETURN $s",
	];
	assert!(
		bound_rows_keep_the_order(&serial, &wide, &statements).await > 0,
		"no batch went to a worker"
	);
}

/// Counts of one group over a key range, whose first morsels finish last.
#[cfg(feature = "kv-rocksdb")]
const COUNTS: &[&str] = &[
	"SELECT count() AS c FROM t WHERE n % 3 = 0 GROUP ALL",
	"SELECT count(n % 2 = 0) AS c, count() AS d FROM t WHERE n > 100 GROUP ALL",
];

/// The counts of the morsels of a key range add up as the morsels finish, and answer what the
/// serial run answers, however late the first morsels are.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn counts_do_not_wait_for_a_slow_morsel() {
	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(&setup(), serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_rocksdb(&setup(), wide_dir.path(), tiny()).await;
	for sql in COUNTS {
		assert_eq!(merges_of(&wide, sql).await, [MergeOrder::Free], "`{sql}`");
	}
	let (early, handed) = reversed_completions(&serial, &wide, COUNTS).await;
	assert!(handed > 0, "no morsel went to a worker");
	assert!(early > 0, "no morsel was merged before an earlier one finished");
}

/// The same over batches the coordinator read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn counts_do_not_wait_for_a_slow_batch() {
	let _turn = TURNS.lock().await;
	let serial = TestDb::new_with_config(&setup(), whole()).await;
	let wide = TestDb::new_with_config(&setup(), tiny()).await;
	let statements = [
		"SELECT count() AS c FROM t WHERE math::abs(n) % 3 = 0 GROUP ALL",
		"SELECT count(n % 2 = 0) AS c, count() AS d FROM t WHERE math::abs(n) > 100 GROUP ALL",
	];
	for sql in statements {
		assert_eq!(merges_of(&wide, sql).await, [MergeOrder::Free], "`{sql}`");
	}
	let (early, handed) = reversed_completions(&serial, &wide, &statements).await;
	assert!(handed > 0, "no batch went to a worker");
	assert!(early > 0, "no batch was merged before an earlier one finished");
}

/// Where an `ORDER BY` orders the rows, no morsel is merged ahead of an earlier one, whatever
/// order the morsels finish in: the rows are the serial run's in order, ties included.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_by_keeps_the_serial_order_whatever_order_morsels_finish_in() {
	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(&merge_setup(), serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_rocksdb(&merge_setup(), wide_dir.path(), tiny()).await;
	let statements = [
		"SELECT n, s FROM t ORDER BY id",
		"SELECT n, s FROM t ORDER BY id DESC",
		"SELECT n, k FROM t ORDER BY k",
		"SELECT n, k FROM t ORDER BY k DESC, n",
	];
	for sql in statements {
		assert_eq!(rows_of(sql), Rows::Ordered);
		assert!(
			merges_of(&wide, sql).await.iter().all(|merge| *merge != MergeOrder::Free),
			"`{sql}` merges its rows as morsels finish"
		);
	}
	let before = counts_of(&wide.exec_ctx().await);
	let (early, handed) = reversed_completions(&serial, &wide, &statements).await;
	assert!(handed > 0, "no morsel went to a worker");
	assert_eq!(early, 0);
	// A sort that keeps every row takes the morsels' runs as they finish, and absorbs them in
	// order: a morsel that finished ahead of an earlier one was held, not merged.
	let positioned = merges_of(&wide, "SELECT n, k FROM t ORDER BY k").await;
	assert_eq!(positioned, [MergeOrder::Positioned]);
	assert!(
		counts_of(&wide.exec_ctx().await).collected_early > before.collected_early,
		"no morsel finished ahead of an earlier one and was held"
	);
}

/// A sort that keeps every row takes the runs of the morsels behind a slow one as they finish and
/// holds them without holding their places: more morsels finish ahead of the slow morsel than the
/// policy lets be in flight, which a merge that counted the held runs against it would not allow,
/// and the rows, ties included, are the serial run's.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sort_run_held_ahead_of_a_slow_morsel_frees_the_place_of_its_morsel() {
	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(&merge_setup(), serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_rocksdb(&merge_setup(), wide_dir.path(), tiny()).await;
	let sql = "SELECT n, k FROM t ORDER BY k, n DESC";
	assert_eq!(merges_of(&wide, sql).await, [MergeOrder::Positioned]);
	let expected = answered(&serial, sql).await;
	let ctx = wide.exec_ctx().await;
	let read_ahead = crate::exec::planner::placement::Policy::from_config(
		&crate::exec::config::ExecConfig::default(),
	)
	.read_ahead();
	let mut early = 0;
	for _ in 0..5 {
		let before = counts_of(&ctx).collected_early;
		// The first morsel a worker claims runs for a long time while the others finish.
		delay_morsels(&ctx, &[1500]);
		assert_equivalent(sql, &answered(&wide, sql).await, &expected);
		early = counts_of(&ctx).collected_early - before;
		if early > read_ahead {
			break;
		}
	}
	delay_morsels(&ctx, &[]);
	assert!(
		early > read_ahead,
		"{early} morsels finished ahead of the slow one, no more than the {read_ahead} in flight"
	);
}

/// A sort that keeps every row, over a query with no store to spill to, holds the runs of the
/// morsels behind a slow one in places: no morsel charges its entries to the query's lease there,
/// so a run the coordinator holds is memory nothing bounds but the places, and no more runs are
/// held than the policy lets be in flight. The rows, ties included, are the serial run's.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sort_run_of_a_query_that_cannot_spill_keeps_the_place_of_its_morsel() {
	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(&merge_setup(), serial_dir.path(), whole()).await;
	let builder = crate::kvs::Datastore::builder().without_temp_store();
	let wide = TestDb::new_on_rocksdb_over(builder, &merge_setup(), wide_dir.path(), tiny()).await;
	let sql = "SELECT n, k FROM t ORDER BY k, n DESC";
	assert_eq!(merges_of(&wide, sql).await, [MergeOrder::Positioned]);
	let expected = answered(&serial, sql).await;
	let ctx = wide.exec_ctx().await;
	let read_ahead = crate::exec::planner::placement::Policy::from_config(
		&crate::exec::config::ExecConfig::default(),
	)
	.read_ahead();
	for attempt in 0..5 {
		let before = counts_of(&ctx).collected_early;
		delay_morsels(&ctx, &[1500]);
		assert_equivalent(sql, &answered(&wide, sql).await, &expected);
		if counts_of(&ctx).collected_early > before || attempt == 4 {
			break;
		}
	}
	delay_morsels(&ctx, &[]);
	let after = counts_of(&ctx);
	assert!(after.collected_early > 0, "no finished morsel was held");
	assert!(
		after.most_results_held <= read_ahead,
		"{} runs were held, more than the {read_ahead} in flight",
		after.most_results_held
	);
}

/// A sort that keeps every row, whose filter passes only a row of the first morsel, holds the
/// empty runs of the morsels behind a slow first morsel in places, with or without a store to
/// spill to: an empty run charges nothing to the query's lease, so the results the coordinator
/// holds stay within the policy's read-ahead however many morsels finish ahead of the slow one.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_sort_runs_held_ahead_of_a_slow_morsel_keep_their_places() {
	let _turn = TURNS.lock().await;
	let sql = "SELECT n, k FROM t WHERE n = 0 ORDER BY k";
	let read_ahead = crate::exec::planner::placement::Policy::from_config(
		&crate::exec::config::ExecConfig::default(),
	)
	.read_ahead();
	let serial_dir = tempfile::tempdir().expect("temporary directory");
	let serial = TestDb::new_on_rocksdb(&merge_setup(), serial_dir.path(), whole()).await;
	let expected = answered(&serial, sql).await;
	for spills in [true, false] {
		let wide_dir = tempfile::tempdir().expect("temporary directory");
		let builder = match spills {
			true => crate::kvs::Datastore::builder(),
			false => crate::kvs::Datastore::builder().without_temp_store(),
		};
		let wide =
			TestDb::new_on_rocksdb_over(builder, &merge_setup(), wide_dir.path(), tiny()).await;
		assert_eq!(merges_of(&wide, sql).await, [MergeOrder::Positioned]);
		let ctx = wide.exec_ctx().await;
		let before = counts_of(&ctx);
		for attempt in 0..5 {
			let early = counts_of(&ctx).collected_early;
			delay_morsels(&ctx, &[1500]);
			assert_equivalent(sql, &answered(&wide, sql).await, &expected);
			if counts_of(&ctx).collected_early > early + read_ahead || attempt == 4 {
				break;
			}
		}
		delay_morsels(&ctx, &[]);
		let after = counts_of(&ctx);
		assert!(after.handed > before.handed, "no morsel went to a worker (spills: {spills})");
		assert!(
			after.collected_early > before.collected_early,
			"no finished morsel was held (spills: {spills})"
		);
		assert!(
			after.most_results_held <= read_ahead,
			"{} results were held, more than the {read_ahead} in flight (spills: {spills})",
			after.most_results_held
		);
	}
}

/// The same over batches.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_by_keeps_the_serial_order_over_batches_whatever_order_they_finish_in() {
	let _turn = TURNS.lock().await;
	let serial = TestDb::new_with_config(&merge_setup(), whole()).await;
	let wide = TestDb::new_with_config(&merge_setup(), tiny()).await;
	let statements = [
		"SELECT n, k FROM t WHERE math::abs(n) >= 0 ORDER BY k",
		"SELECT n, k FROM t WHERE math::abs(n) >= 0 ORDER BY k DESC, n",
		"SELECT n, m FROM t WHERE math::abs(n) >= 0 ORDER BY m, k DESC",
	];
	let (early, handed) = reversed_completions(&serial, &wide, &statements).await;
	assert!(handed > 0, "no batch went to a worker");
	assert_eq!(early, 0);
}

/// Aggregates whose answers depend on the order the morsels are merged in: appends, a joined
/// string, a minimum, and the columns that take the first value their group sees.
#[cfg(feature = "kv-rocksdb")]
const ORDER_SENSITIVE_GROUPS: &[&str] = &[
	"SELECT g, count() AS c, array::group(n) AS ns, array::join(s, ',') AS j, math::min(f) AS lo FROM t GROUP BY g",
	"SELECT m, array::distinct(k) AS ks, s, math::max(n) AS hi FROM t GROUP BY m",
	"SELECT array::join(s, '-') AS j, array::group(g) AS gs, count() AS c FROM t GROUP ALL",
];

/// The same over bodies that call a function on their rows, which run as batches.
const ORDER_SENSITIVE_BATCHES: &[&str] = &[
	"SELECT g, count() AS c, array::group(n) AS ns, array::join(s, ',') AS j, math::min(f) AS lo FROM t WHERE math::abs(n) >= 0 GROUP BY g",
	"SELECT m, array::distinct(k) AS ks, s, math::max(n) AS hi FROM t WHERE math::abs(n) >= 0 GROUP BY m",
	"SELECT array::join(s, '-') AS j, array::group(g) AS gs, count() AS c FROM t WHERE math::abs(n) >= 0 GROUP ALL",
];

/// An aggregate folds its morsels in the order of the serial run, whatever order they finish in:
/// what it appends, joins and keeps first is the serial run's, and no morsel is merged ahead of
/// an earlier one.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_by_keeps_the_serial_fold_whatever_order_morsels_finish_in() {
	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(&merge_setup(), serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_rocksdb(&merge_setup(), wide_dir.path(), tiny()).await;
	plans_combine(&wide, ORDER_SENSITIVE_GROUPS, Combine::Aggregate).await;
	for sql in ORDER_SENSITIVE_GROUPS {
		assert_eq!(merges_of(&wide, sql).await, [MergeOrder::Adjacent], "`{sql}`");
	}
	let before = counts_of(&wide.exec_ctx().await);
	let (early, handed) = reversed_completions(&serial, &wide, ORDER_SENSITIVE_GROUPS).await;
	assert!(handed > 0, "no morsel went to a worker");
	assert_eq!(early, 0);
	// The morsels after the slow ones were held, and those next to one another were combined,
	// the earlier before the later: the answers above are the serial fold's.
	let after = counts_of(&wide.exec_ctx().await);
	assert!(after.collected_early > before.collected_early, "no finished morsel was held");
	assert!(after.combined > before.combined, "no morsels next to one another were combined");
}

/// The same over batches.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_by_keeps_the_serial_fold_over_batches_whatever_order_they_finish_in() {
	let _turn = TURNS.lock().await;
	let serial = TestDb::new_with_config(&merge_setup(), whole()).await;
	let wide = TestDb::new_with_config(&merge_setup(), tiny()).await;
	plans_combine(&wide, ORDER_SENSITIVE_BATCHES, Combine::Aggregate).await;
	let (early, handed) = reversed_completions(&serial, &wide, ORDER_SENSITIVE_BATCHES).await;
	assert!(handed > 0, "no batch went to a worker");
	assert_eq!(early, 0);
}

/// A float fold of one group takes the terms of its morsels in the order of the serial run,
/// whatever order the morsels finish in: its bits are the serial fold's.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ordered_float_fold_keeps_the_serial_bits_whatever_order_morsels_finish_in() {
	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(&float_setup(), serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_rocksdb(&float_setup(), wide_dir.path(), tiny()).await;
	let (early, handed) = reversed_completions(&serial, &wide, FLOAT_FOLDS).await;
	assert!(handed > 0, "no morsel went to a worker");
	assert_eq!(early, 0);
}

/// The same over batches.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ordered_float_fold_over_batches_keeps_the_serial_bits_whatever_order_they_finish_in() {
	let _turn = TURNS.lock().await;
	let serial = TestDb::new_with_config(&float_setup(), whole()).await;
	let wide = TestDb::new_with_config(&float_setup(), tiny()).await;
	let (early, handed) = reversed_completions(&serial, &wide, FLOAT_FOLD_BATCHES).await;
	assert!(handed > 0, "no batch went to a worker");
	assert_eq!(early, 0);
}

/// A statement whose rows fail fails whatever order its morsels finish in, with the failure of
/// one of the rows that fail: a morsel that fails first is raised, and no row of the later ones
/// is answered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_free_merge_raises_one_of_the_failures_of_its_rows() {
	let _turn = TURNS.lock().await;
	let serial = TestDb::new_with_config(&failing_setup(), whole()).await;
	let wide = TestDb::new_with_config(&failing_setup(), tiny()).await;
	let sql = "SELECT <int> s AS i FROM t WHERE math::abs(n) >= 0";
	assert_eq!(merges_of(&wide, sql).await, [MergeOrder::Free]);
	let (expected, _) = answer(&serial, sql).await;
	assert!(expected.is_err(), "`{sql}` fails serially");
	let handed = counts_of(&wide.exec_ctx().await).handed;
	// Another test of the process may hold the node's places: run on until a batch has gone to a
	// worker.
	for _ in 0..5 {
		for delays in [[0, 0, 0, 0], REVERSED, [100, 0, 300, 0]] {
			delay_morsels(&wide.exec_ctx().await, &delays);
			assert_equivalent_among(sql, &answered(&wide, sql).await, &expected, &cast_failures());
		}
		if counts_of(&wide.exec_ctx().await).handed > handed {
			break;
		}
	}
	delay_morsels(&wide.exec_ctx().await, &[]);
	assert!(counts_of(&wide.exec_ctx().await).handed > handed, "no batch went to a worker");
}

/// A morsel holds no more output than a window and the chunk that took it past the window before
/// it parks, so what unmerged morsels hold stays bounded whatever order they are merged in.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_free_merge_holds_each_morsel_to_its_window_and_a_chunk() {
	const QUANTUM: usize = 64;
	let _turn = TURNS.lock().await;
	let sql = "SELECT * FROM t";
	let config = |window: &str| {
		tiny()
			.with_key_value("morsel_target_bytes", "20000")
			.with_key_value("vm_quantum", QUANTUM.to_string())
			.with_key_value("morsel_window_bytes", window)
	};
	let mut held = Vec::new();
	let mut row_bytes = 0;
	for window in ["1", &(1usize << 30).to_string()] {
		let dir = tempfile::tempdir().expect("temporary directory");
		let db = TestDb::new_on_rocksdb(&setup(), dir.path(), config(window)).await;
		assert_eq!(merges_of(&db, sql).await, [MergeOrder::Free], "window {window}");
		let ctx = db.exec_ctx().await;
		for _ in 0..5 {
			let handed = counts_of(&ctx).handed;
			let rows = answered(&db, sql).await.expect("the scan answers");
			assert_eq!(rows.len(), ROWS);
			row_bytes = rows.iter().map(crate::exec::memory::size::value_bytes).max().unwrap_or(0);
			if counts_of(&ctx).handed > handed {
				break;
			}
		}
		held.push(metrics_of(&ctx).window_bytes_max as usize);
	}
	let (tight, loose) = (held[0], held[1]);
	assert!(
		tight > 0 && tight <= 1 + QUANTUM * row_bytes,
		"{tight} bytes held, rows of {row_bytes}"
	);
	assert!(tight < loose, "{tight} bytes held under a window of one byte, {loose} without one");
}

/// Under the node's settings a coordinator cuts a scan by what is left of it: a scan of many
/// morsels' worth is cut at once, into about as many morsels as the rest of it fills, and one that
/// ends within a morsel of where the coordinator stands is never cut. The answers are the serial
/// run's either way.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_scan_is_cut_by_what_is_left_of_it_under_the_default_settings() {
	let _turn = TURNS.lock().await;
	let setup = |rows: u32| {
		format!(
			"DEFINE TABLE record SCHEMALESS;
			 FOR $i IN 0..{rows} {{
				CREATE type::record('record', $i) SET n = $i, text = string::repeat('x', 300), tags = ['a', 'b']
			 }};"
		)
	};
	let sql = "SELECT n FROM record WHERE n % 3 = 0";
	for (rows, cut) in [(30_000u32, true), (400, false)] {
		let (serial_dir, wide_dir) = (
			tempfile::tempdir().expect("temporary directory"),
			tempfile::tempdir().expect("temporary directory"),
		);
		let serial = TestDb::new_on_rocksdb(&setup(rows), serial_dir.path(), whole()).await;
		let wide = TestDb::new_on_rocksdb(
			&setup(rows),
			wide_dir.path(),
			surrealdb_cnf::ConfigMap::empty(),
		)
		.await;
		let (expected, _) = answer(&serial, sql).await;
		let ctx = wide.exec_ctx().await;
		let (mut parts, mut handed) = (0, 0);
		for attempt in 0..5 {
			let before = counts_of(&ctx);
			assert_equivalent(sql, &answered(&wide, sql).await, &expected);
			let after = counts_of(&ctx);
			parts = after.split - before.split;
			handed = after.handed - before.handed;
			if parts > 0 || !cut || attempt == 4 {
				break;
			}
		}
		if cut {
			// About ten megabytes of records, cut in morsels of a megabyte: a few dozen at most,
			// and not the thousand parts of a datastore that sizes them from keys that cluster.
			assert!((2..=32).contains(&parts), "{rows} rows were cut into {parts} morsels");
			// The parts reached workers, not only the queue: a run that passed serially proves
			// nothing about the fan-out.
			assert!(handed > 0, "{rows} rows were cut into {parts} morsels, none handed on");
		} else {
			assert_eq!(parts, 0, "{rows} rows were cut into {parts} morsels");
		}
	}
}

/// The statements of the benches that read a table of a hundred thousand records, over a table
/// that is cut under the node's settings as theirs is: each runs as morsels that workers take, and
/// answers what the serial run answers.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_statements_of_the_scan_benches_hand_morsels_to_workers_under_the_default_settings() {
	let _turn = TURNS.lock().await;
	let setup = "DEFINE TABLE record SCHEMALESS;
		 FOR $i IN 0..30000 {
			CREATE type::record('record', $i) SET
				text = string::repeat('x', 300),
				city = ['London', 'Paris', 'Berlin', 'Tokyo', 'Madrid', 'Rome', 'Sydney', 'Beijing', 'New York'][$i % 9],
				score = ($i * 37 % 10000) * 0.01,
				number = $i % 5000
		 };";
	let statements = [
		"SELECT * FROM record",
		"SELECT id, score FROM record ORDER BY score",
		"SELECT math::sum(score) FROM record GROUP ALL",
		"SELECT city, count() FROM record GROUP BY city",
		"SELECT count(), math::mean(score), math::min(score), math::max(score), math::sum(number) FROM record GROUP ALL",
	];
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(setup, serial_dir.path(), whole()).await;
	let wide =
		TestDb::new_on_rocksdb(setup, wide_dir.path(), surrealdb_cnf::ConfigMap::empty()).await;
	let ctx = wide.exec_ctx().await;
	for sql in statements {
		let (expected, _) = answer(&serial, sql).await;
		let (mut split, mut handed) = (0, 0);
		for attempt in 0..5 {
			let before = counts_of(&ctx);
			assert_equivalent(sql, &answered(&wide, sql).await, &expected);
			let after = counts_of(&ctx);
			(split, handed) = (after.split - before.split, after.handed - before.handed);
			if (split > 0 && handed > 0) || attempt == 4 {
				break;
			}
		}
		assert!(split > 0, "`{sql}` was not cut");
		assert!(handed > 0, "`{sql}` was cut into {split} morsels and none went to a worker");
	}
}

/// A running morsel hands its rows to the coordinator as it produces them: the coordinator takes
/// rows before the morsels that produced them end, in the order of the serial run when the rows
/// are read in order and as the morsels produce them otherwise, and no morsel holds more than its
/// window and the chunk that took it past the window before it parks.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn running_morsels_stream_their_rows_and_hold_a_window_and_a_chunk_at_most() {
	const WINDOW: usize = 512 << 10;
	const SCAN_CHUNK: usize = 500;
	let _turn = TURNS.lock().await;
	let setup = "DEFINE TABLE record SCHEMALESS;
		 FOR $i IN 0..30000 {
			CREATE type::record('record', $i) SET n = $i, text = string::repeat('x', 300), tags = ['a', 'b']
		 };";
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(setup, serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_rocksdb(
		setup,
		wide_dir.path(),
		surrealdb_cnf::ConfigMap::empty()
			.with_key_value("morsel_target_bytes", "4000000")
			.with_key_value("morsel_window_bytes", WINDOW.to_string()),
	)
	.await;
	let ctx = wide.exec_ctx().await;
	let sql = "SELECT * FROM record";
	assert_eq!(merges_of(&wide, sql).await, [MergeOrder::Free]);
	let (expected, _) = answer(&serial, sql).await;
	let before = counts_of(&ctx);
	for attempt in 0..5 {
		assert_equivalent(sql, &answered(&wide, sql).await, &expected);
		if counts_of(&ctx).streamed > before.streamed {
			break;
		}
		assert!(attempt < 4, "no row was taken from a running morsel");
	}
	// The same rows bound by a `LET` are read in order, taken from the morsel at the front.
	let bound = "LET $rows = SELECT * FROM record; RETURN $rows";
	assert_eq!(binding_merges_of(&wide, "SELECT * FROM record").await, [MergeOrder::Serial]);
	let expected = answered(&serial, bound).await;
	let streamed = counts_of(&ctx).streamed;
	for attempt in 0..5 {
		// The morsel at the front starts late, so it is running while the others have ended.
		delay_morsels(&ctx, &[150]);
		assert_eq!(answered(&wide, bound).await, expected, "`{bound}`");
		if counts_of(&ctx).streamed > streamed {
			break;
		}
		assert!(attempt < 4, "no row was taken from the running morsel at the front");
	}
	delay_morsels(&ctx, &[]);
	let row_bytes = match &expected {
		Ok(rows) => rows.iter().map(crate::exec::memory::size::value_bytes).max().unwrap_or(0),
		Err(e) => panic!("the serial run fails: {e}"),
	};
	let held = metrics_of(&ctx).window_bytes_max as usize;
	assert!(
		held <= WINDOW + SCAN_CHUNK * row_bytes,
		"{held} bytes held, rows of {row_bytes}, a window of {WINDOW}"
	);
}

/// The rows a running morsel queues in its outbox are charged to the fan-out lease, and a refusal
/// of a queued chunk's grant parks the morsel right after that chunk, in a run that completes with
/// the serial run's answer.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queued_output_is_charged_and_a_refusal_parks_the_morsel() {
	let _turn = TURNS.lock().await;
	let setup = "DEFINE TABLE record SCHEMALESS;
		 FOR $i IN 0..30000 {
			CREATE type::record('record', $i) SET n = $i, text = string::repeat('x', 300), tags = ['a', 'b']
		 };";
	let sql = "SELECT * FROM record";
	let config = || {
		surrealdb_cnf::ConfigMap::empty()
			.with_key_value("morsel_target_bytes", "4000000")
			.with_key_value("morsel_window_bytes", (512usize << 10).to_string())
	};
	let dirs = [(); 3].map(|()| tempfile::tempdir().expect("temporary directory"));
	let serial = TestDb::new_on_rocksdb(setup, dirs[0].path(), whole()).await;
	let (expected, _) = answer(&serial, sql).await;

	// With the lease granting, the chunks running morsels queue are charged.
	let wide = TestDb::new_on_rocksdb(setup, dirs[1].path(), config()).await;
	let ctx = wide.exec_ctx().await;
	let mut charged = 0;
	for attempt in 0..5 {
		let before = counts_of(&ctx).output_charged;
		assert_equivalent(sql, &answered(&wide, sql).await, &expected);
		charged = counts_of(&ctx).output_charged - before;
		if charged > 0 {
			break;
		}
		assert!(attempt < 4, "no chunk of a running morsel was charged to the lease");
	}
	assert!(charged > 0);

	// With the lease refusing after its first few grants, the run is revoked and still completes,
	// the refused chunk reaching the coordinator without a grant. Which grant the refusal falls
	// on depends on how many the run asks for first, so a few counts are tried.
	let tight = TestDb::new_on_rocksdb(setup, dirs[2].path(), config()).await;
	let ctx = tight.exec_ctx().await;
	let (mut refused, mut parked) = (0, 0);
	for granted in [1, 2, 3, 5, 8] {
		let failing = fail_grants(&ctx, granted);
		for _ in 0..3 {
			let before = counts_of(&ctx);
			let got =
				tokio::time::timeout(std::time::Duration::from_secs(60), answered(&tight, sql))
					.await
					.expect("a refused output grant stalls the statement");
			assert_equivalent(sql, &got, &expected);
			let after = counts_of(&ctx);
			refused += after.output_refused - before.output_refused;
			parked += after.parked_after_refusal - before.parked_after_refusal;
		}
		drop(failing);
	}
	assert!(refused > 0, "no chunk a running morsel queued was refused by the lease");
	assert!(parked > 0, "no morsel parked after the chunk the lease refused");
}

/// The statement of the bench `select_all_100k`, over a table of its shape under the node's
/// settings, plans a merge as morsels finish, reads its key range in morsels that run on workers,
/// and answers the rows the serial run answers.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_select_all_bench_statement_merges_freely_and_fans_out_on_rocksdb() {
	let _turn = TURNS.lock().await;
	let setup = "DEFINE TABLE record SCHEMALESS;
		 FOR $i IN 0..30000 {
			CREATE type::record('record', $i) SET n = $i, text = string::repeat('x', 300), tags = ['a', 'b']
		 };";
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_rocksdb(setup, serial_dir.path(), whole()).await;
	// The node's settings but for morsels of tens of kilobytes: RocksDB finds the boundaries of a
	// range it has not yet flushed to files only at that granularity.
	let wide = TestDb::new_on_rocksdb(
		setup,
		wide_dir.path(),
		surrealdb_cnf::ConfigMap::empty().with_key_value("morsel_target_bytes", "20000"),
	)
	.await;
	let sql = "SELECT * FROM record";
	assert_eq!(merges_of(&wide, sql).await, [MergeOrder::Free]);
	let (expected, _) = answer(&serial, sql).await;
	let ctx = wide.exec_ctx().await;
	for attempt in 0..5 {
		let before = counts_of(&ctx);
		assert_equivalent(sql, &answered(&wide, sql).await, &expected);
		let after = counts_of(&ctx);
		if after.handed > before.handed && after.split > before.split {
			break;
		}
		assert!(attempt < 4, "no morsel of the scan went to a worker: {before:?} {after:?}");
	}
}
