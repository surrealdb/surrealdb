//! The fan-out lease over a datastore: a grant admits what the account of a worker's copied slots
//! admits, a refused grant runs its batch on the coordinator with the serial run's answer, and a
//! revoked lease parks a Range morsel so the coordinator finishes its range, and the held rows of a
//! value run as morsels answer what the serial run answers.

use std::sync::Arc;

use super::super::host::{Out, statement_state};
use super::super::program::Source;
use super::super::tests_morsel::{answered, bounded_on, lowered};
use super::held::{HeldRows, run_held_rows};
use super::lease::FanoutLease;
use super::{counts_of, effective_input};
use crate::exec::memory::{FairGovernor, MemTempStore};
use crate::exec::operators::test_util::TestDb;
use crate::exec::planner::placement::{Combine, MergeOrder, MorselInput, MorselPlan, Parallel};
use crate::expr::ControlFlow;
use crate::val::Value;

/// Workers draw on one pool the whole process shares, so the tests that count on holding places
/// take turns.
static TURNS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Rows of the table the tests read.
const ROWS: usize = 500;

/// Rows a setup statement writes: a request is held to the ceiling a test sets, and a few
/// hundred rows fit the smallest of them.
const CHUNK: usize = 250;

/// Rows a batch holds at least before it goes to a worker. Every batch copies the statement's
/// slots, so a larger value keeps the copies few while the batches of a run still go to workers.
const SPLIT_ROWS: usize = 100;

/// The statements that set up the table `t`: [`ROWS`] rows, written in chunks.
fn setup() -> Vec<String> {
	std::iter::once("DEFINE TABLE t;".to_owned())
		.chain((0..ROWS).step_by(CHUNK).map(|from| {
			format!(
				"FOR $i IN {from}..{} {{ CREATE type::record('t', $i) SET n = $i, k = $i % 7 RETURN NONE }};",
				(from + CHUNK).min(ROWS)
			)
		}))
		.collect()
}

/// Settings that hand every batch of [`SPLIT_ROWS`] rows or more to a worker and let workers take
/// every place, with each query held to `ceiling` pinned bytes.
fn fanning(ceiling: usize) -> surrealdb_cnf::ConfigMap {
	surrealdb_cnf::ConfigMap::empty()
		.with_key_value("morsel_split_rows", SPLIT_ROWS.to_string())
		.with_key_value("morsel_split_pass_ppm", "1000000")
		.with_key_value("morsel_needs_view", "false")
		.with_key_value("morsel_target_bytes", "1")
		.with_key_value("morsel_oltp_reserve", "0")
		.with_key_value("query_memory_minimum", "4096")
		.with_key_value("memory_reserve_chunk", "4096")
		.with_key_value("query_memory_max", ceiling.to_string())
}

/// Settings that never split, with each query held to `ceiling` pinned bytes.
fn whole(ceiling: usize) -> surrealdb_cnf::ConfigMap {
	surrealdb_cnf::ConfigMap::empty()
		.with_key_value("morsel_split_rows", usize::MAX.to_string())
		.with_key_value("query_memory_minimum", "4096")
		.with_key_value("memory_reserve_chunk", "4096")
		.with_key_value("query_memory_max", ceiling.to_string())
}

/// A datastore of [`setup`] whose queries are held to the ceiling `config` sets, over a node with
/// memory to spare. The setup's writes are not what a test bounds.
async fn bounded(config: surrealdb_cnf::ConfigMap) -> TestDb {
	let governor = Arc::new(FairGovernor::new(64 << 20));
	let store = Arc::new(MemTempStore::new(1024, None));
	let db = bounded_on(governor, store, "", config).await;
	for statement in setup() {
		db.run(&statement).await;
	}
	db
}

/// A datastore whose queries are held to the ceiling `config` sets and that holds the table `t`
/// but no row of it, for a test that plans a statement and runs none.
async fn bounded_empty(config: surrealdb_cnf::ConfigMap) -> TestDb {
	let governor = Arc::new(FairGovernor::new(64 << 20));
	let store = Arc::new(MemTempStore::new(1024, None));
	bounded_on(governor, store, "DEFINE TABLE t;", config).await
}

/// A SurrealKV datastore in `dir` that holds the table `t` and its rows, set up as [`bounded`]
/// does.
#[cfg(feature = "kv-surrealkv")]
async fn surrealkv_with_rows(dir: &std::path::Path, config: surrealdb_cnf::ConfigMap) -> TestDb {
	let db = TestDb::new_on_surrealkv("", dir, config).await;
	for statement in setup() {
		db.run(&statement).await;
	}
	db
}

/// A statement whose body reads `$this`, so its rows run as batches, inside a block that binds an
/// array of `elements` values: the statement's slots hold the array, and so does the copy of them
/// each worker keeps.
fn region(elements: usize) -> String {
	format!(
		"RETURN {{ LET $a = array::range(0, {elements}); \
		 RETURN (SELECT n, $this.k AS kk FROM t WHERE array::len($a) > 0 ORDER BY n) }}"
	)
}

/// A worker's copy of the slots is handed a grant exactly where the account that charges a copy of
/// slots, `State::keep_copied`, admits it: the same bytes against the same ceiling are admitted by
/// both or refused by both.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lease_admits_what_keep_copied_admits() {
	let ceiling: usize = 64 << 10;
	// Copies from a small share of the ceiling to well past it.
	for share in [0, 1, 5, 9, 10, 11, 20] {
		let bytes = ceiling * share / 10;
		let db = bounded_empty(fanning(ceiling)).await;
		let ctx = db.exec_ctx().await;
		let granted = {
			let lease = FanoutLease::new(ctx.ctx().memory().cloned());
			let grant = lease.grant(bytes, &ctx).await;
			grant.is_some()
		};
		let kept = {
			let program = lowered(&ctx, "SELECT n FROM t").await;
			let mut state = statement_state(&program, &ctx).expect("a statement state");
			let kept = state.keep_copied(vec![bytes], &ctx).await;
			kept.is_ok()
		};
		assert_eq!(granted, kept, "a copy of {bytes} bytes under a ceiling of {ceiling}");
	}
}

/// A copy of no bytes needs no grant: it is made under a ceiling nothing fits, and it does not
/// revoke the lease.
#[tokio::test]
async fn zero_copy_needs_no_grant() {
	let db = bounded_empty(fanning(1)).await;
	let ctx = db.exec_ctx().await;
	let lease = FanoutLease::new(ctx.ctx().memory().cloned());
	let grant = lease.grant(0, &ctx).await.expect("a copy of nothing");
	assert_eq!(lease.held(), 0);
	assert!(!lease.revoked());
	drop(grant);
}

/// The bytes the serial run of [`region`] holds for each element of the array it binds: the array,
/// the rows it is read beside, and the statement's own slots. A run over `tenths` tenths of a
/// ceiling binds that share of it in elements.
const BYTES_PER_ELEMENT: usize = 190;

/// The elements of an array the serial run of [`region`] holds `tenths` tenths of `ceiling` for.
fn elements_at(ceiling: usize, tenths: usize) -> usize {
	ceiling * tenths / 10 / BYTES_PER_ELEMENT
}

/// A ceiling the serial run stays under but a worker's copy of the slots would pass is no reason
/// to fail: the grant is refused, the batches run on the coordinator, and the answer is the serial
/// run's, whatever the size of the copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "no statement both runs its rows on workers and holds a block's slots: the statement of a block value is planned serially"]
async fn refused_grant_runs_on_coordinator() {
	let _turn = TURNS.lock().await;
	let ceiling: usize = 1 << 20;
	let mut refused = 0;
	for tenths in 7..=10 {
		let sql = region(elements_at(ceiling, tenths));
		let serial = bounded(whole(ceiling)).await;
		let wide = bounded(fanning(ceiling)).await;
		let expected = answered(&serial, &sql).await;
		let before = counts_of(&wide.exec_ctx().await);
		let got = answered(&wide, &sql).await;
		assert_eq!(got, expected, "the fanned-out run of a copy of {tenths} tenths of the ceiling");
		// A grant the query's memory refuses runs its batch on the coordinator.
		if expected.is_ok() {
			refused += counts_of(&wide.exec_ctx().await).refused - before.refused;
		}
	}
	assert!(refused > 0, "no grant was refused under a ceiling the serial run stays under");
}

/// [`region`] with a projection that is a row-independent subquery, which each batch's state
/// evaluates once and keeps in its ledger.
fn region_with_once(elements: usize) -> String {
	format!(
		"RETURN {{ LET $a = array::range(0, {elements}); \
		 RETURN (SELECT n, array::len((SELECT VALUE n FROM t WHERE n < 40)) AS c FROM t \
		 WHERE array::len($a) > 0 ORDER BY n) }}"
	)
}

/// A batch the coordinator runs after a refused grant makes the charges of the serial run and no
/// others: a ceiling the serial run stays under, with the batch's body keeping a value once, is no
/// reason for the fanned-out run to raise.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "no statement both runs its rows on workers and holds a block's slots: the statement of a block value is planned serially"]
async fn refused_grant_batch_keeps_under_the_serial_ceiling() {
	let _turn = TURNS.lock().await;
	let ceiling: usize = 1 << 20;
	let mut refused = 0;
	for tenths in 6..=10 {
		let sql = region_with_once(elements_at(ceiling, tenths));
		let serial = bounded(whole(ceiling)).await;
		let wide = bounded(fanning(ceiling)).await;
		let expected = answered(&serial, &sql).await;
		let before = counts_of(&wide.exec_ctx().await);
		let got = answered(&wide, &sql).await;
		assert_eq!(got, expected, "the fanned-out run of a copy of {tenths} tenths of the ceiling");
		if expected.is_ok() {
			refused += counts_of(&wide.exec_ctx().await).refused - before.refused;
		}
	}
	assert!(refused > 0, "no grant was refused under a ceiling the serial run stays under");
}

/// A live grant is no reason for a charge the serial run makes to raise: a worker that holds its
/// grant for the whole run, with the statement charging past it, answers what the serial run
/// answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grants_never_make_a_serial_charge_raise() {
	let _turn = TURNS.lock().await;
	let ceiling: usize = 1 << 20;
	for tenths in 2..=4 {
		let sql = region_with_once(elements_at(ceiling, tenths));
		let serial = bounded(whole(ceiling)).await;
		let wide = bounded(fanning(ceiling)).await;
		let expected = answered(&serial, &sql).await;
		assert!(expected.is_ok(), "the serial run of {tenths} tenths of the ceiling");
		assert_eq!(answered(&wide, &sql).await, expected, "{tenths} tenths of the ceiling");
	}
}

/// A lease revoked while a Range morsel runs on a worker parks the morsel at its next chunk
/// boundary, and the coordinator runs what is left of its range: the answers equal the serial
/// run's.
#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread")]
async fn revoked_range_parks_and_coordinator_finishes() {
	use super::super::tests_morsel::assert_equivalent;
	use super::fail_grants;

	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = surrealkv_with_rows(serial_dir.path(), whole(usize::MAX)).await;
	let wide = surrealkv_with_rows(wide_dir.path(), fanning(usize::MAX)).await;
	// The first worker is granted; the next grant is refused while it runs.
	let failing = fail_grants(&wide.exec_ctx().await, 1);
	let scans =
		["SELECT n, k FROM t", "SELECT VALUE n FROM t WHERE k = 3", "SELECT n FROM t ORDER BY n"];
	// Other tests of the process may hold the node's places while workers should run.
	for _ in 0..5 {
		for sql in scans {
			let expected = answered(&serial, sql).await;
			assert_equivalent(sql, &answered(&wide, sql).await, &expected);
		}
		if failing.refused() > 0 {
			break;
		}
	}
	assert!(failing.refused() > 0, "no grant was refused");
}

/// Elements of the held array the tests run: [`HELD`] numbers.
const HELD: i64 = 10_000;

/// A statement whose one pipeline reads a value source and casts each row, which fails on a row
/// that is not a number.
const HELD_SQL: &str = "SELECT VALUE <int> $this FROM [1, 2]";

/// The held numbers, with `bad` replacing the element at its position where it is given.
fn held_values(bad: Option<usize>) -> Vec<Value> {
	(0..HELD)
		.map(|at| match bad {
			Some(bad) if bad as i64 == at => Value::from("x"),
			_ => Value::from(at),
		})
		.collect()
}

/// The held numbers as a range.
fn held_range() -> HeldRows {
	HeldRows::Range(common::range::TypedRange::<i64>::from_range(0..HELD).iter())
}

/// The decision that runs a value source's rows as `Batches` morsels.
fn batches() -> Parallel {
	Parallel::Morsels(MorselPlan {
		input: MorselInput::Batches,
		combine: Combine::Concat,
		paged: None,
		prunes: false,
		merge: MergeOrder::Serial,
		ordered_fold: Vec::new(),
	})
}

/// What [`HELD_SQL`]'s value pipeline answers over the rows `rows` builds when the physical plan
/// decided `decision` for it, and how many morsels the run handed to workers.
async fn held_answer(
	db: &TestDb,
	decision: Parallel,
	rows: impl FnOnce() -> HeldRows,
) -> (Result<Vec<Value>, String>, usize) {
	let ctx = db.exec_ctx().await;
	let mut program = lowered(&ctx, HELD_SQL).await;
	let at = program
		.pipelines
		.iter()
		.position(|pipeline| matches!(pipeline.source, Source::Values { .. }))
		.expect("a value source");
	let planned = decision.clone();
	Arc::get_mut(&mut program).expect("a program no one else holds").pipelines[at].parallel =
		decision;
	let pipeline = &program.pipelines[at];
	assert_eq!(
		effective_input(pipeline).is_some(),
		matches!(planned, Parallel::Morsels(_)),
		"the decision names how the rows run"
	);
	let before = counts_of(&ctx).handed;
	let mut state = statement_state(&program, &ctx).expect("a statement state");
	let mut out = Out::Gather(Vec::new());
	let ran = run_held_rows(&program, pipeline, rows(), &mut state, &ctx, &mut out, 64).await;
	let handed = counts_of(&ctx).handed - before;
	let answer = match ran {
		Ok(_) => {
			let Out::Gather(mut rows) = out else {
				unreachable!("the output was gathered");
			};
			rows.append(&mut state.output);
			Ok(rows)
		}
		Err(ControlFlow::Err(e)) => Err(format!("{e:#}")),
		Err(_) => Err("control flow".to_owned()),
	};
	(answer, handed)
}

/// The rows of a held array or range run as `Batches` morsels answer what the serial decision
/// answers, rows, order and first failure alike, and some of the morsels run on workers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn held_rows_batches_equal_serial() {
	let _turn = TURNS.lock().await;
	let db = bounded(fanning(32 << 20)).await;
	let sources: [(&str, &dyn Fn() -> HeldRows); 3] = [
		("an array", &|| HeldRows::Values(held_values(None))),
		("a failing array", &|| HeldRows::Values(held_values(Some(7_000)))),
		("a range", &held_range),
	];
	for (name, rows) in sources {
		let (expected, handed) = held_answer(&db, Parallel::Serial, rows).await;
		assert_eq!(handed, 0, "{name} run serially");
		let mut wide = 0;
		// Other tests of the process may hold the node's places for a while.
		for _ in 0..5 {
			let (got, handed) = held_answer(&db, batches(), rows).await;
			assert_eq!(got, expected, "{name} run as batches");
			wide += handed;
			if wide > 0 {
				break;
			}
		}
		assert!(wide > 0, "{name} never reached a worker");
	}
	let (failed, _) =
		held_answer(&db, batches(), &|| HeldRows::Values(held_values(Some(7_000)))).await;
	assert!(failed.is_err_and(|e| e.contains("Could not cast")), "the first failure is raised");
}

/// A `Serial` decision runs the held rows one at a time on the statement's task: no morsel is
/// handed to a worker, whatever the settings allow.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn held_rows_serial_stays_serial() {
	let _turn = TURNS.lock().await;
	let db = bounded(fanning(32 << 20)).await;
	let (values, handed) =
		held_answer(&db, Parallel::Serial, || HeldRows::Values(held_values(None))).await;
	assert_eq!(handed, 0);
	assert_eq!(values.expect("the rows run").len(), HELD as usize);
	let (range, handed) = held_answer(&db, Parallel::Serial, held_range).await;
	assert_eq!(handed, 0);
	assert_eq!(range.expect("the rows run").len(), HELD as usize);
}
