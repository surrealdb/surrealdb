//! Evaluating the same work once per row.
//!
//! Several points in the expression layer evaluate one expression against a
//! whole batch: a scalar subquery, a graph or reference lookup, a field access
//! or `[*]` that dereferences a record link, and a batch of record fetches.
//! Each of those can overlap its rows, and each therefore has to decide whether
//! overlapping them is safe.
//!
//! [`evaluate_each`] is where that decision lives, and it is the only place it
//! lives. Because the access mode is a parameter rather than something derived
//! here, a new fan-out site cannot be written without stating what it evaluates,
//! which is the property this module exists to preserve.

use std::future::Future;

use crate::exec::{AccessMode, ExecutionContext, trampoline};
use crate::expr::FlowResult;
use crate::val::Value;

/// The mode of dereferencing a record id, for the fan-out sites that resolve
/// record links rather than evaluate a caller's expression.
///
/// Those sites hold no child expression whose mode they could read, so they
/// state one. What a dereference actually evaluates is the *target's* own
/// definitions: its permission predicates, which run under a context that
/// refuses a write, and its computed fields, whose bodies are rejected at
/// definition time if they contain a mutation.
///
/// That definition-time check treats a call to a user-defined function as
/// opaque, because the callee's body is stored separately and can be redefined
/// afterwards. A computed field that calls a function which writes therefore
/// reaches this fan-out, and is the one way the mode below is optimistic. See
/// #856.
pub(crate) const RECORD_DEREFERENCE: AccessMode = AccessMode::ReadOnly;

/// Evaluate `eval_one` against every item, overlapping the rows when that is
/// safe and worth it.
///
/// Results are returned in input order in both cases; only the order the work
/// *runs* in differs.
///
/// `mode` describes the work `eval_one` performs, not the caller. A
/// [`ReadWrite`](AccessMode::ReadWrite) mode is evaluated strictly one row at a
/// time, in input order, for two reasons:
///
/// - Overlapped rows interleave their side effects, so the order a statement's writes land in would
///   depend on the scheduler rather than on the input.
/// - Writes in a plan are serialised on a single transaction, whose savepoints form a stack with no
///   handle: a second writer entering before the first has left would unwind the wrong one.
///
/// A caller with no child expression to read a mode from must still name one,
/// and must say what it evaluated to reach that answer.
///
/// A read-only evaluation is overlapped only once the batch reaches
/// [`ExecConfig::fan_out_row_threshold`](crate::exec::config::ExecConfig::fan_out_row_threshold),
/// which a deployment can raise above any batch size to turn overlapping off.
///
/// Rows polled under a statement's driver ([`trampoline`]) also run one at a
/// time. A row's nested evaluations run as frames of that driver, which polls
/// only its topmost frame, so overlapped rows would share it in two ways that
/// fail:
///
/// - A row waiting in a frame beneath is not polled, and keeps what it holds across that wait: a
///   storage call waiting for a thread off the runtime holds the transaction's lock, and a row
///   nested above that reads the same transaction waits for it without end.
/// - A row that fails does not stop the rows nested above it, which run before the batch is polled
///   again, so a failure deep in a recursion through the rows reaches the batch only after every
///   sibling at every level has run.
pub(crate) async fn evaluate_each<'a, T, Fut>(
	ctx: &ExecutionContext,
	mode: AccessMode,
	items: &'a [T],
	eval_one: impl Fn(&'a T) -> Fut,
) -> FlowResult<Vec<Value>>
where
	Fut: Future<Output = FlowResult<Value>>,
{
	evaluate_each_above(ctx.root().ctx.config.exec.fan_out_row_threshold, mode, items, eval_one)
		.await
}

/// [`evaluate_each`] with the threshold supplied directly, for callers that
/// have no execution context to read it from.
async fn evaluate_each_above<'a, T, Fut>(
	threshold: usize,
	mode: AccessMode,
	items: &'a [T],
	eval_one: impl Fn(&'a T) -> Fut,
) -> FlowResult<Vec<Value>>
where
	Fut: Future<Output = FlowResult<Value>>,
{
	if mode.is_read_write() || items.len() < threshold || trampoline::under_driver() {
		let mut results = Vec::with_capacity(items.len());
		for item in items {
			results.push(eval_one(item).await?);
		}
		return Ok(results);
	}
	futures::future::try_join_all(items.iter().map(eval_one)).await
}

#[cfg(test)]
mod tests {
	use std::cell::RefCell;

	use anyhow::anyhow;

	use super::*;
	use crate::expr::ControlFlow;

	/// Evaluate one item, recording when it starts and finishes around a yield
	/// point so that the interleaving between items is observable.
	///
	/// A concurrent run reaches every start before any end; a sequential run
	/// pairs each start with its own end.
	async fn traced(trace: &RefCell<Vec<String>>, item: &i64) -> FlowResult<Value> {
		trace.borrow_mut().push(format!("start {item}"));
		tokio::task::yield_now().await;
		trace.borrow_mut().push(format!("end {item}"));
		Ok(Value::from(*item))
	}

	/// The threshold the tests below run against, chosen so that a five-row
	/// batch overlaps and a three-row one does not.
	const THRESHOLD: usize = 4;

	/// Run over `items` under `mode`, returning the results and the observed
	/// schedule.
	async fn run(mode: AccessMode, items: &[i64]) -> (Vec<Value>, Vec<String>) {
		let trace = RefCell::new(Vec::new());
		let results = evaluate_each_above(THRESHOLD, mode, items, |item| traced(&trace, item))
			.await
			.expect("evaluation should succeed");
		(results, trace.into_inner())
	}

	/// The schedule a sequential run of `items` produces.
	fn sequential_trace(items: &[i64]) -> Vec<String> {
		items.iter().flat_map(|i| [format!("start {i}"), format!("end {i}")]).collect()
	}

	#[tokio::test]
	async fn a_read_write_evaluation_runs_one_row_at_a_time_in_input_order() {
		let rows = [1, 2, 3, 4, 5];
		let (results, trace) = run(AccessMode::ReadWrite, &rows).await;
		assert_eq!(
			trace,
			sequential_trace(&rows),
			"a writing evaluation must not overlap its rows, however many there are"
		);
		assert_eq!(results, rows.map(Value::from));
	}

	#[tokio::test]
	async fn a_read_only_evaluation_overlaps_its_rows() {
		let rows = [1, 2, 3, 4, 5];
		let (results, trace) = run(AccessMode::ReadOnly, &rows).await;
		assert_eq!(
			trace,
			[
				"start 1", "start 2", "start 3", "start 4", "start 5", "end 1", "end 2", "end 3",
				"end 4", "end 5"
			],
			"a read-only evaluation should overlap the rows it is given"
		);
		// Overlapping changes the order the work runs in, never the order the
		// results come back in.
		assert_eq!(results, rows.map(Value::from));
	}

	#[tokio::test]
	async fn too_few_rows_to_be_worth_overlapping_stay_sequential() {
		// Up to the threshold, a read-only evaluation runs like a writing one.
		for count in 0..THRESHOLD {
			let rows: Vec<i64> = (1..=count as i64).collect();
			let (results, trace) = run(AccessMode::ReadOnly, &rows).await;
			assert_eq!(trace, sequential_trace(&rows), "{count} rows should not overlap");
			assert_eq!(results, rows.iter().copied().map(Value::from).collect::<Vec<_>>());
		}
	}

	/// The threshold `evaluate_each` uses comes from the deployment's config,
	/// not from a constant, so raising it past the batch size turns overlapping
	/// off for a whole datastore.
	#[cfg(feature = "kv-mem")]
	#[tokio::test]
	async fn the_threshold_comes_from_the_execution_config() {
		use surrealdb_cnf::ConfigMap;

		use crate::exec::operators::test_util::TestDb;

		let rows = [1, 2, 3, 4, 5];
		for (threshold, overlaps) in [("2", true), ("1000", false)] {
			let db = TestDb::new_with_config(
				"",
				ConfigMap::empty().with_key_value("fan_out_row_threshold", threshold),
			)
			.await;
			let ctx = db.exec_ctx().await;

			let trace = RefCell::new(Vec::new());
			evaluate_each(&ctx, AccessMode::ReadOnly, &rows, |item| traced(&trace, item))
				.await
				.expect("evaluation should succeed");

			let overlapped = trace.into_inner() != sequential_trace(&rows);
			assert_eq!(
				overlapped,
				overlaps,
				"a threshold of {threshold} should {} five rows",
				if overlaps {
					"overlap"
				} else {
					"not overlap"
				}
			);
		}
	}

	#[tokio::test]
	async fn a_failing_row_stops_a_read_write_evaluation_where_it_failed() {
		let trace = RefCell::new(Vec::new());
		let recorded = &trace;
		let err =
			evaluate_each_above(THRESHOLD, AccessMode::ReadWrite, &[1, 2, 3], |item| async move {
				if *item == 2 {
					return Err(ControlFlow::from(anyhow!("row {item} failed")));
				}
				traced(recorded, item).await
			})
			.await
			.expect_err("the failing row should abort the batch");

		assert!(err.to_string().contains("row 2 failed"), "got {err}");
		// The rows after the failure never ran, which is what makes the
		// sequential arm usable for work with side effects.
		assert_eq!(trace.into_inner(), ["start 1", "end 1"]);
	}

	#[tokio::test]
	async fn a_control_flow_signal_propagates_out_of_both_arms() {
		// Enough rows that the read-only case takes the concurrent arm.
		for mode in [AccessMode::ReadOnly, AccessMode::ReadWrite] {
			let signal =
				evaluate_each_above(THRESHOLD, mode, &[1, 2, 3, 4, 5], |item| async move {
					if *item == 5 {
						return Err(ControlFlow::Break);
					}
					Ok(Value::from(*item))
				})
				.await
				.expect_err("the signal should reach the caller");
			assert!(matches!(signal, ControlFlow::Break), "{mode:?} swallowed the signal");
		}
	}

	/// The rows of a batch polled under a statement's driver run one at a time. A row that
	/// holds a lock across an await is polled on to its release before a later row nests an
	/// evaluation that waits for the lock.
	#[tokio::test]
	async fn under_a_driver_a_row_holding_a_lock_ends_before_a_later_row_nests_a_wait_for_it() {
		use std::sync::Arc;

		use futures::StreamExt;

		use crate::exec::trampoline::{Trampolined, nest};

		let lock = Arc::new(tokio::sync::Mutex::new(()));
		let rows = [1, 2, 3, 4, 5];
		let run = Trampolined::root(Box::pin(futures::stream::once(evaluate_each_above(
			THRESHOLD,
			AccessMode::ReadOnly,
			&rows,
			|item| {
				let lock = Arc::clone(&lock);
				async move {
					match *item {
						1 => {
							let held = lock.lock().await;
							tokio::time::sleep(std::time::Duration::from_millis(20)).await;
							drop(held);
						}
						5 => nest(async move { drop(lock.lock().await) }).await,
						_ => {}
					}
					Ok(Value::from(*item))
				}
			},
		))));
		let answered =
			tokio::time::timeout(std::time::Duration::from_secs(5), run.collect::<Vec<_>>())
				.await
				.expect("every row ends");
		let values = answered.into_iter().next().expect("one answer").expect("the rows succeed");
		assert_eq!(values, rows.map(Value::from));
	}

	/// Under a statement's driver a row whose nested evaluation fails ends the batch before any
	/// later row runs.
	#[tokio::test]
	async fn under_a_driver_a_failing_row_ends_the_batch_before_a_later_row_runs() {
		use std::sync::Arc;
		use std::sync::atomic::{AtomicUsize, Ordering};

		use futures::StreamExt;

		use crate::exec::trampoline::{Trampolined, nest};

		let later = Arc::new(AtomicUsize::new(0));
		let rows = [1, 2, 3, 4, 5];
		let mut run = Trampolined::root(Box::pin(futures::stream::once(evaluate_each_above(
			THRESHOLD,
			AccessMode::ReadOnly,
			&rows,
			|item| {
				let later = Arc::clone(&later);
				let item = *item;
				async move {
					nest(async move {
						if item == 1 {
							return Err(ControlFlow::from(anyhow!("row {item} failed")));
						}
						later.fetch_add(1, Ordering::Relaxed);
						Ok(Value::from(item))
					})
					.await
				}
			},
		))));
		let err = run.next().await.expect("one answer").expect_err("the first row fails");
		assert!(err.to_string().contains("row 1 failed"), "got {err}");
		assert_eq!(later.load(Ordering::Relaxed), 0, "a row after the failure ran");
	}

	/// Records read through a fan-out of record ids on SurrealKV with no inline storage calls, so
	/// that each read holds the transaction while it waits for the thread that reads. Each read
	/// derives its record as a nested evaluation; every statement ends.
	#[cfg(feature = "kv-surrealkv")]
	#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
	async fn records_read_through_a_fan_out_on_offloaded_surrealkv_reads_end() {
		use surrealdb_cnf::ConfigMap;

		use crate::exec::operators::test_util::TestDb;

		for round in 0..10 {
			let dir = temp_dir::TempDir::new().expect("a directory");
			let db = TestDb::new_on_surrealkv(
				"CREATE item:1, item:2, item:3, item:4, item:5;",
				dir.path(),
				ConfigMap::empty().with_key_value("runtime_worker_threads", "1"),
			)
			.await;
			let ds = std::sync::Arc::clone(db.datastore());
			let answers = tokio::time::timeout(
				std::time::Duration::from_secs(10),
				ds.execute(
					"LET $ids = SELECT VALUE id FROM item; (SELECT VALUE id FROM $ids).len();",
					&TestDb::owner(),
					None,
				),
			)
			.await
			.unwrap_or_else(|_| panic!("round {round}: the statement did not end"))
			.expect("the request runs");
			let last = answers.into_iter().last().expect("two answers").result.expect("succeeds");
			assert_eq!(format!("{last:?}"), "Number(Int(5))", "round {round}");
		}
	}
}
