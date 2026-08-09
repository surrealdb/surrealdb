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

use crate::exec::{AccessMode, ExecutionContext};
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
/// - Writes on the streaming path are serialised on a single transaction, whose savepoints form a
///   stack with no handle: a second writer entering before the first has left would unwind the
///   wrong one.
///
/// A caller with no child expression to read a mode from must still name one,
/// and must say what it evaluated to reach that answer.
///
/// A read-only evaluation is overlapped only once the batch reaches
/// [`ExecConfig::fan_out_row_threshold`](crate::exec::config::ExecConfig::fan_out_row_threshold),
/// which a deployment can raise above any batch size to turn overlapping off.
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
	if mode.is_read_write() || items.len() < threshold {
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
}
