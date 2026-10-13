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

/// What the spawned rows' `document_root` is reconstructed as: the row's
/// own value (the default `evaluate_batch` convention) or the enclosing
/// context's root, cloned once and shared (the lookup convention).
#[cfg(not(target_family = "wasm"))]
#[derive(Clone, Copy)]
pub(crate) enum SpawnedDocRoot {
	RowValue,
	Inherited,
}

/// How many chunks run side by side in the spawned fan-out.
#[cfg(not(target_family = "wasm"))]
const SPAWNED_CHUNK_CONCURRENCY: usize = 16;

/// Rows the probe runs before its timer starts, absorbing one-off costs
/// (cursor and context setup) that would otherwise inflate the reading.
#[cfg(not(target_family = "wasm"))]
const PROBE_WARM_ROWS: usize = 2;

/// Rows the probe times to estimate the per-row cost. A sample this wide
/// keeps timer and scheduling noise from tipping cheap rows over the bar,
/// while staying constant so the serial prefix never grows with the batch.
#[cfg(not(target_family = "wasm"))]
const PROBE_TIMED_ROWS: usize = 6;

/// The measured per-row cost below which spawning does not pay: the owned
/// row clones and task dispatch cost roughly this much themselves.
#[cfg(not(target_family = "wasm"))]
const SPAWN_PER_ROW_NANOS: u64 = 5_000;

/// The projected remaining work below which spawning does not pay: at
/// millisecond scale, task dispatch, scheduling latency and cross-core
/// cache traffic eat the parallel saving, and a wrong call here costs
/// real regressions on cheap batches — the bar sits far above timing
/// noise so small workloads always keep their exact sequential cost.
#[cfg(not(target_family = "wasm"))]
const SPAWN_MIN_TOTAL_NANOS: u64 = 8_000_000;

/// Runs contiguous chunks of rows on their own runtime tasks — bounded,
/// results in input order — reconstructing each chunk's evaluation context
/// from owned clones so the tasks are `'static`. This is what row-level
/// plan-executing expressions (graph and reference lookups) need where the
/// cooperative fan-out buys nothing: their work is CPU-bound on an
/// embedded backend, and only separate tasks reach separate cores. Chunks
/// rather than single rows keep the dispatch cost amortized when each row
/// is cheap; several chunks per task slot keep a skewed chunk from
/// stalling the batch. The `JoinSet` aborts whatever is still in flight
/// when it drops, so an error never leaks detached tasks. On failure the
/// lowest-index row's error surfaces: no further chunks are spawned, the
/// bounded in-flight set is drained, and the smallest failing index wins —
/// which error a caller sees never depends on the scheduler.
///
/// The reconstruction restores `local_params`, `skip_fetch_perms`,
/// `computing_record`, `plan_depth` and the `doc_root`-selected
/// `document_root` into each spawned row's context. Callers must gate on
/// `AccessMode::ReadOnly` and an empty `recursion_ctx` — neither survives
/// the context reconstruction.
#[cfg(not(target_family = "wasm"))]
pub(crate) async fn evaluate_rows_spawned(
	expr: std::sync::Arc<dyn crate::exec::PhysicalExpr>,
	ctx: &crate::exec::physical_expr::EvalContext<'_>,
	values: &[Value],
	doc_root: SpawnedDocRoot,
) -> crate::expr::FlowResult<Vec<Value>> {
	use crate::exec::physical_expr::EvalContext;
	let inherited_root: Option<std::sync::Arc<Value>> = match doc_root {
		SpawnedDocRoot::Inherited => ctx.document_root.map(|v| std::sync::Arc::new(v.clone())),
		SpawnedDocRoot::RowValue => None,
	};
	// Block-local `LET` bindings, owned once per batch (never per row) so
	// each spawned chunk can restore them: `from_exec_ctx` reconstructs a
	// context with no `local_params`, and a `$param` that silently resolved
	// to nothing would be a wrong answer, not an error.
	let local_params: Option<
		std::sync::Arc<std::collections::HashMap<surrealdb_strand::Strand, Value>>,
	> = ctx.local_params.map(|m| std::sync::Arc::new(m.clone()));
	// The probe is a constant handful of rows run inline — borrowed
	// values, the exact sequential cost — independent of the batch size:
	// its warm rows absorb one-off setup cost and its timed rows estimate
	// the per-row cost that decides whether the remainder is worth
	// spawning. A constant sample keeps the serial prefix from scaling
	// with the batch, so the achievable parallel speedup is not capped by
	// the probe itself. Below the spawn bars, per-row work is so cheap
	// that the owned clones and task dispatch cost more than they
	// recover, so the remainder stays sequential.
	let probe = (PROBE_WARM_ROWS + PROBE_TIMED_ROWS).min(values.len());
	let mut head = Vec::with_capacity(probe);
	let mut started = web_time::Instant::now();
	for (idx, value) in values.iter().take(probe).enumerate() {
		if idx == PROBE_WARM_ROWS {
			started = web_time::Instant::now();
		}
		let row_ctx = match doc_root {
			SpawnedDocRoot::RowValue => ctx.with_value_and_doc(value),
			SpawnedDocRoot::Inherited => ctx.with_value(value),
		};
		head.push(expr.evaluate(row_ctx).await?);
	}
	let timed = head.len().saturating_sub(PROBE_WARM_ROWS);
	let per_row = started.elapsed().as_nanos() as u64 / timed.max(1) as u64;
	let remaining_work = per_row.saturating_mul(values.len().saturating_sub(probe) as u64);
	if timed == 0
		|| per_row < SPAWN_PER_ROW_NANOS
		|| remaining_work < SPAWN_MIN_TOTAL_NANOS
		|| values.len() <= probe
	{
		let mut results = head;
		results.reserve(values.len().saturating_sub(probe));
		for value in values.iter().skip(probe) {
			let row_ctx = match doc_root {
				SpawnedDocRoot::RowValue => ctx.with_value_and_doc(value),
				SpawnedDocRoot::Inherited => ctx.with_value(value),
			};
			results.push(expr.evaluate(row_ctx).await?);
		}
		return Ok(results);
	}
	let values = &values[probe..];
	// Chunks are sized from the rows that remain after the probe: several
	// per task slot amortize dispatch when rows are cheap while keeping a
	// skewed chunk from stalling the batch.
	let chunk_size = values.len().div_ceil(SPAWNED_CHUNK_CONCURRENCY * 4).max(1);
	let mut out: Vec<Option<Vec<Value>>> = Vec::new();
	out.resize_with(values.len().div_ceil(chunk_size), || None);
	let mut pending: std::collections::VecDeque<_> = values
		.chunks(chunk_size)
		.enumerate()
		.map(|(idx, chunk)| {
			let expr = std::sync::Arc::clone(&expr);
			let exec_ctx = ctx.exec_ctx.clone();
			let chunk = chunk.to_vec();
			let inherited_root = inherited_root.clone();
			let local_params = local_params.clone();
			let skip_fetch_perms = ctx.skip_fetch_perms;
			let computing_record = ctx.computing_record.clone();
			let plan_depth = ctx.plan_depth;
			async move {
				let mut results = Vec::with_capacity(chunk.len());
				for value in &chunk {
					let mut row_ctx = EvalContext::from_exec_ctx(&exec_ctx);
					row_ctx.local_params = local_params.as_deref();
					row_ctx.skip_fetch_perms = skip_fetch_perms;
					row_ctx.computing_record = computing_record.clone();
					row_ctx.plan_depth = plan_depth;
					let row_ctx = match doc_root {
						SpawnedDocRoot::RowValue => row_ctx.with_value_and_doc(value),
						SpawnedDocRoot::Inherited => {
							row_ctx.document_root = inherited_root.as_deref();
							row_ctx.with_value(value)
						}
					};
					match expr.evaluate(row_ctx).await {
						Ok(value) => results.push(value),
						Err(err) => return (idx, Err(err)),
					}
				}
				(idx, Ok(results))
			}
		})
		.collect();
	let mut set = tokio::task::JoinSet::new();
	// The lowest-index failure wins: chunks are contiguous, so the smallest
	// failing chunk index carries the batch's first failing row. On any
	// failure no further chunks are spawned; what is already in flight
	// (bounded by the concurrency cap) is drained so a lower-index error
	// still in progress can overtake one that merely finished first. A
	// panicked task surfaces only when no row error was collected — its
	// position carries no meaning.
	let mut first_err: Option<(usize, crate::expr::ControlFlow)> = None;
	let mut join_err: Option<crate::expr::ControlFlow> = None;
	loop {
		if first_err.is_none() && join_err.is_none() {
			while set.len() < SPAWNED_CHUNK_CONCURRENCY {
				let Some(task) = pending.pop_front() else {
					break;
				};
				set.spawn(task);
			}
		}
		let Some(joined) = set.join_next().await else {
			break;
		};
		match joined {
			Ok((idx, Ok(results))) => out[idx] = Some(results),
			Ok((idx, Err(err))) => {
				if first_err.as_ref().is_none_or(|(lowest, _)| idx < *lowest) {
					first_err = Some((idx, err));
				}
			}
			Err(e) => {
				join_err.get_or_insert_with(|| {
					crate::expr::ControlFlow::Err(anyhow::anyhow!("row fan-out task failed: {e}"))
				});
			}
		}
	}
	if let Some((_, err)) = first_err {
		return Err(err);
	}
	if let Some(err) = join_err {
		return Err(err);
	}
	// `head` already holds the timing probe's own rows, evaluated
	// sequentially and in order; the spawned suffix's chunks follow, each
	// already in order courtesy of `out`'s per-chunk indexing, so the
	// concatenation lines up with the original `values` one for one.
	let mut results = head;
	results.reserve(values.len());
	results.extend(out.into_iter().flat_map(|chunk| chunk.expect("every chunk task completed")));
	Ok(results)
}

#[cfg(not(target_family = "wasm"))]
#[cfg(test)]
mod spawned_tests {
	use std::sync::Arc;
	use std::time::Duration;

	use surrealdb_types::{SqlFormat, ToSql};

	use super::*;
	use crate::exec::operators::test_util::root_ctx;
	use crate::exec::{BoxFut, ContextLevel, EvalContext, PhysicalExpr};
	use crate::val::Number;

	/// A row expression whose cost is a fixed real-time delay rather than CPU
	/// work, so a test can push `evaluate_rows_spawned` past its per-row and
	/// total-work spawn thresholds without depending on how fast the row's own
	/// logic happens to run on the machine executing the test.
	///
	/// Maps each row's integer to `n * 2 + 1`, a value the row's own input
	/// still determines, so a test can check that a result corresponds to the
	/// row that produced it and not merely that the count came out right.
	#[derive(Debug)]
	struct SlowDoubler {
		per_row_delay: Duration,
	}

	impl ToSql for SlowDoubler {
		fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
			f.push_str("SlowDoubler");
		}
	}

	impl PhysicalExpr for SlowDoubler {
		fn name(&self) -> &'static str {
			"SlowDoubler"
		}

		fn as_any(&self) -> &dyn std::any::Any {
			self
		}

		fn required_context(&self) -> ContextLevel {
			ContextLevel::Root
		}

		fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
			Box::pin(async move {
				let n = match ctx.current_value {
					Some(Value::Number(Number::Int(n))) => *n,
					other => panic!("expected an integer row, got {other:?}"),
				};
				tokio::time::sleep(self.per_row_delay).await;
				Ok(Value::from(n * 2 + 1))
			})
		}

		fn access_mode(&self) -> AccessMode {
			AccessMode::ReadOnly
		}
	}

	/// The probe is a constant number of rows, so batches at and just past
	/// its size are the boundary cases: at (or below) it every row runs
	/// inline, one past it the spawned suffix is a single small chunk.
	/// Whichever arm runs, the results must be complete and in order.
	#[tokio::test]
	async fn batches_around_the_probe_boundary_keep_every_row() {
		let probe = (PROBE_WARM_ROWS + PROBE_TIMED_ROWS) as i64;
		for rows in [probe - 1, probe, probe + 1] {
			let values: Vec<Value> = (0..rows).map(Value::from).collect();
			// 10ms per row keeps one remaining row past the projected-work
			// bar, so the `probe + 1` case reliably takes the spawned path.
			let expr: Arc<dyn PhysicalExpr> = Arc::new(SlowDoubler {
				per_row_delay: Duration::from_millis(10),
			});
			let exec_ctx = root_ctx();
			let base = EvalContext::from_exec_ctx(&exec_ctx);
			let results = evaluate_rows_spawned(expr, &base, &values, SpawnedDocRoot::RowValue)
				.await
				.expect("evaluation should succeed");
			let expected: Vec<Value> = (0..rows).map(|n| Value::from(n * 2 + 1)).collect();
			assert_eq!(results, expected, "{rows} rows must come back complete and in order");
		}
	}

	/// A row expression that reads the block-local `$x` binding from the
	/// evaluation context, with a delay that pushes a batch onto the
	/// spawned path — where the context is reconstructed and the binding
	/// must be restored rather than silently dropped.
	#[derive(Debug)]
	struct LocalParamReader;

	impl ToSql for LocalParamReader {
		fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
			f.push_str("LocalParamReader");
		}
	}

	impl PhysicalExpr for LocalParamReader {
		fn name(&self) -> &'static str {
			"LocalParamReader"
		}

		fn as_any(&self) -> &dyn std::any::Any {
			self
		}

		fn required_context(&self) -> ContextLevel {
			ContextLevel::Root
		}

		fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
			Box::pin(async move {
				tokio::time::sleep(Duration::from_millis(1)).await;
				Ok(ctx
					.local_params
					.and_then(|params| params.get("x"))
					.cloned()
					.unwrap_or(Value::None))
			})
		}

		fn access_mode(&self) -> AccessMode {
			AccessMode::ReadOnly
		}
	}

	#[tokio::test]
	async fn spawned_rows_still_see_block_local_params() {
		use std::collections::HashMap;

		use surrealdb_strand::Strand;

		const ROWS: i64 = 100;
		let rows: Vec<Value> = (0..ROWS).map(Value::from).collect();
		let expr: Arc<dyn PhysicalExpr> = Arc::new(LocalParamReader);

		let exec_ctx = root_ctx();
		// The binding lives only in the evaluation context — deliberately
		// not mirrored into the execution context — so a spawned row that
		// loses `local_params` has nothing to fall back on.
		let params: HashMap<Strand, Value> = HashMap::from([(Strand::from("x"), Value::from(42))]);
		let mut base = EvalContext::from_exec_ctx(&exec_ctx);
		base.local_params = Some(&params);

		let results = evaluate_rows_spawned(expr, &base, &rows, SpawnedDocRoot::RowValue)
			.await
			.expect("evaluation should succeed");
		assert_eq!(results.len(), rows.len());
		for (idx, result) in results.iter().enumerate() {
			assert_eq!(
				*result,
				Value::from(42),
				"row {idx} must resolve $x from the restored local params"
			);
		}
	}

	/// A row expression that fails on two chosen rows: the lower-index one
	/// slowly, the higher-index one immediately — so the failure that
	/// *completes* first is not the one that must surface.
	#[derive(Debug)]
	struct FailsTwice;

	impl ToSql for FailsTwice {
		fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
			f.push_str("FailsTwice");
		}
	}

	impl PhysicalExpr for FailsTwice {
		fn name(&self) -> &'static str {
			"FailsTwice"
		}

		fn as_any(&self) -> &dyn std::any::Any {
			self
		}

		fn required_context(&self) -> ContextLevel {
			ContextLevel::Root
		}

		fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
			Box::pin(async move {
				let n = match ctx.current_value {
					Some(Value::Number(Number::Int(n))) => *n,
					other => panic!("expected an integer row, got {other:?}"),
				};
				match n {
					10 => {
						tokio::time::sleep(Duration::from_millis(20)).await;
						Err(crate::expr::ControlFlow::Err(anyhow::anyhow!("row 10 failed")))
					}
					90 => Err(crate::expr::ControlFlow::Err(anyhow::anyhow!("row 90 failed"))),
					_ => {
						tokio::time::sleep(Duration::from_millis(1)).await;
						Ok(Value::from(n))
					}
				}
			})
		}

		fn access_mode(&self) -> AccessMode {
			AccessMode::ReadOnly
		}
	}

	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn the_lowest_index_error_surfaces_regardless_of_completion_order() {
		const ROWS: i64 = 100;
		let rows: Vec<Value> = (0..ROWS).map(Value::from).collect();
		let expr: Arc<dyn PhysicalExpr> = Arc::new(FailsTwice);

		let exec_ctx = root_ctx();
		let base = EvalContext::from_exec_ctx(&exec_ctx);
		let err = evaluate_rows_spawned(expr, &base, &rows, SpawnedDocRoot::RowValue)
			.await
			.expect_err("two rows fail, so the batch must fail");

		assert!(
			err.to_string().contains("row 10 failed"),
			"row 90's immediate failure completes first, but row 10's must surface: got {err}"
		);
	}

	#[tokio::test]
	async fn a_spawned_batch_keeps_every_row_including_the_timing_probe() {
		// 100 rows at 1ms each clears both `SPAWN_PER_ROW_NANOS` (per-row cost)
		// and `SPAWN_MIN_TOTAL_NANOS` (projected remaining work) by two orders
		// of magnitude, so the run reliably takes the concurrent-chunk path
		// rather than staying sequential.
		const ROWS: i64 = 100;
		let rows: Vec<Value> = (0..ROWS).map(Value::from).collect();
		let expr: Arc<dyn PhysicalExpr> = Arc::new(SlowDoubler {
			per_row_delay: Duration::from_millis(1),
		});

		let exec_ctx = root_ctx();
		let base = EvalContext::from_exec_ctx(&exec_ctx);
		let results = evaluate_rows_spawned(expr, &base, &rows, SpawnedDocRoot::RowValue)
			.await
			.expect("evaluation should succeed");

		let expected: Vec<Value> = (0..ROWS).map(|n| Value::from(n * 2 + 1)).collect();
		assert_eq!(
			results.len(),
			rows.len(),
			"every row must produce a result, including the rows the timing probe ran"
		);
		assert_eq!(
			results, expected,
			"each result must correspond to the input row that produced it, in input order"
		);
	}
}
