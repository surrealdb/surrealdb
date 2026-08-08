//! Shared helpers for recursion strategies: RecordId enforcement and concurrent evaluation.

use std::sync::Arc;

use futures::{StreamExt, TryStreamExt, stream};
use surrealdb_types::ToSql;

use crate::exec::parts::is_final;
use crate::exec::{BoxFut, ExecOperator, ExecutionContext, FlowResult};
use crate::val::Value;

/// Maximum number of concurrent path evaluations per depth level.
/// Limits parallelism to avoid overwhelming the KV layer while still
/// allowing progress when individual evaluations block on I/O.
pub(crate) const RECURSION_CONCURRENCY: usize = 16;

/// The `{min..max}` range of a recursive idiom (`.{min..max}`) plus the resolved
/// system `idiom_recursion_limit`, shared by every recursion strategy and
/// constructed once by the dispatching `RecursionOp`.
///
/// `max` carries the user-specified upper bound, or `None` when the user gave no
/// bound. That distinction drives limit handling: an explicit bound stops the
/// recursion silently at that depth, whereas an unbounded recursion is capped at
/// the system limit and *exhausting* that cap is a hard error (matching the
/// legacy `compute()` engine). The two cases are otherwise indistinguishable
/// once the cap is resolved (a user bound equal to the system limit is legal),
/// so the `Option` must be preserved rather than collapsed to a single depth
/// value.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RecursionBounds {
	/// Minimum successful traversals required (`{N..}`); 0 when unspecified.
	pub(crate) min: u32,
	/// User-specified maximum (`{..N}`); `None` when unbounded.
	pub(crate) max: Option<u32>,
	/// The system `idiom_recursion_limit`, resolved from the context config by
	/// the dispatching operator.
	pub(crate) system_limit: u32,
}

impl RecursionBounds {
	/// The effective iteration cap: the user's max if given, otherwise the
	/// system idiom-recursion limit; never above the system limit. The `.min()`
	/// clamp is defence-in-depth only: the parser already rejects user bounds
	/// above the system limit, and a hypothetically clamped bound would still
	/// stop silently rather than error.
	pub(crate) fn cap(&self) -> u32 {
		self.max.unwrap_or(self.system_limit).min(self.system_limit)
	}

	/// Whether exhausting the cap is a hard error. True only when the user gave
	/// no explicit upper bound, so the cap is the system limit.
	pub(crate) fn errors_on_limit(&self) -> bool {
		self.max.is_none()
	}
}

/// Check if a value is a valid recursion target.
///
/// Recursion is intended purely for RecordId traversal. Only `RecordId`
/// values and arrays containing at least one `RecordId` are valid targets.
/// All other types (String, Number, Object, Uuid, etc.) are treated as
/// terminal and stop recursion at that branch.
pub(crate) fn is_recursion_target(value: &Value) -> bool {
	match value {
		Value::RecordId(_) => true,
		Value::Array(arr) => arr.iter().any(is_recursion_target),
		_ => false,
	}
}

/// Evaluate a batch of futures with bounded concurrency.
///
/// When fewer than 2 futures are provided, runs them sequentially to avoid
/// stream combinator overhead. Otherwise, uses `buffered(RECURSION_CONCURRENCY)`
/// to poll up to N futures concurrently -- when one blocks on I/O, others
/// make progress.
///
/// Short-circuits on the first error via `try_collect`.
pub(crate) async fn eval_buffered<'a, T: 'a>(
	futures: Vec<BoxFut<'a, FlowResult<T>>>,
) -> FlowResult<Vec<T>> {
	if futures.len() < 2 {
		let mut results = Vec::with_capacity(futures.len());
		for fut in futures {
			results.push(fut.await?);
		}
		Ok(results)
	} else {
		stream::iter(futures).buffered(RECURSION_CONCURRENCY).try_collect().await
	}
}

/// Like [`eval_buffered`], but collects all results without short-circuiting.
///
/// Used when callers need to inspect each result individually (e.g. to
/// handle path-elimination signals as non-fatal).
pub(crate) async fn eval_buffered_all<'a>(
	futures: Vec<BoxFut<'a, FlowResult<Value>>>,
) -> Vec<FlowResult<Value>> {
	if futures.len() < 2 {
		let mut results = Vec::with_capacity(futures.len());
		for fut in futures {
			results.push(fut.await);
		}
		results
	} else {
		stream::iter(futures).buffered(RECURSION_CONCURRENCY).collect().await
	}
}

/// Extract valid recursion target values from a single batch result value.
///
/// For array values, iterates elements and appends those that are valid
/// recursion targets (see [`is_recursion_target`]) and not final. For
/// non-array values, appends the value if it is a valid target and not
/// final. Only one level of array is traversed; nested arrays are treated
/// as single values.
///
/// Returns an error if a non-final, non-RecordId value is encountered,
/// since recursion is only valid for record graph traversal.
pub(crate) fn collect_discovery_targets(
	v: Value,
	out: &mut Vec<Value>,
) -> crate::exec::FlowResult<()> {
	match v {
		Value::Array(arr) => {
			for inner in arr.0 {
				if is_final(&inner) {
					continue;
				}
				if !is_recursion_target(&inner) {
					return Err(crate::exec::Error::InvalidRecursionTarget {
						value: inner.to_sql(),
					}
					.into());
				}
				out.push(inner);
			}
		}
		v if is_final(&v) => {}
		v if is_recursion_target(&v) => {
			out.push(v);
		}
		v => {
			return Err(crate::exec::Error::InvalidRecursionTarget {
				value: v.to_sql(),
			}
			.into());
		}
	}
	Ok(())
}

/// Discover recursion targets via the body operator for a single input value.
///
/// Executes the fused lookup chain and collects all valid `RecordId` targets
/// from the resulting stream. Returns a boxed future for use with
/// [`eval_buffered`].
pub(crate) fn discover_body_targets<'a>(
	body_op: &'a Arc<dyn ExecOperator>,
	body_ctx: ExecutionContext,
) -> BoxFut<'a, FlowResult<Vec<Value>>> {
	Box::pin(async move {
		let mut discovered = Vec::new();
		let mut body_stream = body_op.execute(&body_ctx)?;
		while let Some(batch_result) = body_stream.next().await {
			let batch = batch_result?;
			for v in batch.into_values() {
				collect_discovery_targets(v, &mut discovered)?;
			}
		}
		Ok(discovered)
	})
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use super::super::tests::{FIXTURES, SYSTEM_LIMIT, body_operator, body_path, exec_error};
	use super::*;
	use crate::exec::operators::test_util::{TestDb, val};
	use crate::expr::ControlFlow;

	// =========================================================================
	// RecursionBounds
	// =========================================================================

	#[test]
	fn an_explicit_user_bound_caps_the_iteration_and_stops_silently() {
		let bounds = RecursionBounds {
			min: 1,
			max: Some(4),
			system_limit: SYSTEM_LIMIT,
		};
		assert_eq!(bounds.cap(), 4);
		assert!(!bounds.errors_on_limit());
	}

	#[test]
	fn an_unbounded_recursion_caps_at_the_system_limit_and_errors_there() {
		let bounds = RecursionBounds {
			min: 1,
			max: None,
			system_limit: SYSTEM_LIMIT,
		};
		assert_eq!(bounds.cap(), SYSTEM_LIMIT);
		assert!(bounds.errors_on_limit());
	}

	#[test]
	fn a_user_bound_equal_to_the_system_limit_still_stops_silently() {
		// The two cases are indistinguishable once the cap is resolved, which is
		// why `max` must stay an `Option` rather than collapse to a depth.
		let bounds = RecursionBounds {
			min: 1,
			max: Some(SYSTEM_LIMIT),
			system_limit: SYSTEM_LIMIT,
		};
		assert_eq!(bounds.cap(), SYSTEM_LIMIT);
		assert!(!bounds.errors_on_limit());
	}

	#[test]
	fn a_user_bound_above_the_system_limit_is_clamped_but_still_silent() {
		// Defence in depth: the parser rejects such a bound, and a clamped bound
		// must not turn into a limit error.
		let bounds = RecursionBounds {
			min: 1,
			max: Some(1_000),
			system_limit: 8,
		};
		assert_eq!(bounds.cap(), 8);
		assert!(!bounds.errors_on_limit());
	}

	// =========================================================================
	// is_recursion_target
	// =========================================================================

	#[tokio::test]
	async fn only_record_ids_and_arrays_holding_one_are_recursion_targets() {
		assert!(is_recursion_target(&val("link:a").await));
		assert!(is_recursion_target(&val("[link:a]").await));
		// `any`, not `all`: one record id in the array is enough.
		assert!(is_recursion_target(&val("['x', link:a]").await));
		// The check recurses through nesting.
		assert!(is_recursion_target(&val("[[link:a]]").await));

		assert!(!is_recursion_target(&val("[]").await));
		assert!(!is_recursion_target(&val("['a', 'b']").await));
		assert!(!is_recursion_target(&Value::None));
		assert!(!is_recursion_target(&Value::Null));
		assert!(!is_recursion_target(&val("'link:a'").await));
		assert!(!is_recursion_target(&val("42").await));
		assert!(!is_recursion_target(&val("{ id: 1 }").await));
		assert!(!is_recursion_target(&val("u'019a1b2c-0000-7000-8000-000000000000'").await));
	}

	// =========================================================================
	// eval_buffered / eval_buffered_all
	// =========================================================================

	/// A future that resolves to `value` after `delay`, so a test can make
	/// completion order differ from input order.
	fn delayed(delay: u64, value: i64) -> BoxFut<'static, FlowResult<Value>> {
		Box::pin(async move {
			common::time::sleep(Duration::from_millis(delay)).await;
			Ok(Value::from(value))
		})
	}

	fn failing(message: &'static str) -> BoxFut<'static, FlowResult<Value>> {
		Box::pin(async move { Err(ControlFlow::Err(anyhow::anyhow!(message))) })
	}

	#[tokio::test]
	async fn eval_buffered_keeps_input_order_when_completion_order_differs() {
		// `path` and `shortest` zip these results back onto their inputs, so the
		// buffering must stay ordered.
		let results = eval_buffered(vec![delayed(30, 1), delayed(1, 2), delayed(15, 3)])
			.await
			.expect("no future fails");
		assert_eq!(results, vec![Value::from(1), Value::from(2), Value::from(3)]);
	}

	#[tokio::test]
	async fn eval_buffered_runs_a_single_future_and_an_empty_batch_sequentially() {
		assert!(
			eval_buffered(Vec::<BoxFut<'static, FlowResult<Value>>>::new())
				.await
				.expect("empty batch")
				.is_empty()
		);
		assert_eq!(
			eval_buffered(vec![delayed(0, 9)]).await.expect("one future"),
			vec![Value::from(9)]
		);
	}

	#[tokio::test]
	async fn eval_buffered_short_circuits_on_the_first_error() {
		let err = eval_buffered(vec![delayed(0, 1), failing("boom"), delayed(0, 3)])
			.await
			.expect_err("the failing future aborts the batch");
		match err {
			ControlFlow::Err(e) => assert_eq!(e.to_string(), "boom"),
			other => panic!("expected an error, got {other}"),
		}
	}

	#[tokio::test]
	async fn eval_buffered_all_returns_every_result_including_the_failures() {
		// The repeat strategy needs each result individually so it can treat a
		// path-elimination signal as non-fatal.
		let results = eval_buffered_all(vec![delayed(0, 1), failing("boom"), delayed(0, 3)]).await;
		assert_eq!(results.len(), 3);
		assert_eq!(results[0].as_ref().expect("first ok"), &Value::from(1));
		assert!(results[1].is_err());
		assert_eq!(results[2].as_ref().expect("third ok"), &Value::from(3));
	}

	#[tokio::test]
	async fn eval_buffered_all_handles_a_single_future_sequentially() {
		let results = eval_buffered_all(vec![failing("boom")]).await;
		assert_eq!(results.len(), 1);
		assert!(results[0].is_err());
	}

	// =========================================================================
	// collect_discovery_targets
	// =========================================================================

	#[tokio::test]
	async fn discovery_targets_from_an_array_skip_dead_ends_and_keep_record_ids() {
		let mut out = Vec::new();
		collect_discovery_targets(val("[link:a, NONE, link:b, NULL, []]").await, &mut out)
			.expect("record ids and dead ends only");
		assert_eq!(out, vec![val("link:a").await, val("link:b").await]);
	}

	#[tokio::test]
	async fn only_one_level_of_array_is_unwrapped_so_a_nested_array_stays_one_target() {
		let mut out = Vec::new();
		collect_discovery_targets(val("[[link:a, link:b]]").await, &mut out)
			.expect("a nested array of record ids is itself a target");
		assert_eq!(out, vec![val("[link:a, link:b]").await]);
	}

	#[tokio::test]
	async fn a_scalar_target_is_kept_a_scalar_dead_end_is_dropped() {
		let mut out = Vec::new();
		collect_discovery_targets(val("link:a").await, &mut out).expect("a record id is a target");
		collect_discovery_targets(Value::None, &mut out).expect("NONE is a dead end");
		collect_discovery_targets(Value::Null, &mut out).expect("NULL is a dead end");
		collect_discovery_targets(val("[]").await, &mut out).expect("an empty array is a dead end");
		assert_eq!(out, vec![val("link:a").await]);
	}

	#[tokio::test]
	async fn a_non_record_value_is_rejected_whether_bare_or_inside_an_array() {
		let mut out = Vec::new();
		let bare = collect_discovery_targets(val("'sample'").await, &mut out)
			.expect_err("a string is not a record graph target");
		assert!(matches!(exec_error(bare), crate::exec::Error::InvalidRecursionTarget { .. }));

		let nested = collect_discovery_targets(val("[link:a, 'sample']").await, &mut out)
			.expect_err("a string element is not a record graph target");
		assert!(matches!(exec_error(nested), crate::exec::Error::InvalidRecursionTarget { .. }));
	}

	// =========================================================================
	// discover_body_targets
	// =========================================================================

	#[tokio::test]
	async fn the_body_operator_discovers_the_targets_of_the_value_it_is_bound_to() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		let path = body_path("node:a->step->node", &ctx).await;
		let body = body_operator(&path).expect("a fused graph body exposes one operator chain");

		let from_a = discover_body_targets(&body, ctx.with_current_value(val("node:a").await))
			.await
			.unwrap();
		assert_eq!(from_a, vec![val("node:b").await]);

		// A node with no outgoing edge discovers nothing rather than failing.
		let from_d = discover_body_targets(&body, ctx.with_current_value(val("node:d").await))
			.await
			.unwrap();
		assert!(from_d.is_empty());
	}
}
