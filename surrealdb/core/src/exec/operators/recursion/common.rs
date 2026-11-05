//! Shared helpers for recursion strategies: RecordId enforcement and concurrent evaluation.

use futures::{StreamExt, TryStreamExt, stream};

use crate::exec::{BoxFut, FlowResult, trampoline};
use crate::val::Value;

/// Maximum number of concurrent path evaluations per depth level.
/// Limits parallelism to avoid overwhelming the KV layer while still
/// allowing progress when individual evaluations block on I/O.
pub(crate) const RECURSION_CONCURRENCY: usize = 16;

/// What a strategy evaluates at each depth: the recursion's body over the
/// value that depth steps from, answering the value it reaches. An
/// evaluation holds no state a concurrent evaluation of another value
/// reads, so a strategy may run a level's evaluations side by side.
pub(crate) trait DepthBody: Sync {
	fn eval<'a>(&'a self, value: &'a Value) -> BoxFut<'a, FlowResult<Value>>;
}

/// The `{min..max}` range of a recursive idiom (`.{min..max}`) plus the resolved
/// system `idiom_recursion_limit`, shared by every recursion strategy and
/// constructed once by the dispatching `RecursionOp`.
///
/// `max` carries the user-specified upper bound, or `None` when the user gave no
/// bound. That distinction drives limit handling: an explicit bound stops the
/// recursion silently at that depth, whereas an unbounded recursion is capped at
/// the system limit and *exhausting* that cap is a hard error. The two cases are otherwise
/// indistinguishable once the cap is resolved (a user bound equal to the system limit is legal),
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
	if let Value::RecordId(_) = value {
		true
	} else if let Value::Array(arr) = value {
		arr.iter().any(is_recursion_target)
	} else {
		false
	}
}

/// Evaluate a batch of futures with bounded concurrency.
///
/// When fewer than 2 futures are provided, runs them sequentially to avoid
/// stream combinator overhead. Otherwise, uses `buffered(RECURSION_CONCURRENCY)`
/// to poll up to N futures concurrently -- when one blocks on I/O, others
/// make progress. Polled under a statement's driver the futures run one at a
/// time, as [`crate::exec::fan_out::evaluate_each`] runs its rows there.
///
/// Short-circuits on the first error via `try_collect`.
pub(crate) async fn eval_buffered<'a, T: 'a>(
	futures: Vec<BoxFut<'a, FlowResult<T>>>,
) -> FlowResult<Vec<T>> {
	if futures.len() < 2 || trampoline::under_driver() {
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
	if futures.len() < 2 || trampoline::under_driver() {
		let mut results = Vec::with_capacity(futures.len());
		for fut in futures {
			results.push(fut.await);
		}
		results
	} else {
		stream::iter(futures).buffered(RECURSION_CONCURRENCY).collect().await
	}
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use super::super::tests::SYSTEM_LIMIT;
	use super::*;
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
		let ControlFlow::Err(e) = err else {
			panic!("expected an error, got {err}");
		};
		assert_eq!(e.to_string(), "boom");
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

	// =========================================================================
	// discover_body_targets
	// =========================================================================
}
