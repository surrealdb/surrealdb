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
					return Err(crate::err::Error::InvalidRecursionTarget {
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
			return Err(crate::err::Error::InvalidRecursionTarget {
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
			for v in batch.values {
				collect_discovery_targets(v, &mut discovered)?;
			}
		}
		Ok(discovered)
	})
}
