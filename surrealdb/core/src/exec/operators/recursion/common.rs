//! Shared helpers for recursion strategies: RecordId enforcement and concurrent evaluation.

use std::sync::Arc;

use futures::{stream, StreamExt, TryStreamExt};

use crate::exec::parts::is_final;
use crate::exec::{BoxFut, ExecOperator, ExecutionContext, FlowResult};
use crate::val::Value;

/// Maximum number of concurrent path evaluations per depth level.
/// Limits parallelism to avoid overwhelming the KV layer while still
/// allowing progress when individual evaluations block on I/O.
pub(crate) const RECURSION_CONCURRENCY: usize = 16;

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
		stream::iter(futures)
			.buffered(RECURSION_CONCURRENCY)
			.try_collect()
			.await
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
		stream::iter(futures)
			.buffered(RECURSION_CONCURRENCY)
			.collect()
			.await
	}
}

/// Extract valid recursion target values from a single batch result value.
///
/// Flattens arrays and filters to only `RecordId` values (and arrays
/// containing them) that are valid for continued graph traversal.
pub(crate) fn collect_discovery_targets(v: Value, out: &mut Vec<Value>) {
	match v {
		Value::Array(arr) => {
			for inner in arr.0 {
				if !is_final(&inner) && is_recursion_target(&inner) {
					out.push(inner);
				}
			}
		}
		v if !is_final(&v) && is_recursion_target(&v) => {
			out.push(v);
		}
		_ => {}
	}
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
				collect_discovery_targets(v, &mut discovered);
			}
		}
		Ok(discovered)
	})
}
