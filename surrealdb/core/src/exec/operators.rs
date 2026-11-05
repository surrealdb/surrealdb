mod aggregate;
pub(crate) mod distinct;
pub(crate) mod fetch;
pub(crate) mod graph;
pub(crate) mod join;
mod knn_topk;
mod layer;
pub(crate) mod limit;
pub(crate) mod link_window;
pub(crate) mod mutate;
mod permission_filter;
mod project;
pub(crate) mod recursion;
pub(crate) mod scan;
mod sort;
mod split;
pub(crate) mod write;

#[cfg(test)]
pub(crate) mod test_util;

pub use aggregate::{AggregateId, aggregate_field_name, group_key_field_name};
pub(crate) use aggregate::{
	aggregate_field_key, first_value_needed, group_key_field_key, set_nested_value,
};
pub(crate) use distinct::SeenSet;
use futures::StreamExt;
pub use graph::{EdgeBinding, EndpointField, ExpandDir, PathMode, ShortestSelector};
pub use join::JoinType;
pub(crate) use join::hash_join::{
	BuildTable, key_has_regex, keys_equal, with_alias, with_alias_owned,
};
pub(crate) use knn_topk::{KnnTopKHeap, extract_vector};
pub(crate) use layer::record_of;
pub(crate) use permission_filter::permission_frame;
pub(crate) use project::projection_writes;
// Scan operators (storage I/O)
pub use scan::CountOutput;
pub(crate) use sort::{SortDirection, compare_values};
pub(crate) use split::{SplitTarget, split_row, split_target};

use crate::err::EngineError;
use crate::exec::{ExecutionContext, FlowResult, OutputShape, ValueBatchStream};
use crate::val::Value;

/// Collect values from a stream into the single value an expression evaluates
/// to, shaped by the plan's declared [`OutputShape`] rather than how many rows
/// it happened to produce.
///
/// - `OutputShape::Scalar` (e.g. `SELECT ONLY`, or any non-query expression) → the one emitted
///   value, or `Value::None` if the plan emitted none.
/// - `OutputShape::Rows` (e.g. an ordinary `SELECT`) → always `Value::Array`, regardless of row
///   count. A caller using the result as a `FOR` range, or any other array-shaped context, must see
///   `[]` for zero rows and `[x]` for one — never `NONE` or the bare element — the same contract
///   [`ScalarSubquery`](crate::exec::vm::subquery::ScalarSubquery) applies to a subquery
///   expression.
///
/// Propagates control flow signals (BREAK, CONTINUE, RETURN, errors).
pub(crate) async fn collect_shaped_value(
	stream: ValueBatchStream,
	shape: OutputShape,
) -> FlowResult<Value> {
	let mut values = collect_stream(stream).await?;
	if shape.is_scalar() {
		Ok(values.pop().unwrap_or(Value::None))
	} else {
		Ok(Value::Array(crate::val::Array(values)))
	}
}

/// Collect all values from a stream into a `Vec`.
///
/// Propagates control flow signals directly.
pub(crate) async fn collect_stream(mut stream: ValueBatchStream) -> FlowResult<Vec<Value>> {
	let mut results = Vec::new();
	while let Some(batch_result) = stream.next().await {
		results.extend(batch_result?.into_values());
	}
	Ok(results)
}

/// Cancellation poll shared by the operators' hot loops.
///
/// Returns `Err(ControlFlow::Err(QueryCancelled))` when the query has been
/// cancelled, matching the canonical scan idiom (`scan/table.rs`). Cheap (one
/// atomic load), so it is safe to call per batch and, in the graph operators,
/// per inner cursor batch / per DFS step. The streaming buffer/monitor wrappers
/// do not inject cancellation, so every operator that does heavy work without
/// pulling a fresh upstream batch (HashJoin build/probe, PathExpand's DFS,
/// Expand's adjacency scan) must poll this itself or it cannot be interrupted.
#[inline]
pub(crate) fn check_cancelled(ctx: &ExecutionContext) -> FlowResult<()> {
	if ctx.cancellation().is_cancelled() {
		return Err(crate::expr::ControlFlow::Err(anyhow::anyhow!(EngineError::QueryCancelled)));
	}
	Ok(())
}

/// The record id of a binding slot, the way a GQL binding row encodes it: the
/// slot holds either the full node/edge record object (so its `id` field is the
/// record id) or, defensively, a bare [`crate::val::RecordId`]. Anything else (a
/// null/none/missing binding, or a non-record value) yields `None`. Shared by the
/// graph traversal operators (`PathExpand`'s `source_record_id`) and the mutation
/// operators (`mutate.rs`), so the single binding-row id convention lives in one
/// place.
pub(crate) fn binding_record_id(
	row: &crate::val::Value,
	name: &str,
) -> Option<crate::val::RecordId> {
	let crate::val::Value::Object(obj) = row else {
		return None;
	};
	match obj.get(name) {
		Some(crate::val::Value::Object(node)) => match node.get("id") {
			Some(crate::val::Value::RecordId(rid)) => Some(rid.clone()),
			_ => None,
		},
		Some(crate::val::Value::RecordId(rid)) => Some(rid.clone()),
		_ => None,
	}
}

/// The `SURREAL_GQL_MAX_OUTPUT_ROWS` guard error, naming the knob. Shared by the
/// GQL fan-out operators (`HashJoin`, `Expand`) when their cumulative emitted-row
/// count exceeds the configured ceiling. Names no user data.
pub(crate) fn gql_output_rows_exceeded(max_rows: usize) -> crate::expr::ControlFlow {
	crate::expr::ControlFlow::Err(anyhow::anyhow!(crate::exec::Error::InvalidStatement(format!(
		"GQL MATCH fan-out exceeded the maximum of {max_rows} output rows \
		 (configurable via SURREAL_GQL_MAX_OUTPUT_ROWS)"
	))))
}
