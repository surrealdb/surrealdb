mod aggregate;
mod bind;
mod compute;
mod current_value_source;
mod distinct;
mod explain;
mod expr;
pub(crate) mod fetch;
mod filter;
mod foreach;
mod graph;
mod info;
mod join;
mod knn_topk;
mod layer;
pub(crate) mod legacy_bridge;
mod let_plan;
pub(crate) mod limit;
mod mutate;
mod permission_filter;
mod project;
mod project_value;
pub(crate) mod recursion;
mod r#return;
pub(crate) mod scan;
mod sequence;
mod sleep;
mod sort;
mod source_expr;
mod split;
mod timeout;
mod union;
mod unwrap_exactly_one;
mod version_scope;

#[cfg(test)]
pub(crate) mod test_util;

pub use aggregate::{
	Aggregate, AggregateExprInfo, AggregateField, ExtractedAggregate, aggregate_field_name,
	group_key_field_name,
};
pub use bind::Bind;
pub use compute::Compute;
pub use current_value_source::CurrentValueSource;
pub use distinct::Distinct;
pub use explain::{AnalyzePlan, ExplainPlan};
pub use expr::ExprPlan;
pub use fetch::Fetch;
pub(crate) use fetch::FetchStep;
pub use filter::Filter;
pub use foreach::ForeachPlan;
use futures::StreamExt;
pub use graph::{
	DistinctEdges, EdgeBinding, EndpointBind, EndpointField, Expand, ExpandDir, PathExpand,
	PathMode, ShortestPathExpand, ShortestSelector,
};
pub use info::{
	DatabaseInfoPlan, IndexInfoPlan, NamespaceInfoPlan, RootInfoPlan, TableInfoPlan, UserInfoPlan,
};
pub use join::{HashJoin, JoinFrame, JoinType, LookupJoin, RecordBatchFetch};
pub use knn_topk::KnnTopK;
pub(crate) use knn_topk::{KnnTopKHeap, KnnVectorSource, extract_vector};
pub(crate) use layer::LayerColumn;
pub use layer::ProjectLayer;
pub use let_plan::LetPlan;
pub use limit::Limit;
pub use mutate::{DeleteBinding, DrainSink, InsertGraph, SingleRowScan, UpdateBinding};
pub(crate) use mutate::{InsertEdgeOp, InsertNodeOp};
pub use permission_filter::PermissionFilterExec;
pub(crate) use project::omit_path_is_expressible;
pub use project::{FieldSelection, Project, Projection, SelectProject};
pub use project_value::ProjectValue;
pub use recursion::RecursionOp;
pub use r#return::ReturnPlan;
pub(crate) use scan::{BitmapNode, KnnPrefilter};
pub use scan::{
	BitmapResolve, DynamicScan, EdgeTableSpec, EmptyScan, FullTextScan, GraphEdgeScan,
	GraphScanOutput, IndexScan, KnnScan, RecordIdScan, ReferenceScan, ReferenceScanOutput,
	TableScan, UnionIndexScan,
};
// Scan operators (storage I/O)
pub use scan::{CountOutput, CountScan};
pub use sequence::SequencePlan;
pub use sleep::SleepPlan;
#[cfg(all(storage, not(target_family = "wasm")))]
pub use sort::{ExternalSort, ExternalSortByKey};
pub(crate) use sort::{OrderByField, SortDirection, SortKey, compare_values};
pub use sort::{RandomShuffle, Sort, SortByKey, SortTopK, SortTopKByKey};
pub use source_expr::SourceExpr;
pub use split::Split;
pub use timeout::Timeout;
pub use union::Union;
pub use unwrap_exactly_one::UnwrapExactlyOne;
pub use version_scope::VersionScope;

use crate::err::EngineError;
use crate::exe::FlowResultExt as _;
use crate::exec::{
	EvalContext, ExecutionContext, FlowResult, OutputShape, PhysicalExpr, ValueBatchStream,
};
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
///   [`ScalarSubquery`](crate::exec::physical_expr::subquery::ScalarSubquery) already applies to a
///   subquery expression.
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

/// Evaluate a statement's `FROM` source expression into the value to iterate.
///
/// The source is a value position, not a statement body, so a `RETURN` raised
/// while evaluating it names that value and must not escape as the enclosing
/// statement's own result: `SELECT * FROM (RETURN t:1)` iterates the record
/// `t:1` and answers with a row, exactly as `SELECT * FROM t:1` does. The
/// signal can come from the expression itself, from a block written there, or
/// from anywhere inside it — a function argument, an array element.
///
/// `BREAK` and `CONTINUE` have no enclosing loop to act on in this position and
/// are rejected as invalid control flow rather than reaching one further out.
///
/// This is where the boundary is drawn for a source expression the planner
/// compiled as one. A record-id source draws its own, per key, because its key
/// is the only part of it that is an expression.
#[inline]
pub(crate) async fn evaluate_from_source(
	expr: &dyn PhysicalExpr,
	ctx: EvalContext<'_>,
) -> FlowResult<Value> {
	Ok(expr.evaluate(ctx).await.catch_return()?)
}

/// Cancellation poll shared by the streaming operators' hot loops.
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
