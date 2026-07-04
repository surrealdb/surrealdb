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
mod ifelse;
mod info;
mod join;
mod knn_topk;
mod let_plan;
mod limit;
// The GQL mutation operators are constructed only by the gql-gated planner
// and depend on the (also gql-gated) `expr::match_plan` IR, so the module is
// compiled only with the feature on.
#[cfg(feature = "gql")]
mod mutate;
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
};
// GQL v2 MATCH operators — constructed only by the gql-gated planner,
// so these re-exports are unused when the feature is off (suppress there only;
// the operators stay compiled and available for future language-neutral reuse).
#[cfg_attr(not(feature = "gql"), allow(unused_imports))]
pub use bind::Bind;
pub use compute::Compute;
pub use current_value_source::CurrentValueSource;
#[cfg_attr(not(feature = "gql"), allow(unused_imports))]
pub use distinct::Distinct;
pub use explain::{AnalyzePlan, ExplainPlan};
pub use expr::ExprPlan;
pub use fetch::Fetch;
pub(crate) use fetch::FetchStep;
pub use filter::Filter;
pub use foreach::ForeachPlan;
#[cfg_attr(not(feature = "gql"), allow(unused_imports))]
pub use graph::{
	DistinctEdges, EdgeBinding, EndpointBind, EndpointField, Expand, ExpandDir, PathExpand,
	PathMode, ShortestPathExpand, ShortestSelector,
};
pub use ifelse::IfElsePlan;
pub use info::{
	DatabaseInfoPlan, IndexInfoPlan, NamespaceInfoPlan, RootInfoPlan, TableInfoPlan, UserInfoPlan,
};
#[cfg_attr(not(feature = "gql"), allow(unused_imports))]
pub use join::{HashJoin, JoinType};
pub use knn_topk::KnnTopK;
pub use let_plan::LetPlan;
pub use limit::Limit;
#[cfg(feature = "gql")]
pub use mutate::{DeleteBinding, DrainSink, InsertGraph, SingleRowScan, UpdateBinding};
#[cfg(feature = "gql")]
pub(crate) use mutate::{InsertEdgeOp, InsertNodeOp};
pub use project::{FieldSelection, Project, Projection, SelectProject};
pub use project_value::ProjectValue;
pub use recursion::RecursionOp;
pub use r#return::ReturnPlan;
// Scan operators (storage I/O)
pub use scan::CountScan;
pub use scan::{
	DynamicScan, EdgeTableSpec, EmptyScan, FullTextScan, GraphEdgeScan, GraphScanOutput, IndexScan,
	KnnScan, RecordIdScan, ReferenceScan, ReferenceScanOutput, TableScan, UnionIndexScan,
};
pub use sequence::SequencePlan;
pub use sleep::SleepPlan;
#[cfg(all(storage, not(target_family = "wasm")))]
pub use sort::{ExternalSort, ExternalSortByKey};
pub use sort::{
	OrderByField, RandomShuffle, Sort, SortByKey, SortDirection, SortKey, SortTopK, SortTopKByKey,
	compare_values,
};
pub use source_expr::SourceExpr;
pub use split::Split;
pub use timeout::Timeout;
pub use union::Union;
pub use unwrap_exactly_one::UnwrapExactlyOne;
pub use version_scope::VersionScope;

use crate::exec::{ExecutionContext, FlowResult};

// `check_cancelled` / `gql_output_rows_exceeded` are used only by the GQL v2
// MATCH operators, which are constructed only by the gql-gated planner
// (`Expr::Match` is `#[cfg(feature = "gql")]`), so they are dead code when
// the feature is off — suppress the lint there only (the `cfg_attr` on each fn),
// keeping dead-code detection active in the default (gql-on) build. Matches
// the per-operator-module treatment.

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
#[cfg_attr(not(feature = "gql"), allow(dead_code))]
pub(crate) fn check_cancelled(ctx: &ExecutionContext) -> FlowResult<()> {
	if ctx.cancellation().is_cancelled() {
		return Err(crate::expr::ControlFlow::Err(anyhow::anyhow!(
			crate::err::Error::QueryCancelled
		)));
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
#[cfg_attr(not(feature = "gql"), allow(dead_code))]
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
#[cfg_attr(not(feature = "gql"), allow(dead_code))]
pub(crate) fn gql_output_rows_exceeded(max_rows: usize) -> crate::expr::ControlFlow {
	crate::expr::ControlFlow::Err(anyhow::anyhow!(crate::err::Error::InvalidStatement(format!(
		"GQL MATCH fan-out exceeded the maximum of {max_rows} output rows \
		 (configurable via SURREAL_GQL_MAX_OUTPUT_ROWS)"
	))))
}
