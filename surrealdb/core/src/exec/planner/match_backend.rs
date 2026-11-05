//! The node builders GQL `MATCH` planning folds over.
//!
//! [`MatchFold`](super::match_plan::MatchFold) decides the shape of a
//! binding-table plan: which anchor each pattern takes, where each conjunct is
//! placed, how clauses join. It builds that shape through a
//! [`BindingBackend`], which owns what a node of the shape is. The IR binder's
//! logical backend in `ir/build/match_fold.rs` implements it. A builder that
//! cannot express a node declines with [`Planning::Decline`], which the fold
//! passes up untouched.
//!
//! Every scalar a builder receives is a [`LogicalExpr`] scoped to the binding
//! row, and every binding is named: the builders never see a [`MatchPlan`]
//! binding id.

use surrealdb_strand::TableName;

use crate::err::{EngineError, Error};
use crate::exec::operators::{
	EdgeBinding, EndpointField, ExpandDir, JoinType, PathMode, ShortestSelector,
};
use crate::exec::vm::NotCovered;
use crate::expr::logical::LogicalExpr;
use crate::expr::match_plan::{MatchOutput, MatchPlan, MutationStage};

/// Why a fold stopped.
pub(crate) enum Planning {
	/// The plan is wrong or the planner refuses it: the error reaches the user.
	Fail(Error),
	/// The backend has no node for a shape the plan holds.
	Decline(NotCovered),
}

impl From<Error> for Planning {
	fn from(error: Error) -> Self {
		Planning::Fail(error)
	}
}

impl From<EngineError> for Planning {
	fn from(error: EngineError) -> Self {
		Planning::Fail(error.into())
	}
}

/// A single-hop expansion off `source`.
pub(crate) struct ExpandSpec {
	pub source: String,
	pub dir: ExpandDir,
	pub edge_tables: Vec<TableName>,
	pub edge_binding: EdgeBinding,
	pub target_binding: String,
	pub target_label: Option<TableName>,
	pub predicate: Option<LogicalExpr>,
	pub optional: bool,
}

/// A quantified expansion off `source`: every path, or a bounded search.
pub(crate) struct PathSpec {
	pub source: String,
	pub dir: ExpandDir,
	pub edge_tables: Vec<TableName>,
	pub min: u32,
	pub max: Option<u32>,
	pub target_binding: String,
	pub target_label: Option<TableName>,
	pub group_binding: Option<String>,
	pub path_binding: Option<String>,
	pub mode: PathMode,
	/// `None` enumerates every path.
	pub selector: Option<ShortestSelector>,
	pub reversed: bool,
}

/// A join of two binding tables on the bindings they share.
pub(crate) struct JoinSpec {
	pub keys: Vec<String>,
	pub join_type: JoinType,
	pub null_template: Vec<String>,
	pub residual: Option<LogicalExpr>,
}

/// An edge-anchored endpoint: the node at an edge's `in` or `out`.
pub(crate) struct EndpointSpec {
	pub edge: String,
	pub field: EndpointField,
	pub name: String,
	pub label: Option<TableName>,
}

/// What the fold builds binding-table plans from.
///
/// A `Node` is a plan whose rows are binding rows: objects keyed by binding
/// name. Builders take the node they extend by value and return the node that
/// extends it.
pub(crate) trait BindingBackend {
	type Node: Clone;

	/// A scan over the records of `label`, filtered by `pushed` (scoped to the
	/// record row), whose rows are the records themselves.
	async fn scan(
		&self,
		label: TableName,
		pushed: Option<LogicalExpr>,
	) -> Result<Self::Node, Planning>;

	/// Rows `input` produces, each wrapped as `{ name: row }`.
	fn bind(&self, input: Self::Node, name: String) -> Result<Self::Node, Planning>;

	/// The node at an edge's endpoint, bound beside the edge.
	fn endpoint(&self, input: Self::Node, spec: EndpointSpec) -> Result<Self::Node, Planning>;

	/// One row with no bindings.
	fn single_row(&self) -> Result<Self::Node, Planning>;

	async fn filter(
		&self,
		input: Self::Node,
		predicate: LogicalExpr,
	) -> Result<Self::Node, Planning>;

	async fn expand(&self, input: Self::Node, spec: ExpandSpec) -> Result<Self::Node, Planning>;

	async fn path_expand(&self, input: Self::Node, spec: PathSpec) -> Result<Self::Node, Planning>;

	/// Enforces GQL DIFFERENT EDGES over the edge bindings named in `edges`: no
	/// two of them may bind the same edge.
	fn distinct_edges(&self, input: Self::Node, edges: Vec<String>)
	-> Result<Self::Node, Planning>;

	/// Join `probe`'s rows with `build`'s.
	async fn join(
		&self,
		build: Self::Node,
		probe: Self::Node,
		spec: JoinSpec,
	) -> Result<Self::Node, Planning>;

	/// One write stage over the rows of `input`.
	async fn mutate(
		&self,
		plan: &MatchPlan,
		input: Self::Node,
		stage: &MutationStage,
	) -> Result<Self::Node, Planning>;

	/// Run `input` to completion and answer no rows.
	fn drain(&self, input: Self::Node) -> Result<Self::Node, Planning>;

	/// The output columns folded by the grouping keys, as final rows keyed by
	/// column name.
	async fn aggregate(
		&self,
		input: Self::Node,
		output: &MatchOutput,
		group_keys: &[LogicalExpr],
	) -> Result<Self::Node, Planning>;

	/// The explicit output columns projected from the binding rows.
	async fn project(
		&self,
		input: Self::Node,
		output: &MatchOutput,
	) -> Result<Self::Node, Planning>;

	/// Only the user-projected columns of rows an aggregate produced.
	async fn drop_hidden(
		&self,
		input: Self::Node,
		output: &MatchOutput,
	) -> Result<Self::Node, Planning>;

	/// The first of each distinct row.
	fn distinct(&self, input: Self::Node) -> Result<Self::Node, Planning>;

	async fn sort(&self, input: Self::Node, output: &MatchOutput) -> Result<Self::Node, Planning>;

	async fn limit(&self, input: Self::Node, output: &MatchOutput) -> Result<Self::Node, Planning>;
}
