//! The node builders GQL `MATCH` planning folds over.
//!
//! [`MatchFold`](super::match_plan::MatchFold) decides the shape of a
//! binding-table plan: which anchor each pattern takes, where each conjunct is
//! placed, how clauses join. It builds that shape through a
//! [`BindingBackend`], which owns what a node of the shape is. The operator
//! tree is one backend ([`TreeBackend`]); the VM's physical plan is the
//! other (`vm::match_plan`). A builder that cannot express a node declines
//! with [`Planning::Decline`], which the fold passes up untouched.
//!
//! Every scalar a builder receives is a [`LogicalExpr`] scoped to the binding
//! row, and every binding is named: the builders never see a [`MatchPlan`]
//! binding id.

#[cfg(test)]
use std::sync::Arc;

use surrealdb_strand::TableName;

#[cfg(test)]
use super::Planner;
use crate::err::{EngineError, Error};
#[cfg(test)]
use crate::exec::ExecOperator;
#[cfg(test)]
use crate::exec::logical::{BatchRetry, ColumnEffects, columns_may_run_column_major};
#[cfg(test)]
use crate::exec::operators::{
	Aggregate, AggregateField, FieldSelection, Filter, Limit, OrderByField, Project, Sort,
	SortDirection,
};
#[cfg(test)]
use crate::exec::operators::{
	Bind, DeleteBinding, Distinct, DistinctEdges, DrainSink, EndpointBind, Expand, HashJoin,
	InsertEdgeOp, InsertGraph, InsertNodeOp, PathExpand, ShortestPathExpand, SingleRowScan,
	UpdateBinding,
};
use crate::exec::operators::{
	EdgeBinding, EndpointField, ExpandDir, JoinType, PathMode, ShortestSelector,
};
use crate::exec::vm::NotCovered;
use crate::expr::Function;
use crate::expr::logical::LogicalExpr;
#[cfg(test)]
use crate::expr::logical::{LogicalFilter, LogicalOp, LogicalSource, SourceRef};
#[cfg(test)]
use crate::expr::match_plan::{BindingKind, UpdateData};
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

impl Planning {
	#[cfg(test)]
	/// The error a backend that never declines raised.
	pub(crate) fn into_error(self) -> Error {
		match self {
			Planning::Fail(error) => error,
			Planning::Decline(not_covered) => EngineError::Internal(not_covered.reason).into(),
		}
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

	/// The DIFFERENT EDGES check across the edge bindings `edges`.
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

/// The operator tree as a backend.
#[cfg(test)]
pub(crate) struct TreeBackend<'a, 'ctx> {
	pub planner: &'a Planner<'ctx>,
}

#[cfg(test)]
type Op = Arc<dyn ExecOperator>;

#[cfg(test)]
impl BindingBackend for TreeBackend<'_, '_> {
	type Node = Op;

	/// The scan is a plan of its own — a `Filter` over a table `Source` —
	/// bound and compiled like any other, so the access-path selection and
	/// predicate pushdown the fold applies to a `SELECT … WHERE` apply here
	/// too, and any residual the source cannot consume stays a filter directly
	/// above it.
	async fn scan(&self, label: TableName, pushed: Option<LogicalExpr>) -> Result<Op, Planning> {
		let source = LogicalOp::Source(LogicalSource {
			source: SourceRef::Table(label),
			schema: None,
			with: None,
			for_update: false,
		});
		let op = match pushed {
			Some(predicate) => LogicalOp::Filter(LogicalFilter {
				input: Box::new(source),
				predicate,
			}),
			None => source,
		};
		let bound = self.planner.bind(op).await?;
		Ok(self.planner.plan_logical(bound).await?)
	}

	fn bind(&self, input: Op, name: String) -> Result<Op, Planning> {
		Ok(Arc::new(Bind::new(input, name)))
	}

	fn endpoint(&self, input: Op, spec: EndpointSpec) -> Result<Op, Planning> {
		Ok(Arc::new(EndpointBind::new(input, spec.edge, spec.field, spec.name, spec.label)))
	}

	fn single_row(&self) -> Result<Op, Planning> {
		Ok(Arc::new(SingleRowScan::new()))
	}

	async fn filter(&self, input: Op, predicate: LogicalExpr) -> Result<Op, Planning> {
		let predicate = self.planner.physical_expr(predicate).await?;
		Ok(Arc::new(Filter::new(input, predicate)))
	}

	async fn expand(&self, input: Op, spec: ExpandSpec) -> Result<Op, Planning> {
		let predicate = match spec.predicate {
			Some(predicate) => Some(self.planner.physical_expr(predicate).await?),
			None => None,
		};
		Ok(Arc::new(Expand::new(
			input,
			spec.source,
			spec.dir,
			spec.edge_tables,
			spec.edge_binding,
			spec.target_binding,
			spec.target_label,
			predicate,
			spec.optional,
		)))
	}

	async fn path_expand(&self, input: Op, spec: PathSpec) -> Result<Op, Planning> {
		Ok(match spec.selector {
			None => Arc::new(PathExpand::new(
				input,
				spec.source,
				spec.dir,
				spec.edge_tables,
				spec.min,
				spec.max,
				spec.target_binding,
				spec.target_label,
				spec.group_binding,
				spec.path_binding,
				spec.mode,
				spec.reversed,
			)),
			Some(selector) => Arc::new(ShortestPathExpand::new(
				input,
				spec.source,
				spec.dir,
				spec.edge_tables,
				spec.min,
				spec.max,
				spec.target_binding,
				spec.target_label,
				spec.group_binding,
				spec.path_binding,
				spec.mode,
				selector,
				spec.reversed,
			)),
		})
	}

	fn distinct_edges(&self, input: Op, edges: Vec<String>) -> Result<Op, Planning> {
		Ok(Arc::new(DistinctEdges::new(input, edges)))
	}

	async fn join(&self, build: Op, probe: Op, spec: JoinSpec) -> Result<Op, Planning> {
		let residual = match spec.residual {
			Some(residual) => Some(self.planner.physical_expr(residual).await?),
			None => None,
		};
		Ok(Arc::new(HashJoin::new(
			build,
			probe,
			spec.keys,
			spec.join_type,
			spec.null_template,
			residual,
		)))
	}

	/// The row-scoped values a stage writes are compiled here, like every other
	/// scalar, and the operator evaluates them against each binding row.
	async fn mutate(
		&self,
		plan: &MatchPlan,
		op: Op,
		stage: &MutationStage,
	) -> Result<Op, Planning> {
		let name = |id| plan.binding(id).name.clone();
		Ok(match stage {
			MutationStage::Update {
				target,
				data,
			} => {
				let name = name(*target);
				let update = match data {
					UpdateData::Set(assignments) => {
						let mut compiled = Vec::with_capacity(assignments.len());
						for (property, value) in assignments {
							compiled.push((
								property.clone(),
								self.planner.physical_expr(value.clone()).await?,
							));
						}
						UpdateBinding::set(op, name, compiled)
					}
					UpdateData::Unset(properties) => {
						UpdateBinding::unset(op, name, properties.clone())
					}
					UpdateData::Content(object) => UpdateBinding::content(
						op,
						name,
						self.planner.physical_expr(object.clone()).await?,
					),
				};
				Arc::new(update)
			}
			MutationStage::Delete {
				target,
				detach,
			} => {
				let is_edge = matches!(
					plan.binding(*target).kind,
					BindingKind::Edge | BindingKind::EdgeGroup
				);
				Arc::new(DeleteBinding::new(op, name(*target), *detach, is_edge))
			}
			MutationStage::Insert(insert) => {
				let mut nodes = Vec::with_capacity(insert.nodes.len());
				for n in &insert.nodes {
					nodes.push(InsertNodeOp {
						name: name(n.binding),
						table: n.label.clone(),
						props: self.planner.physical_expr(n.props.clone()).await?,
					});
				}
				let mut edges = Vec::with_capacity(insert.edges.len());
				for e in &insert.edges {
					edges.push(InsertEdgeOp {
						name: name(e.binding),
						table: e.label.clone(),
						from: name(e.from),
						to: name(e.to),
						props: self.planner.physical_expr(e.props.clone()).await?,
					});
				}
				Arc::new(InsertGraph::new(op, nodes, edges))
			}
		})
	}

	fn drain(&self, input: Op) -> Result<Op, Planning> {
		Ok(Arc::new(DrainSink::new(input)))
	}

	/// Folds the binding rows by the GROUP BY keys (empty keys ⇒ GROUP ALL, a
	/// single group over every row). Classifies each column (projected or
	/// hidden sort-only) three ways:
	/// - exactly a grouping key ⇒ pass it through (`is_group_key`);
	/// - contains an aggregate ⇒ [`Planner::extract_aggregate_info`] (the column carries the fold,
	///   so the helper's implicit `array::group` fallback never fires);
	/// - otherwise it is determined by the grouping keys (the lowering guarantees coverage) ⇒ emit
	///   its first value per group via `fallback_expr`, which is exact because the value is
	///   constant within a group.
	async fn aggregate(
		&self,
		input: Op,
		output: &MatchOutput,
		group_keys: &[LogicalExpr],
	) -> Result<Op, Planning> {
		let planner = self.planner;
		// Physical group-key expressions (empty for GROUP ALL — the operator's
		// `group_by_exprs.is_empty()` check then takes the single-group path).
		let mut group_by_exprs = Vec::with_capacity(group_keys.len());
		for key in group_keys {
			group_by_exprs.push(planner.physical_expr(key.clone()).await?);
		}

		let mut aggregates = Vec::with_capacity(output.columns.len());
		for column in output.columns.iter() {
			if let Some(idx) = group_keys.iter().position(|k| *k == column.expr) {
				// A grouping key column: passed through from the group key vector.
				aggregates.push(AggregateField::new(
					column.name.clone(),
					true,
					Some(idx),
					None,
					None,
				));
			} else if expr_has_aggregate(planner.function_registry(), &column.expr) {
				let (info, fallback) =
					planner.extract_aggregate_info(column.expr.clone(), group_keys).await?;
				aggregates.push(AggregateField::new(
					column.name.clone(),
					false,
					None,
					info,
					fallback,
				));
			} else {
				// Determined by the grouping keys (constant within each group): emit
				// the first value seen. The lowering guarantees coverage.
				let fallback = planner.physical_expr(column.expr.clone()).await?;
				aggregates.push(AggregateField::new(
					column.name.clone(),
					false,
					None,
					None,
					Some(fallback),
				));
			}
		}

		// Whether the operator may evaluate a batch one column at a time
		// rather than one row at a time; see `Aggregate::column_major`. An
		// aggregate evaluates a column again on any failure, so nothing a
		// repetition could expose may be in reach of one.
		let column_effects: Vec<ColumnEffects> = group_keys
			.iter()
			.chain(output.columns.iter().map(|column| &column.expr))
			.map(|expr| ColumnEffects::of(expr, planner.function_registry()))
			.collect();
		let column_major = columns_may_run_column_major(&column_effects, BatchRetry::OnFailure);

		// The logical keys double as the EXPLAIN display, length-aligned with the
		// physical keys so the operator renders `GROUP ALL` only when there are
		// genuinely no keys.
		let mut aggregate = Aggregate::new(input, group_keys.to_vec(), group_by_exprs, aggregates);
		if column_major {
			aggregate = aggregate.with_column_major();
		}
		Ok(Arc::new(aggregate))
	}

	/// Column names are final (the lowering applied naming + dup-check), so
	/// each column is a flat `FieldSelection` aliased to its name.
	async fn project(&self, input: Op, output: &MatchOutput) -> Result<Op, Planning> {
		let planner = self.planner;
		// Judged from the columns as written; see `Project::column_major`.
		// A projection passes a failed column on as the batch's, so it
		// repeats nothing.
		let column_effects: Vec<ColumnEffects> = output
			.columns
			.iter()
			.map(|column| ColumnEffects::of(&column.expr, planner.function_registry()))
			.collect();
		let column_major = columns_may_run_column_major(&column_effects, BatchRetry::None);

		let mut fields = Vec::with_capacity(output.columns.len());
		for column in output.columns.iter() {
			let expr = planner.physical_expr(column.expr.clone()).await?;
			fields.push(FieldSelection::new(&column.name, expr));
		}
		let mut project = Project::new(input, fields, Vec::new(), false);
		if column_major {
			project = project.with_column_major();
		}
		Ok(Arc::new(project))
	}

	/// Every field is a bare read of a name the `Aggregate` already produced,
	/// so none of them has an effect to reorder.
	async fn drop_hidden(&self, input: Op, output: &MatchOutput) -> Result<Op, Planning> {
		let mut fields = Vec::new();
		for column in output.columns.iter().filter(|c| !c.hidden) {
			let expr =
				self.planner.physical_expr(LogicalExpr::Field(column.name.as_str().into())).await?;
			fields.push(FieldSelection::new(&column.name, expr));
		}
		Ok(Arc::new(Project::new(input, fields, Vec::new(), false).with_column_major()))
	}

	fn distinct(&self, input: Op) -> Result<Op, Planning> {
		Ok(Arc::new(Distinct::new(input)))
	}

	async fn sort(&self, input: Op, output: &MatchOutput) -> Result<Op, Planning> {
		let planner = self.planner;
		// `Sort` evaluates its keys itself, so the shape of that evaluation is
		// judged here, as it is for the projection and the aggregate; see
		// `Sort::column_major`. A sort passes a failed key column on as the
		// batch's, so it repeats nothing.
		let key_effects: Vec<ColumnEffects> = output
			.order
			.iter()
			.map(|order| ColumnEffects::of(&order.expr, planner.function_registry()))
			.collect();
		let column_major = columns_may_run_column_major(&key_effects, BatchRetry::None);
		let mut order_by = Vec::with_capacity(output.order.len());
		for order in output.order.iter() {
			let expr = planner.physical_expr(order.expr.clone()).await?;
			order_by.push(OrderByField {
				expr,
				direction: if order.ascending {
					SortDirection::Asc
				} else {
					SortDirection::Desc
				},
				collate: false,
				numeric: false,
			});
		}
		let mut sort = Sort::new(input, order_by);
		if column_major {
			sort = sort.with_column_major();
		}
		Ok(Arc::new(sort))
	}

	async fn limit(&self, input: Op, output: &MatchOutput) -> Result<Op, Planning> {
		let limit = match output.limit.as_ref() {
			Some(e) => Some(self.planner.physical_expr(e.clone()).await?),
			None => None,
		};
		let offset = match output.skip.as_ref() {
			Some(e) => Some(self.planner.physical_expr(e.clone()).await?),
			None => None,
		};
		Ok(Arc::new(Limit::new(input, limit, offset)))
	}
}

/// Whether an expression contains a call to a registered aggregate function.
/// Used to classify an aggregating query's columns: a non-key column with an
/// aggregate folds via `extract_aggregate_info`, one without is a value
/// determined by the grouping keys (first-value).
pub(crate) fn expr_has_aggregate(
	registry: &crate::exec::function::FunctionRegistry,
	expr: &LogicalExpr,
) -> bool {
	expr.any(|e| {
		matches!(
			e,
			LogicalExpr::Call(call)
				if matches!(&call.receiver, Function::Normal(name) if registry.get_aggregate(name.as_str()).is_some())
		)
	})
}
