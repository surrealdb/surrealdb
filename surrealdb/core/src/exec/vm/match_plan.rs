//! A GQL `MATCH` plan as a physical plan of the VM.
//!
//! The binding-table fold of `exec/planner/match_plan.rs` decides the shape of
//! the plan; this backend builds its nodes: the anchors are the plans a
//! `SELECT` of the table maps to, each binding-table stage is a
//! [`Plan::Step`], and the tail is the ordinary aggregate, sort, limit and
//! projection nodes over rows that are binding rows. A shape the backend has
//! no node for is declined, and the query fails with the decline error of a
//! statement the VM does not cover.

use std::sync::Mutex;

use surrealdb_strand::TableName;

use super::binding::{
	BindingStep, EndpointStep, ExpandStep, JoinStep, MutationStep, PathStep, PlanStep,
};
use super::plan::{
	Dependency, PhysicalPlan, Plan, Projection, SortKeyRead, SortKeySpec, ValuesRead,
};
use super::{NotCovered, map, optimize};
use crate::exec::operators::mutate::{InsertEdgeOp, InsertNodeOp, UpdateOp};
use crate::exec::planner::Planner;
use crate::exec::planner::match_backend::{
	BindingBackend, EndpointSpec, ExpandSpec, JoinSpec, PathSpec, Planning,
};
use crate::expr::logical::{
	LogicalExpr, LogicalField, LogicalFields, LogicalFilter, LogicalOp, LogicalSource, OutputPath,
	SourceRef,
};
use crate::expr::match_plan::{BindingKind, MatchOutput, MatchPlan, MutationStage, UpdateData};

/// The backend: the planner the anchors are mapped with, and the catalog
/// objects the anchors read.
pub(crate) struct VmBackend<'a, 'ctx> {
	planner: &'a Planner<'ctx>,
	dependencies: Mutex<Vec<Dependency>>,
}

impl<'a, 'ctx> VmBackend<'a, 'ctx> {
	pub(crate) fn new(planner: &'a Planner<'ctx>) -> Self {
		Self {
			planner,
			dependencies: Mutex::new(Vec::new()),
		}
	}

	/// The catalog objects the plan's anchors were mapped from.
	pub(crate) fn dependencies(&self) -> Vec<Dependency> {
		self.dependencies.lock().map(|held| held.clone()).unwrap_or_default()
	}
}

fn declined(reason: &str) -> Planning {
	Planning::Decline(NotCovered::new(reason.to_owned()))
}

fn step(input: Plan, step: PlanStep) -> Plan {
	Plan::Step {
		input: Box::new(input),
		step: Box::new(step),
	}
}

/// The fields a `MatchOutput`'s columns name, each column's name its output
/// key as it is, however it is spelled.
fn column_fields(output: &MatchOutput, hidden: bool) -> LogicalFields {
	LogicalFields::Select(
		output
			.columns
			.iter()
			.filter(|column| hidden || !column.hidden)
			.map(|column| {
				LogicalField::user(column.expr.clone(), Some(OutputPath::field(&column.name)))
			})
			.collect(),
	)
}

impl BindingBackend for VmBackend<'_, '_> {
	type Node = Plan;

	async fn scan(&self, label: TableName, pushed: Option<LogicalExpr>) -> Result<Plan, Planning> {
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
		let physical = map::map(self.planner, &bound).await.map_err(Planning::Decline)?;
		// A scan the operator tree would refuse is left for it to refuse.
		if physical.refused() {
			return Err(declined("a scan the planner refuses"));
		}
		if !physical.nested.is_empty() || !physical.sources.is_empty() {
			return Err(declined("an anchor holding a statement"));
		}
		let physical = optimize::optimize(physical);
		if let Ok(mut held) = self.dependencies.lock() {
			held.extend(physical.dependencies.iter().cloned());
		}
		Ok(physical.root)
	}

	fn bind(&self, input: Plan, name: String) -> Result<Plan, Planning> {
		Ok(step(
			input,
			BindingStep::Bind {
				name,
			},
		))
	}

	fn endpoint(&self, input: Plan, spec: EndpointSpec) -> Result<Plan, Planning> {
		Ok(step(
			input,
			BindingStep::Endpoint(EndpointStep {
				edge: spec.edge,
				field: spec.field,
				name: spec.name,
				label: spec.label,
			}),
		))
	}

	fn single_row(&self) -> Result<Plan, Planning> {
		// One empty binding row: the seed of a write that reads nothing.
		Ok(Plan::Values {
			expr: LogicalExpr::Object(Vec::new()),
			read: ValuesRead::Expression {
				written: false,
			},
		})
	}

	async fn filter(&self, input: Plan, predicate: LogicalExpr) -> Result<Plan, Planning> {
		Ok(Plan::Filter {
			input: Box::new(input),
			predicate,
			once: None,
		})
	}

	async fn expand(&self, input: Plan, spec: ExpandSpec) -> Result<Plan, Planning> {
		Ok(step(
			input,
			BindingStep::Expand(ExpandStep {
				source: spec.source,
				direction: spec.dir,
				edge_tables: spec.edge_tables,
				edge_binding: spec.edge_binding,
				target_binding: spec.target_binding,
				target_label: spec.target_label,
				predicate: spec.predicate,
				optional: spec.optional,
			}),
		))
	}

	async fn path_expand(&self, input: Plan, spec: PathSpec) -> Result<Plan, Planning> {
		Ok(step(
			input,
			BindingStep::Path(PathStep {
				source: spec.source,
				direction: spec.dir,
				edge_tables: spec.edge_tables,
				min: spec.min,
				max: spec.max,
				target_binding: spec.target_binding,
				target_label: spec.target_label,
				group_binding: spec.group_binding,
				path_binding: spec.path_binding,
				mode: spec.mode,
				selector: spec.selector,
				reversed: spec.reversed,
			}),
		))
	}

	fn distinct_edges(&self, input: Plan, edges: Vec<String>) -> Result<Plan, Planning> {
		Ok(step(
			input,
			BindingStep::DistinctEdges {
				edges,
			},
		))
	}

	async fn join(&self, build: Plan, probe: Plan, spec: JoinSpec) -> Result<Plan, Planning> {
		Ok(step(
			probe,
			BindingStep::Join(JoinStep {
				build: Box::new(build),
				keys: spec.keys,
				join_type: spec.join_type,
				null_template: spec.null_template,
				residual: spec.residual,
			}),
		))
	}

	async fn mutate(
		&self,
		plan: &MatchPlan,
		input: Plan,
		stage: &MutationStage,
	) -> Result<Plan, Planning> {
		let name = |id| plan.binding(id).name.clone();
		let mutation = match stage {
			MutationStage::Update {
				target,
				data,
			} => MutationStep::Update {
				target: name(*target),
				op: match data {
					UpdateData::Set(assignments) => UpdateOp::Set(assignments.clone()),
					UpdateData::Unset(properties) => UpdateOp::Unset(properties.clone()),
					UpdateData::Content(object) => UpdateOp::Content(object.clone()),
				},
			},
			MutationStage::Delete {
				target,
				detach,
			} => MutationStep::Delete {
				target: name(*target),
				detach: *detach,
				is_edge: matches!(
					plan.binding(*target).kind,
					BindingKind::Edge | BindingKind::EdgeGroup
				),
			},
			MutationStage::Insert(insert) => MutationStep::Insert {
				nodes: insert
					.nodes
					.iter()
					.map(|node| InsertNodeOp {
						name: name(node.binding),
						table: node.label.clone(),
						props: node.props.clone(),
					})
					.collect(),
				edges: insert
					.edges
					.iter()
					.map(|edge| InsertEdgeOp {
						name: name(edge.binding),
						table: edge.label.clone(),
						from: name(edge.from),
						to: name(edge.to),
						props: edge.props.clone(),
					})
					.collect(),
			},
		};
		Ok(step(input, BindingStep::Mutate(mutation)))
	}

	fn drain(&self, input: Plan) -> Result<Plan, Planning> {
		Ok(step(input, BindingStep::Drain))
	}

	async fn aggregate(
		&self,
		input: Plan,
		output: &MatchOutput,
		group_keys: &[LogicalExpr],
	) -> Result<Plan, Planning> {
		// The columns the tree answers by their first value: neither a
		// grouping key nor a row holding an aggregate.
		let registry = self.planner.function_registry();
		let first_value = output
			.columns
			.iter()
			.enumerate()
			.filter(|(_, column)| {
				!group_keys.contains(&column.expr)
					&& !crate::exec::planner::match_backend::expr_has_aggregate(
						registry,
						&column.expr,
					)
			})
			.map(|(index, _)| index)
			.collect();
		Ok(Plan::Aggregate {
			input: Box::new(input),
			group: group_keys.to_vec(),
			fields: column_fields(output, true),
			first_value,
			spill: super::plan::Spill::of_aggregate(group_keys),
		})
	}

	async fn project(&self, input: Plan, output: &MatchOutput) -> Result<Plan, Planning> {
		let projection =
			map::projection(&column_fields(output, true), Vec::new()).map_err(Planning::Decline)?;
		Ok(Plan::Project {
			input: Box::new(input),
			projection,
		})
	}

	async fn drop_hidden(&self, input: Plan, output: &MatchOutput) -> Result<Plan, Planning> {
		// Every field is a bare read of a name the aggregate already produced.
		let fields = LogicalFields::Select(
			output
				.columns
				.iter()
				.filter(|column| !column.hidden)
				.map(|column| {
					LogicalField::user(
						LogicalExpr::Field(column.name.as_str().into()),
						Some(OutputPath::field(&column.name)),
					)
				})
				.collect(),
		);
		let projection = map::projection(&fields, Vec::new()).map_err(Planning::Decline)?;
		Ok(Plan::Project {
			input: Box::new(input),
			projection,
		})
	}

	fn distinct(&self, input: Plan) -> Result<Plan, Planning> {
		Ok(step(input, BindingStep::Distinct))
	}

	async fn sort(&self, input: Plan, output: &MatchOutput) -> Result<Plan, Planning> {
		Ok(Plan::Sort {
			input: Box::new(input),
			keys: output
				.order
				.iter()
				.map(|order| SortKeySpec {
					read: SortKeyRead::Computed(order.expr.clone()),
					ascending: order.ascending,
					collate: false,
					numeric: false,
				})
				.collect(),
			keep: None,
			pushdown: false,
			random: false,
			spill: super::plan::Spill::of_sort(None),
		})
	}

	async fn limit(&self, input: Plan, output: &MatchOutput) -> Result<Plan, Planning> {
		Ok(Plan::Limit {
			input: Box::new(input),
			limit: output.limit.clone(),
			start: output.skip.clone(),
		})
	}
}

/// `root`, the plan a fold built, as the physical plan of the query.
pub(crate) fn physical(root: Plan, dependencies: Vec<Dependency>) -> PhysicalPlan {
	// A plan whose last node is a projection already ends in one.
	let root = match root {
		Plan::Project {
			..
		} => root,
		other => Plan::Project {
			input: Box::new(other),
			projection: Projection::All,
		},
	};
	PhysicalPlan {
		catalog: None,
		root,
		dependencies,
		tail: super::plan::Tail::default(),
		sources: Vec::new(),
		nested: Vec::new(),
		scope: super::plan::IndexScope::default(),
	}
}
