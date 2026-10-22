//! `MATCH` in the binder: the fold of a plan's binding table into ordinary logical reads.
//!
//! [`MatchFold`] decides the shape of a binding-table plan (the anchor of each pattern, where each
//! conjunct sits, how clauses join) and builds it through a [`BindingBackend`]. This backend,
//! [`LogicalBackend`], builds a tree of [`FoldNode`]s that holds every decision and binds nothing:
//! the fold clones and abandons nodes while it chooses between fast paths, and a node the fold
//! drops must leave no op behind. [`bind_fold`] runs the fold and then binds the tree it chose.
//!
//! # The form
//!
//! A `MATCH` is one `logical.match` op with one `Subquery` region, whose block holds the folded
//! plan and yields its rows. Binding rows are objects keyed by binding name, so a predicate or a
//! projection reads a binding as a field of the row it is evaluated against.
//!
//! - A scan is a `logical.source` of the table, read row by row; a predicate pushed into the scan
//!   is a `logical.filter` over it.
//! - A step of the binding table (a bind, an endpoint, a single hop, a quantified expansion, the
//!   check that no edge repeats, a join, a write to the bound records) is one `logical.bind_step`
//!   over the rows it extends, with the clause regions its shape lists. A join takes the build rows
//!   first and the probe rows second. A write step holds, after its clause regions, one `Subquery`
//!   region for each write it applies to a record: the statement a hand-written mutation of that
//!   record would be, bound like any write and reading the record and the values of the row from
//!   the parameters the VM binds when the step runs it.
//! - A predicate over the binding rows, a projection, a grouping, a sort, a limit and a `DISTINCT`
//!   are the ordinary nodes of a relational plan, bound as the binder binds them: a predicate and a
//!   projection per row inside a loop, the rest over every row.
//! - The operands of a limit are bound before the rows they bound, so they are evaluated before any
//!   row is read.
//!
//! The fold declines one shape: an anchor whose pushed predicate holds a statement, which the
//! statement fails with as an unsupported construct. Every other shape of a plan has a node here,
//! and a plan the fold refuses is an invariant break of the lowering that produced it.
//!
//! A plan whose last node is not a projection ends in a projection of `*` over its rows, so a
//! plan always ends in one.

use std::sync::Arc;

use futures::FutureExt;
use surrealdb_strand::TableName;

use super::logical::{
	Flow, aggregate_over, bind_projection, bind_source, columns_of, first_result, limit_over,
	row_clause, select_loop, sort_over,
};
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::ids::ValueId;
use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind};
use crate::exec::ir::iface::statements::{
	BindExpandShape, BindInsertEdge, BindInsertNode, BindJoinShape, BindPathShape, BindStepShape,
	MatchBindings, MatchData,
};
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::logical::ops::LogOp;
use crate::exec::ir::logical::seam::{BindCx, Built, ExprHost, bind_expr, invariant};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::operators::mutate::{create_write, delete_write, relate_write, update_write};
use crate::exec::planner::match_backend::{
	BindingBackend, EndpointSpec, ExpandSpec, JoinSpec, PathSpec, Planning,
};
use crate::exec::planner::match_plan::MatchFold;
use crate::exec::vm::NotCovered;
use crate::expr::Function;
use crate::expr::logical::{
	LogicalExpr, LogicalField, LogicalFields, LogicalMatch, LogicalOp, LogicalOrder,
	LogicalOrdering, LogicalSource, OutputPath, SourceRef,
};
use crate::expr::match_plan::{
	BindingId, BindingKind, MatchOutput, MatchPlan, MatchStage, MutationStage, UpdateData,
};

/// The refusal of a `MATCH` the entry's surface leaves out.
pub(crate) const NOT_SERVED_MATCH: &str = "ir build: a MATCH statement is not served by this entry";

/// Binds a `MATCH` statement.
///
/// The binder reaches the fold only through this hook, held by the binding context as an
/// `Option`. The context of an entry that serves `MATCH` holds [`FULL_MATCH_BINDING`]; one that
/// does not holds nothing, names none of the fold, and refuses a `MATCH` with
/// [`NOT_SERVED_MATCH`].
pub(crate) trait MatchBinding {
	/// Binds `statement` into the region `cx` is binding ([`bind_fold`]).
	fn bind(&self, cx: &mut BindCx<'_>, statement: &LogicalMatch) -> Result<Flow, IrError>;
}

/// The binding of every `MATCH`: [`bind_fold`].
pub(crate) struct FullMatchBinding;

/// The one [`FullMatchBinding`], named only by the surface that serves `MATCH`.
pub(crate) static FULL_MATCH_BINDING: FullMatchBinding = FullMatchBinding;

impl MatchBinding for FullMatchBinding {
	fn bind(&self, cx: &mut BindCx<'_>, statement: &LogicalMatch) -> Result<Flow, IrError> {
		bind_fold(cx, statement)
	}
}

/// A node of the folded binding table, as the fold built it and before it is bound.
///
/// Children are shared, so the fold's clones of a node cost a reference.
pub(crate) enum FoldNode {
	/// The records of a table, with the predicate pushed into the scan, scoped to the record.
	Scan {
		label: TableName,
		pushed: Option<LogicalExpr>,
	},
	/// One row with no bindings.
	SingleRow,
	/// The rows of `input` the predicate keeps, scoped to the binding row.
	Filter {
		input: Arc<FoldNode>,
		predicate: LogicalExpr,
	},
	/// A step of the binding table over `inputs` (a join's build rows, then its probe rows), with
	/// one expression per clause region of the shape, in the order the shape lists them, and then
	/// one write per `Subquery` region.
	Step {
		inputs: Vec<Arc<FoldNode>>,
		shape: BindStepShape,
		clauses: Vec<(ClauseRole, LogicalExpr)>,
		writes: Vec<FoldWrite>,
	},
	/// The columns folded by the grouping keys.
	Aggregate {
		input: Arc<FoldNode>,
		columns: LogicalFields,
		group: Vec<LogicalExpr>,
	},
	/// The columns projected from each binding row.
	Project {
		input: Arc<FoldNode>,
		columns: LogicalFields,
	},
	/// The first of each distinct row.
	Distinct {
		input: Arc<FoldNode>,
	},
	Sort {
		input: Arc<FoldNode>,
		ordering: LogicalOrdering,
	},
	Limit {
		input: Arc<FoldNode>,
		limit: Option<LogicalExpr>,
		start: Option<LogicalExpr>,
	},
}

/// A write a mutation step applies to each record of its rows.
pub(crate) struct FoldWrite {
	/// The statement, which reads its record and values from the parameters of the run.
	op: LogicalOp,
	/// The one table every record the statement writes belongs to, which the statement itself
	/// names only through a value.
	table: Option<TableName>,
}

type Node = Arc<FoldNode>;

fn step(
	inputs: Vec<Node>,
	shape: BindStepShape,
	clauses: Vec<(ClauseRole, LogicalExpr)>,
	writes: Vec<FoldWrite>,
) -> Result<Node, Planning> {
	Ok(Arc::new(FoldNode::Step {
		inputs,
		shape,
		clauses,
		writes,
	}))
}

/// The one table every record the binding `id` holds belongs to, when the plan guarantees it: the
/// binding is a node or an edge, and every occurrence of it in the plan (in a pattern of a
/// `MATCH`, mandatory or optional, or created by an `INSERT`) carries the same label. The
/// occurrence that introduces the binding carries a label, so every record it binds is of that
/// table; an unlabeled or differently labeled occurrence anywhere answers `None`. A write bound
/// to the answered table runs that table's gates and stages without re-checking the record's table,
/// so the answer must hold for every record the binding can hold.
fn static_table(plan: &MatchPlan, id: BindingId) -> Option<TableName> {
	if !matches!(plan.binding(id).kind, BindingKind::Node | BindingKind::Edge) {
		return None;
	}
	let mut labels: Vec<Option<&TableName>> = Vec::new();
	for stage in &plan.stages {
		match stage {
			MatchStage::Read(clause) => {
				for pattern in &clause.patterns {
					if pattern.start.binding == id {
						labels.push(pattern.start.label.as_ref());
					}
					for (edge, node) in &pattern.steps {
						if edge.binding == id {
							labels.push(edge.label.as_ref());
						}
						if node.binding == id {
							labels.push(node.label.as_ref());
						}
					}
				}
			}
			MatchStage::Mutate(MutationStage::Insert(insert)) => {
				labels.extend(
					insert.nodes.iter().filter(|node| node.binding == id).map(|n| Some(&n.label)),
				);
				labels.extend(
					insert.edges.iter().filter(|edge| edge.binding == id).map(|e| Some(&e.label)),
				);
			}
			MatchStage::Mutate(MutationStage::Update {
				..
			})
			| MatchStage::Mutate(MutationStage::Delete {
				..
			}) => {}
		}
	}
	let first = labels.first().copied().flatten()?;
	labels.iter().all(|label| *label == Some(first)).then(|| first.clone())
}

/// The columns of `output` as a projection list, each column projected under its output name.
/// Hidden columns are included only when `hidden` is true.
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

/// The backend of the binder's fold: it builds the tree [`bind_fold`] binds.
pub(crate) struct LogicalBackend;

impl BindingBackend for LogicalBackend {
	type Node = Node;

	async fn scan(&self, label: TableName, pushed: Option<LogicalExpr>) -> Result<Node, Planning> {
		if pushed
			.as_ref()
			.is_some_and(|predicate| predicate.any(|expr| matches!(expr, LogicalExpr::Subquery(_))))
		{
			return Err(Planning::Decline(NotCovered::unsupported(
				"an anchor holding a statement",
			)));
		}
		Ok(Arc::new(FoldNode::Scan {
			label,
			pushed,
		}))
	}

	fn bind(&self, input: Node, name: String) -> Result<Node, Planning> {
		step(
			vec![input],
			BindStepShape::Bind {
				name,
			},
			Vec::new(),
			Vec::new(),
		)
	}

	fn endpoint(&self, input: Node, spec: EndpointSpec) -> Result<Node, Planning> {
		step(
			vec![input],
			BindStepShape::Endpoint {
				edge: spec.edge,
				field: spec.field,
				name: spec.name,
				label: spec.label,
			},
			Vec::new(),
			Vec::new(),
		)
	}

	fn single_row(&self) -> Result<Node, Planning> {
		Ok(Arc::new(FoldNode::SingleRow))
	}

	async fn filter(&self, input: Node, predicate: LogicalExpr) -> Result<Node, Planning> {
		Ok(Arc::new(FoldNode::Filter {
			input,
			predicate,
		}))
	}

	async fn expand(&self, input: Node, spec: ExpandSpec) -> Result<Node, Planning> {
		let predicate = spec.predicate.is_some();
		let clauses = spec.predicate.into_iter().map(|p| (ClauseRole::Where, p)).collect();
		step(
			vec![input],
			BindStepShape::Expand(Box::new(BindExpandShape {
				source: spec.source,
				direction: spec.dir,
				edge_tables: spec.edge_tables.into_boxed_slice(),
				edge_binding: spec.edge_binding,
				target_binding: spec.target_binding,
				target_label: spec.target_label,
				predicate,
				optional: spec.optional,
			})),
			clauses,
			Vec::new(),
		)
	}

	async fn path_expand(&self, input: Node, spec: PathSpec) -> Result<Node, Planning> {
		step(
			vec![input],
			BindStepShape::Path(Box::new(BindPathShape {
				source: spec.source,
				direction: spec.dir,
				edge_tables: spec.edge_tables.into_boxed_slice(),
				min: spec.min,
				max: spec.max,
				target_binding: spec.target_binding,
				target_label: spec.target_label,
				group_binding: spec.group_binding,
				path_binding: spec.path_binding,
				mode: spec.mode,
				selector: spec.selector,
				reversed: spec.reversed,
			})),
			Vec::new(),
			Vec::new(),
		)
	}

	fn distinct_edges(&self, input: Node, edges: Vec<String>) -> Result<Node, Planning> {
		step(
			vec![input],
			BindStepShape::DistinctEdges {
				edges: edges.into_boxed_slice(),
			},
			Vec::new(),
			Vec::new(),
		)
	}

	async fn join(&self, build: Node, probe: Node, spec: JoinSpec) -> Result<Node, Planning> {
		let clauses: Vec<_> = spec.residual.into_iter().map(|p| (ClauseRole::Where, p)).collect();
		step(
			vec![build, probe],
			BindStepShape::Join(Box::new(BindJoinShape {
				keys: spec.keys.into_boxed_slice(),
				join_type: spec.join_type,
				null_template: spec.null_template.into_boxed_slice(),
				residual: !clauses.is_empty(),
			})),
			clauses,
			Vec::new(),
		)
	}

	async fn mutate(
		&self,
		plan: &MatchPlan,
		input: Node,
		stage: &MutationStage,
	) -> Result<Node, Planning> {
		let name = |id| plan.binding(id).name.clone();
		let fail = |error: crate::exec::Error| Planning::Fail(error.into());
		match stage {
			MutationStage::Update {
				target,
				data,
			} => {
				let (data, clauses) = match data {
					UpdateData::Set(assignments) => (
						MatchData::Set(assignments.iter().map(|(name, _)| name.clone()).collect()),
						assignments
							.iter()
							.map(|(_, value)| (ClauseRole::SetValue, value.clone()))
							.collect(),
					),
					UpdateData::Unset(names) => {
						(MatchData::Unset(names.clone().into_boxed_slice()), Vec::new())
					}
					UpdateData::Content(object) => {
						(MatchData::Content, vec![(ClauseRole::Content, object.clone())])
					}
				};
				let writes = vec![FoldWrite {
					op: update_write(&data).map_err(fail)?,
					table: static_table(plan, *target),
				}];
				step(
					vec![input],
					BindStepShape::Update {
						target: name(*target),
						data,
					},
					clauses,
					writes,
				)
			}
			MutationStage::Delete {
				target,
				detach,
			} => {
				let writes = vec![FoldWrite {
					op: delete_write().map_err(fail)?,
					table: static_table(plan, *target),
				}];
				step(
					vec![input],
					BindStepShape::Delete {
						target: name(*target),
						detach: *detach,
						is_edge: matches!(
							plan.binding(*target).kind,
							BindingKind::Edge | BindingKind::EdgeGroup
						),
					},
					Vec::new(),
					writes,
				)
			}
			MutationStage::Insert(insert) => {
				let nodes = insert
					.nodes
					.iter()
					.map(|node| BindInsertNode {
						name: name(node.binding),
						table: node.label.clone(),
					})
					.collect();
				let edges = insert
					.edges
					.iter()
					.map(|edge| BindInsertEdge {
						name: name(edge.binding),
						table: edge.label.clone(),
						from: name(edge.from),
						to: name(edge.to),
					})
					.collect();
				let clauses = insert
					.nodes
					.iter()
					.map(|node| (ClauseRole::Content, node.props.clone()))
					.chain(
						insert.edges.iter().map(|edge| (ClauseRole::Content, edge.props.clone())),
					)
					.collect();
				// A `CREATE` and a `RELATE` name their table as a literal.
				let mut writes = Vec::with_capacity(insert.nodes.len() + insert.edges.len());
				for node in &insert.nodes {
					writes.push(FoldWrite {
						op: create_write(&node.label).map_err(fail)?,
						table: None,
					});
				}
				for edge in &insert.edges {
					writes.push(FoldWrite {
						op: relate_write(&edge.label).map_err(fail)?,
						table: None,
					});
				}
				step(
					vec![input],
					BindStepShape::Insert {
						nodes,
						edges,
					},
					clauses,
					writes,
				)
			}
		}
	}

	fn drain(&self, input: Node) -> Result<Node, Planning> {
		step(vec![input], BindStepShape::Drain, Vec::new(), Vec::new())
	}

	async fn aggregate(
		&self,
		input: Node,
		output: &MatchOutput,
		group_keys: &[LogicalExpr],
	) -> Result<Node, Planning> {
		Ok(Arc::new(FoldNode::Aggregate {
			input,
			columns: column_fields(output, true),
			group: group_keys.to_vec(),
		}))
	}

	async fn project(&self, input: Node, output: &MatchOutput) -> Result<Node, Planning> {
		Ok(Arc::new(FoldNode::Project {
			input,
			columns: column_fields(output, true),
		}))
	}

	async fn drop_hidden(&self, input: Node, output: &MatchOutput) -> Result<Node, Planning> {
		// Every column is a bare read of a name the aggregate already produced.
		let columns = LogicalFields::Select(
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
		Ok(Arc::new(FoldNode::Project {
			input,
			columns,
		}))
	}

	fn distinct(&self, input: Node) -> Result<Node, Planning> {
		Ok(Arc::new(FoldNode::Distinct {
			input,
		}))
	}

	async fn sort(&self, input: Node, output: &MatchOutput) -> Result<Node, Planning> {
		let keys = output
			.order
			.iter()
			.map(|order| LogicalOrder {
				value: order.expr.clone(),
				collate: false,
				numeric: false,
				direction: order.ascending,
			})
			.collect();
		Ok(Arc::new(FoldNode::Sort {
			input,
			ordering: LogicalOrdering::Order(keys),
		}))
	}

	async fn limit(&self, input: Node, output: &MatchOutput) -> Result<Node, Planning> {
		Ok(Arc::new(FoldNode::Limit {
			input,
			limit: output.limit.clone(),
			start: output.skip.clone(),
		}))
	}
}

/// The rows a node was bound to.
#[derive(Clone, Copy)]
struct Rows {
	value: ValueId,
	/// Whether the value is a source or a block of values that a loop reads row by row, as opposed
	/// to the output of a node that already produces rows.
	leaf: bool,
}

/// The value of `rows` for a node that needs every row: a leaf is read through a loop that
/// delivers each row unchanged.
fn all_rows(cx: &mut BindCx<'_>, rows: Rows) -> Result<ValueId, IrError> {
	if rows.leaf {
		select_loop(cx, rows.value, |_, row| Ok(row))
	} else {
		Ok(rows.value)
	}
}

/// The positions of the columns of a grouping that take the value of the group's first row: each
/// column that is neither one of the grouping keys `group` nor holds an aggregate call, which the
/// lowering of the `MATCH` admits only where the keys determine it.
fn first_value_columns(
	cx: &BindCx<'_>,
	columns: &LogicalFields,
	group: &[LogicalExpr],
) -> Vec<usize> {
	let registry = cx.registry();
	columns_of(columns)
		.iter()
		.enumerate()
		.filter(|(_, column)| {
			!group.contains(column.expr)
				&& !column.expr.any(|expr| {
					matches!(
						expr,
						LogicalExpr::Call(call)
							if matches!(&call.receiver, Function::Normal(name)
								if registry.get_aggregate(name.as_str()).is_some())
					)
				})
		})
		.map(|(at, _)| at)
		.collect()
}

/// Binds the nodes of `inputs` in order and answers each one's rows, or `None` when one ended the
/// block.
fn all_inputs(cx: &mut BindCx<'_>, inputs: &[Node]) -> Result<Option<Vec<ValueId>>, IrError> {
	let mut rows = Vec::with_capacity(inputs.len());
	for input in inputs {
		let Some(bound) = bind_node(cx, input)? else {
			return Ok(None);
		};
		rows.push(all_rows(cx, bound)?);
	}
	Ok(Some(rows))
}

/// Binds `node` and the nodes below it, answering the rows it produces; `None` when an operand
/// ended the block.
fn bind_node(cx: &mut BindCx<'_>, node: &FoldNode) -> Result<Option<Rows>, IrError> {
	let piped = |value| {
		Ok(Some(Rows {
			value,
			leaf: false,
		}))
	};
	match node {
		FoldNode::Scan {
			label,
			pushed,
		} => {
			let source = LogicalSource {
				source: SourceRef::Table(label.clone()),
				schema: None,
				with: None,
				for_update: false,
			};
			let Flow::Value(rows) = bind_source(cx, &source)? else {
				return Ok(None);
			};
			match pushed {
				Some(predicate) => piped(filter_rows(cx, rows, predicate)?),
				None => Ok(Some(Rows {
					value: rows,
					leaf: true,
				})),
			}
		}
		FoldNode::SingleRow => {
			let Built::Value(value) = bind_expr(cx, &LogicalExpr::Object(Vec::new()))? else {
				return Ok(None);
			};
			let op = cx.emit(
				OpKind::Logical(LogOp::Values {
					for_update: false,
					written: false,
				}),
				&[value],
				&[TypeKnowledge::Unknown],
			)?;
			Ok(Some(Rows {
				value: first_result(cx, op)?,
				leaf: true,
			}))
		}
		FoldNode::Filter {
			input,
			predicate,
		} => {
			let Some(rows) = bind_node(cx, input)? else {
				return Ok(None);
			};
			piped(filter_rows(cx, rows.value, predicate)?)
		}
		FoldNode::Step {
			inputs,
			shape,
			clauses,
			writes,
		} => {
			let Some(operands) = all_inputs(cx, inputs)? else {
				return Ok(None);
			};
			let op = cx.emit(
				OpKind::Logical(LogOp::BindStep(Box::new(shape.clone()))),
				&operands,
				&[TypeKnowledge::Unknown],
			)?;
			for (role, expr) in clauses {
				row_clause(cx, op, RegionKind::Clause(*role), expr)?;
			}
			for write in writes {
				cx.within(op, RegionKind::Subquery, &[], |cx, _| {
					if let Some(table) = &write.table {
						cx.name_next_write(table.clone());
					}
					let bound = super::logical::bind_value(cx, &write.op);
					cx.take_named_table();
					if let Some(value) = bound? {
						cx.yield_logical(&[value])?;
					}
					Ok(())
				})?;
			}
			piped(first_result(cx, op)?)
		}
		FoldNode::Aggregate {
			input,
			columns,
			group,
		} => {
			let Some(rows) = bind_node(cx, input)? else {
				return Ok(None);
			};
			let rows = all_rows(cx, rows)?;
			let first_value = first_value_columns(cx, columns, group);
			piped(aggregate_over(cx, rows, group, columns, first_value)?)
		}
		FoldNode::Project {
			input,
			columns,
		} => {
			let Some(rows) = bind_node(cx, input)? else {
				return Ok(None);
			};
			piped(select_loop(cx, rows.value, |cx, row| {
				bind_projection(cx, columns, &[], false, Vec::new(), row)
			})?)
		}
		FoldNode::Distinct {
			input,
		} => {
			let Some(rows) = bind_node(cx, input)? else {
				return Ok(None);
			};
			let rows = all_rows(cx, rows)?;
			let value =
				cx.emit_value(OpKind::Logical(LogOp::Distinct), &[rows], TypeKnowledge::Unknown)?;
			piped(value)
		}
		FoldNode::Sort {
			input,
			ordering,
		} => {
			let Some(rows) = bind_node(cx, input)? else {
				return Ok(None);
			};
			let rows = all_rows(cx, rows)?;
			piped(sort_over(cx, rows, ordering, false, None)?)
		}
		FoldNode::Limit {
			input,
			limit,
			start,
		} => {
			// The operands are evaluated once, `LIMIT` first, before the rows they bound are read.
			let mut bound = [None, None];
			for (slot, expr) in bound.iter_mut().zip([limit, start]) {
				if let Some(expr) = expr {
					let Built::Value(value) = bind_expr(cx, expr)? else {
						return Ok(None);
					};
					*slot = Some(value);
				}
			}
			let [limit_value, start_value] = bound;
			let Some(rows) = bind_node(cx, input)? else {
				return Ok(None);
			};
			let rows = all_rows(cx, rows)?;
			piped(limit_over(cx, rows, limit_value, start_value)?)
		}
	}
}

/// The loop over `rows` that keeps the rows `predicate` holds for, the predicate scoped to the
/// row it judges.
fn filter_rows(
	cx: &mut BindCx<'_>,
	rows: ValueId,
	predicate: &LogicalExpr,
) -> Result<ValueId, IrError> {
	select_loop(cx, rows, |cx, row| {
		let op = cx.emit(OpKind::Logical(LogOp::Filter), &[row], &[TypeKnowledge::Unknown])?;
		row_clause(cx, op, RegionKind::Clause(ClauseRole::Where), predicate)?;
		first_result(cx, op)
	})
}

/// Binds the `MATCH` statement `statement`: the fold of its plan, bound as one `logical.match`
/// over a single `Subquery` region that holds the plan and yields its rows.
///
/// A plan whose anchor pushes a predicate that holds a statement is declined as an unsupported
/// construct. A plan the fold refuses, or a fold that would wait, is an invariant break: the
/// backend does no I/O.
pub(super) fn bind_fold(cx: &mut BindCx<'_>, statement: &LogicalMatch) -> Result<Flow, IrError> {
	let plan = statement.plan.as_ref();
	let fold = MatchFold {
		backend: &LogicalBackend,
	};
	let root = match fold.plan(plan.clone()).now_or_never() {
		Some(Ok(root)) => root,
		Some(Err(Planning::Fail(error))) => {
			return Err(invariant(&format!("a MATCH plan the fold refuses: {error}")));
		}
		Some(Err(Planning::Decline(declined))) => {
			return Err(match declined.refusal {
				Some(refusal) => IrError::Rejected(Box::new(refusal)),
				None => IrError::Unsupported {
					reason: declined.reason,
				},
			});
		}
		None => return Err(invariant("a MATCH fold that waits")),
	};
	// A plan that does not end in a projection ends in one of `*`.
	let root = if matches!(*root, FoldNode::Project { .. }) {
		root
	} else {
		Arc::new(FoldNode::Project {
			input: root,
			columns: LogicalFields::all(),
		})
	};
	let op = cx.emit(
		OpKind::Logical(LogOp::Match {
			bindings: MatchBindings(plan.bindings.clone().into_boxed_slice()),
		}),
		&[],
		&[TypeKnowledge::Unknown],
	)?;
	let ended = cx.within(op, RegionKind::Subquery, &[], |cx, _| match bind_node(cx, &root)? {
		Some(rows) => {
			let value = all_rows(cx, rows)?;
			cx.yield_logical(&[value])?;
			Ok(false)
		}
		None => Ok(true),
	})?;
	if ended {
		return Ok(Flow::Diverged);
	}
	first_result(cx, op).map(Flow::Value)
}

#[cfg(all(test, feature = "gql"))]
mod tests {
	use super::*;
	use crate::dbs::Capabilities;
	use crate::dbs::capabilities::Targets;
	use crate::exec::function::FunctionRegistry;
	use crate::exec::ir::core::facts::FactCache;
	use crate::exec::ir::core::ir::{Ir, IrScope, Walk, WalkOrder};
	use crate::exec::ir::iface::catalog::{EffectCx, SemanticCatalog};
	use crate::exec::ir::iface::frame::FrameTable;
	use crate::exec::ir::iface::ids::OpId;
	use crate::exec::ir::logical::ops::LogTag;
	use crate::expr::logical::LogicalOp;
	use crate::syn::ParserConfig;

	/// A semantic catalog that holds no definition.
	struct NoCatalog;

	impl SemanticCatalog for NoCatalog {}

	/// The container the `MATCH` query `query` binds to.
	fn bound(query: &str) -> Ir {
		let plan = crate::gql::parse_with_capabilities(
			query,
			&Capabilities::all().with_experimental(Targets::All),
			&ParserConfig::default(),
		)
		.unwrap_or_else(|e| panic!("`{query}` does not parse: {e:?}"))
		.0;
		let LogicalOp::Match(statement) = &plan else {
			panic!("`{query}` is not a MATCH plan");
		};
		let registry = FunctionRegistry::with_builtins();
		let frames = FrameTable::default();
		let effects = EffectCx {
			registry: &registry,
			catalog: &NoCatalog,
			frames: &frames,
		};
		let mut ir = Ir::new(
			IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		);
		let mut facts = FactCache::new();
		{
			let mut cx = BindCx::new(&mut ir, &mut facts, effects).expect("the context builds");
			bind_fold(&mut cx, statement).expect("the plan binds");
		}
		ir
	}

	/// A `MATCH` bound for an entry whose surface holds no `MATCH` hook is refused with
	/// [`NOT_SERVED_MATCH`] and builds no op, and the same statement binds under the surface that
	/// holds it.
	#[test]
	fn a_match_is_refused_where_the_surface_holds_no_match_hook() {
		use crate::exec::surface::{DML, FULL};
		let plan = crate::gql::parse_with_capabilities(
			"MATCH (n:person) RETURN n.name AS name",
			&Capabilities::all().with_experimental(Targets::All),
			&ParserConfig::default(),
		)
		.expect("the query parses")
		.0;
		let LogicalOp::Match(statement) = &plan else {
			panic!("the query is not a MATCH plan");
		};
		let registry = FunctionRegistry::with_builtins();
		let frames = FrameTable::default();
		let effects = EffectCx {
			registry: &registry,
			catalog: &NoCatalog,
			frames: &frames,
		};
		let bind = |surface: Option<&'static crate::exec::surface::Surface>| {
			let mut ir = Ir::new(
				IrScope::Request {
					fragment: None,
				},
				RegionKind::Request,
			);
			let mut facts = FactCache::new();
			let outcome = {
				let mut cx = BindCx::new(&mut ir, &mut facts, effects).expect("the context builds");
				cx.set_surface(surface);
				super::super::stmt::bind_match(&mut cx, statement).map(|_| ())
			};
			(ir, outcome)
		};
		for surface in [None, Some(&DML)] {
			let (ir, outcome) = bind(surface);
			assert!(
				matches!(outcome, Err(IrError::NotYet { what }) if what == NOT_SERVED_MATCH),
				"{outcome:?}"
			);
			assert!(ops_of(&ir, LogTag::Match).is_empty(), "the refusal left an op behind");
		}
		let (ir, outcome) = bind(Some(&FULL));
		outcome.expect("the plan binds");
		assert_eq!(ops_of(&ir, LogTag::Match).len(), 1);
	}

	fn all_ops(ir: &Ir) -> Vec<OpId> {
		let mut out = Vec::new();
		ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
			out.push(op);
			Walk::Continue
		});
		out
	}

	/// Every logical op of tag `tag`, in pre-order.
	fn ops_of(ir: &Ir, tag: LogTag) -> Vec<OpId> {
		all_ops(ir)
			.into_iter()
			.filter(|op| {
				matches!(&ir.op(*op).expect("the op exists").kind, OpKind::Logical(log) if log.tag() == tag)
			})
			.collect()
	}

	/// The shapes of the binding-table steps of `ir`, in pre-order.
	fn steps_of(ir: &Ir) -> Vec<BindStepShape> {
		ops_of(ir, LogTag::BindStep)
			.into_iter()
			.filter_map(|op| match &ir.op(op).expect("the op exists").kind {
				OpKind::Logical(LogOp::BindStep(shape)) => Some((**shape).clone()),
				OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => None,
			})
			.collect()
	}

	/// The ops of the block of the first region of `owner`, in order.
	fn block_of(ir: &Ir, owner: OpId) -> Vec<OpId> {
		let region = ir.op(owner).expect("the op exists").regions[0];
		let block = ir.region(region).expect("the region exists").blocks[0];
		ir.block(block).expect("the block exists").ops.clone()
	}

	/// A `MATCH` is one op with one `Subquery` region that yields its rows, and holds the whole
	/// fold.
	#[test]
	fn a_match_binds_one_op_over_one_subquery_region() {
		let ir = bound("MATCH (n:person) RETURN n.name AS name");
		let matches = ops_of(&ir, LogTag::Match);
		assert_eq!(matches.len(), 1);
		let data = ir.op(matches[0]).expect("the op exists");
		assert_eq!(data.regions.len(), 1);
		assert_eq!(
			ir.region(data.regions[0]).expect("the region exists").kind,
			RegionKind::Subquery
		);
		assert_eq!(data.results.len(), 1);
		// No separate read or write stage op is bound; the fold stays inside the match op.
		for tag in
			[LogTag::MatchRead, LogTag::MatchUpdate, LogTag::MatchDelete, LogTag::MatchInsert]
		{
			assert!(ops_of(&ir, tag).is_empty(), "{tag:?}");
		}
		assert_eq!(ops_of(&ir, LogTag::Source).len(), 1);
		assert_eq!(ops_of(&ir, LogTag::Project).len(), 1);
	}

	/// A single hop binds the anchor as a binding, then expands off it; the expand holds the
	/// predicate region only when the hop has one.
	#[test]
	fn a_single_hop_binds_the_anchor_and_expands_off_it() {
		let ir = bound("MATCH (a:person)-[:knows]->(b:person) RETURN b.name AS name");
		let steps = steps_of(&ir);
		assert!(matches!(steps.first(), Some(BindStepShape::Bind { name }) if name == "a"));
		assert!(steps.iter().any(|step| matches!(step, BindStepShape::Expand(_))));
		for step in &steps {
			if let BindStepShape::Expand(expand) = step {
				assert!(!expand.predicate);
				assert!(!expand.optional);
			}
		}
	}

	/// A predicate over one binding is pushed into the scan of its anchor: a filter over the
	/// source, before the binding step.
	#[test]
	fn a_predicate_over_the_anchor_is_a_filter_over_its_scan() {
		let ir = bound("MATCH (a:person) WHERE a.age > 1 RETURN a.name AS name");
		assert_eq!(ops_of(&ir, LogTag::Filter).len(), 1);
		let filters = ops_of(&ir, LogTag::Filter);
		let filter = ir.op(filters[0]).expect("the op exists");
		assert_eq!(filter.regions.len(), 1);
		assert_eq!(
			ir.region(filter.regions[0]).expect("the region exists").kind,
			RegionKind::Clause(ClauseRole::Where)
		);
	}

	/// A grouping is an aggregate with one region per key and one per column, then a sort.
	#[test]
	fn a_grouping_binds_an_aggregate_and_a_sort() {
		let ir = bound(
			"MATCH (n:person) RETURN n.city AS city, count(*) AS c GROUP BY n.city ORDER BY city",
		);
		let aggregates = ops_of(&ir, LogTag::Aggregate);
		assert_eq!(aggregates.len(), 1);
		let kinds: Vec<RegionKind> = ir
			.op(aggregates[0])
			.expect("the op exists")
			.regions
			.iter()
			.map(|region| ir.region(*region).expect("the region exists").kind)
			.collect();
		assert_eq!(
			kinds.iter().filter(|kind| matches!(kind, RegionKind::ErrorScope(_))).count(),
			1,
			"one region per grouping key"
		);
		assert_eq!(
			kinds
				.iter()
				.filter(|kind| matches!(kind, RegionKind::Clause(ClauseRole::Projection)))
				.count(),
			2,
			"one region per column"
		);
		assert_eq!(ops_of(&ir, LogTag::Sort).len(), 1);
	}

	/// The operands of a limit are bound before the rows they bound: no logical op stands before
	/// the expressions that give them, so they are evaluated before any row is read.
	#[test]
	fn a_limit_is_bound_before_the_rows_it_bounds() {
		let ir = bound("MATCH (n:person) RETURN n.name AS name SKIP 1 LIMIT 2");
		let matches = ops_of(&ir, LogTag::Match);
		let block = block_of(&ir, matches[0]);
		let first_logical = block
			.iter()
			.position(|op| matches!(ir.op(*op).expect("the op exists").kind, OpKind::Logical(_)))
			.expect("the block holds a logical op");
		assert!(
			block[..first_logical]
				.iter()
				.any(|op| matches!(ir.op(*op).expect("the op exists").kind, OpKind::Expr(_))),
			"the operands precede the first logical op"
		);
		let limits = ops_of(&ir, LogTag::Limit);
		assert_eq!(limits.len(), 1);
		// The rows, then the limit, then the start.
		assert_eq!(ir.op(limits[0]).expect("the op exists").operands.len(), 3);
	}

	/// A `DISTINCT` projection binds a distinct over the projected rows.
	#[test]
	fn a_distinct_projection_binds_a_distinct() {
		let ir = bound("MATCH (n:person) RETURN DISTINCT n.city AS city ORDER BY city");
		assert_eq!(ops_of(&ir, LogTag::Distinct).len(), 1);
		assert_eq!(ops_of(&ir, LogTag::Sort).len(), 1);
	}

	/// A write to the matched records is a step of the binding table that writes: a clause region
	/// per assignment, then the `Subquery` region of the write it applies to each record.
	#[test]
	fn a_set_on_a_binding_is_an_update_step_with_a_region_per_assignment_and_one_per_write() {
		let ir = bound("MATCH (n:person) SET n.age = 1, n.seen = true");
		let steps = steps_of(&ir);
		let update = steps
			.iter()
			.find(|step| matches!(step, BindStepShape::Update { .. }))
			.expect("an update step");
		assert_eq!(
			update.region_kinds().as_slice(),
			&[
				RegionKind::Clause(ClauseRole::SetValue),
				RegionKind::Clause(ClauseRole::SetValue),
				RegionKind::Subquery,
			]
		);
		assert_eq!(update.write_regions(), 1);
		assert!(update.writes());
		let step = ops_of(&ir, LogTag::BindStep)
			.into_iter()
			.find(|op| {
				matches!(
					&ir.op(*op).expect("the op exists").kind,
					OpKind::Logical(LogOp::BindStep(shape)) if shape.writes()
				)
			})
			.expect("a write step");
		let held: Vec<RegionKind> = ir
			.op(step)
			.expect("the op exists")
			.regions
			.iter()
			.map(|region| ir.region(*region).expect("the region exists").kind)
			.collect();
		assert_eq!(held.as_slice(), update.region_kinds().as_slice());
	}

	/// An insert holds one write per node, then one per edge.
	#[test]
	fn an_insert_holds_a_write_region_per_node_and_edge() {
		let ir = bound("INSERT (a:person {name: 'x'})-[e:knows]->(b:person {name: 'y'})");
		let steps = steps_of(&ir);
		let insert = steps
			.iter()
			.find(|step| matches!(step, BindStepShape::Insert { .. }))
			.expect("an insert step");
		assert_eq!(insert.write_regions(), 3);
		assert_eq!(insert.region_kinds().len(), 6);
	}

	/// The tables the writes of `query` name: whether each write is bound to no one table.
	fn writes_naming_no_table(query: &str) -> Vec<bool> {
		let ir = bound(query);
		ops_of(&ir, LogTag::Write)
			.into_iter()
			.filter_map(|op| match &ir.op(op).expect("the op exists").kind {
				OpKind::Logical(LogOp::Write(attrs)) => Some(attrs.statement.is_some()),
				OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => None,
			})
			.collect()
	}

	/// A write to a target every occurrence of which carries one label is bound to that table;
	/// one whose target is unlabeled names none.
	#[test]
	fn a_write_of_a_labeled_target_is_bound_to_its_table_and_an_unlabeled_one_to_none() {
		assert_eq!(writes_naming_no_table("MATCH (n:person) SET n.age = 1"), [false]);
		assert_eq!(writes_naming_no_table("MATCH (n:person) DELETE n"), [false]);
		assert_eq!(
			writes_naming_no_table("MATCH (a:person)-[:knows]->(b) SET b.seen = true"),
			[true]
		);
		assert_eq!(
			writes_naming_no_table("MATCH (a:person)-[:knows]->(b:person) SET b.seen = true"),
			[false]
		);
		assert_eq!(writes_naming_no_table("INSERT (n:person {name: 'x'})"), [false]);
	}

	/// The plan of the `MATCH` query `query`.
	fn plan_of(query: &str) -> MatchPlan {
		let plan = crate::gql::parse_with_capabilities(
			query,
			&Capabilities::all().with_experimental(Targets::All),
			&ParserConfig::default(),
		)
		.unwrap_or_else(|e| panic!("`{query}` does not parse: {e:?}"))
		.0;
		let LogicalOp::Match(statement) = plan else {
			panic!("`{query}` is not a MATCH plan");
		};
		statement.plan.as_ref().clone()
	}

	/// The static table of the binding `name` of `query`.
	fn static_table_of(query: &str, name: &str) -> Option<TableName> {
		let plan = plan_of(query);
		let id = plan.bindings.iter().position(|binding| binding.name == name).expect("a binding");
		static_table(&plan, id as BindingId)
	}

	/// The static table is the label every occurrence of a node or edge binding carries: an
	/// unlabeled occurrence, a conflicting one, or a binding that is no node or edge has none.
	#[test]
	fn a_static_table_is_a_label_every_occurrence_of_the_binding_carries() {
		let person = Some(TableName::from("person"));
		assert_eq!(static_table_of("MATCH (n:person) SET n.age = 1", "n"), person);
		assert_eq!(
			static_table_of("MATCH (a:person)-[e:knows]->(b:person) SET e.w = 1", "e"),
			Some(TableName::from("knows"))
		);
		assert_eq!(static_table_of("MATCH (a:person)-[:knows]->(b) SET b.seen = true", "b"), None);
		assert_eq!(
			static_table_of(
				"MATCH (a:person) MATCH (a)-[:knows]->(b:person) SET a.seen = true",
				"a"
			),
			None
		);
		assert_eq!(static_table_of("MATCH (a:person) MATCH (a:city) SET a.seen = true", "a"), None);
		assert_eq!(static_table_of("INSERT (n:person {name: 'x'}) SET n.age = 1", "n"), person);
		assert_eq!(
			static_table_of("MATCH p = (a:person)-[:knows]->(b:person) SET a.seen = true", "p"),
			None
		);
	}

	/// A plan that does not end in a projection ends in a projection of `*`; one that does is not
	/// given a second.
	#[test]
	fn a_plan_always_ends_in_one_projection() {
		for query in ["MATCH (n:person) SET n.age = 1", "MATCH (n:person) RETURN n.name AS name"] {
			let ir = bound(query);
			let projections = ops_of(&ir, LogTag::Project);
			assert_eq!(projections.len(), 1, "{query}");
		}
	}

	/// An anchor that pushes a predicate holding a statement is declined as an unsupported
	/// construct; the same predicate without a statement is pushed.
	#[test]
	fn an_anchor_pushing_a_statement_is_declined() {
		let statement = LogicalExpr::Subquery(Box::new(LogicalOp::Source(LogicalSource {
			source: SourceRef::Table("person".into()),
			schema: None,
			with: None,
			for_update: false,
		})));
		let declined = LogicalBackend.scan("person".into(), Some(statement)).now_or_never();
		let Some(Err(Planning::Decline(declined))) = declined else {
			panic!("the anchor is declined");
		};
		assert_eq!(declined.reason, "an anchor holding a statement");
		let plain = LogicalBackend
			.scan("person".into(), Some(LogicalExpr::Field("age".into())))
			.now_or_never();
		assert!(matches!(plain, Some(Ok(_))));
	}

	/// The logical ops of the block of the match's region, in order.
	fn tags_in_match(ir: &Ir) -> Vec<LogTag> {
		let matches = ops_of(ir, LogTag::Match);
		block_of(ir, matches[0])
			.into_iter()
			.filter_map(|op| match &ir.op(op).expect("the op exists").kind {
				OpKind::Logical(log) => Some(log.tag()),
				OpKind::Expr(_) | OpKind::Phys(_) => None,
			})
			.collect()
	}

	/// Whether `expected` is a subsequence of `tags`.
	fn in_order(tags: &[LogTag], expected: &[LogTag]) -> bool {
		let mut rest = tags.iter();
		expected.iter().all(|tag| rest.any(|held| held == tag))
	}

	/// The order of the ops of a plan: an identity loop over the scan, the binding of each row, and
	/// the projection loop over the bound rows, each a loop of its own, and the plan's value at the
	/// end.
	#[test]
	fn a_read_binds_a_loop_per_node_in_data_flow_order() {
		let ir = bound("MATCH (a:person) RETURN a.name AS n");
		let tags = tags_in_match(&ir);
		assert!(
			in_order(
				&tags,
				&[LogTag::Source, LogTag::Loop, LogTag::BindStep, LogTag::Loop, LogTag::Yield]
			),
			"{tags:?}"
		);
		assert_eq!(tags.iter().filter(|tag| **tag == LogTag::Loop).count(), 2, "{tags:?}");
		assert_eq!(tags.last(), Some(&LogTag::Yield));
	}

	/// A sort and a limit follow the loop that binds the rows, and the closing projection reads the
	/// limited rows in a loop of its own.
	#[test]
	fn a_sort_and_a_limit_stand_between_the_projection_and_the_closing_loop() {
		let ir = bound("MATCH (a:person) RETURN a.name AS n ORDER BY n LIMIT 1");
		let tags = tags_in_match(&ir);
		assert!(
			in_order(
				&tags,
				&[
					LogTag::Source,
					LogTag::Loop,
					LogTag::BindStep,
					LogTag::Sort,
					LogTag::Limit,
					LogTag::Loop,
					LogTag::Yield,
				]
			),
			"{tags:?}"
		);
	}
}
