//! What the VM's mapping reads of a relational pipeline: its clauses, and what a chain of
//! permission and layer nodes over a table source offers the access path.
//!
//! # The relational pipeline
//!
//! A relational tree is read by walking *down* from its top, collecting the
//! clauses the access-path chooser needs — the predicate, ordering, paging,
//! projection, fetch, version, timeout, `ONLY`, split and grouping — into a
//! [`PipelineClauses`], until the walk reaches either a leaf (`Source`,
//! `Values`, `Subquery`, `UnionAll`, `Match`) or a node it does not collect
//! ([`collect_pipeline`]). The mapping then maps the leaf with the whole
//! clause set in hand, through the access-path machinery in `select`, and puts
//! the clauses back on in pipeline order.
//!
//! Clauses are collected in the order a `SELECT` pipeline applies them, from
//! `ONLY` at the top down to the `WHERE` just above the leaf, and each at most
//! once. A clause met out of that order, a node the walk does not collect
//! (`PermissionFilter`, a projection layer, `Distinct`), or a statement in
//! relational position stops the collection: the node becomes the source the
//! clauses collected above it apply to — every one of them, in full. Nothing
//! collected above such a node is pushed beneath it as a per-row filter, a
//! limit or a threshold, which is what upholds the barrier rule for
//! `PermissionFilter` and the layers.
//!
//! A projection layer ([`LogicalOp::is_layer`]) is a `Project` of the form
//! `*, e₁ AS n₁, …` whose every column evaluates in a frame that is not the
//! caller's: the analyzer's encoding of a table's `COMPUTED` fields and field
//! `SELECT` permissions, stacked directly above the table's `PermissionFilter`.
//! The stack is the pipeline's source chain, not a projection stage, so the
//! walk stops at a layer as it stops at a `PermissionFilter`; a `Project` that
//! is not a layer is the user's and is collected as the projection clause.
//!
//! The barrier protects evaluation, not access-path choice. A chain of
//! `PermissionFilter` and layer nodes directly over a table `Source` — with
//! the filter the binding rules placed inside it, holding the conjuncts that
//! read no layer column — is offered the pipeline's predicate and ordering by
//! access-path selection exactly as a bare leaf is: an index lookup reads
//! keys, and the nodes above still judge every row the access path yields.
//! What the offer withholds, and why, is stated where the crossing happens, in
//! [`chain_offer`].
//!
//! # Joins
//!
//! A [`LogicalOp::Join`] is a node the walk does not collect: the clauses
//! above it apply to the joined rows in full. The conjuncts of the pipeline's
//! predicate that read the left row alone were moved beneath the join by the
//! binding rules, as a filter over the left input, so the left input is
//! offered no predicate of the pipeline's; its ordering is offered, less every
//! key that reads the join's alias or the row opaquely, since a join adds to
//! or removes from the left rows without changing them. A semi or anti join
//! lifted out of a table's `PERMISSIONS FOR select` predicate stands in the
//! source chain between the table's layers and its `PermissionFilter`, and is
//! one of the chain's nodes: it withholds nothing from the offer and
//! contributes nothing to it.

// Adding a `LogicalOp` variant must break the walk until it states whether the
// variant is collected, which a wildcard arm would silently absorb.
#![warn(clippy::wildcard_enum_match_arm)]

use std::collections::HashSet;
use std::sync::Arc;

use super::Planner;
use super::util::{expr_contains_matches, has_knn_operator};
use crate::err::{EngineError, Error};
use crate::exec::function::FunctionRegistry;
use crate::exec::logical::reads::{layer_column_names, reads_beneath, reads_row_opaquely};
use crate::exec::logical::{and, conjuncts, is_false};
use crate::expr::logical::{
	EvalFrame, EvalMode, FieldPath, JoinKind, LogicalAggregate, LogicalExpr, LogicalFetch,
	LogicalField, LogicalFields, LogicalFilter, LogicalJoin, LogicalLimit, LogicalLiteral,
	LogicalOnly, LogicalOp, LogicalOrdering, LogicalPermissionFilter, LogicalProject, LogicalSort,
	LogicalSource, LogicalSplit, LogicalTimeout, LogicalVersionScope, Schema, SourceRef,
};
use crate::val::{Strand, TableName};

/// The clauses collected above a pipeline's leaf, in the form the access-path
/// chooser and the pipeline assembler read them.
///
/// Every field is a clause the walk took at most once; `None` (or `false`,
/// or empty) means the tree had no such operator above the leaf. `project`
/// and `group` each carry the projection they own: a grouped query projects
/// inside its aggregation, and the `Project` above it, when present, carries
/// only `OMIT`.
#[derive(Debug, Default)]
pub(crate) struct PipelineClauses {
	pub(crate) only: bool,
	pub(crate) version: Option<LogicalExpr>,
	pub(crate) timeout: Option<LogicalExpr>,
	pub(crate) fetch: Vec<LogicalExpr>,
	pub(crate) project: Option<ProjectClause>,
	pub(crate) limit: Option<LogicalExpr>,
	pub(crate) start: Option<LogicalExpr>,
	pub(crate) order: Option<LogicalOrdering>,
	pub(crate) tempfiles: bool,
	pub(crate) group: Option<GroupClause>,
	pub(crate) split: Option<Vec<LogicalExpr>>,
	pub(crate) cond: Option<LogicalExpr>,
}

/// The `Project` operator's clauses.
#[derive(Debug)]
pub(crate) struct ProjectClause {
	pub(crate) fields: LogicalFields,
}

/// The `Aggregate` operator's clauses. An empty `group` is `GROUP ALL`.
#[derive(Debug)]
pub(crate) struct GroupClause {
	pub(crate) group: Vec<LogicalExpr>,
	pub(crate) fields: LogicalFields,
}

/// Where the collecting walk stopped.
pub(crate) enum PipelineStop {
	/// A leaf: `Source`, `Values`, `Subquery`, `UnionAll` or `Match`. The
	/// clauses apply over the access path chosen for it.
	Leaf(LogicalOp),
	/// A node compiled in place, as an operator over its own folded input.
	/// The clauses apply over it as an opaque source.
	Node(LogicalOp),
}

/// The pipeline position of a collectable clause, top to bottom. A clause is
/// taken only while it sits below every clause taken before it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Stage {
	Only,
	Version,
	Timeout,
	Fetch,
	Project,
	Limit,
	Sort,
	Aggregate,
	Split,
	Filter,
}

/// Whether a clause at `stage` may be taken after the clauses taken so far.
fn admits(taken: Option<Stage>, stage: Stage) -> bool {
	taken.is_none_or(|deepest| stage > deepest)
}

/// Whether a projection is `*` and nothing else, so the operators beneath it
/// produce the rows it returns.
fn is_star_only(fields: &LogicalFields) -> bool {
	*fields == LogicalFields::all()
}

/// Walk down from the top of a relational tree, collecting clauses in
/// pipeline order until a leaf or a node compiled in place. Pure: the
/// physical planner runs it before choosing an access path, and the fast-path
/// tests run it to obtain the shape a statement lowers to.
pub(crate) fn collect_pipeline(op: LogicalOp) -> (PipelineClauses, PipelineStop) {
	let mut clauses = PipelineClauses::default();
	let mut taken: Option<Stage> = None;
	let mut cur = op;
	loop {
		// A projection layer is part of the source chain, never the
		// projection clause; see the module docs.
		let layer = cur.is_layer();
		cur = match cur {
			LogicalOp::Only(LogicalOnly {
				input,
			}) if admits(taken, Stage::Only) => {
				clauses.only = true;
				taken = Some(Stage::Only);
				*input
			}
			LogicalOp::VersionScope(LogicalVersionScope {
				input,
				version,
			}) if admits(taken, Stage::Version) => {
				clauses.version = Some(version);
				taken = Some(Stage::Version);
				*input
			}
			LogicalOp::Timeout(LogicalTimeout {
				input,
				timeout,
			}) if admits(taken, Stage::Timeout) => {
				clauses.timeout = Some(timeout);
				taken = Some(Stage::Timeout);
				*input
			}
			LogicalOp::Fetch(LogicalFetch {
				input,
				fetches,
			}) if admits(taken, Stage::Fetch) => {
				clauses.fetch = fetches;
				taken = Some(Stage::Fetch);
				*input
			}
			LogicalOp::Project(LogicalProject {
				input,
				fields,
				..
			}) if !layer && admits(taken, Stage::Project) => {
				clauses.project = Some(ProjectClause {
					fields,
				});
				taken = Some(Stage::Project);
				*input
			}
			LogicalOp::Limit(LogicalLimit {
				input,
				limit,
				start,
			}) if admits(taken, Stage::Limit) => {
				clauses.limit = limit;
				clauses.start = start;
				taken = Some(Stage::Limit);
				*input
			}
			LogicalOp::Sort(LogicalSort {
				input,
				ordering,
				tempfiles,
			}) if admits(taken, Stage::Sort) => {
				clauses.order = Some(ordering);
				clauses.tempfiles = tempfiles;
				taken = Some(Stage::Sort);
				*input
			}
			// A grouped query projects inside its aggregation, so a `Project`
			// collected above it may carry `OMIT` and nothing else; one that
			// projects fields of its own applies over the finished aggregate.
			LogicalOp::Aggregate(LogicalAggregate {
				input,
				group,
				fields,
			}) if admits(taken, Stage::Aggregate)
				&& clauses.project.as_ref().is_none_or(|p| is_star_only(&p.fields)) =>
			{
				clauses.group = Some(GroupClause {
					group,
					fields,
				});
				taken = Some(Stage::Aggregate);
				*input
			}
			LogicalOp::Split(LogicalSplit {
				input,
				exprs,
			}) if admits(taken, Stage::Split) => {
				clauses.split = Some(exprs);
				taken = Some(Stage::Split);
				*input
			}
			LogicalOp::Filter(LogicalFilter {
				input,
				predicate,
			}) if admits(taken, Stage::Filter) => {
				clauses.cond = Some(predicate);
				taken = Some(Stage::Filter);
				*input
			}
			leaf @ (LogicalOp::Source(_)
			| LogicalOp::Values(_)
			| LogicalOp::Subquery(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Match(_)
			| LogicalOp::Write(_)) => return (clauses, PipelineStop::Leaf(leaf)),
			// A node the walk does not collect (a `Project` here is a layer),
			// a clause met out of pipeline order, or a statement in relational
			// position. Each is compiled in place and is opaque to the clauses
			// above it.
			node @ (LogicalOp::PermissionFilter(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Only(_)
			| LogicalOp::VersionScope(_)
			| LogicalOp::Timeout(_)
			| LogicalOp::Fetch(_)
			| LogicalOp::Project(_)
			| LogicalOp::Limit(_)
			| LogicalOp::Sort(_)
			| LogicalOp::Aggregate(_)
			| LogicalOp::Split(_)
			| LogicalOp::Filter(_)
			| LogicalOp::Explain(_)
			| LogicalOp::Eval(_)
			| LogicalOp::If(_)
			| LogicalOp::Throw(_)
			| LogicalOp::Break(_)
			| LogicalOp::Continue(_)
			| LogicalOp::Sequence(_)
			| LogicalOp::Let(_)
			| LogicalOp::Return(_)
			| LogicalOp::Foreach(_)
			| LogicalOp::Join(_)
			| LogicalOp::Info(_)
			| LogicalOp::Ddl(_)
			| LogicalOp::Sleep(_)) => return (clauses, PipelineStop::Node(node)),
		};
	}
}

/// One column of a projection layer, before its expressions are compiled:
/// the field it lands on, the guard of a field `SELECT` permission written
/// `WHERE p`, the value, the kind the value is coerced to, and the frame the
/// column evaluates in (see [`crate::exec::operators::layer`]).
pub(crate) struct LogicalLayerColumn {
	pub(crate) name: Strand,
	pub(crate) guard: Option<LogicalExpr>,
	pub(crate) value: LogicalExpr,
	pub(crate) kind: Option<crate::expr::Kind>,
	pub(crate) frame: EvalFrame,
}

/// The columns of projection layer `fields`, in plan order, each permission
/// column written `IF p THEN v ELSE NONE END` split into its guard and value.
pub(crate) fn layer_columns(fields: LogicalFields) -> Result<Vec<LogicalLayerColumn>, Error> {
	let LogicalFields::Select(fields) = fields else {
		return Err(EngineError::Internal(
			"a projection layer reached the planner as a VALUE projection".to_string(),
		)
		.into());
	};
	Ok(fields
		.into_iter()
		.filter_map(|field| {
			let LogicalField::Single {
				expr,
				alias,
				kind,
				frame,
			} = field
			else {
				return None;
			};
			let name = match alias {
				Some(alias) => Strand::new(alias.name()),
				None => Strand::new(expr.output_path().name()),
			};
			let (guard, value) = split_judgement(expr, &frame);
			Some(LogicalLayerColumn {
				name,
				guard,
				value,
				kind,
				frame,
			})
		})
		.collect())
}

/// The guard and the value of a permission column written
/// `IF p THEN v ELSE NONE END`, or `(None, expr)` for any other column; see
/// [`Planner::compile_layer`].
fn split_judgement(expr: LogicalExpr, frame: &EvalFrame) -> (Option<LogicalExpr>, LogicalExpr) {
	if matches!(frame.mode, EvalMode::Permission | EvalMode::MutablePermission)
		&& let LogicalExpr::If(judgement) = &expr
		&& judgement.branches.len() == 1
		&& matches!(judgement.otherwise, Some(LogicalExpr::Literal(LogicalLiteral::None)))
		&& let LogicalExpr::If(judgement) = expr
	{
		let (guard, value) = judgement.branches.into_iter().next().expect("one branch");
		return (Some(guard), value);
	}
	(None, expr)
}

/// One node of a chain over a table source, detached from its input.
enum ChainNode {
	Permission {
		predicate: LogicalExpr,
	},
	Layer {
		/// The layer's projection: `*` followed by its columns.
		fields: LogicalFields,
	},
	/// The user's conjuncts the binding rules moved beneath the layers: they
	/// read no column of a layer above them, and stand directly above the
	/// source or the chain's topmost permission node. A lookup join's right
	/// side carries the filter of the lifted subquery the same way.
	Filter {
		predicate: LogicalExpr,
	},
	/// A semi or anti join lifted out of a permission predicate.
	Join,
}

impl ChainNode {
	/// The field names a layer's columns land under; empty for any other
	/// node.
	fn column_names(&self) -> Vec<FieldPath> {
		match self {
			ChainNode::Permission {
				..
			}
			| ChainNode::Filter {
				..
			}
			| ChainNode::Join => Vec::new(),
			ChainNode::Layer {
				fields,
				..
			} => layer_column_names(fields),
		}
	}
}

/// Whether `join` is a semi or anti join lifted out of a permission
/// predicate, which stands in a table's source chain.
fn is_permission_join(join: &LogicalJoin) -> bool {
	join.frame.mode != EvalMode::Caller && matches!(join.kind, JoinKind::Semi | JoinKind::Anti)
}

/// Whether `node` is `PermissionFilter`, layer and permission-join nodes, in
/// any order and at least one deep, with the filters the binding rules placed
/// among them, directly over a table `Source` — the shape
/// [`Planner::fold_uncollected`] plans as one access path.
pub(crate) fn is_layer_chain_over_table(node: &LogicalOp) -> bool {
	let mut cur = node;
	let mut depth = 0usize;
	loop {
		match cur {
			LogicalOp::PermissionFilter(LogicalPermissionFilter {
				input,
				..
			}) => {
				depth += 1;
				cur = input;
			}
			LogicalOp::Filter(LogicalFilter {
				input,
				..
			}) => cur = input,
			LogicalOp::Project(LogicalProject {
				input,
				..
			}) if cur.is_layer() => {
				depth += 1;
				cur = input;
			}
			LogicalOp::Join(join) if is_permission_join(join) => {
				depth += 1;
				cur = join.left();
			}
			LogicalOp::Source(LogicalSource {
				source: SourceRef::Table(_),
				..
			}) => return depth > 0,
			LogicalOp::Source(_)
			| LogicalOp::Values(_)
			| LogicalOp::Match(_)
			| LogicalOp::Write(_)
			| LogicalOp::Subquery(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Join(_)
			| LogicalOp::Split(_)
			| LogicalOp::Aggregate(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Sort(_)
			| LogicalOp::Limit(_)
			| LogicalOp::Project(_)
			| LogicalOp::Fetch(_)
			| LogicalOp::Timeout(_)
			| LogicalOp::VersionScope(_)
			| LogicalOp::Only(_)
			| LogicalOp::Explain(_)
			| LogicalOp::Eval(_)
			| LogicalOp::If(_)
			| LogicalOp::Throw(_)
			| LogicalOp::Break(_)
			| LogicalOp::Continue(_)
			| LogicalOp::Sequence(_)
			| LogicalOp::Let(_)
			| LogicalOp::Return(_)
			| LogicalOp::Foreach(_)
			| LogicalOp::Info(_)
			| LogicalOp::Ddl(_)
			| LogicalOp::Sleep(_) => return false,
		}
	}
}

/// The table whose rows reach a `PermissionFilter` over `input`, and the
/// schema the analyzer resolved for it: the resolved `Source` beneath the
/// permission, layer, filter and permission-join nodes the analyzer and the
/// rules stack over it. `None` for any other shape, which carries no resolved
/// schema to name the table's computed fields.
pub(crate) fn permission_subject(input: &LogicalOp) -> Option<(TableName, Arc<Schema>)> {
	let mut cur = input;
	loop {
		cur = match cur {
			LogicalOp::PermissionFilter(LogicalPermissionFilter {
				input,
				..
			})
			| LogicalOp::Filter(LogicalFilter {
				input,
				..
			}) => input,
			LogicalOp::Project(LogicalProject {
				input,
				..
			}) if cur.is_layer() => input,
			LogicalOp::Join(join) if is_permission_join(join) => join.left(),
			LogicalOp::Source(LogicalSource {
				source,
				schema: Some(schema),
				..
			}) => {
				let table = match source {
					SourceRef::Table(table) => table.clone(),
					SourceRef::RecordId(rid) => rid.table.clone(),
					SourceRef::Mock(_) | SourceRef::Param(_) | SourceRef::Dynamic(_) => {
						return None;
					}
				};
				return Some((table, Arc::clone(schema)));
			}
			LogicalOp::Source(_)
			| LogicalOp::Values(_)
			| LogicalOp::Match(_)
			| LogicalOp::Subquery(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Join(_)
			| LogicalOp::Split(_)
			| LogicalOp::Aggregate(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Sort(_)
			| LogicalOp::Limit(_)
			| LogicalOp::Project(_)
			| LogicalOp::Fetch(_)
			| LogicalOp::Timeout(_)
			| LogicalOp::VersionScope(_)
			| LogicalOp::Only(_)
			| LogicalOp::Explain(_)
			| LogicalOp::Eval(_)
			| LogicalOp::If(_)
			| LogicalOp::Throw(_)
			| LogicalOp::Break(_)
			| LogicalOp::Continue(_)
			| LogicalOp::Sequence(_)
			| LogicalOp::Let(_)
			| LogicalOp::Return(_)
			| LogicalOp::Foreach(_)
			| LogicalOp::Info(_)
			| LogicalOp::Sleep(_)
			| LogicalOp::Write(_)
			| LogicalOp::Ddl(_) => return None,
		};
	}
}

/// The computed fields of `schema` a table permission predicate reads, as
/// [`ComputedFieldGuard::for_reads`](crate::exec::permission::ComputedFieldGuard::for_reads) takes
/// them: `None` when it reads none of them, `Some(None)` when it reads the row other than by name
/// ([`reads_row_opaquely`]), so any of them may be read, and otherwise the
/// names it reads.
pub(crate) fn computed_reads(
	predicate: &LogicalExpr,
	schema: &Schema,
	registry: &FunctionRegistry,
) -> Option<Option<HashSet<String>>> {
	let computed: Vec<&Strand> = schema
		.fields
		.iter()
		.filter(|field| field.computed.is_some())
		.filter_map(|field| field.path.as_field())
		.collect();
	if computed.is_empty() {
		return None;
	}
	if reads_row_opaquely(predicate, registry) {
		return Some(None);
	}
	let reads: HashSet<String> = computed
		.into_iter()
		.filter(|name| reads_beneath(predicate, &[FieldPath::field((*name).clone())]))
		.map(|name| name.as_str().to_owned())
		.collect();
	(!reads.is_empty()).then_some(Some(reads))
}

/// Split a chain [`is_layer_chain_over_table`] accepted into its nodes, top
/// to bottom, and the table `Source` beneath them.
fn peel_layer_chain(node: LogicalOp) -> (Vec<ChainNode>, LogicalOp) {
	let mut chain = Vec::new();
	let mut cur = node;
	loop {
		let layer = cur.is_layer();
		cur = if let LogicalOp::PermissionFilter(LogicalPermissionFilter {
			input,
			predicate,
			..
		}) = cur
		{
			chain.push(ChainNode::Permission {
				predicate,
			});
			*input
		} else if let LogicalOp::Filter(LogicalFilter {
			input,
			predicate,
		}) = cur
		{
			chain.push(ChainNode::Filter {
				predicate,
			});
			*input
		} else if layer
			&& let LogicalOp::Project(LogicalProject {
				input,
				fields,
				..
			}) = cur
		{
			chain.push(ChainNode::Layer {
				fields,
			});
			*input
		} else if let LogicalOp::Join(join) = cur {
			let [left, _right] = *join.inputs;
			chain.push(ChainNode::Join);
			left
		} else {
			return (chain, cur);
		};
	}
}

/// What the planner offers the access path of each table source of a
/// pipeline: the predicate and ordering access-path choice reads, and whether
/// a `LIMIT` can end the pipeline's read early. See
/// [`Planner::access_offer`].
pub(crate) struct AccessOffer {
	pub(crate) cond: Option<LogicalExpr>,
	pub(crate) order: Option<LogicalOrdering>,
	pub(crate) has_query_limit: bool,
	/// Whether a table permission predicate is part of `cond`
	/// ([`ChainOffer::permissioned`]).
	pub(crate) permissioned: bool,
}

impl Planner<'_> {
	/// The [`AccessOffer`] for the pipeline `op` heads, as the pipeline
	/// planner makes it: the clauses [`collect_pipeline`] takes, the
	/// predicate folded by [`Planner::fold_pipeline_cond`], and beneath a
	/// chain of permission and layer nodes what [`chain_offer`] admits of it.
	/// `versioned` says the pipeline has a `VERSION` of its own.
	///
	/// A grouped pipeline's `LIMIT` does not end its read: the grouping
	/// consumes every row first.
	pub(crate) async fn access_offer(&self, op: &LogicalOp, versioned: bool) -> AccessOffer {
		let parts = PipelineParts::of(op);
		let cond = self.fold_in_pipeline(&parts, parts.clauses.cond.clone(), versioned).await;
		let PipelineParts {
			clauses,
			node,
			..
		} = parts;
		let has_query_limit = clauses.limit.is_some() && clauses.group.is_none();
		match node {
			Some(node) if is_layer_chain_over_table(&node) => {
				let (chain, _) = peel_layer_chain(node);
				let offer = chain_offer(
					&chain,
					cond.as_ref(),
					clauses.order.as_ref(),
					self.function_registry(),
				);
				AccessOffer {
					cond: offer.cond,
					order: offer.order.cloned(),
					has_query_limit,
					permissioned: offer.permissioned,
				}
			}
			_ => AccessOffer {
				cond,
				order: clauses.order,
				has_query_limit,
				permissioned: false,
			},
		}
	}

	/// `conds`, each a predicate of the pipeline `op` heads, folded as
	/// [`Self::access_offer`] folds the pipeline's own predicate: what the
	/// access path is chosen from, so a caller that reads a predicate beside
	/// the access path's answer reads the same value operands.
	pub(crate) async fn fold_pipeline_conds(
		&self,
		op: &LogicalOp,
		versioned: bool,
		conds: Vec<LogicalExpr>,
	) -> Vec<LogicalExpr> {
		let parts = PipelineParts::of(op);
		let mut folded = Vec::with_capacity(conds.len());
		for cond in conds {
			folded.extend(self.fold_in_pipeline(&parts, Some(cond), versioned).await);
		}
		folded
	}

	async fn fold_in_pipeline(
		&self,
		parts: &PipelineParts,
		cond: Option<LogicalExpr>,
		versioned: bool,
	) -> Option<LogicalExpr> {
		self.fold_pipeline_cond(
			cond,
			versioned,
			parts.fields(),
			&parts.sources,
			parts.node.as_ref(),
			&parts.predicate_tables,
		)
		.await
	}
}

/// What the collecting walk over a pipeline leaves for folding its predicate:
/// the clauses, the sources and node it stopped at, and the tables whose rows
/// the predicate is evaluated against.
struct PipelineParts {
	clauses: PipelineClauses,
	sources: Vec<LogicalOp>,
	node: Option<LogicalOp>,
	predicate_tables: Vec<TableName>,
}

impl PipelineParts {
	fn of(op: &LogicalOp) -> Self {
		let (clauses, stop) = collect_pipeline(op.clone());
		let (sources, node) = match stop {
			PipelineStop::Leaf(LogicalOp::UnionAll(union)) => (union.inputs, None),
			PipelineStop::Leaf(leaf) => (vec![leaf], None),
			PipelineStop::Node(node) => (Vec::new(), Some(node)),
		};
		let predicate_tables: Vec<TableName> = {
			let mut tables = Vec::new();
			for source in &sources {
				crate::exec::planner::select::tables_beneath(source, &mut tables);
			}
			if let Some(node) = &node {
				crate::exec::planner::select::tables_beneath(node, &mut tables);
			}
			tables.into_iter().cloned().collect()
		};
		Self {
			clauses,
			sources,
			node,
			predicate_tables,
		}
	}

	/// The fields the pipeline projects or groups to.
	fn fields(&self) -> Option<&LogicalFields> {
		match (&self.clauses.group, &self.clauses.project) {
			(Some(group), _) => Some(&group.fields),
			(None, Some(project)) => Some(&project.fields),
			(None, None) => None,
		}
	}
}

/// What a chain of permission, layer and filter nodes admits of the
/// pipeline's predicate and ordering to the source beneath it.
struct ChainOffer<'a> {
	/// The predicate offered to the access path: the conjuncts of the chain's
	/// filters that read no column of a layer beneath them, the pipeline's
	/// conjuncts that read no layer column, and the table permission
	/// predicates where no part of the offer is evaluated per row.
	cond: Option<LogicalExpr>,
	order: Option<&'a LogicalOrdering>,
	/// Whether a table permission predicate is part of `cond`: the access
	/// path chosen from it may then read at the definer's bound, which a
	/// checked caller's `EXPLAIN` must not print.
	permissioned: bool,
}

/// The offer a chain admits; see [`ChainOffer`].
///
/// A user predicate that is `false` outright admits no row, and the folder
/// that reduced it to `false` has already judged that nothing it discarded
/// had to be evaluated; the source is planned as an empty scan. That is
/// decided on the predicate as the pipeline holds it, and on each filter of
/// the chain: a `false` conjunct left beside conjuncts the layers withhold is
/// not a refusal, since the withheld conjuncts read columns the rows must
/// still reach, so the literal is withheld with them and drives nothing.
///
/// A conjunct or order key of the pipeline's is withheld when it reads a
/// layer column — by the field's name, or opaquely: through a row parameter,
/// the document, a call evaluated against the row, or a nested plan, any of
/// which may reach a column the layers set — since beneath the chain the
/// field is the stored value or absent. A conjunct of a filter inside the
/// chain is judged the same way against the layers beneath that filter: the
/// binding rules place a filter beneath every layer whose columns it reads,
/// so its conjuncts are offered, while a filter over a layer whose column it
/// reads — one a join's left input carries — is withheld and judged where it
/// stands.
fn chain_offer<'a>(
	chain: &[ChainNode],
	cond: Option<&LogicalExpr>,
	order: Option<&'a LogicalOrdering>,
	registry: &FunctionRegistry,
) -> ChainOffer<'a> {
	let scoped = |expr: &LogicalExpr| has_knn_operator(expr) || expr_contains_matches(expr);
	let withheld_by = |expr: &LogicalExpr, columns: &[FieldPath]| {
		reads_beneath(expr, columns) || reads_row_opaquely(expr, registry)
	};
	let columns: Vec<FieldPath> = chain.iter().flat_map(ChainNode::column_names).collect();
	let upper =
		cond.and_then(|cond| retain_conjuncts(cond, |c| !is_false(c) && !withheld_by(c, &columns)));
	let order = order.filter(|order| match order {
		LogicalOrdering::Random => true,
		LogicalOrdering::Order(keys) => !keys.iter().any(|key| withheld_by(&key.value, &columns)),
	});
	let mut refused = cond.is_some_and(is_false);
	let mut pushed = Vec::new();
	for (i, node) in chain.iter().enumerate() {
		let ChainNode::Filter {
			predicate,
		} = node
		else {
			continue;
		};
		refused |= is_false(predicate);
		let beneath: Vec<FieldPath> =
			chain[i + 1..].iter().flat_map(ChainNode::column_names).collect();
		pushed.extend(
			conjuncts(predicate)
				.into_iter()
				.filter(|c| !is_false(c) && !withheld_by(c, &beneath))
				.cloned(),
		);
	}
	let pushed = pushed.into_iter().reduce(and);
	let mut offer = match (pushed, upper) {
		(Some(pushed), Some(upper)) => Some(and(pushed, upper)),
		(pushed, upper) => pushed.or(upper),
	};
	// A permission predicate joins the offer only where no part of the offer
	// is evaluated per row: a KNN search evaluates its residual per candidate
	// in the caller's frame, and MATCHES binds to the pipeline's scope.
	let user_scoped = offer.as_ref().is_some_and(scoped);
	let mut permissioned = false;
	for node in chain {
		let ChainNode::Permission {
			predicate,
			..
		} = node
		else {
			continue;
		};
		if is_false(predicate) {
			refused = true;
			break;
		}
		if user_scoped || scoped(predicate) {
			continue;
		}
		permissioned = true;
		offer = Some(match offer {
			Some(offer) => and(offer, predicate.clone()),
			None => predicate.clone(),
		});
	}
	if refused {
		return ChainOffer {
			cond: Some(LogicalExpr::Literal(LogicalLiteral::Bool(false))),
			order,
			permissioned: false,
		};
	}
	ChainOffer {
		cond: offer,
		order,
		permissioned,
	}
}

/// The conjuncts of `cond`'s top-level `AND` chain for which `keep` holds,
/// joined back with `AND`; `None` when none is kept.
fn retain_conjuncts(
	cond: &LogicalExpr,
	keep: impl Fn(&LogicalExpr) -> bool,
) -> Option<LogicalExpr> {
	conjuncts(cond).into_iter().filter(|c| keep(c)).cloned().reduce(and)
}

/// Whether a statement changes the catalog the statements after it would be
/// compiled against.
///
/// A schema statement does, wherever it sits inside the statement: one reached
/// through a block, a `FOR` body or a subquery has still run by the time the
/// next statement of the enclosing block does.
pub(crate) fn changes_the_catalog(op: &LogicalOp) -> bool {
	if matches!(op, LogicalOp::Ddl(_)) {
		return true;
	}
	let mut found = false;
	let _ = op.visit_ops_deep(&mut |node| {
		if matches!(node, LogicalOp::Ddl(_)) {
			found = true;
			return std::ops::ControlFlow::Break(());
		}
		std::ops::ControlFlow::Continue(())
	});
	found
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::expr::Kind;
	use crate::expr::logical::{EvalFrame, lower_select};
	use crate::syn;

	fn lower(sql: &str) -> LogicalOp {
		let ast = syn::parse(sql).expect("parse");
		let mut exprs = ast.expressions;
		assert_eq!(exprs.len(), 1, "expected one statement in {sql:?}");
		let surrealdb_expr::parser::TopLevelExpr::Expr(crate::expr::Expr::Select(select)) =
			exprs.remove(0)
		else {
			panic!("`{sql}` is not a SELECT");
		};
		lower_select(*select).expect("SELECT lowers")
	}

	fn leaf(stop: PipelineStop) -> LogicalOp {
		match stop {
			PipelineStop::Leaf(leaf) => leaf,
			PipelineStop::Node(node) => panic!("expected a leaf, stopped at {}", node.name()),
		}
	}

	#[test]
	fn every_clause_is_collected_from_a_full_statement() {
		// SPLIT and GROUP are mutually exclusive in one statement, so the
		// grouped statement carries every other clause and SPLIT is collected
		// from a statement of its own.
		let (clauses, stop) = collect_pipeline(lower(
			"SELECT a, b OMIT c FROM ONLY t WHERE a > 1 GROUP BY a ORDER BY a LIMIT 1 \
			 START 2 FETCH f VERSION d'2024-01-01T00:00:00Z' TIMEOUT 1s TEMPFILES",
		));
		assert!(clauses.only);
		assert!(clauses.version.is_some());
		assert!(clauses.timeout.is_some());
		assert_eq!(clauses.fetch.len(), 1);
		assert!(clauses.project.is_some(), "the grouped statement omits, so it projects");
		assert!(clauses.limit.is_some());
		assert!(clauses.start.is_some());
		assert!(clauses.order.is_some());
		assert!(clauses.tempfiles);
		assert!(clauses.group.is_some());
		assert!(clauses.split.is_none());
		assert!(clauses.cond.is_some());
		assert!(matches!(leaf(stop), LogicalOp::Source(_)));

		let (clauses, stop) = collect_pipeline(lower("SELECT a FROM t WHERE a > 1 SPLIT a"));
		assert!(clauses.split.is_some());
		assert!(clauses.group.is_none());
		assert!(clauses.cond.is_some());
		assert!(matches!(leaf(stop), LogicalOp::Source(_)));
	}

	#[test]
	fn a_grouped_statement_without_omit_has_no_project() {
		let (clauses, _) = collect_pipeline(lower("SELECT count() FROM t GROUP ALL"));
		assert!(clauses.project.is_none());
		let group = clauses.group.expect("grouped");
		assert!(group.group.is_empty(), "GROUP ALL");
	}

	#[test]
	fn several_sources_stop_at_the_union() {
		let (clauses, stop) = collect_pipeline(lower("SELECT * FROM a, b WHERE x = 1"));
		assert!(clauses.cond.is_some());
		assert!(matches!(leaf(stop), LogicalOp::UnionAll(_)));
	}

	#[test]
	fn a_nested_select_is_a_leaf_of_the_outer_pipeline() {
		let (clauses, stop) = collect_pipeline(lower("SELECT * FROM (SELECT * FROM t) LIMIT 1"));
		assert!(clauses.limit.is_some());
		assert!(matches!(leaf(stop), LogicalOp::Subquery(_)));
	}

	#[test]
	fn a_clause_out_of_pipeline_order_stops_the_walk() {
		// A filter above a sort is not a shape lowering produces; the sort
		// and everything under it compile as their own pipeline.
		let LogicalOp::Project(LogicalProject {
			input,
			..
		}) = lower("SELECT * FROM t ORDER BY a")
		else {
			panic!("expected a projection on top");
		};
		let op = LogicalOp::Filter(LogicalFilter {
			input,
			predicate: LogicalExpr::Literal(LogicalLiteral::Bool(true)),
		});
		let (clauses, stop) = collect_pipeline(op);
		assert!(clauses.cond.is_some());
		assert!(clauses.order.is_none());
		assert!(matches!(stop, PipelineStop::Node(LogicalOp::Sort(_))));
	}

	#[test]
	fn a_projection_with_fields_is_not_folded_into_an_aggregate_beneath_it() {
		let LogicalOp::Project(LogicalProject {
			input,
			..
		}) = lower("SELECT a FROM t")
		else {
			panic!("expected a projection on top");
		};
		let LogicalOp::Source(_) = *input else {
			panic!("expected a bare source");
		};
		let op = LogicalOp::Project(LogicalProject {
			input: Box::new(LogicalOp::Aggregate(LogicalAggregate {
				input,
				group: vec![],
				fields: LogicalFields::all(),
			})),
			fields: LogicalFields::Select(vec![LogicalField::user(
				LogicalExpr::Field("a".into()),
				None,
			)]),
			omit: vec![],
		});
		let (clauses, stop) = collect_pipeline(op);
		assert!(clauses.project.is_some());
		assert!(clauses.group.is_none());
		assert!(matches!(stop, PipelineStop::Node(LogicalOp::Aggregate(_))));
	}

	fn table(name: &str) -> LogicalOp {
		LogicalOp::Source(LogicalSource {
			source: SourceRef::Table(name.into()),
			schema: None,
			with: None,
			for_update: false,
		})
	}

	/// A layer whose columns land under `names`, each a computed literal.
	fn layer_fields(names: &[&str]) -> LogicalFields {
		let mut fields = vec![LogicalField::All];
		fields.extend(names.iter().map(|name| {
			LogicalField::column(
				*name,
				LogicalExpr::Literal(LogicalLiteral::Integer(1)),
				Some(Kind::Int),
				EvalFrame::computed(None),
			)
		}));
		LogicalFields::Select(fields)
	}

	fn layer_over(input: LogicalOp, names: &[&str]) -> LogicalOp {
		LogicalOp::Project(LogicalProject {
			input: Box::new(input),
			fields: layer_fields(names),
			omit: vec![],
		})
	}

	#[test]
	fn a_layer_stops_the_walk_as_a_permission_filter_does() {
		let op = LogicalOp::Filter(LogicalFilter {
			input: Box::new(layer_over(table("t"), &["c"])),
			predicate: LogicalExpr::Literal(LogicalLiteral::Bool(true)),
		});
		let (clauses, stop) = collect_pipeline(op);
		assert!(clauses.cond.is_some(), "the user predicate stays above the layer");
		assert!(clauses.project.is_none(), "a layer is not the projection clause");
		assert!(matches!(&stop, PipelineStop::Node(node) if node.is_layer()));

		// The user's projection over a layer is still collected; the layer
		// beneath it is where the walk stops.
		let op = LogicalOp::Project(LogicalProject {
			input: Box::new(layer_over(table("t"), &["c"])),
			fields: LogicalFields::Select(vec![LogicalField::user(
				LogicalExpr::Field("c".into()),
				None,
			)]),
			omit: vec![],
		});
		let (clauses, stop) = collect_pipeline(op);
		assert!(clauses.project.is_some());
		assert!(matches!(&stop, PipelineStop::Node(node) if node.is_layer()));
	}
}
