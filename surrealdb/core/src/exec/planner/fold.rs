//! The physical planner as a fold over the logical plan.
//!
//! [`Planner::fold`] takes one [`LogicalOp`] and returns one `ExecOperator`.
//! Every variant is compiled from its already-compiled inputs, so a node the
//! statement grammar has no clause for — a `PermissionFilter`, a projection
//! layer — compiles the same way as one it does.
//!
//! # Statements
//!
//! A statement operator compiles directly: `Eval` to an `ExprPlan`, `Sequence`
//! to a `SequencePlan` of its folded statements, `Let`, `Return`, `Foreach`,
//! `Info` and `Sleep` to their operators, `Explain` to an `ExplainPlan` or
//! `AnalyzePlan` over the folded input, and a GQL `Match` to the plan
//! `plan_match` builds. Branches and bodies compile eagerly: lowering has
//! already decided the engine for the whole statement, so nothing is deferred
//! to run time.
//!
//! # The relational pipeline
//!
//! A relational tree is compiled by walking *down* from its top, collecting
//! the clauses the access-path chooser needs — the predicate, ordering, paging,
//! projection, fetch, version, timeout, `ONLY`, split and grouping — into a
//! [`PipelineClauses`], until the walk reaches either a leaf (`Source`,
//! `Values`, `Subquery`, `UnionAll`, `Match`) or a node it does not collect. The
//! leaf is then compiled with the whole clause set in hand, through the
//! access-path machinery in `select`, and the clauses are wrapped back on in
//! pipeline order.
//!
//! Clauses are collected in the order a `SELECT` pipeline applies them, from
//! `ONLY` at the top down to the `WHERE` just above the leaf, and each at most
//! once. A clause met out of that order, a node the walk does not collect
//! (`PermissionFilter`, a projection layer, `Distinct`), or a statement in
//! relational position stops the collection: it is compiled in place as an
//! operator over its own input, and becomes the source the clauses collected
//! above it apply to — every one of them, in full. Nothing collected above such
//! a node is pushed beneath it as a per-row filter, a limit or a threshold,
//! which is what upholds the barrier rule for `PermissionFilter` and the
//! layers.
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
//! read no layer column — is planned with the pipeline's predicate and
//! ordering offered to access-path selection exactly as a bare leaf is: an
//! index lookup reads keys, and the nodes above still judge every row the
//! access path yields. What the offer withholds, and why, is stated where
//! the crossing happens, in [`Planner::fold_uncollected`].
//!
//! # Joins
//!
//! A [`LogicalOp::Join`] is a node the walk does not collect: the clauses
//! above it apply to the joined rows in full. The conjuncts of the pipeline's
//! predicate that read the left row alone were moved beneath the join by the
//! binding rules, as a filter over the left input, so the left input is
//! planned with no predicate of the pipeline's offered to its access path;
//! its ordering is offered, less every key that reads the join's alias or the
//! row opaquely, since a join adds to or removes from the left rows without
//! changing them. The right input compiles in one of two ways
//! ([`Planner::compile_join_parts`]): when the join's one key pair is
//! `(left key, alias.id)` — an `IN (SELECT VALUE id FROM t …)` — and the
//! right input is a table, under any of its permission filter, layers, a
//! filter and a permission join, it becomes a [`LookupJoin`] that reads the
//! table by the left keys, one batched read per left batch, with that chain
//! compiled over a [`RecordBatchFetch`] leaf;
//! otherwise the right input is folded like any plan and the two sides meet
//! in a [`HashJoin`] with the right side built. A semi or anti join lifted out
//! of a table's `PERMISSIONS FOR select` predicate stands in the source chain
//! between the table's layers and its `PermissionFilter`, and is planned as
//! one of the chain's nodes: it withholds nothing from the offer, contributes
//! nothing to it, and its expressions and right side compile in the
//! permission frame.

// Adding a `LogicalOp` variant must break the fold until it states how that
// variant compiles, which a wildcard arm would silently absorb.
#![warn(clippy::wildcard_enum_match_arm)]

use std::sync::Arc;

use surrealdb_cnf::PROTECTED_PARAM_NAMES;

use super::Planner;
use super::select::{FilterAction, PlannedSource, TopKPushdownRequest};
use super::util::{expr_contains_matches, has_knn_operator};
use crate::err::{EngineError, Error};
use crate::exec::function::FunctionRegistry;
use crate::exec::logical::reads::{layer_column_names, reads_beneath, reads_row_opaquely};
use crate::exec::logical::{and, conjuncts, is_false, plan_may_raise};
use crate::exec::operators::scan::resolved::EnforcementMode;
use crate::exec::operators::{
	AnalyzePlan, DatabaseInfoPlan, Distinct, ExplainPlan, ExprPlan, Filter, ForeachPlan, HashJoin,
	IndexInfoPlan, JoinFrame, JoinType, LayerColumn, LetPlan, LookupJoin, NamespaceInfoPlan,
	PermissionFilterExec, ProjectLayer, RecordBatchFetch, ReturnPlan, RootInfoPlan, SequencePlan,
	SleepPlan, TableInfoPlan, UserInfoPlan,
};
use crate::exec::physical_expr::Literal as PhysicalLiteral;
use crate::exec::{CardinalityHint, Error as ExecError, ExecOperator, PhysicalExpr};
use crate::expr::logical::{
	EvalFrame, EvalMode, FieldPath, JoinKind, LogicalAggregate, LogicalDistinct, LogicalEval,
	LogicalExplain, LogicalExpr, LogicalFetch, LogicalField, LogicalFields, LogicalFilter,
	LogicalForeach, LogicalInfo, LogicalJoin, LogicalLet, LogicalLimit, LogicalLiteral,
	LogicalMatch, LogicalOnly, LogicalOp, LogicalOrdering, LogicalPermissionFilter, LogicalProject,
	LogicalReturn, LogicalSequence, LogicalSleep, LogicalSort, LogicalSource, LogicalSplit,
	LogicalTimeout, LogicalVersionScope, RelationAlias, Schema, SourceRef,
};
use crate::iam::AuthLimit;
use crate::val::{Strand, TableName, Value};

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
	pub(crate) omit: Vec<LogicalExpr>,
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
				omit,
			}) if !layer && admits(taken, Stage::Project) => {
				clauses.project = Some(ProjectClause {
					fields,
					omit,
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
			| LogicalOp::Match(_)) => return (clauses, PipelineStop::Leaf(leaf)),
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
			| LogicalOp::Sequence(_)
			| LogicalOp::Let(_)
			| LogicalOp::Return(_)
			| LogicalOp::Foreach(_)
			| LogicalOp::Join(_)
			| LogicalOp::Info(_)
			| LogicalOp::Sleep(_)) => return (clauses, PipelineStop::Node(node)),
		};
	}
}

impl<'ctx> Planner<'ctx> {
	/// Compile a logical plan into an operator tree. One operator in, one
	/// operator out; see the [module docs](self) for how each kind compiles.
	pub(crate) async fn fold(&self, op: LogicalOp) -> Result<Arc<dyn ExecOperator>, Error> {
		match op {
			// A GQL `MATCH` plan is compiled whole (`match_plan.rs`).
			LogicalOp::Match(LogicalMatch {
				plan,
			}) => Box::pin(self.plan_match(*plan)).await,
			// `EXPLAIN` describes the tree everything below it compiles to.
			LogicalOp::Explain(LogicalExplain {
				input,
				analyze,
				format,
			}) => {
				let plan = Box::pin(self.fold(*input)).await?;
				Ok(if analyze {
					Arc::new(AnalyzePlan {
						plan,
						format,
						redact_volatile_explain_attrs: self.ctx.redact_volatile_explain_attrs(),
					})
				} else {
					Arc::new(ExplainPlan {
						plan,
						format,
					})
				})
			}
			LogicalOp::Eval(LogicalEval {
				expr,
			}) => {
				let expr = self.physical_expr(expr).await?;
				Ok(Arc::new(ExprPlan::new(expr)))
			}
			LogicalOp::Sequence(LogicalSequence {
				stmts,
			}) => self.fold_sequence(stmts).await,
			LogicalOp::Let(stmt) => self.fold_let(stmt).await,
			LogicalOp::Return(LogicalReturn {
				what,
				fetch,
			}) => {
				let inner = Box::pin(self.fold(*what)).await?;
				let inner = self.plan_fetch(fetch, inner).await?;
				Ok(Arc::new(ReturnPlan::new(inner)))
			}
			LogicalOp::Foreach(LogicalForeach {
				param,
				range,
				body,
			}) => {
				let range = self.physical_expr(range).await?;
				// The loop parameter binds around the body, whatever it is
				// named, so a read of it there is the element the loop binds
				// and never the ambient context's value.
				let inner = self.shadowing([param.as_str().to_owned()]);
				let body = Box::pin(inner.fold(*body)).await?;
				Ok(Arc::new(ForeachPlan::new(param, range, body)))
			}
			LogicalOp::Info(info) => self.fold_info(*info).await,
			LogicalOp::Sleep(LogicalSleep {
				duration,
			}) => fold_sleep(&duration),
			relational @ (LogicalOp::Source(_)
			| LogicalOp::Values(_)
			| LogicalOp::Subquery(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Join(_)
			| LogicalOp::Filter(_)
			| LogicalOp::PermissionFilter(_)
			| LogicalOp::Split(_)
			| LogicalOp::Aggregate(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Sort(_)
			| LogicalOp::Limit(_)
			| LogicalOp::Project(_)
			| LogicalOp::Fetch(_)
			| LogicalOp::Timeout(_)
			| LogicalOp::VersionScope(_)
			| LogicalOp::Only(_)) => {
				let (clauses, stop) = collect_pipeline(relational);
				self.compile_pipeline(stop, clauses).await
			}
		}
	}

	/// A block: nothing yields `NONE`, one statement is that statement, and
	/// more run in sequence yielding the last.
	///
	/// A block's bindings are its own, so a one-statement block collapses to
	/// its statement only when that statement does not bind: `{ LET $x = 1 }`
	/// keeps its `SequencePlan`, which is what stops the `LET` reaching the
	/// enclosing block, `FOR` body or statement.
	async fn fold_sequence(&self, stmts: Vec<LogicalOp>) -> Result<Arc<dyn ExecOperator>, Error> {
		match stmts.len() {
			0 => Ok(Arc::new(ExprPlan::new(Arc::new(PhysicalLiteral(Value::None))))),
			1 => {
				let stmt =
					Box::pin(self.fold(stmts.into_iter().next().expect("length checked"))).await?;
				if stmt.mutates_context() {
					Ok(Arc::new(SequencePlan::new(vec![stmt])))
				} else {
					Ok(stmt)
				}
			}
			_ => {
				let mut compiled = Vec::with_capacity(stmts.len());
				for stmt in stmts {
					compiled.push(Box::pin(self.fold(stmt)).await?);
				}
				Ok(Arc::new(SequencePlan::new(compiled)))
			}
		}
	}

	/// `LET $name: kind = value`. A protected name is rejected here, before
	/// anything runs, so a `LET` inside a block or a `FOR` body fails the same
	/// way as one at the top level.
	async fn fold_let(&self, stmt: LogicalLet) -> Result<Arc<dyn ExecOperator>, Error> {
		let LogicalLet {
			name,
			kind,
			value,
		} = stmt;
		if PROTECTED_PARAM_NAMES.contains(&name.as_str()) {
			return Err(ExecError::InvalidParam {
				name,
			}
			.into());
		}
		let value = Box::pin(self.fold(*value)).await?;
		Ok(Arc::new(LetPlan::new(Strand::new(name), kind, value)))
	}

	/// `INFO FOR …`: one operator per subject.
	async fn fold_info(&self, info: LogicalInfo) -> Result<Arc<dyn ExecOperator>, Error> {
		Ok(match info {
			LogicalInfo::Root {
				structure,
				version,
			} => Arc::new(RootInfoPlan::new(structure, self.optional_physical_expr(version).await?)),
			LogicalInfo::Ns {
				structure,
				version,
			} => Arc::new(NamespaceInfoPlan::new(
				structure,
				self.optional_physical_expr(version).await?,
			)),
			LogicalInfo::Db {
				structure,
				version,
			} => Arc::new(DatabaseInfoPlan::new(
				structure,
				self.optional_physical_expr(version).await?,
			)),
			LogicalInfo::Table {
				table,
				structure,
				version,
			} => {
				let table = self.physical_expr_as_name(table).await?;
				let version = self.optional_physical_expr(version).await?;
				Arc::new(TableInfoPlan::new(table, structure, version))
			}
			LogicalInfo::User {
				user,
				base,
				structure,
			} => {
				let user = self.physical_expr_as_name(user).await?;
				Arc::new(UserInfoPlan::new(user, base, structure))
			}
			LogicalInfo::Index {
				index,
				table,
				structure,
			} => {
				let index = self.physical_expr_as_name(index).await?;
				let table = self.physical_expr_as_name(table).await?;
				Arc::new(IndexInfoPlan::new(index, table, structure))
			}
		})
	}

	/// An optional scalar, compiled when present.
	async fn optional_physical_expr(
		&self,
		expr: Option<LogicalExpr>,
	) -> Result<Option<Arc<dyn PhysicalExpr>>, Error> {
		match expr {
			Some(expr) => Ok(Some(self.physical_expr(expr).await?)),
			None => Ok(None),
		}
	}

	/// Compile a node the pipeline walk does not collect, as an operator over
	/// its own input.
	///
	/// `PermissionFilter`, a projection layer and `Distinct` wrap their input
	/// directly. Anything else — a clause met out of pipeline order, or a
	/// statement in relational position — is a tree of its own and folds as
	/// one, so its clauses are collected afresh from its top.
	///
	/// `version` is the enclosing `VERSION`, resolved by the pipeline that
	/// collected it, so an access path chosen here refuses what a versioned
	/// read refuses. `cond`, `order` and `downstream_topk` are the predicate,
	/// ordering and top-k hint the pipeline collected above `node`; they are
	/// read only by the barrier crossing below and are never applied here —
	/// the pipeline applies them, in full, above whatever this returns.
	///
	/// # The barrier crossing
	///
	/// `PermissionFilter` and the layers fix the record image the operators
	/// above them may see: nothing a user wrote is evaluated beneath them, and
	/// nothing a user wrote is pushed beneath as a per-row filter, a limit, a
	/// top-k threshold or a pre-decode filter. Choosing the access path is not
	/// evaluation. An index lookup reads keys, and the nodes above still judge
	/// every row the access path yields, so the record image is unchanged
	/// whichever path is chosen. Therefore a chain of `PermissionFilter` and
	/// layer nodes, in any order, directly over a table `Source` is compiled
	/// by planning that source with the pipeline's predicate and ordering
	/// offered to access-path selection — B-tree, full-text, KNN, index union
	/// and bitmap fusion alike — exactly as a bare `Source` leaf is, and
	/// wrapping the nodes back on over the result, innermost first. The
	/// pipeline's `Filter` stays whole above the chain, as does its `Sort`
	/// unless the scan's own ordering satisfies it, which is a property of the
	/// scan and not a filter. A KNN or MATCHES conjunct consumed beneath is
	/// evaluated again above, binding to the pipeline's KNN and MATCHES scopes
	/// as it does over a bare leaf. A layer never drops rows, so what a scan
	/// beneath a chain receives is the same whether the chain holds layers,
	/// permissions or both.
	///
	/// The chain may hold a `Filter` of its own: the binding rules move the
	/// conjuncts of the user's predicate that read no layer column to directly
	/// above the source, or above the chain's topmost permission node. That
	/// filter is the user's predicate as far as the chain is concerned. What
	/// the offer holds, and what it withholds:
	///
	/// - The chain's filter is offered whole. Every conjunct of the pipeline's predicate whose root
	///   field is a column of some layer in the chain is withheld, and so is the ordering when one
	///   of its keys reads such a column. Beneath the layer the field is the stored value, or
	///   absent: an index over a restricted field would report which records hold a value the
	///   caller may not read, and an index over a stored key under a computed name would answer for
	///   a value the row above does not carry. A conjunct of the pipeline's predicate over a field
	///   no layer names reads the same value beneath the stack as above it, so it is offered too;
	///   after the rules such a conjunct holds a KNN or MATCHES operator, which the pipeline
	///   evaluates again above.
	/// - The chain's own `PermissionFilter` predicates join the offer, as an index over a
	///   permission's comparison is as sound as one over the user's — except when the offered
	///   predicate carries a KNN or MATCHES operator, or the permission predicate does. A KNN
	///   search evaluates its residual per candidate in the caller's frame, and a permission
	///   predicate must only ever run in the permission frame. A predicate of `false` (`PERMISSIONS
	///   … NONE`), or a user predicate that is `false` outright, makes the whole offer `false`, so
	///   the source is planned as an empty scan. A `false` conjunct beside withheld conjuncts is
	///   withheld with them: the rows must still reach the columns those conjuncts read.
	/// - Where the chain holds no permission node, the chain's filter is the scan predicate: the
	///   scan applies it per row, so a row it rejects is never decoded into a layer, and the filter
	///   node goes — or keeps the conjuncts the access path reports as its residual, when an index
	///   answered some of them and the scan applies none. Beneath a permission node the filter is
	///   compiled in place: the table permission is judged before the user's predicate, so the scan
	///   must not evaluate a user conjunct on a row the permission removes.
	/// - The pipeline's paging (`scan_limit`, `scan_start`) is offered to the scan beneath a chain
	///   of layers alone: a layer sets columns on a row and never drops or reorders one, so the
	///   rows the scan counts are the rows the pipeline's `Limit` would have counted. The caller
	///   offers paging only when nothing of its own stands between the chain and its `Limit` — no
	///   predicate left above the chain, no split, no grouping. A chain holding a permission node
	///   or a permission join is offered none: the node drops rows the scan has counted. Nor is a
	///   chain that withholds the pipeline's ordering (an order key reads a layer column): the
	///   `Sort` above the chain must see every row, so the paging stays above the `Sort`.
	///
	/// The result reports, as [`PlannedSource`] does for a plain source, whether the scan took the
	/// paging ([`PlannedSource::limit_pushed`]); its predicate is always the pipeline's to judge
	/// above ([`FilterAction::UseOriginal`]).
	#[allow(clippy::too_many_arguments)]
	pub(crate) async fn fold_uncollected(
		&self,
		node: LogicalOp,
		version: Option<Arc<dyn PhysicalExpr>>,
		cond: Option<&LogicalExpr>,
		order: Option<&LogicalOrdering>,
		scan_limit: Option<Arc<dyn PhysicalExpr>>,
		scan_start: Option<Arc<dyn PhysicalExpr>>,
		limit_hint: Option<usize>,
		has_query_limit: bool,
		downstream_topk: bool,
	) -> Result<PlannedSource, Error> {
		if is_layer_chain_over_table(&node) {
			let (chain, source) = peel_layer_chain(node);
			return self
				.fold_chain_over_table(
					chain,
					source,
					version,
					cond,
					order,
					scan_limit,
					scan_start,
					limit_hint,
					has_query_limit,
					downstream_topk,
				)
				.await;
		}
		let layer = node.is_layer();
		let op: Result<Arc<dyn ExecOperator>, Error> = match node {
			LogicalOp::PermissionFilter(LogicalPermissionFilter {
				input,
				predicate,
				auth,
			}) => {
				let input = Box::pin(self.fold(*input)).await?;
				let predicate = self.physical_expr(predicate).await?;
				Ok(Arc::new(
					PermissionFilterExec::new(input, predicate, auth)
						.for_checked_caller(self.compiles_for_checked_caller()),
				))
			}
			LogicalOp::Project(LogicalProject {
				input,
				fields,
				..
			}) if layer => {
				let schema = input.schema();
				let input = Box::pin(self.fold(*input)).await?;
				self.compile_layer(input, fields, &schema).await
			}
			LogicalOp::Distinct(LogicalDistinct {
				input,
			}) => {
				let input = Box::pin(self.fold(*input)).await?;
				Ok(Arc::new(Distinct::new(input)))
			}
			LogicalOp::Join(join) => {
				let (left, parts) = JoinParts::detach(join);
				let left = self.fold_join_left(left, version.clone(), order, &parts).await?;
				self.compile_join_parts(left, parts, version).await
			}
			other @ (LogicalOp::Source(_)
			| LogicalOp::Values(_)
			| LogicalOp::Match(_)
			| LogicalOp::Subquery(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Filter(_)
			| LogicalOp::Split(_)
			| LogicalOp::Aggregate(_)
			| LogicalOp::Sort(_)
			| LogicalOp::Limit(_)
			| LogicalOp::Project(_)
			| LogicalOp::Fetch(_)
			| LogicalOp::Timeout(_)
			| LogicalOp::VersionScope(_)
			| LogicalOp::Only(_)
			| LogicalOp::Explain(_)
			| LogicalOp::Eval(_)
			| LogicalOp::Sequence(_)
			| LogicalOp::Let(_)
			| LogicalOp::Return(_)
			| LogicalOp::Foreach(_)
			| LogicalOp::Info(_)
			| LogicalOp::Sleep(_)) => Box::pin(self.fold(other)).await,
		};
		Ok(PlannedSource::opaque(op?))
	}

	/// The barrier crossing of [`Self::fold_uncollected`]: plan the
	/// table `source` beneath `chain` with the offer the chain admits, then
	/// wrap the chain back on, innermost first.
	///
	/// The scan applies the chain's filter ([`ChainOffer::scan`]) so a row it
	/// rejects is dropped before it is decoded and layered, and the filter
	/// node is then compiled only for the conjuncts the planned source
	/// reports it did not consume ([`FilterAction`]). The pipeline's filter
	/// above the chain still judges its own predicate whole, so the source's
	/// filter action is not reported upward; nor is its top-k handle, which
	/// is for a pipeline that may drop what the scan consumed.
	///
	/// The paging is offered to the scan when the chain holds no permission
	/// node and withholds no ordering: a scan takes it only with the chain's
	/// filter consumed in full and the ordering it was offered satisfied
	/// (each access path's own rule), and the layers above drop nothing, so
	/// a scan that reports the paging taken has counted exactly the rows the
	/// pipeline's `Limit` would have. Beneath a permission node nothing is
	/// offered: the node drops rows the scan would have counted. Nor when
	/// the chain withholds the ordering: the `Sort` above it runs over every
	/// row, and a scan offered no ordering would take the paging beneath it.
	#[allow(clippy::too_many_arguments)]
	async fn fold_chain_over_table(
		&self,
		mut chain: Vec<ChainNode>,
		source: LogicalOp,
		version: Option<Arc<dyn PhysicalExpr>>,
		cond: Option<&LogicalExpr>,
		order: Option<&LogicalOrdering>,
		scan_limit: Option<Arc<dyn PhysicalExpr>>,
		scan_start: Option<Arc<dyn PhysicalExpr>>,
		limit_hint: Option<usize>,
		has_query_limit: bool,
		downstream_topk: bool,
	) -> Result<PlannedSource, Error> {
		let ChainOffer {
			cond,
			scan,
			order: offered,
			permissioned,
		} = chain_offer(&chain, cond, order, self.function_registry());
		let scan_predicate = match &scan {
			Some(scan) => Some(self.physical_expr(scan.clone()).await?),
			None => None,
		};
		// The paging reaches the source only where the source's count is the
		// pipeline's: a permission node drops rows the scan would count, and
		// an ordering the chain withheld sorts above the scan, over every row.
		let order_withheld = order.is_some() && offered.is_none();
		let (scan_limit, scan_start, limit_hint) =
			if order_withheld || chain.iter().any(ChainNode::is_permission) {
				(None, None, None)
			} else {
				(scan_limit, scan_start, limit_hint)
			};
		let planned = self
			.plan_source(
				source,
				version.clone(),
				cond.as_ref(),
				offered,
				None,
				scan_predicate,
				scan_limit,
				scan_start,
				limit_hint,
				has_query_limit,
				downstream_topk,
				&TopKPushdownRequest::NotApplicable,
			)
			.await?;
		let limit_pushed = planned.limit_pushed;
		if let Some(scan) = &scan {
			// The scan took these conjuncts of the chain's filters: each is
			// dropped from its node unless the source reports it still to be
			// judged. A conjunct the scan did not take stays where it stands.
			let taken = conjuncts(scan);
			let consumed = |conjunct: &LogicalExpr| match &planned.filter_action {
				FilterAction::FullyConsumed => true,
				FilterAction::UseOriginal => false,
				FilterAction::Residual(residual) => !conjuncts(residual).contains(&conjunct),
			};
			let mut kept = Vec::with_capacity(chain.len());
			for node in chain {
				match node {
					ChainNode::Filter {
						predicate,
					} => {
						let keep =
							retain_conjuncts(&predicate, |c| !(taken.contains(&c) && consumed(c)));
						if let Some(predicate) = keep {
							kept.push(ChainNode::Filter {
								predicate,
							});
						}
					}
					other @ (ChainNode::Permission {
						..
					}
					| ChainNode::Layer {
						..
					}
					| ChainNode::Join(_)) => kept.push(other),
				}
			}
			chain = kept;
		}
		// A scan reports the paging taken only with its predicate consumed
		// in full, so no filter of the chain remains beneath the paging.
		// A scan reports the paging taken only with its predicate consumed
		// in full, so no filter of the chain remains beneath the paging — save
		// where the source yields no row at all. A predicate that admits none
		// is planned as such a source, and is answered by the plan rather than
		// offered as a scan predicate, so the chain still carries the filter it
		// was folded from; there is nothing beneath it for that filter to judge
		// and nothing for the paging to cut short.
		debug_assert!(
			!limit_pushed
				|| planned.operator.cardinality_hint() == CardinalityHint::Bounded(0)
				|| !chain.iter().any(|node| matches!(node, ChainNode::Filter { .. })),
			"a scan took the paging beneath a filter it did not consume"
		);
		let operator = self.wrap_chain(planned.operator, chain, version, permissioned).await?;
		Ok(PlannedSource {
			operator,
			filter_action: FilterAction::UseOriginal,
			limit_pushed,
			topk_pushdown: None,
		})
	}

	/// Wrap `chain`, top to bottom, back onto its compiled input `op`,
	/// innermost first. `version` is the pipeline's, for a join in the chain
	/// whose right side reads records. `permissioned` says whether a
	/// permission predicate of the chain was offered to `op`'s access path
	/// ([`ChainOffer::permissioned`]), which the permission nodes record so
	/// a checked caller's `EXPLAIN` prints no bound of the definer's.
	async fn wrap_chain(
		&self,
		mut op: Arc<dyn ExecOperator>,
		chain: Vec<ChainNode>,
		version: Option<Arc<dyn PhysicalExpr>>,
		permissioned: bool,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		for node in chain.into_iter().rev() {
			op = match node {
				ChainNode::Permission {
					predicate,
					auth,
				} => {
					let predicate = self.physical_expr(predicate).await?;
					Arc::new(
						PermissionFilterExec::new(op, predicate, auth)
							.for_checked_caller(self.compiles_for_checked_caller())
							.with_access_path_from_predicate(permissioned),
					)
				}
				ChainNode::Layer {
					fields,
					input_schema,
				} => self.compile_layer(op, fields, &input_schema).await?,
				ChainNode::Filter {
					predicate,
				} => Arc::new(Filter::new(op, self.physical_expr(predicate).await?)),
				ChainNode::Join(parts) => {
					Box::pin(self.compile_join_parts(op, *parts, version.clone())).await?
				}
			};
		}
		Ok(op)
	}

	/// The left input of a join, planned with what the pipeline's ordering
	/// admits to its access path; see the module docs on joins. The
	/// pipeline's predicate is not offered: the conjuncts that read the left
	/// row alone stand beneath the join as a filter of their own, and the rest
	/// read the joined row.
	async fn fold_join_left(
		&self,
		left: LogicalOp,
		version: Option<Arc<dyn PhysicalExpr>>,
		order: Option<&LogicalOrdering>,
		parts: &JoinParts,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let registry = self.function_registry();
		let withheld =
			|expr: &LogicalExpr| parts.alias.is_root_of(expr) || reads_row_opaquely(expr, registry);
		let order = order.filter(|order| match order {
			LogicalOrdering::Random => true,
			LogicalOrdering::Order(keys) => !keys.iter().any(|key| withheld(&key.value)),
		});
		if is_layer_chain_over_table(&left) {
			let (chain, source) = peel_layer_chain(left);
			return Ok(Box::pin(self.fold_chain_over_table(
				chain, source, version, None, order, None, None, None, false, false,
			))
			.await?
			.operator);
		}
		if matches!(
			left,
			LogicalOp::Source(LogicalSource {
				source: SourceRef::Table(_),
				..
			})
		) {
			let planned = self
				.plan_source(
					left,
					version,
					None,
					order,
					None,
					None,
					None,
					None,
					None,
					false,
					false,
					&TopKPushdownRequest::NotApplicable,
				)
				.await?;
			return Ok(planned.operator);
		}
		Box::pin(self.fold(left)).await
	}

	/// A join over its compiled `left` input; see the module docs on joins
	/// for the choice between a [`LookupJoin`] and a [`HashJoin`].
	pub(crate) async fn compile_join_parts(
		&self,
		left: Arc<dyn ExecOperator>,
		parts: JoinParts,
		version: Option<Arc<dyn PhysicalExpr>>,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let JoinParts {
			right,
			kind,
			on,
			filter,
			alias,
			frame,
		} = parts;
		let join_type = match kind {
			JoinKind::Inner => JoinType::Inner,
			JoinKind::Left => JoinType::Left,
			JoinKind::Semi => JoinType::Semi,
			JoinKind::Anti => JoinType::Anti,
		};
		let frame = (frame.mode != EvalMode::Caller)
			.then(|| JoinFrame::new(frame.auth, self.compiles_for_checked_caller()));
		let residual = match filter {
			Some(filter) => Some(self.physical_expr(filter).await?),
			None => None,
		};
		let keyed_by_id =
			matches!(on.as_slice(), [(_, right_key)] if is_alias_id(right_key, &alias));
		// The lookup reads the right table at the left side's keys alone; the
		// hash join reads the whole relation, which is what the scalar form
		// the decorrelation rule replaced read for every outer row. Where the
		// chain over the table evaluates something that can raise, the two
		// differ in what they *evaluate* and not only in what they read, and
		// an access path may not remove an evaluation the plan owes.
		if keyed_by_id && is_lookup_chain_over_table(&right) && !plan_may_raise(&right) {
			let (left_key, _) = on.into_iter().next().expect("one key pair");
			let left_key = self.physical_expr(left_key).await?;
			let (chain, table, schema) = peel_lookup_chain(right);
			let leaf: Arc<dyn ExecOperator> = Arc::new(RecordBatchFetch::new(
				table.clone(),
				EnforcementMode::for_source(schema.as_ref()),
				version.clone(),
			));
			// The fetch reads at the left side's keys, not at any bound of the
			// permission predicate's.
			let right = self.wrap_chain(leaf, chain, version, false).await?;
			return Ok(Arc::new(LookupJoin::new(
				left,
				right,
				left_key,
				table,
				join_type,
				alias.name().to_string(),
				residual,
				frame,
			)));
		}
		let build = Box::pin(self.fold(right)).await?;
		let mut probe_keys = Vec::with_capacity(on.len());
		let mut build_keys = Vec::with_capacity(on.len());
		for (left_key, right_key) in on {
			probe_keys.push(self.physical_expr(left_key).await?);
			build_keys.push(self.physical_expr(right_key).await?);
		}
		Ok(Arc::new(HashJoin::documents(
			build,
			left,
			probe_keys,
			build_keys,
			join_type,
			alias.name().to_string(),
			residual,
			frame,
		)))
	}

	/// A projection layer over its compiled `input`: one [`ProjectLayer`]
	/// column per `Single` field of `fields`, in order, each landing under
	/// the field's name. The leading `*` is the row itself and compiles to
	/// nothing.
	///
	/// A permission column of the form `IF p THEN v ELSE NONE END` — the
	/// analyzer's encoding of a field `SELECT` permission `WHERE p` — compiles
	/// to a guarded column ([`LayerColumn::guarded`]): the operator judges `p`
	/// apart from `v`, so an admitted field whose value is `NONE` is told from
	/// a denied one. Every other column compiles to its value alone.
	///
	/// `input_schema` is the schema of the layer's logical input, which names
	/// the computed fields the statement did not derive
	/// ([`ProjectLayer::with_input_schema`]). The layer is marked for a
	/// checked caller when this planner compiles for one, so `EXPLAIN` never
	/// shows such a caller a body or a predicate it is enforced on.
	async fn compile_layer(
		&self,
		input: Arc<dyn ExecOperator>,
		fields: LogicalFields,
		input_schema: &Schema,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let LogicalFields::Select(fields) = fields else {
			return Err(EngineError::Internal(
				"a projection layer reached the planner as a VALUE projection".to_string(),
			)
			.into());
		};
		let mut columns: Vec<LayerColumn> = Vec::with_capacity(fields.len());
		for field in fields {
			let LogicalField::Single {
				expr,
				alias,
				kind,
				frame,
			} = field
			else {
				continue;
			};
			let name = match alias {
				Some(alias) => Strand::new(alias.name()),
				None => Strand::new(expr.output_path().name()),
			};
			let column = match split_judgement(expr, &frame) {
				(Some(guard), value) => LayerColumn::guarded(
					name,
					self.physical_expr(guard).await?,
					self.physical_expr(value).await?,
					kind,
					frame,
				),
				(None, value) => {
					LayerColumn::new(name, self.physical_expr(value).await?, kind, frame)
				}
			};
			columns.push(column);
		}
		Ok(Arc::new(
			ProjectLayer::new(input, columns)
				.with_input_schema(input_schema)
				.for_checked_caller(self.compiles_for_checked_caller()),
		))
	}
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
		auth: Option<AuthLimit>,
	},
	Layer {
		/// The layer's projection: `*` followed by its columns.
		fields: LogicalFields,
		/// The schema of the node's input, which names the computed fields
		/// the statement did not derive; see [`ProjectLayer::with_input_schema`].
		input_schema: Schema,
	},
	/// The user's conjuncts the binding rules moved beneath the layers: they
	/// read no column of a layer above them, and stand directly above the
	/// source or the chain's topmost permission node. A lookup join's right
	/// side carries the filter of the lifted subquery the same way.
	Filter {
		predicate: LogicalExpr,
	},
	/// A semi or anti join lifted out of a permission predicate.
	Join(Box<JoinParts>),
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
			| ChainNode::Join(_) => Vec::new(),
			ChainNode::Layer {
				fields,
				..
			} => layer_column_names(fields),
		}
	}

	/// Whether the node judges rows in the permission frame, so that no
	/// user conjunct may be applied by the scan beneath it.
	fn is_permission(&self) -> bool {
		matches!(self, ChainNode::Permission { .. } | ChainNode::Join(_))
	}
}

/// A [`LogicalJoin`] detached from its left input: everything that compiles
/// it once the left side is compiled.
pub(crate) struct JoinParts {
	right: LogicalOp,
	kind: JoinKind,
	on: Vec<(LogicalExpr, LogicalExpr)>,
	filter: Option<LogicalExpr>,
	alias: RelationAlias,
	frame: EvalFrame,
}

impl JoinParts {
	/// Splits `join` into its left input and the rest.
	fn detach(join: LogicalJoin) -> (LogicalOp, Self) {
		let LogicalJoin {
			inputs,
			kind,
			on,
			filter,
			alias,
			frame,
		} = join;
		let [left, right] = *inputs;
		(
			left,
			Self {
				right,
				kind,
				on,
				filter,
				alias,
				frame,
			},
		)
	}
}

/// Whether `join` is a semi or anti join lifted out of a permission
/// predicate, which stands in a table's source chain.
fn is_permission_join(join: &LogicalJoin) -> bool {
	join.frame.mode != EvalMode::Caller && matches!(join.kind, JoinKind::Semi | JoinKind::Anti)
}

/// Whether `expr` is `alias.id`: the right key that lets a join read its
/// right side by record id.
fn is_alias_id(expr: &LogicalExpr, alias: &RelationAlias) -> bool {
	matches!(
		expr,
		LogicalExpr::Access {
			base,
			field,
		} if field.as_str() == "id" && **base == alias.field()
	)
}

/// Whether `node` is a table `Source`, alone or under filters,
/// `PermissionFilter`s, layers and semi or anti joins in any order — the
/// right side a [`LookupJoin`] can read by record id, every node of which
/// keeps or drops the rows the read yields. A locked read is excluded: there
/// is nothing to lock in a read by key.
fn is_lookup_chain_over_table(node: &LogicalOp) -> bool {
	let mut cur = node;
	loop {
		match cur {
			LogicalOp::Filter(LogicalFilter {
				input,
				..
			})
			| LogicalOp::PermissionFilter(LogicalPermissionFilter {
				input,
				..
			}) => cur = input,
			LogicalOp::Project(LogicalProject {
				input,
				..
			}) if cur.is_layer() => cur = input,
			LogicalOp::Join(join) if matches!(join.kind, JoinKind::Semi | JoinKind::Anti) => {
				cur = join.left()
			}
			LogicalOp::Source(LogicalSource {
				source: SourceRef::Table(_),
				for_update,
				..
			}) => return !*for_update,
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
			| LogicalOp::Sequence(_)
			| LogicalOp::Let(_)
			| LogicalOp::Return(_)
			| LogicalOp::Foreach(_)
			| LogicalOp::Info(_)
			| LogicalOp::Sleep(_) => return false,
		}
	}
}

/// Split a chain [`is_lookup_chain_over_table`] accepted into its nodes, top
/// to bottom, and the table and resolved schema of the `Source` beneath them.
fn peel_lookup_chain(node: LogicalOp) -> (Vec<ChainNode>, TableName, Option<Arc<Schema>>) {
	let mut chain = Vec::new();
	let mut cur = node;
	loop {
		let layer = cur.is_layer();
		cur = if let LogicalOp::Filter(LogicalFilter {
			input,
			predicate,
		}) = cur
		{
			chain.push(ChainNode::Filter {
				predicate,
			});
			*input
		} else if let LogicalOp::PermissionFilter(LogicalPermissionFilter {
			input,
			predicate,
			auth,
		}) = cur
		{
			chain.push(ChainNode::Permission {
				predicate,
				auth,
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
				input_schema: input.schema(),
			});
			*input
		} else if let LogicalOp::Join(join) = cur {
			let (left, parts) = JoinParts::detach(join);
			chain.push(ChainNode::Join(Box::new(parts)));
			left
		} else {
			let name = cur.name();
			let LogicalOp::Source(LogicalSource {
				source: SourceRef::Table(table),
				schema,
				..
			}) = cur
			else {
				unreachable!("a lookup chain ends at a table source, found {name}")
			};
			return (chain, table, schema);
		};
	}
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
			| LogicalOp::Sequence(_)
			| LogicalOp::Let(_)
			| LogicalOp::Return(_)
			| LogicalOp::Foreach(_)
			| LogicalOp::Info(_)
			| LogicalOp::Sleep(_) => return false,
		}
	}
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
			auth,
		}) = cur
		{
			chain.push(ChainNode::Permission {
				predicate,
				auth,
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
				input_schema: input.schema(),
			});
			*input
		} else if let LogicalOp::Join(join) = cur {
			let (left, parts) = JoinParts::detach(join);
			chain.push(ChainNode::Join(Box::new(parts)));
			left
		} else {
			return (chain, cur);
		};
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
	/// The conjuncts of the chain's filters for the scan to apply per row:
	/// those offered that hold no `KNN` or `MATCHES` operator, which the
	/// access path answers. `None` when the chain holds a table permission —
	/// the legacy engine judges the table permission before the user's
	/// predicate, and the scan would judge them on rows the permission
	/// removes — or when no conjunct qualifies.
	scan: Option<LogicalExpr>,
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
	let permissioned = chain.iter().any(ChainNode::is_permission);
	let scan = if permissioned {
		None
	} else {
		pushed.iter().filter(|c| !scoped(c)).cloned().reduce(and)
	};
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
			scan: None,
			order,
			permissioned: false,
		};
	}
	ChainOffer {
		cond: offer,
		scan,
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

/// `SLEEP`: lowering carries the duration as a literal.
fn fold_sleep(duration: &LogicalExpr) -> Result<Arc<dyn ExecOperator>, Error> {
	if let LogicalExpr::Literal(LogicalLiteral::Duration(duration)) = duration {
		return Ok(Arc::new(SleepPlan::new(*duration)));
	}
	Err(EngineError::Internal(format!(
		"SLEEP reached the planner with a non-literal duration: {duration}"
	))
	.into())
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::expr::Kind;
	use crate::expr::logical::{EvalFrame, LogicalSubquery, lower_select};
	use crate::syn;

	fn lower(sql: &str) -> LogicalOp {
		let ast = syn::parse(sql).expect("parse");
		let mut exprs = ast.expressions;
		assert_eq!(exprs.len(), 1, "expected one statement in {sql:?}");
		let crate::sql::TopLevelExpr::Expr(crate::sql::Expr::Select(select)) = exprs.remove(0)
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
	fn a_barrier_stops_the_walk_and_keeps_the_clauses_above_it() {
		let op = LogicalOp::Filter(LogicalFilter {
			input: Box::new(LogicalOp::PermissionFilter(LogicalPermissionFilter {
				input: Box::new(LogicalOp::Source(LogicalSource {
					source: SourceRef::Table("t".into()),
					schema: None,
					with: None,
					for_update: false,
				})),
				predicate: LogicalExpr::Literal(LogicalLiteral::Bool(true)),
				auth: None,
			})),
			predicate: LogicalExpr::Literal(LogicalLiteral::Bool(false)),
		});
		let (clauses, stop) = collect_pipeline(op);
		assert!(clauses.cond.is_some(), "the user predicate stays above the barrier");
		assert!(matches!(stop, PipelineStop::Node(LogicalOp::PermissionFilter(_))));
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

	/// The `WHERE` and `ORDER BY` of `sql`, as the fold collects them.
	fn where_and_order(sql: &str) -> (LogicalExpr, Option<LogicalOrdering>) {
		let clauses = collect_pipeline(lower(sql)).0;
		(clauses.cond.expect("WHERE"), clauses.order)
	}

	/// [`chain_offer`] with the builtin function registry.
	fn offer<'a>(
		chain: &[ChainNode],
		cond: Option<&LogicalExpr>,
		order: Option<&'a LogicalOrdering>,
	) -> ChainOffer<'a> {
		chain_offer(chain, cond, order, &FunctionRegistry::with_builtins())
	}

	fn pushed(predicate: LogicalExpr) -> ChainNode {
		ChainNode::Filter {
			predicate,
		}
	}

	fn permission(predicate: LogicalExpr) -> ChainNode {
		ChainNode::Permission {
			predicate,
			auth: None,
		}
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

	fn layer_node(names: &[&str]) -> ChainNode {
		ChainNode::Layer {
			fields: layer_fields(names),
			input_schema: Schema::open(),
		}
	}

	fn layer_over(input: LogicalOp, names: &[&str]) -> LogicalOp {
		LogicalOp::Project(LogicalProject {
			input: Box::new(input),
			fields: layer_fields(names),
			omit: vec![],
		})
	}

	fn rendered(cond: &Option<LogicalExpr>) -> String {
		cond.as_ref().map(ToString::to_string).unwrap_or_default()
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

	#[test]
	fn a_layer_chain_is_recognised_over_a_table_source_only() {
		let chain = LogicalOp::PermissionFilter(LogicalPermissionFilter {
			input: Box::new(layer_over(layer_over(table("t"), &["a"]), &["b"])),
			predicate: LogicalExpr::Literal(LogicalLiteral::Bool(true)),
			auth: None,
		});
		assert!(is_layer_chain_over_table(&chain));
		let (nodes, source) = peel_layer_chain(chain);
		assert!(matches!(nodes[0], ChainNode::Permission { .. }));
		assert!(matches!(nodes[1], ChainNode::Layer { .. }));
		assert!(matches!(nodes[2], ChainNode::Layer { .. }));
		assert_eq!(nodes.len(), 3);
		assert!(matches!(source, LogicalOp::Source(_)));
		assert_eq!(nodes[1].column_names(), vec![FieldPath::field("b")]);
		assert_eq!(nodes[2].column_names(), vec![FieldPath::field("a")]);
		assert!(nodes[0].column_names().is_empty());

		// The filter the binding rules place beneath the layers is a chain
		// node, but not depth: a filter alone over a source is a bare leaf
		// with a predicate, which the pipeline collects.
		let chain = layer_over(
			LogicalOp::Filter(LogicalFilter {
				input: Box::new(table("t")),
				predicate: LogicalExpr::Field("a".into()),
			}),
			&["c"],
		);
		assert!(is_layer_chain_over_table(&chain));
		let (nodes, _) = peel_layer_chain(chain);
		assert!(matches!(nodes[0], ChainNode::Layer { .. }));
		assert!(
			matches!(&nodes[1], ChainNode::Filter { predicate } if predicate.to_string() == "a")
		);
		assert_eq!(nodes.len(), 2);
		assert!(!is_layer_chain_over_table(&LogicalOp::Filter(LogicalFilter {
			input: Box::new(table("t")),
			predicate: LogicalExpr::Field("a".into()),
		})));

		// A bare source has no chain; a chain over anything but a table
		// source, or with another node inside it, is not one either.
		assert!(!is_layer_chain_over_table(&table("t")));
		assert!(!is_layer_chain_over_table(&layer_over(
			LogicalOp::Subquery(LogicalSubquery {
				input: Box::new(table("t")),
			}),
			&["c"],
		)));
		assert!(!is_layer_chain_over_table(&layer_over(
			LogicalOp::Distinct(LogicalDistinct {
				input: Box::new(table("t")),
			}),
			&["c"],
		)));
		assert!(!is_layer_chain_over_table(&LogicalOp::PermissionFilter(
			LogicalPermissionFilter {
				input: Box::new(LogicalOp::Source(LogicalSource {
					source: SourceRef::Param("p".to_owned().into()),
					schema: None,
					with: None,
					for_update: false,
				})),
				predicate: LogicalExpr::Literal(LogicalLiteral::Bool(true)),
				auth: None,
			}
		)));
		// The user's own projection is not a layer, so it is not a chain node.
		assert!(!is_layer_chain_over_table(&LogicalOp::Project(LogicalProject {
			input: Box::new(table("t")),
			fields: LogicalFields::Select(vec![
				LogicalField::All,
				LogicalField::user(LogicalExpr::Field("a".into()), None),
			]),
			omit: vec![],
		})));
	}

	#[test]
	fn a_permission_predicate_joins_the_offer_unless_a_scoped_operator_is_present() {
		let (owner, _) = where_and_order("SELECT * FROM t WHERE owner = 1");
		let (user, order) = where_and_order("SELECT * FROM t WHERE flag = true ORDER BY a");
		let chain = vec![permission(owner)];

		let ChainOffer {
			cond,
			order: offered_order,
			..
		} = offer(&chain, Some(&user), order.as_ref());
		assert_eq!(rendered(&cond), "flag = true AND owner = 1");
		assert!(offered_order.is_some(), "a layer-free chain withholds no ordering");

		// With no user predicate the permission's is the whole offer.
		let ChainOffer {
			cond,
			..
		} = offer(&chain, None, None);
		assert_eq!(rendered(&cond), "owner = 1");

		// A KNN search evaluates its residual per candidate as the caller, so
		// the permission predicate stays out; the same for MATCHES.
		let (knn, _) = where_and_order("SELECT * FROM t WHERE pt <|2,10|> [1.0] AND flag = true");
		let ChainOffer {
			cond,
			..
		} = offer(&chain, Some(&knn), None);
		assert_eq!(rendered(&cond), knn.to_string());
		let (fts, _) = where_and_order("SELECT * FROM t WHERE body @@ 'x'");
		let ChainOffer {
			cond,
			..
		} = offer(&chain, Some(&fts), None);
		assert_eq!(rendered(&cond), fts.to_string());
		// And a permission predicate carrying either is never offered.
		let ChainOffer {
			cond,
			..
		} = offer(&[permission(fts)], Some(&user), None);
		assert_eq!(rendered(&cond), "flag = true");
	}

	#[test]
	fn a_refusing_permission_makes_the_whole_offer_false() {
		let (user, _) = where_and_order("SELECT * FROM t WHERE flag = true");
		let refused = permission(LogicalExpr::Literal(LogicalLiteral::Bool(false)));
		let ChainOffer {
			cond,
			..
		} = offer(&[refused], Some(&user), None);
		assert!(cond.as_ref().is_some_and(is_false), "{}", rendered(&cond));
		// A user predicate that folded to `false` is one too.
		let ChainOffer {
			cond,
			..
		} = offer(
			&[permission(LogicalExpr::Literal(LogicalLiteral::Bool(true)))],
			Some(&LogicalExpr::Literal(LogicalLiteral::Bool(false))),
			None,
		);
		assert!(cond.as_ref().is_some_and(is_false), "{}", rendered(&cond));
	}

	#[test]
	fn a_layer_withholds_its_columns_from_the_offer() {
		// One layer computes `c`, another guards `secret`; `category` is a
		// passthrough of both.
		let chain = vec![layer_node(&["secret"]), layer_node(&["c"])];

		// Conjuncts on the columns go, by root field; the rest stay.
		let (user, order) = where_and_order(
			"SELECT * FROM t WHERE secret = 'x' AND category = 'a' AND c = 1 AND secret.inner = 2 \
			 ORDER BY secret",
		);
		let ChainOffer {
			cond,
			order: offered_order,
			..
		} = offer(&chain, Some(&user), order.as_ref());
		assert_eq!(rendered(&cond), "category = 'a'");
		assert!(offered_order.is_none(), "an ordering on a column is withheld");

		// A conjunct reading a column anywhere inside it is withheld too.
		let (user, _) =
			where_and_order("SELECT * FROM t WHERE string::len(c) = 1 AND category = 'a'");
		let ChainOffer {
			cond,
			..
		} = offer(&chain, Some(&user), None);
		assert_eq!(rendered(&cond), "category = 'a'");

		// Nothing left: nothing offered. An ordering on a passthrough stays.
		let (user, order) =
			where_and_order("SELECT * FROM t WHERE secret = 'x' ORDER BY category, id");
		let ChainOffer {
			cond,
			order: offered_order,
			..
		} = offer(&chain, Some(&user), order.as_ref());
		assert!(cond.is_none(), "{}", rendered(&cond));
		assert!(offered_order.is_some());

		// A permission predicate still joins what the layers let through.
		let (owner, _) = where_and_order("SELECT * FROM t WHERE owner = 1");
		let (user, _) = where_and_order("SELECT * FROM t WHERE secret = 'x' AND category = 'a'");
		let chain = vec![permission(owner), layer_node(&["secret"])];
		let ChainOffer {
			cond,
			..
		} = offer(&chain, Some(&user), None);
		assert_eq!(rendered(&cond), "category = 'a' AND owner = 1");
	}

	#[test]
	fn a_false_conjunct_beside_a_withheld_read_is_not_a_refusal() {
		let chain = vec![layer_node(&["c"])];
		// The folder kept `c > 0 AND false` whole because reading `c` is not
		// inert; the offer must not turn the leftover `false` into an empty
		// scan the rows never reach.
		let user = and(
			where_and_order("SELECT * FROM t WHERE c > 0").0,
			LogicalExpr::Literal(LogicalLiteral::Bool(false)),
		);
		let ChainOffer {
			cond,
			..
		} = offer(&chain, Some(&user), None);
		assert!(cond.is_none(), "{}", rendered(&cond));
		// A user predicate that is `false` outright still refuses.
		let ChainOffer {
			cond,
			..
		} = offer(&chain, Some(&LogicalExpr::Literal(LogicalLiteral::Bool(false))), None);
		assert!(cond.as_ref().is_some_and(is_false), "{}", rendered(&cond));
	}

	#[test]
	fn the_chains_filter_is_offered_and_applied_by_the_scan_unless_a_permission_stands_over_it() {
		let (category, _) = where_and_order("SELECT * FROM t WHERE category = 'a'");
		let (on_column, _) = where_and_order("SELECT * FROM t WHERE c = 1");
		// The conjuncts the rules pushed beneath the layer are the offer and
		// the scan predicate; the conjunct left above reads the column.
		let chain = vec![layer_node(&["c"]), pushed(category.clone())];
		let ChainOffer {
			cond,
			scan,
			..
		} = offer(&chain, Some(&on_column), None);
		assert_eq!(rendered(&cond), "category = 'a'");
		assert_eq!(rendered(&scan), "category = 'a'");

		// A pushed conjunct with a KNN operator is offered but not applied
		// per row by the scan: the access path answers it.
		let (knn, _) = where_and_order("SELECT * FROM t WHERE pt <|2,10|> [1.0] AND flag = true");
		let chain = vec![layer_node(&["c"]), pushed(knn.clone())];
		let ChainOffer {
			cond,
			scan,
			..
		} = offer(&chain, None, None);
		assert_eq!(rendered(&cond), knn.to_string());
		assert_eq!(rendered(&scan), "flag = true");

		// Over a permission the pushed conjuncts join the offer with the
		// permission's, and the scan applies nothing: the permission is
		// judged first.
		let (owner, _) = where_and_order("SELECT * FROM t WHERE owner = 1");
		let chain = vec![permission(owner), layer_node(&["c"]), pushed(category)];
		let ChainOffer {
			cond,
			scan,
			..
		} = offer(&chain, Some(&on_column), None);
		assert_eq!(rendered(&cond), "category = 'a' AND owner = 1");
		assert!(scan.is_none(), "{}", rendered(&scan));

		// A pushed `false` refuses like a user predicate of `false`.
		let chain =
			vec![layer_node(&["c"]), pushed(LogicalExpr::Literal(LogicalLiteral::Bool(false)))];
		let ChainOffer {
			cond,
			scan,
			..
		} = offer(&chain, Some(&on_column), None);
		assert!(cond.as_ref().is_some_and(is_false), "{}", rendered(&cond));
		assert!(scan.is_none());

		// A filter over a layer whose column it reads — the shape a join's
		// left input carries when the conjunct could not move beneath the
		// layer — is withheld like a conjunct of the pipeline's, and judged
		// where it stands; its other conjuncts are offered.
		let (mixed, _) = where_and_order("SELECT * FROM t WHERE c = 1 AND category = 'a'");
		let chain = vec![pushed(mixed), layer_node(&["c"])];
		let ChainOffer {
			cond,
			scan,
			..
		} = offer(&chain, None, None);
		assert_eq!(rendered(&cond), "category = 'a'");
		assert_eq!(rendered(&scan), "category = 'a'");
		let chain = vec![pushed(on_column), layer_node(&["c"])];
		let ChainOffer {
			cond,
			scan,
			..
		} = offer(&chain, None, None);
		assert!(cond.is_none(), "{}", rendered(&cond));
		assert!(scan.is_none(), "{}", rendered(&scan));
	}
}

/// The crossing, end to end: the analyzer's nodes over a table, planned with
/// the pipeline's clauses offered to the access path beneath them.
#[cfg(all(test, feature = "kv-mem"))]
mod planned_tests {
	use std::sync::Arc;

	use super::*;
	use crate::catalog::providers::CatalogProvider;
	use crate::dbs::{Capabilities, Session};
	use crate::expr::logical::lower_select;
	use crate::kvs::{Datastore, TransactionType};
	use crate::syn;

	fn lower(sql: &str) -> LogicalOp {
		let ast = syn::parse(sql).expect("parse");
		let mut exprs = ast.expressions;
		let crate::sql::TopLevelExpr::Expr(crate::sql::Expr::Select(select)) = exprs.remove(0)
		else {
			panic!("`{sql}` is not a SELECT");
		};
		lower_select(*select).expect("SELECT lowers")
	}

	/// A fresh in-memory datastore with server auth on and `setup` applied as
	/// the root Owner.
	async fn datastore(setup: &str) -> Arc<Datastore> {
		let ds = Datastore::builder()
			.with_capabilities(Capabilities::all())
			.with_auth(true)
			.build_with_path("memory")
			.await
			.expect("in-memory datastore");
		{
			let txn = ds.transaction(TransactionType::Write).await.expect("write txn");
			txn.ensure_ns_db(None, "test", "test").await.expect("ensure test/test");
			txn.commit().await.expect("commit");
		}
		let owner = Session::owner().with_ns("test").with_db("test");
		for response in ds.execute(setup, &owner, None).await.expect("setup executes") {
			response.result.expect("setup statement succeeds");
		}
		ds
	}

	fn record_user() -> Session {
		Session::for_record(
			"test",
			"test",
			"user",
			crate::types::PublicValue::String("user:tobie".to_owned()),
		)
	}

	/// One operator per line, children indented beneath their parent, each
	/// with its EXPLAIN attributes.
	fn render(op: &dyn ExecOperator, out: &mut String, depth: usize) {
		out.push_str(&"    ".repeat(depth));
		out.push_str(op.name());
		for (key, value) in op.attrs() {
			out.push_str(&format!(" [{key}: {value}]"));
		}
		out.push('\n');
		for child in op.children() {
			render(child.as_ref(), out, depth + 1);
		}
	}

	/// Binds `sql` for `session` and compiles it; returns the bound plan and
	/// the rendered operator tree.
	async fn plan(ds: &Datastore, session: &Session, sql: &str) -> (String, String) {
		let txn = Arc::new(ds.transaction(TransactionType::Read).await.expect("txn"));
		let mut ctx = ds.setup_ctx().expect("context");
		ctx.attach_session(session).expect("attach session");
		ctx.set_transaction(Arc::clone(&txn));
		let ctx = ctx.freeze();
		let planner = Planner::with_txn(
			&ctx,
			ds.function_registry(),
			txn,
			Some("test".to_owned()),
			Some("test".to_owned()),
		)
		.with_auth(Arc::clone(&session.au));
		let bound = planner.bind(lower(sql)).await.expect("binds");
		let logical = bound.to_string();
		let op = planner.plan_logical(bound).await.expect("plans");
		let mut physical = String::new();
		render(op.as_ref(), &mut physical, 0);
		(logical, physical)
	}

	/// The operator names of `physical`, top to bottom.
	fn spine(physical: &str) -> Vec<&str> {
		physical.lines().map(|line| line.trim_start().split(' ').next().unwrap_or("")).collect()
	}

	const RESTRICTED_DOCS: &str = "
		DEFINE TABLE docs SCHEMALESS PERMISSIONS FOR select FULL;
		DEFINE FIELD secret ON docs TYPE string PERMISSIONS FOR select NONE;
		DEFINE FIELD category ON docs TYPE string PERMISSIONS FOR select FULL;
		DEFINE INDEX idx_secret ON docs FIELDS secret;
		DEFINE INDEX idx_cat_secret ON docs FIELDS category, secret;
		DEFINE INDEX idx_category ON docs FIELDS category;
	";

	#[tokio::test]
	async fn a_readable_index_drives_the_scan_beneath_a_field_guard() {
		let ds = datastore(RESTRICTED_DOCS).await;
		let (logical, physical) =
			plan(&ds, &record_user(), "SELECT id FROM docs WHERE secret = 'x' AND category = 'a'")
				.await;
		// The user's reads are plain field names; the guard is a layer column
		// under the field's own name. The binding rules leave the conjunct
		// on the guarded column above the layer and move the other beneath.
		assert!(logical.contains("Filter[secret = 'x']"), "{logical}");
		assert!(logical.contains("Filter[category = 'a']"), "{logical}");
		assert!(logical.contains("AS secret @permission"), "{logical}");
		// The readable column's index answers the pushed conjunct in full, so
		// no filter is left beneath the layer; the restricted conjunct is
		// judged above it.
		assert_eq!(
			spine(&physical),
			["SelectProject", "Filter", "ProjectLayer", "IndexScan"],
			"{physical}"
		);
		assert!(physical.contains("IndexScan [index: idx_category]"), "{physical}");
		assert!(physical.contains("Filter [predicate: secret = 'x']"), "{physical}");
		assert!(
			!physical.contains("idx_secret") && !physical.contains("idx_cat_secret"),
			"{physical}"
		);
		// A checked caller is never shown the guard's predicate.
		assert!(
			physical.contains("ProjectLayer [columns: secret = <permission> @permission]"),
			"{physical}"
		);

		// The owner is not guarded: the plan is the bare leaf's.
		let owner = Session::owner().with_ns("test").with_db("test");
		let (_, physical) =
			plan(&ds, &owner, "SELECT id FROM docs WHERE secret = 'x' AND category = 'a'").await;
		assert!(!physical.contains("ProjectLayer"), "{physical}");
		assert!(physical.contains("IndexScan"), "{physical}");
	}

	#[tokio::test]
	async fn a_layer_keeps_its_columns_off_the_access_path() {
		let ds = datastore(RESTRICTED_DOCS).await;
		// `SELECT *` over a restricted field: the readable conjunct drives
		// the scan from beneath the layer and the restricted one is judged
		// above it.
		let (logical, physical) =
			plan(&ds, &record_user(), "SELECT * FROM docs WHERE secret = 'x' AND category = 'a'")
				.await;
		assert!(logical.contains("AS secret @permission"), "{logical}");
		assert_eq!(
			spine(&physical),
			["SelectProject", "Filter", "ProjectLayer", "IndexScan"],
			"{physical}"
		);
		assert!(physical.contains("IndexScan [index: idx_category]"), "{physical}");
		assert!(physical.contains("Filter [predicate: secret = 'x']"), "{physical}");

		// Only the restricted conjunct: no index, and no pushed predicate.
		let (_, physical) =
			plan(&ds, &record_user(), "SELECT * FROM docs WHERE secret = 'x'").await;
		assert_eq!(
			spine(&physical),
			["SelectProject", "Filter", "ProjectLayer", "TableScan"],
			"{physical}"
		);
		assert!(
			!physical.contains("TableScan [table: docs] [direction: Forward] [predicate"),
			"{physical}"
		);

		// An ordering on the restricted field is withheld: the scan is not
		// ordered by it and the sort stays.
		let (_, physical) =
			plan(&ds, &record_user(), "SELECT * FROM docs WHERE category = 'a' ORDER BY secret")
				.await;
		assert!(physical.contains("Sort"), "{physical}");
		assert!(physical.contains("IndexScan [index: idx_category]"), "{physical}");
	}

	#[tokio::test]
	async fn a_computed_column_is_withheld_while_a_passthrough_conjunct_reaches_the_scan() {
		let ds = datastore(
			"DEFINE TABLE items SCHEMAFULL;
			 DEFINE FIELD price ON items TYPE int;
			 DEFINE FIELD total ON items TYPE int COMPUTED price * 2;
			 DEFINE INDEX idx_price ON items FIELDS price;",
		)
		.await;
		let owner = Session::owner().with_ns("test").with_db("test");
		// The computed field is a layer column for every caller; the conjunct
		// on it stays above the layer and the conjunct on the stored field,
		// pushed beneath, drives the scan and is answered by it in full. An
		// unchecked caller sees the body in the plan.
		let (logical, physical) =
			plan(&ds, &owner, "SELECT id FROM items WHERE total = 4 AND price = 2").await;
		assert!(logical.contains("price * 2 AS total : int @computed"), "{logical}");
		assert_eq!(
			spine(&physical),
			["SelectProject", "Filter", "ProjectLayer", "IndexScan"],
			"{physical}"
		);
		assert!(physical.contains("IndexScan [index: idx_price]"), "{physical}");
		assert!(
			physical.contains("ProjectLayer [columns: total = price * 2 : int @computed]"),
			"{physical}"
		);
		assert!(physical.contains("Filter [predicate: total = 4]"), "{physical}");

		// Only the computed conjunct: nothing is offered, the scan is bare.
		let (_, physical) = plan(&ds, &owner, "SELECT id FROM items WHERE total = 4").await;
		assert_eq!(
			spine(&physical),
			["SelectProject", "Filter", "ProjectLayer", "TableScan"],
			"{physical}"
		);
		let scan = physical
			.lines()
			.find(|line| line.trim_start().starts_with("TableScan"))
			.expect("a table scan");
		assert!(!scan.contains("predicate"), "{physical}");
		assert!(physical.contains("Filter [predicate: total = 4]"), "{physical}");
	}

	#[tokio::test]
	async fn a_passthrough_conjunct_is_applied_by_the_scan_beneath_a_layer_chain() {
		let ds = datastore(
			"DEFINE TABLE items SCHEMAFULL;
			 DEFINE FIELD price ON items TYPE int;
			 DEFINE FIELD note ON items TYPE string;
			 DEFINE FIELD total ON items TYPE int COMPUTED price * 2;",
		)
		.await;
		let owner = Session::owner().with_ns("test").with_db("test");
		let scan_line = |physical: &str| {
			physical
				.lines()
				.find(|line| line.trim_start().starts_with("TableScan"))
				.expect("a table scan")
				.to_owned()
		};
		// The conjunct on the stored field is the scan's predicate, so a row
		// it rejects is never layered, and the scan applies it in full: no
		// filter is left beneath the layer, and the one above judges only the
		// conjunct on the computed column.
		let (_, physical) =
			plan(&ds, &owner, "SELECT id FROM items WHERE total = 4 AND note = 'a'").await;
		assert_eq!(
			spine(&physical),
			["SelectProject", "Filter", "ProjectLayer", "TableScan"],
			"{physical}"
		);
		assert!(scan_line(&physical).contains("predicate: note = 'a'"), "{physical}");
		assert!(physical.contains("Filter [predicate: total = 4]"), "{physical}");
		assert_eq!(physical.matches("Filter ").count(), 1, "{physical}");

		// A read through a row parameter may reach a layer column and is
		// withheld, however the field is spelled.
		let (_, physical) =
			plan(&ds, &owner, "SELECT id FROM items WHERE $this.total = 4 AND note = 'a'").await;
		assert!(scan_line(&physical).contains("predicate: note = 'a'"), "{physical}");
		let (_, physical) = plan(&ds, &owner, "SELECT id FROM items WHERE $this.total = 4").await;
		assert!(!scan_line(&physical).contains("predicate"), "{physical}");
		let (_, physical) = plan(
			&ds,
			&owner,
			"SELECT id FROM items WHERE (SELECT VALUE total FROM ONLY $parent) = 4",
		)
		.await;
		assert!(!scan_line(&physical).contains("predicate"), "{physical}");
	}

	#[tokio::test]
	async fn a_guarded_conjunct_over_a_joins_left_input_stays_off_the_access_path() {
		let ds = datastore(&format!(
			"{RESTRICTED_DOCS}
			 DEFINE TABLE cats SCHEMALESS PERMISSIONS FOR select FULL;"
		))
		.await;
		// The membership conjunct became the join; the conjunct on the
		// guarded column could not move beneath the layer and stands over it
		// as the join's left input. It reads the layer's column, so no index
		// over the stored field answers it and the scan applies nothing.
		let (logical, physical) = plan(
			&ds,
			&record_user(),
			"SELECT id FROM docs WHERE secret = 'x' AND category IN (SELECT VALUE name FROM cats)",
		)
		.await;
		assert!(logical.contains("Filter[secret = 'x']"), "{logical}");
		assert!(
			!physical.contains("idx_secret") && !physical.contains("idx_cat_secret"),
			"{physical}"
		);
		assert!(physical.contains("Filter [predicate: secret = 'x']"), "{physical}");
		let scan = physical
			.lines()
			.find(|line| line.trim_start().starts_with("TableScan [table: docs]"))
			.expect("a table scan over docs");
		assert!(!scan.contains("predicate"), "{physical}");
		// The filter is judged over the layered row: it stands above the layer.
		let filter_at = physical.find("Filter [predicate: secret = 'x']").expect("the filter");
		let layer_at = physical.find("ProjectLayer").expect("the layer");
		assert!(filter_at < layer_at, "{physical}");
	}

	#[tokio::test]
	async fn a_table_permission_joins_the_offer_and_paging_stays_above() {
		let ds = datastore(
			"DEFINE TABLE vec SCHEMALESS PERMISSIONS FOR select WHERE owner = $auth;
			 DEFINE INDEX idx_flag ON vec FIELDS flag;
			 DEFINE INDEX idx_owner ON vec FIELDS owner;
			 DEFINE TABLE hidden SCHEMALESS PERMISSIONS FOR select NONE;
			 DEFINE INDEX idx_a ON hidden FIELDS a;",
		)
		.await;
		// The user's conjunct and the permission's are both offered, so both
		// indexes drive the access path (fused into one candidate set) and
		// the filter stays above the permission.
		let (_, physical) = plan(&ds, &record_user(), "SELECT id FROM vec WHERE flag = true").await;
		assert_eq!(
			spine(&physical)[..3],
			["SelectProject", "Filter", "PermissionFilter"],
			"{physical}"
		);
		assert!(physical.contains("[index: idx_flag]"), "{physical}");
		assert!(physical.contains("[index: idx_owner]"), "{physical}");
		assert!(physical.contains("Filter [predicate: flag = true]"), "{physical}");
		// A LIMIT the pipeline can stop on keeps the streaming plan: fusion
		// declines, the limit stays above the permission, and no scan pages.
		let (_, physical) =
			plan(&ds, &record_user(), "SELECT id FROM vec WHERE flag = true LIMIT 1").await;
		assert_eq!(
			spine(&physical),
			["SelectProject", "Limit", "Filter", "PermissionFilter", "IndexScan"],
			"{physical}"
		);
		assert!(physical.contains("Limit [limit: 1]"), "{physical}");
		assert!(
			!physical.contains("[limit: 1] [") && physical.matches("[limit: 1]").count() == 1,
			"{physical}"
		);
		// A predicate no index answers still gets the permission's index.
		let (_, physical) = plan(&ds, &record_user(), "SELECT id FROM vec WHERE other = 1").await;
		assert!(physical.contains("IndexScan [index: idx_owner]"), "{physical}");
		assert!(physical.contains("Filter [predicate: other = 1]"), "{physical}");
		// A refusing permission plans an empty scan whatever the user asked.
		let (_, physical) = plan(&ds, &record_user(), "SELECT id FROM hidden WHERE a = 1").await;
		assert_eq!(
			spine(&physical),
			["SelectProject", "Filter", "PermissionFilter", "EmptyScan"],
			"{physical}"
		);
	}
}
