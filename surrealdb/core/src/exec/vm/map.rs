//! Translating a bound logical plan into a physical plan.
//!
//! Each logical node becomes the physical node that runs it, and nothing is
//! chosen here that could be chosen otherwise. A shape the VM does not cover is
//! declined whole, never half-mapped.
//!
//! A table source's access path is chosen by the planner's decision
//! ([`Planner::decide_access_path`]) over what the pipeline offers it
//! ([`Planner::access_offer`]): the choice the operator tree makes for the
//! same statement, including the indexes it withholds from a caller who may
//! not read their columns. The VM runs a table scan, a record lookup and a
//! B-tree index scan, and declines every other access path.

use surrealdb_strand::TableName;

use super::NotCovered;
use super::plan::{
	BranchRead, Control, CountSelect, Dependency, IndexBranch, IndexScope, JoinKey, KnnFilter,
	KnnPrefilter, Nested, NestedFacts, Omission, PhysicalPlan, Plan, ProjectedField, Projection,
	SortKeyRead, SortKeySpec, Strategy, Tail, ValuesRead, Walk, WalkAction, WalkStep,
	WriteSubjects,
};
use crate::catalog::providers::TableProvider;
use crate::exec::expression_registry::resolve_order_by_alias;
use crate::exec::field_path::{FieldPath, FieldPathPart};
use crate::exec::index::access_path::AccessPath;
use crate::exec::logical::is_effect_free;
use crate::exec::operators::scan::MergeMode;
use crate::exec::operators::scan::resolved::EnforcementMode;
use crate::exec::planner::util::{
	check_forbidden_group_by_params, expr_contains_matches, field_path_from_expr,
	index_covers_ordering, path_steps, strip_knn_and_matches_from_condition,
	strip_knn_from_condition,
};
use crate::exec::planner::write::{classified_targets, target_exprs};
use crate::exec::planner::{
	AccessOffer, CatalogWithin, CountDecision, PlanTimeVersion, Planner, decide_union_merge,
	output_field_path,
};
use crate::expr::logical::{
	EffectSet, EvalFrame, EvalMode, GuardKind, LogicalDestructureField, LogicalExpr, LogicalField,
	LogicalFields, LogicalLiteral, LogicalOp, LogicalOrdering, LogicalSource, LogicalWrite,
	LogicalWriteSource, LogicalWriteStatement, SourceRef,
};
use crate::expr::operator::BinaryOperator;

type Mapped<T> = Result<T, NotCovered>;

/// Map a bound statement, with the statements its expressions hold
/// ([`PhysicalPlan::nested`]).
///
/// The mapping is where a plan reads the catalog, so it runs unsealed
/// ([`crate::kvs::catalog_reads::sealed`]) wherever it is reached from,
/// a lowering included.
pub(super) async fn map(planner: &Planner<'_>, op: &LogicalOp) -> Mapped<PhysicalPlan> {
	crate::kvs::catalog_reads::sealed(false, map_unsealed(planner, op)).await
}

async fn map_unsealed(planner: &Planner<'_>, op: &LogicalOp) -> Mapped<PhysicalPlan> {
	// A statement that explains nothing maps the memberships the logical
	// rules lifted as the predicates they were lifted from; an explained
	// statement keeps its joins, which the operator tree renders.
	let explains = op
		.visit_ops_deep(&mut |node| match node {
			LogicalOp::Explain(_) => std::ops::ControlFlow::Break(()),
			_ => std::ops::ControlFlow::Continue(()),
		})
		.is_break();
	if explains {
		return map_as_bound(planner, op).await;
	}
	let memberships = memberships_of(op)?;
	map_as_bound(planner, &memberships).await
}

/// [`map`] for `op` as it stands: a semi or anti join it holds is not
/// covered.
pub(super) async fn map_as_bound(planner: &Planner<'_>, op: &LogicalOp) -> Mapped<PhysicalPlan> {
	crate::kvs::catalog_reads::sealed(false, map_as_bound_unsealed(planner, op)).await
}

async fn map_as_bound_unsealed(planner: &Planner<'_>, op: &LogicalOp) -> Mapped<PhysicalPlan> {
	crate::exec::compiles::note();
	// The statements an expression of a versioned statement holds are read at
	// its stamp, so they are planned beneath the same one.
	let scoped = match version_of(op) {
		Some(version) => {
			Some(crate::exec::vm::host::out_of_line(|| planner.under_version(version)).await)
		}
		None => None,
	};
	let planner = scoped.as_ref().unwrap_or(planner);
	let mut plan = map_statement(planner, op).await?;
	// A block that runs a statement after a catalog change it cannot model,
	// whose lowering reads what the change may change, is not covered; a
	// block planned late-bound after such a change plans what follows it
	// late-bound.
	if let Plan::Control {
		control,
	} = &plan.root
		&& !planner.binds_late_after_schema()
		&& super::lower::data_dependent_ddl(planner, control, &plan.sources).await
	{
		#[cfg(test)]
		super::speculate::DECLINED.with(|count| count.set(count.get() + 1));
		return Err(NotCovered::new(super::speculate::DATA_DEPENDENT_DDL));
	}
	// A `LET` in an expression of the statement is refused as the operator
	// tree compiles the expression, which is as it plans the statement. A
	// block's statements are refused by the block, as its control flow
	// reaches them ([`Blocks::statement`]).
	if !matches!(plan.root, Plan::Control { .. } | Plan::Refused { .. })
		&& compiles_a_let_statement(op)
	{
		plan.root = Plan::Refused {
			refusal: super::plan::Refusal::InvalidStatement {
				message: LET_IN_AN_EXPRESSION.to_owned(),
			},
		};
		return Ok(plan);
	}
	// A statement written in an expression of one that may redefine what it
	// names is planned late-bound where the expression is lowered, and is
	// never a group mapped with the plan.
	if !planner.catalog_within().nested_late {
		plan.nested = crate::exec::vm::host::out_of_line(|| map_nested(planner, op, &plan)).await;
	}
	let scanned: Vec<TableName> = plan
		.dependencies
		.iter()
		.map(
			|Dependency::Table {
			     table,
			     ..
			 }| table.clone(),
		)
		.collect();
	let catalog = crate::exec::vm::host::out_of_line(|| {
		super::plan_catalog::read(planner, op, scanned.iter())
	})
	.await?;
	// Each table the lowering takes a definition of is a dependency at the
	// catalog version it was read at.
	for read in catalog.tables() {
		let dependency = Dependency::Table {
			table: read.table.clone(),
			version: read.version,
		};
		if !plan.dependencies.contains(&dependency) {
			plan.dependencies.push(dependency);
		}
	}
	plan.catalog = Some(std::sync::Arc::new(catalog));
	Ok(plan)
}

/// The `VERSION` clause of statement `op`, the clause its tail takes
/// ([`Tail::version`]).
fn version_of(op: &LogicalOp) -> Option<&LogicalExpr> {
	let op = match op {
		LogicalOp::Only(only) => &only.input,
		other => other,
	};
	match op {
		LogicalOp::VersionScope(scope) => Some(&scope.version),
		_ => None,
	}
}

/// What the operator tree raises for a `VERSION` clause beside a statement
/// in `FROM`.
const SUBQUERY_SOURCE_VERSION: &str = "VERSION clause cannot be used with a subquery source. \
	 Place the VERSION clause inside the subquery instead.";

/// What the operator tree raises for a `LET` written as an expression.
pub(super) const LET_IN_AN_EXPRESSION: &str =
	"LET statements can only appear at the top level of a query or inside a block expression";

/// Whether `op`, the value a `RETURN` or `LET` takes, is a `LET`.
fn is_a_let(op: &LogicalOp) -> bool {
	match op {
		LogicalOp::Let(_) => true,
		LogicalOp::Eval(eval) => {
			matches!(&eval.expr, LogicalExpr::Subquery(inner) if matches!(**inner, LogicalOp::Let(_)))
		}
		_ => false,
	}
}

/// Block statement `op`, bound, with every semi and anti join the logical
/// rules lifted out of a filter put back as the membership predicate it was
/// lifted from, at any depth of the statement: the rows it keeps are the
/// join's (the lift never changes them), and the VM evaluates the
/// predicate's subquery as it evaluates any statement nested in a filter.
/// `op` itself where it holds none. A join lifted out of a table's
/// permission is put back as a permission filter over its left input, under
/// the frame and `AUTH LIMIT` it carries. A join that is neither a semi nor
/// an anti join, or that has no key, is not covered.
pub(super) fn memberships_of(op: &LogicalOp) -> Mapped<std::borrow::Cow<'_, LogicalOp>> {
	if !lifts_a_membership(op) {
		return Ok(std::borrow::Cow::Borrowed(op));
	}
	op.clone()
		.transform_up(&mut |node| {
			let mut node = match node {
				LogicalOp::Join(join)
					if matches!(
						join.kind,
						crate::expr::logical::JoinKind::Semi | crate::expr::logical::JoinKind::Anti
					) =>
				{
					match membership_filter(join)? {
						// The terms the rules moved beneath the join are judged
						// before the membership: the one conjunction they and
						// it make is the predicate an access path is chosen
						// from.
						LogicalOp::Filter(membership) => match *membership.input {
							LogicalOp::Filter(left) => {
								LogicalOp::Filter(crate::expr::logical::LogicalFilter {
									input: left.input,
									predicate: LogicalExpr::Binary {
										left: Box::new(left.predicate),
										op: BinaryOperator::And,
										right: Box::new(membership.predicate),
									},
								})
							}
							input => LogicalOp::Filter(crate::expr::logical::LogicalFilter {
								input: Box::new(input),
								predicate: membership.predicate,
							}),
						},
						other => other,
					}
				}
				// A term the rules held above the join is judged for the rows
				// the membership kept, after it: the one conjunction, read
				// left to right, as the predicate was written.
				LogicalOp::Filter(outer) => match *outer.input {
					LogicalOp::Filter(inner) if holds_a_membership(&inner.predicate) => {
						LogicalOp::Filter(crate::expr::logical::LogicalFilter {
							input: inner.input,
							predicate: LogicalExpr::Binary {
								left: Box::new(inner.predicate),
								op: BinaryOperator::And,
								right: Box::new(outer.predicate),
							},
						})
					}
					input => LogicalOp::Filter(crate::expr::logical::LogicalFilter {
						input: Box::new(input),
						predicate: outer.predicate,
					}),
				},
				other => other,
			};
			node.visit_scalars_mut(&mut |scalar| {
				scalar.visit_mut(&mut |expr| {
					if let LogicalExpr::Subquery(plan) = expr
						&& lifts_a_membership(plan)
					{
						let taken = std::mem::replace(
							plan.as_mut(),
							LogicalOp::Eval(crate::expr::logical::LogicalEval {
								expr: LogicalExpr::Break,
							}),
						);
						**plan = memberships_of(&taken)?.into_owned();
					}
					Ok(())
				})
			})?;
			Ok(node)
		})
		.map(std::borrow::Cow::Owned)
}

/// The plan of the bound statement `explained`, optimised as the plan it runs
/// from is: a membership the logical rules lifted into a semi or anti join is
/// the predicate the VM evaluates it as ([`memberships_of`]), and the
/// statement nested beneath it is planned with the statement.
///
/// A `LET` is explained by the plan of the value it binds, which
/// [`to_explain_operator`](super::explain::to_explain_operator) renders
/// beneath the binding.
///
/// Not covered where the VM does not cover the statement itself.
pub(super) async fn explained_plan(
	planner: &Planner<'_>,
	explained: &LogicalOp,
) -> Mapped<PhysicalPlan> {
	let explained = match explained {
		LogicalOp::Let(stmt) => {
			if surrealdb_cnf::PROTECTED_PARAM_NAMES.contains(&stmt.name.as_str()) {
				return Err(NotCovered::refused(super::plan::Refusal::ProtectedParam {
					name: stmt.name.clone(),
				}));
			}
			stmt.value.as_ref()
		}
		other => other,
	};
	let statement = memberships_of(explained)?;
	let physical = crate::exec::vm::host::out_of_line(|| map(planner, &statement)).await?;
	Ok(super::optimize::optimize(physical))
}

/// Whether a conjunct of `predicate` is a membership [`membership_filter`] put
/// back.
fn holds_a_membership(predicate: &LogicalExpr) -> bool {
	match predicate {
		LogicalExpr::Binary {
			left,
			op: BinaryOperator::And,
			right,
		} => holds_a_membership(left) || holds_a_membership(right),
		LogicalExpr::Binary {
			op: BinaryOperator::Inside | BinaryOperator::NotInside,
			right,
			..
		} => matches!(right.as_ref(), LogicalExpr::Subquery(_)),
		_ => false,
	}
}

/// Whether `op`, or a statement nested in it, holds a semi or anti join.
fn lifts_a_membership(op: &LogicalOp) -> bool {
	op.visit_ops_deep(&mut |node| match node {
		LogicalOp::Join(join)
			if matches!(
				join.kind,
				crate::expr::logical::JoinKind::Semi | crate::expr::logical::JoinKind::Anti
			) =>
		{
			std::ops::ControlFlow::Break(())
		}
		_ => std::ops::ControlFlow::Continue(()),
	})
	.is_break()
}

/// The prefix of the parameters [`membership_filter`] binds a key to, once per
/// probe row.
const MEMBERSHIP_KEY: &str = "__membership_key_";

/// The filter semi or anti join `join` was lifted from: its left input kept
/// where `outer IN (SELECT VALUE key FROM right WHERE …)` holds (`NOT IN` for
/// an anti join), the subquery's condition the join's further keys and its
/// residual, each read with `$parent` as the left row and the subquery's row
/// as the right one.
fn membership_filter(join: crate::expr::logical::LogicalJoin) -> Mapped<LogicalOp> {
	use crate::expr::logical::{JoinKind, LogicalFilter, LogicalProject};
	// A join lifted out of a `PERMISSIONS` clause is that clause's predicate
	// again, judged under the frame and the `AUTH LIMIT` the join carries.
	let permission = match join.frame.mode {
		EvalMode::Caller if join.frame == EvalFrame::caller() => None,
		EvalMode::Permission => Some(GuardKind::Read),
		EvalMode::MutablePermission => Some(GuardKind::Write),
		_ => {
			return Err(NotCovered::internal(
				"a semi or anti join in a frame with no predicate form",
			));
		}
	};
	let alias = join.alias.clone();
	let frame_auth = join.frame.auth.clone();
	let kind = join.kind;
	let filter = join.filter.clone();
	let mut on = join.on.clone().into_iter();
	let (left, right) = join.into_inputs();
	let Some((outer, key)) = on.next() else {
		return Err(NotCovered::internal("a semi or anti join with no key"));
	};
	// A further key reads the probe row once, where the predicate it came from
	// reads `$parent` for each row of the subquery. The two agree for an
	// object row, which carries what it holds, and not for a row that is a
	// bare value (a record id), whose field is read from the record each time
	// it is named. Over such a row the key is bound once per probe row, ahead
	// of the subquery, and the subquery reads the binding.
	let bind_once = on.len() > 0 && !yields_object_rows(&left);
	if bind_once && permission.is_some() {
		return Err(NotCovered::internal("a join key read off a probe row that is not an object"));
	}
	let in_subquery = |expr: LogicalExpr| in_subquery_frame(expr, &alias);
	let mut condition: Option<LogicalExpr> = None;
	let mut and = |term: LogicalExpr| {
		condition = Some(match condition.take() {
			Some(left) => LogicalExpr::Binary {
				left: Box::new(left),
				op: BinaryOperator::And,
				right: Box::new(term),
			},
			None => term,
		});
	};
	let mut bound: Vec<LogicalOp> = Vec::new();
	for (at, (outer_key, right_key)) in on.enumerate() {
		let read = if bind_once {
			let name = format!("{MEMBERSHIP_KEY}{at}");
			bound.push(LogicalOp::Let(crate::expr::logical::LogicalLet {
				name: name.clone(),
				kind: None,
				value: Box::new(LogicalOp::Eval(crate::expr::logical::LogicalEval {
					expr: outer_key,
				})),
			}));
			LogicalExpr::Param(crate::expr::Param::from(name))
		} else {
			in_subquery(outer_key)
		};
		and(LogicalExpr::Binary {
			left: Box::new(in_subquery(right_key)),
			op: BinaryOperator::Equal,
			right: Box::new(read),
		});
	}
	if let Some(residual) = filter {
		and(in_subquery(residual));
	}
	let input = match condition {
		Some(predicate) => LogicalOp::Filter(LogicalFilter {
			input: Box::new(right),
			predicate,
		}),
		None => right,
	};
	let subquery = LogicalOp::Project(LogicalProject {
		input: Box::new(input),
		fields: LogicalFields::Value {
			expr: in_subquery(key),
			alias: None,
		},
		omit: Vec::new(),
	});
	let op = match kind {
		JoinKind::Semi => BinaryOperator::Inside,
		JoinKind::Anti => BinaryOperator::NotInside,
		JoinKind::Inner | JoinKind::Left => {
			return Err(NotCovered::internal("an inner or left join"));
		}
	};
	// The keys bound once are the statements of a block ahead of the subquery,
	// whose rows the block yields.
	let subquery = if bound.is_empty() {
		subquery
	} else {
		bound.push(subquery);
		LogicalOp::Sequence(crate::expr::logical::LogicalSequence {
			stmts: bound,
		})
	};
	let predicate = LogicalExpr::Binary {
		left: Box::new(outer),
		op,
		right: Box::new(LogicalExpr::Subquery(Box::new(subquery))),
	};
	Ok(match permission {
		None => LogicalOp::Filter(LogicalFilter {
			input: Box::new(left),
			predicate,
		}),
		Some(guard) => LogicalOp::PermissionFilter(crate::expr::logical::LogicalPermissionFilter {
			input: Box::new(left),
			predicate,
			auth: frame_auth,
			guard,
		}),
	})
}

/// Whether every row `op` yields is an object: read from a table or a record,
/// or projected as a set of fields, and passed on by operators that keep
/// rows as they are.
fn yields_object_rows(op: &LogicalOp) -> bool {
	match op {
		LogicalOp::Source(source) => {
			matches!(source.source, SourceRef::Table(_) | SourceRef::RecordId(_))
		}
		LogicalOp::Project(project) => matches!(project.fields, LogicalFields::Select(_)),
		LogicalOp::Filter(inner) => yields_object_rows(&inner.input),
		LogicalOp::PermissionFilter(inner) => yields_object_rows(&inner.input),
		LogicalOp::Limit(inner) => yields_object_rows(&inner.input),
		LogicalOp::Sort(inner) => yields_object_rows(&inner.input),
		LogicalOp::Distinct(inner) => yields_object_rows(&inner.input),
		_ => false,
	}
}

/// `expr`, written in the frame of a join under `alias`, in the frame of the
/// subquery it was lifted from: a field of the right row (`alias.field`) is a
/// field of the subquery's row, and a field of the left row is a field of
/// `$parent`.
fn in_subquery_frame(
	expr: LogicalExpr,
	alias: &crate::expr::logical::RelationAlias,
) -> LogicalExpr {
	match expr {
		LogicalExpr::Access {
			base,
			field,
		} if *base == alias.field() => LogicalExpr::Field(field),
		LogicalExpr::Field(name) => LogicalExpr::Access {
			base: Box::new(LogicalExpr::Param(crate::expr::Param::from("parent".to_owned()))),
			field: name,
		},
		mut other => {
			for child in other.children_mut() {
				let taken = std::mem::replace(child, LogicalExpr::Break);
				*child = in_subquery_frame(taken, alias);
			}
			other
		}
	}
}

/// Whether the operator tree, planning `op`, compiles an expression that holds
/// a `LET` written as an expression: in `op`'s own expressions or its inputs',
/// down to but not into a block, whose statements the block plans.
fn compiles_a_let_statement(op: &LogicalOp) -> bool {
	if matches!(
		op,
		LogicalOp::Sequence(_)
			| LogicalOp::Let(_)
			| LogicalOp::Return(_)
			| LogicalOp::Foreach(_)
			| LogicalOp::Sleep(_)
	) {
		return false;
	}
	op.scalars().into_iter().any(holds_a_let_statement)
		|| op.children().iter().any(compiles_a_let_statement)
}

/// Whether `expr` holds a `LET` written as an expression, which the operator
/// tree refuses as it compiles `expr`: outside a closure's body, which is
/// compiled apart from the expression, and outside a statement nested in it,
/// whose own expressions are its own plan's to refuse.
fn holds_a_let_statement(expr: &LogicalExpr) -> bool {
	match expr {
		LogicalExpr::Subquery(op) => matches!(op.as_ref(), LogicalOp::Let(_)),
		LogicalExpr::Closure(_) => false,
		_ => {
			let mut held = false;
			expr.for_each_child(&mut |child| held |= holds_a_let_statement(child));
			held
		}
	}
}

/// The statements written in `op`'s own expressions, each mapped as a
/// statement of its own, with the facts its strategy is chosen from. A
/// statement in `FROM`, a block's statements and a statement nested in
/// another are their own plans' to list; a statement that does not map is
/// left out, and runs as a program of its own.
async fn map_nested(
	planner: &Planner<'_>,
	op: &LogicalOp,
	plan: &PhysicalPlan,
) -> Vec<std::sync::Arc<Nested>> {
	if matches!(plan.root, Plan::Control { .. }) {
		return Vec::new();
	}
	let mut uses = Vec::new();
	uses_in(op, &mut uses);
	if uses.is_empty() {
		return Vec::new();
	}
	let registry = planner.function_registry();
	let enclosing = crate::exec::logical::effects_of_plan(op, registry);
	let enclosing_writes = matches!(plan.root, Plan::Write { .. })
		|| enclosing.set.writes_data()
		|| enclosing.set.intersects(EffectSet::OPAQUE_BODY | EffectSet::WRITES_EXTERNAL);
	let mut closure_params = Vec::new();
	closure_params_in(op, &mut closure_params);
	let mut nested: Vec<std::sync::Arc<Nested>> = Vec::with_capacity(uses.len());
	for &(statement, _) in &uses {
		if nested.iter().any(|n| n.op == *statement) {
			continue;
		}
		let Ok(mapped) = crate::exec::vm::host::out_of_line(|| map(planner, statement)).await
		else {
			continue;
		};
		if mapped.refused() {
			continue;
		}
		let effects = crate::exec::logical::effects_of_plan(statement, registry);
		let facts = NestedFacts {
			correlated: statement.inherits_row()
				|| names_param(statement, &|name| {
					crate::expr::logical::ROW_PARAMS.contains(&name)
						|| closure_params.iter().any(|param| param == name)
				}),
			repeatable: !effects.impure
				&& !effects.index_operator
				&& effects.set.is_deterministic()
				&& statement.read_only(),
			reads_rows: !matches!(
				mapped.root,
				Plan::Write { .. }
					| Plan::Eval { .. }
					| Plan::Ddl { .. }
					| Plan::Info { .. }
					| Plan::Control { .. }
			),
			enclosing_writes,
		};
		let join = if facts.correlated && facts.reads_rows {
			join_key(statement, &mapped, &closure_params, registry)
		} else {
			None
		};
		nested.push(std::sync::Arc::new(Nested {
			op: statement.clone(),
			plan: mapped,
			facts,
			join,
			strategy: Strategy::Apply,
			// Every place the statement is written, not the first alone.
			membership: facts.reads_rows
				&& uses.iter().filter(|(other, _)| *other == statement).all(|(_, member)| *member),
		}));
	}
	nested
}

/// The join key of correlated statement `statement`, mapped to `plan`: the
/// first conjunct of its innermost predicate, where that is `field = outer`
/// with `outer` a path into `$parent` ([`super::optimize::join_conjunct`]),
/// and the statement less `outer` is repeatable, as
/// [`NestedFacts::repeatable`] says of a statement, and names neither the
/// row it is evaluated for nor a closure parameter of the statement it is
/// written in — in the conjunct's field, the other conjuncts, its clauses,
/// its table's permission and layers, or the statements nested in it.
///
/// `outer` itself is not judged: a probe reads it structurally, through
/// objects alone, and yields every row where the path meets anything else,
/// so the predicate evaluates it as the statement applied per row does.
fn join_key(
	statement: &LogicalOp,
	plan: &PhysicalPlan,
	closure_params: &[String],
	registry: &crate::exec::function::FunctionRegistry,
) -> Option<JoinKey> {
	if statement.inherits_row() {
		return None;
	}
	let predicate = innermost_predicate(&plan.root, None)?;
	let (field, outer) = super::optimize::join_conjunct(predicate)?;
	let mut masked = statement.clone();
	if !mask_join(&mut masked, predicate) {
		return None;
	}
	let names_row = names_param(&masked, &|name| {
		crate::expr::logical::ROW_PARAMS.contains(&name)
			|| closure_params.iter().any(|param| param == name)
	});
	let effects = crate::exec::logical::effects_of_plan(&masked, registry);
	let repeatable = !effects.impure
		&& !effects.index_operator
		&& effects.set.is_deterministic()
		&& masked.read_only();
	(!names_row && repeatable).then(|| JoinKey {
		field: field.clone(),
		outer: outer.clone(),
	})
}

/// The predicate of the innermost [`Plan::Filter`] on `plan`'s chain of
/// single inputs; `found` is the innermost one above `plan`.
fn innermost_predicate<'a>(
	plan: &'a Plan,
	found: Option<&'a LogicalExpr>,
) -> Option<&'a LogicalExpr> {
	match plan {
		Plan::Filter {
			input,
			predicate,
		} => innermost_predicate(input, Some(predicate)),
		Plan::PermissionFilter {
			input,
			..
		}
		| Plan::Layer {
			input,
			..
		}
		| Plan::Split {
			input,
			..
		}
		| Plan::Fetch {
			input,
			..
		}
		| Plan::Sort {
			input,
			..
		}
		| Plan::Limit {
			input,
			..
		}
		| Plan::Aggregate {
			input,
			..
		}
		| Plan::Project {
			input,
			..
		} => innermost_predicate(input, found),
		_ => found,
	}
}

/// Replace the `$parent` side of the join conjunct of the filter of `op`
/// whose predicate is `predicate` with `NONE`. Answers whether such a filter
/// was found.
fn mask_join(op: &mut LogicalOp, predicate: &LogicalExpr) -> bool {
	if let LogicalOp::Filter(filter) = op
		&& filter.predicate == *predicate
	{
		let mut first = &mut filter.predicate;
		while let LogicalExpr::Binary {
			left,
			op: BinaryOperator::And,
			..
		} = first
		{
			first = left;
		}
		if let LogicalExpr::Binary {
			left,
			op: BinaryOperator::Equal,
			right,
		} = first
		{
			let outer = if matches!(left.as_ref(), LogicalExpr::Field(_)) {
				right
			} else {
				left
			};
			**outer = LogicalExpr::Literal(crate::expr::logical::LogicalLiteral::None);
			return true;
		}
		return false;
	}
	op.children_mut().iter_mut().any(|child| mask_join(child, predicate))
}

/// The statements written in the expressions of `op` and its inputs, down to
/// but not into a statement in `FROM`, a block or a nested statement, each
/// with whether that occurrence is the right operand of an `INSIDE` or
/// `NOT INSIDE`.
fn uses_in<'a>(op: &'a LogicalOp, out: &mut Vec<(&'a LogicalOp, bool)>) {
	fn in_expr<'a>(expr: &'a LogicalExpr, out: &mut Vec<(&'a LogicalOp, bool)>) {
		match expr {
			LogicalExpr::Subquery(statement) => out.push((statement, false)),
			LogicalExpr::Binary {
				left,
				op: BinaryOperator::Inside | BinaryOperator::NotInside,
				right,
			} if matches!(right.as_ref(), LogicalExpr::Subquery(_)) => {
				in_expr(left, out);
				if let LogicalExpr::Subquery(statement) = right.as_ref() {
					out.push((statement, true));
				}
			}
			_ => expr.for_each_child(&mut |child| in_expr(child, out)),
		}
	}
	if matches!(
		op,
		LogicalOp::Subquery(_)
			| LogicalOp::Sequence(_)
			| LogicalOp::Let(_)
			| LogicalOp::Return(_)
			| LogicalOp::Foreach(_)
	) {
		return;
	}
	for scalar in op.scalars() {
		in_expr(scalar, out);
	}
	for child in op.children() {
		uses_in(child, out);
	}
}

/// The parameters of every closure literal written in `op`'s expressions, its
/// nested statements' included.
fn closure_params_in(op: &LogicalOp, out: &mut Vec<String>) {
	op.any(|node| {
		for scalar in node.scalars() {
			scalar.any(|expr| {
				match expr {
					LogicalExpr::Closure(closure) => {
						out.extend(closure.args.iter().map(|(param, _)| param.as_str().to_owned()))
					}
					LogicalExpr::Subquery(statement) => closure_params_in(statement, out),
					_ => {}
				}
				false
			});
		}
		false
	});
}

/// Whether `op` names a parameter `names` admits anywhere, in the statements
/// nested in it included. A table's layer binds `$value` to the field it
/// guards or computes, so `$value` read in a layer names nothing outside it.
pub(super) fn names_param(op: &LogicalOp, names: &dyn Fn(&str) -> bool) -> bool {
	op.any(|node| {
		let layer = node.is_layer();
		node.scalars().into_iter().any(|scalar| {
			scalar.any(|expr| match expr {
				LogicalExpr::Param(param) if layer && param.as_str() == "value" => false,
				LogicalExpr::Param(param) => names(param.as_str()),
				LogicalExpr::Subquery(statement) => names_param(statement, names),
				_ => false,
			})
		})
	})
}

/// Map a bound statement alone.
fn map_statement<'a>(
	planner: &'a Planner<'_>,
	op: &'a LogicalOp,
) -> crate::exec::BoxFut<'a, Mapped<PhysicalPlan>> {
	// A boxed `dyn` future: mapping recurses through this, and a caller
	// proving a statement's future `Send` stops at the box.
	Box::pin(async move {
		// The tail wraps the rows in the order the planner builds it: `ONLY`
		// outermost, then `VERSION`, then `TIMEOUT`.
		let mut tail = Tail::default();
		let mut op = op;
		// A plain `EXPLAIN` written inside a statement answers the rendering of
		// the plan of the statement it explains, which is a constant of that plan:
		// it is rendered here, from the plan the VM makes of the statement, and
		// the statement is answered with the rendering. A statement refused as it
		// is planned is refused as the `EXPLAIN` is. `EXPLAIN ANALYZE` runs the
		// statement to render what it measured, as a statement of a block does.
		if let LogicalOp::Explain(explain) = op {
			if explain.analyze {
				let mut blocks = Blocks::of(planner);
				let control =
					crate::exec::vm::host::out_of_line(|| blocks.control(planner, op)).await?;
				return Ok(PhysicalPlan {
					catalog: None,
					root: match blocks.refusal {
						Some(refusal) => Plan::Refused {
							refusal,
						},
						None => Plan::Control {
							control,
						},
					},
					dependencies: Vec::new(),
					tail,
					sources: blocks.sources,
					nested: Vec::new(),
					scope: IndexScope::default(),
				});
			}
			let planned =
				crate::exec::vm::host::out_of_line(|| explained_plan(planner, &explain.input))
					.await?;
			if let Some(refusal) = planned.refusal() {
				return Ok(PhysicalPlan {
					catalog: None,
					root: Plan::Refused {
						refusal: refusal.clone(),
					},
					dependencies: Vec::new(),
					tail,
					sources: Vec::new(),
					nested: Vec::new(),
					scope: IndexScope::default(),
				});
			}
			let rendered = crate::exec::operators::ExplainPlan::render(
				super::explain::to_explain_operator(
					&explain.input,
					&planned,
					None,
					planner.reads_are_checked(),
				)
				.as_ref(),
				explain.format,
			);
			return Ok(PhysicalPlan {
				catalog: None,
				root: Plan::Eval {
					expr: crate::exec::planner::util::value_to_logical_expr(rendered),
					fetches: Vec::new(),
				},
				dependencies: Vec::new(),
				tail,
				sources: Vec::new(),
				nested: Vec::new(),
				scope: IndexScope::default(),
			});
		}
		if let Some(refusal) = locked_read_refusal(planner, op)? {
			return Ok(PhysicalPlan {
				catalog: None,
				root: Plan::Refused {
					refusal,
				},
				dependencies: Vec::new(),
				tail,
				sources: Vec::new(),
				nested: Vec::new(),
				scope: IndexScope::default(),
			});
		}
		if let LogicalOp::Only(only) = op {
			tail.only = true;
			op = &only.input;
		}
		let scoped;
		let planner = if let LogicalOp::VersionScope(scope) = op {
			tail.version = Some(scope.version.clone());
			op = &scope.input;
			scoped =
				crate::exec::vm::host::out_of_line(|| planner.under_version(&scope.version)).await;
			&scoped
		} else {
			planner
		};
		if let LogicalOp::Timeout(timeout) = op {
			tail.timeout = Some(timeout.timeout.clone());
			op = &timeout.input;
		}
		// A statement `IF` none of whose arms holds a statement, and a `THROW`,
		// `BREAK` or `CONTINUE`, is evaluated as the expression it is in value
		// position.
		let evaluated;
		if matches!(
			op,
			LogicalOp::If(_) | LogicalOp::Throw(_) | LogicalOp::Break(_) | LogicalOp::Continue(_)
		) && !maps_as_control(op)
		{
			evaluated = LogicalOp::Eval(crate::expr::logical::LogicalEval {
				expr: op.clone().into_scalar(),
			});
			op = &evaluated;
		}
		// An `IF` whose branches hold statements is control flow, its statements
		// compiled with the program.
		if matches!(op, LogicalOp::Eval(_) | LogicalOp::If(_))
			&& maps_as_control(op)
			&& tail == Tail::default()
		{
			let mut blocks = Blocks::of(planner);
			let control =
				crate::exec::vm::host::out_of_line(|| blocks.control(planner, op)).await?;
			return Ok(PhysicalPlan {
				catalog: None,
				root: match blocks.refusal {
					Some(refusal) => Plan::Refused {
						refusal,
					},
					None => Plan::Control {
						control,
					},
				},
				dependencies: Vec::new(),
				tail,
				sources: blocks.sources,
				nested: Vec::new(),
				scope: IndexScope::default(),
			});
		}
		if let LogicalOp::Eval(eval) = op {
			if tail != Tail::default() {
				return Err(NotCovered::internal("a clause around an expression statement"));
			}
			return Ok(PhysicalPlan {
				catalog: None,
				root: Plan::Eval {
					expr: eval.expr.clone(),
					fetches: Vec::new(),
				},
				dependencies: Vec::new(),
				tail,
				sources: Vec::new(),
				nested: Vec::new(),
				scope: IndexScope::default(),
			});
		}
		if let LogicalOp::Ddl(ddl) = op {
			if tail != Tail::default() {
				return Err(NotCovered::internal("a clause around a schema statement"));
			}
			let checks = super::lower::ddl_checks(planner, &ddl.statement).await?;
			return Ok(PhysicalPlan {
				catalog: None,
				root: Plan::Ddl {
					statement: ddl.statement.clone(),
					checks: std::sync::Arc::new(checks),
				},
				dependencies: Vec::new(),
				tail,
				sources: Vec::new(),
				nested: Vec::new(),
				scope: IndexScope::default(),
			});
		}
		// A block, a `RETURN`, a loop, a `SLEEP` and a `LET` are control; a `LET`
		// of a protected name is refused by the block's mapping of it.
		if matches!(
			op,
			LogicalOp::Sequence(_)
				| LogicalOp::Return(_)
				| LogicalOp::Foreach(_)
				| LogicalOp::Sleep(_)
				| LogicalOp::Let(_)
		) {
			if tail != Tail::default() {
				return Err(NotCovered::internal("a clause around a block"));
			}
			let mut blocks = Blocks::of(planner);
			let control =
				crate::exec::vm::host::out_of_line(|| blocks.control(planner, op)).await?;
			return Ok(PhysicalPlan {
				catalog: None,
				root: match blocks.refusal {
					Some(refusal) => Plan::Refused {
						refusal,
					},
					None => Plan::Control {
						control,
					},
				},
				dependencies: Vec::new(),
				tail,
				sources: blocks.sources,
				nested: Vec::new(),
				scope: IndexScope::default(),
			});
		}
		if let LogicalOp::Info(info) = op {
			if tail != Tail::default() {
				return Err(NotCovered::internal("a clause around an INFO statement"));
			}
			return Ok(PhysicalPlan {
				catalog: None,
				root: Plan::Info {
					info: info.as_ref().clone(),
				},
				dependencies: Vec::new(),
				tail,
				sources: Vec::new(),
				nested: Vec::new(),
				scope: IndexScope::default(),
			});
		}
		if let LogicalOp::Write(write) = op {
			if tail.only || tail.version.is_some() {
				return Err(NotCovered::internal("ONLY or VERSION around a write"));
			}
			let mut plan = map_write(planner, write).await?;
			plan.tail = tail;
			return Ok(plan);
		}
		// `FETCH` applies to the rows the statement returns, beneath its tail.
		let fetches = match op {
			// A `FETCH` over a statement rather than a pipeline — what `RETURN …
			// FETCH` runs — fetches into the value an expression statement yields,
			// or into each row a read yields, beneath the read's own tail.
			LogicalOp::Fetch(fetch) if !starts_a_pipeline(&fetch.input) => {
				if tail != Tail::default() {
					return Err(NotCovered::internal("a clause around a fetched statement"));
				}
				return fetched_statement(planner, &fetch.input, &fetch.fetches).await;
			}
			LogicalOp::Fetch(fetch) => {
				op = &fetch.input;
				Some(fetch.fetches.clone())
			}
			_ => None,
		};
		if tail.version.is_some()
			&& let Some(refusal) = version_over_subquery_refusal(op)
		{
			return Ok(PhysicalPlan {
				catalog: None,
				root: Plan::Refused {
					refusal,
				},
				dependencies: Vec::new(),
				tail,
				sources: Vec::new(),
				nested: Vec::new(),
				scope: IndexScope::default(),
			});
		}
		let versioned = planner.reads_versioned(tail.version.is_some());
		let mut plan = match map_count(planner, op, versioned).await? {
			Some(plan) => plan,
			None => map_rows(planner, op, versioned).await?,
		};
		if let Some(fetches) = fetches
			&& !plan.refused()
		{
			plan.root = Plan::Fetch {
				input: Box::new(plan.root),
				fetches,
			};
		}
		let mut tail = tail;
		tail.tempfiles = asks_for_tempfiles(op);
		plan.tail = tail;
		Ok(plan)
	})
}

/// The refusal the operator tree raises for the `VERSION` of statement `op`
/// when a statement is among its sources: a stamp is read at tables, and a
/// statement in `FROM` is none. The clause belongs inside that statement.
fn version_over_subquery_refusal(op: &LogicalOp) -> Option<super::plan::Refusal> {
	use crate::exec::planner::fold::{PipelineStop, collect_pipeline};
	let (_, stop) = collect_pipeline(op.clone());
	let holds_a_statement = match stop {
		PipelineStop::Leaf(LogicalOp::UnionAll(union)) => {
			union.inputs.iter().any(|input| matches!(input, LogicalOp::Subquery(_)))
		}
		PipelineStop::Leaf(leaf) => matches!(leaf, LogicalOp::Subquery(_)),
		PipelineStop::Node(_) => false,
	};
	holds_a_statement.then(|| super::plan::Refusal::Query {
		message: SUBQUERY_SOURCE_VERSION.to_owned(),
	})
}

/// The refusal the operator tree raises for statement `op`'s `FOR UPDATE` as
/// it plans the pipeline whose leaves carry it, by the rules of
/// [`check_locked_read`](crate::exec::planner::check_locked_read). `None` for
/// a statement whose pipeline locks nothing, or keeps those rules. A pipeline
/// under a `VERSION` of its own is not covered: the tree evaluates the stamp
/// before it checks the rules.
fn locked_read_refusal(
	planner: &Planner<'_>,
	op: &LogicalOp,
) -> Mapped<Option<super::plan::Refusal>> {
	use crate::exec::planner::fold::{PipelineStop, collect_pipeline};
	let locks = |node: &LogicalOp| {
		matches!(
			node,
			LogicalOp::Source(LogicalSource {
				for_update: true,
				..
			}) | LogicalOp::Values(crate::expr::logical::LogicalValues {
				for_update: true,
				..
			})
		)
	};
	if !op.any(locks) {
		return Ok(None);
	}
	let (clauses, stop) = collect_pipeline(op.clone());
	let sources = match stop {
		PipelineStop::Leaf(LogicalOp::UnionAll(union)) => union.inputs,
		PipelineStop::Leaf(leaf) => vec![leaf],
		// A node is planned as a pipeline of its own, whose leaves are its.
		PipelineStop::Node(_) => return Ok(None),
	};
	if !sources.iter().any(locks) {
		return Ok(None);
	}
	if clauses.version.is_some() {
		return Err(NotCovered::refused(super::plan::Refusal::Query {
			message: crate::dbs::FOR_UPDATE_VERSION_ERROR.to_string(),
		}));
	}
	match crate::exec::planner::check_locked_read(&sources, &clauses, planner.version.is_some()) {
		Ok(()) => Ok(None),
		Err(error) => super::plan::Refusal::of(&error).map(Some).ok_or_else(|| {
			NotCovered::internal("a FOR UPDATE the operator tree refuses otherwise")
		}),
	}
}

/// Whether `FETCH` path `fetch` is written as a path whose first step is a
/// field: one a fetch walks into every element of an array as it walks into
/// one of them.
fn starts_at_a_field(fetch: &LogicalExpr) -> bool {
	matches!(
		crate::exec::operators::fetch::fetch_parts(fetch).first(),
		Some(crate::exec::operators::fetch::FetchPart::Field(_))
	)
}

/// Whether `op` is a node of a read's pipeline beneath its tail and `FETCH`,
/// which [`map_rows`] maps.
fn starts_a_pipeline(op: &LogicalOp) -> bool {
	matches!(
		op,
		LogicalOp::Source(_)
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
	)
}

/// Statement `input` with `fetches` fetched into what it yields, in order: an
/// expression statement's one value, or each row of a read, beneath the
/// read's tail and after its own `FETCH`; any other statement's value, as
/// the expression it is.
async fn fetched_statement(
	planner: &Planner<'_>,
	input: &LogicalOp,
	fetches: &[LogicalExpr],
) -> Mapped<PhysicalPlan> {
	if let LogicalOp::Eval(eval) = input {
		return Ok(PhysicalPlan {
			catalog: None,
			root: Plan::Eval {
				expr: eval.expr.clone(),
				fetches: fetches.to_vec(),
			},
			dependencies: Vec::new(),
			tail: Tail::default(),
			sources: Vec::new(),
			nested: Vec::new(),
			scope: IndexScope::default(),
		});
	}
	let mut plan = crate::exec::vm::host::out_of_line(|| map_statement(planner, input)).await?;
	plan.root = match plan.root {
		// A statement refused as it is planned is refused before anything is
		// fetched.
		refused @ Plan::Refused {
			..
		} => {
			plan.root = refused;
			return Ok(plan);
		}
		// A write, a schema or INFO statement or a block yields one value, the
		// value it answers as an expression, which the fetches are taken into.
		Plan::Write {
			..
		}
		| Plan::Eval {
			..
		}
		| Plan::Ddl {
			..
		}
		| Plan::Info {
			..
		}
		| Plan::Control {
			..
		} => {
			return Ok(PhysicalPlan {
				catalog: None,
				root: Plan::Eval {
					expr: LogicalExpr::Subquery(Box::new(input.clone())),
					fetches: fetches.to_vec(),
				},
				dependencies: Vec::new(),
				tail: Tail::default(),
				sources: Vec::new(),
				nested: Vec::new(),
				scope: IndexScope::default(),
			});
		}
		Plan::Fetch {
			input,
			fetches: mut own,
		} => {
			own.extend_from_slice(fetches);
			Plan::Fetch {
				input,
				fetches: own,
			}
		}
		root => Plan::Fetch {
			input: Box::new(root),
			fetches: fetches.to_vec(),
		},
	};
	Ok(plan)
}

/// Whether `control`, an `IF` condition, is a constant, and if so whether it
/// is truthy.
fn constant_truth(control: &Control) -> Option<bool> {
	match control {
		Control::Value(expr) => {
			crate::exec::planner::util::try_literal_to_value(expr).map(|value| value.is_truthy())
		}
		_ => None,
	}
}

/// Whether statement `op` maps to control flow ([`Plan::Control`]): a block,
/// `FOR`, `RETURN` or `SLEEP`, an `IF` whose branches hold statements, or a
/// block binding left of one whose condition it folded.
fn maps_as_control(op: &LogicalOp) -> bool {
	match op {
		LogicalOp::Sequence(_)
		| LogicalOp::Return(_)
		| LogicalOp::Foreach(_)
		| LogicalOp::Sleep(_) => true,
		LogicalOp::Eval(eval) => match &eval.expr {
			LogicalExpr::If(ifs) => ifs
				.branches
				.iter()
				.map(|(_, value)| value)
				.chain(ifs.otherwise.as_ref())
				.any(|value| matches!(value, LogicalExpr::Subquery(_))),
			LogicalExpr::Subquery(inner) => matches!(**inner, LogicalOp::Sequence(_)),
			_ => false,
		},
		LogicalOp::If(ifs) => ifs.arms.iter().any(arm_holds_a_statement),
		_ => false,
	}
}

/// Whether `op`, the value of a `LET` or `RETURN`, is mapped inline as the
/// control flow whose `RETURN` that `LET` or `RETURN` catches. A value holding a
/// schema statement is mapped as a statement, so it is declined or planned
/// late-bound. So is a `RETURN` of a `RETURN`, because its group shapes the
/// value as rows.
fn inline_value(op: &LogicalOp) -> bool {
	maps_as_control(op)
		&& !matches!(op, LogicalOp::Return(_))
		&& !crate::exec::planner::fold::changes_the_catalog(op)
}

/// Whether an arm of a statement `IF` holds a statement: one that is not an
/// expression, or is the expression of a statement in value position.
fn arm_holds_a_statement(arm: &LogicalOp) -> bool {
	match arm {
		LogicalOp::Eval(eval) => matches!(eval.expr, LogicalExpr::Subquery(_)),
		LogicalOp::If(ifs) => ifs.arms.iter().any(arm_holds_a_statement),
		LogicalOp::Throw(_) | LogicalOp::Break(_) | LogicalOp::Continue(_) => false,
		_ => true,
	}
}

/// Whether the statement `op` is the rows of asks for `TEMPFILES` on its sort.
fn asks_for_tempfiles(op: &LogicalOp) -> bool {
	let mut node = op;
	loop {
		node = match node {
			LogicalOp::Sort(sort) => return sort.tempfiles,
			LogicalOp::Project(project) => &project.input,
			LogicalOp::Limit(limit) => &limit.input,
			LogicalOp::Fetch(fetch) => &fetch.input,
			LogicalOp::Split(split) => &split.input,
			_ => return false,
		};
	}
}

/// The target a write's rows `input` names where it is a table or a record
/// read as it stands, with no schema resolved for it.
fn unresolved_target(input: &LogicalOp) -> Option<LogicalExpr> {
	let LogicalOp::Source(crate::expr::logical::LogicalSource {
		source,
		schema: None,
		with: None,
		for_update: false,
	}) = input
	else {
		return None;
	};
	match source {
		SourceRef::Table(table) => Some(LogicalExpr::Table(table.clone())),
		SourceRef::RecordId(rid) => Some(LogicalExpr::RecordId(rid.clone())),
		SourceRef::Mock(_) | SourceRef::Param(_) | SourceRef::Dynamic(_) => None,
	}
}

/// Map a mutation. Its `ONLY` is the statement's own, applied by the write
/// body rather than by the tail.
async fn map_write(planner: &Planner<'_>, write: &LogicalWrite) -> Mapped<PhysicalPlan> {
	let mut dependencies = Vec::new();
	let mut write = write.clone();
	let mut scope = IndexScope::default();
	// An `UPSERT` of tables under a condition reads their records through the
	// access path a read of them under the same condition takes, then writes
	// the record it guarantees when the condition matched none. A table the
	// catalog does not hold has no records to read, and a late-bound write
	// reads its tables' catalog as it runs, so each of those writes the
	// records its targets name.
	let mut upserted = Vec::new();
	if let (LogicalWriteStatement::Upsert(stmt), LogicalWriteSource::Targets(targets)) =
		(&write.statement, &write.source)
		&& write.cond.is_some()
		&& !planner.is_late_bound()
		&& let (Some(txn), Some((ns, db))) =
			(planner.txn(), crate::exec::vm::host::out_of_line(|| planner.ns_db_ids()).await)
	{
		for target in targets {
			let LogicalExpr::Table(table) = target else {
				upserted.clear();
				break;
			};
			if txn.get_tb(ns, db, table, None).await.ok().flatten().is_none() {
				upserted.clear();
				break;
			}
			upserted.push(LogicalOp::Source(LogicalSource {
				source: SourceRef::Table(table.clone()),
				// The records as stored: the write's gate and its stages
				// judge each one.
				schema: Some(std::sync::Arc::new(crate::expr::logical::Schema::open())),
				with: stmt.with.clone(),
				for_update: false,
			}));
		}
	}
	let subjects = match &write.source {
		LogicalWriteSource::Targets(targets)
			if !upserted.is_empty()
				&& let Some(cond) = write.cond.clone() =>
		{
			let source = match <[LogicalOp; 1]>::try_from(std::mem::take(&mut upserted)) {
				Ok([source]) => source,
				Err(inputs) => LogicalOp::UnionAll(crate::expr::logical::LogicalUnionAll {
					inputs,
				}),
			};
			let read = write_rows(planner, &source, &cond).await?;
			write.cond = read.cond;
			scope = read.scope;
			dependencies = read.dependencies;
			WriteSubjects::Guaranteed {
				rows: Box::new(read.root),
				targets: targets.clone(),
			}
		}
		LogicalWriteSource::Targets(targets)
			if matches!(write.statement, LogicalWriteStatement::Relate(_)) =>
		{
			let [from, through, to] =
				<&[LogicalExpr; 3]>::try_from(targets.as_slice()).map_err(|_| {
					NotCovered::internal("a RELATE that names other than three expressions")
				})?;
			WriteSubjects::Edges {
				from: from.clone(),
				through: through.clone(),
				to: to.clone(),
			}
		}
		LogicalWriteSource::Targets(targets) => WriteSubjects::Targets(targets.clone()),
		// A late-bound write over a table or a record its binding left
		// unresolved writes the records stored there, gated as the write runs,
		// as a target naming them does.
		LogicalWriteSource::Rows(input)
			if planner.is_late_bound()
				&& let Some(target) = unresolved_target(input) =>
		{
			WriteSubjects::Targets(vec![target])
		}
		LogicalWriteSource::Rows(input) if classified_targets(input) => {
			WriteSubjects::Targets(target_exprs((**input).clone()))
		}
		// Values an expression yields are the subjects as they stand: a record
		// id among them names the record, which the write reads from storage
		// as it reads a target's, never through the caller's view of it. A
		// subquery's rows are classified targets, taken by the arm above.
		LogicalWriteSource::Rows(input) if matches!(**input, LogicalOp::Values(_)) => {
			let rows = crate::exec::vm::host::out_of_line(|| map_value_source(planner, input))
				.await?
				.ok_or_else(|| NotCovered::internal("a write's rows the VM does not read"))?;
			WriteSubjects::Rows(Box::new(rows))
		}
		LogicalWriteSource::Rows(input) => {
			let read = match write.cond.clone() {
				Some(cond) => write_rows(planner, input, &cond).await?,
				None => {
					let read =
						crate::exec::vm::host::out_of_line(|| map_rows(planner, input, false))
							.await?;
					WriteRows {
						root: read.root,
						dependencies: read.dependencies,
						scope: read.scope,
						cond: None,
					}
				}
			};
			// A write persists the rows it reads; rows put through a table's
			// enforcement are the caller's view of them, not the records.
			if reads_enforced(&read.root) {
				return Err(NotCovered::new("a write over a source its scan enforces"));
			}
			write.cond = read.cond;
			scope = read.scope;
			dependencies = read.dependencies;
			WriteSubjects::Rows(Box::new(read.root))
		}
		LogicalWriteSource::Inserts(inserts) => WriteSubjects::Inserts(inserts.clone()),
	};
	// Subjects no read reaches are judged against the tables the targets
	// name ([`write_index_scope`]).
	if !scope.select && matches!(subjects, WriteSubjects::Targets(_)) {
		scope = write_index_scope(planner, &write).await?;
	}
	Ok(PhysicalPlan {
		catalog: None,
		root: Plan::Write {
			write: Box::new(write),
			subjects,
		},
		dependencies,
		tail: Tail::default(),
		sources: Vec::new(),
		nested: Vec::new(),
		scope,
	})
}

/// The read a write's subjects come from under its condition, and what the
/// write judges each subject by.
struct WriteRows {
	root: Plan,
	dependencies: Vec<Dependency>,
	/// The `MATCHES` clauses and `KNN` operator the condition declares.
	scope: IndexScope,
	/// The condition each subject is judged by: the write's own, less the
	/// `KNN` operators the read's search answered.
	cond: Option<LogicalExpr>,
}

/// The read of `input`'s records under condition `cond`, by the access path a
/// read of `input` filtered by `cond` takes: an index, a full-text or vector
/// search, a record lookup, or the table.
///
/// The read yields candidates and judges none of them, so every record it
/// yields reaches the write's gate, which runs before the condition; the
/// write judges the condition per record. A `KNN` search is the exception:
/// the nearest records it ranks are those the rest of the condition admits,
/// and the operator itself is answered by the search, so it leaves the
/// condition the write judges. A brute-force ranking holds every candidate
/// before it yields one, so it is read as a statement of its own.
async fn write_rows(
	planner: &Planner<'_>,
	input: &LogicalOp,
	cond: &LogicalExpr,
) -> Mapped<WriteRows> {
	let filtered = LogicalOp::Filter(crate::expr::logical::LogicalFilter {
		input: Box::new(input.clone()),
		predicate: cond.clone(),
	});
	let read = crate::exec::vm::host::out_of_line(|| map_rows(planner, &filtered, false)).await?;
	let searched = holds_on_chain(&read.root, &|node| {
		matches!(node, Plan::KnnScan { .. } | Plan::KnnTopK { .. })
	});
	let ranked = holds_on_chain(&read.root, &|node| matches!(node, Plan::KnnTopK { .. }));
	let mut root = unfiltered(read.root);
	if ranked {
		root = Plan::Subquery {
			plan: Box::new(super::optimize::optimize(PhysicalPlan {
				catalog: None,
				root,
				dependencies: Vec::new(),
				tail: Tail::default(),
				sources: Vec::new(),
				nested: Vec::new(),
				scope: read.scope.clone(),
			})),
		};
	}
	// A condition holding a `MATCHES` is judged as the access path reads it,
	// as for a read: with its row-independent operands folded.
	let cond = if expr_contains_matches(cond) {
		planner
			.fold_pipeline_conds(&filtered, false, vec![cond.clone()])
			.await
			.pop()
			.unwrap_or_else(|| cond.clone())
	} else {
		cond.clone()
	};
	let mut cond = if searched {
		strip_knn_from_condition(&cond)
	} else {
		Some(cond)
	};
	// A full-text scan answers its own probe for its own rows, as for a read:
	// where every source is one, the probe each answers leaves the condition.
	let leaves = chain_leaves(&root);
	let probes: Vec<_> = leaves
		.iter()
		.filter_map(|leaf| match leaf {
			Plan::FullTextScan {
				index,
				query,
				operator,
				..
			} => index.cols.first().map(|column| (column.clone(), operator.clone(), query.clone())),
			_ => None,
		})
		.collect();
	if let Some(held) = cond.take() {
		cond = if !probes.is_empty() && probes.len() == leaves.len() {
			let probes: Vec<_> = probes
				.iter()
				.map(|(column, operator, query)| (column, operator, query.as_str()))
				.collect();
			crate::exec::planner::util::strip_fts_probes(&held, &probes)
		} else {
			Some(held)
		};
	}
	Ok(WriteRows {
		root,
		dependencies: read.dependencies,
		scope: read.scope,
		cond,
	})
}

/// The sources at the foot of `plan`'s chain: the inputs of a union, or its
/// one source.
fn chain_leaves(plan: &Plan) -> Vec<&Plan> {
	match plan {
		Plan::Filter {
			input,
			..
		}
		| Plan::Project {
			input,
			..
		}
		| Plan::Layer {
			input,
			..
		}
		| Plan::PermissionFilter {
			input,
			..
		} => chain_leaves(input),
		Plan::Union {
			inputs,
		} => inputs.iter().flat_map(chain_leaves).collect(),
		leaf => vec![leaf],
	}
}

/// `plan` without the filters on its chain above its source, a ranking and
/// what it ranks excepted: the rows every candidate the access path yields.
fn unfiltered(plan: Plan) -> Plan {
	match plan {
		Plan::Filter {
			input,
			..
		} => unfiltered(*input),
		mut node @ (Plan::Project {
			..
		}
		| Plan::Layer {
			..
		}
		| Plan::PermissionFilter {
			..
		}) => {
			if let Plan::Project {
				input,
				..
			}
			| Plan::Layer {
				input,
				..
			}
			| Plan::PermissionFilter {
				input,
				..
			} = &mut node
			{
				let inner = std::mem::replace(
					&mut **input,
					Plan::Union {
						inputs: Vec::new(),
					},
				);
				**input = unfiltered(inner);
			}
			node
		}
		other => other,
	}
}

/// The `MATCHES` clauses and `KNN` operator a write over targets, `write`,
/// declares: a `MATCHES` an `UPDATE` or a `DELETE` evaluates, or an `UPSERT`
/// under a condition, is answered by the full-text index of a table the
/// targets name whole. The plan holds it, and so do the stages a run loads
/// by table name.
pub(super) async fn write_index_scope(
	planner: &Planner<'_>,
	write: &LogicalWrite,
) -> Mapped<IndexScope> {
	let judged = match &write.statement {
		LogicalWriteStatement::Update(_) | LogicalWriteStatement::Delete(_) => true,
		LogicalWriteStatement::Upsert(_) => write.cond.is_some(),
		_ => false,
	};
	let targets = match &write.source {
		LogicalWriteSource::Targets(targets) => targets.clone(),
		LogicalWriteSource::Rows(input) if classified_targets(input) => {
			target_exprs((**input).clone())
		}
		LogicalWriteSource::Rows(input) => match unresolved_target(input) {
			Some(target) => vec![target],
			None => return Ok(IndexScope::default()),
		},
		LogicalWriteSource::Inserts(_) => return Ok(IndexScope::default()),
	};
	if !judged {
		return Ok(IndexScope::default());
	}
	target_scope(planner, write.cond.as_ref(), &targets).await
}

/// The `MATCHES` clauses and `KNN` operator write condition `cond` declares
/// over the subjects `targets` name: an index answers a `MATCHES` for the
/// records of a table named whole, a `type::table` call's included, and never
/// for those a record id or any other expression names.
async fn target_scope(
	planner: &Planner<'_>,
	cond: Option<&LogicalExpr>,
	targets: &[LogicalExpr],
) -> Mapped<IndexScope> {
	let mut known = Vec::new();
	let mut excluded = Vec::new();
	let mut open = false;
	for target in targets {
		match target {
			LogicalExpr::Table(table) => known.push(table.clone()),
			LogicalExpr::RecordId(rid) => excluded.push(rid.table.clone()),
			LogicalExpr::Call(call) if matches!(&call.receiver, crate::expr::Function::Normal(name) if name == "type::table") => {
				open = true
			}
			_ => {}
		}
	}
	let Some(cond) = cond else {
		return index_scope_over(planner, &[], &[], known, excluded, open);
	};
	let stages = [RowStage::Filter(cond.clone())];
	// A `MATCHES` is registered as written and with its row-independent
	// operands folded, as a read of the first table named whole folds it.
	let folded = match known.first() {
		Some(table) if expr_contains_matches(cond) => {
			let op = LogicalOp::Filter(crate::expr::logical::LogicalFilter {
				input: Box::new(LogicalOp::Source(LogicalSource {
					source: SourceRef::Table(table.clone()),
					schema: None,
					with: None,
					for_update: false,
				})),
				predicate: cond.clone(),
			});
			crate::exec::vm::host::out_of_line(|| {
				planner.fold_pipeline_conds(&op, false, vec![cond.clone()])
			})
			.await
			.into_iter()
			.next()
		}
		_ => None,
	};
	index_scope_over(planner, &stages, &[folded], known, excluded, open)
}

/// Map a statement the count fast path answers: one counted row, paged by
/// the statement's `LIMIT` and `START`. `None` when no count path answers it.
async fn map_count(
	planner: &Planner<'_>,
	op: &LogicalOp,
	versioned: bool,
) -> Mapped<Option<PhysicalPlan>> {
	// The count fast path judges the table's permission and its count
	// indexes as planning finds them; a late-bound statement meets them only
	// when it runs, so it counts the rows its enforced read yields.
	if planner.is_late_bound() {
		return Ok(None);
	}
	let offered = planner
		.count_offer(op, versioned)
		.await
		.map_err(|e| NotCovered::internal(format!("the count did not plan: {e}")))?;
	let Some((count, limit, start)) = offered else {
		return Ok(None);
	};
	let table = match &count {
		CountDecision::All {
			source: SourceRef::Table(table),
			..
		}
		| CountDecision::Indexed {
			table,
			..
		} => table.clone(),
		CountDecision::All {
			source: SourceRef::RecordId(record),
			..
		} => record.table.clone(),
		CountDecision::All {
			..
		} => return Err(NotCovered::internal("a count over a source resolved when it runs")),
	};
	let dependency = table_dependency(planner, &table).await?;
	let select = count_select(planner, &table).await?;
	let deltas = count_deltas(planner, &table, &count).await?;
	// An indexed count is planned only where a structure answers it, and a
	// catalog read that finds none has changed under the plan.
	if matches!(
		&count,
		CountDecision::Indexed {
			btree_access: None,
			bitmap_plan: None,
			..
		}
	) && deltas.is_none()
	{
		return Err(NotCovered::internal("an indexed count no index answers"));
	}
	let ordinary = if select == CountSelect::Conditional {
		Some(Box::new(map_rows(planner, op, versioned).await?))
	} else {
		None
	};
	let mut root = Plan::Count {
		count,
		select,
		deltas,
		ordinary,
	};
	if limit.is_some() || start.is_some() {
		root = Plan::Limit {
			input: Box::new(root),
			limit,
			start,
		};
	}
	Ok(Some(PhysicalPlan {
		catalog: None,
		root: Plan::Project {
			input: Box::new(root),
			projection: Projection::All,
		},
		dependencies: vec![dependency],
		tail: Tail::default(),
		sources: Vec::new(),
		nested: Vec::new(),
		scope: IndexScope::default(),
	}))
}

/// What the `SELECT` permission of `table` does to a caller whose reads are
/// checked.
async fn count_select(planner: &Planner<'_>, table: &TableName) -> Mapped<CountSelect> {
	use crate::catalog::Permission;
	use crate::expr::logical::LogicalPermission;
	let Some(txn) = planner.txn() else {
		return Err(NotCovered::internal("no transaction at plan time"));
	};
	let Some((ns, db)) = crate::exec::vm::host::out_of_line(|| planner.ns_db_ids()).await else {
		return Err(NotCovered::unresolved_database(planner));
	};
	let def = txn
		.get_tb(ns, db, table, None)
		.await
		.map_err(|_| NotCovered::internal("the table catalog could not be read"))?;
	// A table that does not exist is not visible; the count reports it missing
	// before any permission is judged.
	let Some(def) = def else {
		return Ok(CountSelect::Deny);
	};
	Ok(match def.select_permission_logical() {
		Ok(LogicalPermission::None) => CountSelect::Deny,
		Ok(LogicalPermission::Full) => CountSelect::Allow,
		Ok(LogicalPermission::Specific(_)) => CountSelect::Conditional,
		Err(_) => match &def.permissions.select {
			Permission::None => CountSelect::Deny,
			Permission::Full => CountSelect::Allow,
			Permission::Specific(_) => CountSelect::Conditional,
		},
	})
}

/// The online `COUNT` index of `table` that answers `count` from its deltas:
/// the one with no guard for a count of the whole table, the one whose guard
/// is the counted condition for an indexed count.
async fn count_deltas(
	planner: &Planner<'_>,
	table: &TableName,
	count: &CountDecision,
) -> Mapped<Option<crate::catalog::IndexId>> {
	use surrealdb_types::ToSql;

	use crate::catalog::Index;
	let Some(txn) = planner.txn() else {
		return Err(NotCovered::internal("no transaction at plan time"));
	};
	let Some((ns, db)) = crate::exec::vm::host::out_of_line(|| planner.ns_db_ids()).await else {
		return Err(NotCovered::unresolved_database(planner));
	};
	let indexes = txn
		.all_tb_indexes(ns, db, table, None)
		.await
		.map_err(|_| NotCovered::internal("the index catalog could not be read"))?;
	let indexes = crate::kvs::index::filter_online_indexes(txn, ns, db, indexes)
		.await
		.map_err(|_| NotCovered::internal("the index catalog could not be read"))?;
	Ok(match count {
		CountDecision::All {
			..
		} => indexes.iter().find(|ix| matches!(&ix.index, Index::Count(None))).map(|ix| ix.index_id),
		CountDecision::Indexed {
			cond,
			..
		} => {
			let cond = cond.to_sql();
			indexes
				.iter()
				.find(|ix| {
					matches!(&ix.index, Index::Count(Some(_)))
						&& ix.count_cond.as_ref().map(|guard| guard.0.to_sql()).as_deref()
							== Some(cond.as_str())
				})
				.map(|ix| ix.index_id)
		}
	})
}

/// Whether `source` is a call whose rows the statement names only when it
/// runs unless [`Planner::resolve_call_source`] can fold it.
fn is_call_source(op: &LogicalOp) -> bool {
	matches!(
		op,
		LogicalOp::Source(LogicalSource {
			source: SourceRef::Dynamic(LogicalExpr::Call(_)),
			for_update: false,
			..
		})
	)
}

/// `op` with every call source whose table or record the call denotes at plan
/// time (`type::table($name)` with `$name` bound) replaced by that table or
/// record, so it gets the access path, paging and ordering of a literal name.
/// The source carries no schema, so its scan enforces the table's permissions
/// when it runs.
fn with_call_sources_resolved(planner: &Planner<'_>, op: &LogicalOp) -> LogicalOp {
	let resolved: Result<LogicalOp, std::convert::Infallible> =
		op.clone().transform_up(&mut |node| {
			Ok(match node {
				node if is_call_source(&node) => {
					let LogicalOp::Source(mut source) = node else {
						unreachable!("a call source is a source")
					};
					source.source = planner.resolve_call_source(source.source);
					LogicalOp::Source(source)
				}
				other => other,
			})
		});
	match resolved {
		Ok(resolved) => resolved,
		Err(never) => match never {},
	}
}

/// Map the part of a statement that shapes its rows.
async fn map_rows(planner: &Planner<'_>, op: &LogicalOp, versioned: bool) -> Mapped<PhysicalPlan> {
	let resolved;
	let op = if op
		.visit(&mut |node| {
			if is_call_source(node) {
				std::ops::ControlFlow::Break(())
			} else {
				std::ops::ControlFlow::Continue(())
			}
		})
		.is_break()
	{
		resolved = with_call_sources_resolved(planner, op);
		&resolved
	} else {
		op
	};
	// A plan with no projection delivers its rows as they are: the bind
	// passes drop `SELECT *` over a closed schema, where it is the identity.
	let mut omit_refusal = None;
	let (mut projection, projected, mut below) = match op {
		// A layer at the top is the table's, not the statement's projection.
		LogicalOp::Project(_) if op.is_layer() => (Projection::All, None, op),
		LogicalOp::Project(project) => {
			let omit = match omissions(planner, &project.omit).await? {
				Ok(omit) => omit,
				Err(refusal) => {
					omit_refusal = Some(refusal);
					Vec::new()
				}
			};
			(projection(&project.fields, omit)?, Some(&project.fields), &*project.input)
		}
		other => (Projection::All, None, other),
	};
	let limit = match below {
		LogicalOp::Limit(limit) => {
			below = &limit.input;
			Some((limit.limit.clone(), limit.start.clone()))
		}
		_ => None,
	};
	let ordering = match below {
		LogicalOp::Sort(sort) => {
			below = &sort.input;
			Some(&sort.ordering)
		}
		_ => None,
	};
	// A grouped projection that reads `$this` or `$parent` is refused as the
	// operator tree plans the read, once it has planned the read's source,
	// which is mapped first. A grouped read that locks its own records is
	// refused for its locking before it reaches here.
	let mut refused = None;
	let aggregate = match below {
		LogicalOp::Aggregate(aggregate) => {
			// The operator tree checks the grouped projection's parameters,
			// then analyses its fields; either may refuse the statement.
			let refusal = check_forbidden_group_by_params(&aggregate.fields).and_then(|()| {
				crate::exec::planner::analyse_aggregation(
					planner.function_registry(),
					&aggregate.fields,
					&aggregate.group,
				)
				.map(|_| ())
			});
			if let Err(error) = refusal {
				// Both raise only query errors, which the refusal carries.
				refused = Some(super::plan::Refusal::of(&error).ok_or_else(|| {
					NotCovered::internal(format!("a grouped projection refused otherwise: {error}"))
				})?);
			}
			below = &aggregate.input;
			Some((aggregate.group.clone(), aggregate.fields.clone()))
		}
		_ => None,
	};
	// `SPLIT` names a field of the row the statement returns, so it splits
	// the projected row: its input is the statement's filtered rows, mapped
	// as rows of their own and projected; the ordering and paging above read
	// the rows it splits them into, under the names the projection gave them.
	// `SPLIT` and `GROUP` are exclusive, which the parser enforces.
	if let LogicalOp::Split(split) = below {
		if split.exprs.iter().any(|target| crate::exec::operators::split_target(target).is_none()) {
			return Err(NotCovered::internal("a SPLIT target with no write position"));
		}
		if aggregate.is_some() {
			return Err(NotCovered::internal("a SPLIT under GROUP"));
		}
		let sort = match ordering {
			Some(LogicalOrdering::Random) => Some(SortClause {
				keys: Vec::new(),
				random: true,
			}),
			Some(ordering) => Some(SortClause {
				keys: sort_keys(ordering, None, planner.function_registry())?,
				random: false,
			}),
			None => None,
		};
		let input =
			crate::exec::vm::host::out_of_line(|| map_rows(planner, &split.input, versioned))
				.await?;
		// The projection is of the rows the statement's predicates filtered,
		// so the index functions in it answer for the clauses those declare.
		let scope = input.scope.clone();
		let rows = match input.root {
			Plan::Project {
				input,
				projection: Projection::All,
			} => *input,
			other => other,
		};
		let split = Plan::Split {
			input: Box::new(Plan::Project {
				input: Box::new(rows),
				projection,
			}),
			targets: split.exprs.clone(),
		};
		return Ok(PhysicalPlan {
			catalog: None,
			root: clauses_over(split, None, sort, limit, Projection::All),
			dependencies: input.dependencies,
			tail: Tail::default(),
			sources: Vec::new(),
			nested: Vec::new(),
			scope,
		});
	}
	// Over an aggregate, the rows sorted are the groups it finished, so a key
	// reads the group row's field of that name rather than recomputing what an
	// alias of the projection names.
	let sort = match ordering {
		Some(LogicalOrdering::Random) => Some(SortClause {
			keys: Vec::new(),
			random: true,
		}),
		Some(ordering) => Some(SortClause {
			keys: sort_keys(
				ordering,
				match aggregate {
					Some(_) => None,
					None => projected.map(|fields| (fields, &mut projection)),
				},
				planner.function_registry(),
			)?,
			random: false,
		}),
		None => None,
	};
	// The statement's predicates, the table's permission and its computed
	// fields and field guards, outermost first. A table's permission sits
	// directly above its source and judges the computed fields it reads
	// itself; its layers sit above it. A predicate reading what a layer
	// derives stays above that layer, and one reading none of it may sit
	// beneath, so predicates and layers interleave.
	let mut stages = Vec::new();
	loop {
		match below {
			LogicalOp::Filter(filter) => {
				stages.push(RowStage::Filter(filter.predicate.clone()));
				below = &filter.input;
			}
			LogicalOp::Project(layer) if below.is_layer() => {
				stages.push(RowStage::Layer(layer.fields.clone(), layer.input.schema()));
				below = &layer.input;
			}
			LogicalOp::PermissionFilter(permission) => {
				stages.push(RowStage::Permission(
					permission.predicate.clone(),
					permission.auth.clone(),
					permission.guard,
					computed_reads(&permission.input, &permission.predicate, planner),
				));
				below = &permission.input;
			}
			_ => break,
		}
	}
	let layered = stages.iter().any(|stage| matches!(stage, RowStage::Layer(..)));
	// A table's permission sits directly above its source when nothing
	// derives beneath it: those permissions are the source's own, beneath
	// every user predicate, which the bind passes move no user expression
	// below.
	let mut permissions = Vec::new();
	while let Some(RowStage::Permission(..)) = stages.last() {
		if let Some(RowStage::Permission(predicate, auth, guard, computed)) = stages.pop() {
			permissions.push((predicate, auth, guard, computed));
		}
	}
	permissions.reverse();
	let interleaved = stages.iter().any(|stage| matches!(stage, RowStage::Permission(..)));
	// `FROM a, b` reads each source in written order.
	let inputs: Vec<&LogicalOp> = match below {
		LogicalOp::UnionAll(union) => union.inputs.iter().collect(),
		other => vec![other],
	};
	// A read at a stamp ranks the nearest rows among the records it reads at
	// the stamp: an ANN index describes the present.
	if let [
		LogicalOp::Source(LogicalSource {
			source: SourceRef::Table(table),
			..
		}),
	] = inputs.as_slice()
	{
		for stage in &mut stages {
			if let RowStage::Filter(predicate) = stage {
				*predicate = crate::exec::vm::host::out_of_line(|| {
					planner.exact_knn_at_stamp(table, predicate.clone())
				})
				.await;
			}
		}
	}
	// A source that is not a table or a record id carries no table's
	// permission or layers of its own.
	if (!permissions.is_empty() || layered || interleaved)
		&& inputs.iter().any(|input| !is_table_source(input))
	{
		return Err(NotCovered::internal("a permission or layer over a value source"));
	}

	// A `MATCHES` is answered by an index from its query as the access path
	// reads it: a row-independent operand folded to the value it names. A
	// predicate holding one is folded the same way.
	let mut folded: Vec<Option<LogicalExpr>> = vec![None; stages.len()];
	let holds_matches: Vec<usize> = stages
		.iter()
		.enumerate()
		.filter_map(|(at, stage)| match stage {
			RowStage::Filter(predicate)
				if crate::exec::planner::util::expr_contains_matches(predicate) =>
			{
				Some(at)
			}
			_ => None,
		})
		.collect();
	if !holds_matches.is_empty() {
		let predicates = holds_matches
			.iter()
			.filter_map(|at| match &stages[*at] {
				RowStage::Filter(predicate) => Some(predicate.clone()),
				RowStage::Layer(..) | RowStage::Permission(..) => None,
			})
			.collect();
		let predicates = crate::exec::vm::host::out_of_line(|| {
			planner.fold_pipeline_conds(op, versioned, predicates)
		})
		.await;
		for (at, predicate) in holds_matches.into_iter().zip(predicates) {
			folded[at] = Some(predicate);
		}
	}
	let scope = index_scope(planner, &stages, &folded, &inputs)?;
	// Beyond the clauses they declare, a predicate holding a `MATCHES` is
	// judged and evaluated as the access path reads it: a probe a scan
	// answers is the conjunct whose query folds to the scan's, and what the
	// scan leaves is evaluated with its operands folded.
	for (stage, read) in stages.iter_mut().zip(folded) {
		if let (RowStage::Filter(predicate), Some(read)) = (stage, read) {
			*predicate = read;
		}
	}
	let mut offer =
		crate::exec::vm::host::out_of_line(|| planner.access_offer(op, versioned)).await;
	let hides_access_paths = offer.permissioned && planner.reads_are_checked();
	// A `KNN` operator the statement's `WHERE` holds beside what a scan or a
	// brute-force search consumes is refused before a row is read, and a
	// brute-force search ranks the rows the other conditions leave.
	let knn_refusal = knn_placement_refusal(&stages);
	let brute = match knn_refusal {
		Some(_) => None,
		None => stages.iter().rev().find_map(|stage| match stage {
			RowStage::Filter(predicate) => {
				crate::exec::planner::util::extract_bruteforce_knn(predicate)
			}
			RowStage::Layer(..) | RowStage::Permission(..) => None,
		}),
	};
	let brute = brute.map(|params| brute_force_search(planner, params));
	// The stage the search ranks the rows beneath: the innermost predicate
	// holding it. Predicates and layers beneath that stage are applied before
	// the ranking, and those above it after.
	let ranked_at = brute.as_ref().and_then(|_| {
		stages.iter().rposition(|stage| match stage {
			RowStage::Filter(predicate) => {
				crate::exec::planner::util::extract_bruteforce_knn(predicate).is_some()
			}
			RowStage::Layer(..) | RowStage::Permission(..) => false,
		})
	});
	if brute.is_some() {
		offer.cond = offer.cond.as_ref().and_then(strip_knn_from_condition);
	}
	let several = inputs.len() > 1;
	let mut leaves = Vec::with_capacity(inputs.len());
	let mut dependencies = Vec::with_capacity(inputs.len());
	// The table each leaf reads its records from; `None` for a value source.
	let mut tables = Vec::with_capacity(inputs.len());
	// Whether each leaf reads rows its scan enforces the table's permission,
	// computed fields and field permissions on.
	let mut enforced = Vec::with_capacity(inputs.len());
	// A statement refused for a `KNN` operator is refused whatever its sources
	// would have said of themselves.
	let sourced: Mapped<()> = async {
		for input in inputs {
			match input {
				LogicalOp::Source(source) if is_table_source(input) => {
					let (leaf, dependency, enforces) =
						map_source(planner, source, &offer, versioned).await?;
					let Dependency::Table {
						table,
						..
					} = &dependency;
					tables.push(Some(table.clone()));
					leaves.push(leaf);
					enforced.push(enforces);
					dependencies.push(dependency);
				}
				// A table's stack of its own over its source, one input of a
				// `FROM` naming several: read as a statement of its own.
				other if holds_a_stack(other) => {
					let inner =
						crate::exec::vm::host::out_of_line(|| map_rows(planner, other, versioned))
							.await?;
					dependencies.extend(inner.dependencies.iter().cloned());
					tables.push(None);
					enforced.push(false);
					leaves.push(Plan::Subquery {
						plan: Box::new(super::optimize::optimize(inner)),
					});
				}
				other => {
					match crate::exec::vm::host::out_of_line(|| map_value_source(planner, other))
						.await?
					{
						Some(leaf) => {
							tables.push(None);
							enforced.push(false);
							leaves.push(leaf);
						}
						None => {
							let (leaf, inner) = crate::exec::vm::host::out_of_line(|| {
								map_barrier(planner, other, versioned)
							})
							.await?;
							dependencies.extend(inner);
							tables.push(None);
							enforced.push(false);
							// Where several sources are read each is a statement of
							// its own, as a union reads storage alone.
							leaves.push(match leaf {
								Plan::Subquery {
									..
								} => leaf,
								rows if several => Plan::Subquery {
									plan: Box::new(super::optimize::optimize(PhysicalPlan {
										catalog: None,
										root: Plan::Project {
											input: Box::new(rows),
											projection: Projection::All,
										},
										dependencies: Vec::new(),
										tail: Tail::default(),
										sources: Vec::new(),
										nested: Vec::new(),
										scope: IndexScope::default(),
									})),
								},
								rows => rows,
							});
						}
					}
				}
			}
		}
		Ok(())
	}
	.await;
	if let Err(declined) = sourced {
		match &knn_refusal {
			Some(refusal) => {
				return Ok(PhysicalPlan {
					catalog: None,
					root: Plan::Refused {
						refusal: refusal.clone(),
					},
					dependencies,
					tail: Tail::default(),
					sources: Vec::new(),
					nested: Vec::new(),
					scope,
				});
			}
			None => return Err(declined),
		}
	}

	if let Some(refusal) = knn_refusal {
		return Ok(PhysicalPlan {
			catalog: None,
			root: Plan::Refused {
				refusal,
			},
			dependencies,
			tail: Tail::default(),
			sources: Vec::new(),
			nested: Vec::new(),
			scope,
		});
	}

	// A full-text scan answers its own probe for its own rows only. A
	// `MATCHES` conjunct leaves the statement's predicates only where every
	// source is a full-text scan and the conjunct is the probe of each; any
	// other conjunct, and every conjunct beside another kind of source, is
	// evaluated per row over the union.
	let all_fts = leaves.iter().all(|l| matches!(l, Plan::FullTextScan { .. }));
	// The probe each full-text scan answers; a scan over an index with no
	// column has none, and then nothing is stripped.
	let fts_probes: Vec<_> = leaves
		.iter()
		.filter_map(|leaf| match leaf {
			Plan::FullTextScan {
				index,
				query,
				operator,
				..
			} => index.cols.first().map(|column| (column.clone(), operator.clone(), query.clone())),
			_ => None,
		})
		.collect();
	let all_fts = all_fts && fts_probes.len() == leaves.len();
	let has_knn = leaves.iter().any(|l| matches!(l, Plan::KnnScan { .. }));
	let strips_knn = has_knn || brute.is_some();
	// Every row the search yields is one its KNN operator admits, so the
	// operator in the selection answers `true` for each, as the operator
	// tree's answers it from the search's results.
	if strips_knn {
		knn_answered(&mut projection);
	}

	// The clauses that read the rows the leaves yield: the statement's
	// predicates, its omissions, and its selection or, under an aggregate,
	// the grouped fields and keys.
	let row_cond = stages
		.iter()
		.filter_map(|stage| match stage {
			RowStage::Filter(predicate) => Some(predicate.clone()),
			RowStage::Layer(..) | RowStage::Permission(..) => None,
		})
		.reduce(|left, right| LogicalExpr::Binary {
			left: Box::new(left),
			op: BinaryOperator::And,
			right: Box::new(right),
		});
	let row_omit: &[LogicalExpr] = match op {
		LogicalOp::Project(project) if !op.is_layer() => &project.omit,
		_ => &[],
	};
	let (row_fields, row_group) = match &aggregate {
		Some((group, fields)) => (Some(fields), Some(group.as_slice())),
		None => (projected, None),
	};

	// An index-backed KNN scan builds its rows from the ids its search returns
	// when nothing reads more of a row than its id. The table's permission
	// and layers read the record, so a table that has either reads it, and a
	// versioned read reads its records at the version.
	if has_knn && brute.is_none() && permissions.is_empty() && !layered && !versioned {
		for leaf in &mut leaves {
			if let Plan::KnnScan {
				table,
				ids_only,
				..
			} = leaf
			{
				*ids_only = planner
					.knn_rows_need_only_ids(
						table,
						row_fields,
						row_omit,
						row_cond.as_ref(),
						ordering,
						row_group,
					)
					.await;
			}
		}
	}

	// A scan that fetches its records by the ids an index lists decodes only
	// the fields the clauses above it name, where nothing else reads the
	// stored record: no permission or layer of the table stands above it and
	// the scan enforces none itself. A KNN scan decodes whole records.
	if permissions.is_empty() && !layered && !interleaved && !enforced.contains(&true) {
		for leaf in &mut leaves {
			let (table, decode) = match leaf {
				Plan::IndexScan {
					table,
					decode,
					..
				}
				| Plan::IndexUnion {
					table,
					decode,
					..
				}
				| Plan::FullTextScan {
					table,
					decode,
					..
				}
				| Plan::Bitmap {
					table,
					decode,
					..
				} => (table.clone(), decode),
				_ => continue,
			};
			*decode = planner
				.scan_decode_fields(
					&table,
					row_fields,
					row_omit,
					row_cond.as_ref(),
					ordering,
					row_group,
				)
				.await;
			// The union a bitmap falls back to yields the same rows.
			if let Plan::Bitmap {
				decode,
				fallback: Some(fallback),
				..
			} = leaf && let Plan::IndexUnion {
				decode: union_decode,
				..
			} = &mut **fallback
			{
				union_decode.clone_from(decode);
			}
		}
	}

	// The number of stages above the search, once the predicates that only a
	// scan or the search answers are stripped and those left empty dropped.
	let mut above_the_search = ranked_at;
	let stages: Vec<RowStage> = if all_fts || strips_knn {
		let probes: Vec<_> = fts_probes
			.iter()
			.map(|(column, operator, query)| (column, operator, query.as_str()))
			.collect();
		let mut kept = Vec::with_capacity(stages.len());
		for (at, stage) in stages.into_iter().enumerate() {
			if ranked_at == Some(at) {
				above_the_search = Some(kept.len());
			}
			match stage {
				RowStage::Filter(predicate) => {
					let mut pred = Some(predicate);
					if all_fts {
						pred = pred.and_then(|p| {
							crate::exec::planner::util::strip_fts_probes(&p, &probes)
						});
					}
					if strips_knn {
						pred = pred
							.and_then(|p| crate::exec::planner::util::strip_knn_from_condition(&p));
					}
					kept.extend(pred.map(RowStage::Filter));
				}
				RowStage::Layer(..) | RowStage::Permission(..) => kept.push(stage),
			}
		}
		kept
	} else {
		stages
	};

	// A KNN scan gates each candidate of its search on the table's permission
	// and the conditions pushed into it; the conditions stay as filters above
	// the scan, which judge the rows the search returns again.
	// Resolve places a table's permission directly above its source, and a
	// permission that reads computed fields derives them itself, so no
	// permission stands above a layer or a predicate here.
	if has_knn && interleaved {
		return Err(NotCovered::internal("a KNN search under a permission above a layer"));
	}
	if has_knn {
		for leaf in &mut leaves {
			if let Plan::KnnScan {
				filter,
				..
			} = leaf
			{
				// A search with no condition still gates its candidates on the
				// table's permission, so the `k` records it returns are `k`
				// the caller may see.
				if filter.is_none() && !permissions.is_empty() {
					*filter = Some(KnnFilter {
						permissions: Vec::new(),
						residual: None,
						matches_condition: None,
						prefilter: None,
					});
				}
				if let Some(filter) = filter {
					filter.permissions = permissions.iter().rev().cloned().collect();
				}
			}
		}
	}
	if has_knn && leaves.len() > 1 {
		knn_union_sources(planner, &mut leaves, &tables).await?;
	}
	if enforced.contains(&true) {
		if has_knn {
			return Err(NotCovered::new("a KNN search over a source its scan enforces"));
		}
		leaves = enforce_leaves(leaves, &tables, &enforced);
	}

	// Predicates were collected outermost first; the innermost runs first.
	let mut root = match <[Plan; 1]>::try_from(leaves) {
		Ok([leaf]) => leaf,
		Err(leaves) => Plan::Union {
			inputs: leaves,
		},
	};
	for (predicate, auth, guard, computed) in permissions.into_iter().rev() {
		root = Plan::PermissionFilter {
			input: Box::new(root),
			predicate,
			auth,
			guard,
			hides_access_paths,
			computed,
		};
	}
	let mut brute = brute;
	let mut rank = |root: Plan| match brute.take() {
		Some((field, vector, k, distance)) => Plan::KnnTopK {
			input: Box::new(root),
			field,
			vector,
			k,
			distance,
		},
		None => root,
	};
	let above_the_search = above_the_search.unwrap_or(0);
	for (at, stage) in stages.into_iter().enumerate().rev() {
		if at + 1 == above_the_search {
			root = rank(root);
		}
		root = match stage {
			RowStage::Filter(predicate) => Plan::Filter {
				input: Box::new(root),
				predicate,
			},
			RowStage::Layer(fields, input_schema) => Plan::Layer {
				input: Box::new(root),
				fields,
				input_schema,
			},
			RowStage::Permission(predicate, auth, guard, computed) => Plan::PermissionFilter {
				input: Box::new(root),
				predicate,
				auth,
				guard,
				hides_access_paths,
				computed,
			},
		};
	}
	root = rank(root);
	let root = clauses_over(root, aggregate, sort, limit, projection);
	Ok(PhysicalPlan {
		catalog: None,
		root: match refused.or(omit_refusal) {
			Some(refusal) => Plan::Refused {
				refusal,
			},
			None => root,
		},
		dependencies,
		tail: Tail::default(),
		sources: Vec::new(),
		nested: Vec::new(),
		scope,
	})
}

/// The refusal the operator tree raises for a `KNN` operator its planning
/// cannot place: one of the retired `<|k|>` form, or one under an `OR` or a
/// `NOT`, where no scan or search consumes it.
fn knn_placement_refusal(stages: &[RowStage]) -> Option<super::plan::Refusal> {
	use crate::exec::planner::util::{has_knn_ktree_operator, has_knn_operator};
	for stage in stages {
		let RowStage::Filter(predicate) = stage else {
			continue;
		};
		if !has_knn_operator(predicate) {
			continue;
		}
		let Some(rest) = strip_knn_from_condition(predicate) else {
			continue;
		};
		if !has_knn_operator(&rest) {
			continue;
		}
		let message = if has_knn_ktree_operator(&rest) {
			"The `<|k|>` KNN operator (KTree / M-Tree) is no longer supported. \
			 Use `<|k, EF|>` against an HNSW index (e.g. \
			 `DEFINE INDEX … HNSW DIMENSION N`), or `<|k, DISTANCE|>` for a \
			 brute-force KNN with an explicit distance metric."
		} else {
			"KNN operators must appear at the top level of the WHERE clause \
			 (joined with AND); nesting `<|k, …|>` inside OR or NOT is not \
			 supported."
		};
		return Some(super::plan::Refusal::Query {
			message: message.to_string(),
		});
	}
	None
}

/// The brute-force search `params` name: the field it ranks, the query
/// vector, how many rows it keeps and the distance. A query vector the
/// statement's parameters do not hold is computed when the statement runs.
fn brute_force_search(
	planner: &Planner<'_>,
	params: crate::exec::planner::util::BruteForceKnnParams,
) -> (LogicalExpr, super::plan::KnnTarget, u32, crate::catalog::Distance) {
	use crate::exec::planner::util::BruteForceKnnVector;
	let vector = match params.vector {
		BruteForceKnnVector::Literal(vector) => super::plan::KnnTarget::Literal(vector),
		BruteForceKnnVector::Deferred(expr) => {
			let value = match &expr {
				LogicalExpr::Param(param) if !planner.param_is_shadowed(param.as_str()) => {
					planner.frozen().value(param.as_str()).cloned()
				}
				other => crate::exec::planner::static_value(other),
			};
			match value.and_then(|value| value.coerce_to::<Vec<crate::val::Number>>().ok()) {
				Some(vector) => super::plan::KnnTarget::Literal(vector),
				None => super::plan::KnnTarget::Deferred(expr),
			}
		}
	};
	(params.field, vector, params.k, params.distance)
}

/// The `MATCHES` clauses of `predicate` whose query is a parameter bound
/// around the statement ([`Planner::param_is_shadowed`]), which is read when
/// the clause runs.
fn runtime_queries(
	planner: &Planner<'_>,
	predicate: &LogicalExpr,
) -> Vec<(crate::idx::ft::MatchRef, super::plan::MatchedField)> {
	let mut found = Vec::new();
	let _ = predicate.visit(&mut |node| {
		if let LogicalExpr::Binary {
			left,
			op: BinaryOperator::Matches(matches),
			right,
		} = node && left.is_path()
			&& let LogicalExpr::Param(param) = right.as_ref()
			&& planner.param_is_shadowed(param.as_str())
		{
			found.push((
				matches.rf.unwrap_or(0),
				super::plan::MatchedField {
					field: left.as_ref().clone(),
					query: String::new(),
					query_param: Some(param.clone()),
				},
			));
		}
		std::ops::ControlFlow::<()>::Continue(())
	});
	found
}

/// The `MATCHES` clauses and `KNN` operator the predicates of a statement
/// declare, as the operator tree's planner collects them for the statement:
/// its table is the first table it reads whole, and an index answers a
/// `MATCHES` for the rows of the tables it reads whole, never for those a
/// record id names.
///
/// `folded` holds, per stage, the predicate with its row-independent operands
/// folded where the stage holds a `MATCHES`: both forms register their
/// clauses, so a query the statement writes as a record path or an index
/// into a literal is the clause its folded value names.
fn index_scope(
	planner: &Planner<'_>,
	stages: &[RowStage],
	folded: &[Option<LogicalExpr>],
	inputs: &[&LogicalOp],
) -> Mapped<IndexScope> {
	let mut known = Vec::new();
	let mut excluded = Vec::new();
	let mut open = false;
	for input in inputs {
		match input {
			LogicalOp::Source(LogicalSource {
				source: SourceRef::Table(table),
				..
			}) => known.push(table.clone()),
			LogicalOp::Source(LogicalSource {
				source: SourceRef::RecordId(rid),
				..
			}) => excluded.push(rid.table.clone()),
			// A source that names its table only when the statement runs.
			LogicalOp::Source(LogicalSource {
				source:
					SourceRef::Param(_)
					| SourceRef::Dynamic(
						LogicalExpr::Call(_)
						| LogicalExpr::Postfix {
							..
						}
						| LogicalExpr::Invoke {
							..
						},
					),
				..
			}) => open = true,
			_ => {}
		}
	}
	index_scope_over(planner, stages, folded, known, excluded, open)
}

/// [`index_scope`] over the tables a statement reads whole (`known`), those
/// it reads by record id (`excluded`), and whether a source names its table
/// only when the statement runs (`open`).
fn index_scope_over(
	planner: &Planner<'_>,
	stages: &[RowStage],
	folded: &[Option<LogicalExpr>],
	known: Vec<TableName>,
	excluded: Vec<TableName>,
	open: bool,
) -> Mapped<IndexScope> {
	let mut scope = IndexScope {
		select: true,
		..IndexScope::default()
	};
	// The stages are collected outermost first, and the tree reads the
	// conjunction in the order it is written.
	for (at, stage) in stages.iter().enumerate().rev() {
		let RowStage::Filter(predicate) = stage else {
			continue;
		};
		scope.knn |= crate::exec::planner::util::has_knn_operator(predicate);
		for predicate in std::iter::once(predicate).chain(folded[at].as_ref()) {
			crate::exec::physical_expr::collect_cond_matches(predicate, &mut scope.registered);
			let found = crate::exec::planner::util::extract_matches_context(
				predicate,
				Some(planner.frozen()),
				|name| planner.param_is_shadowed(name),
			);
			for (rf, info) in found.entries() {
				let clause = super::plan::MatchedField {
					field: info.field.clone(),
					query: info.query.clone(),
					query_param: None,
				};
				match scope.matches.iter_mut().find(|(held, _)| *held == rf) {
					Some(held) => held.1 = clause,
					None => scope.matches.push((rf, clause)),
				}
			}
			// A query read from a name bound around the statement is known
			// only when the clause runs: it is read then.
			for (rf, clause) in runtime_queries(planner, predicate) {
				if !scope.matches.iter().any(|(held, _)| *held == rf) {
					scope.matches.push((rf, clause));
				}
			}
		}
	}
	if !scope.matches.is_empty() {
		scope.table = known.first().cloned();
	}
	scope.executor_tables = crate::exec::physical_expr::ExecutorTables::new(known, excluded, open);
	// A `MATCHES` the registration walk does not reach binds to no index; when a
	// table is read whole the statement is refused before it reads a row, so
	// the outcome does not depend on which rows the condition reaches.
	if scope.executor_tables.any_known() {
		for stage in stages {
			if let RowStage::Filter(predicate) = stage
				&& let Some(found) = crate::exec::physical_expr::first_unregistered_cond_matches(
					predicate,
					&scope.registered,
				) {
				return Err(NotCovered::refused(super::plan::Refusal::NoIndexForMatch {
					exp: surrealdb_types::ToSql::to_sql(&found),
				}));
			}
		}
	}
	Ok(scope)
}

struct SortClause {
	keys: Vec<SortKeySpec>,
	random: bool,
}

/// A node between a statement's source and its clauses: a predicate, or a
/// table's layer.
enum RowStage {
	Filter(LogicalExpr),
	Layer(LogicalFields, crate::expr::logical::Schema),
	/// A table's permission. Resolve places it directly above its source, so
	/// the stages beneath it are only other permissions; the last is the
	/// table's computed fields the permission reads, which it derives itself.
	Permission(
		LogicalExpr,
		Option<surrealdb_iam::AuthLimit>,
		GuardKind,
		Option<super::plan::ComputedReads>,
	),
}

/// The computed fields of the table beneath a permission over `input` that
/// `predicate` reads ([`crate::exec::planner::fold::computed_reads`]), or
/// `None` when it reads none, or the rows come from no table whose resolved
/// schema names its computed fields.
fn computed_reads(
	input: &LogicalOp,
	predicate: &LogicalExpr,
	planner: &Planner<'_>,
) -> Option<super::plan::ComputedReads> {
	let (table, schema) = crate::exec::planner::fold::permission_subject(input)?;
	let reads = crate::exec::planner::fold::computed_reads(
		predicate,
		&schema,
		planner.function_registry(),
	)?;
	let fields = reads.map(|reads| {
		let mut fields: Vec<String> = reads.into_iter().collect();
		fields.sort_unstable();
		fields
	});
	Some(super::plan::ComputedReads {
		table,
		fields,
	})
}

/// `rows` with the clauses a statement applies above its filters: its
/// grouping, then its ordering, its paging and its projection.
fn clauses_over(
	rows: Plan,
	aggregate: Option<(Vec<LogicalExpr>, LogicalFields)>,
	sort: Option<SortClause>,
	limit: Option<(Option<LogicalExpr>, Option<LogicalExpr>)>,
	projection: Projection,
) -> Plan {
	let mut root = rows;
	if let Some((group, fields)) = aggregate {
		root = Plan::Aggregate {
			input: Box::new(root),
			group,
			fields,
			first_value: Vec::new(),
		};
	}
	if let Some(clause) = sort {
		root = Plan::Sort {
			input: Box::new(root),
			keys: clause.keys,
			keep: None,
			pushdown: false,
			random: clause.random,
		};
	}
	if let Some((limit, start)) = limit {
		root = Plan::Limit {
			input: Box::new(root),
			limit,
			start,
		};
	}
	Plan::Project {
		input: Box::new(root),
		projection,
	}
}

/// Whether `plan` reads rows put through a table's enforcement
/// ([`Plan::Enforced`]).
pub(super) fn reads_enforced(plan: &Plan) -> bool {
	holds_on_chain(plan, &|node| matches!(node, Plan::Enforced { .. }))
}

/// Whether `hit` holds of a node of `plan`'s chain of inputs, the plans of
/// its subqueries and the inputs of its unions included.
fn holds_on_chain(plan: &Plan, hit: &dyn Fn(&Plan) -> bool) -> bool {
	if hit(plan) {
		return true;
	}
	match plan {
		Plan::Enforced {
			input,
			..
		}
		| Plan::Filter {
			input,
			..
		}
		| Plan::PermissionFilter {
			input,
			..
		}
		| Plan::Layer {
			input,
			..
		}
		| Plan::Split {
			input,
			..
		}
		| Plan::Fetch {
			input,
			..
		}
		| Plan::KnnTopK {
			input,
			..
		}
		| Plan::Sort {
			input,
			..
		}
		| Plan::Limit {
			input,
			..
		}
		| Plan::Aggregate {
			input,
			..
		}
		| Plan::Distinct {
			input,
		}
		| Plan::Project {
			input,
			..
		} => holds_on_chain(input, hit),
		Plan::Join {
			left,
			right,
			..
		} => holds_on_chain(left, hit) || holds_on_chain(right, hit),
		Plan::Subquery {
			plan,
		} => holds_on_chain(&plan.root, hit),
		Plan::Union {
			inputs,
		} => inputs.iter().any(|input| holds_on_chain(input, hit)),
		_ => false,
	}
}

/// The leaf for a node of a front end's plan that no clause of a `SELECT`
/// names, as a source of the pipeline above it: an inner or left join, a
/// `DISTINCT`, a grouped read the rows above it filter (`HAVING`), or a
/// statement of its own as an input of a union, with what its reads depend on.
///
/// Each side of a join and the input of a `DISTINCT` are read as the
/// statements they are, and the rows above the leaf are those it yields.
async fn map_barrier(
	planner: &Planner<'_>,
	op: &LogicalOp,
	versioned: bool,
) -> Mapped<(Plan, Vec<Dependency>)> {
	use crate::expr::logical::JoinKind;
	match op {
		LogicalOp::Join(join) => {
			let kind = match join.kind {
				JoinKind::Inner => super::plan::JoinMatch::Inner,
				JoinKind::Left => super::plan::JoinMatch::Left,
				// A semi or anti join is mapped as the membership predicate it
				// was lifted from ([`memberships_of`]), never as a source.
				JoinKind::Semi | JoinKind::Anti => {
					return Err(NotCovered::new("a semi or anti join in FROM"));
				}
			};
			if join.frame.mode != EvalMode::Caller {
				return Err(NotCovered::internal(
					"a join judged in a frame that is not the caller's",
				));
			}
			let (left, mut dependencies) = rows_of(planner, join.left(), versioned).await?;
			let (right, inner) = rows_of(planner, join.right(), versioned).await?;
			dependencies.extend(inner);
			let plan = Plan::Join {
				left: Box::new(left),
				right: Box::new(right),
				kind,
				keys: join.on.clone(),
				residual: join.filter.clone(),
				alias: join.alias.name().to_string(),
			};
			Ok((plan, dependencies))
		}
		LogicalOp::Distinct(distinct) => {
			let (rows, dependencies) = rows_of(planner, &distinct.input, versioned).await?;
			Ok((
				Plan::Distinct {
					input: Box::new(rows),
				},
				dependencies,
			))
		}
		LogicalOp::Aggregate(_) => rows_of(planner, op, versioned).await,
		// A statement of its own among the inputs of a union.
		LogicalOp::Project(_)
		| LogicalOp::Sort(_)
		| LogicalOp::Limit(_)
		| LogicalOp::Filter(_)
		| LogicalOp::PermissionFilter(_)
		| LogicalOp::Split(_)
		| LogicalOp::UnionAll(_)
			if starts_a_pipeline(op) =>
		{
			let inner =
				crate::exec::vm::host::out_of_line(|| map_rows(planner, op, versioned)).await?;
			if let Some(refusal) = inner.refusal() {
				return Err(NotCovered::refused(refusal.clone()));
			}
			let dependencies = inner.dependencies.clone();
			Ok((
				Plan::Subquery {
					plan: Box::new(super::optimize::optimize(inner)),
				},
				dependencies,
			))
		}
		other => Err(NotCovered::internal(format!("the source is not covered: {other}"))),
	}
}

/// The rows of `op`, a read mapped as a statement of its own, beneath the
/// projection of all it holds, with what its reads depend on.
async fn rows_of(
	planner: &Planner<'_>,
	op: &LogicalOp,
	versioned: bool,
) -> Mapped<(Plan, Vec<Dependency>)> {
	let inner = crate::exec::vm::host::out_of_line(|| map_rows(planner, op, versioned)).await?;
	if let Some(refusal) = inner.refusal() {
		return Err(NotCovered::refused(refusal.clone()));
	}
	let rows = match inner.root {
		Plan::Project {
			input,
			projection: Projection::All,
		} => *input,
		other => other,
	};
	Ok((rows, inner.dependencies))
}

/// Whether `op` is a table or record-id source under a stack of its own:
/// predicates, a permission and layers, in any order.
fn holds_a_stack(op: &LogicalOp) -> bool {
	let mut node = op;
	let mut depth = 0usize;
	loop {
		node = match node {
			LogicalOp::Filter(filter) => &filter.input,
			LogicalOp::PermissionFilter(permission) => &permission.input,
			LogicalOp::Project(layer) if node.is_layer() => &layer.input,
			other => return depth > 0 && is_table_source(other),
		};
		depth += 1;
	}
}

/// `leaves`, the sources of a statement's `FROM`, with every leaf `enforced`
/// marks put through its table's enforcement. Where several sources are read
/// the leaves are statements of their own, as a union reads storage alone.
fn enforce_leaves(leaves: Vec<Plan>, tables: &[Option<TableName>], enforced: &[bool]) -> Vec<Plan> {
	let several = leaves.len() > 1;
	leaves
		.into_iter()
		.zip(tables)
		.zip(enforced)
		.map(|((leaf, table), enforces)| {
			let leaf = match (enforces, table) {
				(true, Some(table)) => Plan::Enforced {
					input: Box::new(leaf),
					table: table.clone(),
				},
				_ => leaf,
			};
			match leaf {
				Plan::Subquery {
					..
				}
				| Plan::Values {
					..
				} => leaf,
				rows if several => Plan::Subquery {
					plan: Box::new(super::optimize::optimize(PhysicalPlan {
						catalog: None,
						root: Plan::Project {
							input: Box::new(rows),
							projection: Projection::All,
						},
						dependencies: Vec::new(),
						tail: Tail::default(),
						sources: Vec::new(),
						nested: Vec::new(),
						scope: IndexScope::default(),
					})),
				},
				rows => rows,
			}
		})
		.collect()
}

/// Whether `op` is a source naming a table or a record id, read by a scan.
fn is_table_source(op: &LogicalOp) -> bool {
	matches!(
		op,
		LogicalOp::Source(LogicalSource {
			source: SourceRef::Table(_) | SourceRef::RecordId(_),
			..
		})
	)
}

/// The leaf for a source that is not a table: a value, a parameter, a mock or
/// an expression in `FROM`, read as the operator tree reads it
/// ([`ValuesRead`]), or a statement in `FROM`, mapped as a statement of its
/// own. `None` for any other node.
async fn map_value_source(planner: &Planner<'_>, op: &LogicalOp) -> Mapped<Option<Plan>> {
	let (expr, read) = match op {
		LogicalOp::Values(values) => {
			// A value is not a record: `FOR UPDATE` over it is the tree's
			// error to raise.
			if values.for_update {
				return Err(NotCovered::internal("FOR UPDATE over a value"));
			}
			(
				values.expr.clone(),
				ValuesRead::Expression {
					written: values.written,
				},
			)
		}
		LogicalOp::Subquery(subquery) => {
			let plan = super::optimize::optimize(
				crate::exec::vm::host::out_of_line(|| map(planner, &subquery.input)).await?,
			);
			return Ok(Some(Plan::Subquery {
				plan: Box::new(plan),
			}));
		}
		LogicalOp::Source(source) => {
			// `WITH` names the indexes a table scan may use and a schema
			// describes a table resolved at bind time. A value source reads
			// no index, and a table it names at run time is read in full,
			// so neither changes what it yields.
			// A locked read of a value is of the record id it resolves to when
			// the statement runs; any other value is refused then.
			if source.for_update {
				let expr = match &source.source {
					SourceRef::Param(param) => LogicalExpr::Param(param.clone()),
					SourceRef::Dynamic(expr) => expr.clone(),
					SourceRef::Mock(_) | SourceRef::Table(_) | SourceRef::RecordId(_) => {
						return Err(NotCovered::internal("FOR UPDATE over a value source"));
					}
				};
				return Ok(Some(Plan::Values {
					expr,
					read: ValuesRead::Locked,
				}));
			}
			match &source.source {
				SourceRef::Mock(mock) => (
					LogicalExpr::Mock(mock.clone()),
					ValuesRead::Expression {
						written: false,
					},
				),
				SourceRef::Param(param) => (LogicalExpr::Param(param.clone()), ValuesRead::Dynamic),
				SourceRef::Dynamic(
					expr @ (LogicalExpr::Call(_)
					| LogicalExpr::Postfix {
						..
					}
					| LogicalExpr::Invoke {
						..
					}),
				) => (expr.clone(), ValuesRead::Dynamic),
				SourceRef::Dynamic(expr) => (
					expr.clone(),
					ValuesRead::Expression {
						written: false,
					},
				),
				SourceRef::Table(_) | SourceRef::RecordId(_) => return Ok(None),
			}
		}
		_ => return Ok(None),
	};
	Ok(Some(Plan::Values {
		expr,
		read,
	}))
}

/// Map one source to the scan that reads it, with what the scan depends on
/// and whether the scan enforces the table's permission, computed fields and
/// field permissions on the rows it reads.
async fn map_source(
	planner: &Planner<'_>,
	source: &LogicalSource,
	offer: &AccessOffer,
	versioned: bool,
) -> Mapped<(Plan, Dependency, bool)> {
	// A record-id source is read by its key, whatever indexes the table has.
	let (table, record) = match &source.source {
		SourceRef::Table(table) => (table.clone(), None),
		SourceRef::RecordId(record) => {
			(record.table.clone(), Some(LogicalExpr::RecordId(record.clone())))
		}
		_ => return Err(NotCovered::internal("the source is not a table or a record id")),
	};
	// A source the bind passes resolved carries its table permission,
	// computed fields and field guards as the nodes above it: the scan owes
	// nothing more. An unresolved source is enforced by its scan, per row.
	// A late-bound source is enforced whatever the catalog holds now: the
	// table it reads is the one the run finds, which may be defined, or
	// declare permissions, by then.
	let enforces = EnforcementMode::for_source(source.schema.as_ref()) != EnforcementMode::Planned
		&& match (planner.txn(), crate::exec::vm::host::out_of_line(|| planner.ns_db_ids()).await) {
			_ if planner.is_late_bound() => true,
			(Some(txn), Some((ns, db))) => {
				txn.get_tb(ns, db, &table, None).await.ok().flatten().is_some()
			}
			// A namespace or database that is not there reads no table: the
			// run raises the error it owes.
			(Some(_), None) => false,
			(None, _) => true,
		};
	// The locked-read rules are checked for the statement before its sources
	// are mapped ([`locked_read_refusal`]); a table named whole under
	// `FOR UPDATE` is refused there, so only a record id is locked here.
	if source.for_update && record.is_none() {
		return Err(NotCovered::refused(super::plan::Refusal::Query {
			message: crate::dbs::FOR_UPDATE_TARGETS_ERROR.to_string(),
		}));
	}
	// A table of a namespace or database that is not there has no catalog
	// version: the run raises the error it owes before it reads anything.
	let dependency = match crate::exec::vm::host::out_of_line(|| planner.ns_db_ids()).await {
		None if planner.txn().is_some() => Dependency::Table {
			table: table.clone(),
			version: None,
		},
		_ => table_dependency(planner, &table).await?,
	};
	let schema = source.schema.clone();
	let leaf = match record {
		Some(record) => Plan::RecordScan {
			table,
			record,
			schema,
			for_update: source.for_update,
		},
		None => table_access(planner, table, source, offer, versioned).await?,
	};
	Ok((leaf, dependency, enforces))
}

/// The access path for a table source: a lookup of the record the predicate
/// pins, or the path the planner's decision chooses from the offer.
async fn table_access(
	planner: &Planner<'_>,
	table: TableName,
	source: &LogicalSource,
	offer: &AccessOffer,
	versioned: bool,
) -> Mapped<Plan> {
	let schema = source.schema.clone();
	if let Some(record) = planner.record_point_lookup(&table, offer.cond.as_ref()) {
		return Ok(Plan::RecordScan {
			table,
			record,
			schema,
			for_update: false,
		});
	}
	// A versioned read chooses from the indexes the table declared at its
	// stamp ([`Planner::decide_access_path`] reads them there), so a stamp is
	// needed to choose at all. A stamp only the run can evaluate names no
	// catalog to choose from: the table is scanned, which reads at the stamp
	// whatever the indexes were, and a `WITH` hint, which chooses only how the
	// rows are read, is read as the scan. A `MATCHES` or a KNN operator is
	// answered from the index the table declares now, whatever the stamp, so
	// it is chosen as without one.
	let index_operator = offer.cond.as_ref().is_some_and(|cond| {
		expr_contains_matches(cond) || crate::exec::planner::util::has_knn_operator(cond)
	});
	if versioned && !index_operator && !matches!(planner.plan_time_version, PlanTimeVersion::At(_))
	{
		return Ok(Plan::TableScan {
			table,
			direction: crate::kvs::Direction::Forward,
			schema,
			topk: None,
		});
	}
	let Some(decision) = planner
		.decide_access_path(
			&table,
			offer.cond.as_ref(),
			offer.order.as_ref(),
			source.with.as_ref(),
			offer.has_query_limit,
			versioned,
		)
		.await
	else {
		// A namespace or database that is not there has no path to choose:
		// the scan raises the error the read owes before it reads anything.
		if planner.txn().is_some()
			&& crate::exec::vm::host::out_of_line(|| planner.ns_db_ids()).await.is_none()
		{
			return Ok(Plan::TableScan {
				table,
				direction: crate::kvs::Direction::Forward,
				schema,
				topk: None,
			});
		}
		return Err(NotCovered::internal("no plan-time access path"));
	};
	// A union is planned with the ordering the choice was made with, as the
	// operator tree plans it.
	let union_order = if decision.order_withheld {
		None
	} else {
		offer.order.as_ref()
	};
	match decision.path {
		AccessPath::TableScan => Ok(Plan::TableScan {
			table,
			direction: decision.direction,
			schema,
			topk: None,
		}),
		AccessPath::BTreeScan {
			index_ref,
			access,
			direction,
		} => {
			// A scan whose key range leaves a fanning column open reaches a
			// record through several entries, and the host collapses the
			// repeats below everything that counts rows. A record then sits at
			// one key position per element rather than at one place in the
			// order, so the scan delivers no ordering.
			let fans_out =
				crate::exec::index::access_path::access_fans_out(&index_ref.cols, &access);
			// An ordering withheld from the choice is not one the index was
			// chosen for, so it is not covered. A covered ordering names the
			// record's fields, which the sort above reads only when no
			// projection renames another value to the same name.
			let claims_order = !decision.order_withheld && !fans_out;
			let ordered_by = match &offer.order {
				Some(order @ LogicalOrdering::Order(keys))
					if claims_order
						&& index_covers_ordering(&index_ref, &access, direction, order) =>
				{
					keys.iter().map(|key| field_path_from_expr(&key.value).ok()).collect()
				}
				_ => None,
			};
			Ok(Plan::IndexScan {
				table,
				index: index_ref,
				access,
				direction,
				ordered_by,
				schema,
				decode: None,
			})
		}
		// A predicate the planner proved admits no row reads nothing.
		AccessPath::EmptyScan => Ok(Plan::Union {
			inputs: Vec::new(),
		}),
		AccessPath::Union {
			paths,
			dedupe,
		} => index_union(table, paths, dedupe, union_order, schema),
		// An OR fusion carries the union it replaced, planned as the operator
		// tree plans it, for when a branch overflows.
		AccessPath::BitmapFusion {
			root,
			fallback,
		} => {
			let fallback = match fallback.map(|fallback| *fallback) {
				Some(AccessPath::Union {
					paths,
					dedupe,
				}) => Some(Box::new(index_union(
					table.clone(),
					paths,
					dedupe,
					union_order,
					schema.clone(),
				)?)),
				Some(_) => {
					return Err(NotCovered::internal("a bitmap fallback that is not a union"));
				}
				None => None,
			};
			Ok(Plan::Bitmap {
				table,
				root,
				fallback,
				schema,
				decode: None,
			})
		}
		AccessPath::FullTextSearch {
			index_ref,
			query,
			operator,
		} => Ok(Plan::FullTextScan {
			table,
			index: index_ref,
			query,
			operator,
			schema,
			decode: None,
		}),
		AccessPath::KnnSearch {
			index_ref,
			vector,
			k,
			ef,
			prefilter,
		} => {
			// The search gates each candidate on the table's permission and the
			// statement's other conditions, so a scan of the `k` nearest then
			// filtered would hold other rows. The permission joins the filter
			// where the statement's nodes above the scan are known.
			let stripped = offer.cond.as_ref().and_then(strip_knn_from_condition);
			let residual = offer.cond.as_ref().and_then(strip_knn_and_matches_from_condition);
			let needs_matches = !index_ref.definition().uses_shared_doc_ids()
				|| stripped.as_ref().is_some_and(expr_contains_matches);
			let matches_condition = if needs_matches {
				stripped
			} else {
				None
			};
			let prefilter = prefilter.map(|p| KnnPrefilter {
				root: p.root,
				residual: p.residual,
				uncovered_matches: p.uncovered_matches,
			});
			let filter = (residual.is_some() || matches_condition.is_some() || prefilter.is_some())
				.then_some(KnnFilter {
					permissions: Vec::new(),
					residual,
					matches_condition,
					prefilter,
				});
			Ok(Plan::KnnScan {
				table,
				index: index_ref,
				vector,
				k,
				ef,
				filter,
				ids_only: false,
				schema,
			})
		}
	}
}

/// An index union over B-tree scans `paths`, merged as the operator tree
/// merges it for `order`.
fn index_union(
	table: TableName,
	paths: Vec<AccessPath>,
	dedupe: bool,
	order: Option<&LogicalOrdering>,
	schema: Option<std::sync::Arc<crate::expr::logical::Schema>>,
) -> Mapped<Plan> {
	let (paths, merge) = decide_union_merge(paths, dedupe, order);
	let branches = paths
		.into_iter()
		.map(|path| match path {
			AccessPath::BTreeScan {
				index_ref,
				access,
				direction,
			} => Ok(IndexBranch {
				index: index_ref,
				read: BranchRead::BTree(super::plain::ValueData::new(
					access,
					"an index union's access",
				)?),
				direction,
			}),
			AccessPath::FullTextSearch {
				index_ref,
				query,
				operator,
			} => Ok(IndexBranch {
				index: index_ref,
				read: BranchRead::FullText {
					query,
					operator,
				},
				direction: crate::kvs::Direction::Forward,
			}),
			_ => Err(NotCovered::new(
				"a union branch that is neither a B-tree scan nor a full-text query",
			)),
		})
		.collect::<Mapped<Vec<_>>>()?;
	// A merge delivers its key's order in the direction the ordering asked
	// for.
	let ordered_by = match &merge {
		Some(MergeMode::ById(_)) => Some(vec![FieldPath::field("id")]),
		Some(
			MergeMode::ByIndexKey {
				path,
				..
			}
			| MergeMode::ByIndexKeyDedup {
				path,
				..
			},
		) => Some(vec![path.clone()]),
		None => None,
	};
	Ok(Plan::IndexUnion {
		table,
		branches,
		merge,
		ordered_by,
		schema,
		decode: None,
	})
}

/// Read what a scan of `table` depends on, declining a table the VM cannot
/// scan the way the operator tree would.
async fn table_dependency(planner: &Planner<'_>, table: &TableName) -> Mapped<Dependency> {
	let Some(txn) = planner.txn() else {
		return Err(NotCovered::internal("no transaction at plan time"));
	};
	let Some((ns, db)) = crate::exec::vm::host::out_of_line(|| planner.ns_db_ids()).await else {
		return Err(NotCovered::unresolved_database(planner));
	};
	let unread = |_| NotCovered::internal("the table catalog could not be read");
	let version = txn.catalog_version(ns, db, table).await.map_err(unread)?;
	Ok(Dependency::Table {
		table: table.clone(),
		version,
	})
}

pub(super) fn projection(fields: &LogicalFields, omit: Vec<Omission>) -> Mapped<Projection> {
	// `OMIT` removes its paths from the output, after the projected fields are
	// written.
	match fields {
		LogicalFields::Value {
			expr,
			..
		} => Ok(if omit.is_empty() {
			Projection::Value(expr.clone())
		} else {
			Projection::OmittedValue {
				expr: expr.clone(),
				omit,
			}
		}),
		LogicalFields::Select(list) => {
			if let [LogicalField::All] = list.as_slice()
				&& omit.is_empty()
			{
				return Ok(Projection::All);
			}
			let star = list.iter().any(|field| matches!(field, LogicalField::All));
			let mut out = Vec::with_capacity(list.len());
			for field in list {
				let LogicalField::Single {
					expr,
					alias,
					kind,
					frame,
				} = field
				else {
					continue;
				};
				// A column the bind passes added — a computed field, a field
				// guard — runs in a frame other than the caller's.
				if *frame != EvalFrame::caller() || kind.is_some() {
					return Err(NotCovered::internal(
						"a projected column outside the caller's frame",
					));
				}
				out.push(ProjectedField {
					output: output_path(expr, alias.as_ref()),
					expr: expr.clone(),
					aliased: alias.is_some(),
					sort_key: None,
				});
			}
			Ok(if star {
				Projection::Star {
					fields: out,
					omit,
				}
			} else {
				Projection::Fields {
					fields: out,
					omit,
				}
			})
		}
	}
}

/// The paths `omit` removes, in the order they apply.
///
/// An `OMIT` names its paths as written, in a parameter holding a path as
/// text, or in a projection function whose arguments name paths
/// ([`Planner::resolve_field_paths`]). A path of fields and literal indices is
/// removed from an object; any other is walked to the locations it names.
///
/// An expression that names no path is refused as the operator tree refuses
/// it, before any row is read.
pub(super) async fn omissions(
	planner: &Planner<'_>,
	omit: &[LogicalExpr],
) -> Mapped<Result<Vec<Omission>, super::plan::Refusal>> {
	let mut out = Vec::with_capacity(omit.len());
	for expr in omit {
		let paths =
			match crate::exec::vm::host::out_of_line(|| planner.resolve_field_paths(expr.clone()))
				.await
			{
				Ok(paths) => paths,
				Err(error) => {
					return match super::plan::Refusal::of(&error) {
						Some(refusal) => Ok(Err(refusal)),
						None => Err(NotCovered::internal(
							"an OMIT path the operator tree cannot resolve",
						)),
					};
				}
			};
		for path in paths {
			let mut plain = Vec::new();
			if omit_paths(&path, &mut plain).is_some() {
				out.extend(plain.into_iter().map(Omission::Path));
			} else {
				out.push(Omission::Walk {
					walk: walk_to(&path),
					source: path,
				});
			}
		}
	}
	Ok(Ok(out))
}

/// The non-negative literal integer an index step holds, if any. Anything else
/// names its index only once evaluated.
fn literal_index(index: &LogicalExpr) -> Option<usize> {
	match index {
		LogicalExpr::Literal(LogicalLiteral::Integer(idx)) => usize::try_from(*idx).ok(),
		_ => None,
	}
}

/// The walk `path` describes: the outermost node names what is removed, and
/// everything under it is the walk to where.
fn walk_to(path: &LogicalExpr) -> Walk<LogicalExpr> {
	let (base, action) = match path {
		LogicalExpr::Field(name) => (None, WalkAction::RemoveField(name.as_str().to_owned())),
		LogicalExpr::Access {
			base,
			field,
		} => (Some(&**base), WalkAction::RemoveField(field.as_str().to_owned())),
		LogicalExpr::Each(base) => (Some(&**base), WalkAction::Clear),
		LogicalExpr::First(base) => (Some(&**base), WalkAction::RemoveFirst),
		LogicalExpr::Last(base) => (Some(&**base), WalkAction::RemoveLast),
		LogicalExpr::Index {
			base,
			index,
		} => (
			Some(&**base),
			match literal_index(index) {
				Some(index) => WalkAction::RemoveIndex(index),
				None => WalkAction::RemoveIndexAt((**index).clone()),
			},
		),
		LogicalExpr::Filter {
			base,
			predicate,
		} => (Some(&**base), WalkAction::RemoveMatching((**predicate).clone())),
		LogicalExpr::Destructure {
			base,
			fields,
		} => (Some(&**base), WalkAction::Destructure(fields.clone())),
		// A broadcast applies its body to everything its base reaches, which is
		// the base's walk followed by the body's own.
		LogicalExpr::Broadcast {
			base,
			body,
			..
		} => {
			let mut walk = walk_to(body);
			let mut steps = walk_steps(base);
			steps.append(&mut walk.steps);
			return Walk {
				steps,
				action: walk.action,
			};
		}
		// Answers with a value rather than naming a place.
		_ => (None, WalkAction::Nothing),
	};
	Walk {
		steps: base.map(walk_steps).unwrap_or_default(),
		action,
	}
}

/// The steps that reach the locations `path` names. A step that reaches
/// nothing makes the whole walk reach nothing.
fn walk_steps(path: &LogicalExpr) -> Vec<WalkStep<LogicalExpr>> {
	let (base, step) = match path {
		LogicalExpr::Current => return Vec::new(),
		LogicalExpr::Field(name) => (None, WalkStep::Field(name.as_str().to_owned())),
		LogicalExpr::Access {
			base,
			field,
		} => (Some(&**base), WalkStep::Field(field.as_str().to_owned())),
		LogicalExpr::Each(base) => (Some(&**base), WalkStep::Each),
		LogicalExpr::First(base) => (Some(&**base), WalkStep::First),
		LogicalExpr::Last(base) => (Some(&**base), WalkStep::Last),
		LogicalExpr::Index {
			base,
			index,
		} => (
			Some(&**base),
			match literal_index(index) {
				Some(index) => WalkStep::Index(index),
				None => WalkStep::IndexAt((**index).clone()),
			},
		),
		LogicalExpr::Filter {
			base,
			predicate,
		} => (Some(&**base), WalkStep::Matching((**predicate).clone())),
		LogicalExpr::Destructure {
			base,
			fields,
		} => (Some(&**base), WalkStep::Destructure(fields.clone())),
		LogicalExpr::Broadcast {
			base,
			body,
			..
		} => {
			let mut steps = walk_steps(base);
			steps.append(&mut walk_steps(body));
			return steps;
		}
		// Reaches nothing, so neither does anything built on it.
		_ => return vec![WalkStep::Nowhere],
	};
	let mut steps = base.map(walk_steps).unwrap_or_default();
	steps.push(step);
	steps
}

/// One step of an `OMIT` path: a part of a plain path, or a destructure that
/// stands for each field it lists.
#[derive(Clone)]
enum OmitStep<'a> {
	Part(FieldPathPart),
	Destructure(&'a [LogicalDestructureField]),
}

/// Appends to `out` the plain paths `expr` removes, each removed in turn.
///
/// A destructure lists locations rather than naming one, so a path through
/// it is the path through each field it lists: `a.{ b, c.{ d } }.x` removes
/// `a.b.x` and `a.c.d.x`. Every other step is a field, a literal index,
/// `[0]` or `[$]`. A path with any other step — a filter, a method call, a
/// lookup, `.*`, an optional chain, a computed index, a start that is not a
/// path — or a destructure entry that empties a field (`{ c.* }`), names an
/// expression (`{ b: c }`) or is a rest entry is not a plain path (`None`),
/// and is walked instead ([`walk_to`]).
fn omit_paths(expr: &LogicalExpr, out: &mut Vec<FieldPath>) -> Option<()> {
	if !expr.is_path() {
		return None;
	}
	let steps = path_steps(expr)
		.into_iter()
		.map(|step| {
			Some(OmitStep::Part(match step {
				LogicalExpr::Destructure {
					fields,
					..
				} => return Some(OmitStep::Destructure(fields)),
				LogicalExpr::Field(name)
				| LogicalExpr::Access {
					field: name,
					..
				} => FieldPathPart::Field(name.as_str().to_owned()),
				LogicalExpr::First(_) => FieldPathPart::First,
				LogicalExpr::Last(_) => FieldPathPart::Last,
				LogicalExpr::Index {
					index,
					..
				} => match **index {
					LogicalExpr::Literal(LogicalLiteral::Integer(i)) if i >= 0 => {
						FieldPathPart::Index(i as usize)
					}
					_ => return None,
				},
				_ => return None,
			}))
		})
		.collect::<Option<Vec<_>>>()?;
	expand_omit(&mut Vec::with_capacity(steps.len()), &steps, out)
}

/// Appends to `out` each plain path `steps` expands to beneath `prefix`;
/// `None` where a destructure entry is no plain path.
fn expand_omit(
	prefix: &mut Vec<FieldPathPart>,
	steps: &[OmitStep<'_>],
	out: &mut Vec<FieldPath>,
) -> Option<()> {
	let Some((step, rest)) = steps.split_first() else {
		out.push(FieldPath(prefix.clone()));
		return Some(());
	};
	match step {
		OmitStep::Part(part) => {
			prefix.push(part.clone());
			expand_omit(prefix, rest, out)?;
			prefix.pop();
		}
		OmitStep::Destructure(fields) => {
			for field in *fields {
				match field {
					LogicalDestructureField::Field(name) => {
						prefix.push(FieldPathPart::Field(name.as_str().to_owned()));
						expand_omit(prefix, rest, out)?;
					}
					LogicalDestructureField::Destructure(name, nested) => {
						prefix.push(FieldPathPart::Field(name.as_str().to_owned()));
						let mut steps = Vec::with_capacity(rest.len() + 1);
						steps.push(OmitStep::Destructure(nested));
						steps.extend_from_slice(rest);
						expand_omit(prefix, &steps, out)?;
					}
					LogicalDestructureField::All(_)
					| LogicalDestructureField::Aliased(..)
					| LogicalDestructureField::Rest => return None,
				}
				prefix.pop();
			}
		}
	}
	Some(())
}

/// Where a projected field lands, as the operator tree places it: at its
/// alias; at the position a path reads; otherwise under its derived name as
/// one flat key.
fn output_path(expr: &LogicalExpr, alias: Option<&crate::expr::logical::OutputPath>) -> FieldPath {
	match alias {
		Some(alias) => output_field_path(alias),
		None if expr.is_path() => output_field_path(&expr.output_path()),
		None => FieldPath::field(expr.output_path().name()),
	}
}

/// The keys of an `ORDER BY`, each read from the row the sort is handed — the
/// source row, before any projection — as the operator tree reads them.
///
/// A key is resolved against the projection first (`resolve_order_by_alias`):
/// a key naming a projected field or alias sorts by that field's expression.
/// A bare field read, or an unresolved key that is a path, is read as a path;
/// any other key is computed, once per row, for the sort.
///
/// With `shared`, the statement's fields and the projection they map to, a
/// computed key naming a field's alias is that field's value: the field is
/// marked to read the key rather than evaluate its expression again, so the
/// row's one evaluation is both what it sorts by and what it returns, as the
/// operator tree's `Compute` hands its value to the projection. Otherwise a
/// computed key is evaluated again by the projection, which is taken only
/// when the key has no effect, so that one evaluation or two answer alike.
fn sort_keys(
	ordering: &LogicalOrdering,
	shared: Option<(&LogicalFields, &mut Projection)>,
	registry: &crate::exec::function::FunctionRegistry,
) -> Mapped<Vec<SortKeySpec>> {
	let LogicalOrdering::Order(orders) = ordering else {
		return Err(NotCovered::internal("ORDER BY RAND()"));
	};
	// The comparison a collated key needs is not in a build without `collate`.
	if !cfg!(feature = "collate") && orders.iter().any(|order| order.collate) {
		return Err(NotCovered::refused(super::plan::Refusal::Query {
			message: "ORDER BY … COLLATE is not available: this build was compiled without the \
			          `collate` feature"
				.to_owned(),
		}));
	}
	let (projected, mut projection) = match shared {
		Some((fields, projection)) => (Some(fields), Some(projection)),
		None => (None, None),
	};
	let mut keys = Vec::with_capacity(orders.len());
	for (index, order) in orders.iter().enumerate() {
		let resolved = projected.and_then(|fields| resolve_order_by_alias(&order.value, fields));
		let read = match &resolved {
			Some((LogicalExpr::Field(name), _)) => {
				SortKeyRead::Path(FieldPath::field(name.as_str()))
			}
			Some((expr, _)) => SortKeyRead::Computed(expr.clone()),
			None => match field_path_from_expr(&order.value) {
				Ok(path) => SortKeyRead::Path(path),
				Err(_) => SortKeyRead::Computed(order.value.clone()),
			},
		};
		if let SortKeyRead::Computed(expr) = &read {
			let key = u16::try_from(index)
				.map_err(|_| NotCovered::internal("more sort keys than a sort holds"))?;
			let shares = resolved.is_some()
				&& projection.as_deref_mut().is_some_and(|projection| {
					share_sort_key(projection, &order.value, expr, key, registry)
				});
			if !shares && !is_effect_free_over_pairs(expr, registry) {
				return Err(NotCovered::internal("a computed sort key with an effect"));
			}
		}
		keys.push(SortKeySpec {
			read,
			ascending: order.direction,
			collate: order.collate,
			numeric: order.numeric,
		});
	}
	Ok(keys)
}

/// Whether evaluating `expr` has no effect as [`is_effect_free`] reads it,
/// reading a field of a join's alias as the field of an object it is.
///
/// A join sets the build side's row under its alias, and the rows a front
/// end's join pairs are objects: a read through the alias is a read of an
/// object's field, which dereferences nothing. [`is_effect_free`] answers for
/// the alias as it answers for any name, whose value may be a record id.
fn is_effect_free_over_pairs(
	expr: &LogicalExpr,
	registry: &crate::exec::function::FunctionRegistry,
) -> bool {
	use crate::expr::logical::RelationAlias;
	let mut read = expr.clone();
	let _: Result<(), std::convert::Infallible> = read.visit_mut(&mut |node| {
		if let LogicalExpr::Access {
			base,
			field,
		} = node && matches!(base.as_ref(), LogicalExpr::Field(name)
			if RelationAlias::index_of(name.as_str()).is_some())
		{
			*node = LogicalExpr::Field(field.clone());
		}
		Ok(())
	});
	is_effect_free(&read, registry)
}

/// Mark the field of `projection` that sort key `key`, `expr` resolved from
/// the `ORDER BY` value `named`, names, to read the key's value; whether one
/// was marked. A projection function writes its answer at the names it
/// produces rather than returning a value, so its field is not marked.
fn share_sort_key(
	projection: &mut Projection,
	named: &LogicalExpr,
	expr: &LogicalExpr,
	key: u16,
	_registry: &crate::exec::function::FunctionRegistry,
) -> bool {
	match projection {
		Projection::Value(value) if value == expr => {
			*projection = Projection::SortedValue(key);
			true
		}
		Projection::Fields {
			fields,
			..
		}
		| Projection::Star {
			fields,
			..
		} => {
			let Ok(path) = field_path_from_expr(named) else {
				return false;
			};
			match fields
				.iter_mut()
				.find(|field| field.aliased && field.output == path && &field.expr == expr)
			{
				Some(field) => {
					field.sort_key = Some(key);
					true
				}
				None => false,
			}
		}
		_ => false,
	}
}

/// The sources of a `FROM` of several sources that a KNN scan answers part
/// of, with the statement's KNN conjuncts taken from its predicates.
///
/// A KNN conjunct holds for a row whose record a KNN scan of the statement
/// yielded before the row was read, and for no other. A leaf after a scan
/// whose rows may be such records — a record, a range or a value, or any read
/// of a table a scan before it searches — keeps each row whose `id` one of
/// the scans before it yields: a semi join against those scans, which run
/// once when the leaf opens. A leaf reading an existing table no scan before
/// it searches, and one that precedes every scan, whatever it reads, becomes
/// an empty read. One reading a table missing when the statement is planned
/// keeps its read under a filter no row passes, so the read raises the error
/// it owes, and a table a write earlier in the same block creates still
/// contributes no row.
async fn knn_union_sources(
	planner: &Planner<'_>,
	leaves: &mut [Plan],
	tables: &[Option<TableName>],
) -> Mapped<()> {
	// The scans before the leaf in hand, and the tables they search.
	let mut scans: Vec<Plan> = Vec::new();
	let mut scanned: Vec<TableName> = Vec::new();
	let (Some(txn), Some((ns, db))) =
		(planner.txn(), crate::exec::vm::host::out_of_line(|| planner.ns_db_ids()).await)
	else {
		return Err(NotCovered::unresolved_database(planner));
	};
	let statement = |root: Plan| Plan::Subquery {
		plan: Box::new(PhysicalPlan {
			catalog: None,
			root,
			dependencies: Vec::new(),
			tail: Tail::default(),
			sources: Vec::new(),
			nested: Vec::new(),
			scope: IndexScope::default(),
		}),
	};
	for (leaf, table) in leaves.iter_mut().zip(tables) {
		if let Plan::KnnScan {
			table,
			..
		} = leaf
		{
			scanned.push(table.clone());
			scans.push(leaf.clone());
			continue;
		}
		let empty = Plan::Union {
			inputs: Vec::new(),
		};
		let shares_ids = match table {
			Some(table) => scanned.contains(table),
			None => true,
		};
		if !scans.is_empty() && shares_ids {
			// The scans' rows are keyed by `id` under an alias no row of the
			// leaf reads, since the semi join emits each leaf row as it is.
			const ALIAS: &str = "__knn";
			let read = std::mem::replace(leaf, empty);
			let searched = match <[Plan; 1]>::try_from(scans.clone()) {
				Ok([scan]) => scan,
				Err(scans) => Plan::Union {
					inputs: scans,
				},
			};
			*leaf = statement(Plan::Join {
				left: Box::new(read),
				right: Box::new(searched),
				kind: super::plan::JoinMatch::Semi,
				keys: vec![(
					LogicalExpr::Field("id".into()),
					LogicalExpr::Access {
						base: Box::new(LogicalExpr::Field(ALIAS.into())),
						field: "id".into(),
					},
				)],
				residual: None,
				alias: ALIAS.to_owned(),
			});
			continue;
		}
		let unread = |_| NotCovered::internal("the table catalog could not be read");
		let exists = match table {
			Some(table) => txn.get_tb(ns, db, table, None).await.map_err(unread)?.is_some(),
			None => false,
		};
		let read = std::mem::replace(leaf, empty);
		if !exists {
			// A union input is a source, so the read and the filter that drops
			// its rows are a statement of their own.
			*leaf = statement(Plan::Filter {
				input: Box::new(read),
				predicate: LogicalExpr::Literal(LogicalLiteral::Bool(false)),
			});
		}
	}
	Ok(())
}

/// `projection` with each KNN operator a search answers written as `true`.
fn knn_answered(projection: &mut Projection) {
	fn answered(expr: &mut LogicalExpr) {
		let Ok(()) = expr.visit_mut(&mut |node| {
			if let LogicalExpr::Binary {
				op: BinaryOperator::NearestNeighbor(nn),
				..
			} = node && matches!(
				nn.as_ref(),
				crate::expr::operator::NearestNeighbor::K(..)
					| crate::expr::operator::NearestNeighbor::Approximate(..)
			) {
				*node = LogicalExpr::Literal(LogicalLiteral::Bool(true));
			}
			Ok::<_, std::convert::Infallible>(())
		});
	}
	match projection {
		Projection::All | Projection::SortedValue(_) => {}
		Projection::Value(expr)
		| Projection::OmittedValue {
			expr,
			..
		} => answered(expr),
		Projection::Fields {
			fields,
			..
		}
		| Projection::Star {
			fields,
			..
		} => fields.iter_mut().for_each(|field| answered(&mut field.expr)),
	}
}

/// What [`Blocks`] holds that mapping a loop body adds to.
struct BlocksMark {
	sources: usize,
	written: crate::exec::logical::footprint::Footprint,
	implicit: Vec<(surrealdb_strand::TableName, bool)>,
	implicit_any: bool,
	transparent: bool,
	dead: bool,
	refusal: Option<super::plan::Refusal>,
}

/// The state of mapping a block's control flow: the names the blocks around
/// the statement being mapped bind, and the statements mapped so far.
struct Blocks {
	bound: Vec<String>,
	sources: Vec<(LogicalOp, Vec<String>)>,
	/// The catalog the statements mapped from here on are planned against,
	/// as the block's schema statements before them leave it: what their
	/// speculative runs wrote (`vm::speculate`). `None` when the planner may
	/// not speculate, so no schema statement is modelled and a block reading
	/// what one changes is declined whole.
	overlay: Option<crate::kvs::speculation::Overlay>,
	/// Whether the statement being mapped runs once for each time the block
	/// runs, in the order the block holds it: not inside an `IF` branch or a
	/// loop body. Only such a schema statement's effect is one every
	/// statement after it can be planned knowing.
	straight: bool,
	/// Whether every statement mapped so far changes the catalog only as the
	/// statements after it are planned knowing: a schema statement modelled
	/// after one that is not would be planned against a catalog the program
	/// does not know.
	transparent: bool,
	/// What the statements mapped so far that the block was not planned
	/// knowing may have written: every statement's but the schema statements
	/// run speculatively, whose catalog writes are the overlay's. The tables a
	/// write may define are [`Self::implicit`]'s, not this footprint's.
	written: crate::exec::logical::footprint::Footprint,
	/// The tables a statement mapped so far may define by writing to them,
	/// which the overlay does not hold, each with whether every statement that
	/// may define it is a write the program runs itself that defines it
	/// through its own subjects ([`defines_through_own_write`]): a definition
	/// that takes the reservations of a schema statement's
	/// ([`super::program::ImplicitTable`]). A statement after it reads such a
	/// table as the open table with no definition it is until then.
	implicit: Vec<(surrealdb_strand::TableName, bool)>,
	/// Whether a statement mapped so far may write to a table it evaluates,
	/// which may be any table the overlay does not hold.
	implicit_any: bool,
	/// Whether a statement mapped so far is sure to raise whenever the block
	/// reaches it, and runs whenever the block runs: nothing after it is
	/// reached, so nothing after it is compiled.
	dead: bool,
	/// While a loop body is mapped for the iterations after its first: each
	/// schema statement mapped, with its footprint and whether it was folded
	/// away as doing nothing.
	later_schema: Option<Vec<(crate::exec::logical::footprint::Footprint, bool)>>,
	/// Whether the statement being mapped is one the operator tree compiles
	/// when it plans the statement holding the block, before any of it runs:
	/// in each block, the statements up to and including its first schema
	/// statement. It compiles a later one when the block reaches it.
	eager: bool,
	/// The error the operator tree raises as it plans the statement, which
	/// the statement is answered with ([`Plan::Refused`]).
	refusal: Option<super::plan::Refusal>,
	/// Whether the block is planned late-bound after a change: no schema
	/// statement is run speculatively, and whatever follows one that may
	/// change the catalog or the names it resolves in is planned late-bound
	/// ([`Planner::late_binding`]), loading each catalog object it names when
	/// it runs, and resolving the session's namespace and database again.
	late_after: bool,
	/// Whether a statement mapped so far may change the catalog, or the
	/// namespace and database, the statements after it resolve against, in a
	/// way the block was not planned knowing. Only a block planned late-bound
	/// after a change reads it.
	changed: bool,
}

impl Blocks {
	fn of(planner: &Planner<'_>) -> Self {
		let late_after = planner.binds_late_after_schema();
		let speculates = planner.speculation.is_some() && planner.txn.is_some() && !late_after;
		#[cfg(test)]
		let speculates = speculates && !super::speculate::WITHOUT_SPECULATION.with(std::cell::Cell::get);
		Self {
			bound: Vec::new(),
			sources: Vec::new(),
			overlay: speculates.then(crate::kvs::speculation::Overlay::default),
			straight: true,
			transparent: true,
			written: Default::default(),
			implicit: Vec::new(),
			implicit_any: false,
			dead: false,
			later_schema: None,
			eager: true,
			refusal: None,
			late_after,
			changed: false,
		}
	}

	/// Note that expression `expr`, which the block evaluates where it reaches
	/// it, may change the catalog through code it reaches — a query given as
	/// text, a function the plan cannot inspect — in a block planned
	/// late-bound after a change.
	async fn note_hidden_schema(&mut self, planner: &Planner<'_>, expr: &LogicalExpr) {
		if !self.late_after || self.changed {
			return;
		}
		let op = LogicalOp::Eval(crate::expr::logical::LogicalEval {
			expr: expr.clone(),
		});
		let planner = planner.shadowing(self.bound.iter().cloned());
		let may_change = match seen(&planner, &op, &self.bound).await {
			Seen::Unseen(_) => true,
			Seen::Schema(footprint) => footprint.writes_catalog(),
			Seen::Writes {
				footprint,
				..
			} => footprint.writes.iter().any(changes_other_than_a_table),
		};
		if may_change {
			self.changed = true;
		}
	}

	/// What mapping has accumulated, for [`Self::rewind`] to undo when a
	/// loop body is mapped and then left to be planned when it runs.
	fn mark(&self) -> BlocksMark {
		BlocksMark {
			sources: self.sources.len(),
			written: self.written.clone(),
			implicit: self.implicit.clone(),
			implicit_any: self.implicit_any,
			transparent: self.transparent,
			dead: self.dead,
			refusal: self.refusal.clone(),
		}
	}

	/// Undo the mapping since `mark`.
	fn rewind(&mut self, mark: BlocksMark) {
		self.sources.truncate(mark.sources);
		self.written = mark.written;
		self.implicit = mark.implicit;
		self.implicit_any = mark.implicit_any;
		self.transparent = mark.transparent;
		self.dead = mark.dead;
		self.refusal = mark.refusal;
	}

	/// Statement `op`, which the operator tree refuses with `refusal` when the
	/// block reaches it, raising the refusal there.
	fn raised_when_reached(
		&mut self,
		op: &LogicalOp,
		refusal: super::plan::Refusal,
		late_bound: bool,
	) -> Mapped<Control> {
		let source = u16::try_from(self.sources.len())
			.map_err(|_| NotCovered::internal("more statements than a block holds"))?;
		self.sources.push((op.clone(), self.bound.clone()));
		Ok(Self::refusal_statement(source, refusal, late_bound))
	}

	/// The statement at `source` that raises `refusal` when the block
	/// reaches it.
	fn refusal_statement(source: u16, refusal: super::plan::Refusal, late_bound: bool) -> Control {
		Control::Statement {
			plan: Box::new(PhysicalPlan {
				catalog: None,
				root: Plan::Refused {
					refusal,
				},
				dependencies: Vec::new(),
				tail: Tail::default(),
				sources: Vec::new(),
				nested: Vec::new(),
				scope: IndexScope::default(),
			}),
			source,
			changes_catalog: false,
			planned_over: None,
			modelled: None,
			analysis: None,
			late_bound,
			within: CatalogWithin::default(),
			raises_when_reached: true,
		}
	}

	/// The control flow of `op`, a block, `LET`, `FOR`, `RETURN` or `SLEEP`,
	/// or of a statement a block runs.
	async fn control(&mut self, planner: &Planner<'_>, op: &LogicalOp) -> Mapped<Control> {
		use crate::expr::logical::{LogicalLiteral, LogicalOp as Op};
		Ok(match op {
			Op::Sequence(sequence) => {
				let outer = self.bound.len();
				// Only the statements the block reaches have a control in its
				// sequence. A statement past them never runs: it is planned for the
				// refusals it raises, which the block raises, and its control is dropped.
				let reached = sequence.reachable_prefix().len();
				let mut stmts = Vec::with_capacity(reached);
				let first_change =
					sequence.stmts.iter().position(crate::exec::planner::fold::changes_the_catalog);
				let eager = self.eager;
				// What follows a statement that may change the catalog in a way
				// the block cannot know is planned late-bound: it resolves each
				// catalog object it names when it runs.
				let late = planner.late_binding();
				// The state the unreachable statements are planned over, restored
				// after them: what they would bind, write, define or speculate never
				// reaches the plan. The refusals they raise stay.
				let mut unreached = None;
				for (at, statement) in sequence.stmts.iter().enumerate() {
					if at == reached {
						unreached = Some((self.mark(), self.overlay.clone(), self.changed));
					}
					self.eager = eager && first_change.is_none_or(|first| at <= first);
					if self.dead {
						#[cfg(test)]
						super::speculate::FOLDED.with(|count| count.set(count.get() + 1));
						break;
					}
					let planner = match self.late_after && self.changed {
						true => &late,
						false => planner,
					};
					let control =
						crate::exec::vm::host::out_of_line(|| self.control(planner, statement))
							.await?;
					// A `LET` binds its name for the statements after it.
					if let Op::Let(stmt) = statement {
						self.bound.push(stmt.name.clone());
					}
					if unreached.is_none() {
						stmts.push(control);
					}
				}
				if let Some((mark, overlay, changed)) = unreached {
					let refusal = self.refusal.clone();
					self.rewind(mark);
					self.overlay = overlay;
					self.changed = changed;
					self.refusal = refusal;
				}
				self.eager = eager;
				self.bound.truncate(outer);
				Control::Seq(stmts)
			}
			Op::Let(stmt) => {
				// A protected name is refused when the operator tree compiles
				// the `LET`, which is before the statement runs where the tree
				// compiles it with the statement.
				if surrealdb_cnf::PROTECTED_PARAM_NAMES.contains(&stmt.name.as_str()) {
					// After a schema statement the operator tree compiles the
					// `LET` when its block reaches it, so the refusal is
					// raised there.
					if !self.eager {
						return self.raised_when_reached(
							op,
							super::plan::Refusal::ProtectedParam {
								name: stmt.name.clone(),
							},
							planner.is_late_bound(),
						);
					}
					self.refusal.get_or_insert_with(|| super::plan::Refusal::ProtectedParam {
						name: stmt.name.clone(),
					});
				}
				// A stored clause's `LET` of a `LET` raises when the clause
				// reaches it, as the operator tree evaluates the value then.
				if planner.is_stored_clause() && is_a_let(&stmt.value) {
					return self.raised_when_reached(
						op,
						super::plan::Refusal::InvalidStatement {
							message: LET_IN_AN_EXPRESSION.to_owned(),
						},
						planner.is_late_bound(),
					);
				}
				// A statement `IF`, `THROW`, `BREAK` or `CONTINUE` as the value
				// is the value-position expression it evaluates to.
				let evaluated;
				let value_op = match stmt.value.as_ref() {
					value @ (Op::If(_) | Op::Throw(_) | Op::Break(_) | Op::Continue(_))
						if !maps_as_control(value) =>
					{
						evaluated = Op::Eval(crate::expr::logical::LogicalEval {
							expr: value.clone().into_scalar(),
						});
						&evaluated
					}
					value => value,
				};
				// An expression holding no statement is evaluated where the
				// block reaches it; anything else runs as a statement, whose
				// `RETURN` answers the `LET`.
				let value = match value_op {
					Op::Eval(eval) if self.evaluated_in_place(planner, value_op, &eval.expr) => {
						crate::exec::vm::host::out_of_line(|| {
							self.note_hidden_schema(planner, &eval.expr)
						})
						.await;
						Control::Value(eval.expr.clone())
					}
					value if inline_value(value) => {
						crate::exec::vm::host::out_of_line(|| self.inline(planner, value)).await?
					}
					value => {
						crate::exec::vm::host::out_of_line(|| self.statement(planner, value))
							.await?
					}
				};
				Control::Let {
					name: stmt.name.clone(),
					kind: stmt.kind.clone(),
					value: Box::new(value),
				}
			}
			Op::Foreach(foreach) => {
				let entry_changed = self.changed;
				let range =
					crate::exec::vm::host::out_of_line(|| self.value(planner, &foreach.range))
						.await?;
				// A range that may change what the body resolves against
				// leaves the body to be planned when each iteration reaches it.
				let range_changed = self.late_after && self.changed;
				self.bound.push(foreach.param.as_str().to_owned());
				let late = planner.late_binding();
				let peeled = match range_changed {
					true => Ok(None),
					false => {
						crate::exec::vm::host::out_of_line(|| self.peel(planner, &foreach.body))
							.await
					}
				};
				let (first, body) = match peeled {
					Ok(Some((first, rest))) => (Some(Box::new(first)), Ok(rest)),
					Ok(None) if range_changed => (
						None,
						crate::exec::vm::host::out_of_line(|| self.control(&late, &foreach.body))
							.await,
					),
					Ok(None) => {
						let straight = std::mem::replace(&mut self.straight, false);
						// A body one of whose statements may change the catalog
						// runs again after that change, so none of it is planned
						// before the loop: it is planned each time an iteration
						// reaches it.
						let mark = self.late_after.then(|| self.mark());
						if self.late_after {
							self.changed = false;
						}
						let body = crate::exec::vm::host::out_of_line(|| {
							self.control(planner, &foreach.body)
						})
						.await;
						self.straight = straight;
						// A body that may change the catalog runs again after
						// the change, so all of it is planned late-bound.
						let body = match (mark, body) {
							(Some(mark), Ok(_)) if self.changed => {
								self.rewind(mark);
								self.changed = true;
								crate::exec::vm::host::out_of_line(|| {
									self.control(&late, &foreach.body)
								})
								.await
							}
							(_, body) => body,
						};
						if self.late_after {
							self.changed |= entry_changed;
						}
						(None, body)
					}
					Err(why) => (None, Err(why)),
				};
				self.bound.pop();
				Control::For {
					param: foreach.param.clone(),
					range: Box::new(range),
					first,
					body: Box::new(body?),
				}
			}
			Op::Return(ret) => {
				// A stored clause's `RETURN` of a `LET` raises when the clause
				// reaches it, as the operator tree evaluates the `LET` then.
				if planner.is_stored_clause() && is_a_let(&ret.what) {
					return self.raised_when_reached(
						op,
						super::plan::Refusal::InvalidStatement {
							message: LET_IN_AN_EXPRESSION.to_owned(),
						},
						planner.is_late_bound(),
					);
				}
				// `RETURN … FETCH` fetches into each value the statement
				// yields, as a `FETCH` over its rows does: the statement runs
				// as `what` under the `FETCH`, which is also its source.
				if !ret.fetch.is_empty() {
					let fetched = Op::Fetch(crate::expr::logical::LogicalFetch {
						input: ret.what.clone(),
						fetches: ret.fetch.clone(),
					});
					let value = match ret.what.as_ref() {
						Op::Eval(eval) if !holds_a_body(&eval.expr) => self.fetched_value(
							&fetched,
							&eval.expr,
							&ret.fetch,
							planner.is_late_bound(),
						)?,
						// A write's rows are fetched into as the array they make
						// up, its value as a statement: a path starting at a field
						// walks into each element of an array as into one row.
						Op::Write(_) if ret.fetch.iter().all(starts_at_a_field) => {
							let fetched = Op::Fetch(crate::expr::logical::LogicalFetch {
								input: Box::new(Op::Eval(crate::expr::logical::LogicalEval {
									expr: LogicalExpr::Subquery(ret.what.clone()),
								})),
								fetches: ret.fetch.clone(),
							});
							crate::exec::vm::host::out_of_line(|| self.statement(planner, &fetched))
								.await?
						}
						_ => {
							crate::exec::vm::host::out_of_line(|| self.statement(planner, &fetched))
								.await?
						}
					};
					return Ok(Control::Return {
						value: Box::new(value),
					});
				}
				let value = match ret.what.as_ref() {
					what if inline_value(what) => {
						crate::exec::vm::host::out_of_line(|| self.inline(planner, what)).await?
					}
					// An expression's value is its one value, a `RETURN` inside
					// it answering it.
					Op::Eval(eval) => {
						crate::exec::vm::host::out_of_line(|| self.value(planner, &eval.expr))
							.await?
					}
					what => {
						crate::exec::vm::host::out_of_line(|| self.statement(planner, what)).await?
					}
				};
				Control::Return {
					value: Box::new(value),
				}
			}
			Op::Sleep(sleep) => {
				let LogicalExpr::Literal(LogicalLiteral::Duration(duration)) = &sleep.duration
				else {
					return Err(NotCovered::internal("a SLEEP whose duration is evaluated"));
				};
				Control::Sleep {
					duration: *duration,
				}
			}
			Op::If(ifs) => {
				let straight = std::mem::replace(&mut self.straight, false);
				let mapped =
					crate::exec::vm::host::out_of_line(|| self.if_else_op(planner, ifs, straight))
						.await;
				self.straight = straight;
				mapped?
			}
			Op::Throw(throw) => Control::Throw {
				value: Box::new(
					crate::exec::vm::host::out_of_line(|| self.value(planner, &throw.value))
						.await?,
				),
			},
			Op::Break(_) => Control::Break,
			Op::Continue(_) => Control::Continue,
			Op::Eval(eval) => match &eval.expr {
				LogicalExpr::If(ifs) => {
					let straight = std::mem::replace(&mut self.straight, false);
					let mapped =
						crate::exec::vm::host::out_of_line(|| self.if_else(planner, ifs, straight))
							.await;
					self.straight = straight;
					mapped?
				}
				LogicalExpr::Throw(value) => Control::Throw {
					value: Box::new(
						crate::exec::vm::host::out_of_line(|| self.value(planner, value)).await?,
					),
				},
				// A statement written as an expression, as binding leaves an
				// `IF` whose condition it folded, is the statement.
				LogicalExpr::Subquery(_) => {
					crate::exec::vm::host::out_of_line(|| self.branch(planner, &eval.expr)).await?
				}
				// An expression holding no statement is evaluated where the
				// block reaches it, as a `FOR` range is.
				expr if self.evaluated_in_place(planner, op, expr) => {
					crate::exec::vm::host::out_of_line(|| self.note_hidden_schema(planner, expr))
						.await;
					Control::Value(expr.clone())
				}
				_ => crate::exec::vm::host::out_of_line(|| self.statement(planner, op)).await?,
			},
			other => crate::exec::vm::host::out_of_line(|| self.statement(planner, other)).await?,
		})
	}

	/// The control flow of `value`, the value of a `LET` or `RETURN` that
	/// [`inline_value`] holds, bound as a statement of its own is and mapped
	/// with this program, so that a `RETURN` it holds is the `LET` or `RETURN`'s
	/// to catch. A value that does not bind now is mapped as a statement, which
	/// declines or refuses it as the operator tree does.
	async fn inline(&mut self, planner: &Planner<'_>, value: &LogicalOp) -> Mapped<Control> {
		let bound = crate::exec::vm::host::out_of_line(|| self.bound_value(planner, value)).await;
		match bound {
			Some(bound) => {
				crate::exec::vm::host::out_of_line(|| self.control(planner, &bound)).await
			}
			None => crate::exec::vm::host::out_of_line(|| self.statement(planner, value)).await,
		}
	}

	/// `value` bound as [`Self::statement`] binds a statement: against the names
	/// bound around it and the catalog the block's schema statements before it
	/// leave. `None` where it does not bind.
	async fn bound_value(&self, planner: &Planner<'_>, value: &LogicalOp) -> Option<LogicalOp> {
		let planner = planner.shadowing(self.bound.iter().cloned());
		let within =
			crate::exec::vm::host::out_of_line(|| catalog_within(&planner, value, &self.bound))
				.await;
		let planner = planner.reaching(within);
		let over = self.overlay.as_ref().filter(|overlay| !overlay.is_empty());
		let reached = !self.eager;
		match over {
			Some(overlay) => {
				let (speculation, over) = super::speculate::view(&planner, overlay)?;
				let bound = bind_for_map(&over, value, reached).await;
				drop(speculation);
				bound.ok()
			}
			None => bind_for_map(&planner, value, reached).await.ok(),
		}
	}

	/// An `IF` in a statement's place: each condition not settled by the
	/// constants in scope, and its branch; a condition they settle true
	/// decides the `ELSE`. A branch every condition before it settles is
	/// reached as the statements around the `IF` are, `straight` saying how.
	async fn if_else(
		&mut self,
		planner: &Planner<'_>,
		ifs: &crate::expr::logical::LogicalIf,
		straight: bool,
	) -> Mapped<Control> {
		let mut branches = Vec::with_capacity(ifs.branches.len());
		let mut otherwise = None;
		// A branch changes the catalog only when it runs, and the branches are
		// exclusive: each is mapped from what the conditions before it left,
		// and the statements after the `IF` are left to be planned when any
		// of them may have changed it.
		let mut any_changed = false;
		let arms = ifs.branches.len();
		for (at, (source, value)) in ifs.branches.iter().enumerate() {
			let condition =
				crate::exec::vm::host::out_of_line(|| self.value(planner, source)).await?;
			let entering = self.changed;
			// Only a literal `true` that is the last arm is folded: its branch
			// is taken whenever the block reaches the `IF`, and no arm after it
			// exists. Every other condition, including one a constant `LET`
			// settles, is mapped as an ordinary branch, so the statements in an
			// arm that never runs are still planned and their refusals raised.
			let literal_true = matches!(
				crate::exec::planner::util::try_literal_to_value(source),
				Some(crate::val::Value::Bool(true))
			);
			let last = at + 1 == arms && ifs.otherwise.is_none();
			let folded = match literal_true && last {
				true => constant_truth(&condition),
				false => None,
			};
			match folded {
				Some(true) => {
					let settled = branches.is_empty();
					let inner = std::mem::replace(&mut self.straight, straight && settled);
					let taken =
						crate::exec::vm::host::out_of_line(|| self.branch(planner, value)).await;
					self.straight = inner;
					otherwise = Some(taken?);
					any_changed |= std::mem::replace(&mut self.changed, entering);
					break;
				}
				_ => {
					let branch =
						crate::exec::vm::host::out_of_line(|| self.branch(planner, value)).await?;
					any_changed |= std::mem::replace(&mut self.changed, entering);
					branches.push((condition, branch));
				}
			}
		}
		let otherwise = match (otherwise, &ifs.otherwise) {
			(Some(taken), _) => Some(Box::new(taken)),
			(None, Some(value)) => {
				let entering = self.changed;
				let inner = std::mem::replace(&mut self.straight, straight && branches.is_empty());
				let branch =
					crate::exec::vm::host::out_of_line(|| self.branch(planner, value)).await;
				self.straight = inner;
				let branch = branch?;
				any_changed |= std::mem::replace(&mut self.changed, entering);
				Some(Box::new(branch))
			}
			(None, None) => None,
		};
		self.changed |= any_changed;
		Ok(Control::If {
			branches,
			otherwise,
		})
	}

	/// A statement `IF` in a statement's place: [`Self::if_else`] for its
	/// arms, each mapped as [`Self::arm`] maps one.
	async fn if_else_op(
		&mut self,
		planner: &Planner<'_>,
		ifs: &crate::expr::logical::LogicalIfOp,
		straight: bool,
	) -> Mapped<Control> {
		let mut branches = Vec::with_capacity(ifs.conditions.len());
		let mut otherwise = None;
		let mut any_changed = false;
		let arms = ifs.conditions.len();
		for (at, (source, arm)) in ifs.branches().enumerate() {
			let condition =
				crate::exec::vm::host::out_of_line(|| self.value(planner, source)).await?;
			let entering = self.changed;
			// Only a literal `true` that is the last arm, with no `ELSE`, is
			// folded: its arm is taken whenever the block reaches the `IF`, and
			// no arm after it exists. Every other arm, including one a literal
			// `false` or a constant `LET` rules out, is mapped as an ordinary
			// branch, so its statements are planned and their refusals raised.
			let literal_true = matches!(
				crate::exec::planner::util::try_literal_to_value(source),
				Some(crate::val::Value::Bool(true))
			);
			let last = at + 1 == arms && ifs.otherwise().is_none();
			let folded = match literal_true && last {
				true => constant_truth(&condition),
				false => None,
			};
			match folded {
				Some(true) => {
					let settled = branches.is_empty();
					let inner = std::mem::replace(&mut self.straight, straight && settled);
					let taken = crate::exec::vm::host::out_of_line(|| self.arm(planner, arm)).await;
					self.straight = inner;
					otherwise = Some(taken?);
					any_changed |= std::mem::replace(&mut self.changed, entering);
					break;
				}
				_ => {
					let branch =
						crate::exec::vm::host::out_of_line(|| self.arm(planner, arm)).await?;
					any_changed |= std::mem::replace(&mut self.changed, entering);
					branches.push((condition, branch));
				}
			}
		}
		let otherwise = match (otherwise, ifs.otherwise()) {
			(Some(taken), _) => Some(Box::new(taken)),
			(None, Some(arm)) => {
				let entering = self.changed;
				let inner = std::mem::replace(&mut self.straight, straight && branches.is_empty());
				let branch = crate::exec::vm::host::out_of_line(|| self.arm(planner, arm)).await;
				self.straight = inner;
				let branch = branch?;
				any_changed |= std::mem::replace(&mut self.changed, entering);
				Some(Box::new(branch))
			}
			(None, None) => None,
		};
		self.changed |= any_changed;
		Ok(Control::If {
			branches,
			otherwise,
		})
	}

	/// An arm of a statement `IF`: an expression as [`Self::branch`] maps it,
	/// a block, loop, `RETURN`, `SLEEP`, `IF`, `THROW`, `BREAK` or `CONTINUE`
	/// as control flow, and another statement as the block runs one.
	async fn arm(&mut self, planner: &Planner<'_>, arm: &LogicalOp) -> Mapped<Control> {
		use crate::expr::logical::LogicalOp as Op;
		match arm {
			Op::Eval(eval) => {
				crate::exec::vm::host::out_of_line(|| self.branch(planner, &eval.expr)).await
			}
			Op::Sequence(_)
			| Op::Foreach(_)
			| Op::Return(_)
			| Op::Sleep(_)
			| Op::If(_)
			| Op::Throw(_)
			| Op::Break(_)
			| Op::Continue(_) => crate::exec::vm::host::out_of_line(|| self.control(planner, arm)).await,
			other => crate::exec::vm::host::out_of_line(|| self.statement(planner, other)).await,
		}
	}

	/// Whether expression statement `op`, of expression `expr`, is evaluated
	/// in the block's control flow rather than run as a statement: it holds
	/// no statement, closure or call of a user-defined function, is no
	/// `BREAK` or `CONTINUE`, and binds alike whenever the block reaches it.
	fn evaluated_in_place(
		&self,
		planner: &Planner<'_>,
		op: &LogicalOp,
		expr: &LogicalExpr,
	) -> bool {
		!matches!(expr, LogicalExpr::Break | LogicalExpr::Continue)
			&& !holds_a_body(expr)
			&& binds_alike(op, &planner.shadowing(self.bound.iter().cloned()))
	}

	/// A branch of an `IF` in a statement's place: a block, loop, `RETURN`
	/// or `SLEEP` as control flow, `BREAK` and `CONTINUE` as the loop's jumps,
	/// another statement as the block runs one, and any other expression as
	/// a value.
	///
	/// A branch that is a `LET`, rather than a block holding one, is a `LET`
	/// written as an expression, which the operator tree refuses as it
	/// compiles the `IF` with the block, whether or not the branch is taken:
	/// it is mapped as the expression statement it is, which refuses the
	/// block. After a schema statement, or late-bound, the tree compiles the
	/// `IF` when the block reaches it, and the refusal is raised only when
	/// the branch is taken.
	async fn branch(&mut self, planner: &Planner<'_>, value: &LogicalExpr) -> Mapped<Control> {
		use crate::expr::logical::{LogicalEval, LogicalOp as Op};
		match value {
			LogicalExpr::Subquery(op) => match op.as_ref() {
				Op::Sequence(_) | Op::Foreach(_) | Op::Return(_) | Op::Sleep(_) | Op::Eval(_) => {
					crate::exec::vm::host::out_of_line(|| self.control(planner, op)).await
				}
				Op::Let(_) => {
					let op = Op::Eval(LogicalEval {
						expr: value.clone(),
					});
					crate::exec::vm::host::out_of_line(|| self.statement(planner, &op)).await
				}
				other => {
					crate::exec::vm::host::out_of_line(|| self.statement(planner, other)).await
				}
			},
			LogicalExpr::Break
			| LogicalExpr::Continue
			| LogicalExpr::If(_)
			| LogicalExpr::Throw(_) => {
				let op = Op::Eval(LogicalEval {
					expr: value.clone(),
				});
				crate::exec::vm::host::out_of_line(|| self.control(planner, &op)).await
			}
			other => crate::exec::vm::host::out_of_line(|| self.value(planner, other)).await,
		}
	}

	/// An expression of the block's control flow — a `FOR` range, an `IF`
	/// condition, a `RETURN` or `THROW` value — evaluated where the block
	/// reaches it; one holding a statement, or a call whose body is compiled
	/// with the program, runs as an expression statement, so what it holds is
	/// compiled against the catalog the block's schema statements leave.
	async fn value(&mut self, planner: &Planner<'_>, expr: &LogicalExpr) -> Mapped<Control> {
		if !holds_a_body(expr) {
			crate::exec::vm::host::out_of_line(|| self.note_hidden_schema(planner, expr)).await;
			return Ok(Control::Value(expr.clone()));
		}
		let op = LogicalOp::Eval(crate::expr::logical::LogicalEval {
			expr: expr.clone(),
		});
		crate::exec::vm::host::out_of_line(|| self.statement(planner, &op)).await
	}

	/// Expression `expr`, holding no statement, with `fetches` fetched into
	/// its value: evaluated where the block reaches it, as a value of the
	/// block's control flow is, as the statement `fetched` — the expression
	/// under its `FETCH` — which is its source.
	fn fetched_value(
		&mut self,
		fetched: &LogicalOp,
		expr: &LogicalExpr,
		fetches: &[LogicalExpr],
		planner_late: bool,
	) -> Mapped<Control> {
		let source = u16::try_from(self.sources.len())
			.map_err(|_| NotCovered::internal("more statements than a block holds"))?;
		self.sources.push((fetched.clone(), self.bound.clone()));
		let planned_over = self
			.overlay
			.as_ref()
			.filter(|overlay| !overlay.is_empty())
			.cloned()
			.map(std::sync::Arc::new);
		Ok(Control::Statement {
			plan: Box::new(PhysicalPlan {
				catalog: None,
				root: Plan::Eval {
					expr: expr.clone(),
					fetches: fetches.to_vec(),
				},
				dependencies: Vec::new(),
				tail: Tail::default(),
				sources: Vec::new(),
				nested: Vec::new(),
				scope: IndexScope::default(),
			}),
			source,
			changes_catalog: false,
			planned_over,
			modelled: None,
			analysis: None,
			late_bound: planner_late,
			within: CatalogWithin::default(),
			raises_when_reached: false,
		})
	}

	/// A statement a block runs, bound and mapped when the query arrives, to be
	/// lowered with the program as a group of its own.
	///
	/// A schema statement is mapped like any other, its speculative run
	/// modelling for the statements after it what it changes, and
	/// `EXPLAIN ANALYZE` maps the statement it runs, with what it renders of
	/// that run ([`analysis_of`]). The whole statement holding the block is not
	/// covered where a statement does not bind or map against the catalog the
	/// block's schema statements leave, where a statement holds a schema
	/// statement inside it, and where a statement can be compiled only when
	/// the block reaches it: one the operator tree refuses only then, and one
	/// after a schema statement whose binding folds the clock, which the
	/// operator tree binds when the block reaches it ([`binds_the_clock`]).
	async fn statement(&mut self, planner: &Planner<'_>, op: &LogicalOp) -> Mapped<Control> {
		match op {
			LogicalOp::Eval(eval) if matches!(eval.expr, LogicalExpr::Break) => {
				return Ok(Control::Break);
			}
			LogicalOp::Eval(eval) if matches!(eval.expr, LogicalExpr::Continue) => {
				return Ok(Control::Continue);
			}
			LogicalOp::Break(_) => return Ok(Control::Break),
			LogicalOp::Continue(_) => return Ok(Control::Continue),
			// Run as a statement, a statement `IF` or `THROW` is the expression
			// it evaluates to.
			LogicalOp::If(_) | LogicalOp::Throw(_) => {
				let evaluated = LogicalOp::Eval(crate::expr::logical::LogicalEval {
					expr: op.clone().into_scalar(),
				});
				return crate::exec::vm::host::out_of_line(|| self.statement(planner, &evaluated))
					.await;
			}
			_ => {}
		}
		let source = u16::try_from(self.sources.len())
			.map_err(|_| NotCovered::internal("more statements than a block holds"))?;
		self.sources.push((op.clone(), self.bound.clone()));
		let changes_catalog = crate::exec::planner::fold::changes_the_catalog(op);
		// A schema statement inside another statement runs when that statement
		// reaches it, which the statement is not planned knowing: a block
		// planned late-bound after a change plans what follows late-bound.
		if changes_catalog && !matches!(op, LogicalOp::Ddl(_)) && !self.late_after {
			return Err(NotCovered::new("a block statement holding a schema statement"));
		}
		// What the session's namespace and database resolve to is settled
		// when the block starts, not by a speculative run of the statements
		// before one.
		if !self.late_after
			&& matches!(op, LogicalOp::Ddl(ddl) if crate::exec::logical::changes_name_resolution(ddl))
		{
			return Err(NotCovered::new(
				"a schema statement that changes the names it resolves in",
			));
		}
		let planner = planner.shadowing(self.bound.iter().cloned());
		let within =
			crate::exec::vm::host::out_of_line(|| catalog_within(&planner, op, &self.bound)).await;
		let planner = planner.reaching(within);
		// `EXPLAIN ANALYZE` runs the statement it explains: that statement is
		// the one bound and mapped.
		let analyzed = match op {
			LogicalOp::Explain(explain) if explain.analyze => Some(explain.format),
			_ => None,
		};
		let op = match op {
			LogicalOp::Explain(explain) if analyzed.is_some() => &*explain.input,
			op => op,
		};
		if analyzed.is_some() {
			self.sources[source as usize].0 = op.clone();
		}
		// A statement after the block's schema statements is bound and mapped
		// against the catalog as their speculative runs leave it.
		let over = self.overlay.as_ref().filter(|overlay| !overlay.is_empty()).cloned();
		let reached = !self.eager;
		let mapped = match &over {
			Some(overlay) => match super::speculate::view(&planner, overlay) {
				Some((speculation, over)) => {
					let mapped = bind_and_map(&over, op, &self.bound, reached).await;
					drop(speculation);
					mapped
				}
				None => {
					return Err(NotCovered::new(
						"a block statement over a catalog the speculation cannot view",
					));
				}
			},
			None => bind_and_map(&planner, op, &self.bound, reached).await,
		};
		let (physical, seen) = match mapped {
			BoundAndMapped::Mapped(physical, seen) => (*physical, seen),
			// A statement the operator tree refuses as it plans it refuses the
			// block where the tree plans it with the block: the statement is
			// answered with the refusal and nothing of the block runs.
			// A statement after the block's first schema statement, which the
			// operator tree compiles when the block reaches it, raises the
			// refusal there; so does a late-bound statement.
			BoundAndMapped::Refused(refusal) if planner.is_late_bound() || !self.eager => {
				return Ok(Self::refusal_statement(source, refusal, planner.is_late_bound()));
			}
			BoundAndMapped::Refused(refusal) => {
				self.transparent = false;
				self.refusal.get_or_insert(refusal);
				return Ok(Control::Value(LogicalExpr::Literal(
					crate::expr::logical::LogicalLiteral::None,
				)));
			}
			BoundAndMapped::Declined(why) => return Err(why),
		};
		let physical = super::optimize::optimize(physical);
		// A refusal mapped for a statement the operator tree compiles when
		// the block reaches it is raised there.
		let raises_when_reached = matches!(physical.root, Plan::Refused { .. })
			&& (planner.is_late_bound() || !self.eager);
		let analysis = match analyzed {
			Some(format) => Some(Box::new(analysis_of(&physical, &planner, format)?)),
			None => None,
		};
		// A statement that may change the catalog through code it reaches is
		// one the block cannot know the effect of.
		if self.late_after && matches!(seen, Seen::Unseen(_)) {
			self.changed = true;
		}
		let (footprint, modelled) = match seen {
			Seen::Schema(footprint) => {
				let modelled = crate::exec::vm::host::out_of_line(|| {
					self.speculate(&planner, &physical, &footprint, op)
				})
				.await;
				let nothing = modelled.as_deref().is_some_and(does_nothing);
				if let Some(later) = &mut self.later_schema {
					later.push((footprint.clone(), nothing && guarded_by_existence(op)));
				}
				if nothing {
					#[cfg(test)]
					super::speculate::FOLDED.with(|count| count.set(count.get() + 1));
					return Ok(Control::Value(LogicalExpr::Literal(
						crate::expr::logical::LogicalLiteral::None,
					)));
				}
				(footprint, modelled)
			}
			other => (self.seen(other, op), None),
		};
		if modelled.is_none() {
			self.written.absorb(footprint);
			if changes_catalog {
				self.changed = true;
			}
		}
		Ok(Control::Statement {
			plan: Box::new(physical),
			source,
			changes_catalog,
			planned_over: over.map(std::sync::Arc::new),
			modelled,
			analysis,
			late_bound: planner.is_late_bound(),
			within,
			raises_when_reached,
		})
	}

	/// What statement `own`, which is no schema statement, leaves for the
	/// schema statements after it, as [`Seen`] says: its writes to records,
	/// with the tables it may define by writing to them noted in
	/// [`Self::implicit`]. One that may change the catalog through code it
	/// reaches — an event, a function body, a value invoked — or that writes a
	/// definition other than its tables' leaves them unknown.
	fn seen(&mut self, seen: Seen, own: &LogicalOp) -> crate::exec::logical::footprint::Footprint {
		use crate::exec::logical::footprint::{CatalogKind, Resource};
		let (mut footprint, direct) = match seen {
			Seen::Writes {
				footprint,
				direct,
				unnamed,
			} => {
				self.implicit_any |= unnamed;
				(footprint, direct)
			}
			Seen::Unseen(footprint) | Seen::Schema(footprint) => {
				self.transparent = false;
				return footprint;
			}
		};
		let mut defined = Vec::new();
		for written in &footprint.writes {
			match written {
				Resource::Catalog {
					kind: CatalogKind::Table,
					table: Some(table),
					name: None,
				} => defined.push(table.clone()),
				Resource::Catalog {
					..
				}
				| Resource::Unknown => {
					self.transparent = false;
					if self.late_after {
						self.changed = true;
					}
				}
				Resource::Data {
					..
				} => {}
			}
		}
		for table in defined {
			let through_own = direct && defines_through_own_write(own, &table);
			match self.implicit.iter_mut().find(|(implicit, _)| *implicit == table) {
				Some((_, shared)) => *shared &= through_own,
				None => self.implicit.push((table, through_own)),
			}
		}
		footprint.writes.retain(|written| {
			!matches!(
				written,
				Resource::Catalog {
					kind: CatalogKind::Table,
					table: Some(_),
					name: None,
				}
			)
		});
		footprint
	}

	/// Run schema statement `physical` speculatively over the catalog the
	/// block's schema statements before it leave, and when the statements
	/// after it can be planned knowing its effect, plan them against it: its
	/// speculative run, which its own run's draws take the ids of.
	///
	/// A schema statement's effect is known when it runs once for each run of
	/// the block, in the order the block holds it, after statements that change
	/// the catalog only as the program knows, over tables the catalog it is run
	/// against holds as its real run finds them ([`Self::undefined_tables`]),
	/// and its speculative run ends. One whose effect is not known leaves the
	/// catalog the statements after it are planned against as it was, and a
	/// block reading what it changes is declined whole (`lower_block`).
	///
	/// What earlier statements wrote to records does not change what it
	/// defines: its speculative run reads the catalog, and the rows it reads —
	/// an index's build, a view's backfill — it leaves to its real run
	/// (`store::speculative_skip`), which reads them as the block leaves them.
	async fn speculate(
		&mut self,
		planner: &Planner<'_>,
		physical: &PhysicalPlan,
		footprint: &crate::exec::logical::footprint::Footprint,
		op: &LogicalOp,
	) -> Option<std::sync::Arc<super::speculate::Speculated>> {
		let overlay = self.overlay.as_ref()?;
		if !self.straight || !self.transparent {
			return None;
		}
		let Ok(shared) = crate::exec::vm::host::out_of_line(|| {
			self.undefined_tables(planner, overlay, footprint, op)
		})
		.await
		else {
			self.transparent = false;
			return None;
		};
		match super::speculate::run(planner, physical, overlay).await {
			Some(super::speculate::Outcome::Ran(mut run)) => {
				if let Some(table) = &shared {
					self.implicit.retain(|(implicit, _)| implicit != table);
				}
				run.shared = shared;
				self.overlay = Some(run.overlay.clone());
				Some(std::sync::Arc::new(*run))
			}
			Some(super::speculate::Outcome::Raised {
				refused,
			}) => {
				self.transparent = false;
				// Its run raises the refusal, which ends the block: nothing
				// after it runs.
				self.dead |= refused;
				None
			}
			None => {
				self.transparent = false;
				None
			}
		}
	}
}

impl Blocks {
	/// Whether schema statement `op`, of footprint `footprint`, is over tables
	/// the catalog `overlay` leaves holds as its real run finds them, although
	/// a write before it may define a table the overlay does not hold: `Ok(None)`
	/// when it is over no such table, `Ok(Some(table))` when it is over one
	/// such table, which it defines where there is none
	/// ([`crate::exec::logical::footprint::defines_on_demand`]) and which every
	/// write before it defines through its own subjects — its definition then
	/// takes the reservations of the statement's, whichever of the two defines
	/// it first ([`super::program::ImplicitTable`]) — and `Err(())` otherwise.
	async fn undefined_tables(
		&self,
		planner: &Planner<'_>,
		overlay: &crate::kvs::speculation::Overlay,
		footprint: &crate::exec::logical::footprint::Footprint,
		op: &LogicalOp,
	) -> Result<Option<surrealdb_strand::TableName>, ()> {
		use crate::exec::logical::footprint::{CatalogKind, Resource};
		let touched = || footprint.reads.iter().chain(&footprint.writes);
		// After a write to a table it evaluates, any table the overlay does
		// not hold may have been defined.
		if self.implicit_any {
			for resource in touched() {
				let table = match resource {
					Resource::Unknown => return Err(()),
					Resource::Catalog {
						kind:
							CatalogKind::Table
							| CatalogKind::Field
							| CatalogKind::Index
							| CatalogKind::Event
							| CatalogKind::View,
						table,
						..
					} => table.as_ref().ok_or(())?,
					_ => continue,
				};
				if !super::speculate::defines_table(planner, overlay, table).await {
					return Err(());
				}
			}
		}
		let mut over = self.implicit.iter().filter(|(table, _)| {
			let defined = Resource::table_catalog(table.clone());
			touched().any(|resource| defined.overlaps(resource))
		});
		let (first, rest) = (over.next(), over.next());
		match (first, rest) {
			(None, _) => Ok(None),
			(Some((table, true)), None)
				if matches!(op, LogicalOp::Ddl(ddl)
					if crate::exec::logical::footprint::defines_on_demand(&ddl.statement).as_ref()
						== Some(table)) =>
			{
				Ok(Some(table.clone()))
			}
			_ => Err(()),
		}
	}

	/// Map loop body `body` twice when its schema statements change the
	/// catalog on the first iteration and do nothing after it — `DEFINE … IF
	/// NOT EXISTS` — answering the body the first element runs, planned
	/// against the catalog as the block reaches the loop, and the body every
	/// later element runs, planned against the catalog the first leaves, its
	/// schema statements folded away. `None`, the mapping undone, when the
	/// loop is not such a loop.
	///
	/// The first iteration runs once for each run of the block that reaches
	/// the loop with an element, in order, so its schema statements are
	/// modelled as the block's own are. A later iteration's are known to do
	/// nothing only while nothing between them changes what they read: the
	/// later body changes the catalog only as the program knows, and writes
	/// nothing they read.
	async fn peel(
		&mut self,
		planner: &Planner<'_>,
		body: &LogicalOp,
	) -> Mapped<Option<(Control, Control)>> {
		use crate::exec::logical::edges::{Edges, data};
		if !self.straight
			|| !self.transparent
			|| self.overlay.is_none()
			|| !body.any(|op| matches!(op, LogicalOp::Ddl(_)))
		{
			return Ok(None);
		}
		let before = (
			self.overlay.clone(),
			self.transparent,
			self.written.clone(),
			self.dead,
			self.sources.len(),
			self.implicit.clone(),
			self.implicit_any,
		);
		#[cfg(test)]
		let folded = super::speculate::FOLDED.with(std::cell::Cell::get);
		// The catalog ids the peeled mappings' speculative runs hold, released
		// when the mapping is undone, since no program reserves them.
		let held = planner.speculation.as_ref().map(|speculating| {
			let mark = speculating.held.mark();
			(std::sync::Arc::clone(&speculating.held), mark)
		});
		let undo = |this: &mut Self| {
			if let Some((held, mark)) = &held {
				held.release_since(*mark);
			}
			this.overlay.clone_from(&before.0);
			this.transparent = before.1;
			this.written = before.2.clone();
			this.dead = before.3;
			this.sources.truncate(before.4);
			this.implicit.clone_from(&before.5);
			this.implicit_any = before.6;
			this.later_schema = None;
			#[cfg(test)]
			super::speculate::FOLDED.with(|count| count.set(folded));
		};
		let first = crate::exec::vm::host::out_of_line(|| self.control(planner, body)).await?;
		let after_first = self.overlay.clone();
		if self.dead || !self.transparent || after_first == before.0 {
			undo(self);
			return Ok(None);
		}
		let written_first = std::mem::take(&mut self.written);
		self.later_schema = Some(Vec::new());
		let rest = crate::exec::vm::host::out_of_line(|| self.control(planner, body)).await?;
		let later = self.later_schema.take().unwrap_or_default();
		let rest_written = std::mem::take(&mut self.written);
		// A later schema statement that did nothing stays settled only when the
		// rest of the iteration's writes have no `FLOW` or `OUTPUT` edge into it.
		let steady = !self.dead
			&& self.transparent
			&& self.overlay == after_first
			&& !later.is_empty()
			&& later.iter().all(|(footprint, nothing)| {
				*nothing
					&& !data(&rest_written, footprint).intersects(Edges::FLOW.union(Edges::OUTPUT))
			});
		if !steady {
			undo(self);
			return Ok(None);
		}
		self.written = written_first;
		self.written.absorb(rest_written);
		Ok(Some((first, rest)))
	}
}

/// Whether a schema statement's speculative run did nothing a run of it
/// could be told from: it wrote nothing, left no effect for the real run,
/// leased no id, and answered `NONE` — `IF NOT EXISTS` over a definition that
/// exists, `IF EXISTS` over one that does not. Its caller's permission to
/// run it held, since the run passed its checks.
fn does_nothing(run: &super::speculate::Speculated) -> bool {
	!run.changed && !run.skipped && run.draws.is_empty() && run.value.is_none()
}

/// Whether `op` is a schema statement guarded by the existence of what it
/// names — `DEFINE … IF NOT EXISTS`, `REMOVE … IF EXISTS` — which does nothing
/// on any iteration after one that leaves its definition where it is. Any
/// other schema statement may write something new on each iteration: its
/// clauses may read the loop's element, which a speculative run sees only one
/// value of.
fn guarded_by_existence(op: &LogicalOp) -> bool {
	use crate::expr::logical::LogicalDdlStatement;
	use crate::expr::statements::define::DefineKind;
	use crate::expr::statements::{DefineStatement as D, RemoveStatement as R};
	let LogicalOp::Ddl(ddl) = op else {
		return false;
	};
	match &ddl.statement {
		LogicalDdlStatement::Define(define) => {
			let kind = match define.as_ref() {
				D::Namespace(s) => &s.kind,
				D::Database(s) => &s.kind,
				D::Function(s) => &s.kind,
				D::Analyzer(s) => &s.kind,
				D::Param(s) => &s.kind,
				D::Table(s) => &s.kind,
				D::Event(s) => &s.kind,
				D::Field(s) => &s.kind,
				D::Index(s) => &s.kind,
				D::User(s) => &s.kind,
				D::Model(s) => &s.kind,
				D::Access(s) => &s.kind,
				D::Config(s) => &s.kind,
				D::Api(s) => &s.kind,
				D::Bucket(s) => &s.kind,
				D::Sequence(s) => &s.kind,
				D::Module(s) => &s.kind,
			};
			matches!(kind, DefineKind::IfNotExists)
		}
		LogicalDdlStatement::Remove(remove) => match remove.as_ref() {
			R::Namespace(s) => s.if_exists,
			R::Database(s) => s.if_exists,
			R::Function(s) => s.if_exists,
			R::Analyzer(s) => s.if_exists,
			R::Access(s) => s.if_exists,
			R::Param(s) => s.if_exists,
			R::Table(s) => s.if_exists,
			R::Event(s) => s.if_exists,
			R::Field(s) => s.if_exists,
			R::Index(s) => s.if_exists,
			R::User(s) => s.if_exists,
			R::Model(s) => s.if_exists,
			R::Api(s) => s.if_exists,
			R::Bucket(s) => s.if_exists,
			R::Sequence(s) => s.if_exists,
			R::Module(s) => s.if_exists,
			R::Config(s) => s.if_exists,
		},
		LogicalDdlStatement::Alter(_) | LogicalDdlStatement::Rebuild(_) => false,
	}
}

/// What `EXPLAIN ANALYZE` of the statement mapped to `physical` renders, as
/// `planner` plans it: the plan the VM explains for it, every operator of
/// which reports the one set of metrics the statement's run records.
fn analysis_of(
	physical: &PhysicalPlan,
	planner: &Planner<'_>,
	format: crate::expr::ExplainFormat,
) -> Mapped<super::program::Analysis> {
	let analyzed = super::explain::to_analysis_operator(physical, planner.reads_are_checked());
	let shared = || {
		NotCovered::internal(
			"EXPLAIN ANALYZE of a plan whose operators report metrics of their own",
		)
	};
	let pieces =
		crate::exec::operators::analyze_text_pieces(analyzed.as_ref()).ok_or_else(shared)?;
	let json = match format {
		crate::expr::ExplainFormat::Text => None,
		crate::expr::ExplainFormat::Json => {
			let template = crate::exec::operators::analyze_json_template(analyzed.as_ref())
				.ok_or_else(shared)?;
			Some(super::plain::ValueData::new(
				crate::val::Value::Object(template),
				"an analysed plan",
			)?)
		}
	};
	Ok(super::program::Analysis {
		pieces,
		json,
		redact: planner.frozen().redact_volatile_explain_attrs(),
	})
}

/// How a block statement bound and mapped.
enum BoundAndMapped {
	/// Its plan, and what it reads and writes.
	Mapped(Box<PhysicalPlan>, Seen),
	/// It does not bind or map now, or can be compiled only when the block
	/// reaches it, for the reason given: the whole statement holding the
	/// block is not covered.
	Declined(NotCovered),
	/// The operator tree refuses it as it plans it.
	Refused(super::plan::Refusal),
}

/// What a block statement reads and writes, as the schema statements after it
/// are judged against it.
enum Seen {
	/// A schema statement's footprint, its reads included.
	Schema(crate::exec::logical::footprint::Footprint),
	/// What a statement writes that changes the catalog, if at all, only by
	/// defining the tables it writes to (`vm::speculate::seen_writes`);
	/// whether it reaches no code its footprint could not name (`direct`), and
	/// whether code it reaches writes to a table it evaluates (`unnamed`).
	Writes {
		footprint: crate::exec::logical::footprint::Footprint,
		direct: bool,
		unnamed: bool,
	},
	/// The footprint of a statement that may change the catalog through code
	/// it reaches.
	Unseen(crate::exec::logical::footprint::Footprint),
}

/// How statement `op`, which `bound` names are bound around, reaches what code
/// it runs may redefine while it runs ([`CatalogWithin`]).
///
/// The code is what the plan cannot see into: an embedded script, a closure
/// invoked through a value that may come from outside the block, a function
/// body that reaches either or holds a schema statement, an event or field
/// clause of a table it writes that does, and a schema statement held in one
/// of its expressions. A statement that reaches none of it plans as it is.
/// A schema statement is not judged here. A block is judged whole, for what
/// it binds; each statement it runs is judged again for itself as the block
/// maps it.
pub(crate) async fn catalog_within(
	planner: &Planner<'_>,
	op: &LogicalOp,
	bound: &[String],
) -> CatalogWithin {
	if planner.is_late_bound()
		|| planner.txn().is_none()
		|| matches!(op, LogicalOp::Ddl(_) | LogicalOp::Explain(_))
	{
		return CatalogWithin::default();
	}
	// A block's own schema statements are the block's to plan around.
	let block = matches!(op, LogicalOp::Sequence(_) | LogicalOp::Foreach(_));
	let holds_schema = !block && crate::exec::planner::fold::changes_the_catalog(op);
	let may_redefine = holds_schema || {
		let outside = super::speculate::closures_from_outside(planner, [op], bound);
		super::speculate::runs_unseen_schema(planner, op, outside).await
	};
	if !may_redefine {
		return CatalogWithin::default();
	}
	// A write, and a statement holding a write or a schema statement, plan
	// the statements in their expressions as the catalog stands when each
	// runs.
	let writes = matches!(op, LogicalOp::Write(_))
		|| holds_schema
		|| op
			.visit_ops_deep(&mut |node| match node {
				LogicalOp::Write(_) => std::ops::ControlFlow::Break(()),
				_ => std::ops::ControlFlow::Continue(()),
			})
			.is_break();
	CatalogWithin {
		calls_current: true,
		nested_late: writes,
	}
}

/// Whether `written` is a catalog definition other than a table's own, which
/// writing a record may define, or a write to anything at all.
fn changes_other_than_a_table(written: &crate::exec::logical::footprint::Resource) -> bool {
	use crate::exec::logical::footprint::{CatalogKind, Resource};
	match written {
		Resource::Unknown => true,
		Resource::Catalog {
			kind: CatalogKind::Table,
			table: Some(_),
			name: None,
		} => false,
		Resource::Catalog {
			..
		} => true,
		Resource::Data {
			..
		} => false,
	}
}

/// What block statement `op`, which `bound` names are bound around, reads and
/// writes as `planner` reads the catalog.
async fn seen(planner: &Planner<'_>, op: &LogicalOp, bound: &[String]) -> Seen {
	use crate::exec::logical::footprint::Resource;
	// What the statement reads is asked after only of a schema statement,
	// whose outcome may depend on it.
	if crate::exec::planner::fold::changes_the_catalog(op) {
		// Only a schema statement of the block itself is run speculatively; one
		// inside another statement runs when that statement reaches it.
		return match op {
			LogicalOp::Ddl(_) => Seen::Schema(super::speculate::footprint(planner, op, true).await),
			_ => Seen::Unseen(super::speculate::footprint(planner, op, true).await),
		};
	}
	let footprint = super::speculate::footprint(planner, op, false).await;
	let direct = !footprint.writes.contains(&Resource::Unknown);
	if direct && !footprint.writes_catalog() {
		return Seen::Writes {
			footprint,
			direct,
			unnamed: false,
		};
	}
	let outside = super::speculate::closures_from_outside(planner, [op], bound);
	match super::speculate::seen_writes(planner, op, &footprint, outside).await {
		Some((footprint, unnamed)) => Seen::Writes {
			footprint,
			direct,
			unnamed,
		},
		None => Seen::Unseen(footprint),
	}
}

/// Bind and map block statement `op` as `planner` plans the block's
/// statements, `bound` the names bound around it.
///
/// `reached` is whether the operator tree binds the statement when the block
/// reaches it rather than when it plans the block: one whose binding folds the
/// clock then folds it as it stands at that reach, which a binding made when
/// the query arrives cannot, so such a statement is not mapped. A membership
/// the logical rules lifted into a semi or anti join is mapped as the
/// predicate it was lifted from ([`memberships_of`]).
async fn bind_and_map(
	planner: &Planner<'_>,
	op: &LogicalOp,
	bound: &[String],
	reached: bool,
) -> BoundAndMapped {
	let bound_op = match bind_for_map(planner, op, reached).await {
		Ok(bound_op) => bound_op,
		Err(unmapped) => return unmapped,
	};
	let when_reached = reached.then(|| planner.compiled_when_reached());
	let planner = when_reached.as_ref().unwrap_or(planner);
	match crate::exec::vm::host::out_of_line(|| map_as_bound(planner, &bound_op)).await {
		Ok(PhysicalPlan {
			catalog: None,
			root: Plan::Refused {
				refusal,
			},
			..
		}) => BoundAndMapped::Refused(refusal),
		Ok(physical) => BoundAndMapped::Mapped(Box::new(physical), seen(planner, op, bound).await),
		Err(why) => BoundAndMapped::Declined(why),
	}
}

/// The binding half of [`bind_and_map`]: `op` bound with `planner`, and a
/// membership the logical rules lifted into a semi or anti join put back as
/// the predicate it was lifted from ([`memberships_of`]). The error is the
/// [`BoundAndMapped`] that declines or refuses the statement, never
/// [`BoundAndMapped::Mapped`].
async fn bind_for_map(
	planner: &Planner<'_>,
	op: &LogicalOp,
	reached: bool,
) -> Result<LogicalOp, BoundAndMapped> {
	if reached
		&& !planner.is_late_bound()
		&& crate::exec::vm::host::out_of_line(|| binds_the_clock(op, planner)).await
	{
		return Err(BoundAndMapped::Declined(NotCovered::new(
			"a block statement after a schema statement whose binding reads the clock",
		)));
	}
	let when_reached = reached.then(|| planner.compiled_when_reached());
	let planner = when_reached.as_ref().unwrap_or(planner);
	let bound_op = match crate::exec::vm::host::out_of_line(|| {
		crate::exec::planner::bind_statement(planner, op.clone())
	})
	.await
	{
		Ok(bound_op) => bound_op,
		// The operator tree refuses a statement whose binding fails with the
		// error the binding raises.
		Err(e) => {
			return Err(match super::plan::Refusal::of(&e) {
				Some(refusal) => BoundAndMapped::Refused(refusal),
				None => BoundAndMapped::Declined(NotCovered::new(format!(
					"a block statement that does not bind: {e}"
				))),
			});
		}
	};
	match memberships_of(&bound_op) {
		Ok(recorrelated) => Ok(recorrelated.into_owned()),
		Err(why) => Err(BoundAndMapped::Declined(why)),
	}
}

/// Whether binding `op` with `planner` binds it alike every time, the catalog
/// aside: every parameter it reads is one the planner holds as bound around it
/// or one a row binds, and it calls no builtin that reads the clock, which a
/// condition folds when bound.
fn binds_alike(op: &LogicalOp, planner: &Planner<'_>) -> bool {
	binds_alike_as(op, planner, false, false, false)
}

/// [`binds_alike`] for a statement whose parameters are bound once for every
/// evaluation the program makes of it, as a closure body's are: a parameter
/// binds alike, and so does a call of a user-defined function, which binds its
/// own body; only a call that reads the clock binds otherwise.
pub(super) fn binds_alike_recorded(op: &LogicalOp, planner: &Planner<'_>) -> bool {
	binds_alike_as(op, planner, true, true, false)
}

/// [`binds_alike_recorded`] for a closure body, bound once for every
/// invocation of the unit it is lowered to: an index function binds alike,
/// and so does a call that reads the clock where `planner` folds none
/// ([`Planner::folds_clock`]).
pub(super) fn binds_alike_closure(op: &LogicalOp, planner: &Planner<'_>) -> bool {
	binds_alike_as(op, planner, true, true, true)
}

/// Whether binding `op` with `planner` folds a call that reads the clock, in
/// `op` or in the body of a user-defined function it reaches: the operator
/// tree binds a block statement after the block's first schema statement when
/// the block reaches it, so such a statement folds the clock as it stands
/// then. A body whose definition cannot be read is taken to fold it.
pub(super) async fn binds_the_clock(op: &LogicalOp, planner: &Planner<'_>) -> bool {
	if !binds_alike_recorded(op, planner) {
		return true;
	}
	let mut todo = custom_calls(op);
	if todo.is_empty() {
		return false;
	}
	let (Some(txn), Some((ns, db))) =
		(planner.txn().cloned(), crate::exec::vm::host::out_of_line(|| planner.ns_db_ids()).await)
	else {
		return true;
	};
	let cache = planner.frozen().get_cache();
	let mut seen: Vec<String> = Vec::new();
	while let Some(name) = todo.pop() {
		if seen.contains(&name) {
			continue;
		}
		seen.push(name.clone());
		let snapshot = match crate::kvs::cache::ds::FunctionSnapshot::for_function(
			cache.as_deref(),
			&txn,
			ns,
			db,
			&name,
		)
		.await
		{
			Ok(Some(snapshot)) => snapshot,
			_ => return true,
		};
		let Ok(body) = snapshot.body() else {
			return true;
		};
		let args = snapshot.definition().args.iter().map(|(arg, _)| arg.as_str().to_owned());
		if !binds_alike_recorded(body, &planner.shadowing(args)) {
			return true;
		}
		todo.extend(custom_calls(body));
	}
	false
}

/// The user-defined functions `op` calls, without their `fn::` prefix.
fn custom_calls(op: &LogicalOp) -> Vec<String> {
	let mut names = Vec::new();
	let _ = op.visit_ops_deep(&mut |node| {
		node.for_each_scalar(&mut |scalar| {
			let _ = scalar.visit_deep(&mut |expr| {
				if let LogicalExpr::Call(call) = expr
					&& let crate::expr::Function::Custom(name) = &call.receiver
				{
					names.push(name.clone());
				}
				std::ops::ControlFlow::<()>::Continue(())
			});
		});
		std::ops::ControlFlow::<()>::Continue(())
	});
	names
}

/// [`binds_alike`] with the forms `recorded`, `calls_alike` and `closure`
/// admit: under `recorded` a parameter binds alike, under `calls_alike` a call
/// of a user-defined function does, and under `closure` what
/// [`binds_alike_closure`] admits.
fn binds_alike_as(
	op: &LogicalOp,
	planner: &Planner<'_>,
	recorded: bool,
	calls_alike: bool,
	closure: bool,
) -> bool {
	use crate::expr::logical::EffectSet;
	let registry = planner.function_registry();
	let varies = |expr: &LogicalExpr| {
		expr.any(|node| match node {
			LogicalExpr::Param(param) => {
				!recorded
					&& !planner.param_is_shadowed(param.as_str())
					&& !crate::expr::logical::ITERATION_PARAMS.contains(&param.as_str())
			}
			LogicalExpr::Call(call) => match &call.receiver {
				crate::expr::Function::Normal(name) => match registry.get(name) {
					// A call reading the clock binds otherwise where binding
					// folds the clock.
					Some(function) => {
						(!closure || planner.folds_clock())
							&& function.effects().contains(EffectSet::CLOCK)
					}
					// A projection function evaluates against the row, and
					// binding folds none; nor an index function, which a
					// closure body's unit answers from the operator its scope
					// holds when it runs.
					None => {
						let unfolded = (recorded && registry.is_projection(name))
							|| (closure && registry.is_index_function(name));
						!unfolded
					}
				},
				crate::expr::Function::Custom(_) => !calls_alike,
				// A script, a model or a module function is resolved by name
				// and runs when it is called: a closure body binds it alike.
				_ => !closure,
			},
			_ => false,
		})
	};
	let mut alike = true;
	let _ = op.visit_ops_deep(&mut |node| {
		node.for_each_scalar(&mut |scalar| {
			if varies(scalar) {
				alike = false;
			}
		});
		if alike {
			std::ops::ControlFlow::Continue(())
		} else {
			std::ops::ControlFlow::Break(())
		}
	});
	alike
}

/// Whether block statement `op` may define table `table` only through its own
/// subjects: it is a write naming `table` among its targets and holding no
/// statement nested in it, so the table is defined where the write resolves
/// its first record of it, by the program's write (`vm::write`).
fn defines_through_own_write(op: &LogicalOp, table: &surrealdb_strand::TableName) -> bool {
	let LogicalOp::Write(write) = op else {
		return false;
	};
	let names = crate::exec::logical::footprint::write_targets(&write.source)
		.is_some_and(|targets| targets.contains(table));
	let mut statements = 0usize;
	let _ = op.visit_ops_deep(&mut |node| {
		if matches!(node, LogicalOp::Write(_) | LogicalOp::Ddl(_)) {
			statements += 1;
		}
		std::ops::ControlFlow::<()>::Continue(())
	});
	names && statements == 1
}

/// Whether `expr` holds a statement, a closure, or a call of a user-defined
/// function: code whose compilation reads the catalog.
fn holds_a_body(expr: &LogicalExpr) -> bool {
	expr.any(|node| match node {
		LogicalExpr::Subquery(_) | LogicalExpr::Closure(_) => true,
		LogicalExpr::Call(call) => matches!(call.receiver, crate::expr::Function::Custom(_)),
		_ => false,
	})
}
