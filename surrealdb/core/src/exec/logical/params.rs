//! Parameter resolution over the logical plan.
//!
//! Replaces `$param` references whose value is fixed for the whole statement
//! with their literal form, so later passes and the physical planner see a
//! literal where index analysis, constant folding and source classification
//! need one.
//!
//! Two clause positions are rewritten: a predicate (a mutation's `WHERE`
//! included), and a `FROM` source. The query of a `MATCHES` in a statement's
//! own projection is rewritten too, so it names the full-text clause its
//! `WHERE` registers. Every other clause — a mutation's data clause among
//! them — keeps its parameters and resolves them per row. What is
//! substituted in those two positions, and what is deliberately left for
//! runtime:
//!
//! - **Substituted:** `LET` bindings, client bind parameters and session values from the context,
//!   and `DEFINE PARAM`s readable with `Permission::Full`. `bind_param` is the single rule, shared
//!   with the physical planner.
//! - **Unbound names:** a name with no value in the context and no `DEFINE PARAM` in the database
//!   evaluates to `NONE`, and is substituted as `NONE`: a `FROM $p` no binding reaches is the
//!   no-row source a literal `NONE` is, and a predicate on the name folds. The catalog is consulted
//!   only with a transaction and a database; without them the name stays a runtime lookup.
//! - **Row-scoped names** (`BindContext::row_scoped`): bound per row at execution, so their
//!   plan-time value, if any, is the wrong one.
//! - **Names the plan binds itself:** a `LET` in a block or a `FOR` loop variable. Each is a
//!   runtime lookup wherever it appears, and a context value under the same name never wins.
//! - **Names the plan defines or removes:** a `DEFINE PARAM` or `REMOVE PARAM` in a block changes
//!   what the name reads for the statements after it, so the name is a runtime lookup wherever it
//!   appears in the plan.
//! - **Closure bodies:** not entered. A closure is invoked by later statements, so every parameter
//!   its body reads — its own arguments included — is resolved where it is invoked, from the values
//!   captured when the closure was created and the arguments of the call.
//! - **Layer columns:** a `Project`'s fields are neither a predicate nor a source, and a layer
//!   column's body comes from a schema definition rather than from the statement, so its parameters
//!   are bound where the definition is evaluated. A layer binds `$value` for its columns, so a plan
//!   with a layer treats `$value` as a name it binds itself, in the plans nested in a column's body
//!   included.
//! - **Lossy values:** a value whose literal form evaluates to something else
//!   (`substitution_is_lossy`) stays a parameter.
//! - **Steps over a substituted value:** a field, an index or an end read off a substituted
//!   parameter (`$rows.id`, `$rows[0].id`) is replaced by the value the steps read off the literal
//!   (`literal_step_value`), so an index operand is the value a binding holds and not a path to it.
//!   A chain whose steps would read a record (a record id anywhere in what a field step reads) or
//!   whose index is not a literal keeps its steps, which run per row.
//! - **KNN query vectors:** where a name holding an array of numbers is the query vector of a KNN
//!   operator (`<|k, …|>`), it stays a parameter there, and only there. The operator is evaluated
//!   as membership of the row's id in its source's result, never from its operands, and the
//!   physical planner resolves the vector once for the source; as a literal it would be a node per
//!   component for every later pass to walk.
//! - **Record id lists:** where a name holding a non-empty array of record ids is a whole `FROM`
//!   source (`FROM $ids`), the source becomes the `Values` leaf its literal would, with the
//!   parameter as its expression, read when the statement runs. The leaf reads each id's record
//!   whether its expression is the parameter or the literal, and every pass sees the leaf kind it
//!   sees for the literal; as a literal it would be a node per id for every later pass to walk, and
//!   a plan that depends on the ids themselves. The subjects of a write (`UPDATE $ids`) and a
//!   locked read take the literal: the write's table and the locked keys are settled when the plan
//!   is.
//!
//! A scalar subquery is part of the statement — it runs in the same
//! transaction with the same context — so its plan is resolved with the
//! enclosing one; a block body is an expression rather than a predicate or a
//! source and is not touched. A `Source` whose expression becomes a value
//! literal is reclassified as `Values`, so the leaf kinds a bound plan exposes
//! match the kinds the planner gives a literal written in
//! the statement.

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::ops::ControlFlow;

use super::{
	BindContext, Descend, classify_source, visit_binding, visit_binding_mut, visit_binding_pruning,
	visit_binding_pruning_mut, visit_deep,
};
use crate::exec::planner::util::{
	ParamBinding, bind_param, literal_step_value, substitution_is_lossy, try_value_to_logical_expr,
	value_to_logical_expr,
};
use crate::expr::logical::{
	LogicalDdl, LogicalDdlStatement, LogicalEval, LogicalExpr, LogicalFilter, LogicalForeach,
	LogicalJoin, LogicalLet, LogicalOp, LogicalPermissionFilter, LogicalSource, LogicalValues,
	LogicalWrite, LogicalWriteSource, SourceRef,
};
use crate::expr::operator::BinaryOperator;
use crate::val::Value;

/// Resolve every statement-scoped parameter in `op`. Names that cannot or
/// must not be resolved are left in place.
pub(super) async fn resolve_params(op: LogicalOp, bx: &BindContext<'_>) -> LogicalOp {
	// Phase 1: the names the plan references in a resolvable position, and
	// the names it binds itself.
	let ReferencedNames {
		reads:
			Reads {
				names,
				outside_query_vectors,
				outside_sources,
				write_subjects,
			},
		bound,
	} = referenced_and_bound(&op);
	// The names a table permission predicate reads: a privileged frame holds
	// none of the caller's bindings, so those are looked up in the isolated
	// view rather than the caller's.
	let guarded = permission_names(&op, &bound);

	// Phase 2: look each name up once.
	let mut subs = Substitutions::default();
	for name in names {
		if bound.contains(&name) {
			continue;
		}
		let Some(value) = resolved_value(&name, bx, bx.ctx).await else {
			continue;
		};
		if is_query_vector(&value) {
			subs.query_vectors.insert(name.clone());
			// Read only as a query vector, it stays a parameter everywhere:
			// no literal is built for it.
			if !outside_query_vectors.contains(&name) {
				continue;
			}
		}
		if is_record_id_list(&value) {
			subs.record_id_lists.insert(name.clone());
			// Read only as a whole source, it stays a parameter everywhere. The subjects of a
			// write take the literal, so a name that is one keeps it beside its other reads.
			if !outside_sources.contains(&name) && !write_subjects.contains(&name) {
				continue;
			}
		}
		subs.literals.insert(name, value_to_logical_expr(value));
	}
	// A permission predicate is definer code, compiled apart from the
	// statement: every name it reads is substituted, the query vector of a KNN
	// operator included.
	let mut privileged = Substitutions::default();
	if !guarded.is_empty() {
		let isolated = crate::ctx::Context::new_isolated(bx.ctx).freeze();
		for name in guarded {
			if let Some(value) = resolved_value(&name, bx, &isolated).await {
				privileged.literals.insert(name, value_to_logical_expr(value));
			}
		}
	}
	if subs.literals.is_empty() && subs.record_id_lists.is_empty() && privileged.literals.is_empty()
	{
		return op;
	}

	// Phase 3: substitute, then re-derive each leaf's kind from its new
	// expression.
	substitute_all(op, &subs, &privileged)
}

/// The literals [`resolve_params`] substitutes, and the names among them it
/// leaves in place as the query vector of a KNN operator or as a whole `FROM`
/// source.
#[derive(Default)]
struct Substitutions {
	literals: HashMap<String, LogicalExpr>,
	query_vectors: HashSet<String>,
	record_id_lists: HashSet<String>,
}

/// Whether `value` can be a KNN query vector: a non-empty array of numbers.
fn is_query_vector(value: &Value) -> bool {
	matches!(value, Value::Array(a) if !a.is_empty() && a.iter().all(|v| matches!(v, Value::Number(_))))
}

/// Whether `value` is a list of records to read: a non-empty array of record ids, none a range.
fn is_record_id_list(value: &Value) -> bool {
	matches!(value, Value::Array(a) if !a.is_empty()
		&& a.iter().all(|v| matches!(v, Value::RecordId(rid) if !rid.key.is_range())))
}

/// The value `name` takes in the context `ctx`, or `None` where the name is
/// read when the statement runs or its literal form would evaluate to
/// something else.
async fn resolved_value(
	name: &str,
	bx: &BindContext<'_>,
	ctx: &crate::ctx::FrozenContext,
) -> Option<Value> {
	// A statement that inlines no catalog definition reads a `DEFINE PARAM`
	// when it runs: only the context's values are taken.
	let (txn, ns_db) = match bx.inlines_definitions {
		true => (bx.txn.as_ref(), bx.ns_db),
		false => (None, None),
	};
	let value = match bind_param(name, ctx, txn, ns_db, bx.row_scoped).await {
		ParamBinding::Value(value) => value,
		ParamBinding::Unbound => Value::None,
		ParamBinding::Runtime => return None,
	};
	if substitution_is_lossy(&value) {
		return None;
	}
	Some(value)
}

/// The names read by the predicate of a table permission in `op`, or of a
/// permission filter nested in it, and in the resolvable positions of every
/// plan such a predicate holds, that `bound` does not bind.
fn permission_names(op: &LogicalOp, bound: &HashSet<String>) -> HashSet<String> {
	let mut names = HashSet::new();
	visit_deep(op, &mut |node| {
		if let LogicalOp::PermissionFilter(LogicalPermissionFilter {
			predicate,
			..
		}) = node
		{
			collect_names(predicate, &mut names);
			let _ = visit_binding(predicate, &mut |expr| {
				if let LogicalExpr::Subquery(plan) = expr {
					names.extend(referenced_and_bound(plan).reads.names);
				}
				ControlFlow::<()>::Continue(())
			});
		}
	});
	names.retain(|name| !bound.contains(name));
	names
}

/// The names a plan references in a position [`resolve_params`] substitutes.
struct ReferencedNames {
	reads: Reads,
	/// The names the plan binds itself.
	bound: HashSet<String>,
}

/// The names read in a position [`resolve_params`] substitutes, by how they are read.
#[derive(Default)]
struct Reads {
	/// Every name read there.
	names: HashSet<String>,
	/// The names read there other than as the query vector of a KNN operator.
	outside_query_vectors: HashSet<String>,
	/// The names read there other than as a whole `FROM` source.
	outside_sources: HashSet<String>,
	/// The names read as a whole source of a write's subjects.
	write_subjects: HashSet<String>,
}

/// The names `op` references in a position [`resolve_params`] substitutes,
/// and the names it binds itself, in `op` and every plan nested in it.
fn referenced_and_bound(op: &LogicalOp) -> ReferencedNames {
	let mut reads = Reads::default();
	let mut bound: HashSet<String> = HashSet::new();
	visit_deep(op, &mut |node| {
		match node {
			LogicalOp::Let(LogicalLet {
				name,
				..
			}) => {
				bound.insert(name.clone());
			}
			LogicalOp::Foreach(LogicalForeach {
				param,
				..
			}) => {
				bound.insert(param.as_str().to_owned());
			}
			// A layer binds `$value` for each of its columns.
			LogicalOp::Project(_) => {
				if node.is_layer() {
					bound.insert("value".to_owned());
				}
			}
			LogicalOp::Filter(LogicalFilter {
				predicate,
				..
			})
			| LogicalOp::PermissionFilter(LogicalPermissionFilter {
				predicate,
				..
			}) => collect_resolvable_names(predicate, &mut reads),
			// A mutation's `WHERE` is a predicate: it chooses the records the
			// write reads, by the access path a read under it would take.
			LogicalOp::Write(LogicalWrite {
				cond,
				source,
				..
			}) => {
				if let Some(cond) = cond {
					collect_resolvable_names(cond, &mut reads);
				}
				if let LogicalWriteSource::Rows(rows) = source {
					source_params(rows, &mut reads.write_subjects);
				}
			}
			LogicalOp::Join(LogicalJoin {
				on,
				filter,
				..
			}) => {
				for (left, right) in on {
					collect_resolvable_names(left, &mut reads);
					collect_resolvable_names(right, &mut reads);
				}
				if let Some(filter) = filter {
					collect_resolvable_names(filter, &mut reads);
				}
			}
			LogicalOp::Source(LogicalSource {
				source,
				for_update,
				..
			}) => match source {
				SourceRef::Param(param) => {
					reads.names.insert(param.as_str().to_owned());
					reads.outside_query_vectors.insert(param.as_str().to_owned());
					// A locked read takes the literal ([`substitute_node`]).
					if *for_update {
						reads.outside_sources.insert(param.as_str().to_owned());
					}
				}
				SourceRef::RecordId(_) | SourceRef::Dynamic(_) => {
					let mut exprs = Vec::new();
					source.exprs(&mut exprs);
					for expr in exprs {
						collect_resolvable_names(expr, &mut reads);
					}
				}
				SourceRef::Table(_) | SourceRef::Mock(_) => {}
			},
			// Operators holding no predicate and no source expression.
			// Listed rather than matched with a wildcard: an operator added
			// later that carries either must be considered here, not silently
			// skipped.
			LogicalOp::Values(_)
			| LogicalOp::Subquery(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Split(_)
			| LogicalOp::Aggregate(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Sort(_)
			| LogicalOp::Window(_)
			| LogicalOp::Limit(_)
			| LogicalOp::Fetch(_)
			| LogicalOp::Timeout(_)
			| LogicalOp::VersionScope(_)
			| LogicalOp::Only(_)
			| LogicalOp::Match(_)
			| LogicalOp::Explain(_)
			| LogicalOp::Eval(_)
			| LogicalOp::If(_)
			| LogicalOp::Throw(_)
			| LogicalOp::Break(_)
			| LogicalOp::Continue(_)
			| LogicalOp::Sequence(_)
			| LogicalOp::Return(_)
			| LogicalOp::Info(_)
			| LogicalOp::Sleep(_) => {}
			// A `DEFINE PARAM` or `REMOVE PARAM` changes the value its name
			// reads for every statement after it in the plan, so that name is
			// read where it runs rather than at its bind-time value.
			LogicalOp::Ddl(LogicalDdl {
				statement,
			}) => {
				if let Some(name) = changed_param(statement) {
					bound.insert(name.to_owned());
				}
			}
		}
	});
	ReferencedNames {
		reads,
		bound,
	}
}

/// Adds the names `rows` reads as a whole source to `names`: its own, or those of the leaves of the
/// union it is. Every other plan reads no parameter as a whole source.
fn source_params(rows: &LogicalOp, names: &mut HashSet<String>) {
	if let LogicalOp::Source(LogicalSource {
		source: SourceRef::Param(param),
		..
	}) = rows
	{
		names.insert(param.as_str().to_owned());
	} else if let LogicalOp::UnionAll(union) = rows {
		for input in &union.inputs {
			source_params(input, names);
		}
	}
}

/// Replaces the `Values` leaf [`substitute_node`] made of each record id list parameter among the
/// subjects `rows` is, directly or as a leaf of the union it is, with the list's literal.
fn subject_literals(rows: &mut LogicalOp, subs: &Substitutions) {
	let literal = if let LogicalOp::Values(LogicalValues {
		expr: LogicalExpr::Param(param),
		for_update,
		..
	}) = rows
	{
		if subs.record_id_lists.contains(param.as_str()) {
			subs.literals
				.get(param.as_str())
				.map(|literal| classify_source(literal.clone(), None, None, *for_update))
		} else {
			None
		}
	} else if let LogicalOp::UnionAll(union) = rows {
		for input in &mut union.inputs {
			subject_literals(input, subs);
		}
		None
	} else {
		None
	};
	if let Some(literal) = literal {
		*rows = literal;
	}
}

/// The parameter a schema statement defines, redefines or removes.
pub(super) fn changed_param(statement: &LogicalDdlStatement) -> Option<&str> {
	use crate::expr::statements::{DefineStatement, RemoveStatement};
	match statement {
		LogicalDdlStatement::Define(define) => {
			if let DefineStatement::Param(param) = define.as_ref() {
				Some(param.name.as_str())
			} else {
				None
			}
		}
		LogicalDdlStatement::Remove(remove) => {
			if let RemoveStatement::Param(param) = remove.as_ref() {
				Some(param.name.as_str())
			} else {
				None
			}
		}
		LogicalDdlStatement::Alter(_) | LogicalDdlStatement::Rebuild(_) => None,
	}
}

/// `op` with `subs` substituted for the parameters in every resolvable
/// position, and `privileged` for those in a table permission predicate and
/// throughout every plan such a predicate holds, each leaf's kind re-derived
/// from its new expression. A plan inside a closure body is not reached.
fn substitute_all(op: LogicalOp, subs: &Substitutions, privileged: &Substitutions) -> LogicalOp {
	let Ok(op) = op.transform_up(&mut |mut node| {
		// A permission predicate is definer code: the plans it holds read the
		// values a privileged frame holds, as the predicate itself does.
		let nested = match matches!(node, LogicalOp::PermissionFilter(_)) {
			true => privileged,
			false => subs,
		};
		let Ok(()) = node.visit_scalars_mut(&mut |scalar| {
			visit_binding_mut(scalar, &mut |expr| {
				if let LogicalExpr::Subquery(plan) = expr {
					let taken = std::mem::replace(
						plan.as_mut(),
						LogicalOp::Eval(LogicalEval {
							expr: LogicalExpr::Break,
						}),
					);
					**plan = substitute_all(taken, nested, privileged);
				}
				Ok::<_, Infallible>(())
			})
		});
		Ok::<_, Infallible>(substitute_node(node, subs, privileged))
	});
	op
}

/// `node` with `subs` substituted for the parameters in its own resolvable
/// positions, and `privileged` for those of a table permission predicate; the
/// plans its scalars hold are [`substitute_all`]'s.
fn substitute_node(node: LogicalOp, subs: &Substitutions, privileged: &Substitutions) -> LogicalOp {
	match node {
		LogicalOp::Filter(LogicalFilter {
			input,
			mut predicate,
		}) => {
			substitute(&mut predicate, subs);
			LogicalOp::Filter(LogicalFilter {
				input,
				predicate,
			})
		}
		LogicalOp::PermissionFilter(LogicalPermissionFilter {
			input,
			mut predicate,
			auth,
			guard,
		}) => {
			substitute(&mut predicate, privileged);
			LogicalOp::PermissionFilter(LogicalPermissionFilter {
				input,
				predicate,
				auth,
				guard,
			})
		}
		LogicalOp::Write(mut write) => {
			if let Some(cond) = &mut write.cond {
				substitute(cond, subs);
			}
			// The subjects were substituted as a read's source is, ahead of this node: a write
			// of a record id list takes the literal, so its table is settled when the plan is.
			if let LogicalWriteSource::Rows(rows) = &mut write.source {
				subject_literals(rows, subs);
			}
			LogicalOp::Write(write)
		}
		// A `MATCHES` of the statement's own projection takes the query its `WHERE` takes, so
		// the two name one full-text clause.
		LogicalOp::Project(mut project) => {
			if !project.is_layer() {
				let mut exprs = Vec::new();
				project.fields.exprs_mut(&mut exprs);
				for expr in exprs {
					substitute_matches_queries(expr, subs);
				}
			}
			LogicalOp::Project(project)
		}
		LogicalOp::Join(mut join) => {
			for (left, right) in &mut join.on {
				substitute(left, subs);
				substitute(right, subs);
			}
			if let Some(filter) = &mut join.filter {
				substitute(filter, subs);
			}
			LogicalOp::Join(join)
		}
		LogicalOp::Source(LogicalSource {
			source,
			schema,
			with,
			for_update,
		}) => match source {
			SourceRef::Param(param) => match subs.literals.get(param.as_str()) {
				// A locked read takes the literal: what it locks is settled when the plan is.
				Some(literal) if for_update || !subs.record_id_lists.contains(param.as_str()) => {
					classify_source(literal.clone(), None, with, for_update)
				}
				_ if !for_update && subs.record_id_lists.contains(param.as_str()) => {
					LogicalOp::Values(LogicalValues {
						expr: LogicalExpr::Param(param),
						for_update,
						written: false,
					})
				}
				_ => LogicalOp::Source(LogicalSource {
					source: SourceRef::Param(param),
					schema,
					with,
					for_update,
				}),
			},
			mut source @ SourceRef::RecordId(_) => {
				let mut exprs = Vec::new();
				source.exprs_mut(&mut exprs);
				for expr in exprs {
					substitute(expr, subs);
				}
				LogicalOp::Source(LogicalSource {
					source,
					schema,
					with,
					for_update,
				})
			}
			SourceRef::Dynamic(mut expr) => {
				substitute(&mut expr, subs);
				classify_source(expr, None, with, for_update)
			}
			source @ (SourceRef::Table(_) | SourceRef::Mock(_)) => {
				LogicalOp::Source(LogicalSource {
					source,
					schema,
					with,
					for_update,
				})
			}
		},
		// Operators holding no predicate and no source expression. Listed
		// rather than matched with a wildcard: an operator added later that
		// carries a statement expression must be considered here.
		other @ (LogicalOp::Values(_)
		| LogicalOp::Subquery(_)
		| LogicalOp::UnionAll(_)
		| LogicalOp::Split(_)
		| LogicalOp::Aggregate(_)
		| LogicalOp::Distinct(_)
		| LogicalOp::Sort(_)
		| LogicalOp::Window(_)
		| LogicalOp::Limit(_)
		| LogicalOp::Fetch(_)
		| LogicalOp::Timeout(_)
		| LogicalOp::VersionScope(_)
		| LogicalOp::Only(_)
		| LogicalOp::Match(_)
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
		| LogicalOp::Sleep(_)) => other,
	}
}

/// The parameter names `expr` references, not entering a nested plan, whose
/// own predicates and sources are visited as nodes of their own, and not
/// entering a closure body, whose parameters are read where the closure is
/// invoked.
fn collect_names(expr: &LogicalExpr, names: &mut HashSet<String>) {
	let _ = visit_binding(expr, &mut |e| {
		if let LogicalExpr::Param(param) = e {
			names.insert(param.as_str().to_owned());
		}
		ControlFlow::<()>::Continue(())
	});
}

/// Adds the names `expr` reads to `reads`, in one walk; a nested plan and a
/// closure body are not entered. None of them is a whole `FROM` source.
fn collect_resolvable_names(expr: &LogicalExpr, reads: &mut Reads) {
	visit_binding_pruning(expr, &mut |e| {
		if let LogicalExpr::Binary {
			left,
			op: BinaryOperator::NearestNeighbor(_),
			right,
		} = e && let LogicalExpr::Param(param) = right.as_ref()
		{
			reads.names.insert(param.as_str().to_owned());
			reads.outside_sources.insert(param.as_str().to_owned());
			collect_resolvable_names(left, reads);
			return Descend::Over;
		}
		if let LogicalExpr::Param(param) = e {
			reads.names.insert(param.as_str().to_owned());
			reads.outside_query_vectors.insert(param.as_str().to_owned());
			reads.outside_sources.insert(param.as_str().to_owned());
		}
		Descend::Into
	});
}

/// Replaces parameter references with their literals in `subs`, not entering
/// a nested plan or a closure body, and leaving a query-vector name in place
/// as the right operand of a KNN operator.
fn substitute(expr: &mut LogicalExpr, subs: &Substitutions) {
	visit_binding_pruning_mut(expr, &mut |e| {
		if let LogicalExpr::Binary {
			left,
			op: BinaryOperator::NearestNeighbor(_),
			right,
		} = e && let LogicalExpr::Param(param) = right.as_ref()
			&& subs.query_vectors.contains(param.as_str())
		{
			substitute(left, subs);
			return Descend::Over;
		}
		if let LogicalExpr::Param(param) = e
			&& let Some(literal) = subs.literals.get(param.as_str())
		{
			*e = literal.clone();
			return Descend::Over;
		}
		if let Some(root) = step_root(e)
			&& let LogicalExpr::Param(param) = root
			&& let Some(literal) = subs.literals.get(param.as_str())
		{
			*root = literal.clone();
			// The chain's own operands, an index such as `$i` in `$rows[$i].id`, are substituted
			// before the chain is read, so it folds whole.
			substitute(e, subs);
			if let Some(read) = literal_step_value(e).and_then(try_value_to_logical_expr) {
				*e = read;
				return Descend::Over;
			}
		}
		Descend::Into
	});
}

/// The expression a chain of field, index and end steps starts from, when `expr` is such a
/// chain.
fn step_root(expr: &mut LogicalExpr) -> Option<&mut LogicalExpr> {
	let base = step_base(expr)?;
	if is_step(base) {
		step_root(base)
	} else {
		Some(base)
	}
}

/// The expression the step `expr` reads from, when `expr` is a field, index or end step.
fn step_base(expr: &mut LogicalExpr) -> Option<&mut LogicalExpr> {
	if let LogicalExpr::Access {
		base,
		..
	}
	| LogicalExpr::Index {
		base,
		..
	}
	| LogicalExpr::First(base)
	| LogicalExpr::Last(base) = expr
	{
		Some(base.as_mut())
	} else {
		None
	}
}

/// Whether `expr` is a field, index or end step.
fn is_step(expr: &LogicalExpr) -> bool {
	matches!(
		expr,
		LogicalExpr::Access { .. }
			| LogicalExpr::Index { .. }
			| LogicalExpr::First(_)
			| LogicalExpr::Last(_)
	)
}

/// Replaces the parameter that is the right operand of a `MATCHES` in `expr` with its literal in
/// `subs`, not entering a nested plan or a closure body. Every other parameter is left in place.
fn substitute_matches_queries(expr: &mut LogicalExpr, subs: &Substitutions) {
	visit_binding_pruning_mut(expr, &mut |e| {
		if let LogicalExpr::Binary {
			op: BinaryOperator::Matches(_),
			right,
			..
		} = e && let LogicalExpr::Param(param) = right.as_ref()
			&& let Some(literal) = subs.literals.get(param.as_str())
		{
			**right = literal.clone();
		}
		Descend::Into
	});
}

#[cfg(all(test, feature = "kv-mem"))]
mod tests {
	use std::sync::Arc;

	use surrealdb_strand::TableName;

	use super::super::lower_test as lower;
	use super::*;
	use crate::ctx::FrozenContext;
	use crate::exec::function::FunctionRegistry;
	use crate::exec::planner::util::SELECT_ITERATION_PARAMS;
	use crate::expr::logical::{EvalFrame, LogicalField, LogicalFields, LogicalProject};
	use crate::expr::variables::Variables;
	use crate::expr::{BinaryOperator, Param};
	use crate::kvs::Datastore;
	use crate::val::{Closure, Set};

	async fn frozen_with(values: Vec<(&str, Value)>) -> FrozenContext {
		let ds = Datastore::new("memory").await.expect("datastore");
		let mut ctx = ds.setup_ctx().expect("context");
		for (name, value) in values {
			ctx.add_value(name.to_owned(), Arc::new(value));
		}
		ctx.freeze()
	}

	async fn bound(op: LogicalOp, values: Vec<(&str, Value)>) -> LogicalOp {
		let ctx = frozen_with(values).await;
		let registry = FunctionRegistry::with_builtins();
		let bx = BindContext {
			ctx: &ctx,
			txn: None,
			ns_db: None,
			auth: None,
			perms_suppressed: false,
			row_scoped: SELECT_ITERATION_PARAMS,
			registry: &registry,
			inlines_definitions: true,
			binds_nested: true,
		};
		resolve_params(op, &bx).await
	}

	fn int(n: i64) -> Value {
		Value::from(n)
	}

	fn source(table: &str) -> LogicalOp {
		LogicalOp::Source(LogicalSource {
			source: SourceRef::Table(TableName::from(table)),
			schema: None,
			with: None,
			for_update: false,
		})
	}

	#[tokio::test]
	async fn substitutes_statement_scoped_params_in_a_predicate() {
		let plan =
			bound(lower("SELECT * FROM t WHERE a > $p"), vec![("p", int(5))]).await.to_string();
		assert!(plan.contains("Filter[a > 5]"), "{plan}");
	}

	/// A field, index or end read off a substituted parameter is the value it reads, so an index
	/// operand is the bound value and not a path to it; a chain that would read a record keeps its
	/// steps.
	#[tokio::test]
	async fn steps_over_a_substituted_param_read_its_value() {
		let rows = Value::from(vec![Value::from(crate::val::Object::from(
			std::collections::BTreeMap::from([(
				surrealdb_strand::Strand::from("id"),
				Value::RecordId(crate::val::RecordId::new(
					TableName::from("u"),
					crate::val::RecordIdKey::Number(1),
				)),
			)]),
		))]);
		let plan = bound(
			lower(
				"SELECT * FROM t WHERE a IN $rows.id AND b = $rows[0].id AND c = $rows.id.x AND d = $rows[$i].id",
			),
			vec![("rows", rows), ("i", int(0))],
		)
		.await
		.to_string();
		assert!(plan.contains("a INSIDE [u:1]"), "{plan}");
		assert!(plan.contains("b = (u:1)"), "{plan}");
		assert!(plan.contains("c = [{ id: u:1 }].id.x"), "{plan}");
		assert!(plan.contains("d = (u:1)"), "{plan}");
	}

	fn vector(components: &[f64]) -> Value {
		Value::from(components.iter().map(|c| Value::from(*c)).collect::<Vec<_>>())
	}

	/// A parameter read only as a KNN operator's query vector stays one
	/// parameter node, and no literal is built for it.
	#[tokio::test]
	async fn a_query_vector_param_stays_a_parameter() {
		let plan = bound(
			lower("SELECT id FROM t WHERE v <|3,40|> $q"),
			vec![("q", vector(&[1.0, 2.0, 3.0]))],
		)
		.await
		.to_string();
		assert!(plan.contains("v <|3,40|> $q"), "{plan}");
		assert!(!plan.contains("[1f, 2f, 3f]"), "{plan}");
	}

	/// The same parameter read elsewhere in the predicate is substituted
	/// there, and stays a parameter as the query vector.
	#[tokio::test]
	async fn a_query_vector_param_read_elsewhere_is_substituted_there() {
		let plan = bound(
			lower("SELECT id FROM t WHERE v <|3,40|> $q AND w = $q"),
			vec![("q", vector(&[1.0, 2.0, 3.0]))],
		)
		.await
		.to_string();
		assert!(plan.contains("v <|3,40|> $q"), "{plan}");
		assert!(plan.contains("w = [1f, 2f, 3f]"), "{plan}");
	}

	#[tokio::test]
	async fn other_clauses_keep_their_parameters_for_runtime() {
		// A name outside a predicate or a source is resolved per row, where it
		// may mean something different each time.
		let plan = bound(
			lower("SELECT $p AS q FROM t ORDER BY a LIMIT $p START $p FETCH $p"),
			vec![("p", int(5))],
		)
		.await
		.to_string();
		for expected in ["$p AS q", "LIMIT $p", "START $p"] {
			assert!(plan.contains(expected), "missing {expected:?} in:\n{plan}");
		}
	}

	/// A mutation's `WHERE` is a predicate, substituted like a `SELECT`'s, so
	/// its access path sees the bound a context value (a `LET`, or a client
	/// bind parameter) holds. Its data clause keeps the parameter.
	#[tokio::test]
	async fn substitutes_a_mutation_condition_and_not_its_data() {
		for statement in [
			"DELETE t WHERE a < $p AND b = $this.b",
			"UPDATE t SET c = $p WHERE a < $p AND b = $this.b",
		] {
			let plan = bound(lower(statement), vec![("p", int(5)), ("this", int(1))]).await;
			let LogicalOp::Write(write) = &plan else {
				panic!("{statement}: not a write:\n{plan}");
			};
			let cond = write.cond.as_ref().expect("a condition").to_string();
			assert_eq!(cond, "a < 5 AND b = $this.b", "{statement}");
			if let Some(data) = &write.data {
				let values: Vec<String> = data.values().iter().map(|v| v.to_string()).collect();
				assert_eq!(values, ["$p"], "{statement}");
			}
		}
	}

	#[tokio::test]
	async fn leaves_row_scoped_and_unknown_params_alone() {
		// `$this` and `$parent` have a plan-time value here, and must still
		// not be substituted: their real value is bound per row. `$missing`
		// is bound without a database to consult, so it stays a runtime
		// lookup.
		let plan = bound(
			lower("SELECT * FROM t WHERE a = $this.b AND c = $parent.d AND e = $missing"),
			vec![("this", int(1)), ("parent", int(2))],
		)
		.await
		.to_string();
		for expected in ["$this.b", "$parent.d", "$missing"] {
			assert!(plan.contains(expected), "missing {expected:?} in:\n{plan}");
		}
	}

	#[tokio::test]
	async fn a_from_param_becomes_a_source_or_a_values_leaf() {
		let plan = bound(
			lower("SELECT * FROM $tb, $arr"),
			vec![
				("tb", Value::Table(TableName::from("person"))),
				("arr", Value::from(vec![int(1), int(2)])),
			],
		)
		.await;
		let leaves: Vec<_> = plan.children()[0].children().iter().map(LogicalOp::name).collect();
		assert_eq!(leaves, ["Source", "Values"]);
		let rendered = plan.to_string();
		assert!(rendered.contains("Source[person]"), "{rendered}");
		assert!(rendered.contains("Values[[1, 2]]"), "{rendered}");
	}

	fn ids(table: &str, keys: &[i64]) -> Value {
		Value::from(
			keys.iter()
				.map(|key| Value::RecordId(crate::val::RecordId::new(TableName::from(table), *key)))
				.collect::<Vec<_>>(),
		)
	}

	/// A parameter holding record ids, read as a whole source, becomes the `Values` leaf its
	/// literal would, holding the parameter, and no literal is built for it; read in a predicate
	/// too, it is substituted there alone. The subjects of a write, a locked read, an array
	/// holding a range and an empty array take the literal.
	#[tokio::test]
	async fn a_record_id_list_source_reads_the_parameter() {
		let list = ids("person", &[2, 1]);
		let plan = bound(lower("SELECT * FROM $ids"), vec![("ids", list.clone())]).await;
		let leaves: Vec<_> = plan.children().iter().map(LogicalOp::name).collect();
		assert_eq!(leaves, ["Values"], "{plan}");
		let rendered = plan.to_string();
		assert!(rendered.contains("Values[$ids]"), "{rendered}");
		assert!(!rendered.contains("person:2"), "{rendered}");
		for statement in ["UPDATE $ids SET a = 1", "DELETE $ids"] {
			let plan = bound(lower(statement), vec![("ids", list.clone())]).await.to_string();
			assert!(!plan.contains("$ids"), "{statement}:\n{plan}");
			assert!(plan.contains("person:2"), "{statement}:\n{plan}");
		}
		let plan = bound(lower("SELECT * FROM $ids WHERE id IN $ids"), vec![("ids", list.clone())])
			.await
			.to_string();
		assert!(plan.contains("Values[$ids]"), "{plan}");
		assert!(plan.contains("id INSIDE [person:2, person:1]"), "{plan}");
		let plan =
			bound(lower("SELECT * FROM $ids FOR UPDATE"), vec![("ids", list)]).await.to_string();
		assert!(!plan.contains("$ids"), "{plan}");
		let ranged = Value::from(vec![
			Value::RecordId(crate::val::RecordId::new(TableName::from("person"), 1_i64)),
			Value::RecordId(crate::val::RecordId {
				table: TableName::from("person"),
				key: crate::val::RecordIdKey::Range(Box::new(crate::val::RecordIdKeyRange {
					start: std::ops::Bound::Unbounded,
					end: std::ops::Bound::Unbounded,
				})),
			}),
		]);
		for value in [ranged, Value::from(Vec::<Value>::new())] {
			let plan = bound(lower("SELECT * FROM $ids"), vec![("ids", value)]).await.to_string();
			assert!(!plan.contains("$ids"), "{plan}");
		}
	}

	/// A parameter that is the subject of a write and a whole source elsewhere in the plan takes
	/// the literal at the write alone: the write names the tables of the ids when the plan is
	/// planned, and every read of the list keeps the parameter.
	#[tokio::test]
	async fn a_record_id_list_is_a_write_subject_and_a_source_in_one_plan() {
		let list = ids("person", &[2, 1]);
		for statement in [
			"{ UPDATE $ids SET n += 1; RETURN (SELECT n FROM $ids); }",
			"UPDATE $ids SET total = (SELECT VALUE math::sum(n) FROM $ids GROUP ALL)[0]",
			"DELETE $ids WHERE n IN (SELECT VALUE n FROM $ids)",
		] {
			let plan = bound(lower(statement), vec![("ids", list.clone())]).await;
			let mut subjects = Vec::new();
			let mut reads = 0;
			visit_deep(&plan, &mut |node| {
				if let LogicalOp::Write(LogicalWrite {
					source,
					..
				}) = node
				{
					subjects.push(super::super::footprint::write_targets(source));
				} else if let LogicalOp::Values(LogicalValues {
					expr: LogicalExpr::Param(_),
					..
				}) = node
				{
					reads += 1;
				}
			});
			let person = TableName::from("person");
			assert_eq!(subjects, [Some(vec![person.clone(), person])], "{statement}:\n{plan}");
			assert_eq!(reads, 1, "{statement}:\n{plan}");
		}
	}

	#[tokio::test]
	async fn descends_into_scalar_subqueries() {
		// A scalar subquery runs in the statement's transaction with the
		// statement's context, so its predicate resolves with the enclosing one.
		let plan = bound(
			lower("SELECT * FROM t WHERE a IN (SELECT b FROM u WHERE c = $p) AND d = $p"),
			vec![("p", int(5))],
		)
		.await
		.to_string();
		assert!(plan.contains("c = 5"), "subquery predicate should resolve:\n{plan}");
		assert!(plan.contains("d = 5"), "outer reference should resolve:\n{plan}");
	}

	#[tokio::test]
	async fn does_not_enter_blocks_and_respects_closure_scope() {
		// A block body is an expression, not a predicate. A closure argument
		// is bound within the closure body alone: the body is left as it is,
		// and the same name outside the closure is the context's.
		let plan = bound(
			lower("SELECT { RETURN $p } AS b, |$q: int| $q AS c FROM t WHERE d = $p AND e = $q"),
			vec![("p", int(5)), ("q", int(6))],
		)
		.await
		.to_string();
		assert!(plan.contains("Eval[$p]"), "block body should be untouched:\n{plan}");
		assert!(plan.contains("|$q: int| $q"), "closure body should be untouched:\n{plan}");
		assert!(plan.contains("d = 5"), "outer reference should resolve:\n{plan}");
		assert!(plan.contains("e = 6"), "the name outside the closure is the context's:\n{plan}");
	}

	#[tokio::test]
	async fn descends_into_relational_subqueries() {
		let plan =
			bound(lower("SELECT * FROM (SELECT * FROM t WHERE a = $p)"), vec![("p", int(5))])
				.await
				.to_string();
		assert!(plan.contains("a = 5"), "{plan}");
	}

	#[tokio::test]
	async fn never_substitutes_a_name_a_let_or_a_loop_binds() {
		let plan = bound(
			lower("{ LET $x = 1; FOR $y IN [1] { SELECT * FROM t WHERE a = $x AND b = $y }; }"),
			vec![("x", int(7)), ("y", int(8))],
		)
		.await
		.to_string();
		assert!(plan.contains("a = $x AND b = $y"), "{plan}");
	}

	#[tokio::test]
	async fn a_layer_column_keeps_its_parameters() {
		// A column's body is a schema definition's, bound where the definition
		// is evaluated; a predicate above the layer still resolves.
		let op = LogicalOp::Filter(LogicalFilter {
			input: Box::new(LogicalOp::Project(LogicalProject {
				input: Box::new(source("t")),
				fields: LogicalFields::Select(vec![
					LogicalField::All,
					LogicalField::column(
						"c",
						LogicalExpr::Param(Param::from("p".to_owned())),
						None,
						EvalFrame::computed(None),
					),
				]),
				omit: vec![],
			})),
			predicate: LogicalExpr::Binary {
				left: Box::new(LogicalExpr::Field("c".into())),
				op: BinaryOperator::MoreThan,
				right: Box::new(LogicalExpr::Param(Param::from("p".to_owned()))),
			},
		});
		let plan = bound(op, vec![("p", int(7))]).await.to_string();
		assert!(plan.contains("Project[*, $p AS c @computed]"), "{plan}");
		assert!(plan.contains("Filter[c > 7]"), "{plan}");
	}

	#[tokio::test]
	async fn a_layer_binds_value_for_the_plans_nested_in_its_columns() {
		use super::super::test_fixture::{Fixture, owner};
		// `$value` is bound per column when the layer evaluates it, so it is
		// never the unbound name the catalog would report it as — in a plan
		// nested in a column's body included, where it is a predicate.
		let fixture = Fixture::new("").await;
		let bound = fixture.bound_as(&owner(), vec![]).await;
		let bx = fixture.bind_context(&bound);
		let op = LogicalOp::Project(LogicalProject {
			input: Box::new(source("t")),
			fields: LogicalFields::Select(vec![
				LogicalField::All,
				LogicalField::column(
					"c",
					LogicalExpr::Subquery(Box::new(lower("SELECT * FROM u WHERE f = $value"))),
					None,
					EvalFrame::computed(None),
				),
			]),
			omit: vec![],
		});
		let plan = resolve_params(op, &bx).await.to_string();
		assert!(plan.contains("Filter[f = $value]"), "{plan}");
	}

	#[tokio::test]
	async fn a_set_is_substituted_as_a_set_literal() {
		// A set has a literal form of its own, so it is substituted like any
		// other value, so the predicate does not read the parameter afterwards.
		let plan = bound(
			lower("SELECT * FROM t WHERE a IN $s"),
			vec![("s", Value::Set(Set::from(vec![int(1)])))],
		)
		.await
		.to_string();
		assert!(!plan.contains("$s"), "{plan}");
		assert!(plan.contains("a INSIDE {"), "{plan}");
	}

	#[tokio::test]
	async fn an_unbound_name_is_none_once_the_catalog_has_been_asked() {
		use super::super::test_fixture::{Fixture, owner};
		let fixture = Fixture::new(
			"DEFINE PARAM $open VALUE 3 PERMISSIONS FULL; \
			 DEFINE PARAM $gated VALUE 4 PERMISSIONS WHERE $auth.id = user:x;",
		)
		.await;
		let bound = fixture.bound_as(&owner(), vec![("ctx", int(1))]).await;
		let bx = fixture.bind_context(&bound);
		let plan = resolve_params(
			lower(
				"SELECT * FROM t, $absent WHERE a = $ctx AND b = $open AND c = $gated AND d = $absent",
			),
			&bx,
		)
		.await
		.to_string();
		// The context value and the readable `DEFINE PARAM` are literals; the
		// gated one is judged per read; the absent one is `NONE` everywhere,
		// the source included.
		assert!(plan.contains("a = 1 AND b = 3 AND c = $gated AND d = NONE"), "{plan}");
		assert!(plan.contains("Values[NONE]"), "{plan}");
	}

	#[tokio::test]
	async fn lossy_values_are_left_for_runtime() {
		// A closure has no literal form, so it must stay a parameter.
		let closure = Closure::Expr {
			args: Vec::new(),
			returns: None,
			body: crate::expr::Expr::Literal(crate::expr::Literal::None),
			captures: Variables::default(),
		};
		let plan = bound(
			lower("SELECT * FROM t WHERE a = $f"),
			vec![("f", Value::Closure(Box::new(closure)))],
		)
		.await
		.to_string();
		assert!(plan.contains("$f"), "{plan}");
	}
}
