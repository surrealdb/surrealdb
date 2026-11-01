//! Plan-time row-scope analysis for SELECT statements.
//!
//! Encodes the rules around `$this`, `$self`, and `$parent` references in
//! expressions. All plan-time consumers (selective-scan field extraction,
//! `WherePart::needs_parent`, GROUP BY validation, the KNN query-vector
//! classification and its question of whether the vector reads the row being
//! scanned) call into this module so the scoping invariants are enforced in
//! exactly one place, on one traversal of the scalars and of the statement
//! plans they hold.
//!
//! Scoping rules:
//!
//! - `$this` / `$self` denote the current SELECT's iteration row. Field accesses through them
//!   belong to the current row's table.
//! - `$parent` denotes the *outer* SELECT's iteration row. Field accesses through it belong to the
//!   outer row, not the current row.
//! - A SELECT in scalar position is not descended into during analysis. Its `$parent` binds to the
//!   current SELECT's `$this`, so it does not add to the current SELECT's outer-row dependency.
//!   Nested-subquery patterns rely on this, and the language tests cover it. A block, `LET`,
//!   `RETURN` or `FOR` in scalar position runs in the enclosing scope and is descended into.
//!
//! This module is plan-time only. Runtime sites (`parts/filter.rs`, `parts/lookup.rs`, etc.)
//! read the flags produced here through `WherePart::needs_parent` and related methods. They bind
//! each row at run time through `EvalContext::current_value` and `document_root`.

use std::ops::ControlFlow;

use crate::exec::function::FunctionRegistry;
use crate::expr::logical::{LogicalExpr, LogicalOp, LogicalSource, SourceRef};
use crate::expr::{Function, Param};

/// Row-scoped parameter names. These are the logical layer's `ITERATION_PARAMS`, which the effect
/// fold also reads. `util::SELECT_ITERATION_PARAMS` re-exports this list, and the binding passes
/// receive it as `BindContext::row_scoped`.
pub(crate) const ROW_SCOPED_PARAMS: &[&str] = crate::expr::logical::ITERATION_PARAMS;

/// Variants of row-scoped parameter references.
///
/// `$this` and `$self` denote the current row; `$parent` denotes the outer
/// row. Plan-time consumers treat these differently — same-row references
/// propagate field accesses into the current row's selective-scan set,
/// outer-row references do not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RowScopeKind {
	This,
	Self_,
	Parent,
}

impl RowScopeKind {
	/// Returns `Some(kind)` when `name` is a row-scoped parameter, else `None`.
	pub(crate) fn from_param_name(name: &str) -> Option<Self> {
		match name {
			"this" => Some(RowScopeKind::This),
			"self" => Some(RowScopeKind::Self_),
			"parent" => Some(RowScopeKind::Parent),
			_ => None,
		}
	}
}

/// How deeply row-scope analysis walks an expression tree.
///
/// The two consumers need different rules:
///
/// - `CurrentSelect`: stop at SELECT subquery boundaries. Used when the result feeds the current
///   SELECT's own decisions, such as `WherePart::needs_parent`. A subquery's `$parent` binds to the
///   current SELECT's `$this`, so it does not count.
/// - `Recursive`: descend through SELECT subqueries. Used for blanket prohibitions. GROUP BY
///   validation is the main case: a `$parent` reference anywhere in a grouped projection is
///   invalid, even inside a subquery whose `$parent` resolves to the outer SELECT's `$this`. That
///   `$this` is the iteration document, which GROUP BY collapses away.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AnalysisScope {
	CurrentSelect,
	Recursive,
}

/// Returns `true` when `expr` references `$parent` within the current
/// SELECT scope (SELECT subqueries skipped).
pub(crate) fn references_parent(expr: &LogicalExpr) -> bool {
	let mut walker =
		ParamWalker::new(|kind| kind == RowScopeKind::Parent, AnalysisScope::CurrentSelect);
	let _ = walker.walk_expr(expr);
	walker.found
}

/// Returns the first row-scoped parameter referenced by `expr`, walking
/// at the requested `scope`.
///
/// Used to produce specific diagnostic messages (e.g. distinguishing
/// `$this` from `$parent` in GROUP BY errors). The `scope` parameter
/// matters: see [`AnalysisScope`].
pub(crate) fn first_row_scoped_reference(
	expr: &LogicalExpr,
	scope: AnalysisScope,
) -> Option<RowScopeKind> {
	let mut walker = ParamWalker::new(|_| true, scope);
	let _ = walker.walk_expr(expr);
	walker.first_kind
}

/// Whether `op`, a plan in scalar position, reads the row it is evaluated
/// over: a statement operator that does not rescope the row parameters, or a
/// plan that names a row parameter anywhere inside it.
pub(crate) fn reads_enclosing_row(op: &LogicalOp) -> bool {
	if !rescopes_row_params(op) {
		return true;
	}
	let mut walker = ParamWalker::new(|_| true, AnalysisScope::Recursive);
	let _ = walker.walk_op(op);
	walker.found
}

/// Whether `op`, a plan in scalar position, names `$this`, `$self` or `$parent` anywhere inside
/// it, as a scalar or as a `FROM` source.
pub(crate) fn names_row_param(op: &LogicalOp) -> bool {
	let mut walker = ParamWalker::new(|_| true, AnalysisScope::Recursive);
	let _ = walker.walk_op(op);
	walker.found
}

/// Whether `op`, a plan in scalar position, names `$parent` anywhere inside it, as a scalar or
/// as a `FROM` source.
pub(crate) fn names_parent(op: &LogicalOp) -> bool {
	let mut walker =
		ParamWalker::new(|kind| kind == RowScopeKind::Parent, AnalysisScope::Recursive);
	let _ = walker.walk_op(op);
	walker.found
}

/// Whether a rescoping plan in scalar position (see [`rescopes_row_params`])
/// reads the row of the expression it appears in — the row the subquery
/// binds to `$parent`, and to `$this` for what it evaluates before it has
/// rows of its own — following what each clause binds when it runs.
///
/// That is a `$parent` at the plan's own level — in any of its scalars or its
/// inputs', a SELECT nested in scalar position rescoping again and so not
/// counted — or any row-scoped parameter in a source it reads (`FROM $this`,
/// `FROM $parent.items`, a record-id key holding one).
///
/// The WHERE condition, the projection, the ORDER BY and a table's
/// `PERMISSIONS FOR select` predicate (`PermissionFilter`) are evaluated per
/// row of the plan, with that row as their document. In them a `[WHERE …]`
/// predicate, a lookup's clauses and a block or other statement plan bind
/// `$parent` to the document they are evaluated against, the plan's own row
/// or a value derived from it, so a `$parent` there is not the enclosing row
/// (see [`references_parent_at_row_level`]).
///
/// The source is evaluated before the plan has a row, with no document: a
/// `[WHERE …]` or a lookup there binds no row of its own, so its `$parent`
/// is the enclosing row, and a row-scoped parameter anywhere in it counts —
/// except inside a destructure's bodies, which are evaluated against the
/// destructured value as their document (see [`source_reads_enclosing_row`]).
/// Every other scalar — `LIMIT`, `START` and the rest — counts any `$parent`
/// it holds. A `GROUP BY` holding `$parent` is refused by the grouping's own
/// validation.
///
/// A closure literal is a value: its body runs only when the closure is
/// invoked, so a `$parent` or row parameter in it is not counted. A body that
/// reads the enclosing row when it is invoked inside a KNN query vector's
/// evaluation fails then (see [`crate::exec::context::ForbiddenRow`]).
///
/// [`reads_enclosing_row`] answers the coarser question the lowering asks —
/// whether a plan names a row parameter anywhere — and stays conservative.
pub(crate) fn subquery_reads_enclosing_row(op: &LogicalOp) -> bool {
	let own_scalars_read_row = match op {
		LogicalOp::Source(LogicalSource {
			source,
			..
		}) => {
			matches!(source, SourceRef::Param(param)
				if RowScopeKind::from_param_name(param.as_str()).is_some())
				|| op.scalars().into_iter().any(source_reads_enclosing_row)
		}
		LogicalOp::Filter(_)
		| LogicalOp::PermissionFilter(_)
		| LogicalOp::Project(_)
		| LogicalOp::Window(_)
		| LogicalOp::Sort(_) => op.scalars().into_iter().any(references_parent_at_row_level),
		LogicalOp::Values(_)
		| LogicalOp::Match(_)
		| LogicalOp::Subquery(_)
		| LogicalOp::Write(_)
		| LogicalOp::Ddl(_)
		| LogicalOp::UnionAll(_)
		| LogicalOp::Join(_)
		| LogicalOp::Split(_)
		| LogicalOp::Aggregate(_)
		| LogicalOp::Distinct(_)
		| LogicalOp::Limit(_)
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
		| LogicalOp::If(_)
		| LogicalOp::Throw(_)
		| LogicalOp::Break(_)
		| LogicalOp::Continue(_)
		| LogicalOp::Info(_)
		| LogicalOp::Sleep(_) => op
			.scalars()
			.into_iter()
			.any(|scalar| row_param_outside_closures(scalar, |kind| kind == RowScopeKind::Parent)),
	};
	own_scalars_read_row || op.children().iter().any(subquery_reads_enclosing_row)
}

/// Whether `expr`, evaluated per row of a SELECT with that row as its
/// document, reads `$parent` as the SELECT's enclosing row: a `$parent` at the
/// expression's own level. A closure literal is a value and is not looked
/// into (see [`subquery_reads_enclosing_row`]).
///
/// A `[WHERE …]` predicate and a lookup's clauses bind `$parent` to the
/// document, and a block or other statement plan in scalar position binds the
/// document as its `$parent`: a `$parent` inside them is the SELECT's row or a
/// value derived from it, and only their bases are looked into. A
/// destructure's bodies are evaluated against the destructured value as their
/// document, so a `[WHERE …]` or a lookup there binds that value, but a bare
/// `$parent` in a body is still the parameter, the enclosing row, and counts.
/// A SELECT nested in scalar position rescopes and is not looked into.
fn references_parent_at_row_level(expr: &LogicalExpr) -> bool {
	match expr {
		LogicalExpr::Param(p) => {
			RowScopeKind::from_param_name(p.as_str()) == Some(RowScopeKind::Parent)
		}
		LogicalExpr::Filter {
			base,
			..
		}
		| LogicalExpr::Lookup {
			base,
			..
		} => references_parent_at_row_level(base),
		LogicalExpr::Subquery(_) | LogicalExpr::Closure(_) => false,
		LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Field(_)
		| LogicalExpr::Doc
		| LogicalExpr::Current
		| LogicalExpr::Access {
			..
		}
		| LogicalExpr::Index {
			..
		}
		| LogicalExpr::First(_)
		| LogicalExpr::Last(_)
		| LogicalExpr::Each(_)
		| LogicalExpr::Flatten {
			..
		}
		| LogicalExpr::Method {
			..
		}
		| LogicalExpr::Destructure {
			..
		}
		| LogicalExpr::Optional {
			..
		}
		| LogicalExpr::Recurse(_)
		| LogicalExpr::RepeatRecurse(_)
		| LogicalExpr::Broadcast {
			..
		}
		| LogicalExpr::SetField {
			..
		}
		| LogicalExpr::Prefix {
			..
		}
		| LogicalExpr::Postfix {
			..
		}
		| LogicalExpr::Invoke {
			..
		}
		| LogicalExpr::Binary {
			..
		}
		| LogicalExpr::Call(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue => any_child(expr, references_parent_at_row_level),
	}
}

/// Whether a SELECT's source expression `expr` reads the SELECT's enclosing
/// row: any row-scoped parameter in it, outside closure literals (see
/// [`row_param_outside_closures`]), except in a destructure's bodies.
///
/// The source is evaluated before the SELECT has a row and with no document,
/// so a `[WHERE …]` or a lookup in it binds no row of its own and its
/// `$parent` is the enclosing row. A destructure's bodies are evaluated with
/// the destructured value as the value in scope and the document: `$this`
/// and a `[WHERE …]` or a lookup there read that value, and only a bare
/// `$parent` is the enclosing row ([`references_parent_at_row_level`]).
fn source_reads_enclosing_row(expr: &LogicalExpr) -> bool {
	match expr {
		LogicalExpr::Destructure {
			base,
			..
		} => {
			source_reads_enclosing_row(base)
				|| any_body_child(expr, base, references_parent_at_row_level)
		}
		LogicalExpr::Closure(_) => false,
		LogicalExpr::Param(p) => RowScopeKind::from_param_name(p.as_str()).is_some(),
		LogicalExpr::Subquery(op) => plan_row_param_outside_closures(op, |_| true),
		LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Field(_)
		| LogicalExpr::Doc
		| LogicalExpr::Current
		| LogicalExpr::Access {
			..
		}
		| LogicalExpr::Index {
			..
		}
		| LogicalExpr::First(_)
		| LogicalExpr::Last(_)
		| LogicalExpr::Each(_)
		| LogicalExpr::Flatten {
			..
		}
		| LogicalExpr::Filter {
			..
		}
		| LogicalExpr::Method {
			..
		}
		| LogicalExpr::Optional {
			..
		}
		| LogicalExpr::Lookup {
			..
		}
		| LogicalExpr::Recurse(_)
		| LogicalExpr::RepeatRecurse(_)
		| LogicalExpr::Broadcast {
			..
		}
		| LogicalExpr::SetField {
			..
		}
		| LogicalExpr::Prefix {
			..
		}
		| LogicalExpr::Postfix {
			..
		}
		| LogicalExpr::Invoke {
			..
		}
		| LogicalExpr::Binary {
			..
		}
		| LogicalExpr::Call(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue => any_child(expr, source_reads_enclosing_row),
	}
}

/// Whether `expr` references a row-scoped parameter that `accept` admits,
/// within the current SELECT scope, outside closure literals: a SELECT
/// subquery rescopes and is a leaf, a statement plan in scalar position (a
/// block, `LET`, `RETURN`) is looked into, and a closure literal is a value
/// whose body is not.
fn row_param_outside_closures(expr: &LogicalExpr, accept: fn(RowScopeKind) -> bool) -> bool {
	match expr {
		LogicalExpr::Closure(_) => false,
		LogicalExpr::Param(p) => RowScopeKind::from_param_name(p.as_str()).is_some_and(accept),
		LogicalExpr::Subquery(op) => plan_row_param_outside_closures(op, accept),
		LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Field(_)
		| LogicalExpr::Doc
		| LogicalExpr::Current
		| LogicalExpr::Access {
			..
		}
		| LogicalExpr::Index {
			..
		}
		| LogicalExpr::First(_)
		| LogicalExpr::Last(_)
		| LogicalExpr::Each(_)
		| LogicalExpr::Flatten {
			..
		}
		| LogicalExpr::Filter {
			..
		}
		| LogicalExpr::Method {
			..
		}
		| LogicalExpr::Destructure {
			..
		}
		| LogicalExpr::Optional {
			..
		}
		| LogicalExpr::Lookup {
			..
		}
		| LogicalExpr::Recurse(_)
		| LogicalExpr::RepeatRecurse(_)
		| LogicalExpr::Broadcast {
			..
		}
		| LogicalExpr::SetField {
			..
		}
		| LogicalExpr::Prefix {
			..
		}
		| LogicalExpr::Postfix {
			..
		}
		| LogicalExpr::Invoke {
			..
		}
		| LogicalExpr::Binary {
			..
		}
		| LogicalExpr::Call(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue => any_child(expr, |child| row_param_outside_closures(child, accept)),
	}
}

/// [`row_param_outside_closures`] for a plan in scalar position: a statement
/// plan is looked into, and a plan that rescopes is a leaf.
fn plan_row_param_outside_closures(op: &LogicalOp, accept: fn(RowScopeKind) -> bool) -> bool {
	statement_plan_holds(op, &mut |scalar| row_param_outside_closures(scalar, accept), &mut |_| {
		false
	})
}

/// Whether a plan in scalar position rescopes the row parameters.
///
/// A SELECT — any relational operator — binds `$this` to its own rows and
/// `$parent` to the enclosing row, so a reference inside it is about the
/// subquery's rows. The statement operators (a bare expression, a block,
/// `LET`, `RETURN`, `FOR`, `INFO`, a schema statement, `SLEEP`) run in the
/// enclosing scope and do not.
pub(crate) fn rescopes_row_params(op: &LogicalOp) -> bool {
	!matches!(
		op,
		LogicalOp::Eval(_)
			| LogicalOp::Sequence(_)
			| LogicalOp::Let(_)
			| LogicalOp::Return(_)
			| LogicalOp::Foreach(_)
			| LogicalOp::Info(_)
			| LogicalOp::Ddl(_)
			| LogicalOp::Sleep(_)
	)
}

// ============================================================================
// Shared traversal
// ============================================================================

/// Whether a child of `expr` satisfies `f`, the children taken in order and
/// the walk stopping at the first that does.
fn any_child(expr: &LogicalExpr, mut f: impl FnMut(&LogicalExpr) -> bool) -> bool {
	let mut found = false;
	expr.for_each_child(&mut |child| found = found || f(child));
	found
}

/// [`any_child`] over the children of `step` other than `base`: the body a
/// path step evaluates against an element, a lookup's clauses, a
/// destructure's bodies.
fn any_body_child(
	step: &LogicalExpr,
	base: &LogicalExpr,
	mut f: impl FnMut(&LogicalExpr) -> bool,
) -> bool {
	any_child(step, |child| !std::ptr::eq(child, base) && f(child))
}

/// Whether `op`, a plan in scalar position, holds what `scalar` finds in one
/// of its scalars. A statement plan (a block, `LET`, `RETURN`, `FOR`, …) runs
/// in the enclosing scope, so its scalars and its inputs' are looked into; a
/// plan that rescopes the row parameters ([`rescopes_row_params`]) — the plan
/// itself or one of its inputs — is answered by `rescoped` instead.
fn statement_plan_holds(
	op: &LogicalOp,
	scalar: &mut dyn FnMut(&LogicalExpr) -> bool,
	rescoped: &mut dyn FnMut(&LogicalOp) -> bool,
) -> bool {
	if rescopes_row_params(op) {
		return rescoped(op);
	}
	op.scalars().into_iter().any(&mut *scalar)
		|| op.children().iter().any(|child| statement_plan_holds(child, scalar, rescoped))
}

// ============================================================================
// The row a KNN query vector reads
// ============================================================================

/// Whether a KNN operator's right-hand side reads the row being scanned.
///
/// It does when it holds a field, `@`, the row as the root of a path (a
/// leading lookup), `$this`/`$self`, or a call to a projection builtin
/// (`FunctionRegistry::is_projection`, e.g. `type::field`), which reads the
/// row's fields by name, in a position evaluated against the row: the
/// expression itself, any operand of it, a block's statements, and every
/// index expression and method argument of a path, wherever in the path it
/// sits (they are evaluated against the document the path started from).
/// The same forms inside a body evaluated against an element — a `[WHERE …]`
/// predicate, a broadcast, an optional's continuation, a recursion, a
/// lookup's clauses — are the element, not the row.
///
/// A closure literal is a value: its body runs only when the closure is
/// invoked, so it is not looked into here. A body that reads the row when it
/// is invoked during the vector's evaluation fails then, with the same error
/// (see [`crate::exec::context::ForbiddenRow`]).
///
/// It also does, wherever in the expression it appears, through a form that
/// binds `$parent` to the row being scanned:
/// - a `[WHERE …]` predicate or a graph or reference lookup whose own expressions reference
///   `$parent`;
/// - a SELECT subquery that reads its enclosing row (see [`subquery_reads_enclosing_row`]): a
///   `$parent` at its own level, or a row-scoped parameter in a source it reads.
///
/// A `$parent` anywhere else is the enclosing statement's row and is not the
/// row being scanned.
///
/// An aliased destructure (`$o.{ v: … }`) evaluates each body with the
/// destructured value as the value in scope and as the document: its fields,
/// `$this`, index expressions and method arguments, and the `$parent` that a
/// `[WHERE …]`, a lookup or a correlated SELECT in it binds, are all that
/// value. A destructure reads the row being scanned only through the
/// expression it destructures: `$o.{ v: [5f, 0f][WHERE $parent.enabled] }.v`
/// does not, `point.{ v: … }` does.
pub(crate) fn reads_scanned_row(expr: &LogicalExpr, registry: &FunctionRegistry) -> bool {
	reads_value_in(expr, ValueInScope::Row, registry) || binds_parent_to_row(expr)
}

/// What a field, `@` or `$this` denotes where [`reads_value_in`] meets it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ValueInScope {
	/// The row being scanned.
	Row,
	/// The element a step's body is evaluated against.
	Element,
}

/// The operand half of [`reads_scanned_row`], at `scope`. A SELECT
/// subquery's own rows are its own; whether it reads its enclosing row is
/// [`binds_parent_to_row`]'s to say. A statement plan in scalar position runs
/// in the enclosing scope and is looked into. A closure literal is a value.
fn reads_value_in(expr: &LogicalExpr, scope: ValueInScope, registry: &FunctionRegistry) -> bool {
	match expr {
		LogicalExpr::Field(_) | LogicalExpr::Doc | LogicalExpr::Current => {
			scope == ValueInScope::Row
		}
		LogicalExpr::Param(param) => scope == ValueInScope::Row && is_scanned_row_param(param),
		LogicalExpr::Subquery(op) => statement_plan_holds(
			op,
			&mut |scalar| reads_value_in(scalar, scope, registry),
			&mut |_| false,
		),
		LogicalExpr::Closure(_) => false,
		// A projection builtin (`type::field`, `type::fields`) reads the fields
		// of the value in scope by the names its arguments evaluate to.
		LogicalExpr::Call(call)
			if scope == ValueInScope::Row
				&& matches!(&call.receiver, Function::Normal(name) if registry.is_projection(name)) =>
		{
			true
		}
		LogicalExpr::Index {
			base,
			index,
		} => {
			reads_value_in(base, scope, registry)
				|| reads_value_in(index, ValueInScope::Row, registry)
		}
		LogicalExpr::Method {
			base,
			args,
			..
		} => {
			reads_value_in(base, scope, registry)
				|| args.iter().any(|arg| reads_value_in(arg, ValueInScope::Row, registry))
		}
		LogicalExpr::Filter {
			base,
			..
		}
		| LogicalExpr::Broadcast {
			base,
			..
		}
		| LogicalExpr::Optional {
			base,
			..
		}
		| LogicalExpr::Lookup {
			base,
			..
		} => body_reads_value(expr, base, scope, registry),
		LogicalExpr::Recurse(recurse) => body_reads_value(expr, &recurse.base, scope, registry),
		// A destructure's bodies are evaluated with the destructured value as
		// the value in scope and as the document: nothing in them reaches the
		// row being scanned (see [`reads_scanned_row`]).
		LogicalExpr::Destructure {
			base,
			..
		} => reads_value_in(base, scope, registry),
		LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Access {
			..
		}
		| LogicalExpr::First(_)
		| LogicalExpr::Last(_)
		| LogicalExpr::Each(_)
		| LogicalExpr::Flatten {
			..
		}
		| LogicalExpr::RepeatRecurse(_)
		| LogicalExpr::SetField {
			..
		}
		| LogicalExpr::Prefix {
			..
		}
		| LogicalExpr::Postfix {
			..
		}
		| LogicalExpr::Invoke {
			..
		}
		| LogicalExpr::Binary {
			..
		}
		| LogicalExpr::Call(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue => any_child(expr, |child| reads_value_in(child, scope, registry)),
	}
}

/// [`reads_value_in`] for a step whose `base` is evaluated at `scope` and
/// whose other children are a body evaluated against an element.
fn body_reads_value(
	step: &LogicalExpr,
	base: &LogicalExpr,
	scope: ValueInScope,
	registry: &FunctionRegistry,
) -> bool {
	reads_value_in(base, scope, registry)
		|| any_body_child(step, base, |child| {
			reads_value_in(child, ValueInScope::Element, registry)
		})
}

/// The `$parent` half of [`reads_scanned_row`]: anywhere in `expr` where the
/// document is still the row being scanned — a block in scalar position
/// included; a closure literal's body and a destructure's bodies, where it is
/// not, excluded — a `[WHERE …]` predicate or a lookup that references
/// `$parent` (both bind it to the document) or a SELECT subquery that reads
/// its enclosing row (a scalar subquery binds the document as its
/// `$parent`).
fn binds_parent_to_row(expr: &LogicalExpr) -> bool {
	let here = match expr {
		LogicalExpr::Closure(_) => return false,
		LogicalExpr::Filter {
			predicate,
			..
		} => references_parent(predicate),
		LogicalExpr::Lookup {
			base,
			..
		} => any_body_child(expr, base, references_parent),
		// A plan in scalar position: a SELECT reads the row when it [reads its
		// enclosing row](subquery_reads_enclosing_row); a statement plan when
		// one of its scalars, or of its inputs, does.
		LogicalExpr::Subquery(op) => {
			return statement_plan_holds(
				op,
				&mut binds_parent_to_row,
				&mut subquery_reads_enclosing_row,
			);
		}
		// Its bodies bind `$parent` to the destructured value, not the row.
		LogicalExpr::Destructure {
			base,
			..
		} => return binds_parent_to_row(base),
		LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Param(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Field(_)
		| LogicalExpr::Doc
		| LogicalExpr::Current
		| LogicalExpr::Access {
			..
		}
		| LogicalExpr::Index {
			..
		}
		| LogicalExpr::First(_)
		| LogicalExpr::Last(_)
		| LogicalExpr::Each(_)
		| LogicalExpr::Flatten {
			..
		}
		| LogicalExpr::Method {
			..
		}
		| LogicalExpr::Optional {
			..
		}
		| LogicalExpr::Recurse(_)
		| LogicalExpr::RepeatRecurse(_)
		| LogicalExpr::Broadcast {
			..
		}
		| LogicalExpr::SetField {
			..
		}
		| LogicalExpr::Prefix {
			..
		}
		| LogicalExpr::Postfix {
			..
		}
		| LogicalExpr::Invoke {
			..
		}
		| LogicalExpr::Binary {
			..
		}
		| LogicalExpr::Call(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue => false,
	};
	here || any_child(expr, binds_parent_to_row)
}

/// Whether `expr`, a KNN operator's right-hand side, reads `$parent` where the
/// vector's own frame resolves it: the enclosing statement's row, which
/// [`reads_scanned_row`] admits. A `$parent` a `[WHERE …]` or a lookup binds
/// to the document, one in a destructure's bodies, one a SELECT binds to its
/// own enclosing row, and one in a closure literal are not that read; a
/// statement plan in scalar position runs in the enclosing scope and is
/// looked into.
///
/// Where nothing around the statement binds `$parent`, such a read falls back
/// to the document the operator is evaluated against, per row the row being
/// scanned, so the planner refuses it as it refuses that row.
pub(crate) fn reads_enclosing_parent(expr: &LogicalExpr) -> bool {
	match expr {
		LogicalExpr::Param(param) => {
			RowScopeKind::from_param_name(param.as_str()) == Some(RowScopeKind::Parent)
		}
		LogicalExpr::Closure(_) => false,
		LogicalExpr::Subquery(op) => {
			statement_plan_holds(op, &mut reads_enclosing_parent, &mut |_| false)
		}
		LogicalExpr::Filter {
			base,
			..
		}
		| LogicalExpr::Lookup {
			base,
			..
		}
		| LogicalExpr::Destructure {
			base,
			..
		} => reads_enclosing_parent(base),
		LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Field(_)
		| LogicalExpr::Doc
		| LogicalExpr::Current
		| LogicalExpr::Access {
			..
		}
		| LogicalExpr::Index {
			..
		}
		| LogicalExpr::First(_)
		| LogicalExpr::Last(_)
		| LogicalExpr::Each(_)
		| LogicalExpr::Flatten {
			..
		}
		| LogicalExpr::Method {
			..
		}
		| LogicalExpr::Optional {
			..
		}
		| LogicalExpr::Recurse(_)
		| LogicalExpr::RepeatRecurse(_)
		| LogicalExpr::Broadcast {
			..
		}
		| LogicalExpr::SetField {
			..
		}
		| LogicalExpr::Prefix {
			..
		}
		| LogicalExpr::Postfix {
			..
		}
		| LogicalExpr::Invoke {
			..
		}
		| LogicalExpr::Binary {
			..
		}
		| LogicalExpr::Call(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue => any_child(expr, reads_enclosing_parent),
	}
}

/// Whether `param` is `$this` or `$self`, the row being scanned. `$parent`
/// is the enclosing statement's row.
fn is_scanned_row_param(param: &Param) -> bool {
	matches!(
		RowScopeKind::from_param_name(param.as_str()),
		Some(RowScopeKind::This | RowScopeKind::Self_)
	)
}

/// Shared walker backing the public analysis functions. The
/// `AnalysisScope` decides whether a rescoping subquery is entered.
struct ParamWalker<F: FnMut(RowScopeKind) -> bool> {
	predicate: F,
	scope: AnalysisScope,
	found: bool,
	first_kind: Option<RowScopeKind>,
}

impl<F: FnMut(RowScopeKind) -> bool> ParamWalker<F> {
	fn new(predicate: F, scope: AnalysisScope) -> Self {
		Self {
			predicate,
			scope,
			found: false,
			first_kind: None,
		}
	}

	/// Records one parameter reference. Breaks when the predicate accepts it.
	fn visit_param(&mut self, name: &str) -> ControlFlow<()> {
		if let Some(kind) = RowScopeKind::from_param_name(name) {
			if self.first_kind.is_none() {
				self.first_kind = Some(kind);
			}
			if (self.predicate)(kind) {
				self.found = true;
				return ControlFlow::Break(());
			}
		}
		ControlFlow::Continue(())
	}

	/// Walks a scalar and, through each `Subquery` it meets, the plan inside
	/// according to [`Self::walk_op`]. Breaks at the first parameter the
	/// predicate accepts.
	fn walk_expr(&mut self, expr: &LogicalExpr) -> ControlFlow<()> {
		expr.visit(&mut |e| {
			if let LogicalExpr::Param(p) = e {
				self.visit_param(p.as_str())
			} else if let LogicalExpr::Subquery(op) = e {
				self.walk_op(op)
			} else {
				ControlFlow::Continue(())
			}
		})
	}

	/// Walks every parameter a plan reads — a `FROM $param` source and every
	/// scalar — each node's own before its inputs'. Under
	/// [`AnalysisScope::CurrentSelect`] a rescoping plan is a leaf.
	fn walk_op(&mut self, op: &LogicalOp) -> ControlFlow<()> {
		if self.scope == AnalysisScope::CurrentSelect && rescopes_row_params(op) {
			return ControlFlow::Continue(());
		}
		if let LogicalOp::Source(LogicalSource {
			source: SourceRef::Param(param),
			..
		}) = op
		{
			self.visit_param(param.as_str())?;
		}
		for scalar in op.scalars() {
			self.walk_expr(scalar)?;
		}
		for child in op.children() {
			self.walk_op(child)?;
		}
		ControlFlow::Continue(())
	}
}
