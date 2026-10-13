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
//! - A SELECT in scalar position is *not* descended into during analysis: its `$parent` rescopes to
//!   the current SELECT's `$this` and so does not contribute to the current SELECT's outer-row
//!   dependency. This is load-bearing for nested-subquery patterns and pinned by the
//!   `parent_nested_subqueries.surql` and `7184_parent_in_nested_subquery_graph_where.surql`
//!   reproductions. A block, `LET`, `RETURN` or `FOR` in scalar position runs in the enclosing
//!   scope and is descended into.
//!
//! This module is plan-time only. Runtime sites (`parts/filter.rs`,
//! `parts/lookup.rs`, etc.) consume the flags
//! produced here via `WherePart::needs_parent` and friends; they do their
//! own per-row binding through `EvalContext::current_value` /
//! `document_root`.

use std::ops::ControlFlow;

use crate::exec::function::FunctionRegistry;
use crate::expr::logical::{LogicalExpr, LogicalOp, LogicalSource, SourceRef};
use crate::expr::{Function, Param};

/// Row-scoped parameter names: the logical layer's `ITERATION_PARAMS`,
/// which its effect fold reads too. `util::SELECT_ITERATION_PARAMS` re-exports
/// this, and the binding passes receive it as `BindContext::row_scoped`.
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

/// Whose row a path's field reads are about, decided by the expression the
/// path starts from ([`LogicalExpr::path_root`]).
///
/// - `ThisRow`: the path starts at a field of the row, at the row itself (a leading lookup, `@`) or
///   at `$this`/`$self`. Its field reads name columns of the *current* row's table.
/// - `OuterRow`: the path starts at `$parent`. Its field reads name columns of the *outer* row.
/// - `Opaque`: the path starts at any other expression (another parameter, a computed value). Which
///   fields it reads cannot be known statically; callers fall back to "all fields needed".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PathRoot {
	ThisRow,
	OuterRow,
	Opaque,
}

/// Classify the expression a path starts from, without descending into the
/// steps applied to it. An expression that is not a path is its own root.
pub(crate) fn classify_path_root(path: &LogicalExpr) -> PathRoot {
	match path.path_root() {
		LogicalExpr::Field(_) | LogicalExpr::Doc | LogicalExpr::Current => PathRoot::ThisRow,
		LogicalExpr::Param(p) => match RowScopeKind::from_param_name(p.as_str()) {
			Some(RowScopeKind::This | RowScopeKind::Self_) => PathRoot::ThisRow,
			Some(RowScopeKind::Parent) => PathRoot::OuterRow,
			None => PathRoot::Opaque,
		},
		_ => PathRoot::Opaque,
	}
}

/// How deeply row-scope analysis should walk an expression tree.
///
/// Two consumers care about different rules:
///
/// - `CurrentSelect`: stop at SELECT subquery boundaries. Used when the analysis informs the
///   *current* SELECT's optimisation decisions (e.g. `WherePart::needs_parent`). A subquery's
///   `$parent` rescopes to the current SELECT's `$this` and so does not contribute.
/// - `Recursive`: descend through SELECT subqueries. Used when the analysis is a blanket
///   prohibition. GROUP BY validation is the prototypical case: a `$parent` reference anywhere
///   inside a grouped projection — even inside a subquery whose `$parent` nominally resolves to the
///   outer SELECT's `$this` — is invalid, because that `$this` is the iteration document the GROUP
///   BY has collapsed away.
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
		| LogicalOp::Sort(_) => op.scalars().into_iter().any(references_parent_at_row_level),
		_ => op
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
		_ => any_child(expr, references_parent_at_row_level),
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
		_ => any_child(expr, source_reads_enclosing_row),
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
		_ => any_child(expr, |child| row_param_outside_closures(child, accept)),
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
		_ => any_child(expr, |child| reads_value_in(child, scope, registry)),
	}
}

/// Whether `expr`, evaluated where a value is in scope, may read that value. It
/// does where it reads it itself — a field, `@`, the value as the root of a
/// path, `$this`/`$self`, a projection builtin, or a statement in scalar
/// position that does, outside the bodies a step evaluates against an element
/// and outside closure literals — and it may wherever it holds code whose
/// reads the expression does not show, which is taken to read it: a call of a
/// user-defined function, a script or a module
/// ([`evaluates_against_row`](crate::exec::planner::util::evaluates_against_row)),
/// a closure invoked or a method called (which may be a closure an object
/// holds), and a statement in scalar position, whose `$parent` is that value.
/// A lowering that would hand `expr` the value in scope can skip doing so only
/// when it may read none.
pub(crate) fn may_read_value_in_scope(expr: &LogicalExpr, registry: &FunctionRegistry) -> bool {
	reads_value_in(expr, ValueInScope::Row, registry)
		|| expr.any(|node| match node {
			LogicalExpr::Call(call) => {
				crate::exec::planner::util::evaluates_against_row(call, registry)
			}
			LogicalExpr::Invoke {
				..
			}
			| LogicalExpr::Method {
				..
			}
			| LogicalExpr::Subquery(_) => true,
			_ => false,
		})
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
		_ => false,
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
		_ => any_child(expr, reads_enclosing_parent),
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
		expr.visit(&mut |e| match e {
			LogicalExpr::Param(p) => self.visit_param(p.as_str()),
			LogicalExpr::Subquery(op) => self.walk_op(op),
			_ => ControlFlow::Continue(()),
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

#[cfg(test)]
mod tests {
	use super::*;
	use crate::expr::logical::lower_expr;

	fn parse_expr(src: &str) -> LogicalExpr {
		// Wrap in a RETURN so it parses as a single top-level statement,
		// then lower the returned expression.
		let ast = crate::syn::parse(&format!("RETURN {src};")).expect("parse");
		let mut exprs = ast.expressions;
		assert_eq!(exprs.len(), 1, "expected one statement");
		let surrealdb_expr::parser::TopLevelExpr::Expr(crate::expr::Expr::Return(out)) =
			exprs.remove(0)
		else {
			panic!("unexpected statement shape");
		};
		lower_expr(out.what).expect("lowers")
	}

	// 1. Top-level $parent.x
	#[test]
	fn references_parent_top_level() {
		assert!(references_parent(&parse_expr("$parent.x")));
		assert_eq!(
			first_row_scoped_reference(&parse_expr("$parent.x"), AnalysisScope::CurrentSelect),
			Some(RowScopeKind::Parent),
		);
	}

	// 2. $parent nested inside method-call arguments
	#[test]
	fn references_parent_in_method_arg() {
		assert!(references_parent(&parse_expr("array::find($parent.refs, 'foo')")));
	}

	// 3. $parent inside a graph-traversal WHERE part
	#[test]
	fn references_parent_in_graph_where() {
		assert!(references_parent(&parse_expr("user->knows[WHERE out = $parent.id]")));
	}

	// 4. $parent inside an ORDER BY subquery — analysis must NOT detect it on the outer expression
	//    because subqueries rescope $parent.
	#[test]
	fn references_parent_skips_select_subquery() {
		// `(SELECT ... FROM $parent->edge ORDER BY x)` as part of a larger
		// expression: the inner $parent is the SUBQUERY's parent, which is
		// the *outer* row. From the OUTER expression's perspective there is
		// no $parent dependency.
		let e = parse_expr("(SELECT * FROM $parent->edge ORDER BY x)");
		assert!(!references_parent(&e));
	}

	// 5. $parent inside a block in scalar position — a block runs in the enclosing scope, so its
	//    `$parent` IS the outer SELECT's `$parent`.
	#[test]
	fn references_parent_enters_block() {
		let e = parse_expr("{ RETURN $parent.id }");
		assert!(references_parent(&e));
		let e = parse_expr("{ LET $x = $parent.id; RETURN $x }");
		assert!(references_parent(&e));
	}

	// 6. $parent deep field path
	#[test]
	fn references_parent_deep_path() {
		assert!(references_parent(&parse_expr("$parent.visibility.tags")));
	}

	// 7. Nested SELECT — outer's analysis ignores inner $parent. `outer SELECT { inner SELECT {
	//    $parent.x } }`: the outer has no $parent reference, only the inner does (and that's the
	//    outer's $this, not propagated by this analyser).
	#[test]
	fn nested_select_does_not_propagate_inner_parent() {
		let e = parse_expr("(SELECT (SELECT * FROM tbl WHERE id = $parent.id) FROM x)");
		assert!(!references_parent(&e));
	}

	// 8. Field literally named `parent` (not $parent) — must NOT trigger.
	#[test]
	fn bare_parent_field_is_not_param() {
		let e = parse_expr("parent.sub");
		assert!(!references_parent(&e));
		assert!(first_row_scoped_reference(&e, AnalysisScope::Recursive).is_none());
	}

	// 9. classify_path_root: a path from $this → ThisRow.
	#[test]
	fn classify_path_root_this() {
		assert_eq!(classify_path_root(&parse_expr("$this.cat")), PathRoot::ThisRow);
		assert_eq!(classify_path_root(&parse_expr("$self.cat")), PathRoot::ThisRow);
		assert_eq!(classify_path_root(&parse_expr("$this.a.b[0]")), PathRoot::ThisRow);
	}

	// 10. classify_path_root: a path from $parent → OuterRow.
	#[test]
	fn classify_path_root_parent() {
		assert_eq!(classify_path_root(&parse_expr("$parent.cat")), PathRoot::OuterRow);
	}

	// 11. classify_path_root: a path from the row (a field, a lookup) → ThisRow.
	#[test]
	fn classify_path_root_unrooted() {
		assert_eq!(classify_path_root(&parse_expr("name")), PathRoot::ThisRow);
		assert_eq!(classify_path_root(&parse_expr("user.name")), PathRoot::ThisRow);
		assert_eq!(classify_path_root(&parse_expr("->likes->person")), PathRoot::ThisRow);
	}

	// 12. classify_path_root: opaque starts.
	#[test]
	fn classify_path_root_opaque() {
		// $param (non-row-scoped) → opaque
		assert_eq!(classify_path_root(&parse_expr("$other.cat")), PathRoot::Opaque);
		assert_eq!(classify_path_root(&parse_expr("(SELECT * FROM t).cat")), PathRoot::Opaque);
	}

	// 13. first_row_scoped_reference distinguishes $this from $parent.
	#[test]
	fn references_any_for_this_only() {
		let e = parse_expr("$this.x + 1");
		assert!(!references_parent(&e));
		assert_eq!(
			first_row_scoped_reference(&e, AnalysisScope::CurrentSelect),
			Some(RowScopeKind::This),
		);
	}

	// 14. $parent inside a WHERE part and inside a closure body.
	#[test]
	fn references_parent_in_where_part() {
		assert!(references_parent(&parse_expr("foo[WHERE bar = $parent.id]")));
		assert!(references_parent(&parse_expr("array::find(foo, |$x| $x.bar = $parent.id)")));
	}

	// 15. AnalysisScope::Recursive descends into SELECT subqueries. GROUP BY validation depends on
	//     this: `(SELECT $parent FROM ...)` in a grouped projection must error even though the
	//     subquery rescopes the inner $parent — the outer's $this is undefined under GROUP BY and
	//     the inner reference still names it.
	#[test]
	fn recursive_scope_descends_into_subqueries() {
		let e = parse_expr("(SELECT $parent FROM 1)");
		assert!(!references_parent(&e), "current-scope walk skips subqueries");
		assert_eq!(
			first_row_scoped_reference(&e, AnalysisScope::Recursive),
			Some(RowScopeKind::Parent),
			"recursive walk must find $parent inside the subquery",
		);
	}

	// 17. A row parameter as the `FROM` source of a subquery is a reference like any other: the
	//     recursive walk finds it (GROUP BY validation), the current-scope walk does not (the
	//     subquery rescopes it).
	#[test]
	fn param_source_of_a_subquery_is_a_reference() {
		let e = parse_expr("(SELECT id FROM $parent)");
		assert!(!references_parent(&e), "current-scope walk skips subqueries");
		assert_eq!(
			first_row_scoped_reference(&e, AnalysisScope::Recursive),
			Some(RowScopeKind::Parent),
		);
		let e = parse_expr("(SELECT count() FROM $this GROUP ALL)");
		assert_eq!(
			first_row_scoped_reference(&e, AnalysisScope::Recursive),
			Some(RowScopeKind::This),
		);
		let e = parse_expr("(SELECT id FROM $other)");
		assert!(first_row_scoped_reference(&e, AnalysisScope::Recursive).is_none());
	}

	// 18. subquery_reads_enclosing_row: a subquery reads the row it is evaluated for through a
	//     `$parent` at its own level or a row-scoped parameter in its source; a `$this` in its own
	//     clauses is its own row, and a SELECT nested in it rescopes again.
	#[test]
	fn subquery_reads_enclosing_row_follows_the_subquery_scope() {
		let reads = |src: &str| match parse_expr(src) {
			LogicalExpr::Subquery(op) => subquery_reads_enclosing_row(&op),
			other => panic!("expected a subquery, got {other:?}"),
		};
		assert!(reads("(SELECT VALUE v FROM q WHERE id = $parent.r)"));
		assert!(reads("(SELECT VALUE v FROM $this)"));
		assert!(reads("(SELECT VALUE v FROM $parent.items)"));
		assert!(reads("(SELECT VALUE $parent.v FROM q)"));
		assert!(!reads("(SELECT VALUE v FROM q)"));
		assert!(!reads("(SELECT VALUE v FROM q WHERE $this.ok)"));
		assert!(!reads("(SELECT VALUE (SELECT VALUE x FROM t WHERE y = $parent.z) FROM q)"));
	}

	// 19. subquery_reads_enclosing_row: per row, a `[WHERE …]`, a lookup's clauses, a destructure
	//     body and a block bind `$parent` to the plan's own row or a value derived from it, and a
	//     closure literal is a value wherever it sits; their bases, the source and the plan's own
	//     level still read the enclosing row.
	#[test]
	fn subquery_reads_enclosing_row_follows_document_rebinding() {
		let reads = |src: &str| match parse_expr(src) {
			LogicalExpr::Subquery(op) => subquery_reads_enclosing_row(&op),
			other => panic!("expected a subquery, got {other:?}"),
		};
		for src in [
			"(SELECT VALUE [[5f, 0f]][WHERE $parent.enabled][0] FROM ONLY { enabled: true })",
			"(SELECT VALUE person:a->likes[WHERE w = $parent.w][0].v FROM ONLY { w: 4 })",
			"(SELECT VALUE [[5f, 0f]][WHERE $parent.enabled][0] FROM qv LIMIT 1)",
			"(SELECT VALUE v FROM qv WHERE [1][WHERE $parent.enabled])",
			"(SELECT VALUE { o: 1 }.{ v: [1][WHERE $parent.o] } FROM ONLY { e: 1 })",
			"(SELECT VALUE v FROM ONLY $o.{ v: [[5f, 0f]][WHERE $parent.enabled][0] })",
			"(SELECT VALUE v FROM ONLY $o.{ v: person:a->likes[WHERE w = $parent.w][0].v })",
			"(SELECT VALUE v FROM ONLY $o.{ v: (SELECT VALUE v FROM qv WHERE n = $parent.n)[0] })",
			"(SELECT VALUE v FROM ONLY $o.{ v: $this.v })",
			"(SELECT VALUE { RETURN $parent.v } FROM qv)",
			"(SELECT VALUE [1].map(|$y| $parent.v) FROM qv)",
			"(SELECT VALUE fn::constant(|$x| $parent.point) FROM ONLY { enabled: true })",
			"(SELECT VALUE v FROM fn::constant(|$x| $parent.point))",
			"(SELECT VALUE v FROM qv LIMIT fn::constant(|$x| $parent.n)[0])",
		] {
			assert!(!reads(src), "{src}");
		}
		for src in [
			"(SELECT VALUE v FROM qv WHERE n = $parent.point[0])",
			"(SELECT VALUE $parent.v FROM qv)",
			"(SELECT VALUE v FROM $parent.items)",
			"(SELECT VALUE v FROM [{ v: 1 }][WHERE $parent.enabled])",
			"(SELECT VALUE $parent.vs[WHERE enabled][0] FROM ONLY { k: 1 })",
			"(SELECT VALUE v FROM qv LIMIT [1][WHERE $parent.n][0])",
			"(SELECT VALUE { o: 1 }.{ v: $parent.o } FROM ONLY { e: 1 })",
			"(SELECT VALUE v FROM ONLY $o.{ v: $parent.v })",
			"(SELECT VALUE v FROM $vs[WHERE $parent.enabled])",
			"(SELECT VALUE v FROM ONLY $parent.{ v })",
			"(SELECT VALUE v FROM ONLY $this.{ v })",
			"(SELECT VALUE v FROM { RETURN [{ v: $parent.v }] })",
		] {
			assert!(reads(src), "{src}");
		}
	}

	// 20. subquery_reads_enclosing_row: a table's `PERMISSIONS FOR select` predicate is evaluated
	//     per row of the plan with that row as its document, as the WHERE condition is: a `[WHERE
	//     …]` or a lookup in it binds `$parent` to the protected row, while a bare `$parent` is the
	//     enclosing row.
	#[test]
	fn subquery_reads_enclosing_row_classifies_a_permission_predicate_per_row() {
		let guarded = |predicate: &str| {
			let LogicalExpr::Subquery(op) = parse_expr("(SELECT VALUE v FROM p)") else {
				panic!("expected a subquery");
			};
			let LogicalOp::Project(mut project) = *op else {
				panic!("expected a projection over the source");
			};
			project.input = Box::new(LogicalOp::PermissionFilter(
				crate::expr::logical::LogicalPermissionFilter {
					input: project.input,
					predicate: parse_expr(predicate),
					auth: None,
					guard: crate::expr::logical::GuardKind::Read,
				},
			));
			subquery_reads_enclosing_row(&LogicalOp::Project(project))
		};
		for predicate in [
			"array::len(acl[WHERE $parent.enabled]) > 0",
			"array::len(person:a->likes[WHERE w = $parent.w]) > 0",
			"array::len(->likes[WHERE w = $parent.w]) >= 0",
			"$this.x = 1",
			"enabled = true",
		] {
			assert!(!guarded(predicate), "{predicate}");
		}
		assert!(guarded("$parent.x = 1"));
	}

	// 16. A SELECT nested inside a block is still a SELECT scope.
	#[test]
	fn select_inside_block_still_rescopes() {
		let e = parse_expr("{ RETURN (SELECT * FROM t WHERE a = $parent.id) }");
		assert!(!references_parent(&e));
		assert_eq!(
			first_row_scoped_reference(&e, AnalysisScope::Recursive),
			Some(RowScopeKind::Parent),
		);
	}
}
