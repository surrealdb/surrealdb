//! Plan-time row-scope analysis for SELECT statements.
//!
//! Encodes the rules around `$this`, `$self`, and `$parent` references in
//! expressions. All plan-time consumers (selective-scan field extraction,
//! `WherePart::needs_parent`, GROUP BY validation) call into this module so
//! the scoping invariants are enforced in exactly one place.
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
//! `parts/lookup.rs`, `physical_expr/subquery.rs`, etc.) consume the flags
//! produced here via `WherePart::needs_parent` and friends; they do their
//! own per-row binding through `EvalContext::current_value` /
//! `document_root`.

use std::ops::ControlFlow;

use crate::expr::logical::{LogicalExpr, LogicalOp, LogicalSource, SourceRef};

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

/// Whether a plan in scalar position rescopes the row parameters.
///
/// A SELECT — any relational operator — binds `$this` to its own rows and
/// `$parent` to the enclosing row, so a reference inside it is about the
/// subquery's rows. The statement operators (a bare expression, a block,
/// `LET`, `RETURN`, `FOR`, `INFO`, a schema statement, `SLEEP`) run in the
/// enclosing scope and do not.
fn rescopes_row_params(op: &LogicalOp) -> bool {
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
