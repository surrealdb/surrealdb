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
//! - SELECT subqueries are *not* descended into during analysis: their `$parent` rescopes to the
//!   current SELECT's `$this` and so does not contribute to the current SELECT's outer-row
//!   dependency. This is load-bearing for nested-subquery patterns and pinned by the
//!   `parent_nested_subqueries.surql` and `7184_parent_in_nested_subquery_graph_where.surql`
//!   reproductions.
//!
//! This module is plan-time only. Runtime sites (`parts/filter.rs`,
//! `parts/lookup.rs`, `physical_expr/subquery.rs`, etc.) consume the flags
//! produced here via `WherePart::needs_parent` and friends; they do their
//! own per-row binding through `EvalContext::current_value` /
//! `document_root`. Don't migrate them as part of the same refactor.

use crate::expr::expression::Expr;
use crate::expr::idiom::Idiom;
use crate::expr::part::Part;
use crate::expr::visit::{Visit, Visitor};

/// Row-scoped parameter names.
///
/// Source of truth — `util.rs::SELECT_ITERATION_PARAMS` re-exports this.
pub(crate) const ROW_SCOPED_PARAMS: &[&str] = &["this", "self", "parent"];

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

/// Classification of an `Idiom`'s starting anchor.
///
/// Determines whose row the idiom's field accesses are about:
///
/// - `ThisRow`: idiom is rooted at `$this`/`$self`, or has no explicit `Part::Start`
///   (default-rooted at `$this`). Subsequent `Part::Field` names denote columns of the *current*
///   row's table.
/// - `OuterRow`: idiom is rooted at `$parent`. Subsequent `Part::Field` names denote columns of the
///   *outer* row.
/// - `Opaque`: idiom is rooted at any other `Part::Start(Expr::*)` (computed start, parameter-bound
///   table, etc.). Static analysis of field membership is not possible; callers fall back to "all
///   fields needed."
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IdiomRoot {
	ThisRow,
	OuterRow,
	Opaque,
}

/// Classify an `Idiom`'s starting anchor without descending into nested
/// expressions.
pub(crate) fn classify_idiom_root(idiom: &Idiom) -> IdiomRoot {
	match idiom.0.first() {
		None => IdiomRoot::ThisRow,
		Some(Part::Start(Expr::Param(p))) => match RowScopeKind::from_param_name(p.as_str()) {
			Some(RowScopeKind::This | RowScopeKind::Self_) => IdiomRoot::ThisRow,
			Some(RowScopeKind::Parent) => IdiomRoot::OuterRow,
			None => IdiomRoot::Opaque,
		},
		Some(Part::Start(_)) => IdiomRoot::Opaque,
		// First part is something other than Part::Start (e.g.
		// Part::Field, Part::Lookup) — idiom is unrooted, defaults to
		// $this scope.
		Some(_) => IdiomRoot::ThisRow,
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
/// SELECT scope (subqueries skipped). Replaces the inline
/// `ast_expr_references_parent` visitor in `idiom.rs`.
pub(crate) fn references_parent(expr: &Expr) -> bool {
	let mut walker =
		ParamWalker::new(|kind| kind == RowScopeKind::Parent, AnalysisScope::CurrentSelect);
	let _ = walker.visit_expr(expr);
	walker.found
}

/// Returns the first row-scoped parameter referenced by `expr`, walking
/// at the requested `scope`.
///
/// Used to produce specific diagnostic messages (e.g. distinguishing
/// `$this` from `$parent` in GROUP BY errors). The `scope` parameter
/// matters: see [`AnalysisScope`].
pub(crate) fn first_row_scoped_reference(
	expr: &Expr,
	scope: AnalysisScope,
) -> Option<RowScopeKind> {
	let mut walker = ParamWalker::new(|_| true, scope);
	let _ = walker.visit_expr(expr);
	walker.first_kind
}

/// Shared visitor backing the public analysis functions. The
/// `AnalysisScope` parameter controls whether `visit_select` descends.
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
}

impl<F: FnMut(RowScopeKind) -> bool> Visitor for ParamWalker<F> {
	type Error = std::convert::Infallible;

	fn visit_expr(&mut self, e: &Expr) -> Result<(), Self::Error> {
		if self.found {
			return Ok(());
		}
		if let Expr::Param(p) = e
			&& let Some(kind) = RowScopeKind::from_param_name(p.as_str())
		{
			if self.first_kind.is_none() {
				self.first_kind = Some(kind);
			}
			if (self.predicate)(kind) {
				self.found = true;
				return Ok(());
			}
		}
		e.visit(self)
	}

	fn visit_select(
		&mut self,
		s: &crate::expr::statements::SelectStatement,
	) -> Result<(), Self::Error> {
		match self.scope {
			AnalysisScope::CurrentSelect => Ok(()),
			AnalysisScope::Recursive => s.visit(self),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::syn;

	fn parse_expr(src: &str) -> Expr {
		// Wrap in a RETURN so it parses as a single top-level statement,
		// convert from the parser AST to the runtime AST, and peel off
		// the OutputStatement wrapper to get the inner expression.
		let ast = syn::parse(&format!("RETURN {src};")).expect("parse");
		let mut exprs = ast.expressions;
		assert_eq!(exprs.len(), 1, "expected one statement");
		let top: crate::expr::TopLevelExpr = exprs.remove(0).into();
		match top {
			crate::expr::TopLevelExpr::Expr(Expr::Return(ret)) => ret.what.clone(),
			other => panic!("unexpected statement shape: {other:?}"),
		}
	}

	fn parse_idiom(src: &str) -> Idiom {
		match parse_expr(src) {
			Expr::Idiom(i) => i,
			other => panic!("expected Idiom, got {other:?}"),
		}
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

	// 5. $parent inside DML subquery body — same scoping rule applies.
	#[test]
	fn references_parent_skips_create_subquery() {
		let e = parse_expr("(CREATE x SET y = $parent.id)");
		// Note: CREATE is a statement-level construct; the parser may wrap
		// it as Expr::Create. The ParamWalker doesn't have a special-case
		// for CREATE/UPDATE/DELETE here because they don't rescope the same
		// way SELECT subqueries do — $parent inside a CREATE SET expression
		// IS the outer SELECT's $parent.
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

	// 9. classify_idiom_root: $this rooted idiom → ThisRow.
	#[test]
	fn classify_idiom_root_this() {
		assert_eq!(classify_idiom_root(&parse_idiom("$this.cat")), IdiomRoot::ThisRow);
		assert_eq!(classify_idiom_root(&parse_idiom("$self.cat")), IdiomRoot::ThisRow);
	}

	// 10. classify_idiom_root: $parent rooted → OuterRow.
	#[test]
	fn classify_idiom_root_parent() {
		assert_eq!(classify_idiom_root(&parse_idiom("$parent.cat")), IdiomRoot::OuterRow);
	}

	// 11. classify_idiom_root: unrooted (bare field) → ThisRow.
	#[test]
	fn classify_idiom_root_unrooted() {
		assert_eq!(classify_idiom_root(&parse_idiom("name")), IdiomRoot::ThisRow);
		assert_eq!(classify_idiom_root(&parse_idiom("user.name")), IdiomRoot::ThisRow);
	}

	// 12. classify_idiom_root: opaque starts.
	#[test]
	fn classify_idiom_root_opaque() {
		// $param (non-row-scoped) → opaque
		assert_eq!(classify_idiom_root(&parse_idiom("$other.cat")), IdiomRoot::Opaque);
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

	// 14. $parent inside a Part::Where predicate.
	#[test]
	fn references_parent_in_where_part() {
		assert!(references_parent(&parse_expr("foo[WHERE bar = $parent.id]")));
		assert!(references_parent(&parse_expr("array::find(foo, |$x| $x.bar = $parent.id)")));
	}
}
