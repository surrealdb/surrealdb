//! WHERE-clause analysis and rewriting over the logical scalar type.
//!
//! Predicate inspection (top-level OR, KNN/FTS detection), brute-force KNN
//! parameter extraction, condition stripping after a scan operator has
//! consumed parts of the predicate, MATCHES context collection for index
//! functions, and the record-id point-lookup detector.
//!
//! Every walk here stops at a [`LogicalExpr::Subquery`]: a nested plan is
//! bound and planned on its own, and its predicates do not contribute to the
//! outer scan's analysis. The field reference is any path expression
//! ([`LogicalExpr::is_path`]); the literal side of a comparison is whatever
//! [`try_literal_to_value`] accepts.
//!
//! Index columns come from the catalog as expression-layer idioms. They are
//! compared against logical paths by [`idiom_matches_column`], which is the
//! one place that comparison lives.

use std::collections::HashSet;
use std::convert::Infallible;
use std::ops::{Bound, ControlFlow};

use surrealdb_strand::TableName;
use surrealdb_types::ToSql;

use super::fields::{idiom_is_id, path_steps};
use super::literals::try_literal_to_value;
use crate::catalog::Distance;
use crate::expr::logical::{LogicalExpr, LogicalLiteral, LogicalRecordIdKey};
use crate::expr::operator::{MatchesOperator, NearestNeighbor};
use crate::expr::{BinaryOperator, Idiom, Part};
use crate::val::{Number, Value};

// ============================================================================
// Predicate / Validation Helpers
// ============================================================================

/// Check if a condition has a top-level OR operator.
///
/// Used to prevent LIMIT/START pushdown into Scan when the condition may
/// trigger a multi-index union at runtime. Union streams don't maintain
/// a global ordering, so pushing LIMIT would truncate results arbitrarily.
pub(crate) fn has_top_level_or(cond: Option<&LogicalExpr>) -> bool {
	matches!(
		cond,
		Some(LogicalExpr::Binary {
			op: BinaryOperator::Or,
			..
		})
	)
}

/// Check if an expression contains any KNN (nearest neighbor) operators.
pub(crate) fn has_knn_operator(expr: &LogicalExpr) -> bool {
	scan_knn_operators(expr).found_any
}

/// Check if an expression contains a brute-force KNN operator (`NearestNeighbor::K`).
///
/// Used to distinguish between brute-force KNN with parameter-based vectors
/// (where `extract_bruteforce_knn` fails) and HNSW KNN (`Approximate`).
pub(crate) fn has_knn_k_operator(expr: &LogicalExpr) -> bool {
	scan_knn_operators(expr).found_k
}

/// Check if an expression contains a `KTree` KNN operator (`<|k|>` form).
///
/// `KTree` was backed by the M-Tree index, which has been removed. The
/// streaming planner uses this to distinguish "unsupported variant" from
/// "KNN nested under OR/NOT" when emitting the user-facing error.
pub(crate) fn has_knn_ktree_operator(expr: &LogicalExpr) -> bool {
	scan_knn_operators(expr).found_ktree
}

/// Which KNN operator variants an expression tree contains.
#[derive(Default)]
struct KnnOperators {
	found_any: bool,
	found_k: bool,
	found_ktree: bool,
}

fn scan_knn_operators(expr: &LogicalExpr) -> KnnOperators {
	let mut found = KnnOperators::default();
	let _ = expr.visit(&mut |e| {
		if let LogicalExpr::Binary {
			op: BinaryOperator::NearestNeighbor(nn),
			..
		} = e
		{
			found.found_any = true;
			match nn.as_ref() {
				NearestNeighbor::K(..) => found.found_k = true,
				NearestNeighbor::KTree(..) => found.found_ktree = true,
				NearestNeighbor::Approximate(..) => {}
			}
		}
		ControlFlow::<()>::Continue(())
	});
	found
}

/// Parameters extracted from a brute-force KNN expression.
pub(crate) struct BruteForceKnnParams {
	/// The path to the vector field, a path expression.
	pub field: LogicalExpr,
	/// The query vector.
	pub vector: BruteForceKnnVector,
	/// Number of nearest neighbors.
	pub k: u32,
	/// Distance metric.
	pub distance: Distance,
}

/// The query-vector side of a brute-force KNN expression.
pub(crate) enum BruteForceKnnVector {
	/// A plan-time literal numeric array.
	Literal(Vec<Number>),
	/// A non-literal expression the legacy planner would compute once at
	/// plan time with no document context (bind parameter, function call,
	/// array with computed elements, binary of computables). Evaluated once
	/// at stream open by `KnnTopK` and coerced to `array<number>` with the
	/// same coercion the legacy tree applies.
	Deferred(LogicalExpr),
}

/// Whether the legacy planner's `Tree::eval_value` would compute this
/// expression at plan time (`Node::Computable` / `Node::Computed`). Fields,
/// subqueries, closures, prefix expressions and every other node kind are
/// `Unsupported` there — a brute-force KNN with such a right-hand side never
/// registers an executor entry and evaluates to `false` per row instead.
fn is_plan_time_computable(expr: &LogicalExpr) -> bool {
	match expr {
		// `Tree::eval_array` fully computes array elements, whatever they are.
		LogicalExpr::Array(_) => true,
		LogicalExpr::Literal(
			LogicalLiteral::Integer(_)
			| LogicalLiteral::Bool(_)
			| LogicalLiteral::String(_)
			| LogicalLiteral::Duration(_)
			| LogicalLiteral::Uuid(_)
			| LogicalLiteral::Datetime(_)
			| LogicalLiteral::None
			| LogicalLiteral::Null
			| LogicalLiteral::Decimal(_)
			| LogicalLiteral::Float(_),
		) => true,
		LogicalExpr::RecordId(_) => true,
		LogicalExpr::Param(_) | LogicalExpr::Call(_) => true,
		LogicalExpr::Binary {
			left,
			right,
			..
		} => is_plan_time_computable(left) && is_plan_time_computable(right),
		_ => false,
	}
}

/// Extract brute-force KNN parameters from a WHERE clause.
///
/// Returns the parameters of the first `NearestNeighbor::K(k, dist)`
/// expression, in pre-order, whose left side is a field and whose right side
/// is a literal vector or a plan-time computable expression (see
/// [`BruteForceKnnVector`]). Anything else (a field, a subquery, …) on the
/// right stays in the condition and evaluates to `false` per row, like a
/// legacy KNN expression without an executor entry.
pub(crate) fn extract_bruteforce_knn(cond: &LogicalExpr) -> Option<BruteForceKnnParams> {
	let mut params = None;
	let _ = cond.visit(&mut |e| {
		if let LogicalExpr::Binary {
			left,
			op: BinaryOperator::NearestNeighbor(nn),
			right,
		} = e && let NearestNeighbor::K(k, dist) = nn.as_ref()
			&& left.is_path()
		{
			let vector = if let Some(vector) = extract_literal_vector(right) {
				Some(BruteForceKnnVector::Literal(vector))
			} else if is_plan_time_computable(right) {
				Some(BruteForceKnnVector::Deferred(right.as_ref().clone()))
			} else {
				None
			};
			if let Some(vector) = vector {
				params = Some(BruteForceKnnParams {
					field: left.as_ref().clone(),
					vector,
					k: *k,
					distance: dist.clone().into(),
				});
				return ControlFlow::Break(());
			}
		}
		ControlFlow::Continue(())
	});
	params
}

/// Strip the MATCHES (`@@`) conjuncts a full-text scan answers from a WHERE
/// clause, returning the residual.
///
/// A conjunct is stripped only when it is the scan's own probe: its operator is
/// `operator`, its query is the literal string `query`, and its left side is
/// the index's first column `column`. Every other conjunct — a MATCHES on
/// another field, with another reference or another query — stays in the
/// residual, where it is evaluated per row or refused; the scan's rows say
/// nothing about it.
///
/// Returns `None` when the entire condition is consumed (just a single `@@`),
/// or `Some(residual)` when additional predicates remain (e.g., `content @@ 'x' AND status = 'a'`).
pub(crate) fn strip_fts_condition(
	cond: &LogicalExpr,
	column: &Idiom,
	operator: &MatchesOperator,
	query: &str,
) -> Option<LogicalExpr> {
	strip_and_simplify(cond.clone(), |e| {
		let LogicalExpr::Binary {
			left,
			op: BinaryOperator::Matches(op),
			right,
		} = e
		else {
			return false;
		};
		op == operator
			&& matches!(try_literal_to_value(right), Some(Value::String(s)) if s.as_str() == query)
			&& idiom_matches_column(left, column)
	})
}

/// Whether `e` is a KNN operator a scan operator consumes: `NearestNeighbor::K`
/// (by `KnnTopK`) or `NearestNeighbor::Approximate` (by `KnnScan` via HNSW).
fn is_handled_knn(e: &LogicalExpr) -> bool {
	matches!(
		e,
		LogicalExpr::Binary {
			op: BinaryOperator::NearestNeighbor(nn),
			..
		} if matches!(nn.as_ref(), NearestNeighbor::K(..) | NearestNeighbor::Approximate(..))
	)
}

/// Strip handled KNN operators from a WHERE clause, returning the residual condition.
///
/// Both `NearestNeighbor::K` (consumed by `KnnTopK`) and `NearestNeighbor::Approximate`
/// (consumed by `KnnScan` via HNSW index) are stripped. `KTree` is left in place --
/// the caller should verify the residual contains no remaining KNN operators and
/// return an error if it does.
pub(crate) fn strip_knn_from_condition(cond: &LogicalExpr) -> Option<LogicalExpr> {
	strip_and_simplify(cond.clone(), is_handled_knn)
}

/// Strip handled KNN operators AND every conjunct containing a MATCHES
/// operator from a WHERE clause, returning the in-traversal residual for a
/// `KnnScan`.
///
/// The residual is evaluated per candidate inside the ANN search through the
/// legacy compute path, where MATCHES has no query executor and computes to
/// `false` — leaving a MATCHES conjunct in would reject every candidate and
/// return zero rows. Dropping a conjunct from the in-traversal residual only
/// admits more candidates (the outer `Filter` re-applies the KNN-stripped
/// WHERE — with `MATCHES` evaluated through its physical operator — so final
/// rows stay correct); it can never produce wrong rows.
///
/// The user-visible trade-off when a MATCHES conjunct is NOT exactly
/// bitmap-coverable (no FULLTEXT index, stale index format, negated MATCHES,
/// ...): non-matching candidates occupy top-K slots inside the search, so
/// the statement may return **fewer than k rows even when k matching rows
/// exist** — the k nearest overall are found first and then filtered, rather
/// than the k nearest *matching* rows. Covered conjuncts (an online,
/// current-format FULLTEXT index) avoid it entirely by joining the allow-list.
pub(crate) fn strip_knn_and_matches_from_condition(cond: &LogicalExpr) -> Option<LogicalExpr> {
	strip_and_simplify(cond.clone(), |e| is_handled_knn(e) || expr_contains_matches(e))
}

/// Whether a MATCHES operator appears in `expr`, looking through operator
/// applications and call arguments. Other shapes (a field's own parts, an
/// `IF`, a subquery) are not searched: a MATCHES inside them has no index
/// context of the enclosing scan to resolve against. The logical rules'
/// `mentions_index_operator` answers a different question — whether a
/// conjunct names an index of its statement anywhere inside it, so that no
/// rule moves it out of that statement — and so searches every shape.
pub(crate) fn expr_contains_matches(expr: &LogicalExpr) -> bool {
	match expr {
		LogicalExpr::Binary {
			left,
			op,
			right,
		} => {
			matches!(op, BinaryOperator::Matches(_))
				|| expr_contains_matches(left)
				|| expr_contains_matches(right)
		}
		LogicalExpr::Prefix {
			expr: inner,
			..
		} => expr_contains_matches(inner),
		LogicalExpr::Call(call) => call.args.iter().any(expr_contains_matches),
		_ => false,
	}
}

/// Walk the top-level AND chain of `expr` and replace each leaf for which
/// `should_strip` returns `true` with `true`. After the walk, collapse the
/// boolean sentinels via [`simplify_and_sentinels`]. Returns the rewritten
/// expression, or `None` if the whole condition reduced to `true`.
///
/// Shared body for the per-clause strippers ([`strip_fts_condition`],
/// [`strip_knn_from_condition`], [`strip_index_conditions`]). Does not
/// descend below AND boundaries; KNN/FTS operators nested under OR/NOT
/// remain so the planner can detect and reject those shapes.
fn strip_and_simplify<F>(mut expr: LogicalExpr, mut should_strip: F) -> Option<LogicalExpr>
where
	F: FnMut(&LogicalExpr) -> bool,
{
	fn walk<F: FnMut(&LogicalExpr) -> bool>(expr: &mut LogicalExpr, f: &mut F) {
		if let LogicalExpr::Binary {
			left,
			op: BinaryOperator::And,
			right,
		} = expr
		{
			walk(left, f);
			walk(right, f);
		} else if f(expr) {
			*expr = LogicalExpr::Literal(LogicalLiteral::Bool(true));
		}
	}
	walk(&mut expr, &mut should_strip);
	let Ok(()) = expr.visit_mut(&mut |e| {
		simplify_and_sentinels(e);
		Ok::<_, Infallible>(())
	});
	if matches!(expr, LogicalExpr::Literal(LogicalLiteral::Bool(true))) {
		None
	} else {
		Some(expr)
	}
}

/// Collapses boolean-literal sentinels in one AND node: `true AND x → x`,
/// `x AND true → x`, `true AND true → true`. Applied bottom-up over a
/// condition after stripping, so a chain of consumed leaves collapses to a
/// single `true`.
fn simplify_and_sentinels(expr: &mut LogicalExpr) {
	let is_true = |e: &LogicalExpr| matches!(e, LogicalExpr::Literal(LogicalLiteral::Bool(true)));
	if let LogicalExpr::Binary {
		left,
		op: BinaryOperator::And,
		right,
	} = expr
	{
		match (is_true(left), is_true(right)) {
			(true, true) => *expr = LogicalExpr::Literal(LogicalLiteral::Bool(true)),
			(true, false) => {
				let r = std::mem::replace(right.as_mut(), LogicalExpr::Break);
				*expr = r;
			}
			(false, true) => {
				let l = std::mem::replace(left.as_mut(), LogicalExpr::Break);
				*expr = l;
			}
			(false, false) => {}
		}
	}
}

// ---------------------------------------------------------------------------
// Index column comparison
// ---------------------------------------------------------------------------

/// Whether a logical path names the same path as a catalog index column.
///
/// The path's steps ([`path_steps`]) are compared one to one with the
/// column's parts. Steps that hold no expression compare structurally; a step
/// that does (a filter, an index expression, a method call, a lookup) compares
/// by its rendering, which is identical across the two representations. An
/// expression that is not a path never matches a column.
pub(crate) fn idiom_matches_column(expr: &LogicalExpr, col: &Idiom) -> bool {
	if !expr.is_path() {
		return false;
	}
	let steps = path_steps(expr);
	steps.len() == col.0.len()
		&& steps.iter().zip(col.0.iter()).all(|(step, p)| step_matches(step, p))
}

fn step_matches(step: &LogicalExpr, p: &Part) -> bool {
	match (step, p) {
		(LogicalExpr::Each(_), Part::All)
		| (
			LogicalExpr::Flatten {
				..
			},
			Part::Flatten,
		)
		| (LogicalExpr::Last(_), Part::Last)
		| (LogicalExpr::First(_), Part::First)
		| (
			LogicalExpr::Optional {
				..
			},
			Part::Optional,
		)
		| (LogicalExpr::Doc, Part::Doc)
		| (LogicalExpr::RepeatRecurse(_), Part::RepeatRecurse) => true,
		(LogicalExpr::Field(a), Part::Field(b))
		| (
			LogicalExpr::Access {
				field: a,
				..
			},
			Part::Field(b),
		) => a == b,
		(
			LogicalExpr::Filter {
				..
			}
			| LogicalExpr::Lookup {
				..
			}
			| LogicalExpr::Index {
				..
			}
			| LogicalExpr::Method {
				..
			}
			| LogicalExpr::Destructure {
				..
			}
			| LogicalExpr::Recurse(_),
			_,
		) => step_rendering(step) == p.to_sql(),
		// The expression a path starts from renders as a whole.
		(start, _) if !start.is_path() => start.to_sql() == p.to_sql(),
		_ => false,
	}
}

/// How one step renders on its own: the step's rendering with its base's
/// rendering removed from the front, `[WHERE x]` for a filter, `->likes` for
/// a lookup.
fn step_rendering(step: &LogicalExpr) -> String {
	let sql = step.to_sql();
	let Some(base) = step.step_base() else {
		return sql;
	};
	let base_sql = base.to_sql();
	match sql.strip_prefix(base_sql.as_str()) {
		Some(suffix) => suffix.to_owned(),
		None => sql,
	}
}

/// Whether a field reference in a containment leaf (`field CONTAINS v`,
/// `v INSIDE field`) is served by an array-element index column: the column
/// must flatten (`.*` / `[*]`), and the two paths must agree once every
/// flatten is removed from both.
pub(crate) fn idiom_matches_containment(expr: &LogicalExpr, index_col: &Idiom) -> bool {
	if !expr.is_path() || !index_col.0.iter().any(|p| matches!(p, Part::All)) {
		return false;
	}
	let col_without_all: Vec<&Part> =
		index_col.0.iter().filter(|p| !matches!(p, Part::All)).collect();
	let expr_without_all: Vec<&LogicalExpr> =
		path_steps(expr).into_iter().filter(|s| !matches!(s, LogicalExpr::Each(_))).collect();
	col_without_all.len() == expr_without_all.len()
		&& expr_without_all
			.iter()
			.zip(col_without_all.iter())
			.all(|(step, p)| step_matches(step, p))
}

// ---------------------------------------------------------------------------
// Index condition stripping
// ---------------------------------------------------------------------------

/// Strip conditions covered by a BTree index access path from a WHERE clause.
///
/// Returns `None` when all conditions are consumed (no Filter needed),
/// or `Some(residual)` when conditions remain that the index does not cover.
pub(crate) fn strip_index_conditions(
	cond: &LogicalExpr,
	access: &crate::exec::index::access_path::BTreeAccess,
	cols: &[Idiom],
) -> Option<LogicalExpr> {
	let matcher = IndexConditionMatcher {
		cols,
		access,
	};
	strip_and_simplify(cond.clone(), |e| match e {
		LogicalExpr::Binary {
			left,
			op,
			right,
		} => matcher.matches_access(left, op, right),
		_ => false,
	})
}

/// Strip conditions covered by a union of BTree access paths from a WHERE
/// clause.
///
/// Recognises `field CONTAINSANY [l0, l1, ...]` (field on left) and the
/// symmetric `[l0, l1, ...] ANYINSIDE field` (field on right).  The branches
/// must all share the same index and all use that index's leading
/// array-element column (via [`idiom_matches_containment`]).
///
/// A union over branch values `V` emits every row whose indexed array holds
/// *at least one* member of `V`, so a leaf `field CONTAINSANY L` is only
/// implied — and hence only strippable — when `V ⊆ L`.  The converse
/// (`L ⊆ V`) does **not** imply the leaf: the union may have been built from
/// a different AND conjunct (a nested OR, or another containment leaf) whose
/// values reach outside `L`, and a row admitted through one of those
/// branches need not satisfy `L` at all.  Callers pass the whole WHERE
/// clause, so every top-level AND leaf is tested independently of which
/// conjunct produced the union.
///
/// `CONTAINSALL` / `ALLINSIDE` leaves are deliberately **not** considered
/// covered: their semantics require every value to match the *same* row,
/// but the union returns rows matching *any* value.  A residual `Filter`
/// is left in place above the `UnionIndexScan` to enforce the
/// intersection.  Tracking issue #236 covers a future
/// intersection-operator follow-up.
pub(crate) fn strip_union_index_conditions(
	cond: &LogicalExpr,
	paths: &[crate::exec::index::access_path::AccessPath],
) -> Option<LogicalExpr> {
	use crate::exec::index::access_path::{AccessPath, BTreeAccess};

	// Collect (index_first_column, set_of_branch_values) from the paths.
	// All branches must agree on the index and its first column.  The
	// `HashSet<&Value>` collapses branches that repeat a value (a union
	// built from `[a, a]`), so `union_covers_leaf` tests each distinct
	// branch value once per candidate leaf.
	let mut first_col: Option<&Idiom> = None;
	let mut branch_values: HashSet<&Value> = HashSet::with_capacity(paths.len());
	for path in paths {
		let AccessPath::BTreeScan {
			index_ref,
			access,
			..
		} = path
		else {
			return Some(cond.clone());
		};
		let col = index_ref.definition().cols.first()?;
		match first_col {
			None => first_col = Some(col),
			Some(prev) if prev == col => {}
			Some(_) => return Some(cond.clone()),
		}
		match access {
			BTreeAccess::Equality(v) => {
				branch_values.insert(v);
			}
			BTreeAccess::Compound {
				prefix,
				range: None,
			} if prefix.len() == 1 => {
				branch_values.insert(&prefix[0]);
			}
			_ => return Some(cond.clone()),
		}
	}
	let Some(col) = first_col else {
		return Some(cond.clone());
	};

	strip_and_simplify(cond.clone(), |e| match e {
		LogicalExpr::Binary {
			left,
			op,
			right,
		} => union_covers_leaf(col, &branch_values, left, op, right),
		_ => false,
	})
}

/// Returns `true` when a `CONTAINSANY` (field-left) or `ANYINSIDE`
/// (field-right) leaf is implied by the union, i.e. every branch value is one
/// of the leaf's literals so no branch can admit a row the leaf rejects.
fn union_covers_leaf(
	col: &Idiom,
	branch_values: &HashSet<&crate::val::Value>,
	left: &LogicalExpr,
	op: &BinaryOperator,
	right: &LogicalExpr,
) -> bool {
	let (idiom, lit) = match op {
		BinaryOperator::ContainAny if left.is_path() => (left, right),
		BinaryOperator::AnyInside if right.is_path() => (right, left),
		// CONTAINSALL / ALLINSIDE need intersection; the union does
		// not cover them.  See `strip_union_index_conditions` rustdoc.
		//
		// `field IN [..]` is not covered either, though the union's branches
		// are its literals: the leaf names the column's own value while the
		// entries are per element, so a column holding an array satisfies a
		// branch without satisfying the leaf. The union runs its branches
		// without a predicate, so stripping it would drop the only check
		// that separates the two.
		_ => return false,
	};
	if !idiom_matches_containment(idiom, col) {
		return false;
	}
	let Some(Value::Array(arr)) = try_literal_to_value(lit) else {
		return false;
	};
	// Every branch value must be one of the leaf's literals.  The union emits
	// a row on the strength of a single branch, so a branch value outside the
	// literals would admit rows the leaf rejects.  An empty union guarantees
	// nothing about a row and must never cover a leaf; the `all` below would
	// be vacuously true over it.
	//
	// This also keeps the always-false `field CONTAINSANY []` / `[]
	// ANYINSIDE field` in the residual filter: no non-empty branch set is a
	// subset of the empty literal array.
	//
	// A `HashSet<&Value>` of the literals keeps the per-branch lookup at
	// O(1) average, so a small union checked against a large array literal
	// stays linear.  A hash/`Eq` disagreement across numeric kinds can only
	// miss a match, which leaves the leaf in the residual filter — the safe
	// direction.
	//
	// A NONE or NULL branch never covers the leaf: an empty array is keyed as
	// NULL at its column and an absent field as NONE, so such a branch admits
	// rows holding no matching element.
	if branch_values.is_empty() {
		return false;
	}
	let leaf_values: HashSet<&Value> = arr.0.iter().collect();
	branch_values.iter().all(|v| !v.is_nullish() && leaf_values.contains(*v))
}

/// Decides whether a single binary comparison leaf is covered by a chosen
/// BTree access pattern. Used as the leaf predicate by
/// [`strip_index_conditions`].
struct IndexConditionMatcher<'a> {
	/// Index columns in definition order (`IndexDefinition.cols`).
	cols: &'a [Idiom],
	/// The chosen access pattern describing which conditions are covered.
	access: &'a crate::exec::index::access_path::BTreeAccess,
}

impl IndexConditionMatcher<'_> {
	/// Check whether a binary comparison leaf is covered by the access pattern.
	fn matches_access(&self, left: &LogicalExpr, op: &BinaryOperator, right: &LogicalExpr) -> bool {
		use crate::exec::index::access_path::BTreeAccess;

		// Extract field, value, and the effective operator (normalized so the
		// field is always on the left side of the comparison). `idiom_on_left`
		// tracks the original position because some operator rewrites are
		// only valid when the field was the left operand (see the Inside
		// normalisation below).
		let (idiom, value, effective_op, idiom_on_left) = match (left, right) {
			(i, other) if i.is_path() => match try_literal_to_value(other) {
				Some(v) => (i, v, op.clone(), true),
				None => return false,
			},
			(other, i) if i.is_path() => match try_literal_to_value(other) {
				Some(v) => {
					let flipped = match op {
						BinaryOperator::LessThan => BinaryOperator::MoreThan,
						BinaryOperator::LessThanEqual => BinaryOperator::MoreThanEqual,
						BinaryOperator::MoreThan => BinaryOperator::LessThan,
						BinaryOperator::MoreThanEqual => BinaryOperator::LessThanEqual,
						other => other.clone(),
					};
					(i, v, flipped, false)
				}
				None => return false,
			},
			_ => return false,
		};

		// Normalise the leaf so the strip path sees the same canonical
		// form the analyzer produced: a single-element `field IN [v]` is
		// rewritten by `extract_simple_condition` (analyzer side) to
		// `field = v`, but ONLY when the field is on the left.
		// `[v] IN field` (Right-position) has different runtime
		// semantics — `[v].contains(field)` — and the analyzer correctly
		// produces no candidate for it, so we must not strip it from the
		// residual filter either. Gate the rewrite on `idiom_on_left`.
		let (effective_op, value) = if idiom_on_left
			&& matches!(effective_op, BinaryOperator::Inside)
			&& let Value::Array(arr) = &value
			&& arr.len() == 1
		{
			(BinaryOperator::Equal, arr[0].clone())
		} else {
			(effective_op, value)
		};

		let is_equality =
			matches!(effective_op, BinaryOperator::Equal | BinaryOperator::ExactEqual);

		// `field CONTAINS scalar` (field on left) and `scalar INSIDE field`
		// (field on right) are covered when the matching index column is an
		// array-element flatten (`.*`/`[*]`) and the scalar matches the
		// access's prefix/equality value. Mirrors the analyser's
		// `try_match_containment` so a containment leaf is stripped from the
		// residual, which in turn enables `strip_index_conditions` to report
		// `FullyConsumed` and the SELECT planner to push LIMIT into the scan.
		//
		// A NONE or NULL value is never covered: an empty array has no
		// element to index and is keyed as NULL at its column, and an absent
		// field is keyed as NONE, so an entry carrying either does not prove
		// the column holds such an element.
		let is_containment = ((matches!(effective_op, BinaryOperator::Contain) && idiom_on_left)
			|| (matches!(effective_op, BinaryOperator::Inside) && !idiom_on_left))
			&& !value.is_nullish();

		match self.access {
			BTreeAccess::Compound {
				prefix,
				range,
			} => {
				// Check equality conditions against prefix values.
				if is_equality {
					for (col, val) in self.cols.iter().zip(prefix.iter()) {
						if idiom_matches_column(idiom, col) && value == *val {
							return true;
						}
					}
				}
				// CONTAINS / INSIDE leaf on an array-element column the
				// prefix reaches: the analyser pinned that column to this
				// value, so the scan range already filters to matching
				// rows. Positional like the equality arm above — the
				// analyser extends a prefix across any column a containment
				// leaf pins, not only the leading one.
				if is_containment {
					for (col, val) in self.cols.iter().zip(prefix.iter()) {
						if idiom_matches_containment(idiom, col) && value == *val {
							return true;
						}
					}
				}
				// Check range condition on the column after the prefix.
				if let Some((range_op, range_val)) = range
					&& let Some(col) = self.cols.get(prefix.len())
					&& idiom_matches_column(idiom, col)
				{
					if effective_op == *range_op && value == *range_val {
						return true;
					}
					// `field != NONE` after an equality prefix is encoded
					// as `Some((MoreThan, NONE))` by
					// `analyze_compound_conditions`. Mirror the Range arm
					// so the residual Filter doesn't keep this leaf.
					if matches!(range_op, BinaryOperator::MoreThan)
						&& matches!(range_val, Value::None)
						&& effective_op == BinaryOperator::NotEqual
						&& matches!(value, Value::None)
					{
						return true;
					}
				}
				false
			}
			BTreeAccess::Equality(val) => {
				let Some(col) = self.cols.first() else {
					return false;
				};
				if is_equality && idiom_matches_column(idiom, col) && value == *val {
					return true;
				}
				// CONTAINS / INSIDE on the single-column array-element index:
				// strip the leaf for the same reason as the Compound case.
				if is_containment && idiom_matches_containment(idiom, col) && value == *val {
					return true;
				}
				false
			}
			BTreeAccess::Range {
				range,
			} => {
				let Some(col) = self.cols.first() else {
					return false;
				};
				if !idiom_matches_column(idiom, col) {
					return false;
				}
				// A range literal became this access whole, so the leaf is
				// covered exactly when the literal's bounds are the access's.
				if matches!(effective_op, BinaryOperator::Inside)
					&& idiom_on_left
					&& let Value::Range(r) = &value
					&& **r == *range
				{
					return true;
				}
				match &range.start {
					Bound::Included(x) => {
						if effective_op == BinaryOperator::MoreThanEqual && value == *x {
							return true;
						}
					}
					Bound::Excluded(x) => {
						if effective_op == BinaryOperator::MoreThan && value == *x {
							return true;
						}

						if matches!(x, Value::None)
							&& matches!(value, Value::None)
							&& effective_op == BinaryOperator::NotEqual
						{
							return true;
						}
					}
					Bound::Unbounded => {}
				}

				match &range.end {
					Bound::Included(x) => {
						if effective_op == BinaryOperator::LessThanEqual && value == *x {
							return true;
						}
					}
					Bound::Excluded(x) => {
						if effective_op == BinaryOperator::LessThan && value == *x {
							return true;
						}
					}
					Bound::Unbounded => {}
				}

				false
			}
			// FullText and KNN access types have their own stripping logic.
			_ => false,
		}
	}
}

/// Extract a `Vec<Number>` from an array literal whose every element is a
/// numeric literal.
fn extract_literal_vector(expr: &LogicalExpr) -> Option<Vec<Number>> {
	let LogicalExpr::Array(items) = expr else {
		return None;
	};
	let mut nums = Vec::with_capacity(items.len());
	for elem in items {
		match elem {
			LogicalExpr::Literal(LogicalLiteral::Integer(i)) => nums.push(Number::Int(*i)),
			LogicalExpr::Literal(LogicalLiteral::Float(f)) => nums.push(Number::Float(*f)),
			LogicalExpr::Literal(LogicalLiteral::Decimal(d)) => nums.push(Number::Decimal(*d)),
			_ => return None,
		}
	}
	Some(nums)
}

// ============================================================================
// Record ID Point-Lookup Extraction
// ============================================================================

/// Check whether a WHERE condition contains `id = <RecordId literal>` in its
/// top-level AND chain, where the RecordId's table matches the FROM table.
///
/// Returns the `RecordId` literal expression when found, `None` otherwise.
/// Does NOT extract from OR branches (those require a full table scan).
///
/// This enables the planner to convert `SELECT * FROM table WHERE id = table:x`
/// into a direct point lookup (`RecordIdScan`) instead of a full table scan.
///
/// Only matches point-key RecordIds (not range keys like `table:1..5`).
pub(crate) fn extract_record_id_point_lookup(
	cond: &LogicalExpr,
	table_name: &TableName,
) -> Option<LogicalExpr> {
	find_id_equality_in_and_chain(cond, table_name)
}

/// Walk the top-level AND chain looking for `id = <RecordId literal>`.
fn find_id_equality_in_and_chain(
	expr: &LogicalExpr,
	table_name: &TableName,
) -> Option<LogicalExpr> {
	match expr {
		// AND: check both branches
		LogicalExpr::Binary {
			left,
			op: BinaryOperator::And,
			right,
		} => find_id_equality_in_and_chain(left, table_name)
			.or_else(|| find_id_equality_in_and_chain(right, table_name)),

		// Equality: check for `id = <RecordId>` or `<RecordId> = id`
		LogicalExpr::Binary {
			left,
			op: BinaryOperator::Equal | BinaryOperator::ExactEqual,
			right,
		} => check_id_recordid_pair(left, right, table_name)
			.or_else(|| check_id_recordid_pair(right, left, table_name)),

		// Any other node (OR, comparisons, etc.): no match
		_ => None,
	}
}

/// Check if `idiom_side` is the `id` field and `lit_side` is a matching
/// RecordId literal with a non-range key.
fn check_id_recordid_pair(
	idiom_side: &LogicalExpr,
	lit_side: &LogicalExpr,
	table_name: &TableName,
) -> Option<LogicalExpr> {
	if idiom_is_id(idiom_side)
		&& let LogicalExpr::RecordId(rid) = lit_side
		&& &rid.table == table_name
		&& !matches!(rid.key, LogicalRecordIdKey::Range(_))
	{
		Some(lit_side.clone())
	} else {
		None
	}
}

// ============================================================================
// MATCHES Context Extraction
// ============================================================================

/// Extract MATCHES clause information from a WHERE condition for index functions.
///
/// Accepts an optional `FrozenContext` to resolve bind parameters (`$query`)
/// that appear on the right-hand side of `@N@` operators.
pub(crate) fn extract_matches_context(
	cond: &LogicalExpr,
	ctx: Option<&crate::ctx::FrozenContext>,
) -> crate::exec::function::MatchesContext {
	let mut matches = crate::exec::function::MatchesContext::new();
	let _ = cond.visit(&mut |e| {
		if let LogicalExpr::Binary {
			left,
			op: BinaryOperator::Matches(matches_op),
			right,
		} = e && left.is_path()
		{
			// Extract the query string from the right-hand side.
			// Supports both literal strings and bind parameters.
			let query_str = match right.as_ref() {
				LogicalExpr::Literal(LogicalLiteral::String(s)) => Some(s.as_str().to_owned()),
				LogicalExpr::Param(param) => {
					// Resolve the bind parameter from the frozen context
					ctx.and_then(|ctx| {
						ctx.value(param.as_str()).and_then(|v| {
							if let crate::val::Value::String(s) = v {
								Some(s.as_str().to_owned())
							} else {
								None
							}
						})
					})
				}
				_ => None,
			};

			if let Some(query) = query_str {
				let match_ref = matches_op.rf.unwrap_or(0);
				matches.insert(
					match_ref,
					crate::exec::function::MatchInfo {
						field: left.as_ref().clone(),
						query,
					},
				);
			}
		}
		ControlFlow::<()>::Continue(())
	});
	matches
}

/// The primary table the in-scope MATCHES entries were collected against.
///
/// A scope only carries a table when the SELECT that built it had a literal
/// table source, so this falls back to the placeholder `"unknown"` rather than
/// naming a real table it cannot confirm. That placeholder matches no index, so
/// the index function resolves against no full-text entry and yields `NONE` per
/// row — the same outcome as a MATCHES whose index does not cover the idiom.
pub(crate) fn extract_table_from_matches(
	matches_context: &crate::exec::function::MatchesContext,
) -> TableName {
	if let Some(table) = matches_context.table() {
		return table.clone();
	}
	TableName::from("unknown".to_string())
}

#[cfg(test)]
mod tests {
	use std::str::FromStr;
	use std::sync::Arc;

	use surrealdb_strand::Strand;
	use surrealdb_types::ToSql;

	use super::*;
	use crate::catalog::{Index, IndexDefinition, IndexId};
	use crate::exec::index::access_path::{AccessPath, BTreeAccess, IndexRef};
	use crate::expr::logical::lower_expr;
	use crate::kvs::Direction;

	fn parse_cond(snippet: &str) -> LogicalExpr {
		let src = format!("SELECT * FROM t WHERE {snippet}");
		let mut exprs = crate::syn::parse(&src).expect("parse").expressions;
		assert_eq!(exprs.len(), 1, "expected one statement from {src:?}");
		match exprs.remove(0) {
			crate::sql::TopLevelExpr::Expr(crate::sql::Expr::Select(s)) => {
				lower_expr(s.cond.expect("WHERE").0).expect("lowers")
			}
			other => panic!("expected SELECT, got {other:?}"),
		}
	}

	/// A union of equality scans over an index on `col`, one branch per value.
	fn union_paths(col: &str, values: &[&str]) -> Vec<AccessPath> {
		let def = IndexDefinition {
			index_id: IndexId(1),
			name: Strand::from("ix"),
			table_name: "t".into(),
			cols: vec![Idiom::from_str(col).expect("valid idiom")],
			index: Index::Idx,
			count_cond: None,
			comment: None,
			prepare_remove: false,
			format_version: 1,
		};
		let indexes: Arc<[IndexDefinition]> = Arc::from(vec![def].into_boxed_slice());
		values
			.iter()
			.map(|v| AccessPath::BTreeScan {
				index_ref: IndexRef::new(Arc::clone(&indexes), 0),
				access: BTreeAccess::Equality(Value::from(*v)),
				direction: Direction::Forward,
			})
			.collect()
	}

	/// Residual left after stripping, rendered for comparison. `None` means
	/// the whole condition was consumed and no `Filter` survives.
	fn residual(cond: &str, col: &str, branches: &[&str]) -> Option<String> {
		strip_union_index_conditions(&parse_cond(cond), &union_paths(col, branches))
			.map(|c| c.to_sql())
	}

	#[test]
	fn strips_leaf_matching_the_branch_values() {
		// The union was built from this leaf: branch values == literals.
		assert_eq!(residual("tags CONTAINSANY ['a', 'b']", "tags.*", &["a", "b"]), None);
		assert_eq!(residual("['a', 'b'] ANYINSIDE tags", "tags.*", &["a", "b"]), None);
	}

	#[test]
	fn strips_leaf_wider_than_the_branch_values() {
		// Every branch value is a literal, so each branch implies the leaf.
		assert_eq!(residual("tags CONTAINSANY ['a', 'b', 'c']", "tags.*", &["a", "b"]), None);
	}

	#[test]
	fn keeps_leaf_narrower_than_the_branch_values() {
		// The `'b'` branch admits rows without `'a'`, so the leaf must be
		// re-applied. This is the nested-OR shape: the union comes from the
		// OR conjunct, and the sibling CONTAINSANY is strictly narrower.
		assert_eq!(
			residual(
				"tags CONTAINSANY ['a'] AND (tags CONTAINS 'a' OR tags CONTAINS 'b')",
				"tags.*",
				&["a", "b"],
			)
			.as_deref(),
			Some("tags CONTAINSANY ['a'] AND (tags CONTAINS 'a' OR tags CONTAINS 'b')"),
		);
		// Two containment siblings: the wider leaf is implied by the union,
		// the narrower one is not.
		assert_eq!(
			residual(
				"tags CONTAINSANY ['a', 'b'] AND tags CONTAINSANY ['a']",
				"tags.*",
				&["a", "b"]
			)
			.as_deref(),
			Some("tags CONTAINSANY ['a']"),
		);
	}

	#[test]
	fn keeps_leaf_disjoint_from_the_branch_values() {
		assert_eq!(
			residual("tags CONTAINSANY ['z']", "tags.*", &["a", "b"]).as_deref(),
			Some("tags CONTAINSANY ['z']"),
		);
	}

	#[test]
	fn keeps_empty_containsany_leaf() {
		// `tags CONTAINSANY []` is always FALSE and must reject every row.
		assert_eq!(
			residual("tags CONTAINSANY [] AND tags CONTAINSANY ['a']", "tags.*", &["a"]).as_deref(),
			Some("tags CONTAINSANY []"),
		);
	}

	#[test]
	fn keeps_containsall_leaf() {
		// CONTAINSALL needs intersection; a union of branches cannot prove it.
		assert_eq!(
			residual("tags CONTAINSALL ['a', 'b']", "tags.*", &["a", "b"]).as_deref(),
			Some("tags CONTAINSALL ['a', 'b']"),
		);
	}

	#[test]
	fn keeps_leaf_on_a_different_field() {
		assert_eq!(
			residual("other CONTAINSANY ['a', 'b']", "tags.*", &["a", "b"]).as_deref(),
			Some("other CONTAINSANY ['a', 'b']"),
		);
	}

	#[test]
	fn keeps_leaf_when_index_column_is_not_an_array_flatten() {
		// A scalar column's equality branch says `tags = 'a'`, not
		// `'a' ∈ tags`, so it cannot imply a containment leaf.
		assert_eq!(
			residual("tags CONTAINSANY ['a', 'b']", "tags", &["a", "b"]).as_deref(),
			Some("tags CONTAINSANY ['a', 'b']"),
		);
	}

	#[test]
	fn index_columns_compare_against_logical_paths() {
		let col = |s: &str| Idiom::from_str(s).expect("valid idiom");
		let path = |s: &str| {
			lower_expr(crate::sql::Expr::Idiom(crate::syn::idiom(s).expect("idiom")))
				.expect("lowers")
		};
		let a_b = path("a.b");
		assert!(idiom_matches_column(&a_b, &col("a.b")));
		assert!(!idiom_matches_column(&a_b, &col("a")));
		assert!(!idiom_matches_column(&a_b, &col("a.c")));
		assert!(idiom_matches_containment(&a_b, &col("a.b.*")));
		assert!(idiom_matches_containment(&a_b, &col("a.*.b")));
		assert!(!idiom_matches_containment(&a_b, &col("a.b")));
		// Steps that hold an expression compare by their rendering; a
		// broadcast contributes no step of its own.
		assert!(idiom_matches_column(&path("a.b[0]"), &col("a.b[0]")));
		assert!(idiom_matches_column(&path("a[WHERE x = 1].b"), &col("a[WHERE x = 1].b")));
		assert!(!idiom_matches_column(&path("a[WHERE x = 1].b"), &col("a[WHERE x = 2].b")));
		assert!(idiom_matches_column(&path("a.*"), &col("a.*")));
		assert!(idiom_matches_column(&path("a[*].b"), &col("a[*].b")));
		assert!(!idiom_matches_column(&parse_cond("a + 1"), &col("a")));
	}

	#[test]
	fn knn_and_matches_strippers_leave_the_rest() {
		let cond = parse_cond("v <|2, EUCLIDEAN|> [1, 2] AND a = 1");
		assert!(has_knn_operator(&cond));
		assert!(has_knn_k_operator(&cond));
		assert_eq!(strip_knn_from_condition(&cond).map(|c| c.to_sql()).as_deref(), Some("a = 1"));
		let params = extract_bruteforce_knn(&cond).expect("brute-force KNN");
		assert_eq!(params.k, 2);
		assert!(matches!(params.vector, BruteForceKnnVector::Literal(ref v) if v.len() == 2));
		let title = Idiom::from_str("title").expect("valid idiom");
		let op = |sql: &str| match parse_cond(sql) {
			LogicalExpr::Binary {
				op: BinaryOperator::Matches(op),
				..
			} => op,
			other => panic!("expected a MATCHES, got {}", other.to_sql()),
		};
		let (any, first) = (op("title @@ 'x'"), op("title @1@ 'x'"));
		let strip = |sql: &str, op: &MatchesOperator| {
			strip_fts_condition(&parse_cond(sql), &title, op, "x").map(|c| c.to_sql())
		};
		assert_eq!(strip("title @@ 'x' AND a = 1", &any).as_deref(), Some("a = 1"));
		assert_eq!(strip("title @@ 'x'", &any), None);
		// Only the scan's own probe is consumed: a MATCHES on another field,
		// with another reference or another query, stays in the residual.
		assert_eq!(
			strip("title @1@ 'x' AND body @2@ 'y'", &first).as_deref(),
			Some("body @2@ 'y'")
		);
		assert_eq!(
			strip("title @1@ 'x' AND title @2@ 'x'", &first).as_deref(),
			Some("title @2@ 'x'")
		);
		assert_eq!(
			strip("title @1@ 'x' AND title @1@ $q", &first).as_deref(),
			Some("title @1@ $q")
		);
	}

	#[test]
	fn point_lookup_is_found_in_the_and_chain_only() {
		let t = TableName::from("t");
		assert_eq!(
			extract_record_id_point_lookup(&parse_cond("a = 1 AND id = t:1"), &t)
				.map(|e| e.to_sql())
				.as_deref(),
			Some("t:1")
		);
		assert!(extract_record_id_point_lookup(&parse_cond("a = 1 OR id = t:1"), &t).is_none());
		assert!(extract_record_id_point_lookup(&parse_cond("id = u:1"), &t).is_none());
		assert!(extract_record_id_point_lookup(&parse_cond("id = t:1..5"), &t).is_none());
	}
}
