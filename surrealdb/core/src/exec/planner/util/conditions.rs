//! WHERE-clause analysis and rewriting.
//!
//! Predicate inspection (top-level OR, KNN/FTS detection), brute-force KNN
//! parameter extraction, condition stripping after a scan operator has
//! consumed parts of the predicate, MATCHES context collection for index
//! functions, and the record-id point-lookup detector.
//!
//! The visitors here all stop at SELECT subquery boundaries: subqueries
//! have their own planning pass and their predicates do not contribute to
//! the outer scan's analysis.

use std::collections::HashSet;

use super::literals::try_literal_to_value;
use crate::catalog::Distance;
use crate::exec::index::analysis::idiom_matches_containment;
use crate::expr::operator::NearestNeighbor;
use crate::expr::visit::{MutVisitor, Visit, VisitMut, Visitor};
use crate::expr::{BinaryOperator, Cond, Expr, Idiom, Literal};
use crate::val::Number;

// ============================================================================
// Predicate / Validation Helpers
// ============================================================================

/// Check if a condition has a top-level OR operator.
///
/// Used to prevent LIMIT/START pushdown into Scan when the condition may
/// trigger a multi-index union at runtime. Union streams don't maintain
/// a global ordering, so pushing LIMIT would truncate results arbitrarily.
pub(crate) fn has_top_level_or(cond: Option<&Cond>) -> bool {
	match cond {
		Some(c) => matches!(
			c.0,
			Expr::Binary {
				op: BinaryOperator::Or,
				..
			}
		),
		None => false,
	}
}

/// Check if an expression contains any KNN (nearest neighbor) operators.
pub(crate) fn has_knn_operator(expr: &Expr) -> bool {
	scan_knn_operators(expr).found_any
}

/// Check if an expression contains a brute-force KNN operator (`NearestNeighbor::K`).
///
/// Used to distinguish between brute-force KNN with parameter-based vectors
/// (where `extract_bruteforce_knn` fails) and HNSW KNN (`Approximate`).
pub(crate) fn has_knn_k_operator(expr: &Expr) -> bool {
	scan_knn_operators(expr).found_k
}

/// Check if an expression contains a `KTree` KNN operator (`<|k|>` form).
///
/// `KTree` was backed by the M-Tree index, which has been removed. The
/// streaming planner uses this to distinguish "unsupported variant" from
/// "KNN nested under OR/NOT" when emitting the user-facing error.
pub(crate) fn has_knn_ktree_operator(expr: &Expr) -> bool {
	scan_knn_operators(expr).found_ktree
}

fn scan_knn_operators(expr: &Expr) -> KnnOperatorChecker {
	let mut checker = KnnOperatorChecker {
		found_any: false,
		found_k: false,
		found_ktree: false,
	};
	let _ = checker.visit_expr(expr);
	checker
}

/// Visitor that detects the presence of KNN operators in an expression tree.
struct KnnOperatorChecker {
	found_any: bool,
	found_k: bool,
	found_ktree: bool,
}

impl Visitor for KnnOperatorChecker {
	type Error = std::convert::Infallible;

	fn visit_expr(&mut self, expr: &Expr) -> Result<(), Self::Error> {
		if let Expr::Binary {
			op: BinaryOperator::NearestNeighbor(nn),
			..
		} = expr
		{
			self.found_any = true;
			match nn.as_ref() {
				NearestNeighbor::K(..) => self.found_k = true,
				NearestNeighbor::KTree(..) => self.found_ktree = true,
				NearestNeighbor::Approximate(..) => {}
			}
		}
		expr.visit(self)
	}

	// Don't descend into subqueries -- only check outer WHERE.
	fn visit_select(&mut self, _: &crate::expr::SelectStatement) -> Result<(), Self::Error> {
		Ok(())
	}
}

/// Parameters extracted from a brute-force KNN expression.
pub(crate) struct BruteForceKnnParams {
	/// The idiom path to the vector field.
	pub field: Idiom,
	/// The query vector.
	pub vector: Vec<Number>,
	/// Number of nearest neighbors.
	pub k: u32,
	/// Distance metric.
	pub distance: Distance,
}

/// Extract brute-force KNN parameters from a WHERE clause.
///
/// Returns the parameters if a `NearestNeighbor::K(k, dist)` expression is
/// found at the top level of AND-connected conditions.
pub(crate) fn extract_bruteforce_knn(cond: &Cond) -> Option<BruteForceKnnParams> {
	let mut expr = cond.0.clone();
	let mut extractor = BruteForceKnnExtractor {
		params: None,
	};
	let _ = extractor.visit_mut_expr(&mut expr);
	extractor.params
}

/// Strip the MATCHES (`@@`) predicate from a WHERE clause, returning the residual.
///
/// Returns `None` when the entire condition is consumed (just a single `@@`),
/// or `Some(residual)` when additional predicates remain (e.g., `content @@ 'x' AND status = 'a'`).
pub(crate) fn strip_fts_condition(cond: &Cond) -> Option<Cond> {
	strip_and_simplify(cond.0.clone(), |e| {
		matches!(
			e,
			Expr::Binary {
				op: BinaryOperator::Matches(_),
				..
			}
		)
	})
	.map(Cond)
}

/// Strip handled KNN operators from a WHERE clause, returning the residual condition.
///
/// Both `NearestNeighbor::K` (consumed by `KnnTopK`) and `NearestNeighbor::Approximate`
/// (consumed by `KnnScan` via HNSW index) are stripped. `KTree` is left in place --
/// the caller should verify the residual contains no remaining KNN operators and
/// return an error if it does.
pub(crate) fn strip_knn_from_condition(cond: &Cond) -> Option<Cond> {
	strip_and_simplify(cond.0.clone(), |e| {
		matches!(
			e,
			Expr::Binary {
				op: BinaryOperator::NearestNeighbor(nn),
				..
			} if matches!(nn.as_ref(), NearestNeighbor::K(..) | NearestNeighbor::Approximate(..))
		)
	})
	.map(Cond)
}

/// Walk the top-level AND chain of `expr` and replace each leaf for which
/// `should_strip` returns `true` with `Literal::Bool(true)`. After the
/// walk, collapse boolean sentinels via [`BoolSimplifier`]. Returns the
/// rewritten expression, or `None` if the whole condition reduced to
/// `true`.
///
/// Shared body for the per-clause strippers ([`strip_fts_condition`],
/// [`strip_knn_from_condition`], [`strip_index_conditions`]). Does not
/// descend below AND boundaries; KNN/FTS operators nested under OR/NOT
/// remain so the planner can detect and reject those shapes.
fn strip_and_simplify<F>(mut expr: Expr, mut should_strip: F) -> Option<Expr>
where
	F: FnMut(&Expr) -> bool,
{
	fn walk<F: FnMut(&Expr) -> bool>(expr: &mut Expr, f: &mut F) {
		if let Expr::Binary {
			left,
			op: BinaryOperator::And,
			right,
		} = expr
		{
			walk(left, f);
			walk(right, f);
		} else if f(expr) {
			*expr = Expr::Literal(Literal::Bool(true));
		}
	}
	walk(&mut expr, &mut should_strip);
	let _ = BoolSimplifier.visit_mut_expr(&mut expr);
	if matches!(expr, Expr::Literal(Literal::Bool(true))) {
		None
	} else {
		Some(expr)
	}
}

// ---------------------------------------------------------------------------
// Index condition stripping
// ---------------------------------------------------------------------------

/// Strip conditions covered by a BTree index access path from a WHERE clause.
///
/// Returns `None` when all conditions are consumed (no Filter needed),
/// or `Some(residual)` when conditions remain that the index does not cover.
pub(crate) fn strip_index_conditions(
	cond: &Cond,
	access: &crate::exec::index::access_path::BTreeAccess,
	cols: &[Idiom],
) -> Option<Cond> {
	let matcher = IndexConditionMatcher {
		cols,
		access,
	};
	strip_and_simplify(cond.0.clone(), |e| match e {
		Expr::Binary {
			left,
			op,
			right,
		} => matcher.matches_access(left, op, right),
		_ => false,
	})
	.map(Cond)
}

/// Strip conditions covered by a union of BTree access paths from a WHERE
/// clause.
///
/// Recognises `field CONTAINSANY [v0, v1, ...]` (idiom on left) and the
/// symmetric `[v0, v1, ...] ANYINSIDE field` (idiom on right) when every
/// literal value appears as the leading equality/prefix value of some
/// branch.  The branches must all share the same index and all use that
/// index's leading array-element column (via `idiom_matches_containment`).
///
/// `CONTAINSALL` / `ALLINSIDE` leaves are deliberately **not** considered
/// covered: their semantics require every value to match the *same* row,
/// but the union returns rows matching *any* value.  A residual `Filter`
/// is left in place above the `UnionIndexScan` to enforce the
/// intersection.  Tracking issue #236 covers a future
/// intersection-operator follow-up.
pub(crate) fn strip_union_index_conditions(
	cond: &Cond,
	paths: &[crate::exec::index::access_path::AccessPath],
) -> Option<Cond> {
	use crate::exec::index::access_path::{AccessPath, BTreeAccess};
	use crate::val::Value;

	// Collect (index_first_column, set_of_branch_values) from the paths.
	// All branches must agree on the index and its first column.  A
	// `HashSet<&Value>` keeps `union_covers_leaf`'s per-literal lookup
	// at O(1) average — important when a query mixes a small union
	// (≤ MAX_IN_EXPANSION_SIZE branches) with a large array literal on
	// some other AND-leaf that the matcher still has to check.
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

	strip_and_simplify(cond.0.clone(), |e| match e {
		Expr::Binary {
			left,
			op,
			right,
		} => union_covers_leaf(col, &branch_values, left, op, right),
		_ => false,
	})
	.map(Cond)
}

/// Returns `true` when a `CONTAINSANY` (idiom-left) or `ANYINSIDE`
/// (idiom-right) leaf is fully covered by the set of branch values.
fn union_covers_leaf(
	col: &Idiom,
	branch_values: &HashSet<&crate::val::Value>,
	left: &Expr,
	op: &BinaryOperator,
	right: &Expr,
) -> bool {
	use crate::exec::index::analysis::idiom_matches_containment;
	use crate::val::Value;

	let (idiom, lit) = match op {
		BinaryOperator::ContainAny => match (left, right) {
			(Expr::Idiom(i), Expr::Literal(l)) => (i, l),
			_ => return false,
		},
		BinaryOperator::AnyInside => match (left, right) {
			(Expr::Literal(l), Expr::Idiom(i)) => (i, l),
			_ => return false,
		},
		// CONTAINSALL / ALLINSIDE need intersection; the union does
		// not cover them.  See `strip_union_index_conditions` rustdoc.
		_ => return false,
	};
	if !idiom_matches_containment(idiom, col) {
		return false;
	}
	let Some(Value::Array(arr)) = try_literal_to_value(lit) else {
		return false;
	};
	// `field CONTAINSANY []` / `[] ANYINSIDE field` evaluates to FALSE
	// (no values to match), so the leaf must stay in the residual
	// filter to reject all rows.  `arr.0.iter().all(...)` would be
	// vacuously true on the empty array, silently dropping the
	// always-false constraint.
	if arr.0.is_empty() {
		return false;
	}
	// Every literal value must appear in some branch's prefix.
	arr.0.iter().all(|v| branch_values.contains(v))
}

/// Decides whether a single binary comparison leaf is covered by a chosen
/// BTree access pattern. Used as the leaf predicate by
/// [`strip_index_conditions`].
struct IndexConditionMatcher<'a> {
	/// Index columns in definition order.
	cols: &'a [Idiom],
	/// The chosen access pattern describing which conditions are covered.
	access: &'a crate::exec::index::access_path::BTreeAccess,
}

impl IndexConditionMatcher<'_> {
	/// Check whether a binary comparison leaf is covered by the access pattern.
	fn matches_access(&self, left: &Expr, op: &BinaryOperator, right: &Expr) -> bool {
		use crate::exec::index::access_path::BTreeAccess;

		// Extract idiom, value, and the effective operator (normalized so the
		// idiom is always on the left side of the comparison). `idiom_on_left`
		// tracks the original position because some operator rewrites are
		// only valid when the idiom was the left operand (see the Inside
		// normalisation below).
		let (idiom, value, effective_op, idiom_on_left) = match (left, right) {
			(Expr::Idiom(i), Expr::Literal(lit)) => {
				if let Some(v) = try_literal_to_value(lit) {
					(i, v, op.clone(), true)
				} else {
					return false;
				}
			}
			(Expr::Literal(lit), Expr::Idiom(i)) => {
				if let Some(v) = try_literal_to_value(lit) {
					let flipped = match op {
						BinaryOperator::LessThan => BinaryOperator::MoreThan,
						BinaryOperator::LessThanEqual => BinaryOperator::MoreThanEqual,
						BinaryOperator::MoreThan => BinaryOperator::LessThan,
						BinaryOperator::MoreThanEqual => BinaryOperator::LessThanEqual,
						other => other.clone(),
					};
					(i, v, flipped, false)
				} else {
					return false;
				}
			}
			_ => return false,
		};

		use crate::val::Value;

		// Normalise the leaf so the strip path sees the same canonical
		// form the analyzer produced: a single-element `field IN [v]` is
		// rewritten by `extract_simple_condition` (analyzer side) to
		// `field = v`, but ONLY when the idiom is on the left.
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

		// `field CONTAINS scalar` (idiom on left) and `scalar INSIDE field`
		// (idiom on right) are covered when the matching index column is an
		// array-element flatten (`.*`/`[*]`) and the scalar matches the
		// access's prefix/equality value. Mirrors the analyser's
		// `try_match_containment` so a containment leaf is stripped from the
		// residual, which in turn enables `strip_index_conditions` to report
		// `FullyConsumed` and the SELECT planner to push LIMIT into the scan.
		let is_containment = (matches!(effective_op, BinaryOperator::Contain) && idiom_on_left)
			|| (matches!(effective_op, BinaryOperator::Inside) && !idiom_on_left);

		match self.access {
			BTreeAccess::Compound {
				prefix,
				range,
			} => {
				// Check equality conditions against prefix values.
				if is_equality {
					for (col, val) in self.cols.iter().zip(prefix.iter()) {
						if idiom == col && value == *val {
							return true;
						}
					}
				}
				// CONTAINS / INSIDE leaf on the leading array-element column:
				// the analyser made it the Compound prefix's first value, so
				// the scan range already filters to matching rows.
				if is_containment
					&& let Some(col) = self.cols.first()
					&& let Some(val) = prefix.first()
					&& idiom_matches_containment(idiom, col)
					&& value == *val
				{
					return true;
				}
				// Check range condition on the column after the prefix.
				if let Some((range_op, range_val)) = range
					&& let Some(col) = self.cols.get(prefix.len())
					&& idiom == col
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
				if is_equality && idiom == col && value == *val {
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
				from,
				to,
			} => {
				let Some(col) = self.cols.first() else {
					return false;
				};
				if idiom != col {
					return false;
				}
				// Check the from (lower) bound.
				if let Some(from) = from {
					let expected_op = if from.inclusive {
						BinaryOperator::MoreThanEqual
					} else {
						BinaryOperator::MoreThan
					};
					if effective_op == expected_op && value == from.value {
						return true;
					}
					// `field != NONE` is equivalent to an exclusive lower bound
					// at NONE because NONE sorts first in the BTree key
					// ordering. `!= NULL` is NOT recognised here — that
					// pushdown is unsafe (it would also drop NONE rows). See
					// the matching note in
					// `IndexAnalyzer::match_operator_to_access`.
					if !from.inclusive
						&& matches!(from.value, Value::None)
						&& effective_op == BinaryOperator::NotEqual
						&& value == from.value
					{
						return true;
					}
				}
				// Check the to (upper) bound.
				if let Some(to) = to {
					let expected_op = if to.inclusive {
						BinaryOperator::LessThanEqual
					} else {
						BinaryOperator::LessThan
					};
					if effective_op == expected_op && value == to.value {
						return true;
					}
				}
				false
			}
			// FullText and KNN access types have their own stripping logic.
			_ => false,
		}
	}
}

/// Extracts a single brute-force KNN (`NearestNeighbor::K`) expression,
/// replacing it with `Literal::Bool(true)`. The extracted parameters are
/// stashed in `params`.
struct BruteForceKnnExtractor {
	params: Option<BruteForceKnnParams>,
}

impl MutVisitor for BruteForceKnnExtractor {
	type Error = std::convert::Infallible;

	fn visit_mut_expr(&mut self, expr: &mut Expr) -> Result<(), Self::Error> {
		// Already found one -- stop looking.
		if self.params.is_some() {
			return Ok(());
		}
		if let Expr::Binary {
			left,
			op: BinaryOperator::NearestNeighbor(nn),
			right,
		} = expr && let NearestNeighbor::K(k, dist) = nn.as_ref()
			&& let Expr::Idiom(idiom) = left.as_ref()
			&& let Some(vector) = extract_literal_vector(right)
		{
			self.params = Some(BruteForceKnnParams {
				field: idiom.clone(),
				vector,
				k: *k,
				distance: dist.clone(),
			});
			*expr = Expr::Literal(Literal::Bool(true));
			return Ok(());
		}
		expr.visit_mut(self)
	}

	// Don't descend into subqueries.
	fn visit_mut_select(
		&mut self,
		_: &mut crate::expr::SelectStatement,
	) -> Result<(), Self::Error> {
		Ok(())
	}
}

/// Reusable postorder pass that collapses boolean-literal sentinels in AND
/// chains: `true AND x → x`, `x AND true → x`, `true AND true → true`.
///
/// Used after `KnnStripper` / `BruteForceKnnExtractor` to clean up the tree.
struct BoolSimplifier;

impl MutVisitor for BoolSimplifier {
	type Error = std::convert::Infallible;

	fn visit_mut_expr(&mut self, expr: &mut Expr) -> Result<(), Self::Error> {
		// Postorder: recurse first, then simplify this node.
		expr.visit_mut(self)?;

		if let Expr::Binary {
			left,
			op: BinaryOperator::And,
			right,
		} = expr
		{
			let l_true = matches!(left.as_ref(), Expr::Literal(Literal::Bool(true)));
			let r_true = matches!(right.as_ref(), Expr::Literal(Literal::Bool(true)));
			match (l_true, r_true) {
				(true, true) => *expr = Expr::Literal(Literal::Bool(true)),
				(true, false) => {
					let r = std::mem::replace(right.as_mut(), Expr::Literal(Literal::None));
					*expr = r;
				}
				(false, true) => {
					let l = std::mem::replace(left.as_mut(), Expr::Literal(Literal::None));
					*expr = l;
				}
				_ => {}
			}
		}
		Ok(())
	}

	// Don't descend into subqueries.
	fn visit_mut_select(
		&mut self,
		_: &mut crate::expr::SelectStatement,
	) -> Result<(), Self::Error> {
		Ok(())
	}
}

/// Extract a `Vec<Number>` from a literal array expression.
fn extract_literal_vector(expr: &Expr) -> Option<Vec<Number>> {
	match expr {
		Expr::Literal(lit) => {
			if let Literal::Array(arr) = lit {
				let mut nums = Vec::with_capacity(arr.len());
				for elem in arr.iter() {
					match elem {
						Expr::Literal(Literal::Integer(i)) => {
							nums.push(Number::Int(*i));
						}
						Expr::Literal(Literal::Float(f)) => {
							nums.push(Number::Float(*f));
						}
						Expr::Literal(Literal::Decimal(d)) => {
							nums.push(Number::Decimal(*d));
						}
						_ => return None,
					}
				}
				Some(nums)
			} else {
				None
			}
		}
		_ => None,
	}
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
	cond: &Cond,
	table_name: &crate::val::TableName,
) -> Option<Expr> {
	find_id_equality_in_and_chain(&cond.0, table_name)
}

/// Walk the top-level AND chain looking for `id = <RecordId literal>`.
fn find_id_equality_in_and_chain(expr: &Expr, table_name: &crate::val::TableName) -> Option<Expr> {
	match expr {
		// AND: check both branches
		Expr::Binary {
			left,
			op: BinaryOperator::And,
			right,
		} => find_id_equality_in_and_chain(left, table_name)
			.or_else(|| find_id_equality_in_and_chain(right, table_name)),

		// Equality: check for `id = <RecordId>` or `<RecordId> = id`
		Expr::Binary {
			left,
			op: BinaryOperator::Equal | BinaryOperator::ExactEqual,
			right,
		} => check_id_recordid_pair(left, right, table_name)
			.or_else(|| check_id_recordid_pair(right, left, table_name)),

		// Any other node (OR, comparisons, etc.): no match
		_ => None,
	}
}

/// Check if `idiom_side` is the `id` idiom and `lit_side` is a matching
/// RecordId literal with a non-range key.
fn check_id_recordid_pair(
	idiom_side: &Expr,
	lit_side: &Expr,
	table_name: &crate::val::TableName,
) -> Option<Expr> {
	if let Expr::Idiom(idiom) = idiom_side
		&& idiom.is_id()
		&& let Expr::Literal(Literal::RecordId(rid)) = lit_side
		&& &rid.table == table_name
		&& !matches!(rid.key, crate::expr::RecordIdKeyLit::Range(_))
	{
		Some(lit_side.clone())
	} else {
		None
	}
}

/// Check if a source expression represents a "value source" (array, primitive).
pub(crate) fn is_value_source_expr(expr: &Expr) -> bool {
	match expr {
		Expr::Literal(Literal::Array(_)) => true,
		Expr::Literal(Literal::String(_))
		| Expr::Literal(Literal::Integer(_))
		| Expr::Literal(Literal::Float(_))
		| Expr::Literal(Literal::Decimal(_))
		| Expr::Literal(Literal::Bool(_))
		| Expr::Literal(Literal::None)
		| Expr::Literal(Literal::Null) => true,
		Expr::Table(_) => false,
		Expr::Literal(Literal::RecordId(_)) => false,
		Expr::Param(_) => false,
		Expr::Select(_) => false,
		_ => false,
	}
}

/// Check if ALL source expressions are value sources.
pub(crate) fn all_value_sources(sources: &[Expr]) -> bool {
	!sources.is_empty() && sources.iter().all(is_value_source_expr)
}

// ============================================================================
// MATCHES Context Extraction
// ============================================================================

/// Extract MATCHES clause information from a WHERE condition for index functions.
///
/// Accepts an optional `FrozenContext` to resolve bind parameters (`$query`)
/// that appear on the right-hand side of `@N@` operators.
pub(crate) fn extract_matches_context(
	cond: &Cond,
	ctx: Option<&crate::ctx::FrozenContext>,
) -> crate::exec::function::MatchesContext {
	let mut collector = MatchesCollector(crate::exec::function::MatchesContext::new(), ctx);
	let _ = collector.visit_expr(&cond.0);
	collector.0
}

/// Visitor that collects MATCHES clause entries from expression trees.
struct MatchesCollector<'a>(
	crate::exec::function::MatchesContext,
	Option<&'a crate::ctx::FrozenContext>,
);

impl Visitor for MatchesCollector<'_> {
	type Error = std::convert::Infallible;

	fn visit_expr(&mut self, expr: &Expr) -> Result<(), Self::Error> {
		if let Expr::Binary {
			left,
			op: BinaryOperator::Matches(matches_op),
			right,
		} = expr && let Expr::Idiom(idiom) = left.as_ref()
		{
			// Extract the query string from the right-hand side.
			// Supports both literal strings and bind parameters.
			let query_str = match right.as_ref() {
				Expr::Literal(Literal::String(s)) => Some(s.as_str().to_owned()),
				Expr::Param(param) => {
					// Resolve the bind parameter from the frozen context
					self.1.and_then(|ctx| {
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
				self.0.insert(
					match_ref,
					crate::exec::function::MatchInfo {
						idiom: idiom.clone(),
						query,
					},
				);
			}
		}
		expr.visit(self)
	}

	// Don't descend into subqueries -- only collect outer MATCHES.
	fn visit_select(&mut self, _: &crate::expr::SelectStatement) -> Result<(), Self::Error> {
		Ok(())
	}
}

/// Try to extract the primary table name from the frozen context.
pub(crate) fn extract_table_from_context(ctx: &crate::ctx::FrozenContext) -> crate::val::TableName {
	if let Some(mc) = ctx.get_matches_context()
		&& let Some(table) = mc.table()
	{
		return table.clone();
	}
	crate::val::TableName::from("unknown".to_string())
}
