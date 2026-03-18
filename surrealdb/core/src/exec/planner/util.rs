//! Utility functions for the planner.
//!
//! Most functions are pure and perform static conversions, validation, or
//! predicate checks. `resolve_condition_params` is the exception: it requires
//! `FrozenContext` for parameter resolution and a transaction for `DEFINE PARAM`
//! fallback.

use crate::catalog::Distance;
use crate::catalog::providers::DatabaseProvider;
use crate::err::Error;
use crate::exec::field_path::{FieldPath, FieldPathPart};
use crate::exec::function::FunctionRegistry;
use crate::expr::field::{Field, Fields};
use crate::expr::operator::NearestNeighbor;
use crate::expr::visit::{MutVisitor, Visit, VisitMut, Visitor};
use crate::expr::{BinaryOperator, Cond, Expr, Idiom, Literal, Param};
use crate::val::Number;

// ============================================================================
// Literal / Value Conversion
// ============================================================================

/// Best-effort conversion of a `Literal` to a `Value`.
///
/// Handles all scalar types, simple record IDs (Number/String/Uuid keys), and
/// arrays of convertible expressions. Returns `None` for types that require
/// async computation or are otherwise unsupported (Object, Set, Generate keys,
/// Range keys, etc.).
///
/// Used by both the planner (for physical expression compilation) and the index
/// analyzer (for index matching).
pub(crate) fn try_literal_to_value(
	lit: &crate::expr::literal::Literal,
) -> Option<crate::val::Value> {
	use crate::expr::literal::Literal;
	use crate::val::Value;

	match lit {
		Literal::None => Some(Value::None),
		Literal::Null => Some(Value::Null),
		Literal::Bool(x) => Some(Value::Bool(*x)),
		Literal::Float(x) => Some(Value::Number(Number::Float(*x))),
		Literal::Integer(i) => Some(Value::Number(Number::Int(*i))),
		Literal::Decimal(d) => Some(Value::Number(Number::Decimal(*d))),
		Literal::String(s) => Some(Value::String(s.clone())),
		Literal::Uuid(u) => Some(Value::Uuid(*u)),
		Literal::Datetime(dt) => Some(Value::Datetime(dt.clone())),
		Literal::Duration(d) => Some(Value::Duration(*d)),
		Literal::RecordId(rid) => {
			// Convert simple record ID literals (Number, String, Uuid keys).
			// Complex keys (Array, Object, Generate, Range) may contain
			// expressions requiring async computation and are skipped.
			use crate::expr::RecordIdKeyLit;
			let key = match &rid.key {
				RecordIdKeyLit::Number(n) => crate::val::RecordIdKey::Number(*n),
				RecordIdKeyLit::String(s) => crate::val::RecordIdKey::String(s.clone()),
				RecordIdKeyLit::Uuid(u) => crate::val::RecordIdKey::Uuid(*u),
				_ => return None,
			};
			Some(Value::RecordId(crate::val::RecordId::new(rid.table.clone(), key)))
		}
		Literal::Array(arr) => {
			let values: Option<Vec<Value>> = arr.iter().map(try_expr_to_value).collect();
			values.map(|v| Value::Array(v.into()))
		}
		// Types that cannot be converted without async or are unsupported
		Literal::Bytes(_)
		| Literal::Regex(_)
		| Literal::Geometry(_)
		| Literal::File(_)
		| Literal::Object(_)
		| Literal::Set(_)
		| Literal::UnboundedRange => None,
	}
}

/// Try to convert an expression to a constant value.
pub(crate) fn try_expr_to_value(expr: &Expr) -> Option<crate::val::Value> {
	match expr {
		Expr::Literal(lit) => try_literal_to_value(lit),
		_ => None,
	}
}

// ============================================================================
// Constant-folding: resolve deterministic expressions to literals
// ============================================================================

/// Fold constant, document-independent expressions in a `WHERE` condition to
/// literal values. This enables proper index range access for expressions like
/// `time::now() - 365d` which would otherwise be opaque to index analysis.
///
/// Must be called **after** [`resolve_condition_params`] so that parameter
/// references have already been replaced with literals.
///
/// Only folds expressions that:
/// - Contain no field/idiom references (document-independent)
/// - Are deterministic built-in functions or arithmetic on literals
/// - Pure functions (math::*, string::*, type::*, etc.) where all args are literals
///
/// `time::now()` is evaluated once at plan time, consistent with how most
/// databases evaluate `NOW()` once per statement/transaction.
pub(crate) fn fold_condition_expressions(cond: &mut Cond, registry: &FunctionRegistry) {
	let mut folder = ExpressionFolder {
		registry,
	};
	let _ = folder.visit_mut_expr(&mut cond.0);
}

/// MutVisitor that replaces constant expression subtrees with their literal
/// values. Processes bottom-up: children are folded first, then the parent
/// node is checked.
struct ExpressionFolder<'a> {
	registry: &'a FunctionRegistry,
}

impl MutVisitor for ExpressionFolder<'_> {
	type Error = std::convert::Infallible;

	fn visit_mut_expr(&mut self, expr: &mut Expr) -> Result<(), Self::Error> {
		// First recurse into children (bottom-up folding)
		expr.visit_mut(self)?;

		// Then try to fold this node to a literal
		if let Some(folded) = try_fold_to_literal(expr, self.registry) {
			*expr = folded;
		}
		Ok(())
	}

	fn visit_mut_select(
		&mut self,
		_: &mut crate::expr::SelectStatement,
	) -> Result<(), Self::Error> {
		// Don't recurse into subqueries — they have their own planning.
		Ok(())
	}
}

/// Attempt to reduce a constant expression to an `Expr::Literal`.
///
/// Handles:
/// - `time::now()` → `Literal::Datetime(now)` (special case: non-pure but per-statement)
/// - Pure function calls where all arguments are already literals (math::floor, string::lowercase,
///   type::int, etc.)
/// - Binary arithmetic on two literals (datetime ± duration, number ± number, etc.)
fn try_fold_to_literal(expr: &Expr, registry: &FunctionRegistry) -> Option<Expr> {
	use crate::expr::Function;
	use crate::val::{Datetime, Value};

	match expr {
		// time::now() → current datetime literal
		// Special case: time::now() is not pure (depends on clock) but we
		// intentionally fold it once per statement, matching SQL semantics.
		Expr::FunctionCall(fc)
			if matches!(&fc.receiver, Function::Normal(name) if name == "time::now")
				&& fc.arguments.is_empty() =>
		{
			Some(Value::Datetime(Datetime::now()).into_literal())
		}

		// Pure function call where all arguments are already literals.
		// After bottom-up folding, nested expressions like `math::floor(20 + 0.5)`
		// will have their arguments folded first, so we only need to check
		// whether the immediate arguments are literals.
		Expr::FunctionCall(fc) => {
			let Function::Normal(name) = &fc.receiver else {
				return None;
			};
			let func = registry.get(name.as_str())?;
			if !func.is_pure() || func.is_async() {
				return None;
			}
			// All arguments must be convertible to constant Values
			let args: Option<Vec<Value>> = fc.arguments.iter().map(try_expr_to_value).collect();
			let args = args?;
			// Invoke the function synchronously — safe because it's pure
			let result = func.invoke(args).ok()?;
			Some(result.into_literal())
		}

		// Binary operation where both operands are already literals
		Expr::Binary {
			left,
			op,
			right,
		} => {
			let left_val = try_expr_to_value(left)?;
			let right_val = try_expr_to_value(right)?;
			let result = try_eval_binary(op, left_val, right_val)?;
			Some(result.into_literal())
		}

		_ => None,
	}
}

/// Evaluate a binary operation on two concrete Values.
/// Returns `None` if the operation is unsupported or fails.
fn try_eval_binary(
	op: &BinaryOperator,
	left: crate::val::Value,
	right: crate::val::Value,
) -> Option<crate::val::Value> {
	use crate::val::{TryAdd, TrySub};

	match op {
		BinaryOperator::Add => left.try_add(right).ok(),
		BinaryOperator::Subtract => left.try_sub(right).ok(),
		// We intentionally limit folding to add/sub to avoid unexpected
		// behavior with division-by-zero, overflow, etc. These cover the
		// common datetime ± duration patterns.
		_ => None,
	}
}

/// Convert a `Literal` to a `Value` for static (non-computed) cases.
///
/// Delegates to [`try_literal_to_value`] for common types, then handles
/// planner-specific types (UnboundedRange, Bytes, Regex, Geometry, File).
/// Returns `Error::Internal` for types that should have been handled upstream
/// by `physical_expr()` (RecordId, Array, Object, Set).
pub(super) fn literal_to_value(
	lit: crate::expr::literal::Literal,
) -> Result<crate::val::Value, Error> {
	use crate::expr::literal::Literal;
	use crate::val::{Range, Value};

	// Try the shared conversion first (handles scalars, simple RecordIds, arrays)
	if let Some(value) = try_literal_to_value(&lit) {
		return Ok(value);
	}

	// Handle types that try_literal_to_value doesn't cover but are valid here
	match lit {
		Literal::UnboundedRange => Ok(Value::Range(Box::new(Range::unbounded()))),
		Literal::Bytes(b) => Ok(Value::Bytes(b)),
		Literal::Regex(r) => Ok(Value::Regex(r)),
		Literal::Geometry(g) => Ok(Value::Geometry(g)),
		Literal::File(f) => Ok(Value::File(f)),
		// Everything else should be handled upstream in physical_expr()
		other => Err(Error::Internal(format!(
			"Literal should be handled upstream in physical_expr(): {:?}",
			std::mem::discriminant(&other)
		))),
	}
}

/// Convert a `RecordIdKeyLit` to an `Expr`.
pub(super) fn key_lit_to_expr(lit: &crate::expr::RecordIdKeyLit) -> Result<Expr, Error> {
	use crate::expr::RecordIdKeyLit;
	match lit {
		RecordIdKeyLit::Number(n) => Ok(Expr::Literal(crate::expr::literal::Literal::Integer(*n))),
		RecordIdKeyLit::String(s) => {
			Ok(Expr::Literal(crate::expr::literal::Literal::String(s.clone())))
		}
		RecordIdKeyLit::Uuid(u) => Ok(Expr::Literal(crate::expr::literal::Literal::Uuid(*u))),
		RecordIdKeyLit::Array(exprs) => {
			Ok(Expr::Literal(crate::expr::literal::Literal::Array(exprs.clone())))
		}
		RecordIdKeyLit::Object(entries) => {
			Ok(Expr::Literal(crate::expr::literal::Literal::Object(entries.clone())))
		}
		RecordIdKeyLit::Generate(_) => Err(Error::Query {
			message: "Generated keys (rand, ulid, uuid) cannot be used in graph range bounds"
				.to_string(),
		}),
		RecordIdKeyLit::Range(_) => Err(Error::Query {
			message: "Nested range keys cannot be used in graph range bounds".to_string(),
		}),
	}
}

// ============================================================================
// Predicate / Validation Helpers
// ============================================================================

/// Check if a condition has a top-level OR operator.
///
/// Used to prevent LIMIT/START pushdown into Scan when the condition may
/// trigger a multi-index union at runtime. Union streams don't maintain
/// a global ordering, so pushing LIMIT would truncate results arbitrarily.
pub(super) fn has_top_level_or(cond: Option<&Cond>) -> bool {
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
pub(super) fn has_knn_operator(expr: &Expr) -> bool {
	let mut checker = KnnOperatorChecker {
		found_any: false,
		found_k: false,
	};
	let _ = checker.visit_expr(expr);
	checker.found_any
}

/// Check if an expression contains a brute-force KNN operator (`NearestNeighbor::K`).
///
/// Used to distinguish between brute-force KNN with parameter-based vectors
/// (where `extract_bruteforce_knn` fails) and HNSW KNN (`Approximate`).
pub(super) fn has_knn_k_operator(expr: &Expr) -> bool {
	let mut checker = KnnOperatorChecker {
		found_any: false,
		found_k: false,
	};
	let _ = checker.visit_expr(expr);
	checker.found_k
}

/// Visitor that detects the presence of KNN operators in an expression tree.
struct KnnOperatorChecker {
	found_any: bool,
	found_k: bool,
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
			if matches!(nn.as_ref(), NearestNeighbor::K(..)) {
				self.found_k = true;
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
pub(super) struct BruteForceKnnParams {
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
pub(super) fn extract_bruteforce_knn(cond: &Cond) -> Option<BruteForceKnnParams> {
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
	let mut expr = cond.0.clone();
	let _ = FtsStripper.visit_mut_expr(&mut expr);
	let _ = BoolSimplifier.visit_mut_expr(&mut expr);
	if matches!(expr, Expr::Literal(Literal::Bool(true))) {
		None
	} else {
		Some(Cond(expr))
	}
}

/// Strip handled KNN operators from a WHERE clause, returning the residual condition.
///
/// Both `NearestNeighbor::K` (consumed by `KnnTopK`) and `NearestNeighbor::Approximate`
/// (consumed by `KnnScan` via HNSW index) are stripped. `KTree` is left in place --
/// the caller should verify the residual contains no remaining KNN operators and
/// return an error if it does.
pub(crate) fn strip_knn_from_condition(cond: &Cond) -> Option<Cond> {
	let mut expr = cond.0.clone();
	let _ = KnnStripper.visit_mut_expr(&mut expr);
	let _ = BoolSimplifier.visit_mut_expr(&mut expr);
	if matches!(expr, Expr::Literal(Literal::Bool(true))) {
		None
	} else {
		Some(Cond(expr))
	}
}

// ---------------------------------------------------------------------------
// Index condition stripping
// ---------------------------------------------------------------------------

/// Strip conditions covered by a BTree index access path from a WHERE clause.
///
/// Returns `None` when all conditions are consumed (no Filter needed),
/// or `Some(residual)` when conditions remain that the index does not cover.
///
/// Follows the same pattern as [`strip_knn_from_condition`]: clone the
/// expression tree, replace matched leaves with `Literal(true)`, then run
/// [`BoolSimplifier`] to collapse sentinels.
pub(crate) fn strip_index_conditions(
	cond: &Cond,
	access: &crate::exec::index::access_path::BTreeAccess,
	cols: &[Idiom],
) -> Option<Cond> {
	let mut expr = cond.0.clone();
	let mut stripper = IndexConditionStripper {
		cols,
		access,
	};
	let _ = stripper.visit_mut_expr(&mut expr);
	let _ = BoolSimplifier.visit_mut_expr(&mut expr);
	if matches!(expr, Expr::Literal(Literal::Bool(true))) {
		None
	} else {
		Some(Cond(expr))
	}
}

/// Replaces index-covered comparison leaves in an AND tree with
/// `Literal::Bool(true)`. Run [`BoolSimplifier`] afterwards to collapse
/// the resulting `true AND x` chains.
struct IndexConditionStripper<'a> {
	/// Index columns in definition order.
	cols: &'a [Idiom],
	/// The chosen access pattern describing which conditions are covered.
	access: &'a crate::exec::index::access_path::BTreeAccess,
}

impl IndexConditionStripper<'_> {
	/// Check whether a binary comparison leaf is covered by the access pattern.
	fn matches_access(&self, left: &Expr, op: &BinaryOperator, right: &Expr) -> bool {
		use crate::exec::index::access_path::BTreeAccess;

		// Extract idiom, value, and the effective operator (normalized so the
		// idiom is always on the left side of the comparison).
		let (idiom, value, effective_op) = match (left, right) {
			(Expr::Idiom(i), Expr::Literal(lit)) => {
				if let Some(v) = try_literal_to_value(lit) {
					(i, v, op.clone())
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
					(i, v, flipped)
				} else {
					return false;
				}
			}
			_ => return false,
		};

		let is_equality =
			matches!(effective_op, BinaryOperator::Equal | BinaryOperator::ExactEqual);

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
				// Check range condition on the column after the prefix.
				if let Some((range_op, range_val)) = range
					&& let Some(col) = self.cols.get(prefix.len())
					&& idiom == col && effective_op == *range_op
					&& value == *range_val
				{
					return true;
				}
				false
			}
			BTreeAccess::Equality(val) => {
				if let Some(col) = self.cols.first() {
					is_equality && idiom == col && value == *val
				} else {
					false
				}
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

impl MutVisitor for IndexConditionStripper<'_> {
	type Error = std::convert::Infallible;

	fn visit_mut_expr(&mut self, expr: &mut Expr) -> Result<(), Self::Error> {
		match expr {
			// Recurse into AND branches.
			Expr::Binary {
				left,
				op: BinaryOperator::And,
				right,
			} => {
				self.visit_mut_expr(left)?;
				self.visit_mut_expr(right)?;
			}
			// Leaf comparison — check if the index covers it.
			Expr::Binary {
				left,
				op,
				right,
			} => {
				if self.matches_access(left, op, right) {
					*expr = Expr::Literal(Literal::Bool(true));
				}
			}
			_ => {}
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

// ---------------------------------------------------------------------------
// MutVisitors for KNN condition rewriting
// ---------------------------------------------------------------------------

/// Replaces handled KNN expressions (`NearestNeighbor::K` and
/// `NearestNeighbor::Approximate`) with `Literal::Bool(true)`.
/// Run `BoolSimplifier` afterwards to collapse the resulting
/// `true AND x` chains.
struct KnnStripper;

impl MutVisitor for KnnStripper {
	type Error = std::convert::Infallible;

	fn visit_mut_expr(&mut self, expr: &mut Expr) -> Result<(), Self::Error> {
		// Replace handled KNN expressions with `true`.
		if let Expr::Binary {
			op: BinaryOperator::NearestNeighbor(nn),
			..
		} = expr && matches!(
			nn.as_ref(),
			NearestNeighbor::K(..) | NearestNeighbor::Approximate(..)
		) {
			*expr = Expr::Literal(Literal::Bool(true));
			return Ok(());
		}
		// Only recurse into AND chains. KNN operators nested under OR/NOT
		// must be preserved so that `has_knn_operator` can detect and reject
		// those unsupported shapes.
		if let Expr::Binary {
			left,
			op: BinaryOperator::And,
			right,
		} = expr
		{
			self.visit_mut_expr(left)?;
			self.visit_mut_expr(right)?;
		}
		Ok(())
	}

	// Don't strip KNN inside subqueries -- only top-level WHERE.
	fn visit_mut_select(
		&mut self,
		_: &mut crate::expr::SelectStatement,
	) -> Result<(), Self::Error> {
		Ok(())
	}
}

/// Replaces MATCHES (`@@`) expressions with `Literal::Bool(true)`.
/// Run `BoolSimplifier` afterwards to collapse the resulting
/// `true AND x` chains.
struct FtsStripper;

impl MutVisitor for FtsStripper {
	type Error = std::convert::Infallible;

	fn visit_mut_expr(&mut self, expr: &mut Expr) -> Result<(), Self::Error> {
		if let Expr::Binary {
			op: BinaryOperator::Matches(_),
			..
		} = expr
		{
			*expr = Expr::Literal(Literal::Bool(true));
			return Ok(());
		}
		if let Expr::Binary {
			left,
			op: BinaryOperator::And,
			right,
		} = expr
		{
			self.visit_mut_expr(left)?;
			self.visit_mut_expr(right)?;
		}
		Ok(())
	}

	fn visit_mut_select(
		&mut self,
		_: &mut crate::expr::SelectStatement,
	) -> Result<(), Self::Error> {
		Ok(())
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

// ---------------------------------------------------------------------------
// Parameter pre-resolution
// ---------------------------------------------------------------------------

/// Collects all `Expr::Param` names referenced in a condition, skipping
/// subqueries. Used as the first pass before async resolution.
struct ParamCollector {
	names: std::collections::HashSet<String>,
}

impl Visitor for ParamCollector {
	type Error = std::convert::Infallible;

	fn visit_expr(&mut self, expr: &Expr) -> Result<(), Self::Error> {
		if let Expr::Param(param) = expr {
			self.names.insert(param.as_str().to_string());
		}
		expr.visit(self)
	}

	fn visit_select(&mut self, _: &crate::expr::SelectStatement) -> Result<(), Self::Error> {
		Ok(())
	}
}

/// Replaces `Expr::Param` nodes with `Expr::Literal` using a pre-built value
/// map. Applied after async resolution has populated the map.
struct ParamResolver<'a> {
	values: &'a std::collections::HashMap<String, crate::val::Value>,
}

impl MutVisitor for ParamResolver<'_> {
	type Error = std::convert::Infallible;

	fn visit_mut_expr(&mut self, expr: &mut Expr) -> Result<(), Self::Error> {
		if let Expr::Param(param) = expr
			&& let Some(value) = self.values.get(param.as_str())
		{
			*expr = value.clone().into_literal();
			return Ok(());
		}
		expr.visit_mut(self)
	}

	fn visit_mut_select(
		&mut self,
		_: &mut crate::expr::SelectStatement,
	) -> Result<(), Self::Error> {
		Ok(())
	}
}

/// Parameters injected per-row during SELECT document iteration.
/// These change value for every row and cannot be resolved at plan time.
///
/// - `this`/`self`: the current document being iterated
/// - `parent`: the outer document in correlated subqueries
///
/// Event/live/field params (`$before`, `$after`, `$value`, `$input`, `$event`)
/// are intentionally excluded because the exec planner only handles
/// top-level statements.  SELECTs inside event handlers, live query
/// notifications, and field evaluators run through the legacy `compute()`
/// path, where those params are resolved at runtime via `ctx.value()`.
/// If the exec planner is ever extended to those contexts, this guard
/// set would need to be expanded accordingly.
pub(crate) const SELECT_ITERATION_PARAMS: &[&str] = &["this", "self", "parent"];

/// Resolve a single parameter to its value at plan time.
///
/// Resolution order:
/// 1. Context values (LET bindings, client bind parameters, session params)
/// 2. Row-scoped guard (skip params whose values change per-row)
/// 3. DEFINE PARAM values with `Permission::Full` from the transaction store
///
/// Context values are checked first so that `LET` bindings shadow the
/// row-scoped guard. The `row_scoped` set is caller-provided so that
/// different planning contexts can guard the appropriate params (e.g.
/// SELECT iteration guards `$this`/`$self`/`$parent`; a future LIVE query
/// planner would additionally guard `$event`/`$before`/`$after`/`$value`).
///
/// DEFINE PARAMs with `Permission::None` or `Permission::Specific` are left
/// for runtime resolution where the full permission machinery is available.
pub(super) async fn resolve_param_value(
	name: &str,
	ctx: &crate::ctx::FrozenContext,
	ns_db: Option<(crate::catalog::NamespaceId, crate::catalog::DatabaseId)>,
	row_scoped: &[&str],
) -> Option<crate::val::Value> {
	if let Some(value) = ctx.value(name) {
		return Some(value.clone());
	}
	if row_scoped.contains(&name) {
		return None;
	}
	if let Some((ns, db)) = ns_db
		&& let Some(txn) = ctx.try_tx()
		&& let Ok(param_def) = txn.get_db_param(ns, db, name).await
		&& matches!(param_def.permissions, crate::catalog::Permission::Full)
	{
		return Some(param_def.value.clone());
	}
	None
}

/// Resolve bind-parameter references in a `WHERE` condition to their literal
/// values. Returns a new `Cond` with `Expr::Param` nodes replaced by
/// `Expr::Literal` wherever the value is available.
///
/// Resolution order for each parameter:
/// 1. Context values (`LET` bindings, client bind parameters, session params)
/// 2. Database-level defined parameters (`DEFINE PARAM`) via the transaction store, when `ns_db`
///    IDs are provided.
///
/// `row_scoped` names are skipped during DEFINE PARAM fallback (step 2) since
/// their values change per-row at runtime. Callers provide the appropriate
/// set for their planning context (e.g. [`SELECT_ITERATION_PARAMS`]).
///
/// Parameters that cannot be resolved are left as-is.
pub(crate) async fn resolve_condition_params(
	cond: &Cond,
	ctx: &crate::ctx::FrozenContext,
	ns_db: Option<(crate::catalog::NamespaceId, crate::catalog::DatabaseId)>,
	row_scoped: &[&str],
) -> Cond {
	// Pass 1: collect all param names referenced in the condition.
	let mut collector = ParamCollector {
		names: std::collections::HashSet::new(),
	};
	let _ = collector.visit_expr(&cond.0);
	if collector.names.is_empty() {
		return cond.clone();
	}

	// Pass 2: resolve each parameter via the shared resolution path.
	let mut resolved = std::collections::HashMap::with_capacity(collector.names.len());
	for name in &collector.names {
		if let Some(value) = resolve_param_value(name, ctx, ns_db, row_scoped).await {
			resolved.insert(name.clone(), value);
		}
	}

	if resolved.is_empty() {
		return cond.clone();
	}

	// Pass 3: apply substitutions.
	let mut expr = cond.0.clone();
	let _ = ParamResolver {
		values: &resolved,
	}
	.visit_mut_expr(&mut expr);
	Cond(expr)
}

/// Rewrite projection function calls with a single string literal argument
/// (e.g. `type::field("name")`) to `Expr::Idiom` so the index analyzer can
/// match them against indexed columns.
///
/// Projection functions like `type::field(s)` and a bare `Idiom(s)` both
/// reference the same document field, but the index analyzer only recognises
/// `Expr::Idiom`. This pass bridges the gap after param resolution and
/// constant folding have reduced the argument to a string literal.
///
/// Uses `FunctionRegistry::is_projection` so any current or future
/// projection function is handled without hardcoding names.
pub(crate) fn resolve_projection_field_idioms(cond: &mut Cond, registry: &FunctionRegistry) {
	let mut resolver = ProjectionFieldResolver {
		registry,
	};
	let _ = resolver.visit_mut_expr(&mut cond.0);
}

struct ProjectionFieldResolver<'a> {
	registry: &'a FunctionRegistry,
}

impl MutVisitor for ProjectionFieldResolver<'_> {
	type Error = std::convert::Infallible;

	fn visit_mut_expr(&mut self, expr: &mut Expr) -> Result<(), Self::Error> {
		use crate::expr::function::Function;

		expr.visit_mut(self)?;

		if let Expr::FunctionCall(fc) = expr
			&& let Function::Normal(name) = &fc.receiver
			&& self.registry.is_projection(name)
			&& fc.arguments.len() == 1
			&& let Expr::Literal(Literal::String(s)) = &fc.arguments[0]
			&& let Ok(idiom) = crate::syn::idiom(s)
		{
			*expr = Expr::Idiom(idiom.into());
		}
		Ok(())
	}

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
pub(super) fn extract_record_id_point_lookup(
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
pub(super) fn is_value_source_expr(expr: &Expr) -> bool {
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
pub(super) fn all_value_sources(sources: &[Expr]) -> bool {
	!sources.is_empty() && sources.iter().all(is_value_source_expr)
}

// ============================================================================
// MATCHES Context Extraction
// ============================================================================

/// Extract MATCHES clause information from a WHERE condition for index functions.
///
/// Accepts an optional `FrozenContext` to resolve bind parameters (`$query`)
/// that appear on the right-hand side of `@N@` operators.
pub(super) fn extract_matches_context(
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
				Expr::Literal(Literal::String(s)) => Some(s.clone()),
				Expr::Param(param) => {
					// Resolve the bind parameter from the frozen context
					self.1.and_then(|ctx| {
						ctx.value(param.as_str()).and_then(|v| {
							if let crate::val::Value::String(s) = v {
								Some(s.clone())
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
pub(super) fn extract_table_from_context(ctx: &crate::ctx::FrozenContext) -> crate::val::TableName {
	if let Some(mc) = ctx.get_matches_context()
		&& let Some(table) = mc.table()
	{
		return table.clone();
	}
	crate::val::TableName::from("unknown".to_string())
}

// ============================================================================
// VERSION Extraction
// ============================================================================

/// Extract version expression from VERSION clause.
///
/// Returns a physical expression that, when evaluated at execution time,
/// produces the version timestamp (u64).
pub(super) async fn extract_version(
	version_expr: Expr,
	planner: &super::Planner<'_>,
) -> Result<Option<std::sync::Arc<dyn crate::exec::PhysicalExpr>>, Error> {
	match version_expr {
		Expr::Literal(Literal::None) => Ok(None),
		_ => {
			let expr = planner.physical_expr(version_expr).await?;
			Ok(Some(expr))
		}
	}
}

// ============================================================================
// GROUP BY Validation
// ============================================================================

/// Check if fields contain `$this` or `$parent` parameters (invalid in GROUP BY).
pub(super) fn check_forbidden_group_by_params(fields: &Fields) -> Result<(), Error> {
	match fields {
		Fields::Value(selector) => check_expr_for_forbidden_params(&selector.expr),
		Fields::Select(field_list) => {
			for field in field_list {
				match field {
					Field::All => {}
					Field::Single(selector) => {
						check_expr_for_forbidden_params(&selector.expr)?;
					}
				}
			}
			Ok(())
		}
	}
}

fn check_expr_for_forbidden_params(expr: &Expr) -> Result<(), Error> {
	expr.visit(&mut ForbiddenParamChecker)
}

/// Visitor that detects `$this`, `$self`, or `$parent` parameters which are
/// invalid inside GROUP BY projections. The default `visit_expr`
/// implementation handles all the recursion; we only need to inspect params.
struct ForbiddenParamChecker;

impl Visitor for ForbiddenParamChecker {
	type Error = Error;

	fn visit_param(&mut self, param: &Param) -> Result<(), Self::Error> {
		let name = param.as_str();
		if name == "this" || name == "self" {
			return Err(Error::Query {
				message: "Found a `$this` parameter refering to the document of a group by select statement\nSelect statements with a group by currently have no defined document to refer to".to_string(),
			});
		}
		if name == "parent" {
			return Err(Error::Query {
				message: "Found a `$parent` parameter refering to the document of a GROUP select statement\nSelect statements with a GROUP BY or GROUP ALL currently have no defined document to refer to".to_string(),
			});
		}
		Ok(())
	}
}

// ============================================================================
// Pushdown Eligibility
// ============================================================================

/// Check if ORDER BY is compatible with the natural KV scan direction.
///
/// Returns `true` when ORDER BY is absent, or is exactly `id ASC` or `id DESC`
/// with no COLLATE/NUMERIC modifiers. In these cases the scan already produces
/// rows in the requested order and no separate Sort operator is needed.
pub(super) fn order_is_scan_compatible(order: Option<&crate::expr::order::Ordering>) -> bool {
	use crate::expr::order::Ordering;
	match order {
		None => true,
		Some(Ordering::Random) => false,
		Some(Ordering::Order(list)) => {
			list.0.len() == 1 && list.0[0].value.is_id() && !list.0[0].collate && !list.0[0].numeric
		}
	}
}

/// Check if an index + scan direction satisfies the given ORDER BY.
///
/// Builds the same `SortProperty` vector that `IndexScan::output_ordering()`
/// would produce and checks whether it satisfies the ORDER BY requirements.
/// This allows the planner to decide on limit pushdown before the IndexScan
/// operator is created.
///
/// For compound access with an equality prefix, the prefix columns all have
/// the same value and do not define ordering. They are skipped so that the
/// effective ordering starts from the first non-equality column.
///
/// For single-column Equality access (`WHERE col = val`), ALL index columns
/// are constant, so they are all skipped.  Leading ORDER BY fields that
/// reference constant (equality-pinned) columns are also stripped from the
/// requirement because any direction trivially matches a single-valued column.
pub(crate) fn index_covers_ordering(
	index_ref: &crate::exec::index::access_path::IndexRef,
	access: &crate::exec::index::access_path::BTreeAccess,
	direction: crate::idx::planner::ScanDirection,
	order: &crate::expr::order::Ordering,
) -> bool {
	use crate::exec::index::access_path::BTreeAccess;
	use crate::exec::operators::SortDirection;
	use crate::exec::ordering::{OutputOrdering, SortProperty};
	use crate::expr::order::Ordering;

	let Ordering::Order(order_list) = order else {
		return false; // Random ordering can't be satisfied by an index
	};

	// Convert ORDER BY to required SortProperty
	let required: Vec<SortProperty> = order_list
		.iter()
		.filter_map(|field| {
			crate::exec::field_path::FieldPath::try_from(&field.value).ok().map(|path| {
				let direction = if field.direction {
					SortDirection::Asc
				} else {
					SortDirection::Desc
				};
				SortProperty {
					path,
					direction,
					collate: field.collate,
					numeric: field.numeric,
				}
			})
		})
		.collect();

	if required.len() != order_list.len() {
		return false;
	}

	// Determine which index columns are equality-pinned (constant value).
	let ix_def = index_ref.definition();
	let (skip_cols, equality_field_paths) = match access {
		BTreeAccess::Compound {
			prefix,
			..
		} => {
			let paths: Vec<_> = ix_def
				.cols
				.iter()
				.take(prefix.len())
				.filter_map(|idiom| crate::exec::field_path::FieldPath::try_from(idiom).ok())
				.collect();
			(prefix.len(), paths)
		}
		BTreeAccess::Equality(_) => {
			let paths: Vec<_> = ix_def
				.cols
				.iter()
				.filter_map(|idiom| crate::exec::field_path::FieldPath::try_from(idiom).ok())
				.collect();
			(ix_def.cols.len(), paths)
		}
		_ => (0, vec![]),
	};

	// Strip leading ORDER BY fields that reference equality-pinned columns.
	// These columns have a single constant value, so any direction trivially
	// satisfies the ordering requirement for them.
	let required: Vec<SortProperty> =
		required.into_iter().skip_while(|prop| equality_field_paths.contains(&prop.path)).collect();

	// Build the index ordering (same as IndexScan::output_ordering())
	let dir = match direction {
		crate::idx::planner::ScanDirection::Forward => SortDirection::Asc,
		crate::idx::planner::ScanDirection::Backward => SortDirection::Desc,
	};
	let mut cols: Vec<SortProperty> = ix_def
		.cols
		.iter()
		.skip(skip_cols)
		.filter_map(|idiom| {
			crate::exec::field_path::FieldPath::try_from(idiom).ok().map(|path| SortProperty {
				path,
				direction: dir,
				collate: false,
				numeric: false,
			})
		})
		.collect();

	// For non-unique indexes (Idx), the record ID is stored in the BTree
	// key after the field values.  Entries are implicitly sorted by record
	// ID, so we append an `id` property to the effective ordering.  This
	// allows `ORDER BY col DESC, id DESC` to be satisfied by a backward
	// compound index scan.
	//
	// When all index columns are skipped (Equality), the ordering is
	// *only* by record ID — we still append it.
	if !index_ref.is_unique() && !ix_def.cols.is_empty() {
		cols.push(SortProperty {
			path: crate::exec::field_path::FieldPath::field("id"),
			direction: dir,
			collate: false,
			numeric: false,
		});
	}

	// If all required fields were stripped (all constant), the ordering
	// is trivially satisfied.
	if required.is_empty() {
		return true;
	}

	if cols.is_empty() {
		return false;
	}

	OutputOrdering::Sorted(cols).satisfies(&required)
}

// ============================================================================
// LIMIT Helpers
// ============================================================================

/// Try to get the effective limit (start + limit) if both are literals.
pub(super) fn get_effective_limit_literal(
	start: &Option<crate::expr::start::Start>,
	limit: &Option<crate::expr::limit::Limit>,
) -> Option<usize> {
	let limit_val = limit.as_ref().and_then(|l| match &l.0 {
		Expr::Literal(Literal::Integer(n)) if *n >= 0 => Some(*n as usize),
		Expr::Literal(Literal::Float(n)) if *n >= 0.0 => Some(*n as usize),
		_ => None,
	})?;

	let start_val = start
		.as_ref()
		.map(|s| match &s.0 {
			Expr::Literal(Literal::Integer(n)) if *n >= 0 => Some(*n as usize),
			Expr::Literal(Literal::Float(n)) if *n >= 0.0 => Some(*n as usize),
			_ => None,
		})
		.unwrap_or(Some(0))?;

	start_val.checked_add(limit_val)
}

// ============================================================================
// Field Name Derivation
// ============================================================================

/// Derive a field name from an expression for projection output.
pub(super) fn derive_field_name(expr: &Expr) -> String {
	match expr {
		Expr::Idiom(idiom) => idiom_to_field_name(idiom),
		Expr::Param(param) => param.as_str().to_string(),
		Expr::FunctionCall(call) => {
			let idiom: crate::expr::idiom::Idiom = call.receiver.to_idiom();
			idiom_to_field_name(&idiom)
		}
		_ => {
			use surrealdb_types::ToSql;
			expr.to_sql()
		}
	}
}

/// Extract a field name from an idiom.
pub(super) fn idiom_to_field_name(idiom: &crate::expr::idiom::Idiom) -> String {
	use surrealdb_types::ToSql;

	use crate::expr::part::Part;

	for part in idiom.0.iter() {
		if let Part::Lookup(lookup) = part
			&& let Some(alias) = &lookup.alias
		{
			return idiom_to_field_name(alias);
		}
	}

	let simplified = idiom.simplify();

	if simplified.len() == 1
		&& let Some(Part::Field(name)) = simplified.first()
	{
		return name.clone();
	}
	simplified.to_sql()
}

/// Extract a field path from an idiom for nested output construction.
pub(super) fn idiom_to_field_path(idiom: &crate::expr::idiom::Idiom) -> FieldPath {
	use surrealdb_types::ToSql;

	use crate::expr::part::Part;

	for part in idiom.0.iter() {
		if let Part::Lookup(lookup) = part
			&& lookup.alias.is_some()
		{
			return FieldPath::field(idiom_to_field_name(idiom));
		}
	}

	let has_lookups = idiom.0.iter().any(|p| matches!(p, Part::Lookup(_)));

	if !has_lookups {
		let name = idiom_to_field_name(idiom);
		// FIXME: This should be implemented in a way that requries first formating to a string.
		// It should instead manually figure out the parts of the field.
		// This will probably break if a field name has a dot in the name like in valid path
		// "foo.`a.b`"
		if name.contains('.') && !name.contains(['[', '(', ' ']) {
			return FieldPath(
				name.split('.').map(|s| FieldPathPart::Field(s.to_string())).collect(),
			);
		}
		return FieldPath::field(name);
	}

	let mut parts = Vec::new();
	for part in idiom.0.iter() {
		match part {
			Part::Lookup(lookup) => {
				let lookup_key = lookup.to_sql();
				parts.push(FieldPathPart::Lookup(lookup_key));
			}
			Part::Field(name) => {
				parts.push(FieldPathPart::Field(name.clone()));
			}
			_ => {}
		}
	}

	if parts.is_empty() {
		return FieldPath::field(idiom.to_sql());
	}

	FieldPath(parts)
}

// ============================================================================
// COUNT() Fast-Path Detection
// ============================================================================

/// Check if a SELECT statement is eligible for the CountScan optimisation.
///
/// Returns `true` when the query matches:
///   `SELECT count() FROM <single-table> WHERE <cond> GROUP ALL`
/// with a WHERE clause and no SPLIT, ORDER BY, FETCH, or OMIT clauses.
///
/// When this returns `true`, the `IndexCountScan` operator can be used.
/// At execution time it will look up the table's indexes and, if a COUNT
/// index with a matching condition exists, sum delta counts instead of
/// scanning all records.
#[allow(clippy::too_many_arguments)]
pub(super) fn is_indexed_count_eligible(
	fields: &Fields,
	group: &Option<crate::expr::group::Groups>,
	cond: &Option<crate::expr::cond::Cond>,
	split: &Option<crate::expr::split::Splits>,
	order: &Option<crate::expr::order::Ordering>,
	fetch: &Option<crate::expr::fetch::Fetchs>,
	omit: &[Expr],
	what: &[Expr],
) -> bool {
	// Must be count()-only fields.
	if !fields.is_count_all_only() {
		return false;
	}
	// Must have GROUP ALL.
	let Some(groups) = group else {
		return false;
	};
	if !groups.is_group_all_only() {
		return false;
	}
	// Must have a WHERE clause (the no-WHERE case is handled by `is_count_all_eligible`).
	if cond.is_none() {
		return false;
	}
	// No SPLIT, ORDER BY, FETCH, or OMIT.
	if split.is_some() || order.is_some() || fetch.is_some() || !omit.is_empty() {
		return false;
	}
	// Source must be a single table (no record-id ranges for indexed counts).
	if what.len() != 1 {
		return false;
	}
	matches!(&what[0], Expr::Table(_) | Expr::Param(_))
}

/// Returns `true` when the query matches:
///   `SELECT count() FROM <single-table-or-range> GROUP ALL`
/// with no WHERE, SPLIT, ORDER BY, FETCH, or OMIT clauses.
///
/// The CountScan operator replaces the entire Scan -> Aggregate -> Project
/// pipeline with a single `txn.count()` call on the KV key range.
#[allow(clippy::too_many_arguments)]
pub(super) fn is_count_all_eligible(
	fields: &Fields,
	group: &Option<crate::expr::group::Groups>,
	cond: &Option<crate::expr::cond::Cond>,
	split: &Option<crate::expr::split::Splits>,
	order: &Option<crate::expr::order::Ordering>,
	fetch: &Option<crate::expr::fetch::Fetchs>,
	omit: &[Expr],
	what: &[Expr],
) -> bool {
	// Must be count()-only fields (no arguments, no other fields).
	if !fields.is_count_all_only() {
		return false;
	}
	// Must have GROUP ALL (explicit `GROUP ALL` in the AST = Some(Groups(vec![]))).
	let Some(groups) = group else {
		return false;
	};
	if !groups.is_group_all_only() {
		return false;
	}
	// No WHERE clause (index-accelerated WHERE is a follow-up).
	if cond.is_some() {
		return false;
	}
	// No SPLIT, ORDER BY, FETCH, or OMIT.
	if split.is_some() || order.is_some() || fetch.is_some() || !omit.is_empty() {
		return false;
	}
	// Source must be a single table, record-id, or param (resolving to table).
	if what.len() != 1 {
		return false;
	}
	matches!(
		&what[0],
		Expr::Table(_)
			| Expr::Literal(crate::expr::literal::Literal::RecordId(_))
			| Expr::Param(_)
			| Expr::Postfix { .. }
	)
}

/// Extract the output field names for a CountScan fast-path query.
///
/// For each `count()` field in the SELECT list, this returns the alias name
/// (if `AS alias` is present) or the default derived name (`"count"`).
///
/// Pre-condition: `is_count_all_eligible` returned `true`, so every field is
/// a `count()` function call.
pub(super) fn extract_count_field_names(fields: &Fields) -> Vec<String> {
	match fields {
		Fields::Value(selector) => {
			if let Some(alias) = &selector.alias {
				vec![idiom_to_field_name(alias)]
			} else {
				vec![derive_field_name(&selector.expr)]
			}
		}
		Fields::Select(field_list) => field_list
			.iter()
			.filter_map(|f| match f {
				Field::Single(selector) => {
					if let Some(alias) = &selector.alias {
						Some(idiom_to_field_name(alias))
					} else {
						Some(derive_field_name(&selector.expr))
					}
				}
				_ => None,
			})
			.collect(),
	}
}
