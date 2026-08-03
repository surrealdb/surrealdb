//! Literal/value conversion and constant folding.
//!
//! Pure (non-async) helpers that convert between AST literals and runtime
//! values, plus the plan-time constant folder used to reduce
//! deterministic, document-independent expressions in WHERE clauses to
//! literals so the index analyzer can match them.

use std::ops::Bound;

use crate::err::{EngineError, Error};
use crate::exec::Error as ExecError;
use crate::exec::function::FunctionRegistry;
use crate::expr::visit::{MutVisitor, VisitMut};
use crate::expr::{BinaryOperator, Cond, Expr};
use crate::val::{Number, RecordIdKey, Value};

/// `true` when substituting `value`'s literal form into an expression would
/// change evaluation results, at any nesting depth. `Value::into_literal` is
/// lossy for two variants: a closure literal drops the closure's captures,
/// and a set becomes an array literal, which evaluates to an array — and
/// arrays compare unequal to sets at runtime. Callers must leave the original
/// expression in place (runtime evaluation yields the correct value) instead
/// of substituting a lossy form.
pub(crate) fn substitution_is_lossy(value: &Value) -> bool {
	fn bound_is_lossy(bound: &Bound<Value>) -> bool {
		match bound {
			Bound::Included(v) | Bound::Excluded(v) => substitution_is_lossy(v),
			Bound::Unbounded => false,
		}
	}
	fn key_bound_is_lossy(bound: &Bound<RecordIdKey>) -> bool {
		match bound {
			Bound::Included(k) | Bound::Excluded(k) => key_is_lossy(k),
			Bound::Unbounded => false,
		}
	}
	fn key_is_lossy(key: &RecordIdKey) -> bool {
		match key {
			RecordIdKey::Number(_) | RecordIdKey::String(_) | RecordIdKey::Uuid(_) => false,
			RecordIdKey::Array(a) => a.iter().any(substitution_is_lossy),
			RecordIdKey::Object(o) => o.values().any(substitution_is_lossy),
			RecordIdKey::Range(r) => key_bound_is_lossy(&r.start) || key_bound_is_lossy(&r.end),
		}
	}
	match value {
		Value::Closure(_) | Value::Set(_) => true,
		Value::Array(a) => a.iter().any(substitution_is_lossy),
		Value::Object(o) => o.values().any(substitution_is_lossy),
		Value::Range(r) => bound_is_lossy(&r.start) || bound_is_lossy(&r.end),
		Value::RecordId(rid) => key_is_lossy(&rid.key),
		_ => false,
	}
}

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
		Literal::Datetime(dt) => Some(Value::Datetime(*dt)),
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

/// Fold constant, document-independent expressions in a `WHERE` condition to
/// literal values. This enables proper index range access for expressions like
/// `time::now() - 365d` which would otherwise be opaque to index analysis.
///
/// Must be called **after** [`super::params::resolve_condition_params`] so
/// that parameter references have already been replaced with literals.
///
/// Only folds expressions that:
/// - Contain no field/idiom references (document-independent)
/// - Are arithmetic on literals, or calls to a built-in that is both context-free and deterministic
///   (math::*, string::*, type::*, etc.) with all arguments already literals. `rand::*` is
///   context-free but not deterministic, so it is left for per-row evaluation.
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
/// - `time::now()` → `Literal::Datetime(now)` (special case: non-deterministic, but folded once per
///   statement by deliberate exception)
/// - Calls to a context-free, deterministic function whose arguments are all already literals
///   (math::floor, string::lowercase, type::int, etc.)
/// - Binary arithmetic on two literals (datetime ± duration, number ± number, etc.)
///
/// Both conditions on a call matter and they are different questions:
/// [`ScalarFunction::is_pure`] asks whether it can run without an execution
/// context, [`ScalarFunction::is_deterministic`] whether repeated calls agree.
/// `rand::*` answers yes then no, and folding it would freeze one draw into
/// every row.
fn try_fold_to_literal(expr: &Expr, registry: &FunctionRegistry) -> Option<Expr> {
	use crate::expr::Function;
	use crate::val::{Datetime, Value};

	match expr {
		// time::now() → current datetime literal
		// Special case: time::now() is not deterministic (it reads the clock),
		// so the generic branch below would refuse it. Folding it once per
		// statement is intended, matching SQL semantics, hence this arm ahead
		// of the determinism gate.
		Expr::FunctionCall(fc)
			if matches!(&fc.receiver, Function::Normal(name) if name == "time::now")
				&& fc.arguments.is_empty() =>
		{
			Some(Value::Datetime(Datetime::now()).into_literal())
		}

		// Context-free, deterministic call whose arguments are all literals.
		// After bottom-up folding, nested expressions like `math::floor(20 + 0.5)`
		// will have their arguments folded first, so we only need to check
		// whether the immediate arguments are literals.
		Expr::FunctionCall(fc) => {
			let Function::Normal(name) = &fc.receiver else {
				return None;
			};
			let func = registry.get(name.as_str())?;
			// `is_pure` only says the call needs no context; folding also
			// requires that repeated calls agree, or the single plan-time
			// result would stand in for every row.
			if !func.is_pure() || func.is_async() || !func.is_deterministic() {
				return None;
			}
			// All arguments must be convertible to constant Values
			let args: Option<Vec<Value>> = fc.arguments.iter().map(try_expr_to_value).collect();
			let args = args?;
			// Sound to invoke here and reuse for the statement: purity means no
			// context is needed, determinism that one draw stands for all rows
			let result = func.invoke(args).ok()?;
			Some(result.into_literal())
		}

		// `x INSIDE []` is provably false — no element can match an empty
		// array. Folding to `false` lets the surrounding AND/OR collapse
		// via the short-circuit rules below, ultimately yielding
		// `AccessPath::EmptyScan` for the whole SELECT.
		Expr::Binary {
			op: BinaryOperator::Inside,
			right,
			..
		} if is_empty_array_literal(right) => {
			Some(Expr::Literal(crate::expr::literal::Literal::Bool(false)))
		}

		// AND / OR short-circuits when one side is a constant boolean.
		// Without this, `field IN [] AND ...` would never simplify because
		// the AND's left operand is not a literal.
		Expr::Binary {
			left,
			op,
			right,
		} => {
			if let Some(short) = try_short_circuit_bool(left, op, right) {
				return Some(short);
			}
			let left_val = try_expr_to_value(left)?;
			let right_val = try_expr_to_value(right)?;
			let result = try_eval_binary(op, left_val, right_val)?;
			Some(result.into_literal())
		}

		_ => None,
	}
}

/// Returns `true` if `expr` is a literal array with no elements.
fn is_empty_array_literal(expr: &Expr) -> bool {
	matches!(expr, Expr::Literal(crate::expr::literal::Literal::Array(arr)) if arr.is_empty())
}

/// Try to short-circuit a logical AND/OR when one operand is a constant bool.
///
/// - `false AND _` and `_ AND false` → `false`
/// - `true AND x`  and `x AND true`  → the other operand
/// - `true OR _`   and `_ OR true`   → `true`
/// - `false OR x`  and `x OR false`  → the other operand
fn try_short_circuit_bool(left: &Expr, op: &BinaryOperator, right: &Expr) -> Option<Expr> {
	use crate::expr::literal::Literal;
	let l_bool = match left {
		Expr::Literal(Literal::Bool(b)) => Some(*b),
		_ => None,
	};
	let r_bool = match right {
		Expr::Literal(Literal::Bool(b)) => Some(*b),
		_ => None,
	};
	match (op, l_bool, r_bool) {
		(BinaryOperator::And, Some(false), _) | (BinaryOperator::And, _, Some(false)) => {
			Some(Expr::Literal(Literal::Bool(false)))
		}
		(BinaryOperator::And, Some(true), _) => Some(right.clone()),
		(BinaryOperator::And, _, Some(true)) => Some(left.clone()),
		(BinaryOperator::Or, Some(true), _) | (BinaryOperator::Or, _, Some(true)) => {
			Some(Expr::Literal(Literal::Bool(true)))
		}
		(BinaryOperator::Or, Some(false), _) => Some(right.clone()),
		(BinaryOperator::Or, _, Some(false)) => Some(left.clone()),
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
/// Returns `EngineError::Internal` for types that should have been handled upstream
/// by `physical_expr()` (RecordId, Array, Object, Set).
pub(crate) fn literal_to_value(
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
		other => Err(EngineError::Internal(format!(
			"Literal should be handled upstream in physical_expr(): {:?}",
			std::mem::discriminant(&other)
		))
		.into()),
	}
}

/// Convert a `RecordIdKeyLit` to an `Expr`.
pub(crate) fn key_lit_to_expr(lit: &crate::expr::RecordIdKeyLit) -> Result<Expr, Error> {
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
		RecordIdKeyLit::Generate(_) => Err(ExecError::Query {
			message: "Generated keys (rand, ulid, uuid) cannot be used in graph range bounds"
				.to_string(),
		}
		.into()),
		RecordIdKeyLit::Range(_) => Err(ExecError::Query {
			message: "Nested range keys cannot be used in graph range bounds".to_string(),
		}
		.into()),
	}
}
