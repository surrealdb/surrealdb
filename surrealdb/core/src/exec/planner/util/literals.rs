//! Literal/value conversion and constant folding.
//!
//! Pure (non-async) helpers that convert between AST literals and runtime
//! values, plus the plan-time constant folder used to reduce
//! deterministic, document-independent expressions in WHERE clauses to
//! literals so the index analyzer can match them.

use std::collections::HashSet;
use std::ops::Bound;

use crate::dbs::capabilities::Capabilities;
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
/// - Call only functions `capabilities` permits the caller to execute.
///
/// `time::now()` is evaluated once at plan time, consistent with how most
/// databases evaluate `NOW()` once per statement/transaction.
pub(crate) fn fold_condition_expressions(
	cond: &mut Cond,
	registry: &FunctionRegistry,
	capabilities: &Capabilities,
	restricted_fields: Option<&HashSet<String>>,
) {
	let mut folder = ExpressionFolder {
		registry,
		capabilities,
		restricted_fields,
	};
	let _ = folder.visit_mut_expr(&mut cond.0);
}

/// `true` when a bare field read off any row these `FROM` sources produce
/// evaluates to a plain value without fetching anything.
///
/// A table's rows are records, and therefore objects, so a field read is a key
/// lookup. A scalar source yields that scalar, and a field read off a scalar is
/// `NONE`. Any other source can yield a record id — directly, or inside an
/// array the read maps over — where the same read dereferences it and runs
/// computed fields and permissions on the target.
///
/// Every source must qualify: `plan_sources` hands the same condition to each
/// one, so a single dereferencing source makes the whole condition unsafe to
/// discard. Unrecognised sources are excluded, which costs an optimisation
/// rather than an enforcement.
pub(crate) fn source_field_reads_are_inert(what: &[Expr]) -> bool {
	use crate::expr::literal::Literal;
	what.iter().all(|e| match e {
		Expr::Table(_) => true,
		// Every `Constant` yields a float, datetime or duration, so its row is
		// a scalar like any other.
		Expr::Constant(_) => true,
		// An object row's bare field read is a key lookup like a record's; the
		// expressions building the object are evaluated by the source either way,
		// fold or no fold.
		Expr::Literal(Literal::Object(_)) => true,
		Expr::Literal(lit) => matches!(
			lit,
			Literal::None
				| Literal::Null
				| Literal::Bool(_)
				| Literal::Float(_)
				| Literal::Integer(_)
				| Literal::Decimal(_)
				| Literal::String(_)
				| Literal::Bytes(_)
				| Literal::Regex(_)
				| Literal::Duration(_)
				| Literal::Datetime(_)
				| Literal::Uuid(_)
				| Literal::Geometry(_)
				| Literal::File(_)
		),
		_ => false,
	})
}

/// MutVisitor that replaces constant expression subtrees with their literal
/// values. Processes bottom-up: children are folded first, then the parent
/// node is checked.
struct ExpressionFolder<'a> {
	registry: &'a FunctionRegistry,
	capabilities: &'a Capabilities,
	/// Names whose read is not inert, or `None` when no field read reaching
	/// this condition is. See [`source_field_reads_are_inert`].
	restricted_fields: Option<&'a HashSet<String>>,
}

impl MutVisitor for ExpressionFolder<'_> {
	type Error = std::convert::Infallible;

	fn visit_mut_expr(&mut self, expr: &mut Expr) -> Result<(), Self::Error> {
		// First recurse into children (bottom-up folding)
		expr.visit_mut(self)?;

		// Then try to fold this node to a literal
		if let Some(folded) =
			try_fold_to_literal(expr, self.registry, self.capabilities, self.restricted_fields)
		{
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

/// `true` when evaluating `expr` cannot dispatch anything a capability gates,
/// so a fold may discard it without hiding an error evaluation would raise.
///
/// This is a fail-closed whitelist. Every variant is listed and anything not
/// explicitly cleared counts as capable of invoking something, so a new
/// `Expr`, `Part` or `BinaryOperator` variant stops compiling until someone
/// decides which side it falls on rather than silently becoming discardable.
///
/// Enumerating the ways a subtree *can* reach a denial does not work, because
/// there are too many and they do not share a shape: a built-in enforces
/// network, scripting or experimental capabilities of its own beyond its
/// name; a method call is an `Idiom` part and a closure call an
/// `Expr::Postfix`, neither of them an `Expr::FunctionCall`; and a
/// user-defined function, module, script or `eval::surql` runs a body that is
/// not visible at plan time at all. Clearing only the shapes that provably
/// reach nothing is the side of that line which fails safe.
///
/// Being capability-independent is deliberate. An operand that can invoke
/// something is kept whatever the configuration permits, which also stops a
/// fold from discarding a call with side effects — what evaluation would have
/// run, it still runs.
fn evaluation_is_inert(expr: &Expr, restricted_fields: Option<&HashSet<String>>) -> bool {
	use crate::expr::part::Part;

	fn operator_is_inert(op: &BinaryOperator) -> bool {
		match op {
			BinaryOperator::Subtract
			| BinaryOperator::Add
			| BinaryOperator::Multiply
			| BinaryOperator::Divide
			| BinaryOperator::Remainder
			| BinaryOperator::Power
			| BinaryOperator::Equal
			| BinaryOperator::ExactEqual
			| BinaryOperator::NotEqual
			| BinaryOperator::AllEqual
			| BinaryOperator::AnyEqual
			| BinaryOperator::Or
			| BinaryOperator::And
			| BinaryOperator::NullCoalescing
			| BinaryOperator::TenaryCondition
			| BinaryOperator::LessThan
			| BinaryOperator::LessThanEqual
			| BinaryOperator::MoreThan
			| BinaryOperator::MoreThanEqual
			| BinaryOperator::Contain
			| BinaryOperator::NotContain
			| BinaryOperator::Inside
			| BinaryOperator::NotInside
			| BinaryOperator::ContainAll
			| BinaryOperator::ContainAny
			| BinaryOperator::ContainNone
			| BinaryOperator::AllInside
			| BinaryOperator::AnyInside
			| BinaryOperator::NoneInside
			| BinaryOperator::Outside
			| BinaryOperator::Intersects
			// A range operator builds a `Value::Range` from its two bounds and
			// dispatches nothing; the bounds themselves are checked above.
			| BinaryOperator::Range
			| BinaryOperator::RangeInclusive
			| BinaryOperator::RangeSkip
			| BinaryOperator::RangeSkipInclusive => true,
			// `@@` and `<|..|>` dispatch index functions.
			BinaryOperator::Matches(_) | BinaryOperator::NearestNeighbor(_) => false,
		}
	}

	match expr {
		// Composite literals evaluate their element expressions in place.
		Expr::Literal(lit) => literal_is_inert(lit, restricted_fields),
		Expr::Table(_) | Expr::Mock(_) | Expr::Constant(_) => true,
		// A parameter that still reaches here is one resolution deliberately
		// left alone: a closure, whose captures make substitution lossy, or a
		// `DEFINE PARAM` whose permission clause is evaluated per read. Both
		// dispatch on evaluation, so neither may be discarded. A parameter
		// carrying a plain value was already replaced by its literal, and
		// folds on that literal instead.
		Expr::Param(_) => false,
		// A single field read is inert only when the row it reads from is a
		// record and reading that particular field evaluates nothing. Field
		// access recurses through arrays and fetches any record id it lands
		// on, evaluating that record's computed fields and permission
		// expressions — so over a source like `[[target:1]]` even a one-part
		// idiom dereferences, and from the second part onward the receiver can
		// be a record id whatever the source. Over a table the read is a plain
		// key lookup, so `a IN []` still collapses, while `ref.bad IN []` and
		// a read of a field materialisation has to compute are kept.
		Expr::Idiom(idiom) => match (restricted_fields, idiom.0.as_slice()) {
			(Some(restricted), [Part::Field(name)]) => !restricted.contains(name.as_str()),
			_ => false,
		},
		Expr::Prefix {
			expr,
			..
		} => evaluation_is_inert(expr, restricted_fields),
		Expr::Binary {
			left,
			op,
			right,
		} => {
			operator_is_inert(op)
				&& evaluation_is_inert(left, restricted_fields)
				&& evaluation_is_inert(right, restricted_fields)
		}
		// Everything else can reach a dispatch: a call of any kind, a closure
		// body, a block or statement, or a subquery with its own planning.
		Expr::Postfix {
			..
		}
		| Expr::FunctionCall(_)
		| Expr::Closure(_)
		| Expr::Block(_)
		| Expr::Break
		| Expr::Continue
		| Expr::Return(_)
		| Expr::Throw(_)
		| Expr::IfElse(_)
		| Expr::Select(_)
		| Expr::Create(_)
		| Expr::Update(_)
		| Expr::Upsert(_)
		| Expr::Delete(_)
		| Expr::Relate(_)
		| Expr::Insert(_)
		| Expr::Define(_)
		| Expr::Remove(_)
		| Expr::Rebuild(_)
		| Expr::Alter(_)
		| Expr::Info(_)
		| Expr::Foreach(_)
		| Expr::Let(_)
		| Expr::Sleep(_)
		| Expr::Explain {
			..
		}
		| Expr::Match(_) => false,
	}
}

/// `true` when every expression a literal carries is itself inert.
fn literal_is_inert(
	lit: &crate::expr::literal::Literal,
	restricted_fields: Option<&HashSet<String>>,
) -> bool {
	use crate::expr::literal::Literal;

	match lit {
		Literal::None
		| Literal::Null
		| Literal::Bool(_)
		| Literal::Float(_)
		| Literal::Integer(_)
		| Literal::Decimal(_)
		| Literal::String(_)
		| Literal::Uuid(_)
		| Literal::Datetime(_)
		| Literal::Duration(_)
		| Literal::Bytes(_)
		| Literal::Regex(_)
		| Literal::Geometry(_)
		| Literal::File(_)
		| Literal::UnboundedRange => true,
		Literal::Array(exprs) | Literal::Set(exprs) => {
			exprs.iter().all(|e| evaluation_is_inert(e, restricted_fields))
		}
		Literal::Object(entries) => {
			entries.iter().all(|e| evaluation_is_inert(&e.value, restricted_fields))
		}
		// A record id builds a value from its key; whether that dispatches
		// depends on the key.
		Literal::RecordId(rid) => record_id_key_is_inert(&rid.key, restricted_fields),
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
///
/// A call is folded only when `capabilities` permits the caller to execute it.
/// Folding runs the function here, at plan time, and no physical expression is
/// built for what it replaces — so this is the only place the function-name
/// capability can be enforced for a folded call. A denied call is left
/// unfolded rather than rejected here: it stays an ordinary call and fails at
/// evaluation with `FunctionNotAllowed`, which keeps the error identical to
/// the unfolded path and keeps plan construction (including `EXPLAIN`)
/// infallible.
fn try_fold_to_literal(
	expr: &Expr,
	registry: &FunctionRegistry,
	capabilities: &Capabilities,
	restricted_fields: Option<&HashSet<String>>,
) -> Option<Expr> {
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
				&& fc.arguments.is_empty()
				&& capabilities.allows_function_name("time::now") =>
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
			// Folding executes the call, so the caller must be allowed to run
			// it. Left unfolded when denied, so the physical expression built
			// for it raises `FunctionNotAllowed` at evaluation.
			if !capabilities.allows_function_name(name.as_str()) {
				return None;
			}
			// A pure built-in behind an experimental target is gated the same
			// way, and for the same reason: the physical expression raises the
			// experimental error at evaluation.
			if let Some(target) = crate::exec::function::experimental_target(name.as_str())
				&& !capabilities.allows_experimental(&target)
			{
				return None;
			}
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
			left,
			right,
		} if is_empty_array_literal(right) && evaluation_is_inert(left, restricted_fields) => {
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
			if let Some(short) = try_short_circuit_bool(left, op, right, restricted_fields) {
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
///
/// Only the operand that runtime evaluation would actually reach needs
/// protecting. `AND`/`OR` short-circuit left to right, so `false AND x` and
/// `true OR x` never evaluate `x`: collapsing them discards a subtree that
/// would not have run anyway, and folding is sound even when `x` holds a
/// denied call. The mirrored arms are different — `x AND false` and
/// `x OR true` do evaluate `x` first — so those are refused when `x` holds a
/// call the caller may not execute; see [`evaluation_is_inert`]. The arms
/// that return the other operand discard only a bool literal, which can hold
/// no call.
/// `true` when this expression can only evaluate to a boolean.
///
/// `AND` and `OR` yield one of their operands rather than a boolean: `7 OR true`
/// is `7`, and `7 AND true` is `true`. A rewrite that replaces the whole
/// expression with a constant, or with the operand alone, therefore only
/// preserves the value when the left operand could not have carried one of its
/// own. Truthiness is not enough — the folded node can sit anywhere in the
/// condition, including under a comparison that reads its value.
fn is_boolean_valued(expr: &Expr) -> bool {
	use crate::expr::literal::Literal;
	match expr {
		Expr::Literal(Literal::Bool(_)) => true,
		Expr::Prefix {
			op: crate::expr::PrefixOperator::Not,
			..
		} => true,
		Expr::Binary {
			op,
			..
		} => match op {
			BinaryOperator::Equal
			| BinaryOperator::ExactEqual
			| BinaryOperator::NotEqual
			| BinaryOperator::AllEqual
			| BinaryOperator::AnyEqual
			| BinaryOperator::LessThan
			| BinaryOperator::LessThanEqual
			| BinaryOperator::MoreThan
			| BinaryOperator::MoreThanEqual
			| BinaryOperator::Contain
			| BinaryOperator::NotContain
			| BinaryOperator::ContainAll
			| BinaryOperator::ContainAny
			| BinaryOperator::ContainNone
			| BinaryOperator::Inside
			| BinaryOperator::NotInside
			| BinaryOperator::AllInside
			| BinaryOperator::AnyInside
			| BinaryOperator::NoneInside
			| BinaryOperator::Outside
			| BinaryOperator::Intersects
			| BinaryOperator::Matches(_) => true,
			// `AND`/`OR` return an operand, `??`/`?:` select one, and the rest
			// are arithmetic, ranges or a KNN operator.
			BinaryOperator::And
			| BinaryOperator::Or
			| BinaryOperator::NullCoalescing
			| BinaryOperator::TenaryCondition
			| BinaryOperator::Subtract
			| BinaryOperator::Add
			| BinaryOperator::Multiply
			| BinaryOperator::Divide
			| BinaryOperator::Remainder
			| BinaryOperator::Power
			| BinaryOperator::Range
			| BinaryOperator::RangeInclusive
			| BinaryOperator::RangeSkip
			| BinaryOperator::RangeSkipInclusive
			| BinaryOperator::NearestNeighbor(_) => false,
		},
		_ => false,
	}
}

fn try_short_circuit_bool(
	left: &Expr,
	op: &BinaryOperator,
	right: &Expr,
	restricted_fields: Option<&HashSet<String>>,
) -> Option<Expr> {
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
		// `false AND x` / `true OR x`: `x` is never evaluated at runtime, so
		// discarding it is what evaluation would have done.
		(BinaryOperator::And, Some(false), _) => Some(Expr::Literal(Literal::Bool(false))),
		(BinaryOperator::Or, Some(true), _) => Some(Expr::Literal(Literal::Bool(true))),
		// `x AND false` / `x OR true`: `x` runs first, so it has to survive
		// when it holds a call the caller may not execute, and its value has
		// to be one the rewrite can stand in for.
		(BinaryOperator::And, _, Some(false))
			if is_boolean_valued(left) && evaluation_is_inert(left, restricted_fields) =>
		{
			Some(Expr::Literal(Literal::Bool(false)))
		}
		(BinaryOperator::Or, _, Some(true))
			if is_boolean_valued(left) && evaluation_is_inert(left, restricted_fields) =>
		{
			Some(Expr::Literal(Literal::Bool(true)))
		}
		(BinaryOperator::And, Some(true), _) => Some(right.clone()),
		(BinaryOperator::Or, Some(false), _) => Some(right.clone()),
		(BinaryOperator::And, _, Some(true)) if is_boolean_valued(left) => Some(left.clone()),
		(BinaryOperator::Or, _, Some(false)) if is_boolean_valued(left) => Some(left.clone()),
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

/// `true` when building a record-id key evaluates nothing that could dispatch,
/// and yields the same key every time.
///
/// A scalar key is just a value. A composite one carries expressions, which are
/// held to the same standard as anywhere else. `Generate` is excluded because
/// it draws a fresh key per evaluation, so discarding it would freeze one draw
/// — the same reason `rand::*` is never folded.
fn record_id_key_is_inert(
	key: &crate::expr::RecordIdKeyLit,
	restricted_fields: Option<&HashSet<String>>,
) -> bool {
	use std::ops::Bound;

	use crate::expr::RecordIdKeyLit;

	fn bound_is_inert(
		bound: &Bound<RecordIdKeyLit>,
		restricted_fields: Option<&HashSet<String>>,
	) -> bool {
		match bound {
			Bound::Included(k) | Bound::Excluded(k) => record_id_key_is_inert(k, restricted_fields),
			Bound::Unbounded => true,
		}
	}

	match key {
		RecordIdKeyLit::Number(_) | RecordIdKeyLit::String(_) | RecordIdKeyLit::Uuid(_) => true,
		RecordIdKeyLit::Array(exprs) => {
			exprs.iter().all(|e| evaluation_is_inert(e, restricted_fields))
		}
		RecordIdKeyLit::Object(entries) => {
			entries.iter().all(|e| evaluation_is_inert(&e.value, restricted_fields))
		}
		RecordIdKeyLit::Range(range) => {
			bound_is_inert(&range.start, restricted_fields)
				&& bound_is_inert(&range.end, restricted_fields)
		}
		RecordIdKeyLit::Generate(_) => false,
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
