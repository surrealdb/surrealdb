//! Literal/value conversion and constant folding over the logical scalar type.
//!
//! Pure (non-async) helpers that convert between [`LogicalExpr`] literals and
//! runtime values in both directions, plus the plan-time constant folder that
//! reduces deterministic, row-independent expressions to literals so the
//! index analyzer can match them.
//!
//! A literal here is any scalar that denotes a value without evaluation: a
//! [`LogicalLiteral`] leaf, or an `Array`, `Set`, `Object` or `RecordId`
//! whose every element is itself such a literal. A [`LogicalExpr::Subquery`]
//! never converts and never folds.

use std::collections::{BTreeMap, HashSet};
use std::ops::Bound;

use crate::dbs::capabilities::Capabilities;
use crate::exec::function::FunctionRegistry;
use crate::exec::index::access_path::{BoundOperand, ClockBound, ClockSign};
use crate::expr::logical::{
	LogicalExpr, LogicalLiteral, LogicalOp, LogicalPostfixOperator, LogicalRecordIdKey,
	LogicalRecordIdKeyRange, LogicalRecordIdLit, LogicalSource, LogicalValues, Schema, SourceRef,
};
use crate::expr::{BinaryOperator, Function, Kind, PrefixOperator};
use crate::val::{Number, Range, RecordId, RecordIdKey, RecordIdKeyRange, Value};

/// Field names whose read is not inert on the rows a condition is evaluated
/// over. See [`source_field_reads_are_inert`] and
/// [`try_fold_to_literal`]'s `restricted_fields`.
pub(crate) type RestrictedFields = HashSet<String>;

// ============================================================================
// Value -> LogicalExpr
// ============================================================================

/// `true` when `value` has no literal form that evaluates back to it, at any
/// nesting depth. A closure is the one such value: its literal drops the
/// captures. Callers must leave the original expression in place (runtime
/// evaluation yields the correct value) instead of substituting a lossy
/// form; [`try_value_to_logical_expr`] answers `None` for exactly these.
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
		Value::Closure(_) => true,
		Value::Array(a) => a.iter().any(substitution_is_lossy),
		Value::Set(s) => s.iter().any(substitution_is_lossy),
		Value::Object(o) => o.values().any(substitution_is_lossy),
		Value::Range(r) => bound_is_lossy(&r.start) || bound_is_lossy(&r.end),
		Value::RecordId(rid) => key_is_lossy(&rid.key),
		Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::String(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Uuid(_)
		| Value::Geometry(_)
		| Value::Bytes(_)
		| Value::Table(_)
		| Value::File(_)
		| Value::Regex(_) => false,
	}
}

/// The logical literal that evaluates to `value`.
///
/// The inverse of [`try_literal_to_value`]: every value converts except a
/// closure, whose captures have no literal form. Callers check
/// [`substitution_is_lossy`] first; a value it rejects has no logical form and
/// is a precondition violation here.
pub(crate) fn value_to_logical_expr(value: Value) -> LogicalExpr {
	try_value_to_logical_expr(value)
		.expect("value has a logical literal form; `substitution_is_lossy` admits it")
}

/// [`value_to_logical_expr`], answering `None` for a value with no literal
/// form (see [`substitution_is_lossy`]).
pub(crate) fn try_value_to_logical_expr(value: Value) -> Option<LogicalExpr> {
	let leaf = |literal| Some(LogicalExpr::Literal(literal));
	match value {
		Value::None => leaf(LogicalLiteral::None),
		Value::Null => leaf(LogicalLiteral::Null),
		Value::Bool(x) => leaf(LogicalLiteral::Bool(x)),
		Value::Number(Number::Int(x)) => leaf(LogicalLiteral::Integer(x)),
		Value::Number(Number::Float(x)) => leaf(LogicalLiteral::Float(x)),
		Value::Number(Number::Decimal(x)) => leaf(LogicalLiteral::Decimal(x)),
		Value::String(x) => leaf(LogicalLiteral::String(x)),
		Value::Duration(x) => leaf(LogicalLiteral::Duration(x)),
		Value::Datetime(x) => leaf(LogicalLiteral::Datetime(x)),
		Value::Uuid(x) => leaf(LogicalLiteral::Uuid(x)),
		Value::Geometry(x) => leaf(LogicalLiteral::Geometry(x)),
		Value::Bytes(x) => leaf(LogicalLiteral::Bytes(x)),
		Value::Regex(x) => leaf(LogicalLiteral::Regex(x)),
		Value::File(x) => leaf(LogicalLiteral::File(x)),
		Value::Table(t) => Some(LogicalExpr::Table(t)),
		Value::Array(array) => Some(LogicalExpr::Array(values_to_exprs(array.0)?)),
		Value::Set(set) => Some(LogicalExpr::Set(values_to_exprs(Vec::from(set))?)),
		Value::Object(object) => Some(LogicalExpr::Object(object_to_entries(object)?)),
		Value::RecordId(rid) => Some(LogicalExpr::RecordId(Box::new(LogicalRecordIdLit {
			table: rid.table,
			key: key_to_logical(rid.key)?,
		}))),
		Value::Range(range) => range_to_logical(*range),
		Value::Closure(_) => None,
	}
}

fn values_to_exprs(values: Vec<Value>) -> Option<Vec<LogicalExpr>> {
	values.into_iter().map(try_value_to_logical_expr).collect()
}

fn object_to_entries(object: crate::val::Object) -> Option<Vec<(String, LogicalExpr)>> {
	object
		.0
		.into_iter()
		.map(|(k, v)| Some((k.into_string(), try_value_to_logical_expr(v)?)))
		.collect()
}

fn key_to_logical(key: RecordIdKey) -> Option<LogicalRecordIdKey> {
	Some(match key {
		RecordIdKey::Number(x) => LogicalRecordIdKey::Number(x),
		RecordIdKey::String(x) => LogicalRecordIdKey::String(x),
		RecordIdKey::Uuid(x) => LogicalRecordIdKey::Uuid(x),
		RecordIdKey::Array(array) => LogicalRecordIdKey::Array(values_to_exprs(array.0)?),
		RecordIdKey::Object(object) => LogicalRecordIdKey::Object(object_to_entries(object)?),
		RecordIdKey::Range(range) => {
			LogicalRecordIdKey::Range(Box::new(key_range_to_logical(*range)?))
		}
	})
}

fn key_range_to_logical(range: RecordIdKeyRange) -> Option<LogicalRecordIdKeyRange> {
	let bound = |b: Bound<RecordIdKey>| {
		Some(match b {
			Bound::Included(k) => Bound::Included(key_to_logical(k)?),
			Bound::Excluded(k) => Bound::Excluded(key_to_logical(k)?),
			Bound::Unbounded => Bound::Unbounded,
		})
	};
	Some(LogicalRecordIdKeyRange {
		start: bound(range.start)?,
		end: bound(range.end)?,
	})
}

/// A range value denotes the operator application that builds it, as the
/// expression layer's literal form does: a bound on one side is a prefix or
/// postfix range operator, bounds on both sides a binary one.
fn range_to_logical(range: Range) -> Option<LogicalExpr> {
	let lit = try_value_to_logical_expr;
	Some(match (range.start, range.end) {
		(Bound::Unbounded, Bound::Unbounded) => {
			LogicalExpr::Literal(LogicalLiteral::UnboundedRange)
		}
		(Bound::Included(x), Bound::Unbounded) => LogicalExpr::Postfix {
			expr: Box::new(lit(x)?),
			op: LogicalPostfixOperator::Range,
		},
		(Bound::Excluded(x), Bound::Unbounded) => LogicalExpr::Postfix {
			expr: Box::new(lit(x)?),
			op: LogicalPostfixOperator::RangeSkip,
		},
		(Bound::Unbounded, Bound::Included(y)) => LogicalExpr::Prefix {
			op: PrefixOperator::RangeInclusive,
			expr: Box::new(lit(y)?),
		},
		(Bound::Unbounded, Bound::Excluded(y)) => LogicalExpr::Prefix {
			op: PrefixOperator::Range,
			expr: Box::new(lit(y)?),
		},
		(Bound::Included(x), Bound::Included(y)) => LogicalExpr::Binary {
			left: Box::new(lit(x)?),
			op: BinaryOperator::RangeInclusive,
			right: Box::new(lit(y)?),
		},
		(Bound::Excluded(x), Bound::Included(y)) => LogicalExpr::Binary {
			left: Box::new(lit(x)?),
			op: BinaryOperator::RangeSkipInclusive,
			right: Box::new(lit(y)?),
		},
		(Bound::Included(x), Bound::Excluded(y)) => LogicalExpr::Binary {
			left: Box::new(lit(x)?),
			op: BinaryOperator::Range,
			right: Box::new(lit(y)?),
		},
		(Bound::Excluded(x), Bound::Excluded(y)) => LogicalExpr::Binary {
			left: Box::new(lit(x)?),
			op: BinaryOperator::RangeSkip,
			right: Box::new(lit(y)?),
		},
	})
}

// ============================================================================
// LogicalExpr -> Value
// ============================================================================

/// The value a leaf literal denotes. Infallible: a leaf holds no expression.
pub(crate) fn leaf_literal_to_value(lit: &LogicalLiteral) -> Value {
	match lit {
		LogicalLiteral::None => Value::None,
		LogicalLiteral::Null => Value::Null,
		LogicalLiteral::UnboundedRange => Value::Range(Box::new(Range::unbounded())),
		LogicalLiteral::Bool(x) => Value::Bool(*x),
		LogicalLiteral::Float(x) => Value::Number(Number::Float(*x)),
		LogicalLiteral::Integer(x) => Value::Number(Number::Int(*x)),
		LogicalLiteral::Decimal(x) => Value::Number(Number::Decimal(*x)),
		LogicalLiteral::String(x) => Value::String(x.clone()),
		LogicalLiteral::Bytes(x) => Value::Bytes(x.clone()),
		LogicalLiteral::Regex(x) => Value::Regex(x.clone()),
		LogicalLiteral::Duration(x) => Value::Duration(*x),
		LogicalLiteral::Datetime(x) => Value::Datetime(*x),
		LogicalLiteral::Uuid(x) => Value::Uuid(*x),
		LogicalLiteral::Geometry(x) => Value::Geometry(x.clone()),
		LogicalLiteral::File(x) => Value::File(x.clone()),
	}
}

/// The value a literal expression denotes, when it denotes one.
///
/// A leaf always does, and so does a `Table`, which denotes its name. An
/// `Array`, `Set` or `Object` does when every element does; a `RecordId` when
/// its key is a number, string or uuid — a compound key holds expressions and
/// a generated key is allocated at evaluation. Every other expression,
/// including a `Subquery`, answers `None`.
///
/// Used by both the planner (for physical expression compilation) and the
/// index analyzer (for index matching).
pub(crate) fn try_literal_to_value(expr: &LogicalExpr) -> Option<Value> {
	match expr {
		LogicalExpr::Literal(lit) => Some(leaf_literal_to_value(lit)),
		LogicalExpr::Table(table) => Some(Value::Table(table.clone())),
		// The ids are generated at evaluation, so a mock denotes no one value.
		LogicalExpr::Mock(_) => None,
		// A range written between two literals is itself a literal: the
		// operator says which end is open, and both operands must fold for
		// the range to have a value at all.
		LogicalExpr::Binary {
			left,
			op:
				op @ (BinaryOperator::Range
				| BinaryOperator::RangeInclusive
				| BinaryOperator::RangeSkip
				| BinaryOperator::RangeSkipInclusive),
			right,
		} => {
			let start = try_literal_to_value(left)?;
			let end = try_literal_to_value(right)?;
			let start = match op {
				BinaryOperator::Range | BinaryOperator::RangeInclusive => Bound::Included(start),
				_ => Bound::Excluded(start),
			};
			let end = match op {
				BinaryOperator::Range | BinaryOperator::RangeSkip => Bound::Excluded(end),
				_ => Bound::Included(end),
			};
			Some(Value::Range(Box::new(Range {
				start,
				end,
			})))
		}
		LogicalExpr::Array(items) => {
			let values: Option<Vec<Value>> = items.iter().map(try_literal_to_value).collect();
			values.map(|v| Value::Array(crate::val::Array(v)))
		}
		LogicalExpr::Set(items) => {
			let mut set = crate::val::Set::new();
			for item in items {
				set.insert(try_literal_to_value(item)?);
			}
			Some(Value::Set(set))
		}
		LogicalExpr::Object(entries) => {
			let mut map = BTreeMap::new();
			for (key, value) in entries {
				map.insert(key.clone(), try_literal_to_value(value)?);
			}
			Some(Value::Object(crate::val::Object::from(map)))
		}
		LogicalExpr::RecordId(rid) => {
			let key = match &rid.key {
				LogicalRecordIdKey::Number(n) => RecordIdKey::Number(*n),
				LogicalRecordIdKey::String(s) => RecordIdKey::String(s.clone()),
				LogicalRecordIdKey::Uuid(u) => RecordIdKey::Uuid(*u),
				LogicalRecordIdKey::Array(_)
				| LogicalRecordIdKey::Object(_)
				| LogicalRecordIdKey::Generate(_)
				| LogicalRecordIdKey::Range(_) => return None,
			};
			Some(Value::RecordId(RecordId::new(rid.table.clone(), key)))
		}
		LogicalExpr::Param(_)
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
		| LogicalExpr::Closure(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue
		| LogicalExpr::Subquery(_) => None,
	}
}

/// The statement clock `expr` reads as an index bound: `time::now()`, or
/// `time::now() + <duration literal>` or `time::now() - <duration literal>`.
///
/// Anything else that reads the clock, a nested offset or an offset that is not
/// a duration literal among them, answers `None` and stays a filter.
pub(crate) fn try_clock_bound(expr: &LogicalExpr) -> Option<ClockBound> {
	fn is_now(expr: &LogicalExpr) -> bool {
		matches!(
			expr,
			LogicalExpr::Call(call)
				if call.args.is_empty()
					&& matches!(&call.receiver, Function::Normal(name) if name == "time::now")
		)
	}
	if is_now(expr) {
		return Some(ClockBound {
			offset: None,
		});
	}
	let LogicalExpr::Binary {
		left,
		op,
		right,
	} = expr
	else {
		return None;
	};
	let sign = match op {
		BinaryOperator::Add => ClockSign::Plus,
		BinaryOperator::Subtract => ClockSign::Minus,
		_ => return None,
	};
	if !is_now(left) {
		return None;
	}
	let Some(Value::Duration(by)) = try_literal_to_value(right) else {
		return None;
	};
	Some(ClockBound {
		offset: Some((sign, by)),
	})
}

/// The bound operand `expr` denotes: its literal value, or where `clock` the
/// statement clock it reads ([`try_clock_bound`]). The caller sets `clock` only
/// where the caller of the statement may call `time::now()`, because a scan
/// reads the clock without a function-name check of its own.
pub(crate) fn try_bound_operand(expr: &LogicalExpr, clock: bool) -> Option<BoundOperand> {
	if let Some(value) = try_literal_to_value(expr) {
		return Some(BoundOperand::Literal(value));
	}
	if clock {
		return try_clock_bound(expr).map(BoundOperand::StatementClock);
	}
	None
}

// ============================================================================
// Constant folding
// ============================================================================

/// [`fold_condition_expressions`], with the aggregate exception under the
/// caller's control.
///
/// `keep_aggregates` leaves a call the registry knows as an aggregate whole,
/// its arguments still folded. Set it where the aggregate extractor will read
/// the expression afterwards and match the call by name — a projection or an
/// aggregation's fields — because several aggregates are also scalar
/// functions (`count`, `math::max`, `math::sum`) and a folded one would be a
/// constant before the extractor saw it. Leave it clear elsewhere: in a
/// predicate the extractor never looks, and `math::max([1, 2])` is just a
/// call on literals.
pub(crate) fn fold_expressions(
	expr: &mut LogicalExpr,
	registry: &FunctionRegistry,
	capabilities: &Capabilities,
	restricted_fields: Option<&RestrictedFields>,
	keep_aggregates: bool,
) -> bool {
	fn fold(
		expr: &mut LogicalExpr,
		registry: &FunctionRegistry,
		capabilities: &Capabilities,
		restricted_fields: Option<&RestrictedFields>,
		keep_aggregates: bool,
		changed: &mut bool,
	) {
		// A closure body is evaluated where the closure is invoked, by later
		// statements in other transactions; nothing in it is folded here.
		if !matches!(expr, LogicalExpr::Closure(_)) {
			for child in expr.children_mut() {
				fold(child, registry, capabilities, restricted_fields, keep_aggregates, changed);
			}
		}
		if keep_aggregates && is_aggregate_call(expr, registry) {
			return;
		}
		if let Some(folded) = try_fold_to_literal(expr, registry, capabilities, restricted_fields) {
			*expr = folded;
			*changed = true;
		}
	}
	let mut changed = false;
	fold(expr, registry, capabilities, restricted_fields, keep_aggregates, &mut changed);
	changed
}

/// Whether `expr` is a call to a function the registry also knows as an
/// aggregate, which the aggregate extractor matches by name after binding.
fn is_aggregate_call(expr: &LogicalExpr, registry: &FunctionRegistry) -> bool {
	let LogicalExpr::Call(call) = expr else {
		return false;
	};
	let Function::Normal(name) = &call.receiver else {
		return false;
	};
	registry.is_aggregate(name)
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
/// Every source must qualify: the same condition is handed to each one, so a
/// single dereferencing source makes the whole condition unsafe to discard.
/// Unrecognised sources are excluded, which costs an optimisation rather than
/// an enforcement — a nested plan is one of them: its rows can carry record
/// ids just as a table's do, so a field read off one may still dereference.
///
/// `sources` are the leaves a `FROM` clause lowered to: the inputs of a
/// `UnionAll`, or the single source beneath the pipeline.
pub(crate) fn source_field_reads_are_inert(sources: &[LogicalOp]) -> bool {
	sources.iter().all(|source| match source {
		LogicalOp::Source(LogicalSource {
			source,
			..
		}) => match source {
			SourceRef::Table(_) => true,
			SourceRef::Dynamic(expr) => expr_source_field_reads_are_inert(expr),
			SourceRef::RecordId(_) | SourceRef::Mock(_) | SourceRef::Param(_) => false,
		},
		LogicalOp::Values(LogicalValues {
			expr,
			..
		}) => expr_source_field_reads_are_inert(expr),
		// Not a leaf source. A relational input reads whatever its own
		// sources read; a statement's rows are unknown here.
		_ => false,
	})
}

/// [`source_field_reads_are_inert`] for one source in expression form.
fn expr_source_field_reads_are_inert(e: &LogicalExpr) -> bool {
	match e {
		LogicalExpr::Table(_) => true,
		// Every `Constant` yields a float, datetime or duration, so its row is
		// a scalar like any other.
		LogicalExpr::Constant(_) => true,
		// An object row's bare field read is a key lookup like a record's; the
		// expressions building the object are evaluated by the source either
		// way, fold or no fold.
		LogicalExpr::Object(_) => true,
		LogicalExpr::Literal(lit) => matches!(
			lit,
			LogicalLiteral::None
				| LogicalLiteral::Null
				| LogicalLiteral::Bool(_)
				| LogicalLiteral::Float(_)
				| LogicalLiteral::Integer(_)
				| LogicalLiteral::Decimal(_)
				| LogicalLiteral::String(_)
				| LogicalLiteral::Bytes(_)
				| LogicalLiteral::Regex(_)
				| LogicalLiteral::Duration(_)
				| LogicalLiteral::Datetime(_)
				| LogicalLiteral::Uuid(_)
				| LogicalLiteral::Geometry(_)
				| LogicalLiteral::File(_)
		),
		// An array can hold record ids the read maps over; a record id names
		// stored rows; everything else is resolved at evaluation.
		LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Mock(_)
		| LogicalExpr::Param(_)
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
		| LogicalExpr::Closure(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue
		| LogicalExpr::Subquery(_) => false,
	}
}

/// `true` when evaluating `expr` cannot dispatch anything a capability gates,
/// so a fold may discard it without hiding an error evaluation would raise.
///
/// This is a fail-closed whitelist. Every variant is listed and anything not
/// explicitly cleared counts as capable of invoking something, so a new
/// `LogicalExpr` or `BinaryOperator` variant stops compiling
/// until someone decides which side it falls on rather than silently becoming
/// discardable.
///
/// Enumerating the ways a subtree *can* reach a denial does not work, because
/// there are too many and they do not share a shape: a built-in enforces
/// network, scripting or experimental capabilities of its own beyond its
/// name; a method call is a path step and a closure call an `Invoke`,
/// neither of them a `Call`; and a user-defined function, module, script or
/// `eval::surql` runs a body that is not visible at plan time at all. Clearing
/// only the shapes that provably reach nothing is the side of that line which
/// fails safe.
///
/// Being capability-independent is deliberate. An operand that can invoke
/// something is kept whatever the configuration permits, which also stops a
/// fold from discarding a call with side effects — what evaluation would have
/// run, it still runs.
fn evaluation_is_inert(expr: &LogicalExpr, restricted_fields: Option<&RestrictedFields>) -> bool {
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
		// A leaf literal holds no expression.
		LogicalExpr::Literal(_) => true,
		// Composite literals evaluate their element expressions in place.
		LogicalExpr::Array(items) | LogicalExpr::Set(items) => {
			items.iter().all(|e| evaluation_is_inert(e, restricted_fields))
		}
		LogicalExpr::Object(entries) => {
			entries.iter().all(|(_, e)| evaluation_is_inert(e, restricted_fields))
		}
		// A record id builds a value from its key; whether that dispatches
		// depends on the key.
		LogicalExpr::RecordId(rid) => record_id_key_is_inert(&rid.key, restricted_fields),
		LogicalExpr::Table(_) | LogicalExpr::Constant(_) | LogicalExpr::Mock(_) => true,
		// A parameter that reaches here is left unresolved on purpose: a closure,
		// whose captures make substitution lossy, or a `DEFINE PARAM` whose
		// permission clause runs on every read. Both dispatch on evaluation, so
		// neither may be discarded. A parameter holding a plain value is replaced
		// by its literal before this point and folds on that literal instead.
		LogicalExpr::Param(_) => false,
		// A single field read is inert only when the row it reads from is a
		// record and reading that particular field evaluates nothing. Field
		// access recurses through arrays and fetches any record id it lands
		// on, evaluating that record's computed fields and permission
		// expressions — so over a source like `[[target:1]]` even a bare field
		// read dereferences, and from the first step onward the receiver can
		// be a record id whatever the source. Over a table the read is a plain
		// key lookup, so `a IN []` still collapses, while `ref.bad IN []` and
		// a read of a field materialisation has to compute are kept. Every
		// other path form is a step on a value that may be a record id, or a
		// filter, method or lookup that evaluates expressions of its own.
		LogicalExpr::Field(name) => {
			restricted_fields.is_some_and(|restricted| !restricted.contains(name.as_str()))
		}
		LogicalExpr::Doc
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
		} => false,
		// Setting a key on an object dispatches nothing of its own.
		LogicalExpr::SetField {
			base,
			value,
			..
		} => {
			evaluation_is_inert(base, restricted_fields)
				&& evaluation_is_inert(value, restricted_fields)
		}
		LogicalExpr::Prefix {
			expr,
			..
		} => evaluation_is_inert(expr, restricted_fields),
		LogicalExpr::Binary {
			left,
			op,
			right,
		} => {
			operator_is_inert(op)
				&& evaluation_is_inert(left, restricted_fields)
				&& evaluation_is_inert(right, restricted_fields)
		}
		// Everything else can reach a dispatch: a call of any kind, a closure
		// body, control flow, or a statement with its own planning.
		LogicalExpr::Postfix {
			..
		}
		| LogicalExpr::Invoke {
			..
		}
		| LogicalExpr::Call(_)
		| LogicalExpr::Closure(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue
		| LogicalExpr::Subquery(_) => false,
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
	key: &LogicalRecordIdKey,
	restricted_fields: Option<&RestrictedFields>,
) -> bool {
	fn bound_is_inert(
		bound: &Bound<LogicalRecordIdKey>,
		restricted_fields: Option<&RestrictedFields>,
	) -> bool {
		match bound {
			Bound::Included(k) | Bound::Excluded(k) => record_id_key_is_inert(k, restricted_fields),
			Bound::Unbounded => true,
		}
	}

	match key {
		LogicalRecordIdKey::Number(_)
		| LogicalRecordIdKey::String(_)
		| LogicalRecordIdKey::Uuid(_) => true,
		LogicalRecordIdKey::Array(exprs) => {
			exprs.iter().all(|e| evaluation_is_inert(e, restricted_fields))
		}
		LogicalRecordIdKey::Object(entries) => {
			entries.iter().all(|(_, e)| evaluation_is_inert(e, restricted_fields))
		}
		LogicalRecordIdKey::Range(range) => {
			bound_is_inert(&range.start, restricted_fields)
				&& bound_is_inert(&range.end, restricted_fields)
		}
		LogicalRecordIdKey::Generate(_) => false,
	}
}

/// Attempt to reduce a constant expression to a literal.
///
/// Handles:
/// - Calls to a context-free, deterministic function whose arguments are all already literals
///   (math::floor, string::lowercase, type::int, etc.)
/// - Binary arithmetic on two literals (datetime ± duration, number ± number, etc.)
/// - `AND`/`OR` with a constant boolean operand, and `x INSIDE []`
/// - A field of an object literal, an index into a literal, and the first or last element of a
///   collection literal (`{ a: 1 }.a`, `['x', 'y'][1]`) — the shapes a resolved parameter or a
///   folded record path leaves behind
///
/// A call that reads the clock is not folded wherever it appears, `time::now()`
/// in a condition included: a plan holds no instant, and the call reads the
/// statement's clock when it runs (see `doc/adr/0006-clock-reads-keep-their-evaluation.md`).
/// The index analyzer reads `time::now()` and `time::now() ± <duration literal>`
/// as an index bound that resolves when the scan opens ([`try_bound_operand`]).
///
/// Both conditions on a call matter and they are different questions:
/// `ScalarFunction::is_pure` asks whether it can run without an execution
/// context, `ScalarFunction::is_deterministic` whether repeated calls agree.
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
///
/// `restricted_fields` is the set of field names whose read is not inert over
/// the rows the expression is evaluated against, or `None` when no field read
/// is; see [`source_field_reads_are_inert`]. A fold that would discard a
/// field read declines unless the set clears that field.
pub(crate) fn try_fold_to_literal(
	expr: &LogicalExpr,
	registry: &FunctionRegistry,
	capabilities: &Capabilities,
	restricted_fields: Option<&RestrictedFields>,
) -> Option<LogicalExpr> {
	match expr {
		// Context-free, deterministic call whose arguments are all literals.
		// After bottom-up folding, nested expressions like `math::floor(20 + 0.5)`
		// will have their arguments folded first, so we only need to check
		// whether the immediate arguments are literals.
		LogicalExpr::Call(call) => {
			let Function::Normal(name) = &call.receiver else {
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
			let args: Option<Vec<Value>> = call.args.iter().map(try_literal_to_value).collect();
			let args = args?;
			// Sound to invoke here and reuse for the statement: purity means no
			// context is needed, determinism that one draw stands for all rows.
			// A result with no literal form (a closure) stays a call.
			let result = func.invoke(args).ok()?;
			try_value_to_logical_expr(result)
		}

		// `x INSIDE []` is provably false — no element can match an empty
		// array. Folding to `false` lets the surrounding AND/OR collapse
		// via the short-circuit rules below, ultimately yielding
		// `AccessPath::EmptyScan` for the whole SELECT.
		LogicalExpr::Binary {
			op: BinaryOperator::Inside,
			left,
			right,
		} if is_empty_array_literal(right) && evaluation_is_inert(left, restricted_fields) => {
			Some(LogicalExpr::Literal(LogicalLiteral::Bool(false)))
		}

		// AND / OR short-circuits when one side is a constant boolean.
		// Without this, `field IN [] AND ...` would never simplify because
		// the AND's left operand is not a literal.
		LogicalExpr::Binary {
			left,
			op,
			right,
		} => {
			if let Some(short) = try_short_circuit_bool(left, op, right, restricted_fields) {
				return Some(short);
			}
			let left_val = try_literal_to_value(left)?;
			let right_val = try_literal_to_value(right)?;
			let result = try_eval_binary(op, left_val, right_val)?;
			try_value_to_logical_expr(result)
		}

		// A field of a literal, as the runtime field step reads it: an
		// object's field (what a resolved parameter such as `$session`
		// becomes), a geometry's GeoJSON field, and `NONE` off any other
		// scalar. A record id keeps its runtime step, which fetches the
		// record, and so does an array, which maps the access over elements
		// that may be record ids.
		LogicalExpr::Access {
			base,
			field,
		} => match try_literal_to_value(base)? {
			Value::Object(obj) => {
				try_value_to_logical_expr(obj.get(field.as_str()).cloned().unwrap_or(Value::None))
			}
			Value::Geometry(geo) => try_value_to_logical_expr(
				geo.as_object().get(field.as_str()).cloned().unwrap_or(Value::None),
			),
			Value::RecordId(_) | Value::Array(_) => None,
			_ => Some(LogicalExpr::Literal(LogicalLiteral::None)),
		},

		// A literal indexed by a literal. The runtime index step reads no
		// storage for any base, so its own evaluation is the fold.
		LogicalExpr::Index {
			base,
			index,
		} => {
			let base = try_literal_to_value(base)?;
			let index = try_literal_to_value(index)?;
			let result = crate::exec::vm::access::index::evaluate_index(&base, &index).ok()?;
			try_value_to_logical_expr(result)
		}

		// An end of a collection literal; a value that is not one answers
		// `NONE`, as the runtime step does.
		LogicalExpr::First(base) => match try_literal_to_value(base)? {
			Value::Array(arr) => {
				try_value_to_logical_expr(arr.first().cloned().unwrap_or(Value::None))
			}
			Value::Set(set) => {
				try_value_to_logical_expr(set.first().cloned().unwrap_or(Value::None))
			}
			_ => Some(LogicalExpr::Literal(LogicalLiteral::None)),
		},
		LogicalExpr::Last(base) => match try_literal_to_value(base)? {
			Value::Array(arr) => {
				try_value_to_logical_expr(arr.last().cloned().unwrap_or(Value::None))
			}
			Value::Set(set) => {
				try_value_to_logical_expr(set.last().cloned().unwrap_or(Value::None))
			}
			_ => Some(LogicalExpr::Literal(LogicalLiteral::None)),
		},

		// Nothing else folds. A `Subquery` in particular is a plan of its own,
		// bound and folded when it is planned.
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
		| LogicalExpr::Closure(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue
		| LogicalExpr::Subquery(_) => None,
	}
}

/// Returns `true` if `expr` is an array literal with no elements.
fn is_empty_array_literal(expr: &LogicalExpr) -> bool {
	matches!(expr, LogicalExpr::Array(items) if items.is_empty())
}

/// `true` when this expression can only evaluate to a boolean.
///
/// `AND` and `OR` yield one of their operands rather than a boolean: `7 OR true`
/// is `7`, and `7 AND true` is `true`. A rewrite that replaces the whole
/// expression with a constant, or with the operand alone, therefore only
/// preserves the value when the left operand could not have carried one of its
/// own. Truthiness is not enough — the folded node can sit anywhere in the
/// condition, including under a comparison that reads its value.
///
/// The answer is the expression's kind against an open schema: only a shape
/// that is `Bool` whatever its fields hold qualifies.
fn is_boolean_valued(expr: &LogicalExpr) -> bool {
	expr.kind(&Schema::open()) == Kind::Bool
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
fn try_short_circuit_bool(
	left: &LogicalExpr,
	op: &BinaryOperator,
	right: &LogicalExpr,
	restricted_fields: Option<&RestrictedFields>,
) -> Option<LogicalExpr> {
	let as_bool = |e: &LogicalExpr| match e {
		LogicalExpr::Literal(LogicalLiteral::Bool(b)) => Some(*b),
		_ => None,
	};
	let bool_lit = |b: bool| LogicalExpr::Literal(LogicalLiteral::Bool(b));
	let l_bool = as_bool(left);
	let r_bool = as_bool(right);
	match (op, l_bool, r_bool) {
		// `false AND x` / `true OR x`: `x` is never evaluated at runtime, so
		// discarding it is what evaluation would have done.
		(BinaryOperator::And, Some(false), _) => Some(bool_lit(false)),
		(BinaryOperator::Or, Some(true), _) => Some(bool_lit(true)),
		// `x AND false` / `x OR true`: `x` runs first, so it has to survive
		// when it holds a call the caller may not execute, and its value has
		// to be one the rewrite can stand in for.
		(BinaryOperator::And, _, Some(false))
			if is_boolean_valued(left) && evaluation_is_inert(left, restricted_fields) =>
		{
			Some(bool_lit(false))
		}
		(BinaryOperator::Or, _, Some(true))
			if is_boolean_valued(left) && evaluation_is_inert(left, restricted_fields) =>
		{
			Some(bool_lit(true))
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
fn try_eval_binary(op: &BinaryOperator, left: Value, right: Value) -> Option<Value> {
	use crate::val::{TryAdd, TrySub};

	match op {
		BinaryOperator::Add => left.try_add(right).ok(),
		BinaryOperator::Subtract => left.try_sub(right).ok(),
		// Folding is limited to add/sub to avoid unexpected behaviour with
		// division-by-zero, overflow, etc. These cover the common
		// datetime ± duration patterns.
		_ => None,
	}
}

/// Fold constant, row-independent subexpressions of a `WHERE` predicate to
/// literal values, bottom-up. This enables proper index range access for
/// expressions like `time::now() - 365d` which would otherwise be opaque to
/// index analysis.
///
/// Must be called **after** parameter resolution
/// ([`super::params::resolve_condition_params`]) so that parameter references
/// have already been replaced with literals.
///
/// Only folds expressions that:
/// - Contain no field references (row-independent)
/// - Are arithmetic on literals, or calls to a built-in that is both context-free and deterministic
///   (math::*, string::*, type::*, etc.) with all arguments already literals. `rand::*` is
///   context-free but not deterministic, so it is left for per-row evaluation.
/// - Call only functions `capabilities` permits the caller to execute.
///
/// A clock read such as `time::now()` is left as a call.
///
/// A [`LogicalExpr::Subquery`] is not entered: the plan inside it is bound and
/// folded on its own.
///
/// Returns whether anything folded, so a caller driving this to a fixpoint
/// can tell a pass that changed the expression from one that did not.
#[cfg(test)]
pub(crate) fn fold_condition_expressions(
	expr: &mut LogicalExpr,
	registry: &FunctionRegistry,
	capabilities: &Capabilities,
	restricted_fields: Option<&RestrictedFields>,
) -> bool {
	fold_expressions(expr, registry, capabilities, restricted_fields, false)
}

#[cfg(test)]
mod tests {
	use std::ops::Bound;

	use surrealdb_strand::TableName;
	use surrealdb_types::ToSql;

	use super::*;
	use crate::expr::logical::lower_expr;

	fn expr(sql: &str) -> LogicalExpr {
		let ast = crate::syn::parse(&format!("RETURN {sql}")).expect("expression should parse");
		let surrealdb_expr::parser::TopLevelExpr::Expr(crate::expr::Expr::Return(out)) =
			ast.expressions.into_iter().next().expect("one statement")
		else {
			panic!("expected a RETURN");
		};
		lower_expr(out.what).expect("expression should lower")
	}

	#[test]
	fn values_round_trip_through_their_logical_literal() {
		let values = vec![
			Value::None,
			Value::Null,
			Value::Bool(true),
			Value::Number(Number::Int(7)),
			Value::Number(Number::Float(1.5)),
			Value::from("text"),
			Value::Array(crate::val::Array(vec![Value::Number(Number::Int(1)), Value::from("x")])),
			Value::RecordId(RecordId::new(TableName::from("t"), RecordIdKey::Number(1))),
			Value::Table(TableName::from("t")),
		];
		for value in values {
			assert!(!substitution_is_lossy(&value), "{value:?}");
			let literal = value_to_logical_expr(value.clone());
			assert_eq!(try_literal_to_value(&literal), Some(value.clone()), "{value:?}");
		}
	}

	#[test]
	fn a_set_value_keeps_its_set_form() {
		let mut set = crate::val::Set::new();
		set.insert(Value::Number(Number::Int(1)));
		set.insert(Value::Number(Number::Int(2)));
		let value = Value::Set(set);
		assert!(!substitution_is_lossy(&value));
		let literal = value_to_logical_expr(value.clone());
		assert!(matches!(literal, LogicalExpr::Set(_)), "{literal:?}");
		assert_eq!(try_literal_to_value(&literal), Some(value));
	}

	#[test]
	fn a_range_value_becomes_its_operator_form() {
		let range = Range {
			start: Bound::Included(Value::Number(Number::Int(1))),
			end: Bound::Excluded(Value::Number(Number::Int(5))),
		};
		let literal = value_to_logical_expr(Value::Range(Box::new(range)));
		assert_eq!(literal.to_sql(), "1..5");
		assert_eq!(
			value_to_logical_expr(Value::Range(Box::new(Range::unbounded()))),
			LogicalExpr::Literal(LogicalLiteral::UnboundedRange)
		);
	}

	#[test]
	fn a_compound_record_key_is_not_a_static_value() {
		assert!(try_literal_to_value(&expr("t:[1, 2]")).is_none());
		assert!(try_literal_to_value(&expr("t:1")).is_some());
		assert!(try_literal_to_value(&expr("[1, a]")).is_none());
		assert!(try_literal_to_value(&expr("{ a: 1 }")).is_some());
	}

	#[test]
	fn access_on_a_literal_folds_to_its_value() {
		let registry = FunctionRegistry::with_builtins();
		let capabilities = Capabilities::all();
		let fold = |sql: &str| {
			let mut e = expr(sql);
			fold_condition_expressions(&mut e, &registry, &capabilities, None);
			e.to_sql()
		};
		assert_eq!(fold("{ a: { b: 'x' } }.a.b"), "'x'");
		assert_eq!(fold("['x', 'y'][0]"), "'x'");
		assert_eq!(fold("['x', 'y'][$]"), "'y'");
		assert_eq!(fold("['x', 'y'][5]"), "NONE");
		assert_eq!(fold("{ a: 1 }.b"), "NONE");
		assert_eq!(fold("a = { a: 'x' }.a"), "a = 'x'");
		assert_eq!(fold("'x'.a"), "NONE");
		assert_eq!(fold("(1).a"), "NONE");
		assert_eq!(fold("(1.0, 2.0).type"), "'Point'");
		// A base that may fetch keeps its runtime step.
		assert_eq!(fold("t:1.a"), "(t:1).a");
		assert_eq!(fold("[t:1].a"), "[t:1].a");
		// A row-dependent operand keeps it too.
		assert_eq!(fold("['x', 'y'][a]"), "['x', 'y'][a]");
	}

	#[test]
	fn short_circuits_preserve_operand_values() {
		let registry = FunctionRegistry::with_builtins();
		let capabilities = Capabilities::all();
		let fold = |sql: &str| {
			let mut e = expr(sql);
			fold_condition_expressions(&mut e, &registry, &capabilities, None);
			e.to_sql()
		};
		assert_eq!(fold("false AND a > 1"), "false");
		assert_eq!(fold("true OR a > 1"), "true");
		assert_eq!(fold("true AND a > 1"), "a > 1");
		assert_eq!(fold("a > 1 AND true"), "a > 1");
		// `7 AND true` is `true` at runtime, but `7 OR false` is `7`: the
		// left operand is not boolean-valued, so it is kept whole.
		assert_eq!(fold("a OR false"), "a OR false");
		assert_eq!(fold("1 + 1"), "2");
		assert_eq!(fold("math::floor(20.5)"), "20f");
	}

	#[test]
	fn a_field_read_is_discarded_only_when_cleared() {
		let registry = FunctionRegistry::with_builtins();
		let capabilities = Capabilities::all();
		let mut e = expr("a IN []");
		fold_condition_expressions(&mut e, &registry, &capabilities, None);
		assert_eq!(e.to_sql(), "a INSIDE []");
		let mut e = expr("a IN []");
		fold_condition_expressions(&mut e, &registry, &capabilities, Some(&HashSet::new()));
		assert_eq!(e.to_sql(), "false");
		let restricted: HashSet<String> = ["a".to_string()].into_iter().collect();
		let mut e = expr("a IN []");
		fold_condition_expressions(&mut e, &registry, &capabilities, Some(&restricted));
		assert_eq!(e.to_sql(), "a INSIDE []");
	}

	#[test]
	fn a_subquery_is_never_entered() {
		let registry = FunctionRegistry::with_builtins();
		let capabilities = Capabilities::all();
		let mut e = expr("(SELECT * FROM t WHERE 1 + 1 = 2)");
		let before = e.clone();
		fold_condition_expressions(&mut e, &registry, &capabilities, None);
		assert_eq!(e, before);
	}
}
