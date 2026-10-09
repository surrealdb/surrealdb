//! The conjuncts of a filter's predicate: their cost classes, which of them
//! may move, and the rewrites that reorder or drop them.
//!
//! A filter keeps a row when its predicate is truthy, and `AND` evaluates
//! its operands left to right, stopping at the first falsy one. Moving a
//! conjunct past another changes which of the two is evaluated for a row the
//! first rejects, so a conjunct moves only past conjuncts that are *total*:
//! they raise no error, have no effect, and read nothing beyond the row,
//! the parameters and the literals they name. Between two total conjuncts
//! the order decides only how much work a rejected row costs, never what the
//! statement raises, writes or returns. A conjunct that is not total stays
//! where it is written, and no conjunct crosses it.

use crate::expr::BinaryOperator;
use crate::expr::logical::{LogicalExpr, LogicalLiteral, LogicalRecordIdKey};
use crate::expr::operator::PrefixOperator;

/// What a conjunct costs to evaluate per row, cheapest first. A conjunct's
/// class is the dearest class of anything it evaluates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum CostClass {
	/// Comparisons of fields of the row, parameters and literals.
	Field,
	/// A call of a builtin function.
	Builtin,
	/// A call of a compiled catalog unit: a user-defined function, a model,
	/// a module, or a closure.
	Unit,
	/// A statement evaluated per row.
	Subquery,
}

/// The cost class of `expr`.
pub(super) fn cost_class(expr: &LogicalExpr) -> CostClass {
	let own = match expr {
		LogicalExpr::Subquery(_) => return CostClass::Subquery,
		LogicalExpr::Call(call) => match &call.receiver {
			crate::expr::Function::Normal(_) => CostClass::Builtin,
			_ => CostClass::Unit,
		},
		LogicalExpr::Closure(_)
		| LogicalExpr::Invoke {
			..
		} => CostClass::Unit,
		LogicalExpr::Method {
			..
		} => CostClass::Builtin,
		_ => CostClass::Field,
	};
	let mut class = own;
	expr.for_each_child(&mut |child| {
		class = class.max(cost_class(child));
	});
	class
}

/// Whether evaluating `expr` against any row raises no error, has no effect,
/// and reads nothing but the row's own fields, parameters and literals: a
/// comparison or containment test over such operands, a boolean combination
/// of such, or a builtin type test of one.
pub(super) fn total(expr: &LogicalExpr) -> bool {
	match expr {
		LogicalExpr::Literal(_)
		| LogicalExpr::Param(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Field(_)
		| LogicalExpr::Doc => true,
		LogicalExpr::Array(items) | LogicalExpr::Set(items) => items.iter().all(total),
		LogicalExpr::RecordId(_) => literal_record(expr),
		LogicalExpr::Binary {
			left,
			op,
			right,
		} => total_operator(op) && total(left) && total(right),
		LogicalExpr::Prefix {
			op: PrefixOperator::Not,
			expr,
		} => total(expr),
		LogicalExpr::Call(call) => match (&call.receiver, call.args.as_slice()) {
			(crate::expr::Function::Normal(name), [arg]) => is_type_test(name) && total(arg),
			_ => false,
		},
		_ => false,
	}
}

/// Whether binary operator `op` answers for any pair of values without
/// raising: equality, ordering, containment and the boolean connectives.
pub(super) fn total_operator(op: &BinaryOperator) -> bool {
	matches!(
		op,
		BinaryOperator::Equal
			| BinaryOperator::ExactEqual
			| BinaryOperator::NotEqual
			| BinaryOperator::AllEqual
			| BinaryOperator::AnyEqual
			| BinaryOperator::LessThan
			| BinaryOperator::LessThanEqual
			| BinaryOperator::MoreThan
			| BinaryOperator::MoreThanEqual
			| BinaryOperator::And
			| BinaryOperator::Or
			| BinaryOperator::NullCoalescing
			| BinaryOperator::TenaryCondition
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
	)
}

/// Whether builtin `name` called with one argument is a type test, which
/// takes a value of any kind and answers a boolean.
fn is_type_test(name: &str) -> bool {
	name.strip_prefix("type::is_").is_some_and(|kind| !kind.is_empty())
}

/// The conjuncts of `predicate`'s top-level `AND`, in evaluation order.
pub(super) fn conjuncts(predicate: &LogicalExpr) -> Vec<&LogicalExpr> {
	let mut out = Vec::new();
	fn walk<'a>(expr: &'a LogicalExpr, out: &mut Vec<&'a LogicalExpr>) {
		match expr {
			LogicalExpr::Binary {
				left,
				op: BinaryOperator::And,
				right,
			} => {
				walk(left, out);
				walk(right, out);
			}
			other => out.push(other),
		}
	}
	walk(predicate, &mut out);
	out
}

/// `conjuncts` joined by `AND`, left-associated, or `None` for none.
pub(super) fn conjoin(conjuncts: Vec<LogicalExpr>) -> Option<LogicalExpr> {
	conjuncts.into_iter().reduce(|left, right| LogicalExpr::Binary {
		left: Box::new(left),
		op: BinaryOperator::And,
		right: Box::new(right),
	})
}

/// `predicate` with each run of adjacent total conjuncts ordered by cost
/// class, cheapest first, conjuncts of one class keeping their written
/// order. `None` when that order is the written one.
pub(super) fn ordered(predicate: &LogicalExpr) -> Option<LogicalExpr> {
	let parts = conjuncts(predicate);
	if parts.len() < 2 {
		return None;
	}
	let mut order: Vec<usize> = (0..parts.len()).collect();
	let mut start = 0;
	while start < parts.len() {
		if !total(parts[start]) {
			start += 1;
			continue;
		}
		let mut end = start;
		while end < parts.len() && total(parts[end]) {
			end += 1;
		}
		order[start..end].sort_by_key(|&at| cost_class(parts[at]));
		start = end;
	}
	if order.iter().enumerate().all(|(at, &was)| at == was) {
		return None;
	}
	conjoin(order.into_iter().map(|at| parts[at].clone()).collect())
}

/// `predicate` without the conjuncts `answered` holds true of every row it
/// is evaluated for, which are dropped only where they are total: such a
/// conjunct neither rejects a row nor raises, so its absence changes nothing
/// the filter does. `Some(None)` when nothing is left to judge; `None` when
/// nothing is dropped.
pub(super) fn without_answered(
	predicate: &LogicalExpr,
	answered: &dyn Fn(&LogicalExpr) -> bool,
) -> Option<Option<LogicalExpr>> {
	let parts = conjuncts(predicate);
	let kept: Vec<LogicalExpr> = parts
		.iter()
		.filter(|part| !(total(part) && answered(part)))
		.map(|&part| part.clone())
		.collect();
	if kept.len() == parts.len() {
		return None;
	}
	Some(conjoin(kept))
}

/// Whether `conjunct` is `id = record` or `record = id`, which a point
/// lookup of `record` holds true of the one row it yields. Only a record
/// whose key is a literal is matched: a key that holds an expression may
/// evaluate otherwise in the filter than it did for the lookup.
pub(super) fn pins_record(conjunct: &LogicalExpr, record: &LogicalExpr) -> bool {
	let LogicalExpr::Binary {
		left,
		op: BinaryOperator::Equal | BinaryOperator::ExactEqual,
		right,
	} = conjunct
	else {
		return false;
	};
	let is_id = |expr: &LogicalExpr| match expr {
		LogicalExpr::Field(name) => name.as_str() == "id",
		LogicalExpr::Doc => true,
		_ => false,
	};
	let other = match (is_id(left), is_id(right)) {
		(true, false) => right.as_ref(),
		(false, true) => left.as_ref(),
		_ => return false,
	};
	other == record && literal_record(record)
}

/// Whether `expr` is a record id literal of one record whose key holds no
/// expression.
pub(super) fn literal_record(expr: &LogicalExpr) -> bool {
	let LogicalExpr::RecordId(record) = expr else {
		return false;
	};
	literal_key(&record.key)
}

fn literal_key(key: &LogicalRecordIdKey) -> bool {
	match key {
		LogicalRecordIdKey::Number(_)
		| LogicalRecordIdKey::String(_)
		| LogicalRecordIdKey::Uuid(_) => true,
		LogicalRecordIdKey::Array(items) => items.iter().all(literal_value),
		LogicalRecordIdKey::Object(fields) => fields.iter().all(|(_, value)| literal_value(value)),
		LogicalRecordIdKey::Generate(_) | LogicalRecordIdKey::Range(_) => false,
	}
}

fn literal_value(expr: &LogicalExpr) -> bool {
	match expr {
		LogicalExpr::Literal(literal) => !matches!(literal, LogicalLiteral::UnboundedRange),
		LogicalExpr::Array(items) => items.iter().all(literal_value),
		LogicalExpr::Object(fields) => fields.iter().all(|(_, value)| literal_value(value)),
		_ => false,
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn field(name: &str) -> LogicalExpr {
		LogicalExpr::Field(name.into())
	}

	fn int(n: i64) -> LogicalExpr {
		LogicalExpr::Literal(LogicalLiteral::Integer(n))
	}

	fn bin(left: LogicalExpr, op: BinaryOperator, right: LogicalExpr) -> LogicalExpr {
		LogicalExpr::Binary {
			left: Box::new(left),
			op,
			right: Box::new(right),
		}
	}

	fn call(name: &str, args: Vec<LogicalExpr>) -> LogicalExpr {
		LogicalExpr::Call(Box::new(crate::expr::logical::LogicalCall {
			receiver: crate::expr::Function::Normal(name.into()),
			args,
		}))
	}

	fn custom(name: &str, args: Vec<LogicalExpr>) -> LogicalExpr {
		LogicalExpr::Call(Box::new(crate::expr::logical::LogicalCall {
			receiver: crate::expr::Function::Custom(name.into()),
			args,
		}))
	}

	fn and(parts: Vec<LogicalExpr>) -> LogicalExpr {
		conjoin(parts).expect("at least one conjunct")
	}

	#[test]
	fn a_type_test_moves_behind_a_field_comparison() {
		let test = call("type::is_string", vec![field("name")]);
		let cmp = bin(field("age"), BinaryOperator::MoreThan, int(30));
		let reordered = ordered(&and(vec![test.clone(), cmp.clone()])).expect("reordered");
		assert_eq!(reordered, and(vec![cmp, test]));
	}

	#[test]
	fn a_fallible_call_holds_its_place_and_nothing_crosses_it() {
		let len = bin(call("string::len", vec![field("name")]), BinaryOperator::MoreThan, int(3));
		let test = call("type::is_string", vec![field("name")]);
		let cmp = bin(field("age"), BinaryOperator::MoreThan, int(30));
		assert_eq!(ordered(&and(vec![len.clone(), cmp.clone()])), None);
		// The type test and the comparison sit on either side of the call.
		assert_eq!(ordered(&and(vec![test, len, cmp])), None);
	}

	#[test]
	fn a_unit_call_or_subquery_is_never_total() {
		assert!(!total(&custom("fn::check", vec![field("a")])));
		assert_eq!(cost_class(&custom("fn::check", vec![field("a")])), CostClass::Unit);
		assert_eq!(
			cost_class(&bin(call("string::len", vec![field("a")]), BinaryOperator::Equal, int(1))),
			CostClass::Builtin
		);
		assert!(!total(&bin(field("a"), BinaryOperator::Add, int(1))));
	}

	#[test]
	fn a_written_order_already_cheapest_first_is_kept() {
		let cmp = bin(field("age"), BinaryOperator::MoreThan, int(30));
		let test = call("type::is_string", vec![field("name")]);
		assert_eq!(ordered(&and(vec![cmp, test])), None);
	}

	#[test]
	fn the_pinned_record_is_dropped_and_the_rest_kept() {
		let record = LogicalExpr::RecordId(Box::new(crate::expr::logical::LogicalRecordIdLit {
			table: "t".into(),
			key: LogicalRecordIdKey::Number(1),
		}));
		let pin = bin(field("id"), BinaryOperator::Equal, record.clone());
		let cmp = bin(field("age"), BinaryOperator::MoreThan, int(30));
		let left =
			without_answered(&and(vec![pin.clone(), cmp.clone()]), &|c| pins_record(c, &record));
		assert_eq!(left, Some(Some(cmp)));
		assert_eq!(without_answered(&pin, &|c| pins_record(c, &record)), Some(None));
	}
}
