//! Plan-time-compiled comparisons for key-resident graph predicates.
//!
//! A key-resident predicate (every referenced field is `id`, `in` or
//! `out`) whose shape is a conjunction of comparisons against record-id
//! literals compiles into a [`KeyPredicateMatcher`]: a synchronous, typed
//! test the scan runs on each adjacency entry as it streams off the
//! cursor — no candidate object, no evaluation context, no async
//! machinery. Every comparison delegates to [`RecordId`]'s own ordering,
//! which is exactly what `Value` comparison uses for two record ids, so
//! the matcher answers precisely as the evaluator would; any predicate
//! shape it cannot express stays on the evaluator path.

// Dead-code lints are allowed for the whole module.
#![allow(dead_code)]

use crate::exec::parts::LookupDirection;
use crate::expr::BinaryOperator;
use crate::expr::logical::{LogicalExpr, LogicalRecordIdKey};
use crate::val::{RecordId, RecordIdKey};

/// Which synthesized field one comparison reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyField {
	Id,
	In,
	Out,
}

/// One `field <op> record-id-literal` comparison.
#[derive(Debug, Clone, PartialEq, Eq)]
struct KeyTest {
	field: KeyField,
	op: BinaryOperator,
	operand: RecordId,
}

/// A conjunction of [`KeyTest`]s covering an entire predicate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KeyPredicateMatcher {
	tests: Vec<KeyTest>,
}

impl KeyPredicateMatcher {
	/// Compiles `cond` when every conjunct is a comparison between a bare
	/// `id`/`in`/`out` field read and a static record-id literal, in either
	/// operand order. `direction` must be a single direction: under `Both`
	/// the `in`/`out` mapping differs per emitted edge in a way the
	/// evaluator resolves by falling back, so nothing is compiled.
	pub(crate) fn compile(cond: &LogicalExpr, direction: LookupDirection) -> Option<Self> {
		if matches!(direction, LookupDirection::Both) {
			return None;
		}
		let mut tests = Vec::new();
		let mut stack = vec![cond];
		while let Some(expr) = stack.pop() {
			let LogicalExpr::Binary {
				left,
				op,
				right,
			} = expr
			else {
				return None;
			};
			if matches!(op, BinaryOperator::And) {
				stack.push(left);
				stack.push(right);
			} else {
				tests.push(compile_test(left, op, right)?);
			}
		}
		Some(Self {
			tests,
		})
	}
}

/// Compiles one comparison, accepting the literal on either side (the
/// operator flips accordingly).
fn compile_test(left: &LogicalExpr, op: &BinaryOperator, right: &LogicalExpr) -> Option<KeyTest> {
	let (field, operand, op) = match (key_field(left), key_field(right)) {
		(Some(field), None) => (field, record_id_literal(right)?, op.clone()),
		(None, Some(field)) => (field, record_id_literal(left)?, flip(op)?),
		_ => return None,
	};
	match op {
		BinaryOperator::Equal
		| BinaryOperator::ExactEqual
		| BinaryOperator::NotEqual
		| BinaryOperator::LessThan
		| BinaryOperator::LessThanEqual
		| BinaryOperator::MoreThan
		| BinaryOperator::MoreThanEqual => Some(KeyTest {
			field,
			op,
			operand,
		}),
		BinaryOperator::Subtract
		| BinaryOperator::Add
		| BinaryOperator::Multiply
		| BinaryOperator::Divide
		| BinaryOperator::Remainder
		| BinaryOperator::Power
		| BinaryOperator::AllEqual
		| BinaryOperator::AnyEqual
		| BinaryOperator::Or
		| BinaryOperator::And
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
		| BinaryOperator::Outside
		| BinaryOperator::Intersects
		| BinaryOperator::Range
		| BinaryOperator::RangeInclusive
		| BinaryOperator::RangeSkip
		| BinaryOperator::RangeSkipInclusive
		| BinaryOperator::Matches(_)
		| BinaryOperator::NearestNeighbor(_) => None,
	}
}

/// The comparison with its operands swapped.
fn flip(op: &BinaryOperator) -> Option<BinaryOperator> {
	Some(match op {
		BinaryOperator::Equal => BinaryOperator::Equal,
		BinaryOperator::ExactEqual => BinaryOperator::ExactEqual,
		BinaryOperator::NotEqual => BinaryOperator::NotEqual,
		BinaryOperator::LessThan => BinaryOperator::MoreThan,
		BinaryOperator::LessThanEqual => BinaryOperator::MoreThanEqual,
		BinaryOperator::MoreThan => BinaryOperator::LessThan,
		BinaryOperator::MoreThanEqual => BinaryOperator::LessThanEqual,
		BinaryOperator::Subtract
		| BinaryOperator::Add
		| BinaryOperator::Multiply
		| BinaryOperator::Divide
		| BinaryOperator::Remainder
		| BinaryOperator::Power
		| BinaryOperator::AllEqual
		| BinaryOperator::AnyEqual
		| BinaryOperator::Or
		| BinaryOperator::And
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
		| BinaryOperator::Outside
		| BinaryOperator::Intersects
		| BinaryOperator::Range
		| BinaryOperator::RangeInclusive
		| BinaryOperator::RangeSkip
		| BinaryOperator::RangeSkipInclusive
		| BinaryOperator::Matches(_)
		| BinaryOperator::NearestNeighbor(_) => return None,
	})
}

/// Recognizes a bare `id` / `in` / `out` field read.
fn key_field(expr: &LogicalExpr) -> Option<KeyField> {
	let LogicalExpr::Field(name) = expr else {
		return None;
	};
	match name.as_str() {
		"id" => Some(KeyField::Id),
		"in" => Some(KeyField::In),
		"out" => Some(KeyField::Out),
		_ => None,
	}
}

/// Recognizes a static record-id literal with a scalar key.
fn record_id_literal(expr: &LogicalExpr) -> Option<RecordId> {
	let LogicalExpr::RecordId(lit) = expr else {
		return None;
	};
	let key = match &lit.key {
		LogicalRecordIdKey::Number(n) => RecordIdKey::Number(*n),
		LogicalRecordIdKey::String(s) => RecordIdKey::String(s.clone()),
		LogicalRecordIdKey::Uuid(u) => RecordIdKey::Uuid(*u),
		// Generated keys are not static; composite keys can nest
		// arbitrary expressions — leave both to the evaluator.
		LogicalRecordIdKey::Array(_)
		| LogicalRecordIdKey::Object(_)
		| LogicalRecordIdKey::Generate(_)
		| LogicalRecordIdKey::Range(_) => return None,
	};
	Some(RecordId::new(lit.table.clone(), key))
}
