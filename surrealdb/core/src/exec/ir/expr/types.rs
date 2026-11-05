//! The kind the language guarantees for the result of an expression op.
//!
//! The answer is language-level knowledge only: what the grammar and the op's attributes fix,
//! whatever the operands hold. Knowledge that comes from storage (a definition, a hint, a table's
//! migrated rows) is left to the analysis layer. An op that is not listed answers
//! [`TypeKnowledge::Unknown`], which every consumer treats as "no knowledge": arithmetic is
//! unknown even over operands of a known kind, and a compound literal is unknown because no
//! function maps a compound [`Value`] to its [`Kind`].

#![deny(clippy::wildcard_enum_match_arm)]

use super::ops::{ConvertAttrs, ExprOp, OnFail};
use crate::exec::ir::iface::types::{FactSource, TypeKnowledge};
use crate::expr::{BinaryOperator, Kind, PrefixOperator};
use crate::val::{Number, Value};

/// The kind the language guarantees for the result of `op`, given its operands' knowledge.
///
/// Every answer here depends on the op's attributes alone, so `operands` is not read; it is part
/// of the signature so that an answer that does depend on an operand needs no change to callers.
/// A `Fact` is consumed without a check downstream, so an entry is added only when the language
/// guarantees it for every input the op accepts.
pub(crate) fn result_type(op: &ExprOp, _operands: &[&TypeKnowledge]) -> TypeKnowledge {
	match op {
		ExprOp::Literal(value) => match literal_kind(value) {
			Some(kind) => TypeKnowledge::Fact {
				kind,
				source: FactSource::Literal,
			},
			None => TypeKnowledge::Unknown,
		},
		ExprOp::Convert(attrs) => {
			let ConvertAttrs {
				kind,
				on_fail,
				..
			} = &**attrs;
			let kind = match on_fail {
				OnFail::Error => kind.clone(),
				OnFail::None if kind.can_be_none() => kind.clone(),
				OnFail::None => Kind::option(kind.clone()),
			};
			derived(kind)
		}
		ExprOp::Binary {
			op,
		} => {
			if yields_bool(op) {
				derived(Kind::Bool)
			} else {
				TypeKnowledge::Unknown
			}
		}
		ExprOp::Prefix(op) => match &**op {
			PrefixOperator::Not => derived(Kind::Bool),
			PrefixOperator::Positive
			| PrefixOperator::Negate
			| PrefixOperator::Range
			| PrefixOperator::RangeInclusive
			| PrefixOperator::Cast(..) => TypeKnowledge::Unknown,
		},
		ExprOp::Truthy => derived(Kind::Bool),
		ExprOp::Array
		| ExprOp::Set
		| ExprOp::Object {
			..
		}
		| ExprOp::RecordId(..)
		| ExprOp::FreeParam {
			..
		}
		| ExprOp::SessionParam {
			..
		}
		| ExprOp::CatalogParam {
			..
		}
		| ExprOp::Table {
			..
		}
		| ExprOp::Mock(..)
		| ExprOp::Constant {
			..
		}
		| ExprOp::Field {
			..
		}
		| ExprOp::ComputedRead(..)
		| ExprOp::Doc
		| ExprOp::Access {
			..
		}
		| ExprOp::Deref {
			..
		}
		| ExprOp::Index
		| ExprOp::First
		| ExprOp::Last
		| ExprOp::Each
		| ExprOp::Flatten {
			..
		}
		| ExprOp::Dispatch(..)
		| ExprOp::Method {
			..
		}
		| ExprOp::Destructure {
			..
		}
		| ExprOp::Optional
		| ExprOp::RepeatRecurse
		| ExprOp::SetField {
			..
		}
		| ExprOp::TupleRow {
			..
		}
		| ExprOp::Postfix {
			..
		}
		| ExprOp::Invoke
		| ExprOp::ShortCircuit {
			..
		}
		| ExprOp::Call(..)
		| ExprOp::ClosureCreate(..)
		| ExprOp::If
		| ExprOp::Throw
		| ExprOp::Subquery(_)
		| ExprOp::Yield
		| ExprOp::Agg {
			..
		}
		| ExprOp::Now {
			..
		}
		| ExprOp::Raise(..)
		| ExprOp::DepthCharge {
			..
		} => TypeKnowledge::Unknown,
	}
}

fn derived(kind: Kind) -> TypeKnowledge {
	TypeKnowledge::Fact {
		kind,
		source: FactSource::Derived,
	}
}

/// The kind of a literal scalar, or `None` for a value that is not a plain scalar.
fn literal_kind(value: &Value) -> Option<Kind> {
	match value {
		Value::None => Some(Kind::None),
		Value::Null => Some(Kind::Null),
		Value::Bool(_) => Some(Kind::Bool),
		Value::Number(Number::Int(_)) => Some(Kind::Int),
		Value::Number(Number::Float(_)) => Some(Kind::Float),
		Value::Number(Number::Decimal(_)) => Some(Kind::Decimal),
		Value::Number(Number::BigDecimal(_)) => Some(Kind::BigDecimal),
		Value::Number(Number::I8(_)) => Some(Kind::I8),
		Value::Number(Number::U8(_)) => Some(Kind::U8),
		Value::Number(Number::I16(_)) => Some(Kind::I16),
		Value::Number(Number::U16(_)) => Some(Kind::U16),
		Value::Number(Number::I32(_)) => Some(Kind::I32),
		Value::Number(Number::U32(_)) => Some(Kind::U32),
		Value::Number(Number::U64(_)) => Some(Kind::U64),
		Value::Number(Number::F16(_)) => Some(Kind::F16),
		Value::Number(Number::F32(_)) => Some(Kind::F32),
		Value::String(_) => Some(Kind::String),
		Value::Bytes(_) => Some(Kind::Bytes),
		Value::Duration(_) => Some(Kind::Duration),
		Value::Datetime(_) => Some(Kind::Datetime),
		Value::Date(_) => Some(Kind::Date),
		Value::Time(_) => Some(Kind::Time),
		Value::Timestamp(_) => Some(Kind::Timestamp),
		Value::Interval(_) => Some(Kind::Interval),
		Value::Uuid(_) => Some(Kind::Uuid),
		Value::Regex(_) => Some(Kind::Regex),
		Value::Array(_)
		| Value::Set(_)
		| Value::Object(_)
		| Value::Geometry(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Range(_)
		| Value::Closure(_) => None,
	}
}

/// Whether a binary operator always yields a boolean: the comparison and containment operators.
/// The four lazy operators (`&&`, `||`, `??`, `?:`) yield one of their operands, so they are not
/// listed, and neither are the full-text and nearest-neighbour operators, whose result is
/// storage-dependent.
fn yields_bool(op: &BinaryOperator) -> bool {
	match op {
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
		| BinaryOperator::Intersects => true,
		BinaryOperator::Or
		| BinaryOperator::And
		| BinaryOperator::NullCoalescing
		| BinaryOperator::TenaryCondition
		| BinaryOperator::Add
		| BinaryOperator::Subtract
		| BinaryOperator::Multiply
		| BinaryOperator::Divide
		| BinaryOperator::Remainder
		| BinaryOperator::Power
		| BinaryOperator::Range
		| BinaryOperator::RangeInclusive
		| BinaryOperator::RangeSkip
		| BinaryOperator::RangeSkipInclusive
		| BinaryOperator::Matches(..)
		| BinaryOperator::NearestNeighbor(..) => false,
	}
}

#[cfg(test)]
mod tests {
	use rust_decimal::Decimal;
	use surrealdb_strand::Strand;

	use super::*;
	use crate::exec::ir::expr::ops::{ConvertMode, ConvertSite};
	use crate::val::{Datetime, Duration, Object, Regex, Uuid};

	fn literal(value: Value) -> ExprOp {
		ExprOp::Literal(Box::new(value))
	}

	fn convert(kind: Kind, on_fail: OnFail) -> ExprOp {
		ExprOp::Convert(Box::new(ConvertAttrs {
			mode: ConvertMode::Cast,
			kind,
			site: ConvertSite(0),
			on_fail,
		}))
	}

	fn binary(op: BinaryOperator) -> ExprOp {
		ExprOp::Binary {
			op,
		}
	}

	fn fact(kind: Kind, source: FactSource) -> TypeKnowledge {
		TypeKnowledge::Fact {
			kind,
			source,
		}
	}

	fn answer(op: &ExprOp) -> TypeKnowledge {
		result_type(op, &[])
	}

	#[test]
	fn result_type_rows() {
		// Scalar literals carry their own kind, sourced from the literal.
		let scalars = [
			(Value::None, Kind::None),
			(Value::Null, Kind::Null),
			(Value::Bool(true), Kind::Bool),
			(Value::from(1_i64), Kind::Int),
			(Value::Number(Number::Float(1.5)), Kind::Float),
			(Value::Number(Number::Decimal(Decimal::ONE)), Kind::Decimal),
			(
				Value::Number(Number::BigDecimal(Box::new(surrealdb_types::BigDecimal::from(
					1_i64,
				)))),
				Kind::BigDecimal,
			),
			(Value::String(Strand::new("a")), Kind::String),
			(Value::Bytes(vec![1_u8].into()), Kind::Bytes),
			(Value::Duration(Duration::new(1, 0)), Kind::Duration),
			(
				Value::Datetime(Datetime::from(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH)),
				Kind::Datetime,
			),
			(Value::Uuid(Uuid::nil()), Kind::Uuid),
			(Value::Regex("a+".parse::<Regex>().expect("a valid pattern")), Kind::Regex),
		];
		for (value, kind) in scalars {
			assert_eq!(
				answer(&literal(value.clone())),
				fact(kind, FactSource::Literal),
				"{value:?}"
			);
		}

		// A compound literal has no kind function.
		assert_eq!(answer(&literal(Value::Object(Object::default()))), TypeKnowledge::Unknown);

		// A converting op that errors on failure yields exactly the target kind.
		assert_eq!(
			answer(&convert(Kind::Int, OnFail::Error)),
			fact(Kind::Int, FactSource::Derived)
		);

		// A converting op that yields NONE on failure yields the option of the target kind, which
		// is what the parser produces for `option<int>`.
		let option_int = crate::syn::kind("option<int>").expect("a valid kind");
		assert_eq!(option_int, Kind::Either(vec![Kind::None, Kind::Int]));
		assert_eq!(
			answer(&convert(Kind::Int, OnFail::None)),
			fact(option_int, FactSource::Derived)
		);

		// A target that already admits NONE is kept as it is.
		for kind in [Kind::Any, Kind::None, Kind::option(Kind::String)] {
			assert_eq!(
				answer(&convert(kind.clone(), OnFail::None)),
				fact(kind, FactSource::Derived)
			);
		}

		// Comparison and containment yield a boolean, whatever the operands.
		let comparisons = [
			BinaryOperator::Equal,
			BinaryOperator::ExactEqual,
			BinaryOperator::NotEqual,
			BinaryOperator::AllEqual,
			BinaryOperator::AnyEqual,
			BinaryOperator::LessThan,
			BinaryOperator::LessThanEqual,
			BinaryOperator::MoreThan,
			BinaryOperator::MoreThanEqual,
			BinaryOperator::Contain,
			BinaryOperator::NotContain,
			BinaryOperator::ContainAll,
			BinaryOperator::ContainAny,
			BinaryOperator::ContainNone,
			BinaryOperator::Inside,
			BinaryOperator::NotInside,
			BinaryOperator::AllInside,
			BinaryOperator::AnyInside,
			BinaryOperator::NoneInside,
			BinaryOperator::Outside,
			BinaryOperator::Intersects,
		];
		for op in comparisons {
			assert_eq!(
				answer(&binary(op.clone())),
				fact(Kind::Bool, FactSource::Derived),
				"{op:?}"
			);
		}

		// The lazy operators yield one of their operands.
		for op in [
			BinaryOperator::And,
			BinaryOperator::Or,
			BinaryOperator::NullCoalescing,
			BinaryOperator::TenaryCondition,
		] {
			assert_eq!(answer(&binary(op.clone())), TypeKnowledge::Unknown, "{op:?}");
		}

		// Truthiness and `!` yield a boolean; the other prefix operators do not.
		assert_eq!(answer(&ExprOp::Truthy), fact(Kind::Bool, FactSource::Derived));
		assert_eq!(
			answer(&ExprOp::Prefix(Box::new(PrefixOperator::Not))),
			fact(Kind::Bool, FactSource::Derived)
		);
		assert_eq!(
			answer(&ExprOp::Prefix(Box::new(PrefixOperator::Negate))),
			TypeKnowledge::Unknown
		);

		// Arithmetic is unknown even over two operands known to be integers.
		let int = fact(Kind::Int, FactSource::Literal);
		for op in [BinaryOperator::Add, BinaryOperator::Multiply, BinaryOperator::Power] {
			assert_eq!(
				result_type(&binary(op.clone()), &[&int, &int]),
				TypeKnowledge::Unknown,
				"{op:?}"
			);
		}

		// An op that is not listed is unknown.
		assert_eq!(answer(&ExprOp::Doc), TypeKnowledge::Unknown);
	}
}
