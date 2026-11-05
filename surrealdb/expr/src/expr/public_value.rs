//! Canonical expression construction for programmatic statements and schema output.
use std::ops::Bound;

use surrealdb_types::Value as PublicValue;

use super::literal::ObjectEntry;
use super::{
	BinaryOperator, Expr, Literal, PostfixOperator, PrefixOperator, RecordIdKeyLit, RecordIdLit,
};
use crate::val::convert_public_value_to_internal;
use crate::val::table_name_public::IntoTableName;

impl Expr {
	/// Builds the canonical surface literal, preserving nested sets, record-id
	/// keys and the public range spelling used by RPC and schema exports.
	pub fn from_surface_value(value: PublicValue) -> Self {
		match value {
			PublicValue::Array(values) => Self::Literal(Literal::Array(
				values.into_iter().map(Self::from_surface_value).collect(),
			)),
			PublicValue::Set(values) => Self::Literal(Literal::Set(
				values.into_iter().map(Self::from_surface_value).collect(),
			)),
			PublicValue::Object(values) => Self::Literal(Literal::Object(
				values
					.into_iter()
					.map(|(key, value)| ObjectEntry {
						key: key.into(),
						value: Self::from_surface_value(value),
					})
					.collect(),
			)),
			PublicValue::RecordId(id) => Self::Literal(Literal::RecordId(RecordIdLit {
				table: id.table.into_table_name(),
				key: Self::surface_record_id_key(id.key),
			})),
			PublicValue::Range(range) => match range.into_inner() {
				(Bound::Unbounded, Bound::Unbounded) => Self::Literal(Literal::UnboundedRange),
				(Bound::Unbounded, Bound::Excluded(end)) => Self::Prefix {
					op: PrefixOperator::Range,
					expr: Box::new(Self::from_surface_value(end)),
				},
				(Bound::Unbounded, Bound::Included(end)) => Self::Prefix {
					op: PrefixOperator::RangeInclusive,
					expr: Box::new(Self::from_surface_value(end)),
				},
				(Bound::Included(start), Bound::Excluded(end)) => Self::Binary {
					left: Box::new(Self::from_surface_value(start)),
					op: BinaryOperator::Range,
					right: Box::new(Self::from_surface_value(end)),
				},
				(Bound::Included(start), Bound::Included(end)) => Self::Binary {
					left: Box::new(Self::from_surface_value(start)),
					op: BinaryOperator::RangeInclusive,
					right: Box::new(Self::from_surface_value(end)),
				},
				(Bound::Excluded(start), Bound::Excluded(end)) => Self::Binary {
					left: Box::new(Self::from_surface_value(start)),
					op: BinaryOperator::RangeSkip,
					right: Box::new(Self::from_surface_value(end)),
				},
				(Bound::Excluded(start), Bound::Included(end)) => Self::Binary {
					left: Box::new(Self::from_surface_value(start)),
					op: BinaryOperator::RangeSkipInclusive,
					right: Box::new(Self::from_surface_value(end)),
				},
				(Bound::Included(start), Bound::Unbounded) => Self::Postfix {
					expr: Box::new(Self::from_surface_value(start)),
					op: PostfixOperator::Range,
				},
				(Bound::Excluded(start), Bound::Unbounded) => Self::Postfix {
					expr: Box::new(Self::from_surface_value(start)),
					op: PostfixOperator::RangeSkip,
				},
			},
			value @ (PublicValue::None
			| PublicValue::Null
			| PublicValue::Bool(_)
			| PublicValue::Number(_)
			| PublicValue::String(_)
			| PublicValue::Bytes(_)
			| PublicValue::Duration(_)
			| PublicValue::Datetime(_)
			| PublicValue::Date(_)
			| PublicValue::Time(_)
			| PublicValue::Timestamp(_)
			| PublicValue::Interval(_)
			| PublicValue::Uuid(_)
			| PublicValue::Geometry(_)
			| PublicValue::Table(_)
			| PublicValue::File(_)
			| PublicValue::Regex(_)) => convert_public_value_to_internal(value).into_literal(),
		}
	}
	fn surface_record_id_key(key: surrealdb_types::RecordIdKey) -> RecordIdKeyLit {
		use surrealdb_types::RecordIdKey as Key;
		match key {
			Key::Number(value) => RecordIdKeyLit::Number(value),
			Key::String(value) => RecordIdKeyLit::String(value.into()),
			Key::Uuid(value) => RecordIdKeyLit::Uuid(crate::val::Uuid(value.into_inner())),
			Key::Array(values) => {
				RecordIdKeyLit::Array(values.into_iter().map(Self::from_surface_value).collect())
			}
			Key::Object(values) => RecordIdKeyLit::Object(
				values
					.into_iter()
					.map(|(key, value)| ObjectEntry {
						key: key.into(),
						value: Self::from_surface_value(value),
					})
					.collect(),
			),
			Key::Range(range) => RecordIdKeyLit::Range(Box::new(super::RecordIdKeyRangeLit {
				start: range.start.map(Self::surface_record_id_key),
				end: range.end.map(Self::surface_record_id_key),
			})),
		}
	}
}

#[cfg(test)]
mod tests {
	use std::ops::Bound;

	use surrealdb_types::{
		Number, Range, RecordId, RecordIdKey, RecordIdKeyRange, ToSql, Value as PublicValue,
	};

	use super::Expr;

	#[test]
	fn from_surface_value_preserves_range_bound_kinds() {
		let full = PublicValue::Range(Box::new(Range {
			start: Bound::Unbounded,
			end: Bound::Unbounded,
		}));
		assert_eq!(Expr::from_surface_value(full).to_sql(), "..");

		let prefix = PublicValue::Range(Box::new(Range {
			start: Bound::Unbounded,
			end: Bound::Excluded(PublicValue::Number(Number::Int(5))),
		}));
		assert_eq!(Expr::from_surface_value(prefix).to_sql(), "..5");

		let prefix_inc = PublicValue::Range(Box::new(Range {
			start: Bound::Unbounded,
			end: Bound::Included(PublicValue::Number(Number::Int(5))),
		}));
		assert_eq!(Expr::from_surface_value(prefix_inc).to_sql(), "..=5");

		let postfix = PublicValue::Range(Box::new(Range {
			start: Bound::Included(PublicValue::Number(Number::Int(1))),
			end: Bound::Unbounded,
		}));
		assert_eq!(Expr::from_surface_value(postfix).to_sql(), "1..");

		let postfix_skip = PublicValue::Range(Box::new(Range {
			start: Bound::Excluded(PublicValue::Number(Number::Int(1))),
			end: Bound::Unbounded,
		}));
		assert_eq!(Expr::from_surface_value(postfix_skip).to_sql(), "1>..");

		let skip = PublicValue::Range(Box::new(Range {
			start: Bound::Excluded(PublicValue::Number(Number::Int(1))),
			end: Bound::Excluded(PublicValue::Number(Number::Int(5))),
		}));
		assert_eq!(Expr::from_surface_value(skip).to_sql(), "1>..5");

		let skip_inc = PublicValue::Range(Box::new(Range {
			start: Bound::Excluded(PublicValue::Number(Number::Int(1))),
			end: Bound::Included(PublicValue::Number(Number::Int(5))),
		}));
		assert_eq!(Expr::from_surface_value(skip_inc).to_sql(), "1>..=5");
	}

	#[test]
	fn from_public_value_preserves_record_id_range_key() {
		let rid = PublicValue::RecordId(RecordId {
			table: "t".into(),
			key: RecordIdKey::Range(Box::new(RecordIdKeyRange {
				start: Bound::Excluded(RecordIdKey::Number(1)),
				end: Bound::Included(RecordIdKey::Number(5)),
			})),
		});
		assert_eq!(Expr::from_public_value(rid).to_sql(), "t:1>..=5");

		let postfix_inc = Range {
			start: Bound::Included(PublicValue::Number(Number::Int(1))),
			end: Bound::Unbounded,
		};
		assert_eq!(Expr::from(postfix_inc).to_sql(), "1..");

		let postfix_skip = Range {
			start: Bound::Excluded(PublicValue::Number(Number::Int(1))),
			end: Bound::Unbounded,
		};
		assert_eq!(Expr::from(postfix_skip).to_sql(), "1>..");
	}
}
