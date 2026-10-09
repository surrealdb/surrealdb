//! Canonical expression construction for programmatic statements and schema output.
use std::ops::Bound;

use surrealdb_types::Value as PublicValue;

use super::literal::ObjectEntry;
use super::{BinaryOperator, Expr, Literal, RecordIdKeyLit, RecordIdLit};
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
			PublicValue::Range(range) => {
				let (start, end) = range.into_inner();
				let op = match (&start, &end) {
					(Bound::Included(_), Bound::Included(_)) => BinaryOperator::RangeInclusive,
					_ => BinaryOperator::Range,
				};
				let bound = |value| match value {
					Bound::Included(value) | Bound::Excluded(value) => {
						Self::from_surface_value(value)
					}
					Bound::Unbounded => Self::Literal(Literal::None),
				};
				Self::Binary {
					left: Box::new(bound(start)),
					op,
					right: Box::new(bound(end)),
				}
			}
			value => convert_public_value_to_internal(value).into_literal(),
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
