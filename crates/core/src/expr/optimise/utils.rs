use std::collections::BTreeMap;

use anyhow::Result;

use crate::expr::{Expr, Literal, RecordIdKeyLit, RecordIdLit};
use crate::val::{Array, Number, Object, Range, RecordId, RecordIdKey, Value};

/// Convert a static literal to a Value synchronously.
///
/// This function mirrors the logic of `Literal::compute()` but works
/// synchronously for static literals that don't require async evaluation.
///
/// For arrays and objects, the children must already be converted to
/// `Expr::Value` by the recursive traversal.
pub(super) fn literal_to_value(literal: &Literal) -> Result<Value> {
	let value = match literal {
		Literal::None => Value::None,
		Literal::Null => Value::Null,
		Literal::UnboundedRange => Value::Range(Box::new(Range::unbounded())),
		Literal::Bool(x) => Value::Bool(*x),
		Literal::Float(x) => Value::Number(Number::Float(*x)),
		Literal::Integer(i) => Value::Number(Number::Int(*i)),
		Literal::Decimal(d) => Value::Number(Number::Decimal(*d)),
		Literal::String(strand) => Value::String(strand.clone()),
		Literal::Bytes(bytes) => Value::Bytes(bytes.clone()),
		Literal::Regex(regex) => Value::Regex(regex.clone()),
		Literal::RecordId(record_id_lit) => {
			Value::RecordId(record_id_key_lit_to_record_id(record_id_lit)?)
		}
		Literal::Array(exprs) => {
			let mut array = Vec::with_capacity(exprs.len());
			for e in exprs.iter() {
				match e {
					Expr::Value(v) => array.push(v.clone()),
					_ => {
						anyhow::bail!("Array contains non-value expression in static literal")
					}
				}
			}
			Value::Array(Array(array))
		}
		Literal::Set(exprs) => {
			let mut set = crate::val::Set::new();
			for e in exprs.iter() {
				match e {
					Expr::Value(v) => {
						set.insert(v.clone());
					}
					_ => anyhow::bail!("Set contains non-value expression in static literal"),
				}
			}
			Value::Set(set)
		}
		Literal::Object(items) => {
			let mut map = BTreeMap::new();
			for i in items.iter() {
				match &i.value {
					Expr::Value(v) => {
						map.insert(i.key.clone(), v.clone());
					}
					_ => {
						anyhow::bail!("Object contains non-value expression in static literal")
					}
				}
			}
			Value::Object(Object(map))
		}
		Literal::Duration(duration) => Value::Duration(*duration),
		Literal::Datetime(datetime) => Value::Datetime(datetime.clone()),
		Literal::Uuid(uuid) => Value::Uuid(*uuid),
		Literal::Geometry(geometry) => Value::Geometry(geometry.clone()),
		Literal::File(file) => Value::File(file.clone()),
	};
	Ok(value)
}

/// Convert a RecordIdLit to a RecordId synchronously
fn record_id_key_lit_to_record_id(record_id_lit: &RecordIdLit) -> Result<RecordId> {
	let key = record_id_key_lit_to_key(&record_id_lit.key)?;
	Ok(RecordId {
		table: record_id_lit.table.clone(),
		key,
	})
}

/// Convert a RecordIdKeyLit to a RecordIdKey synchronously
fn record_id_key_lit_to_key(key_lit: &RecordIdKeyLit) -> Result<RecordIdKey> {
	let key = match key_lit {
		RecordIdKeyLit::Number(v) => RecordIdKey::Number(*v),
		RecordIdKeyLit::String(v) => RecordIdKey::String(v.clone()),
		RecordIdKeyLit::Uuid(v) => RecordIdKey::Uuid(*v),
		RecordIdKeyLit::Array(v) => {
			let mut res = Vec::new();
			for expr in v.iter() {
				match expr {
					Expr::Value(val) => res.push(val.clone()),
					_ => anyhow::bail!("RecordId array key contains non-value expression"),
				}
			}
			RecordIdKey::Array(Array(res))
		}
		RecordIdKeyLit::Object(v) => {
			let mut res = Object::default();
			for entry in v.iter() {
				match &entry.value {
					Expr::Value(val) => {
						res.insert(entry.key.clone(), val.clone());
					}
					_ => anyhow::bail!("RecordId object key contains non-value expression"),
				}
			}
			RecordIdKey::Object(res)
		}
		RecordIdKeyLit::Generate(v) => v.compute(),
		RecordIdKeyLit::Range(_) => {
			anyhow::bail!("RecordId range keys cannot be converted statically")
		}
	};
	Ok(key)
}

/// Helper to extract a Value from an Expr if it's an Expr::Value
pub(super) fn expr_as_value(expr: &Expr) -> Option<&Value> {
	match expr {
		Expr::Value(v) => Some(v),
		_ => None,
	}
}

/// Helper to convert Expr::Constant to Value
pub(super) fn constant_to_value(constant: &crate::expr::Constant) -> Result<Value> {
	constant.compute()
}
