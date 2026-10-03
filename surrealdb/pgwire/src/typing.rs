use std::collections::BTreeSet;

use surrealdb_types::{Number, Value};

/// The Postgres wire types emitted by the listener, with their catalog OIDs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PgType {
	Bool,
	Int8,
	Float8,
	Numeric,
	Text,
	Timestamptz,
	Interval,
	Uuid,
	Bytea,
	Jsonb,
}

impl PgType {
	pub(super) fn oid(self) -> i32 {
		match self {
			Self::Bool => 16,
			Self::Bytea => 17,
			Self::Int8 => 20,
			Self::Text => 25,
			Self::Float8 => 701,
			Self::Timestamptz => 1184,
			Self::Interval => 1186,
			Self::Numeric => 1700,
			Self::Uuid => 2950,
			Self::Jsonb => 3802,
		}
	}

	/// The `pg_type.typlen` value: fixed byte width, or -1 for variable-length types.
	pub(super) fn typlen(self) -> i16 {
		match self {
			Self::Bool => 1,
			Self::Int8 | Self::Float8 | Self::Timestamptz => 8,
			Self::Interval | Self::Uuid => 16,
			Self::Numeric | Self::Text | Self::Bytea | Self::Jsonb => -1,
		}
	}
}

#[derive(Debug, Clone)]
pub(super) struct PgColumn {
	pub(super) name: String,
	pub(super) ty: PgType,
}

/// A statement result reshaped for the wire: typed columns plus rows of
/// optional cells, where `None` encodes SQL NULL.
#[derive(Debug)]
pub(super) struct ResultShape {
	pub(super) columns: Vec<PgColumn>,
	pub(super) rows: Vec<Vec<Option<Value>>>,
}

/// The wire type a single value naturally maps to; `None` for NONE/NULL.
fn base_type(value: &Value) -> Option<PgType> {
	match value {
		Value::None | Value::Null => None,
		Value::Bool(_) => Some(PgType::Bool),
		Value::Number(Number::Int(_)) => Some(PgType::Int8),
		Value::Number(Number::Float(_)) => Some(PgType::Float8),
		Value::Number(Number::Decimal(_)) => Some(PgType::Numeric),
		Value::String(_) => Some(PgType::Text),
		Value::Datetime(_) => Some(PgType::Timestamptz),
		Value::Duration(_) => Some(PgType::Interval),
		Value::Uuid(_) => Some(PgType::Uuid),
		Value::Bytes(_) => Some(PgType::Bytea),
		Value::RecordId(_)
		| Value::Range(_)
		| Value::Regex(_)
		| Value::File(_)
		| Value::Table(_) => Some(PgType::Text),
		Value::Object(_) | Value::Array(_) | Value::Set(_) | Value::Geometry(_) => {
			Some(PgType::Jsonb)
		}
	}
}

/// Widen two cell types into a column type both can encode to.
fn promote(a: PgType, b: PgType) -> PgType {
	use PgType::{Float8, Int8, Jsonb, Numeric};
	if a == b {
		return a;
	}
	match (a, b) {
		(Int8, Float8) | (Float8, Int8) => Float8,
		(Int8 | Float8, Numeric) | (Numeric, Int8 | Float8) => Numeric,
		_ => Jsonb,
	}
}

fn column_type(cells: impl Iterator<Item = Option<PgType>>) -> PgType {
	cells.flatten().reduce(promote).unwrap_or(PgType::Text)
}

fn cell(value: Value) -> Option<Value> {
	match value {
		Value::None | Value::Null => None,
		v => Some(v),
	}
}

/// Reshape one statement result for the wire.
///
/// A result whose rows are all objects becomes a multi-column result set: the
/// columns are the union of the objects' keys (key-sorted — `Object` is a
/// `BTreeMap`, so projection order is not preserved), each typed by promoting
/// the types of the values present in it. Any other result becomes a single
/// `value` column with one row per array element, or a single row for a
/// scalar.
pub(super) fn shape_result(value: Value) -> ResultShape {
	let values = match value {
		Value::Array(arr) => arr.into_inner(),
		v => vec![v],
	};
	if !values.is_empty() && values.iter().all(|v| matches!(v, Value::Object(_))) {
		let mut objects = Vec::with_capacity(values.len());
		for value in values {
			if let Value::Object(obj) = value {
				objects.push(obj.into_inner());
			}
		}
		let mut names = BTreeSet::new();
		for obj in &objects {
			for key in obj.keys() {
				if !names.contains(key) {
					names.insert(key.clone());
				}
			}
		}
		let names: Vec<String> = names.into_iter().collect();
		let mut rows = Vec::with_capacity(objects.len());
		for mut obj in objects {
			let row: Vec<Option<Value>> =
				names.iter().map(|name| obj.remove(name).and_then(cell)).collect();
			rows.push(row);
		}
		let columns = names
			.into_iter()
			.enumerate()
			.map(|(i, name)| PgColumn {
				ty: column_type(rows.iter().map(|row| row[i].as_ref().and_then(base_type))),
				name,
			})
			.collect();
		ResultShape {
			columns,
			rows,
		}
	} else {
		let rows: Vec<Vec<Option<Value>>> = values.into_iter().map(|v| vec![cell(v)]).collect();
		let ty = column_type(rows.iter().map(|row| row[0].as_ref().and_then(base_type)));
		ResultShape {
			columns: vec![PgColumn {
				name: "value".to_string(),
				ty,
			}],
			rows,
		}
	}
}

/// Reshape a result as a single `result jsonb` column, one row per top-level
/// array element (or a single row for a scalar). Used by the extended
/// protocol's driver-prepared path, where column types cannot be known before
/// execution so every value is delivered as JSON.
pub(super) fn shape_result_jsonb(value: Value) -> ResultShape {
	let values = match value {
		Value::Array(arr) => arr.into_inner(),
		v => vec![v],
	};
	let rows = values.into_iter().map(|v| vec![cell(v)]).collect();
	ResultShape {
		columns: vec![PgColumn {
			name: "result".to_string(),
			ty: PgType::Jsonb,
		}],
		rows,
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_types::{Object, Value};

	use super::*;

	fn obj(entries: &[(&str, Value)]) -> Value {
		let mut object = Object::new();
		for (key, value) in entries {
			object.insert(key.to_string(), value.clone());
		}
		Value::Object(object)
	}

	#[test]
	fn promotion_ladder() {
		assert_eq!(promote(PgType::Int8, PgType::Int8), PgType::Int8);
		assert_eq!(promote(PgType::Int8, PgType::Float8), PgType::Float8);
		assert_eq!(promote(PgType::Float8, PgType::Numeric), PgType::Numeric);
		assert_eq!(promote(PgType::Int8, PgType::Numeric), PgType::Numeric);
		assert_eq!(promote(PgType::Int8, PgType::Text), PgType::Jsonb);
		assert_eq!(promote(PgType::Uuid, PgType::Bool), PgType::Jsonb);
	}

	#[test]
	fn objects_become_columns() {
		let value = Value::Array(
			vec![
				obj(&[("id", Value::Number(Number::Int(1))), ("name", Value::String("a".into()))]),
				obj(&[
					("id", Value::Number(Number::Int(2))),
					("age", Value::Number(Number::Int(30))),
				]),
			]
			.into(),
		);
		let shape = shape_result(value);
		let names: Vec<&str> = shape.columns.iter().map(|c| c.name.as_str()).collect();
		assert_eq!(names, vec!["age", "id", "name"]);
		assert_eq!(shape.columns[0].ty, PgType::Int8);
		assert_eq!(shape.columns[1].ty, PgType::Int8);
		assert_eq!(shape.columns[2].ty, PgType::Text);
		assert_eq!(shape.rows.len(), 2);
		assert!(shape.rows[0][0].is_none());
		assert!(shape.rows[1][2].is_none());
	}

	#[test]
	fn mixed_numbers_promote() {
		let value = Value::Array(
			vec![
				obj(&[("n", Value::Number(Number::Int(1)))]),
				obj(&[("n", Value::Number(Number::Float(1.5)))]),
			]
			.into(),
		);
		let shape = shape_result(value);
		assert_eq!(shape.columns[0].ty, PgType::Float8);
	}

	#[test]
	fn scalars_become_value_column() {
		let shape = shape_result(Value::Array(
			vec![Value::Number(Number::Int(1)), Value::Number(Number::Int(2))].into(),
		));
		assert_eq!(shape.columns.len(), 1);
		assert_eq!(shape.columns[0].name, "value");
		assert_eq!(shape.columns[0].ty, PgType::Int8);
		assert_eq!(shape.rows.len(), 2);
	}

	#[test]
	fn empty_result_defaults_to_text() {
		let shape = shape_result(Value::Array(Vec::<Value>::new().into()));
		assert_eq!(shape.columns.len(), 1);
		assert_eq!(shape.columns[0].ty, PgType::Text);
		assert!(shape.rows.is_empty());
	}

	#[test]
	fn all_null_column_defaults_to_text() {
		let value =
			Value::Array(vec![obj(&[("x", Value::Null)]), obj(&[("x", Value::None)])].into());
		let shape = shape_result(value);
		assert_eq!(shape.columns[0].ty, PgType::Text);
		assert!(shape.rows.iter().all(|row| row[0].is_none()));
	}
}
