use std::collections::BTreeSet;

use surrealdb_types::{Number, Value};

/// The Postgres wire types emitted by the listener, with their catalog OIDs.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(super) enum PgType {
	Bool,
	Bytea,
	Char,
	Name,
	Int8,
	Int2,
	Int4,
	Text,
	Oid,
	Json,
	Cidr,
	Float4,
	Float8,
	MacAddr,
	Inet,
	Bpchar,
	Varchar,
	Date,
	Time,
	Timestamp,
	Timestamptz,
	Interval,
	Timetz,
	Numeric,
	Regclass,
	Record,
	Uuid,
	Jsonb,
	Array(Box<PgType>),
}

impl PgType {
	pub(super) fn oid(&self) -> i32 {
		match self {
			Self::Bool => 16,
			Self::Bytea => 17,
			Self::Char => 18,
			Self::Name => 19,
			Self::Int8 => 20,
			Self::Int2 => 21,
			Self::Int4 => 23,
			Self::Text => 25,
			Self::Oid => 26,
			Self::Json => 114,
			Self::Cidr => 650,
			Self::Float4 => 700,
			Self::Float8 => 701,
			Self::MacAddr => 829,
			Self::Inet => 869,
			Self::Bpchar => 1042,
			Self::Varchar => 1043,
			Self::Date => 1082,
			Self::Time => 1083,
			Self::Timestamp => 1114,
			Self::Timestamptz => 1184,
			Self::Interval => 1186,
			Self::Timetz => 1266,
			Self::Numeric => 1700,
			Self::Regclass => 2205,
			Self::Record => 2249,
			Self::Uuid => 2950,
			Self::Jsonb => 3802,
			Self::Array(elem) => match elem.as_ref() {
				Self::Bool => 1000,
				Self::Bytea => 1001,
				Self::Char => 1002,
				Self::Name => 1003,
				Self::Int2 => 1005,
				Self::Int4 => 1007,
				Self::Text => 1009,
				Self::Bpchar => 1014,
				Self::Varchar => 1015,
				Self::Int8 => 1016,
				Self::Float4 => 1021,
				Self::Float8 => 1022,
				Self::Oid => 1028,
				Self::MacAddr => 1040,
				Self::Inet => 1041,
				Self::Cidr => 651,
				Self::Timestamp => 1115,
				Self::Date => 1182,
				Self::Time => 1183,
				Self::Timestamptz => 1185,
				Self::Interval => 1187,
				Self::Numeric => 1231,
				Self::Timetz => 1270,
				Self::Json => 199,
				Self::Jsonb => 3807,
				Self::Uuid => 2951,
				Self::Regclass => 2210,
				_ => 2277, // anyarray
			},
		}
	}

	/// The `pg_type.typlen` value: fixed byte width, or -1 for variable-length types.
	pub(super) fn typlen(&self) -> i16 {
		match self {
			Self::Bool | Self::Char => 1,
			Self::Int2 => 2,
			Self::Int4 | Self::Float4 | Self::Date | Self::Oid | Self::Regclass => 4,
			Self::MacAddr => 6,
			Self::Int8 | Self::Float8 | Self::Timestamp | Self::Timestamptz | Self::Time => 8,
			Self::Timetz => 12,
			Self::Interval | Self::Uuid => 16,
			Self::Name => 64,
			Self::Numeric
			| Self::Text
			| Self::Varchar
			| Self::Bpchar
			| Self::Bytea
			| Self::Json
			| Self::Jsonb
			| Self::Inet
			| Self::Cidr
			| Self::Record
			| Self::Array(_) => -1,
		}
	}

	#[allow(dead_code)]
	pub(super) fn from_oid(oid: i32) -> Option<Self> {
		match oid {
			16 => Some(Self::Bool),
			17 => Some(Self::Bytea),
			18 => Some(Self::Char),
			19 => Some(Self::Name),
			20 => Some(Self::Int8),
			21 => Some(Self::Int2),
			23 => Some(Self::Int4),
			25 => Some(Self::Text),
			26 => Some(Self::Oid),
			114 => Some(Self::Json),
			650 => Some(Self::Cidr),
			700 => Some(Self::Float4),
			701 => Some(Self::Float8),
			829 => Some(Self::MacAddr),
			869 => Some(Self::Inet),
			1042 => Some(Self::Bpchar),
			1043 => Some(Self::Varchar),
			1082 => Some(Self::Date),
			1083 => Some(Self::Time),
			1114 => Some(Self::Timestamp),
			1184 => Some(Self::Timestamptz),
			1186 => Some(Self::Interval),
			1266 => Some(Self::Timetz),
			1700 => Some(Self::Numeric),
			2205 => Some(Self::Regclass),
			2249 => Some(Self::Record),
			2950 => Some(Self::Uuid),
			3802 => Some(Self::Jsonb),
			// Arrays
			1000 => Some(Self::Array(Box::new(Self::Bool))),
			1001 => Some(Self::Array(Box::new(Self::Bytea))),
			1002 => Some(Self::Array(Box::new(Self::Char))),
			1003 => Some(Self::Array(Box::new(Self::Name))),
			1005 => Some(Self::Array(Box::new(Self::Int2))),
			1007 => Some(Self::Array(Box::new(Self::Int4))),
			1009 => Some(Self::Array(Box::new(Self::Text))),
			1014 => Some(Self::Array(Box::new(Self::Bpchar))),
			1015 => Some(Self::Array(Box::new(Self::Varchar))),
			1016 => Some(Self::Array(Box::new(Self::Int8))),
			1021 => Some(Self::Array(Box::new(Self::Float4))),
			1022 => Some(Self::Array(Box::new(Self::Float8))),
			1028 => Some(Self::Array(Box::new(Self::Oid))),
			1040 => Some(Self::Array(Box::new(Self::MacAddr))),
			1041 => Some(Self::Array(Box::new(Self::Inet))),
			651 => Some(Self::Array(Box::new(Self::Cidr))),
			1115 => Some(Self::Array(Box::new(Self::Timestamp))),
			1182 => Some(Self::Array(Box::new(Self::Date))),
			1183 => Some(Self::Array(Box::new(Self::Time))),
			1185 => Some(Self::Array(Box::new(Self::Timestamptz))),
			1187 => Some(Self::Array(Box::new(Self::Interval))),
			1231 => Some(Self::Array(Box::new(Self::Numeric))),
			1270 => Some(Self::Array(Box::new(Self::Timetz))),
			199 => Some(Self::Array(Box::new(Self::Json))),
			3807 => Some(Self::Array(Box::new(Self::Jsonb))),
			2951 => Some(Self::Array(Box::new(Self::Uuid))),
			2210 => Some(Self::Array(Box::new(Self::Regclass))),
			2277 => Some(Self::Array(Box::new(Self::Text))),
			_ => None,
		}
	}

	pub(super) fn from_sql_type(ty: surrealdb_sqlplan::SqlType) -> Self {
		match ty {
			surrealdb_sqlplan::SqlType::Bool => Self::Bool,
			surrealdb_sqlplan::SqlType::Bytea => Self::Bytea,
			surrealdb_sqlplan::SqlType::Char => Self::Char,
			surrealdb_sqlplan::SqlType::Name => Self::Name,
			surrealdb_sqlplan::SqlType::Int2 => Self::Int2,
			surrealdb_sqlplan::SqlType::Int4 => Self::Int4,
			surrealdb_sqlplan::SqlType::Int8 => Self::Int8,
			surrealdb_sqlplan::SqlType::Float4 => Self::Float4,
			surrealdb_sqlplan::SqlType::Float8 => Self::Float8,
			surrealdb_sqlplan::SqlType::Numeric => Self::Numeric,
			surrealdb_sqlplan::SqlType::Text => Self::Text,
			surrealdb_sqlplan::SqlType::Varchar => Self::Varchar,
			surrealdb_sqlplan::SqlType::Bpchar => Self::Bpchar,
			surrealdb_sqlplan::SqlType::Date => Self::Date,
			surrealdb_sqlplan::SqlType::Time => Self::Time,
			surrealdb_sqlplan::SqlType::Timetz => Self::Timetz,
			surrealdb_sqlplan::SqlType::Timestamp => Self::Timestamp,
			surrealdb_sqlplan::SqlType::TimestampTz => Self::Timestamptz,
			surrealdb_sqlplan::SqlType::Interval => Self::Interval,
			surrealdb_sqlplan::SqlType::Uuid => Self::Uuid,
			surrealdb_sqlplan::SqlType::Json => Self::Json,
			surrealdb_sqlplan::SqlType::Array => Self::Array(Box::new(Self::Text)),
			surrealdb_sqlplan::SqlType::Inet => Self::Inet,
			surrealdb_sqlplan::SqlType::Cidr => Self::Cidr,
			surrealdb_sqlplan::SqlType::MacAddr => Self::MacAddr,
			surrealdb_sqlplan::SqlType::Oid => Self::Oid,
			surrealdb_sqlplan::SqlType::Regclass => Self::Regclass,
			surrealdb_sqlplan::SqlType::Record => Self::Record,
			surrealdb_sqlplan::SqlType::Unknown => Self::Text,
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
		Value::Array(arr) => {
			let inner = arr.as_slice();
			if inner.is_empty() {
				Some(PgType::Array(Box::new(PgType::Text)))
			} else {
				let elem_ty = column_type(inner.iter().map(base_type));
				Some(PgType::Array(Box::new(elem_ty)))
			}
		}
		Value::RecordId(_)
		| Value::Range(_)
		| Value::Regex(_)
		| Value::File(_)
		| Value::Table(_) => Some(PgType::Text),
		Value::Object(_) | Value::Set(_) | Value::Geometry(_) => Some(PgType::Jsonb),
	}
}

/// Widen two cell types into a column type both can encode to.
fn promote(a: PgType, b: PgType) -> PgType {
	if a == b {
		return a;
	}
	use PgType::*;
	match (a, b) {
		// Integer ladder
		(Int2, Int4) | (Int4, Int2) => Int4,
		(Int2 | Int4, Int8) | (Int8, Int2 | Int4) => Int8,
		// Int to float
		(Int2 | Int4, Float4) | (Float4, Int2 | Int4) => Float4,
		(Int2 | Int4 | Int8 | Float4, Float8) | (Float8, Int2 | Int4 | Int8 | Float4) => Float8,
		// Number to numeric
		(Int2 | Int4 | Int8 | Float4 | Float8, Numeric)
		| (Numeric, Int2 | Int4 | Int8 | Float4 | Float8) => Numeric,
		// Strings
		(Char | Bpchar | Varchar, Text) | (Text, Char | Bpchar | Varchar) => Text,
		(Bpchar, Varchar) | (Varchar, Bpchar) => Varchar,
		// Temporal
		(Date, Timestamp) | (Timestamp, Date) => Timestamp,
		(Date | Timestamp, Timestamptz) | (Timestamptz, Date | Timestamp) => Timestamptz,
		(Time, Timetz) | (Timetz, Time) => Timetz,
		// Arrays
		(Array(elem_a), Array(elem_b)) => Array(Box::new(promote(*elem_a, *elem_b))),
		// Fallback
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

/// Reshape a statement result using a static result descriptor from the query planner.
pub(super) fn shape_result_with_descriptor(
	descriptor: &surrealdb_sqlplan::ResultDescriptor,
	value: Value,
) -> ResultShape {
	let values = match value {
		Value::Array(arr) => arr.into_inner(),
		v => vec![v],
	};
	let columns: Vec<PgColumn> = descriptor
		.columns
		.iter()
		.map(|c| PgColumn {
			name: c.name.clone(),
			ty: PgType::from_sql_type(c.ty),
		})
		.collect();

	let mut rows = Vec::with_capacity(values.len());
	for value in values {
		if let Value::Object(obj) = value {
			let obj_map = obj.into_inner();
			let row: Vec<Option<Value>> = descriptor
				.columns
				.iter()
				.enumerate()
				.map(|(i, col)| {
					let val = obj_map
						.get(&col.name)
						.or_else(|| col.original_column.as_ref().and_then(|orig| obj_map.get(orig)))
						.or_else(|| {
							if obj_map.len() == descriptor.columns.len() {
								obj_map.values().nth(i)
							} else {
								None
							}
						});
					val.cloned().and_then(cell)
				})
				.collect();
			rows.push(row);
		} else if descriptor.columns.len() == 1 {
			rows.push(vec![cell(value)]);
		}
	}

	ResultShape {
		columns,
		rows,
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
