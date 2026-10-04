//! Type mapping and text serialization for MySQL resultsets.

use std::collections::BTreeSet;

use surrealdb_types::{Number, ToSql, Value};

use crate::codec::write_lenenc_str;
use crate::constants::*;
use crate::msg::ColumnDefinition41;

/// Map a SurrealDB value to a MySQL type code.
pub fn infer_mysql_type(value: &Value) -> u8 {
	match value {
		Value::None | Value::Null => MYSQL_TYPE_NULL,
		Value::Bool(_) => MYSQL_TYPE_TINY,
		Value::Number(Number::Int(_)) => MYSQL_TYPE_LONGLONG,
		Value::Number(Number::Float(_)) => MYSQL_TYPE_DOUBLE,
		Value::Number(Number::Decimal(_)) => MYSQL_TYPE_NEWDECIMAL,
		Value::String(_) => MYSQL_TYPE_VAR_STRING,
		Value::Datetime(_) => MYSQL_TYPE_DATETIME,
		Value::Duration(_) => MYSQL_TYPE_TIME,
		Value::Uuid(_) => MYSQL_TYPE_VAR_STRING,
		Value::Bytes(_) => MYSQL_TYPE_BLOB,
		Value::RecordId(_)
		| Value::Range(_)
		| Value::Regex(_)
		| Value::File(_)
		| Value::Table(_) => MYSQL_TYPE_VAR_STRING,
		Value::Object(_) | Value::Array(_) | Value::Set(_) | Value::Geometry(_) => MYSQL_TYPE_JSON,
	}
}

/// Convert a SurrealDB value into its MySQL text-protocol string representation.
pub fn value_to_text(value: &Value) -> Option<String> {
	match value {
		Value::None | Value::Null => None,
		Value::Bool(b) => Some(
			if *b {
				"1"
			} else {
				"0"
			}
			.to_string(),
		),
		Value::Number(Number::Int(i)) => Some(i.to_string()),
		Value::Number(Number::Float(f)) => Some(f.to_string()),
		Value::Number(Number::Decimal(d)) => Some(d.to_string()),
		Value::String(s) => Some(s.clone()),
		Value::Datetime(dt) => Some(dt.to_string()),
		Value::Duration(d) => Some(d.to_string()),
		Value::Uuid(u) => Some(u.to_string()),
		v @ (Value::RecordId(_)
		| Value::Range(_)
		| Value::Regex(_)
		| Value::File(_)
		| Value::Table(_)) => Some(v.to_sql()),
		Value::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
		other => Some(other.to_sql()),
	}
}

/// Structure holding the reshaped tabular result ready for wire transmission.
pub struct ResultShape {
	pub columns: Vec<ColumnDefinition41>,
	pub rows: Vec<Vec<Option<String>>>,
}

/// Reshape a SurrealDB query result `Value` into tabular column definitions and string rows.
pub fn shape_result(schema: &str, table: &str, value: Value) -> ResultShape {
	match value {
		Value::Array(items) if !items.is_empty() => {
			// Check if items are objects (tabular)
			let first_is_object = matches!(items.first(), Some(Value::Object(_)));
			if first_is_object {
				// Collect all unique keys in deterministic order
				let mut keys = BTreeSet::new();
				for item in &items {
					if let Value::Object(obj) = item {
						for k in obj.keys() {
							keys.insert(k.clone());
						}
					}
				}
				let col_names: Vec<String> = keys.into_iter().collect();

				// Determine column types
				let mut col_types = vec![MYSQL_TYPE_VAR_STRING; col_names.len()];
				for (idx, col_name) in col_names.iter().enumerate() {
					for item in &items {
						if let Value::Object(obj) = item
							&& let Some(val) = obj.get(col_name)
							&& !matches!(val, Value::None | Value::Null)
						{
							col_types[idx] = infer_mysql_type(val);
							break;
						}
					}
				}

				let columns: Vec<ColumnDefinition41> = col_names
					.iter()
					.zip(col_types)
					.map(|(name, ty)| ColumnDefinition41::new(schema, table, name, ty))
					.collect();

				let mut rows = Vec::with_capacity(items.len());
				for item in items {
					if let Value::Object(obj) = item {
						let row =
							col_names.iter().map(|k| obj.get(k).and_then(value_to_text)).collect();
						rows.push(row);
					}
				}

				ResultShape {
					columns,
					rows,
				}
			} else {
				// Array of non-objects
				let col_type = items.first().map(infer_mysql_type).unwrap_or(MYSQL_TYPE_VAR_STRING);
				let columns = vec![ColumnDefinition41::new(schema, table, "value", col_type)];
				let rows = items.into_iter().map(|item| vec![value_to_text(&item)]).collect();
				ResultShape {
					columns,
					rows,
				}
			}
		}
		Value::Object(obj) => {
			let col_names: Vec<String> = obj.keys().cloned().collect();
			let columns = col_names
				.iter()
				.map(|k| {
					let ty = obj.get(k).map(infer_mysql_type).unwrap_or(MYSQL_TYPE_VAR_STRING);
					ColumnDefinition41::new(schema, table, k, ty)
				})
				.collect();
			let row = col_names.iter().map(|k| obj.get(k).and_then(value_to_text)).collect();
			ResultShape {
				columns,
				rows: vec![row],
			}
		}
		Value::None | Value::Null => ResultShape {
			columns: Vec::new(),
			rows: Vec::new(),
		},
		other => {
			let ty = infer_mysql_type(&other);
			let columns = vec![ColumnDefinition41::new(schema, table, "result", ty)];
			let row = vec![value_to_text(&other)];
			ResultShape {
				columns,
				rows: vec![row],
			}
		}
	}
}

/// Reshape a SurrealDB query result using a static result descriptor from the query planner.
pub fn shape_result_with_descriptor(
	descriptor: &surrealdb_sqlplan::ResultDescriptor,
	schema: &str,
	table: &str,
	value: Value,
) -> ResultShape {
	let columns: Vec<ColumnDefinition41> = descriptor
		.columns
		.iter()
		.map(|c| {
			let tbl = c.table.as_deref().unwrap_or(table);
			ColumnDefinition41::new(schema, tbl, &c.name, c.ty.to_mysql_type())
		})
		.collect();

	let items = match value {
		Value::Array(items) => items.into_inner(),
		v => vec![v],
	};

	let mut rows = Vec::with_capacity(items.len());
	for item in items {
		if let Value::Object(obj) = item {
			let row = descriptor
				.columns
				.iter()
				.enumerate()
				.map(|(i, col)| {
					let val = obj
						.get(&col.name)
						.or_else(|| col.original_column.as_ref().and_then(|orig| obj.get(orig)))
						.or_else(|| {
							if obj.len() == descriptor.columns.len() {
								obj.values().nth(i)
							} else {
								None
							}
						});
					val.and_then(value_to_text)
				})
				.collect();
			rows.push(row);
		} else if descriptor.columns.len() == 1 {
			rows.push(vec![value_to_text(&item)]);
		}
	}

	ResultShape {
		columns,
		rows,
	}
}

/// Encode one text row packet into bytes.
pub fn encode_text_row(row: &[Option<String>]) -> Vec<u8> {
	let mut buf = Vec::with_capacity(row.len() * 16);
	for cell in row {
		match cell {
			None => buf.push(NULL_VALUE),
			Some(s) => write_lenenc_str(&mut buf, s),
		}
	}
	buf
}
