//! Type mapping and text serialization for MySQL resultsets.

use std::collections::BTreeSet;

use anyhow::{Result, ensure};
use surrealdb_types::{Number, ToSql, Value};

use crate::codec::{read_lenenc_bytes, write_lenenc_str};
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
	pub raw_rows: Vec<Vec<Value>>,
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
				let mut raw_rows = Vec::with_capacity(items.len());
				for item in items {
					if let Value::Object(obj) = item {
						let row =
							col_names.iter().map(|k| obj.get(k).and_then(value_to_text)).collect();
						let raw_row = col_names
							.iter()
							.map(|k| obj.get(k).cloned().unwrap_or(Value::None))
							.collect();
						rows.push(row);
						raw_rows.push(raw_row);
					}
				}

				ResultShape {
					columns,
					rows,
					raw_rows,
				}
			} else {
				// Array of non-objects
				let col_type = items.first().map(infer_mysql_type).unwrap_or(MYSQL_TYPE_VAR_STRING);
				let columns = vec![ColumnDefinition41::new(schema, table, "value", col_type)];
				let rows = items.iter().map(|item| vec![value_to_text(item)]).collect();
				let raw_rows = items.into_iter().map(|item| vec![item]).collect();
				ResultShape {
					columns,
					rows,
					raw_rows,
				}
			}
		}
		Value::Array(_) => ResultShape {
			columns: Vec::new(),
			rows: Vec::new(),
			raw_rows: Vec::new(),
		},
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
			let raw_row =
				col_names.iter().map(|k| obj.get(k).cloned().unwrap_or(Value::None)).collect();
			ResultShape {
				columns,
				rows: vec![row],
				raw_rows: vec![raw_row],
			}
		}
		Value::None | Value::Null => ResultShape {
			columns: Vec::new(),
			rows: Vec::new(),
			raw_rows: Vec::new(),
		},
		other => {
			let ty = infer_mysql_type(&other);
			let columns = vec![ColumnDefinition41::new(schema, table, "result", ty)];
			let row = vec![value_to_text(&other)];
			let raw_row = vec![other];
			ResultShape {
				columns,
				rows: vec![row],
				raw_rows: vec![raw_row],
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
		Value::None | Value::Null => Vec::new(),
		v => vec![v],
	};

	let mut rows = Vec::with_capacity(items.len());
	let mut raw_rows = Vec::with_capacity(items.len());
	for item in items {
		if let Value::Object(obj) = item {
			let mut row = Vec::with_capacity(descriptor.columns.len());
			let mut raw_row = Vec::with_capacity(descriptor.columns.len());
			for (i, col) in descriptor.columns.iter().enumerate() {
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
				row.push(val.and_then(value_to_text));
				raw_row.push(val.cloned().unwrap_or(Value::None));
			}
			rows.push(row);
			raw_rows.push(raw_row);
		} else if descriptor.columns.len() == 1 {
			rows.push(vec![value_to_text(&item)]);
			raw_rows.push(vec![item]);
		}
	}

	ResultShape {
		columns,
		rows,
		raw_rows,
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

/// Binary representation of a row for the MySQL binary protocol (COM_STMT_EXECUTE).
///
/// Header: 0x00
/// Null bitmap: `(num_columns + 7 + 2) / 8` bytes with offset 2
/// Values: binary encoded for each non-null column
pub fn serialize_binary_row(columns: &[ColumnDefinition41], values: &[Value]) -> Vec<u8> {
	let mut buf = Vec::with_capacity(64);
	// 1. Packet header 0x00 for binary row
	buf.push(0x00);

	// 2. NULL-bitmap: length is (num_columns + 7 + 2) / 8 bytes
	let null_bitmap_len = (columns.len() + 9) / 8;
	let mut null_bitmap = vec![0u8; null_bitmap_len];

	for (i, val) in values.iter().enumerate().take(columns.len()) {
		if matches!(val, Value::None | Value::Null) {
			let byte_idx = (i + 2) / 8;
			let bit_idx = (i + 2) % 8;
			null_bitmap[byte_idx] |= 1 << bit_idx;
		}
	}
	buf.extend_from_slice(&null_bitmap);

	// 3. Values for non-null columns
	for (i, val) in values.iter().enumerate().take(columns.len()) {
		if matches!(val, Value::None | Value::Null) {
			continue;
		}
		let col_type = columns.get(i).map(|c| c.column_type).unwrap_or(MYSQL_TYPE_VAR_STRING);
		encode_binary_value(&mut buf, col_type, val);
	}

	buf
}

/// Encode a single SurrealDB value into MySQL binary protocol format for resultsets.
pub fn encode_binary_value(buf: &mut Vec<u8>, col_type: u8, val: &Value) {
	match col_type {
		MYSQL_TYPE_TINY => {
			let byte = match val {
				Value::Bool(b) => u8::from(*b),
				Value::Number(Number::Int(i)) => *i as u8,
				_ => 0u8,
			};
			buf.push(byte);
		}
		MYSQL_TYPE_SHORT | MYSQL_TYPE_YEAR => {
			let n = match val {
				Value::Number(Number::Int(i)) => *i as i16,
				_ => 0i16,
			};
			buf.extend_from_slice(&n.to_le_bytes());
		}
		MYSQL_TYPE_LONG | MYSQL_TYPE_INT24 => {
			let n = match val {
				Value::Number(Number::Int(i)) => *i as i32,
				_ => 0i32,
			};
			buf.extend_from_slice(&n.to_le_bytes());
		}
		MYSQL_TYPE_LONGLONG => {
			let n = match val {
				Value::Number(Number::Int(i)) => *i,
				_ => 0i64,
			};
			buf.extend_from_slice(&n.to_le_bytes());
		}
		MYSQL_TYPE_FLOAT => {
			let f = match val {
				Value::Number(Number::Float(fl)) => *fl as f32,
				_ => 0.0f32,
			};
			buf.extend_from_slice(&f.to_le_bytes());
		}
		MYSQL_TYPE_DOUBLE => {
			let f = match val {
				Value::Number(Number::Float(fl)) => *fl,
				_ => 0.0f64,
			};
			buf.extend_from_slice(&f.to_le_bytes());
		}
		_ => {
			// Length-encoded string for all other types (string, decimal, json, blob, etc.)
			if let Some(text) = value_to_text(val) {
				write_lenenc_str(buf, &text);
			} else {
				buf.push(0x00);
			}
		}
	}
}

/// Decode a parameter sent in MySQL binary protocol from `COM_STMT_EXECUTE`.
pub fn decode_binary_param(cursor: &mut &[u8], type_code: u8, unsigned: bool) -> Result<Value> {
	match type_code {
		MYSQL_TYPE_NULL => Ok(Value::Null),
		MYSQL_TYPE_TINY => {
			ensure!(!cursor.is_empty(), "unexpected EOF reading MYSQL_TYPE_TINY");
			let b = cursor[0];
			*cursor = &cursor[1..];
			if unsigned {
				Ok(Value::Number(Number::Int(b as i64)))
			} else {
				Ok(Value::Number(Number::Int((b as i8) as i64)))
			}
		}
		MYSQL_TYPE_SHORT | MYSQL_TYPE_YEAR => {
			ensure!(cursor.len() >= 2, "unexpected EOF reading MYSQL_TYPE_SHORT");
			let b = [cursor[0], cursor[1]];
			*cursor = &cursor[2..];
			if unsigned {
				Ok(Value::Number(Number::Int(u16::from_le_bytes(b) as i64)))
			} else {
				Ok(Value::Number(Number::Int(i16::from_le_bytes(b) as i64)))
			}
		}
		MYSQL_TYPE_LONG | MYSQL_TYPE_INT24 => {
			ensure!(cursor.len() >= 4, "unexpected EOF reading MYSQL_TYPE_LONG");
			let b = [cursor[0], cursor[1], cursor[2], cursor[3]];
			*cursor = &cursor[4..];
			if unsigned {
				Ok(Value::Number(Number::Int(u32::from_le_bytes(b) as i64)))
			} else {
				Ok(Value::Number(Number::Int(i32::from_le_bytes(b) as i64)))
			}
		}
		MYSQL_TYPE_LONGLONG => {
			ensure!(cursor.len() >= 8, "unexpected EOF reading MYSQL_TYPE_LONGLONG");
			let b = [
				cursor[0], cursor[1], cursor[2], cursor[3], cursor[4], cursor[5], cursor[6],
				cursor[7],
			];
			*cursor = &cursor[8..];
			if unsigned {
				let v = u64::from_le_bytes(b);
				Ok(Value::Number(Number::Int(v as i64)))
			} else {
				Ok(Value::Number(Number::Int(i64::from_le_bytes(b))))
			}
		}
		MYSQL_TYPE_FLOAT => {
			ensure!(cursor.len() >= 4, "unexpected EOF reading MYSQL_TYPE_FLOAT");
			let b = [cursor[0], cursor[1], cursor[2], cursor[3]];
			*cursor = &cursor[4..];
			let f = f32::from_le_bytes(b);
			Ok(Value::Number(Number::Float(f as f64)))
		}
		MYSQL_TYPE_DOUBLE => {
			ensure!(cursor.len() >= 8, "unexpected EOF reading MYSQL_TYPE_DOUBLE");
			let b = [
				cursor[0], cursor[1], cursor[2], cursor[3], cursor[4], cursor[5], cursor[6],
				cursor[7],
			];
			*cursor = &cursor[8..];
			let f = f64::from_le_bytes(b);
			Ok(Value::Number(Number::Float(f)))
		}
		MYSQL_TYPE_DATE | MYSQL_TYPE_DATETIME | MYSQL_TYPE_TIMESTAMP => {
			ensure!(!cursor.is_empty(), "unexpected EOF reading datetime length");
			let len = cursor[0] as usize;
			*cursor = &cursor[1..];
			ensure!(cursor.len() >= len, "unexpected EOF reading datetime body");
			let data = &cursor[..len];
			*cursor = &cursor[len..];
			if len == 0 {
				Ok(Value::String("0000-00-00 00:00:00".to_string()))
			} else if len >= 4 {
				let year = u16::from_le_bytes([data[0], data[1]]);
				let month = data[2];
				let day = data[3];
				if len == 4 {
					Ok(Value::String(format!("{year:04}-{month:02}-{day:02}")))
				} else if len >= 7 {
					let hour = data[4];
					let min = data[5];
					let sec = data[6];
					if len == 7 {
						Ok(Value::String(format!(
							"{year:04}-{month:02}-{day:02} {hour:02}:{min:02}:{sec:02}"
						)))
					} else if len == 11 {
						let micro = u32::from_le_bytes([data[7], data[8], data[9], data[10]]);
						Ok(Value::String(format!(
							"{year:04}-{month:02}-{day:02} {hour:02}:{min:02}:{sec:02}.{micro:06}"
						)))
					} else {
						Ok(Value::String(format!(
							"{year:04}-{month:02}-{day:02} {hour:02}:{min:02}:{sec:02}"
						)))
					}
				} else {
					Ok(Value::String(format!("{year:04}-{month:02}-{day:02}")))
				}
			} else {
				Ok(Value::Null)
			}
		}
		MYSQL_TYPE_TIME => {
			ensure!(!cursor.is_empty(), "unexpected EOF reading time length");
			let len = cursor[0] as usize;
			*cursor = &cursor[1..];
			ensure!(cursor.len() >= len, "unexpected EOF reading time body");
			let data = &cursor[..len];
			*cursor = &cursor[len..];
			if len == 0 {
				Ok(Value::String("00:00:00".to_string()))
			} else if len >= 8 {
				let is_neg = data[0] != 0;
				let days = u32::from_le_bytes([data[1], data[2], data[3], data[4]]);
				let hour = data[5] as u32 + days * 24;
				let min = data[6];
				let sec = data[7];
				let sign = if is_neg {
					"-"
				} else {
					""
				};
				if len >= 12 {
					let micro = u32::from_le_bytes([data[8], data[9], data[10], data[11]]);
					Ok(Value::String(format!("{sign}{hour:02}:{min:02}:{sec:02}.{micro:06}")))
				} else {
					Ok(Value::String(format!("{sign}{hour:02}:{min:02}:{sec:02}")))
				}
			} else {
				Ok(Value::String("00:00:00".to_string()))
			}
		}
		_ => {
			// String / Blob / JSON / Decimal (lenenc-string format)
			let bytes = read_lenenc_bytes(cursor).unwrap_or(&[]);
			match String::from_utf8(bytes.to_vec()) {
				Ok(s) => Ok(Value::String(s)),
				Err(_) => Ok(Value::Bytes(bytes.to_vec().into())),
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_shape_result_empty_array() {
		let val = Value::Array(surrealdb_types::Array::new());
		let shape = shape_result("test", "items", val);
		assert!(shape.columns.is_empty(), "Empty array should yield 0 columns");
		assert!(shape.rows.is_empty(), "Empty array should yield 0 rows");
	}

	#[test]
	fn test_shape_result_non_empty_array() {
		let mut obj = surrealdb_types::Object::new();
		obj.insert("id".to_string(), Value::Number(Number::Int(1)));
		obj.insert("name".to_string(), Value::String("test".to_string()));
		let val = Value::Array(vec![Value::Object(obj)].into());
		let shape = shape_result("test", "items", val);
		assert_eq!(shape.columns.len(), 2);
		assert_eq!(shape.rows.len(), 1);
		assert_eq!(shape.raw_rows.len(), 1);
	}

	#[test]
	fn test_serialize_binary_row() {
		let cols = vec![
			ColumnDefinition41::new("test", "tbl", "id", MYSQL_TYPE_LONGLONG),
			ColumnDefinition41::new("test", "tbl", "name", MYSQL_TYPE_VAR_STRING),
			ColumnDefinition41::new("test", "tbl", "age", MYSQL_TYPE_LONG),
		];
		// row: id=42, name=NULL, age=30
		let row = vec![Value::Number(Number::Int(42)), Value::Null, Value::Number(Number::Int(30))];
		let bytes = serialize_binary_row(&cols, &row);
		// Header byte 0x00
		assert_eq!(bytes[0], 0x00);
		// Null bitmap: len is (3 + 9) / 8 = 1 byte
		// Bit 2 is col 0 (0), bit 3 is col 1 (1, NULL), bit 4 is col 2 (0) -> 1 << 3 = 0x08
		assert_eq!(bytes[1], 0x08);
		// Following is col 0 (LONGLONG 42) -> 8 bytes LE: 42, 0, 0, 0, 0, 0, 0, 0
		assert_eq!(&bytes[2..10], &42i64.to_le_bytes());
		// Col 1 is NULL, skipped in value stream!
		// Col 2 is LONG 30 -> 4 bytes LE: 30, 0, 0, 0
		assert_eq!(&bytes[10..14], &30i32.to_le_bytes());
		assert_eq!(bytes.len(), 14);
	}

	#[test]
	fn test_decode_binary_param() {
		// Test integer param
		let int_data = 12345i32.to_le_bytes();
		let mut cursor = &int_data[..];
		let val = decode_binary_param(&mut cursor, MYSQL_TYPE_LONG, false).unwrap();
		assert_eq!(val, Value::Number(Number::Int(12345)));

		// Test string param
		let mut str_data = Vec::new();
		write_lenenc_str(&mut str_data, "hello world");
		let mut cursor = &str_data[..];
		let val = decode_binary_param(&mut cursor, MYSQL_TYPE_VAR_STRING, false).unwrap();
		assert_eq!(val, Value::String("hello world".to_string()));
	}
}
