//! Type mapping and text serialization for MySQL resultsets.

use std::collections::BTreeSet;

use anyhow::{Result, ensure};
use surrealdb_types::{Number, ToSql, Value, Wide};

use crate::codec::{read_lenenc_bytes, write_lenenc_str};
use crate::constants::*;
use crate::msg::ColumnDefinition41;

/// Map a SurrealDB value to a MySQL type code.
pub fn infer_mysql_type(value: &Value) -> u8 {
	match value {
		Value::None | Value::Null => MYSQL_TYPE_NULL,
		Value::Bool(_) => MYSQL_TYPE_TINY,
		Value::Number(n) => match n.wide() {
			Wide::Int(_) => MYSQL_TYPE_LONGLONG,
			Wide::Float(_) => MYSQL_TYPE_DOUBLE,
			Wide::Decimal(_) => MYSQL_TYPE_NEWDECIMAL,
		},
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

/// The calendar fields of a UTC datetime, as MySQL `DATETIME` carries them.
struct DatetimeParts {
	year: u16,
	month: u8,
	day: u8,
	hour: u8,
	minute: u8,
	second: u8,
	micros: u32,
}

/// Split a datetime into `DATETIME` fields; `None` for years outside 0..=9999,
/// which MySQL cannot represent.
fn datetime_parts(dt: &surrealdb_types::Datetime) -> Option<DatetimeParts> {
	let utc = (*dt).into_inner();
	let seconds = utc.timestamp();
	let (year, month, day) = civil_date(seconds.div_euclid(86_400));
	if !(0..=9999).contains(&year) {
		return None;
	}
	let second_of_day = seconds.rem_euclid(86_400);
	Some(DatetimeParts {
		year: year as u16,
		month,
		day,
		hour: (second_of_day / 3600) as u8,
		minute: (second_of_day % 3600 / 60) as u8,
		second: (second_of_day % 60) as u8,
		micros: utc.timestamp_subsec_micros(),
	})
}

/// The year, month and day of the proleptic Gregorian date `days` days after 1970-01-01.
fn civil_date(days: i64) -> (i64, u8, u8) {
	let shifted = days + 719_468;
	let era = shifted.div_euclid(146_097);
	let day_of_era = shifted.rem_euclid(146_097);
	let year_of_era =
		(day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
	let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
	let month_from_march = (5 * day_of_year + 2) / 153;
	let day = (day_of_year - (153 * month_from_march + 2) / 5 + 1) as u8;
	let month = (if month_from_march < 10 {
		month_from_march + 3
	} else {
		month_from_march - 9
	}) as u8;
	let year = year_of_era + era * 400 + i64::from(month <= 2);
	(year, month, day)
}

/// The MySQL `DATETIME` text form, `YYYY-MM-DD HH:MM:SS[.ffffff]`, in UTC.
fn datetime_text(dt: &surrealdb_types::Datetime) -> String {
	match datetime_parts(dt) {
		Some(p) => {
			let mut out = format!(
				"{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
				p.year, p.month, p.day, p.hour, p.minute, p.second
			);
			if p.micros != 0 {
				out.push_str(&format!(".{:06}", p.micros));
			}
			out
		}
		None => dt.to_string(),
	}
}

/// The longest `TIME` MySQL holds, `838:59:59`, in seconds.
const MAX_TIME_SECS: u64 = 838 * 3600 + 59 * 60 + 59;

/// The seconds and microseconds of a duration as a `TIME` holds them: a duration beyond
/// `838:59:59` is that limit.
fn time_parts(d: &surrealdb_types::Duration) -> (u64, u32) {
	let d = (*d).into_inner();
	if d.as_secs() >= MAX_TIME_SECS {
		(MAX_TIME_SECS, 0)
	} else {
		(d.as_secs(), d.subsec_micros())
	}
}

/// The MySQL `TIME` text form, `HH:MM:SS[.ffffff]`, with hours counting every whole hour of the
/// duration, up to the `838:59:59` MySQL holds.
fn time_text(d: &surrealdb_types::Duration) -> String {
	let (secs, micros) = time_parts(d);
	let mut out = format!("{:02}:{:02}:{:02}", secs / 3600, (secs % 3600) / 60, secs % 60);
	if micros != 0 {
		out.push_str(&format!(".{micros:06}"));
	}
	out
}

/// The text of `value` in a column MySQL types `col_type`: a datetime in a `DATE` column is its
/// date and in a `TIME` column its time of day; every other value is its usual text.
fn typed_text(col_type: u8, value: &Value) -> Option<String> {
	match (col_type, value) {
		(MYSQL_TYPE_DATE, Value::Datetime(dt)) => {
			datetime_parts(dt).map(|p| format!("{:04}-{:02}-{:02}", p.year, p.month, p.day))
		}
		(MYSQL_TYPE_TIME, Value::Datetime(dt)) => datetime_parts(dt).map(|p| {
			let mut out = format!("{:02}:{:02}:{:02}", p.hour, p.minute, p.second);
			if p.micros != 0 {
				out.push_str(&format!(".{:06}", p.micros));
			}
			out
		}),
		_ => value_to_text(value),
	}
}

/// The MySQL type of a column whose cells are `cells`: the type they share, or text when their
/// kinds differ, since a client reads every cell as the column's type.
fn column_type<'a>(cells: impl Iterator<Item = &'a Value>) -> u8 {
	let mut found = None;
	for cell in cells {
		if matches!(cell, Value::None | Value::Null) {
			continue;
		}
		let ty = infer_mysql_type(cell);
		match found {
			None => found = Some(ty),
			Some(existing) if existing == ty => {}
			Some(_) => return MYSQL_TYPE_VAR_STRING,
		}
	}
	found.unwrap_or(MYSQL_TYPE_VAR_STRING)
}

/// Convert a SurrealDB value into its MySQL text-protocol string representation.
///
/// Objects, arrays, sets and geometries are rendered as JSON text so that a
/// `JSON`-typed column holds a valid JSON document.
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
		Value::Number(n) => {
			if let Number::F32(f) = n {
				Some(f.to_string())
			} else {
				match n.wide() {
					Wide::Int(i) => Some(i.to_string()),
					Wide::Float(f) => Some(f.to_string()),
					Wide::Decimal(d) => Some(d.to_string()),
				}
			}
		}
		Value::String(s) => Some(s.clone()),
		Value::Datetime(dt) => Some(datetime_text(dt)),
		Value::Duration(d) => Some(time_text(d)),
		Value::Uuid(u) => Some(u.to_string()),
		v @ (Value::RecordId(_)
		| Value::Range(_)
		| Value::Regex(_)
		| Value::File(_)
		| Value::Table(_)) => Some(v.to_sql()),
		Value::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
		v @ (Value::Object(_) | Value::Array(_) | Value::Set(_) | Value::Geometry(_)) => {
			Some(v.clone().into_json_value().to_string())
		}
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
				let col_types: Vec<u8> = col_names
					.iter()
					.map(|col_name| {
						column_type(items.iter().filter_map(|item| {
							if let Value::Object(obj) = item {
								obj.get(col_name)
							} else {
								None
							}
						}))
					})
					.collect();

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
				let col_type = column_type(items.iter());
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
		other @ (Value::Bool(_)
		| Value::Number(_)
		| Value::String(_)
		| Value::Bytes(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Uuid(_)
		| Value::Geometry(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Range(_)
		| Value::Regex(_)
		| Value::Set(_)) => {
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
	// A `*` the planner could not list is shaped from the rows. With no rows the descriptor's own
	// placeholder column stands, so a `SELECT` still answers with a result set.
	if descriptor.has_dynamic_columns()
		&& !matches!(&value, Value::Array(items) if items.is_empty())
		&& !matches!(&value, Value::None | Value::Null)
	{
		return shape_result(schema, table, value);
	}
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
		v @ (Value::Bool(_)
		| Value::Number(_)
		| Value::String(_)
		| Value::Bytes(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Uuid(_)
		| Value::Geometry(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Range(_)
		| Value::Regex(_)
		| Value::Object(_)
		| Value::Set(_)) => vec![v],
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
				let col_type = columns[i].column_type;
				row.push(val.and_then(|v| typed_text(col_type, v)));
				raw_row.push(val.cloned().unwrap_or(Value::None));
			}
			rows.push(row);
			raw_rows.push(raw_row);
		} else if descriptor.columns.len() == 1 {
			rows.push(vec![typed_text(columns[0].column_type, &item)]);
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
				Value::Number(n) => match n.wide() {
					Wide::Int(i) => i as u8,
					Wide::Float(_) | Wide::Decimal(_) => 0u8,
				},
				Value::None
				| Value::Null
				| Value::String(_)
				| Value::Bytes(_)
				| Value::Duration(_)
				| Value::Datetime(_)
				| Value::Uuid(_)
				| Value::Geometry(_)
				| Value::Table(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_)
				| Value::Array(_)
				| Value::Object(_)
				| Value::Set(_) => 0u8,
			};
			buf.push(byte);
		}
		MYSQL_TYPE_SHORT | MYSQL_TYPE_YEAR => {
			let n = match val {
				Value::Number(n) => match n.wide() {
					Wide::Int(i) => i as i16,
					Wide::Float(_) | Wide::Decimal(_) => 0i16,
				},
				Value::None
				| Value::Null
				| Value::Bool(_)
				| Value::String(_)
				| Value::Bytes(_)
				| Value::Duration(_)
				| Value::Datetime(_)
				| Value::Uuid(_)
				| Value::Geometry(_)
				| Value::Table(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_)
				| Value::Array(_)
				| Value::Object(_)
				| Value::Set(_) => 0i16,
			};
			buf.extend_from_slice(&n.to_le_bytes());
		}
		MYSQL_TYPE_LONG | MYSQL_TYPE_INT24 => {
			let n = match val {
				Value::Number(n) => match n.wide() {
					Wide::Int(i) => i as i32,
					Wide::Float(_) | Wide::Decimal(_) => 0i32,
				},
				Value::None
				| Value::Null
				| Value::Bool(_)
				| Value::String(_)
				| Value::Bytes(_)
				| Value::Duration(_)
				| Value::Datetime(_)
				| Value::Uuid(_)
				| Value::Geometry(_)
				| Value::Table(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_)
				| Value::Array(_)
				| Value::Object(_)
				| Value::Set(_) => 0i32,
			};
			buf.extend_from_slice(&n.to_le_bytes());
		}
		MYSQL_TYPE_LONGLONG => {
			let n = match val {
				Value::Number(n) => match n.wide() {
					Wide::Int(i) => i,
					Wide::Float(_) | Wide::Decimal(_) => 0i64,
				},
				Value::None
				| Value::Null
				| Value::Bool(_)
				| Value::String(_)
				| Value::Bytes(_)
				| Value::Duration(_)
				| Value::Datetime(_)
				| Value::Uuid(_)
				| Value::Geometry(_)
				| Value::Table(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_)
				| Value::Array(_)
				| Value::Object(_)
				| Value::Set(_) => 0i64,
			};
			buf.extend_from_slice(&n.to_le_bytes());
		}
		MYSQL_TYPE_FLOAT => {
			let f = match val {
				Value::Number(n) => match n.wide() {
					Wide::Float(fl) => fl as f32,
					Wide::Int(_) | Wide::Decimal(_) => 0.0f32,
				},
				Value::None
				| Value::Null
				| Value::Bool(_)
				| Value::String(_)
				| Value::Bytes(_)
				| Value::Duration(_)
				| Value::Datetime(_)
				| Value::Uuid(_)
				| Value::Geometry(_)
				| Value::Table(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_)
				| Value::Array(_)
				| Value::Object(_)
				| Value::Set(_) => 0.0f32,
			};
			buf.extend_from_slice(&f.to_le_bytes());
		}
		MYSQL_TYPE_DOUBLE => {
			let f = match val {
				Value::Number(n) => match n.wide() {
					Wide::Float(fl) => fl,
					Wide::Int(_) | Wide::Decimal(_) => 0.0f64,
				},
				Value::None
				| Value::Null
				| Value::Bool(_)
				| Value::String(_)
				| Value::Bytes(_)
				| Value::Duration(_)
				| Value::Datetime(_)
				| Value::Uuid(_)
				| Value::Geometry(_)
				| Value::Table(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_)
				| Value::Array(_)
				| Value::Object(_)
				| Value::Set(_) => 0.0f64,
			};
			buf.extend_from_slice(&f.to_le_bytes());
		}
		MYSQL_TYPE_DATE | MYSQL_TYPE_DATETIME | MYSQL_TYPE_TIMESTAMP => {
			encode_binary_datetime(buf, col_type, val);
		}
		MYSQL_TYPE_TIME => encode_binary_time(buf, val),
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

/// The `DATETIME` fields a binary-protocol cell carries: a datetime value, or text in
/// `YYYY-MM-DD[ HH:MM:SS[.ffffff]]` form (a `T` may separate the parts and a trailing `Z` is read
/// as UTC).
fn binary_datetime_parts(val: &Value) -> Option<DatetimeParts> {
	match val {
		Value::Datetime(dt) => datetime_parts(dt),
		Value::String(text) => {
			let text = text.strip_suffix('Z').unwrap_or(text);
			let (date, time) = text.split_once([' ', 'T']).unwrap_or((text, ""));
			let mut date = date.split('-');
			let (hms, frac) = time.split_once('.').unwrap_or((time, ""));
			let hms = if hms.is_empty() {
				"0:0:0"
			} else {
				hms
			};
			let mut hms = hms.split(':');
			let micros = if frac.is_empty() {
				0
			} else {
				let mut digits: String = frac.chars().take(6).collect();
				while digits.len() < 6 {
					digits.push('0');
				}
				digits.parse().ok()?
			};
			let parts = DatetimeParts {
				year: date.next()?.parse().ok()?,
				month: date.next()?.parse().ok()?,
				day: date.next()?.parse().ok()?,
				hour: hms.next().map_or(Some(0), |v| v.parse().ok())?,
				minute: hms.next().map_or(Some(0), |v| v.parse().ok())?,
				second: hms.next().map_or(Some(0), |v| v.parse().ok())?,
				micros,
			};
			(parts.year <= 9999).then_some(parts)
		}
		Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::Bytes(_)
		| Value::Duration(_)
		| Value::Uuid(_)
		| Value::Geometry(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Range(_)
		| Value::Regex(_)
		| Value::Array(_)
		| Value::Object(_)
		| Value::Set(_) => None,
	}
}

/// Encode a `DATE`, `DATETIME` or `TIMESTAMP` cell in the binary protocol: a
/// length byte (0, 4, 7 or 11) followed by that many bytes of calendar fields.
/// A value MySQL cannot represent is sent as the zero date.
fn encode_binary_datetime(buf: &mut Vec<u8>, col_type: u8, val: &Value) {
	let Some(p) = binary_datetime_parts(val) else {
		buf.push(0);
		return;
	};
	let date_only = col_type == MYSQL_TYPE_DATE;
	let has_time = !date_only && (p.hour | p.minute | p.second) != 0;
	let len: u8 = if date_only {
		4
	} else if p.micros != 0 {
		11
	} else if has_time {
		7
	} else {
		4
	};
	buf.push(len);
	buf.extend_from_slice(&p.year.to_le_bytes());
	buf.push(p.month);
	buf.push(p.day);
	if len >= 7 {
		buf.push(p.hour);
		buf.push(p.minute);
		buf.push(p.second);
	}
	if len == 11 {
		buf.extend_from_slice(&p.micros.to_le_bytes());
	}
}

/// Encode a `TIME` cell in the binary protocol: a length byte (0, 8 or 12)
/// followed by the sign, whole days, clock fields and microseconds.
fn encode_binary_time(buf: &mut Vec<u8>, val: &Value) {
	let (secs, micros) = match val {
		Value::Duration(d) => time_parts(d),
		Value::Datetime(dt) => match datetime_parts(dt) {
			Some(p) => (
				u64::from(p.hour) * 3600 + u64::from(p.minute) * 60 + u64::from(p.second),
				p.micros,
			),
			None => (0, 0),
		},
		Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::String(_)
		| Value::Bytes(_)
		| Value::Uuid(_)
		| Value::Geometry(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Range(_)
		| Value::Regex(_)
		| Value::Array(_)
		| Value::Object(_)
		| Value::Set(_) => (0, 0),
	};
	if secs == 0 && micros == 0 {
		buf.push(0);
		return;
	}
	buf.push(if micros != 0 {
		12
	} else {
		8
	});
	buf.push(0);
	buf.extend_from_slice(&((secs / 86_400) as u32).to_le_bytes());
	buf.push(((secs % 86_400) / 3600) as u8);
	buf.push(((secs % 3600) / 60) as u8);
	buf.push((secs % 60) as u8);
	if micros != 0 {
		buf.extend_from_slice(&micros.to_le_bytes());
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

	fn duration_of_hours(hours: u64) -> Value {
		Value::Duration(surrealdb_types::Duration::from_secs(hours * 3600))
	}

	#[test]
	fn a_column_of_one_kind_keeps_its_type_and_mixed_kinds_are_text() {
		let int = |i| Value::Number(Number::Int(i));
		assert_eq!(column_type([int(1), Value::Null, int(2)].iter()), MYSQL_TYPE_LONGLONG);
		assert_eq!(column_type([int(1), Value::String("a".into())].iter()), MYSQL_TYPE_VAR_STRING);
		assert_eq!(
			column_type([int(1), Value::Number(Number::Float(1.5))].iter()),
			MYSQL_TYPE_VAR_STRING
		);
		assert_eq!(column_type([Value::None, Value::Null].iter()), MYSQL_TYPE_VAR_STRING);
	}

	#[test]
	fn a_time_stops_at_the_longest_mysql_holds() {
		let long = duration_of_hours(1000);
		assert_eq!(value_to_text(&long).as_deref(), Some("838:59:59"));
		let mut buf = Vec::new();
		encode_binary_time(&mut buf, &long);
		// 838:59:59 is 34 days 22:59:59.
		assert_eq!(buf, vec![8, 0, 34, 0, 0, 0, 22, 59, 59]);
		let mut buf = Vec::new();
		encode_binary_time(&mut buf, &duration_of_hours(26));
		assert_eq!(buf, vec![8, 0, 1, 0, 0, 0, 2, 0, 0]);
		let mut buf = Vec::new();
		encode_binary_time(&mut buf, &Value::Duration(surrealdb_types::Duration::from_secs(0)));
		assert_eq!(buf, vec![0]);
	}

	#[test]
	fn a_datetime_in_a_time_or_date_column_is_its_time_of_day_or_its_date() {
		let dt = Value::Datetime(
			surrealdb_types::Datetime::from_timestamp(1_705_321_845, 123_456_000)
				.expect("a datetime"),
		);
		assert_eq!(typed_text(MYSQL_TYPE_DATE, &dt).as_deref(), Some("2024-01-15"));
		assert_eq!(typed_text(MYSQL_TYPE_TIME, &dt).as_deref(), Some("12:30:45.123456"));
		assert_eq!(
			typed_text(MYSQL_TYPE_DATETIME, &dt).as_deref(),
			Some("2024-01-15 12:30:45.123456")
		);
		let mut buf = Vec::new();
		encode_binary_time(&mut buf, &dt);
		assert_eq!(buf, vec![12, 0, 0, 0, 0, 0, 12, 30, 45, 0x40, 0xe2, 0x01, 0x00]);
	}

	#[test]
	fn a_year_beyond_9999_is_no_mysql_datetime() {
		let far =
			surrealdb_types::Datetime::from_timestamp(253_402_300_800, 0).expect("year 10000");
		assert!(datetime_parts(&far).is_none());
		let mut buf = Vec::new();
		encode_binary_datetime(&mut buf, MYSQL_TYPE_DATETIME, &Value::Datetime(far));
		assert_eq!(buf, vec![0], "sent as the zero datetime");
		assert!(binary_datetime_parts(&Value::String("12000-01-01".into())).is_none());
	}

	#[test]
	fn datetime_text_in_a_binary_cell_may_be_a_date_alone_or_end_in_z() {
		for text in ["2024-01-15", "2024-01-15T00:00:00Z", "2024-01-15 00:00:00"] {
			let mut buf = Vec::new();
			encode_binary_datetime(&mut buf, MYSQL_TYPE_DATETIME, &Value::String(text.into()));
			assert_eq!(buf, vec![4, 0xe8, 0x07, 1, 15], "{text}");
		}
		let mut buf = Vec::new();
		encode_binary_datetime(
			&mut buf,
			MYSQL_TYPE_DATETIME,
			&Value::String("2024-01-15T12:30:45Z".into()),
		);
		assert_eq!(buf, vec![7, 0xe8, 0x07, 1, 15, 12, 30, 45]);
	}

	/// Every kind of value a string column can receive, with the text its own typed column shows.
	fn string_column_values() -> Vec<(&'static str, Value, &'static str)> {
		let at = |seconds, nanos| {
			Value::Datetime(
				surrealdb_types::Datetime::from_timestamp(seconds, nanos).expect("a datetime"),
			)
		};
		let lasting = |secs, nanos| {
			Value::Duration(surrealdb_types::Duration::from(std::time::Duration::new(secs, nanos)))
		};
		let int = |i| Value::Number(Number::Int(i));
		let string = |s: &str| Value::String(s.to_string());
		vec![
			("datetime", at(315_532_800, 0), "1980-01-01 00:00:00"),
			(
				"datetime, with a fraction",
				at(1_577_882_096, 789_000_000),
				"2020-01-01 12:34:56.789000",
			),
			(
				"datetime, nanoseconds are dropped",
				at(1_705_322_096, 123_456_789),
				"2024-01-15 12:34:56.123456",
			),
			("datetime, before 1970", at(-1, 0), "1969-12-31 23:59:59"),
			("datetime, year 0", at(-62_167_219_200, 0), "0000-01-01 00:00:00"),
			("datetime, last second of 9999", at(253_402_300_799, 0), "9999-12-31 23:59:59"),
			("datetime, beyond MySQL's years", at(253_402_300_800, 0), "+10000-01-01T00:00:00Z"),
			("duration, zero", lasting(0, 0), "00:00:00"),
			("duration, 1h30m", lasting(5_400, 0), "01:30:00"),
			("duration, 26h counts every hour", lasting(93_600, 0), "26:00:00"),
			("duration, milliseconds", lasting(0, 1_000_000), "00:00:00.001000"),
			("duration, beyond TIME is the limit", lasting(3_600_000, 0), "838:59:59"),
			("bool, true", Value::Bool(true), "1"),
			("bool, false", Value::Bool(false), "0"),
			("int", int(-42), "-42"),
			("float", Value::Number(Number::Float(1.5)), "1.5"),
			(
				"uuid",
				Value::Uuid(surrealdb_types::Uuid::from(uuid::Uuid::from_u128(
					0x018f_6b2e_4c3d_7a11_9a52_0123_4567_89ab,
				))),
				"018f6b2e-4c3d-7a11-9a52-0123456789ab",
			),
			("string", string("hello"), "hello"),
			("array", Value::Array(vec![int(1), string("a")].into()), "[1,\"a\"]"),
			(
				"array of datetimes",
				Value::Array(vec![at(315_532_800, 0)].into()),
				"[\"1980-01-01T00:00:00Z\"]",
			),
		]
	}

	#[test]
	fn a_string_column_shows_a_value_as_its_own_typed_column_does() {
		let mut mismatches = Vec::new();
		for (name, value, expected) in string_column_values() {
			let own = infer_mysql_type(&value);
			for ty in [own, MYSQL_TYPE_VAR_STRING, MYSQL_TYPE_VARCHAR, MYSQL_TYPE_STRING] {
				let shown = typed_text(ty, &value);
				if shown.as_deref() != Some(expected) {
					mismatches
						.push(format!("{name} in type {ty:#x}: {shown:?}, expected {expected:?}"));
				}
			}
			let mut binary = Vec::new();
			encode_binary_value(&mut binary, MYSQL_TYPE_VAR_STRING, &value);
			let mut lenenc = Vec::new();
			write_lenenc_str(&mut lenenc, expected);
			if binary != lenenc {
				mismatches.push(format!(
					"{name} in a binary string cell: {binary:?}, expected {lenenc:?}"
				));
			}
		}
		assert!(mismatches.is_empty(), "\n{}", mismatches.join("\n"));
	}

	#[test]
	fn the_civil_date_of_a_day_count_handles_leap_years_and_dates_before_the_epoch() {
		assert_eq!(civil_date(0), (1970, 1, 1));
		assert_eq!(civil_date(-1), (1969, 12, 31));
		assert_eq!(civil_date(11_016), (2000, 2, 29));
		assert_eq!(civil_date(11_017), (2000, 3, 1));
		assert_eq!(civil_date(19_782), (2024, 2, 29));
		assert_eq!(civil_date(-719_528), (0, 1, 1));
		assert_eq!(civil_date(2_932_896), (9999, 12, 31));
	}

	#[test]
	fn datetime_fields_come_from_the_timestamp() {
		let at = |seconds, nanos| {
			datetime_parts(
				&surrealdb_types::Datetime::from_timestamp(seconds, nanos).expect("a datetime"),
			)
		};
		let p = at(951_825_599, 999_999_000).expect("fields");
		assert_eq!(
			(p.year, p.month, p.day, p.hour, p.minute, p.second, p.micros),
			(2000, 2, 29, 11, 59, 59, 999_999)
		);
		let first = at(-62_167_219_200, 0).expect("year 0 is a MySQL year");
		assert_eq!((first.year, first.month, first.day), (0, 1, 1));
		assert!(at(-62_167_219_201, 0).is_none(), "year -1 is not");
		let last = at(253_402_300_799, 0).expect("the last second of 9999");
		assert_eq!((last.year, last.month, last.day, last.hour), (9999, 12, 31, 23));
		assert!(at(253_402_300_800, 0).is_none());
	}
}
