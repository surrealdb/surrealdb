//! Type mapping and text serialization for MySQL resultsets.

use std::collections::BTreeSet;

use anyhow::{Result, ensure};
use surrealdb_mysql::ast::expr::Expr;
use surrealdb_mysql::ast::literal::Literal;
use surrealdb_mysql::ast::operator::UnaryOperator;
use surrealdb_mysql::ast::stmt::{SelectItem, SelectStmt};
use surrealdb_mysql::lexer::span::Span;
use surrealdb_sqlplan::ResultDescriptor;
use surrealdb_sqlplan::reported_type::{MySqlReportedType, mysql_reported_type};
use surrealdb_types::{Number, ToSql, Value, Wide};

use crate::codec::geometry::Wkb;
use crate::codec::{read_lenenc_bytes, write_lenenc_bytes, write_lenenc_str};
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
			Wide::Decimal(_) | Wide::BigDecimal(_) => MYSQL_TYPE_NEWDECIMAL,
		},
		Value::String(_) => MYSQL_TYPE_VAR_STRING,
		Value::Datetime(_) => MYSQL_TYPE_TIMESTAMP,
		Value::Timestamp(_) => MYSQL_TYPE_DATETIME,
		Value::Date(_) => MYSQL_TYPE_DATE,
		Value::Duration(_) | Value::Time(_) | Value::Interval(_) => MYSQL_TYPE_TIME,
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

/// The `DATETIME` fields of an infinite date, timestamp or datetime. MySQL has no infinity, so the
/// value is clamped to the latest datetime it holds (`9999-12-31 23:59:59.999999`) or the earliest
/// one (`1000-01-01 00:00:00`).
fn clamped_infinity(positive: bool) -> DatetimeParts {
	if positive {
		DatetimeParts {
			year: 9999,
			month: 12,
			day: 31,
			hour: 23,
			minute: 59,
			second: 59,
			micros: 999_999,
		}
	} else {
		DatetimeParts {
			year: 1000,
			month: 1,
			day: 1,
			hour: 0,
			minute: 0,
			second: 0,
			micros: 0,
		}
	}
}

/// Split a datetime into `DATETIME` fields; `None` for years outside 0..=9999,
/// which MySQL cannot represent. An infinite datetime is clamped, see [`clamped_infinity`].
fn datetime_parts(dt: &surrealdb_types::Datetime) -> Option<DatetimeParts> {
	if dt.is_infinite() {
		return Some(clamped_infinity(*dt == surrealdb_types::Datetime::MAX_UTC));
	}
	let utc = (*dt).into_inner();
	parts_of_seconds(utc.timestamp(), utc.timestamp_subsec_micros())
}

/// Split the wall time `seconds` after 1970-01-01T00:00:00 into `DATETIME` fields.
fn parts_of_seconds(seconds: i64, micros: u32) -> Option<DatetimeParts> {
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
		micros,
	})
}

/// The `DATETIME` fields of a `date` (at midnight) or a `timestamp`.
fn temporal_parts(val: &Value) -> Option<DatetimeParts> {
	match val {
		Value::Date(d) if d.is_infinite() => Some(clamped_infinity(d.days() > 0)),
		Value::Date(d) => parts_of_seconds(i64::from(d.days()) * 86_400, 0),
		Value::Timestamp(t) if t.is_infinite() => Some(clamped_infinity(t.seconds() > 0)),
		Value::Timestamp(t) => parts_of_seconds(t.seconds(), t.nanos() / 1_000),
		Value::None
		| Value::Null
		| Value::Bool(_)
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
		| Value::Array(_)
		| Value::Object(_)
		| Value::Set(_)
		| Value::Time(_)
		| Value::Interval(_) => None,
	}
}

fn date_text_of(p: &DatetimeParts) -> String {
	format!("{:04}-{:02}-{:02}", p.year, p.month, p.day)
}

fn datetime_text_of(p: &DatetimeParts) -> String {
	let mut out = format!(
		"{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
		p.year, p.month, p.day, p.hour, p.minute, p.second
	);
	if p.micros != 0 {
		out.push_str(&format!(".{:06}", p.micros));
	}
	out
}

/// The sign, whole seconds and microseconds of a value in a `TIME` column, which holds a signed
/// duration up to `838:59:59` either way: a duration, a time of day, the time of day of a
/// timestamp or datetime, or an interval (a month counts as 30 days, as an interval compares).
/// An interval beyond the limit is the limit.
fn time_value_parts(val: &Value) -> Option<(bool, u64, u32)> {
	let clamp = |negative: bool, total_micros: u128| {
		let secs = total_micros / 1_000_000;
		if secs >= u128::from(MAX_TIME_SECS) {
			(negative, MAX_TIME_SECS, 0)
		} else {
			(negative, secs as u64, (total_micros % 1_000_000) as u32)
		}
	};
	match val {
		Value::Duration(d) => {
			let (secs, micros) = time_parts(d);
			Some((false, secs, micros))
		}
		Value::Datetime(dt) => datetime_parts(dt).map(|p| {
			(
				false,
				u64::from(p.hour) * 3600 + u64::from(p.minute) * 60 + u64::from(p.second),
				p.micros,
			)
		}),
		Value::Timestamp(t) => {
			let micros =
				t.seconds().rem_euclid(86_400) as u128 * 1_000_000 + u128::from(t.nanos() / 1_000);
			Some(clamp(false, micros))
		}
		Value::Time(t) => Some(clamp(false, u128::from(t.nanos() / 1_000))),
		Value::Interval(i) => {
			let total = i.normalised_micros();
			Some(clamp(total < 0, total.unsigned_abs()))
		}
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
		| Value::Set(_)
		| Value::Date(_) => None,
	}
}

/// The `TIME` text form of a value in a `TIME` column, with a `-` for a negative one.
fn time_value_text(val: &Value) -> Option<String> {
	let (negative, secs, micros) = time_value_parts(val)?;
	let mut out = format!(
		"{}{:02}:{:02}:{:02}",
		if negative {
			"-"
		} else {
			""
		},
		secs / 3600,
		(secs % 3600) / 60,
		secs % 60
	);
	if micros != 0 {
		out.push_str(&format!(".{micros:06}"));
	}
	Some(out)
}

/// The days after 1970-01-01 of a proleptic Gregorian date, or `None` when the month or day is
/// not one the calendar has.
fn days_from_civil(year: i64, month: u8, day: u8) -> Option<i64> {
	if !(1..=12).contains(&month) || day == 0 {
		return None;
	}
	let shifted_year = year - i64::from(month <= 2);
	let era = shifted_year.div_euclid(400);
	let year_of_era = shifted_year.rem_euclid(400);
	let month_from_march = (i64::from(month) + 9) % 12;
	let day_of_year = (153 * month_from_march + 2) / 5 + i64::from(day) - 1;
	let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
	let days = era * 146_097 + day_of_era - 719_468;
	(civil_date(days) == (year, month, day)).then_some(days)
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

/// The JSON document of `value`: a string is a quoted JSON string and a boolean `true` or
/// `false`, as a `JSON` column holds them.
fn json_text(value: &Value) -> String {
	let mut json = Vec::new();
	value.write_sql_json(&mut json);
	String::from_utf8_lossy(&json).into_owned()
}

/// The text of `value` in a column MySQL types `col_type`: a datetime in a `DATE` column is its
/// date and in a `TIME` column its time of day, any value in a `JSON` column is its JSON document;
/// every other value is its usual text.
fn typed_text(col_type: u8, value: &Value) -> Option<String> {
	if let Some(year) = crate::cells::year_cell(col_type, value) {
		return Some(year);
	}
	match (col_type, value) {
		(MYSQL_TYPE_JSON, Value::None | Value::Null) => None,
		(MYSQL_TYPE_JSON, v) => Some(json_text(v)),
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
		(MYSQL_TYPE_DATE, Value::Date(_) | Value::Timestamp(_)) => {
			temporal_parts(value).map(|p| date_text_of(&p))
		}
		(MYSQL_TYPE_TIME, Value::Time(_) | Value::Timestamp(_) | Value::Interval(_)) => {
			time_value_text(value)
		}
		_ => value_to_text(value),
	}
}

/// The text of a `SET` value: its members joined by commas in the declaration order of `labels`,
/// which is how MySQL reads a set back. `None` for `NULL`, and for a value that is no collection
/// of labels.
fn member_list_text(labels: &[String], value: &Value) -> Option<String> {
	fn label(value: &Value) -> Option<&str> {
		if let Value::String(s) = value {
			Some(s.as_str())
		} else {
			None
		}
	}
	let held: Vec<&str> = if let Value::Array(items) = value {
		items.iter().filter_map(label).collect()
	} else if let Value::Set(items) = value {
		items.iter().filter_map(label).collect()
	} else if matches!(value, Value::None | Value::Null) {
		return None;
	} else {
		return value_to_text(value);
	};
	let mut members: Vec<&str> =
		labels.iter().map(String::as_str).filter(|label| held.contains(label)).collect();
	members.dedup();
	Some(members.join(","))
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
					Wide::BigDecimal(d) => Some(d.to_string()),
				}
			}
		}
		Value::String(s) => Some(s.clone()),
		Value::Datetime(dt) => Some(datetime_text(dt)),
		Value::Duration(d) => Some(time_text(d)),
		Value::Date(_) => temporal_parts(value).map(|p| date_text_of(&p)),
		Value::Timestamp(_) => temporal_parts(value).map(|p| datetime_text_of(&p)),
		Value::Time(_) | Value::Interval(_) => time_value_text(value),
		Value::Uuid(u) => Some(u.to_string()),
		v @ (Value::RecordId(_)
		| Value::Range(_)
		| Value::Regex(_)
		| Value::File(_)
		| Value::Table(_)) => Some(v.to_sql()),
		Value::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
		v @ (Value::Object(_) | Value::Array(_) | Value::Set(_) | Value::Geometry(_)) => {
			Some(json_text(v))
		}
	}
}

/// Structure holding the reshaped tabular result ready for wire transmission.
pub struct ResultShape {
	pub columns: Vec<ColumnDefinition41>,
	pub rows: Vec<Vec<Option<String>>>,
	pub raw_rows: Vec<Vec<Value>>,
}

/// The longest name, in bytes, that MySQL reports for a column that has no alias.
const MAX_UNALIASED_COLUMN_NAME: usize = 255;

/// Name each unaliased expression column of `descriptor` as MySQL names it, so that a client
/// reading a result by name finds `COUNT(*)` and `a + b` where it wrote them.
///
/// `select` is the statement `descriptor` describes and `sql` the text it was parsed from.
/// MySQL names such a column with the text of the expression as it was written, comments,
/// spacing and letter case included, cut to 255 bytes at a character boundary. Five kinds of
/// expression have a name of their own, which parentheses and a leading `+` leave alone: a
/// column reference is named by its identifier as written, a number or a `?` placeholder by its
/// own text, `NULL` by `NULL`, and a string by its value without the leading whitespace.
///
/// Only the names shown to the client are set; the field each column's values are read from
/// (`ResultColumn::key`) is not touched. The descriptor is returned as it is when its columns
/// do not line up with the select list, which is the case once a wildcard has been expanded.
pub fn name_columns_as_written(
	mut descriptor: ResultDescriptor,
	select: &SelectStmt,
	sql: &str,
) -> ResultDescriptor {
	if descriptor.columns.len() != select.projection.len() {
		return descriptor;
	}
	for (column, item) in descriptor.columns.iter_mut().zip(&select.projection) {
		let SelectItem::Expr {
			expr,
			alias: None,
			span,
		} = item
		else {
			continue;
		};
		if let Some(name) = written_name(expr, *span, sql) {
			column.name = name;
		}
	}
	descriptor
}

/// The name MySQL gives the unaliased select item `expr`, written at `item` in `sql`, or `None`
/// when `item` is not a range of `sql`.
fn written_name(expr: &Expr, item: Span, sql: &str) -> Option<String> {
	let mut atom = expr;
	while let Expr::UnaryOp {
		op: UnaryOperator::Plus,
		expr: operand,
		..
	} = atom
	{
		atom = operand;
	}
	let text = |span: Span| sql.get(span.start..span.end);
	let name = if let Expr::ColumnRef(column) = atom {
		column.fields.last()?.as_str()
	} else if let Expr::Literal(Literal::Null, _) = atom {
		"NULL"
	} else if let Expr::Literal(
		Literal::String(value)
		| Literal::IntroducedString {
			value,
			..
		},
		_,
	) = atom
	{
		value.trim_start_matches([' ', '\t', '\n', '\r', '\x0b', '\x0c'])
	} else if let Expr::Literal(
		Literal::Integer(0..) | Literal::Unsigned(_) | Literal::Float(_) | Literal::Decimal(_),
		span,
	)
	| Expr::Param(_, span) = atom
	{
		// A negative integer is the folded minimum 64-bit integer, which is written with its
		// minus and so has no name of its own.
		text(*span)?
	} else {
		text(item)?
	};
	Some(name[..name.floor_char_boundary(MAX_UNALIASED_COLUMN_NAME)].to_string())
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
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
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
	// The MySQL type each column is declared as, read in this dialect.
	let reports: Vec<Option<MySqlReportedType>> = descriptor
		.columns
		.iter()
		.map(|c| {
			c.declared
				.as_ref()
				.filter(|_| c.enum_type.is_none() && c.composite.is_none())
				.and_then(mysql_reported_type)
		})
		.collect();
	let columns: Vec<ColumnDefinition41> = descriptor
		.columns
		.iter()
		.zip(&reports)
		.map(|(c, report)| {
			let tbl = c.table.as_deref().unwrap_or(table);
			let mut column = column_definition(schema, tbl, c);
			// MySQL announces an `ENUM` or `SET` column as a string, told apart by its flag; a
			// client decodes the cells by the string type and its character set. An array of
			// labels is no `SET`: it keeps its order and its repeats, and is announced as its
			// cells are.
			match &c.enum_type {
				Some(surrealdb_sqlplan::ResultEnum {
					shape: surrealdb_sqlplan::EnumShape::Scalar,
					..
				}) => {
					column.column_type = MYSQL_TYPE_STRING;
					column.flags |= ENUM_FLAG;
				}
				Some(surrealdb_sqlplan::ResultEnum {
					shape: surrealdb_sqlplan::EnumShape::Set,
					..
				}) => {
					column.column_type = MYSQL_TYPE_STRING;
					column.flags |= SET_FLAG;
				}
				Some(surrealdb_sqlplan::ResultEnum {
					shape: surrealdb_sqlplan::EnumShape::Array,
					..
				})
				| None => {}
			}
			if c.ty.mysql_unsigned() {
				column.flags |= UNSIGNED_FLAG;
			}
			if let Some(report) = report {
				declare_column(&mut column, report);
			}
			column
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
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
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
				// A column the planner keyed is read from that field alone: another column may
				// share its name, and a missing field is an absent value, not a hint to read a
				// neighbour.
				let val = match &col.key {
					Some(key) => obj.get(key),
					None => obj
						.get(&col.name)
						.or_else(|| col.original_column.as_ref().and_then(|orig| obj.get(orig)))
						.or_else(|| {
							if obj.len() == descriptor.columns.len() {
								obj.values().nth(i)
							} else {
								None
							}
						}),
				};
				let col_type = columns[i].column_type;
				let members = col
					.enum_type
					.as_ref()
					.filter(|held| held.shape == surrealdb_sqlplan::EnumShape::Set);
				let text = match (members, val) {
					(Some(held), Some(v)) => member_list_text(&held.labels, v),
					(Some(_), None) | (None, _) => val.and_then(|v| typed_text(col_type, v)),
				};
				let text = match (&reports[i], text) {
					(Some(report), Some(text)) => Some(declared_text(report, text)),
					(_, text) => text,
				};
				// The binary protocol sends a set's cell as the text of its members.
				raw_row.push(match (members, &text) {
					(Some(_), Some(text)) => Value::String(text.clone()),
					(Some(_), None) | (None, _) => val.cloned().unwrap_or(Value::None),
				});
				row.push(text);
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

/// The definition of a result column the planner described: its MySQL type and flags, and for a
/// `YEAR`, `BIT(n)`, or spatial column the length, flags, and SRID MySQL announces it with.
pub fn column_definition(
	schema: &str,
	table: &str,
	column: &surrealdb_sqlplan::ResultColumn,
) -> ColumnDefinition41 {
	let mut definition =
		ColumnDefinition41::new(schema, table, &column.name, column.ty.to_mysql_type());
	if column.ty.mysql_unsigned() {
		definition.flags |= UNSIGNED_FLAG;
	}
	if column.ty == surrealdb_sqlplan::SqlType::Year {
		definition.column_length = 4;
		definition.flags |= ZEROFILL_FLAG;
	}
	if let surrealdb_sqlplan::SqlType::MySqlBit(width) = column.ty {
		definition.column_length = u32::from(width);
		definition.character_set = CHARSET_BINARY;
		definition.flags |= BINARY_FLAG;
	}
	if let surrealdb_sqlplan::SqlType::Geometry(srid) = column.ty {
		definition.srid = srid;
		definition.column_length = u32::MAX;
		definition.character_set = CHARSET_BINARY;
		definition.flags |= BINARY_FLAG | BLOB_FLAG;
	}
	definition
}

/// Give `column` the type, length, decimals and flags of the declared type `report`.
fn declare_column(column: &mut ColumnDefinition41, report: &MySqlReportedType) {
	column.column_type = report.type_code;
	column.column_length = report.length;
	column.decimals = report.decimals;
	column.flags &= !(UNSIGNED_FLAG | ZEROFILL_FLAG | BINARY_FLAG | BLOB_FLAG);
	if report.unsigned {
		column.flags |= UNSIGNED_FLAG;
	}
	if report.zerofill {
		column.flags |= ZEROFILL_FLAG;
	}
	if report.binary {
		column.flags |= BINARY_FLAG;
		column.character_set = CHARSET_BINARY;
	}
	if report.blob {
		column.flags |= BLOB_FLAG;
	}
	if let Some(srid) = report.srid {
		column.srid = srid;
	}
}

/// The text of a value of a column declared `report`, given its usual `text`: a `YEAR` has four
/// digits, and a temporal type with fractional seconds has exactly that many digits after the
/// point, as MySQL writes them.
fn declared_text(report: &MySqlReportedType, text: String) -> String {
	match report.type_code {
		MYSQL_TYPE_YEAR => match text.parse::<u16>() {
			Ok(year) => crate::codec::year::year_text(year),
			Err(_) => text,
		},
		MYSQL_TYPE_DATETIME | MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIME => {
			with_fraction_digits(text, usize::from(report.decimals))
		}
		MYSQL_TYPE_NEWDECIMAL => with_scale_digits(text, usize::from(report.decimals)),
		_ => text,
	}
}

/// `text`, a plain decimal, with at least `scale` digits after the point, as a `DECIMAL(p,s)`
/// value is written. A fraction longer than `scale` is kept, and any other text is returned as
/// it is.
fn with_scale_digits(text: String, scale: usize) -> String {
	let digits = text.strip_prefix('-').unwrap_or(&text);
	let (whole, fraction) = digits.split_once('.').unwrap_or((digits, ""));
	if scale == 0
		|| fraction.len() >= scale
		|| whole.is_empty()
		|| !whole.bytes().chain(fraction.bytes()).all(|b| b.is_ascii_digit())
	{
		return text;
	}
	let mut out = String::with_capacity(text.len() + 1 + scale);
	out.push_str(&text);
	if fraction.is_empty() && !text.contains('.') {
		out.push('.');
	}
	out.extend(std::iter::repeat_n('0', scale - fraction.len()));
	out
}

/// `text`, a time or a datetime, with exactly `digits` fractional-second digits: the fraction
/// it has is cut or padded with zeros, and none is written for zero digits.
fn with_fraction_digits(text: String, digits: usize) -> String {
	let Some(colon) = text.rfind(':') else {
		return text;
	};
	let (whole, fraction) = match text[colon..].find('.') {
		Some(dot) => (&text[..colon + dot], &text[colon + dot + 1..]),
		None => (text.as_str(), ""),
	};
	if !fraction.bytes().all(|b| b.is_ascii_digit()) {
		return text;
	}
	if digits == 0 {
		return whole.to_owned();
	}
	let mut out = String::with_capacity(whole.len() + 1 + digits);
	out.push_str(whole);
	out.push('.');
	out.extend(fraction.chars().chain(std::iter::repeat('0')).take(digits));
	out
}

/// The bytes a cell of `column` travels as in both protocols when its type is sent as bytes
/// rather than text: a `BIT(n)` value as its `ceil(n / 8)` big-endian bytes, a spatial value as
/// its SRID and WKB, and a byte string in a string or blob column as its bytes unchanged, since
/// text would replace any byte sequence that is not UTF-8. `None` for every other column, and for
/// a value of another kind, which travels as its text.
pub(crate) fn raw_cell(column: &ColumnDefinition41, value: &Value) -> Option<Vec<u8>> {
	match (column.column_type, value) {
		(
			MYSQL_TYPE_TINY_BLOB
			| MYSQL_TYPE_MEDIUM_BLOB
			| MYSQL_TYPE_LONG_BLOB
			| MYSQL_TYPE_BLOB
			| MYSQL_TYPE_VAR_STRING
			| MYSQL_TYPE_STRING
			| MYSQL_TYPE_VARCHAR,
			Value::Bytes(bytes),
		) => Some(bytes.to_vec()),
		(MYSQL_TYPE_BIT, Value::Number(number)) => {
			let bits = if let Number::U64(n) = number {
				*n
			} else {
				match number.wide() {
					Wide::Int(i) => u64::try_from(i).ok()?,
					Wide::Float(_) | Wide::Decimal(_) | Wide::BigDecimal(_) => return None,
				}
			};
			crate::codec::bit::bit_bytes(bits, column.column_length)
		}
		(MYSQL_TYPE_GEOMETRY, Value::Geometry(geometry)) => {
			Some(crate::codec::geometry::encode(column.srid, &wkb_of(geometry)))
		}
		_ => None,
	}
}

/// A geometry value as the plain coordinates of its WKB.
fn wkb_of(geometry: &surrealdb_types::Geometry) -> Wkb {
	use surrealdb_types::Geometry;
	// The coordinates of a line string, and the rings of a polygon, exterior first.
	macro_rules! ring {
		($line:expr) => {
			$line.coords().map(|c| (c.x, c.y)).collect::<Vec<(f64, f64)>>()
		};
	}
	macro_rules! rings {
		($polygon:expr) => {{
			let polygon = $polygon;
			let mut rings = vec![ring!(polygon.exterior())];
			rings.extend(polygon.interiors().iter().map(|interior| ring!(interior)));
			rings
		}};
	}
	match geometry {
		Geometry::Point(point) => Wkb::Point((point.x(), point.y())),
		Geometry::Line(line) => Wkb::LineString(ring!(line)),
		Geometry::Polygon(polygon) => Wkb::Polygon(rings!(polygon)),
		Geometry::MultiPoint(points) => {
			Wkb::MultiPoint(points.0.iter().map(|point| (point.x(), point.y())).collect())
		}
		Geometry::MultiLine(lines) => {
			Wkb::MultiLineString(lines.0.iter().map(|line| ring!(line)).collect())
		}
		Geometry::MultiPolygon(polygons) => {
			Wkb::MultiPolygon(polygons.0.iter().map(|polygon| rings!(polygon)).collect())
		}
		Geometry::Collection(members) => {
			Wkb::GeometryCollection(members.iter().map(wkb_of).collect())
		}
	}
}

/// Encode one text row of a result: each cell as its text, except a cell of a `BIT` or a spatial
/// column, which travels as its bytes ([`raw_cell`]). `raw_row` holds the values of the cells.
pub fn encode_shape_text_row(
	columns: &[ColumnDefinition41],
	row: &[Option<String>],
	raw_row: &[Value],
) -> Vec<u8> {
	let mut buf = Vec::with_capacity(row.len() * 16);
	for (index, cell) in row.iter().enumerate() {
		let raw = columns
			.get(index)
			.zip(raw_row.get(index))
			.and_then(|(column, value)| raw_cell(column, value));
		match (cell, raw) {
			(None, _) => buf.push(NULL_VALUE),
			(Some(_), Some(bytes)) => write_lenenc_bytes(&mut buf, &bytes),
			(Some(text), None) => write_lenenc_str(&mut buf, text),
		}
	}
	buf
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
		if let Some(bytes) = columns.get(i).and_then(|column| raw_cell(column, val)) {
			write_lenenc_bytes(&mut buf, &bytes);
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
					Wide::Float(_) | Wide::Decimal(_) | Wide::BigDecimal(_) => 0u8,
				},
				Value::None
				| Value::Null
				| Value::String(_)
				| Value::Bytes(_)
				| Value::Duration(_)
				| Value::Datetime(_)
				| Value::Date(_)
				| Value::Time(_)
				| Value::Timestamp(_)
				| Value::Interval(_)
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
					Wide::Float(_) | Wide::Decimal(_) | Wide::BigDecimal(_) => 0i16,
				},
				Value::None
				| Value::Null
				| Value::Bool(_)
				| Value::String(_)
				| Value::Bytes(_)
				| Value::Duration(_)
				| Value::Datetime(_)
				| Value::Date(_)
				| Value::Time(_)
				| Value::Timestamp(_)
				| Value::Interval(_)
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
					Wide::Float(_) | Wide::Decimal(_) | Wide::BigDecimal(_) => 0i32,
				},
				Value::None
				| Value::Null
				| Value::Bool(_)
				| Value::String(_)
				| Value::Bytes(_)
				| Value::Duration(_)
				| Value::Datetime(_)
				| Value::Date(_)
				| Value::Time(_)
				| Value::Timestamp(_)
				| Value::Interval(_)
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
				// A `u64` beyond `i64::MAX` travels as the bits of the same eight bytes, which an
				// `UNSIGNED` LONGLONG reads back as that value.
				Value::Number(Number::U64(v)) => *v as i64,
				Value::Number(n) => match n.wide() {
					Wide::Int(i) => i,
					Wide::Float(_) | Wide::Decimal(_) | Wide::BigDecimal(_) => 0i64,
				},
				Value::None
				| Value::Null
				| Value::Bool(_)
				| Value::String(_)
				| Value::Bytes(_)
				| Value::Duration(_)
				| Value::Datetime(_)
				| Value::Date(_)
				| Value::Time(_)
				| Value::Timestamp(_)
				| Value::Interval(_)
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
				Value::Number(n) => n.to_f64().unwrap_or(f64::NAN) as f32,
				Value::None
				| Value::Null
				| Value::Bool(_)
				| Value::String(_)
				| Value::Bytes(_)
				| Value::Duration(_)
				| Value::Datetime(_)
				| Value::Date(_)
				| Value::Time(_)
				| Value::Timestamp(_)
				| Value::Interval(_)
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
				Value::Number(n) => n.to_f64().unwrap_or(f64::NAN),
				Value::None
				| Value::Null
				| Value::Bool(_)
				| Value::String(_)
				| Value::Bytes(_)
				| Value::Duration(_)
				| Value::Datetime(_)
				| Value::Date(_)
				| Value::Time(_)
				| Value::Timestamp(_)
				| Value::Interval(_)
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
			if let Some(text) = typed_text(col_type, val) {
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
		Value::Date(_) | Value::Timestamp(_) => temporal_parts(val),
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
		| Value::Time(_)
		| Value::Interval(_)
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
	let (negative, secs, micros) = time_value_parts(val).unwrap_or((false, 0, 0));
	if secs == 0 && micros == 0 {
		buf.push(0);
		return;
	}
	buf.push(if micros != 0 {
		12
	} else {
		8
	});
	buf.push(u8::from(negative));
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
				// An unsigned value beyond `i64::MAX` keeps its magnitude as a `u64`.
				let v = u64::from_le_bytes(b);
				Ok(Value::Number(i64::try_from(v).map_or(Number::U64(v), Number::Int)))
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
				return Ok(Value::String(if type_code == MYSQL_TYPE_DATE {
					"0000-00-00".to_string()
				} else {
					"0000-00-00 00:00:00".to_string()
				}));
			}
			// The protocol sends 4 (date), 7 (date and time) or 11 (with microseconds) bytes. A
			// `DATE` keeps the date of the longer forms, as MySQL does when it stores a datetime
			// in a `DATE`.
			ensure!(matches!(len, 4 | 7 | 11), "invalid binary datetime length {len}");
			let year = i64::from(u16::from_le_bytes([data[0], data[1]]));
			let (month, day) = (data[2], data[3]);
			let (hour, min, sec) = if len >= 7 {
				(i64::from(data[4]), i64::from(data[5]), i64::from(data[6]))
			} else {
				(0, 0, 0)
			};
			let micro = if len >= 11 {
				u32::from_le_bytes([data[7], data[8], data[9], data[10]])
			} else {
				0
			};
			// A year past 9999 or a time of day out of its range is refused. A date MySQL admits
			// and the calendar does not (a zero month or day) stays text, as MySQL writes the
			// parameter's type: a `DATE` as a date alone, a value sent with microseconds with
			// all six of them.
			ensure!(year <= 9999, "invalid binary year {year}: MySQL holds years 0000 to 9999");
			ensure!(
				hour < 24 && min < 60 && sec < 60 && micro < 1_000_000,
				"invalid binary time of day {hour:02}:{min:02}:{sec:02}.{micro:06}"
			);
			let Some(days) = days_from_civil(year, month, day) else {
				let mut text = format!("{year:04}-{month:02}-{day:02}");
				if type_code != MYSQL_TYPE_DATE {
					text.push_str(&format!(" {hour:02}:{min:02}:{sec:02}"));
				}
				if len == 11 && type_code != MYSQL_TYPE_DATE {
					text.push_str(&format!(".{micro:06}"));
				}
				return Ok(Value::String(text));
			};
			let seconds = days * 86_400 + hour * 3600 + min * 60 + sec;
			let out_of_range = || anyhow::anyhow!("datetime out of range");
			Ok(match type_code {
				MYSQL_TYPE_DATE => Value::Date(
					i32::try_from(days)
						.ok()
						.and_then(surrealdb_types::Date::from_finite_days)
						.ok_or_else(out_of_range)?,
				),
				MYSQL_TYPE_DATETIME => Value::Timestamp(
					surrealdb_types::Timestamp::from_finite_parts(seconds, micro * 1_000)
						.ok_or_else(out_of_range)?,
				),
				_ => Value::Datetime(
					surrealdb_types::Datetime::from_timestamp(seconds, micro * 1_000)
						.ok_or_else(out_of_range)?,
				),
			})
		}
		MYSQL_TYPE_TIME => {
			ensure!(!cursor.is_empty(), "unexpected EOF reading time length");
			let len = cursor[0] as usize;
			*cursor = &cursor[1..];
			ensure!(cursor.len() >= len, "unexpected EOF reading time body");
			let data = &cursor[..len];
			*cursor = &cursor[len..];
			// The protocol sends 0 (zero), 8 (no microseconds) or 12 bytes.
			ensure!(matches!(len, 0 | 8 | 12), "invalid binary time length {len}");
			if len == 0 {
				return Ok(Value::Interval(surrealdb_types::Interval::ZERO));
			}
			// Minutes, seconds and microseconds within their unit, and the whole within the
			// `838:59:59` a `TIME` holds, before anything is scaled: the day count alone can
			// overflow the micros. The hours may pass 23, as a client that writes the whole
			// duration in them sends it.
			ensure!(data[0] <= 1, "invalid binary time sign {}", data[0]);
			let negative = data[0] == 1;
			let days = u64::from(u32::from_le_bytes([data[1], data[2], data[3], data[4]]));
			let (hours, minutes, seconds) = (data[5], data[6], data[7]);
			let micro = if len >= 12 {
				u32::from_le_bytes([data[8], data[9], data[10], data[11]])
			} else {
				0
			};
			ensure!(
				minutes < 60 && seconds < 60 && micro < 1_000_000,
				"time parameter field out of range"
			);
			let secs = days
				.checked_mul(86_400)
				.map(|d| d + u64::from(hours) * 3600 + u64::from(minutes) * 60 + u64::from(seconds))
				.filter(|secs| *secs < MAX_TIME_SECS || (*secs == MAX_TIME_SECS && micro == 0))
				.ok_or_else(|| anyhow::anyhow!("time parameter beyond 838:59:59"))?;
			let total = secs as i64 * 1_000_000 + i64::from(micro);
			Ok(Value::Interval(surrealdb_types::Interval::new(
				0,
				0,
				if negative {
					-total
				} else {
					total
				},
			)))
		}
		_ => {
			// Every other type is a length-encoded string of bytes.
			let bytes = read_lenenc_bytes(cursor).unwrap_or(&[]);
			decode_string_param(bytes, type_code)
		}
	}
}

/// The value of a parameter that travels as the bytes `bytes`, read as its declared type
/// `type_code`, never by what the bytes happen to look like.
///
/// A blob is always bytes; a `DECIMAL` is the number its text spells; a `BIT` and a spatial value
/// are read from their binary forms. Any other type is text, except that bytes which are not
/// UTF-8 stay bytes, since a client sends a binary value under a string type.
pub fn decode_string_param(bytes: &[u8], type_code: u8) -> Result<Value> {
	match type_code {
		MYSQL_TYPE_BIT => crate::cells::bit_param(bytes),
		MYSQL_TYPE_GEOMETRY => crate::cells::geometry_param(bytes),
		MYSQL_TYPE_TINY_BLOB | MYSQL_TYPE_MEDIUM_BLOB | MYSQL_TYPE_LONG_BLOB | MYSQL_TYPE_BLOB => {
			Ok(Value::Bytes(bytes.to_vec().into()))
		}
		MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL => {
			let text = std::str::from_utf8(bytes)
				.map_err(|_| anyhow::anyhow!("a DECIMAL parameter is not text"))?
				.trim();
			if let Ok(decimal) = text.parse::<surrealdb_types::Decimal>() {
				Ok(Value::Number(Number::Decimal(decimal)))
			} else if let Ok(big) = text.parse::<surrealdb_types::BigDecimal>() {
				Ok(Value::Number(Number::BigDecimal(Box::new(big))))
			} else {
				Err(anyhow::anyhow!("invalid DECIMAL parameter {text:?}"))
			}
		}
		_ => Ok(match std::str::from_utf8(bytes) {
			Ok(text) => Value::String(text.to_owned()),
			Err(_) => Value::Bytes(bytes.to_vec().into()),
		}),
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_sqlplan::declared_type::DeclaredSqlType;

	use super::*;

	#[test]
	fn a_u64_beyond_the_signed_range_is_sent_as_the_bits_an_unsigned_longlong_reads() {
		let value = Value::Number(Number::U64(u64::MAX));
		let mut buf = Vec::new();
		encode_binary_value(&mut buf, MYSQL_TYPE_LONGLONG, &value);
		assert_eq!(buf, u64::MAX.to_le_bytes());
		let back = decode_binary_param(&mut &buf[..], MYSQL_TYPE_LONGLONG, true).unwrap();
		assert_eq!(back, value);
		assert_eq!(value_to_text(&value).as_deref(), Some("18446744073709551615"));
	}

	#[test]
	fn a_u64_at_the_signed_boundary_is_sent_as_the_same_bits_in_both_protocols() {
		let boundary = i64::MAX as u64 + 1;
		let value = Value::Number(Number::U64(boundary));
		let mut buf = Vec::new();
		encode_binary_value(&mut buf, MYSQL_TYPE_LONGLONG, &value);
		assert_eq!(buf, boundary.to_le_bytes());
		assert_eq!(value_to_text(&value).as_deref(), Some("9223372036854775808"));
	}

	#[test]
	fn a_declared_u64_column_is_announced_as_unsigned_longlong() {
		let descriptor = surrealdb_sqlplan::ResultDescriptor {
			columns: vec![surrealdb_sqlplan::ResultColumn {
				name: "v".to_string(),
				ty: surrealdb_sqlplan::SqlType::UInt8,
				..surrealdb_sqlplan::ResultColumn::default()
			}],
		};
		let mut obj = surrealdb_types::Object::new();
		obj.insert("v".to_string(), Value::Number(Number::U64(u64::MAX)));
		let shape = shape_result_with_descriptor(&descriptor, "test", "rs", Value::Object(obj));
		let column = &shape.columns[0];
		assert_eq!(column.column_type, MYSQL_TYPE_LONGLONG);
		assert_ne!(column.flags & UNSIGNED_FLAG, 0);
		assert_eq!(shape.rows[0][0].as_deref(), Some("18446744073709551615"));
	}

	#[test]
	fn a_declared_signed_column_is_not_announced_unsigned() {
		let descriptor = surrealdb_sqlplan::ResultDescriptor {
			columns: vec![surrealdb_sqlplan::ResultColumn {
				name: "v".to_string(),
				ty: surrealdb_sqlplan::SqlType::Int8,
				..surrealdb_sqlplan::ResultColumn::default()
			}],
		};
		let mut obj = surrealdb_types::Object::new();
		obj.insert("v".to_string(), Value::Number(Number::Int(-1)));
		let shape = shape_result_with_descriptor(&descriptor, "test", "rs", Value::Object(obj));
		assert_eq!(shape.columns[0].column_type, MYSQL_TYPE_LONGLONG);
		assert_eq!(shape.columns[0].flags & UNSIGNED_FLAG, 0);
	}

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
	fn a_shaped_result_keeps_the_temporal_kinds() {
		let mut obj = surrealdb_types::Object::new();
		obj.insert("d".to_string(), date("2024-01-15"));
		obj.insert("i".to_string(), interval(90_000_000));
		obj.insert("t".to_string(), timestamp("2024-01-15T12:30:45"));
		let shape = shape_result("test", "items", Value::Array(vec![Value::Object(obj)].into()));
		let types: Vec<u8> = shape.columns.iter().map(|c| c.column_type).collect();
		assert_eq!(types, vec![MYSQL_TYPE_DATE, MYSQL_TYPE_TIME, MYSQL_TYPE_DATETIME]);
		assert_eq!(shape.rows[0][0].as_deref(), Some("2024-01-15"));
		assert_eq!(shape.rows[0][2].as_deref(), Some("2024-01-15 12:30:45"));
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

	#[test]
	fn unsigned_longlong_param_keeps_its_magnitude() {
		let decode = |v: u64| {
			let data = v.to_le_bytes();
			decode_binary_param(&mut &data[..], MYSQL_TYPE_LONGLONG, true).unwrap()
		};
		assert_eq!(decode(7), Value::Number(Number::Int(7)));
		assert_eq!(decode(i64::MAX as u64), Value::Number(Number::Int(i64::MAX)));
		assert_eq!(decode(u64::MAX), Value::Number(Number::U64(u64::MAX)));
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
	fn a_time_parameter_beyond_what_mysql_holds_is_refused() {
		let decode = |body: &[u8]| {
			let mut data = vec![body.len() as u8];
			data.extend_from_slice(body);
			decode_binary_param(&mut &data[..], MYSQL_TYPE_TIME, false)
		};
		// 838:59:59 either way is the longest a `TIME` holds.
		let micros = (838 * 3600 + 59 * 60 + 59) * 1_000_000;
		assert_eq!(
			decode(&[1, 34, 0, 0, 0, 22, 59, 59]).unwrap(),
			Value::Interval(surrealdb_types::Interval::new(0, 0, -micros))
		);
		// A second, or a microsecond, past it.
		assert!(decode(&[0, 34, 0, 0, 0, 23, 0, 0]).is_err());
		assert!(decode(&[0, 34, 0, 0, 0, 22, 59, 59, 1, 0, 0, 0]).is_err());
		// A day count whose micros overflow an `i64`.
		assert!(decode(&[0, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0]).is_err());
		// A field past its unit; the hours alone may pass 23, within the range of a `TIME`.
		assert_eq!(
			decode(&[0, 0, 0, 0, 0, 200, 0, 0]).unwrap(),
			Value::Interval(surrealdb_types::Interval::new(0, 0, 200 * 3_600_000_000))
		);
		assert!(decode(&[0, 34, 0, 0, 0, 24, 0, 0]).is_err());
		assert!(decode(&[0, 0, 0, 0, 0, 0, 60, 0]).is_err());
		assert!(decode(&[0, 0, 0, 0, 0, 0, 0, 0, 0x40, 0x42, 0x0F, 0]).is_err());
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

	fn date(text: &str) -> Value {
		Value::Date(text.parse::<surrealdb_types::Date>().unwrap())
	}

	fn timestamp(text: &str) -> Value {
		Value::Timestamp(text.parse::<surrealdb_types::Timestamp>().unwrap())
	}

	fn interval(micros: i64) -> Value {
		Value::Interval(surrealdb_types::Interval::new(0, 0, micros))
	}

	#[test]
	fn the_temporal_values_have_their_own_wire_types() {
		assert_eq!(infer_mysql_type(&date("2026-10-27")), MYSQL_TYPE_DATE);
		assert_eq!(infer_mysql_type(&timestamp("2026-10-27T12:30:00")), MYSQL_TYPE_DATETIME);
		assert_eq!(infer_mysql_type(&interval(1)), MYSQL_TYPE_TIME);
		let at = surrealdb_types::Datetime::from_timestamp(0, 0).unwrap();
		assert_eq!(infer_mysql_type(&Value::Datetime(at)), MYSQL_TYPE_TIMESTAMP);
	}

	#[test]
	fn the_temporal_values_have_mysql_text_forms() {
		assert_eq!(value_to_text(&date("2026-10-27")).as_deref(), Some("2026-10-27"));
		assert_eq!(
			value_to_text(&timestamp("2026-10-27T12:30:00.5")).as_deref(),
			Some("2026-10-27 12:30:00.500000")
		);
		assert_eq!(
			value_to_text(&Value::Time("12:30:00.123456789".parse().unwrap())).as_deref(),
			Some("12:30:00.123456")
		);
		let hours = |h: i64| h * 3_600_000_000;
		assert_eq!(value_to_text(&interval(hours(100))).as_deref(), Some("100:00:00"));
		assert_eq!(
			value_to_text(&interval(-hours(1) - 500_000)).as_deref(),
			Some("-01:00:00.500000")
		);
		// A `TIME` holds no more than 838:59:59 either way.
		assert_eq!(value_to_text(&interval(hours(900))).as_deref(), Some("838:59:59"));
		assert_eq!(value_to_text(&interval(-hours(900))).as_deref(), Some("-838:59:59"));
	}

	#[test]
	fn a_binary_time_carries_its_sign() {
		let mut buf = Vec::new();
		encode_binary_time(&mut buf, &interval(-(26 * 3_600_000_000 + 5_000_000)));
		// Negative, one day, 2 hours, 0 minutes, 5 seconds.
		assert_eq!(buf, vec![8, 1, 1, 0, 0, 0, 2, 0, 5]);
		let mut buf = Vec::new();
		encode_binary_time(&mut buf, &interval(0));
		assert_eq!(buf, vec![0]);
	}

	#[test]
	fn a_binary_date_and_datetime_carry_their_calendar_fields() {
		let mut buf = Vec::new();
		encode_binary_datetime(&mut buf, MYSQL_TYPE_DATE, &date("2024-01-15"));
		assert_eq!(buf, vec![4, 0xe8, 0x07, 1, 15]);
		let mut buf = Vec::new();
		encode_binary_datetime(&mut buf, MYSQL_TYPE_DATETIME, &timestamp("2024-01-15T12:30:45"));
		assert_eq!(buf, vec![7, 0xe8, 0x07, 1, 15, 12, 30, 45]);
	}

	#[test]
	fn a_binary_date_sent_with_a_time_keeps_its_date() {
		for body in [
			&[4u8, 0xe8, 0x07, 1, 15][..],
			&[7, 0xe8, 0x07, 1, 15, 12, 30, 45],
			&[11, 0xe8, 0x07, 1, 15, 0, 0, 0, 0, 0, 0, 0],
		] {
			let mut cursor = body;
			assert_eq!(
				decode_binary_param(&mut cursor, MYSQL_TYPE_DATE, false).unwrap(),
				date("2024-01-15"),
				"{body:?}"
			);
		}
		let mut cursor: &[u8] = &[5, 0xe8, 0x07, 1, 15, 12];
		assert!(decode_binary_param(&mut cursor, MYSQL_TYPE_DATE, false).is_err());
	}

	#[test]
	fn a_string_parameter_is_read_as_its_declared_type() {
		let text = |bytes: &[u8], ty: u8| decode_string_param(bytes, ty).unwrap();
		// Bytes that are valid text stay bytes under a blob type.
		assert_eq!(
			text(b"abc", MYSQL_TYPE_BLOB),
			Value::Bytes(surrealdb_types::Bytes::from(b"abc".to_vec()))
		);
		// Text that reads as a number stays text under a string type.
		assert_eq!(text(b"42", MYSQL_TYPE_VAR_STRING), Value::String("42".to_string()));
		assert_eq!(text(b"{\"a\":1}", MYSQL_TYPE_JSON), Value::String("{\"a\":1}".to_string()));
		// Bytes that are not UTF-8 stay bytes under a string type.
		assert_eq!(
			text(&[0xff, 0xfe], MYSQL_TYPE_STRING),
			Value::Bytes(surrealdb_types::Bytes::from(vec![0xff, 0xfe]))
		);
		// A DECIMAL is the number its text spells.
		assert!(matches!(
			text(b"12.50", MYSQL_TYPE_NEWDECIMAL),
			Value::Number(Number::Decimal(d)) if d.to_string() == "12.50"
		));
		assert!(decode_string_param(b"abc", MYSQL_TYPE_NEWDECIMAL).is_err());
	}

	#[test]
	fn a_byte_string_cell_travels_as_its_bytes() {
		let bytes = Value::Bytes(surrealdb_types::Bytes::from(vec![0xff, 0x00, 0x41]));
		let blob = ColumnDefinition41::new("db", "t", "c", MYSQL_TYPE_BLOB);
		assert_eq!(raw_cell(&blob, &bytes), Some(vec![0xff, 0x00, 0x41]));
		let number = ColumnDefinition41::new("db", "t", "c", MYSQL_TYPE_LONGLONG);
		assert_eq!(raw_cell(&number, &bytes), None);
	}

	#[test]
	fn a_json_column_holds_a_scalar_as_a_json_document() {
		assert_eq!(
			typed_text(MYSQL_TYPE_JSON, &Value::String("a\"b".into())).as_deref(),
			Some("\"a\\\"b\"")
		);
		assert_eq!(typed_text(MYSQL_TYPE_JSON, &Value::Bool(true)).as_deref(), Some("true"));
		assert_eq!(typed_text(MYSQL_TYPE_JSON, &Value::Null), None);
		let mut binary = Vec::new();
		encode_binary_value(&mut binary, MYSQL_TYPE_JSON, &Value::Bool(false));
		let mut expected = Vec::new();
		write_lenenc_str(&mut expected, "false");
		assert_eq!(binary, expected);
	}

	#[test]
	fn a_binary_year_past_9999_is_refused() {
		// 10000 is 0x2710; 9999 (0x270F) is the last year MySQL holds.
		for ty in [MYSQL_TYPE_DATE, MYSQL_TYPE_DATETIME, MYSQL_TYPE_TIMESTAMP] {
			let mut cursor: &[u8] = &[4, 0x10, 0x27, 1, 1];
			assert!(decode_binary_param(&mut cursor, ty, false).is_err(), "{ty}");
			let mut cursor: &[u8] = &[4, 0xFF, 0xFF, 1, 1];
			assert!(decode_binary_param(&mut cursor, ty, false).is_err(), "{ty}");
			let mut cursor: &[u8] = &[4, 0x0F, 0x27, 12, 31];
			assert!(decode_binary_param(&mut cursor, ty, false).is_ok(), "{ty}");
		}
	}

	#[test]
	fn a_binary_time_of_another_length_is_refused() {
		for len in [1u8, 7, 9, 11, 13] {
			let body = [vec![len], vec![0; len as usize]].concat();
			let mut cursor = body.as_slice();
			assert!(decode_binary_param(&mut cursor, MYSQL_TYPE_TIME, false).is_err(), "{len}");
		}
		let mut cursor: &[u8] = &[8, 2, 0, 0, 0, 0, 1, 0, 0];
		assert!(decode_binary_param(&mut cursor, MYSQL_TYPE_TIME, false).is_err(), "sign 2");
		for len in [0u8, 8, 12] {
			let body = [vec![len], vec![0; len as usize]].concat();
			let mut cursor = body.as_slice();
			assert!(decode_binary_param(&mut cursor, MYSQL_TYPE_TIME, false).is_ok(), "{len}");
		}
	}

	#[test]
	fn a_binary_datetime_of_another_length_is_refused() {
		for body in [
			&[1u8, 0xe8][..],
			&[3, 0xe8, 0x07, 1],
			&[5, 0xe8, 0x07, 1, 15, 12],
			&[8, 0xe8, 0x07, 1, 15, 12, 30, 45, 0],
		] {
			let mut cursor = body;
			assert!(
				decode_binary_param(&mut cursor, MYSQL_TYPE_DATETIME, false).is_err(),
				"{body:?}"
			);
		}
	}

	#[test]
	fn binary_parameters_decode_into_the_new_kinds() {
		let mut cursor: &[u8] = &[4, 0xe8, 0x07, 1, 15];
		assert_eq!(
			decode_binary_param(&mut cursor, MYSQL_TYPE_DATE, false).unwrap(),
			date("2024-01-15")
		);
		let mut cursor: &[u8] = &[7, 0xe8, 0x07, 1, 15, 12, 30, 45];
		assert_eq!(
			decode_binary_param(&mut cursor, MYSQL_TYPE_DATETIME, false).unwrap(),
			timestamp("2024-01-15T12:30:45")
		);
		let mut cursor: &[u8] = &[7, 0xe8, 0x07, 1, 15, 12, 30, 45];
		assert!(matches!(
			decode_binary_param(&mut cursor, MYSQL_TYPE_TIMESTAMP, false).unwrap(),
			Value::Datetime(_)
		));
		let mut cursor: &[u8] = &[8, 1, 1, 0, 0, 0, 2, 0, 5];
		assert_eq!(
			decode_binary_param(&mut cursor, MYSQL_TYPE_TIME, false).unwrap(),
			interval(-(26 * 3_600_000_000 + 5_000_000))
		);
		// A zero month is a date the calendar does not have, which stays text written as the
		// parameter's type: a date alone, or a datetime with the microseconds it was sent with.
		let text = |body: &[u8], ty: u8| {
			let mut cursor = body;
			decode_binary_param(&mut cursor, ty, false).unwrap()
		};
		let string = |s: &str| Value::String(s.to_string());
		assert_eq!(text(&[4, 0xe8, 0x07, 0, 0], MYSQL_TYPE_DATE), string("2024-00-00"));
		assert_eq!(text(&[0], MYSQL_TYPE_DATE), string("0000-00-00"));
		assert_eq!(
			text(&[7, 0xe8, 0x07, 0, 0, 12, 0, 0], MYSQL_TYPE_DATETIME),
			string("2024-00-00 12:00:00")
		);
		let micros = 123_456u32.to_le_bytes();
		assert_eq!(
			text(
				&[11, 0xe8, 0x07, 0, 0, 12, 0, 0, micros[0], micros[1], micros[2], micros[3]],
				MYSQL_TYPE_DATETIME
			),
			string("2024-00-00 12:00:00.123456")
		);
		// An impossible time of day is an error, not text, on a real or an incomplete date.
		for body in [
			&[7, 0xe8, 0x07, 1, 1, 25, 0, 0][..],
			&[7, 0xe8, 0x07, 1, 1, 0, 60, 0],
			&[7, 0xe8, 0x07, 0, 0, 0, 0, 60],
			&[11, 0xe8, 0x07, 1, 1, 0, 0, 0, 0x40, 0x42, 0x0f, 0x00],
		] {
			let mut cursor = body;
			assert!(
				decode_binary_param(&mut cursor, MYSQL_TYPE_DATETIME, false).is_err(),
				"{body:?}"
			);
		}
	}
	fn enum_column(
		name: &str,
		shape: surrealdb_sqlplan::EnumShape,
	) -> surrealdb_sqlplan::ResultColumn {
		surrealdb_sqlplan::ResultColumn {
			name: name.to_owned(),
			enum_type: Some(surrealdb_sqlplan::ResultEnum {
				id: None,
				labels: ["small", "medium", "large"].map(str::to_owned).to_vec(),
				shape,
			}),
			..Default::default()
		}
	}

	#[test]
	fn an_enum_column_is_typed_and_flagged_by_its_declaration() {
		let descriptor = surrealdb_sqlplan::ResultDescriptor {
			columns: vec![
				enum_column("size", surrealdb_sqlplan::EnumShape::Scalar),
				enum_column("sizes", surrealdb_sqlplan::EnumShape::Set),
			],
		};
		let row = Value::Object(surrealdb_types::Object::from_iter([
			("size".to_owned(), Value::String("medium".to_owned())),
			(
				"sizes".to_owned(),
				Value::Set(surrealdb_types::Set::from(vec![
					Value::String("large".to_owned()),
					Value::String("small".to_owned()),
				])),
			),
		]));
		let shape =
			shape_result_with_descriptor(&descriptor, "db", "t", Value::Array(vec![row].into()));
		assert_eq!(shape.columns[0].column_type, MYSQL_TYPE_STRING);
		assert_eq!(shape.columns[0].flags & ENUM_FLAG, ENUM_FLAG);
		assert_eq!(shape.columns[1].column_type, MYSQL_TYPE_STRING);
		assert_eq!(shape.columns[1].flags & SET_FLAG, SET_FLAG);
		assert_eq!(shape.rows[0][0].as_deref(), Some("medium"));
		// The members read back in declaration order, not the order the set holds them in.
		assert_eq!(shape.rows[0][1].as_deref(), Some("small,large"));
		assert_eq!(shape.raw_rows[0][1], Value::String("small,large".to_owned()));
	}

	#[test]
	fn an_array_of_labels_keeps_its_order_and_repeats() {
		let descriptor = surrealdb_sqlplan::ResultDescriptor {
			columns: vec![enum_column("sizes", surrealdb_sqlplan::EnumShape::Array)],
		};
		let labels = ["large", "small", "large"].map(|l| Value::String(l.to_owned()));
		let row = Value::Object(surrealdb_types::Object::from_iter([(
			"sizes".to_owned(),
			Value::Array(labels.to_vec().into()),
		)]));
		let shape =
			shape_result_with_descriptor(&descriptor, "db", "t", Value::Array(vec![row].into()));
		assert_eq!(shape.columns[0].flags & (SET_FLAG | ENUM_FLAG), 0);
		assert_ne!(shape.columns[0].column_type, MYSQL_TYPE_SET);
		assert_eq!(shape.rows[0][0].as_deref(), Some(r#"["large","small","large"]"#));
	}

	#[test]
	fn the_empty_set_reads_as_the_empty_text() {
		let labels = ["a".to_owned(), "b".to_owned()];
		assert_eq!(
			member_list_text(&labels, &Value::Set(surrealdb_types::Set::new())).as_deref(),
			Some("")
		);
		assert_eq!(member_list_text(&labels, &Value::Null), None);
	}

	/// A descriptor of one column `v` of kind `ty` declared as `text` in `dialect`.
	fn declared_descriptor(
		dialect: surrealdb_sqlplan::SqlTypeDialect,
		text: &str,
		ty: surrealdb_sqlplan::SqlType,
	) -> surrealdb_sqlplan::ResultDescriptor {
		surrealdb_sqlplan::ResultDescriptor {
			columns: vec![surrealdb_sqlplan::ResultColumn {
				name: "v".to_string(),
				ty,
				declared: Some(DeclaredSqlType::parse(dialect, text).expect("a declared type")),
				..surrealdb_sqlplan::ResultColumn::default()
			}],
		}
	}

	/// The shape of one row holding `value` in a column declared as `text` in `dialect`.
	fn shape_declared(
		dialect: surrealdb_sqlplan::SqlTypeDialect,
		text: &str,
		ty: surrealdb_sqlplan::SqlType,
		value: Value,
	) -> ResultShape {
		let mut obj = surrealdb_types::Object::new();
		obj.insert("v".to_string(), value);
		let descriptor = declared_descriptor(dialect, text, ty);
		shape_result_with_descriptor(
			&descriptor,
			"db",
			"t",
			Value::Array(vec![Value::Object(obj)].into()),
		)
	}

	#[test]
	fn a_declared_column_is_defined_by_its_mysql_type() {
		use surrealdb_sqlplan::SqlType;
		use surrealdb_sqlplan::SqlTypeDialect::MySql;
		let defined = |text: &str, ty: SqlType| {
			let shape = shape_declared(MySql, text, ty, Value::None);
			let column = shape.columns.into_iter().next().expect("a column");
			(column.column_type, column.column_length, column.decimals, column.flags)
		};
		let (code, length, decimals, flags) = defined("year", SqlType::Int2);
		assert_eq!((code, length, decimals), (MYSQL_TYPE_YEAR, 4, 0));
		assert_eq!(flags & (UNSIGNED_FLAG | ZEROFILL_FLAG), UNSIGNED_FLAG | ZEROFILL_FLAG);
		let (code, length, decimals, flags) = defined("bit(8)", SqlType::Int8);
		assert_eq!((code, length, decimals), (MYSQL_TYPE_BIT, 8, 0));
		// MySQL flags a BIT column UNSIGNED, as it holds the unsigned integer of its bits.
		assert_eq!(flags & UNSIGNED_FLAG, UNSIGNED_FLAG);
		assert_eq!(defined("decimal(10,2)", SqlType::Numeric).0, MYSQL_TYPE_NEWDECIMAL);
		assert_eq!(defined("decimal(10,2)", SqlType::Numeric).1, 12);
		assert_eq!(defined("decimal(10,2)", SqlType::Numeric).2, 2);
		assert_eq!(defined("datetime(3)", SqlType::Timestamp).0, MYSQL_TYPE_DATETIME);
		assert_eq!(defined("datetime(3)", SqlType::Timestamp).2, 3);
		assert_eq!(defined("varchar(20)", SqlType::Text).0, MYSQL_TYPE_VAR_STRING);
		assert_eq!(defined("varchar(20)", SqlType::Text).1, 80);
		assert_eq!(defined("char(5)", SqlType::Text).0, MYSQL_TYPE_STRING);
		assert_eq!(defined("char(5)", SqlType::Text).1, 20);
		assert_eq!(defined("int unsigned", SqlType::Int8).3 & UNSIGNED_FLAG, UNSIGNED_FLAG);
		let shape = shape_declared(MySql, "binary(16)", SqlType::Bytea, Value::None);
		assert_eq!(shape.columns[0].flags & BINARY_FLAG, BINARY_FLAG);
		assert_eq!(shape.columns[0].character_set, CHARSET_BINARY);
		let shape = shape_declared(MySql, "point srid 4326", SqlType::Unknown, Value::None);
		assert_eq!(
			(shape.columns[0].column_type, shape.columns[0].srid),
			(MYSQL_TYPE_GEOMETRY, 4326)
		);
	}

	#[test]
	fn a_postgres_declared_column_is_defined_by_the_nearest_mysql_type() {
		use surrealdb_sqlplan::SqlType;
		use surrealdb_sqlplan::SqlTypeDialect::PostgreSql;
		let shape = shape_declared(PostgreSql, "numeric(10,2)", SqlType::Numeric, Value::None);
		assert_eq!(
			(shape.columns[0].column_type, shape.columns[0].decimals),
			(MYSQL_TYPE_NEWDECIMAL, 2)
		);
		let shape = shape_declared(PostgreSql, "timestamp(3)", SqlType::Timestamp, Value::None);
		assert_eq!(
			(shape.columns[0].column_type, shape.columns[0].decimals),
			(MYSQL_TYPE_DATETIME, 3)
		);
		let shape = shape_declared(PostgreSql, "text", SqlType::Text, Value::None);
		assert_eq!(shape.columns[0].column_type, MYSQL_TYPE_BLOB);
	}

	#[test]
	fn a_year_is_written_with_four_digits() {
		use surrealdb_sqlplan::SqlTypeDialect::MySql;
		let shape = shape_declared(
			MySql,
			"year",
			surrealdb_sqlplan::SqlType::Int2,
			Value::Number(Number::Int(2026)),
		);
		assert_eq!(shape.rows[0][0].as_deref(), Some("2026"));
		let shape = shape_declared(
			MySql,
			"year",
			surrealdb_sqlplan::SqlType::Int2,
			Value::Number(Number::Int(0)),
		);
		assert_eq!(shape.rows[0][0].as_deref(), Some("0000"));
	}

	#[test]
	fn a_temporal_value_has_as_many_fraction_digits_as_its_declaration() {
		assert_eq!(
			with_fraction_digits("2026-10-30 12:00:00.5".into(), 3),
			"2026-10-30 12:00:00.500"
		);
		assert_eq!(
			with_fraction_digits("2026-10-30 12:00:00.123456".into(), 3),
			"2026-10-30 12:00:00.123"
		);
		assert_eq!(
			with_fraction_digits("2026-10-30 12:00:00.123".into(), 0),
			"2026-10-30 12:00:00"
		);
		assert_eq!(with_fraction_digits("2026-10-30 12:00:00".into(), 2), "2026-10-30 12:00:00.00");
		assert_eq!(with_fraction_digits("-838:59:59".into(), 1), "-838:59:59.0");
		assert_eq!(with_fraction_digits("2026-10-30".into(), 3), "2026-10-30");
	}

	#[test]
	fn a_decimal_value_has_the_digits_of_its_scale() {
		assert_eq!(with_scale_digits("12.5".into(), 2), "12.50");
		assert_eq!(with_scale_digits("-3".into(), 2), "-3.00");
		assert_eq!(with_scale_digits("1.234".into(), 2), "1.234");
		assert_eq!(with_scale_digits("7".into(), 0), "7");
		assert_eq!(with_scale_digits("NaN".into(), 2), "NaN");
		assert_eq!(with_scale_digits("1e5".into(), 2), "1e5");
	}

	#[test]
	fn a_bit_value_travels_as_its_bytes_in_both_protocols() {
		use surrealdb_sqlplan::SqlTypeDialect::MySql;
		let shape = shape_declared(
			MySql,
			"bit(12)",
			surrealdb_sqlplan::SqlType::Int8,
			Value::Number(Number::Int(0x0a5)),
		);
		assert_eq!(shape.columns[0].column_length, 12);
		let text = encode_shape_text_row(&shape.columns, &shape.rows[0], &shape.raw_rows[0]);
		// A length of two and the two big-endian bytes of the value.
		assert_eq!(text, vec![2, 0x00, 0xa5]);
		let binary = serialize_binary_row(&shape.columns, &shape.raw_rows[0]);
		assert!(binary.ends_with(&[2, 0x00, 0xa5]), "{binary:?}");
		// A NULL stays a NULL.
		let shape = shape_declared(MySql, "bit(8)", surrealdb_sqlplan::SqlType::Int8, Value::Null);
		assert_eq!(
			encode_shape_text_row(&shape.columns, &shape.rows[0], &shape.raw_rows[0]),
			vec![NULL_VALUE]
		);
	}
}
