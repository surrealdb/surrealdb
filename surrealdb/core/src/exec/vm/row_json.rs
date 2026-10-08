//! JSON rows for output frames.
//!
//! Writes a [`Value`] as the JSON text the RPC layer's JSON encoder
//! (`rpc::format::json::encode_str`) writes for the public value it converts
//! to: `surrealdb_types::Value::into_json_value`, then `serde_json`'s compact
//! formatter. Numbers and strings go through `serde_json` itself, so their
//! formatting and escaping are its own; text forms (datetimes, durations,
//! record ids) are rendered into a reused scratch string and escaped from
//! there. Kinds rare in result rows go through the public conversion.

use chrono::SecondsFormat;
use surrealdb_types::{SqlFormat, ToSql};

use crate::val::{Number, Value};

/// Encode `value`, at nesting depth `depth`, as JSON text into `out`.
/// `scratch` is a reused buffer for text renderings.
pub(super) fn encode_json(
	out: &mut Vec<u8>,
	value: &Value,
	depth: usize,
	scratch: &mut String,
) -> anyhow::Result<()> {
	too_deep(depth)?;
	match value {
		Value::None | Value::Null => out.extend_from_slice(b"null"),
		Value::Bool(true) => out.extend_from_slice(b"true"),
		Value::Bool(false) => out.extend_from_slice(b"false"),
		Value::Number(n) => number(out, *n, scratch)?,
		Value::String(s) => string(out, s.as_str())?,
		Value::Duration(d) => {
			scratch.clear();
			surrealdb_types::Duration::from_std(d.0).fmt_sql(scratch, SqlFormat::SingleLine);
			string(out, scratch)?;
		}
		Value::Datetime(d) => {
			use std::fmt::Write as _;

			use chrono::Datelike as _;
			scratch.clear();
			// RFC 3339 with as many fractional digits as the nanoseconds need
			// (0, 3, 6 or 9) and `Z`, written in place; a year outside four
			// digits takes chrono's own rendering.
			if (0..=9999).contains(&d.0.year()) {
				let _ = write!(scratch, "{}", d.0.format("%Y-%m-%dT%H:%M:%S%.fZ"));
			} else {
				scratch.push_str(&d.0.to_rfc3339_opts(SecondsFormat::AutoSi, true));
			}
			string(out, scratch)?;
		}
		Value::Uuid(u) => {
			let mut buffer = [0u8; uuid::fmt::Hyphenated::LENGTH];
			let text = u.0.hyphenated().encode_lower(&mut buffer);
			string(out, text)?;
		}
		Value::Array(a) => {
			out.push(b'[');
			for (i, v) in a.iter().enumerate() {
				if i > 0 {
					out.push(b',');
				}
				encode_json(out, v, depth + 1, scratch)?;
			}
			out.push(b']');
		}
		Value::Object(o) => {
			out.push(b'{');
			for (i, (k, v)) in o.iter().enumerate() {
				if i > 0 {
					out.push(b',');
				}
				string(out, k.as_str())?;
				out.push(b':');
				encode_json(out, v, depth + 1, scratch)?;
			}
			out.push(b'}');
		}
		Value::Bytes(b) => {
			out.push(b'[');
			for (i, byte) in b.0.iter().enumerate() {
				if i > 0 {
					out.push(b',');
				}
				serde_json::to_writer(&mut *out, byte)?;
			}
			out.push(b']');
		}
		Value::RecordId(rid) => {
			// The key nests one level under the id, as the public conversion
			// counts it.
			too_deep(depth + 1)?;
			match &rid.key {
				crate::val::RecordIdKey::Number(n) => {
					record_id_text_int(out, rid.table.as_str(), *n, scratch)?
				}
				crate::val::RecordIdKey::String(s) => {
					record_id_text_str(out, rid.table.as_str(), s.as_str(), scratch)?
				}
				_ => public(out, value, depth)?,
			}
		}
		Value::Set(_)
		| Value::Geometry(_)
		| Value::Table(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_) => public(out, value, depth)?,
	}
	Ok(())
}

/// Encode `scalar`, a scalar kind or a decoded value at depth `depth`, as
/// JSON text: what [`encode_json`] writes for the value it denotes.
pub(super) fn encode_json_scalar(
	out: &mut Vec<u8>,
	scalar: &super::scalar::Scalar<'_>,
	depth: usize,
	scratch: &mut String,
) -> anyhow::Result<()> {
	use super::scalar::Scalar;
	match scalar {
		Scalar::None | Scalar::Null => out.extend_from_slice(b"null"),
		Scalar::Bool(true) => out.extend_from_slice(b"true"),
		Scalar::Bool(false) => out.extend_from_slice(b"false"),
		Scalar::Number(n) => number(out, *n, scratch)?,
		Scalar::Str(s) => string(out, s)?,
		Scalar::Bytes(b) => {
			out.push(b'[');
			for (i, byte) in b.iter().enumerate() {
				if i > 0 {
					out.push(b',');
				}
				serde_json::to_writer(&mut *out, byte)?;
			}
			out.push(b']');
		}
		Scalar::Duration(d) => encode_json(out, &Value::Duration(*d), depth, scratch)?,
		Scalar::Datetime(d) => encode_json(out, &Value::Datetime(*d), depth, scratch)?,
		Scalar::Uuid(u) => encode_json(out, &Value::Uuid(*u), depth, scratch)?,
		Scalar::Value(v) => encode_json(out, v, depth, scratch)?,
	}
	Ok(())
}

/// Write a record id of table `table` and key `key` (a number or a string)
/// as JSON text: the public record id's SurrealQL rendering, as a string.
pub(super) fn record_id_text(
	out: &mut Vec<u8>,
	table: &str,
	key: &crate::val::RecordIdKey,
	scratch: &mut String,
) -> anyhow::Result<()> {
	match key {
		crate::val::RecordIdKey::Number(n) => record_id_text_int(out, table, *n, scratch),
		crate::val::RecordIdKey::String(s) => record_id_text_str(out, table, s.as_str(), scratch),
		_ => anyhow::bail!("a record id key rendered in place that is not a number or a string"),
	}
}

/// Write a record id of table `table` and string key `key` as JSON text.
pub(super) fn record_id_text_str(
	out: &mut Vec<u8>,
	table: &str,
	key: &str,
	scratch: &mut String,
) -> anyhow::Result<()> {
	scratch.clear();
	surrealdb_types::EscapeSqonIdent(table).fmt_sql(scratch, SqlFormat::SingleLine);
	scratch.push(':');
	surrealdb_types::EscapeRecordKey(key).fmt_sql(scratch, SqlFormat::SingleLine);
	string(out, scratch)
}

/// Write a record id of table `table` and integer key `key` as JSON text.
pub(super) fn record_id_text_int(
	out: &mut Vec<u8>,
	table: &str,
	key: i64,
	scratch: &mut String,
) -> anyhow::Result<()> {
	use std::fmt::Write as _;
	scratch.clear();
	surrealdb_types::EscapeSqonIdent(table).fmt_sql(scratch, SqlFormat::SingleLine);
	let _ = write!(scratch, ":{key}");
	string(out, scratch)
}

/// Write `s` as a JSON string.
pub(super) fn json_string(out: &mut Vec<u8>, s: &str) -> anyhow::Result<()> {
	string(out, s)
}

fn number(out: &mut Vec<u8>, n: Number, scratch: &mut String) -> anyhow::Result<()> {
	match n {
		Number::Int(v) => serde_json::to_writer(&mut *out, &v)?,
		Number::Float(v) if v.is_finite() => serde_json::to_writer(&mut *out, &v)?,
		Number::Float(_) => out.extend_from_slice(b"null"),
		Number::Decimal(d) => {
			use std::fmt::Write as _;
			scratch.clear();
			let _ = write!(scratch, "{d}");
			string(out, scratch)?;
		}
	}
	Ok(())
}

#[inline]
fn string(out: &mut Vec<u8>, s: &str) -> anyhow::Result<()> {
	serde_json::to_writer(&mut *out, s)?;
	Ok(())
}

/// Encode `value` through the public conversion and `into_json_value`: the
/// definition the direct encoding answers to, for the kinds it does not
/// write itself.
#[cold]
fn public(out: &mut Vec<u8>, value: &Value, depth: usize) -> anyhow::Result<()> {
	too_deep(depth)?;
	let converted = crate::val::convert_value_to_public_value(value.clone())?;
	serde_json::to_writer(&mut *out, &converted.into_json_value())?;
	Ok(())
}

fn too_deep(depth: usize) -> anyhow::Result<()> {
	if depth > crate::val::MAX_VALUE_DEPTH {
		anyhow::bail!(crate::expr::Error::ValueTooDeep {
			max: crate::val::MAX_VALUE_DEPTH,
		});
	}
	Ok(())
}
