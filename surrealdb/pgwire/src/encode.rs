use std::fmt::Write as _;
use std::str::FromStr;

use chrono::Timelike as _;
use rust_decimal::Decimal;
use surrealdb_types::{Bytes, Datetime, Number, ToSql, Uuid, Value};

use super::error::PgError;
use super::typing::PgType;

// Postgres type OIDs recognised when decoding Bind parameters.
const OID_BOOL: i32 = 16;
const OID_BYTEA: i32 = 17;
const OID_CHAR: i32 = 18;
const OID_NAME: i32 = 19;
const OID_INT8: i32 = 20;
const OID_INT2: i32 = 21;
const OID_INT4: i32 = 23;
const OID_TEXT: i32 = 25;
const OID_OID: i32 = 26;
const OID_JSON: i32 = 114;
const OID_CIDR: i32 = 650;
const OID_FLOAT4: i32 = 700;
const OID_FLOAT8: i32 = 701;
const OID_MACADDR: i32 = 829;
const OID_INET: i32 = 869;
const OID_BPCHAR: i32 = 1042;
const OID_VARCHAR: i32 = 1043;
const OID_DATE: i32 = 1082;
const OID_TIME: i32 = 1083;
const OID_TIMESTAMP: i32 = 1114;
const OID_TIMESTAMPTZ: i32 = 1184;
const OID_INTERVAL: i32 = 1186;
const OID_TIMETZ: i32 = 1266;
const OID_NUMERIC: i32 = 1700;
const OID_REGCLASS: i32 = 2205;
const OID_RECORD: i32 = 2249;
const OID_ANYARRAY: i32 = 2277;
const OID_UUID: i32 = 2950;
const OID_JSONB: i32 = 3802;

// Array OIDs
const OID_ARRAY_BOOL: i32 = 1000;
const OID_ARRAY_BYTEA: i32 = 1001;
const OID_ARRAY_CHAR: i32 = 1002;
const OID_ARRAY_NAME: i32 = 1003;
const OID_ARRAY_INT2: i32 = 1005;
const OID_ARRAY_INT4: i32 = 1007;
const OID_ARRAY_TEXT: i32 = 1009;
const OID_ARRAY_BPCHAR: i32 = 1014;
const OID_ARRAY_VARCHAR: i32 = 1015;
const OID_ARRAY_INT8: i32 = 1016;
const OID_ARRAY_FLOAT4: i32 = 1021;
const OID_ARRAY_FLOAT8: i32 = 1022;
const OID_ARRAY_OID: i32 = 1028;
const OID_ARRAY_MACADDR: i32 = 1040;
const OID_ARRAY_INET: i32 = 1041;
const OID_ARRAY_CIDR: i32 = 651;
const OID_ARRAY_TIMESTAMP: i32 = 1115;
const OID_ARRAY_DATE: i32 = 1182;
const OID_ARRAY_TIME: i32 = 1183;
const OID_ARRAY_TIMESTAMPTZ: i32 = 1185;
const OID_ARRAY_INTERVAL: i32 = 1187;
const OID_ARRAY_NUMERIC: i32 = 1231;
const OID_ARRAY_TIMETZ: i32 = 1270;
const OID_ARRAY_JSON: i32 = 199;
const OID_ARRAY_JSONB: i32 = 3807;
const OID_ARRAY_UUID: i32 = 2951;
const OID_ARRAY_REGCLASS: i32 = 2210;

/// Microseconds between the Unix epoch (1970-01-01) and the Postgres epoch
/// (2000-01-01), the reference point for binary timestamps.
const PG_EPOCH_UNIX_MICROS: i64 = 946_684_800_000_000;

/// Encode one cell into the Postgres text format for its column type.
///
/// Column types are inferred from the values themselves (see
/// [`super::typing::shape_result`]), so a mismatch here is an internal
/// invariant failure, not a user error.
pub(super) fn encode_text(value: Value, ty: &PgType) -> Result<Vec<u8>, PgError> {
	match ty {
		PgType::Bool => match value {
			Value::Bool(true) => Ok(b"t".to_vec()),
			Value::Bool(false) => Ok(b"f".to_vec()),
			v => Err(mismatch(&v, ty)),
		},
		PgType::Int2 => match value {
			Value::Number(Number::Int(i)) => Ok((i as i16).to_string().into_bytes()),
			Value::String(s) if s.parse::<i16>().is_ok() => Ok(s.into_bytes()),
			v => Err(mismatch(&v, ty)),
		},
		PgType::Int4 => match value {
			Value::Number(Number::Int(i)) => Ok((i as i32).to_string().into_bytes()),
			Value::String(s) if s.parse::<i32>().is_ok() => Ok(s.into_bytes()),
			v => Err(mismatch(&v, ty)),
		},
		PgType::Int8 => match value {
			Value::Number(Number::Int(i)) => Ok(i.to_string().into_bytes()),
			Value::String(s) if s.parse::<i64>().is_ok() => Ok(s.into_bytes()),
			v => Err(mismatch(&v, ty)),
		},
		PgType::Oid | PgType::Regclass => match value {
			Value::Number(Number::Int(i)) => Ok((i as u32).to_string().into_bytes()),
			Value::String(s) => Ok(s.into_bytes()),
			v => Err(mismatch(&v, ty)),
		},
		PgType::Float4 => match value {
			Value::Number(Number::Int(i)) => Ok(float_text(i as f64)),
			Value::Number(Number::Float(f)) => Ok(float_text(f)),
			Value::String(s) if s.parse::<f32>().is_ok() => Ok(s.into_bytes()),
			v => Err(mismatch(&v, ty)),
		},
		PgType::Float8 => match value {
			Value::Number(Number::Int(i)) => Ok(float_text(i as f64)),
			Value::Number(Number::Float(f)) => Ok(float_text(f)),
			Value::String(s) if s.parse::<f64>().is_ok() => Ok(s.into_bytes()),
			v => Err(mismatch(&v, ty)),
		},
		PgType::Numeric => match value {
			Value::Number(Number::Int(i)) => Ok(i.to_string().into_bytes()),
			Value::Number(Number::Decimal(d)) => Ok(d.to_string().into_bytes()),
			Value::Number(Number::Float(f)) => Ok(numeric_float_text(f)),
			Value::String(s) => Ok(s.into_bytes()),
			v => Err(mismatch(&v, ty)),
		},
		PgType::Char => match value {
			Value::String(s) => {
				let first = s.chars().next().unwrap_or('\0');
				Ok(first.to_string().into_bytes())
			}
			Value::Number(Number::Int(i)) => Ok(vec![i as u8]),
			v => Err(mismatch(&v, ty)),
		},
		PgType::Name | PgType::Text | PgType::Bpchar | PgType::Varchar => match value {
			Value::String(s) => Ok(s.into_bytes()),
			Value::Number(Number::Int(i)) => Ok(i.to_string().into_bytes()),
			Value::Number(Number::Float(f)) => Ok(float_text(f)),
			Value::Number(Number::Decimal(d)) => Ok(d.to_string().into_bytes()),
			Value::Bool(b) => Ok(b.to_string().into_bytes()),
			v @ (Value::RecordId(_)
			| Value::Range(_)
			| Value::Regex(_)
			| Value::File(_)
			| Value::Table(_)) => Ok(v.to_sql().into_bytes()),
			v @ (Value::Object(_) | Value::Array(_)) => serde_json::to_vec(&v.into_json_value())
				.map_err(|e| PgError::internal(format!("failed to encode value as json: {e}"))),
			v => Err(mismatch(&v, ty)),
		},
		PgType::Date => match value {
			Value::Datetime(dt) => Ok(date_text(dt.into_inner().date_naive())),
			Value::String(s) => {
				if let Some(nd) = parse_date(&s) {
					Ok(date_text(nd))
				} else {
					Ok(s.into_bytes())
				}
			}
			v => Err(mismatch(&v, ty)),
		},
		PgType::Time => match value {
			Value::Datetime(dt) => Ok(time_text(dt.into_inner().time())),
			Value::Duration(d) => Ok(interval_text(d.into_inner())),
			Value::String(s) => Ok(s.into_bytes()),
			v => Err(mismatch(&v, ty)),
		},
		PgType::Timetz => match value {
			Value::Datetime(dt) => Ok(timetz_text(dt.into_inner())),
			Value::Duration(d) => {
				let mut out = interval_text(d.into_inner());
				out.extend_from_slice(b"+00");
				Ok(out)
			}
			Value::String(s) => Ok(s.into_bytes()),
			v => Err(mismatch(&v, ty)),
		},
		PgType::Timestamp => match value {
			Value::Datetime(dt) => Ok(timestamp_text(dt.into_inner())),
			Value::String(s) => Ok(s.into_bytes()),
			v => Err(mismatch(&v, ty)),
		},
		PgType::Timestamptz => match value {
			Value::Datetime(dt) => Ok(timestamptz_text(dt.into_inner())),
			Value::String(s) => Ok(s.into_bytes()),
			v => Err(mismatch(&v, ty)),
		},
		PgType::Interval => match value {
			Value::Duration(d) => Ok(interval_text(d.into_inner())),
			v => Err(mismatch(&v, ty)),
		},
		PgType::Uuid => match value {
			Value::Uuid(u) => Ok(u.to_string().into_bytes()),
			Value::String(s) => Ok(s.into_bytes()),
			v => Err(mismatch(&v, ty)),
		},
		PgType::Bytea => match value {
			Value::Bytes(b) => {
				let bytes = b.into_inner();
				let mut out = String::with_capacity(2 + bytes.len() * 2);
				out.push_str("\\x");
				for byte in &bytes {
					write!(out, "{byte:02x}").expect("writing to a String cannot fail");
				}
				Ok(out.into_bytes())
			}
			v => Err(mismatch(&v, ty)),
		},
		PgType::Json | PgType::Jsonb => serde_json::to_vec(&value.into_json_value())
			.map_err(|e| PgError::internal(format!("failed to encode value as json: {e}"))),
		PgType::Inet | PgType::Cidr | PgType::MacAddr => match value {
			Value::String(s) => Ok(s.into_bytes()),
			v => Err(mismatch(&v, ty)),
		},
		PgType::Record => match value {
			Value::Object(obj) => record_text(&obj),
			v => Err(mismatch(&v, ty)),
		},
		PgType::Array(elem_ty) => match value {
			Value::Array(arr) => array_text(arr.as_slice(), elem_ty),
			Value::String(s) => Ok(s.into_bytes()),
			v => Err(mismatch(&v, ty)),
		},
	}
}

fn mismatch(value: &Value, ty: &PgType) -> PgError {
	PgError::internal(format!("cannot encode a {} cell into a {ty:?} column", value.kind()))
}

fn date_text(nd: chrono::NaiveDate) -> Vec<u8> {
	use chrono::Datelike;
	let year = nd.year();
	let (display_year, era) = if year <= 0 {
		(1 - year, " BC")
	} else {
		(year, "")
	};
	format!("{:04}-{:02}-{:02}{era}", display_year, nd.month(), nd.day()).into_bytes()
}

fn time_text(nt: chrono::NaiveTime) -> Vec<u8> {
	use chrono::Timelike;
	let subsec_micros = nt.nanosecond() / 1_000;
	if subsec_micros == 0 {
		format!("{:02}:{:02}:{:02}", nt.hour(), nt.minute(), nt.second()).into_bytes()
	} else {
		let frac = format!(".{subsec_micros:06}");
		let frac = frac.trim_end_matches('0');
		format!("{:02}:{:02}:{:02}{frac}", nt.hour(), nt.minute(), nt.second()).into_bytes()
	}
}

fn timetz_text(dt: chrono::DateTime<chrono::Utc>) -> Vec<u8> {
	let mut out = time_text(dt.time());
	out.extend_from_slice(b"+00");
	out
}

fn timestamp_text(dt: chrono::DateTime<chrono::Utc>) -> Vec<u8> {
	use chrono::{Datelike, Timelike};
	let year = dt.year();
	let (display_year, era) = if year <= 0 {
		(1 - year, " BC")
	} else {
		(year, "")
	};
	let subsec_micros = dt.timestamp_subsec_micros();
	if subsec_micros == 0 {
		format!(
			"{:04}-{:02}-{:02} {:02}:{:02}:{:02}{era}",
			display_year,
			dt.month(),
			dt.day(),
			dt.hour(),
			dt.minute(),
			dt.second()
		)
	} else {
		let frac = format!(".{subsec_micros:06}");
		let frac = frac.trim_end_matches('0');
		format!(
			"{:04}-{:02}-{:02} {:02}:{:02}:{:02}{frac}{era}",
			display_year,
			dt.month(),
			dt.day(),
			dt.hour(),
			dt.minute(),
			dt.second()
		)
	}
	.into_bytes()
}

/// Render a UTC datetime in the Postgres `timestamptz` output style, e.g.
/// `2024-01-15 12:34:56.123456+00`. Nanoseconds truncate to microsecond
/// precision (Postgres timestamps carry microseconds), and trailing
/// fractional zeros are trimmed — the fractional part is dropped entirely when
/// zero, matching Postgres.
fn timestamptz_text(dt: chrono::DateTime<chrono::Utc>) -> Vec<u8> {
	use chrono::{Datelike, Timelike};
	// Postgres never prints a sign on the year: a proleptic year <= 0 is
	// rendered as `NNNN ... BC` (astronomical year 0 is 1 BC), with the era tag
	// appended after the zone. chrono's `%Y` would instead emit a negative or
	// under-padded year, which no Postgres client expects.
	let year = dt.year();
	let (display_year, era) = if year <= 0 {
		(1 - year, " BC")
	} else {
		(year, "")
	};
	let mut out = format!(
		"{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
		display_year,
		dt.month(),
		dt.day(),
		dt.hour(),
		dt.minute(),
		dt.second()
	);
	let micros = dt.nanosecond() / 1_000;
	if micros > 0 {
		let frac = format!(".{micros:06}");
		out.push_str(frac.trim_end_matches('0'));
	}
	out.push_str("+00");
	out.push_str(era);
	out.into_bytes()
}

/// Postgres renders non-finite `float8` values as `NaN`, `Infinity` and
/// `-Infinity`; Rust's `Display` forms differ, so map them explicitly.
fn float_display(f: f64) -> String {
	if f.is_nan() {
		"NaN".to_string()
	} else if f == f64::INFINITY {
		"Infinity".to_string()
	} else if f == f64::NEG_INFINITY {
		"-Infinity".to_string()
	} else {
		format!("{f}")
	}
}

fn float_text(f: f64) -> Vec<u8> {
	float_display(f).into_bytes()
}

/// Render a float for a `numeric` column. Non-finite values fall back to
/// `NaN`, matching how Postgres stores a non-finite numeric.
fn numeric_float_text(f: f64) -> Vec<u8> {
	if f.is_finite() {
		float_display(f).into_bytes()
	} else {
		b"NaN".to_vec()
	}
}

/// Render a duration in the Postgres default interval output style, e.g.
/// `3 days 04:05:06.789`. Sub-microsecond precision is truncated.
fn interval_text(d: std::time::Duration) -> Vec<u8> {
	let secs = d.as_secs();
	let micros = d.subsec_micros();
	let days = secs / 86_400;
	let rem = secs % 86_400;
	let (h, m, s) = (rem / 3600, rem % 3600 / 60, rem % 60);
	let mut out = String::new();
	if days > 0 {
		let plural = if days == 1 {
			""
		} else {
			"s"
		};
		write!(out, "{days} day{plural}").expect("writing to a String cannot fail");
	}
	if days == 0 || rem > 0 || micros > 0 {
		if !out.is_empty() {
			out.push(' ');
		}
		write!(out, "{h:02}:{m:02}:{s:02}").expect("writing to a String cannot fail");
		if micros > 0 {
			let frac = format!(".{micros:06}");
			out.push_str(frac.trim_end_matches('0'));
		}
	}
	out.into_bytes()
}

fn parse_date(text: &str) -> Option<chrono::NaiveDate> {
	let (body, bc) = strip_era(text);
	let nd = chrono::NaiveDate::parse_from_str(body.trim(), "%Y-%m-%d").ok()?;
	if bc {
		use chrono::Datelike;
		nd.with_year(1 - nd.year())
	} else {
		Some(nd)
	}
}

fn parse_time(text: &str) -> Option<chrono::NaiveTime> {
	let mut clean = text.trim();
	if let Some(pos) = clean.rfind(['+', '-']).filter(|&p| p > 0) {
		clean = &clean[..pos];
	}
	for fmt in ["%H:%M:%S%.f", "%H:%M:%S"] {
		if let Ok(t) = chrono::NaiveTime::parse_from_str(clean.trim(), fmt) {
			return Some(t);
		}
	}
	None
}

fn parse_timetz(text: &str) -> Option<(chrono::NaiveTime, i32)> {
	let clean = text.trim();
	let (time_part, offset_secs) = if let Some(pos) = clean.rfind(['+', '-']).filter(|&p| p > 0) {
		let (tp, op) = clean.split_at(pos);
		let sign = if op.starts_with('-') {
			-1
		} else {
			1
		};
		let op_val = op.trim_start_matches(['+', '-']);
		let secs = if let Some((h, m)) = op_val.split_once(':') {
			let hours: i32 = h.parse().ok()?;
			let mins: i32 = m.parse().ok()?;
			sign * (hours * 3600 + mins * 60)
		} else {
			let hours: i32 = op_val.parse().ok()?;
			sign * hours * 3600
		};
		(tp, secs)
	} else {
		(clean, 0)
	};
	let t = parse_time(time_part)?;
	Some((t, offset_secs))
}

fn parse_ip_network(text: &str) -> Option<(std::net::IpAddr, u8)> {
	let trimmed = text.trim();
	let (addr_str, prefix_str) = match trimmed.split_once('/') {
		Some((a, p)) => (a, Some(p)),
		None => (trimmed, None),
	};
	let ip = addr_str.trim().parse::<std::net::IpAddr>().ok()?;
	let prefix = match prefix_str {
		Some(p) => p.trim().parse::<u8>().ok()?,
		None => match ip {
			std::net::IpAddr::V4(_) => 32,
			std::net::IpAddr::V6(_) => 128,
		},
	};
	Some((ip, prefix))
}

fn encode_inet_binary(ip: std::net::IpAddr, prefix: u8, is_cidr: bool) -> Vec<u8> {
	let mut out = Vec::new();
	match ip {
		std::net::IpAddr::V4(v4) => {
			out.push(2); // PGSQL_AF_INET
			out.push(prefix);
			out.push(if is_cidr {
				1
			} else {
				0
			});
			out.push(4);
			out.extend_from_slice(&v4.octets());
		}
		std::net::IpAddr::V6(v6) => {
			out.push(3); // PGSQL_AF_INET6
			out.push(prefix);
			out.push(if is_cidr {
				1
			} else {
				0
			});
			out.push(16);
			out.extend_from_slice(&v6.octets());
		}
	}
	out
}

fn decode_inet_binary(bytes: &[u8], is_cidr: bool) -> Result<Value, PgError> {
	let oid = if is_cidr {
		OID_CIDR
	} else {
		OID_INET
	};
	if bytes.len() < 4 {
		return Err(param_err(oid, "truncated inet header"));
	}
	let family = bytes[0];
	let prefix = bytes[1];
	let len = bytes[3] as usize;
	if bytes.len() < 4 + len {
		return Err(param_err(oid, "truncated inet payload"));
	}
	let ip_bytes = &bytes[4..4 + len];
	let ip_str = match (family, len) {
		(2, 4) => {
			let octets: [u8; 4] =
				ip_bytes.try_into().map_err(|_| param_err(oid, "invalid ipv4 octets length"))?;
			let v4 = std::net::Ipv4Addr::from(octets);
			if prefix == 32 && !is_cidr {
				v4.to_string()
			} else {
				format!("{v4}/{prefix}")
			}
		}
		(3, 16) => {
			let octets: [u8; 16] =
				ip_bytes.try_into().map_err(|_| param_err(oid, "invalid ipv6 octets length"))?;
			let v6 = std::net::Ipv6Addr::from(octets);
			if prefix == 128 && !is_cidr {
				v6.to_string()
			} else {
				format!("{v6}/{prefix}")
			}
		}
		_ => return Err(param_err(oid, "invalid inet address family or length")),
	};
	Ok(Value::String(ip_str))
}

fn parse_macaddr(s: &str) -> Option<[u8; 6]> {
	let clean = s.trim();
	let parts: Vec<&str> = if clean.contains(':') {
		clean.split(':').collect()
	} else if clean.contains('-') {
		clean.split('-').collect()
	} else {
		return None;
	};
	if parts.len() != 6 {
		return None;
	}
	let mut out = [0u8; 6];
	for (i, part) in parts.iter().enumerate() {
		out[i] = u8::from_str_radix(part, 16).ok()?;
	}
	Some(out)
}

fn format_macaddr(bytes: [u8; 6]) -> String {
	format!(
		"{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
		bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5]
	)
}

fn array_text(arr: &[Value], elem_ty: &PgType) -> Result<Vec<u8>, PgError> {
	let mut out = Vec::new();
	out.push(b'{');
	for (i, elem) in arr.iter().enumerate() {
		if i > 0 {
			out.push(b',');
		}
		match elem {
			Value::None | Value::Null => {
				out.extend_from_slice(b"NULL");
			}
			Value::Array(nested) => {
				let nested_bytes = array_text(nested.as_slice(), elem_ty)?;
				out.extend_from_slice(&nested_bytes);
			}
			v => {
				let text = encode_text(v.clone(), elem_ty)?;
				let s = std::str::from_utf8(&text)
					.map_err(|_| PgError::internal("invalid utf8 in array element"))?;
				let needs_quotes = s.is_empty()
					|| s.eq_ignore_ascii_case("NULL")
					|| s.contains(['{', '}', ',', '"', '\\', ' ', '\t', '\n']);
				if needs_quotes {
					out.push(b'"');
					for b in s.bytes() {
						if b == b'"' || b == b'\\' {
							out.push(b'\\');
						}
						out.push(b);
					}
					out.push(b'"');
				} else {
					out.extend_from_slice(s.as_bytes());
				}
			}
		}
	}
	out.push(b'}');
	Ok(out)
}

fn array_binary(arr: &[Value], elem_ty: &PgType) -> Result<Vec<u8>, PgError> {
	let mut out = Vec::new();
	if arr.is_empty() {
		out.extend_from_slice(&0i32.to_be_bytes()); // ndim = 0
		out.extend_from_slice(&0i32.to_be_bytes()); // has_null = 0
		out.extend_from_slice(&elem_ty.oid().to_be_bytes());
		return Ok(out);
	}
	let has_null = arr.iter().any(|v| matches!(v, Value::None | Value::Null));
	out.extend_from_slice(&1i32.to_be_bytes()); // ndim = 1
	out.extend_from_slice(
		&(if has_null {
			1i32
		} else {
			0i32
		})
		.to_be_bytes(),
	);
	out.extend_from_slice(&elem_ty.oid().to_be_bytes());
	out.extend_from_slice(&(arr.len() as i32).to_be_bytes()); // dim_len
	out.extend_from_slice(&1i32.to_be_bytes()); // lower_bound = 1

	for elem in arr {
		match elem {
			Value::None | Value::Null => {
				out.extend_from_slice(&(-1i32).to_be_bytes());
			}
			v => {
				let bytes = encode_binary(v.clone(), elem_ty)?;
				out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
				out.extend_from_slice(&bytes);
			}
		}
	}
	Ok(out)
}

fn record_text(obj: &surrealdb_types::Object) -> Result<Vec<u8>, PgError> {
	let mut out = Vec::new();
	out.push(b'(');
	for (i, (_k, v)) in obj.iter().enumerate() {
		if i > 0 {
			out.push(b',');
		}
		match v {
			Value::None | Value::Null => {} // empty in record text represents NULL
			val => {
				let text = encode_text(val.clone(), &PgType::Text)?;
				let s = std::str::from_utf8(&text)
					.map_err(|_| PgError::internal("invalid utf8 in record field"))?;
				let needs_quotes =
					s.is_empty() || s.contains(['(', ')', ',', '"', '\\', ' ', '\t']);
				if needs_quotes {
					out.push(b'"');
					for b in s.bytes() {
						if b == b'"' || b == b'\\' {
							out.push(b'\\');
						}
						out.push(b);
					}
					out.push(b'"');
				} else {
					out.extend_from_slice(s.as_bytes());
				}
			}
		}
	}
	out.push(b')');
	Ok(out)
}

fn record_binary(obj: &surrealdb_types::Object) -> Result<Vec<u8>, PgError> {
	let mut out = Vec::new();
	out.extend_from_slice(&(obj.len() as i32).to_be_bytes()); // nfields
	for (_k, v) in obj.iter() {
		out.extend_from_slice(&OID_TEXT.to_be_bytes()); // field_oid
		match v {
			Value::None | Value::Null => {
				out.extend_from_slice(&(-1i32).to_be_bytes());
			}
			val => {
				let bytes = encode_binary(val.clone(), &PgType::Text)?;
				out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
				out.extend_from_slice(&bytes);
			}
		}
	}
	Ok(out)
}

/// Encode one cell into the Postgres binary format for its column type. Like
/// [`encode_text`], a type mismatch is an internal invariant failure.
pub(super) fn encode_binary(value: Value, ty: &PgType) -> Result<Vec<u8>, PgError> {
	match ty {
		PgType::Bool => match value {
			Value::Bool(b) => Ok(vec![u8::from(b)]),
			v => Err(mismatch(&v, ty)),
		},
		PgType::Int2 => match value {
			Value::Number(Number::Int(i)) => Ok((i as i16).to_be_bytes().to_vec()),
			Value::String(s) => match s.parse::<i16>() {
				Ok(i) => Ok(i.to_be_bytes().to_vec()),
				Err(_) => Err(mismatch(&Value::String(s), ty)),
			},
			v => Err(mismatch(&v, ty)),
		},
		PgType::Int4 => match value {
			Value::Number(Number::Int(i)) => Ok((i as i32).to_be_bytes().to_vec()),
			Value::String(s) => match s.parse::<i32>() {
				Ok(i) => Ok(i.to_be_bytes().to_vec()),
				Err(_) => Err(mismatch(&Value::String(s), ty)),
			},
			v => Err(mismatch(&v, ty)),
		},
		PgType::Int8 => match value {
			Value::Number(Number::Int(i)) => Ok(i.to_be_bytes().to_vec()),
			Value::String(s) => match s.parse::<i64>() {
				Ok(i) => Ok(i.to_be_bytes().to_vec()),
				Err(_) => Err(mismatch(&Value::String(s), ty)),
			},
			v => Err(mismatch(&v, ty)),
		},
		PgType::Oid | PgType::Regclass => match value {
			Value::Number(Number::Int(i)) => Ok((i as u32).to_be_bytes().to_vec()),
			Value::String(s) => match s.parse::<u32>() {
				Ok(u) => Ok(u.to_be_bytes().to_vec()),
				Err(_) => Err(mismatch(&Value::String(s), ty)),
			},
			v => Err(mismatch(&v, ty)),
		},
		PgType::Float4 => match value {
			Value::Number(Number::Int(i)) => Ok((i as f32).to_be_bytes().to_vec()),
			Value::Number(Number::Float(f)) => Ok((f as f32).to_be_bytes().to_vec()),
			v => Err(mismatch(&v, ty)),
		},
		PgType::Float8 => match value {
			Value::Number(Number::Int(i)) => Ok((i as f64).to_be_bytes().to_vec()),
			Value::Number(Number::Float(f)) => Ok(f.to_be_bytes().to_vec()),
			v => Err(mismatch(&v, ty)),
		},
		PgType::Numeric => match value {
			Value::Number(Number::Decimal(d)) => Ok(encode_numeric_binary(&d.to_string())),
			Value::Number(Number::Int(i)) => Ok(encode_numeric_binary(&i.to_string())),
			Value::Number(Number::Float(f)) => Ok(encode_numeric_binary(&float_display(f))),
			Value::String(s) => Ok(encode_numeric_binary(&s)),
			v => Err(mismatch(&v, ty)),
		},
		PgType::Char => match value {
			Value::String(s) => Ok(vec![s.bytes().next().unwrap_or(0)]),
			Value::Number(Number::Int(i)) => Ok(vec![i as u8]),
			v => Err(mismatch(&v, ty)),
		},
		PgType::Name | PgType::Text | PgType::Bpchar | PgType::Varchar => match value {
			Value::String(s) => Ok(s.into_bytes()),
			Value::Number(Number::Int(i)) => Ok(i.to_string().into_bytes()),
			Value::Number(Number::Float(f)) => Ok(float_text(f)),
			Value::Number(Number::Decimal(d)) => Ok(d.to_string().into_bytes()),
			Value::Bool(b) => Ok(b.to_string().into_bytes()),
			v @ (Value::RecordId(_)
			| Value::Range(_)
			| Value::Regex(_)
			| Value::File(_)
			| Value::Table(_)) => Ok(v.to_sql().into_bytes()),
			v => Err(mismatch(&v, ty)),
		},
		PgType::Date => match value {
			Value::Datetime(dt) => {
				let nd = dt.into_inner().date_naive();
				let pg_epoch_date =
					chrono::NaiveDate::from_ymd_opt(2000, 1, 1).expect("valid epoch date");
				let days = (nd - pg_epoch_date).num_days() as i32;
				Ok(days.to_be_bytes().to_vec())
			}
			Value::String(s) => {
				if let Some(nd) = parse_date(&s) {
					let pg_epoch_date =
						chrono::NaiveDate::from_ymd_opt(2000, 1, 1).expect("valid epoch date");
					let days = (nd - pg_epoch_date).num_days() as i32;
					Ok(days.to_be_bytes().to_vec())
				} else {
					Err(mismatch(&Value::String(s), ty))
				}
			}
			v => Err(mismatch(&v, ty)),
		},
		PgType::Time => match value {
			Value::Datetime(dt) => {
				let t = dt.into_inner().time();
				let micros = i64::from(t.num_seconds_from_midnight()) * 1_000_000
					+ i64::from(t.nanosecond() / 1_000);
				Ok(micros.to_be_bytes().to_vec())
			}
			Value::Duration(d) => {
				let micros = i64::try_from(d.into_inner().as_micros())
					.map_err(|_| PgError::feature_not_supported("duration too large for time"))?;
				Ok(micros.to_be_bytes().to_vec())
			}
			Value::String(s) => {
				if let Some(t) = parse_time(&s) {
					let micros = i64::from(t.num_seconds_from_midnight()) * 1_000_000
						+ i64::from(t.nanosecond() / 1_000);
					Ok(micros.to_be_bytes().to_vec())
				} else {
					Err(mismatch(&Value::String(s), ty))
				}
			}
			v => Err(mismatch(&v, ty)),
		},
		PgType::Timetz => match value {
			Value::Datetime(dt) => {
				let t = dt.into_inner().time();
				let micros = i64::from(t.num_seconds_from_midnight()) * 1_000_000
					+ i64::from(t.nanosecond() / 1_000);
				let mut out = Vec::with_capacity(12);
				out.extend_from_slice(&micros.to_be_bytes());
				out.extend_from_slice(&0i32.to_be_bytes()); // UTC offset 0
				Ok(out)
			}
			Value::Duration(d) => {
				let micros = i64::try_from(d.into_inner().as_micros())
					.map_err(|_| PgError::feature_not_supported("duration too large for timetz"))?;
				let mut out = Vec::with_capacity(12);
				out.extend_from_slice(&micros.to_be_bytes());
				out.extend_from_slice(&0i32.to_be_bytes());
				Ok(out)
			}
			Value::String(s) => {
				if let Some((t, offset_secs)) = parse_timetz(&s) {
					let micros = i64::from(t.num_seconds_from_midnight()) * 1_000_000
						+ i64::from(t.nanosecond() / 1_000);
					let mut out = Vec::with_capacity(12);
					out.extend_from_slice(&micros.to_be_bytes());
					out.extend_from_slice(&(-offset_secs).to_be_bytes());
					Ok(out)
				} else {
					Err(mismatch(&Value::String(s), ty))
				}
			}
			v => Err(mismatch(&v, ty)),
		},
		PgType::Timestamp => match value {
			Value::Datetime(dt) => {
				let dt = dt.into_inner();
				let unix_micros =
					dt.timestamp() * 1_000_000 + i64::from(dt.timestamp_subsec_micros());
				Ok((unix_micros - PG_EPOCH_UNIX_MICROS).to_be_bytes().to_vec())
			}
			Value::String(s) if s.eq_ignore_ascii_case("infinity") => {
				Ok(i64::MAX.to_be_bytes().to_vec())
			}
			Value::String(s) if s.eq_ignore_ascii_case("-infinity") => {
				Ok(i64::MIN.to_be_bytes().to_vec())
			}
			v => Err(mismatch(&v, ty)),
		},
		PgType::Timestamptz => match value {
			Value::Datetime(dt) => {
				let dt = dt.into_inner();
				let unix_micros =
					dt.timestamp() * 1_000_000 + i64::from(dt.timestamp_subsec_micros());
				Ok((unix_micros - PG_EPOCH_UNIX_MICROS).to_be_bytes().to_vec())
			}
			Value::String(s) if s.eq_ignore_ascii_case("infinity") => {
				Ok(i64::MAX.to_be_bytes().to_vec())
			}
			Value::String(s) if s.eq_ignore_ascii_case("-infinity") => {
				Ok(i64::MIN.to_be_bytes().to_vec())
			}
			v => Err(mismatch(&v, ty)),
		},
		PgType::Interval => match value {
			Value::Duration(d) => {
				let d = d.into_inner();
				let micros = i64::try_from(d.as_micros()).map_err(|_| {
					PgError::feature_not_supported("duration is too large to encode as an interval")
				})?;
				let mut out = Vec::with_capacity(16);
				out.extend_from_slice(&micros.to_be_bytes());
				out.extend_from_slice(&0i32.to_be_bytes());
				out.extend_from_slice(&0i32.to_be_bytes());
				Ok(out)
			}
			v => Err(mismatch(&v, ty)),
		},
		PgType::Uuid => match value {
			Value::Uuid(u) => Ok(u.into_inner().into_bytes().to_vec()),
			v => Err(mismatch(&v, ty)),
		},
		PgType::Bytea => match value {
			Value::Bytes(b) => Ok(b.into_inner().to_vec()),
			v => Err(mismatch(&v, ty)),
		},
		PgType::Json => {
			let json = serde_json::to_vec(&value.into_json_value())
				.map_err(|e| PgError::internal(format!("failed to encode value as json: {e}")))?;
			Ok(json)
		}
		PgType::Jsonb => {
			let json = serde_json::to_vec(&value.into_json_value())
				.map_err(|e| PgError::internal(format!("failed to encode value as json: {e}")))?;
			let mut out = Vec::with_capacity(json.len() + 1);
			out.push(1);
			out.extend_from_slice(&json);
			Ok(out)
		}
		PgType::Inet => match value {
			Value::String(s) => {
				let (ip, prefix) = parse_ip_network(&s)
					.ok_or_else(|| PgError::invalid_text(format!("invalid inet address: {s}")))?;
				Ok(encode_inet_binary(ip, prefix, false))
			}
			v => Err(mismatch(&v, ty)),
		},
		PgType::Cidr => match value {
			Value::String(s) => {
				let (ip, prefix) = parse_ip_network(&s)
					.ok_or_else(|| PgError::invalid_text(format!("invalid cidr address: {s}")))?;
				Ok(encode_inet_binary(ip, prefix, true))
			}
			v => Err(mismatch(&v, ty)),
		},
		PgType::MacAddr => match value {
			Value::String(s) => {
				let bytes = parse_macaddr(&s)
					.ok_or_else(|| PgError::invalid_text(format!("invalid macaddr: {s}")))?;
				Ok(bytes.to_vec())
			}
			v => Err(mismatch(&v, ty)),
		},
		PgType::Record => match value {
			Value::Object(obj) => record_binary(&obj),
			v => Err(mismatch(&v, ty)),
		},
		PgType::Array(elem_ty) => match value {
			Value::Array(arr) => array_binary(arr.as_slice(), elem_ty),
			v => Err(mismatch(&v, ty)),
		},
	}
}

/// Encode a decimal string (e.g. `-123.45`, `0.001`, `42`) into the Postgres
/// `numeric` binary wire format: ndigits, weight, sign, dscale, then the
/// Encode a decimal string (e.g. `-123.45`, `0.001`, `42`) into the Postgres
/// `numeric` binary wire format: ndigits, weight, sign, dscale, then the
/// base-10000 digit groups (most significant first). Non-finite forms
/// (`NaN`/`Infinity`) fall back to the NaN sign, matching how Postgres stores
/// a non-finite numeric.
fn encode_numeric_binary(s: &str) -> Vec<u8> {
	const NUMERIC_POS: u16 = 0x0000;
	const NUMERIC_NEG: u16 = 0x4000;
	const NUMERIC_NAN: u16 = 0xC000;

	let mut out = Vec::new();
	let mut emit = |ndigits: i16, weight: i16, sign: u16, dscale: i16, digits: &[i16]| {
		out.extend_from_slice(&ndigits.to_be_bytes());
		out.extend_from_slice(&weight.to_be_bytes());
		out.extend_from_slice(&sign.to_be_bytes());
		out.extend_from_slice(&dscale.to_be_bytes());
		for d in digits {
			out.extend_from_slice(&d.to_be_bytes());
		}
	};

	let lower = s.to_ascii_lowercase();
	if lower.contains("nan") || lower.contains("inf") {
		emit(0, 0, NUMERIC_NAN, 0, &[]);
		return out;
	}

	let neg = s.starts_with('-');
	let unsigned = s.trim_start_matches(['-', '+']);
	let (int_part, frac_part) = match unsigned.split_once('.') {
		Some((i, f)) => (i, f),
		None => (unsigned, ""),
	};
	let dscale = frac_part.len() as i16;

	// Align the integer part to a left-padded multiple of four digits and the
	// fractional part to a right-padded multiple of four, so each four-digit
	// slice is one base-10000 group straddling no decimal boundary.
	let int_pad = (4 - int_part.len() % 4) % 4;
	let frac_pad = (4 - frac_part.len() % 4) % 4;
	let mut all = String::with_capacity(int_pad + int_part.len() + frac_part.len() + frac_pad);
	for _ in 0..int_pad {
		all.push('0');
	}
	all.push_str(int_part);
	all.push_str(frac_part);
	for _ in 0..frac_pad {
		all.push('0');
	}
	let int_groups = (int_part.len() + int_pad) / 4;

	let bytes = all.as_bytes();
	let mut digits: Vec<i16> = bytes
		.chunks(4)
		.map(|chunk| chunk.iter().fold(0i16, |acc, b| acc * 10 + i16::from(b - b'0')))
		.collect();

	// weight is the base-10000 exponent of the first digit group.
	let mut weight = int_groups as i32 - 1;
	// Trim leading zero groups (dropping each raises the weight of digits[0]).
	while digits.first() == Some(&0) {
		digits.remove(0);
		weight -= 1;
	}
	// Trim trailing zero groups (dscale already records the decimal precision).
	while digits.last() == Some(&0) {
		digits.pop();
	}
	if digits.is_empty() {
		emit(0, 0, NUMERIC_POS, dscale, &[]);
		return out;
	}
	let sign = if neg {
		NUMERIC_NEG
	} else {
		NUMERIC_POS
	};
	emit(digits.len() as i16, weight as i16, sign, dscale, &digits);
	out
}

/// Decode one Bind parameter into a [`Value`]. `format` is 0 (text) or 1
/// (binary); `oid` is the client-declared parameter type (0 = unspecified).
///
/// An unspecified or unrecognised type is taken as text and returned as a
/// [`Value::String`]; SurrealQL can cast it in-query (e.g. `<int> $1`).
pub(super) fn decode_param(bytes: Option<&[u8]>, oid: i32, format: i16) -> Result<Value, PgError> {
	let Some(bytes) = bytes else {
		return Ok(Value::Null);
	};
	if format == 1 {
		decode_binary(bytes, oid)
	} else {
		let text = std::str::from_utf8(bytes)
			.map_err(|_| PgError::protocol("parameter is not valid UTF-8"))?;
		decode_text(text, oid)
	}
}

fn param_err(oid: i32, detail: impl std::fmt::Display) -> PgError {
	PgError::invalid_text(format!("invalid parameter for type oid {oid}: {detail}"))
}

/// Convert a `serde_json` value into a SurrealDB [`Value`]. Used to bind
/// `json`/`jsonb` parameters, whose text/binary payloads are JSON.
fn json_to_value(json: serde_json::Value) -> Value {
	match json {
		serde_json::Value::Null => Value::Null,
		serde_json::Value::Bool(b) => Value::Bool(b),
		serde_json::Value::Number(n) => {
			if let Some(i) = n.as_i64() {
				Value::Number(Number::Int(i))
			} else {
				// Non-integers (and out-of-i64-range integers) become floats.
				Value::Number(Number::Float(n.as_f64().unwrap_or(f64::NAN)))
			}
		}
		serde_json::Value::String(s) => Value::String(s),
		serde_json::Value::Array(items) => {
			Value::Array(items.into_iter().map(json_to_value).collect::<Vec<_>>().into())
		}
		serde_json::Value::Object(map) => {
			let mut obj = surrealdb_types::Object::new();
			for (k, v) in map {
				obj.insert(k, json_to_value(v));
			}
			Value::Object(obj)
		}
	}
}

fn decode_text(text: &str, oid: i32) -> Result<Value, PgError> {
	if let Some(elem_oid) = is_array_oid(oid) {
		return decode_array_text(text, elem_oid);
	}
	match oid {
		OID_BOOL => match text {
			"t" | "true" | "TRUE" | "1" | "yes" | "on" => Ok(Value::Bool(true)),
			"f" | "false" | "FALSE" | "0" | "no" | "off" => Ok(Value::Bool(false)),
			other => Err(param_err(oid, format!("not a boolean: {other}"))),
		},
		OID_INT2 | OID_INT4 | OID_INT8 => i64::from_str(text)
			.map(|i| Value::Number(Number::Int(i)))
			.map_err(|e| param_err(oid, e)),
		OID_OID | OID_REGCLASS => u32::from_str(text)
			.map(|u| Value::Number(Number::Int(i64::from(u))))
			.or_else(|_| i64::from_str(text).map(|i| Value::Number(Number::Int(i))))
			.or_else(|_| Ok(Value::String(text.to_string()))),
		OID_FLOAT4 | OID_FLOAT8 => f64::from_str(text)
			.map(|f| Value::Number(Number::Float(f)))
			.map_err(|e| param_err(oid, e)),
		OID_NUMERIC => {
			let lower = text.to_ascii_lowercase();
			if lower == "nan" {
				Ok(Value::Number(Number::Float(f64::NAN)))
			} else if lower == "infinity"
				|| lower == "+infinity"
				|| lower == "inf"
				|| lower == "+inf"
			{
				Ok(Value::Number(Number::Float(f64::INFINITY)))
			} else if lower == "-infinity" || lower == "-inf" {
				Ok(Value::Number(Number::Float(f64::NEG_INFINITY)))
			} else {
				rust_decimal::Decimal::from_str(text)
					.or_else(|_| rust_decimal::Decimal::from_scientific(text))
					.map(|d| Value::Number(Number::Decimal(d)))
					.map_err(|e| param_err(oid, e))
			}
		}
		OID_UUID => Uuid::from_str(text).map(Value::Uuid).map_err(|e| param_err(oid, e)),
		OID_DATE => {
			if let Some(nd) = parse_date(text) {
				let dt = nd.and_hms_opt(0, 0, 0).expect("valid time").and_utc();
				utc_datetime(dt).ok_or_else(|| param_err(oid, "date out of range"))
			} else {
				Err(param_err(oid, "not a date"))
			}
		}
		OID_TIME | OID_TIMETZ => {
			if let Some(t) = parse_time(text) {
				let micros = i64::from(t.num_seconds_from_midnight()) * 1_000_000
					+ i64::from(t.nanosecond() / 1_000);
				Ok(Value::Duration(std::time::Duration::from_micros(micros as u64).into()))
			} else {
				Ok(Value::String(text.to_string()))
			}
		}
		OID_TIMESTAMP | OID_TIMESTAMPTZ => {
			let lower = text.to_ascii_lowercase();
			if lower == "infinity" {
				Ok(Value::String("infinity".to_string()))
			} else if lower == "-infinity" {
				Ok(Value::String("-infinity".to_string()))
			} else {
				parse_datetime(text).ok_or_else(|| param_err(oid, "not a timestamp"))
			}
		}
		OID_BYTEA => decode_bytea_text(text).ok_or_else(|| param_err(oid, "not a bytea literal")),
		OID_JSON | OID_JSONB => {
			let json: serde_json::Value =
				serde_json::from_str(text).map_err(|e| param_err(oid, e))?;
			Ok(json_to_value(json))
		}
		OID_INET | OID_CIDR | OID_MACADDR | OID_RECORD => Ok(Value::String(text.to_string())),
		_ => Ok(Value::String(text.to_string())),
	}
}

fn decode_binary(bytes: &[u8], oid: i32) -> Result<Value, PgError> {
	if let Some(elem_oid) = is_array_oid(oid) {
		return decode_array_binary(bytes, elem_oid);
	}
	let int = |n: usize| -> Result<i64, PgError> {
		match (n, bytes) {
			(1, [a]) => Ok(i64::from(*a as i8)),
			(2, [a, b]) => Ok(i64::from(i16::from_be_bytes([*a, *b]))),
			(4, [a, b, c, d]) => Ok(i64::from(i32::from_be_bytes([*a, *b, *c, *d]))),
			(8, [a, b, c, d, e, f, g, h]) => {
				Ok(i64::from_be_bytes([*a, *b, *c, *d, *e, *f, *g, *h]))
			}
			_ => Err(param_err(oid, "wrong byte length for integer")),
		}
	};
	match oid {
		OID_BOOL => match bytes {
			[0] => Ok(Value::Bool(false)),
			[_] => Ok(Value::Bool(true)),
			_ => Err(param_err(oid, "wrong byte length for bool")),
		},
		OID_CHAR => match bytes {
			[b] => Ok(Value::String((*b as char).to_string())),
			_ => Err(param_err(oid, "wrong byte length for char")),
		},
		OID_INT2 => Ok(Value::Number(Number::Int(int(2)?))),
		OID_INT4 => Ok(Value::Number(Number::Int(int(4)?))),
		OID_INT8 => Ok(Value::Number(Number::Int(int(8)?))),
		OID_OID | OID_REGCLASS => {
			let raw: [u8; 4] = bytes.try_into().map_err(|_| param_err(oid, "wrong length"))?;
			Ok(Value::Number(Number::Int(i64::from(u32::from_be_bytes(raw)))))
		}
		OID_FLOAT4 => {
			let raw: [u8; 4] = bytes.try_into().map_err(|_| param_err(oid, "wrong length"))?;
			Ok(Value::Number(Number::Float(f64::from(f32::from_be_bytes(raw)))))
		}
		OID_FLOAT8 => {
			let raw: [u8; 8] = bytes.try_into().map_err(|_| param_err(oid, "wrong length"))?;
			Ok(Value::Number(Number::Float(f64::from_be_bytes(raw))))
		}
		OID_UUID => {
			let raw: [u8; 16] = bytes.try_into().map_err(|_| param_err(oid, "wrong length"))?;
			Ok(Value::Uuid(Uuid::from(uuid::Uuid::from_bytes(raw))))
		}
		OID_BYTEA => Ok(Value::Bytes(Bytes::from(bytes.to_vec()))),
		OID_DATE => {
			let raw: [u8; 4] = bytes.try_into().map_err(|_| param_err(oid, "wrong length"))?;
			let days = i32::from_be_bytes(raw);
			let pg_epoch_date =
				chrono::NaiveDate::from_ymd_opt(2000, 1, 1).expect("valid epoch date");
			let nd = pg_epoch_date + chrono::Duration::days(i64::from(days));
			let dt = nd.and_hms_opt(0, 0, 0).expect("valid time").and_utc();
			utc_datetime(dt).ok_or_else(|| param_err(oid, "out of range"))
		}
		OID_TIME => {
			let raw: [u8; 8] = bytes.try_into().map_err(|_| param_err(oid, "wrong length"))?;
			let micros = i64::from_be_bytes(raw);
			if micros < 0 {
				return Err(param_err(oid, "negative time"));
			}
			Ok(Value::Duration(std::time::Duration::from_micros(micros as u64).into()))
		}
		OID_TIMETZ => {
			if bytes.len() < 12 {
				return Err(param_err(oid, "wrong length for timetz"));
			}
			let raw: [u8; 8] =
				bytes[0..8].try_into().map_err(|_| param_err(oid, "wrong length for timetz"))?;
			let micros = i64::from_be_bytes(raw);
			if micros < 0 {
				return Err(param_err(oid, "negative time"));
			}
			Ok(Value::Duration(std::time::Duration::from_micros(micros as u64).into()))
		}
		OID_TIMESTAMP | OID_TIMESTAMPTZ => {
			let raw: [u8; 8] = bytes.try_into().map_err(|_| param_err(oid, "wrong length"))?;
			let pg_micros = i64::from_be_bytes(raw);
			if pg_micros == i64::MAX {
				return Ok(Value::String("infinity".to_string()));
			}
			if pg_micros == i64::MIN {
				return Ok(Value::String("-infinity".to_string()));
			}
			let unix_micros = pg_micros
				.checked_add(PG_EPOCH_UNIX_MICROS)
				.ok_or_else(|| param_err(oid, "out of range"))?;
			datetime_from_unix_micros(unix_micros).ok_or_else(|| param_err(oid, "out of range"))
		}
		OID_NUMERIC => decode_numeric_binary(bytes, oid),
		OID_INTERVAL => decode_interval_binary(bytes, oid),
		OID_JSON | OID_JSONB => {
			// jsonb has a leading version byte; json does not.
			let json_bytes = if oid == OID_JSONB && bytes.first() == Some(&1) {
				&bytes[1..]
			} else {
				bytes
			};
			let json: serde_json::Value =
				serde_json::from_slice(json_bytes).map_err(|e| param_err(oid, e))?;
			Ok(json_to_value(json))
		}
		OID_INET => decode_inet_binary(bytes, false),
		OID_CIDR => decode_inet_binary(bytes, true),
		OID_MACADDR => {
			let raw: [u8; 6] =
				bytes.try_into().map_err(|_| param_err(oid, "wrong length for macaddr"))?;
			Ok(Value::String(format_macaddr(raw)))
		}
		OID_RECORD => {
			if bytes.len() < 4 {
				return Err(param_err(oid, "truncated record header"));
			}
			let raw_hdr: [u8; 4] =
				bytes[0..4].try_into().map_err(|_| param_err(oid, "truncated record header"))?;
			let nfields = i32::from_be_bytes(raw_hdr) as usize;
			let mut obj = surrealdb_types::Object::new();
			let mut offset = 4;
			for idx in 0..nfields {
				if offset + 8 > bytes.len() {
					return Err(param_err(oid, "truncated record field header"));
				}
				let raw_oid: [u8; 4] = bytes[offset..offset + 4]
					.try_into()
					.map_err(|_| param_err(oid, "truncated record field header"))?;
				let field_oid = i32::from_be_bytes(raw_oid);
				let raw_len: [u8; 4] = bytes[offset + 4..offset + 8]
					.try_into()
					.map_err(|_| param_err(oid, "truncated record field header"))?;
				let field_len = i32::from_be_bytes(raw_len);
				offset += 8;
				let val = if field_len == -1 {
					Value::Null
				} else if field_len < 0 {
					return Err(param_err(oid, "invalid field length"));
				} else {
					let field_len = field_len as usize;
					if offset + field_len > bytes.len() {
						return Err(param_err(oid, "truncated record field bytes"));
					}
					let val_bytes = &bytes[offset..offset + field_len];
					offset += field_len;
					decode_binary(val_bytes, field_oid)?
				};
				obj.insert(format!("f{idx}"), val);
			}
			Ok(Value::Object(obj))
		}
		OID_TEXT | OID_VARCHAR | OID_BPCHAR | OID_NAME => std::str::from_utf8(bytes)
			.map(|s| Value::String(s.to_string()))
			.map_err(|_| param_err(oid, "not valid UTF-8")),
		0 => std::str::from_utf8(bytes)
			.map(|s| Value::String(s.to_string()))
			.map_err(|_| param_err(oid, "unspecified binary parameter is not valid UTF-8")),
		_ => Err(param_err(oid, "unsupported binary parameter type")),
	}
}

/// Decode the Postgres `numeric` binary wire format into a [`Decimal`].
/// Inverse of [`encode_numeric_binary`].
fn decode_numeric_binary(bytes: &[u8], oid: i32) -> Result<Value, PgError> {
	if bytes.len() < 8 {
		return Err(param_err(oid, "numeric header truncated"));
	}
	let rd16 = |i: usize| i16::from_be_bytes([bytes[i], bytes[i + 1]]);
	let ndigits = rd16(0);
	let weight = rd16(2);
	let sign = u16::from_be_bytes([bytes[4], bytes[5]]);
	let dscale = rd16(6);
	if sign == 0xC000 || sign == 0xD000 || sign == 0xF000 {
		return Err(param_err(oid, "NaN is not representable"));
	}
	let ndigits = usize::try_from(ndigits).map_err(|_| param_err(oid, "negative ndigits"))?;
	if bytes.len() < 8 + ndigits * 2 {
		return Err(param_err(oid, "numeric digits truncated"));
	}
	// Concatenate the base-10000 groups into one integer mantissa, then scale
	// it by the group weight of the least significant group.
	let mut mantissa: i128 = 0;
	for i in 0..ndigits {
		let group = rd16(8 + i * 2);
		if !(0..10_000).contains(&group) {
			return Err(param_err(oid, "numeric digit out of range"));
		}
		mantissa = mantissa
			.checked_mul(10_000)
			.and_then(|m| m.checked_add(i128::from(group)))
			.ok_or_else(|| param_err(oid, "numeric too large"))?;
	}
	let exp10 = 4 * (i32::from(weight) - (ndigits as i32 - 1));
	let value = if exp10 >= 0 {
		let factor =
			10i128.checked_pow(exp10 as u32).ok_or_else(|| param_err(oid, "numeric too large"))?;
		let scaled =
			mantissa.checked_mul(factor).ok_or_else(|| param_err(oid, "numeric too large"))?;
		Decimal::try_from_i128_with_scale(scaled, 0).map_err(|e| param_err(oid, e))?
	} else {
		let scale = u32::try_from(-exp10).expect("exp10 is negative here");
		Decimal::try_from_i128_with_scale(mantissa, scale).map_err(|e| param_err(oid, e))?
	};
	let mut value = if sign == 0x4000 {
		-value
	} else {
		value
	};
	// The reconstructed mantissa is the exact value; `dscale` is only the
	// client's display scale. Honour it to trim the trailing zero groups the
	// group alignment introduced (e.g. `123.45` arrives as `.4500`), but only
	// when doing so is lossless — a too-small `dscale` must never round away
	// real digits (a client sending `dscale = 0` for `1.5` must not yield `2`).
	if let Ok(scale) = u32::try_from(dscale)
		&& scale <= 28
		&& scale >= value.normalize().scale()
	{
		value.rescale(scale);
	}
	Ok(Value::Number(Number::Decimal(value)))
}

/// Decode the Postgres `interval` binary wire format (micros, days, months)
/// into a [`std::time::Duration`]. Month components are calendar-relative and
/// have no absolute-duration equivalent, so a non-zero month field is rejected.
fn decode_interval_binary(bytes: &[u8], oid: i32) -> Result<Value, PgError> {
	let raw: [u8; 16] = bytes.try_into().map_err(|_| param_err(oid, "wrong length"))?;
	let micros = i64::from_be_bytes(raw[0..8].try_into().expect("8 bytes"));
	let days = i32::from_be_bytes(raw[8..12].try_into().expect("4 bytes"));
	let months = i32::from_be_bytes(raw[12..16].try_into().expect("4 bytes"));
	if months != 0 {
		return Err(param_err(oid, "month intervals are not representable as a duration"));
	}
	let total_micros = i64::from(days)
		.checked_mul(86_400_000_000)
		.and_then(|d| d.checked_add(micros))
		.ok_or_else(|| param_err(oid, "interval out of range"))?;
	let total_micros =
		u64::try_from(total_micros).map_err(|_| param_err(oid, "negative interval"))?;
	Ok(Value::Duration(std::time::Duration::from_micros(total_micros).into()))
}

fn parse_datetime(text: &str) -> Option<Value> {
	// A Postgres BC timestamp ends with a ` BC` era tag and shows a positive
	// proleptic year (astronomical year = 1 - displayed year, so year 0 is 1 BC);
	// strip and remember it, parse the remainder, then convert the year back —
	// this keeps `timestamptz_text` output round-trippable. An ` AD` tag (which
	// Postgres omits by default) is tolerated too.
	let (body, bc) = strip_era(text);
	let value = parse_datetime_body(body)?;
	if !bc {
		return Some(value);
	}
	let Value::Datetime(dt) = value else {
		return None;
	};
	use chrono::Datelike;
	let inner = dt.into_inner();
	inner.with_year(1 - inner.year()).and_then(utc_datetime)
}

/// Split a trailing ` BC`/` AD` era tag off a Postgres timestamp text, returning
/// the remainder and whether the era is BC.
fn strip_era(text: &str) -> (&str, bool) {
	for (suffix, bc) in [(" BC", true), (" bc", true), (" AD", false), (" ad", false)] {
		if let Some(body) = text.strip_suffix(suffix) {
			return (body, bc);
		}
	}
	(text, false)
}

fn parse_datetime_body(text: &str) -> Option<Value> {
	// RFC 3339 with an explicit offset: `2024-01-15T12:34:56Z`, `...+00:00`.
	if let Ok(dt) = text.parse::<Datetime>() {
		return Some(Value::Datetime(dt));
	}
	// Postgres separates the date and time with a space; normalise it to `T`
	// and retry RFC 3339.
	let normalised = text.replacen(' ', "T", 1);
	if let Ok(dt) = normalised.parse::<Datetime>() {
		return Some(Value::Datetime(dt));
	}
	// `timestamptz` text renders the zone as an hours-only (or hours:minutes)
	// numeric offset like `+00` / `-05`, which RFC 3339 rejects; parse those.
	for fmt in ["%Y-%m-%dT%H:%M:%S%.f%#z", "%Y-%m-%dT%H:%M:%S%#z"] {
		if let Ok(dt) = chrono::DateTime::parse_from_str(&normalised, fmt) {
			return utc_datetime(dt.with_timezone(&chrono::Utc));
		}
	}
	// `timestamp` (without time zone) carries no offset at all; assume UTC, as
	// Postgres does when such a value is handed to a `timestamptz` context.
	for fmt in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S"] {
		if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(&normalised, fmt) {
			return utc_datetime(naive.and_utc());
		}
	}
	None
}

/// Build a [`Value::Datetime`] from a UTC chrono datetime, or `None` if it
/// falls outside the representable range.
fn utc_datetime(dt: chrono::DateTime<chrono::Utc>) -> Option<Value> {
	Datetime::from_timestamp(dt.timestamp(), dt.timestamp_subsec_nanos()).map(Value::Datetime)
}

fn datetime_from_unix_micros(unix_micros: i64) -> Option<Value> {
	let secs = unix_micros.div_euclid(1_000_000);
	let nanos = (unix_micros.rem_euclid(1_000_000) * 1_000) as u32;
	Datetime::from_timestamp(secs, nanos).map(Value::Datetime)
}

fn is_array_oid(oid: i32) -> Option<i32> {
	match oid {
		OID_ARRAY_BOOL => Some(OID_BOOL),
		OID_ARRAY_BYTEA => Some(OID_BYTEA),
		OID_ARRAY_CHAR => Some(OID_CHAR),
		OID_ARRAY_NAME => Some(OID_NAME),
		OID_ARRAY_INT2 => Some(OID_INT2),
		OID_ARRAY_INT4 => Some(OID_INT4),
		OID_ARRAY_TEXT => Some(OID_TEXT),
		OID_ARRAY_BPCHAR => Some(OID_BPCHAR),
		OID_ARRAY_VARCHAR => Some(OID_VARCHAR),
		OID_ARRAY_INT8 => Some(OID_INT8),
		OID_ARRAY_FLOAT4 => Some(OID_FLOAT4),
		OID_ARRAY_FLOAT8 => Some(OID_FLOAT8),
		OID_ARRAY_OID => Some(OID_OID),
		OID_ARRAY_MACADDR => Some(OID_MACADDR),
		OID_ARRAY_INET => Some(OID_INET),
		OID_ARRAY_CIDR => Some(OID_CIDR),
		OID_ARRAY_TIMESTAMP => Some(OID_TIMESTAMP),
		OID_ARRAY_DATE => Some(OID_DATE),
		OID_ARRAY_TIME => Some(OID_TIME),
		OID_ARRAY_TIMESTAMPTZ => Some(OID_TIMESTAMPTZ),
		OID_ARRAY_INTERVAL => Some(OID_INTERVAL),
		OID_ARRAY_NUMERIC => Some(OID_NUMERIC),
		OID_ARRAY_TIMETZ => Some(OID_TIMETZ),
		OID_ARRAY_JSON => Some(OID_JSON),
		OID_ARRAY_JSONB => Some(OID_JSONB),
		OID_ARRAY_UUID => Some(OID_UUID),
		OID_ARRAY_REGCLASS => Some(OID_REGCLASS),
		OID_ANYARRAY => Some(OID_TEXT),
		_ => None,
	}
}

fn decode_array_text(text: &str, elem_oid: i32) -> Result<Value, PgError> {
	let trimmed = text.trim();
	if !trimmed.starts_with('{') || !trimmed.ends_with('}') {
		return Err(param_err(elem_oid, "array must start with { and end with }"));
	}
	let inner = &trimmed[1..trimmed.len() - 1];
	if inner.trim().is_empty() {
		return Ok(Value::Array(Vec::<Value>::new().into()));
	}

	let mut elements = Vec::new();
	let mut current = String::new();
	let mut in_quotes = false;
	let mut escape = false;
	let mut brace_depth = 0;

	let chars: Vec<char> = inner.chars().collect();
	let mut i = 0;
	while i < chars.len() {
		let c = chars[i];
		if escape {
			current.push(c);
			escape = false;
			i += 1;
			continue;
		}
		if c == '\\' {
			escape = true;
			i += 1;
			continue;
		}
		if c == '"' {
			in_quotes = !in_quotes;
			i += 1;
			continue;
		}
		if !in_quotes {
			if c == '{' {
				brace_depth += 1;
				current.push(c);
				i += 1;
				continue;
			} else if c == '}' {
				brace_depth -= 1;
				current.push(c);
				i += 1;
				continue;
			}
			if brace_depth == 0 && c == ',' {
				let trimmed_elem = current.trim();
				if !in_quotes && trimmed_elem.eq_ignore_ascii_case("NULL") {
					elements.push(Value::Null);
				} else if trimmed_elem.starts_with('{') {
					elements.push(decode_array_text(trimmed_elem, elem_oid)?);
				} else {
					elements.push(decode_text(trimmed_elem, elem_oid)?);
				}
				current.clear();
				i += 1;
				continue;
			}
		}
		current.push(c);
		i += 1;
	}

	let trimmed_elem = current.trim();
	if !trimmed_elem.is_empty() || in_quotes {
		if !in_quotes && trimmed_elem.eq_ignore_ascii_case("NULL") {
			elements.push(Value::Null);
		} else if trimmed_elem.starts_with('{') {
			elements.push(decode_array_text(trimmed_elem, elem_oid)?);
		} else {
			elements.push(decode_text(trimmed_elem, elem_oid)?);
		}
	}

	Ok(Value::Array(elements.into()))
}

fn decode_array_binary(bytes: &[u8], expected_elem_oid: i32) -> Result<Value, PgError> {
	if bytes.len() < 12 {
		return Err(param_err(expected_elem_oid, "array binary header too short"));
	}
	let raw_ndim: [u8; 4] = bytes[0..4]
		.try_into()
		.map_err(|_| param_err(expected_elem_oid, "array binary header too short"))?;
	let ndim = i32::from_be_bytes(raw_ndim);
	let raw_oid: [u8; 4] = bytes[8..12]
		.try_into()
		.map_err(|_| param_err(expected_elem_oid, "array binary header too short"))?;
	let elem_oid = i32::from_be_bytes(raw_oid);
	let target_oid = if elem_oid != 0 {
		elem_oid
	} else {
		expected_elem_oid
	};

	if ndim == 0 {
		return Ok(Value::Array(Vec::<Value>::new().into()));
	}
	if bytes.len() < 12 + (ndim as usize) * 8 {
		return Err(param_err(target_oid, "array binary dimensions header too short"));
	}

	let mut total_elems = 1usize;
	let mut offset = 12;
	for _ in 0..ndim {
		let raw_dim: [u8; 4] = bytes[offset..offset + 4]
			.try_into()
			.map_err(|_| param_err(target_oid, "array dim header truncated"))?;
		let dim_len = i32::from_be_bytes(raw_dim);
		offset += 8;
		if dim_len > 0 {
			total_elems = total_elems
				.checked_mul(dim_len as usize)
				.ok_or_else(|| param_err(target_oid, "array dimensions overflow"))?;
		} else {
			return Ok(Value::Array(Vec::<Value>::new().into()));
		}
	}

	let mut elements = Vec::with_capacity(total_elems);
	for _ in 0..total_elems {
		if offset + 4 > bytes.len() {
			return Err(param_err(target_oid, "array binary element length truncated"));
		}
		let raw_len: [u8; 4] = bytes[offset..offset + 4]
			.try_into()
			.map_err(|_| param_err(target_oid, "array element length truncated"))?;
		let elem_len = i32::from_be_bytes(raw_len);
		offset += 4;
		if elem_len == -1 {
			elements.push(Value::Null);
		} else if elem_len < 0 {
			return Err(param_err(target_oid, "invalid negative element length"));
		} else {
			let elem_len = elem_len as usize;
			if offset + elem_len > bytes.len() {
				return Err(param_err(target_oid, "array binary element bytes truncated"));
			}
			let elem_bytes = &bytes[offset..offset + elem_len];
			offset += elem_len;
			elements.push(decode_binary(elem_bytes, target_oid)?);
		}
	}

	Ok(Value::Array(elements.into()))
}

fn decode_bytea_text(text: &str) -> Option<Value> {
	if let Some(hex) = text.strip_prefix("\\x") {
		if hex.len() % 2 != 0 {
			return None;
		}
		let mut out = Vec::with_capacity(hex.len() / 2);
		let bytes = hex.as_bytes();
		for pair in bytes.chunks(2) {
			let hi = (pair[0] as char).to_digit(16)?;
			let lo = (pair[1] as char).to_digit(16)?;
			out.push((hi * 16 + lo) as u8);
		}
		return Some(Value::Bytes(Bytes::from(out)));
	}
	// Escape format:
	let mut out = Vec::new();
	let bytes = text.as_bytes();
	let mut i = 0;
	while i < bytes.len() {
		if bytes[i] == b'\\' {
			i += 1;
			if i >= bytes.len() {
				return None;
			}
			if bytes[i] == b'\\' {
				out.push(b'\\');
				i += 1;
			} else if i + 2 < bytes.len()
				&& (b'0'..=b'7').contains(&bytes[i])
				&& (b'0'..=b'7').contains(&bytes[i + 1])
				&& (b'0'..=b'7').contains(&bytes[i + 2])
			{
				let val =
					(bytes[i] - b'0') * 64 + (bytes[i + 1] - b'0') * 8 + (bytes[i + 2] - b'0');
				out.push(val);
				i += 3;
			} else {
				out.push(bytes[i]);
				i += 1;
			}
		} else {
			out.push(bytes[i]);
			i += 1;
		}
	}
	Some(Value::Bytes(Bytes::from(out)))
}

#[cfg(test)]
mod tests {
	use surrealdb_types::{Number, Value};

	use super::*;

	#[allow(clippy::needless_pass_by_value)]
	fn text(value: Value, ty: PgType) -> String {
		String::from_utf8(encode_text(value, &ty).unwrap()).unwrap()
	}

	#[test]
	fn scalars() {
		assert_eq!(text(Value::Bool(true), PgType::Bool), "t");
		assert_eq!(text(Value::Number(Number::Int(-42)), PgType::Int8), "-42");
		assert_eq!(text(Value::Number(Number::Float(1.5)), PgType::Float8), "1.5");
		assert_eq!(text(Value::Number(Number::Int(3)), PgType::Float8), "3");
		assert_eq!(text(Value::String("hi".into()), PgType::Text), "hi");
	}

	#[test]
	fn non_finite_floats() {
		assert_eq!(text(Value::Number(Number::Float(f64::NAN)), PgType::Float8), "NaN");
		assert_eq!(text(Value::Number(Number::Float(f64::INFINITY)), PgType::Float8), "Infinity");
		assert_eq!(
			text(Value::Number(Number::Float(f64::NEG_INFINITY)), PgType::Float8),
			"-Infinity"
		);
	}

	#[test]
	fn intervals() {
		use std::time::Duration;
		assert_eq!(text(Value::Duration(Duration::ZERO.into()), PgType::Interval), "00:00:00");
		assert_eq!(
			text(Value::Duration(Duration::from_secs(90_061).into()), PgType::Interval),
			"1 day 01:01:01"
		);
		assert_eq!(
			text(Value::Duration(Duration::from_millis(1_500).into()), PgType::Interval),
			"00:00:01.5"
		);
		assert_eq!(
			text(Value::Duration(Duration::from_secs(86_400 * 2).into()), PgType::Interval),
			"2 days"
		);
	}

	#[test]
	fn timestamptz_trims_fractional_zeros() {
		use surrealdb_types::Datetime;
		let parse = |s: &str| s.parse::<Datetime>().unwrap();
		// Whole seconds: no fractional part at all.
		assert_eq!(
			text(Value::Datetime(parse("2024-01-15T12:34:56Z")), PgType::Timestamptz),
			"2024-01-15 12:34:56+00"
		);
		// Trailing zeros trimmed.
		assert_eq!(
			text(Value::Datetime(parse("2024-01-15T12:34:56.500Z")), PgType::Timestamptz),
			"2024-01-15 12:34:56.5+00"
		);
		// Nanoseconds truncate to microseconds.
		assert_eq!(
			text(Value::Datetime(parse("2024-01-15T12:34:56.123456789Z")), PgType::Timestamptz),
			"2024-01-15 12:34:56.123456+00"
		);
	}

	#[test]
	fn bytea_hex() {
		let bytes = surrealdb_types::Bytes::from(vec![0x00u8, 0xff, 0x10]);
		assert_eq!(text(Value::Bytes(bytes), PgType::Bytea), "\\x00ff10");
	}

	#[test]
	fn jsonb_renders_via_json_conversion() {
		let out = text(Value::Array(vec![Value::Number(Number::Int(1))].into()), PgType::Jsonb);
		assert_eq!(out, "[1]");
	}

	#[test]
	fn mismatches_error() {
		assert!(encode_text(Value::Bool(true), &PgType::Int8).is_err());
	}

	#[allow(clippy::needless_pass_by_value)]
	fn bin(value: Value, ty: PgType) -> Vec<u8> {
		encode_binary(value, &ty).unwrap()
	}

	#[test]
	fn binary_scalars() {
		assert_eq!(bin(Value::Bool(true), PgType::Bool), vec![1]);
		assert_eq!(bin(Value::Bool(false), PgType::Bool), vec![0]);
		assert_eq!(bin(Value::Number(Number::Int(1)), PgType::Int8), 1i64.to_be_bytes());
		assert_eq!(bin(Value::Number(Number::Float(1.5)), PgType::Float8), 1.5f64.to_be_bytes());
	}

	#[test]
	fn binary_timestamptz_is_micros_since_2000() {
		use surrealdb_types::Datetime;
		// 2000-01-01T00:00:00Z is the Postgres epoch: zero microseconds.
		let epoch: Datetime = "2000-01-01T00:00:00Z".parse().unwrap();
		assert_eq!(bin(Value::Datetime(epoch), PgType::Timestamptz), 0i64.to_be_bytes());
		// One second later.
		let later: Datetime = "2000-01-01T00:00:01Z".parse().unwrap();
		assert_eq!(bin(Value::Datetime(later), PgType::Timestamptz), 1_000_000i64.to_be_bytes());
	}

	#[test]
	fn binary_interval_layout() {
		use std::time::Duration;
		// 90061s = 1 day + 1h1m1s -> micros field, then days=0, months=0.
		let out = bin(Value::Duration(Duration::from_secs(90_061).into()), PgType::Interval);
		assert_eq!(out.len(), 16);
		assert_eq!(&out[0..8], &(90_061i64 * 1_000_000).to_be_bytes());
		assert_eq!(&out[8..12], &0i32.to_be_bytes());
		assert_eq!(&out[12..16], &0i32.to_be_bytes());
	}

	#[test]
	fn binary_jsonb_has_version_byte() {
		let out = bin(Value::Array(vec![Value::Number(Number::Int(1))].into()), PgType::Jsonb);
		assert_eq!(out[0], 1);
		assert_eq!(&out[1..], b"[1]");
	}

	/// Decode the numeric binary header and digit groups for assertions.
	fn numeric_parts(s: &str) -> (i16, i16, u16, i16, Vec<i16>) {
		let bytes = encode_numeric_binary(s);
		let rd16 = |i: usize| i16::from_be_bytes([bytes[i], bytes[i + 1]]);
		let ndigits = rd16(0);
		let weight = rd16(2);
		let sign = u16::from_be_bytes([bytes[4], bytes[5]]);
		let dscale = rd16(6);
		let digits = (0..ndigits as usize).map(|i| rd16(8 + i * 2)).collect();
		(ndigits, weight, sign, dscale, digits)
	}

	#[test]
	fn numeric_binary_known_vectors() {
		// 12345 = 1 * 10000 + 2345 -> two groups [1, 2345], weight 1, scale 0.
		assert_eq!(numeric_parts("12345"), (2, 1, 0x0000, 0, vec![1, 2345]));
		// Negative sign.
		assert_eq!(numeric_parts("-1"), (1, 0, 0x4000, 0, vec![1]));
		// Pure fraction 0.001234 -> digits [12, 3400], weight -1, scale 6.
		assert_eq!(numeric_parts("0.001234"), (2, -1, 0x0000, 6, vec![12, 3400]));
		// Zero keeps its declared scale but has no digit groups.
		assert_eq!(numeric_parts("0.00"), (0, 0, 0x0000, 2, vec![]));
		// NaN.
		assert_eq!(numeric_parts("NaN"), (0, 0, 0xC000, 0, vec![]));
	}

	#[test]
	fn decode_text_params() {
		assert_eq!(decode_param(Some(b"42"), OID_INT8, 0).unwrap(), Value::Number(Number::Int(42)));
		assert_eq!(decode_param(Some(b"t"), OID_BOOL, 0).unwrap(), Value::Bool(true));
		assert_eq!(
			decode_param(Some(b"hi"), OID_TEXT, 0).unwrap(),
			Value::String("hi".to_string())
		);
		// Unspecified type falls back to a string.
		assert_eq!(decode_param(Some(b"5"), 0, 0).unwrap(), Value::String("5".to_string()));
		// NULL.
		assert_eq!(decode_param(None, OID_INT8, 0).unwrap(), Value::Null);
	}

	#[test]
	fn decode_binary_params_roundtrip() {
		let raw = 12345i64.to_be_bytes();
		assert_eq!(
			decode_param(Some(&raw), OID_INT8, 1).unwrap(),
			Value::Number(Number::Int(12345))
		);
		let raw = 2.5f64.to_be_bytes();
		assert_eq!(
			decode_param(Some(&raw), OID_FLOAT8, 1).unwrap(),
			Value::Number(Number::Float(2.5))
		);
	}

	#[test]
	fn numeric_binary_round_trips() {
		// Whatever the encoder emits for a decimal, the decoder must recover it.
		for s in ["12345", "-1", "0.001234", "42", "-9876.5432", "0"] {
			let wire = encode_numeric_binary(s);
			let decoded = decode_param(Some(&wire), OID_NUMERIC, 1).unwrap();
			let expected = Decimal::from_str(s).unwrap();
			assert_eq!(decoded, Value::Number(Number::Decimal(expected)), "for {s}");
		}
	}

	#[test]
	fn numeric_binary_nan_is_rejected() {
		let wire = encode_numeric_binary("NaN");
		assert!(decode_param(Some(&wire), OID_NUMERIC, 1).is_err());
	}

	#[test]
	fn unknown_binary_oid_errors() {
		// An unmodelled binary type is rejected rather than silently kept as bytes.
		assert!(decode_param(Some(&[0u8, 1, 2, 3]), 99999, 1).is_err());
	}

	#[test]
	fn unspecified_binary_oid_falls_back_to_string() {
		// A binary-format parameter with OID 0 (unspecified) decodes as a string,
		// mirroring the text path, instead of erroring on the format choice.
		assert_eq!(decode_param(Some(b"hello"), 0, 1).unwrap(), Value::String("hello".to_string()));
	}

	#[test]
	fn numeric_column_renders_non_finite_as_nan() {
		// Postgres `numeric` accepts only `NaN` among non-finite forms.
		assert_eq!(text(Value::Number(Number::Float(f64::INFINITY)), PgType::Numeric), "NaN");
		assert_eq!(text(Value::Number(Number::Float(f64::NEG_INFINITY)), PgType::Numeric), "NaN");
		assert_eq!(text(Value::Number(Number::Float(f64::NAN)), PgType::Numeric), "NaN");
		// Finite floats still render normally in a numeric column.
		assert_eq!(text(Value::Number(Number::Float(1.5)), PgType::Numeric), "1.5");
	}

	#[test]
	fn parses_offsetless_and_hours_only_timestamps() {
		let expected: Datetime = "2024-01-15T12:34:56Z".parse().unwrap();
		// `timestamp` text (no offset) is assumed UTC.
		assert_eq!(
			decode_param(Some(b"2024-01-15 12:34:56"), OID_TIMESTAMP, 0).unwrap(),
			Value::Datetime(expected)
		);
		// `timestamptz` text uses an hours-only offset like `+00`.
		assert_eq!(
			decode_param(Some(b"2024-01-15 12:34:56+00"), OID_TIMESTAMPTZ, 0).unwrap(),
			Value::Datetime(expected)
		);
		// A non-zero hours-only offset is applied (07:34-05:00 == 12:34Z).
		assert_eq!(
			decode_param(Some(b"2024-01-15 07:34:56-05"), OID_TIMESTAMPTZ, 0).unwrap(),
			Value::Datetime(expected)
		);
		// Fractional seconds still parse.
		let frac: Datetime = "2024-01-15T12:34:56.5Z".parse().unwrap();
		assert_eq!(
			decode_param(Some(b"2024-01-15 12:34:56.5+00"), OID_TIMESTAMPTZ, 0).unwrap(),
			Value::Datetime(frac)
		);
	}

	#[test]
	fn numeric_binary_dscale_does_not_round_real_digits() {
		// A hand-crafted numeric wire value for 1.5 but with a lying dscale of 0
		// must NOT be rounded to 2.
		let mut wire = Vec::new();
		wire.extend_from_slice(&2i16.to_be_bytes()); // ndigits
		wire.extend_from_slice(&0i16.to_be_bytes()); // weight
		wire.extend_from_slice(&0u16.to_be_bytes()); // sign: positive
		wire.extend_from_slice(&0i16.to_be_bytes()); // dscale: 0 (too small)
		wire.extend_from_slice(&1i16.to_be_bytes()); // group 0: 1
		wire.extend_from_slice(&5000i16.to_be_bytes()); // group 1: .5000
		let Value::Number(Number::Decimal(d)) = decode_param(Some(&wire), OID_NUMERIC, 1).unwrap()
		else {
			panic!("expected a decimal");
		};
		assert_eq!(d.normalize(), Decimal::from_str("1.5").unwrap());
	}

	#[test]
	fn timestamptz_renders_bc_years() {
		use chrono::TimeZone;
		// Astronomical year 0 is 1 BC; -44 is 45 BC.
		let y0 = chrono::Utc.with_ymd_and_hms(0, 1, 1, 0, 0, 0).unwrap();
		assert_eq!(String::from_utf8(timestamptz_text(y0)).unwrap(), "0001-01-01 00:00:00+00 BC");
		let ym44 = chrono::Utc.with_ymd_and_hms(-44, 3, 15, 0, 0, 0).unwrap();
		assert_eq!(String::from_utf8(timestamptz_text(ym44)).unwrap(), "0045-03-15 00:00:00+00 BC");
		// A normal AD year is unaffected.
		let ad = chrono::Utc.with_ymd_and_hms(2024, 1, 15, 12, 34, 56).unwrap();
		assert_eq!(String::from_utf8(timestamptz_text(ad)).unwrap(), "2024-01-15 12:34:56+00");
	}

	#[test]
	fn bc_timestamp_round_trips() {
		use chrono::TimeZone;
		// A BC value the server renders must parse back to the same instant when
		// returned as a text parameter (the ` BC` era tag is accepted on decode).
		let dt = chrono::Utc.with_ymd_and_hms(-44, 3, 15, 0, 0, 0).unwrap();
		let text = String::from_utf8(timestamptz_text(dt)).unwrap();
		assert_eq!(text, "0045-03-15 00:00:00+00 BC");
		assert_eq!(parse_datetime(&text), utc_datetime(dt));
		// Year 0 (== 1 BC) round-trips too.
		let y0 = chrono::Utc.with_ymd_and_hms(0, 1, 1, 0, 0, 0).unwrap();
		let y0_text = String::from_utf8(timestamptz_text(y0)).unwrap();
		assert_eq!(parse_datetime(&y0_text), utc_datetime(y0));
	}

	#[test]
	fn interval_binary_round_trips() {
		use std::time::Duration;
		let wire =
			encode_binary(Value::Duration(Duration::from_secs(90_061).into()), &PgType::Interval)
				.unwrap();
		assert_eq!(
			decode_param(Some(&wire), OID_INTERVAL, 1).unwrap(),
			Value::Duration(Duration::from_secs(90_061).into())
		);
	}

	#[test]
	fn int2_int4_codecs_round_trip() {
		// Int2
		let val = Value::Number(Number::Int(42));
		let text_bytes = encode_text(val.clone(), &PgType::Int2).unwrap();
		assert_eq!(String::from_utf8(text_bytes).unwrap(), "42");
		let bin_bytes = encode_binary(val.clone(), &PgType::Int2).unwrap();
		assert_eq!(bin_bytes, 42i16.to_be_bytes());
		assert_eq!(decode_param(Some(b"42"), OID_INT2, 0).unwrap(), val);
		assert_eq!(decode_param(Some(&bin_bytes), OID_INT2, 1).unwrap(), val);

		// Int4
		let val4 = Value::Number(Number::Int(123456));
		let text4 = encode_text(val4.clone(), &PgType::Int4).unwrap();
		assert_eq!(String::from_utf8(text4).unwrap(), "123456");
		let bin4 = encode_binary(val4.clone(), &PgType::Int4).unwrap();
		assert_eq!(bin4, 123456i32.to_be_bytes());
		assert_eq!(decode_param(Some(b"123456"), OID_INT4, 0).unwrap(), val4);
		assert_eq!(decode_param(Some(&bin4), OID_INT4, 1).unwrap(), val4);
	}

	#[test]
	fn float4_codecs_round_trip() {
		let val = Value::Number(Number::Float(12.5));
		let text_bytes = encode_text(val.clone(), &PgType::Float4).unwrap();
		assert_eq!(String::from_utf8(text_bytes).unwrap(), "12.5");
		let bin_bytes = encode_binary(val.clone(), &PgType::Float4).unwrap();
		assert_eq!(bin_bytes, 12.5f32.to_be_bytes());
		assert_eq!(decode_param(Some(b"12.5"), OID_FLOAT4, 0).unwrap(), val);
		assert_eq!(decode_param(Some(&bin_bytes), OID_FLOAT4, 1).unwrap(), val);
	}

	#[test]
	fn date_time_timetz_codecs() {
		// Date
		let date_str = "2024-03-15";
		let val_date = Value::String(date_str.to_string());
		let text_date = encode_text(val_date.clone(), &PgType::Date).unwrap();
		assert_eq!(String::from_utf8(text_date).unwrap(), "2024-03-15");
		let bin_date = encode_binary(val_date, &PgType::Date).unwrap();
		// 2024-03-15 is days since 2000-01-01
		let pg_epoch = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
		let target = chrono::NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
		let days = (target - pg_epoch).num_days() as i32;
		assert_eq!(bin_date, days.to_be_bytes());

		// Time
		let time_str = "14:30:15.5";
		let val_time = Value::String(time_str.to_string());
		let text_time = encode_text(val_time.clone(), &PgType::Time).unwrap();
		assert_eq!(String::from_utf8(text_time).unwrap(), "14:30:15.5");
		let bin_time = encode_binary(val_time, &PgType::Time).unwrap();
		let micros: i64 = (14 * 3600 + 30 * 60 + 15) * 1_000_000 + 500_000;
		assert_eq!(bin_time, micros.to_be_bytes());

		// Timetz
		let val_timetz = Value::String("14:30:15+00".to_string());
		let text_timetz = encode_text(val_timetz.clone(), &PgType::Timetz).unwrap();
		assert_eq!(String::from_utf8(text_timetz).unwrap(), "14:30:15+00");
		let bin_timetz = encode_binary(val_timetz, &PgType::Timetz).unwrap();
		assert_eq!(bin_timetz.len(), 12);
	}

	#[test]
	fn macaddr_and_network_codecs() {
		let mac = Value::String("08:00:2b:01:02:03".to_string());
		let text_mac = encode_text(mac.clone(), &PgType::MacAddr).unwrap();
		assert_eq!(String::from_utf8(text_mac).unwrap(), "08:00:2b:01:02:03");
		let bin_mac = encode_binary(mac.clone(), &PgType::MacAddr).unwrap();
		assert_eq!(bin_mac, vec![0x08, 0x00, 0x2b, 0x01, 0x02, 0x03]);
		assert_eq!(decode_param(Some(&bin_mac), OID_MACADDR, 1).unwrap(), mac);

		// Inet
		let ip = Value::String("192.168.1.100/24".to_string());
		let bin_ip = encode_binary(ip.clone(), &PgType::Inet).unwrap();
		let decoded_ip = decode_param(Some(&bin_ip), OID_INET, 1).unwrap();
		assert_eq!(decoded_ip, ip);
	}

	#[test]
	fn array_codecs_text_and_binary() {
		let arr = Value::Array(
			vec![
				Value::Number(Number::Int(1)),
				Value::Number(Number::Int(2)),
				Value::Number(Number::Int(3)),
			]
			.into(),
		);
		let text_arr = encode_text(arr.clone(), &PgType::Array(Box::new(PgType::Int4))).unwrap();
		assert_eq!(String::from_utf8(text_arr).unwrap(), "{1,2,3}");
		let bin_arr = encode_binary(arr.clone(), &PgType::Array(Box::new(PgType::Int4))).unwrap();
		let decoded_bin = decode_param(Some(&bin_arr), OID_ARRAY_INT4, 1).unwrap();
		assert_eq!(decoded_bin, arr);

		// Text array with quotes and null
		let str_arr = Value::Array(
			vec![
				Value::String("hello, world".to_string()),
				Value::Null,
				Value::String("foo\"bar".to_string()),
			]
			.into(),
		);
		let text_str_arr =
			encode_text(str_arr.clone(), &PgType::Array(Box::new(PgType::Text))).unwrap();
		assert_eq!(
			String::from_utf8(text_str_arr.clone()).unwrap(),
			"{\"hello, world\",NULL,\"foo\\\"bar\"}"
		);
		let decoded_text = decode_param(Some(&text_str_arr), OID_ARRAY_TEXT, 0).unwrap();
		assert_eq!(decoded_text, str_arr);
	}

	#[test]
	fn bytea_escape_decoding() {
		// Hex format
		assert_eq!(
			decode_bytea_text("\\xdeadbeef").unwrap(),
			Value::Bytes(vec![0xde, 0xad, 0xbe, 0xef].into())
		);
		// Escape format with octal
		assert_eq!(
			decode_bytea_text("\\\\abc\\001").unwrap(),
			Value::Bytes(vec![b'\\', b'a', b'b', b'c', 1].into())
		);
	}

	#[test]
	fn record_codecs() {
		let mut obj = surrealdb_types::Object::new();
		obj.insert("a".to_string(), Value::Number(Number::Int(1)));
		obj.insert("b".to_string(), Value::String("test".to_string()));
		let val = Value::Object(obj);
		let text_rec = encode_text(val.clone(), &PgType::Record).unwrap();
		assert_eq!(String::from_utf8(text_rec).unwrap(), "(1,test)");
		let bin_rec = encode_binary(val, &PgType::Record).unwrap();
		assert!(!bin_rec.is_empty());
	}
}
