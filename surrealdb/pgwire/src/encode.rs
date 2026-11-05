use std::fmt::Write as _;
use std::io::Write as _;
use std::str::FromStr;

use chrono::Timelike as _;
use rust_decimal::Decimal;
use surrealdb_sqlplan::array_literal::{
	ArrayElement, MAX_ARRAY_DIMENSIONS, is_array_space, parse_array_literal,
};
use surrealdb_sqlplan::descriptor::{
	ENUM_ARRAY_OID_OFFSET as ENUM_ARRAY_OFFSET, ENUM_TYPE_OID_BASE as ENUM_TYPE_BASE,
	VECTOR_ARRAY_OID, VECTOR_OID,
};
use surrealdb_types::{
	BigDecimal, Bytes, Date, Datetime, Number, Timestamp, ToSql, Uuid, Value, Wide,
};

use super::error::PgError;
use super::families::{self, Family};
use super::typing::{PgType, base_type, rectangular_dims};

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
const OID_VECTOR: i32 = VECTOR_OID;
const OID_ARRAY_VECTOR: i32 = VECTOR_ARRAY_OID;
const ENUM_TYPE_OID_BASE: i32 = ENUM_TYPE_BASE as i32;
const ENUM_ARRAY_OID_OFFSET: i32 = ENUM_ARRAY_OFFSET as i32;

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
/// The days from 1970-01-01 to 2000-01-01, the epoch of the binary `date`.
const PG_EPOCH_UNIX_DAYS: i32 = 10_957;

/// The sign of an infinite date, timestamp or datetime: `Some(true)` for `infinity` and
/// `Some(false)` for `-infinity`. Those are the greatest and least values of each kind, so they
/// order after and before every finite value and are written as `infinity` in the text form and as
/// the maximum and minimum of the integer types in the binary form. `None` for any other value.
fn infinite_sign(value: &Value) -> Option<bool> {
	if let Value::Date(d) = value {
		return d.is_infinite().then_some(*d == Date::infinity());
	}
	if let Value::Timestamp(t) = value {
		return t.is_infinite().then_some(*t == Timestamp::infinity());
	}
	if let Value::Datetime(dt) = value {
		return dt.is_infinite().then_some(*dt == Datetime::MAX_UTC);
	}
	None
}

/// The binary form of an infinite value of the given sign for the column type: the maximum or
/// minimum of the `date` day count (`i32`) or of the `timestamp` and `timestamptz` microsecond
/// count (`i64`). `None` for any other column type, which the caller encodes as usual.
fn infinite_binary(ty: &PgType, positive: bool) -> Option<Vec<u8>> {
	if matches!(ty, PgType::Date) {
		let days = if positive {
			i32::MAX
		} else {
			i32::MIN
		};
		return Some(days.to_be_bytes().to_vec());
	}
	if matches!(ty, PgType::Timestamp | PgType::Timestamptz) {
		let micros = if positive {
			i64::MAX
		} else {
			i64::MIN
		};
		return Some(micros.to_be_bytes().to_vec());
	}
	None
}

/// The sign of the text `infinity`, `+infinity` or `-infinity`, in any letter case, as PostgreSQL
/// reads them for `date`, `timestamp` and `timestamptz`. `None` for any other text.
fn infinity_text_sign(text: &str) -> Option<bool> {
	let (positive, rest) = match text.strip_prefix('-') {
		Some(rest) => (false, rest),
		None => (true, text.strip_prefix('+').unwrap_or(text)),
	};
	rest.eq_ignore_ascii_case("infinity").then_some(positive)
}

/// The text form of an infinite date, timestamp or datetime.
fn infinity_text(positive: bool) -> Vec<u8> {
	if positive {
		b"infinity".to_vec()
	} else {
		b"-infinity".to_vec()
	}
}

/// The microseconds since 2000-01-01 00:00:00 of the wall time a `timestamp` or `date` names, read
/// as UTC where the column has a zone; `None` for any other value.
fn wall_micros_since_pg_epoch(value: &Value) -> Option<Result<i64, PgError>> {
	let unix_micros = match value {
		Value::Timestamp(t) => t
			.seconds()
			.checked_mul(1_000_000)
			.and_then(|m| m.checked_add(i64::from(t.nanos() / 1_000))),
		Value::Date(d) => i64::from(d.days()).checked_mul(86_400_000_000),
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
		| Value::Interval(_) => return None,
	};
	Some(
		unix_micros
			.and_then(|m| m.checked_sub(PG_EPOCH_UNIX_MICROS))
			.ok_or_else(|| PgError::new("22008", "timestamp out of range")),
	)
}

/// Encode one cell into the Postgres text format for its column type.
///
/// Column types are inferred from the values themselves (see
/// [`super::typing::shape_result`]), so a mismatch here is an internal
/// invariant failure, not a user error.
pub(super) fn encode_text(value: Value, ty: &PgType) -> Result<Vec<u8>, PgError> {
	if let Some(positive) = infinite_sign(&value)
		&& matches!(ty, PgType::Date | PgType::Timestamp | PgType::Timestamptz)
	{
		return Ok(infinity_text(positive));
	}
	match ty {
		// `void` is the empty string in both formats.
		PgType::Void => Ok(Vec::new()),
		PgType::Bool => {
			if let Value::Bool(b) = value {
				Ok(if b {
					b"t".to_vec()
				} else {
					b"f".to_vec()
				})
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Int2 => {
			if let Value::Number(ref n) = value {
				match n.wide() {
					Wide::Int(i) => Ok(narrow::<i16>(i, ty)?.to_string().into_bytes()),
					Wide::Float(_) | Wide::Decimal(_) | Wide::BigDecimal(_) => {
						Err(mismatch(&value, ty))
					}
				}
			} else if let Value::String(ref s) = value
				&& s.parse::<i16>().is_ok()
			{
				Ok(s.clone().into_bytes())
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Int4 => {
			if let Value::Number(ref n) = value {
				match n.wide() {
					Wide::Int(i) => Ok(narrow::<i32>(i, ty)?.to_string().into_bytes()),
					Wide::Float(_) | Wide::Decimal(_) | Wide::BigDecimal(_) => {
						Err(mismatch(&value, ty))
					}
				}
			} else if let Value::String(ref s) = value
				&& s.parse::<i32>().is_ok()
			{
				Ok(s.clone().into_bytes())
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Int8 => {
			if let Value::Number(ref n) = value {
				match n.wide() {
					Wide::Int(i) => Ok(i.to_string().into_bytes()),
					Wide::Float(_) | Wide::Decimal(_) | Wide::BigDecimal(_) => {
						Err(mismatch(&value, ty))
					}
				}
			} else if let Value::String(ref s) = value
				&& s.parse::<i64>().is_ok()
			{
				Ok(s.clone().into_bytes())
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Oid | PgType::Regclass => {
			if let Value::Number(ref n) = value {
				match n.wide() {
					Wide::Int(i) => Ok((i as u32).to_string().into_bytes()),
					Wide::Float(_) | Wide::Decimal(_) | Wide::BigDecimal(_) => {
						Err(mismatch(&value, ty))
					}
				}
			} else if let Value::String(s) = value {
				Ok(s.into_bytes())
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Float4 => {
			if let Value::Number(ref n) = value {
				if let Number::F32(f) = *n {
					Ok(float32_text(f))
				} else if let Some(f) = n.to_f64() {
					Ok(float_text(f))
				} else {
					Err(mismatch(&value, ty))
				}
			} else if let Value::String(ref s) = value
				&& s.parse::<f32>().is_ok()
			{
				Ok(s.clone().into_bytes())
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Float8 => {
			// Every number is written with the digits of the `f64` it equals, as the binary format
			// carries it: an `f32` is not shortened to its own digits.
			if let Value::Number(ref n) = value
				&& let Some(f) = n.to_f64()
			{
				Ok(float_text(f))
			} else if let Value::String(ref s) = value
				&& s.parse::<f64>().is_ok()
			{
				Ok(s.clone().into_bytes())
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Numeric => {
			if let Value::Number(ref n) = value {
				if let Number::F32(f) = *n {
					Ok(numeric_float32_text(f))
				} else {
					match n.wide() {
						Wide::Int(i) => Ok(i.to_string().into_bytes()),
						Wide::Decimal(d) => Ok(d.to_string().into_bytes()),
						Wide::Float(f) => Ok(numeric_float_text(f)),
						Wide::BigDecimal(d) => Ok(d.to_string().into_bytes()),
					}
				}
			} else if let Value::String(s) = value {
				Ok(s.into_bytes())
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Char => {
			if let Value::String(s) = value {
				let first = s.chars().next().unwrap_or('\0');
				Ok(first.to_string().into_bytes())
			} else if let Value::Number(ref n) = value {
				match n.wide() {
					Wide::Int(i) => Ok(vec![i as u8]),
					Wide::Float(_) | Wide::Decimal(_) | Wide::BigDecimal(_) => {
						Err(mismatch(&value, ty))
					}
				}
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Name | PgType::Text | PgType::Bpchar | PgType::Varchar | PgType::Enum(_) => {
			text_form(value, ty)
		}
		PgType::Inet
		| PgType::Cidr
		| PgType::MacAddr
		| PgType::Macaddr8
		| PgType::Bit
		| PgType::Varbit
		| PgType::Money
		| PgType::PgLsn
		| PgType::Regproc
		| PgType::Regprocedure
		| PgType::Regoper
		| PgType::Regoperator
		| PgType::Regtype
		| PgType::Regnamespace
		| PgType::Regrole
		| PgType::Regconfig
		| PgType::Regdictionary
		| PgType::Regcollation
		| PgType::Xid
		| PgType::Cid
		| PgType::Xid8
		| PgType::Point
		| PgType::Lseg
		| PgType::Path
		| PgType::PgBox
		| PgType::Polygon
		| PgType::Line
		| PgType::Circle
		| PgType::Int4range
		| PgType::Numrange
		| PgType::Tsrange
		| PgType::Tstzrange
		| PgType::Daterange
		| PgType::Int8range
		| PgType::Int4multirange
		| PgType::Nummultirange
		| PgType::Tsmultirange
		| PgType::Tstzmultirange
		| PgType::Datemultirange
		| PgType::Int8multirange => match Family::of(ty) {
			Some(family) => families::encode_text(&family, value, ty),
			None => Err(mismatch(&value, ty)),
		},
		PgType::Xml
		| PgType::Tsvector
		| PgType::Tsquery
		| PgType::Jsonpath
		| PgType::Tid
		| PgType::PgSnapshot
		| PgType::TxidSnapshot => text_form(value, ty),
		PgType::Date => {
			if let Value::Datetime(dt) = value {
				Ok(date_text(dt.into_inner().date_naive()))
			} else if let Value::Date(d) = value {
				Ok(date_text(d.into_inner()))
			} else if let Value::Timestamp(t) = value {
				Ok(date_text(t.into_inner().date()))
			} else if let Value::String(s) = value {
				if let Some(nd) = parse_date(&s) {
					Ok(date_text(nd))
				} else {
					Ok(s.into_bytes())
				}
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Time => {
			if let Value::Datetime(dt) = value {
				Ok(time_text(dt.into_inner().time()))
			} else if let Value::Time(t) = value {
				Ok(time_nanos_text(t.nanos()))
			} else if let Value::Timestamp(t) = value {
				Ok(time_nanos_text(time_of_day_nanos(&t)))
			} else if let Value::Duration(d) = value {
				Ok(interval_text(d.into_inner()))
			} else if let Value::String(s) = value {
				Ok(s.into_bytes())
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Timetz => {
			if let Value::Datetime(dt) = value {
				Ok(timetz_text(dt.into_inner()))
			} else if let Value::Time(t) = value {
				let mut out = time_nanos_text(t.nanos());
				out.extend_from_slice(b"+00");
				Ok(out)
			} else if let Value::Duration(d) = value {
				let mut out = interval_text(d.into_inner());
				out.extend_from_slice(b"+00");
				Ok(out)
			} else if let Value::String(s) = value {
				Ok(s.into_bytes())
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Timestamp => {
			if let Value::Datetime(dt) = value {
				Ok(timestamp_text(dt.into_inner()))
			} else if let Value::Timestamp(t) = value {
				Ok(timestamp_text(t.into_inner().and_utc()))
			} else if let Value::Date(d) = value {
				Ok(timestamp_text(midnight_utc(d.into_inner())))
			} else if let Value::String(s) = value {
				Ok(s.into_bytes())
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Timestamptz => {
			if let Value::Datetime(dt) = value {
				Ok(timestamptz_text(dt.into_inner()))
			} else if let Value::Timestamp(t) = value {
				Ok(timestamptz_text(t.into_inner().and_utc()))
			} else if let Value::Date(d) = value {
				Ok(timestamptz_text(midnight_utc(d.into_inner())))
			} else if let Value::String(s) = value {
				Ok(s.into_bytes())
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Interval => {
			if let Value::Duration(d) = value {
				Ok(interval_text(d.into_inner()))
			} else if let Value::Interval(i) = value {
				Ok(interval_postgres_text(i.months(), i.days(), i.micros()))
			} else if let Value::Time(t) = value {
				Ok(interval_postgres_text(0, 0, (t.nanos() / 1_000) as i64))
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Uuid => {
			if let Value::Uuid(u) = value {
				Ok(u.to_string().into_bytes())
			} else if let Value::String(s) = value {
				Ok(s.into_bytes())
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Bytea => {
			if let Value::Bytes(b) = value {
				Ok(bytea_hex(&b.into_inner()).into_bytes())
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Json | PgType::Jsonb => {
			let mut json = Vec::new();
			value.write_sql_json(&mut json);
			Ok(json)
		}
		PgType::Vector => crate::vector::text(value),
		PgType::Composite {
			fields,
			..
		} => {
			if let Value::Object(obj) = value {
				composite_text(&obj, fields)
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Record => {
			if let Value::Object(obj) = value {
				record_text(&obj)
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Array(elem_ty) => {
			if let Value::Array(arr) = value {
				check_rectangular(arr.as_slice(), elem_ty)?;
				array_text(arr.as_slice(), elem_ty)
			} else if let Value::String(s) = value {
				Ok(s.into_bytes())
			} else {
				Err(mismatch(&value, ty))
			}
		}
	}
}

/// The text a value takes in a `text`-family column, whatever the value's own
/// kind: the text of the column type the value has on its own
/// ([`base_type`]), so a column inferred from its rows and a column declared
/// as text show one value in one form. A string is its own text, and a record
/// id, range, regex, file or table is its SurrealQL source; every other kind
/// is encoded by [`encode_text`] for its own type. None of those types is a
/// text type, so that call cannot come back to this function.
fn text_form(value: Value, ty: &PgType) -> Result<Vec<u8>, PgError> {
	match value {
		Value::String(s) => Ok(s.into_bytes()),
		v @ (Value::RecordId(_)
		| Value::Range(_)
		| Value::Regex(_)
		| Value::File(_)
		| Value::Table(_)) => Ok(v.to_sql().into_bytes()),
		v @ (Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::Bytes(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_)
		| Value::Geometry(_)
		| Value::Array(_)
		| Value::Object(_)
		| Value::Set(_)) => match base_type(&v) {
			Some(own) => encode_text(v, &own),
			None => Err(mismatch(&v, ty)),
		},
	}
}

/// The `bytea` hex text form: a backslash and `x`, then two lowercase digits per byte.
fn bytea_hex(bytes: &[u8]) -> String {
	let mut out = String::with_capacity(2 + bytes.len() * 2);
	out.push_str("\\x");
	for byte in bytes {
		write!(out, "{byte:02x}").expect("writing to a String cannot fail");
	}
	out
}

pub(super) fn mismatch(value: &Value, ty: &PgType) -> PgError {
	PgError::internal(format!("cannot encode a {} cell into a {ty:?} column", value.kind()))
}

/// An integer cell in the width of its `smallint` or `integer` column. A value the column cannot
/// hold is refused as PostgreSQL refuses it, never wrapped.
fn narrow<T: TryFrom<i64>>(value: i64, ty: &PgType) -> Result<T, PgError> {
	T::try_from(value).map_err(|_| {
		let name = if *ty == PgType::Int2 {
			"smallint"
		} else {
			"integer"
		};
		PgError::new("22003", format!("{name} out of range"))
	})
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
	let mut out = Vec::with_capacity(15);
	write!(out, "{:02}:{:02}:{:02}", nt.hour(), nt.minute(), nt.second())
		.expect("writing to a Vec cannot fail");
	push_fraction(&mut out, nt.nanosecond() / 1_000);
	out
}

fn timetz_text(dt: chrono::DateTime<chrono::Utc>) -> Vec<u8> {
	let mut out = time_text(dt.time());
	out.extend_from_slice(b"+00");
	out
}

/// Render a UTC datetime in the Postgres `timestamp` output style, e.g.
/// `2024-01-15 12:34:56.123456`; see [`datetime_text`].
fn timestamp_text(dt: chrono::DateTime<chrono::Utc>) -> Vec<u8> {
	datetime_text(dt, false)
}

/// Render a UTC datetime in the Postgres `timestamptz` output style, e.g.
/// `2024-01-15 12:34:56.123456+00`; see [`datetime_text`].
fn timestamptz_text(dt: chrono::DateTime<chrono::Utc>) -> Vec<u8> {
	datetime_text(dt, true)
}

/// The fields of a UTC datetime in the Postgres output style, followed by the
/// `+00` zone when `zone` is set. Nanoseconds truncate to microsecond
/// precision (Postgres timestamps carry microseconds), and trailing fractional
/// zeros are trimmed: the fractional part is dropped entirely when zero,
/// matching Postgres. Postgres never prints a sign on the year: a proleptic
/// year <= 0 is rendered as `NNNN ... BC` (astronomical year 0 is 1 BC), with
/// the era tag appended after the zone. chrono's `%Y` would instead emit a
/// negative or under-padded year, which no Postgres client expects.
fn datetime_text(dt: chrono::DateTime<chrono::Utc>, zone: bool) -> Vec<u8> {
	use chrono::{Datelike, Timelike};
	let year = dt.year();
	let (display_year, era) = if year <= 0 {
		(1 - year, " BC")
	} else {
		(year, "")
	};
	let mut out = Vec::with_capacity(32);
	write!(
		out,
		"{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
		display_year,
		dt.month(),
		dt.day(),
		dt.hour(),
		dt.minute(),
		dt.second()
	)
	.expect("writing to a Vec cannot fail");
	push_fraction(&mut out, dt.timestamp_subsec_micros());
	if zone {
		out.extend_from_slice(b"+00");
	}
	out.extend_from_slice(era.as_bytes());
	out
}

/// The nanoseconds since midnight of the time of day of a timestamp.
fn time_of_day_nanos(t: &surrealdb_types::Timestamp) -> u64 {
	let time = t.into_inner().time();
	u64::from(time.num_seconds_from_midnight()) * 1_000_000_000 + u64::from(time.nanosecond())
}

/// The UTC midnight that opens `date`.
fn midnight_utc(date: chrono::NaiveDate) -> chrono::DateTime<chrono::Utc> {
	date.and_time(chrono::NaiveTime::MIN).and_utc()
}

/// A time of day, given as nanoseconds since midnight, in the `time` output style. `24:00:00`,
/// which `time` admits, is written as such.
fn time_nanos_text(nanos: u64) -> Vec<u8> {
	let micros = nanos / 1_000;
	let secs = micros / 1_000_000;
	let mut out = Vec::with_capacity(15);
	write!(out, "{:02}:{:02}:{:02}", secs / 3600, secs / 60 % 60, secs % 60)
		.expect("writing to a Vec cannot fail");
	push_fraction(&mut out, (micros % 1_000_000) as u32);
	out
}

/// An interval in the PostgreSQL `postgres` output style, `1 year 2 mons 3 days 04:05:06.5`: each
/// non-zero field of the date part, then the time part, with a `+` on a positive field that
/// follows a negative one, and `00:00:00` for the zero interval.
fn interval_postgres_text(months: i32, days: i32, micros: i64) -> Vec<u8> {
	let (years, mons) = (months / 12, months % 12);
	let mut out = Vec::with_capacity(48);
	let mut is_zero = true;
	let mut is_before = false;
	for (value, unit) in
		[(i64::from(years), "year"), (i64::from(mons), "mon"), (i64::from(days), "day")]
	{
		if value == 0 {
			continue;
		}
		if !is_zero {
			out.push(b' ');
		}
		if is_before && value > 0 {
			out.push(b'+');
		}
		let plural = if value == 1 {
			""
		} else {
			"s"
		};
		write!(out, "{value} {unit}{plural}").expect("writing to a Vec cannot fail");
		is_before = value < 0;
		is_zero = false;
	}
	if is_zero || micros != 0 {
		if !is_zero {
			out.push(b' ');
		}
		if micros < 0 {
			out.push(b'-');
		} else if is_before {
			out.push(b'+');
		}
		let abs = micros.unsigned_abs();
		let secs = abs / 1_000_000;
		write!(out, "{:02}:{:02}:{:02}", secs / 3600, secs / 60 % 60, secs % 60)
			.expect("writing to a Vec cannot fail");
		push_fraction(&mut out, (abs % 1_000_000) as u32);
	}
	out
}

/// Append the fractional seconds of `micros` as `.` and up to six digits with
/// trailing zeros trimmed, or nothing when there are none. A count of a whole
/// second or more (chrono's leap-second range) keeps only its sub-second part.
fn push_fraction(out: &mut Vec<u8>, micros: u32) {
	let micros = micros % 1_000_000;
	if micros == 0 {
		return;
	}
	let mut digits = [b'0'; 6];
	let mut rest = micros;
	for digit in digits.iter_mut().rev() {
		*digit = b'0' + (rest % 10) as u8;
		rest /= 10;
	}
	let len = digits.iter().rposition(|digit| *digit != b'0').map_or(0, |last| last + 1);
	out.push(b'.');
	out.extend_from_slice(&digits[..len]);
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

fn float32_display(f: f32) -> String {
	if f.is_nan() {
		"NaN".to_string()
	} else if f == f32::INFINITY {
		"Infinity".to_string()
	} else if f == f32::NEG_INFINITY {
		"-Infinity".to_string()
	} else {
		format!("{f}")
	}
}

fn float_text(f: f64) -> Vec<u8> {
	float_display(f).into_bytes()
}

fn float32_text(f: f32) -> Vec<u8> {
	float32_display(f).into_bytes()
}

/// Render a float for a `numeric` column. A `numeric` holds `NaN`, `Infinity` and `-Infinity`,
/// which are rendered as a float renders them.
fn numeric_float_text(f: f64) -> Vec<u8> {
	float_display(f).into_bytes()
}

fn numeric_float32_text(f: f32) -> Vec<u8> {
	float32_display(f).into_bytes()
}

/// Render a duration in the Postgres default interval output style, e.g.
/// `3 days 04:05:06.789`. Sub-microsecond precision is truncated.
fn interval_text(d: std::time::Duration) -> Vec<u8> {
	let secs = d.as_secs();
	let micros = d.subsec_micros();
	let days = secs / 86_400;
	let rem = secs % 86_400;
	let (h, m, s) = (rem / 3600, rem % 3600 / 60, rem % 60);
	let mut out = Vec::with_capacity(32);
	if days > 0 {
		let plural = if days == 1 {
			""
		} else {
			"s"
		};
		write!(out, "{days} day{plural}").expect("writing to a Vec cannot fail");
	}
	if days == 0 || rem > 0 || micros > 0 {
		if !out.is_empty() {
			out.push(b' ');
		}
		write!(out, "{h:02}:{m:02}:{s:02}").expect("writing to a Vec cannot fail");
		push_fraction(&mut out, micros);
	}
	out
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
	for fmt in ["%H:%M:%S%.f", "%H:%M:%S", "%H:%M"] {
		if let Ok(t) = chrono::NaiveTime::parse_from_str(clean.trim(), fmt) {
			return Some(t);
		}
	}
	None
}

/// The seconds of a zone offset written without its sign: `h`, `hh`, `hhmm`, `hhmmss`, or the
/// colon forms `hh:mm` and `hh:mm:ss`. PostgreSQL accepts offsets up to 15:59:59.
fn parse_zone_offset(text: &str) -> Option<i32> {
	let two = |part: &str| -> Option<i32> {
		if part.len() == 2 && part.bytes().all(|b| b.is_ascii_digit()) {
			part.parse().ok()
		} else {
			None
		}
	};
	let (hours, minutes, seconds) = if text.contains(':') {
		let mut parts = text.split(':');
		let hours = parts.next()?;
		let minutes = parts.next()?;
		let seconds = parts.next();
		if parts.next().is_some() {
			return None;
		}
		let hours = if hours.len() == 1 {
			hours.parse::<i32>().ok()?
		} else {
			two(hours)?
		};
		(hours, two(minutes)?, seconds.map(two).unwrap_or(Some(0))?)
	} else if !text.bytes().all(|b| b.is_ascii_digit()) {
		return None;
	} else {
		match text.len() {
			1 | 2 => (text.parse().ok()?, 0, 0),
			4 => (two(&text[..2])?, two(&text[2..])?, 0),
			6 => (two(&text[..2])?, two(&text[2..4])?, two(&text[4..])?),
			_ => return None,
		}
	};
	if hours > 15 || minutes > 59 || seconds > 59 {
		return None;
	}
	Some(hours * 3600 + minutes * 60 + seconds)
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
		(tp, sign * parse_zone_offset(op.trim_start_matches(['+', '-']))?)
	} else {
		(clean, 0)
	};
	let t = parse_time(time_part)?;
	Some((t, offset_secs))
}

/// A `time` text as nanoseconds since midnight, `24:00:00` included in any of its spellings.
fn parse_time_nanos(text: &str) -> Option<u64> {
	let clean = text.trim();
	if is_end_of_day(clean) {
		return Some(86_400_000_000_000);
	}
	let t = parse_time(clean)?;
	Some(u64::from(t.num_seconds_from_midnight()) * 1_000_000_000 + u64::from(t.nanosecond()))
}

/// Whether `text` is `24:00`, `24:00:00` or `24:00:00` with a fraction of zeros: the inclusive end
/// of PostgreSQL's `time` range, which every other field must leave at zero.
fn is_end_of_day(text: &str) -> bool {
	let Some(rest) = text.strip_prefix("24:00") else {
		return false;
	};
	let rest = match rest.strip_prefix(":00") {
		Some(after_seconds) => after_seconds,
		None => return rest.is_empty(),
	};
	match rest.strip_prefix('.') {
		Some(fraction) => !fraction.is_empty() && fraction.bytes().all(|b| b == b'0'),
		None => rest.is_empty(),
	}
}

/// A `timetz` text as the nanoseconds since midnight in UTC, wrapped to the day: the offset is
/// applied on input and not kept.
fn parse_timetz_nanos(text: &str) -> Option<u64> {
	let (t, offset_secs) = parse_timetz(text)?;
	let local =
		i128::from(t.num_seconds_from_midnight()) * 1_000_000_000 + i128::from(t.nanosecond());
	let utc = (local - i128::from(offset_secs) * 1_000_000_000).rem_euclid(86_400_000_000_000);
	u64::try_from(utc).ok()
}

fn time_value(nanos: u64) -> Option<Value> {
	surrealdb_types::Time::from_nanos(nanos).map(Value::Time)
}

/// A `timestamp` text, whose zone designator, if any, is ignored as PostgreSQL ignores it.
fn parse_timestamp_text(text: &str) -> Option<Value> {
	let (body, bc) = strip_era(text);
	let normalised = body.trim().replacen(' ', "T", 1);
	let naive = ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d"]
		.iter()
		.find_map(|fmt| chrono::NaiveDateTime::parse_from_str(&normalised, fmt).ok())
		.or_else(|| {
			chrono::NaiveDate::parse_from_str(&normalised, "%Y-%m-%d")
				.ok()
				.map(|d| d.and_time(chrono::NaiveTime::MIN))
		})
		.or_else(|| {
			["%Y-%m-%dT%H:%M:%S%.f%#z", "%Y-%m-%dT%H:%M:%S%#z"].iter().find_map(|fmt| {
				chrono::DateTime::parse_from_str(&normalised, fmt).ok().map(|dt| dt.naive_local())
			})
		})
		.or_else(|| normalised.parse::<Datetime>().ok().map(|dt| dt.into_inner().naive_utc()))?;
	let naive = if bc {
		use chrono::Datelike;
		naive.with_year(1 - naive.year())?
	} else {
		naive
	};
	Some(Value::Timestamp(naive.into()))
}

/// Refuse an array of arrays whose sub-arrays do not share one shape: PostgreSQL has no
/// multidimensional array of that shape.
fn check_rectangular(arr: &[Value], elem_ty: &PgType) -> Result<(), PgError> {
	if matches!(elem_ty, PgType::Array(_)) && !arr.is_empty() && rectangular_dims(arr).is_none() {
		return Err(PgError::new(
			"2202E",
			"multidimensional arrays must have array expressions with matching dimensions",
		));
	}
	Ok(())
}

/// The type of the elements of the innermost array of `elem_ty`, and the number of its dimensions
/// counting the outermost one.
fn leaf_type(elem_ty: &PgType) -> (&PgType, usize) {
	let mut leaf = elem_ty;
	let mut dimensions = 1;
	while let PgType::Array(inner) = leaf {
		leaf = inner;
		dimensions += 1;
	}
	(leaf, dimensions)
}

/// The values at the `depth` innermost levels of `arr`, in row-major order.
fn collect_leaves<'a>(arr: &'a [Value], depth: usize, out: &mut Vec<&'a Value>) {
	for value in arr {
		if let Value::Array(sub) = value
			&& depth > 1
		{
			collect_leaves(sub.as_slice(), depth - 1, out);
		} else {
			out.push(value);
		}
	}
}

fn array_text(arr: &[Value], elem_ty: &PgType) -> Result<Vec<u8>, PgError> {
	let mut out = Vec::new();
	out.push(b'{');
	for (i, elem) in arr.iter().enumerate() {
		if i > 0 {
			out.push(b',');
		}
		// A sub-array of a multidimensional array is written in its own braces, unquoted.
		if let PgType::Array(inner) = elem_ty {
			let Value::Array(sub) = elem else {
				return Err(mismatch(elem, elem_ty));
			};
			out.extend(array_text(sub.as_slice(), inner)?);
		} else if elem.is_nullish() {
			out.extend_from_slice(b"NULL");
		} else {
			let text = encode_text(elem.clone(), elem_ty)?;
			let s = std::str::from_utf8(&text)
				.map_err(|_| PgError::internal("invalid utf8 in array element"))?;
			let needs_quotes = s.is_empty()
				|| s.eq_ignore_ascii_case("NULL")
				|| s.chars()
					.any(|c| matches!(c, '{' | '}' | ',' | '"' | '\\') || is_array_space(c));
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
	out.push(b'}');
	Ok(out)
}

/// The binary form of an array: one dimension per level of `elem_ty` plus the outermost, lower
/// bound 1 in each, and the innermost elements in row-major order. An empty array has no
/// dimensions, whatever its type.
fn array_binary(arr: &[Value], elem_ty: &PgType) -> Result<Vec<u8>, PgError> {
	let (leaf_ty, ndim) = leaf_type(elem_ty);
	let mut out = Vec::new();
	if arr.is_empty() {
		out.extend_from_slice(&0i32.to_be_bytes()); // ndim = 0
		out.extend_from_slice(&0i32.to_be_bytes()); // has_null = 0
		out.extend_from_slice(&leaf_ty.oid().to_be_bytes());
		return Ok(out);
	}
	let dims = if ndim == 1 {
		vec![arr.len()]
	} else {
		match rectangular_dims(arr) {
			Some(dims) if dims.len() == ndim => dims,
			_ => return Err(mismatch(&Value::Array(arr.to_vec().into()), elem_ty)),
		}
	};
	let mut leaves = Vec::new();
	collect_leaves(arr, ndim, &mut leaves);
	let has_null = leaves.iter().any(|value| value.is_nullish());
	out.extend_from_slice(&(ndim as i32).to_be_bytes());
	out.extend_from_slice(
		&(if has_null {
			1i32
		} else {
			0i32
		})
		.to_be_bytes(),
	);
	out.extend_from_slice(&leaf_ty.oid().to_be_bytes());
	for dim in dims {
		out.extend_from_slice(&(dim as i32).to_be_bytes()); // dim_len
		out.extend_from_slice(&1i32.to_be_bytes()); // lower_bound = 1
	}

	for elem in leaves {
		if elem.is_nullish() {
			out.extend_from_slice(&(-1i32).to_be_bytes());
		} else {
			let bytes = encode_binary(elem.clone(), leaf_ty)?;
			out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
			out.extend_from_slice(&bytes);
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
		if !v.is_nullish() {
			let text = encode_text(v.clone(), &PgType::Text)?;
			push_record_field(&mut out, &text)?;
		}
	}
	out.push(b')');
	Ok(out)
}

/// Append one field of a record's text form, quoted where the record syntax needs it.
fn push_record_field(out: &mut Vec<u8>, text: &[u8]) -> Result<(), PgError> {
	let s =
		std::str::from_utf8(text).map_err(|_| PgError::internal("invalid utf8 in record field"))?;
	let needs_quotes = s.is_empty()
		|| s.contains(['(', ')', ',', '"', '\\'])
		|| s.chars().any(char::is_whitespace);
	if needs_quotes {
		out.push(b'"');
		for b in s.bytes() {
			// A quote or a backslash is doubled, as `record_out` writes it.
			if b == b'"' || b == b'\\' {
				out.push(b);
			}
			out.push(b);
		}
		out.push(b'"');
	} else {
		out.extend_from_slice(s.as_bytes());
	}
	Ok(())
}

/// The text form of a composite value: each field in declaration order, as its own type encodes
/// it. A field the object does not hold is NULL.
fn composite_text(
	obj: &surrealdb_types::Object,
	fields: &[(String, PgType)],
) -> Result<Vec<u8>, PgError> {
	let mut out = vec![b'('];
	for (i, (name, ty)) in fields.iter().enumerate() {
		if i > 0 {
			out.push(b',');
		}
		let value = obj.get(name.as_str()).cloned().unwrap_or(Value::Null);
		if !value.is_nullish() {
			let text = encode_text(value, ty)?;
			push_record_field(&mut out, &text)?;
		}
	}
	out.push(b')');
	Ok(out)
}

/// The binary form of a composite value: the field count, then for each field its type's OID and
/// its binary value with a length, -1 for a NULL field.
fn composite_binary(
	obj: &surrealdb_types::Object,
	fields: &[(String, PgType)],
) -> Result<Vec<u8>, PgError> {
	let mut out = Vec::new();
	out.extend_from_slice(&(fields.len() as i32).to_be_bytes());
	for (name, ty) in fields {
		out.extend_from_slice(&ty.oid().to_be_bytes());
		let value = obj.get(name.as_str()).cloned().unwrap_or(Value::Null);
		if value.is_nullish() {
			out.extend_from_slice(&(-1i32).to_be_bytes());
		} else {
			let bytes = encode_binary(value, ty)?;
			out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
			out.extend_from_slice(&bytes);
		}
	}
	Ok(out)
}

fn record_binary(obj: &surrealdb_types::Object) -> Result<Vec<u8>, PgError> {
	let mut out = Vec::new();
	out.extend_from_slice(&(obj.len() as i32).to_be_bytes()); // nfields
	for (_k, v) in obj.iter() {
		out.extend_from_slice(&OID_TEXT.to_be_bytes()); // field_oid
		if v.is_nullish() {
			out.extend_from_slice(&(-1i32).to_be_bytes());
		} else {
			let bytes = encode_binary(v.clone(), &PgType::Text)?;
			out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
			out.extend_from_slice(&bytes);
		}
	}
	Ok(out)
}

/// Encode one cell into the Postgres binary format for its column type. Like
/// [`encode_text`], a type mismatch is an internal invariant failure.
pub(super) fn encode_binary(value: Value, ty: &PgType) -> Result<Vec<u8>, PgError> {
	if let Some(bytes) = infinite_sign(&value).and_then(|positive| infinite_binary(ty, positive)) {
		return Ok(bytes);
	}
	match ty {
		PgType::Void => Ok(Vec::new()),
		PgType::Bool => {
			if let Value::Bool(b) = value {
				Ok(vec![u8::from(b)])
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Int2 => {
			if let Value::Number(ref n) = value {
				match n.wide() {
					Wide::Int(i) => Ok(narrow::<i16>(i, ty)?.to_be_bytes().to_vec()),
					Wide::Float(_) | Wide::Decimal(_) | Wide::BigDecimal(_) => {
						Err(mismatch(&value, ty))
					}
				}
			} else if let Value::String(s) = value {
				match s.parse::<i16>() {
					Ok(i) => Ok(i.to_be_bytes().to_vec()),
					Err(_) => Err(mismatch(&Value::String(s), ty)),
				}
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Int4 => {
			if let Value::Number(ref n) = value {
				match n.wide() {
					Wide::Int(i) => Ok(narrow::<i32>(i, ty)?.to_be_bytes().to_vec()),
					Wide::Float(_) | Wide::Decimal(_) | Wide::BigDecimal(_) => {
						Err(mismatch(&value, ty))
					}
				}
			} else if let Value::String(s) = value {
				match s.parse::<i32>() {
					Ok(i) => Ok(i.to_be_bytes().to_vec()),
					Err(_) => Err(mismatch(&Value::String(s), ty)),
				}
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Int8 => {
			if let Value::Number(ref n) = value {
				match n.wide() {
					Wide::Int(i) => Ok(i.to_be_bytes().to_vec()),
					Wide::Float(_) | Wide::Decimal(_) | Wide::BigDecimal(_) => {
						Err(mismatch(&value, ty))
					}
				}
			} else if let Value::String(s) = value {
				match s.parse::<i64>() {
					Ok(i) => Ok(i.to_be_bytes().to_vec()),
					Err(_) => Err(mismatch(&Value::String(s), ty)),
				}
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Oid | PgType::Regclass => {
			if let Value::Number(ref n) = value {
				match n.wide() {
					Wide::Int(i) => Ok((i as u32).to_be_bytes().to_vec()),
					Wide::Float(_) | Wide::Decimal(_) | Wide::BigDecimal(_) => {
						Err(mismatch(&value, ty))
					}
				}
			} else if let Value::String(s) = value {
				match s.parse::<u32>() {
					Ok(u) => Ok(u.to_be_bytes().to_vec()),
					Err(_) => Err(mismatch(&Value::String(s), ty)),
				}
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Float4 => {
			if let Value::Number(ref n) = value
				&& let Some(f) = n.to_f64()
			{
				Ok((f as f32).to_be_bytes().to_vec())
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Float8 => {
			if let Value::Number(ref n) = value
				&& let Some(f) = n.to_f64()
			{
				Ok(f.to_be_bytes().to_vec())
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Numeric => {
			if let Value::Number(ref n) = value {
				if let Number::F32(f) = *n {
					Ok(encode_numeric_binary(&float32_display(f)))
				} else {
					match n.wide() {
						Wide::Decimal(d) => Ok(encode_numeric_binary(&d.to_string())),
						Wide::Int(i) => Ok(encode_numeric_binary(&i.to_string())),
						Wide::Float(f) => Ok(encode_numeric_binary(&float_display(f))),
						Wide::BigDecimal(d) => bigdecimal_binary(d),
					}
				}
			} else if let Value::String(s) = value {
				Ok(encode_numeric_binary(&s))
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Char => {
			if let Value::String(s) = value {
				Ok(vec![s.bytes().next().unwrap_or(0)])
			} else if let Value::Number(ref n) = value {
				match n.wide() {
					Wide::Int(i) => Ok(vec![i as u8]),
					Wide::Float(_) | Wide::Decimal(_) | Wide::BigDecimal(_) => {
						Err(mismatch(&value, ty))
					}
				}
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Name | PgType::Text | PgType::Bpchar | PgType::Varchar | PgType::Enum(_) => {
			text_form(value, ty)
		}
		PgType::Date => {
			if let Value::Date(d) = value {
				Ok((d.days() - PG_EPOCH_UNIX_DAYS).to_be_bytes().to_vec())
			} else if let Value::Timestamp(t) = value {
				let days =
					i32::try_from(t.seconds().div_euclid(86_400) - i64::from(PG_EPOCH_UNIX_DAYS))
						.map_err(|_| PgError::new("22008", "date out of range"))?;
				Ok(days.to_be_bytes().to_vec())
			} else if let Value::Datetime(dt) = value {
				let nd = dt.into_inner().date_naive();
				let pg_epoch_date =
					chrono::NaiveDate::from_ymd_opt(2000, 1, 1).expect("valid epoch date");
				let days = (nd - pg_epoch_date).num_days() as i32;
				Ok(days.to_be_bytes().to_vec())
			} else if let Value::String(s) = value {
				if let Some(nd) = parse_date(&s) {
					let pg_epoch_date =
						chrono::NaiveDate::from_ymd_opt(2000, 1, 1).expect("valid epoch date");
					let days = (nd - pg_epoch_date).num_days() as i32;
					Ok(days.to_be_bytes().to_vec())
				} else {
					Err(mismatch(&Value::String(s), ty))
				}
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Time => {
			if let Value::Time(t) = value {
				Ok(((t.nanos() / 1_000) as i64).to_be_bytes().to_vec())
			} else if let Value::Timestamp(t) = value {
				Ok(((time_of_day_nanos(&t) / 1_000) as i64).to_be_bytes().to_vec())
			} else if let Value::Datetime(dt) = value {
				let t = dt.into_inner().time();
				let micros = i64::from(t.num_seconds_from_midnight()) * 1_000_000
					+ i64::from(t.nanosecond() / 1_000);
				Ok(micros.to_be_bytes().to_vec())
			} else if let Value::Duration(d) = value {
				let micros = i64::try_from(d.into_inner().as_micros())
					.map_err(|_| PgError::feature_not_supported("duration too large for time"))?;
				Ok(micros.to_be_bytes().to_vec())
			} else if let Value::String(s) = value {
				if let Some(t) = parse_time(&s) {
					let micros = i64::from(t.num_seconds_from_midnight()) * 1_000_000
						+ i64::from(t.nanosecond() / 1_000);
					Ok(micros.to_be_bytes().to_vec())
				} else {
					Err(mismatch(&Value::String(s), ty))
				}
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Timetz => {
			if let Value::Time(t) = value {
				let mut out = Vec::with_capacity(12);
				out.extend_from_slice(&((t.nanos() / 1_000) as i64).to_be_bytes());
				out.extend_from_slice(&0i32.to_be_bytes());
				Ok(out)
			} else if let Value::Datetime(dt) = value {
				let t = dt.into_inner().time();
				let micros = i64::from(t.num_seconds_from_midnight()) * 1_000_000
					+ i64::from(t.nanosecond() / 1_000);
				let mut out = Vec::with_capacity(12);
				out.extend_from_slice(&micros.to_be_bytes());
				out.extend_from_slice(&0i32.to_be_bytes()); // UTC offset 0
				Ok(out)
			} else if let Value::Duration(d) = value {
				let micros = i64::try_from(d.into_inner().as_micros())
					.map_err(|_| PgError::feature_not_supported("duration too large for timetz"))?;
				let mut out = Vec::with_capacity(12);
				out.extend_from_slice(&micros.to_be_bytes());
				out.extend_from_slice(&0i32.to_be_bytes());
				Ok(out)
			} else if let Value::String(s) = value {
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
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Timestamp => {
			if let Value::Datetime(dt) = value {
				let dt = dt.into_inner();
				let unix_micros =
					dt.timestamp() * 1_000_000 + i64::from(dt.timestamp_subsec_micros());
				Ok((unix_micros - PG_EPOCH_UNIX_MICROS).to_be_bytes().to_vec())
			} else if let Some(micros) = wall_micros_since_pg_epoch(&value) {
				micros.map(|m| m.to_be_bytes().to_vec())
			} else if let Value::String(ref s) = value
				&& s.eq_ignore_ascii_case("infinity")
			{
				Ok(i64::MAX.to_be_bytes().to_vec())
			} else if let Value::String(ref s) = value
				&& s.eq_ignore_ascii_case("-infinity")
			{
				Ok(i64::MIN.to_be_bytes().to_vec())
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Timestamptz => {
			if let Value::Datetime(dt) = value {
				let dt = dt.into_inner();
				let unix_micros =
					dt.timestamp() * 1_000_000 + i64::from(dt.timestamp_subsec_micros());
				Ok((unix_micros - PG_EPOCH_UNIX_MICROS).to_be_bytes().to_vec())
			} else if let Some(micros) = wall_micros_since_pg_epoch(&value) {
				micros.map(|m| m.to_be_bytes().to_vec())
			} else if let Value::String(ref s) = value
				&& s.eq_ignore_ascii_case("infinity")
			{
				Ok(i64::MAX.to_be_bytes().to_vec())
			} else if let Value::String(ref s) = value
				&& s.eq_ignore_ascii_case("-infinity")
			{
				Ok(i64::MIN.to_be_bytes().to_vec())
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Interval => {
			if let Value::Interval(i) = value {
				let mut out = Vec::with_capacity(16);
				out.extend_from_slice(&i.micros().to_be_bytes());
				out.extend_from_slice(&i.days().to_be_bytes());
				out.extend_from_slice(&i.months().to_be_bytes());
				Ok(out)
			} else if let Value::Time(t) = value {
				let mut out = Vec::with_capacity(16);
				out.extend_from_slice(&((t.nanos() / 1_000) as i64).to_be_bytes());
				out.extend_from_slice(&0i32.to_be_bytes());
				out.extend_from_slice(&0i32.to_be_bytes());
				Ok(out)
			} else if let Value::Duration(d) = value {
				let d = d.into_inner();
				let micros = i64::try_from(d.as_micros()).map_err(|_| {
					PgError::feature_not_supported("duration is too large to encode as an interval")
				})?;
				let mut out = Vec::with_capacity(16);
				out.extend_from_slice(&micros.to_be_bytes());
				out.extend_from_slice(&0i32.to_be_bytes());
				out.extend_from_slice(&0i32.to_be_bytes());
				Ok(out)
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Uuid => {
			if let Value::Uuid(u) = value {
				Ok(u.into_inner().into_bytes().to_vec())
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Bytea => {
			if let Value::Bytes(b) = value {
				Ok(b.into_inner().to_vec())
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Json => {
			let mut json = Vec::new();
			value.write_sql_json(&mut json);
			Ok(json)
		}
		PgType::Jsonb => {
			// The binary form is a version byte, then the text.
			let mut out = vec![1];
			value.write_sql_json(&mut out);
			Ok(out)
		}
		// An `xml` document travels as its text.
		PgType::Xml => {
			if let Value::String(text) = value {
				Ok(text.into_bytes())
			} else {
				Err(mismatch(&value, ty))
			}
		}
		// A `jsonpath` is a version byte, 1, then its text.
		PgType::Jsonpath => {
			if let Value::String(text) = value {
				let mut out = Vec::with_capacity(text.len() + 1);
				out.push(1);
				out.extend_from_slice(text.as_bytes());
				Ok(out)
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Inet
		| PgType::Cidr
		| PgType::MacAddr
		| PgType::Macaddr8
		| PgType::Bit
		| PgType::Varbit
		| PgType::Money
		| PgType::PgLsn
		| PgType::Regproc
		| PgType::Regprocedure
		| PgType::Regoper
		| PgType::Regoperator
		| PgType::Regtype
		| PgType::Regnamespace
		| PgType::Regrole
		| PgType::Regconfig
		| PgType::Regdictionary
		| PgType::Regcollation
		| PgType::Xid
		| PgType::Cid
		| PgType::Xid8
		| PgType::Point
		| PgType::Lseg
		| PgType::Path
		| PgType::PgBox
		| PgType::Polygon
		| PgType::Line
		| PgType::Circle
		| PgType::Int4range
		| PgType::Numrange
		| PgType::Tsrange
		| PgType::Tstzrange
		| PgType::Daterange
		| PgType::Int8range
		| PgType::Int4multirange
		| PgType::Nummultirange
		| PgType::Tsmultirange
		| PgType::Tstzmultirange
		| PgType::Datemultirange
		| PgType::Int8multirange => match Family::of(ty) {
			Some(family) => families::encode_binary(&family, value, ty),
			None => Err(mismatch(&value, ty)),
		},
		// These types have a text form only; no binary codec exists for them.
		PgType::Tsvector
		| PgType::Tsquery
		| PgType::Tid
		| PgType::PgSnapshot
		| PgType::TxidSnapshot => Err(PgError::feature_not_supported(format!(
			"binary format is not available for type oid {}; request the text format",
			ty.oid()
		))),
		PgType::Vector => crate::vector::binary(value),
		PgType::Composite {
			fields,
			..
		} => {
			if let Value::Object(obj) = value {
				composite_binary(&obj, fields)
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Record => {
			if let Value::Object(obj) = value {
				record_binary(&obj)
			} else {
				Err(mismatch(&value, ty))
			}
		}
		PgType::Array(elem_ty) => {
			if let Value::Array(arr) = value {
				check_rectangular(arr.as_slice(), elem_ty)?;
				array_binary(arr.as_slice(), elem_ty)
			} else {
				Err(mismatch(&value, ty))
			}
		}
	}
}

/// The binary `numeric` of a bigdecimal, from its exact digits and display scale.
fn bigdecimal_binary(d: &surrealdb_types::BigDecimal) -> Result<Vec<u8>, PgError> {
	let (ndigits, weight, sign, dscale, digits) =
		d.to_pg_numeric().map_err(|e| PgError::new("22003", e.to_string()))?;
	let dscale = i16::try_from(dscale)
		.map_err(|_| PgError::new("22003", "display scale is out of range for numeric"))?;
	let mut out = Vec::with_capacity(8 + 2 * digits.len());
	out.extend_from_slice(&ndigits.to_be_bytes());
	out.extend_from_slice(&weight.to_be_bytes());
	out.extend_from_slice(&sign.to_be_bytes());
	out.extend_from_slice(&dscale.to_be_bytes());
	for group in &digits {
		out.extend_from_slice(&group.to_be_bytes());
	}
	Ok(out)
}

/// Encode a decimal string (e.g. `-123.45`, `0.001`, `42`) into the Postgres
/// `numeric` binary wire format: ndigits, weight, sign, dscale, then the
/// base-10000 digit groups (most significant first). `NaN`, `Infinity` and
/// `-Infinity` are written with the sign word Postgres uses for each.
fn encode_numeric_binary(s: &str) -> Vec<u8> {
	const NUMERIC_POS: u16 = 0x0000;
	const NUMERIC_NEG: u16 = 0x4000;
	const NUMERIC_NAN: u16 = 0xC000;
	const NUMERIC_PINF: u16 = 0xD000;
	const NUMERIC_NINF: u16 = 0xF000;

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
	if lower.contains("nan") {
		emit(0, 0, NUMERIC_NAN, 0, &[]);
		return out;
	}
	if lower.contains("inf") {
		let sign = if lower.starts_with('-') {
			NUMERIC_NINF
		} else {
			NUMERIC_PINF
		};
		emit(0, 0, sign, 0, &[]);
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

/// A decoded temporal parameter, declared as `declared`, as the value of the temporal type
/// `target` it is compared with or stored into, so it matches the values of a column of that
/// type. A `date` becomes the midnight of its day and a `timestamp` and a `timestamptz` convert
/// as PostgreSQL converts them with a UTC session zone. Any other value or pair of types is
/// returned as it is, and a conversion down to a `date` is never made, since it would drop the
/// time of day a comparison uses.
pub(super) fn coerce_temporal_param(value: Value, declared: i32, target: i32) -> Value {
	let temporal = |oid: i32| matches!(oid, OID_DATE | OID_TIMESTAMP | OID_TIMESTAMPTZ);
	if declared == target || !temporal(declared) || !temporal(target) {
		return value;
	}
	let midnight = |date: Date| date.into_inner().and_time(chrono::NaiveTime::MIN);
	match (value, target) {
		(Value::Timestamp(t), OID_TIMESTAMPTZ) => {
			Value::Datetime(Datetime::from(t.into_inner().and_utc()))
		}
		(Value::Date(d), OID_TIMESTAMP) if d.is_infinite() => {
			Value::Timestamp(if d == Date::infinity() {
				Timestamp::infinity()
			} else {
				Timestamp::neg_infinity()
			})
		}
		(Value::Date(d), OID_TIMESTAMPTZ) if d.is_infinite() => {
			Value::Datetime(if d == Date::infinity() {
				Datetime::MAX_UTC
			} else {
				Datetime::MIN_UTC
			})
		}
		(Value::Date(d), OID_TIMESTAMP) => Value::Timestamp(Timestamp::from(midnight(d))),
		(Value::Date(d), OID_TIMESTAMPTZ) => Value::Datetime(Datetime::from(midnight(d).and_utc())),
		(Value::Datetime(dt), OID_TIMESTAMP) => {
			Value::Timestamp(Timestamp::from(dt.into_inner().naive_utc()))
		}
		(other, _) => other,
	}
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

/// The value a text parameter of type `oid` reads as.
pub(super) fn decode_text(text: &str, oid: i32) -> Result<Value, PgError> {
	if let Some(elem_oid) = is_array_oid(oid) {
		return decode_array_text(text, elem_oid);
	}
	if let Some((family, ty)) = family_of_oid(oid) {
		return families::decode_text(&family, text, &ty);
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
				decode_numeric_text(text, oid)
			}
		}
		OID_UUID => Uuid::from_str(text).map(Value::Uuid).map_err(|e| param_err(oid, e)),
		OID_DATE => match infinity_text_sign(text) {
			Some(positive) => Ok(Value::Date(if positive {
				Date::infinity()
			} else {
				Date::neg_infinity()
			})),
			None => {
				let text = if text.eq_ignore_ascii_case("epoch") {
					"1970-01-01"
				} else {
					text
				};
				match parse_date(text) {
					Some(nd) => Ok(Value::Date(nd.into())),
					None => Err(param_err(oid, "not a date")),
				}
			}
		},
		OID_TIME => match parse_time_nanos(text) {
			Some(nanos) => time_value(nanos).ok_or_else(|| param_err(oid, "time out of range")),
			None => Err(param_err(oid, "not a time")),
		},
		OID_TIMETZ => match parse_timetz_nanos(text) {
			Some(nanos) => time_value(nanos).ok_or_else(|| param_err(oid, "time out of range")),
			None => Err(param_err(oid, "not a time")),
		},
		OID_TIMESTAMP => match infinity_text_sign(text) {
			Some(positive) => Ok(Value::Timestamp(if positive {
				Timestamp::infinity()
			} else {
				Timestamp::neg_infinity()
			})),
			None => {
				let text = if text.eq_ignore_ascii_case("epoch") {
					"1970-01-01T00:00:00"
				} else {
					text
				};
				parse_timestamp_text(text).ok_or_else(|| param_err(oid, "not a timestamp"))
			}
		},
		OID_TIMESTAMPTZ => match infinity_text_sign(text) {
			Some(positive) => Ok(Value::Datetime(if positive {
				Datetime::MAX_UTC
			} else {
				Datetime::MIN_UTC
			})),
			None => {
				let text = if text.eq_ignore_ascii_case("epoch") {
					"1970-01-01T00:00:00+00:00"
				} else {
					text
				};
				parse_datetime(text).ok_or_else(|| param_err(oid, "not a timestamp"))
			}
		},
		OID_INTERVAL => surrealdb_types::Interval::from_str(text)
			.map(Value::Interval)
			.map_err(|e| param_err(oid, e)),
		OID_BYTEA => decode_bytea_text(text).ok_or_else(|| param_err(oid, "not a bytea literal")),
		OID_JSON | OID_JSONB => {
			let json: serde_json::Value =
				serde_json::from_str(text).map_err(|e| param_err(oid, e))?;
			Ok(json_to_value(json))
		}
		OID_VECTOR => crate::vector::decode_text(text),
		OID_RECORD => Ok(Value::String(text.to_string())),
		_ => Ok(Value::String(text.to_string())),
	}
}

const MAX_BINARY_DECODE_DEPTH: usize = MAX_ARRAY_DIMENSIONS;

/// The value a binary parameter of type `oid` reads as.
pub(super) fn decode_binary(bytes: &[u8], oid: i32) -> Result<Value, PgError> {
	decode_binary_at(bytes, oid, 0)
}

fn decode_binary_at(bytes: &[u8], oid: i32, depth: usize) -> Result<Value, PgError> {
	if depth > MAX_BINARY_DECODE_DEPTH {
		return Err(param_err(oid, "binary parameter nesting exceeds maximum depth"));
	}
	if let Some(elem_oid) = is_array_oid(oid) {
		return decode_array_binary(bytes, elem_oid, depth + 1);
	}
	if let Some((family, ty)) = family_of_oid(oid) {
		return families::decode_binary(&family, bytes, &ty);
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
		OID_VECTOR => crate::vector::decode_binary(bytes),
		OID_DATE => {
			let raw: [u8; 4] = bytes.try_into().map_err(|_| param_err(oid, "wrong length"))?;
			let pg_days = i32::from_be_bytes(raw);
			if pg_days == i32::MAX {
				return Ok(Value::Date(Date::infinity()));
			}
			if pg_days == i32::MIN {
				return Ok(Value::Date(Date::neg_infinity()));
			}
			let days = pg_days
				.checked_add(PG_EPOCH_UNIX_DAYS)
				.ok_or_else(|| param_err(oid, "out of range"))?;
			surrealdb_types::Date::from_finite_days(days)
				.map(Value::Date)
				.ok_or_else(|| param_err(oid, "out of range"))
		}
		OID_TIME => {
			let raw: [u8; 8] = bytes.try_into().map_err(|_| param_err(oid, "wrong length"))?;
			let micros = i64::from_be_bytes(raw);
			binary_time(micros, oid)
		}
		OID_TIMETZ => {
			if bytes.len() != 12 {
				return Err(param_err(oid, "wrong length for timetz"));
			}
			let raw: [u8; 8] =
				bytes[0..8].try_into().map_err(|_| param_err(oid, "wrong length for timetz"))?;
			let zone: [u8; 4] =
				bytes[8..12].try_into().map_err(|_| param_err(oid, "wrong length for timetz"))?;
			// The zone is the seconds west of UTC, so UTC is the local time plus the zone. As in
			// PostgreSQL's `timetz_recv`, the local time is within a day (24:00 included) and the
			// zone within 15:59:59 of UTC; only a valid pair is wrapped to the UTC day.
			let micros = i64::from_be_bytes(raw);
			let zone = i32::from_be_bytes(zone);
			if !(0..=86_400_000_000).contains(&micros) {
				return Err(param_err(oid, "time out of range"));
			}
			if !(-57_599..=57_599).contains(&zone) {
				return Err(param_err(oid, "time zone displacement out of range"));
			}
			let utc = i128::from(micros) + i128::from(zone) * 1_000_000;
			let utc = i64::try_from(utc.rem_euclid(86_400_000_000))
				.map_err(|_| param_err(oid, "out of range"))?;
			binary_time(utc, oid)
		}
		OID_TIMESTAMP => {
			let raw: [u8; 8] = bytes.try_into().map_err(|_| param_err(oid, "wrong length"))?;
			let pg_micros = i64::from_be_bytes(raw);
			if pg_micros == i64::MAX {
				return Ok(Value::Timestamp(Timestamp::infinity()));
			}
			if pg_micros == i64::MIN {
				return Ok(Value::Timestamp(Timestamp::neg_infinity()));
			}
			let unix_micros = pg_micros
				.checked_add(PG_EPOCH_UNIX_MICROS)
				.ok_or_else(|| param_err(oid, "out of range"))?;
			surrealdb_types::Timestamp::from_finite_parts(
				unix_micros.div_euclid(1_000_000),
				(unix_micros.rem_euclid(1_000_000) * 1_000) as u32,
			)
			.map(Value::Timestamp)
			.ok_or_else(|| param_err(oid, "out of range"))
		}
		OID_TIMESTAMPTZ => {
			let raw: [u8; 8] = bytes.try_into().map_err(|_| param_err(oid, "wrong length"))?;
			let pg_micros = i64::from_be_bytes(raw);
			if pg_micros == i64::MAX {
				return Ok(Value::Datetime(Datetime::MAX_UTC));
			}
			if pg_micros == i64::MIN {
				return Ok(Value::Datetime(Datetime::MIN_UTC));
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
		OID_RECORD => {
			if bytes.len() < 4 {
				return Err(param_err(oid, "truncated record header"));
			}
			let raw_hdr: [u8; 4] =
				bytes[0..4].try_into().map_err(|_| param_err(oid, "truncated record header"))?;
			let nfields = usize::try_from(i32::from_be_bytes(raw_hdr))
				.map_err(|_| param_err(oid, "negative record field count"))?;
			if nfields > bytes.len().saturating_sub(4) / 8 {
				return Err(param_err(oid, "truncated record field header"));
			}
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
					decode_binary_at(val_bytes, field_oid, depth + 1)?
				};
				obj.insert(format!("f{idx}"), val);
			}
			if offset != bytes.len() {
				return Err(param_err(oid, "record binary has trailing bytes"));
			}
			Ok(Value::Object(obj))
		}
		OID_TEXT | OID_VARCHAR | OID_BPCHAR | OID_NAME => std::str::from_utf8(bytes)
			.map(|s| Value::String(s.to_string()))
			.map_err(|_| param_err(oid, "not valid UTF-8")),
		oid if is_enum_oid(oid) => std::str::from_utf8(bytes)
			.map(|s| Value::String(s.to_string()))
			.map_err(|_| param_err(oid, "not valid UTF-8")),
		0 => std::str::from_utf8(bytes)
			.map(|s| Value::String(s.to_string()))
			.map_err(|_| param_err(oid, "unspecified binary parameter is not valid UTF-8")),
		_ => Err(param_err(oid, "unsupported binary parameter type")),
	}
}

/// Decode the Postgres `numeric` binary wire format. A value that fits a [`Decimal`] exactly is
/// a `decimal`; any other finite value is a `bigdecimal` with the same digits and scale.
/// Inverse of [`encode_numeric_binary`].
fn decode_numeric_binary(bytes: &[u8], oid: i32) -> Result<Value, PgError> {
	if bytes.len() < 8 {
		return Err(param_err(oid, "numeric header truncated"));
	}
	let rd16 = |i: usize| i16::from_be_bytes([bytes[i], bytes[i + 1]]);
	let ndigits = rd16(0);
	let weight = rd16(2);
	let sign = u16::from_be_bytes([bytes[4], bytes[5]]);
	if sign == 0xC000 {
		return Err(param_err(oid, "NaN is not representable"));
	}
	if sign == 0xD000 || sign == 0xF000 {
		if ndigits != 0 {
			return Err(param_err(oid, "a non-finite numeric has no digits"));
		}
		let infinity = if sign == 0xD000 {
			f64::INFINITY
		} else {
			f64::NEG_INFINITY
		};
		return Ok(Value::Number(Number::Float(infinity)));
	}
	if sign != 0x0000 && sign != 0x4000 {
		return Err(param_err(oid, "invalid numeric sign"));
	}
	let dscale = u16::try_from(rd16(6)).map_err(|_| param_err(oid, "display scale is negative"))?;
	if dscale > 16_383 {
		return Err(param_err(oid, "display scale exceeds 16383"));
	}
	let ndigits = usize::try_from(ndigits).map_err(|_| param_err(oid, "negative ndigits"))?;
	if bytes.len() < 8 + ndigits * 2 {
		return Err(param_err(oid, "numeric digits truncated"));
	}
	if bytes.len() > 8 + ndigits * 2 {
		return Err(param_err(oid, "numeric binary has trailing bytes"));
	}
	let groups: Vec<i16> = (0..ndigits).map(|i| rd16(8 + i * 2)).collect();
	if groups.iter().any(|group| !(0..10_000).contains(group)) {
		return Err(param_err(oid, "numeric digit out of range"));
	}
	if let Some(value) = decimal_from_groups(weight, sign, dscale, &groups) {
		return Ok(Value::Number(Number::Decimal(value)));
	}
	bigdecimal_from_groups(oid, weight, sign, dscale, &groups)
		.map(|value| Value::Number(Number::BigDecimal(Box::new(value))))
}

/// The value of base-10000 digit `groups` as a [`Decimal`], or `None` when it does not fit one
/// exactly. The display scale `dscale` is honoured when it is at most 28 and at least the scale
/// the value needs, so the trailing zero groups of the alignment are trimmed. A smaller `dscale`
/// never rounds the value.
fn decimal_from_groups(weight: i16, sign: u16, dscale: u16, groups: &[i16]) -> Option<Decimal> {
	// The groups form the coefficient of value * 10^exp10, where `exp10` is 4 times the power of
	// 10000 of the last group.
	let mut mantissa: i128 = 0;
	for &group in groups {
		mantissa = mantissa.checked_mul(10_000)?.checked_add(i128::from(group))?;
	}
	let exp10 = 4 * (i32::from(weight) - (groups.len() as i32 - 1));
	let mut value = if exp10 >= 0 {
		let factor = 10i128.checked_pow(exp10.unsigned_abs())?;
		Decimal::try_from_i128_with_scale(mantissa.checked_mul(factor)?, 0).ok()?
	} else {
		Decimal::try_from_i128_with_scale(mantissa, exp10.unsigned_abs()).ok()?
	};
	if sign == 0x4000 {
		value = -value;
	}
	let scale = u32::from(dscale);
	if scale <= 28 && scale >= value.normalize().scale() {
		value.rescale(scale);
	}
	Some(value)
}

/// The value of base-10000 digit `groups` as a [`BigDecimal`], exact. Its scale is the one it
/// needs, or `dscale` when that is larger, so a display scale is honoured without rounding.
fn bigdecimal_from_groups(
	oid: i32,
	weight: i16,
	sign: u16,
	dscale: u16,
	groups: &[i16],
) -> Result<BigDecimal, PgError> {
	let exact = if groups.iter().all(|&group| group == 0) {
		BigDecimal::from(0i64)
	} else {
		let digits: String = groups.iter().map(|group| format!("{group:04}")).collect();
		let exp10 = 4 * (i64::from(weight) - (groups.len() as i64 - 1));
		let sign = if sign == 0x4000 {
			"-"
		} else {
			""
		};
		let text = format!("{sign}{digits}e{exp10}");
		text.parse::<BigDecimal>().map_err(|e| param_err(oid, e))?.normalized()
	};
	if i64::from(dscale) > exact.scale() {
		exact.rescale(u32::from(dscale)).map_err(|e| param_err(oid, e))
	} else {
		Ok(exact)
	}
}

/// Decode numeric text. A value that fits a [`Decimal`] exactly is a `decimal` written with the
/// scale it was written with, and any other finite value is a `bigdecimal`.
fn decode_numeric_text(text: &str, oid: i32) -> Result<Value, PgError> {
	let parsed: BigDecimal = text.parse().map_err(|e| param_err(oid, e))?;
	let written = parsed.scale();
	match Decimal::try_from(&parsed) {
		Ok(mut value) => {
			if let Ok(scale) = u32::try_from(written)
				&& scale <= 28
				&& scale >= value.scale()
			{
				value.rescale(scale);
			}
			Ok(Value::Number(Number::Decimal(value)))
		}
		Err(_) => Ok(Value::Number(Number::BigDecimal(Box::new(parsed)))),
	}
}

/// Decode the Postgres `interval` binary wire format (micros, days, months)
/// into a [`std::time::Duration`]. Month components are calendar-relative and
/// have no absolute-duration equivalent, so a non-zero month field is rejected.
fn decode_interval_binary(bytes: &[u8], oid: i32) -> Result<Value, PgError> {
	let raw: [u8; 16] = bytes.try_into().map_err(|_| param_err(oid, "wrong length"))?;
	let micros = i64::from_be_bytes(raw[0..8].try_into().expect("8 bytes"));
	let days = i32::from_be_bytes(raw[8..12].try_into().expect("4 bytes"));
	let months = i32::from_be_bytes(raw[12..16].try_into().expect("4 bytes"));
	Ok(Value::Interval(surrealdb_types::Interval::new(months, days, micros)))
}

/// A binary `time`, in microseconds since midnight.
fn binary_time(micros: i64, oid: i32) -> Result<Value, PgError> {
	u64::try_from(micros)
		.ok()
		.and_then(|m| m.checked_mul(1_000))
		.and_then(time_value)
		.ok_or_else(|| param_err(oid, "time out of range"))
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

/// Whether `oid` is the type of an array of a user-defined enum.
fn is_enum_array_oid(oid: i32) -> bool {
	(ENUM_TYPE_OID_BASE + ENUM_ARRAY_OID_OFFSET..ENUM_TYPE_OID_BASE + 2 * ENUM_ARRAY_OID_OFFSET)
		.contains(&oid)
}

/// Whether `oid` is the type of a user-defined enum, whose values travel as their label.
fn is_enum_oid(oid: i32) -> bool {
	(ENUM_TYPE_OID_BASE..ENUM_TYPE_OID_BASE + ENUM_ARRAY_OID_OFFSET).contains(&oid)
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
		OID_ARRAY_VECTOR => Some(OID_VECTOR),
		OID_ANYARRAY => Some(OID_TEXT),
		oid if is_enum_array_oid(oid) => Some(oid - ENUM_ARRAY_OID_OFFSET),
		// An array of one of the declared-type families.
		oid => {
			if let Some(PgType::Array(element)) = PgType::from_oid(oid)
				&& Family::of(&element).is_some()
			{
				Some(element.oid())
			} else {
				None
			}
		}
	}
}

/// The family of a parameter OID and its wire type, for the types [`families`] decodes.
fn family_of_oid(oid: i32) -> Option<(Family, PgType)> {
	let ty = PgType::from_oid(oid)?;
	Family::of(&ty).map(|family| (family, ty))
}

fn decode_array_text(text: &str, elem_oid: i32) -> Result<Value, PgError> {
	let items = parse_array_literal(text)
		.ok_or_else(|| PgError::invalid_text(format!("malformed array literal: \"{text}\"")))?;
	decode_array_elements(items, elem_oid)
}

fn decode_array_elements(items: Vec<ArrayElement>, elem_oid: i32) -> Result<Value, PgError> {
	let values = items
		.into_iter()
		.map(|item| match item {
			ArrayElement::Null => Ok(Value::Null),
			ArrayElement::Text(text) => decode_text(&text, elem_oid),
			ArrayElement::Array(items) => decode_array_elements(items, elem_oid),
		})
		.collect::<Result<Vec<_>, _>>()?;
	Ok(Value::Array(values.into()))
}

fn decode_array_binary(
	bytes: &[u8],
	expected_elem_oid: i32,
	depth: usize,
) -> Result<Value, PgError> {
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
	// The payload names its element type; a declared parameter type must agree with it.
	if elem_oid != 0 && expected_elem_oid != 0 && elem_oid != expected_elem_oid {
		return Err(param_err(
			expected_elem_oid,
			"array element type does not match the parameter",
		));
	}
	let target_oid = if elem_oid != 0 {
		elem_oid
	} else {
		expected_elem_oid
	};

	if ndim < 0 {
		return Err(param_err(target_oid, "negative array dimensions"));
	}
	// Every return consumes the payload exactly; trailing bytes are a malformed parameter.
	let trailing = || param_err(target_oid, "array binary has trailing bytes");
	if ndim == 0 {
		if bytes.len() != 12 {
			return Err(trailing());
		}
		return Ok(Value::Array(Vec::<Value>::new().into()));
	}
	let ndim = ndim as usize;
	if ndim > MAX_ARRAY_DIMENSIONS {
		return Err(param_err(target_oid, "array dimensions exceed limit"));
	}
	if bytes.len() < 12 + ndim * 8 {
		return Err(param_err(target_oid, "array binary dimensions header too short"));
	}

	let mut total_elems = 1usize;
	let mut dims = Vec::with_capacity(ndim);
	let mut offset = 12;
	for _ in 0..ndim {
		let raw_dim: [u8; 4] = bytes[offset..offset + 4]
			.try_into()
			.map_err(|_| param_err(target_oid, "array dim header truncated"))?;
		let dim_len = i32::from_be_bytes(raw_dim);
		let raw_lower: [u8; 4] = bytes[offset + 4..offset + 8]
			.try_into()
			.map_err(|_| param_err(target_oid, "array dim header truncated"))?;
		if i32::from_be_bytes(raw_lower) != 1 {
			return Err(param_err(target_oid, "array lower bounds other than 1 are not supported"));
		}
		offset += 8;
		if dim_len > 0 {
			total_elems = total_elems
				.checked_mul(dim_len as usize)
				.ok_or_else(|| param_err(target_oid, "array dimensions overflow"))?;
			dims.push(dim_len as usize);
		} else if dim_len == 0 {
			total_elems = 0;
		} else {
			return Err(param_err(target_oid, "negative array dimension length"));
		}
	}

	if total_elems == 0 {
		if offset != bytes.len() {
			return Err(trailing());
		}
		return Ok(Value::Array(Vec::<Value>::new().into()));
	}
	if total_elems > (bytes.len() - offset) / 4 {
		return Err(param_err(target_oid, "array binary element length truncated"));
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
			elements.push(decode_binary_at(elem_bytes, target_oid, depth)?);
		}
	}
	if offset != bytes.len() {
		return Err(trailing());
	}

	Ok(nest_elements(&mut elements.into_iter(), &dims))
}

/// The array of the `dims` lengths, nested one level per dimension, that takes its innermost
/// elements from `elements` in row-major order.
fn nest_elements(elements: &mut impl Iterator<Item = Value>, dims: &[usize]) -> Value {
	let mut items = Vec::new();
	if let Some((&len, inner)) = dims.split_first() {
		for _ in 0..len {
			items.push(if inner.is_empty() {
				elements.next().unwrap_or(Value::Null)
			} else {
				nest_elements(elements, inner)
			});
		}
	}
	Value::Array(items.into())
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

	const TEXT_FAMILY: [PgType; 4] = [PgType::Text, PgType::Varchar, PgType::Bpchar, PgType::Name];

	fn int(i: i64) -> Value {
		Value::Number(Number::Int(i))
	}

	fn string(s: &str) -> Value {
		Value::String(s.to_string())
	}

	fn array(values: Vec<Value>) -> Value {
		Value::Array(values.into())
	}

	fn object(entries: Vec<(&str, Value)>) -> Value {
		let mut object = surrealdb_types::Object::new();
		for (key, value) in entries {
			object.insert(key.to_string(), value);
		}
		Value::Object(object)
	}

	fn at(seconds: i64, nanos: u32) -> Value {
		Value::Datetime(
			surrealdb_types::Datetime::from_timestamp(seconds, nanos).expect("a datetime"),
		)
	}

	fn lasting(secs: u64, nanos: u32) -> Value {
		Value::Duration(std::time::Duration::new(secs, nanos).into())
	}

	fn record(table: &str, key: &str) -> Value {
		Value::RecordId(surrealdb_types::RecordId::new(table, key))
	}

	/// Every kind of value a text-family column can receive, with the text its
	/// own typed column shows for it.
	fn text_family_values() -> Vec<(&'static str, Value, String)> {
		let uuid = Uuid::from(uuid::Uuid::from_u128(0x018f_6b2e_4c3d_7a11_9a52_0123_4567_89ab));
		let uuid_text = "018f6b2e-4c3d-7a11-9a52-0123456789ab";
		let float = |f: f64| Value::Number(Number::Float(f));
		let decimal = |mantissa: i64, scale: u32| {
			Value::Number(Number::Decimal(Decimal::new(mantissa, scale)))
		};
		let date = |text: &str| text.to_string();
		vec![
			("datetime", at(315_532_800, 0), date("1980-01-01 00:00:00+00")),
			(
				"datetime, millis",
				at(1_577_882_096, 789_000_000),
				date("2020-01-01 12:34:56.789+00"),
			),
			(
				"datetime, nanos truncate",
				at(1_705_322_096, 123_456_789),
				date("2024-01-15 12:34:56.123456+00"),
			),
			("datetime, before 1970", at(-1, 0), date("1969-12-31 23:59:59+00")),
			(
				"datetime, before 1970 with a fraction",
				at(-1, 500_000_000),
				date("1969-12-31 23:59:59.5+00"),
			),
			("datetime, year 1", at(-62_135_596_800, 0), date("0001-01-01 00:00:00+00")),
			("datetime, year 0 is 1 BC", at(-62_167_219_200, 0), date("0001-01-01 00:00:00+00 BC")),
			(
				"datetime, last microsecond of 9999",
				at(253_402_300_799, 999_999_000),
				date("9999-12-31 23:59:59.999999+00"),
			),
			("datetime, year 10000", at(253_402_300_800, 0), date("10000-01-01 00:00:00+00")),
			(
				"datetime, far future",
				at(8_210_266_876_799, 999_998_999),
				date("262142-12-31 23:59:59.999998+00"),
			),
			(
				"datetime, the greatest value is infinity",
				at(8_210_266_876_799, 999_999_999),
				"infinity".into(),
			),
			("duration, zero", lasting(0, 0), "00:00:00".into()),
			("duration, milliseconds", lasting(0, 1_000_000), "00:00:00.001".into()),
			("duration, fraction", lasting(1, 500_000_000), "00:00:01.5".into()),
			("duration, a microsecond", lasting(0, 1_000), "00:00:00.000001".into()),
			("duration, below a microsecond", lasting(0, 1), "00:00:00".into()),
			("duration, 1h30m", lasting(5_400, 0), "01:30:00".into()),
			("duration, 26h", lasting(93_600, 0), "1 day 02:00:00".into()),
			("duration, 1d", lasting(86_400, 0), "1 day".into()),
			("duration, 2d", lasting(172_800, 0), "2 days".into()),
			("duration, 1w", lasting(604_800, 0), "7 days".into()),
			("duration, 1y", lasting(31_536_000, 0), "365 days".into()),
			(
				"duration, days and a fraction",
				lasting(86_400, 500_000_000),
				"1 day 00:00:00.5".into(),
			),
			(
				"duration, the largest",
				lasting(u64::MAX, 999_999_999),
				"213503982334601 days 07:00:15.999999".into(),
			),
			("uuid", Value::Uuid(uuid), uuid_text.into()),
			("bytes", Value::Bytes(vec![0xde, 0xad].into()), "\\xdead".into()),
			("bytes, empty", Value::Bytes(Vec::<u8>::new().into()), "\\x".into()),
			("decimal", decimal(12_345, 2), "123.45".into()),
			("decimal, negative", decimal(-5, 3), "-0.005".into()),
			("decimal, trailing zeros", decimal(100, 2), "1.00".into()),
			("float", float(1.5), "1.5".into()),
			("float, whole", float(3.0), "3".into()),
			("float, negative zero", float(-0.0), "-0".into()),
			("float, NaN", float(f64::NAN), "NaN".into()),
			("float, Infinity", float(f64::INFINITY), "Infinity".into()),
			("float, -Infinity", float(f64::NEG_INFINITY), "-Infinity".into()),
			("float, huge", float(f64::MAX), f64::MAX.to_string()),
			("float, tiny", float(f64::MIN_POSITIVE), f64::MIN_POSITIVE.to_string()),
			("int", int(-42), "-42".into()),
			("int, largest", int(i64::MAX), i64::MAX.to_string()),
			("int, smallest", int(i64::MIN), i64::MIN.to_string()),
			("bool, true", Value::Bool(true), "t".into()),
			("bool, false", Value::Bool(false), "f".into()),
			("string", string("hello"), "hello".into()),
			("string, empty", string(""), "".into()),
			("string, punctuation", string("a,b {c} \"d\""), "a,b {c} \"d\"".into()),
			("string, unicode", string("caf\u{e9} \u{1f600}"), "caf\u{e9} \u{1f600}".into()),
			("array, empty", array(vec![]), "{}".into()),
			("array, ints", array(vec![int(1), int(2), int(3)]), "{1,2,3}".into()),
			("array, strings", array(vec![string("a"), string("b c")]), "{a,\"b c\"}".into()),
			(
				"array, strings that need quoting",
				array(vec![
					string(""),
					string("NULL"),
					string("a,b"),
					string("say \"hi\""),
					string("back\\slash"),
				]),
				"{\"\",\"NULL\",\"a,b\",\"say \\\"hi\\\"\",\"back\\\\slash\"}".into(),
			),
			("array, with a hole", array(vec![int(1), Value::None, int(3)]), "{1,NULL,3}".into()),
			("array, bools", array(vec![Value::Bool(true), Value::Bool(false)]), "{t,f}".into()),
			("array, floats and ints", array(vec![float(1.5), int(2)]), "{1.5,2}".into()),
			(
				"array, datetimes",
				array(vec![at(315_532_800, 0), at(1_577_882_096, 789_000_000)]),
				"{\"1980-01-01 00:00:00+00\",\"2020-01-01 12:34:56.789+00\"}".into(),
			),
			(
				"array, durations",
				array(vec![lasting(5_400, 0), lasting(93_600, 0)]),
				"{01:30:00,\"1 day 02:00:00\"}".into(),
			),
			("array, uuids", array(vec![Value::Uuid(uuid)]), format!("{{{uuid_text}}}")),
			(
				"array, bytes",
				array(vec![Value::Bytes(vec![0xde, 0xad].into())]),
				"{\"\\\\xdead\"}".into(),
			),
			(
				"array, mixed kinds are jsonb elements",
				array(vec![int(1), string("a"), Value::Bool(true)]),
				"{1,\"\\\"a\\\"\",true}".into(),
			),
			(
				"array, objects are jsonb elements",
				array(vec![object(vec![("a", int(1))])]),
				"{\"{\\\"a\\\":1}\"}".into(),
			),
			("array, record ids", array(vec![record("person", "tobie")]), "{person:tobie}".into()),
			(
				"array of arrays is a multidimensional array",
				array(vec![array(vec![int(1), int(2)]), array(vec![int(3), int(4)])]),
				"{{1,2},{3,4}}".into(),
			),
			(
				"array of jagged arrays is one json document",
				array(vec![array(vec![int(1)]), array(vec![int(2), int(3)])]),
				"[[1],[2,3]]".into(),
			),
			(
				"object",
				object(vec![("a", int(1)), ("b", array(vec![int(1), int(2)]))]),
				"{\"a\":1,\"b\":[1,2]}".into(),
			),
			("object, empty", object(vec![]), "{}".into()),
			("set", Value::Set(vec![int(1), int(2)].into()), "[1,2]".into()),
			("record id", record("person", "tobie"), "person:tobie".into()),
			("range", Value::Range(Box::new(surrealdb_types::Range::from(1i64..5))), "1..5".into()),
			("table", Value::Table(surrealdb_types::Table::new("person")), "person".into()),
		]
	}

	#[test]
	fn a_text_column_shows_a_value_as_its_own_typed_column_does() {
		let mut mismatches = Vec::new();
		for (name, value, expected) in text_family_values() {
			for ty in TEXT_FAMILY {
				let shown = text(value.clone(), ty.clone());
				if shown != expected {
					mismatches.push(format!("{name} in {ty:?}: {shown:?}, expected {expected:?}"));
				}
				// A text column's binary cell is its text.
				let binary = String::from_utf8(encode_binary(value.clone(), &ty).unwrap()).unwrap();
				if binary != expected {
					mismatches.push(format!(
						"{name} in {ty:?} (binary): {binary:?}, expected {expected:?}"
					));
				}
			}
		}
		assert!(mismatches.is_empty(), "\n{}", mismatches.join("\n"));
	}

	#[test]
	fn a_text_cell_equals_the_text_cell_of_the_column_inferred_from_the_rows() {
		let mut mismatches = Vec::new();
		for (name, value, _) in text_family_values() {
			let mut row = surrealdb_types::Object::new();
			row.insert("c".to_string(), value.clone());
			let rows = array(vec![Value::Object(row.clone()), Value::Object(row)]);
			let shape = crate::typing::shape_result(rows);
			let inferred = &shape.columns[0].ty;
			let cell = shape.rows[0][0].clone().expect("a cell");
			let typed = text(cell, inferred.clone());
			let as_text = text(value.clone(), PgType::Text);
			if typed != as_text {
				mismatches.push(format!(
					"{name}: {inferred:?} column {typed:?}, text column {as_text:?}"
				));
			}
		}
		assert!(mismatches.is_empty(), "\n{}", mismatches.join("\n"));
	}

	#[test]
	fn values_that_a_typed_column_would_show_differently_are_typed_as_expected() {
		// The cases above compare a text column with a typed one only if the typed one differs.
		for (name, ty) in [
			("datetime", PgType::Timestamptz),
			("duration, 1h30m", PgType::Interval),
			("bool, true", PgType::Bool),
			("array, ints", PgType::Array(Box::new(PgType::Int8))),
			("array, datetimes", PgType::Array(Box::new(PgType::Timestamptz))),
			("array, mixed kinds are jsonb elements", PgType::Array(Box::new(PgType::Jsonb))),
			(
				"array of arrays is a multidimensional array",
				PgType::Array(Box::new(PgType::Array(Box::new(PgType::Int8)))),
			),
		] {
			let (_, value, _) =
				text_family_values().into_iter().find(|(n, ..)| *n == name).expect(name);
			assert_eq!(base_type(&value), Some(ty), "{name}");
		}
	}

	#[test]
	fn the_elements_of_a_text_array_column_show_as_a_text_column_does() {
		let ty = PgType::Array(Box::new(PgType::Text));
		let elements = array(vec![
			at(1_577_882_096, 789_000_000),
			lasting(5_400, 0),
			Value::Bool(true),
			int(7),
			string("x"),
		]);
		assert_eq!(text(elements, ty), "{\"2020-01-01 12:34:56.789+00\",01:30:00,t,7,x}");
	}

	#[test]
	fn the_fields_of_a_record_column_show_as_a_text_column_does() {
		let value = object(vec![
			("born", at(315_532_800, 0)),
			("flag", Value::Bool(false)),
			("took", lasting(5_400, 0)),
		]);
		assert_eq!(text(value, PgType::Record), "(\"1980-01-01 00:00:00+00\",f,01:30:00)");
	}

	#[test]
	fn nothing_has_no_text() {
		for ty in TEXT_FAMILY {
			assert!(encode_text(Value::None, &ty).is_err());
			assert!(encode_text(Value::Null, &ty).is_err());
		}
	}

	#[test]
	fn fractional_seconds_keep_only_their_significant_digits() {
		let mut out = Vec::new();
		for (micros, expected) in [
			(0, ""),
			(1, ".000001"),
			(10, ".00001"),
			(500_000, ".5"),
			(120_000, ".12"),
			(123_456, ".123456"),
			(999_999, ".999999"),
			(1_000_000, ""),
			(1_500_000, ".5"),
		] {
			out.clear();
			push_fraction(&mut out, micros);
			assert_eq!(String::from_utf8(out.clone()).unwrap(), expected, "{micros}");
		}
	}

	#[test]
	fn nested_arrays_in_a_jsonb_array_are_json_elements() {
		let inner =
			Value::Array(vec![Value::Number(Number::Int(2)), Value::Number(Number::Int(3))].into());
		let arr = Value::Array(vec![Value::Number(Number::Int(1)), inner].into());
		assert_eq!(text(arr, PgType::Array(Box::new(PgType::Jsonb))), "{1,\"[2,3]\"}");
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
	fn numeric_binary_beyond_a_decimal_is_a_bigdecimal() {
		// Forty digits, sixty-five places below the point, and 2^96, which lies just past the
		// largest decimal mantissa.
		for s in [
			"1234567890123456789012345678901234567890",
			"-1234567890123456789012345678901234567890.5",
			"0.00000000000000000000000000000000000000000000000000000000000000001",
			"79228162514264337593543950336",
		] {
			let wire = encode_numeric_binary(s);
			let decoded = decode_param(Some(&wire), OID_NUMERIC, 1).unwrap();
			let expected: BigDecimal = s.parse().unwrap();
			assert_eq!(decoded, Value::Number(Number::BigDecimal(Box::new(expected))), "for {s}");
		}
	}

	#[test]
	fn numeric_binary_bigdecimal_keeps_the_display_scale() {
		// The alignment leaves a trailing zero group; the display scale of 2 is kept as written.
		let wire = encode_numeric_binary("1234567890123456789012345678901234567890.50");
		let Value::Number(Number::BigDecimal(d)) =
			decode_param(Some(&wire), OID_NUMERIC, 1).unwrap()
		else {
			panic!("expected a bigdecimal");
		};
		assert_eq!(d.scale(), 2);
		assert_eq!(d.to_string(), "1234567890123456789012345678901234567890.50");
	}

	#[test]
	fn numeric_text_beyond_a_decimal_is_a_bigdecimal() {
		let text = "1234567890123456789012345678901234567890.0000000000000000000000000000001";
		let decoded = decode_param(Some(text.as_bytes()), OID_NUMERIC, 0).unwrap();
		let expected: BigDecimal = text.parse().unwrap();
		assert_eq!(decoded, Value::Number(Number::BigDecimal(Box::new(expected))));
		// A value that fits a decimal stays a decimal, with the scale it was written with.
		let Value::Number(Number::Decimal(d)) =
			decode_param(Some(b"1.50"), OID_NUMERIC, 0).unwrap()
		else {
			panic!("expected a decimal");
		};
		assert_eq!(d.scale(), 2);
	}

	#[test]
	fn temporal_params_take_the_type_they_are_compared_with() {
		let ts = Value::Timestamp("2024-01-15T12:34:56".parse().unwrap());
		let tz = coerce_temporal_param(ts.clone(), OID_TIMESTAMP, OID_TIMESTAMPTZ);
		assert_eq!(tz, Value::Datetime("2024-01-15T12:34:56Z".parse().unwrap()));
		assert_eq!(coerce_temporal_param(tz, OID_TIMESTAMPTZ, OID_TIMESTAMP), ts);
		let date = Value::Date(Date::from(chrono::NaiveDate::from_ymd_opt(2024, 1, 15).unwrap()));
		assert_eq!(
			coerce_temporal_param(date.clone(), OID_DATE, OID_TIMESTAMP),
			Value::Timestamp("2024-01-15T00:00:00".parse().unwrap())
		);
		assert_eq!(
			coerce_temporal_param(date.clone(), OID_DATE, OID_TIMESTAMPTZ),
			Value::Datetime("2024-01-15T00:00:00Z".parse().unwrap())
		);
		assert_eq!(
			coerce_temporal_param(Value::Date(Date::infinity()), OID_DATE, OID_TIMESTAMPTZ),
			Value::Datetime(Datetime::MAX_UTC)
		);
		// No conversion down to a date, and none for a type that is not temporal.
		assert_eq!(coerce_temporal_param(ts.clone(), OID_TIMESTAMP, OID_DATE), ts);
		assert_eq!(coerce_temporal_param(date.clone(), OID_DATE, OID_DATE), date);
		assert_eq!(coerce_temporal_param(int(1), OID_INT8, OID_TIMESTAMP), int(1));
	}

	#[test]
	fn a_time_without_seconds_is_a_time() {
		assert_eq!(
			decode_text("09:30", OID_TIME).unwrap(),
			Value::Time(surrealdb_types::Time::from_nanos(34_200_000_000_000).unwrap())
		);
	}

	#[test]
	fn a_timetz_offset_reads_in_every_spelling() {
		let utc_nanos = |text: &str| {
			let Value::Time(t) = decode_text(text, OID_TIMETZ).unwrap() else {
				panic!("expected a time");
			};
			t.nanos()
		};
		let hour = 3_600_000_000_000u64;
		let noon = 12 * hour;
		for offset in ["+0530", "+05:30"] {
			assert_eq!(utc_nanos(&format!("12:00:00{offset}")), noon - 5 * hour - hour / 2);
		}
		assert_eq!(utc_nanos("12:00:00+05"), noon - 5 * hour);
		assert_eq!(utc_nanos("12:00:00-0530"), noon + 5 * hour + hour / 2);
		assert!(decode_text("12:00:00+9999999999", OID_TIMETZ).is_err());
		assert!(decode_text("12:00:00+1600", OID_TIMETZ).is_err());
	}

	#[test]
	fn record_fields_double_their_quotes_and_backslashes() {
		let mut out = Vec::new();
		push_record_field(&mut out, b"a\"b\\c").unwrap();
		assert_eq!(out, b"\"a\"\"b\\\\c\"");
		let mut out = Vec::new();
		push_record_field(&mut out, b"a\nb").unwrap();
		assert_eq!(out, b"\"a\nb\"");
	}

	#[test]
	fn numeric_binary_infinity_round_trips() {
		let pos = encode_numeric_binary("Infinity");
		assert_eq!(
			decode_param(Some(&pos), OID_NUMERIC, 1).unwrap(),
			Value::Number(Number::Float(f64::INFINITY))
		);
		let neg = encode_numeric_binary("-Infinity");
		assert_eq!(
			decode_param(Some(&neg), OID_NUMERIC, 1).unwrap(),
			Value::Number(Number::Float(f64::NEG_INFINITY))
		);
	}

	#[test]
	fn numeric_binary_nan_is_rejected() {
		let wire = encode_numeric_binary("NaN");
		assert!(decode_param(Some(&wire), OID_NUMERIC, 1).is_err());
	}

	#[test]
	fn unknown_binary_oid_errors() {
		// An unmodelled binary type is rejected rather than silently kept as bytes.
		assert!(decode_param(Some(&[0u8, 1, 2, 3]), 2_000_000_000, 1).is_err());
	}

	#[test]
	fn unspecified_binary_oid_falls_back_to_string() {
		// A binary-format parameter with OID 0 (unspecified) decodes as a string,
		// mirroring the text path, instead of erroring on the format choice.
		assert_eq!(decode_param(Some(b"hello"), 0, 1).unwrap(), Value::String("hello".to_string()));
	}

	#[test]
	fn numeric_column_renders_non_finite_forms() {
		assert_eq!(text(Value::Number(Number::Float(f64::INFINITY)), PgType::Numeric), "Infinity");
		assert_eq!(
			text(Value::Number(Number::Float(f64::NEG_INFINITY)), PgType::Numeric),
			"-Infinity"
		);
		assert_eq!(text(Value::Number(Number::Float(f64::NAN)), PgType::Numeric), "NaN");
		// Finite floats still render normally in a numeric column.
		assert_eq!(text(Value::Number(Number::Float(1.5)), PgType::Numeric), "1.5");
	}

	#[test]
	fn parses_offsetless_and_hours_only_timestamps() {
		let expected: Datetime = "2024-01-15T12:34:56Z".parse().unwrap();
		// `timestamp` text (no offset) is a wall time with no zone.
		assert_eq!(
			decode_param(Some(b"2024-01-15 12:34:56"), OID_TIMESTAMP, 0).unwrap(),
			Value::Timestamp("2024-01-15T12:34:56".parse().unwrap())
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
			Value::Interval(surrealdb_types::Interval::new(0, 0, 90_061_000_000))
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
	fn narrow_integer_columns_refuse_wider_values() {
		let wide = Value::Number(Number::Int(i64::from(i32::MAX) + 1));
		for ty in [PgType::Int2, PgType::Int4] {
			let text = encode_text(wide.clone(), &ty).unwrap_err();
			let binary = encode_binary(wide.clone(), &ty).unwrap_err();
			assert_eq!(text.code, "22003");
			assert_eq!(binary.code, "22003");
		}
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

	fn rows(rows: Vec<Vec<Value>>) -> Value {
		Value::Array(
			rows.into_iter().map(|row| Value::Array(row.into())).collect::<Vec<_>>().into(),
		)
	}

	#[test]
	fn a_rectangular_array_of_arrays_is_written_as_a_multidimensional_array() {
		let int4_grid = PgType::Array(Box::new(PgType::Array(Box::new(PgType::Int4))));
		let grid = rows(vec![vec![int(1), Value::Null], vec![int(3), int(4)]]);
		assert_eq!(
			String::from_utf8(encode_text(grid.clone(), &int4_grid).unwrap()).unwrap(),
			"{{1,NULL},{3,4}}"
		);
		let bytes = encode_binary(grid.clone(), &int4_grid).unwrap();
		let mut header = Vec::new();
		header.extend_from_slice(&2i32.to_be_bytes()); // ndim
		header.extend_from_slice(&1i32.to_be_bytes()); // has_null
		header.extend_from_slice(&23i32.to_be_bytes()); // int4
		for len in [2i32, 2] {
			header.extend_from_slice(&len.to_be_bytes());
			header.extend_from_slice(&1i32.to_be_bytes()); // lower bound
		}
		assert_eq!(&bytes[..header.len()], header.as_slice());
		assert_eq!(decode_param(Some(&bytes), 1007, 1).unwrap(), grid);
	}

	#[test]
	fn a_multidimensional_array_of_strings_quotes_its_elements_and_nulls() {
		let text_grid = PgType::Array(Box::new(PgType::Array(Box::new(PgType::Text))));
		let grid = rows(vec![
			vec![Value::String("a,b".into()), Value::String("NULL".into())],
			vec![Value::Null, Value::String("c d".into())],
		]);
		assert_eq!(
			String::from_utf8(encode_text(grid.clone(), &text_grid).unwrap()).unwrap(),
			"{{\"a,b\",\"NULL\"},{NULL,\"c d\"}}"
		);
		let bytes = encode_binary(grid.clone(), &text_grid).unwrap();
		assert_eq!(decode_param(Some(&bytes), 1009, 1).unwrap(), grid);
		let text = b"{{\"a,b\",\"NULL\"},{NULL,\"c d\"}}";
		assert_eq!(decode_param(Some(text), 1009, 0).unwrap(), grid);
	}

	#[test]
	fn a_ragged_array_of_arrays_is_not_a_multidimensional_array() {
		let int4_grid = PgType::Array(Box::new(PgType::Array(Box::new(PgType::Int4))));
		let ragged = rows(vec![vec![int(1)], vec![int(2), int(3)]]);
		assert!(encode_text(ragged.clone(), &int4_grid).is_err());
		assert!(encode_binary(ragged, &int4_grid).is_err());
		let mixed = Value::Array(vec![int(1), Value::Array(vec![int(2)].into())].into());
		assert!(encode_text(mixed, &int4_grid).is_err());
	}

	#[test]
	fn an_empty_array_of_arrays_has_no_dimensions() {
		let int4_grid = PgType::Array(Box::new(PgType::Array(Box::new(PgType::Int4))));
		let empty = Value::Array(Vec::<Value>::new().into());
		let bytes = encode_binary(empty.clone(), &int4_grid).unwrap();
		assert_eq!(bytes[..4], 0i32.to_be_bytes());
		assert_eq!(decode_param(Some(&bytes), 1007, 1).unwrap(), empty);
		assert_eq!(decode_param(Some(b"{}"), 1007, 0).unwrap(), empty);
	}

	#[test]
	fn a_multidimensional_text_parameter_decodes_to_nested_arrays() {
		let grid = rows(vec![vec![int(1), int(2)], vec![int(3), Value::Null]]);
		assert_eq!(decode_param(Some(b"{{1,2},{3,NULL}}"), 1007, 0).unwrap(), grid);
		assert!(decode_param(Some(b"{{1,2},{3}}"), 1007, 0).is_err(), "ragged text");
		assert!(decode_param(Some(b"{{1,2},3}"), 1007, 0).is_err(), "mixed text");
	}

	#[test]
	fn a_multidimensional_binary_parameter_decodes_to_nested_arrays() {
		let mut bytes = Vec::new();
		bytes.extend_from_slice(&2i32.to_be_bytes());
		bytes.extend_from_slice(&0i32.to_be_bytes());
		bytes.extend_from_slice(&23i32.to_be_bytes());
		for len in [2i32, 3] {
			bytes.extend_from_slice(&len.to_be_bytes());
			bytes.extend_from_slice(&1i32.to_be_bytes());
		}
		// Six int4 elements, row-major: {{1,2,3},{4,5,6}}.
		for value in 1i32..=6 {
			bytes.extend_from_slice(&4i32.to_be_bytes());
			bytes.extend_from_slice(&value.to_be_bytes());
		}
		assert_eq!(
			decode_param(Some(&bytes), 1007, 1).unwrap(),
			rows(vec![vec![int(1), int(2), int(3)], vec![int(4), int(5), int(6)]])
		);
	}

	#[test]
	fn a_binary_array_with_a_lower_bound_other_than_one_is_refused() {
		let mut bytes = Vec::new();
		bytes.extend_from_slice(&1i32.to_be_bytes());
		bytes.extend_from_slice(&0i32.to_be_bytes());
		bytes.extend_from_slice(&23i32.to_be_bytes());
		bytes.extend_from_slice(&1i32.to_be_bytes()); // length
		bytes.extend_from_slice(&0i32.to_be_bytes()); // lower bound 0
		bytes.extend_from_slice(&4i32.to_be_bytes());
		bytes.extend_from_slice(&7i32.to_be_bytes());
		let err = decode_param(Some(&bytes), 1007, 1).unwrap_err();
		assert!(format!("{err:?}").contains("lower bound"), "{err:?}");
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
	fn a_binary_numeric_is_consumed_exactly_with_a_known_sign() {
		// 12 as one base-10000 group: ndigits 1, weight 0, sign +, dscale 0.
		let twelve = [0, 1, 0, 0, 0, 0, 0, 0, 0, 12];
		assert!(decode_numeric_binary(&twelve, OID_NUMERIC).is_ok());
		let negative = [0, 1, 0, 0, 0x40, 0, 0, 0, 0, 12];
		assert!(decode_numeric_binary(&negative, OID_NUMERIC).is_ok());
		let bad_sign = [0, 1, 0, 0, 0, 1, 0, 0, 0, 12];
		assert!(decode_numeric_binary(&bad_sign, OID_NUMERIC).is_err());
		let trailing = [&twelve[..], &[0, 34]].concat();
		assert!(decode_numeric_binary(&trailing, OID_NUMERIC).is_err());
		let zero_with_groups = [0, 0, 0, 0, 0, 0, 0, 0, 0, 7];
		assert!(decode_numeric_binary(&zero_with_groups, OID_NUMERIC).is_err());
	}

	#[test]
	fn a_binary_record_with_trailing_bytes_is_refused() {
		// One int4 field holding 7.
		let record = [0, 0, 0, 1, 0, 0, 0, 23, 0, 0, 0, 4, 0, 0, 0, 7];
		assert!(decode_param(Some(&record), OID_RECORD, 1).is_ok());
		let padded = [&record[..], &[0, 0, 0, 23, 0, 0, 0, 4, 0, 0, 0, 8]].concat();
		assert!(decode_param(Some(&padded), OID_RECORD, 1).is_err());
	}

	#[test]
	fn a_binary_array_with_trailing_bytes_is_refused() {
		let arr = Value::Array(vec![Value::Number(Number::Int(1))].into());
		let mut bin = encode_binary(arr, &PgType::Array(Box::new(PgType::Int4))).unwrap();
		bin.extend_from_slice(&[0, 0, 0, 0]);
		assert!(decode_param(Some(&bin), OID_ARRAY_INT4, 1).is_err());
		// No dimensions, then bytes that would be a dimension.
		let empty = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 23];
		assert!(decode_param(Some(&empty), OID_ARRAY_INT4, 1).is_ok());
		let padded = [&empty[..], &[0, 0, 0, 1, 0, 0, 0, 1]].concat();
		assert!(decode_param(Some(&padded), OID_ARRAY_INT4, 1).is_err());
		// A zero-length dimension holds no elements.
		let zero = [&[0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 23][..], &[0, 0, 0, 0, 0, 0, 0, 1]].concat();
		assert!(decode_param(Some(&zero), OID_ARRAY_INT4, 1).is_ok());
		let zero_padded = [&zero[..], &[0, 0, 0, 4]].concat();
		assert!(decode_param(Some(&zero_padded), OID_ARRAY_INT4, 1).is_err());
		// A header naming `text` elements for an `int4[]` parameter.
		let text_header = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 25];
		assert!(decode_param(Some(&text_header), OID_ARRAY_INT4, 1).is_err());
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
	fn text_array_parameters_read_as_postgresql_reads_array_input() {
		let decode = |text: &str| decode_param(Some(text.as_bytes()), OID_ARRAY_TEXT, 0);
		let strings = |items: &[&str]| {
			Value::Array(
				items.iter().map(|s| Value::String(s.to_string())).collect::<Vec<_>>().into(),
			)
		};
		assert_eq!(
			decode("{\"NULL\",null}").unwrap(),
			Value::Array(vec![Value::String("NULL".into()), Value::Null].into())
		);
		assert_eq!(decode("{a,\"\"}").unwrap(), strings(&["a", ""]));
		assert_eq!(decode("{\" a \",b }").unwrap(), strings(&[" a ", "b"]));
		assert_eq!(decode("{\"a\\\\b\",\"c\\\"d\"}").unwrap(), strings(&["a\\b", "c\"d"]));
		assert_eq!(
			decode("{{\"a,b\",c},{\"}\",\"{\"}}").unwrap(),
			Value::Array(vec![strings(&["a,b", "c"]), strings(&["}", "{"])].into())
		);
		for malformed in ["a,b", "{a,,b}", "{a", "{\"a}", "{{a},b}", "{{a},{b,c}}", "{a} b"] {
			let err = decode(malformed).unwrap_err();
			assert_eq!(err.code, "22P02", "{malformed}");
		}
		assert_eq!(
			decode_param(Some(b"{1, -2 ,NULL}"), OID_ARRAY_INT8, 0).unwrap(),
			Value::Array(
				vec![Value::Number(Number::Int(1)), Value::Number(Number::Int(-2)), Value::Null]
					.into()
			)
		);
	}

	#[test]
	fn text_array_values_round_trip_through_their_literal() {
		let values = [
			"", "NULL", "null", " lead", "trail ", "a,b", "{x}", "q\"q", "b\\s", "tab\t", "cr\r",
			"vt\u{b}", "ff\u{c}", "plain",
		];
		let arr = Value::Array(
			values.iter().map(|s| Value::String(s.to_string())).collect::<Vec<_>>().into(),
		);
		let text = encode_text(arr.clone(), &PgType::Array(Box::new(PgType::Text))).unwrap();
		assert_eq!(decode_param(Some(&text), OID_ARRAY_TEXT, 0).unwrap(), arr);
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

	#[test]
	fn binary_date_extremes_are_infinity() {
		let pos = decode_param(Some(&i32::MAX.to_be_bytes()), OID_DATE, 1).unwrap();
		assert_eq!(pos, Value::Date(Date::infinity()));
		let neg = decode_param(Some(&i32::MIN.to_be_bytes()), OID_DATE, 1).unwrap();
		assert_eq!(neg, Value::Date(Date::neg_infinity()));
		assert!(decode_param(Some(&(i32::MAX - 1).to_be_bytes()), OID_DATE, 1).is_err());
	}

	#[test]
	fn text_infinity_and_epoch_parse_for_every_temporal_oid() {
		for text in ["infinity", "+infinity", "Infinity", "INFINITY"] {
			let got = decode_param(Some(text.as_bytes()), OID_DATE, 0).unwrap();
			assert_eq!(got, Value::Date(Date::infinity()), "{text}");
			let got = decode_param(Some(text.as_bytes()), OID_TIMESTAMP, 0).unwrap();
			assert_eq!(got, Value::Timestamp(Timestamp::infinity()), "{text}");
			let got = decode_param(Some(text.as_bytes()), OID_TIMESTAMPTZ, 0).unwrap();
			assert_eq!(got, Value::Datetime(Datetime::MAX_UTC), "{text}");
		}
		for text in ["-infinity", "-Infinity", "-INFINITY"] {
			let got = decode_param(Some(text.as_bytes()), OID_DATE, 0).unwrap();
			assert_eq!(got, Value::Date(Date::neg_infinity()), "{text}");
			let got = decode_param(Some(text.as_bytes()), OID_TIMESTAMP, 0).unwrap();
			assert_eq!(got, Value::Timestamp(Timestamp::neg_infinity()), "{text}");
			let got = decode_param(Some(text.as_bytes()), OID_TIMESTAMPTZ, 0).unwrap();
			assert_eq!(got, Value::Datetime(Datetime::MIN_UTC), "{text}");
		}
		let epoch = decode_param(Some(b"epoch"), OID_DATE, 0).unwrap();
		assert_eq!(epoch, Value::Date(Date::from_days(0).unwrap()));
		let epoch = decode_param(Some(b"epoch"), OID_TIMESTAMP, 0).unwrap();
		assert_eq!(epoch, timestamp("1970-01-01T00:00:00"));
		let epoch = decode_param(Some(b"epoch"), OID_TIMESTAMPTZ, 0).unwrap();
		assert_eq!(epoch, Value::Datetime(Datetime::from_timestamp(0, 0).unwrap()));
	}

	#[test]
	fn binary_timestamp_extremes_are_infinity() {
		let pos = decode_param(Some(&i64::MAX.to_be_bytes()), OID_TIMESTAMP, 1).unwrap();
		assert_eq!(pos, Value::Timestamp(Timestamp::infinity()));
		let neg = decode_param(Some(&i64::MIN.to_be_bytes()), OID_TIMESTAMP, 1).unwrap();
		assert_eq!(neg, Value::Timestamp(Timestamp::neg_infinity()));
		let pos = decode_param(Some(&i64::MAX.to_be_bytes()), OID_TIMESTAMPTZ, 1).unwrap();
		assert_eq!(pos, Value::Datetime(Datetime::MAX_UTC));
		let neg = decode_param(Some(&i64::MIN.to_be_bytes()), OID_TIMESTAMPTZ, 1).unwrap();
		assert_eq!(neg, Value::Datetime(Datetime::MIN_UTC));
	}

	#[test]
	fn infinity_encodes_as_infinity_in_text_and_as_extremes_in_binary() {
		let pos_date = Value::Date(Date::infinity());
		let neg_ts = Value::Timestamp(Timestamp::neg_infinity());
		let pos_dt = Value::Datetime(Datetime::MAX_UTC);
		assert_eq!(encode_text(pos_date.clone(), &PgType::Date).unwrap(), b"infinity");
		assert_eq!(encode_text(neg_ts.clone(), &PgType::Timestamp).unwrap(), b"-infinity");
		assert_eq!(encode_text(pos_dt.clone(), &PgType::Timestamptz).unwrap(), b"infinity");
		assert_eq!(encode_text(neg_ts.clone(), &PgType::Date).unwrap(), b"-infinity");
		assert_eq!(
			encode_binary(pos_date, &PgType::Date).unwrap(),
			i32::MAX.to_be_bytes().to_vec()
		);
		assert_eq!(
			encode_binary(neg_ts, &PgType::Timestamp).unwrap(),
			i64::MIN.to_be_bytes().to_vec()
		);
		assert_eq!(
			encode_binary(pos_dt, &PgType::Timestamptz).unwrap(),
			i64::MAX.to_be_bytes().to_vec()
		);
	}

	#[test]
	fn infinity_round_trips_through_both_formats() {
		for value in [
			Value::Date(Date::infinity()),
			Value::Date(Date::neg_infinity()),
			Value::Timestamp(Timestamp::infinity()),
			Value::Timestamp(Timestamp::neg_infinity()),
		] {
			let (oid, ty) = if matches!(value, Value::Date(_)) {
				(OID_DATE, PgType::Date)
			} else {
				(OID_TIMESTAMP, PgType::Timestamp)
			};
			let text = encode_text(value.clone(), &ty).unwrap();
			assert_eq!(decode_param(Some(&text), oid, 0).unwrap(), value);
			let bytes = encode_binary(value.clone(), &ty).unwrap();
			assert_eq!(decode_param(Some(&bytes), oid, 1).unwrap(), value);
		}
	}

	#[test]
	fn binary_array_huge_dimensions_do_not_panic() {
		let mut wire = Vec::new();
		wire.extend_from_slice(&2i32.to_be_bytes()); // ndim = 2
		wire.extend_from_slice(&0i32.to_be_bytes()); // flags
		wire.extend_from_slice(&OID_INT4.to_be_bytes()); // elem_oid
		wire.extend_from_slice(&i32::MAX.to_be_bytes()); // dim 0 = 2^31 - 1
		wire.extend_from_slice(&1i32.to_be_bytes()); // lbound 0
		wire.extend_from_slice(&i32::MAX.to_be_bytes()); // dim 1 = 2^31 - 1
		wire.extend_from_slice(&1i32.to_be_bytes()); // lbound 1
		assert!(decode_param(Some(&wire), OID_ARRAY_INT4, 1).is_err());

		// Negative ndim is also rejected cleanly.
		let mut neg_ndim = Vec::new();
		neg_ndim.extend_from_slice(&(-1i32).to_be_bytes());
		neg_ndim.extend_from_slice(&0i32.to_be_bytes());
		neg_ndim.extend_from_slice(&OID_INT4.to_be_bytes());
		assert!(decode_param(Some(&neg_ndim), OID_ARRAY_INT4, 1).is_err());
	}

	#[test]
	fn binary_nested_array_and_record_recursion_is_bounded() {
		// Build a deeply nested binary array payload where each outer array has elem_oid =
		// OID_ARRAY_INT4.
		let mut payload = Vec::new();
		payload.extend_from_slice(&0i32.to_be_bytes()); // innermost: ndim = 0
		payload.extend_from_slice(&0i32.to_be_bytes());
		payload.extend_from_slice(&OID_INT4.to_be_bytes());

		for _ in 0..(MAX_BINARY_DECODE_DEPTH + 2) {
			let mut outer = Vec::new();
			outer.extend_from_slice(&1i32.to_be_bytes()); // ndim = 1
			outer.extend_from_slice(&0i32.to_be_bytes()); // flags
			outer.extend_from_slice(&OID_ARRAY_INT4.to_be_bytes()); // elem_oid = int4[]
			outer.extend_from_slice(&1i32.to_be_bytes()); // dim_len = 1
			outer.extend_from_slice(&1i32.to_be_bytes()); // lbound = 1
			outer.extend_from_slice(&(payload.len() as i32).to_be_bytes());
			outer.extend_from_slice(&payload);
			payload = outer;
		}
		assert!(decode_param(Some(&payload), OID_ARRAY_INT4, 1).is_err());
	}

	fn date(text: &str) -> Value {
		Value::Date(text.parse::<surrealdb_types::Date>().unwrap())
	}

	fn time_of(text: &str) -> Value {
		Value::Time(text.parse::<surrealdb_types::Time>().unwrap())
	}

	fn timestamp(text: &str) -> Value {
		Value::Timestamp(text.parse::<surrealdb_types::Timestamp>().unwrap())
	}

	fn interval(months: i32, days: i32, micros: i64) -> Value {
		Value::Interval(surrealdb_types::Interval::new(months, days, micros))
	}

	#[test]
	fn the_temporal_values_have_their_own_types() {
		assert_eq!(base_type(&date("2026-10-27")), Some(PgType::Date));
		assert_eq!(base_type(&time_of("12:30:00")), Some(PgType::Time));
		assert_eq!(base_type(&timestamp("2026-10-27T12:30:00")), Some(PgType::Timestamp));
		assert_eq!(base_type(&interval(1, 0, 0)), Some(PgType::Interval));
		assert_eq!(PgType::Date.oid(), 1082);
		assert_eq!(PgType::Time.oid(), 1083);
		assert_eq!(PgType::Timestamp.oid(), 1114);
		assert_eq!(PgType::Interval.oid(), 1186);
		assert_eq!(PgType::Array(Box::new(PgType::Date)).oid(), 1182);
		assert_eq!(PgType::Array(Box::new(PgType::Timestamp)).oid(), 1115);
		assert_eq!(PgType::Array(Box::new(PgType::Interval)).oid(), 1187);
	}

	#[test]
	fn the_temporal_values_have_postgresql_text_forms() {
		assert_eq!(text(date("2026-10-27"), PgType::Date), "2026-10-27");
		assert_eq!(text(date("0000-01-01"), PgType::Date), "0001-01-01 BC");
		assert_eq!(text(time_of("12:30:00.123456789"), PgType::Time), "12:30:00.123456");
		assert_eq!(text(time_of("24:00:00"), PgType::Time), "24:00:00");
		assert_eq!(text(time_of("12:30:00"), PgType::Timetz), "12:30:00+00");
		assert_eq!(
			text(timestamp("2026-10-27T12:30:00.5"), PgType::Timestamp),
			"2026-10-27 12:30:00.5"
		);
		assert_eq!(
			text(timestamp("2026-10-27T12:30:00"), PgType::Timestamptz),
			"2026-10-27 12:30:00+00"
		);
		assert_eq!(text(date("2026-10-27"), PgType::Timestamp), "2026-10-27 00:00:00");
	}

	#[test]
	fn an_interval_is_written_in_the_postgres_style() {
		let hms = |h: i64, m: i64, s: i64, us: i64| ((h * 60 + m) * 60 + s) * 1_000_000 + us;
		let cases = [
			(interval(0, 0, 0), "00:00:00"),
			(interval(14, 3, hms(4, 5, 6, 500_000)), "1 year 2 mons 3 days 04:05:06.5"),
			(interval(12, 0, 0), "1 year"),
			(interval(1, 0, 0), "1 mon"),
			(interval(0, 1, 0), "1 day"),
			(interval(0, 2, 0), "2 days"),
			(interval(0, 0, hms(100, 0, 0, 0)), "100:00:00"),
			(interval(-14, 0, 0), "-1 years -2 mons"),
			(interval(0, -3, 0), "-3 days"),
			(interval(0, 0, -hms(1, 2, 3, 0)), "-01:02:03"),
			(interval(0, 3, -hms(1, 0, 0, 0)), "3 days -01:00:00"),
			(interval(0, -3, hms(1, 0, 0, 0)), "-3 days +01:00:00"),
			(interval(-12, 5, 0), "-1 years +5 days"),
		];
		for (value, want) in cases {
			assert_eq!(text(value.clone(), PgType::Interval), want, "{}", value.to_sql());
		}
	}

	#[test]
	fn the_temporal_binary_forms_count_from_2000() {
		// 2000-01-01 is the epoch of `date` and `timestamp`.
		assert_eq!(bin(date("2000-01-01"), PgType::Date), 0i32.to_be_bytes());
		assert_eq!(bin(date("2000-01-02"), PgType::Date), 1i32.to_be_bytes());
		assert_eq!(bin(date("1999-12-31"), PgType::Date), (-1i32).to_be_bytes());
		assert_eq!(bin(time_of("00:00:01.5"), PgType::Time), 1_500_000i64.to_be_bytes());
		assert_eq!(bin(time_of("24:00:00"), PgType::Time), 86_400_000_000i64.to_be_bytes());
		assert_eq!(
			bin(timestamp("2000-01-01T00:00:01"), PgType::Timestamp),
			1_000_000i64.to_be_bytes()
		);
		assert_eq!(
			bin(timestamp("1999-12-31T23:59:59"), PgType::Timestamp),
			(-1_000_000i64).to_be_bytes()
		);
		let mut want = Vec::new();
		want.extend_from_slice(&(-5_000_000i64).to_be_bytes());
		want.extend_from_slice(&(-3i32).to_be_bytes());
		want.extend_from_slice(&14i32.to_be_bytes());
		assert_eq!(bin(interval(14, -3, -5_000_000), PgType::Interval), want);
	}

	#[test]
	fn the_temporal_binary_forms_decode_into_the_new_kinds() {
		let decoded = |bytes: &[u8], oid: i32| decode_param(Some(bytes), oid, 1).unwrap();
		assert_eq!(decoded(&1i32.to_be_bytes(), OID_DATE), date("2000-01-02"));
		assert_eq!(decoded(&1_500_000i64.to_be_bytes(), OID_TIME), time_of("00:00:01.5"));
		assert_eq!(
			decoded(&(-1_000_000i64).to_be_bytes(), OID_TIMESTAMP),
			timestamp("1999-12-31T23:59:59")
		);
		assert!(matches!(
			decoded(&1_000_000i64.to_be_bytes(), OID_TIMESTAMPTZ),
			Value::Datetime(_)
		));
		let wire = bin(interval(14, -3, -5_000_000), PgType::Interval);
		assert_eq!(decoded(&wire, OID_INTERVAL), interval(14, -3, -5_000_000));
		// A `timetz` of 10:00 at an offset of two hours east is 08:00 UTC: the zone is the seconds
		// west of UTC, so -7200.
		let mut timetz = Vec::new();
		timetz.extend_from_slice(&36_000_000_000i64.to_be_bytes());
		timetz.extend_from_slice(&(-7200i32).to_be_bytes());
		assert_eq!(decoded(&timetz, OID_TIMETZ), time_of("08:00:00"));
		let timetz_of = |micros: i64, zone: i32| {
			let mut wire = micros.to_be_bytes().to_vec();
			wire.extend_from_slice(&zone.to_be_bytes());
			decode_param(Some(&wire), OID_TIMETZ, 1)
		};
		assert!(timetz_of(86_400_000_001, 0).is_err());
		assert!(timetz_of(-1, 0).is_err());
		assert!(timetz_of(0, 57_600).is_err());
		assert!(timetz_of(0, -57_600).is_err());
		assert!(timetz_of(86_400_000_000, -57_599).is_ok());
		// A finite date or timestamp at the edge of the range is refused, not read as infinity.
		let last_day = Date::infinity().days() - PG_EPOCH_UNIX_DAYS;
		assert!(decode_param(Some(&last_day.to_be_bytes()), OID_DATE, 1).is_err());
		assert!(decode_param(Some(&(last_day - 1).to_be_bytes()), OID_DATE, 1).is_ok());
		assert_eq!(
			decode_param(Some(&i32::MAX.to_be_bytes()), OID_DATE, 1).unwrap(),
			Value::Date(Date::infinity())
		);
	}

	#[test]
	fn the_temporal_text_forms_decode_into_the_new_kinds() {
		let decoded = |text: &str, oid: i32| decode_param(Some(text.as_bytes()), oid, 0).unwrap();
		assert_eq!(decoded("2026-10-27", OID_DATE), date("2026-10-27"));
		assert_eq!(decoded("12:30:00.5", OID_TIME), time_of("12:30:00.5"));
		for end in ["24:00", "24:00:00", "24:00:00.0", "24:00:00.000000"] {
			assert_eq!(decoded(end, OID_TIME), time_of("24:00:00"), "{end}");
		}
		for bad in ["24:00:01", "24:00:00.1", "24:01", "24:00:00."] {
			assert!(parse_time_nanos(bad).is_none(), "{bad}");
		}
		assert_eq!(decoded("10:00:00+02", OID_TIMETZ), time_of("08:00:00"));
		assert_eq!(decoded("00:30:00+02", OID_TIMETZ), time_of("22:30:00"));
		assert_eq!(decoded("2026-10-27 12:30:00", OID_TIMESTAMP), timestamp("2026-10-27T12:30:00"));
		// A `timestamp` ignores a zone designator.
		assert_eq!(
			decoded("2026-10-27 12:30:00+05", OID_TIMESTAMP),
			timestamp("2026-10-27T12:30:00")
		);
		assert!(matches!(decoded("2026-10-27 12:30:00+05", OID_TIMESTAMPTZ), Value::Datetime(_)));
		assert_eq!(
			decoded("1 year 2 mons 3 days 04:05:06.5", OID_INTERVAL),
			interval(14, 3, 14_706_500_000)
		);
		assert_eq!(decoded("-3 days", OID_INTERVAL), interval(0, -3, 0));
		assert!(decode_param(Some(b"not an interval"), OID_INTERVAL, 0).is_err());
	}
}
