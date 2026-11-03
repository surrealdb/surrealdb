#![deny(clippy::wildcard_enum_match_arm)]

use std::fmt;
use std::ops::Bound;
use std::str::FromStr as _;

use common::decimal::DecimalExt;
use geo_types::Point;
use rust_decimal::Decimal;
use surrealdb_cnf::GENERATION_ALLOCATION_LIMIT;
use surrealdb_types::{BigDecimal, ToSql};

use super::coerce::ElementPosition;
use crate::expr::Kind;
use crate::expr::kind::{GeometryKind, HasKind, KindLiteral};
use crate::syn;
use crate::val::range::IntegerRangeExt;
use crate::val::{
	Array, Bytes, Closure, Date, Datetime, Duration, File, Geometry, Interval, Null, Number,
	Object, Range, RecordId, Regex, Set, SqlNone, TableName, Time, Timestamp, Uuid, Value,
};

#[derive(Clone, Debug)]
pub enum CastError {
	// Coercion error at the end.
	InvalidKind {
		from: Value,
		into: String,
	},
	InvalidLength {
		len: usize,
		into: String,
	},
	/// Cast failed because element of type didn't match.
	ElementOf {
		inner: Box<CastError>,
		into: String,
		position: Option<ElementPosition>,
	},
	// Annoying error which doesn't fit in with the rest of the errors and breaks the trait
	// pattern.
	//
	// Remove once the move to anyhow is complete.
	RangeSizeLimit {
		value: Box<Range>,
	},
}
impl std::error::Error for CastError {}
impl fmt::Display for CastError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			CastError::InvalidKind {
				from,
				into,
			} => {
				write!(f, "Could not cast into `{into}` using input `{from}`", from = from.to_sql())
			}
			CastError::ElementOf {
				inner,
				into,
				position,
			} => {
				inner.fmt(f)?;
				match position {
					Some(ElementPosition::Index(i)) => {
						write!(f, " when casting element at index {i} of `{into}`")
					}
					Some(ElementPosition::Key(k)) => {
						write!(f, " when casting value for key '{k}' of `{into}`")
					}
					None => write!(f, " when casting an element of `{into}`"),
				}
			}
			CastError::InvalidLength {
				len,
				into,
			} => {
				write!(f, "Expected `{into}` but found a collection of length `{len}`")
			}
			CastError::RangeSizeLimit {
				value,
			} => {
				write!(
					f,
					"Casting range `{value}` to an array would create an array larger then the max allocation limit.",
					value = value.to_sql()
				)
			}
		}
	}
}

pub trait CastErrorExt {
	fn with_element_of_at_index<F>(self, index: usize, f: F) -> Self
	where
		F: Fn() -> String;
}

impl<T> CastErrorExt for Result<T, CastError> {
	fn with_element_of_at_index<F>(self, index: usize, f: F) -> Self
	where
		F: Fn() -> String,
	{
		match self {
			Ok(x) => Ok(x),
			Err(e) => Err(CastError::ElementOf {
				inner: Box::new(e),
				into: f(),
				position: Some(ElementPosition::Index(index)),
			}),
		}
	}
}

/// Trait for converting the value using casting rules, calling the functions on
/// this trait results in similar behavior as casting does in surrealql like
/// `<string> 1`.
///
/// Casting rules are more loose then coercing rules.
pub trait Cast: Sized {
	/// Returns true if calling cast on the value will succeed.
	///
	/// If `T::can_cast(&v)` returns `true` then `T::cast(v) should not return
	/// an error.
	fn can_cast(v: &Value) -> bool;

	/// Cast a value to the self type.
	fn cast(v: Value) -> Result<Self, CastError>;
}

impl Cast for Value {
	fn can_cast(_: &Value) -> bool {
		true
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		Ok(v)
	}
}

impl Cast for SqlNone {
	fn can_cast(v: &Value) -> bool {
		matches!(v, Value::None)
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		if v.is_none() {
			Ok(SqlNone)
		} else {
			Err(CastError::InvalidKind {
				from: v,
				into: "none".to_string(),
			})
		}
	}
}

impl Cast for Null {
	fn can_cast(v: &Value) -> bool {
		matches!(v, Value::Null)
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		if v.is_null() {
			Ok(Null)
		} else {
			Err(CastError::InvalidKind {
				from: v,
				into: "null".to_string(),
			})
		}
	}
}

impl Cast for bool {
	fn can_cast(v: &Value) -> bool {
		v.is_bool() || v.is_strand_and(|x| matches!(x.as_str(), "true" | "false"))
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		if let Value::Bool(b) = v {
			return Ok(b);
		}
		if let Value::String(ref x) = v {
			match x.as_str() {
				"true" => return Ok(true),
				"false" => return Ok(false),
				_ => {}
			}
		}
		Err(CastError::InvalidKind {
			from: v,
			into: "bool".to_string(),
		})
	}
}

impl Cast for i64 {
	/// Must agree with `cast`: a union kind casts to the first member
	/// `can_cast` accepts and then requires `cast` to succeed.
	fn can_cast(v: &Value) -> bool {
		v.is_number_and(|n| n.as_int_lossless().is_some())
			|| v.is_strand_and(|v| v.parse::<i64>().is_ok())
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		if let Value::Number(ref n) = v {
			// An int, or a float/decimal that is a whole number within the
			// `i64` range. Anything else is refused, never rounded or clamped
			// to the nearest bound.
			return n.as_int_lossless().ok_or_else(|| CastError::InvalidKind {
				from: v,
				into: "int".into(),
			});
		}
		// Attempt to convert a string value
		if let Value::String(ref s) = v
			&& let Ok(i) = s.parse::<i64>()
		{
			return Ok(i);
		}
		Err(CastError::InvalidKind {
			from: v,
			into: "int".into(),
		})
	}
}

impl Cast for f64 {
	fn can_cast(v: &Value) -> bool {
		v.is_number_and(|n| n.as_float_lossless().is_some())
			|| v.is_strand_and(|v| v.parse::<f64>().is_ok())
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		if let Value::Number(ref n) = v {
			return n.as_float_lossless().ok_or_else(|| CastError::InvalidKind {
				from: v,
				into: "float".into(),
			});
		}
		// Attempt to convert a string value
		if let Value::String(ref s) = v
			&& let Ok(f) = s.parse::<f64>()
		{
			return Ok(f);
		}
		// Anything else raises an error
		Err(CastError::InvalidKind {
			from: v,
			into: "float".into(),
		})
	}
}

impl Cast for BigDecimal {
	fn can_cast(v: &Value) -> bool {
		v.is_number_and(|n| n.as_bigdecimal_cast().is_some())
			|| v.is_strand_and(|v| v.parse::<BigDecimal>().is_ok())
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		if let Value::Number(ref n) = v {
			return n.as_bigdecimal_cast().ok_or_else(|| CastError::InvalidKind {
				from: v,
				into: "bigdecimal".into(),
			});
		}
		if let Value::String(ref s) = v
			&& let Ok(b) = s.parse::<BigDecimal>()
		{
			return Ok(b);
		}
		Err(CastError::InvalidKind {
			from: v,
			into: "bigdecimal".into(),
		})
	}
}

impl Cast for Decimal {
	fn can_cast(v: &Value) -> bool {
		v.is_number_and(|n| n.as_decimal_lossless().is_some())
			|| v.is_strand_and(|v| Decimal::from_str_normalized(v).is_ok())
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		if let Value::Number(ref n) = v {
			return n.as_decimal_lossless().ok_or_else(|| CastError::InvalidKind {
				from: v,
				into: "decimal".into(),
			});
		}
		// Attempt to convert a string value
		if let Value::String(ref s) = v
			&& let Ok(d) = Decimal::from_str_normalized(s)
		{
			return Ok(d);
		}
		// Anything else raises an error
		Err(CastError::InvalidKind {
			from: v,
			into: "decimal".into(),
		})
	}
}

impl Cast for Number {
	fn can_cast(v: &Value) -> bool {
		v.is_number() || v.is_strand_and(|s| Number::from_str(s).is_ok())
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		if let Value::Number(v) = v {
			return Ok(v);
		}
		if let Value::String(ref s) = v
			&& let Ok(n) = Number::from_str(s)
		{
			return Ok(n);
		}
		// Anything else raises an error
		Err(CastError::InvalidKind {
			from: v,
			into: "number".into(),
		})
	}
}

macro_rules! impl_int_cast {
	($($ty:ident),* $(,)?) => {
		$(
			impl Cast for $ty {
				fn can_cast(v: &Value) -> bool {
					v.is_number_and(|n| n.as_int_lossless().and_then(|x| $ty::try_from(x).ok()).is_some())
						|| v.is_strand_and(|v| v.parse::<$ty>().is_ok())
				}

				fn cast(v: Value) -> Result<Self, CastError> {
					if let Value::Number(ref n) = v {
						return n.as_int_lossless().and_then(|x| $ty::try_from(x).ok()).ok_or_else(|| CastError::InvalidKind {
							from: v,
							into: stringify!($ty).into(),
						});
					}
					if let Value::String(ref s) = v
						&& let Ok(i) = s.parse::<$ty>()
					{
						return Ok(i);
					}
					Err(CastError::InvalidKind {
						from: v,
						into: stringify!($ty).into(),
					})
				}
			}
		)*
	};
}
impl_int_cast!(i8, u8, i16, u16, i32, u32);

impl Cast for u64 {
	fn can_cast(v: &Value) -> bool {
		v.is_number_and(|n| n.as_u64_lossless().is_some())
			|| v.is_strand_and(|v| v.parse::<u64>().is_ok())
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		if let Value::Number(ref n) = v {
			return n.as_u64_lossless().ok_or_else(|| CastError::InvalidKind {
				from: v,
				into: "u64".into(),
			});
		}
		if let Value::String(ref s) = v
			&& let Ok(i) = s.parse::<u64>()
		{
			return Ok(i);
		}
		Err(CastError::InvalidKind {
			from: v,
			into: "u64".into(),
		})
	}
}

impl Cast for half::f16 {
	fn can_cast(v: &Value) -> bool {
		v.is_number_and(|n| n.as_f16().is_some())
			|| v.is_strand_and(|s| {
				s.parse::<f64>().ok().and_then(|f| Number::Float(f).as_f16()).is_some()
			})
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		if let Value::Number(ref n) = v {
			return n.as_f16().ok_or_else(|| CastError::InvalidKind {
				from: v,
				into: "f16".into(),
			});
		}
		if let Value::String(ref s) = v
			&& let Some(f) = s.parse::<f64>().ok().and_then(|f| Number::Float(f).as_f16())
		{
			return Ok(f);
		}
		Err(CastError::InvalidKind {
			from: v,
			into: "f16".into(),
		})
	}
}

impl Cast for f32 {
	fn can_cast(v: &Value) -> bool {
		v.is_number_and(|n| n.as_f32().is_some())
			|| v.is_strand_and(|s| {
				s.parse::<f64>().ok().and_then(|f| Number::Float(f).as_f32()).is_some()
			})
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		if let Value::Number(ref n) = v {
			return n.as_f32().ok_or_else(|| CastError::InvalidKind {
				from: v,
				into: "f32".into(),
			});
		}
		if let Value::String(ref s) = v
			&& let Some(f) = s.parse::<f64>().ok().and_then(|f| Number::Float(f).as_f32())
		{
			return Ok(f);
		}
		Err(CastError::InvalidKind {
			from: v,
			into: "f32".into(),
		})
	}
}

impl Cast for String {
	fn can_cast(v: &Value) -> bool {
		if v.is_nullish() {
			false
		} else if let Value::Bytes(b) = v {
			std::str::from_utf8(b).is_ok()
		} else {
			true
		}
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		match v {
			Value::Bytes(b) => match String::from_utf8(b.0.to_vec()) {
				Ok(x) => Ok(x),
				Err(e) => Err(CastError::InvalidKind {
					from: Value::Bytes(Bytes::from(e.into_bytes())),
					into: "string".to_owned(),
				}),
			},

			Value::Null => Ok("NULL".into()),
			Value::None => Ok("NONE".into()),
			Value::String(x) => Ok(x.into_string()),
			Value::Uuid(x) => Ok(x.to_string()),
			Value::Datetime(x) => Ok(x.to_string()),
			Value::Date(x) => Ok(x.to_string()),
			Value::Time(x) => Ok(x.to_string()),
			Value::Timestamp(x) => Ok(x.to_string()),
			Value::Interval(x) => Ok(x.to_string()),
			Value::Number(Number::Decimal(x)) => Ok(x.to_string()),
			Value::Number(Number::BigDecimal(x)) => Ok(x.to_string()),
			Value::Bool(_)
			| Value::Number(
				Number::Int(_)
				| Number::Float(_)
				| Number::I8(_)
				| Number::U8(_)
				| Number::I16(_)
				| Number::U16(_)
				| Number::I32(_)
				| Number::U32(_)
				| Number::U64(_)
				| Number::F16(_)
				| Number::F32(_),
			)
			| Value::Duration(_)
			| Value::Array(_)
			| Value::Set(_)
			| Value::Object(_)
			| Value::Geometry(_)
			| Value::Table(_)
			| Value::RecordId(_)
			| Value::File(_)
			| Value::Regex(_)
			| Value::Range(_)
			| Value::Closure(_) => Ok(v.to_sql()),
		}
	}
}

impl Cast for Uuid {
	fn can_cast(v: &Value) -> bool {
		v.is_uuid() || v.is_strand_and(|s| Uuid::from_str(s).is_ok())
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		if let Value::Uuid(u) = v {
			return Ok(u);
		}
		if let Value::String(ref s) = v
			&& let Ok(u) = Uuid::from_str(s)
		{
			return Ok(u);
		}
		Err(CastError::InvalidKind {
			from: v,
			into: "uuid".into(),
		})
	}
}

/// The date, time and timestamp casts accept a full datetime string, which is cast from the
/// datetime it reads as.
macro_rules! impl_temporal_cast {
	($ty:ident, $name:literal, $text:expr, $from:expr) => {
		impl Cast for $ty {
			fn can_cast(v: &Value) -> bool {
				Self::convert(v).is_some()
			}

			fn cast(v: Value) -> Result<Self, CastError> {
				match Self::convert(&v) {
					Some(x) => Ok(x),
					None => Err(CastError::InvalidKind {
						from: v,
						into: $name.into(),
					}),
				}
			}
		}

		impl $ty {
			fn convert(v: &Value) -> Option<Self> {
				let from: fn(&Value) -> Option<$ty> = $from;
				if let Some(x) = from(v) {
					return Some(x);
				}
				if let Value::String(s) = v {
					let text: fn(&str) -> Option<$ty> = $text;
					if let Some(x) = text(s) {
						return Some(x);
					}
				}
				None
			}
		}
	};
}

impl Cast for Datetime {
	fn can_cast(v: &Value) -> bool {
		Self::convert(v).is_some()
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		match Self::convert(&v) {
			Some(x) => Ok(x),
			None => Err(CastError::InvalidKind {
				from: v,
				into: "datetime".into(),
			}),
		}
	}
}

impl Datetime {
	fn convert(v: &Value) -> Option<Self> {
		if let Value::Datetime(x) = v {
			Some(*x)
		} else if let Value::Date(x) = v {
			Some((*x).into())
		} else if let Value::Timestamp(x) = v {
			Some((*x).into())
		} else if let Value::String(s) = v {
			Datetime::from_str(s).ok()
		} else {
			None
		}
	}
}

impl_temporal_cast!(
	Date,
	"date",
	|s| Date::from_str(s).ok().or_else(|| Datetime::from_str(s).ok().map(|d| d.date())),
	|v| {
		if let Value::Date(x) = v {
			Some(*x)
		} else if let Value::Datetime(x) = v {
			Some(x.date())
		} else if let Value::Timestamp(x) = v {
			Some(x.date())
		} else {
			None
		}
	}
);

impl_temporal_cast!(
	Time,
	"time",
	|s| Time::from_str(s).ok().or_else(|| Datetime::from_str(s).ok().map(|d| d.time())),
	|v| {
		if let Value::Time(x) = v {
			Some(*x)
		} else if let Value::Datetime(x) = v {
			Some(x.time())
		} else if let Value::Timestamp(x) = v {
			Some(x.time())
		} else {
			None
		}
	}
);

impl_temporal_cast!(
	Timestamp,
	"timestamp",
	|s| Timestamp::from_str(s).ok().or_else(|| Datetime::from_str(s).ok().map(Timestamp::from)),
	|v| {
		if let Value::Timestamp(x) = v {
			Some(*x)
		} else if let Value::Datetime(x) = v {
			Some((*x).into())
		} else if let Value::Date(x) = v {
			Some(x.at_midnight())
		} else {
			None
		}
	}
);

impl_temporal_cast!(Interval, "interval", |s| Interval::from_str(s).ok(), |v| {
	if let Value::Interval(x) = v {
		Some(*x)
	} else if let Value::Time(x) = v {
		// The time part as microseconds, truncating to the interval's resolution.
		i64::try_from(x.nanos() / 1_000).ok().map(|m| Interval::new(0, 0, m))
	} else if let Value::Duration(x) = v {
		if x.0.subsec_nanos() % 1_000 != 0 {
			return None;
		}
		i64::try_from(x.0.as_micros()).ok().map(|m| Interval::new(0, 0, m))
	} else {
		None
	}
});

impl Cast for Duration {
	fn can_cast(v: &Value) -> bool {
		Self::convert(v).is_some() || v.is_strand_and(|s| Duration::from_str(s).is_ok())
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		if let Some(d) = Self::convert(&v) {
			return Ok(d);
		}
		// Attempt to parse a string
		if let Value::String(ref s) = v
			&& let Ok(d) = Duration::from_str(s)
		{
			return Ok(d);
		}
		// Anything else raises an error
		Err(CastError::InvalidKind {
			from: v,
			into: "duration".into(),
		})
	}
}

impl Duration {
	fn convert(v: &Value) -> Option<Self> {
		if let Value::Duration(x) = v {
			Some(*x)
		} else if let Value::Time(x) = v {
			// Nanoseconds since midnight.
			Some(Duration::from_nanos(x.nanos()))
		} else if let Value::Interval(x) = v {
			// A month is 30 days, as PostgreSQL's epoch convention, and a negative total is
			// refused.
			let micros = u128::try_from(x.normalised_micros()).ok()?;
			let secs = u64::try_from(micros / 1_000_000).ok()?;
			let nanos = (micros % 1_000_000) as u32 * 1_000;
			Some(Duration::new(secs, nanos))
		} else {
			None
		}
	}
}

impl Cast for Bytes {
	fn can_cast(v: &Value) -> bool {
		v.is_bytes()
			|| v.is_strand()
			|| v.is_array_and(|x| x.iter().all(|v| value_to_byte(v).is_some()))
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		let v = match v.try_into_bytes() {
			Ok(b) => return Ok(b),
			Err(v) => v,
		};
		let v = match v.try_into_strand() {
			Ok(s) => return Ok(Bytes::from(s.into_string().into_bytes())),
			Err(v) => v,
		};
		if let Value::Array(ref x) = v
			&& let Some(bytes) = x.0.iter().map(value_to_byte).collect::<Option<Vec<u8>>>()
		{
			return Ok(Bytes::from(bytes));
		}
		Err(CastError::InvalidKind {
			from: v,
			into: "bytes".into(),
		})
	}
}

// `TryFrom<Number> for u8` is intentionally not reused: it truncates fractional
// floats (via `num_traits::ToPrimitive::to_u8`) instead of rejecting them, which
// would reintroduce the truncation bug this helper is here to prevent.
fn value_to_byte(v: &Value) -> Option<u8> {
	if let Value::Number(n) = v {
		return n.as_int_lossless().and_then(|x| u8::try_from(x).ok());
	}
	if let Value::String(s) = v {
		return s.parse::<u8>().ok();
	}
	None
}

impl Cast for Array {
	fn can_cast(v: &Value) -> bool {
		v.is_array()
			|| v.is_bytes()
			|| v.is_set()
			|| v.is_range_and(|r| r.can_coerce_to_typed::<i64>())
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		let v = match v.try_into_array() {
			Ok(x) => return Ok(x),
			Err(v) => v,
		};
		let v = match v.try_into_set() {
			Ok(s) => return Ok(s.into_iter().collect()),
			Err(v) => v,
		};
		let v = match v.try_into_range() {
			Ok(range) => {
				if !range.can_coerce_to_typed::<i64>() {
					return Err(CastError::InvalidKind {
						from: Value::Range(range),
						into: "array".to_string(),
					});
				}
				// checked above
				let range = range.coerce_to_typed::<i64>().expect("range type checked above");
				if range.len().is_none_or(|n| n > *GENERATION_ALLOCATION_LIMIT) {
					return Err(CastError::RangeSizeLimit {
						value: Box::new(Range::from(range)),
					});
				}

				return Ok(range.cast_to_array());
			}
			Err(v) => v,
		};
		let v = match v.try_into_bytes() {
			Ok(x) => return Ok(Array(x.0.into_iter().map(|x| Value::from(x as i64)).collect())),
			Err(v) => v,
		};
		Err(CastError::InvalidKind {
			from: v,
			into: "array".into(),
		})
	}
}

impl Cast for Set {
	fn can_cast(v: &Value) -> bool {
		matches!(v, Value::Set(_) | Value::Array(_))
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		let v = match v.try_into_set() {
			Ok(x) => return Ok(x),
			Err(v) => v,
		};
		// Convert array to set, automatically deduplicating
		v.try_into_array().map(|x| Set::from(x.0)).map_err(|from| CastError::InvalidKind {
			from,
			into: "set".into(),
		})
	}
}

impl Cast for Regex {
	fn can_cast(v: &Value) -> bool {
		v.is_regex() || v.is_strand_and(|x| Regex::from_str(x).is_ok())
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		let v = match v.try_into_regex() {
			Ok(x) => return Ok(x),
			Err(v) => v,
		};
		if let Value::String(ref x) = v
			&& let Ok(r) = Regex::from_str(x)
		{
			return Ok(r);
		}
		Err(CastError::InvalidKind {
			from: v,
			into: "regex".to_string(),
		})
	}
}

impl Cast for Box<Range> {
	fn can_cast(v: &Value) -> bool {
		v.is_range() || v.is_array_and(|x| x.len() == 2)
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		let v = match v.try_into_range() {
			Ok(x) => return Ok(x),
			Err(v) => v,
		};
		let v = match v.try_into_array() {
			Ok(x) => {
				if x.len() != 2 {
					return Err(CastError::InvalidKind {
						from: Value::Array(x),
						into: "range".to_string(),
					});
				}

				let mut iter = x.into_iter();
				// checked above
				let beg = iter.next().expect("array length checked above");
				// checked above
				let end = iter.next().expect("array length checked above");

				return Ok(Box::new(Range {
					start: Bound::Included(beg),
					end: Bound::Excluded(end),
				}));
			}
			Err(v) => v,
		};
		Err(CastError::InvalidKind {
			from: v,
			into: "range".into(),
		})
	}
}

impl Cast for Point<f64> {
	fn can_cast(v: &Value) -> bool {
		v.is_geometry_and(|g| matches!(g, Geometry::Point(_))) || v.is_array_and(|x| x.len() == 2)
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		if let Value::Geometry(Geometry::Point(v)) = v {
			return Ok(v);
		}
		let v = match v.try_into_array() {
			Ok(x) => {
				if x.len() != 2 {
					return Err(CastError::InvalidKind {
						from: Value::Array(x),
						into: "point".to_string(),
					});
				}

				if !x[0].can_coerce_to::<f64>() || !x[1].can_coerce_to::<f64>() {
					return Err(CastError::InvalidKind {
						from: Value::Array(x),
						into: "point".to_string(),
					});
				}

				let mut iter = x.into_iter();
				// checked above
				let x = iter
					.next()
					.expect("array length checked above")
					.cast_to::<f64>()
					.expect("value type checked above");
				// checked above
				let y = iter
					.next()
					.expect("array length checked above")
					.cast_to::<f64>()
					.expect("value type checked above");

				return Ok(Point::new(x, y));
			}
			Err(v) => v,
		};
		Err(CastError::InvalidKind {
			from: v,
			into: "point".into(),
		})
	}
}

impl Cast for RecordId {
	fn can_cast(v: &Value) -> bool {
		v.is_record() || v.is_strand_and(|x| syn::record_id(x).is_ok())
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		let v = match v.try_into_record() {
			Ok(x) => return Ok(x),
			Err(v) => v,
		};
		if let Value::String(ref x) = v
			&& let Ok(r) = syn::record_id(x)
		{
			return Ok(r.into());
		}
		Err(CastError::InvalidKind {
			from: v,
			into: "record".to_string(),
		})
	}
}

impl Cast for surrealdb_strand::TableName {
	fn can_cast(v: &Value) -> bool {
		matches!(v, Value::Table(_) | Value::String(_))
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		let v = match v.try_into_table() {
			Ok(x) => return Ok(x),
			Err(v) => v,
		};
		v.try_into_strand().map(surrealdb_strand::TableName::new).map_err(|from| {
			CastError::InvalidKind {
				from,
				into: "table".to_string(),
			}
		})
	}
}

impl<T: Cast> Cast for Option<T> {
	fn can_cast(v: &Value) -> bool {
		if let Value::None = v {
			return true;
		}
		T::can_cast(v)
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		if v.is_none() {
			Ok(None)
		} else {
			T::cast(v).map(Some)
		}
	}
}

macro_rules! impl_direct {
	($($name:ident => $inner:ty $(= $kind:ident)?),*$(,)?) => {
		$(
		impl Cast for $inner {
			fn can_cast(v: &Value) -> bool{
				matches!(v, Value::$name(_))
			}

			fn cast(v: Value) -> Result<Self, CastError> {
				if let Value::$name(x) = v {
					return Ok(x);
				} else {
					return Err(CastError::InvalidKind{
						from: v,
						into: impl_direct!(@kindof $inner $(= $kind)?),
					});
				}
			}
		}
		)*
	};

	(@kindof $inner:ty = $kind:ident) => {
		<$kind as HasKind>::kind().to_sql()
	};

	(@kindof $inner:ty) => {
		<$inner as HasKind>::kind().to_sql()
	};
}

// Types which directly match one enum variant or fail
impl_direct! {
	Closure => Box<Closure> = Closure,
	Object => Object,
	File => File,
}

/// A geometry, or a GeoJSON object describing one
/// ([`Geometry::try_from_object`]).
impl Cast for Geometry {
	fn can_cast(v: &Value) -> bool {
		v.is_geometry() || v.is_object_and(|x| Geometry::try_from_object(x).is_some())
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		let v = match v.try_into_geometry() {
			Ok(x) => return Ok(x),
			Err(v) => v,
		};
		if let Value::Object(ref x) = v
			&& let Some(x) = Geometry::try_from_object(x)
		{
			return Ok(x);
		}
		Err(CastError::InvalidKind {
			from: v,
			into: <Geometry as HasKind>::kind().to_sql(),
		})
	}
}

impl Value {
	pub fn can_cast_to<T: Cast>(&self) -> bool {
		T::can_cast(self)
	}

	pub fn can_cast_to_kind(&self, kind: &Kind) -> bool {
		match kind {
			Kind::Any => true,
			Kind::None => self.can_cast_to::<SqlNone>(),
			Kind::Null => self.can_cast_to::<Null>(),
			Kind::Bool => self.can_cast_to::<bool>(),
			Kind::Int => self.can_cast_to::<i64>(),
			Kind::Float => self.can_cast_to::<f64>(),
			Kind::Decimal => self.can_cast_to::<Decimal>(),
			Kind::BigDecimal => self.can_cast_to::<BigDecimal>(),
			Kind::Number => self.can_cast_to::<Number>(),
			Kind::I8 => self.can_cast_to::<i8>(),
			Kind::U8 => self.can_cast_to::<u8>(),
			Kind::I16 => self.can_cast_to::<i16>(),
			Kind::U16 => self.can_cast_to::<u16>(),
			Kind::I32 => self.can_cast_to::<i32>(),
			Kind::U32 => self.can_cast_to::<u32>(),
			Kind::U64 => self.can_cast_to::<u64>(),
			Kind::F16 => self.can_cast_to::<half::f16>(),
			Kind::F32 => self.can_cast_to::<f32>(),
			Kind::String => self.can_cast_to::<String>(),
			Kind::Datetime => self.can_cast_to::<Datetime>(),
			Kind::Date => self.can_cast_to::<Date>(),
			Kind::Time => self.can_cast_to::<Time>(),
			Kind::Timestamp => self.can_cast_to::<Timestamp>(),
			Kind::Interval => self.can_cast_to::<Interval>(),
			Kind::Duration => self.can_cast_to::<Duration>(),
			Kind::Object => self.can_cast_to::<crate::val::Object>(),
			Kind::Bytes => self.can_cast_to::<Bytes>(),
			Kind::Uuid => self.can_cast_to::<Uuid>(),
			Kind::Regex => self.can_cast_to::<Regex>(),
			Kind::Range => self.can_cast_to::<Box<Range>>(),
			Kind::Function(_, _) => self.can_cast_to::<Box<Closure>>(),
			Kind::Set(t, l) => match l {
				Some(l) => self.can_cast_to_set_len(t, *l),
				None => self.can_cast_to_set(t),
			},
			Kind::Array(t, l) => match l {
				Some(l) => self.can_cast_to_array_len(t, *l),
				None => self.can_cast_to_array(t),
			},
			Kind::Table(t) => {
				if t.is_empty() {
					self.can_cast_to::<String>()
				} else {
					self.can_cast_to_table(t)
				}
			}
			Kind::Record(t) => {
				if t.is_empty() {
					self.can_cast_to::<RecordId>()
				} else {
					self.can_cast_to_record(t)
				}
			}
			Kind::Geometry(t) => {
				if t.is_empty() {
					self.can_cast_to::<Geometry>()
				} else {
					self.can_cast_to_geometry(t)
				}
			}
			Kind::Either(k) => k.iter().any(|x| self.can_cast_to_kind(x)),
			Kind::Literal(lit) => self.can_cast_to_literal(lit),
			Kind::File(buckets) => {
				if buckets.is_empty() {
					self.can_cast_to::<File>()
				} else {
					self.can_cast_to_file_buckets(buckets)
				}
			}
			Kind::Enum(labels) => self.is_enum_member(labels),
			// A named type's body is judged where the catalog is reachable.
			Kind::Named(_) => true,
		}
	}

	fn can_cast_to_array_len(&self, kind: &Kind, len: u64) -> bool {
		self.is_array_and(|a| a.len() as u64 == len && a.iter().all(|x| x.can_cast_to_kind(kind)))
	}

	fn can_cast_to_array(&self, kind: &Kind) -> bool {
		self.is_array_and(|a| a.iter().all(|x| x.can_cast_to_kind(kind)))
	}

	fn can_cast_to_set_len(&self, kind: &Kind, len: u64) -> bool {
		self.is_set_and(|s| s.len() as u64 == len && s.iter().all(|x| x.can_cast_to_kind(kind)))
	}

	fn can_cast_to_set(&self, kind: &Kind) -> bool {
		self.is_set_and(|s| s.iter().all(|x| x.can_cast_to_kind(kind)))
	}

	fn can_cast_to_table(&self, val: &[TableName]) -> bool {
		// Strings can be cast to tables
		self.is_table_and(|t| t.is_table_type(val)) || self.is_strand()
	}

	fn can_cast_to_record(&self, val: &[TableName]) -> bool {
		self.is_record_and(|t| t.is_table_type(val))
	}

	fn can_cast_to_geometry(&self, val: &[GeometryKind]) -> bool {
		if let Value::Object(x) = self {
			Geometry::try_from_object(x).is_some_and(|g| val.contains(&g.geometry_kind()))
		} else {
			self.is_geometry_type(val)
		}
	}

	fn can_cast_to_literal(&self, val: &KindLiteral) -> bool {
		if matches!(val, KindLiteral::Array(_) | KindLiteral::Object(_)) {
			// The members are cast, so whether they can be is what the cast does.
			self.clone().cast_to_literal(val).is_ok()
		} else {
			val.validate_value(self)
		}
	}

	fn can_cast_to_file_buckets(&self, buckets: &[String]) -> bool {
		matches!(self, Value::File(f) if f.is_bucket_type(buckets))
	}

	pub fn cast_to<T: Cast>(self) -> Result<T, CastError> {
		T::cast(self)
	}

	/// Try to convert this value to the specified `Kind`
	pub fn cast_to_kind(self, kind: &Kind) -> Result<Value, CastError> {
		// Attempt to convert to the desired type
		match kind {
			Kind::Any => Ok(self),
			Kind::None => self.cast_to::<SqlNone>().map(|_| Value::None),
			Kind::Null => self.cast_to::<Null>().map(|_| Value::Null),
			Kind::Bool => self.cast_to::<bool>().map(Value::from),
			Kind::Int => self.cast_to::<i64>().map(Value::from),
			Kind::Float => self.cast_to::<f64>().map(Value::from),
			Kind::Decimal => self.cast_to::<Decimal>().map(Value::from),
			Kind::BigDecimal => {
				self.cast_to::<BigDecimal>().map(|b| Value::Number(Number::BigDecimal(Box::new(b))))
			}
			Kind::Number => self.cast_to::<Number>().map(Value::from),
			Kind::I8 => self.cast_to::<i8>().map(|v| Value::Number(Number::I8(v))),
			Kind::U8 => self.cast_to::<u8>().map(|v| Value::Number(Number::U8(v))),
			Kind::I16 => self.cast_to::<i16>().map(|v| Value::Number(Number::I16(v))),
			Kind::U16 => self.cast_to::<u16>().map(|v| Value::Number(Number::U16(v))),
			Kind::I32 => self.cast_to::<i32>().map(|v| Value::Number(Number::I32(v))),
			Kind::U32 => self.cast_to::<u32>().map(|v| Value::Number(Number::U32(v))),
			Kind::U64 => self.cast_to::<u64>().map(|v| Value::Number(Number::U64(v))),
			Kind::F16 => self.cast_to::<half::f16>().map(|v| Value::Number(Number::F16(v))),
			Kind::F32 => self.cast_to::<f32>().map(|v| Value::Number(Number::F32(v))),
			Kind::String => self.cast_to::<String>().map(Value::from),
			Kind::Datetime => self.cast_to::<Datetime>().map(Value::from),
			Kind::Date => self.cast_to::<Date>().map(Value::from),
			Kind::Time => self.cast_to::<Time>().map(Value::from),
			Kind::Timestamp => self.cast_to::<Timestamp>().map(Value::from),
			Kind::Interval => self.cast_to::<Interval>().map(Value::from),
			Kind::Duration => self.cast_to::<Duration>().map(Value::from),
			Kind::Object => self.cast_to::<crate::val::Object>().map(Value::from),
			Kind::Bytes => self.cast_to::<Bytes>().map(Value::from),
			Kind::Uuid => self.cast_to::<Uuid>().map(Value::from),
			Kind::Regex => self.cast_to::<Regex>().map(Value::from),
			Kind::Range => self.cast_to::<Box<Range>>().map(Value::from),
			Kind::Function(_, _) => self.cast_to::<Box<Closure>>().map(Value::from),
			Kind::Set(t, l) => match l {
				Some(l) => self.cast_to_set_type_len(t, *l).map(Value::from),
				None => self.cast_to_set_type(t).map(Value::from),
			},
			Kind::Array(t, l) => match l {
				Some(l) => self.cast_to_array_len(t, *l).map(Value::from),
				None => self.cast_to_array(t).map(Value::from),
			},
			Kind::Table(t) => match t.is_empty() {
				true => self
					.cast_to::<String>()
					.map(|s| Value::Table(surrealdb_strand::TableName::new(s))),
				false => self.cast_to_table(t).map(Value::from),
			},
			Kind::Record(t) => match t.is_empty() {
				true => self.cast_to::<RecordId>().map(Value::from),
				false => self.cast_to_record(t).map(Value::from),
			},
			Kind::Geometry(t) => match t.is_empty() {
				true => self.cast_to::<Geometry>().map(Value::from),
				false => self.cast_to_geometry(t).map(Value::from),
			},
			Kind::Either(k) => {
				let Some(k) = self.union_member(k, Value::can_cast_to_kind) else {
					return Err(CastError::InvalidKind {
						from: self,
						into: kind.to_sql(),
					});
				};

				Ok(self.cast_to_kind(k).expect(
					"If can_coerce_to_kind returns true then coerce_to_kind must not error",
				))
			}
			Kind::Literal(lit) => self.cast_to_literal(lit),
			Kind::File(buckets) => {
				if buckets.is_empty() {
					self.cast_to::<File>().map(Value::from)
				} else {
					self.cast_to_file_buckets(buckets).map(Value::from)
				}
			}
			// A named type's body is judged where the catalog is reachable.
			Kind::Named(_) => Ok(self),
			Kind::Enum(labels) => {
				if self.is_enum_member(labels) {
					Ok(self)
				} else {
					Err(CastError::InvalidKind {
						from: self,
						into: kind.to_sql(),
					})
				}
			}
		}
	}

	/// Try to convert this value to a Literal, returns a `Value` with the
	/// converted value.
	///
	/// Array and object literals cast each member to its member kind, as
	/// `array<T>` casts its elements: the shape (length, keys, optional
	/// members) must match, and every member must be castable. Any other
	/// literal is a value that must match exactly.
	pub fn cast_to_literal(self, literal: &KindLiteral) -> Result<Value, CastError> {
		match (literal, self) {
			(KindLiteral::Array(kinds), Value::Array(array)) => {
				let original = array.clone();
				if array.len() != kinds.len() {
					return Err(literal_mismatch(Value::Array(original), literal));
				}
				array
					.into_iter()
					.zip(kinds)
					.map(|(value, kind)| value.cast_to_kind(kind))
					.collect::<Result<Array, CastError>>()
					.map(Value::Array)
					.map_err(|_| literal_mismatch(Value::Array(original), literal))
			}
			(KindLiteral::Object(kinds), Value::Object(mut object)) => {
				let original = object.clone();
				let extra = object.iter().any(|(key, _)| !kinds.contains_key(key));
				let missing = kinds
					.iter()
					.any(|(key, kind)| object.get(key.as_str()).is_none() && !kind.can_be_none());
				if extra || missing {
					return Err(literal_mismatch(Value::Object(original), literal));
				}
				for (key, value) in object.iter_mut() {
					if let Some(kind) = kinds.get(key) {
						let taken = std::mem::replace(value, Value::None);
						*value = taken.cast_to_kind(kind).map_err(|_| {
							literal_mismatch(Value::Object(original.clone()), literal)
						})?;
					}
				}
				Ok(Value::Object(object))
			}
			(literal, value) => {
				if literal.validate_value(&value) {
					Ok(value)
				} else {
					Err(literal_mismatch(value, literal))
				}
			}
		}
	}

	/// Try to convert this value to a Table of a certain type
	fn cast_to_table(self, val: &[TableName]) -> Result<surrealdb_strand::TableName, CastError> {
		let self_val = match self.try_into_table() {
			Ok(v) if v.is_table_type(val) => return Ok(v),
			Ok(v) => Value::Table(v),
			Err(v) => v,
		};
		if let Value::String(ref v) = self_val {
			// Check if the string is a valid table name and matches the allowed types
			let table = surrealdb_strand::TableName::new(v.clone());
			if table.is_table_type(val) {
				return Ok(table);
			}
		}

		let mut kind = "table<".to_string();
		for (idx, t) in val.iter().enumerate() {
			if idx != 0 {
				kind.push('|');
			}
			kind.push_str(t.as_str())
		}
		kind.push('>');

		Err(CastError::InvalidKind {
			from: self_val,
			into: kind,
		})
	}

	/// Try to convert this value to a Record of a certain type
	fn cast_to_record(self, val: &[TableName]) -> Result<RecordId, CastError> {
		let expected_kind = || -> String {
			let mut kind = "record<".to_string();
			for (idx, t) in val.iter().enumerate() {
				if idx != 0 {
					kind.push('|');
				}
				kind.push_str(t.as_str())
			}
			kind.push('>');
			kind
		};

		let self_val = match self.try_into_record() {
			Ok(v) if v.is_table_type(val) => return Ok(v),
			Ok(v) => Value::RecordId(v),
			Err(v) => v,
		};
		if let Value::String(ref v) = self_val
			&& let Ok(x) = syn::record_id(v.as_str())
		{
			let record_id = RecordId::from(x);
			if record_id.is_table_type(val) {
				return Ok(record_id);
			}
		}

		Err(CastError::InvalidKind {
			from: self_val,
			into: expected_kind(),
		})
	}

	/// Try to convert this value to a `Geometry` of a certain type
	fn cast_to_geometry(self, val: &[GeometryKind]) -> Result<Geometry, CastError> {
		// Geometries are allowed if correct type
		if self.is_geometry_type(val)
			&& let Value::Geometry(v) = self
		{
			return Ok(v);
		}
		if let Value::Object(ref x) = self
			&& let Some(x) = Geometry::try_from_object(x)
			&& val.contains(&x.geometry_kind())
		{
			return Ok(x);
		}
		// An array of two numbers can be cast into a point
		if self.is_array()
			&& val.contains(&GeometryKind::Point)
			&& let Some(p) = Geometry::array_to_point(&self)
		{
			return Ok(Geometry::Point(p));
		}
		// Anything else raises an error
		Err(CastError::InvalidKind {
			from: self,
			into: "geometry".into(),
		})
	}

	/// Try to convert this value to ab `Array` of a certain type
	fn cast_to_array(self, kind: &Kind) -> Result<Array, CastError> {
		self.cast_to::<Array>()?
			.into_iter()
			.enumerate()
			.map(|(i, value)| {
				value
					.cast_to_kind(kind)
					.with_element_of_at_index(i, || format!("array<{}>", kind.to_sql()))
			})
			.collect::<Result<Array, CastError>>()
	}

	/// Try to convert this value to ab `Array` of a certain type and length
	fn cast_to_array_len(self, kind: &Kind, len: u64) -> Result<Array, CastError> {
		let array = self.cast_to::<Array>()?;

		if (array.len() as u64) != len {
			return Err(CastError::InvalidLength {
				len: array.len(),
				into: format!("array<{},{}>", kind.to_sql(), len),
			});
		}

		array
			.into_iter()
			.enumerate()
			.map(|(i, value)| {
				value
					.cast_to_kind(kind)
					.with_element_of_at_index(i, || format!("array<{}>", kind.to_sql()))
			})
			.collect::<Result<Array, CastError>>()
	}

	/// Try to convert this value to a `Set` of a certain type
	pub fn cast_to_set_type(self, kind: &Kind) -> Result<Set, CastError> {
		self.cast_to::<Array>()?
			.into_iter()
			.enumerate()
			.map(|(i, value)| {
				value
					.cast_to_kind(kind)
					.with_element_of_at_index(i, || format!("set<{}>", kind.to_sql()))
			})
			.collect::<Result<Set, CastError>>()
	}

	/// Try to convert this value to a `Set` of a certain type and length
	pub fn cast_to_set_type_len(self, kind: &Kind, len: u64) -> Result<Set, CastError> {
		let set = self
			.cast_to::<Array>()?
			.into_iter()
			.enumerate()
			.map(|(i, value)| {
				value
					.cast_to_kind(kind)
					.with_element_of_at_index(i, || format!("set<{}>", kind.to_sql()))
			})
			.collect::<Result<Set, CastError>>()?;

		if (set.len() as u64) != len {
			return Err(CastError::InvalidLength {
				len: set.len(),
				into: format!("set<{},{}>", kind.to_sql(), len),
			});
		}

		Ok(set)
	}

	pub fn cast_to_file_buckets(self, buckets: &[String]) -> Result<File, CastError> {
		let v = self.cast_to::<File>()?;

		if v.is_bucket_type(buckets) {
			return Ok(v);
		}

		let mut kind = "file<".to_owned();
		for (idx, t) in buckets.iter().enumerate() {
			if idx != 0 {
				kind.push('|');
			}
			kind.push_str(t.as_str())
		}
		kind.push('>');
		Err(CastError::InvalidKind {
			from: v.into(),
			into: kind,
		})
	}
}

fn literal_mismatch(from: Value, literal: &KindLiteral) -> CastError {
	CastError::InvalidKind {
		from,
		into: literal.to_sql(),
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::Strand;

	use super::*;

	#[test]
	fn test_cast_to_table_generic() {
		// Test casting string to generic table type
		let value = Value::String(Strand::new_static("users"));
		let kind = Kind::Table(vec![]);
		let result = value.cast_to_kind(&kind);
		assert!(result.is_ok());
		if let Ok(Value::Table(table)) = result {
			assert_eq!(table.as_str(), "users");
		} else {
			panic!("Expected Value::Table");
		}
	}

	#[test]
	fn test_cast_to_table_specific() {
		// Test casting string to specific table type (matching)
		let value = Value::String(Strand::new_static("users"));
		let kind = Kind::Table(vec!["users".into()]);
		let result = value.cast_to_kind(&kind);
		assert!(result.is_ok());
		if let Ok(Value::Table(table)) = result {
			assert_eq!(table.as_str(), "users");
		}

		// Test casting string to specific table type (not matching)
		let value = Value::String(Strand::new_static("posts"));
		let kind = Kind::Table(vec!["users".into()]);
		let result = value.cast_to_kind(&kind);
		assert!(result.is_err());
	}

	#[test]
	fn test_cast_to_table_union() {
		// Test casting string to union of table types
		let value = Value::String(Strand::new_static("posts"));
		let kind = Kind::Table(vec!["users".into(), "posts".into()]);
		let result = value.cast_to_kind(&kind);
		assert!(result.is_ok());
		if let Ok(Value::Table(table)) = result {
			assert_eq!(table.as_str(), "posts");
		}

		// Test casting string that doesn't match any in the union
		let value = Value::String(Strand::new_static("comments"));
		let kind = Kind::Table(vec!["users".into(), "posts".into()]);
		let result = value.cast_to_kind(&kind);
		assert!(result.is_err());
	}

	#[test]
	fn test_cast_table_to_table() {
		// Test casting table value to table type
		let value = Value::Table(surrealdb_strand::TableName::new("users".to_string()));
		let kind = Kind::Table(vec!["users".into()]);
		let result = value.cast_to_kind(&kind);
		assert!(result.is_ok());
	}

	#[test]
	fn test_can_cast_to_table() {
		// Test can_cast_to_kind for tables - String can always cast to generic table
		let value = Value::String(Strand::new_static("users"));
		let kind = Kind::Table(vec![]);
		assert!(value.can_cast_to_kind(&kind));

		// Table value can cast to table type
		let value = Value::Table(surrealdb_strand::TableName::new("users".to_string()));
		let kind = Kind::Table(vec!["users".into()]);
		assert!(value.can_cast_to_kind(&kind));

		// Table value cannot cast to wrong table type
		let value = Value::Table(surrealdb_strand::TableName::new("posts".to_string()));
		let kind = Kind::Table(vec!["users".into()]);
		assert!(!value.can_cast_to_kind(&kind));
	}

	fn float(f: f64) -> Value {
		Value::Number(Number::Float(f))
	}

	fn decimal(mantissa: i128, scale: u32) -> Value {
		Value::Number(Number::Decimal(Decimal::from_i128_with_scale(mantissa, scale)))
	}

	#[test]
	fn test_cast_float_to_int_accepts_whole_floats_in_range() {
		assert_eq!(float(5.0).cast_to::<i64>().unwrap(), 5);
		assert_eq!(float(-5.0).cast_to::<i64>().unwrap(), -5);
		// Negative zero is the integer zero
		assert_eq!(float(-0.0).cast_to::<i64>().unwrap(), 0);
		// 2^53: every smaller integer is also an f64
		assert_eq!(float(9_007_199_254_740_992.0).cast_to::<i64>().unwrap(), 9_007_199_254_740_992);
		// -2^63 is `i64::MIN`
		assert_eq!(float(i64::MIN as f64).cast_to::<i64>().unwrap(), i64::MIN);
		// The largest f64 below 2^63
		assert_eq!(float(2f64.powi(63) - 1024.0).cast_to::<i64>().unwrap(), i64::MAX - 1023);
	}

	#[test]
	fn test_cast_float_to_int_refuses_what_is_not_one_i64() {
		for f in [
			// 2^63: one past `i64::MAX`, which `i64::MAX as f64` also rounds up to
			2f64.powi(63),
			i64::MAX as f64,
			// The f64 below -2^63
			-2f64.powi(63) - 2048.0,
			1e30,
			-1e30,
			f64::MAX,
			f64::MIN,
			f64::NAN,
			f64::INFINITY,
			f64::NEG_INFINITY,
			0.5,
			-0.5,
		] {
			let err = float(f).cast_to::<i64>().unwrap_err();
			assert!(
				matches!(&err, CastError::InvalidKind { into, .. } if into == "int"),
				"{f} should be refused as an int, got: {err}"
			);
			assert!(!float(f).can_cast_to::<i64>(), "{f} must not report as castable to int");
		}
	}

	/// Values of every shape that matter to an `int` cast: the bounds, the
	/// first value past each bound, fractions, the non-finite floats and
	/// strings.
	fn cast_battery() -> Vec<Value> {
		let mut out = vec![
			Value::Number(Number::Int(0)),
			Value::Number(Number::Int(i64::MIN)),
			Value::Number(Number::Int(i64::MAX)),
			decimal(0, 0),
			decimal(15, 1),
			decimal(-15, 1),
			// A whole decimal carrying a trailing scale
			decimal(50, 1),
			decimal(i64::MAX as i128, 0),
			decimal(i64::MAX as i128 + 1, 0),
			decimal(i64::MIN as i128, 0),
			decimal(i64::MIN as i128 - 1, 0),
			decimal(10_000_000_000_000_000_000, 0),
			Value::Number(Number::Decimal(Decimal::MAX)),
			Value::Number(Number::Decimal(Decimal::MIN)),
			Value::String(Strand::new_static("5")),
			Value::String(Strand::new_static("1.5")),
			Value::String(Strand::new_static("1e10")),
			Value::String(Strand::new_static("1e40")),
			Value::String(Strand::new_static("9223372036854775808")),
			Value::String(Strand::new_static("abc")),
		];
		for f in [
			0.0,
			-0.0,
			5.0,
			0.5,
			1e30,
			-1e30,
			i64::MAX as f64,
			i64::MIN as f64,
			f64::MAX,
			f64::NAN,
			f64::INFINITY,
			f64::NEG_INFINITY,
		] {
			out.push(float(f));
		}
		out
	}

	#[test]
	fn test_can_cast_agrees_with_cast_to_int() {
		// A union kind casts to the first member `can_cast_to_kind` accepts
		// and treats a failure there as unreachable, so the two must never
		// disagree.
		for value in cast_battery() {
			for kind in Kind::NUMERIC {
				assert_eq!(
					value.can_cast_to_kind(&kind),
					value.clone().cast_to_kind(&kind).is_ok(),
					"can_cast and cast disagree for {value:?} into {}",
					kind.to_sql()
				);
			}
		}
	}

	#[test]
	fn test_cast_union_with_int_never_claims_a_value_it_cannot_cast() {
		let fractional = decimal(15, 1);
		let huge = float(1e30);
		let either = Kind::Either(vec![Kind::Int, Kind::Float]);
		// The alternatives that can hold the value are still reached
		assert!(matches!(
			fractional.cast_to_kind(&either).unwrap(),
			Value::Number(Number::Float(f)) if f == 1.5
		));
		assert!(matches!(
			huge.cast_to_kind(&either).unwrap(),
			Value::Number(Number::Float(f)) if f == 1e30
		));
		// When the first member can hold the value, it still wins
		assert!(matches!(float(5.0).cast_to_kind(&either).unwrap(), Value::Number(Number::Int(5))));
		assert!(matches!(
			decimal(5, 0).cast_to_kind(&either).unwrap(),
			Value::Number(Number::Int(5))
		));
		// A sized number whose exact kind is in the union keeps its width
		let sized_either = Kind::Either(vec![Kind::Int, Kind::I8]);
		assert!(matches!(
			Value::Number(Number::I8(5)).cast_to_kind(&sized_either).unwrap(),
			Value::Number(Number::I8(5))
		));
		// Finite string literals that overflow `f32` or `f16` are refused
		assert!(Value::String(Strand::new_static("1e40")).cast_to_kind(&Kind::F32).is_err());
		assert!(Value::String(Strand::new_static("1e40")).cast_to_kind(&Kind::F16).is_err());
		assert!(Value::String(Strand::new_static("1e10")).cast_to_kind(&Kind::F16).is_err());
		// No alternative takes the value: an error, not a panic
		let none = Kind::Either(vec![Kind::Int, Kind::Bool]);
		assert!(decimal(15, 1).cast_to_kind(&none).is_err());
		assert!(float(1e30).cast_to_kind(&none).is_err());
	}
}
