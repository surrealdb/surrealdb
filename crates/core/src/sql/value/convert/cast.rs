use std::{fmt, ops::Bound, str::FromStr as _};

use geo::Point;
use rust_decimal::Decimal;

use crate::sql::{
	Array, Bytes, Closure, Datetime, DecimalExt, Duration, File, Geometry, Ident, Kind, Literal,
	Number, Object, Range, Regex, Strand, Table, Thing, Uuid, SqlValue, array::Uniq as _,
	kind::HasKind, value::Null,
};

#[derive(Clone, Debug)]
pub enum CastError {
	// Coercion error at the end.
	InvalidKind {
		from: SqlValue,
		into: String,
	},
	InvalidLength {
		len: usize,
		into: String,
	},
	/// Coerce failed because element of type didn't match.
	ElementOf {
		inner: Box<CastError>,
		into: String,
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
				write!(f, "Expected `{into}` but found a `{from}`")
			}
			CastError::ElementOf {
				inner,
				into,
			} => {
				inner.fmt(f)?;
				write!(f, " when coercing an element of `{into}`")
			}
			CastError::InvalidLength {
				len,
				into,
			} => {
				write!(f, "Expected `{into}` buf found an collection of length `{len}`")
			}
			CastError::RangeSizeLimit {
				value,
			} => {
				write!(
					f,
					"Casting range `{value}` to an array would create an array larger then the max allocation limit."
				)
			}
		}
	}
}

pub trait CastErrorExt {
	fn with_element_of<F>(self, f: F) -> Self
	where
		F: Fn() -> String;
}

impl<T> CastErrorExt for Result<T, CastError> {
	fn with_element_of<F>(self, f: F) -> Self
	where
		F: Fn() -> String,
	{
		match self {
			Ok(x) => Ok(x),
			Err(e) => Err(CastError::ElementOf {
				inner: Box::new(e),
				into: f(),
			}),
		}
	}
}

/// Trait for converting the value using casting rules, calling the functions on this trait results
/// in similar behavior as casting does in surrealql like `<string> 1`.
///
/// Casting rules are more loose then coercing rules.
pub trait Cast: Sized {
	/// Returns true if calling cast on the value will succeed.
	///
	/// If `T::can_cast(&v)` returns `true` then `T::cast(v) should not return an error.
	fn can_cast(v: &SqlValue) -> bool;

	/// Cast a value to the self type.
	fn cast(v: SqlValue) -> Result<Self, CastError>;
}

impl Cast for SqlValue {
	fn can_cast(_: &SqlValue) -> bool {
		true
	}

	fn cast(v: SqlValue) -> Result<Self, CastError> {
		Ok(v)
	}
}

impl Cast for Null {
	fn can_cast(v: &SqlValue) -> bool {
		matches!(v, SqlValue::Null)
	}

	fn cast(v: SqlValue) -> Result<Self, CastError> {
		match v {
			SqlValue::Null => Ok(Null),
			x => Err(CastError::InvalidKind {
				from: x,
				into: "null".to_string(),
			}),
		}
	}
}

impl Cast for bool {
	fn can_cast(v: &SqlValue) -> bool {
		match v {
			SqlValue::Bool(_) => true,
			SqlValue::Strand(x) => **x == "true" || **x == "false",
			_ => false,
		}
	}

	fn cast(v: SqlValue) -> Result<Self, CastError> {
		match v {
			SqlValue::Bool(b) => Ok(b),
			SqlValue::Strand(x) => match x.as_str() {
				"true" => Ok(true),
				"false" => Ok(false),
				_ => Err(CastError::InvalidKind {
					from: SqlValue::Strand(x),
					into: "bool".to_string(),
				}),
			},
			x => Err(CastError::InvalidKind {
				from: x,
				into: "bool".to_string(),
			}),
		}
	}
}

impl Cast for i64 {
	fn can_cast(v: &SqlValue) -> bool {
		match v {
			SqlValue::Number(Number::Int(_)) => true,
			SqlValue::Number(Number::Float(v)) => v.fract() == 0.0,
			SqlValue::Number(Number::Decimal(v)) => v.is_integer() || i64::try_from(*v).is_ok(),
			SqlValue::Strand(v) => v.parse::<i64>().is_ok(),
			_ => false,
		}
	}

	fn cast(v: SqlValue) -> Result<Self, CastError> {
		match v {
			// Allow any int number
			SqlValue::Number(Number::Int(x)) => Ok(x),
			// Attempt to convert an float number
			SqlValue::Number(Number::Float(v)) if v.fract() == 0.0 => Ok(v as i64),
			// Attempt to convert a decimal number
			SqlValue::Number(Number::Decimal(d)) if d.is_integer() => match d.try_into() {
				Ok(v) => Ok(v),
				_ => Err(CastError::InvalidKind {
					from: v,
					into: "int".into(),
				}),
			},
			// Attempt to convert a string value
			SqlValue::Strand(ref s) => match s.parse::<i64>() {
				Ok(v) => Ok(v),
				_ => Err(CastError::InvalidKind {
					from: v,
					into: "int".into(),
				}),
			},
			_ => Err(CastError::InvalidKind {
				from: v,
				into: "int".into(),
			}),
		}
	}
}

impl Cast for f64 {
	fn can_cast(v: &SqlValue) -> bool {
		match v {
			SqlValue::Number(Number::Int(_) | Number::Float(_)) => true,
			SqlValue::Number(Number::Decimal(v)) => v.is_integer() || i64::try_from(*v).is_ok(),
			SqlValue::Strand(v) => v.parse::<f64>().is_ok(),
			_ => false,
		}
	}

	fn cast(v: SqlValue) -> Result<Self, CastError> {
		match v {
			SqlValue::Number(Number::Float(i)) => Ok(i),
			SqlValue::Number(Number::Int(f)) => Ok(f as f64),
			SqlValue::Number(Number::Decimal(d)) => match d.try_into() {
				// The Decimal can be parsed as a Float
				Ok(v) => Ok(v),
				// The Decimal loses precision
				_ => Err(CastError::InvalidKind {
					from: v,
					into: "float".into(),
				}),
			},
			// Attempt to convert a string value
			SqlValue::Strand(ref s) => match s.parse::<f64>() {
				// The string can be parsed as a Float
				Ok(v) => Ok(v),
				// This string is not a float
				_ => Err(CastError::InvalidKind {
					from: v,
					into: "float".into(),
				}),
			},
			// Anything else raises an error
			_ => Err(CastError::InvalidKind {
				from: v,
				into: "float".into(),
			}),
		}
	}
}

impl Cast for Decimal {
	fn can_cast(v: &SqlValue) -> bool {
		match v {
			SqlValue::Number(_) => true,
			SqlValue::Strand(v) => v.parse::<f64>().is_ok(),
			_ => false,
		}
	}

	fn cast(v: SqlValue) -> Result<Self, CastError> {
		match v {
			SqlValue::Number(Number::Decimal(d)) => Ok(d),
			// Attempt to convert an int number
			SqlValue::Number(Number::Int(ref i)) => Ok(Decimal::from(*i)),
			// Attempt to convert an float number
			SqlValue::Number(Number::Float(ref f)) => match Decimal::try_from(*f) {
				// The Float can be represented as a Decimal
				Ok(d) => Ok(d),
				// This Float does not convert to a Decimal
				_ => Err(CastError::InvalidKind {
					from: v,
					into: "decimal".into(),
				}),
			},
			// Attempt to convert a string value
			SqlValue::Strand(ref s) => match Decimal::from_str_normalized(s) {
				// The string can be parsed as a Decimal
				Ok(v) => Ok(v),
				// This string is not a Decimal
				_ => Err(CastError::InvalidKind {
					from: v,
					into: "decimal".into(),
				}),
			},
			// Anything else raises an error
			_ => Err(CastError::InvalidKind {
				from: v,
				into: "decimal".into(),
			}),
		}
	}
}

impl Cast for Number {
	fn can_cast(v: &SqlValue) -> bool {
		match v {
			SqlValue::Number(_) => true,
			SqlValue::Strand(s) => Number::from_str(s).is_ok(),
			_ => false,
		}
	}

	fn cast(v: SqlValue) -> Result<Self, CastError> {
		match v {
			SqlValue::Number(v) => Ok(v),
			SqlValue::Strand(ref s) => Number::from_str(s).map_err(|_| CastError::InvalidKind {
				from: v,
				into: "number".into(),
			}),
			// Anything else raises an error
			_ => Err(CastError::InvalidKind {
				from: v,
				into: "number".into(),
			}),
		}
	}
}

impl Cast for Strand {
	fn can_cast(v: &SqlValue) -> bool {
		match v {
			SqlValue::None | SqlValue::Null => false,
			SqlValue::Bytes(b) => std::str::from_utf8(b).is_ok(),
			_ => true,
		}
	}

	fn cast(v: SqlValue) -> Result<Self, CastError> {
		match v {
			SqlValue::Bytes(b) => match String::from_utf8(b.0) {
				Ok(x) => Ok(Strand(x)),
				Err(e) => Err(CastError::InvalidKind {
					from: SqlValue::Bytes(Bytes(e.into_bytes())),
					into: "string".to_owned(),
				}),
			},

			SqlValue::Null | SqlValue::None => Err(CastError::InvalidKind {
				from: v,
				into: "string".into(),
			}),

			SqlValue::Strand(x) => Ok(x),
			SqlValue::Uuid(x) => Ok(x.to_raw().into()),
			SqlValue::Datetime(x) => Ok(x.to_raw().into()),
			x => Ok(Strand(x.to_string())),
		}
	}
}

impl Cast for String {
	fn can_cast(v: &SqlValue) -> bool {
		Strand::can_cast(v)
	}

	fn cast(v: SqlValue) -> Result<Self, CastError> {
		Strand::cast(v).map(|x| x.0)
	}
}

impl Cast for Uuid {
	fn can_cast(v: &SqlValue) -> bool {
		match v {
			SqlValue::Uuid(_) => true,
			SqlValue::Strand(s) => Uuid::from_str(s).is_ok(),
			_ => false,
		}
	}

	fn cast(v: SqlValue) -> Result<Self, CastError> {
		match v {
			SqlValue::Uuid(u) => Ok(u),
			SqlValue::Strand(ref s) => Uuid::from_str(s).map_err(|_| CastError::InvalidKind {
				from: v,
				into: "uuid".into(),
			}),
			_ => Err(CastError::InvalidKind {
				from: v,
				into: "uuid".into(),
			}),
		}
	}
}

impl Cast for Datetime {
	fn can_cast(v: &SqlValue) -> bool {
		match v {
			SqlValue::Datetime(_) => true,
			SqlValue::Strand(s) => Datetime::from_str(s).is_ok(),
			_ => false,
		}
	}

	fn cast(v: SqlValue) -> Result<Self, CastError> {
		match v {
			// Datetimes are allowed
			SqlValue::Datetime(v) => Ok(v),
			// Attempt to parse a string
			SqlValue::Strand(ref s) => Datetime::from_str(s).map_err(|_| CastError::InvalidKind {
				from: v,
				into: "datetime".into(),
			}),
			// Anything else raises an error
			_ => Err(CastError::InvalidKind {
				from: v,
				into: "datetime".into(),
			}),
		}
	}
}

impl Cast for Duration {
	fn can_cast(v: &SqlValue) -> bool {
		match v {
			SqlValue::Duration(_) => true,
			SqlValue::Strand(s) => Duration::from_str(s).is_ok(),
			_ => false,
		}
	}

	fn cast(v: SqlValue) -> Result<Self, CastError> {
		match v {
			// Datetimes are allowed
			SqlValue::Duration(v) => Ok(v),
			// Attempt to parse a string
			SqlValue::Strand(ref s) => Duration::from_str(s).map_err(|_| CastError::InvalidKind {
				from: v,
				into: "duration".into(),
			}),
			// Anything else raises an error
			_ => Err(CastError::InvalidKind {
				from: v,
				into: "duration".into(),
			}),
		}
	}
}

impl Cast for Bytes {
	fn can_cast(v: &SqlValue) -> bool {
		match v {
			SqlValue::Bytes(_) | SqlValue::Strand(_) => true,
			SqlValue::Array(x) => x.iter().all(|x| x.can_cast_to::<i64>()),
			_ => false,
		}
	}

	fn cast(v: SqlValue) -> Result<Self, CastError> {
		match v {
			SqlValue::Bytes(b) => Ok(b),
			SqlValue::Strand(s) => Ok(Bytes(s.0.into_bytes())),
			SqlValue::Array(x) => {
				// Optimization to check first if the conversion can succeed to avoid possibly
				// cloning large values.
				if !x.0.iter().all(|x| x.can_cast_to::<i64>()) {
					return Err(CastError::InvalidKind {
						from: x.into(),
						into: "bytes".to_owned(),
					});
				}

				let mut res = Vec::new();

				for v in x.0.into_iter() {
					// Unwrap condition checked above.
					let x = v.clone().cast_to::<i64>().unwrap();
					// TODO: Fix truncation.
					res.push(x as u8);
				}

				Ok(Bytes(res))
			}
			_ => Err(CastError::InvalidKind {
				from: v,
				into: "bytes".into(),
			}),
		}
	}
}

impl Cast for Array {
	fn can_cast(v: &SqlValue) -> bool {
		match v {
			SqlValue::Array(_) | SqlValue::Bytes(_) => true,
			SqlValue::Range(r) => r.can_coerce_to_typed::<i64>(),
			_ => false,
		}
	}

	fn cast(v: SqlValue) -> Result<Self, CastError> {
		match v {
			SqlValue::Array(x) => Ok(x),
			SqlValue::Range(range) => {
				if !range.can_coerce_to_typed::<i64>() {
					return Err(CastError::InvalidKind {
						from: SqlValue::Range(range),
						into: "array".to_string(),
					});
				}
				// unwrap checked above
				let range = range.coerce_to_typed::<i64>().unwrap();
				range.clone().cast_to_array().ok_or_else(|| CastError::RangeSizeLimit {
					value: Box::new(Range::from(range)),
				})
			}
			SqlValue::Bytes(x) => Ok(Array(x.0.into_iter().map(|x| SqlValue::from(x as i64)).collect())),
			_ => Err(CastError::InvalidKind {
				from: v,
				into: "array".into(),
			}),
		}
	}
}

impl Cast for Regex {
	fn can_cast(v: &SqlValue) -> bool {
		match v {
			SqlValue::Regex(_) => true,
			SqlValue::Strand(x) => Regex::from_str(x).is_ok(),
			_ => false,
		}
	}

	fn cast(v: SqlValue) -> Result<Self, CastError> {
		match v {
			SqlValue::Regex(x) => Ok(x),
			SqlValue::Strand(x) => match Regex::from_str(&x) {
				Ok(x) => Ok(x),
				Err(_) => Err(CastError::InvalidKind {
					from: SqlValue::Strand(x),
					into: "regex".to_string(),
				}),
			},
			x => Err(CastError::InvalidKind {
				from: x,
				into: "regex".to_string(),
			}),
		}
	}
}

impl Cast for Box<Range> {
	fn can_cast(v: &SqlValue) -> bool {
		match v {
			SqlValue::Range(_) => true,
			SqlValue::Array(x) => x.len() == 2,
			_ => false,
		}
	}

	fn cast(v: SqlValue) -> Result<Self, CastError> {
		match v {
			SqlValue::Range(x) => Ok(x),
			SqlValue::Array(x) => {
				if x.len() != 2 {
					return Err(CastError::InvalidKind {
						from: SqlValue::Array(x),
						into: "range".to_string(),
					});
				}

				let mut iter = x.into_iter();
				// unwrap checked above.
				let beg = iter.next().unwrap();
				// unwrap checked above.
				let end = iter.next().unwrap();

				Ok(Box::new(Range {
					beg: Bound::Included(beg),
					end: Bound::Excluded(end),
				}))
			}
			_ => Err(CastError::InvalidKind {
				from: v,
				into: "range".into(),
			}),
		}
	}
}

impl Cast for Point<f64> {
	fn can_cast(v: &SqlValue) -> bool {
		match v {
			SqlValue::Geometry(Geometry::Point(_)) => true,
			SqlValue::Array(x) => x.len() == 2,
			_ => false,
		}
	}

	fn cast(v: SqlValue) -> Result<Self, CastError> {
		match v {
			SqlValue::Geometry(Geometry::Point(v)) => Ok(v),
			SqlValue::Array(x) => {
				if x.len() != 2 {
					return Err(CastError::InvalidKind {
						from: SqlValue::Array(x),
						into: "point".to_string(),
					});
				}

				if !x[0].can_coerce_to::<f64>() || !x[1].can_coerce_to::<f64>() {
					return Err(CastError::InvalidKind {
						from: SqlValue::Array(x),
						into: "point".to_string(),
					});
				}

				let mut iter = x.into_iter();
				// Both unwraps checked above.
				let x = iter.next().unwrap().cast_to::<f64>().unwrap();
				// Both unwraps checked above.
				let y = iter.next().unwrap().cast_to::<f64>().unwrap();

				Ok(Point::new(x, y))
			}
			_ => Err(CastError::InvalidKind {
				from: v,
				into: "point".into(),
			}),
		}
	}
}

impl Cast for Thing {
	fn can_cast(v: &SqlValue) -> bool {
		match v {
			SqlValue::Thing(_) => true,
			SqlValue::Strand(x) => Thing::from_str(x).is_ok(),
			_ => false,
		}
	}

	fn cast(v: SqlValue) -> Result<Self, CastError> {
		match v {
			SqlValue::Thing(x) => Ok(x),
			SqlValue::Strand(x) => match Thing::from_str(x.as_ref()) {
				Ok(x) => Ok(x),
				Err(_) => Err(CastError::InvalidKind {
					from: SqlValue::Strand(x),
					into: "record".to_string(),
				}),
			},
			from => Err(CastError::InvalidKind {
				from,
				into: "record".to_string(),
			}),
		}
	}
}

macro_rules! impl_direct {
	($($name:ident => $inner:ty $(= $kind:ident)?),*$(,)?) => {
		$(
		impl Cast for $inner {
			fn can_cast(v: &SqlValue) -> bool{
				matches!(v, SqlValue::$name(_))
			}

			fn cast(v: SqlValue) -> Result<Self, CastError> {
				if let SqlValue::$name(x) = v {
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
		<$kind as HasKind>::kind().to_string()
	};

	(@kindof $inner:ty) => {
		<$inner as HasKind>::kind().to_string()
	};
}

// Types which directly match one enum variant or fail
impl_direct! {
	Closure => Box<Closure> = Closure,
	Object => Object,
	Geometry => Geometry,
	File => File,
}

impl SqlValue {
	pub fn can_cast_to<T: Cast>(&self) -> bool {
		T::can_cast(self)
	}

	pub fn can_cast_to_kind(&self, kind: &Kind) -> bool {
		match kind {
			Kind::Any => true,
			Kind::Null => self.can_cast_to::<Null>(),
			Kind::Bool => self.can_cast_to::<bool>(),
			Kind::Int => self.can_cast_to::<i64>(),
			Kind::Float => self.can_cast_to::<f64>(),
			Kind::Decimal => self.can_cast_to::<Decimal>(),
			Kind::Number => self.can_cast_to::<Number>(),
			Kind::String => self.can_cast_to::<Strand>(),
			Kind::Datetime => self.can_cast_to::<Datetime>(),
			Kind::Duration => self.can_cast_to::<Duration>(),
			Kind::Object => self.can_cast_to::<Object>(),
			Kind::Point => self.can_cast_to::<Point<f64>>(),
			Kind::Bytes => self.can_cast_to::<Bytes>(),
			Kind::Uuid => self.can_cast_to::<Uuid>(),
			Kind::Regex => self.can_cast_to::<Regex>(),
			Kind::Range => self.can_cast_to::<Box<Range>>(),
			Kind::Function(_, _) => self.can_cast_to::<Box<Closure>>(),
			Kind::Set(t, l) => match l {
				Some(l) => self.can_cast_to_array_len(t, *l),
				None => self.can_cast_to_array(t),
			},
			Kind::Array(t, l) => match l {
				Some(l) => self.can_cast_to_array_len(t, *l),
				None => self.can_cast_to_array(t),
			},
			Kind::Record(t) => {
				if t.is_empty() {
					self.can_cast_to::<Thing>()
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
			Kind::Option(k) => match self {
				Self::None => true,
				v => v.can_cast_to_kind(k),
			},
			Kind::Either(k) => k.iter().any(|x| self.can_cast_to_kind(x)),
			Kind::Literal(lit) => self.can_cast_to_literal(lit),
			Kind::References(_, _) => false,
			Kind::File(buckets) => {
				if buckets.is_empty() {
					self.can_cast_to::<File>()
				} else {
					self.can_cast_to_file_buckets(buckets)
				}
			}
		}
	}

	fn can_cast_to_array_len(&self, kind: &Kind, len: u64) -> bool {
		match self {
			SqlValue::Array(a) => a.len() as u64 == len && a.iter().all(|x| x.can_cast_to_kind(kind)),
			_ => false,
		}
	}

	fn can_cast_to_array(&self, kind: &Kind) -> bool {
		match self {
			SqlValue::Array(a) => a.iter().all(|x| x.can_cast_to_kind(kind)),
			_ => false,
		}
	}

	fn can_cast_to_record(&self, val: &[Table]) -> bool {
		match self {
			SqlValue::Thing(t) => t.is_record_type(val),
			_ => false,
		}
	}

	fn can_cast_to_geometry(&self, val: &[String]) -> bool {
		self.is_geometry_type(val)
	}

	fn can_cast_to_literal(&self, val: &Literal) -> bool {
		val.validate_value(self)
	}

	fn can_cast_to_file_buckets(&self, buckets: &[Ident]) -> bool {
		matches!(self, SqlValue::File(f) if f.is_bucket_type(buckets))
	}

	pub fn cast_to<T: Cast>(self) -> Result<T, CastError> {
		T::cast(self)
	}

	/// Try to convert this value to the specified `Kind`
	pub fn cast_to_kind(self, kind: &Kind) -> Result<SqlValue, CastError> {
		// Attempt to convert to the desired type
		match kind {
			Kind::Any => Ok(self),
			Kind::Null => self.cast_to::<Null>().map(|_| SqlValue::Null),
			Kind::Bool => self.cast_to::<bool>().map(SqlValue::from),
			Kind::Int => self.cast_to::<i64>().map(SqlValue::from),
			Kind::Float => self.cast_to::<f64>().map(SqlValue::from),
			Kind::Decimal => self.cast_to::<Decimal>().map(SqlValue::from),
			Kind::Number => self.cast_to::<Number>().map(SqlValue::from),
			Kind::String => self.cast_to::<Strand>().map(SqlValue::from),
			Kind::Datetime => self.cast_to::<Datetime>().map(SqlValue::from),
			Kind::Duration => self.cast_to::<Duration>().map(SqlValue::from),
			Kind::Object => self.cast_to::<Object>().map(SqlValue::from),
			Kind::Point => self.cast_to::<Point<f64>>().map(SqlValue::from),
			Kind::Bytes => self.cast_to::<Bytes>().map(SqlValue::from),
			Kind::Uuid => self.cast_to::<Uuid>().map(SqlValue::from),
			Kind::Regex => self.cast_to::<Regex>().map(SqlValue::from),
			Kind::Range => self.cast_to::<Box<Range>>().map(SqlValue::from),
			Kind::Function(_, _) => self.cast_to::<Box<Closure>>().map(SqlValue::from),
			Kind::Set(t, l) => match l {
				Some(l) => self.cast_to_set_type_len(t, *l).map(SqlValue::from),
				None => self.cast_to_set_type(t).map(SqlValue::from),
			},
			Kind::Array(t, l) => match l {
				Some(l) => self.cast_to_array_len(t, *l).map(SqlValue::from),
				None => self.cast_to_array(t).map(SqlValue::from),
			},
			Kind::Record(t) => match t.is_empty() {
				true => self.cast_to::<Thing>().map(SqlValue::from),
				false => self.cast_to_record(t).map(SqlValue::from),
			},
			Kind::Geometry(t) => match t.is_empty() {
				true => self.cast_to::<Geometry>().map(SqlValue::from),
				false => self.cast_to_geometry(t).map(SqlValue::from),
			},
			Kind::Option(k) => match self {
				Self::None => Ok(Self::None),
				v => v.cast_to_kind(k),
			},
			Kind::Either(k) => {
				let Some(k) = k.iter().find(|x| self.can_cast_to_kind(x)) else {
					return Err(CastError::InvalidKind {
						from: self,
						into: kind.to_string(),
					});
				};

				Ok(self.cast_to_kind(k).expect(
					"If can_coerce_to_kind returns true then coerce_to_kind must not error",
				))
			}
			Kind::Literal(lit) => self.cast_to_literal(lit),
			Kind::References(_, _) => Err(CastError::InvalidKind {
				from: self,
				into: kind.to_string(),
			}),
			Kind::File(buckets) => {
				if buckets.is_empty() {
					self.cast_to::<File>().map(SqlValue::from)
				} else {
					self.cast_to_file_buckets(buckets).map(SqlValue::from)
				}
			}
		}
	}

	/// Try to convert this value to a Literal, returns a `Value` with the coerced value
	pub(crate) fn cast_to_literal(self, literal: &Literal) -> Result<SqlValue, CastError> {
		if literal.validate_value(&self) {
			Ok(self)
		} else {
			Err(CastError::InvalidKind {
				from: self,
				into: literal.to_string(),
			})
		}
	}

	/// Try to convert this value to a Record of a certain type
	fn cast_to_record(self, val: &[Table]) -> Result<Thing, CastError> {
		match self {
			SqlValue::Thing(v) if v.is_record_type(val) => Ok(v),
			SqlValue::Strand(v) => match Thing::from_str(v.as_str()) {
				Ok(x) if x.is_record_type(val) => Ok(x),
				_ => {
					let mut kind = "record<".to_string();
					for (idx, t) in val.iter().enumerate() {
						if idx != 0 {
							kind.push('|');
						}
						kind.push_str(t.as_str())
					}
					kind.push('>');

					Err(CastError::InvalidKind {
						from: SqlValue::Strand(v),
						into: kind,
					})
				}
			},
			x => {
				let mut kind = "record<".to_string();
				for (idx, t) in val.iter().enumerate() {
					if idx != 0 {
						kind.push('|');
					}
					kind.push_str(t.as_str())
				}
				kind.push('>');

				Err(CastError::InvalidKind {
					from: x,
					into: kind,
				})
			}
		}
	}

	/// Try to convert this value to a `Geometry` of a certain type
	fn cast_to_geometry(self, val: &[String]) -> Result<Geometry, CastError> {
		match self {
			// Geometries are allowed if correct type
			SqlValue::Geometry(v) if self.is_geometry_type(val) => Ok(v),
			// Anything else raises an error
			_ => Err(CastError::InvalidKind {
				from: self,
				into: "geometry".into(),
			}),
		}
	}

	/// Try to convert this value to ab `Array` of a certain type
	fn cast_to_array(self, kind: &Kind) -> Result<Array, CastError> {
		self.cast_to::<Array>()?
			.into_iter()
			.map(|value| value.cast_to_kind(kind))
			.collect::<Result<Array, CastError>>()
			.with_element_of(|| format!("array<{kind}>"))
	}

	/// Try to convert this value to ab `Array` of a certain type and length
	fn cast_to_array_len(self, kind: &Kind, len: u64) -> Result<Array, CastError> {
		let array = self.cast_to::<Array>()?;

		if (array.len() as u64) != len {
			return Err(CastError::InvalidLength {
				len: array.len(),
				into: format!("array<{kind},{len}>"),
			});
		}

		array
			.into_iter()
			.map(|value| value.cast_to_kind(kind))
			.collect::<Result<Array, CastError>>()
			.with_element_of(|| format!("array<{kind}>"))
	}

	/// Try to convert this value to an `Array` of a certain type, unique values
	pub(crate) fn cast_to_set_type(self, kind: &Kind) -> Result<Array, CastError> {
		let array = self.cast_to::<Array>()?;

		let array = array
			.into_iter()
			.map(|value| value.cast_to_kind(kind))
			.collect::<Result<Array, CastError>>()
			.with_element_of(|| format!("array<{kind}>"))?
			.uniq();

		Ok(array)
	}

	/// Try to convert this value to an `Array` of a certain type, unique values, and length
	pub(crate) fn cast_to_set_type_len(self, kind: &Kind, len: u64) -> Result<Array, CastError> {
		let array = self.cast_to::<Array>()?;

		let array = array
			.into_iter()
			.map(|value| value.cast_to_kind(kind))
			.collect::<Result<Array, CastError>>()
			.with_element_of(|| format!("array<{kind}>"))?
			.uniq();

		if (array.len() as u64) != len {
			return Err(CastError::InvalidLength {
				len: array.len(),
				into: format!("set<{kind},{len}>"),
			});
		}

		Ok(array)
	}

	pub(crate) fn cast_to_file_buckets(self, buckets: &[Ident]) -> Result<File, CastError> {
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
