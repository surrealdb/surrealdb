use std::fmt;
use std::ops::Bound;
use std::str::FromStr as _;

use geo::Point;
use rust_decimal::Decimal;

use crate::cnf::GENERATION_ALLOCATION_LIMIT;
use crate::expr::Kind;
use crate::expr::kind::{GeometryKind, HasKind, KindLiteral};
use crate::syn;
use crate::val::array::Uniq;
use crate::val::{
	Array, Bytes, Closure, Datetime, DecimalExt, Duration, File, Geometry, Null, Number, Object,
	Range, RecordId, Regex, Strand, Uuid, Value,
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

impl Cast for Null {
	fn can_cast(v: &Value) -> bool {
		matches!(v, Value::Null)
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		match v {
			Value::Null => Ok(Null),
			x => Err(CastError::InvalidKind {
				from: x,
				into: "null".to_string(),
			}),
		}
	}
}

impl Cast for bool {
	fn can_cast(v: &Value) -> bool {
		match v {
			Value::Bool(_) => true,
			Value::Strand(x) => matches!(x.as_str(), "true" | "false"),
			_ => false,
		}
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		match v {
			Value::Bool(b) => Ok(b),
			Value::Strand(x) => match x.as_str() {
				"true" => Ok(true),
				"false" => Ok(false),
				_ => Err(CastError::InvalidKind {
					from: Value::Strand(x),
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
	fn can_cast(v: &Value) -> bool {
		match v {
			Value::Number(Number::Int(_)) => true,
			Value::Number(Number::Float(v)) => v.fract() == 0.0,
			Value::Number(Number::Decimal(v)) => v.is_integer() || i64::try_from(*v).is_ok(),
			Value::Strand(v) => v.parse::<i64>().is_ok(),
			_ => false,
		}
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		match v {
			// Allow any int number
			Value::Number(Number::Int(x)) => Ok(x),
			// Attempt to convert an float number
			Value::Number(Number::Float(v)) if v.fract() == 0.0 => Ok(v as i64),
			// Attempt to convert a decimal number
			Value::Number(Number::Decimal(d)) if d.is_integer() => match d.try_into() {
				Ok(v) => Ok(v),
				_ => Err(CastError::InvalidKind {
					from: v,
					into: "int".into(),
				}),
			},
			// Attempt to convert a string value
			Value::Strand(ref s) => match s.parse::<i64>() {
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
	fn can_cast(v: &Value) -> bool {
		match v {
			Value::Number(Number::Int(_) | Number::Float(_)) => true,
			Value::Number(Number::Decimal(v)) => v.is_integer() || i64::try_from(*v).is_ok(),
			Value::Strand(v) => v.parse::<f64>().is_ok(),
			_ => false,
		}
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		match v {
			Value::Number(Number::Float(i)) => Ok(i),
			Value::Number(Number::Int(f)) => Ok(f as f64),
			Value::Number(Number::Decimal(d)) => match d.try_into() {
				// The Decimal can be parsed as a Float
				Ok(v) => Ok(v),
				// The Decimal loses precision
				_ => Err(CastError::InvalidKind {
					from: v,
					into: "float".into(),
				}),
			},
			// Attempt to convert a string value
			Value::Strand(ref s) => match s.parse::<f64>() {
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
	fn can_cast(v: &Value) -> bool {
		match v {
			Value::Number(_) => true,
			Value::Strand(v) => v.parse::<f64>().is_ok(),
			_ => false,
		}
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		match v {
			Value::Number(Number::Decimal(d)) => Ok(d),
			// Attempt to convert an int number
			Value::Number(Number::Int(ref i)) => Ok(Decimal::from(*i)),
			// Attempt to convert an float number
			Value::Number(Number::Float(ref f)) => match Decimal::try_from(*f) {
				// The Float can be represented as a Decimal
				Ok(d) => Ok(d),
				// This Float does not convert to a Decimal
				_ => Err(CastError::InvalidKind {
					from: v,
					into: "decimal".into(),
				}),
			},
			// Attempt to convert a string value
			Value::Strand(ref s) => match Decimal::from_str_normalized(s) {
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
	fn can_cast(v: &Value) -> bool {
		match v {
			Value::Number(_) => true,
			Value::Strand(s) => Number::from_str(s).is_ok(),
			_ => false,
		}
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		match v {
			Value::Number(v) => Ok(v),
			Value::Strand(ref s) => Number::from_str(s).map_err(|_| CastError::InvalidKind {
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
	fn can_cast(v: &Value) -> bool {
		match v {
			Value::None | Value::Null => false,
			Value::Bytes(b) => !b.contains(&0) && std::str::from_utf8(b).is_ok(),
			_ => true,
		}
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		match v {
			Value::Bytes(b) => match String::from_utf8(b.0) {
				Ok(x) => {
					if x.contains('\0') {
						Err(CastError::InvalidKind {
							from: Value::Bytes(Bytes(x.into_bytes())),
							into: "string".to_owned(),
						})
					} else {
						// Safety: Condition checked above.
						Ok(unsafe { Strand::new_unchecked(x) })
					}
				}
				Err(e) => Err(CastError::InvalidKind {
					from: Value::Bytes(Bytes(e.into_bytes())),
					into: "string".to_owned(),
				}),
			},

			Value::Null | Value::None => Err(CastError::InvalidKind {
				from: v,
				into: "string".into(),
			}),

			Value::Strand(x) => Ok(x),
			Value::Uuid(x) => Ok(x.to_raw().into()),
			Value::Datetime(x) => Ok(x.into_raw_string().into()),
			// TODO: Handle null bytes
			x => Ok(unsafe { Strand::new_unchecked(x.to_string()) }),
		}
	}
}

impl Cast for String {
	fn can_cast(v: &Value) -> bool {
		Strand::can_cast(v)
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		Strand::cast(v).map(|x| x.into_string())
	}
}

impl Cast for Uuid {
	fn can_cast(v: &Value) -> bool {
		match v {
			Value::Uuid(_) => true,
			Value::Strand(s) => Uuid::from_str(s).is_ok(),
			_ => false,
		}
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		match v {
			Value::Uuid(u) => Ok(u),
			Value::Strand(ref s) => Uuid::from_str(s).map_err(|_| CastError::InvalidKind {
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
	fn can_cast(v: &Value) -> bool {
		match v {
			Value::Datetime(_) => true,
			Value::Strand(s) => Datetime::from_str(s).is_ok(),
			_ => false,
		}
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		match v {
			// Datetimes are allowed
			Value::Datetime(v) => Ok(v),
			// Attempt to parse a string
			Value::Strand(ref s) => Datetime::from_str(s).map_err(|_| CastError::InvalidKind {
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
	fn can_cast(v: &Value) -> bool {
		match v {
			Value::Duration(_) => true,
			Value::Strand(s) => Duration::from_str(s).is_ok(),
			_ => false,
		}
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		match v {
			// Datetimes are allowed
			Value::Duration(v) => Ok(v),
			// Attempt to parse a string
			Value::Strand(ref s) => Duration::from_str(s).map_err(|_| CastError::InvalidKind {
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
	fn can_cast(v: &Value) -> bool {
		match v {
			Value::Bytes(_) | Value::Strand(_) => true,
			Value::Array(x) => x.iter().all(|x| x.can_cast_to::<i64>()),
			_ => false,
		}
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		match v {
			Value::Bytes(b) => Ok(b),
			Value::Strand(s) => Ok(Bytes(s.into_string().into_bytes())),
			Value::Array(x) => {
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
	fn can_cast(v: &Value) -> bool {
		match v {
			Value::Array(_) | Value::Bytes(_) => true,
			Value::Range(r) => r.can_coerce_to_typed::<i64>(),
			_ => false,
		}
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		match v {
			Value::Array(x) => Ok(x),
			Value::Range(range) => {
				if !range.can_coerce_to_typed::<i64>() {
					return Err(CastError::InvalidKind {
						from: Value::Range(range),
						into: "array".to_string(),
					});
				}
				// unwrap checked above
				let range = range.coerce_to_typed::<i64>().unwrap();
				if range.len() > *GENERATION_ALLOCATION_LIMIT {
					return Err(CastError::RangeSizeLimit {
						value: Box::new(Range::from(range)),
					});
				}

				Ok(range.cast_to_array())
			}

			Value::Bytes(x) => Ok(Array(x.0.into_iter().map(|x| Value::from(x as i64)).collect())),
			_ => Err(CastError::InvalidKind {
				from: v,
				into: "array".into(),
			}),
		}
	}
}

impl Cast for Regex {
	fn can_cast(v: &Value) -> bool {
		match v {
			Value::Regex(_) => true,
			Value::Strand(x) => Regex::from_str(x).is_ok(),
			_ => false,
		}
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		match v {
			Value::Regex(x) => Ok(x),
			Value::Strand(x) => match Regex::from_str(&x) {
				Ok(x) => Ok(x),
				Err(_) => Err(CastError::InvalidKind {
					from: Value::Strand(x),
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
	fn can_cast(v: &Value) -> bool {
		match v {
			Value::Range(_) => true,
			Value::Array(x) => x.len() == 2,
			_ => false,
		}
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		match v {
			Value::Range(x) => Ok(x),
			Value::Array(x) => {
				if x.len() != 2 {
					return Err(CastError::InvalidKind {
						from: Value::Array(x),
						into: "range".to_string(),
					});
				}

				let mut iter = x.into_iter();
				// unwrap checked above.
				let beg = iter.next().unwrap();
				// unwrap checked above.
				let end = iter.next().unwrap();

				Ok(Box::new(Range {
					start: Bound::Included(beg),
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
	fn can_cast(v: &Value) -> bool {
		match v {
			Value::Geometry(Geometry::Point(_)) => true,
			Value::Array(x) => x.len() == 2,
			_ => false,
		}
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		match v {
			Value::Geometry(Geometry::Point(v)) => Ok(v),
			Value::Array(x) => {
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

impl Cast for RecordId {
	fn can_cast(v: &Value) -> bool {
		match v {
			Value::RecordId(_) => true,
			Value::Strand(x) => syn::record_id(x).is_ok(),
			_ => false,
		}
	}

	fn cast(v: Value) -> Result<Self, CastError> {
		match v {
			Value::RecordId(x) => Ok(x),
			Value::Strand(x) => match syn::record_id(&x) {
				Ok(x) => Ok(x),
				Err(_) => Err(CastError::InvalidKind {
					from: Value::Strand(x),
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

impl Value {
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
			Kind::Option(k) => match self {
				Self::None => true,
				v => v.can_cast_to_kind(k),
			},
			Kind::Either(k) => k.iter().any(|x| self.can_cast_to_kind(x)),
			Kind::Literal(lit) => self.can_cast_to_literal(lit),
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
			Value::Array(a) => a.len() as u64 == len && a.iter().all(|x| x.can_cast_to_kind(kind)),
			_ => false,
		}
	}

	fn can_cast_to_array(&self, kind: &Kind) -> bool {
		match self {
			Value::Array(a) => a.iter().all(|x| x.can_cast_to_kind(kind)),
			_ => false,
		}
	}

	fn can_cast_to_record(&self, val: &[String]) -> bool {
		match self {
			Value::RecordId(t) => t.is_record_type(val),
			_ => false,
		}
	}

	fn can_cast_to_geometry(&self, val: &[GeometryKind]) -> bool {
		self.is_geometry_type(val)
	}

	fn can_cast_to_literal(&self, val: &KindLiteral) -> bool {
		val.validate_value(self)
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
			Kind::Null => self.cast_to::<Null>().map(|_| Value::Null),
			Kind::Bool => self.cast_to::<bool>().map(Value::from),
			Kind::Int => self.cast_to::<i64>().map(Value::from),
			Kind::Float => self.cast_to::<f64>().map(Value::from),
			Kind::Decimal => self.cast_to::<Decimal>().map(Value::from),
			Kind::Number => self.cast_to::<Number>().map(Value::from),
			Kind::String => self.cast_to::<Strand>().map(Value::from),
			Kind::Datetime => self.cast_to::<Datetime>().map(Value::from),
			Kind::Duration => self.cast_to::<Duration>().map(Value::from),
			Kind::Object => self.cast_to::<Object>().map(Value::from),
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
			Kind::Record(t) => match t.is_empty() {
				true => self.cast_to::<RecordId>().map(Value::from),
				false => self.cast_to_record(t).map(Value::from),
			},
			Kind::Geometry(t) => match t.is_empty() {
				true => self.cast_to::<Geometry>().map(Value::from),
				false => self.cast_to_geometry(t).map(Value::from),
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
			Kind::File(buckets) => {
				if buckets.is_empty() {
					self.cast_to::<File>().map(Value::from)
				} else {
					self.cast_to_file_buckets(buckets).map(Value::from)
				}
			}
		}
	}

	/// Try to convert this value to a Literal, returns a `Value` with the
	/// coerced value
	pub(crate) fn cast_to_literal(self, literal: &KindLiteral) -> Result<Value, CastError> {
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
	fn cast_to_record(self, val: &[String]) -> Result<RecordId, CastError> {
		match self {
			Value::RecordId(v) if v.is_record_type(val) => Ok(v),
			Value::Strand(v) => match syn::record_id(v.as_str()) {
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
						from: Value::Strand(v),
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
	fn cast_to_geometry(self, val: &[GeometryKind]) -> Result<Geometry, CastError> {
		match self {
			// Geometries are allowed if correct type
			Value::Geometry(v) if self.is_geometry_type(val) => Ok(v),
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

	/// Try to convert this value to an `Array` of a certain type, unique
	/// values, and length
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

	pub(crate) fn cast_to_file_buckets(self, buckets: &[String]) -> Result<File, CastError> {
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
