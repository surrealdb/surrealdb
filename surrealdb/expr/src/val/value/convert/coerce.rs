#![deny(clippy::wildcard_enum_match_arm)]

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::hash::BuildHasher;

use geo_types::Point;
use rust_decimal::Decimal;
use surrealdb_types::{BigDecimal, ToSql};

use crate::expr::Kind;
use crate::expr::kind::{GeometryKind, HasKind, KindLiteral};
use crate::val::{
	Array, Bytes, Closure, Date, Datetime, Duration, File, Geometry, Interval, Null, Number,
	Object, Range, RecordId, Regex, Set, SqlNone, Strand, TableName, Time, Timestamp, Uuid, Value,
};

/// Identifies which element of a collection caused a coercion failure.
#[derive(Clone, Debug, PartialEq)]
pub enum ElementPosition {
	/// Zero-based index into an array or set.
	Index(usize),
	/// Key of an object field.
	Key(String),
}

#[derive(Clone, Debug)]
pub enum CoerceError {
	// Coercion error at the end.
	InvalidKind {
		from: Value,
		into: String,
	},
	InvalidLength {
		len: usize,
		into: String,
	},
	// Coerce failed because element of type didn't match.
	ElementOf {
		inner: Box<CoerceError>,
		into: String,
		position: Option<ElementPosition>,
	},
}
impl std::error::Error for CoerceError {}
impl CoerceError {
	/// Redacts the source value from this coercion error so error messages do
	/// not disclose values held by fields whose `FOR select` permission denies
	/// the caller. `NONE` carries no stored value and is left as `NONE`. The
	/// position of a failing element is dropped as well, since an array index
	/// or object key is structure derived from the hidden value.
	pub fn redact(self) -> Self {
		match self {
			CoerceError::InvalidKind {
				from: Value::None,
				into,
			} => CoerceError::InvalidKind {
				from: Value::None,
				into,
			},
			CoerceError::InvalidKind {
				into,
				..
			}
			| CoerceError::InvalidLength {
				into,
				..
			} => CoerceError::InvalidKind {
				from: Value::from("[REDACTED]"),
				into,
			},
			CoerceError::ElementOf {
				inner,
				into,
				..
			} => CoerceError::ElementOf {
				inner: Box::new(inner.redact()),
				into,
				position: None,
			},
		}
	}
}
impl fmt::Display for CoerceError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			CoerceError::InvalidKind {
				from,
				into,
			} => {
				write!(f, "Expected `{into}` but found `{from}`", from = from.to_sql())
			}
			CoerceError::ElementOf {
				inner,
				into,
				position,
			} => {
				inner.fmt(f)?;
				match position {
					Some(ElementPosition::Index(i)) => {
						write!(f, " when coercing element at index {i} of `{into}`")
					}
					Some(ElementPosition::Key(k)) => {
						write!(f, " when coercing value for key '{k}' of `{into}`")
					}
					None => write!(f, " when coercing an element of `{into}`"),
				}
			}
			CoerceError::InvalidLength {
				len,
				into,
			} => {
				write!(f, "Expected `{into}` but found a collection of length `{len}`")
			}
		}
	}
}

pub trait CoerceErrorExt {
	fn with_element_of<F>(self, f: F) -> Self
	where
		F: Fn() -> String;

	fn with_element_of_at_index<F>(self, index: usize, f: F) -> Self
	where
		F: Fn() -> String;
}

impl<T> CoerceErrorExt for Result<T, CoerceError> {
	fn with_element_of<F>(self, f: F) -> Self
	where
		F: Fn() -> String,
	{
		match self {
			Ok(x) => Ok(x),
			Err(e) => Err(CoerceError::ElementOf {
				inner: Box::new(e),
				into: f(),
				position: None,
			}),
		}
	}

	fn with_element_of_at_index<F>(self, index: usize, f: F) -> Self
	where
		F: Fn() -> String,
	{
		match self {
			Ok(x) => Ok(x),
			Err(e) => Err(CoerceError::ElementOf {
				inner: Box::new(e),
				into: f(),
				position: Some(ElementPosition::Index(index)),
			}),
		}
	}
}

/// Trait for converting the value using coercion rules.
///
/// Coercion rules are applied whenever a Value needs to be of a specific
/// [`Kind`]. This happens when a value is applied to a place with a type like
/// table fields and function parameters.
///
/// Coercion rules are more strict then casting rules.
/// Calling this method will succeed if the value can be unified with the kind
/// of the target
pub trait Coerce: Sized {
	/// Returns if calling coerce on the value will succeed or not.
	///
	/// If `T::can_coerce(&v)` returns `false` then `T::coerce(v) should not
	fn can_coerce(v: &Value) -> bool;

	/// Coerce a value.
	fn coerce(v: Value) -> Result<Self, CoerceError>;
}

impl Coerce for Value {
	fn can_coerce(_: &Value) -> bool {
		true
	}

	fn coerce(v: Value) -> Result<Self, CoerceError> {
		Ok(v)
	}
}

impl Coerce for SqlNone {
	fn can_coerce(v: &Value) -> bool {
		matches!(v, Value::None)
	}

	fn coerce(v: Value) -> Result<Self, CoerceError> {
		if v.is_none() {
			Ok(SqlNone)
		} else {
			Err(CoerceError::InvalidKind {
				from: v,
				into: "none".to_string(),
			})
		}
	}
}

impl Coerce for Null {
	fn can_coerce(v: &Value) -> bool {
		matches!(v, Value::Null)
	}

	fn coerce(v: Value) -> Result<Null, CoerceError> {
		if v.is_null() {
			// Allow any null value
			Ok(Null)
		} else {
			// Anything else raises an error
			Err(CoerceError::InvalidKind {
				from: v,
				into: "null".into(),
			})
		}
	}
}

impl Coerce for i64 {
	/// A number coerces exactly when it denotes one `i64`, which is what
	/// `coerce` checks too: the two must agree, because a union kind coerces
	/// to the first member `can_coerce` accepts and then requires `coerce` to
	/// succeed.
	fn can_coerce(v: &Value) -> bool {
		matches!(v, Value::Number(n) if n.as_int_lossless().is_some())
	}

	fn coerce(val: Value) -> Result<Self, CoerceError> {
		// Allow any int number, or a float/decimal that is a whole number
		// within the `i64` range. Anything else is refused, never rounded or
		// clamped to the nearest bound.
		val.try_into_number().and_then(|n| n.as_int_lossless().ok_or(Value::Number(n))).map_err(
			|from| CoerceError::InvalidKind {
				from,
				into: "int".into(),
			},
		)
	}
}

impl Coerce for f64 {
	fn can_coerce(v: &Value) -> bool {
		let Value::Number(n) = v else {
			return false;
		};
		match n {
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
			| Number::F32(_) => true,
			Number::Decimal(d) => f64::try_from(*d).is_ok(),
			Number::BigDecimal(b) => b.to_f64().is_finite(),
		}
	}

	/// Try to coerce this value to an `f64`
	fn coerce(val: Value) -> Result<f64, CoerceError> {
		val.try_into_number().and_then(|n| n.as_float_lossless().ok_or(Value::Number(n))).map_err(
			|from| CoerceError::InvalidKind {
				from,
				into: "float".into(),
			},
		)
	}
}

impl Coerce for BigDecimal {
	fn can_coerce(v: &Value) -> bool {
		matches!(v, Value::Number(n) if n.as_bigdecimal_lossless().is_some())
	}

	fn coerce(val: Value) -> Result<Self, CoerceError> {
		val.try_into_number()
			.and_then(|n| n.as_bigdecimal_lossless().ok_or(Value::Number(n)))
			.map_err(|from| CoerceError::InvalidKind {
				from,
				into: "bigdecimal".into(),
			})
	}
}

impl Coerce for Decimal {
	fn can_coerce(v: &Value) -> bool {
		let Value::Number(n) = v else {
			return false;
		};
		match n {
			Number::Int(_)
			| Number::Decimal(_)
			| Number::I8(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::I32(_)
			| Number::U32(_)
			| Number::U64(_) => true,
			Number::Float(f) => Decimal::try_from(*f).is_ok(),
			Number::F16(f) => Decimal::try_from(f.to_f64()).is_ok(),
			Number::F32(f) => Decimal::try_from(*f as f64).is_ok(),
			Number::BigDecimal(b) => Decimal::try_from(b.as_ref()).is_ok(),
		}
	}

	fn coerce(val: Value) -> Result<Self, CoerceError> {
		val.try_into_number().and_then(|n| n.as_decimal_lossless().ok_or(Value::Number(n))).map_err(
			|from| CoerceError::InvalidKind {
				from,
				into: "decimal".into(),
			},
		)
	}
}

macro_rules! impl_int_coerce {
	($($ty:ident),* $(,)?) => {
		$(
			impl Coerce for $ty {
				fn can_coerce(v: &Value) -> bool {
					matches!(v, Value::Number(n) if n.as_int_lossless().and_then(|x| $ty::try_from(x).ok()).is_some())
				}

				fn coerce(val: Value) -> Result<Self, CoerceError> {
					val.try_into_number()
						.and_then(|n| n.as_int_lossless().and_then(|x| $ty::try_from(x).ok()).ok_or(Value::Number(n)))
						.map_err(|from| CoerceError::InvalidKind {
							from,
							into: stringify!($ty).into(),
						})
				}
			}
		)*
	};
}
impl_int_coerce!(i8, u8, i16, u16, i32, u32);

impl Coerce for u64 {
	fn can_coerce(v: &Value) -> bool {
		matches!(v, Value::Number(n) if n.as_u64_lossless().is_some())
	}

	fn coerce(val: Value) -> Result<Self, CoerceError> {
		val.try_into_number().and_then(|n| n.as_u64_lossless().ok_or(Value::Number(n))).map_err(
			|from| CoerceError::InvalidKind {
				from,
				into: "u64".into(),
			},
		)
	}
}

impl Coerce for half::f16 {
	fn can_coerce(v: &Value) -> bool {
		matches!(v, Value::Number(n) if n.as_f16().is_some())
	}

	fn coerce(val: Value) -> Result<Self, CoerceError> {
		val.try_into_number().and_then(|n| n.as_f16().ok_or(Value::Number(n))).map_err(|from| {
			CoerceError::InvalidKind {
				from,
				into: "f16".into(),
			}
		})
	}
}

impl Coerce for f32 {
	fn can_coerce(v: &Value) -> bool {
		matches!(v, Value::Number(n) if n.as_f32().is_some())
	}

	fn coerce(val: Value) -> Result<Self, CoerceError> {
		val.try_into_number().and_then(|n| n.as_f32().ok_or(Value::Number(n))).map_err(|from| {
			CoerceError::InvalidKind {
				from,
				into: "f32".into(),
			}
		})
	}
}

impl Coerce for File {
	fn can_coerce(v: &Value) -> bool {
		matches!(v, Value::File(_))
	}

	fn coerce(v: Value) -> Result<Self, CoerceError> {
		if let Value::File(x) = v {
			Ok(x)
		} else {
			Err(CoerceError::InvalidKind {
				from: v,
				into: "file".to_string(),
			})
		}
	}
}

impl Coerce for Point<f64> {
	fn can_coerce(v: &Value) -> bool {
		matches!(v, Value::Geometry(Geometry::Point(_)))
	}

	fn coerce(v: Value) -> Result<Self, CoerceError> {
		if let Value::Geometry(Geometry::Point(x)) = v {
			Ok(x)
		} else {
			Err(CoerceError::InvalidKind {
				from: v,
				into: "point".to_string(),
			})
		}
	}
}

impl<T: Coerce + HasKind> Coerce for Option<T> {
	fn can_coerce(v: &Value) -> bool {
		if let Value::None = v {
			return true;
		}
		T::can_coerce(v)
	}

	fn coerce(v: Value) -> Result<Self, CoerceError> {
		if v.is_none() {
			Ok(None)
		} else {
			Ok(Some(T::coerce(v)?))
		}
	}
}

impl<T: Coerce + HasKind> Coerce for Vec<T> {
	fn can_coerce(v: &Value) -> bool {
		let Value::Array(a) = v else {
			return false;
		};
		a.iter().all(T::can_coerce)
	}

	fn coerce(v: Value) -> Result<Self, CoerceError> {
		if !v.is_array() {
			return Err(CoerceError::InvalidKind {
				from: v,
				into: <Self as HasKind>::kind().to_sql(),
			});
		}
		// checked above
		let array = v.into_array().expect("value type checked above");

		let mut res = Vec::with_capacity(array.0.len());
		for (i, x) in array.0.into_iter().enumerate() {
			res.push(
				x.coerce_to::<T>()
					.with_element_of_at_index(i, || <Self as HasKind>::kind().to_sql())?,
			);
		}
		Ok(res)
	}
}

impl<T: Coerce + HasKind> Coerce for BTreeMap<String, T> {
	fn can_coerce(v: &Value) -> bool {
		let Value::Object(a) = v else {
			return false;
		};
		a.values().all(T::can_coerce)
	}

	fn coerce(v: Value) -> Result<Self, CoerceError> {
		if !v.is_object() {
			return Err(CoerceError::InvalidKind {
				from: v,
				into: Object::kind().to_sql(),
			});
		};
		// checked above
		let obj = v.into_object().expect("value type checked above");

		let mut res = BTreeMap::new();
		for (k, v) in obj.0 {
			// object<T> kinds don't actually exist in surql.
			let key = k.into_string();
			let value = match v.coerce_to::<T>() {
				Ok(v) => v,
				Err(e) => {
					return Err(CoerceError::ElementOf {
						inner: Box::new(e),
						into: format!("object<{}>", <T as HasKind>::kind().to_sql()),
						position: Some(ElementPosition::Key(key)),
					});
				}
			};
			res.insert(key, value);
		}
		Ok(res)
	}
}

impl<T: Coerce + HasKind, S: BuildHasher + Default> Coerce for HashMap<String, T, S> {
	fn can_coerce(v: &Value) -> bool {
		let Value::Object(a) = v else {
			return false;
		};
		a.values().all(T::can_coerce)
	}

	fn coerce(v: Value) -> Result<Self, CoerceError> {
		if !v.is_object() {
			return Err(CoerceError::InvalidKind {
				from: v,
				into: Kind::of::<Object>().to_sql(),
			});
		};
		// checked above
		let obj = v.into_object().expect("value type checked above");

		let mut res = HashMap::default();
		for (k, v) in obj.0 {
			// object<T> kinds don't actually exist in surql.
			let key = k.into_string();
			let value = match v.coerce_to::<T>() {
				Ok(v) => v,
				Err(e) => {
					return Err(CoerceError::ElementOf {
						inner: Box::new(e),
						into: format!("object<{}>", <T as HasKind>::kind().to_sql()),
						position: Some(ElementPosition::Key(key)),
					});
				}
			};
			res.insert(key, value);
		}
		Ok(res)
	}
}

macro_rules! impl_direct {
	($($name:ident => $inner:ty $(= $kind:ident)?),*$(,)?) => {
		$(
		impl Coerce for $inner {
			fn can_coerce(v: &Value) -> bool{
				matches!(v, Value::$name(_))
			}

			fn coerce(v: Value) -> Result<Self, CoerceError> {
				if let Value::$name(x) = v {
					return Ok(x);
				} else {
					return Err(CoerceError::InvalidKind{
						from: v,
						into: impl_direct!(@kindof $inner $(= $kind)?),
					});
				}
			}
		}
		)*
	};

	(@kindof $inner:ty = $kind:ident) => {
		Kind::of::<$kind>().to_sql()
	};

	(@kindof $inner:ty) => {
		Kind::of::<$inner>().to_sql()
	};
}

// Types which directly match one enum variant or fail
impl_direct! {
	Bool => bool,
	Number => Number,
	Uuid => Uuid,
	Closure => Box<Closure> = Closure,
	Range => Box<Range> = Range,
	Time => Time,
	Interval => Interval,
	Duration => Duration,
	Bytes => Bytes,
	Object => Object,
	Array => Array,
	RecordId => RecordId,
	String => Strand = String,
	Regex => Regex,
	Table => TableName,
}

// The temporal kinds coerce into each other only when no information is lost: a date widens to
// a timestamp or datetime at midnight, a timestamp reads as UTC, and a datetime narrows to a date
// only when it is exactly midnight UTC.
impl Coerce for Datetime {
	fn can_coerce(v: &Value) -> bool {
		matches!(v, Value::Datetime(_) | Value::Date(_) | Value::Timestamp(_))
	}

	fn coerce(v: Value) -> Result<Self, CoerceError> {
		if let Value::Datetime(x) = v {
			Ok(x)
		} else if let Value::Date(x) = v {
			Ok(x.into())
		} else if let Value::Timestamp(x) = v {
			Ok(x.into())
		} else {
			Err(CoerceError::InvalidKind {
				from: v,
				into: "datetime".into(),
			})
		}
	}
}

/// The date of a datetime that is exactly midnight UTC, or the infinite date of an infinite
/// datetime, which the same-signed infinite date widens to.
fn exact_or_infinite_date(x: &Datetime) -> Option<Date> {
	x.exact_date().or_else(|| (*x == Datetime::from(Date::infinity())).then(Date::infinity))
}

impl Coerce for Date {
	fn can_coerce(v: &Value) -> bool {
		matches!(v, Value::Date(_)) || v.is_datetime_and(|x| exact_or_infinite_date(x).is_some())
	}

	fn coerce(v: Value) -> Result<Self, CoerceError> {
		if let Value::Date(x) = v {
			return Ok(x);
		}
		if let Value::Datetime(x) = v {
			return exact_or_infinite_date(&x).ok_or(CoerceError::InvalidKind {
				from: v,
				into: "date (a datetime that is not midnight UTC needs the cast `<date>`)".into(),
			});
		}
		Err(CoerceError::InvalidKind {
			from: v,
			into: "date".into(),
		})
	}
}

impl Coerce for Timestamp {
	fn can_coerce(v: &Value) -> bool {
		matches!(v, Value::Timestamp(_) | Value::Date(_) | Value::Datetime(_))
	}

	fn coerce(v: Value) -> Result<Self, CoerceError> {
		if let Value::Timestamp(x) = v {
			Ok(x)
		} else if let Value::Date(x) = v {
			Ok(x.at_midnight())
		} else if let Value::Datetime(x) = v {
			Ok(x.into())
		} else {
			Err(CoerceError::InvalidKind {
				from: v,
				into: "timestamp".into(),
			})
		}
	}
}

/// A geometry, or a GeoJSON object describing one
/// ([`Geometry::try_from_object`]): the form a geometry takes as data, such as
/// decoded JSON or an object built from computed parts.
impl Coerce for Geometry {
	fn can_coerce(v: &Value) -> bool {
		v.is_geometry() || v.is_object_and(|x| Geometry::try_from_object(x).is_some())
	}

	fn coerce(v: Value) -> Result<Self, CoerceError> {
		let v = match v.try_into_geometry() {
			Ok(x) => return Ok(x),
			Err(v) => v,
		};
		if let Value::Object(ref x) = v
			&& let Some(x) = Geometry::try_from_object(x)
		{
			return Ok(x);
		}
		Err(CoerceError::InvalidKind {
			from: v,
			into: Kind::of::<Geometry>().to_sql(),
		})
	}
}

impl Coerce for Set {
	fn can_coerce(v: &Value) -> bool {
		matches!(v, Value::Set(_) | Value::Array(_))
	}

	fn coerce(v: Value) -> Result<Self, CoerceError> {
		let v = match v.try_into_set() {
			Ok(x) => return Ok(x),
			Err(v) => v,
		};
		v.try_into_array().map(|x| Set::from(x.0)).map_err(|from| CoerceError::InvalidKind {
			from,
			into: Kind::of::<Set>().to_sql(),
		})
	}
}

/// `String` coercion delegates to `Strand` and converts on success
/// so existing `coerce_to::<String>()` call sites keep working.
impl Coerce for String {
	fn can_coerce(v: &Value) -> bool {
		matches!(v, Value::String(_))
	}

	fn coerce(v: Value) -> Result<Self, CoerceError> {
		v.try_into_strand().map(Strand::into_string).map_err(|from| CoerceError::InvalidKind {
			from,
			into: Kind::of::<String>().to_sql(),
		})
	}
}

// Coerce to runtime value implementations
impl Value {
	pub fn can_coerce_to<T: Coerce>(&self) -> bool {
		T::can_coerce(self)
	}

	/// Whether taking this value as `kind` turns it into another value: an
	/// object read as the geometry it describes, or a number converted across
	/// a sized numeric boundary (preserving a sized number whose exact kind is
	/// a union member while keeping declared-member order among `int`, `float`,
	/// and `decimal`), or a date, datetime or timestamp taken as another of the
	/// three.
	pub fn converts_to(&self, kind: &Kind) -> bool {
		match (self, kind) {
			(Value::Object(_), Kind::Geometry(_)) => true,
			(
				Value::Number(Number::Int(_) | Number::Float(_) | Number::Decimal(_)),
				Kind::Int | Kind::Float | Kind::Decimal,
			) => false,
			(Value::Number(Number::I8(_)), Kind::I8)
			| (Value::Number(Number::U8(_)), Kind::U8)
			| (Value::Number(Number::I16(_)), Kind::I16)
			| (Value::Number(Number::U16(_)), Kind::U16)
			| (Value::Number(Number::I32(_)), Kind::I32)
			| (Value::Number(Number::U32(_)), Kind::U32)
			| (Value::Number(Number::U64(_)), Kind::U64)
			| (Value::Number(Number::F16(_)), Kind::F16)
			| (Value::Number(Number::F32(_)), Kind::F32)
			| (Value::Number(Number::BigDecimal(_)), Kind::BigDecimal) => false,
			(
				Value::Number(_),
				Kind::Int
				| Kind::Float
				| Kind::Decimal
				| Kind::BigDecimal
				| Kind::I8
				| Kind::U8
				| Kind::I16
				| Kind::U16
				| Kind::I32
				| Kind::U32
				| Kind::U64
				| Kind::F16
				| Kind::F32,
			) => true,
			// A date, a datetime and a timestamp coerce to one another, each changing kind.
			(Value::Date(_) | Value::Timestamp(_), Kind::Datetime)
			| (Value::Datetime(_), Kind::Date)
			| (Value::Date(_) | Value::Datetime(_), Kind::Timestamp) => true,
			_ => false,
		}
	}

	/// The member of the union `members` this value is taken as, of those
	/// `takes` admits it to: the first that takes it unchanged, and only when
	/// none does, the first that converts it ([`Value::converts_to`]). An
	/// object a union can hold as an object stays one; it becomes a geometry
	/// only where no other member holds it.
	pub fn union_member<'a>(
		&self,
		members: &'a [Kind],
		takes: impl Fn(&Value, &Kind) -> bool,
	) -> Option<&'a Kind> {
		members
			.iter()
			.find(|k| !self.converts_to(k) && takes(self, k))
			.or_else(|| members.iter().find(|k| self.converts_to(k) && takes(self, k)))
	}

	/// Whether this value is a string among the inline enum's `labels`.
	pub fn is_enum_member(&self, labels: &[String]) -> bool {
		if let Value::String(s) = self {
			labels.iter().any(|l| l == s.as_str())
		} else {
			false
		}
	}

	pub fn can_coerce_to_kind(&self, kind: &Kind) -> bool {
		match kind {
			Kind::Any => true,
			Kind::None => self.can_coerce_to::<SqlNone>(),
			Kind::Null => self.can_coerce_to::<Null>(),
			Kind::Bool => self.can_coerce_to::<bool>(),
			Kind::Int => self.can_coerce_to::<i64>(),
			Kind::Float => self.can_coerce_to::<f64>(),
			Kind::Decimal => self.can_coerce_to::<Decimal>(),
			Kind::BigDecimal => self.can_coerce_to::<BigDecimal>(),
			Kind::Number => self.can_coerce_to::<Number>(),
			Kind::I8 => self.can_coerce_to::<i8>(),
			Kind::U8 => self.can_coerce_to::<u8>(),
			Kind::I16 => self.can_coerce_to::<i16>(),
			Kind::U16 => self.can_coerce_to::<u16>(),
			Kind::I32 => self.can_coerce_to::<i32>(),
			Kind::U32 => self.can_coerce_to::<u32>(),
			Kind::U64 => self.can_coerce_to::<u64>(),
			Kind::F16 => self.can_coerce_to::<half::f16>(),
			Kind::F32 => self.can_coerce_to::<f32>(),
			Kind::String => self.can_coerce_to::<String>(),
			Kind::Datetime => self.can_coerce_to::<Datetime>(),
			Kind::Date => self.can_coerce_to::<Date>(),
			Kind::Time => self.can_coerce_to::<Time>(),
			Kind::Timestamp => self.can_coerce_to::<Timestamp>(),
			Kind::Interval => self.can_coerce_to::<Interval>(),
			Kind::Duration => self.can_coerce_to::<Duration>(),
			Kind::Object => self.can_coerce_to::<crate::val::Object>(),
			Kind::Bytes => self.can_coerce_to::<Bytes>(),
			Kind::Uuid => self.can_coerce_to::<Uuid>(),
			Kind::Regex => self.can_coerce_to::<Regex>(),
			Kind::Range => self.can_coerce_to::<Box<Range>>(),
			Kind::Function(_, _) => self.can_coerce_to::<Box<Closure>>(),
			Kind::Set(t, l) => match l {
				Some(l) => self.can_coerce_to_set_len(t, *l),
				None => self.can_coerce_to_set(t),
			},
			Kind::Array(t, l) => match l {
				Some(l) => self.can_coerce_to_array_len(t, *l),
				None => self.can_coerce_to_array(t),
			},
			Kind::Table(t) => {
				if t.is_empty() {
					self.can_coerce_to::<String>()
				} else {
					self.can_coerce_to_table(t)
				}
			}
			Kind::Record(t) => {
				if t.is_empty() {
					self.can_coerce_to::<RecordId>()
				} else {
					self.can_coerce_to_record(t)
				}
			}
			Kind::Geometry(t) => {
				if t.is_empty() {
					self.can_coerce_to::<Geometry>()
				} else {
					self.can_coerce_to_geometry(t)
				}
			}
			Kind::Either(k) => k.iter().any(|x| self.can_coerce_to_kind(x)),
			Kind::Literal(lit) => self.can_coerce_to_literal(lit),
			Kind::File(buckets) => {
				if buckets.is_empty() {
					self.can_coerce_to::<File>()
				} else {
					self.can_coerce_to_file_buckets(buckets)
				}
			}
			Kind::Enum(labels) => self.is_enum_member(labels),
			// A named type's body is judged where the catalog is reachable.
			Kind::Named(_) => true,
		}
	}

	fn can_coerce_to_array_len(&self, kind: &Kind, len: u64) -> bool {
		self.is_array_and(|a| a.len() as u64 == len && a.iter().all(|x| x.can_coerce_to_kind(kind)))
	}

	fn can_coerce_to_array(&self, kind: &Kind) -> bool {
		self.is_array_and(|a| a.iter().all(|x| x.can_coerce_to_kind(kind)))
	}

	fn can_coerce_to_set_len(&self, kind: &Kind, len: u64) -> bool {
		// An array becomes a set by deduplicating its converted elements, so
		// its length says nothing about the set's: the conversion decides.
		self.is_set_and(|s| s.len() as u64 == len && s.iter().all(|x| x.can_coerce_to_kind(kind)))
			|| (self.is_array() && self.clone().coerce_to_set_kind_len(kind, len).is_ok())
	}

	fn can_coerce_to_set(&self, kind: &Kind) -> bool {
		self.is_set_and(|s| s.iter().all(|x| x.can_coerce_to_kind(kind)))
			|| self.is_array_and(|a| a.iter().all(|x| x.can_coerce_to_kind(kind)))
	}

	fn can_coerce_to_table(&self, val: &[TableName]) -> bool {
		self.is_table_and(|t| val.is_empty() || val.contains(t))
			|| self.is_strand_and(|s| {
				// Allow strings to be coerced to tables
				if val.is_empty() {
					true
				} else {
					let s = TableName::from(s.clone());
					val.contains(&s)
				}
			})
	}

	fn can_coerce_to_record(&self, val: &[TableName]) -> bool {
		self.is_record_and(|t| val.is_empty() || val.contains(&t.table))
	}

	fn can_coerce_to_geometry(&self, val: &[GeometryKind]) -> bool {
		if let Value::Object(x) = self {
			Geometry::try_from_object(x).is_some_and(|g| val.contains(&g.geometry_kind()))
		} else {
			self.is_geometry_type(val)
		}
	}

	fn can_coerce_to_literal(&self, val: &KindLiteral) -> bool {
		if matches!(val, KindLiteral::Array(_) | KindLiteral::Object(_)) {
			// The members are converted, so whether they can be is what the
			// conversion does: a member kind may admit a value by length or
			// shape that its conversion then refuses (a set deduplicating
			// below its declared length).
			self.clone().coerce_to_literal(val).is_ok()
		} else {
			val.validate_value(self)
		}
	}

	fn can_coerce_to_file_buckets(&self, buckets: &[String]) -> bool {
		matches!(self, Value::File(f) if f.is_bucket_type(buckets))
	}

	/// Convert the value using coercion rules.
	///
	/// Coercion rules are more strict then coverting rules.
	/// Calling this method will succeed if the value can by unified with the
	/// kind of the target
	///
	/// This method is a shorthand for `T::coerce(self)`
	pub fn coerce_to<T: Coerce>(self) -> Result<T, CoerceError> {
		T::coerce(self)
	}

	/// Coerce the value as [`Self::coerce_to`] does, naming `into` rather
	/// than the kind of `T` when the value is of the wrong kind: a clause that
	/// wanted the value is more use to the reader than the kind it wanted.
	pub fn coerce_naming<T: Coerce>(self, into: &str) -> Result<T, CoerceError> {
		match T::coerce(self) {
			Err(CoerceError::InvalidKind {
				from,
				..
			}) => Err(CoerceError::InvalidKind {
				from,
				into: into.to_string(),
			}),
			x => x,
		}
	}

	/// Try to coerce this value to the specified `Kind`
	pub fn coerce_to_kind(self, kind: &Kind) -> Result<Value, CoerceError> {
		// Attempt to convert to the desired type
		match kind {
			Kind::Any => Ok(self),
			Kind::None => self.coerce_to::<SqlNone>().map(|_| Value::None),
			Kind::Null => self.coerce_to::<Null>().map(Value::from),
			Kind::Bool => self.coerce_to::<bool>().map(Value::from),
			Kind::Int => self.coerce_to::<i64>().map(Value::from),
			Kind::Float => self.coerce_to::<f64>().map(Value::from),
			Kind::Decimal => self.coerce_to::<Decimal>().map(Value::from),
			Kind::BigDecimal => self
				.coerce_to::<BigDecimal>()
				.map(|b| Value::Number(Number::BigDecimal(Box::new(b)))),
			Kind::Number => self.coerce_to::<Number>().map(Value::from),
			Kind::I8 => self.coerce_to::<i8>().map(|v| Value::Number(Number::I8(v))),
			Kind::U8 => self.coerce_to::<u8>().map(|v| Value::Number(Number::U8(v))),
			Kind::I16 => self.coerce_to::<i16>().map(|v| Value::Number(Number::I16(v))),
			Kind::U16 => self.coerce_to::<u16>().map(|v| Value::Number(Number::U16(v))),
			Kind::I32 => self.coerce_to::<i32>().map(|v| Value::Number(Number::I32(v))),
			Kind::U32 => self.coerce_to::<u32>().map(|v| Value::Number(Number::U32(v))),
			Kind::U64 => self.coerce_to::<u64>().map(|v| Value::Number(Number::U64(v))),
			Kind::F16 => self.coerce_to::<half::f16>().map(|v| Value::Number(Number::F16(v))),
			Kind::F32 => self.coerce_to::<f32>().map(|v| Value::Number(Number::F32(v))),
			Kind::String => self.coerce_to::<String>().map(Value::from),
			Kind::Datetime => self.coerce_to::<Datetime>().map(Value::from),
			Kind::Date => self.coerce_to::<Date>().map(Value::from),
			Kind::Time => self.coerce_to::<Time>().map(Value::from),
			Kind::Timestamp => self.coerce_to::<Timestamp>().map(Value::from),
			Kind::Interval => self.coerce_to::<Interval>().map(Value::from),
			Kind::Duration => self.coerce_to::<Duration>().map(Value::from),
			Kind::Object => self.coerce_to::<crate::val::Object>().map(Value::from),
			Kind::Bytes => self.coerce_to::<Bytes>().map(Value::from),
			Kind::Uuid => self.coerce_to::<Uuid>().map(Value::from),
			Kind::Regex => self.coerce_to::<Regex>().map(Value::from),
			Kind::Range => self.coerce_to::<Box<Range>>().map(Value::from),
			Kind::Function(_, _) => self.coerce_to::<Box<Closure>>().map(Value::from),
			Kind::Set(t, l) => match l {
				Some(l) => self.coerce_to_set_kind_len(t, *l).map(Value::from),
				None => self.coerce_to_set_kind(t).map(Value::from),
			},
			Kind::Array(t, l) => match l {
				Some(l) => self.coerce_to_array_type_len(t, *l).map(Value::from),
				None => self.coerce_to_array_type(t).map(Value::from),
			},
			Kind::Table(t) => {
				if t.is_empty() {
					self.coerce_to::<String>()
						.map(|s| Value::Table(surrealdb_strand::TableName::new(s)))
				} else {
					self.coerce_to_table_kind(t).map(Value::from)
				}
			}
			Kind::Record(t) => {
				if t.is_empty() {
					self.coerce_to::<RecordId>().map(Value::from)
				} else {
					self.coerce_to_record_kind(t).map(Value::from)
				}
			}
			Kind::Geometry(t) => {
				if t.is_empty() {
					self.coerce_to::<Geometry>().map(Value::from)
				} else {
					self.coerce_to_geometry_kind(t).map(Value::from)
				}
			}
			Kind::Either(k) => {
				// Check first for valid kind, then convert to not consume the value
				let Some(k) = self.union_member(k, Value::can_coerce_to_kind) else {
					return Err(CoerceError::InvalidKind {
						from: self,
						into: kind.to_sql(),
					});
				};

				Ok(self.coerce_to_kind(k).expect(
					"If can_coerce_to_kind returns true then coerce_to_kind must not error",
				))
			}
			Kind::Literal(lit) => self.coerce_to_literal(lit),
			Kind::File(buckets) => {
				if buckets.is_empty() {
					self.coerce_to::<File>().map(Value::from)
				} else {
					self.coerce_to_file_buckets(buckets).map(Value::from)
				}
			}
			// A named type's body is judged where the catalog is reachable.
			Kind::Named(_) => Ok(self),
			Kind::Enum(labels) => {
				if self.is_enum_member(labels) {
					Ok(self)
				} else {
					Err(CoerceError::InvalidKind {
						from: self,
						into: kind.to_sql(),
					})
				}
			}
		}
	}

	/// Try to coerce this value to a Literal, returns a `Value` with the
	/// coerced value
	///
	/// Array and object literals convert their members to the member kinds,
	/// as `array<T>` converts its elements, so `[1.0]` coerced to `[int]` is
	/// `[1]`. `validate_value` admits exactly the values whose members all
	/// coerce, so the conversion below cannot fail once it has passed.
	pub fn coerce_to_literal(self, literal: &KindLiteral) -> Result<Value, CoerceError> {
		if !literal.validate_value(&self) {
			return Err(CoerceError::InvalidKind {
				from: self,
				into: literal.to_sql(),
			});
		}
		match (literal, self) {
			(KindLiteral::Array(kinds), Value::Array(array)) => array
				.into_iter()
				.zip(kinds)
				.map(|(value, kind)| value.coerce_to_kind(kind))
				.collect::<Result<Array, CoerceError>>()
				.map(Value::Array),
			(KindLiteral::Object(kinds), Value::Object(mut object)) => {
				for (key, value) in object.iter_mut() {
					if let Some(kind) = kinds.get(key) {
						let taken = std::mem::replace(value, Value::None);
						*value = taken.coerce_to_kind(kind)?;
					}
				}
				Ok(Value::Object(object))
			}
			(_, value) => Ok(value),
		}
	}

	/// Try to coerce this value to a Table of a certain type
	pub fn coerce_to_table_kind(
		self,
		val: &[TableName],
	) -> Result<surrealdb_strand::TableName, CoerceError> {
		// Tables are allowed if correct type
		let this = match self.try_into_table() {
			Ok(v) if val.is_empty() || val.contains(&v) => return Ok(v),
			Ok(v) => Value::Table(v),
			Err(v) => v,
		};
		// Allow strings to be coerced to tables
		let this = match this.try_into_strand() {
			Ok(s) => {
				if val.is_empty() {
					return Ok(surrealdb_strand::TableName::new(s));
				}

				let t = TableName::from(s);
				if val.contains(&t) {
					return Ok(t);
				}

				Value::String(t.into())
			}
			Err(v) => v,
		};

		let mut kind = "table<".to_string();
		for (idx, t) in val.iter().enumerate() {
			if idx != 0 {
				kind.push('|');
			}
			kind.push_str(t.as_str())
		}
		kind.push('>');
		Err(CoerceError::InvalidKind {
			from: this,
			into: kind,
		})
	}

	/// Try to coerce this value to a Record of a certain type
	pub fn coerce_to_record_kind(self, val: &[TableName]) -> Result<RecordId, CoerceError> {
		// Records are allowed if correct type
		let this = match self.try_into_record() {
			Ok(v) if val.is_empty() || val.contains(&v.table) => return Ok(v),
			Ok(v) => Value::RecordId(v),
			Err(v) => v,
		};

		let mut kind = "record<".to_string();
		for (idx, t) in val.iter().enumerate() {
			if idx != 0 {
				kind.push('|');
			}
			kind.push_str(t.as_str())
		}
		kind.push('>');
		Err(CoerceError::InvalidKind {
			from: this,
			into: kind,
		})
	}

	/// Try to coerce this value to a `Geometry` of a certain type
	pub fn coerce_to_geometry_kind(self, val: &[GeometryKind]) -> Result<Geometry, CoerceError> {
		let this = match self.try_into_geometry() {
			Ok(x) if val.contains(&x.geometry_kind()) => return Ok(x),
			Ok(x) => Value::Geometry(x),
			Err(v) => v,
		};
		if let Value::Object(ref x) = this
			&& let Some(x) = Geometry::try_from_object(x)
			&& val.contains(&x.geometry_kind())
		{
			return Ok(x);
		}
		Err(CoerceError::InvalidKind {
			from: this,
			into: "geometry".into(),
		})
	}

	/// Try to coerce this value to an `Array` of a certain type
	pub fn coerce_to_array_type(self, kind: &Kind) -> Result<Array, CoerceError> {
		self.coerce_to::<Array>()?
			.into_iter()
			.enumerate()
			.map(|(i, value)| {
				value
					.coerce_to_kind(kind)
					.with_element_of_at_index(i, || format!("array<{}>", kind.to_sql()))
			})
			.collect::<Result<Array, CoerceError>>()
	}

	/// Try to coerce this value to an `Array` of a certain type, and length
	pub fn coerce_to_array_type_len(self, kind: &Kind, len: u64) -> Result<Array, CoerceError> {
		let array = self.coerce_to::<Array>()?;

		if array.len() as u64 != len {
			return Err(CoerceError::InvalidLength {
				len: array.len(),
				into: format!("array<{},{}>", kind.to_sql(), len),
			});
		}

		array
			.into_iter()
			.enumerate()
			.map(|(i, value)| {
				value
					.coerce_to_kind(kind)
					.with_element_of_at_index(i, || format!("array<{}>", kind.to_sql()))
			})
			.collect::<Result<Array, CoerceError>>()
	}

	/// Try to coerce this value to a `Set` of a certain type
	pub fn coerce_to_set_kind(self, kind: &Kind) -> Result<Set, CoerceError> {
		self.coerce_to::<Set>()?
			.into_iter()
			.enumerate()
			.map(|(i, value)| {
				value
					.coerce_to_kind(kind)
					.with_element_of_at_index(i, || format!("set<{}>", kind.to_sql()))
			})
			.collect::<Result<Set, CoerceError>>()
	}

	/// Try to coerce this value to a `Set` of a certain type and length
	pub fn coerce_to_set_kind_len(self, kind: &Kind, len: u64) -> Result<Set, CoerceError> {
		let set = self
			.coerce_to::<Set>()?
			.into_iter()
			.enumerate()
			.map(|(i, value)| {
				value
					.coerce_to_kind(kind)
					.with_element_of_at_index(i, || format!("set<{}>", kind.to_sql()))
			})
			.collect::<Result<Set, CoerceError>>()?;

		if set.len() as u64 != len {
			return Err(CoerceError::InvalidLength {
				into: format!("set<{},{}>", kind.to_sql(), len),
				len: set.len(),
			});
		}

		Ok(set)
	}

	pub fn coerce_to_file_buckets(self, buckets: &[String]) -> Result<File, CoerceError> {
		let v = self.coerce_to::<File>()?;

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
		Err(CoerceError::InvalidKind {
			from: v.into(),
			into: kind,
		})
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::Strand;

	use super::*;

	#[test]
	fn test_coerce_array_element_position_in_error() {
		// An array where the second element (index 1) is the wrong type.
		let value = Value::Array(Array(vec![
			Value::Number(1.into()),
			Value::String(Strand::new_static("bad")),
			Value::Number(3.into()),
		]));
		let kind = Kind::Int;
		let err = value.coerce_to_array_type(&kind).unwrap_err();
		let msg = err.to_string();
		assert!(msg.contains("index 1"), "error message should mention index 1, got: {msg}");
	}

	#[test]
	fn test_coerce_object_key_position_in_error() {
		use std::collections::BTreeMap;

		let mut map = BTreeMap::new();
		map.insert(surrealdb_strand::Strand::new_static("valid"), Value::Number(1.into()));
		map.insert(
			surrealdb_strand::Strand::new_static("bad_field"),
			Value::String(Strand::new_static("not_an_int")),
		);
		let value = Value::Object(Object(map.into()));

		let err = value.coerce_to::<BTreeMap<String, i64>>().unwrap_err();
		let msg = err.to_string();
		assert!(
			msg.contains("bad_field"),
			"error message should mention the key 'bad_field', got: {msg}"
		);
	}

	#[test]
	fn test_coerce_vec_element_position_in_error() {
		let value = Value::Array(Array(vec![
			Value::Number(1.into()),
			Value::Number(2.into()),
			Value::String(Strand::new_static("oops")),
		]));

		let err = value.coerce_to::<Vec<i64>>().unwrap_err();
		let msg = err.to_string();
		assert!(msg.contains("index 2"), "error message should mention index 2, got: {msg}");
	}

	#[test]
	fn test_redacted_coerce_error_drops_value_and_position() {
		let value = Value::Array(Array(vec![
			Value::Number(1.into()),
			Value::Number(2.into()),
			Value::String(Strand::new_static("secret")),
		]));
		let err = value.coerce_to_array_type(&Kind::Int).unwrap_err();
		assert!(err.to_string().contains("index 2"));

		let msg = err.redact().to_string();
		assert!(!msg.contains("secret"), "redacted error shows the value: {msg}");
		assert!(!msg.contains("index"), "redacted error shows the position: {msg}");
		assert!(msg.contains("[REDACTED]"), "redacted error should say so: {msg}");
	}

	#[test]
	fn test_coerce_to_table_generic() {
		// Test coercing string to generic table type
		let value = Value::String(Strand::new_static("users"));
		let kind = Kind::Table(vec![]);
		let result = value.coerce_to_kind(&kind);
		assert!(result.is_ok());
		if let Ok(Value::Table(table)) = result {
			assert_eq!(table.as_str(), "users");
		}
	}

	#[test]
	fn test_coerce_to_table_specific() {
		// Coercion should fail for wrong table name (more strict than cast)
		let value = Value::String(Strand::new_static("posts"));
		let kind = Kind::Table(vec!["users".into()]);
		let result = value.coerce_to_kind(&kind);
		// Coercion from string to specific table type should fail because
		// coercion is stricter and only allows exact matches
		assert!(result.is_err());
	}

	#[test]
	fn test_coerce_table_to_table() {
		// Test coercing table value to matching table type
		let value = Value::Table("users".into());
		let kind = Kind::Table(vec!["users".into()]);
		let result = value.coerce_to_kind(&kind);
		assert!(result.is_ok());

		// Test coercing table value to non-matching table type
		let value = Value::Table("posts".into());
		let kind = Kind::Table(vec!["users".into()]);
		let result = value.coerce_to_kind(&kind);
		assert!(result.is_err());
	}

	#[test]
	fn test_can_coerce_to_table() {
		// Test can_coerce_to_kind for tables
		let value = Value::Table("users".into());
		let kind = Kind::Table(vec!["users".into()]);
		assert!(value.can_coerce_to_kind(&kind));

		// Wrong table name
		let value = Value::Table("posts".into());
		let kind = Kind::Table(vec!["users".into()]);
		assert!(!value.can_coerce_to_kind(&kind));

		// Wrong type
		let value = Value::Number(42.into());
		let kind = Kind::Table(vec![]);
		assert!(!value.can_coerce_to_kind(&kind));
	}

	#[test]
	fn test_coerce_table_empty_tables_list() {
		// Test with empty tables list (should accept any table)
		let value = Value::Table("anything".into());
		let kind = Kind::Table(vec![]);
		let result = value.coerce_to_kind(&kind);
		assert!(result.is_err()); // Coercion from string is strict
	}

	fn float(f: f64) -> Value {
		Value::Number(Number::Float(f))
	}

	fn decimal(mantissa: i128, scale: u32) -> Value {
		Value::Number(Number::Decimal(Decimal::from_i128_with_scale(mantissa, scale)))
	}

	#[test]
	fn test_coerce_float_to_int_accepts_whole_floats_in_range() {
		assert_eq!(float(5.0).coerce_to::<i64>().unwrap(), 5);
		assert_eq!(float(-5.0).coerce_to::<i64>().unwrap(), -5);
		// Negative zero is the integer zero
		assert_eq!(float(-0.0).coerce_to::<i64>().unwrap(), 0);
		// 2^53: every smaller integer is also an f64
		assert_eq!(
			float(9_007_199_254_740_992.0).coerce_to::<i64>().unwrap(),
			9_007_199_254_740_992
		);
		// -2^63 is `i64::MIN`
		assert_eq!(float(i64::MIN as f64).coerce_to::<i64>().unwrap(), i64::MIN);
		// The largest f64 below 2^63
		assert_eq!(float(2f64.powi(63) - 1024.0).coerce_to::<i64>().unwrap(), i64::MAX - 1023);
	}

	#[test]
	fn test_coerce_float_to_int_refuses_what_is_not_one_i64() {
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
			let err = float(f).coerce_to::<i64>().unwrap_err();
			assert!(
				matches!(&err, CoerceError::InvalidKind { into, .. } if into == "int"),
				"{f} should be refused as an int, got: {err}"
			);
			assert!(!float(f).can_coerce_to::<i64>(), "{f} must not report as coercible to int");
		}
	}

	#[test]
	fn test_coerce_float_out_of_range_refused_like_decimal() {
		// A float and a decimal out of the `i64` range fail the same way
		let float_err = float(1e30).coerce_to::<i64>().unwrap_err();
		let decimal_err = decimal(10_000_000_000_000_000_000, 0).coerce_to::<i64>().unwrap_err();
		assert!(matches!(float_err, CoerceError::InvalidKind { ref into, .. } if into == "int"));
		assert!(matches!(decimal_err, CoerceError::InvalidKind { ref into, .. } if into == "int"));
	}

	/// Numbers of every shape that matter to an `int`: the bounds, the first
	/// value past each bound, fractions, and the non-finite floats.
	fn number_battery() -> Vec<Value> {
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
			// Fractional and out of range at once
			decimal(100_000_000_000_000_000_005, 1),
			Value::Number(Number::Decimal(Decimal::MAX)),
			Value::Number(Number::Decimal(Decimal::MIN)),
		];
		for f in [
			0.0,
			-0.0,
			5.0,
			0.5,
			-0.5,
			1e30,
			-1e30,
			i64::MAX as f64,
			i64::MIN as f64,
			f64::MAX,
			f64::MIN,
			f64::MIN_POSITIVE,
			f64::NAN,
			f64::INFINITY,
			f64::NEG_INFINITY,
		] {
			out.push(float(f));
		}
		out
	}

	#[test]
	fn test_can_coerce_agrees_with_coerce_for_numbers() {
		// A union kind coerces to the first member `can_coerce_to_kind`
		// accepts and treats a failure there as unreachable, so the two must
		// never disagree.
		for value in number_battery() {
			for kind in Kind::NUMERIC {
				assert_eq!(
					value.can_coerce_to_kind(&kind),
					value.clone().coerce_to_kind(&kind).is_ok(),
					"can_coerce and coerce disagree for {value:?} into {}",
					kind.to_sql()
				);
			}
		}
	}

	#[test]
	fn test_coerce_union_with_int_never_claims_a_value_it_cannot_coerce() {
		let fractional = decimal(15, 1);
		let huge = float(1e30);
		let unions = [
			Kind::Either(vec![Kind::Int, Kind::String]),
			Kind::Either(vec![Kind::None, Kind::Int]),
			Kind::Array(Box::new(Kind::Either(vec![Kind::Int, Kind::String])), None),
		];
		for kind in &unions {
			let wrap = |v: Value| {
				if matches!(kind, Kind::Array(..)) {
					Value::Array(Array(vec![v]))
				} else {
					v
				}
			};
			assert!(wrap(fractional.clone()).coerce_to_kind(kind).is_err());
			assert!(wrap(huge.clone()).coerce_to_kind(kind).is_err());
		}
		// An alternative that can hold the value is still reached
		let either = Kind::Either(vec![Kind::Int, Kind::Float]);
		assert!(matches!(
			huge.coerce_to_kind(&either).unwrap(),
			Value::Number(Number::Float(f)) if f == 1e30
		));
		assert!(matches!(
			fractional.coerce_to_kind(&either).unwrap(),
			Value::Number(Number::Float(f)) if f == 1.5
		));
		// When the first member can hold the value, it still wins
		assert!(matches!(
			float(5.0).coerce_to_kind(&either).unwrap(),
			Value::Number(Number::Int(5))
		));
		assert!(matches!(
			decimal(5, 0).coerce_to_kind(&either).unwrap(),
			Value::Number(Number::Int(5))
		));
		// A sized number whose exact kind is in the union keeps its width
		let sized_either = Kind::Either(vec![Kind::Int, Kind::I8]);
		assert!(matches!(
			Value::Number(Number::I8(5)).coerce_to_kind(&sized_either).unwrap(),
			Value::Number(Number::I8(5))
		));
	}

	#[test]
	fn test_union_member_prefers_the_temporal_kind_a_value_already_is() {
		use crate::val::{Date, Datetime, Timestamp};
		let date = Value::Date(Date(19_723));
		let datetime = Value::Datetime(Datetime::from(Date(19_723)));
		let timestamp = Value::Timestamp(Timestamp::from(Datetime::from(Date(19_723))));
		let temporal = [Kind::Datetime, Kind::Date, Kind::Timestamp];
		for (value, own) in
			[(&date, Kind::Date), (&datetime, Kind::Datetime), (&timestamp, Kind::Timestamp)]
		{
			for kind in temporal.iter().filter(|kind| value.can_coerce_to_kind(kind)) {
				assert_eq!(value.converts_to(kind), *kind != own, "{value:?} as {kind:?}");
			}
			assert_eq!(value.union_member(&temporal, Value::can_coerce_to_kind), Some(&own));
		}
		let either = Kind::Either(vec![Kind::Datetime, Kind::Date]);
		assert!(matches!(date.coerce_to_kind(&either).unwrap(), Value::Date(_)));
	}
}
