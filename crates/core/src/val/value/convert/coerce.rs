use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::hash::BuildHasher;

use geo::Point;
use rust_decimal::Decimal;

use crate::expr::Kind;
use crate::expr::kind::{GeometryKind, HasKind, KindLiteral};
use crate::val::array::Uniq;
use crate::val::{
	Array, Bytes, Closure, Datetime, Duration, File, Geometry, Null, Number, Object, Range,
	RecordId, Regex, Strand, Uuid, Value,
};

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
	},
}
impl std::error::Error for CoerceError {}
impl fmt::Display for CoerceError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			CoerceError::InvalidKind {
				from,
				into,
			} => {
				write!(f, "Expected `{into}` but found `{from}`")
			}
			CoerceError::ElementOf {
				inner,
				into,
			} => {
				inner.fmt(f)?;
				write!(f, " when coercing an element of `{into}`")
			}
			CoerceError::InvalidLength {
				len,
				into,
			} => {
				write!(f, "Expected `{into}` but found an collection of length `{len}`")
			}
		}
	}
}

pub trait CoerceErrorExt {
	fn with_element_of<F>(self, f: F) -> Self
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

impl Coerce for Null {
	fn can_coerce(v: &Value) -> bool {
		matches!(v, Value::Null)
	}

	fn coerce(v: Value) -> Result<Null, CoerceError> {
		match v {
			// Allow any null value
			Value::Null => Ok(Null),
			// Anything else raises an error
			_ => Err(CoerceError::InvalidKind {
				from: v,
				into: "null".into(),
			}),
		}
	}
}

impl Coerce for i64 {
	fn can_coerce(v: &Value) -> bool {
		let Value::Number(n) = v else {
			return false;
		};
		match n {
			Number::Int(_) => true,
			Number::Float(f) => f.fract() == 0.0,
			Number::Decimal(d) => i64::try_from(*d).is_ok(),
		}
	}

	fn coerce(val: Value) -> Result<Self, CoerceError> {
		match val {
			// Allow any int number
			Value::Number(Number::Int(v)) => Ok(v),
			// Attempt to convert an float number
			Value::Number(Number::Float(v)) if v.fract() == 0.0 => Ok(v as i64),
			// Attempt to convert a decimal number
			Value::Number(Number::Decimal(v)) if v.is_integer() => match v.try_into() {
				// The Decimal can be represented as an i64
				Ok(v) => Ok(v),
				// The Decimal is out of bounds
				_ => Err(CoerceError::InvalidKind {
					from: val,
					into: "int".into(),
				}),
			},
			// Anything else raises an error
			_ => Err(CoerceError::InvalidKind {
				from: val,
				into: "int".into(),
			}),
		}
	}
}

impl Coerce for f64 {
	fn can_coerce(v: &Value) -> bool {
		let Value::Number(n) = v else {
			return false;
		};
		match n {
			Number::Int(_) | Number::Float(_) => true,
			Number::Decimal(d) => f64::try_from(*d).is_ok(),
		}
	}

	/// Try to coerce this value to an `f64`
	fn coerce(val: Value) -> Result<f64, CoerceError> {
		match val {
			// Allow any float number
			Value::Number(Number::Float(v)) => Ok(v),
			// Attempt to convert an int number
			Value::Number(Number::Int(v)) => Ok(v as f64),
			// Attempt to convert a decimal number
			Value::Number(Number::Decimal(v)) => match v.try_into() {
				// The Decimal can be represented as a f64
				Ok(v) => Ok(v),
				// This Decimal loses precision
				_ => Err(CoerceError::InvalidKind {
					from: val,
					into: "float".into(),
				}),
			},
			// Anything else raises an error
			_ => Err(CoerceError::InvalidKind {
				from: val,
				into: "float".into(),
			}),
		}
	}
}

impl Coerce for Decimal {
	fn can_coerce(v: &Value) -> bool {
		let Value::Number(n) = v else {
			return false;
		};
		match n {
			Number::Int(_) | Number::Decimal(_) => true,
			Number::Float(f) => Decimal::try_from(*f).is_ok(),
		}
	}

	fn coerce(val: Value) -> Result<Self, CoerceError> {
		match val {
			// Allow any decimal number
			Value::Number(Number::Decimal(x)) => Ok(x),
			// Attempt to convert an int number
			Value::Number(Number::Int(v)) => Ok(Decimal::from(v)),
			// Attempt to convert an float number
			Value::Number(Number::Float(v)) => match Decimal::try_from(v).ok() {
				// The Float can be represented as a Decimal
				Some(v) => Ok(v),
				// This Float does not convert to a Decimal
				None => Err(CoerceError::InvalidKind {
					from: val,
					into: "decimal".into(),
				}),
			},
			// Anything else raises an error
			_ => Err(CoerceError::InvalidKind {
				from: val,
				into: "decimal".into(),
			}),
		}
	}
}

impl Coerce for String {
	fn can_coerce(v: &Value) -> bool {
		Strand::can_coerce(v)
	}

	fn coerce(v: Value) -> Result<Self, CoerceError> {
		Strand::coerce(v).map(|x| x.into_string())
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
				into: <Self as HasKind>::kind().to_string(),
			});
		}
		// Unwrap checked above
		let array = v.into_array().unwrap();

		let mut res = Vec::with_capacity(array.0.len());
		for x in array.0 {
			// TODO: Improve error message here.
			res.push(x.coerce_to::<T>().with_element_of(|| <Self as HasKind>::kind().to_string())?)
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
				into: Object::kind().to_string(),
			});
		};
		// Unwrap checked above
		let obj = v.into_object().unwrap();

		let mut res = BTreeMap::new();
		for (k, v) in obj.0 {
			// TODO: Improve error message here.
			// object<T> kinds don't actually exist in surql.
			res.insert(
				k,
				v.coerce_to::<T>()
					.with_element_of(|| format!("object<{}>", <T as HasKind>::kind()))?,
			);
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
				into: Kind::of::<Object>().to_string(),
			});
		};
		// Unwrap checked above
		let obj = v.into_object().unwrap();

		let mut res = HashMap::default();
		for (k, v) in obj.0 {
			// TODO: Improve error message here.
			// object<T> kinds don't actually exist in surql.
			res.insert(
				k,
				v.coerce_to::<T>()
					.with_element_of(|| format!("object<{}>", <T as HasKind>::kind()))?,
			);
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
		Kind::of::<$kind>().to_string()
	};

	(@kindof $inner:ty) => {
		Kind::of::<$inner>().to_string()
	};
}

// Types which directly match one enum variant or fail
impl_direct! {
	Bool => bool,
	Number => Number,
	Uuid => Uuid,
	Closure => Box<Closure> = Closure,
	Range => Box<Range> = Range,
	Datetime => Datetime,
	Duration => Duration,
	Bytes => Bytes,
	Object => Object,
	Array => Array,
	RecordId => RecordId,
	Strand => Strand,
	Geometry => Geometry,
	Regex => Regex,
}

// Coerce to runtime value implementations
impl Value {
	pub fn can_coerce_to<T: Coerce>(&self) -> bool {
		T::can_coerce(self)
	}

	pub fn can_coerce_to_kind(&self, kind: &Kind) -> bool {
		match kind {
			Kind::Any => true,
			Kind::Null => self.can_coerce_to::<Null>(),
			Kind::Bool => self.can_coerce_to::<bool>(),
			Kind::Int => self.can_coerce_to::<i64>(),
			Kind::Float => self.can_coerce_to::<f64>(),
			Kind::Decimal => self.can_coerce_to::<Decimal>(),
			Kind::Number => self.can_coerce_to::<Number>(),
			Kind::String => self.can_coerce_to::<Strand>(),
			Kind::Datetime => self.can_coerce_to::<Datetime>(),
			Kind::Duration => self.can_coerce_to::<Duration>(),
			Kind::Object => self.can_coerce_to::<Object>(),
			Kind::Bytes => self.can_coerce_to::<Bytes>(),
			Kind::Uuid => self.can_coerce_to::<Uuid>(),
			Kind::Regex => self.can_coerce_to::<Regex>(),
			Kind::Range => self.can_coerce_to::<Box<Range>>(),
			Kind::Function(_, _) => self.can_coerce_to::<Box<Closure>>(),
			Kind::Set(t, l) => match l {
				Some(l) => self.can_coerce_to_array_len(t, *l),
				None => self.can_coerce_to_array(t),
			},
			Kind::Array(t, l) => match l {
				Some(l) => self.can_coerce_to_array_len(t, *l),
				None => self.can_coerce_to_array(t),
			},
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
			Kind::Option(k) => match self {
				Self::None => true,
				v => v.can_coerce_to_kind(k),
			},
			Kind::Either(k) => k.iter().any(|x| self.can_coerce_to_kind(x)),
			Kind::Literal(lit) => self.can_coerce_to_literal(lit),
			Kind::File(buckets) => {
				if buckets.is_empty() {
					self.can_coerce_to::<File>()
				} else {
					self.can_coerce_to_file_buckets(buckets)
				}
			}
		}
	}

	fn can_coerce_to_array_len(&self, kind: &Kind, len: u64) -> bool {
		match self {
			Value::Array(a) => {
				a.len() as u64 == len && a.iter().all(|x| x.can_coerce_to_kind(kind))
			}
			_ => false,
		}
	}

	fn can_coerce_to_array(&self, kind: &Kind) -> bool {
		match self {
			Value::Array(a) => a.iter().all(|x| x.can_coerce_to_kind(kind)),
			_ => false,
		}
	}

	fn can_coerce_to_record(&self, val: &[String]) -> bool {
		match self {
			Value::RecordId(t) => val.is_empty() || val.contains(&t.table),
			_ => false,
		}
	}

	fn can_coerce_to_geometry(&self, val: &[GeometryKind]) -> bool {
		self.is_geometry_type(val)
	}

	fn can_coerce_to_literal(&self, val: &KindLiteral) -> bool {
		val.validate_value(self)
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

	/// Try to coerce this value to the specified `Kind`
	pub(crate) fn coerce_to_kind(self, kind: &Kind) -> Result<Value, CoerceError> {
		// Attempt to convert to the desired type
		match kind {
			Kind::Any => Ok(self),
			Kind::Null => self.coerce_to::<Null>().map(Value::from),
			Kind::Bool => self.coerce_to::<bool>().map(Value::from),
			Kind::Int => self.coerce_to::<i64>().map(Value::from),
			Kind::Float => self.coerce_to::<f64>().map(Value::from),
			Kind::Decimal => self.coerce_to::<Decimal>().map(Value::from),
			Kind::Number => self.coerce_to::<Number>().map(Value::from),
			Kind::String => self.coerce_to::<Strand>().map(Value::from),
			Kind::Datetime => self.coerce_to::<Datetime>().map(Value::from),
			Kind::Duration => self.coerce_to::<Duration>().map(Value::from),
			Kind::Object => self.coerce_to::<Object>().map(Value::from),
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
			Kind::Option(k) => match self {
				Self::None => Ok(Self::None),
				v => v.coerce_to_kind(k),
			},
			Kind::Either(k) => {
				// Check first for valid kind, then convert to not consume the value
				let Some(k) = k.iter().find(|x| self.can_coerce_to_kind(x)) else {
					return Err(CoerceError::InvalidKind {
						from: self,
						into: kind.to_string(),
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
		}
	}

	/// Try to coerce this value to a Literal, returns a `Value` with the
	/// coerced value
	pub(crate) fn coerce_to_literal(self, literal: &KindLiteral) -> Result<Value, CoerceError> {
		if literal.validate_value(&self) {
			Ok(self)
		} else {
			Err(CoerceError::InvalidKind {
				from: self,
				into: literal.to_string(),
			})
		}
	}

	/// Try to coerce this value to a Record of a certain type
	pub(crate) fn coerce_to_record_kind(self, val: &[String]) -> Result<RecordId, CoerceError> {
		let this = match self {
			// Records are allowed if correct type
			Value::RecordId(v) => {
				if val.is_empty() || val.contains(&v.table) {
					return Ok(v);
				} else {
					Value::RecordId(v)
				}
			}
			x => x,
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
	pub(crate) fn coerce_to_geometry_kind(
		self,
		val: &[GeometryKind],
	) -> Result<Geometry, CoerceError> {
		if self.is_geometry_type(val) {
			let Value::Geometry(x) = self else {
				// Checked above in is_geometry_type
				unreachable!()
			};
			Ok(x)
		} else {
			Err(CoerceError::InvalidKind {
				from: self,
				into: "geometry".into(),
			})
		}
	}

	/// Try to coerce this value to an `Array` of a certain type
	pub(crate) fn coerce_to_array_type(self, kind: &Kind) -> Result<Array, CoerceError> {
		self.coerce_to::<Array>()?
			.into_iter()
			.map(|value| value.coerce_to_kind(kind))
			.collect::<Result<Array, CoerceError>>()
			.with_element_of(|| format!("array<{kind}>"))
	}

	/// Try to coerce this value to an `Array` of a certain type, and length
	pub(crate) fn coerce_to_array_type_len(
		self,
		kind: &Kind,
		len: u64,
	) -> Result<Array, CoerceError> {
		let array = self.coerce_to::<Array>()?;

		if array.len() as u64 != len {
			return Err(CoerceError::InvalidLength {
				len: array.len(),
				into: format!("array<{kind},{len}>"),
			});
		}

		array
			.into_iter()
			.map(|value| value.coerce_to_kind(kind))
			.collect::<Result<Array, CoerceError>>()
			.with_element_of(|| format!("array<{kind}>"))
	}

	/// Try to coerce this value to an `Array` of a certain type, unique values
	pub(crate) fn coerce_to_set_kind(self, kind: &Kind) -> Result<Array, CoerceError> {
		self.coerce_to::<Array>()?
			.uniq()
			.into_iter()
			.map(|value| value.coerce_to_kind(kind))
			.collect::<Result<Array, CoerceError>>()
			.with_element_of(|| format!("set<{kind}>"))
	}

	/// Try to coerce this value to an `Array` of a certain type, unique values,
	/// and length
	pub(crate) fn coerce_to_set_kind_len(
		self,
		kind: &Kind,
		len: u64,
	) -> Result<Array, CoerceError> {
		let array = self
			.coerce_to::<Array>()?
			.uniq()
			.into_iter()
			.map(|value| value.coerce_to_kind(kind))
			.collect::<Result<Array, CoerceError>>()
			.with_element_of(|| format!("set<{kind}>"))?;

		if array.len() as u64 != len {
			return Err(CoerceError::InvalidLength {
				into: format!("set<{kind}, {len}>"),
				len: array.len(),
			});
		}

		Ok(array)
	}

	pub(crate) fn coerce_to_file_buckets(self, buckets: &[String]) -> Result<File, CoerceError> {
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
