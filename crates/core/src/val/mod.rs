#![allow(clippy::derive_ord_xor_partial_ord)]

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::fmt::{self, Write};
use std::ops::Bound;

use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use geo::Point;
use revision::revisioned;
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};
use storekey::{BorrowDecode, Encode};
use surrealdb_types::{ToSql, write_sql};

use crate::err::Error;
use crate::expr;
use crate::expr::kind::GeometryKind;
use crate::expr::statements::info::InfoStructure;
use crate::fmt::{Pretty, QuoteStr};
use crate::sql::expression::convert_public_value_to_internal;

pub(crate) mod array;
pub(crate) mod bytes;
pub(crate) mod closure;
pub(crate) mod datetime;
pub(crate) mod duration;
pub(crate) mod file;
pub(crate) mod geometry;
pub(crate) mod number;
pub(crate) mod object;
pub(crate) mod range;
pub(crate) mod record;
pub(crate) mod record_id;
pub(crate) mod regex;
pub(crate) mod table;
pub(crate) mod uuid;
pub(crate) mod value;

pub(crate) use self::array::Array;
pub(crate) use self::bytes::Bytes;
pub(crate) use self::closure::Closure;
pub(crate) use self::datetime::Datetime;
pub(crate) use self::duration::Duration;
pub(crate) use self::file::File;
pub(crate) use self::geometry::Geometry;
pub(crate) use self::number::{DecimalExt, Number};
pub(crate) use self::object::Object;
pub(crate) use self::range::Range;
pub(crate) use self::record_id::{RecordId, RecordIdKey, RecordIdKeyRange};
pub(crate) use self::regex::Regex;
pub(crate) use self::table::Table;
pub(crate) use self::uuid::Uuid;
pub(crate) use self::value::{CastError, CoerceError};

/// Marker type for a different serialization format for value which does not encode type
/// information which is not required for indexing.
pub enum IndexFormat {}

/// Marker type for value conversions from Value::None
#[derive(Clone, Copy, Eq, PartialEq, PartialOrd)]
pub struct SqlNone;

/// Marker type for value conversions from Value::Null
#[derive(Clone, Copy, Eq, PartialEq, PartialOrd)]
pub struct Null;

#[revisioned(revision = 1)]
#[derive(
	Clone, Debug, Default, PartialEq, PartialOrd, Serialize, Deserialize, Hash, Encode, BorrowDecode,
)]
#[serde(rename = "$surrealdb::private::Value")]
#[storekey(format = "()")]
#[storekey(format = "IndexFormat")]
pub(crate) enum Value {
	#[default]
	None,
	Null,
	Bool(bool),
	Number(Number),
	String(String),
	Duration(Duration),
	Datetime(Datetime),
	Uuid(Uuid),
	Array(Array),
	Object(Object),
	Geometry(Geometry),
	Bytes(Bytes),
	Table(Table),
	RecordId(RecordId),
	File(File),
	#[serde(skip)]
	Regex(Regex),
	Range(Box<Range>),
	#[serde(skip)]
	Closure(Box<Closure>),
	// Add new variants here
}

impl Eq for Value {}

impl Ord for Value {
	fn cmp(&self, other: &Self) -> Ordering {
		self.partial_cmp(other).unwrap_or(Ordering::Equal)
	}
}

impl Value {
	// -----------------------------------
	// Initial record value
	// -----------------------------------

	/// Create an empty Object Value
	pub fn empty_object() -> Self {
		Value::Object(Object::default())
	}

	// -----------------------------------
	// Simple value detection
	// -----------------------------------

	/// Check if this Value is not NONE
	pub fn is_some(&self) -> bool {
		!matches!(self, Value::None)
	}

	/// Check if this Value is NONE or NULL
	pub fn is_nullish(&self) -> bool {
		matches!(self, Value::None | Value::Null)
	}

	/// Check if this Value is NONE
	pub fn is_empty_array(&self) -> bool {
		if let Value::Array(v) = self {
			v.is_empty()
		} else {
			false
		}
	}

	/// Check if this Value is truthy
	pub fn is_truthy(&self) -> bool {
		match self {
			Value::Bool(v) => *v,
			Value::Uuid(_) => true,
			Value::RecordId(_) => true,
			Value::Geometry(_) => true,
			Value::Datetime(_) => true,
			Value::Bytes(v) => !v.is_empty(),
			Value::Array(v) => !v.is_empty(),
			Value::Object(v) => !v.is_empty(),
			Value::String(v) => !v.is_empty(),
			Value::Number(v) => v.is_truthy(),
			Value::Duration(v) => v.as_nanos() > 0,
			_ => false,
		}
	}

	/// Check if this Value is an int Number
	pub fn is_int(&self) -> bool {
		matches!(self, Value::Number(Number::Int(_)))
	}

	/// Check if this Value is a float Number
	pub fn is_float(&self) -> bool {
		matches!(self, Value::Number(Number::Float(_)))
	}

	/// Check if this Value is a decimal Number
	pub fn is_decimal(&self) -> bool {
		matches!(self, Value::Number(Number::Decimal(_)))
	}

	/// Check if this Value is a RecordId of a specific type
	pub fn is_record_type(&self, types: &[String]) -> bool {
		match self {
			Value::RecordId(v) => v.is_table_type(types),
			_ => false,
		}
	}

	/// Check if this Value is a Geometry of a specific type
	pub fn is_geometry_type(&self, types: &[GeometryKind]) -> bool {
		match self {
			Value::Geometry(Geometry::Point(_)) => {
				types.iter().any(|t| matches!(t, GeometryKind::Point))
			}
			Value::Geometry(Geometry::Line(_)) => {
				types.iter().any(|t| matches!(t, GeometryKind::Line))
			}
			Value::Geometry(Geometry::Polygon(_)) => {
				types.iter().any(|t| matches!(t, GeometryKind::Polygon))
			}
			Value::Geometry(Geometry::MultiPoint(_)) => {
				types.iter().any(|t| matches!(t, GeometryKind::MultiPoint))
			}
			Value::Geometry(Geometry::MultiLine(_)) => {
				types.iter().any(|t| matches!(t, GeometryKind::MultiLine))
			}
			Value::Geometry(Geometry::MultiPolygon(_)) => {
				types.iter().any(|t| matches!(t, GeometryKind::MultiPolygon))
			}
			Value::Geometry(Geometry::Collection(_)) => {
				types.iter().any(|t| matches!(t, GeometryKind::Collection))
			}
			_ => false,
		}
	}

	// -----------------------------------
	// Simple conversion of value
	// -----------------------------------

	/// Converts this Value into an unquoted String
	pub fn into_raw_string(self) -> String {
		match self {
			Value::String(v) => v,
			Value::Uuid(v) => v.to_raw(),
			Value::Datetime(v) => v.to_raw_string(),
			_ => self.to_string(),
		}
	}

	/// Converts this Value into an unquoted String
	pub fn to_raw_string(&self) -> String {
		match self {
			Value::String(v) => v.clone(),
			Value::Uuid(v) => v.to_raw(),
			Value::Datetime(v) => v.to_raw_string(),
			_ => self.to_string(),
		}
	}

	/// Returns the surql representation of the kind of the value as a string.
	///
	/// # Warning
	/// This function is not fully implement for all variants, make sure you
	/// don't accidentally use it where it can return an invalid value.
	pub fn kind_of(&self) -> &'static str {
		match self {
			Self::None => "none",
			Self::Null => "null",
			Self::Bool(_) => "bool",
			Self::Uuid(_) => "uuid",
			Self::Array(_) => "array",
			Self::Object(_) => "object",
			Self::String(_) => "string",
			Self::Duration(_) => "duration",
			Self::Datetime(_) => "datetime",
			Self::Closure(_) => "function",
			Self::Number(Number::Int(_)) => "int",
			Self::Number(Number::Float(_)) => "float",
			Self::Number(Number::Decimal(_)) => "decimal",
			Self::Geometry(Geometry::Point(_)) => "geometry<point>",
			Self::Geometry(Geometry::Line(_)) => "geometry<line>",
			Self::Geometry(Geometry::Polygon(_)) => "geometry<polygon>",
			Self::Geometry(Geometry::MultiPoint(_)) => "geometry<multipoint>",
			Self::Geometry(Geometry::MultiLine(_)) => "geometry<multiline>",
			Self::Geometry(Geometry::MultiPolygon(_)) => "geometry<multipolygon>",
			Self::Geometry(Geometry::Collection(_)) => "geometry<collection>",
			Self::Regex(_) => "regex",
			Self::File(_) => "file",
			Self::Bytes(_) => "bytes",
			Self::Range(_) => "range",
			Self::RecordId(_) => "record",
			Self::Table(_) => "table",
		}
	}

	// -----------------------------------
	// Value operations
	// -----------------------------------

	/// Check if this Value is equal to another Value
	pub fn equal(&self, other: &Value) -> bool {
		match self {
			Value::None => other.is_none(),
			Value::Null => other.is_null(),
			Value::Bool(v) => match other {
				Value::Bool(w) => v == w,
				_ => false,
			},
			Value::Uuid(v) => match other {
				Value::Uuid(w) => v == w,
				_ => false,
			},
			Value::RecordId(v) => match other {
				Value::RecordId(w) => v == w,
				// TODO(3.0.0): Decide if we want to keep this behavior.
				//Value::Regex(w) => w.regex().is_match(v.to_raw().as_str()),
				_ => false,
			},
			Value::String(v) => match other {
				Value::String(w) => v == w,
				Value::Regex(w) => w.inner().is_match(v.as_str()),
				_ => false,
			},
			Value::Regex(v) => match other {
				Value::Regex(w) => v == w,
				// TODO(3.0.0): Decide if we want to keep this behavior.
				//Value::RecordId(w) => v.regex().is_match(w.to_raw().as_str()),
				Value::String(w) => v.inner().is_match(w.as_str()),
				_ => false,
			},
			Value::Array(v) => match other {
				Value::Array(w) => v == w,
				_ => false,
			},
			Value::Object(v) => match other {
				Value::Object(w) => v == w,
				_ => false,
			},
			Value::Number(v) => match other {
				Value::Number(w) => v == w,
				_ => false,
			},
			Value::Geometry(v) => match other {
				Value::Geometry(w) => v == w,
				_ => false,
			},
			Value::Duration(v) => match other {
				Value::Duration(w) => v == w,
				_ => false,
			},
			Value::Datetime(v) => match other {
				Value::Datetime(w) => v == w,
				_ => false,
			},
			_ => self == other,
		}
	}

	/// Check if all Values in an Array are equal to another Value
	pub fn all_equal(&self, other: &Value) -> bool {
		match self {
			Value::Array(v) => v.iter().all(|v| v.equal(other)),
			_ => self.equal(other),
		}
	}

	/// Check if any Values in an Array are equal to another Value
	pub fn any_equal(&self, other: &Value) -> bool {
		match self {
			Value::Array(v) => v.iter().any(|v| v.equal(other)),
			_ => self.equal(other),
		}
	}

	/// Check if this Value contains another Value
	pub fn contains(&self, other: &Value) -> bool {
		match self {
			Value::Array(v) => v.iter().any(|v| v.equal(other)),
			Value::Uuid(v) => match other {
				Value::String(w) => v.to_raw().contains(w.as_str()),
				_ => false,
			},
			Value::String(v) => match other {
				Value::String(w) => v.contains(w.as_str()),
				_ => false,
			},
			Value::Geometry(v) => match other {
				Value::Geometry(w) => v.contains(w),
				_ => false,
			},
			Value::Object(v) => match other {
				Value::String(w) => v.0.contains_key(&**w),
				_ => false,
			},
			Value::Range(r) => {
				let beg = match &r.start {
					Bound::Unbounded => true,
					Bound::Included(beg) => beg.le(other),
					Bound::Excluded(beg) => beg.lt(other),
				};

				beg && match &r.end {
					Bound::Unbounded => true,
					Bound::Included(end) => end.ge(other),
					Bound::Excluded(end) => end.gt(other),
				}
			}
			_ => false,
		}
	}

	/// Check if all Values in an Array contain another Value
	pub fn contains_all(&self, other: &Value) -> bool {
		match other {
			Value::Array(v) if v.iter().all(|v| v.is_strand()) && self.is_strand() => {
				// confirmed as strand so all return false is unreachable
				let Value::String(this) = self else {
					return false;
				};
				v.iter().all(|s| {
					let Value::String(other_string) = s else {
						return false;
					};
					this.contains(&**other_string)
				})
			}
			Value::Array(v) => v.iter().all(|v| match self {
				Value::Array(w) => w.iter().any(|w| v.equal(w)),
				Value::Geometry(_) => self.contains(v),
				_ => false,
			}),
			Value::String(other_strand) => match self {
				Value::String(s) => s.contains(&**other_strand),
				_ => false,
			},
			_ => false,
		}
	}

	/// Check if any Values in an Array contain another Value
	pub fn contains_any(&self, other: &Value) -> bool {
		match other {
			Value::Array(v) if v.iter().all(|v| v.is_strand()) && self.is_strand() => {
				// confirmed as strand so all return false is unreachable
				let Value::String(this) = self else {
					return false;
				};
				v.iter().any(|s| {
					let Value::String(other_string) = s else {
						return false;
					};
					this.contains(&**other_string)
				})
			}
			Value::Array(v) => v.iter().any(|v| match self {
				Value::Array(w) => w.iter().any(|w| v.equal(w)),
				Value::Geometry(_) => self.contains(v),
				_ => false,
			}),
			Value::String(other_strand) => match self {
				Value::String(s) => s.contains(&**other_strand),
				_ => false,
			},
			_ => false,
		}
	}

	/// Check if this Value intersects another Value
	pub fn intersects(&self, other: &Value) -> bool {
		match self {
			Value::Geometry(v) => match other {
				Value::Geometry(w) => v.intersects(w),
				_ => false,
			},
			_ => false,
		}
	}

	// -----------------------------------
	// Sorting operations
	// -----------------------------------

	/// Compare this Value to another Value lexicographically
	pub fn lexical_cmp(&self, other: &Value) -> Option<Ordering> {
		match (self, other) {
			(Value::String(a), Value::String(b)) => Some(lexicmp::lexical_cmp(a, b)),
			_ => self.partial_cmp(other),
		}
	}

	/// Compare this Value to another Value using natural numerical comparison
	pub fn natural_cmp(&self, other: &Value) -> Option<Ordering> {
		match (self, other) {
			(Value::String(a), Value::String(b)) => Some(lexicmp::natural_cmp(a, b)),
			_ => self.partial_cmp(other),
		}
	}

	/// Compare this Value to another Value lexicographically and using natural
	/// numerical comparison
	pub fn natural_lexical_cmp(&self, other: &Value) -> Option<Ordering> {
		match (self, other) {
			(Value::String(a), Value::String(b)) => Some(lexicmp::natural_lexical_cmp(a, b)),
			_ => self.partial_cmp(other),
		}
	}

	/// Turns this value into a literal evaluating to the same value.
	pub fn into_literal(self) -> expr::Expr {
		match self {
			Value::None => expr::Expr::Literal(expr::Literal::None),
			Value::Null => expr::Expr::Literal(expr::Literal::Null),
			Value::Bool(x) => expr::Expr::Literal(expr::Literal::Bool(x)),
			Value::Number(Number::Int(i)) => expr::Expr::Literal(expr::Literal::Integer(i)),
			Value::Number(Number::Float(f)) => expr::Expr::Literal(expr::Literal::Float(f)),
			Value::Number(Number::Decimal(d)) => expr::Expr::Literal(expr::Literal::Decimal(d)),
			Value::String(strand) => expr::Expr::Literal(expr::Literal::String(strand)),
			Value::Duration(duration) => expr::Expr::Literal(expr::Literal::Duration(duration)),
			Value::Datetime(datetime) => expr::Expr::Literal(expr::Literal::Datetime(datetime)),
			Value::Uuid(uuid) => expr::Expr::Literal(expr::Literal::Uuid(uuid)),
			Value::Array(array) => expr::Expr::Literal(expr::Literal::Array(array.into_literal())),
			Value::Object(object) => {
				expr::Expr::Literal(expr::Literal::Object(object.into_literal()))
			}
			Value::Geometry(geometry) => expr::Expr::Literal(expr::Literal::Geometry(geometry)),
			Value::Bytes(bytes) => expr::Expr::Literal(expr::Literal::Bytes(bytes)),
			Value::RecordId(record_id) => {
				expr::Expr::Literal(expr::Literal::RecordId(record_id.into_literal()))
			}
			Value::Regex(regex) => expr::Expr::Literal(expr::Literal::Regex(regex)),
			Value::File(file) => expr::Expr::Literal(expr::Literal::File(file)),
			Value::Closure(closure) => expr::Expr::Literal(expr::Literal::Closure(closure)),
			Value::Range(range) => range.into_literal(),
			Value::Table(t) => expr::Expr::Table(t.into_string()),
		}
	}
}

impl fmt::Display for Value {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let mut f = Pretty::from(f);
		match &self {
			Value::None => write!(f, "NONE"),
			Value::Null => write!(f, "NULL"),
			Value::Array(v) => write!(f, "{v}"),
			Value::Bool(v) => write!(f, "{v}"),
			Value::Bytes(v) => write!(f, "{v}"),
			Value::Datetime(v) => write!(f, "{v}"),
			Value::Duration(v) => write!(f, "{v}"),
			Value::Geometry(v) => write!(f, "{v}"),
			Value::Number(v) => write!(f, "{v}"),
			Value::Object(v) => write!(f, "{v}"),
			Value::Range(v) => write!(f, "{v}"),
			Value::Regex(v) => write!(f, "{v}"),
			Value::String(v) => write!(f, "{}", QuoteStr(v)),
			Value::RecordId(v) => write!(f, "{v}"),
			Value::Uuid(v) => write!(f, "{v}"),
			Value::Closure(v) => write!(f, "{v}"),
			Value::File(v) => write!(f, "{v}"),
			Value::Table(v) => write!(f, "{v}"),
		}
	}
}

impl ToSql for Value {
	fn fmt_sql(&self, f: &mut String) {
		write_sql!(f, "{}", self)
	}
}

impl InfoStructure for Value {
	fn structure(self) -> Value {
		self.to_string().into()
	}
}

// ------------------------------

pub(crate) trait TryAdd<Rhs = Self> {
	type Output;
	fn try_add(self, rhs: Rhs) -> Result<Self::Output>;
}

use std::ops::Add;

impl TryAdd for Value {
	type Output = Self;
	fn try_add(self, other: Self) -> Result<Self> {
		Ok(match (self, other) {
			(Self::Number(v), Self::Number(w)) => Self::Number(v.try_add(w)?),
			(Self::String(mut v), Self::String(w)) => {
				v.push_str(&w);
				Value::String(v)
			}
			(Self::Datetime(v), Self::Duration(w)) => Self::Datetime(w.try_add(v)?),
			(Self::Duration(v), Self::Datetime(w)) => Self::Datetime(v.try_add(w)?),
			(Self::Duration(v), Self::Duration(w)) => Self::Duration(v.try_add(w)?),
			(Self::Array(v), Self::Array(w)) => Self::Array(v.concat(w)),
			(Self::Object(v), Self::Object(w)) => Self::Object(v.add(w)),
			(v, w) => bail!(Error::TryAdd(v.to_raw_string(), w.to_raw_string())),
		})
	}
}

// ------------------------------

pub(crate) trait TrySub<Rhs = Self> {
	type Output;
	fn try_sub(self, v: Rhs) -> Result<Self::Output>;
}

impl TrySub for Value {
	type Output = Self;
	fn try_sub(self, other: Self) -> Result<Self> {
		Ok(match (self, other) {
			(Self::Number(v), Self::Number(w)) => Self::Number(v.try_sub(w)?),
			(Self::Datetime(v), Self::Datetime(w)) => Self::Duration(v.try_sub(w)?),
			(Self::Datetime(v), Self::Duration(w)) => Self::Datetime(w.try_sub(v)?),
			(Self::Duration(v), Self::Datetime(w)) => Self::Datetime(v.try_sub(w)?),
			(Self::Duration(v), Self::Duration(w)) => Self::Duration(v.try_sub(w)?),
			(v, w) => bail!(Error::TrySub(v.to_raw_string(), w.to_raw_string())),
		})
	}
}

// ------------------------------

pub(crate) trait TryMul<Rhs = Self> {
	type Output;
	fn try_mul(self, v: Self) -> Result<Self::Output>;
}

impl TryMul for Value {
	type Output = Self;
	fn try_mul(self, other: Self) -> Result<Self> {
		Ok(match (self, other) {
			(Self::Number(v), Self::Number(w)) => Self::Number(v.try_mul(w)?),
			(v, w) => bail!(Error::TryMul(v.to_raw_string(), w.to_raw_string())),
		})
	}
}

// ------------------------------

pub(crate) trait TryDiv<Rhs = Self> {
	type Output;
	fn try_div(self, v: Self) -> Result<Self::Output>;
}

impl TryDiv for Value {
	type Output = Self;
	fn try_div(self, other: Self) -> Result<Self> {
		Ok(match (self, other) {
			(Self::Number(v), Self::Number(w)) => Self::Number(v.try_div(w)?),
			(v, w) => bail!(Error::TryDiv(v.to_raw_string(), w.to_raw_string())),
		})
	}
}

// ------------------------------

pub(crate) trait TryFloatDiv<Rhs = Self> {
	type Output;
	fn try_float_div(self, v: Self) -> Result<Self::Output>;
}

impl TryFloatDiv for Value {
	type Output = Self;
	fn try_float_div(self, other: Self) -> Result<Self::Output> {
		Ok(match (self, other) {
			(Self::Number(v), Self::Number(w)) => Self::Number(v.try_float_div(w)?),
			(v, w) => bail!(Error::TryDiv(v.to_raw_string(), w.to_raw_string())),
		})
	}
}

// ------------------------------

pub(crate) trait TryRem<Rhs = Self> {
	type Output;
	fn try_rem(self, v: Self) -> Result<Self::Output>;
}

impl TryRem for Value {
	type Output = Self;
	fn try_rem(self, other: Self) -> Result<Self> {
		Ok(match (self, other) {
			(Self::Number(v), Self::Number(w)) => Self::Number(v.try_rem(w)?),
			(v, w) => bail!(Error::TryRem(v.to_raw_string(), w.to_raw_string())),
		})
	}
}

// ------------------------------

pub(crate) trait TryPow<Rhs = Self> {
	type Output;
	fn try_pow(self, v: Self) -> Result<Self::Output>;
}

impl TryPow for Value {
	type Output = Self;
	fn try_pow(self, other: Self) -> Result<Self> {
		Ok(match (self, other) {
			(Value::Number(v), Value::Number(w)) => Self::Number(v.try_pow(w)?),
			(v, w) => bail!(Error::TryPow(v.to_raw_string(), w.to_raw_string())),
		})
	}
}

// ------------------------------

pub(crate) trait TryNeg<Rhs = Self> {
	type Output;
	fn try_neg(self) -> Result<Self::Output>;
}

impl TryNeg for Value {
	type Output = Self;
	fn try_neg(self) -> Result<Self> {
		Ok(match self {
			Self::Number(n) => Self::Number(n.try_neg()?),
			v => bail!(Error::TryNeg(v.to_string())),
		})
	}
}

// Conversion methods.

/// Macro implementing conversion methods for the variants of the value enum.
macro_rules! subtypes {
	($($name:ident$( ( $($t:tt)* ) )? => ($is:ident,$as:ident,$into:ident)),*$(,)?) => {
		impl Value {
			$(
				subtypes!{@method $name $( ($($t)*) )? => $is,$as,$into}
			)*
		}

			$(
				subtypes!{@from $name $( ($($t)*) )? => $is,$as,$into}
			)*

	};

	(@pat $name:ident($t:ty)) => {
		Value::$name(_)
	};

	(@pat $name:ident) => {
		Value::$name
	};

	(@method $name:ident($t:ty) => $is:ident,$as:ident,$into:ident) => {
		#[doc = concat!("Check if the value is a [`",stringify!($name),"`]")]
		#[allow(dead_code)]
		pub fn $is(&self) -> bool{
			matches!(self,Value::$name(_))
		}

		#[doc = concat!("Return a reference to [`",stringify!($name),"`] if the value is of that type")]
		#[allow(dead_code)]
		pub fn $as(&self) -> Option<&$t>{
			if let Value::$name(x) = self{
				Some(x)
			}else{
				None
			}
		}

		#[doc = concat!("Turns the value into a [`",stringify!($name),"`] returning None if the value is not of that type")]
		#[allow(dead_code)]
		pub fn $into(self) -> Option<$t>{
			if let Value::$name(x) = self{
				Some(x)
			}else{
				None
			}
		}
	};

	(@method $name:ident => $is:ident,$as:ident,$into:ident) => {
		#[doc = concat!("Check if the value is a [`",stringify!($name),"`]")]
		#[allow(dead_code)]
		pub fn $is(&self) -> bool{
			matches!(self,Value::$name)
		}
	};


	(@from $name:ident(Box<$inner:ident>) => $is:ident,$as:ident,$into:ident) => {
		impl From<$inner> for Value {
			fn from(v: $inner) -> Self{
				Value::$name(Box::new(v))
			}
		}

		impl From<Box<$inner>> for Value {
			fn from(v: Box<$inner>) -> Self{
				Value::$name(v)
			}
		}
	};

	(@from $name:ident($t:ident) => $is:ident,$as:ident,$into:ident) => {
		impl From<$t> for Value {
			fn from(v: $t) -> Self{
				Value::$name(v)
			}
		}
	};

	(@from $name:ident => $is:ident,$as:ident,$into:ident) => {
		// skip
	};

}

subtypes! {
	None => (is_none,_unused,_unused),
	Null => (is_null,_unused,_unused),
	Bool(bool) => (is_bool,as_bool,into_bool),
	Number(Number) => (is_number,as_number,into_number),
	String(String) => (is_strand,as_strand,into_strand),
	Table(Table) => (is_table,as_table,into_table),
	Duration(Duration) => (is_duration,as_duration,into_duration),
	Datetime(Datetime) => (is_datetime,as_datetime,into_datetime),
	Uuid(Uuid) => (is_uuid,as_uuid,into_uuid),
	Array(Array) => (is_array,as_array,into_array),
	Object(Object) => (is_object,as_object,into_object),
	Geometry(Geometry) => (is_geometry,as_geometry,into_geometry),
	Bytes(Bytes) => (is_bytes,as_bytes,into_bytes),
	RecordId(RecordId) => (is_record,as_record,into_record),
	Regex(Regex) => (is_regex,as_regex,into_regex),
	Range(Box<Range>) => (is_range,as_range,into_range),
	Closure(Box<Closure>) => (is_closure,as_closure,into_closure),
	File(File) => (is_file,as_file,into_file),
}

macro_rules! impl_from_number {
	($($n:ident),*$(,)?) => {
		$(
			impl From<$n> for Value{
				fn from(v: $n) -> Self{
					Value::Number(Number::from(v))
				}
			}
		)*
	};
}
impl_from_number!(i8, i16, i32, i64, u8, u16, u32, isize, f32, f64, Decimal);

impl From<Vec<Value>> for Value {
	fn from(value: Vec<Value>) -> Self {
		Value::Array(Array(value))
	}
}

impl From<Null> for Value {
	fn from(_v: Null) -> Self {
		Value::Null
	}
}

// TODO: Remove these implementations
// They truncate by default and therefore should not be implement for value.
impl From<i128> for Value {
	fn from(v: i128) -> Self {
		Value::Number(Number::from(v))
	}
}

impl From<u64> for Value {
	fn from(v: u64) -> Self {
		Value::Number(Number::from(v))
	}
}

impl From<u128> for Value {
	fn from(v: u128) -> Self {
		Value::Number(Number::from(v))
	}
}

impl From<usize> for Value {
	fn from(v: usize) -> Self {
		Value::Number(Number::from(v))
	}
}

impl From<&str> for Value {
	fn from(v: &str) -> Self {
		Self::String(v.to_owned())
	}
}

impl From<DateTime<Utc>> for Value {
	fn from(v: DateTime<Utc>) -> Self {
		Value::Datetime(Datetime::from(v))
	}
}

impl From<Point<f64>> for Value {
	fn from(v: Point<f64>) -> Self {
		Value::Geometry(Geometry::from(v))
	}
}

impl From<HashMap<&str, Value>> for Value {
	fn from(v: HashMap<&str, Value>) -> Self {
		Value::Object(Object::from(v))
	}
}

impl From<HashMap<String, Value>> for Value {
	fn from(v: HashMap<String, Value>) -> Self {
		Value::Object(Object::from(v))
	}
}

impl From<BTreeMap<String, Value>> for Value {
	fn from(v: BTreeMap<String, Value>) -> Self {
		Value::Object(Object::from(v))
	}
}

impl From<BTreeMap<&str, Value>> for Value {
	fn from(v: BTreeMap<&str, Value>) -> Self {
		Value::Object(Object::from(v))
	}
}

impl TryFrom<Value> for crate::types::PublicValue {
	type Error = anyhow::Error;

	fn try_from(s: Value) -> Result<Self, Self::Error> {
		convert_value_to_public_value(s)
	}
}

impl From<crate::types::PublicValue> for Value {
	fn from(s: crate::types::PublicValue) -> Self {
		convert_public_value_to_internal(s)
	}
}

impl FromIterator<Value> for Value {
	fn from_iter<I: IntoIterator<Item = Value>>(iter: I) -> Self {
		Value::Array(Array(iter.into_iter().collect()))
	}
}

impl FromIterator<(String, Value)> for Value {
	fn from_iter<I: IntoIterator<Item = (String, Value)>>(iter: I) -> Self {
		Value::Object(Object(iter.into_iter().collect()))
	}
}

/// Convert our internal value `crate::val::Value` to the public value `surrealdb_types::Value`.
///
/// In the future, as the two types diverge, this function will need access to the context in order
/// to convert certain values to the public value.
pub(crate) fn convert_value_to_public_value(
	value: crate::val::Value,
) -> Result<surrealdb_types::Value> {
	match value {
		crate::val::Value::None => Ok(surrealdb_types::Value::None),
		crate::val::Value::Null => Ok(surrealdb_types::Value::Null),
		crate::val::Value::Bool(value) => Ok(surrealdb_types::Value::Bool(value)),
		crate::val::Value::Number(value) => convert_number_to_public(value),
		crate::val::Value::String(value) => Ok(surrealdb_types::Value::String(value)),
		crate::val::Value::Datetime(value) => convert_datetime_to_public(value),
		crate::val::Value::Duration(value) => convert_duration_to_public(value),
		crate::val::Value::Uuid(value) => convert_uuid_to_public(value),
		crate::val::Value::Array(value) => convert_array_to_public(value),
		crate::val::Value::Object(value) => convert_object_to_public(value),
		crate::val::Value::Geometry(value) => convert_geometry_to_public(value),
		crate::val::Value::Bytes(value) => convert_bytes_to_public(value),
		crate::val::Value::RecordId(value) => convert_record_id_to_public(value),
		crate::val::Value::File(value) => convert_file_to_public(value),
		crate::val::Value::Range(value) => convert_range_to_public(*value),
		crate::val::Value::Regex(value) => convert_regex_to_public(value),
		crate::val::Value::Table(value) => Ok(surrealdb_types::Value::Table(value.into())),
		crate::val::Value::Closure(_) => {
			Err(anyhow::anyhow!("Closure values cannot be converted to public value"))
		}
	}
}

fn convert_number_to_public(value: crate::val::Number) -> Result<surrealdb_types::Value> {
	let number = match value {
		crate::val::Number::Int(i) => surrealdb_types::Number::Int(i),
		crate::val::Number::Float(f) => surrealdb_types::Number::Float(f),
		crate::val::Number::Decimal(d) => surrealdb_types::Number::Decimal(d),
	};
	Ok(surrealdb_types::Value::Number(number))
}

fn convert_datetime_to_public(value: crate::val::Datetime) -> Result<surrealdb_types::Value> {
	Ok(surrealdb_types::Value::Datetime(surrealdb_types::Datetime::new(value.0)))
}

fn convert_duration_to_public(value: crate::val::Duration) -> Result<surrealdb_types::Value> {
	Ok(surrealdb_types::Value::Duration(surrealdb_types::Duration::from_std(value.0)))
}

fn convert_uuid_to_public(value: crate::val::Uuid) -> Result<surrealdb_types::Value> {
	Ok(surrealdb_types::Value::Uuid(surrealdb_types::Uuid(value.0)))
}

fn convert_bytes_to_public(value: crate::val::Bytes) -> Result<surrealdb_types::Value> {
	Ok(surrealdb_types::Value::Bytes(surrealdb_types::Bytes::new(value.0)))
}

fn convert_regex_to_public(value: crate::val::Regex) -> Result<surrealdb_types::Value> {
	Ok(surrealdb_types::Value::Regex(surrealdb_types::Regex(value.0)))
}

fn convert_file_to_public(value: crate::val::File) -> Result<surrealdb_types::Value> {
	Ok(surrealdb_types::Value::File(surrealdb_types::File::new(
		value.bucket.clone(),
		value.key.clone(),
	)))
}

fn convert_geometry_to_public(value: crate::val::Geometry) -> Result<surrealdb_types::Value> {
	use surrealdb_types::Geometry as PublicGeometry;
	let geometry = match value {
		crate::val::Geometry::Point(p) => PublicGeometry::Point(p),
		crate::val::Geometry::Line(l) => PublicGeometry::Line(l),
		crate::val::Geometry::Polygon(p) => PublicGeometry::Polygon(p),
		crate::val::Geometry::MultiPoint(mp) => PublicGeometry::MultiPoint(mp),
		crate::val::Geometry::MultiLine(ml) => PublicGeometry::MultiLine(ml),
		crate::val::Geometry::MultiPolygon(mp) => PublicGeometry::MultiPolygon(mp),
		crate::val::Geometry::Collection(c) => {
			let converted: Result<Vec<_>> = c
				.into_iter()
				.map(|g| {
					if let surrealdb_types::Value::Geometry(g) = convert_geometry_to_public(g)? {
						Ok(g)
					} else {
						Err(anyhow::anyhow!("Failed to convert geometry collection item"))
					}
				})
				.collect();
			PublicGeometry::Collection(converted?)
		}
	};
	Ok(surrealdb_types::Value::Geometry(geometry))
}

fn convert_array_to_public(value: crate::val::Array) -> Result<surrealdb_types::Value> {
	let converted: Result<Vec<_>> =
		value.0.into_iter().map(convert_value_to_public_value).collect();
	Ok(surrealdb_types::Value::Array(surrealdb_types::Array::from_values(converted?)))
}

fn convert_object_to_public(value: crate::val::Object) -> Result<surrealdb_types::Value> {
	let converted = convert_object_to_public_map(value)?;
	Ok(surrealdb_types::Value::Object(surrealdb_types::Object::from_map(converted)))
}

pub(crate) fn convert_object_to_public_map(
	value: crate::val::Object,
) -> Result<BTreeMap<String, surrealdb_types::Value>> {
	value.0.into_iter().map(|(k, v)| convert_value_to_public_value(v).map(|v| (k, v))).collect()
}

fn convert_record_id_to_public(value: crate::val::RecordId) -> Result<surrealdb_types::Value> {
	let key = convert_record_id_key_to_public(value.key)?;
	Ok(surrealdb_types::Value::RecordId(surrealdb_types::RecordId {
		table: value.table.into(),
		key,
	}))
}

fn convert_record_id_key_to_public(
	key: crate::val::RecordIdKey,
) -> Result<surrealdb_types::RecordIdKey> {
	match key {
		crate::val::RecordIdKey::Number(n) => Ok(surrealdb_types::RecordIdKey::Number(n)),
		crate::val::RecordIdKey::String(s) => Ok(surrealdb_types::RecordIdKey::String(s)),
		crate::val::RecordIdKey::Uuid(u) => {
			Ok(surrealdb_types::RecordIdKey::Uuid(surrealdb_types::Uuid(u.0)))
		}
		crate::val::RecordIdKey::Array(a) => {
			let converted_array = convert_array_to_public(a)?;
			if let surrealdb_types::Value::Array(arr) = converted_array {
				Ok(surrealdb_types::RecordIdKey::Array(arr))
			} else {
				Err(anyhow::anyhow!("Failed to convert record id key array"))
			}
		}
		crate::val::RecordIdKey::Object(o) => {
			let converted_object = convert_object_to_public(o)?;
			if let surrealdb_types::Value::Object(obj) = converted_object {
				Ok(surrealdb_types::RecordIdKey::Object(obj))
			} else {
				Err(anyhow::anyhow!("Failed to convert record id key object"))
			}
		}
		crate::val::RecordIdKey::Range(r) => {
			let start = match r.start {
				std::ops::Bound::Included(k) => {
					std::ops::Bound::Included(convert_record_id_key_to_public(k)?)
				}
				std::ops::Bound::Excluded(k) => {
					std::ops::Bound::Excluded(convert_record_id_key_to_public(k)?)
				}
				std::ops::Bound::Unbounded => std::ops::Bound::Unbounded,
			};
			let end = match r.end {
				std::ops::Bound::Included(k) => {
					std::ops::Bound::Included(convert_record_id_key_to_public(k)?)
				}
				std::ops::Bound::Excluded(k) => {
					std::ops::Bound::Excluded(convert_record_id_key_to_public(k)?)
				}
				std::ops::Bound::Unbounded => std::ops::Bound::Unbounded,
			};
			Ok(surrealdb_types::RecordIdKey::Range(Box::new(surrealdb_types::RecordIdKeyRange {
				start,
				end,
			})))
		}
	}
}

fn convert_range_to_public(value: crate::val::Range) -> Result<surrealdb_types::Value> {
	let start = match value.start {
		std::ops::Bound::Included(v) => {
			std::ops::Bound::Included(convert_value_to_public_value(v)?)
		}
		std::ops::Bound::Excluded(v) => {
			std::ops::Bound::Excluded(convert_value_to_public_value(v)?)
		}
		std::ops::Bound::Unbounded => std::ops::Bound::Unbounded,
	};
	let end = match value.end {
		std::ops::Bound::Included(v) => {
			std::ops::Bound::Included(convert_value_to_public_value(v)?)
		}
		std::ops::Bound::Excluded(v) => {
			std::ops::Bound::Excluded(convert_value_to_public_value(v)?)
		}
		std::ops::Bound::Unbounded => std::ops::Bound::Unbounded,
	};
	Ok(surrealdb_types::Value::Range(Box::new(surrealdb_types::Range {
		start,
		end,
	})))
}

#[cfg(test)]
mod tests {
	use chrono::{TimeZone, Utc};
	use geo::{MultiLineString, MultiPoint, MultiPolygon, line_string, point, polygon};
	use rstest::rstest;
	use rust_decimal::Decimal;
	use serde_json::{Value as Json, json};

	use super::*;
	use crate::syn;
	use crate::types::{
		PublicArray, PublicBytes, PublicDatetime, PublicDuration, PublicGeometry, PublicNumber,
		PublicObject, PublicRecordId, PublicRecordIdKey, PublicUuid, PublicValue,
	};
	use crate::val::Uuid;

	macro_rules! parse_val {
		($input:expr) => {
			crate::val::convert_public_value_to_internal(syn::value($input).unwrap())
		};
	}

	#[test]
	fn check_none() {
		assert!(Value::None.is_none());
		assert!(!Value::Null.is_none());
		assert!(!Value::from(1).is_none());
	}

	#[test]
	fn check_null() {
		assert!(Value::Null.is_null());
		assert!(!Value::None.is_null());
		assert!(!Value::from(1).is_null());
	}

	#[test]
	fn convert_truthy() {
		assert!(!Value::None.is_truthy());
		assert!(!Value::Null.is_truthy());
		assert!(Value::Bool(true).is_truthy());
		assert!(!Value::Bool(false).is_truthy());
		assert!(!Value::from(0).is_truthy());
		assert!(Value::from(1).is_truthy());
		assert!(Value::from(-1).is_truthy());
		assert!(Value::from(1.1).is_truthy());
		assert!(Value::from(-1.1).is_truthy());
		assert!(Value::from("true").is_truthy());
		assert!(Value::from("false").is_truthy());
		assert!(Value::from("falsey").is_truthy());
		assert!(Value::from("something").is_truthy());
		assert!(Value::from(Uuid::nil()).is_truthy());
		assert!(Value::from(Utc.with_ymd_and_hms(1948, 12, 3, 0, 0, 0).unwrap()).is_truthy());
	}

	#[test]
	fn convert_string() {
		assert_eq!(String::from("NONE"), Value::None.into_raw_string());
		assert_eq!(String::from("NULL"), Value::Null.into_raw_string());
		assert_eq!(String::from("true"), Value::Bool(true).into_raw_string());
		assert_eq!(String::from("false"), Value::Bool(false).into_raw_string());
		assert_eq!(String::from("0"), Value::from(0).into_raw_string());
		assert_eq!(String::from("1"), Value::from(1).into_raw_string());
		assert_eq!(String::from("-1"), Value::from(-1).into_raw_string());
		assert_eq!(String::from("1.1f"), Value::from(1.1).into_raw_string());
		assert_eq!(String::from("-1.1f"), Value::from(-1.1).into_raw_string());
		assert_eq!(String::from("3"), Value::from("3").into_raw_string());
		assert_eq!(String::from("true"), Value::from("true").into_raw_string());
		assert_eq!(String::from("false"), Value::from("false").into_raw_string());
		assert_eq!(String::from("something"), Value::from("something").into_raw_string());
	}

	#[test]
	fn check_size() {
		assert!(64 >= std::mem::size_of::<Value>(), "size of value too big");
	}

	#[rstest]
	#[case::none(Value::None, 2)]
	#[case::null(Value::Null, 2)]
	#[case::bool(Value::Bool(true), 3)]
	#[case::bool(Value::Bool(false), 3)]
	#[case::string(Value::from("test"), 7)]
	#[case::object(Value::from(syn::value("{ hello: 'world' }").unwrap()), 18)]
	#[case::object(Value::from(syn::value("{ compact: true, schema: 0 }").unwrap()), 27)]
	fn check_serialize(#[case] value: Value, #[case] expected: usize) {
		let enc: Vec<u8> = revision::to_vec(&value).unwrap();
		assert_eq!(expected, enc.len());
	}

	#[test]
	fn serialize_deserialize() {
		let val = parse_val!(
			"{ test: { something: [1, 'two', null, test:tobie, { trueee: false, noneee: null }] } }"
		);
		let res = parse_val!(
			"{ test: { something: [1, 'two', null, test:tobie, { trueee: false, noneee: null }] } }"
		);
		let enc: Vec<u8> = revision::to_vec(&val).unwrap();
		let dec: Value = revision::from_slice(&enc).unwrap();
		assert_eq!(res, dec);
	}

	#[rstest]
	#[case::none(PublicValue::None, json!(null), PublicValue::Null)]
	#[case::null(PublicValue::Null, json!(null), PublicValue::Null)]
	#[case::bool(PublicValue::Bool(true), json!(true), PublicValue::Bool(true))]
	#[case::bool(PublicValue::Bool(false), json!(false), PublicValue::Bool(false))]
	#[case::number(
		PublicValue::Number(PublicNumber::Int(i64::MIN)),
		json!(i64::MIN),
		PublicValue::Number(PublicNumber::Int(i64::MIN)),
	)]
	#[case::number(
		PublicValue::Number(PublicNumber::Int(i64::MAX)),
		json!(i64::MAX),
		PublicValue::Number(PublicNumber::Int(i64::MAX)),
	)]
	#[case::number(
		PublicValue::Number(PublicNumber::Float(1.23)),
		json!(1.23),
		PublicValue::Number(PublicNumber::Float(1.23)),
	)]
	#[case::number(
		PublicValue::Number(PublicNumber::Float(f64::NEG_INFINITY)),
		json!(null),
		PublicValue::Null,
	)]
	#[case::number(
		PublicValue::Number(PublicNumber::Float(f64::MIN)),
		json!(-1.7976931348623157e308),
		PublicValue::Number(PublicNumber::Float(f64::MIN)),
	)]
	#[case::number(
		PublicValue::Number(PublicNumber::Float(0.0)),
		json!(0.0),
		PublicValue::Number(PublicNumber::Float(0.0)),
	)]
	#[case::number(
		PublicValue::Number(PublicNumber::Float(f64::MAX)),
		json!(1.7976931348623157e308),
		PublicValue::Number(PublicNumber::Float(f64::MAX)),
	)]
	#[case::number(
		PublicValue::Number(PublicNumber::Float(f64::INFINITY)),
		json!(null),
		PublicValue::Null,
	)]
	#[case::number(
		PublicValue::Number(PublicNumber::Float(f64::NAN)),
		json!(null),
		PublicValue::Null,
	)]
	#[case::number(
		PublicValue::Number(PublicNumber::Decimal(Decimal::new(123, 2))),
		json!("1.23"),
		PublicValue::String("1.23".into()),
	)]
	#[case::strand(
		PublicValue::String("".into()),
		json!(""),
		PublicValue::String("".into()),
	)]
	#[case::strand(
		PublicValue::String("foo".into()),
		json!("foo"),
		PublicValue::String("foo".into()),
	)]
	#[case::duration(
		PublicValue::Duration(PublicDuration::ZERO),
		json!("0ns"),
		PublicValue::String("0ns".into()),
	)]
	#[case::duration(
		PublicValue::Duration(PublicDuration::MAX),
		json!("584942417355y3w5d7h15s999ms999µs999ns"),
		PublicValue::String("584942417355y3w5d7h15s999ms999µs999ns".into()),
	)]
	#[case::datetime(
		PublicValue::Datetime(PublicDatetime::MIN_UTC),
		json!("-262143-01-01T00:00:00Z"),
		PublicValue::String("-262143-01-01T00:00:00Z".into()),
	)]
	#[case::datetime(
		PublicValue::Datetime(PublicDatetime::MAX_UTC),
		json!("+262142-12-31T23:59:59.999999999Z"),
		PublicValue::String("+262142-12-31T23:59:59.999999999Z".into()),
	)]
	#[case::uuid(
		PublicValue::Uuid(PublicUuid::nil()),
		json!("00000000-0000-0000-0000-000000000000"),
		PublicValue::String("00000000-0000-0000-0000-000000000000".into()),
	)]
	#[case::uuid(
		PublicValue::Uuid(PublicUuid::max()),
		json!("ffffffff-ffff-ffff-ffff-ffffffffffff"),
		PublicValue::String("ffffffff-ffff-ffff-ffff-ffffffffffff".into()),
	)]
	#[case::bytes(
		PublicValue::Bytes(PublicBytes::default()),
		json!([]),
		PublicValue::Array(PublicArray::new()),
	)]
	#[case::bytes(
		PublicValue::Bytes(PublicBytes::from(b"foo".to_vec())),
		json!([102, 111, 111]),
		PublicValue::Array(PublicArray::from(vec![
			PublicValue::Number(PublicNumber::Int(102)),
			PublicValue::Number(PublicNumber::Int(111)),
			PublicValue::Number(PublicNumber::Int(111)),
		])),
	)]
	#[case::record_id(
		PublicValue::RecordId(PublicRecordId::new("foo", PublicRecordIdKey::String("bar".into()))) ,
		json!("foo:bar"),
		PublicValue::RecordId(PublicRecordId::new("foo", PublicRecordIdKey::String("bar".into()))) ,
	)]
	#[case::array(
		PublicValue::Array(PublicArray::new()),
		json!([]),
		PublicValue::Array(PublicArray::new()),
	)]
	#[case::array(
		PublicValue::Array(PublicArray::from(vec![PublicValue::Bool(true), PublicValue::Bool(false)])),
		json!([true, false]),
		PublicValue::Array(PublicArray::from(vec![PublicValue::Bool(true), PublicValue::Bool(false)])),
	)]
	#[case::object(
		PublicValue::Object(PublicObject::new()),
		json!({}),
		PublicValue::Object(PublicObject::new()),
	)]
	#[case::object(
		PublicValue::Object(PublicObject::from_iter([("done".to_owned(), PublicValue::Bool(true))])),
		json!({"done": true}),
		PublicValue::Object(PublicObject::from_iter([("done".to_owned(), PublicValue::Bool(true))])),
	)]
	#[case::geometry_point(
		PublicValue::Geometry(PublicGeometry::Point(point! { x: 10., y: 20. })),
		json!({ "type": "Point", "coordinates": [10., 20.]}),
		PublicValue::Geometry(PublicGeometry::Point(point! { x: 10., y: 20. })),
	)]
	#[case::geometry_line(
		PublicValue::Geometry(PublicGeometry::Line(line_string![
			( x: 0., y: 0. ),
			( x: 10., y: 0. ),
		])),
		json!({ "type": "LineString", "coordinates": [[0., 0.], [10., 0.]]}),
		PublicValue::Geometry(PublicGeometry::Line(line_string![
			( x: 0., y: 0. ),
			( x: 10., y: 0. ),
		])),
	)]
	#[case::geometry_polygon(
		PublicValue::Geometry(PublicGeometry::Polygon(polygon![
			(x: -111., y: 45.),
			(x: -111., y: 41.),
			(x: -104., y: 41.),
			(x: -104., y: 45.),
		])),
		json!({ "type": "Polygon", "coordinates": [[
			[-111., 45.],
			[-111., 41.],
			[-104., 41.],
			[-104., 45.],
			[-111., 45.],
		]]}),
		PublicValue::Geometry(PublicGeometry::Polygon(polygon![
			(x: -111., y: 45.),
			(x: -111., y: 41.),
			(x: -104., y: 41.),
			(x: -104., y: 45.),
		])),
	)]
	#[case::geometry_multi_point(
		PublicValue::Geometry(PublicGeometry::MultiPoint(MultiPoint::new(vec![
			point! { x: 0., y: 0. },
			point! { x: 1., y: 2. },
		]))),
		json!({ "type": "MultiPoint", "coordinates": [[0., 0.], [1., 2.]]}),
		PublicValue::Geometry(PublicGeometry::MultiPoint(MultiPoint::new(vec![
			point! { x: 0., y: 0. },
			point! { x: 1., y: 2. },
		]))),
	)]
	#[case::geometry_multi_line(
		PublicValue::Geometry(
			PublicGeometry::MultiLine(
				MultiLineString::new(vec![
					line_string![( x: 0., y: 0. ), ( x: 1., y: 2. )],
				])
			)
		),
		json!({ "type": "MultiLineString", "coordinates": [[[0., 0.], [1., 2.]]]}),
		PublicValue::Geometry(
			PublicGeometry::MultiLine(
				MultiLineString::new(vec![
					line_string![( x: 0., y: 0. ), ( x: 1., y: 2. )],
				])
			)
		),
	)]
	#[case::geometry_multi_polygon(
		PublicValue::Geometry(PublicGeometry::MultiPolygon(MultiPolygon::new(vec![
			polygon![
				(x: -111., y: 45.),
				(x: -111., y: 41.),
				(x: -104., y: 41.),
				(x: -104., y: 45.),
			],
		]))),
		json!({ "type": "MultiPolygon", "coordinates": [[[
			[-111., 45.],
			[-111., 41.],
			[-104., 41.],
			[-104., 45.],
			[-111., 45.],
		]]]})
	,	PublicValue::Geometry(PublicGeometry::MultiPolygon(MultiPolygon::new(vec![
			polygon![
				(x: -111., y: 45.),
				(x: -111., y: 41.),
				(x: -104., y: 41.),
				(x: -104., y: 45.),
			],
		]))),
	)]
	#[case::geometry_collection(
		PublicValue::Geometry(PublicGeometry::Collection(vec![])),
		json!({
			"type": "GeometryCollection",
			"geometries": [],
		}),
		PublicValue::Geometry(PublicGeometry::Collection(vec![])),
	)]
	#[case::geometry_collection_with_point(
		PublicValue::Geometry(PublicGeometry::Collection(vec![PublicGeometry::Point(point! { x: 10., y: 20. })])),
		json!({
		"type": "GeometryCollection",
		"geometries": [ { "type": "Point", "coordinates": [10., 20.] } ],
	}),
		PublicValue::Geometry(PublicGeometry::Collection(vec![PublicGeometry::Point(point! { x: 10., y: 20. })])),
	)]
	#[case::geometry_collection_with_line(
		PublicValue::Geometry(PublicGeometry::Collection(vec![PublicGeometry::Line(line_string![
			( x: 0., y: 0. ),
			( x: 10., y: 0. ),
		])])),
		json!({
			"type": "GeometryCollection",
			"geometries": [ { "type": "LineString", "coordinates": [[0., 0.], [10., 0.]] } ],
		}),
		PublicValue::Geometry(PublicGeometry::Collection(vec![PublicGeometry::Line(line_string![
			( x: 0., y: 0. ),
			( x: 10., y: 0. ),
		])])),
	)]

	fn test_json(
		#[case] value: PublicValue,
		#[case] expected: Json,
		#[case] expected_deserialized: PublicValue,
	) {
		let json_value = value.into_json_value();
		assert_eq!(json_value, expected);

		let json_str = serde_json::to_string(&json_value).expect("Failed to serialize to JSON");
		let deserialized_sql_value = crate::syn::value_legacy_strand(&json_str).unwrap();
		assert_eq!(deserialized_sql_value, expected_deserialized);
	}
}
