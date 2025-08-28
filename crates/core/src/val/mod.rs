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

use crate::err::Error;
use crate::expr::fmt::Pretty;
use crate::expr::kind::GeometryKind;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{self, Kind};

pub mod array;
pub mod bytes;
pub mod closure;
pub mod datetime;
pub mod duration;
pub mod file;
pub mod geometry;
pub mod number;
pub mod object;
pub mod range;
pub mod record;
pub mod record_id;
pub mod regex;
pub mod strand;
pub mod table;
pub mod uuid;
pub mod value;

pub use self::array::Array;
pub use self::bytes::Bytes;
pub use self::closure::Closure;
pub use self::datetime::Datetime;
pub use self::duration::Duration;
pub use self::file::File;
pub use self::geometry::Geometry;
pub use self::number::{DecimalExt, Number};
pub use self::object::Object;
pub use self::range::Range;
pub use self::record_id::{RecordId, RecordIdKey, RecordIdKeyRange};
pub use self::regex::Regex;
pub use self::strand::{Strand, StrandRef};
pub use self::table::Table;
pub use self::uuid::Uuid;
pub use self::value::{CastError, CoerceError};

/// Marker type for value conversions from Value::None
#[derive(Clone, Copy, Eq, PartialEq, PartialOrd)]
pub struct SqlNone;

/// Marker type for value conversions from Value::Null
#[derive(Clone, Copy, Eq, PartialEq, PartialOrd)]
pub struct Null;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::Value")]
pub enum Value {
	#[default]
	None,
	Null,
	Bool(bool),
	Number(Number),
	Strand(Strand),
	Duration(Duration),
	Datetime(Datetime),
	Uuid(Uuid),
	Array(Array),
	Object(Object),
	Geometry(Geometry),
	Bytes(Bytes),
	RecordId(RecordId),
	Table(Table),
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

	/// Check if this Value is TRUE or 'true'
	pub fn is_true(&self) -> bool {
		matches!(self, Value::Bool(true))
	}

	/// Check if this Value is FALSE or 'false'
	pub fn is_false(&self) -> bool {
		matches!(self, Value::Bool(false))
	}

	/// Check if this Value is truthy
	pub fn is_truthy(&self) -> bool {
		match self {
			Value::Bool(v) => *v,
			Value::Uuid(_) => true,
			Value::RecordId(_) => true,
			Value::Geometry(_) => true,
			Value::Datetime(_) => true,
			Value::Array(v) => !v.is_empty(),
			Value::Object(v) => !v.is_empty(),
			Value::Strand(v) => !v.is_empty(),
			Value::Number(v) => v.is_truthy(),
			Value::Duration(v) => v.as_nanos() > 0,
			// TODO: Table, range, bytes and closure should probably also have certain truthy
			// values.
			_ => false,
		}
	}

	/// Check if this Value is a Thing, and belongs to a certain table
	pub fn is_record_of_table(&self, table: String) -> bool {
		match self {
			Value::RecordId(RecordId {
				table: tb,
				..
			}) => *tb == table,
			_ => false,
		}
	}

	/// Check if this Value is an int Number
	pub fn is_int(&self) -> bool {
		matches!(self, Value::Number(Number::Int(_)))
	}

	pub fn into_int(self) -> Option<i64> {
		if let Number::Int(x) = self.into_number()? {
			Some(x)
		} else {
			None
		}
	}

	/// Check if this Value is a float Number
	pub fn is_float(&self) -> bool {
		matches!(self, Value::Number(Number::Float(_)))
	}

	pub fn into_float(self) -> Option<f64> {
		if let Number::Float(x) = self.into_number()? {
			Some(x)
		} else {
			None
		}
	}

	/// Check if this Value is a decimal Number
	pub fn is_decimal(&self) -> bool {
		matches!(self, Value::Number(Number::Decimal(_)))
	}

	pub fn into_decimal(self) -> Option<Decimal> {
		if let Number::Decimal(x) = self.into_number()? {
			Some(x)
		} else {
			None
		}
	}

	/// Check if this Value is a Thing of a specific type
	pub fn is_record_type(&self, types: &[String]) -> bool {
		match self {
			Value::RecordId(v) => v.is_record_type(types),
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
	pub fn as_raw_string(self) -> String {
		match self {
			Value::Strand(v) => v.into_string(),
			Value::Uuid(v) => v.to_raw(),
			Value::Datetime(v) => v.into_raw_string(),
			_ => self.to_string(),
		}
	}

	/// Converts this Value into an unquoted String
	pub fn to_raw_string(&self) -> String {
		match self {
			Value::Strand(v) => v.clone().into_string(),
			Value::Uuid(v) => v.to_raw(),
			Value::Datetime(v) => v.into_raw_string(),
			_ => self.to_string(),
		}
	}

	// -----------------------------------
	// Simple output of value type
	// -----------------------------------

	pub fn kind(&self) -> Option<Kind> {
		match self {
			Value::None => None,
			Value::Null => Some(Kind::Null),
			Value::Bool(_) => Some(Kind::Bool),
			Value::Number(_) => Some(Kind::Number),
			Value::Strand(_) => Some(Kind::String),
			Value::Duration(_) => Some(Kind::Duration),
			Value::Datetime(_) => Some(Kind::Datetime),
			Value::Uuid(_) => Some(Kind::Uuid),
			Value::Array(arr) => Some(Kind::Array(
				Box::new(arr.first().and_then(|v| v.kind()).unwrap_or_default()),
				None,
			)),
			Value::Object(_) => Some(Kind::Object),
			Value::Geometry(geo) => Some(Kind::Geometry(vec![geo.kind()])),
			Value::Bytes(_) => Some(Kind::Bytes),
			Value::Regex(_) => Some(Kind::Regex),
			Value::RecordId(thing) => Some(Kind::Record(vec![thing.table.clone()])),
			Value::Closure(closure) => {
				let args_kinds =
					closure.args.iter().map(|(_, kind)| kind.clone()).collect::<Vec<_>>();
				let returns_kind = closure.returns.clone().map(Box::new);

				Some(Kind::Function(Some(args_kinds), returns_kind))
			}
			//Value::Refs(_) => None,
			Value::File(file) => Some(Kind::File(vec![file.bucket.clone()])),
			_ => None,
		}
	}

	/// Returns the surql representation of the kind of the value as a string.
	///
	/// # Warning
	/// This function is not fully implement for all variants, make sure you
	/// don't accidentally use it where it can return an invalid value.
	pub fn kind_of(&self) -> &'static str {
		// TODO: Look at this function, there are a whole bunch of options for which
		// this returns "incorrect type" which might sneak into the results where it
		// shouldn.t
		match self {
			Self::None => "none",
			Self::Null => "null",
			Self::Bool(_) => "bool",
			Self::Uuid(_) => "uuid",
			Self::Array(_) => "array",
			Self::Object(_) => "object",
			Self::Strand(_) => "string",
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
			Self::RecordId(_) => "thing",
			// TODO: Dubious types
			Self::Table(_) => "table",
		}
	}

	// -----------------------------------
	// Record ID extraction
	// -----------------------------------

	/// Fetch the record id if there is one
	pub fn record(self) -> Option<RecordId> {
		match self {
			// This is an object so look for the id field
			Value::Object(mut v) => match v.remove("id") {
				Some(Value::RecordId(v)) => Some(v),
				_ => None,
			},
			// This is an array so take the first item
			Value::Array(mut v) => match v.len() {
				1 => v.remove(0).record(),
				_ => None,
			},
			// This is a record id already
			Value::RecordId(v) => Some(v),
			// There is no valid record id
			_ => None,
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
			Value::Strand(v) => match other {
				Value::Strand(w) => v == w,
				Value::Regex(w) => w.regex().is_match(v.as_str()),
				_ => false,
			},
			Value::Regex(v) => match other {
				Value::Regex(w) => v == w,
				// TODO(3.0.0): Decide if we want to keep this behavior.
				//Value::RecordId(w) => v.regex().is_match(w.to_raw().as_str()),
				Value::Strand(w) => v.regex().is_match(w.as_str()),
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
				Value::Strand(w) => v.to_raw().contains(w.as_str()),
				_ => false,
			},
			Value::Strand(v) => match other {
				Value::Strand(w) => v.contains(w.as_str()),
				_ => false,
			},
			Value::Geometry(v) => match other {
				Value::Geometry(w) => v.contains(w),
				_ => false,
			},
			Value::Object(v) => match other {
				Value::Strand(w) => v.0.contains_key(&**w),
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
				let Value::Strand(this) = self else {
					return false;
				};
				v.iter().all(|s| {
					let Value::Strand(other_string) = s else {
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
			Value::Strand(other_strand) => match self {
				Value::Strand(s) => s.contains(&**other_strand),
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
				let Value::Strand(this) = self else {
					return false;
				};
				v.iter().any(|s| {
					let Value::Strand(other_string) = s else {
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
			Value::Strand(other_strand) => match self {
				Value::Strand(s) => s.contains(&**other_strand),
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
			(Value::Strand(a), Value::Strand(b)) => Some(lexicmp::lexical_cmp(a, b)),
			_ => self.partial_cmp(other),
		}
	}

	/// Compare this Value to another Value using natural numerical comparison
	pub fn natural_cmp(&self, other: &Value) -> Option<Ordering> {
		match (self, other) {
			(Value::Strand(a), Value::Strand(b)) => Some(lexicmp::natural_cmp(a, b)),
			_ => self.partial_cmp(other),
		}
	}

	/// Compare this Value to another Value lexicographically and using natural
	/// numerical comparison
	pub fn natural_lexical_cmp(&self, other: &Value) -> Option<Ordering> {
		match (self, other) {
			(Value::Strand(a), Value::Strand(b)) => Some(lexicmp::natural_lexical_cmp(a, b)),
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
			Value::Strand(strand) => expr::Expr::Literal(expr::Literal::Strand(strand)),
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
			Value::Table(t) => expr::Expr::Table(t.into()),
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
			Value::Strand(v) => write!(f, "{v}"),
			Value::RecordId(v) => write!(f, "{v}"),
			Value::Uuid(v) => write!(f, "{v}"),
			Value::Closure(v) => write!(f, "{v}"),
			Value::File(v) => write!(f, "{v}"),
			Value::Table(v) => write!(f, "{v}"),
		}
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
			(Self::Strand(v), Self::Strand(w)) => Self::Strand(v.try_add(w)?),
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
		pub enum Type {
			$($name),*
		}

		impl Value {

			pub fn type_of(&self) -> Type {
				match &self{
					$(subtypes!{@pat $name $( ($($t)*) )?} => Type::$name),*
				}
			}

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
		pub fn $is(&self) -> bool{
			matches!(self,Value::$name(_))
		}

		#[doc = concat!("Return a reference to [`",stringify!($name),"`] if the value is of that type")]
		pub fn $as(&self) -> Option<&$t>{
			if let Value::$name(x) = self{
				Some(x)
			}else{
				None
			}
		}

		#[doc = concat!("Turns the value into a [`",stringify!($name),"`] returning None if the value is not of that type")]
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
		pub fn $is(&self) -> bool{
			matches!(self,Value::$name)
		}
	};


	(@from $name:ident(Box<$inner:ident>) => $is:ident,$as:ident,$into:ident) => {
		impl From<$inner> for Value{
			fn from(v: $inner) -> Self{
				Value::$name(Box::new(v))
			}
		}

		impl From<Box<$inner>> for Value{
			fn from(v: Box<$inner>) -> Self{
				Value::$name(v)
			}
		}
	};

	(@from $name:ident($t:ident) => $is:ident,$as:ident,$into:ident) => {
		impl From<$t> for Value{
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
	Strand(Strand) => (is_strand,as_strand,into_strand),
	Table(Table) => (is_table,as_table,into_table),
	Duration(Duration) => (is_duration,as_duration,into_duration),
	Datetime(Datetime) => (is_datetime,as_datetime,into_datetime),
	Uuid(Uuid) => (is_uuid,as_uuid,into_uuid),
	Array(Array) => (is_array,as_array,into_array),
	Object(Object) => (is_object,as_object,into_object),
	Geometry(Geometry) => (is_geometry,as_geometry,into_geometry),
	Bytes(Bytes) => (is_bytes,as_bytes,into_bytes),
	RecordId(RecordId) => (is_thing,as_thing,into_thing),
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

impl From<String> for Value {
	fn from(v: String) -> Self {
		Self::Strand(Strand::from(v))
	}
}

impl From<&str> for Value {
	fn from(v: &str) -> Self {
		Self::Strand(Strand::from(v))
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

#[cfg(test)]
mod tests {
	use chrono::TimeZone;

	use super::*;
	use crate::syn;

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
	fn check_true() {
		assert!(!Value::None.is_true());
		assert!(Value::Bool(true).is_true());
		assert!(!Value::Bool(false).is_true());
		assert!(!Value::from(1).is_true());
		assert!(!Value::from("something").is_true());
	}

	#[test]
	fn check_false() {
		assert!(!Value::None.is_false());
		assert!(!Value::Bool(true).is_false());
		assert!(Value::Bool(false).is_false());
		assert!(!Value::from(1).is_false());
		assert!(!Value::from("something").is_false());
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
		assert!(Value::from(Uuid::new()).is_truthy());
		assert!(Value::from(Utc.with_ymd_and_hms(1948, 12, 3, 0, 0, 0).unwrap()).is_truthy());
	}

	#[test]
	fn convert_string() {
		assert_eq!(String::from("NONE"), Value::None.as_raw_string());
		assert_eq!(String::from("NULL"), Value::Null.as_raw_string());
		assert_eq!(String::from("true"), Value::Bool(true).as_raw_string());
		assert_eq!(String::from("false"), Value::Bool(false).as_raw_string());
		assert_eq!(String::from("0"), Value::from(0).as_raw_string());
		assert_eq!(String::from("1"), Value::from(1).as_raw_string());
		assert_eq!(String::from("-1"), Value::from(-1).as_raw_string());
		assert_eq!(String::from("1.1f"), Value::from(1.1).as_raw_string());
		assert_eq!(String::from("-1.1f"), Value::from(-1.1).as_raw_string());
		assert_eq!(String::from("3"), Value::from("3").as_raw_string());
		assert_eq!(String::from("true"), Value::from("true").as_raw_string());
		assert_eq!(String::from("false"), Value::from("false").as_raw_string());
		assert_eq!(String::from("something"), Value::from("something").as_raw_string());
	}

	#[test]
	fn check_size() {
		assert!(64 >= std::mem::size_of::<Value>(), "size of value too big");
	}

	#[test]
	fn check_serialize() {
		let enc: Vec<u8> = revision::to_vec(&Value::None).unwrap();
		assert_eq!(2, enc.len());
		let enc: Vec<u8> = revision::to_vec(&Value::Null).unwrap();
		assert_eq!(2, enc.len());
		let enc: Vec<u8> = revision::to_vec(&Value::Bool(true)).unwrap();
		assert_eq!(3, enc.len());
		let enc: Vec<u8> = revision::to_vec(&Value::Bool(false)).unwrap();
		assert_eq!(3, enc.len());
		let enc: Vec<u8> = revision::to_vec(&Value::from("test")).unwrap();
		assert_eq!(8, enc.len());
		let enc: Vec<u8> = revision::to_vec(&syn::value("{ hello: 'world' }").unwrap()).unwrap();
		assert_eq!(19, enc.len());
		let enc: Vec<u8> =
			revision::to_vec(&syn::value("{ compact: true, schema: 0 }").unwrap()).unwrap();
		assert_eq!(27, enc.len());
	}

	#[test]
	fn serialize_deserialize() {
		let val = syn::value(
			"{ test: { something: [1, 'two', null, test:tobie, { trueee: false, noneee: null }] } }",
		)
		.unwrap();
		let res = syn::value(
			"{ test: { something: [1, 'two', null, test:tobie, { trueee: false, noneee: null }] } }",
		)
		.unwrap();
		let enc: Vec<u8> = revision::to_vec(&val).unwrap();
		let dec: Value = revision::from_slice(&enc).unwrap();
		assert_eq!(res, dec);
	}
}
