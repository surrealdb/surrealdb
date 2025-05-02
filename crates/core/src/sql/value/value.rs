#![allow(clippy::derive_ord_xor_partial_ord)]

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::fnc::util::string::fuzzy::Fuzzy;
use crate::sql::id::range::IdRange;
use crate::sql::range::OldRange;
use crate::sql::reference::Refs;
use crate::sql::statements::info::InfoStructure;
use crate::sql::{
	fmt::{Fmt, Pretty},
	id::{Gen, Id},
	model::Model,
	Array, Block, Bytes, Cast, Constant, Datetime, Duration, Edges, Expression, File, Function,
	Future, Geometry, Idiom, Mock, Number, Object, Operation, Param, Part, Query, Range, Regex,
	Strand, Subquery, Table, Tables, Thing, Uuid,
};
use crate::sql::{Closure, ControlFlow, FlowResult, Ident, Kind};
use chrono::{DateTime, Utc};

use geo::Point;
use reblessive::tree::Stk;
use revision::revisioned;
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter, Write};
use std::ops::{Bound, Deref};

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Value";

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Values(pub Vec<Value>);

impl<V> From<V> for Values
where
	V: Into<Vec<Value>>,
{
	fn from(value: V) -> Self {
		Self(value.into())
	}
}

impl Deref for Values {
	type Target = Vec<Value>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl IntoIterator for Values {
	type Item = Value;
	type IntoIter = std::vec::IntoIter<Self::Item>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl Display for Values {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&Fmt::comma_separated(&self.0), f)
	}
}

impl InfoStructure for Values {
	fn structure(self) -> Value {
		self.into_iter().map(Value::structure).collect::<Vec<_>>().into()
	}
}

impl From<&Tables> for Values {
	fn from(tables: &Tables) -> Self {
		Self(tables.0.iter().map(|t| Value::Table(t.clone())).collect())
	}
}

/// Marker type for value conversions from Value::None
#[derive(Clone, Copy, Eq, PartialEq, PartialOrd)]
pub struct SqlNone;

/// Marker type for value conversions from Value::Null
#[derive(Clone, Copy, Eq, PartialEq, PartialOrd)]
pub struct Null;

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Value")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Value {
	// These value types are simple values which
	// can be used in query responses sent to
	// the client. They typically do not need to
	// be computed, unless an un-computed value
	// is present inside an Array or Object type.
	// These types can also be used within indexes
	// and sort according to their order below.
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
	Thing(Thing),
	// These Value types are un-computed values
	// and are not used in query responses sent
	// to the client. These types need to be
	// computed, in order to convert them into
	// one of the simple types listed above.
	// These types are first computed into a
	// simple type before being used in indexes.
	Param(Param),
	Idiom(Idiom),
	Table(Table),
	Mock(Mock),
	Regex(Regex),
	Cast(Box<Cast>),
	Block(Box<Block>),
	#[revision(end = 2, convert_fn = "convert_old_range", fields_name = "OldValueRangeFields")]
	Range(OldRange),
	#[revision(start = 2)]
	Range(Box<Range>),
	Edges(Box<Edges>),
	Future(Box<Future>),
	Constant(Constant),
	Function(Box<Function>),
	Subquery(Box<Subquery>),
	Expression(Box<Expression>),
	Query(Query),
	Model(Box<Model>),
	Closure(Box<Closure>),
	Refs(Refs),
	File(File),
	// Add new variants here
}

impl Value {
	fn convert_old_range(
		fields: OldValueRangeFields,
		_revision: u16,
	) -> Result<Self, revision::Error> {
		Ok(Value::Thing(Thing {
			tb: fields.0.tb,
			id: Id::Range(Box::new(IdRange {
				beg: fields.0.beg,
				end: fields.0.end,
			})),
		}))
	}
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
	pub fn base() -> Self {
		Value::Object(Object::default())
	}

	// -----------------------------------
	// Builtin types
	// -----------------------------------

	/// Convert this Value to a Result
	pub fn ok(self) -> Result<Value, Error> {
		Ok(self)
	}

	/// Convert this Value to an Option
	pub fn some(self) -> Option<Value> {
		match self {
			Value::None => None,
			val => Some(val),
		}
	}

	// -----------------------------------
	// Simple value detection
	// -----------------------------------

	/// Check if this Value is NONE or NULL
	pub fn is_none_or_null(&self) -> bool {
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

	/// Check if this Value not NONE or NULL
	pub fn is_some(&self) -> bool {
		!self.is_none() && !self.is_null()
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
			Value::Thing(_) => true,
			Value::Geometry(_) => true,
			Value::Datetime(_) => true,
			Value::Array(v) => !v.is_empty(),
			Value::Object(v) => !v.is_empty(),
			Value::Strand(v) => !v.is_empty(),
			Value::Number(v) => v.is_truthy(),
			Value::Duration(v) => v.as_nanos() > 0,
			_ => false,
		}
	}

	/// Check if this Value is a single Thing
	pub fn is_thing_single(&self) -> bool {
		match self {
			Value::Thing(t) => !matches!(t.id, Id::Range(_)),
			_ => false,
		}
	}

	/// Check if this Value is a single Thing
	pub fn is_thing_range(&self) -> bool {
		matches!(
			self,
			Value::Thing(Thing {
				id: Id::Range(_),
				..
			})
		)
	}

	/// Check if this Value is a Thing, and belongs to a certain table
	pub fn is_record_of_table(&self, table: String) -> bool {
		match self {
			Value::Thing(Thing {
				tb,
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
	pub fn is_record_type(&self, types: &[Table]) -> bool {
		match self {
			Value::Thing(v) => v.is_record_type(types),
			_ => false,
		}
	}

	/// Check if this Value is a Geometry of a specific type
	pub fn is_geometry_type(&self, types: &[String]) -> bool {
		match self {
			Value::Geometry(Geometry::Point(_)) => {
				types.iter().any(|t| matches!(t.as_str(), "feature" | "point"))
			}
			Value::Geometry(Geometry::Line(_)) => {
				types.iter().any(|t| matches!(t.as_str(), "feature" | "line"))
			}
			Value::Geometry(Geometry::Polygon(_)) => {
				types.iter().any(|t| matches!(t.as_str(), "feature" | "polygon"))
			}
			Value::Geometry(Geometry::MultiPoint(_)) => {
				types.iter().any(|t| matches!(t.as_str(), "feature" | "multipoint"))
			}
			Value::Geometry(Geometry::MultiLine(_)) => {
				types.iter().any(|t| matches!(t.as_str(), "feature" | "multiline"))
			}
			Value::Geometry(Geometry::MultiPolygon(_)) => {
				types.iter().any(|t| matches!(t.as_str(), "feature" | "multipolygon"))
			}
			Value::Geometry(Geometry::Collection(_)) => {
				types.iter().any(|t| matches!(t.as_str(), "feature" | "collection"))
			}
			_ => false,
		}
	}

	/// Returns if selecting on this value returns a single result.
	pub fn is_singular_selector(&self) -> bool {
		match self {
			Value::Object(_) => true,
			t @ Value::Thing(_) => t.is_thing_single(),
			_ => false,
		}
	}

	// -----------------------------------
	// Simple conversion of value
	// -----------------------------------

	/// Convert this Value into a String
	pub fn as_string(self) -> String {
		match self {
			Value::Strand(v) => v.0,
			Value::Uuid(v) => v.to_raw(),
			Value::Datetime(v) => v.to_raw(),
			_ => self.to_string(),
		}
	}

	/// Converts this Value into an unquoted String
	pub fn as_raw_string(self) -> String {
		match self {
			Value::Strand(v) => v.0,
			Value::Uuid(v) => v.to_raw(),
			Value::Datetime(v) => v.to_raw(),
			_ => self.to_string(),
		}
	}

	// -----------------------------------
	// Expensive conversion of value
	// -----------------------------------

	/// Converts this Value into an unquoted String
	pub fn to_raw_string(&self) -> String {
		match self {
			Value::Strand(v) => v.0.clone(),
			Value::Uuid(v) => v.to_raw(),
			Value::Datetime(v) => v.to_raw(),
			_ => self.to_string(),
		}
	}

	/// Converts this Value into a field name
	pub fn to_idiom(&self) -> Idiom {
		match self {
			Value::Idiom(v) => v.simplify(),
			Value::Param(v) => v.to_raw().into(),
			Value::Strand(v) => v.0.to_string().into(),
			Value::Datetime(v) => v.0.to_string().into(),
			Value::Future(_) => "future".to_string().into(),
			Value::Function(v) => v.to_idiom(),
			_ => self.to_string().into(),
		}
	}

	/// Returns if this value can be the start of a idiom production.
	pub fn can_start_idiom(&self) -> bool {
		match self {
			Value::Function(x) => !x.is_script(),
			Value::Model(_)
			| Value::Subquery(_)
			| Value::Constant(_)
			| Value::Datetime(_)
			| Value::Duration(_)
			| Value::Uuid(_)
			| Value::Number(_)
			| Value::Object(_)
			| Value::Array(_)
			| Value::Param(_)
			| Value::Edges(_)
			| Value::Thing(_)
			| Value::Table(_) => true,
			_ => false,
		}
	}

	/// Try to convert this Value into a set of JSONPatch operations
	pub fn to_operations(&self) -> Result<Vec<Operation>, Error> {
		match self {
			Value::Array(v) => v
				.iter()
				.map(|v| match v {
					Value::Object(v) => v.to_operation(),
					_ => Err(Error::InvalidPatch {
						message: String::from("Operation must be an object"),
					}),
				})
				.collect::<Result<Vec<_>, Error>>(),
			_ => Err(Error::InvalidPatch {
				message: String::from("Operations must be an array"),
			}),
		}
	}

	/// Converts a `surrealdb::sq::Value` into a `serde_json::Value`
	///
	/// This converts certain types like `Thing` into their simpler formats
	/// instead of the format used internally by SurrealDB.
	pub fn into_json(self) -> Json {
		self.into()
	}

	// -----------------------------------
	// Simple conversion of values
	// -----------------------------------

	/// Treat a string as a table name
	pub fn could_be_table(self) -> Value {
		match self {
			Value::Strand(v) => Value::Table(v.0.into()),
			_ => self,
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
			Value::Geometry(geo) => Some(Kind::Geometry(vec![geo.as_type().to_string()])),
			Value::Bytes(_) => Some(Kind::Bytes),
			Value::Thing(thing) => Some(Kind::Record(vec![thing.tb.clone().into()])),
			Value::Param(_) => None,
			Value::Idiom(_) => None,
			Value::Table(_) => None,
			Value::Mock(_) => None,
			Value::Regex(_) => None,
			Value::Cast(_) => None,
			Value::Block(_) => None,
			Value::Range(_) => None,
			Value::Edges(_) => None,
			Value::Future(_) => None,
			Value::Constant(_) => None,
			Value::Function(_) => None,
			Value::Subquery(_) => None,
			Value::Query(_) => None,
			Value::Model(_) => None,
			Value::Closure(_) => Some(Kind::Function(None, None)),
			Value::Refs(_) => None,
			Value::Expression(_) => None,
			Value::File(file) => Some(Kind::File(vec![Ident::from(file.bucket.as_str())])),
		}
	}

	/// Returns the surql representation of the kind of the value as a string.
	///
	/// # Warning
	/// This function is not fully implement for all variants, make sure you don't accidentally use
	/// it where it can return an invalid value.
	pub fn kindof(&self) -> &'static str {
		// TODO: Look at this function, there are a whole bunch of options for which this returns
		// "incorrect type" which might sneak into the results where it shouldn.t
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
			Self::Bytes(_) => "bytes",
			Self::Range(_) => "range",
			_ => "incorrect type",
		}
	}

	// -----------------------------------
	// Record ID extraction
	// -----------------------------------

	/// Fetch the record id if there is one
	pub fn record(self) -> Option<Thing> {
		match self {
			// This is an object so look for the id field
			Value::Object(mut v) => match v.remove("id") {
				Some(Value::Thing(v)) => Some(v),
				_ => None,
			},
			// This is an array so take the first item
			Value::Array(mut v) => match v.len() {
				1 => v.remove(0).record(),
				_ => None,
			},
			// This is a record id already
			Value::Thing(v) => Some(v),
			// There is no valid record id
			_ => None,
		}
	}

	// -----------------------------------
	// JSON Path conversion
	// -----------------------------------

	/// Converts this Value into a JSONPatch path
	pub(crate) fn jsonpath(&self) -> Idiom {
		self.to_raw_string()
			.as_str()
			.trim_start_matches('/')
			.split(&['.', '/'][..])
			.map(Part::from)
			.collect::<Vec<Part>>()
			.into()
	}

	// -----------------------------------
	// JSON Path conversion
	// -----------------------------------

	/// Checks whether this value is a static value
	pub(crate) fn is_static(&self) -> bool {
		match self {
			Value::None => true,
			Value::Null => true,
			Value::Bool(_) => true,
			Value::Bytes(_) => true,
			Value::Uuid(_) => true,
			Value::Thing(_) => true,
			Value::Number(_) => true,
			Value::Strand(_) => true,
			Value::Duration(_) => true,
			Value::Datetime(_) => true,
			Value::Geometry(_) => true,
			Value::Array(v) => v.is_static(),
			Value::Object(v) => v.is_static(),
			Value::Expression(v) => v.is_static(),
			Value::Function(v) => v.is_static(),
			Value::Cast(v) => v.is_static(),
			Value::Constant(_) => true,
			_ => false,
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
			Value::Thing(v) => match other {
				Value::Thing(w) => v == w,
				Value::Regex(w) => w.regex().is_match(v.to_raw().as_str()),
				_ => false,
			},
			Value::Strand(v) => match other {
				Value::Strand(w) => v == w,
				Value::Regex(w) => w.regex().is_match(v.as_str()),
				_ => false,
			},
			Value::Regex(v) => match other {
				Value::Regex(w) => v == w,
				Value::Thing(w) => v.regex().is_match(w.to_raw().as_str()),
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

	/// Fuzzy check if this Value is equal to another Value
	pub fn fuzzy(&self, other: &Value) -> bool {
		match self {
			Value::Uuid(v) => match other {
				Value::Strand(w) => v.to_raw().as_str().fuzzy_match(w.as_str()),
				_ => false,
			},
			Value::Strand(v) => match other {
				Value::Strand(w) => v.as_str().fuzzy_match(w.as_str()),
				_ => false,
			},
			_ => self.equal(other),
		}
	}

	/// Fuzzy check if all Values in an Array are equal to another Value
	pub fn all_fuzzy(&self, other: &Value) -> bool {
		match self {
			Value::Array(v) => v.iter().all(|v| v.fuzzy(other)),
			_ => self.fuzzy(other),
		}
	}

	/// Fuzzy check if any Values in an Array are equal to another Value
	pub fn any_fuzzy(&self, other: &Value) -> bool {
		match self {
			Value::Array(v) => v.iter().any(|v| v.fuzzy(other)),
			_ => self.fuzzy(other),
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
				Value::Strand(w) => v.0.contains_key(&w.0),
				_ => false,
			},
			Value::Range(r) => {
				let beg = match &r.beg {
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
					this.0.contains(&other_string.0)
				})
			}
			Value::Array(v) => v.iter().all(|v| match self {
				Value::Array(w) => w.iter().any(|w| v.equal(w)),
				Value::Geometry(_) => self.contains(v),
				_ => false,
			}),
			Value::Strand(other_strand) => match self {
				Value::Strand(s) => s.0.contains(&other_strand.0),
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
					this.0.contains(&other_string.0)
				})
			}
			Value::Array(v) => v.iter().any(|v| match self {
				Value::Array(w) => w.iter().any(|w| v.equal(w)),
				Value::Geometry(_) => self.contains(v),
				_ => false,
			}),
			Value::Strand(other_strand) => match self {
				Value::Strand(s) => s.0.contains(&other_strand.0),
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

	/// Compare this Value to another Value lexicographically and using natural numerical comparison
	pub fn natural_lexical_cmp(&self, other: &Value) -> Option<Ordering> {
		match (self, other) {
			(Value::Strand(a), Value::Strand(b)) => Some(lexicmp::natural_lexical_cmp(a, b)),
			_ => self.partial_cmp(other),
		}
	}

	pub fn can_be_range_bound(&self) -> bool {
		matches!(
			self,
			Value::None
				| Value::Null
				| Value::Array(_)
				| Value::Block(_)
				| Value::Bool(_)
				| Value::Datetime(_)
				| Value::Duration(_)
				| Value::Geometry(_)
				| Value::Number(_)
				| Value::Object(_)
				| Value::Param(_)
				| Value::Strand(_)
				| Value::Subquery(_)
				| Value::Table(_)
				| Value::Uuid(_)
		)
	}

	/// Validate that a Value is computed or contains only computed Values
	pub fn validate_computed(&self) -> Result<(), Error> {
		use Value::*;
		match self {
			None | Null | Bool(_) | Number(_) | Strand(_) | Duration(_) | Datetime(_) | Uuid(_)
			| Geometry(_) | Bytes(_) | Thing(_) => Ok(()),
			Array(a) => a.validate_computed(),
			Object(o) => o.validate_computed(),
			Range(r) => r.validate_computed(),
			_ => Err(Error::NonComputed),
		}
	}

	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		match self {
			Value::Cast(v) => v.writeable(),
			Value::Block(v) => v.writeable(),
			Value::Idiom(v) => v.writeable(),
			Value::Array(v) => v.iter().any(Value::writeable),
			Value::Object(v) => v.iter().any(|(_, v)| v.writeable()),
			Value::Function(v) => v.writeable(),
			Value::Model(m) => m.args.iter().any(Value::writeable),
			Value::Subquery(v) => v.writeable(),
			Value::Expression(v) => v.writeable(),
			_ => false,
		}
	}
	/// Process this type returning a computed simple Value.
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> FlowResult<Value> {
		// Prevent infinite recursion due to casting, expressions, etc.
		let opt = &opt.dive(1)?;

		let res = match self {
			Value::Cast(v) => return v.compute(stk, ctx, opt, doc).await,
			Value::Thing(v) => stk.run(|stk| v.compute(stk, ctx, opt, doc)).await,
			Value::Block(v) => return stk.run(|stk| v.compute(stk, ctx, opt, doc)).await,
			Value::Range(v) => return stk.run(|stk| v.compute(stk, ctx, opt, doc)).await,
			Value::Param(v) => stk.run(|stk| v.compute(stk, ctx, opt, doc)).await,
			Value::Idiom(v) => return stk.run(|stk| v.compute(stk, ctx, opt, doc)).await,
			Value::Array(v) => return stk.run(|stk| v.compute(stk, ctx, opt, doc)).await,
			Value::Object(v) => return stk.run(|stk| v.compute(stk, ctx, opt, doc)).await,
			Value::Future(v) => stk.run(|stk| v.compute(stk, ctx, opt, doc)).await,
			Value::Constant(v) => v.compute(),
			Value::Function(v) => return v.compute(stk, ctx, opt, doc).await,
			Value::Model(v) => return v.compute(stk, ctx, opt, doc).await,
			Value::Subquery(v) => return stk.run(|stk| v.compute(stk, ctx, opt, doc)).await,
			Value::Expression(v) => return stk.run(|stk| v.compute(stk, ctx, opt, doc)).await,
			Value::Refs(v) => v.compute(ctx, opt, doc).await,
			Value::Edges(v) => v.compute(stk, ctx, opt, doc).await,
			_ => Ok(self.to_owned()),
		};

		res.map_err(ControlFlow::from)
	}
}

impl fmt::Display for Value {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let mut f = Pretty::from(f);
		match self {
			Value::None => write!(f, "NONE"),
			Value::Null => write!(f, "NULL"),
			Value::Array(v) => write!(f, "{v}"),
			Value::Block(v) => write!(f, "{v}"),
			Value::Bool(v) => write!(f, "{v}"),
			Value::Bytes(v) => write!(f, "{v}"),
			Value::Cast(v) => write!(f, "{v}"),
			Value::Constant(v) => write!(f, "{v}"),
			Value::Datetime(v) => write!(f, "{v}"),
			Value::Duration(v) => write!(f, "{v}"),
			Value::Edges(v) => write!(f, "{v}"),
			Value::Expression(v) => write!(f, "{v}"),
			Value::Function(v) => write!(f, "{v}"),
			Value::Model(v) => write!(f, "{v}"),
			Value::Future(v) => write!(f, "{v}"),
			Value::Geometry(v) => write!(f, "{v}"),
			Value::Idiom(v) => write!(f, "{v}"),
			Value::Mock(v) => write!(f, "{v}"),
			Value::Number(v) => write!(f, "{v}"),
			Value::Object(v) => write!(f, "{v}"),
			Value::Param(v) => write!(f, "{v}"),
			Value::Range(v) => write!(f, "{v}"),
			Value::Regex(v) => write!(f, "{v}"),
			Value::Strand(v) => write!(f, "{v}"),
			Value::Query(v) => write!(f, "{v}"),
			Value::Subquery(v) => write!(f, "{v}"),
			Value::Table(v) => write!(f, "{v}"),
			Value::Thing(v) => write!(f, "{v}"),
			Value::Uuid(v) => write!(f, "{v}"),
			Value::Closure(v) => write!(f, "{v}"),
			Value::Refs(v) => write!(f, "{v}"),
			Value::File(v) => write!(f, "{v}"),
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
	fn try_add(self, rhs: Rhs) -> Result<Self::Output, Error>;
}

impl TryAdd for Value {
	type Output = Self;
	fn try_add(self, other: Self) -> Result<Self, Error> {
		Ok(match (self, other) {
			(Self::Number(v), Self::Number(w)) => Self::Number(v.try_add(w)?),
			(Self::Strand(v), Self::Strand(w)) => Self::Strand(v.try_add(w)?),
			(Self::Datetime(v), Self::Duration(w)) => Self::Datetime(w.try_add(v)?),
			(Self::Duration(v), Self::Datetime(w)) => Self::Datetime(v.try_add(w)?),
			(Self::Duration(v), Self::Duration(w)) => Self::Duration(v.try_add(w)?),
			(v, w) => return Err(Error::TryAdd(v.to_raw_string(), w.to_raw_string())),
		})
	}
}

// ------------------------------

pub(crate) trait TrySub<Rhs = Self> {
	type Output;
	fn try_sub(self, v: Rhs) -> Result<Self::Output, Error>;
}

impl TrySub for Value {
	type Output = Self;
	fn try_sub(self, other: Self) -> Result<Self, Error> {
		Ok(match (self, other) {
			(Self::Number(v), Self::Number(w)) => Self::Number(v.try_sub(w)?),
			(Self::Datetime(v), Self::Datetime(w)) => Self::Duration(v.try_sub(w)?),
			(Self::Datetime(v), Self::Duration(w)) => Self::Datetime(w.try_sub(v)?),
			(Self::Duration(v), Self::Datetime(w)) => Self::Datetime(v.try_sub(w)?),
			(Self::Duration(v), Self::Duration(w)) => Self::Duration(v.try_sub(w)?),
			(v, w) => return Err(Error::TrySub(v.to_raw_string(), w.to_raw_string())),
		})
	}
}

// ------------------------------

pub(crate) trait TryMul<Rhs = Self> {
	type Output;
	fn try_mul(self, v: Self) -> Result<Self::Output, Error>;
}

impl TryMul for Value {
	type Output = Self;
	fn try_mul(self, other: Self) -> Result<Self, Error> {
		Ok(match (self, other) {
			(Self::Number(v), Self::Number(w)) => Self::Number(v.try_mul(w)?),
			(v, w) => return Err(Error::TryMul(v.to_raw_string(), w.to_raw_string())),
		})
	}
}

// ------------------------------

pub(crate) trait TryDiv<Rhs = Self> {
	type Output;
	fn try_div(self, v: Self) -> Result<Self::Output, Error>;
}

impl TryDiv for Value {
	type Output = Self;
	fn try_div(self, other: Self) -> Result<Self, Error> {
		Ok(match (self, other) {
			(Self::Number(v), Self::Number(w)) => Self::Number(v.try_div(w)?),
			(v, w) => return Err(Error::TryDiv(v.to_raw_string(), w.to_raw_string())),
		})
	}
}

// ------------------------------

pub(crate) trait TryFloatDiv<Rhs = Self> {
	type Output;
	fn try_float_div(self, v: Self) -> Result<Self::Output, Error>;
}

impl TryFloatDiv for Value {
	type Output = Self;
	fn try_float_div(self, other: Self) -> Result<Self::Output, Error> {
		Ok(match (self, other) {
			(Self::Number(v), Self::Number(w)) => Self::Number(v.try_float_div(w)?),
			(v, w) => return Err(Error::TryDiv(v.to_raw_string(), w.to_raw_string())),
		})
	}
}

// ------------------------------

pub(crate) trait TryRem<Rhs = Self> {
	type Output;
	fn try_rem(self, v: Self) -> Result<Self::Output, Error>;
}

impl TryRem for Value {
	type Output = Self;
	fn try_rem(self, other: Self) -> Result<Self, Error> {
		Ok(match (self, other) {
			(Self::Number(v), Self::Number(w)) => Self::Number(v.try_rem(w)?),
			(v, w) => return Err(Error::TryRem(v.to_raw_string(), w.to_raw_string())),
		})
	}
}

// ------------------------------

pub(crate) trait TryPow<Rhs = Self> {
	type Output;
	fn try_pow(self, v: Self) -> Result<Self::Output, Error>;
}

impl TryPow for Value {
	type Output = Self;
	fn try_pow(self, other: Self) -> Result<Self, Error> {
		Ok(match (self, other) {
			(Value::Number(v), Value::Number(w)) => Self::Number(v.try_pow(w)?),
			(v, w) => return Err(Error::TryPow(v.to_raw_string(), w.to_raw_string())),
		})
	}
}

// ------------------------------

pub(crate) trait TryNeg<Rhs = Self> {
	type Output;
	fn try_neg(self) -> Result<Self::Output, Error>;
}

impl TryNeg for Value {
	type Output = Self;
	fn try_neg(self) -> Result<Self, Error> {
		Ok(match self {
			Self::Number(n) => Self::Number(n.try_neg()?),
			v => return Err(Error::TryNeg(v.to_string())),
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
			if let Value::$name(ref x) = self{
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
	Duration(Duration) => (is_duration,as_duration,into_duration),
	Datetime(Datetime) => (is_datetime,as_datetime,into_datetime),
	Uuid(Uuid) => (is_uuid,as_uuid,into_uuid),
	Array(Array) => (is_array,as_array,into_array),
	Object(Object) => (is_object,as_object,into_object),
	Geometry(Geometry) => (is_geometry,as_geometry,into_geometry),
	Bytes(Bytes) => (is_bytes,as_bytes,into_bytes),
	Thing(Thing) => (is_thing,as_thing,into_thing),
	Param(Param) => (is_param,as_param,into_param),
	Idiom(Idiom) => (is_idiom,as_idiom,into_idiom),
	Table(Table) => (is_table,as_table,into_table),
	Mock(Mock) => (is_mock,as_mock,into_mock),
	Regex(Regex) => (is_regex,as_regex,into_regex),
	Cast(Box<Cast>) => (is_cast,as_cast,into_cast),
	Block(Box<Block>) => (is_bock,as_block,into_block),
	Range(Box<Range>) => (is_range,as_range,into_range),
	Edges(Box<Edges>) => (is_edges,as_edges,into_edges),
	Future(Box<Future>) => (is_future,as_future,into_future),
	Constant(Constant) => (is_constant,as_constant,into_constant),
	Function(Box<Function>) => (is_function,as_function,into_function),
	Subquery(Box<Subquery>) => (is_subquery,as_subquery,into_subquery),
	Expression(Box<Expression>) => (is_expression,as_expression,into_expression),
	Query(Query) => (is_query,as_query,into_query),
	Model(Box<Model>) => (is_model,as_model,into_model),
	Closure(Box<Closure>) => (is_closure,as_closure,into_closure),
	Refs(Refs) => (is_refs,as_refs,into_refs),
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

impl<T> From<Vec<T>> for Value
where
	Value: From<T>,
{
	fn from(value: Vec<T>) -> Self {
		let v = value.into_iter().map(Value::from).collect();
		Value::Array(Array(v))
	}
}

impl<T> From<Option<T>> for Value
where
	Value: From<T>,
{
	fn from(value: Option<T>) -> Self {
		if let Some(x) = value {
			Value::from(x)
		} else {
			Value::None
		}
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

impl From<Operation> for Value {
	fn from(v: Operation) -> Self {
		Value::Object(Object::from(v))
	}
}

impl From<uuid::Uuid> for Value {
	fn from(v: uuid::Uuid) -> Self {
		Value::Uuid(Uuid(v))
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

impl From<IdRange> for Value {
	fn from(v: IdRange) -> Self {
		let beg = match v.beg {
			Bound::Included(beg) => Bound::Included(Value::from(beg)),
			Bound::Excluded(beg) => Bound::Excluded(Value::from(beg)),
			Bound::Unbounded => Bound::Unbounded,
		};

		let end = match v.end {
			Bound::Included(end) => Bound::Included(Value::from(end)),
			Bound::Excluded(end) => Bound::Excluded(Value::from(end)),
			Bound::Unbounded => Bound::Unbounded,
		};

		Value::Range(Box::new(Range {
			beg,
			end,
		}))
	}
}

impl From<Id> for Value {
	fn from(v: Id) -> Self {
		match v {
			Id::Number(v) => v.into(),
			Id::String(v) => v.into(),
			Id::Uuid(v) => v.into(),
			Id::Array(v) => v.into(),
			Id::Object(v) => v.into(),
			Id::Generate(v) => match v {
				Gen::Rand => Id::rand().into(),
				Gen::Ulid => Id::ulid().into(),
				Gen::Uuid => Id::uuid().into(),
			},
			Id::Range(v) => v.deref().to_owned().into(),
		}
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
	use crate::syn::Parse;

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
		assert_eq!(String::from("NONE"), Value::None.as_string());
		assert_eq!(String::from("NULL"), Value::Null.as_string());
		assert_eq!(String::from("true"), Value::Bool(true).as_string());
		assert_eq!(String::from("false"), Value::Bool(false).as_string());
		assert_eq!(String::from("0"), Value::from(0).as_string());
		assert_eq!(String::from("1"), Value::from(1).as_string());
		assert_eq!(String::from("-1"), Value::from(-1).as_string());
		assert_eq!(String::from("1.1f"), Value::from(1.1).as_string());
		assert_eq!(String::from("-1.1f"), Value::from(-1.1).as_string());
		assert_eq!(String::from("3"), Value::from("3").as_string());
		assert_eq!(String::from("true"), Value::from("true").as_string());
		assert_eq!(String::from("false"), Value::from("false").as_string());
		assert_eq!(String::from("something"), Value::from("something").as_string());
	}

	#[test]
	fn check_size() {
		assert!(64 >= std::mem::size_of::<Value>(), "size of value too big");
		assert!(104 >= std::mem::size_of::<Error>());
		assert!(104 >= std::mem::size_of::<Result<Value, Error>>());
		assert!(24 >= std::mem::size_of::<crate::sql::number::Number>());
		assert!(24 >= std::mem::size_of::<crate::sql::strand::Strand>());
		assert!(16 >= std::mem::size_of::<crate::sql::duration::Duration>());
		assert!(12 >= std::mem::size_of::<crate::sql::datetime::Datetime>());
		assert!(24 >= std::mem::size_of::<crate::sql::array::Array>());
		assert!(24 >= std::mem::size_of::<crate::sql::object::Object>());
		assert!(48 >= std::mem::size_of::<crate::sql::geometry::Geometry>());
		assert!(24 >= std::mem::size_of::<crate::sql::param::Param>());
		assert!(24 >= std::mem::size_of::<crate::sql::idiom::Idiom>());
		assert!(24 >= std::mem::size_of::<crate::sql::table::Table>());
		assert!(56 >= std::mem::size_of::<crate::sql::thing::Thing>());
		assert!(40 >= std::mem::size_of::<crate::sql::mock::Mock>());
		assert!(32 >= std::mem::size_of::<crate::sql::regex::Regex>());
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
		let enc: Vec<u8> = revision::to_vec(&Value::parse("{ hello: 'world' }")).unwrap();
		assert_eq!(19, enc.len());
		let enc: Vec<u8> = revision::to_vec(&Value::parse("{ compact: true, schema: 0 }")).unwrap();
		assert_eq!(27, enc.len());
	}

	#[test]
	fn serialize_deserialize() {
		let val = Value::parse(
			"{ test: { something: [1, 'two', null, test:tobie, { trueee: false, noneee: nulll }] } }",
		);
		let res = Value::parse(
			"{ test: { something: [1, 'two', null, test:tobie, { trueee: false, noneee: nulll }] } }",
		);
		let enc: Vec<u8> = revision::to_vec(&val).unwrap();
		let dec: Value = revision::from_slice(&enc).unwrap();
		assert_eq!(res, dec);
	}

	#[test]
	fn test_value_from_vec_i32() {
		let vector: Vec<i32> = vec![1, 2, 3, 4, 5, 6];
		let value = Value::from(vector);
		assert!(matches!(value, Value::Array(Array(_))));
	}

	#[test]
	fn test_value_from_vec_f32() {
		let vector: Vec<f32> = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
		let value = Value::from(vector);
		assert!(matches!(value, Value::Array(Array(_))));
	}
}
