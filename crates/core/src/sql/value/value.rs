#![allow(clippy::derive_ord_xor_partial_ord)]

use crate::err::Error;
use crate::sql::id::range::IdRange;
use crate::sql::range::OldRange;
use crate::sql::reference::Refs;

use crate::sql::{
	Array, Block, Bytes, Cast, Constant, Datetime, Duration, Edges, Expression, File, Function,
	Future, Geometry, Idiom, Mock, Number, Object, Operation, Param, Part, Query, Range, Regex,
	Strand, Subquery, Table, Tables, Thing, Uuid,
	fmt::{Fmt, Pretty},
	id::{Gen, Id},
	model::Model,
};
use crate::sql::{Closure, Ident, Kind, ToSql};
use anyhow::{Result, bail};
use chrono::{DateTime, Utc};

use geo::Point;
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
pub struct SqlValues(pub Vec<SqlValue>);

impl<V> From<V> for SqlValues
where
	V: Into<Vec<SqlValue>>,
{
	fn from(value: V) -> Self {
		Self(value.into())
	}
}

impl Deref for SqlValues {
	type Target = Vec<SqlValue>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl IntoIterator for SqlValues {
	type Item = SqlValue;
	type IntoIter = std::vec::IntoIter<Self::Item>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl Display for SqlValues {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&Fmt::comma_separated(&self.0), f)
	}
}

impl From<&Tables> for SqlValues {
	fn from(tables: &Tables) -> Self {
		Self(tables.0.iter().map(|t| SqlValue::Table(t.clone())).collect())
	}
}

impl From<SqlValues> for crate::expr::Values {
	fn from(v: SqlValues) -> Self {
		Self(v.0.into_iter().map(Into::into).collect())
	}
}

impl From<crate::expr::Values> for SqlValues {
	fn from(v: crate::expr::Values) -> Self {
		Self(v.0.into_iter().map(Into::into).collect())
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
pub enum SqlValue {
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

impl SqlValue {
	fn convert_old_range(
		fields: OldValueRangeFields,
		_revision: u16,
	) -> Result<Self, revision::Error> {
		Ok(SqlValue::RecordId(Thing {
			tb: fields.0.tb,
			id: Id::Range(Box::new(IdRange {
				beg: fields.0.beg,
				end: fields.0.end,
			})),
		}))
	}

	pub(crate) fn get_field_value(&self, name: &str) -> SqlValue {
		match self {
			SqlValue::Object(v) => v.get(name).cloned().unwrap_or(SqlValue::None),
			_ => SqlValue::None,
		}
	}
}

impl Eq for SqlValue {}

impl Ord for SqlValue {
	fn cmp(&self, other: &Self) -> Ordering {
		self.partial_cmp(other).unwrap_or(Ordering::Equal)
	}
}

impl SqlValue {
	// -----------------------------------
	// Initial record value
	// -----------------------------------

	/// Create an empty Object Value
	pub fn base() -> Self {
		SqlValue::Object(Object::default())
	}

	// -----------------------------------
	// Builtin types
	// -----------------------------------

	/// Convert this Value to a Result
	pub fn ok(self) -> Result<SqlValue> {
		Ok(self)
	}

	/// Convert this Value to an Option
	pub fn some(self) -> Option<SqlValue> {
		match self {
			SqlValue::None => None,
			val => Some(val),
		}
	}

	// -----------------------------------
	// Simple value detection
	// -----------------------------------

	/// Check if this Value is NONE or NULL
	pub fn is_none_or_null(&self) -> bool {
		matches!(self, SqlValue::None | SqlValue::Null)
	}

	/// Check if this Value is NONE
	pub fn is_empty_array(&self) -> bool {
		if let SqlValue::Array(v) = self {
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
		matches!(self, SqlValue::Bool(true))
	}

	/// Check if this Value is FALSE or 'false'
	pub fn is_false(&self) -> bool {
		matches!(self, SqlValue::Bool(false))
	}

	/// Check if this Value is truthy
	pub fn is_truthy(&self) -> bool {
		match self {
			SqlValue::Bool(v) => *v,
			SqlValue::Uuid(_) => true,
			SqlValue::RecordId(_) => true,
			SqlValue::Geometry(_) => true,
			SqlValue::Datetime(_) => true,
			SqlValue::Array(v) => !v.is_empty(),
			SqlValue::Object(v) => !v.is_empty(),
			SqlValue::Strand(v) => !v.is_empty(),
			SqlValue::Number(v) => v.is_truthy(),
			SqlValue::Duration(v) => v.as_nanos() > 0,
			_ => false,
		}
	}

	/// Check if this Value is a single Thing
	pub fn is_thing_single(&self) -> bool {
		match self {
			SqlValue::RecordId(t) => !matches!(t.id, Id::Range(_)),
			_ => false,
		}
	}

	/// Check if this Value is a single Thing
	pub fn is_thing_range(&self) -> bool {
		matches!(
			self,
			SqlValue::RecordId(Thing {
				id: Id::Range(_),
				..
			})
		)
	}

	/// Check if this Value is a Thing, and belongs to a certain table
	pub fn is_record_of_table(&self, table: String) -> bool {
		match self {
			SqlValue::RecordId(Thing {
				tb,
				..
			}) => *tb == table,
			_ => false,
		}
	}

	/// Check if this Value is an int Number
	pub fn is_int(&self) -> bool {
		matches!(self, SqlValue::Number(Number::Int(_)))
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
		matches!(self, SqlValue::Number(Number::Float(_)))
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
		matches!(self, SqlValue::Number(Number::Decimal(_)))
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
			SqlValue::RecordId(v) => v.is_record_type(types),
			_ => false,
		}
	}

	/// Check if this Value is a Geometry of a specific type
	pub fn is_geometry_type(&self, types: &[String]) -> bool {
		match self {
			SqlValue::Geometry(Geometry::Point(_)) => {
				types.iter().any(|t| matches!(t.as_str(), "feature" | "point"))
			}
			SqlValue::Geometry(Geometry::Line(_)) => {
				types.iter().any(|t| matches!(t.as_str(), "feature" | "line"))
			}
			SqlValue::Geometry(Geometry::Polygon(_)) => {
				types.iter().any(|t| matches!(t.as_str(), "feature" | "polygon"))
			}
			SqlValue::Geometry(Geometry::MultiPoint(_)) => {
				types.iter().any(|t| matches!(t.as_str(), "feature" | "multipoint"))
			}
			SqlValue::Geometry(Geometry::MultiLine(_)) => {
				types.iter().any(|t| matches!(t.as_str(), "feature" | "multiline"))
			}
			SqlValue::Geometry(Geometry::MultiPolygon(_)) => {
				types.iter().any(|t| matches!(t.as_str(), "feature" | "multipolygon"))
			}
			SqlValue::Geometry(Geometry::Collection(_)) => {
				types.iter().any(|t| matches!(t.as_str(), "feature" | "collection"))
			}
			_ => false,
		}
	}

	/// Returns if selecting on this value returns a single result.
	pub fn is_singular_selector(&self) -> bool {
		match self {
			SqlValue::Object(_) => true,
			t @ SqlValue::RecordId(_) => t.is_thing_single(),
			_ => false,
		}
	}

	// -----------------------------------
	// Simple conversion of value
	// -----------------------------------

	/// Convert this Value into a String
	pub fn as_string(self) -> String {
		match self {
			SqlValue::Strand(v) => v.0,
			SqlValue::Uuid(v) => v.to_raw(),
			SqlValue::Datetime(v) => v.to_raw(),
			_ => self.to_string(),
		}
	}

	/// Converts this Value into an unquoted String
	pub fn as_raw_string(self) -> String {
		match self {
			SqlValue::Strand(v) => v.0,
			SqlValue::Uuid(v) => v.to_raw(),
			SqlValue::Datetime(v) => v.to_raw(),
			_ => self.to_string(),
		}
	}

	// -----------------------------------
	// Expensive conversion of value
	// -----------------------------------

	/// Converts this Value into an unquoted String
	pub fn to_raw_string(&self) -> String {
		match self {
			SqlValue::Strand(v) => v.0.clone(),
			SqlValue::Uuid(v) => v.to_raw(),
			SqlValue::Datetime(v) => v.to_raw(),
			_ => self.to_string(),
		}
	}

	/// Converts this Value into a field name
	pub fn to_idiom(&self) -> Idiom {
		match self {
			SqlValue::Idiom(v) => v.simplify(),
			SqlValue::Param(v) => v.to_raw().into(),
			SqlValue::Strand(v) => v.0.to_string().into(),
			SqlValue::Datetime(v) => v.0.to_string().into(),
			SqlValue::Future(_) => "future".to_string().into(),
			SqlValue::Function(v) => v.to_idiom(),
			_ => self.to_string().into(),
		}
	}

	/// Returns if this value can be the start of a idiom production.
	pub fn can_start_idiom(&self) -> bool {
		match self {
			SqlValue::Function(x) => !x.is_script(),
			SqlValue::Model(_)
			| SqlValue::Subquery(_)
			| SqlValue::Constant(_)
			| SqlValue::Datetime(_)
			| SqlValue::Duration(_)
			| SqlValue::Uuid(_)
			| SqlValue::Number(_)
			| SqlValue::Object(_)
			| SqlValue::Array(_)
			| SqlValue::Param(_)
			| SqlValue::Edges(_)
			| SqlValue::RecordId(_)
			| SqlValue::Table(_) => true,
			_ => false,
		}
	}

	/// Try to convert this Value into a set of JSONPatch operations
	pub fn to_operations(&self) -> Result<Vec<Operation>> {
		match self {
			SqlValue::Array(v) => v
				.iter()
				.map(|v| match v {
					SqlValue::Object(v) => v.to_operation(),
					_ => Err(anyhow::Error::new(Error::InvalidPatch {
						message: String::from("Operation must be an object"),
					})),
				})
				.collect::<Result<Vec<_>>>(),
			_ => Err(anyhow::Error::new(Error::InvalidPatch {
				message: String::from("Operations must be an array"),
			})),
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
	pub fn could_be_table(self) -> SqlValue {
		match self {
			SqlValue::Strand(v) => SqlValue::Table(v.0.into()),
			_ => self,
		}
	}

	// -----------------------------------
	// Simple output of value type
	// -----------------------------------

	pub fn kind(&self) -> Option<Kind> {
		match self {
			SqlValue::None => None,
			SqlValue::Null => Some(Kind::Null),
			SqlValue::Bool(_) => Some(Kind::Bool),
			SqlValue::Number(_) => Some(Kind::Number),
			SqlValue::Strand(_) => Some(Kind::String),
			SqlValue::Duration(_) => Some(Kind::Duration),
			SqlValue::Datetime(_) => Some(Kind::Datetime),
			SqlValue::Uuid(_) => Some(Kind::Uuid),
			SqlValue::Array(arr) => Some(Kind::Array(
				Box::new(arr.first().and_then(|v| v.kind()).unwrap_or_default()),
				None,
			)),
			SqlValue::Object(_) => Some(Kind::Object),
			SqlValue::Geometry(geo) => Some(Kind::Geometry(vec![geo.as_type().to_string()])),
			SqlValue::Bytes(_) => Some(Kind::Bytes),
			SqlValue::RecordId(thing) => Some(Kind::Record(vec![thing.tb.clone().into()])),
			SqlValue::Param(_) => None,
			SqlValue::Idiom(_) => None,
			SqlValue::Table(_) => None,
			SqlValue::Mock(_) => None,
			SqlValue::Regex(_) => None,
			SqlValue::Cast(_) => None,
			SqlValue::Block(_) => None,
			SqlValue::Range(_) => None,
			SqlValue::Edges(_) => None,
			SqlValue::Future(_) => None,
			SqlValue::Constant(_) => None,
			SqlValue::Function(_) => None,
			SqlValue::Subquery(_) => None,
			SqlValue::Query(_) => None,
			SqlValue::Model(_) => None,
			SqlValue::Closure(closure) => {
				let args_kinds =
					closure.args.iter().map(|(_, kind)| kind.clone()).collect::<Vec<_>>();
				let returns_kind = closure.returns.clone().map(Box::new);

				Some(Kind::Function(Some(args_kinds), returns_kind))
			}
			SqlValue::Refs(_) => None,
			SqlValue::Expression(_) => None,
			SqlValue::File(file) => Some(Kind::File(vec![Ident::from(file.bucket.as_str())])),
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
			SqlValue::Object(mut v) => match v.remove("id") {
				Some(SqlValue::RecordId(v)) => Some(v),
				_ => None,
			},
			// This is an array so take the first item
			SqlValue::Array(mut v) => match v.len() {
				1 => v.remove(0).record(),
				_ => None,
			},
			// This is a record id already
			SqlValue::RecordId(v) => Some(v),
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
			SqlValue::None => true,
			SqlValue::Null => true,
			SqlValue::Bool(_) => true,
			SqlValue::Bytes(_) => true,
			SqlValue::Uuid(_) => true,
			SqlValue::RecordId(_) => true,
			SqlValue::Number(_) => true,
			SqlValue::Strand(_) => true,
			SqlValue::Duration(_) => true,
			SqlValue::Datetime(_) => true,
			SqlValue::Geometry(_) => true,
			SqlValue::Array(v) => v.is_static(),
			SqlValue::Object(v) => v.is_static(),
			SqlValue::Expression(v) => v.is_static(),
			SqlValue::Function(v) => v.is_static(),
			SqlValue::Cast(v) => v.is_static(),
			SqlValue::Constant(_) => true,
			_ => false,
		}
	}

	// -----------------------------------
	// Value operations
	// -----------------------------------

	/// Check if this Value is equal to another Value
	pub fn equal(&self, other: &SqlValue) -> bool {
		match self {
			SqlValue::None => other.is_none(),
			SqlValue::Null => other.is_null(),
			SqlValue::Bool(v) => match other {
				SqlValue::Bool(w) => v == w,
				_ => false,
			},
			SqlValue::Uuid(v) => match other {
				SqlValue::Uuid(w) => v == w,
				_ => false,
			},
			SqlValue::RecordId(v) => match other {
				SqlValue::RecordId(w) => v == w,
				SqlValue::Regex(w) => w.regex().is_match(v.to_raw().as_str()),
				_ => false,
			},
			SqlValue::Strand(v) => match other {
				SqlValue::Strand(w) => v == w,
				SqlValue::Regex(w) => w.regex().is_match(v.as_str()),
				_ => false,
			},
			SqlValue::Regex(v) => match other {
				SqlValue::Regex(w) => v == w,
				SqlValue::RecordId(w) => v.regex().is_match(w.to_raw().as_str()),
				SqlValue::Strand(w) => v.regex().is_match(w.as_str()),
				_ => false,
			},
			SqlValue::Array(v) => match other {
				SqlValue::Array(w) => v == w,
				_ => false,
			},
			SqlValue::Object(v) => match other {
				SqlValue::Object(w) => v == w,
				_ => false,
			},
			SqlValue::Number(v) => match other {
				SqlValue::Number(w) => v == w,
				_ => false,
			},
			SqlValue::Geometry(v) => match other {
				SqlValue::Geometry(w) => v == w,
				_ => false,
			},
			SqlValue::Duration(v) => match other {
				SqlValue::Duration(w) => v == w,
				_ => false,
			},
			SqlValue::Datetime(v) => match other {
				SqlValue::Datetime(w) => v == w,
				_ => false,
			},
			_ => self == other,
		}
	}

	/// Check if all Values in an Array are equal to another Value
	pub fn all_equal(&self, other: &SqlValue) -> bool {
		match self {
			SqlValue::Array(v) => v.iter().all(|v| v.equal(other)),
			_ => self.equal(other),
		}
	}

	/// Check if any Values in an Array are equal to another Value
	pub fn any_equal(&self, other: &SqlValue) -> bool {
		match self {
			SqlValue::Array(v) => v.iter().any(|v| v.equal(other)),
			_ => self.equal(other),
		}
	}

	/// Check if this Value contains another Value
	pub fn contains(&self, other: &SqlValue) -> bool {
		match self {
			SqlValue::Array(v) => v.iter().any(|v| v.equal(other)),
			SqlValue::Uuid(v) => match other {
				SqlValue::Strand(w) => v.to_raw().contains(w.as_str()),
				_ => false,
			},
			SqlValue::Strand(v) => match other {
				SqlValue::Strand(w) => v.contains(w.as_str()),
				_ => false,
			},
			SqlValue::Geometry(v) => match other {
				SqlValue::Geometry(w) => v.contains(w),
				_ => false,
			},
			SqlValue::Object(v) => match other {
				SqlValue::Strand(w) => v.0.contains_key(&w.0),
				_ => false,
			},
			SqlValue::Range(r) => {
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
	pub fn contains_all(&self, other: &SqlValue) -> bool {
		match other {
			SqlValue::Array(v) if v.iter().all(|v| v.is_strand()) && self.is_strand() => {
				// confirmed as strand so all return false is unreachable
				let SqlValue::Strand(this) = self else {
					return false;
				};
				v.iter().all(|s| {
					let SqlValue::Strand(other_string) = s else {
						return false;
					};
					this.0.contains(&other_string.0)
				})
			}
			SqlValue::Array(v) => v.iter().all(|v| match self {
				SqlValue::Array(w) => w.iter().any(|w| v.equal(w)),
				SqlValue::Geometry(_) => self.contains(v),
				_ => false,
			}),
			SqlValue::Strand(other_strand) => match self {
				SqlValue::Strand(s) => s.0.contains(&other_strand.0),
				_ => false,
			},
			_ => false,
		}
	}

	/// Check if any Values in an Array contain another Value
	pub fn contains_any(&self, other: &SqlValue) -> bool {
		match other {
			SqlValue::Array(v) if v.iter().all(|v| v.is_strand()) && self.is_strand() => {
				// confirmed as strand so all return false is unreachable
				let SqlValue::Strand(this) = self else {
					return false;
				};
				v.iter().any(|s| {
					let SqlValue::Strand(other_string) = s else {
						return false;
					};
					this.0.contains(&other_string.0)
				})
			}
			SqlValue::Array(v) => v.iter().any(|v| match self {
				SqlValue::Array(w) => w.iter().any(|w| v.equal(w)),
				SqlValue::Geometry(_) => self.contains(v),
				_ => false,
			}),
			SqlValue::Strand(other_strand) => match self {
				SqlValue::Strand(s) => s.0.contains(&other_strand.0),
				_ => false,
			},
			_ => false,
		}
	}

	/// Check if this Value intersects another Value
	pub fn intersects(&self, other: &SqlValue) -> bool {
		match self {
			SqlValue::Geometry(v) => match other {
				SqlValue::Geometry(w) => v.intersects(w),
				_ => false,
			},
			_ => false,
		}
	}

	// -----------------------------------
	// Sorting operations
	// -----------------------------------

	/// Compare this Value to another Value lexicographically
	pub fn lexical_cmp(&self, other: &SqlValue) -> Option<Ordering> {
		match (self, other) {
			(SqlValue::Strand(a), SqlValue::Strand(b)) => Some(lexicmp::lexical_cmp(a, b)),
			_ => self.partial_cmp(other),
		}
	}

	/// Compare this Value to another Value using natural numerical comparison
	pub fn natural_cmp(&self, other: &SqlValue) -> Option<Ordering> {
		match (self, other) {
			(SqlValue::Strand(a), SqlValue::Strand(b)) => Some(lexicmp::natural_cmp(a, b)),
			_ => self.partial_cmp(other),
		}
	}

	/// Compare this Value to another Value lexicographically and using natural numerical comparison
	pub fn natural_lexical_cmp(&self, other: &SqlValue) -> Option<Ordering> {
		match (self, other) {
			(SqlValue::Strand(a), SqlValue::Strand(b)) => Some(lexicmp::natural_lexical_cmp(a, b)),
			_ => self.partial_cmp(other),
		}
	}

	/// Validate that a Value is computed or contains only computed Values
	pub fn validate_computed(&self) -> Result<()> {
		use SqlValue::*;
		match self {
			None | Null | Bool(_) | Number(_) | Strand(_) | Duration(_) | Datetime(_) | Uuid(_)
			| Geometry(_) | Bytes(_) | Thing(_) => Ok(()),
			Array(a) => a.validate_computed(),
			Object(o) => o.validate_computed(),
			Range(r) => r.validate_computed(),
			_ => Err(anyhow::Error::new(Error::NonComputed)),
		}
	}
}

impl fmt::Display for SqlValue {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let mut f = Pretty::from(f);
		match self {
			SqlValue::None => write!(f, "NONE"),
			SqlValue::Null => write!(f, "NULL"),
			SqlValue::Array(v) => write!(f, "{v}"),
			SqlValue::Block(v) => write!(f, "{v}"),
			SqlValue::Bool(v) => write!(f, "{v}"),
			SqlValue::Bytes(v) => write!(f, "{v}"),
			SqlValue::Cast(v) => write!(f, "{v}"),
			SqlValue::Constant(v) => write!(f, "{v}"),
			SqlValue::Datetime(v) => write!(f, "{v}"),
			SqlValue::Duration(v) => write!(f, "{v}"),
			SqlValue::Edges(v) => write!(f, "{v}"),
			SqlValue::Expression(v) => write!(f, "{v}"),
			SqlValue::Function(v) => write!(f, "{v}"),
			SqlValue::Model(v) => write!(f, "{v}"),
			SqlValue::Future(v) => write!(f, "{v}"),
			SqlValue::Geometry(v) => write!(f, "{v}"),
			SqlValue::Idiom(v) => write!(f, "{v}"),
			SqlValue::Mock(v) => write!(f, "{v}"),
			SqlValue::Number(v) => write!(f, "{v}"),
			SqlValue::Object(v) => write!(f, "{v}"),
			SqlValue::Param(v) => write!(f, "{v}"),
			SqlValue::Range(v) => write!(f, "{v}"),
			SqlValue::Regex(v) => write!(f, "{v}"),
			SqlValue::Strand(v) => write!(f, "{}", v.to_sql()),
			SqlValue::Query(v) => write!(f, "{v}"),
			SqlValue::Subquery(v) => write!(f, "{v}"),
			SqlValue::Table(v) => write!(f, "{v}"),
			SqlValue::RecordId(v) => write!(f, "{v}"),
			SqlValue::Uuid(v) => write!(f, "{v}"),
			SqlValue::Closure(v) => write!(f, "{v}"),
			SqlValue::Refs(v) => write!(f, "{v}"),
			SqlValue::File(v) => write!(f, "{v}"),
		}
	}
}

impl From<SqlValue> for crate::expr::Value {
	fn from(v: SqlValue) -> Self {
		match v {
			SqlValue::None => crate::expr::Value::None,
			SqlValue::Null => crate::expr::Value::Null,
			SqlValue::Bool(v) => crate::expr::Value::Bool(v),
			SqlValue::Number(v) => crate::expr::Value::Number(v.into()),
			SqlValue::Strand(v) => crate::expr::Value::Strand(v.into()),
			SqlValue::Duration(v) => crate::expr::Value::Duration(v.into()),
			SqlValue::Datetime(v) => crate::expr::Value::Datetime(v.into()),
			SqlValue::Uuid(v) => crate::expr::Value::Uuid(v.into()),
			SqlValue::Array(v) => crate::expr::Value::Array(v.into()),
			SqlValue::Object(v) => crate::expr::Value::Object(v.into()),
			SqlValue::Geometry(v) => crate::expr::Value::Geometry(v.into()),
			SqlValue::Bytes(v) => crate::expr::Value::Bytes(v.into()),
			SqlValue::RecordId(v) => crate::expr::Value::RecordId(v.into()),
			SqlValue::Param(v) => crate::expr::Value::Param(v.into()),
			SqlValue::Idiom(v) => crate::expr::Value::Idiom(v.into()),
			SqlValue::Table(v) => crate::expr::Value::Table(v.into()),
			SqlValue::Mock(v) => crate::expr::Value::Mock(v.into()),
			SqlValue::Regex(v) => crate::expr::Value::Regex(v.into()),
			SqlValue::Cast(v) => crate::expr::Value::Cast(Box::new((*v).into())),
			SqlValue::Block(v) => crate::expr::Value::Block(Box::new((*v).into())),
			SqlValue::Range(v) => crate::expr::Value::Range(Box::new((*v).into())),
			SqlValue::Edges(v) => crate::expr::Value::Edges(Box::new((*v).into())),
			SqlValue::Future(v) => crate::expr::Value::Future(Box::new((*v).into())),
			SqlValue::Constant(v) => crate::expr::Value::Constant(v.into()),
			SqlValue::Function(v) => crate::expr::Value::Function(Box::new((*v).into())),
			SqlValue::Model(v) => crate::expr::Value::Model(Box::new((*v).into())),
			SqlValue::Subquery(v) => crate::expr::Value::Subquery(Box::new((*v).into())),
			SqlValue::Expression(v) => crate::expr::Value::Expression(Box::new((*v).into())),
			SqlValue::Query(v) => crate::expr::Value::Query(v.into()),
			SqlValue::Closure(v) => crate::expr::Value::Closure(Box::new((*v).into())),
			SqlValue::Refs(v) => crate::expr::Value::Refs(v.into()),
			SqlValue::File(v) => crate::expr::Value::File(v.into()),
		}
	}
}

impl From<crate::expr::Value> for SqlValue {
	fn from(v: crate::expr::Value) -> Self {
		match v {
			crate::expr::Value::None => SqlValue::None,
			crate::expr::Value::Null => SqlValue::Null,
			crate::expr::Value::Bool(v) => SqlValue::Bool(v),
			crate::expr::Value::Number(v) => SqlValue::Number(v.into()),
			crate::expr::Value::Strand(v) => SqlValue::Strand(v.into()),
			crate::expr::Value::Duration(v) => SqlValue::Duration(v.into()),
			crate::expr::Value::Datetime(v) => SqlValue::Datetime(v.into()),
			crate::expr::Value::Uuid(v) => SqlValue::Uuid(v.into()),
			crate::expr::Value::Array(v) => SqlValue::Array(v.into()),
			crate::expr::Value::Object(v) => SqlValue::Object(v.into()),
			crate::expr::Value::Geometry(v) => SqlValue::Geometry(v.into()),
			crate::expr::Value::Bytes(v) => SqlValue::Bytes(v.into()),
			crate::expr::Value::RecordId(v) => SqlValue::RecordId(v.into()),
			crate::expr::Value::Param(v) => SqlValue::Param(v.into()),
			crate::expr::Value::Idiom(v) => SqlValue::Idiom(v.into()),
			crate::expr::Value::Table(v) => SqlValue::Table(v.into()),
			crate::expr::Value::Mock(v) => SqlValue::Mock(v.into()),
			crate::expr::Value::Regex(v) => SqlValue::Regex(v.into()),
			crate::expr::Value::Cast(v) => SqlValue::Cast(Box::new((*v).into())),
			crate::expr::Value::Block(v) => SqlValue::Block(Box::new((*v).into())),
			crate::expr::Value::Range(v) => SqlValue::Range(Box::new((*v).into())),
			crate::expr::Value::Edges(v) => SqlValue::Edges(Box::new((*v).into())),
			crate::expr::Value::Future(v) => SqlValue::Future(Box::new((*v).into())),
			crate::expr::Value::Constant(v) => SqlValue::Constant(v.into()),
			crate::expr::Value::Function(v) => SqlValue::Function(Box::new((*v).into())),
			crate::expr::Value::Model(v) => SqlValue::Model(Box::new((*v).into())),
			crate::expr::Value::Subquery(v) => SqlValue::Subquery(Box::new((*v).into())),
			crate::expr::Value::Expression(v) => SqlValue::Expression(Box::new((*v).into())),
			crate::expr::Value::Query(v) => SqlValue::Query(v.into()),
			crate::expr::Value::Closure(v) => SqlValue::Closure(Box::new((*v).into())),
			crate::expr::Value::Refs(v) => SqlValue::Refs(v.into()),
			crate::expr::Value::File(v) => SqlValue::File(v.into()),
		}
	}
}

// ------------------------------

pub(crate) trait TryAdd<Rhs = Self> {
	type Output;
	fn try_add(self, rhs: Rhs) -> Result<Self::Output>;
}

use std::ops::Add;

impl TryAdd for SqlValue {
	type Output = Self;
	fn try_add(self, other: Self) -> Result<Self> {
		Ok(match (self, other) {
			(Self::Number(v), Self::Number(w)) => Self::Number(v.try_add(w)?),
			(Self::Strand(v), Self::Strand(w)) => Self::Strand(v.try_add(w)?),
			(Self::Datetime(v), Self::Duration(w)) => Self::Datetime(w.try_add(v)?),
			(Self::Duration(v), Self::Datetime(w)) => Self::Datetime(v.try_add(w)?),
			(Self::Duration(v), Self::Duration(w)) => Self::Duration(v.try_add(w)?),
			(Self::Array(v), Self::Array(w)) => Self::Array(v.add(w)),
			(Self::Object(v), Self::Object(w)) => Self::Object(v.add(w)),
			(v, w) => bail!(Error::TryAdd(v.to_raw_string(), w.to_raw_string())),
		})
	}
}

// ------------------------------

pub(crate) trait TryNeg<Rhs = Self> {
	type Output;
	fn try_neg(self) -> Result<Self::Output>;
}

impl TryNeg for SqlValue {
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

		impl SqlValue {

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
		SqlValue::$name(_)
	};

	(@pat $name:ident) => {
		SqlValue::$name
	};

	(@method $name:ident($t:ty) => $is:ident,$as:ident,$into:ident) => {
		#[doc = concat!("Check if the value is a [`",stringify!($name),"`]")]
		pub fn $is(&self) -> bool{
			matches!(self,SqlValue::$name(_))
		}

		#[doc = concat!("Return a reference to [`",stringify!($name),"`] if the value is of that type")]
		pub fn $as(&self) -> Option<&$t>{
			if let SqlValue::$name(x) = self{
				Some(x)
			}else{
				None
			}
		}

		#[doc = concat!("Turns the value into a [`",stringify!($name),"`] returning None if the value is not of that type")]
		pub fn $into(self) -> Option<$t>{
			if let SqlValue::$name(x) = self{
				Some(x)
			}else{
				None
			}
		}
	};

	(@method $name:ident => $is:ident,$as:ident,$into:ident) => {
		#[doc = concat!("Check if the value is a [`",stringify!($name),"`]")]
		pub fn $is(&self) -> bool{
			matches!(self,SqlValue::$name)
		}
	};


	(@from $name:ident(Box<$inner:ident>) => $is:ident,$as:ident,$into:ident) => {
		impl From<$inner> for SqlValue{
			fn from(v: $inner) -> Self{
				SqlValue::$name(Box::new(v))
			}
		}

		impl From<Box<$inner>> for SqlValue{
			fn from(v: Box<$inner>) -> Self{
				SqlValue::$name(v)
			}
		}
	};

	(@from $name:ident($t:ident) => $is:ident,$as:ident,$into:ident) => {
		impl From<$t> for SqlValue{
			fn from(v: $t) -> Self{
				SqlValue::$name(v)
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
			impl From<$n> for SqlValue{
				fn from(v: $n) -> Self{
					SqlValue::Number(Number::from(v))
				}
			}
		)*
	};
}
impl_from_number!(i8, i16, i32, i64, u8, u16, u32, isize, f32, f64, Decimal);

impl<T> From<Vec<T>> for SqlValue
where
	SqlValue: From<T>,
{
	fn from(value: Vec<T>) -> Self {
		let v = value.into_iter().map(SqlValue::from).collect();
		SqlValue::Array(Array(v))
	}
}

impl<T> From<Option<T>> for SqlValue
where
	SqlValue: From<T>,
{
	fn from(value: Option<T>) -> Self {
		if let Some(x) = value {
			SqlValue::from(x)
		} else {
			SqlValue::None
		}
	}
}

impl From<Null> for SqlValue {
	fn from(_v: Null) -> Self {
		SqlValue::Null
	}
}

// TODO: Remove these implementations
// They truncate by default and therefore should not be implement for value.
impl From<i128> for SqlValue {
	fn from(v: i128) -> Self {
		SqlValue::Number(Number::from(v))
	}
}

impl From<u64> for SqlValue {
	fn from(v: u64) -> Self {
		SqlValue::Number(Number::from(v))
	}
}

impl From<u128> for SqlValue {
	fn from(v: u128) -> Self {
		SqlValue::Number(Number::from(v))
	}
}

impl From<usize> for SqlValue {
	fn from(v: usize) -> Self {
		SqlValue::Number(Number::from(v))
	}
}

impl From<String> for SqlValue {
	fn from(v: String) -> Self {
		Self::Strand(Strand::from(v))
	}
}

impl From<&str> for SqlValue {
	fn from(v: &str) -> Self {
		Self::Strand(Strand::from(v))
	}
}

impl From<DateTime<Utc>> for SqlValue {
	fn from(v: DateTime<Utc>) -> Self {
		SqlValue::Datetime(Datetime::from(v))
	}
}

impl From<Point<f64>> for SqlValue {
	fn from(v: Point<f64>) -> Self {
		SqlValue::Geometry(Geometry::from(v))
	}
}

impl From<Operation> for SqlValue {
	fn from(v: Operation) -> Self {
		SqlValue::Object(Object::from(v))
	}
}

impl From<uuid::Uuid> for SqlValue {
	fn from(v: uuid::Uuid) -> Self {
		SqlValue::Uuid(Uuid(v))
	}
}

impl From<HashMap<&str, SqlValue>> for SqlValue {
	fn from(v: HashMap<&str, SqlValue>) -> Self {
		SqlValue::Object(Object::from(v))
	}
}

impl From<HashMap<String, SqlValue>> for SqlValue {
	fn from(v: HashMap<String, SqlValue>) -> Self {
		SqlValue::Object(Object::from(v))
	}
}

impl From<BTreeMap<String, SqlValue>> for SqlValue {
	fn from(v: BTreeMap<String, SqlValue>) -> Self {
		SqlValue::Object(Object::from(v))
	}
}

impl From<BTreeMap<&str, SqlValue>> for SqlValue {
	fn from(v: BTreeMap<&str, SqlValue>) -> Self {
		SqlValue::Object(Object::from(v))
	}
}

impl From<IdRange> for SqlValue {
	fn from(v: IdRange) -> Self {
		let beg = match v.beg {
			Bound::Included(beg) => Bound::Included(SqlValue::from(beg)),
			Bound::Excluded(beg) => Bound::Excluded(SqlValue::from(beg)),
			Bound::Unbounded => Bound::Unbounded,
		};

		let end = match v.end {
			Bound::Included(end) => Bound::Included(SqlValue::from(end)),
			Bound::Excluded(end) => Bound::Excluded(SqlValue::from(end)),
			Bound::Unbounded => Bound::Unbounded,
		};

		SqlValue::Range(Box::new(Range {
			beg,
			end,
		}))
	}
}

impl From<Id> for SqlValue {
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

impl FromIterator<SqlValue> for SqlValue {
	fn from_iter<I: IntoIterator<Item = SqlValue>>(iter: I) -> Self {
		SqlValue::Array(Array(iter.into_iter().collect()))
	}
}

impl FromIterator<(String, SqlValue)> for SqlValue {
	fn from_iter<I: IntoIterator<Item = (String, SqlValue)>>(iter: I) -> Self {
		SqlValue::Object(Object(iter.into_iter().collect()))
	}
}

#[cfg(test)]
mod tests {

	use chrono::TimeZone;

	use super::*;
	use crate::syn::Parse;

	#[test]
	fn check_none() {
		assert!(SqlValue::None.is_none());
		assert!(!SqlValue::Null.is_none());
		assert!(!SqlValue::from(1).is_none());
	}

	#[test]
	fn check_null() {
		assert!(SqlValue::Null.is_null());
		assert!(!SqlValue::None.is_null());
		assert!(!SqlValue::from(1).is_null());
	}

	#[test]
	fn check_true() {
		assert!(!SqlValue::None.is_true());
		assert!(SqlValue::Bool(true).is_true());
		assert!(!SqlValue::Bool(false).is_true());
		assert!(!SqlValue::from(1).is_true());
		assert!(!SqlValue::from("something").is_true());
	}

	#[test]
	fn check_false() {
		assert!(!SqlValue::None.is_false());
		assert!(!SqlValue::Bool(true).is_false());
		assert!(SqlValue::Bool(false).is_false());
		assert!(!SqlValue::from(1).is_false());
		assert!(!SqlValue::from("something").is_false());
	}

	#[test]
	fn convert_truthy() {
		assert!(!SqlValue::None.is_truthy());
		assert!(!SqlValue::Null.is_truthy());
		assert!(SqlValue::Bool(true).is_truthy());
		assert!(!SqlValue::Bool(false).is_truthy());
		assert!(!SqlValue::from(0).is_truthy());
		assert!(SqlValue::from(1).is_truthy());
		assert!(SqlValue::from(-1).is_truthy());
		assert!(SqlValue::from(1.1).is_truthy());
		assert!(SqlValue::from(-1.1).is_truthy());
		assert!(SqlValue::from("true").is_truthy());
		assert!(SqlValue::from("false").is_truthy());
		assert!(SqlValue::from("falsey").is_truthy());
		assert!(SqlValue::from("something").is_truthy());
		assert!(SqlValue::from(Uuid::new()).is_truthy());
		assert!(SqlValue::from(Utc.with_ymd_and_hms(1948, 12, 3, 0, 0, 0).unwrap()).is_truthy());
	}

	#[test]
	fn convert_string() {
		assert_eq!(String::from("NONE"), SqlValue::None.as_string());
		assert_eq!(String::from("NULL"), SqlValue::Null.as_string());
		assert_eq!(String::from("true"), SqlValue::Bool(true).as_string());
		assert_eq!(String::from("false"), SqlValue::Bool(false).as_string());
		assert_eq!(String::from("0"), SqlValue::from(0).as_string());
		assert_eq!(String::from("1"), SqlValue::from(1).as_string());
		assert_eq!(String::from("-1"), SqlValue::from(-1).as_string());
		assert_eq!(String::from("1.1f"), SqlValue::from(1.1).as_string());
		assert_eq!(String::from("-1.1f"), SqlValue::from(-1.1).as_string());
		assert_eq!(String::from("3"), SqlValue::from("3").as_string());
		assert_eq!(String::from("true"), SqlValue::from("true").as_string());
		assert_eq!(String::from("false"), SqlValue::from("false").as_string());
		assert_eq!(String::from("something"), SqlValue::from("something").as_string());
	}

	#[test]
	fn check_size() {
		assert!(64 >= std::mem::size_of::<SqlValue>(), "size of value too big");
		assert!(104 >= std::mem::size_of::<Error>());
		assert!(104 >= std::mem::size_of::<Result<SqlValue>>());
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
		let enc: Vec<u8> = revision::to_vec(&SqlValue::None).unwrap();
		assert_eq!(2, enc.len());
		let enc: Vec<u8> = revision::to_vec(&SqlValue::Null).unwrap();
		assert_eq!(2, enc.len());
		let enc: Vec<u8> = revision::to_vec(&SqlValue::Bool(true)).unwrap();
		assert_eq!(3, enc.len());
		let enc: Vec<u8> = revision::to_vec(&SqlValue::Bool(false)).unwrap();
		assert_eq!(3, enc.len());
		let enc: Vec<u8> = revision::to_vec(&SqlValue::from("test")).unwrap();
		assert_eq!(8, enc.len());
		let enc: Vec<u8> = revision::to_vec(&SqlValue::parse("{ hello: 'world' }")).unwrap();
		assert_eq!(19, enc.len());
		let enc: Vec<u8> =
			revision::to_vec(&SqlValue::parse("{ compact: true, schema: 0 }")).unwrap();
		assert_eq!(27, enc.len());
	}

	#[test]
	fn serialize_deserialize() {
		let val = SqlValue::parse(
			"{ test: { something: [1, 'two', null, test:tobie, { trueee: false, noneee: nulll }] } }",
		);
		let res = SqlValue::parse(
			"{ test: { something: [1, 'two', null, test:tobie, { trueee: false, noneee: nulll }] } }",
		);
		let enc: Vec<u8> = revision::to_vec(&val).unwrap();
		let dec: SqlValue = revision::from_slice(&enc).unwrap();
		assert_eq!(res, dec);
	}

	#[test]
	fn test_value_from_vec_i32() {
		let vector: Vec<i32> = vec![1, 2, 3, 4, 5, 6];
		let value = SqlValue::from(vector);
		assert!(matches!(value, SqlValue::Array(Array(_))));
	}

	#[test]
	fn test_value_from_vec_f32() {
		let vector: Vec<f32> = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
		let value = SqlValue::from(vector);
		assert!(matches!(value, SqlValue::Array(Array(_))));
	}
}
