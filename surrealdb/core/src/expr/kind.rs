use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};
use std::fmt::{self, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use geo::{LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon};
use revision::revisioned;
use rust_decimal::Decimal;
use surrealdb_types::{SqlFormat, ToSql};

use crate::expr::statements::info::InfoStructure;
use crate::expr::{Expr, Literal, Part, Value};
use crate::val::{
	Array, Bytes, Closure, Datetime, Duration, File, Geometry, Number, Range, RecordId, Regex, Set,
	TableName, Uuid,
};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum GeometryKind {
	Point,
	Line,
	Polygon,
	MultiPoint,
	MultiLine,
	MultiPolygon,
	Collection,
}

impl ToSql for GeometryKind {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		match self {
			GeometryKind::Point => f.push_str("point"),
			GeometryKind::Line => f.push_str("line"),
			GeometryKind::Polygon => f.push_str("polygon"),
			GeometryKind::MultiPoint => f.push_str("multipoint"),
			GeometryKind::MultiLine => f.push_str("multiline"),
			GeometryKind::MultiPolygon => f.push_str("multipolygon"),
			GeometryKind::Collection => f.push_str("collection"),
		}
	}
}

impl FromStr for GeometryKind {
	type Err = anyhow::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"point" => Ok(GeometryKind::Point),
			"line" => Ok(GeometryKind::Line),
			"polygon" => Ok(GeometryKind::Polygon),
			"multipoint" => Ok(GeometryKind::MultiPoint),
			"multiline" => Ok(GeometryKind::MultiLine),
			"multipolygon" => Ok(GeometryKind::MultiPolygon),
			"collection" => Ok(GeometryKind::Collection),
			_ => Err(anyhow::anyhow!("invalid geometry kind: {s}")),
		}
	}
}

impl From<GeometryKind> for crate::types::PublicGeometryKind {
	fn from(k: GeometryKind) -> Self {
		match k {
			GeometryKind::Point => crate::types::PublicGeometryKind::Point,
			GeometryKind::Line => crate::types::PublicGeometryKind::Line,
			GeometryKind::Polygon => crate::types::PublicGeometryKind::Polygon,
			GeometryKind::MultiPoint => crate::types::PublicGeometryKind::MultiPoint,
			GeometryKind::MultiLine => crate::types::PublicGeometryKind::MultiLine,
			GeometryKind::MultiPolygon => crate::types::PublicGeometryKind::MultiPolygon,
			GeometryKind::Collection => crate::types::PublicGeometryKind::Collection,
		}
	}
}

impl From<crate::types::PublicGeometryKind> for GeometryKind {
	fn from(k: crate::types::PublicGeometryKind) -> Self {
		match k {
			crate::types::PublicGeometryKind::Point => GeometryKind::Point,
			crate::types::PublicGeometryKind::Line => GeometryKind::Line,
			crate::types::PublicGeometryKind::Polygon => GeometryKind::Polygon,
			crate::types::PublicGeometryKind::MultiPoint => GeometryKind::MultiPoint,
			crate::types::PublicGeometryKind::MultiLine => GeometryKind::MultiLine,
			crate::types::PublicGeometryKind::MultiPolygon => GeometryKind::MultiPolygon,
			crate::types::PublicGeometryKind::Collection => GeometryKind::Collection,
		}
	}
}

/// The kind, or data type, of a value or field.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum Kind {
	/// The most generic type, can be anything.
	#[default]
	Any,
	/// None type.
	None,
	/// Null type.
	Null,
	/// Boolean type.
	Bool,
	/// Bytes type.
	Bytes,
	/// Datetime type.
	Datetime,
	/// Decimal type.
	Decimal,
	/// Duration type.
	Duration,
	/// 64-bit floating point type.
	Float,
	/// 64-bit signed integer type.
	Int,
	/// Number type, can be either a float, int or decimal.
	/// This is the most generic type for numbers.
	Number,
	/// Object type.
	Object,
	/// String type.
	String,
	/// UUID type.
	Uuid,
	/// Regular expression type.
	Regex,
	/// A table type.
	Table(Vec<TableName>),
	/// A record type.
	Record(Vec<TableName>),
	/// A geometry type.
	/// The vec contains the geometry types as strings, for example `"point"` or
	/// `"polygon"`.
	Geometry(Vec<GeometryKind>),
	/// An either type.
	/// Can be any of the kinds in the vec.
	Either(Vec<Kind>),
	/// A set type.
	Set(Box<Kind>, Option<u64>),
	/// An array type.
	Array(Box<Kind>, Option<u64>),
	/// A function type.
	/// The first option is the argument types, the second is the optional
	/// return type.
	Function(Option<Vec<Kind>>, Option<Box<Kind>>),
	/// A range type.
	Range,
	/// A literal type.
	/// The literal type is used to represent a type that can only be a single
	/// value. For example, `"a"` is a literal type which can only ever be
	/// `"a"`. This can be used in the `Kind::Either` type to represent an
	/// enum.
	Literal(KindLiteral),
	/// A file type.
	/// If the kind was specified without a bucket the vec will be empty.
	/// So `<file>` is just `Kind::File(Vec::new())`
	File(Vec<String>),
}

impl Kind {
	/// Returns the kind of a type.
	pub(crate) fn of<T: HasKind>() -> Kind {
		T::kind()
	}

	/// Returns true if this type is an `any`
	pub(crate) fn is_any(&self) -> bool {
		matches!(self, Kind::Any)
	}

	/// Returns true if this type is a record
	pub(crate) fn is_record(&self) -> bool {
		matches!(self, Kind::Record(_))
	}

	/// Returns true if this type is optional
	pub(crate) fn can_be_none(&self) -> bool {
		match self {
			Kind::None | Kind::Any => true,
			Kind::Either(x) => x.iter().any(|x| x.can_be_none()),
			_ => false,
		}
	}

	/// Returns true if this type is a literal, or contains a literal
	pub(crate) fn contains_literal(&self) -> bool {
		match self {
			Kind::Literal(_) => true,
			Kind::Either(x) => x.iter().any(|x| x.contains_literal()),
			_ => false,
		}
	}

	/// Returns true if this type permits access to named sub-fields.
	///
	/// This covers `object`, `any`, literal object types (e.g. `{ key: string }`),
	/// and union types where every non-none variant permits sub-field access.
	pub(crate) fn allows_sub_fields(&self) -> bool {
		match self {
			Kind::Any | Kind::Object | Kind::Array(..) | Kind::Set(..) => true,
			Kind::Literal(KindLiteral::Object(_) | KindLiteral::Array(_)) => true,
			Kind::Either(kinds) => {
				kinds.iter().all(|k| matches!(k, Kind::None) || k.allows_sub_fields())
			}
			_ => false,
		}
	}

	// Return the kind of the contained value.
	//
	// For example: for `array<number>` or `set<number>` this returns `number`.
	// For `array<number> | set<float>` this returns `number | float`.
	pub(crate) fn inner_kind(&self) -> Option<Kind> {
		match self {
			Kind::Any
			| Kind::None
			| Kind::Null
			| Kind::Bool
			| Kind::Bytes
			| Kind::Datetime
			| Kind::Decimal
			| Kind::Duration
			| Kind::Float
			| Kind::Int
			| Kind::Number
			| Kind::Object
			| Kind::String
			| Kind::Uuid
			| Kind::Regex
			| Kind::Table(_)
			| Kind::Record(_)
			| Kind::Geometry(_)
			| Kind::Function(_, _)
			| Kind::Range
			| Kind::Literal(_)
			| Kind::File(_) => None,
			Kind::Array(x, _) | Kind::Set(x, _) => Some(x.as_ref().clone()),
			Kind::Either(x) => {
				// a either shouldn't be able to contain a either itself so recursing here
				// should be fine.
				let kinds: Vec<Kind> = x.iter().filter_map(Self::inner_kind).collect();
				if kinds.is_empty() {
					None
				} else {
					Some(Kind::Either(kinds))
				}
			}
		}
	}

	pub(crate) fn allows_nested_kind(&self, path: &[Part], kind: &Kind) -> bool {
		// ANY type won't cause a mismatch
		if self.is_any() || kind.is_any() {
			return true;
		}

		if !path.is_empty() {
			match self {
				Kind::Object => return matches!(path.first(), Some(Part::Field(_) | Part::All)),
				Kind::Either(kinds) => {
					return kinds
						.iter()
						.all(|k| matches!(k, Kind::None) || k.allows_nested_kind(path, kind));
				}
				Kind::Array(inner, len) | Kind::Set(inner, len) => {
					return match path.first() {
						Some(Part::All) => inner.allows_nested_kind(&path[1..], kind),
						Some(Part::Value(Expr::Literal(Literal::Integer(i)))) => {
							if let Some(len) = len
								&& *i >= *len as i64
							{
								return false;
							}

							inner.allows_nested_kind(&path[1..], kind)
						}
						_ => false,
					};
				}
				_ => (),
			}
		}

		match self {
			// Check if the two kinds match when we reach the end of the path
			_ if path.is_empty() && self == kind => true,
			// Check if the literal matches the kind
			Kind::Literal(lit) => lit.allows_nested_kind(path, kind),
			// Check if any of the kinds in the either match the kind
			Kind::Either(kinds) => {
				kinds.iter().all(|k| matches!(k, Kind::None) || k.allows_nested_kind(path, kind))
			}
			_ => false,
		}
	}

	pub(crate) fn flatten(self) -> Vec<Kind> {
		match self {
			Kind::Either(x) => x.into_iter().flat_map(|k| k.flatten()).collect(),
			_ => vec![self],
		}
	}

	pub(crate) fn either(kinds: Vec<Kind>) -> Kind {
		let mut seen = HashSet::new();
		let mut kinds = kinds
			.into_iter()
			.flat_map(|k| k.flatten())
			.filter(|k| seen.insert(k.clone()))
			.collect::<Vec<_>>();
		match kinds.len() {
			0 => Kind::None,
			1 => kinds.remove(0),
			_ => Kind::Either(kinds),
		}
	}

	pub(crate) fn option(kind: Kind) -> Kind {
		Kind::either(vec![Kind::None, kind])
	}
}

/// Trait for retrieving the `kind` equivalent of a rust type.
///
/// Returns the most general kind for a type.
/// For example Number could be either number or float or int or decimal but the
/// most general is number.
///
/// This trait is only implemented for types which can only be retrieve from
pub trait HasKind {
	fn kind() -> Kind;
}

impl<T: HasKind> HasKind for Option<T> {
	fn kind() -> Kind {
		Kind::option(T::kind())
	}
}

impl<T: HasKind> HasKind for Vec<T> {
	fn kind() -> Kind {
		let kind = T::kind();
		Kind::Array(Box::new(kind), None)
	}
}

impl HasKind for Array {
	fn kind() -> Kind {
		Kind::Array(Box::new(Kind::Any), None)
	}
}

impl HasKind for Set {
	fn kind() -> Kind {
		Kind::Set(Box::new(Kind::Any), None)
	}
}

impl<T: HasKind, const SIZE: usize> HasKind for [T; SIZE] {
	fn kind() -> Kind {
		let kind = T::kind();
		Kind::Array(Box::new(kind), Some(SIZE as u64))
	}
}

impl HasKind for RecordId {
	fn kind() -> Kind {
		Kind::Record(Vec::new())
	}
}

impl HasKind for Geometry {
	fn kind() -> Kind {
		Kind::Geometry(Vec::new())
	}
}

impl HasKind for Closure {
	fn kind() -> Kind {
		// The inner values of function are currently completely unused.
		Kind::Function(None, None)
	}
}

impl HasKind for Regex {
	fn kind() -> Kind {
		Kind::Regex
	}
}

impl HasKind for File {
	fn kind() -> Kind {
		Kind::File(Vec::new())
	}
}

impl HasKind for TableName {
	fn kind() -> Kind {
		Kind::Table(Vec::new())
	}
}

macro_rules! impl_basic_has_kind{
	($($name:ident => $kind:ident),*$(,)?) => {
		$(
			impl HasKind for $name{
				fn kind() -> Kind{
					Kind::$kind
				}
			}
		)*
	}
}

impl_basic_has_kind! {
	bool => Bool,

	i64 => Int,
	f64 => Float,
	Decimal => Decimal,

	String => String,
	Bytes => Bytes,
	Number => Number,
	Datetime => Datetime,
	Duration => Duration,
	Uuid => Uuid,
	Range => Range,
}

impl HasKind for crate::val::Object {
	fn kind() -> Kind {
		Kind::Object
	}
}

macro_rules! impl_geometry_has_kind{
	($($name:ty => $kind:expr),*$(,)?) => {
		$(
			impl HasKind for $name{
				fn kind() -> Kind{
					Kind::Geometry(vec![$kind])
				}
			}
		)*
	}
}
impl_geometry_has_kind! {
	Point<f64> => GeometryKind::Point,
	LineString<f64> => GeometryKind::Line,
	MultiPoint<f64> => GeometryKind::MultiPoint,
	Polygon<f64> => GeometryKind::Polygon,
	MultiLineString<f64> => GeometryKind::MultiLine,
	MultiPolygon<f64> => GeometryKind::MultiPolygon,
}

impl From<&Kind> for Box<Kind> {
	#[inline]
	fn from(v: &Kind) -> Self {
		Box::new(v.clone())
	}
}

impl ToSql for Kind {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let kind: crate::sql::Kind = self.clone().into();
		kind.fmt_sql(f, fmt);
	}
}

impl Display for Kind {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "{}", self.to_sql())
	}
}

impl InfoStructure for Kind {
	fn structure(self) -> Value {
		self.to_string().into()
	}
}

impl From<crate::types::PublicKind> for Kind {
	fn from(v: crate::types::PublicKind) -> Self {
		match v {
			crate::types::PublicKind::Any => Kind::Any,
			crate::types::PublicKind::None => Kind::None,
			crate::types::PublicKind::Null => Kind::Null,
			crate::types::PublicKind::Bool => Kind::Bool,
			crate::types::PublicKind::Bytes => Kind::Bytes,
			crate::types::PublicKind::Datetime => Kind::Datetime,
			crate::types::PublicKind::Decimal => Kind::Decimal,
			crate::types::PublicKind::Duration => Kind::Duration,
			crate::types::PublicKind::Float => Kind::Float,
			crate::types::PublicKind::Int => Kind::Int,
			crate::types::PublicKind::Number => Kind::Number,
			crate::types::PublicKind::Object => Kind::Object,
			crate::types::PublicKind::String => Kind::String,
			crate::types::PublicKind::Uuid => Kind::Uuid,
			crate::types::PublicKind::Regex => Kind::Regex,
			crate::types::PublicKind::Range => Kind::Range,
			crate::types::PublicKind::Table(table) => {
				Kind::Table(table.into_iter().map(TableName::from).collect())
			}
			crate::types::PublicKind::Record(tables) => {
				Kind::Record(tables.into_iter().map(TableName::from).collect())
			}
			crate::types::PublicKind::Geometry(kinds) => {
				Kind::Geometry(kinds.into_iter().map(Into::into).collect())
			}
			crate::types::PublicKind::Either(kinds) => {
				Kind::Either(kinds.into_iter().map(Kind::from).collect())
			}
			crate::types::PublicKind::Set(kind, size) => {
				Kind::Set(Box::new(Kind::from(*kind)), size)
			}
			crate::types::PublicKind::Array(kind, size) => {
				Kind::Array(Box::new(Kind::from(*kind)), size)
			}
			crate::types::PublicKind::Function(args, ret) => Kind::Function(
				args.map(|a| a.into_iter().map(Kind::from).collect()),
				ret.map(|r| Box::new(Kind::from(*r))),
			),
			crate::types::PublicKind::File(bucket) => Kind::File(bucket),
			crate::types::PublicKind::Literal(lit) => Kind::Literal(lit.into()),
		}
	}
}

impl From<Kind> for crate::types::PublicKind {
	fn from(v: Kind) -> Self {
		match v {
			Kind::Any => crate::types::PublicKind::Any,
			Kind::None => crate::types::PublicKind::None,
			Kind::Null => crate::types::PublicKind::Null,
			Kind::Bool => crate::types::PublicKind::Bool,
			Kind::Bytes => crate::types::PublicKind::Bytes,
			Kind::Datetime => crate::types::PublicKind::Datetime,
			Kind::Decimal => crate::types::PublicKind::Decimal,
			Kind::Duration => crate::types::PublicKind::Duration,
			Kind::Float => crate::types::PublicKind::Float,
			Kind::Int => crate::types::PublicKind::Int,
			Kind::Number => crate::types::PublicKind::Number,
			Kind::Object => crate::types::PublicKind::Object,
			Kind::String => crate::types::PublicKind::String,
			Kind::Uuid => crate::types::PublicKind::Uuid,
			Kind::Regex => crate::types::PublicKind::Regex,
			Kind::Range => crate::types::PublicKind::Range,
			Kind::Table(tables) => {
				crate::types::PublicKind::Table(tables.into_iter().map(Into::into).collect())
			}
			Kind::Record(tables) => {
				crate::types::PublicKind::Record(tables.into_iter().map(Into::into).collect())
			}
			Kind::Geometry(kinds) => {
				crate::types::PublicKind::Geometry(kinds.into_iter().map(Into::into).collect())
			}
			Kind::Either(kinds) => {
				crate::types::PublicKind::Either(kinds.into_iter().map(Into::into).collect())
			}
			Kind::Set(kind, size) => crate::types::PublicKind::Set(Box::new((*kind).into()), size),
			Kind::Array(kind, size) => {
				crate::types::PublicKind::Array(Box::new((*kind).into()), size)
			}
			Kind::Function(args, ret) => crate::types::PublicKind::Function(
				args.map(|a| a.into_iter().map(Into::into).collect()),
				ret.map(|r| Box::new((*r).into())),
			),
			Kind::File(bucket) => crate::types::PublicKind::File(bucket),
			Kind::Literal(lit) => crate::types::PublicKind::Literal(lit.into()),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug)]
pub enum KindLiteral {
	String(String),
	Integer(i64),
	Float(f64),
	Decimal(Decimal),
	Duration(Duration),
	Array(Vec<Kind>),
	Object(BTreeMap<String, Kind>),
	// This variant is just for a performance optimization.
	// Should probably be removed when we have a planner.
	//DiscriminatedObject(String, Vec<BTreeMap<String, Kind>>),
	Bool(bool),
}

impl From<crate::types::PublicKindLiteral> for KindLiteral {
	fn from(v: crate::types::PublicKindLiteral) -> Self {
		match v {
			crate::types::PublicKindLiteral::String(s) => KindLiteral::String(s),
			crate::types::PublicKindLiteral::Integer(i) => KindLiteral::Integer(i),
			crate::types::PublicKindLiteral::Float(f) => KindLiteral::Float(f),
			crate::types::PublicKindLiteral::Decimal(d) => KindLiteral::Decimal(d),
			crate::types::PublicKindLiteral::Duration(d) => {
				KindLiteral::Duration(crate::val::Duration(*d))
			}
			crate::types::PublicKindLiteral::Array(kinds) => {
				KindLiteral::Array(kinds.into_iter().map(Kind::from).collect())
			}
			crate::types::PublicKindLiteral::Object(obj) => {
				KindLiteral::Object(obj.into_iter().map(|(k, v)| (k, Kind::from(v))).collect())
			}
			crate::types::PublicKindLiteral::Bool(b) => KindLiteral::Bool(b),
		}
	}
}

impl From<KindLiteral> for crate::types::PublicKindLiteral {
	fn from(v: KindLiteral) -> Self {
		match v {
			KindLiteral::String(s) => crate::types::PublicKindLiteral::String(s),
			KindLiteral::Integer(i) => crate::types::PublicKindLiteral::Integer(i),
			KindLiteral::Float(f) => crate::types::PublicKindLiteral::Float(f),
			KindLiteral::Decimal(d) => crate::types::PublicKindLiteral::Decimal(d),
			KindLiteral::Duration(d) => {
				crate::types::PublicKindLiteral::Duration(crate::types::PublicDuration::from(d.0))
			}
			KindLiteral::Array(kinds) => {
				crate::types::PublicKindLiteral::Array(kinds.into_iter().map(Into::into).collect())
			}
			KindLiteral::Object(obj) => crate::types::PublicKindLiteral::Object(
				obj.into_iter().map(|(k, v)| (k, v.into())).collect(),
			),
			KindLiteral::Bool(b) => crate::types::PublicKindLiteral::Bool(b),
		}
	}
}

impl PartialEq for KindLiteral {
	fn eq(&self, other: &Self) -> bool {
		match self {
			KindLiteral::String(strand) => {
				if let KindLiteral::String(other) = other {
					strand == other
				} else {
					false
				}
			}
			KindLiteral::Integer(x) => {
				if let KindLiteral::Integer(other) = other {
					x == other
				} else {
					false
				}
			}
			KindLiteral::Float(x) => {
				if let KindLiteral::Float(other) = other {
					x.to_bits() == other.to_bits()
				} else {
					false
				}
			}
			KindLiteral::Decimal(decimal) => {
				if let KindLiteral::Decimal(other) = other {
					decimal == other
				} else {
					false
				}
			}
			KindLiteral::Duration(duration) => {
				if let KindLiteral::Duration(other) = other {
					duration == other
				} else {
					false
				}
			}
			KindLiteral::Array(kinds) => {
				if let KindLiteral::Array(other) = other {
					kinds == other
				} else {
					false
				}
			}
			KindLiteral::Object(btree_map) => {
				if let KindLiteral::Object(other) = other {
					btree_map == other
				} else {
					false
				}
			}
			/*
			KindLiteral::DiscriminatedObject(a, b) => {
				if let KindLiteral::DiscriminatedObject(c, d) = other {
					a == c && b == d
				} else {
					false
				}
			}
			*/
			KindLiteral::Bool(a) => {
				if let KindLiteral::Bool(b) = other {
					a == b
				} else {
					false
				}
			}
		}
	}
}
impl Eq for KindLiteral {}
impl Hash for KindLiteral {
	fn hash<H: Hasher>(&self, state: &mut H) {
		std::mem::discriminant(self).hash(state);
		match self {
			KindLiteral::String(strand) => strand.hash(state),
			KindLiteral::Integer(x) => x.hash(state),
			KindLiteral::Float(x) => x.to_bits().hash(state),
			KindLiteral::Decimal(decimal) => decimal.hash(state),
			KindLiteral::Duration(duration) => duration.hash(state),
			KindLiteral::Array(kinds) => kinds.hash(state),
			KindLiteral::Object(btree_map) => btree_map.hash(state),
			/*
			KindLiteral::DiscriminatedObject(a, b) => {
				a.hash(state);
				b.hash(state);
			}
			*/
			KindLiteral::Bool(x) => x.hash(state),
		}
	}
}

impl KindLiteral {
	pub fn to_kind(&self) -> Kind {
		match self {
			Self::String(_) => Kind::String,
			Self::Integer(_) | Self::Float(_) | Self::Decimal(_) => Kind::Number,
			Self::Duration(_) => Kind::Duration,
			Self::Array(a) => {
				if let Some(inner) = a.first()
					&& a.iter().all(|x| x == inner)
				{
					return Kind::Array(Box::new(inner.to_owned()), Some(a.len() as u64));
				}

				Kind::Array(Box::new(Kind::Any), None)
			}
			Self::Object(_) => Kind::Object,
			//Self::DiscriminatedObject(_, _) => Kind::Object,
			Self::Bool(_) => Kind::Bool,
		}
	}

	pub(crate) fn validate_value(&self, value: &Value) -> bool {
		match self {
			Self::String(v) => match value {
				Value::String(s) => s == v,
				_ => false,
			},
			Self::Integer(v) => match value {
				Value::Number(n) => *n == Number::Int(*v),
				_ => false,
			},
			Self::Float(v) => match value {
				Value::Number(n) => *n == Number::Float(*v),
				_ => false,
			},
			Self::Decimal(v) => match value {
				Value::Number(n) => *n == Number::Decimal(*v),
				_ => false,
			},
			Self::Duration(v) => match value {
				Value::Duration(n) => n == v,
				_ => false,
			},
			Self::Bool(v) => match value {
				Value::Bool(b) => b == v,
				_ => false,
			},
			Self::Array(a) => match value {
				Value::Array(x) => {
					if a.len() != x.len() {
						return false;
					}

					for (i, inner) in a.iter().enumerate() {
						if let Some(value) = x.get(i) {
							if !value.can_coerce_to_kind(inner) {
								return false;
							}
						} else {
							return false;
						}
					}

					true
				}
				_ => false,
			},
			Self::Object(lit) => match value {
				Value::Object(val) => {
					let mut lit_iter = lit.iter();
					let mut val_iter = val.iter();

					let mut lit_next = lit_iter.next();
					let mut val_next = val_iter.next();

					while lit_next.is_some() || val_next.is_some() {
						match (lit_next, val_next) {
							(Some((lit_k, lit_kind)), Some((val_k, val_v))) => {
								match lit_k.cmp(val_k) {
									Ordering::Less => {
										// Key in type but not in value - check if optional
										if !lit_kind.can_be_none() {
											return false;
										}
										lit_next = lit_iter.next();
									}
									Ordering::Equal => {
										// Key in both - validate type
										if !val_v.can_coerce_to_kind(lit_kind) {
											return false;
										}
										lit_next = lit_iter.next();
										val_next = val_iter.next();
									}
									Ordering::Greater => {
										// Key in value but not in type - extra key!
										return false;
									}
								}
							}
							(Some((_, lit_kind)), None) => {
								// Remaining keys in type but not in value - check if optional
								if !lit_kind.can_be_none() {
									return false;
								}
								lit_next = lit_iter.next();
							}
							(None, Some(_)) => {
								// Remaining keys in value but not in type - extra keys!
								return false;
							}
							(None, None) => break,
						}
					}

					true
				}
				_ => false,
			},
			/*
			Self::DiscriminatedObject(key, discriminants) => match value {
				Value::Object(x) => {
					let Some(value) = x.get(key) else {
						return false;
					};
					if let Some(o) =
						discriminants.iter().find(|o| value.can_coerce_to_kind(&o[key]))
					{
						if o.len() < x.len() {
							return false;
						}

						for (k, v) in o.iter() {
							if let Some(value) = x.get(k) {
								if !value.can_coerce_to_kind(v) {
									return false;
								}
							} else if !v.can_be_none() {
								return false;
							}
						}

						true
					} else {
						false
					}
				}
				_ => false,
			},
			*/
		}
	}

	pub(crate) fn allows_nested_kind(&self, path: &[Part], kind: &Kind) -> bool {
		// ANY type won't cause a mismatch
		if kind.is_any() {
			return true;
		}

		// We reached the end of the path
		// Check if the literal is equal to the kind
		if path.is_empty() {
			return match kind {
				Kind::Literal(lit) => self == lit,
				_ => &self.to_kind() == kind,
			};
		}

		match self {
			KindLiteral::Array(x) => match path.first() {
				Some(Part::All) => x.iter().all(|y| y.allows_nested_kind(&path[1..], kind)),
				Some(part) => part
					.as_old_index()
					.and_then(|idx| x.get(idx))
					.map(|x| x.allows_nested_kind(&path[1..], kind))
					.unwrap_or(false),
				None => false,
			},
			KindLiteral::Object(x) => match path.first() {
				Some(Part::All) => x.iter().all(|(_, y)| y.allows_nested_kind(&path[1..], kind)),
				Some(Part::Field(k)) => {
					if let Some(y) = x.get(&**k) {
						y.allows_nested_kind(&path[1..], kind)
					} else {
						false
					}
				}
				_ => false,
			},
			/*
			KindLiteral::DiscriminatedObject(_, discriminants) => match path.first() {
				Some(Part::All) => discriminants
					.iter()
					.all(|o| o.iter().all(|(_, y)| y.allows_nested_kind(&path[1..], kind))),
				Some(Part::Field(k)) => discriminants.iter().all(|o| {
					if let Some(y) = o.get(&**k) {
						y.allows_nested_kind(&path[1..], kind)
					} else {
						false
					}
				}),
				_ => false,
			},
			*/
			_ => false,
		}
	}
}

impl ToSql for KindLiteral {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let lit: crate::sql::kind::KindLiteral = self.clone().into();
		lit.fmt_sql(f, fmt)
	}
}
