use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter, Write};
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use geo::{LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon};
use revision::revisioned;
use rust_decimal::Decimal;

use super::escape::EscapeKey;
use crate::expr::fmt::{Fmt, Pretty, is_pretty, pretty_indent};
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Expr, Literal, Part, Value};
use crate::val::{
	Array, Bytes, Closure, Datetime, Duration, File, Geometry, Number, Object, Range, RecordId,
	Regex, Strand, Uuid,
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

impl Display for GeometryKind {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			GeometryKind::Point => write!(f, "point"),
			GeometryKind::Line => write!(f, "line"),
			GeometryKind::Polygon => write!(f, "polygon"),
			GeometryKind::MultiPoint => write!(f, "multipoint"),
			GeometryKind::MultiLine => write!(f, "multiline"),
			GeometryKind::MultiPolygon => write!(f, "multipolygon"),
			GeometryKind::Collection => write!(f, "collection"),
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

/// The kind, or data type, of a value or field.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Kind {
	/// The most generic type, can be anything.
	Any,
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
	/// A record type.
	Record(Vec<String>),
	/// A geometry type.
	/// The vec contains the geometry types as strings, for example `"point"` or
	/// `"polygon"`.
	Geometry(Vec<GeometryKind>),
	/// An optional type.
	Option(Box<Kind>),
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

impl Default for Kind {
	fn default() -> Self {
		Self::Any
	}
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
		matches!(self, Kind::Option(_) | Kind::Any)
	}

	/// Returns true if this type is a literal, or contains a literal
	pub(crate) fn contains_literal(&self) -> bool {
		if matches!(self, Kind::Literal(_)) {
			return true;
		}

		if let Kind::Option(x) = self {
			return x.contains_literal();
		}

		if let Kind::Either(x) = self {
			return x.iter().any(|x| x.contains_literal());
		}

		false
	}

	// Return the kind of the contained value.
	//
	// For example: for `array<number>` or `set<number>` this returns `number`.
	// For `array<number> | set<float>` this returns `number | float`.
	pub(crate) fn inner_kind(&self) -> Option<Kind> {
		let mut this = self;
		loop {
			match &this {
				Kind::Any
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
				| Kind::Record(_)
				| Kind::Geometry(_)
				| Kind::Function(_, _)
				| Kind::Range
				| Kind::Literal(_)
				| Kind::File(_) => return None,
				Kind::Option(x) => {
					this = x;
				}
				Kind::Array(x, _) | Kind::Set(x, _) => return Some(x.as_ref().clone()),
				Kind::Either(x) => {
					// a either shouldn't be able to contain a either itself so recursing here
					// should be fine.
					let kinds: Vec<Kind> = x.iter().filter_map(Self::inner_kind).collect();
					if kinds.is_empty() {
						return None;
					}
					return Some(Kind::Either(kinds));
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
					return kinds.iter().all(|k| k.allows_nested_kind(path, kind));
				}
				Kind::Array(inner, len) | Kind::Set(inner, len) => {
					return match path.first() {
						Some(Part::All) => inner.allows_nested_kind(&path[1..], kind),
						Some(Part::Value(Expr::Literal(Literal::Integer(i)))) => {
							if let Some(len) = len {
								if *i >= *len as i64 {
									return false;
								}
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
			Kind::Literal(lit) => lit.allows_nested_kind(path, kind),
			Kind::Option(inner) => inner.allows_nested_kind(path, kind),
			_ if path.is_empty() => self == kind,
			_ => false,
		}
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
		let kind = T::kind();
		if matches!(kind, Kind::Option(_)) {
			kind
		} else {
			Kind::Option(Box::new(kind))
		}
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
	Strand => String,
	Bytes => Bytes,
	Number => Number,
	Datetime => Datetime,
	Duration => Duration,
	Uuid => Uuid,
	Object => Object,
	Range => Range,
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

impl Display for Kind {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Kind::Any => f.write_str("any"),
			Kind::Null => f.write_str("null"),
			Kind::Bool => f.write_str("bool"),
			Kind::Bytes => f.write_str("bytes"),
			Kind::Datetime => f.write_str("datetime"),
			Kind::Decimal => f.write_str("decimal"),
			Kind::Duration => f.write_str("duration"),
			Kind::Float => f.write_str("float"),
			Kind::Int => f.write_str("int"),
			Kind::Number => f.write_str("number"),
			Kind::Object => f.write_str("object"),
			Kind::String => f.write_str("string"),
			Kind::Uuid => f.write_str("uuid"),
			Kind::Regex => f.write_str("regex"),
			Kind::Function(_, _) => f.write_str("function"),
			Kind::Option(k) => write!(f, "option<{}>", k),
			Kind::Record(k) => {
				if k.is_empty() {
					f.write_str("record")
				} else {
					write!(f, "record<{}>", Fmt::verbar_separated(k))
				}
			}
			Kind::Geometry(k) => {
				if k.is_empty() {
					f.write_str("geometry")
				} else {
					write!(f, "geometry<{}>", Fmt::verbar_separated(k))
				}
			}
			Kind::Set(k, l) => match (k, l) {
				(k, None) if k.is_any() => f.write_str("set"),
				(k, None) => write!(f, "set<{}>", k),
				(k, Some(l)) => write!(f, "set<{}, {}>", k, l),
			},
			Kind::Array(k, l) => match (k, l) {
				(k, None) if k.is_any() => f.write_str("array"),
				(k, None) => write!(f, "array<{}>", k),
				(k, Some(l)) => write!(f, "array<{}, {}>", k, l),
			},
			Kind::Either(k) => write!(f, "{}", Fmt::verbar_separated(k)),
			Kind::Range => f.write_str("range"),
			Kind::Literal(l) => write!(f, "{}", l),
			Kind::File(k) => {
				if k.is_empty() {
					f.write_str("file")
				} else {
					write!(f, "file<{}>", Fmt::verbar_separated(k))
				}
			}
		}
	}
}

impl InfoStructure for Kind {
	fn structure(self) -> Value {
		self.to_string().into()
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug)]
pub enum KindLiteral {
	String(Strand),
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
				if let Some(inner) = a.first() {
					if a.iter().all(|x| x == inner) {
						return Kind::Array(Box::new(inner.to_owned()), Some(a.len() as u64));
					}
				}

				Kind::Array(Box::new(Kind::Any), None)
			}
			Self::Object(_) => Kind::Object,
			//Self::DiscriminatedObject(_, _) => Kind::Object,
			Self::Bool(_) => Kind::Bool,
		}
	}

	pub fn validate_value(&self, value: &Value) -> bool {
		match self {
			Self::String(v) => match value {
				Value::Strand(s) => s == v,
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
			Self::Object(o) => match value {
				Value::Object(x) => {
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

impl Display for KindLiteral {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			KindLiteral::String(s) => write!(f, "{s}"),
			KindLiteral::Integer(n) => write!(f, "{}", n),
			KindLiteral::Float(n) => write!(f, "{}f", n),
			KindLiteral::Decimal(n) => write!(f, "{}dec", n),
			KindLiteral::Duration(d) => write!(f, "{}", d),
			KindLiteral::Bool(b) => write!(f, "{}", b),
			KindLiteral::Array(a) => {
				let mut f = Pretty::from(f);
				f.write_char('[')?;
				if !a.is_empty() {
					let indent = pretty_indent();
					write!(f, "{}", Fmt::pretty_comma_separated(a.as_slice()))?;
					drop(indent);
				}
				f.write_char(']')
			}
			KindLiteral::Object(o) => {
				let mut f = Pretty::from(f);
				if is_pretty() {
					f.write_char('{')?;
				} else {
					f.write_str("{ ")?;
				}
				if !o.is_empty() {
					let indent = pretty_indent();
					write!(
						f,
						"{}",
						Fmt::pretty_comma_separated(o.iter().map(|args| Fmt::new(
							args,
							|(k, v), f| write!(f, "{}: {}", EscapeKey(k), v)
						)),)
					)?;
					drop(indent);
				}
				if is_pretty() {
					f.write_char('}')
				} else {
					f.write_str(" }")
				}
			} /*
			  KindLiteral::DiscriminatedObject(_, discriminants) => {
				  let mut f = Pretty::from(f);

				  for (i, o) in discriminants.iter().enumerate() {
					  if i > 0 {
						  f.write_str(" | ")?;
					  }

					  if is_pretty() {
						  f.write_char('{')?;
					  } else {
						  f.write_str("{ ")?;
					  }
					  if !o.is_empty() {
						  let indent = pretty_indent();
						  write!(
							  f,
							  "{}",
							  Fmt::pretty_comma_separated(o.iter().map(|args| Fmt::new(
								  args,
								  |(k, v), f| write!(f, "{}: {}", EscapeKey(k), v)
							  )),)
						  )?;
						  drop(indent);
					  }
					  if is_pretty() {
						  f.write_char('}')?;
					  } else {
						  f.write_str(" }")?;
					  }
				  }

				  Ok(())
			  }
			  */
		}
	}
}
