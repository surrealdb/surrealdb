use std::collections::{BTreeMap, HashSet};
use std::fmt::Write;
use std::hash;

use rust_decimal::Decimal;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::{EscapeIdent, EscapeKey, Fmt, QuoteStr};
use crate::types::PublicDuration;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			GeometryKind::Point => write_sql!(f, fmt, "point"),
			GeometryKind::Line => write_sql!(f, fmt, "line"),
			GeometryKind::Polygon => write_sql!(f, fmt, "polygon"),
			GeometryKind::MultiPoint => write_sql!(f, fmt, "multipoint"),
			GeometryKind::MultiLine => write_sql!(f, fmt, "multiline"),
			GeometryKind::MultiPolygon => write_sql!(f, fmt, "multipolygon"),
			GeometryKind::Collection => write_sql!(f, fmt, "collection"),
		}
	}
}

impl From<GeometryKind> for crate::expr::kind::GeometryKind {
	fn from(v: GeometryKind) -> Self {
		match v {
			GeometryKind::Point => crate::expr::kind::GeometryKind::Point,
			GeometryKind::Line => crate::expr::kind::GeometryKind::Line,
			GeometryKind::Polygon => crate::expr::kind::GeometryKind::Polygon,
			GeometryKind::MultiPoint => crate::expr::kind::GeometryKind::MultiPoint,
			GeometryKind::MultiLine => crate::expr::kind::GeometryKind::MultiLine,
			GeometryKind::MultiPolygon => crate::expr::kind::GeometryKind::MultiPolygon,
			GeometryKind::Collection => crate::expr::kind::GeometryKind::Collection,
		}
	}
}

impl From<crate::expr::kind::GeometryKind> for GeometryKind {
	fn from(v: crate::expr::kind::GeometryKind) -> Self {
		match v {
			crate::expr::kind::GeometryKind::Point => GeometryKind::Point,
			crate::expr::kind::GeometryKind::Line => GeometryKind::Line,
			crate::expr::kind::GeometryKind::Polygon => GeometryKind::Polygon,
			crate::expr::kind::GeometryKind::MultiPoint => GeometryKind::MultiPoint,
			crate::expr::kind::GeometryKind::MultiLine => GeometryKind::MultiLine,
			crate::expr::kind::GeometryKind::MultiPolygon => GeometryKind::MultiPolygon,
			crate::expr::kind::GeometryKind::Collection => GeometryKind::Collection,
		}
	}
}

impl From<GeometryKind> for crate::types::PublicGeometryKind {
	fn from(v: GeometryKind) -> Self {
		match v {
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
	fn from(v: crate::types::PublicGeometryKind) -> Self {
		match v {
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
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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
	Table(Vec<String>),
	/// A record type.
	Record(Vec<String>),
	/// A geometry type.
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
}

impl From<Kind> for crate::expr::Kind {
	fn from(v: Kind) -> Self {
		match v {
			Kind::Any => crate::expr::Kind::Any,
			Kind::None => crate::expr::Kind::None,
			Kind::Null => crate::expr::Kind::Null,
			Kind::Bool => crate::expr::Kind::Bool,
			Kind::Bytes => crate::expr::Kind::Bytes,
			Kind::Datetime => crate::expr::Kind::Datetime,
			Kind::Decimal => crate::expr::Kind::Decimal,
			Kind::Duration => crate::expr::Kind::Duration,
			Kind::Float => crate::expr::Kind::Float,
			Kind::Int => crate::expr::Kind::Int,
			Kind::Number => crate::expr::Kind::Number,
			Kind::Object => crate::expr::Kind::Object,
			Kind::String => crate::expr::Kind::String,
			Kind::Uuid => crate::expr::Kind::Uuid,
			Kind::Regex => crate::expr::Kind::Regex,
			Kind::Table(tables) => crate::expr::Kind::Table(tables),
			Kind::Record(tables) => crate::expr::Kind::Record(tables),
			Kind::Geometry(geometries) => {
				crate::expr::Kind::Geometry(geometries.into_iter().map(Into::into).collect())
			}
			Kind::Either(kinds) => {
				crate::expr::Kind::Either(kinds.into_iter().map(Into::into).collect())
			}
			Kind::Set(k, l) => crate::expr::Kind::Set(Box::new(k.as_ref().clone().into()), l),
			Kind::Array(k, l) => crate::expr::Kind::Array(Box::new(k.as_ref().clone().into()), l),
			Kind::Function(args, ret) => crate::expr::Kind::Function(
				args.map(|args| args.into_iter().map(Into::into).collect()),
				ret.map(|ret| Box::new((*ret).into())),
			),
			Kind::Range => crate::expr::Kind::Range,
			Kind::Literal(l) => crate::expr::Kind::Literal(l.into()),
			Kind::File(k) => crate::expr::Kind::File(k),
		}
	}
}

impl From<crate::expr::Kind> for Kind {
	fn from(v: crate::expr::Kind) -> Self {
		match v {
			crate::expr::Kind::Any => Kind::Any,
			crate::expr::Kind::None => Kind::None,
			crate::expr::Kind::Null => Kind::Null,
			crate::expr::Kind::Bool => Kind::Bool,
			crate::expr::Kind::Bytes => Kind::Bytes,
			crate::expr::Kind::Datetime => Kind::Datetime,
			crate::expr::Kind::Decimal => Kind::Decimal,
			crate::expr::Kind::Duration => Kind::Duration,
			crate::expr::Kind::Float => Kind::Float,
			crate::expr::Kind::Int => Kind::Int,
			crate::expr::Kind::Number => Kind::Number,
			crate::expr::Kind::Object => Kind::Object,
			crate::expr::Kind::String => Kind::String,
			crate::expr::Kind::Uuid => Kind::Uuid,
			crate::expr::Kind::Regex => Kind::Regex,
			crate::expr::Kind::Table(tables) => Kind::Table(tables),
			crate::expr::Kind::Record(tables) => Kind::Record(tables),
			crate::expr::Kind::Geometry(geometries) => {
				Kind::Geometry(geometries.into_iter().map(Into::into).collect())
			}
			crate::expr::Kind::Either(kinds) => {
				let kinds: Vec<Kind> = kinds.into_iter().map(Into::into).collect();
				if kinds.is_empty() {
					return Self::Either(vec![Self::Any]);
				}
				Self::Either(kinds)
			}
			crate::expr::Kind::Set(k, l) => Self::Set(Box::new((*k).into()), l),
			crate::expr::Kind::Array(k, l) => Self::Array(Box::new((*k).into()), l),
			crate::expr::Kind::Function(args, ret) => Self::Function(
				args.map(|args| args.into_iter().map(Into::into).collect()),
				ret.map(|ret| Box::new((*ret).into())),
			),
			crate::expr::Kind::Range => Self::Range,
			crate::expr::Kind::Literal(l) => Self::Literal(l.into()),
			crate::expr::Kind::File(k) => Kind::File(k),
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
			Kind::Table(k) => crate::types::PublicKind::Table(k),
			Kind::Record(k) => crate::types::PublicKind::Record(k),
			Kind::Geometry(k) => {
				crate::types::PublicKind::Geometry(k.into_iter().map(Into::into).collect())
			}
			Kind::Either(k) => {
				crate::types::PublicKind::Either(k.into_iter().map(Into::into).collect())
			}
			Kind::Set(k, l) => crate::types::PublicKind::Set(Box::new((*k).into()), l),
			Kind::Array(k, l) => crate::types::PublicKind::Array(Box::new((*k).into()), l),
			Kind::Function(args, ret) => crate::types::PublicKind::Function(
				args.map(|args| args.into_iter().map(Into::into).collect()),
				ret.map(|ret| Box::new((*ret).into())),
			),
			Kind::Range => crate::types::PublicKind::Range,
			Kind::Literal(l) => crate::types::PublicKind::Literal(l.into()),
			Kind::File(k) => crate::types::PublicKind::File(k),
		}
	}
}

impl From<crate::types::PublicKind> for Kind {
	fn from(v: crate::types::PublicKind) -> Self {
		match v {
			crate::types::PublicKind::None => Kind::None,
			crate::types::PublicKind::Null => Kind::Null,
			crate::types::PublicKind::Any => Kind::Any,
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
			crate::types::PublicKind::Table(k) => Kind::Table(k),
			crate::types::PublicKind::Record(k) => Kind::Record(k),
			crate::types::PublicKind::Geometry(k) => {
				Kind::Geometry(k.into_iter().map(Into::into).collect())
			}
			crate::types::PublicKind::Either(k) => {
				Kind::Either(k.into_iter().map(Into::into).collect())
			}
			crate::types::PublicKind::Set(k, l) => Kind::Set(Box::new((*k).into()), l),
			crate::types::PublicKind::Array(k, l) => Kind::Array(Box::new((*k).into()), l),
			crate::types::PublicKind::Function(args, ret) => Kind::Function(
				args.map(|args| args.into_iter().map(Into::into).collect()),
				ret.map(|ret| Box::new((*ret).into())),
			),
			crate::types::PublicKind::Range => Kind::Range,
			crate::types::PublicKind::Literal(l) => Kind::Literal(l.into()),
			crate::types::PublicKind::File(k) => Kind::File(k),
		}
	}
}

impl ToSql for Kind {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Kind::Any => write_sql!(f, fmt, "any"),
			Kind::None => write_sql!(f, fmt, "none"),
			Kind::Null => write_sql!(f, fmt, "null"),
			Kind::Bool => write_sql!(f, fmt, "bool"),
			Kind::Bytes => write_sql!(f, fmt, "bytes"),
			Kind::Datetime => write_sql!(f, fmt, "datetime"),
			Kind::Decimal => write_sql!(f, fmt, "decimal"),
			Kind::Duration => write_sql!(f, fmt, "duration"),
			Kind::Float => write_sql!(f, fmt, "float"),
			Kind::Int => write_sql!(f, fmt, "int"),
			Kind::Number => write_sql!(f, fmt, "number"),
			Kind::Object => write_sql!(f, fmt, "object"),
			Kind::String => write_sql!(f, fmt, "string"),
			Kind::Uuid => write_sql!(f, fmt, "uuid"),
			Kind::Regex => write_sql!(f, fmt, "regex"),
			Kind::Function(_, _) => write_sql!(f, fmt, "function"),
			Kind::Table(k) => {
				if k.is_empty() {
					write_sql!(f, fmt, "table")
				} else {
					write_sql!(
						f,
						fmt,
						"table<{}>",
						Fmt::verbar_separated(k.iter().map(EscapeIdent))
					)
				}
			}
			Kind::Record(k) => {
				if k.is_empty() {
					write_sql!(f, fmt, "record")
				} else {
					write_sql!(
						f,
						fmt,
						"record<{}>",
						Fmt::verbar_separated(k.iter().map(EscapeIdent))
					)
				}
			}
			Kind::Geometry(k) => {
				if k.is_empty() {
					write_sql!(f, fmt, "geometry")
				} else {
					write_sql!(f, fmt, "geometry<{}>", Fmt::verbar_separated(k))
				}
			}
			Kind::Set(k, l) => match (k, l) {
				(k, None) if matches!(**k, Kind::Any) => write_sql!(f, fmt, "set"),
				(k, None) => write_sql!(f, fmt, "set<{k}>"),
				(k, Some(l)) => write_sql!(f, fmt, "set<{k}, {l}>"),
			},
			Kind::Array(k, l) => match (k, l) {
				(k, None) if matches!(**k, Kind::Any) => write_sql!(f, fmt, "array"),
				(k, None) => write_sql!(f, fmt, "array<{k}>"),
				(k, Some(l)) => write_sql!(f, fmt, "array<{k}, {l}>"),
			},
			Kind::Either(k) => write_sql!(f, fmt, "{}", Fmt::verbar_separated(k)),
			Kind::Range => write_sql!(f, fmt, "range"),
			Kind::Literal(l) => write_sql!(f, fmt, "{}", l),
			Kind::File(k) => {
				if k.is_empty() {
					write_sql!(f, fmt, "file")
				} else {
					write_sql!(f, fmt, "file<{}>", Fmt::verbar_separated(k))
				}
			}
		}
	}
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum KindLiteral {
	String(String),
	Integer(i64),
	Float(f64),
	Decimal(Decimal),
	Duration(PublicDuration),
	Array(Vec<Kind>),
	Object(BTreeMap<String, Kind>),
	Bool(bool),
}

impl hash::Hash for KindLiteral {
	fn hash<H: hash::Hasher>(&self, state: &mut H) {
		match self {
			Self::String(v) => v.hash(state),
			Self::Integer(v) => v.hash(state),
			Self::Float(v) => v.to_bits().hash(state),
			Self::Decimal(v) => v.hash(state),
			Self::Duration(v) => v.hash(state),
			Self::Array(v) => v.hash(state),
			Self::Object(v) => v.hash(state),
			Self::Bool(v) => v.hash(state),
		}
	}
}

impl PartialEq for KindLiteral {
	fn eq(&self, other: &Self) -> bool {
		match self {
			KindLiteral::String(a) => {
				if let KindLiteral::String(b) = other {
					a == b
				} else {
					false
				}
			}
			KindLiteral::Integer(a) => {
				if let KindLiteral::Integer(b) = other {
					a == b
				} else {
					false
				}
			}
			KindLiteral::Float(a) => {
				if let KindLiteral::Float(b) = other {
					// Uses exact bit equility instead of normal floating point equilitiy
					a.to_bits() == b.to_bits()
				} else {
					false
				}
			}
			KindLiteral::Decimal(a) => {
				if let KindLiteral::Decimal(b) = other {
					a == b
				} else {
					false
				}
			}
			KindLiteral::Duration(a) => {
				if let KindLiteral::Duration(b) = other {
					a == b
				} else {
					false
				}
			}
			KindLiteral::Array(a) => {
				if let KindLiteral::Array(b) = other {
					a == b
				} else {
					false
				}
			}
			KindLiteral::Object(a) => {
				if let KindLiteral::Object(b) = other {
					a == b
				} else {
					false
				}
			}
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

impl ToSql for KindLiteral {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			KindLiteral::String(s) => write_sql!(f, fmt, "{}", QuoteStr(s)),
			KindLiteral::Integer(n) => write_sql!(f, fmt, "{}", n),
			KindLiteral::Float(n) => write_sql!(f, fmt, "{}", n),
			KindLiteral::Decimal(n) => write_sql!(f, fmt, "{}", n),
			KindLiteral::Duration(d) => write_sql!(f, fmt, "{}", d),
			KindLiteral::Bool(b) => write_sql!(f, fmt, "{}", b),
			KindLiteral::Array(a) => {
				f.push('[');
				if !a.is_empty() {
					let fmt = fmt.increment();
					write_sql!(f, fmt, "{}", Fmt::pretty_comma_separated(a.as_slice()));
				}
				f.push(']')
			}
			KindLiteral::Object(o) => {
				if fmt.is_pretty() {
					f.push('{');
				} else {
					f.push_str("{ ");
				}
				if !o.is_empty() {
					let fmt = fmt.increment();
					write_sql!(
						f,
						fmt,
						"{}",
						Fmt::pretty_comma_separated(o.iter().map(|args| Fmt::new(
							args,
							|(k, v), f, fmt| write_sql!(f, fmt, "{}: {}", EscapeKey(k), v)
						)),)
					);
				}
				if fmt.is_pretty() {
					f.push('}')
				} else {
					f.push_str(" }")
				}
			}
		}
	}
}

impl From<KindLiteral> for crate::expr::kind::KindLiteral {
	fn from(v: KindLiteral) -> Self {
		match v {
			KindLiteral::String(s) => crate::expr::kind::KindLiteral::String(s),
			KindLiteral::Integer(i) => crate::expr::kind::KindLiteral::Integer(i),
			KindLiteral::Float(f) => crate::expr::kind::KindLiteral::Float(f),
			KindLiteral::Decimal(d) => crate::expr::kind::KindLiteral::Decimal(d),
			KindLiteral::Duration(d) => crate::expr::kind::KindLiteral::Duration(d.into()),
			KindLiteral::Array(a) => {
				crate::expr::kind::KindLiteral::Array(a.into_iter().map(Into::into).collect())
			}
			KindLiteral::Object(o) => crate::expr::kind::KindLiteral::Object(
				o.into_iter().map(|(k, v)| (k, v.into())).collect(),
			),
			KindLiteral::Bool(b) => crate::expr::kind::KindLiteral::Bool(b),
		}
	}
}

impl From<crate::expr::kind::KindLiteral> for KindLiteral {
	fn from(v: crate::expr::kind::KindLiteral) -> Self {
		match v {
			crate::expr::kind::KindLiteral::String(s) => Self::String(s),
			crate::expr::kind::KindLiteral::Integer(i) => Self::Integer(i),
			crate::expr::kind::KindLiteral::Float(f) => Self::Float(f),
			crate::expr::kind::KindLiteral::Decimal(d) => Self::Decimal(d),
			crate::expr::kind::KindLiteral::Duration(d) => Self::Duration(d.into()),
			crate::expr::kind::KindLiteral::Array(a) => {
				Self::Array(a.into_iter().map(Into::into).collect())
			}
			crate::expr::kind::KindLiteral::Object(o) => {
				Self::Object(o.into_iter().map(|(k, v)| (k, v.into())).collect())
			}
			crate::expr::kind::KindLiteral::Bool(b) => Self::Bool(b),
		}
	}
}

impl From<KindLiteral> for crate::types::PublicKindLiteral {
	fn from(v: KindLiteral) -> Self {
		match v {
			KindLiteral::Bool(b) => crate::types::PublicKindLiteral::Bool(b),
			KindLiteral::Integer(i) => crate::types::PublicKindLiteral::Integer(i),
			KindLiteral::Float(f) => crate::types::PublicKindLiteral::Float(f),
			KindLiteral::Decimal(d) => crate::types::PublicKindLiteral::Decimal(d),
			KindLiteral::String(s) => crate::types::PublicKindLiteral::String(s),
			KindLiteral::Duration(d) => crate::types::PublicKindLiteral::Duration(d),
			KindLiteral::Array(a) => {
				crate::types::PublicKindLiteral::Array(a.into_iter().map(Into::into).collect())
			}
			KindLiteral::Object(o) => crate::types::PublicKindLiteral::Object(
				o.into_iter().map(|(k, v)| (k, v.into())).collect(),
			),
		}
	}
}

impl From<crate::types::PublicKindLiteral> for KindLiteral {
	fn from(v: crate::types::PublicKindLiteral) -> Self {
		match v {
			crate::types::PublicKindLiteral::Bool(b) => Self::Bool(b),
			crate::types::PublicKindLiteral::Integer(i) => Self::Integer(i),
			crate::types::PublicKindLiteral::Float(f) => Self::Float(f),
			crate::types::PublicKindLiteral::Decimal(d) => Self::Decimal(d),
			crate::types::PublicKindLiteral::String(s) => Self::String(s),
			crate::types::PublicKindLiteral::Duration(d) => Self::Duration(d),
			crate::types::PublicKindLiteral::Array(a) => {
				Self::Array(a.into_iter().map(Into::into).collect())
			}
			crate::types::PublicKindLiteral::Object(o) => {
				Self::Object(o.into_iter().map(|(k, v)| (k, v.into())).collect())
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use rstest::rstest;

	use super::*;

	#[rstest]
	#[case::any(Kind::Any, "any")]
	#[case::none(Kind::None, "none")]
	#[case::null(Kind::Null, "null")]
	#[case::bool(Kind::Bool, "bool")]
	#[case::bytes(Kind::Bytes, "bytes")]
	#[case::datetime(Kind::Datetime, "datetime")]
	#[case::decimal(Kind::Decimal, "decimal")]
	#[case::duration(Kind::Duration, "duration")]
	#[case::float(Kind::Float, "float")]
	#[case::int(Kind::Int, "int")]
	#[case::number(Kind::Number, "number")]
	#[case::object(Kind::Object, "object")]
	#[case::string(Kind::String, "string")]
	#[case::uuid(Kind::Uuid, "uuid")]
	#[case::regex(Kind::Regex, "regex")]
	#[case::range(Kind::Range, "range")]
	#[case::function(Kind::Function(None, None), "function")]
	#[case::table_empty(Kind::Table(vec![]), "table")]
	#[case::table_single(Kind::Table(vec!["users".to_string()]), "table<users>")]
	#[case::table_multiple(Kind::Table(vec!["users".to_string(), "posts".to_string()]), "table<users | posts>")]
	#[case::record_empty(Kind::Record(vec![]), "record")]
	#[case::record_single(Kind::Record(vec!["users".to_string()]), "record<users>")]
	#[case::geometry_empty(Kind::Geometry(vec![]), "geometry")]
	#[case::geometry_single(Kind::Geometry(vec![GeometryKind::Point]), "geometry<point>")]
	#[case::set_any(Kind::Set(Box::new(Kind::Any), None), "set")]
	#[case::set_typed(Kind::Set(Box::new(Kind::String), None), "set<string>")]
	#[case::array_any(Kind::Array(Box::new(Kind::Any), None), "array")]
	#[case::array_typed(Kind::Array(Box::new(Kind::String), Some(5)), "array<string, 5>")]
	#[case::either(Kind::Either(vec![Kind::String, Kind::Int]), "string | int")]
	#[case::file_empty(Kind::File(vec![]), "file")]
	#[case::file_single(Kind::File(vec!["bucket".to_string()]), "file<bucket>")]
	fn test_kind_display(#[case] kind: Kind, #[case] expected: &str) {
		assert_eq!(kind.to_string(), expected);
	}

	#[rstest]
	#[case::any(Kind::Any)]
	#[case::none(Kind::None)]
	#[case::null(Kind::Null)]
	#[case::bool(Kind::Bool)]
	#[case::bytes(Kind::Bytes)]
	#[case::datetime(Kind::Datetime)]
	#[case::decimal(Kind::Decimal)]
	#[case::duration(Kind::Duration)]
	#[case::float(Kind::Float)]
	#[case::int(Kind::Int)]
	#[case::number(Kind::Number)]
	#[case::object(Kind::Object)]
	#[case::string(Kind::String)]
	#[case::uuid(Kind::Uuid)]
	#[case::regex(Kind::Regex)]
	#[case::range(Kind::Range)]
	#[case::table(Kind::Table(vec!["users".to_string()]))]
	#[case::record(Kind::Record(vec!["users".to_string()]))]
	#[case::geometry(Kind::Geometry(vec![GeometryKind::Point]))]
	#[case::set(Kind::Set(Box::new(Kind::String), None))]
	#[case::array(Kind::Array(Box::new(Kind::String), None))]
	#[case::either(Kind::Either(vec![Kind::String, Kind::Int]))]
	#[case::file(Kind::File(vec!["bucket".to_string()]))]
	fn test_kind_conversions_expr(#[case] sql_kind: Kind) {
		let expr_kind: crate::expr::Kind = sql_kind.clone().into();
		let back_to_sql: Kind = expr_kind.into();
		assert_eq!(sql_kind, back_to_sql);
	}

	#[rstest]
	#[case::any(Kind::Any)]
	#[case::none(Kind::None)]
	#[case::null(Kind::Null)]
	#[case::bool(Kind::Bool)]
	#[case::bytes(Kind::Bytes)]
	#[case::datetime(Kind::Datetime)]
	#[case::decimal(Kind::Decimal)]
	#[case::duration(Kind::Duration)]
	#[case::float(Kind::Float)]
	#[case::int(Kind::Int)]
	#[case::number(Kind::Number)]
	#[case::object(Kind::Object)]
	#[case::string(Kind::String)]
	#[case::uuid(Kind::Uuid)]
	#[case::regex(Kind::Regex)]
	#[case::range(Kind::Range)]
	#[case::table(Kind::Table(vec!["users".to_string()]))]
	#[case::record(Kind::Record(vec!["users".to_string()]))]
	#[case::geometry(Kind::Geometry(vec![GeometryKind::Point]))]
	#[case::set(Kind::Set(Box::new(Kind::String), None))]
	#[case::array(Kind::Array(Box::new(Kind::String), None))]
	#[case::either(Kind::Either(vec![Kind::String, Kind::Int]))]
	#[case::file(Kind::File(vec!["bucket".to_string()]))]
	fn test_kind_conversions_public(#[case] sql_kind: Kind) {
		let public_kind: crate::types::PublicKind = sql_kind.clone().into();
		let back_to_sql: Kind = public_kind.into();
		assert_eq!(sql_kind, back_to_sql);
	}

	#[rstest]
	#[case::any(Kind::Any)]
	#[case::table(Kind::Table(vec!["users".to_string()]))]
	#[case::record(Kind::Record(vec!["users".to_string()]))]
	#[case::geometry(Kind::Geometry(vec![GeometryKind::Point]))]
	fn test_kind_flatten(#[case] kind: Kind) {
		let flattened = kind.clone().flatten();
		assert_eq!(flattened.len(), 1);
		assert_eq!(flattened[0], kind);
	}

	#[test]
	fn test_kind_either() {
		let kinds =
			vec![Kind::Table(vec!["users".to_string()]), Kind::Table(vec!["posts".to_string()])];
		let either = Kind::either(kinds.clone());
		assert!(matches!(either, Kind::Either(_)));
		if let Kind::Either(inner) = either {
			assert_eq!(inner.len(), 2);
		}
	}
}
