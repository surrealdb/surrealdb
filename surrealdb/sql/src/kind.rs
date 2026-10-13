use std::collections::{BTreeMap, HashSet};
use std::fmt::Display;
use std::hash;

use common::fmt::{EscapeKwFreeIdent, EscapeObjectKey, Float, Fmt, QuoteStr, SqlDuration};
use rust_decimal::Decimal;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::TableName;

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
	Table(Vec<TableName>),
	/// A record type.
	Record(Vec<TableName>),
	/// A geometry type.
	Geometry(Vec<GeometryKind>),
	/// An either type.
	/// Can be any of the kinds in the vec.
	Either(
		#[cfg_attr(feature = "arbitrary", arbitrary(with = crate::arbitrary::either_kind))]
		Vec<Kind>,
	),
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
	pub fn flatten(self) -> Vec<Kind> {
		match self {
			Kind::Either(x) => x.into_iter().flat_map(|k| k.flatten()).collect(),
			_ => vec![self],
		}
	}

	pub fn either(kinds: Vec<Kind>) -> Kind {
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

impl ToSql for Kind {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Kind::Any => f.push_str("any"),
			Kind::None => f.push_str("none"),
			Kind::Null => f.push_str("null"),
			Kind::Bool => f.push_str("bool"),
			Kind::Bytes => f.push_str("bytes"),
			Kind::Datetime => f.push_str("datetime"),
			Kind::Decimal => f.push_str("decimal"),
			Kind::Duration => f.push_str("duration"),
			Kind::Float => f.push_str("float"),
			Kind::Int => f.push_str("int"),
			Kind::Number => f.push_str("number"),
			Kind::Object => f.push_str("object"),
			Kind::String => f.push_str("string"),
			Kind::Uuid => f.push_str("uuid"),
			Kind::Regex => f.push_str("regex"),
			Kind::Function(_, _) => f.push_str("function"),
			Kind::Table(k) => {
				if k.is_empty() {
					f.push_str("table");
				} else {
					write_sql!(
						f,
						fmt,
						"table<{}>",
						Fmt::verbar_separated(k.iter().map(|x| EscapeKwFreeIdent(x.as_str())))
					);
				}
			}
			Kind::Record(k) => {
				if k.is_empty() {
					f.push_str("record");
				} else {
					write_sql!(
						f,
						fmt,
						"record<{}>",
						Fmt::verbar_separated(k.iter().map(|x| EscapeKwFreeIdent(x.as_str())))
					);
				}
			}
			Kind::Geometry(k) => {
				if k.is_empty() {
					f.push_str("geometry");
				} else {
					write_sql!(f, fmt, "geometry<{}>", Fmt::verbar_separated(k));
				}
			}
			Kind::Set(k, l) => match (k, l) {
				(k, None) if matches!(**k, Kind::Any) => f.push_str("set"),
				(k, None) => write_sql!(f, fmt, "set<{k}>"),
				(k, Some(l)) => write_sql!(f, fmt, "set<{k}, {l}>"),
			},
			Kind::Array(k, l) => match (k, l) {
				(k, None) if matches!(**k, Kind::Any) => f.push_str("array"),
				(k, None) => write_sql!(f, fmt, "array<{k}>"),
				(k, Some(l)) => write_sql!(f, fmt, "array<{k}, {l}>"),
			},
			Kind::Either(k) => write_sql!(f, fmt, "{}", Fmt::verbar_separated(k)),
			Kind::Range => f.push_str("range"),
			Kind::Literal(l) => l.fmt_sql(f, fmt),
			Kind::File(k) => {
				if k.is_empty() {
					f.push_str("file");
				} else {
					write_sql!(
						f,
						fmt,
						"file<{}>",
						Fmt::verbar_separated(k.iter().map(|x| EscapeKwFreeIdent(x)))
					);
				}
			}
		}
	}
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum KindLiteral {
	String(Strand),
	Integer(i64),
	Float(f64),
	Decimal(Decimal),
	Duration(std::time::Duration),
	Array(Vec<Kind>),
	Object(BTreeMap<Strand, Kind>),
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
			KindLiteral::Float(n) => write_sql!(f, fmt, " {}", Float(*n)),
			KindLiteral::Decimal(n) => write_sql!(f, fmt, " {}", n),
			KindLiteral::Duration(d) => write_sql!(f, fmt, "{}", SqlDuration(*d)),
			KindLiteral::Bool(b) => write_sql!(f, fmt, "{}", b),
			KindLiteral::Array(a) => {
				f.push('[');
				if !a.is_empty() {
					let fmt = fmt.increment();
					write_sql!(f, fmt, "{}", Fmt::pretty_comma_separated(a.as_slice()));
				}
				f.push(']');
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
							|(k, v), f, fmt| {
								write_sql!(f, fmt, "{}: {}", EscapeObjectKey(k), v)
							}
						)),)
					);
				}
				if fmt.is_pretty() {
					f.push('}');
				} else {
					f.push_str(" }");
				}
			}
		}
	}
}

impl Display for Kind {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.to_sql())
	}
}

/// Whether a kind admits an object anywhere in its structure.
///
/// Mirrors `expr::statements::define::kind_contains_object`. Duplicated at this
/// layer so the parser and the AST's fuzz generator can ask the question
/// without lowering a `sql::Kind` into an `expr::Kind` first.
pub fn kind_contains_object(kind: &Kind) -> bool {
	match kind {
		Kind::Object => true,
		Kind::Either(kinds) => kinds.iter().any(kind_contains_object),
		Kind::Array(inner, _) | Kind::Set(inner, _) => kind_contains_object(inner),
		Kind::Literal(KindLiteral::Object(_)) => true,
		Kind::Literal(KindLiteral::Array(kinds)) => kinds.iter().any(kind_contains_object),
		_ => false,
	}
}

impl From<GeometryKind> for surrealdb_types::GeometryKind {
	fn from(v: GeometryKind) -> Self {
		match v {
			GeometryKind::Point => surrealdb_types::GeometryKind::Point,
			GeometryKind::Line => surrealdb_types::GeometryKind::Line,
			GeometryKind::Polygon => surrealdb_types::GeometryKind::Polygon,
			GeometryKind::MultiPoint => surrealdb_types::GeometryKind::MultiPoint,
			GeometryKind::MultiLine => surrealdb_types::GeometryKind::MultiLine,
			GeometryKind::MultiPolygon => surrealdb_types::GeometryKind::MultiPolygon,
			GeometryKind::Collection => surrealdb_types::GeometryKind::Collection,
		}
	}
}

impl From<surrealdb_types::GeometryKind> for GeometryKind {
	fn from(v: surrealdb_types::GeometryKind) -> Self {
		match v {
			surrealdb_types::GeometryKind::Point => GeometryKind::Point,
			surrealdb_types::GeometryKind::Line => GeometryKind::Line,
			surrealdb_types::GeometryKind::Polygon => GeometryKind::Polygon,
			surrealdb_types::GeometryKind::MultiPoint => GeometryKind::MultiPoint,
			surrealdb_types::GeometryKind::MultiLine => GeometryKind::MultiLine,
			surrealdb_types::GeometryKind::MultiPolygon => GeometryKind::MultiPolygon,
			surrealdb_types::GeometryKind::Collection => GeometryKind::Collection,
		}
	}
}

impl From<Kind> for surrealdb_types::Kind {
	fn from(v: Kind) -> Self {
		match v {
			Kind::Any => surrealdb_types::Kind::Any,
			Kind::None => surrealdb_types::Kind::None,
			Kind::Null => surrealdb_types::Kind::Null,
			Kind::Bool => surrealdb_types::Kind::Bool,
			Kind::Bytes => surrealdb_types::Kind::Bytes,
			Kind::Datetime => surrealdb_types::Kind::Datetime,
			Kind::Decimal => surrealdb_types::Kind::Decimal,
			Kind::Duration => surrealdb_types::Kind::Duration,
			Kind::Float => surrealdb_types::Kind::Float,
			Kind::Int => surrealdb_types::Kind::Int,
			Kind::Number => surrealdb_types::Kind::Number,
			Kind::Object => surrealdb_types::Kind::Object,
			Kind::String => surrealdb_types::Kind::String,
			Kind::Uuid => surrealdb_types::Kind::Uuid,
			Kind::Regex => surrealdb_types::Kind::Regex,
			Kind::Table(k) => surrealdb_types::Kind::Table(k.into_iter().map(Into::into).collect()),
			Kind::Record(k) => {
				surrealdb_types::Kind::Record(k.into_iter().map(Into::into).collect())
			}
			Kind::Geometry(k) => {
				surrealdb_types::Kind::Geometry(k.into_iter().map(Into::into).collect())
			}
			Kind::Either(k) => {
				surrealdb_types::Kind::Either(k.into_iter().map(Into::into).collect())
			}
			Kind::Set(k, l) => surrealdb_types::Kind::Set(Box::new((*k).into()), l),
			Kind::Array(k, l) => surrealdb_types::Kind::Array(Box::new((*k).into()), l),
			Kind::Function(args, ret) => surrealdb_types::Kind::Function(
				args.map(|args| args.into_iter().map(Into::into).collect()),
				ret.map(|ret| Box::new((*ret).into())),
			),
			Kind::Range => surrealdb_types::Kind::Range,
			Kind::Literal(l) => surrealdb_types::Kind::Literal(l.into()),
			Kind::File(k) => surrealdb_types::Kind::File(k),
		}
	}
}

impl From<surrealdb_types::Kind> for Kind {
	fn from(v: surrealdb_types::Kind) -> Self {
		match v {
			surrealdb_types::Kind::None => Kind::None,
			surrealdb_types::Kind::Null => Kind::Null,
			surrealdb_types::Kind::Any => Kind::Any,
			surrealdb_types::Kind::Bool => Kind::Bool,
			surrealdb_types::Kind::Bytes => Kind::Bytes,
			surrealdb_types::Kind::Datetime => Kind::Datetime,
			surrealdb_types::Kind::Decimal => Kind::Decimal,
			surrealdb_types::Kind::Duration => Kind::Duration,
			surrealdb_types::Kind::Float => Kind::Float,
			surrealdb_types::Kind::Int => Kind::Int,
			surrealdb_types::Kind::Number => Kind::Number,
			surrealdb_types::Kind::Object => Kind::Object,
			surrealdb_types::Kind::String => Kind::String,
			surrealdb_types::Kind::Uuid => Kind::Uuid,
			surrealdb_types::Kind::Regex => Kind::Regex,
			surrealdb_types::Kind::Table(k) => Kind::Table(k.into_iter().map(Into::into).collect()),
			surrealdb_types::Kind::Record(k) => {
				Kind::Record(k.into_iter().map(Into::into).collect())
			}
			surrealdb_types::Kind::Geometry(k) => {
				Kind::Geometry(k.into_iter().map(Into::into).collect())
			}
			surrealdb_types::Kind::Either(k) => {
				Kind::Either(k.into_iter().map(Into::into).collect())
			}
			surrealdb_types::Kind::Set(k, l) => Kind::Set(Box::new((*k).into()), l),
			surrealdb_types::Kind::Array(k, l) => Kind::Array(Box::new((*k).into()), l),
			surrealdb_types::Kind::Function(args, ret) => Kind::Function(
				args.map(|args| args.into_iter().map(Into::into).collect()),
				ret.map(|ret| Box::new((*ret).into())),
			),
			surrealdb_types::Kind::Range => Kind::Range,
			surrealdb_types::Kind::Literal(l) => Kind::Literal(l.into()),
			surrealdb_types::Kind::File(k) => Kind::File(k),
		}
	}
}

impl From<KindLiteral> for surrealdb_types::KindLiteral {
	fn from(v: KindLiteral) -> Self {
		match v {
			KindLiteral::Bool(b) => surrealdb_types::KindLiteral::Bool(b),
			KindLiteral::Integer(i) => surrealdb_types::KindLiteral::Integer(i),
			KindLiteral::Float(f) => surrealdb_types::KindLiteral::Float(f),
			KindLiteral::Decimal(d) => surrealdb_types::KindLiteral::Decimal(d),
			KindLiteral::String(s) => surrealdb_types::KindLiteral::String(s.into_string()),
			KindLiteral::Duration(d) => {
				surrealdb_types::KindLiteral::Duration(surrealdb_types::Duration::from(d))
			}
			KindLiteral::Array(a) => {
				surrealdb_types::KindLiteral::Array(a.into_iter().map(Into::into).collect())
			}
			KindLiteral::Object(o) => surrealdb_types::KindLiteral::Object(
				o.into_iter().map(|(k, v)| (k.into_string(), v.into())).collect(),
			),
		}
	}
}

impl From<surrealdb_types::KindLiteral> for KindLiteral {
	fn from(v: surrealdb_types::KindLiteral) -> Self {
		match v {
			surrealdb_types::KindLiteral::Bool(b) => Self::Bool(b),
			surrealdb_types::KindLiteral::Integer(i) => Self::Integer(i),
			surrealdb_types::KindLiteral::Float(f) => Self::Float(f),
			surrealdb_types::KindLiteral::Decimal(d) => Self::Decimal(d),
			surrealdb_types::KindLiteral::String(s) => Self::String(s.into()),
			surrealdb_types::KindLiteral::Duration(d) => Self::Duration(d.into_inner()),
			surrealdb_types::KindLiteral::Array(a) => {
				Self::Array(a.into_iter().map(Into::into).collect())
			}
			surrealdb_types::KindLiteral::Object(o) => {
				Self::Object(o.into_iter().map(|(k, v)| (k.into(), v.into())).collect())
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
	#[case::table_single(Kind::Table(vec!["users".into()]), "table<users>")]
	#[case::table_multiple(Kind::Table(vec!["users".into(), "posts".into()]), "table<users | posts>")]
	#[case::record_empty(Kind::Record(vec![]), "record")]
	#[case::record_single(Kind::Record(vec!["users".into()]), "record<users>")]
	#[case::geometry_empty(Kind::Geometry(vec![]), "geometry")]
	#[case::geometry_single(Kind::Geometry(vec![GeometryKind::Point]), "geometry<point>")]
	#[case::set_any(Kind::Set(Box::new(Kind::Any), None), "set")]
	#[case::set_typed(Kind::Set(Box::new(Kind::String), None), "set<string>")]
	#[case::array_any(Kind::Array(Box::new(Kind::Any), None), "array")]
	#[case::array_typed(Kind::Array(Box::new(Kind::String), Some(5)), "array<string, 5>")]
	#[case::either(Kind::Either(vec![Kind::String, Kind::Int]), "string | int")]
	#[case::file_empty(Kind::File(vec![]), "file")]
	#[case::file_single(Kind::File(vec!["bucket".to_string()]), "file<bucket>")]
	fn test_kind_to_sql(#[case] kind: Kind, #[case] expected: &str) {
		assert_eq!(kind.to_sql(), expected);
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
	#[case::table(Kind::Table(vec!["users".into()]))]
	#[case::record(Kind::Record(vec!["users".into()]))]
	#[case::geometry(Kind::Geometry(vec![GeometryKind::Point]))]
	#[case::set(Kind::Set(Box::new(Kind::String), None))]
	#[case::array(Kind::Array(Box::new(Kind::String), None))]
	#[case::either(Kind::Either(vec![Kind::String, Kind::Int]))]
	#[case::file(Kind::File(vec!["bucket".to_string()]))]
	fn test_kind_conversions_public(#[case] sql_kind: Kind) {
		let public_kind: surrealdb_types::Kind = sql_kind.clone().into();
		let back_to_sql: Kind = public_kind.into();
		assert_eq!(sql_kind, back_to_sql);
	}

	#[rstest]
	#[case::any(Kind::Any)]
	#[case::table(Kind::Table(vec!["users".into()]))]
	#[case::record(Kind::Record(vec!["users".into()]))]
	#[case::geometry(Kind::Geometry(vec![GeometryKind::Point]))]
	fn test_kind_flatten(#[case] kind: Kind) {
		let flattened = kind.clone().flatten();
		assert_eq!(flattened.len(), 1);
		assert_eq!(flattened[0], kind);
	}

	#[test]
	fn test_kind_either() {
		let kinds = vec![Kind::Table(vec!["users".into()]), Kind::Table(vec!["posts".into()])];
		let either = Kind::either(kinds);
		assert!(matches!(either, Kind::Either(_)));
		if let Kind::Either(inner) = either {
			assert_eq!(inner.len(), 2);
		}
	}
}
