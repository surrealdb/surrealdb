use super::escape::EscapeKey;
use crate::key::index::bt::Bt;
use crate::sql::fmt::{Fmt, Pretty, is_pretty, pretty_indent};
use crate::sql::{Ident, Idiom};
use crate::val::{Duration, Strand};
use rust_decimal::Decimal;
use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter, Write};

/// The kind, or data type, of a value or field.
#[derive(Clone, Debug, PartialEq, Eq)]
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
	/// Geometric 2D point type with longitude *then* latitude coordinates.
	/// This follows the GeoJSON spec.
	Point,
	/// String type.
	String,
	/// UUID type.
	Uuid,
	/// Regular expression type.
	Regex,
	/// A record type.
	Record(Vec<Ident>),
	/// A geometry type.
	/// The vec contains the geometry types as strings, for example `"point"` or `"polygon"`.
	Geometry(Vec<String>),
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
	/// The first option is the argument types, the second is the optional return type.
	Function(Option<Vec<Kind>>, Option<Box<Kind>>),
	/// A range type.
	Range,
	/// A literal type.
	/// The literal type is used to represent a type that can only be a single value.
	/// For example, `"a"` is a literal type which can only ever be `"a"`.
	/// This can be used in the `Kind::Either` type to represent an enum.
	Literal(KindLiteral),
	/// A references type representing a link to another table or field.
	References(Option<Ident>, Option<Idiom>),
	/// A file type.
	/// If the kind was specified without a bucket the vec will be empty.
	/// So `<file>` is just `Kind::File(Vec::new())`
	File(Vec<Ident>),
}

impl Default for Kind {
	fn default() -> Self {
		Self::Any
	}
}

impl From<Kind> for crate::expr::Kind {
	fn from(v: Kind) -> Self {
		match v {
			Kind::Any => crate::expr::Kind::Any,
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
			Kind::Point => crate::expr::Kind::Point,
			Kind::String => crate::expr::Kind::String,
			Kind::Uuid => crate::expr::Kind::Uuid,
			Kind::Regex => crate::expr::Kind::Regex,
			Kind::Record(tables) => {
				crate::expr::Kind::Record(tables.into_iter().map(Into::into).collect())
			}
			Kind::Geometry(geometries) => {
				crate::expr::Kind::Geometry(geometries.into_iter().collect())
			}
			Kind::Option(k) => crate::expr::Kind::Option(Box::new(k.as_ref().clone().into())),
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
			Kind::References(t, i) => {
				crate::expr::Kind::References(t.map(Into::into), i.map(Into::into))
			}
			Kind::File(k) => crate::expr::Kind::File(k.into_iter().map(Into::into).collect()),
		}
	}
}

impl From<crate::expr::Kind> for Kind {
	fn from(v: crate::expr::Kind) -> Self {
		match v {
			crate::expr::Kind::Any => Kind::Any,
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
			crate::expr::Kind::Point => Kind::Point,
			crate::expr::Kind::String => Kind::String,
			crate::expr::Kind::Uuid => Kind::Uuid,
			crate::expr::Kind::Regex => Kind::Regex,
			crate::expr::Kind::Record(tables) => {
				Kind::Record(tables.into_iter().map(From::from).collect())
			}
			crate::expr::Kind::Geometry(geometries) => {
				Kind::Geometry(geometries.into_iter().collect())
			}
			crate::expr::Kind::Option(k) => Kind::Option(Box::new((*k).into())),
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
			crate::expr::Kind::References(t, i) => {
				Self::References(t.map(From::from), i.map(From::from))
			}
			crate::expr::Kind::File(k) => {
				Kind::File(k.into_iter().map(Into::<Ident>::into).collect())
			}
		}
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
			Kind::Point => f.write_str("point"),
			Kind::String => f.write_str("string"),
			Kind::Uuid => f.write_str("uuid"),
			Kind::Regex => f.write_str("regex"),
			Kind::Function(_, _) => f.write_str("function"),
			Kind::Option(k) => write!(f, "option<{}>", k),
			Kind::Record(k) => {
				if k.is_empty() {
					write!(f, "record")
				} else {
					write!(f, "record<{}>", Fmt::verbar_separated(k))
				}
			}
			Kind::Geometry(k) => {
				if k.is_empty() {
					write!(f, "geometry")
				} else {
					write!(f, "geometry<{}>", Fmt::verbar_separated(k))
				}
			}
			Kind::Set(k, l) => match (k, l) {
				(k, None) if matches!(**k, Kind::Any) => write!(f, "set"),
				(k, None) => write!(f, "set<{k}>"),
				(k, Some(l)) => write!(f, "set<{k}, {l}>"),
			},
			Kind::Array(k, l) => match (k, l) {
				(k, None) if matches!(**k, Kind::Any) => write!(f, "array"),
				(k, None) => write!(f, "array<{k}>"),
				(k, Some(l)) => write!(f, "array<{k}, {l}>"),
			},
			Kind::Either(k) => write!(f, "{}", Fmt::verbar_separated(k)),
			Kind::Range => f.write_str("range"),
			Kind::Literal(l) => write!(f, "{}", l),
			Kind::References(t, i) => match (t, i) {
				(Some(t), None) => write!(f, "references<{}>", t),
				(Some(t), Some(i)) => write!(f, "references<{}, {}>", t, i),
				(None, _) => f.write_str("references"),
			},
			Kind::File(k) => {
				if k.is_empty() {
					write!(f, "file")
				} else {
					write!(f, "file<{}>", Fmt::verbar_separated(k))
				}
			}
		}
	}
}

#[derive(Clone, Debug)]
pub enum KindLiteral {
	String(Strand),
	Integer(i64),
	Float(f64),
	Decimal(Decimal),
	Duration(Duration),
	Array(Vec<Kind>),
	Object(BTreeMap<String, Kind>),
	Bool(bool),
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

impl Display for KindLiteral {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			KindLiteral::String(s) => write!(f, "{}", s),
			KindLiteral::Integer(n) => write!(f, "{}", n),
			KindLiteral::Float(n) => write!(f, "{}", n),
			KindLiteral::Decimal(n) => write!(f, "{}", n),
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
			}
		}
	}
}

impl From<KindLiteral> for crate::expr::kind::KindLiteral {
	fn from(v: KindLiteral) -> Self {
		match v {
			KindLiteral::String(s) => crate::expr::kind::KindLiteral::String(s.into()),
			KindLiteral::Integer(i) => crate::expr::kind::KindLiteral::Integer(i),
			KindLiteral::Float(f) => crate::expr::kind::KindLiteral::Float(f),
			KindLiteral::Decimal(d) => crate::expr::kind::KindLiteral::Decimal(d),
			KindLiteral::Duration(d) => crate::expr::kind::KindLiteral::Duration(d),
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
			crate::expr::kind::KindLiteral::String(s) => Self::String(s.into()),
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
