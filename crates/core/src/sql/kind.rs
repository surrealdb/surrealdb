use super::escape::EscapeKey;
use super::{
	Array, Bytes, Closure, Datetime, Duration, File, Geometry, Ident, Idiom, Number, Object, Range,
	Regex, Strand, Thing, Uuid,
};

use crate::sql::{
	SqlValue, Table,
	fmt::{Fmt, Pretty, is_pretty, pretty_indent},
};
use geo::{LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon};
use revision::revisioned;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter, Write};

/// The kind, or data type, of a value or field.
#[revisioned(revision = 2)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
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
	#[revision(start = 2)]
	Regex,
	/// A record type.
	Record(Vec<Table>),
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
	Literal(Literal),
	/// A references type representing a link to another table or field.
	References(Option<Table>, Option<Idiom>),
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

impl Kind {
	/// Returns the kind of a type.
	pub(crate) fn of<T: HasKind>() -> Kind {
		T::kind()
	}

	/// Returns true if this type is an `any`
	pub(crate) fn is_any(&self) -> bool {
		matches!(self, Kind::Any)
	}

	/// Returns true if this type is optional
	pub(crate) fn can_be_none(&self) -> bool {
		matches!(self, Kind::Option(_) | Kind::Any)
	}

	/// Returns the kind in case of a literal, otherwise returns the kind itself
	fn to_non_literal_kind(&self) -> Self {
		match self {
			Kind::Literal(l) => l.to_kind(),
			k => k.to_owned(),
		}
	}

	/// Returns Some if this type can be converted into a discriminated object, None otherwise
	pub(crate) fn to_discriminated(&self) -> Option<Kind> {
		match self {
			Kind::Either(nested) => {
				if let Some(nested) = nested
					.iter()
					.map(|k| match k {
						Kind::Literal(Literal::Object(o)) => Some(o),
						_ => None,
					})
					.collect::<Option<Vec<&BTreeMap<String, Kind>>>>()
				{
					if let Some(first) = nested.first() {
						let mut key: Option<String> = None;

						'key: for (k, v) in first.iter() {
							let mut kinds: Vec<Kind> = vec![v.to_owned()];
							for item in nested[1..].iter() {
								if let Some(kind) = item.get(k) {
									match kind {
										Kind::Literal(l)
											if kinds.contains(&l.to_kind())
												|| kinds.contains(&Kind::Literal(l.to_owned())) =>
										{
											continue 'key;
										}
										kind if kinds
											.iter()
											.any(|k| *kind == k.to_non_literal_kind()) =>
										{
											continue 'key;
										}
										kind => {
											kinds.push(kind.to_owned());
										}
									}
								} else {
									continue 'key;
								}
							}

							key = Some(k.clone());
							break;
						}

						if let Some(key) = key {
							return Some(Kind::Literal(Literal::DiscriminatedObject(
								key.clone(),
								nested.into_iter().map(|o| o.to_owned()).collect(),
							)));
						}
					}
				}

				None
			}
			_ => None,
		}
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
				Kind::Record(tables.into_iter().map(Into::<Table>::into).collect())
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
				Self::References(t.map(Into::into), i.map(Into::into))
			}
			crate::expr::Kind::File(k) => {
				Kind::File(k.into_iter().map(Into::<Ident>::into).collect())
			}
		}
	}
}

/// Trait for retrieving the `kind` equivalent of a rust type.
///
/// Returns the most general kind for a type.
/// For example Number could be either number or float or int or decimal but the most general is
/// number.
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

impl HasKind for Thing {
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
	($($name:ty => $kind:literal),*$(,)?) => {
		$(
			impl HasKind for $name{
				fn kind() -> Kind{
					Kind::Geometry(vec![$kind.to_string()])
				}
			}
		)*
	}
}
impl_geometry_has_kind! {
	Point<f64> => "point",
	LineString<f64> => "line",
	MultiPoint<f64> => "multipoint",
	Polygon<f64> => "polygon",
	MultiLineString<f64> => "multiline",
	MultiPolygon<f64> => "multipolygon",
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
				(k, None) if k.is_any() => write!(f, "set"),
				(k, None) => write!(f, "set<{k}>"),
				(k, Some(l)) => write!(f, "set<{k}, {l}>"),
			},
			Kind::Array(k, l) => match (k, l) {
				(k, None) if k.is_any() => write!(f, "array"),
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

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Literal {
	String(Strand),
	Number(Number),
	Duration(Duration),
	Array(Vec<Kind>),
	Object(BTreeMap<String, Kind>),
	DiscriminatedObject(String, Vec<BTreeMap<String, Kind>>),
	Bool(bool),
}

impl Literal {
	pub fn to_kind(&self) -> Kind {
		match self {
			Self::String(_) => Kind::String,
			Self::Number(_) => Kind::Number,
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
			Self::DiscriminatedObject(_, _) => Kind::Object,
			Self::Bool(_) => Kind::Bool,
		}
	}

	pub fn validate_value(&self, value: &SqlValue) -> bool {
		match self {
			Self::String(v) => match value {
				SqlValue::Strand(s) => s == v,
				_ => false,
			},
			Self::Number(v) => match value {
				SqlValue::Number(n) => n == v,
				_ => false,
			},
			Self::Duration(v) => match value {
				SqlValue::Duration(n) => n == v,
				_ => false,
			},
			Self::Bool(v) => match value {
				SqlValue::Bool(b) => b == v,
				_ => false,
			},
			Self::Array(a) => match value {
				SqlValue::Array(x) => {
					if a.len() != x.len() {
						return false;
					}

					for (i, inner) in a.iter().enumerate() {
						if let Some(value) = x.get(i) {
							if value.to_owned().coerce_to_kind(inner).is_err() {
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
				SqlValue::Object(x) => {
					if o.len() < x.len() {
						return false;
					}

					for (k, v) in o.iter() {
						if let Some(value) = x.get(k) {
							if value.to_owned().coerce_to_kind(v).is_err() {
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
			Self::DiscriminatedObject(key, discriminants) => match value {
				SqlValue::Object(x) => {
					let value = x.get(key).unwrap_or(&SqlValue::None);
					if let Some(o) = discriminants
						.iter()
						.find(|o| value.to_owned().coerce_to_kind(&o[key]).is_ok())
					{
						if o.len() < x.len() {
							return false;
						}

						for (k, v) in o.iter() {
							if let Some(value) = x.get(k) {
								if value.to_owned().coerce_to_kind(v).is_err() {
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
		}
	}
}

impl Display for Literal {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Literal::String(s) => write!(f, "{}", s),
			Literal::Number(n) => write!(f, "{}", n),
			Literal::Duration(d) => write!(f, "{}", d),
			Literal::Bool(b) => write!(f, "{}", b),
			Literal::Array(a) => {
				let mut f = Pretty::from(f);
				f.write_char('[')?;
				if !a.is_empty() {
					let indent = pretty_indent();
					write!(f, "{}", Fmt::pretty_comma_separated(a.as_slice()))?;
					drop(indent);
				}
				f.write_char(']')
			}
			Literal::Object(o) => {
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
			Literal::DiscriminatedObject(_, discriminants) => {
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
		}
	}
}

impl From<Literal> for crate::expr::Literal {
	fn from(v: Literal) -> Self {
		match v {
			Literal::String(s) => Self::String(s.into()),
			Literal::Number(n) => Self::Number(n.into()),
			Literal::Duration(d) => Self::Duration(d.into()),
			Literal::Array(a) => Self::Array(a.into_iter().map(Into::into).collect()),
			Literal::Object(o) => Self::Object(o.into_iter().map(|(k, v)| (k, v.into())).collect()),
			Literal::DiscriminatedObject(k, o) => Self::DiscriminatedObject(
				k,
				o.into_iter()
					.map(|o| o.into_iter().map(|(k, v)| (k, v.into())).collect())
					.collect(),
			),
			Literal::Bool(b) => Self::Bool(b),
		}
	}
}

impl From<crate::expr::Literal> for Literal {
	fn from(v: crate::expr::Literal) -> Self {
		match v {
			crate::expr::Literal::String(s) => Self::String(s.into()),
			crate::expr::Literal::Number(n) => Self::Number(n.into()),
			crate::expr::Literal::Duration(d) => Self::Duration(d.into()),
			crate::expr::Literal::Array(a) => Self::Array(a.into_iter().map(Into::into).collect()),
			crate::expr::Literal::Object(o) => {
				Self::Object(o.into_iter().map(|(k, v)| (k, v.into())).collect())
			}
			crate::expr::Literal::DiscriminatedObject(k, o) => Self::DiscriminatedObject(
				k,
				o.into_iter()
					.map(|o| o.into_iter().map(|(k, v)| (k, v.into())).collect())
					.collect(),
			),
			crate::expr::Literal::Bool(b) => Self::Bool(b),
		}
	}
}
