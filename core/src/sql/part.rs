use crate::{
	cnf::IDIOM_RECURSION_LIMIT,
	err::Error,
	sql::{fmt::Fmt, strand::no_nul_bytes, Graph, Ident, Idiom, Number, Value},
};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Write;
use std::str;

use super::fmt::{is_pretty, pretty_indent};

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Part {
	All,
	Flatten,
	Last,
	First,
	Field(Ident),
	Index(Number),
	Where(Value),
	Graph(Graph),
	Value(Value),
	Start(Value),
	Method(#[serde(with = "no_nul_bytes")] String, Vec<Value>),
	#[revision(start = 2)]
	Destructure(Vec<DestructurePart>),
	Optional,
	#[revision(start = 3)]
	Nest(Idiom),
	#[revision(start = 3)]
	Recurse(Recurse),
	#[revision(start = 3)]
	Doc,
	#[revision(start = 3)]
	RepeatRecurse,
}

impl From<i32> for Part {
	fn from(v: i32) -> Self {
		Self::Index(v.into())
	}
}

impl From<isize> for Part {
	fn from(v: isize) -> Self {
		Self::Index(v.into())
	}
}

impl From<usize> for Part {
	fn from(v: usize) -> Self {
		Self::Index(v.into())
	}
}

impl From<String> for Part {
	fn from(v: String) -> Self {
		Self::Field(v.into())
	}
}

impl From<Number> for Part {
	fn from(v: Number) -> Self {
		Self::Index(v)
	}
}

impl From<Ident> for Part {
	fn from(v: Ident) -> Self {
		Self::Field(v)
	}
}

impl From<Graph> for Part {
	fn from(v: Graph) -> Self {
		Self::Graph(v)
	}
}

impl From<&str> for Part {
	fn from(v: &str) -> Self {
		match v.parse::<isize>() {
			Ok(v) => Self::from(v),
			_ => Self::from(v.to_owned()),
		}
	}
}

impl Part {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		match self {
			Part::Start(v) => v.writeable(),
			Part::Where(v) => v.writeable(),
			Part::Value(v) => v.writeable(),
			Part::Method(_, v) => v.iter().any(Value::writeable),
			_ => false,
		}
	}
	/// Returns a yield if an alias is specified
	pub(crate) fn alias(&self) -> Option<&Idiom> {
		match self {
			Part::Graph(v) => v.alias.as_ref(),
			_ => None,
		}
	}
}

impl fmt::Display for Part {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Part::All => f.write_str("[*]"),
			Part::Last => f.write_str("[$]"),
			Part::First => f.write_str("[0]"),
			Part::Start(v) => write!(f, "{v}"),
			Part::Field(v) => write!(f, ".{v}"),
			Part::Flatten => f.write_str("…"),
			Part::Index(v) => write!(f, "[{v}]"),
			Part::Where(v) => write!(f, "[WHERE {v}]"),
			Part::Graph(v) => write!(f, "{v}"),
			Part::Value(v) => write!(f, "[{v}]"),
			Part::Method(v, a) => write!(f, ".{v}({})", Fmt::comma_separated(a)),
			Part::Destructure(v) => {
				f.write_str(".{")?;
				if !is_pretty() {
					f.write_char(' ')?;
				}
				if !v.is_empty() {
					let indent = pretty_indent();
					write!(f, "{}", Fmt::pretty_comma_separated(v))?;
					drop(indent);
				}
				if is_pretty() {
					f.write_char('}')
				} else {
					f.write_str(" }")
				}
			}
			Part::Optional => write!(f, "?"),
			Part::Nest(v) => write!(f, ".({v})"),
			Part::Recurse(v) => write!(f, ".{{{v}}}"),
			Part::Doc => write!(f, "@"),
			Part::RepeatRecurse => write!(f, ".@"),
		}
	}
}

// ------------------------------

pub trait Next<'a> {
	fn next(&'a self) -> &'a [Part];
}

impl<'a> Next<'a> for &'a [Part] {
	fn next(&'a self) -> &'a [Part] {
		match self.len() {
			0 => &[],
			_ => &self[1..],
		}
	}
}

// ------------------------------

pub trait Skip<'a> {
	fn skip(&'a self, amount: usize) -> &'a [Part];
}

impl<'a> Skip<'a> for &'a [Part] {
	fn skip(&'a self, amount: usize) -> &'a [Part] {
		match self.len() {
			0 => &[],
			_ => &self[amount..],
		}
	}
}

// ------------------------------

pub trait NextMethod<'a> {
	fn next_method(&'a self) -> &'a [Part];
}

impl<'a> NextMethod<'a> for &'a [Part] {
	fn next_method(&'a self) -> &'a [Part] {
		match self.iter().position(|p| matches!(p, Part::Method(_, _))) {
			None => &[],
			Some(i) => &self[i..],
		}
	}
}

impl<'a> NextMethod<'a> for &'a Idiom {
	fn next_method(&'a self) -> &'a [Part] {
		match self.iter().position(|p| matches!(p, Part::Method(_, _))) {
			None => &[],
			Some(i) => &self[i..],
		}
	}
}

// ------------------------------

pub trait SliceRepeatRecurse<'a> {
	fn slice_repeat_recurse(&'a self) -> Option<(&'a [Part], &'a [Part])>;
}

impl<'a> SliceRepeatRecurse<'a> for &'a [Part] {
	fn slice_repeat_recurse(&'a self) -> Option<(&'a [Part], &'a [Part])> {
		match self.iter().position(|p| matches!(p, Part::RepeatRecurse)) {
			None => None,
			Some(i) => Some((&self[..=i], &self[(i + 1)..])),
		}
	}
}

impl<'a> SliceRepeatRecurse<'a> for &'a Idiom {
	fn slice_repeat_recurse(&'a self) -> Option<(&'a [Part], &'a [Part])> {
		match self.iter().position(|p| matches!(p, Part::RepeatRecurse)) {
			None => None,
			Some(i) => Some((&self[..=i], &self[(i + 1)..])),
		}
	}
}

// ------------------------------

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum DestructurePart {
	All(Ident),
	Field(Ident),
	Aliased(Ident, Idiom),
	Destructure(Ident, Vec<DestructurePart>),
}

impl DestructurePart {
	pub fn field(&self) -> &Ident {
		match self {
			DestructurePart::All(v) => v,
			DestructurePart::Field(v) => v,
			DestructurePart::Aliased(v, _) => v,
			DestructurePart::Destructure(v, _) => v,
		}
	}

	pub fn path(&self) -> Vec<Part> {
		match self {
			DestructurePart::All(v) => vec![Part::Field(v.clone()), Part::All],
			DestructurePart::Field(v) => vec![Part::Field(v.clone())],
			DestructurePart::Aliased(_, v) => v.0.clone(),
			DestructurePart::Destructure(f, d) => {
				vec![Part::Field(f.clone()), Part::Destructure(d.clone())]
			}
		}
	}
}

impl fmt::Display for DestructurePart {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			DestructurePart::All(fd) => write!(f, "{fd}.*"),
			DestructurePart::Field(fd) => write!(f, "{fd}"),
			DestructurePart::Aliased(fd, v) => write!(f, "{fd}: {v}"),
			DestructurePart::Destructure(fd, d) => {
				write!(f, "{fd}{}", Part::Destructure(d.clone()))
			}
		}
	}
}

// ------------------------------

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Recurse {
	Fixed(i64),
	Range(Option<i64>, Option<i64>),
}

impl Recurse {
	pub fn min(&self) -> Result<i64, Error> {
		let min = match self {
			Recurse::Fixed(v) => v.to_owned(),
			Recurse::Range(min, _) => min.unwrap_or(1),
		};

		if min < 1 {
			Err(Error::InvalidBound {
				found: min.to_string(),
				expected: "at least 1".into(),
			})
		} else {
			Ok(min)
		}
	}

	pub fn max(&self) -> Result<Option<i64>, Error> {
		let max = match self {
			Recurse::Fixed(v) => Some(v.to_owned()),
			Recurse::Range(_, max) => max.to_owned(),
		};

		match max {
			Some(max) if max > (*IDIOM_RECURSION_LIMIT as i64) => Err(Error::InvalidBound {
				found: max.to_string(),
				expected: format!("{} at most", *IDIOM_RECURSION_LIMIT),
			}),
			max => Ok(max),
		}
	}
}

impl fmt::Display for Recurse {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Recurse::Fixed(v) => write!(f, "{v}"),
			Recurse::Range(beg, end) => match (beg, end) {
				(None, None) => write!(f, ".."),
				(Some(beg), None) => write!(f, "{beg}.."),
				(None, Some(end)) => write!(f, "..{end}"),
				(Some(beg), Some(end)) => write!(f, "{beg}..{end}"),
			},
		}
	}
}
