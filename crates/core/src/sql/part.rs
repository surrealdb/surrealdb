use crate::{
	cnf::IDIOM_RECURSION_LIMIT,
	err::Error,
	sql::{Graph, Ident, Idiom, Number, SqlValue, fmt::Fmt, strand::no_nul_bytes},
};
use anyhow::Result;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Write;
use std::str;

use super::fmt::{is_pretty, pretty_indent};

#[revisioned(revision = 4)]
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
	Where(SqlValue),
	Graph(Graph),
	Value(SqlValue),
	Start(SqlValue),
	Method(#[serde(with = "no_nul_bytes")] String, Vec<SqlValue>),
	#[revision(start = 2)]
	Destructure(Vec<DestructurePart>),
	Optional,
	#[revision(
		start = 3,
		end = 4,
		convert_fn = "convert_recurse_add_instruction",
		fields_name = "OldRecurseFields"
	)]
	Recurse(Recurse, Option<Idiom>),
	#[revision(start = 4)]
	Recurse(Recurse, Option<Idiom>, Option<RecurseInstruction>),
	#[revision(start = 3)]
	Doc,
	#[revision(start = 3)]
	RepeatRecurse,
}

impl Part {
	fn convert_recurse_add_instruction(
		fields: OldRecurseFields,
		_revision: u16,
	) -> Result<Self, revision::Error> {
		Ok(Part::Recurse(fields.0, fields.1, None))
	}
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

impl From<Part> for crate::expr::Part {
	fn from(v: Part) -> Self {
		match v {
			Part::All => Self::All,
			Part::Flatten => Self::Flatten,
			Part::Last => Self::Last,
			Part::First => Self::First,
			Part::Field(ident) => Self::Field(ident.into()),
			Part::Index(number) => Self::Index(number.into()),
			Part::Where(value) => Self::Where(value.into()),
			Part::Graph(graph) => Self::Graph(graph.into()),
			Part::Value(value) => Self::Value(value.into()),
			Part::Start(value) => Self::Start(value.into()),
			Part::Method(method, values) => {
				Self::Method(method, values.into_iter().map(Into::into).collect())
			}
			Part::Destructure(parts) => {
				Self::Destructure(parts.into_iter().map(Into::into).collect())
			}
			Part::Optional => Self::Optional,
			Part::Recurse(recurse, idiom, instructions) => {
				let idiom = idiom.map(|idiom| idiom.into());
				let instructions = instructions.map(Into::into);
				crate::expr::Part::Recurse(recurse.into(), idiom, instructions)
			}
			Part::Doc => crate::expr::Part::Doc,
			Part::RepeatRecurse => crate::expr::Part::RepeatRecurse,
		}
	}
}

impl From<crate::expr::Part> for Part {
	fn from(v: crate::expr::Part) -> Self {
		match v {
			crate::expr::Part::All => Self::All,
			crate::expr::Part::Flatten => Self::Flatten,
			crate::expr::Part::Last => Self::Last,
			crate::expr::Part::First => Self::First,
			crate::expr::Part::Field(ident) => Self::Field(ident.into()),
			crate::expr::Part::Index(number) => Self::Index(number.into()),
			crate::expr::Part::Where(value) => Self::Where(value.into()),
			crate::expr::Part::Graph(graph) => Self::Graph(graph.into()),
			crate::expr::Part::Value(value) => Self::Value(value.into()),
			crate::expr::Part::Start(value) => Self::Start(value.into()),
			crate::expr::Part::Method(method, values) => {
				Self::Method(method, values.into_iter().map(Into::<SqlValue>::into).collect())
			}
			crate::expr::Part::Destructure(parts) => {
				Self::Destructure(parts.into_iter().map(Into::<DestructurePart>::into).collect())
			}
			crate::expr::Part::Optional => Self::Optional,
			crate::expr::Part::Recurse(recurse, idiom, instructions) => Self::Recurse(
				recurse.into(),
				idiom.map(|idiom| idiom.into()),
				instructions.map(Into::into),
			),
			crate::expr::Part::Doc => Self::Doc,
			crate::expr::Part::RepeatRecurse => Self::RepeatRecurse,
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
			Part::Recurse(v, nest, instruction) => {
				write!(f, ".{{{v}")?;
				if let Some(instruction) = instruction {
					write!(f, "+{instruction}")?;
				}
				write!(f, "}}")?;

				if let Some(nest) = nest {
					write!(f, "({nest})")?;
				}

				Ok(())
			}
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

	pub fn idiom(&self) -> Idiom {
		Idiom(self.path())
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

impl From<DestructurePart> for crate::expr::part::DestructurePart {
	fn from(v: DestructurePart) -> Self {
		match v {
			DestructurePart::All(v) => Self::All(v.into()),
			DestructurePart::Field(v) => Self::Field(v.into()),
			DestructurePart::Aliased(v, idiom) => Self::Aliased(v.into(), idiom.into()),
			DestructurePart::Destructure(v, d) => {
				Self::Destructure(v.into(), d.into_iter().map(Into::into).collect())
			}
		}
	}
}

impl From<crate::expr::part::DestructurePart> for DestructurePart {
	fn from(v: crate::expr::part::DestructurePart) -> Self {
		match v {
			crate::expr::part::DestructurePart::All(v) => Self::All(v.into()),
			crate::expr::part::DestructurePart::Field(v) => Self::Field(v.into()),
			crate::expr::part::DestructurePart::Aliased(v, idiom) => {
				Self::Aliased(v.into(), idiom.into())
			}
			crate::expr::part::DestructurePart::Destructure(v, d) => Self::Destructure(
				v.into(),
				d.into_iter().map(Into::<DestructurePart>::into).collect(),
			),
		}
	}
}

// ------------------------------

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Recurse {
	Fixed(u32),
	Range(Option<u32>, Option<u32>),
}

impl TryInto<(u32, Option<u32>)> for Recurse {
	type Error = anyhow::Error;

	fn try_into(self) -> Result<(u32, Option<u32>)> {
		let v = match self {
			Recurse::Fixed(v) => (v, Some(v)),
			Recurse::Range(min, max) => {
				let min = min.unwrap_or(1);
				(min, max)
			}
		};

		match v {
			(min, _) if min < 1 => Err(anyhow::Error::new(Error::InvalidBound {
				found: min.to_string(),
				expected: "at least 1".into(),
			})),
			(_, Some(max)) if max > (*IDIOM_RECURSION_LIMIT as u32) => {
				Err(anyhow::Error::new(Error::InvalidBound {
					found: max.to_string(),
					expected: format!("{} at most", *IDIOM_RECURSION_LIMIT),
				}))
			}
			v => Ok(v),
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

impl From<Recurse> for crate::expr::part::Recurse {
	fn from(v: Recurse) -> Self {
		match v {
			Recurse::Fixed(v) => Self::Fixed(v),
			Recurse::Range(min, max) => Self::Range(min, max),
		}
	}
}

impl From<crate::expr::part::Recurse> for Recurse {
	fn from(v: crate::expr::part::Recurse) -> Self {
		match v {
			crate::expr::part::Recurse::Fixed(v) => Self::Fixed(v),
			crate::expr::part::Recurse::Range(min, max) => Self::Range(min, max),
		}
	}
}
// ------------------------------

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum RecurseInstruction {
	Path {
		// Do we include the starting point in the paths?
		inclusive: bool,
	},
	Collect {
		// Do we include the starting point in the collection?
		inclusive: bool,
	},
	Shortest {
		// What ending node are we looking for?
		expects: SqlValue,
		// Do we include the starting point in the collection?
		inclusive: bool,
	},
}

impl fmt::Display for RecurseInstruction {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Path {
				inclusive,
			} => {
				write!(f, "path")?;

				if *inclusive {
					write!(f, "+inclusive")?;
				}

				Ok(())
			}
			Self::Collect {
				inclusive,
			} => {
				write!(f, "collect")?;

				if *inclusive {
					write!(f, "+inclusive")?;
				}

				Ok(())
			}
			Self::Shortest {
				expects,
				inclusive,
			} => {
				write!(f, "shortest={expects}")?;

				if *inclusive {
					write!(f, "+inclusive")?;
				}

				Ok(())
			}
		}
	}
}

impl From<RecurseInstruction> for crate::expr::part::RecurseInstruction {
	fn from(v: RecurseInstruction) -> Self {
		match v {
			RecurseInstruction::Path {
				inclusive,
			} => Self::Path {
				inclusive,
			},
			RecurseInstruction::Collect {
				inclusive,
			} => Self::Collect {
				inclusive,
			},
			RecurseInstruction::Shortest {
				expects,
				inclusive,
			} => Self::Shortest {
				expects: expects.into(),
				inclusive,
			},
		}
	}
}

impl From<crate::expr::part::RecurseInstruction> for RecurseInstruction {
	fn from(v: crate::expr::part::RecurseInstruction) -> Self {
		match v {
			crate::expr::part::RecurseInstruction::Path {
				inclusive,
			} => Self::Path {
				inclusive,
			},
			crate::expr::part::RecurseInstruction::Collect {
				inclusive,
			} => Self::Collect {
				inclusive,
			},
			crate::expr::part::RecurseInstruction::Shortest {
				expects,
				inclusive,
			} => Self::Shortest {
				expects: expects.into(),
				inclusive,
			},
		}
	}
}
