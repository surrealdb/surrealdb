use std::fmt;
use std::fmt::Write;

use super::fmt::{is_pretty, pretty_indent};
use crate::sql::fmt::Fmt;
use crate::sql::{Expr, Ident, Idiom, Lookup};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Part {
	All,
	Flatten,
	Last,
	First,
	Field(Ident),
	Where(Expr),
	Graph(Lookup),
	Value(Expr),
	Start(Expr),
	Method(String, Vec<Expr>),
	Destructure(Vec<DestructurePart>),
	Optional,
	Recurse(Recurse, Option<Idiom>, Option<RecurseInstruction>),
	Doc,
	RepeatRecurse,
}

impl From<Part> for crate::expr::Part {
	fn from(v: Part) -> Self {
		match v {
			Part::All => Self::All,
			Part::Flatten => Self::Flatten,
			Part::Last => Self::Last,
			Part::First => Self::First,
			Part::Field(ident) => Self::Field(ident.into()),
			Part::Where(value) => Self::Where(value.into()),
			Part::Graph(graph) => Self::Lookup(graph.into()),
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
			crate::expr::Part::Where(value) => Self::Where(value.into()),
			crate::expr::Part::Lookup(graph) => Self::Graph(graph.into()),
			crate::expr::Part::Value(value) => Self::Value(value.into()),
			crate::expr::Part::Start(value) => Self::Start(value.into()),
			crate::expr::Part::Method(method, values) => {
				Self::Method(method, values.into_iter().map(Into::into).collect())
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

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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
				write!(f, "{}{}", fd, Part::Destructure(d.clone()))
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

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Recurse {
	Fixed(u32),
	Range(Option<u32>, Option<u32>),
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

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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
		expects: Expr,
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
