use std::fmt::Write;

use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::{EscapeIdent, EscapeKwFreeIdent, Fmt};
use crate::sql::{Expr, Idiom, Lookup};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) enum Part {
	All,
	Flatten,
	Last,
	First,
	Field(String),
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
			Part::Field(ident) => Self::Field(ident),
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
			crate::expr::Part::Field(ident) => Self::Field(ident),
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

impl ToSql for Part {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Part::All => write_sql!(f, fmt, ".*"),
			Part::Last => write_sql!(f, fmt, "[$]"),
			Part::First => write_sql!(f, fmt, "[0]"),
			Part::Start(v) => v.fmt_sql(f, fmt),
			Part::Field(v) => write_sql!(f, fmt, ".{}", EscapeKwFreeIdent(v)),
			Part::Flatten => write_sql!(f, fmt, "…"),
			Part::Where(v) => write_sql!(f, fmt, "[WHERE {}]", v),
			Part::Graph(v) => v.fmt_sql(f, fmt),
			Part::Value(v) => write_sql!(f, fmt, "[{}]", v),
			Part::Method(v, a) => {
				write_sql!(f, fmt, ".{v}({})", Fmt::comma_separated(a))
			}
			Part::Destructure(v) => {
				f.push_str(".{");
				if fmt.is_pretty() {
					let inner_fmt = fmt.increment();
					if !v.is_empty() {
						f.push('\n');
						for (i, item) in v.iter().enumerate() {
							if i > 0 {
								inner_fmt.write_separator(f);
							}
							inner_fmt.write_indent(f);
							item.fmt_sql(f, inner_fmt);
						}
						f.push('\n');
						fmt.write_indent(f);
					}
				} else {
					f.push(' ');
					for (i, item) in v.iter().enumerate() {
						if i > 0 {
							f.push_str(", ");
						}
						item.fmt_sql(f, fmt);
					}
					f.push(' ');
				}
				f.push('}');
			}
			Part::Optional => f.push('?'),
			Part::Recurse(v, nest, instruction) => {
				f.push_str(".{");
				v.fmt_sql(f, fmt);
				if let Some(instruction) = instruction {
					f.push('+');
					instruction.fmt_sql(f, fmt);
				}
				f.push('}');
				if let Some(nest) = nest {
					f.push('(');
					nest.fmt_sql(f, fmt);
					f.push(')');
				}
			}
			Part::Doc => f.push('@'),
			Part::RepeatRecurse => f.push_str(".@"),
		}
	}
}

// ------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum DestructurePart {
	All(String),
	Field(String),
	Aliased(String, Idiom),
	Destructure(String, Vec<DestructurePart>),
}

impl ToSql for DestructurePart {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		match self {
			DestructurePart::All(fd) => write_sql!(f, sql_fmt, "{}.*", EscapeIdent(fd)),
			DestructurePart::Field(fd) => write_sql!(f, sql_fmt, "{}", EscapeIdent(fd)),
			DestructurePart::Aliased(fd, v) => write_sql!(f, sql_fmt, "{}: {v}", EscapeIdent(fd)),
			DestructurePart::Destructure(fd, d) => {
				write_sql!(f, sql_fmt, "{}{}", EscapeIdent(&fd), Part::Destructure(d.clone()))
			}
		}
	}
}

impl From<DestructurePart> for crate::expr::part::DestructurePart {
	fn from(v: DestructurePart) -> Self {
		match v {
			DestructurePart::All(v) => Self::All(v),
			DestructurePart::Field(v) => Self::Field(v),
			DestructurePart::Aliased(v, idiom) => Self::Aliased(v, idiom.into()),
			DestructurePart::Destructure(v, d) => {
				Self::Destructure(v, d.into_iter().map(Into::into).collect())
			}
		}
	}
}

impl From<crate::expr::part::DestructurePart> for DestructurePart {
	fn from(v: crate::expr::part::DestructurePart) -> Self {
		match v {
			crate::expr::part::DestructurePart::All(v) => Self::All(v),
			crate::expr::part::DestructurePart::Field(v) => Self::Field(v),
			crate::expr::part::DestructurePart::Aliased(v, idiom) => Self::Aliased(v, idiom.into()),
			crate::expr::part::DestructurePart::Destructure(v, d) => {
				Self::Destructure(v, d.into_iter().map(Into::<DestructurePart>::into).collect())
			}
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

impl ToSql for Recurse {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		match self {
			Recurse::Fixed(v) => write_sql!(f, sql_fmt, "{v}"),
			Recurse::Range(beg, end) => match (beg, end) {
				(None, None) => write_sql!(f, sql_fmt, ".."),
				(Some(beg), None) => write_sql!(f, sql_fmt, "{beg}.."),
				(None, Some(end)) => write_sql!(f, sql_fmt, "..{end}"),
				(Some(beg), Some(end)) => write_sql!(f, sql_fmt, "{beg}..{end}"),
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

impl ToSql for RecurseInstruction {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		match self {
			Self::Path {
				inclusive,
			} => {
				write_sql!(f, sql_fmt, "path");

				if *inclusive {
					write_sql!(f, sql_fmt, "+inclusive");
				}
			}
			Self::Collect {
				inclusive,
			} => {
				write_sql!(f, sql_fmt, "collect");

				if *inclusive {
					write_sql!(f, sql_fmt, "+inclusive");
				}
			}
			Self::Shortest {
				expects,
				inclusive,
			} => {
				write_sql!(f, sql_fmt, "shortest={expects}");

				if *inclusive {
					write_sql!(f, sql_fmt, "+inclusive");
				}
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
