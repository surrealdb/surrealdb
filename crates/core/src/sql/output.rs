use std::fmt::{self, Display};

use crate::sql::field::{Fields, Selector};
use crate::sql::{Expr, Field, Literal};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) enum Output {
	#[default]
	None,
	Null,
	Diff,
	After,
	Before,
	Fields(Fields),
}

impl Display for Output {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("RETURN ")?;
		match self {
			Self::None => f.write_str("NONE"),
			Self::Null => f.write_str("NULL"),
			Self::Diff => f.write_str("DIFF"),
			Self::After => f.write_str("AFTER"),
			Self::Before => f.write_str("BEFORE"),
			Self::Fields(v) => {
				// We need to escape a possible `RETURN NONE` where `NONE` is a value
				match v {
					Fields::Select(fields) => {
						let mut iter = fields.iter();
						match iter.next() {
							Some(Field::Single(Selector {
								expr: Expr::Literal(Literal::None),
								alias,
							})) => {
								f.write_str("(NONE)")?;
								if let Some(alias) = alias {
									write!(f, " AS {alias}")?;
								}
							}
							Some(x) => {
								x.fmt(f)?;
							}
							None => {}
						}

						for x in iter {
							write!(f, ", {x}")?
						}

						Ok(())
					}
					x => x.fmt(f),
				}
			}
		}
	}
}

impl From<Output> for crate::expr::Output {
	fn from(v: Output) -> Self {
		match v {
			Output::None => Self::None,
			Output::Null => Self::Null,
			Output::Diff => Self::Diff,
			Output::After => Self::After,
			Output::Before => Self::Before,
			Output::Fields(v) => Self::Fields(v.into()),
		}
	}
}

impl From<crate::expr::Output> for Output {
	fn from(v: crate::expr::Output) -> Self {
		match v {
			crate::expr::Output::None => Self::None,
			crate::expr::Output::Null => Self::Null,
			crate::expr::Output::Diff => Self::Diff,
			crate::expr::Output::After => Self::After,
			crate::expr::Output::Before => Self::Before,
			crate::expr::Output::Fields(v) => Self::Fields(v.into()),
		}
	}
}
