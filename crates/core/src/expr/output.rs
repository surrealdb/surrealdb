use std::fmt::{self, Display};

use crate::expr::field::{Fields, Selector};
use crate::expr::{Expr, Field, Literal};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
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
			Self::Fields(v) => match v {
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
			},
		}
	}
}
