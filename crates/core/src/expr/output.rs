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
			Self::Fields(v) => {
				let starts_with_none = match v {
					Fields::Value(selector) => {
						matches!(selector.expr, Expr::Literal(Literal::None))
					}
					Fields::Select(fields) => fields
						.first()
						.map(|x| {
							matches!(
								x,
								Field::Single(Selector {
									expr: Expr::Literal(Literal::None),
									..
								})
							)
						})
						.unwrap_or(false),
				};
				if starts_with_none {
					f.write_str("(")?;
					Display::fmt(v, f)?;
					f.write_str(")")
				} else {
					Display::fmt(v, f)
				}
			}
		}
	}
}
