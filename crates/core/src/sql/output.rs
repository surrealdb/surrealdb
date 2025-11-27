use std::fmt::{self, Display};

use surrealdb_types::{SqlFormat, ToSql, write_sql};

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

impl ToSql for Output {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("RETURN ");
		match self {
			Self::None => f.push_str("NONE"),
			Self::Null => f.push_str("NULL"),
			Self::Diff => f.push_str("DIFF"),
			Self::After => f.push_str("AFTER"),
			Self::Before => f.push_str("BEFORE"),
			Self::Fields(v) => {
				// We need to escape a possible `RETURN NONE` where `NONE` is a value
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
					f.push('(');
					v.fmt_sql(f, fmt);
					f.push(')');
				} else {
					v.fmt_sql(f, fmt);
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
