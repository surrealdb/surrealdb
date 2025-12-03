use surrealdb_types::{SqlFormat, ToSql};

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
					f.push(')')
				} else {
					v.fmt_sql(f, fmt)
				}
			}
		}
	}
}
