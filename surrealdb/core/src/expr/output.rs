use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::field::{Fields, Selector};
use crate::expr::{Expr, Field, Literal};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash, priority_lfu::DeepSizeOf)]
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
			Self::Fields(v) => match v {
				Fields::Select(fields) => {
					let mut iter = fields.iter();
					match iter.next() {
						Some(Field::Single(Selector {
							expr: Expr::Literal(Literal::None),
							alias,
						})) => {
							f.push_str("(NONE)");
							if let Some(alias) = alias {
								write_sql!(f, fmt, " AS {alias}");
							}
						}
						Some(x) => {
							x.fmt_sql(f, fmt);
						}
						None => {}
					}

					for x in iter {
						write_sql!(f, fmt, ", {x}");
					}
				}
				x => x.fmt_sql(f, fmt),
			},
		}
	}
}
