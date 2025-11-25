use std::fmt::Write;

use crate::sql::{Expr, Idiom};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) enum Fields {
	/// Fields had the `VALUE` clause and should only return the given selector
	Value(Box<Field>),
	/// Normal fields where an object with the selected fields is expected
	Select(Vec<Field>),
}

impl Fields {
	// Shorthand for `Fields::Select(vec![Field::all])`
	pub fn all() -> Fields {
		Fields::Select(vec![Field::All])
	}

	pub fn none() -> Fields {
		Fields::Select(vec![])
	}

	pub fn contains_all(&self) -> bool {
		match self {
			Fields::Value(field) => matches!(**field, Field::All),
			Fields::Select(fields) => fields.iter().all(|x| matches!(x, Field::All)),
		}
	}

	pub fn is_empty(&self) -> bool {
		match self {
			Fields::Value(_field) => false,
			Fields::Select(fields) => fields.is_empty(),
		}
	}
}

impl From<Fields> for crate::expr::field::Fields {
	fn from(v: Fields) -> Self {
		match v {
			Fields::Value(x) => crate::expr::field::Fields::Value(Box::new((*x).into())),
			Fields::Select(x) => {
				crate::expr::field::Fields::Select(x.into_iter().map(From::from).collect())
			}
		}
	}
}

impl From<crate::expr::field::Fields> for Fields {
	fn from(v: crate::expr::field::Fields) -> Self {
		match v {
			crate::expr::field::Fields::Value(x) => Fields::Value(Box::new((*x).into())),
			crate::expr::field::Fields::Select(x) => {
				Fields::Select(x.into_iter().map(From::from).collect())
			}
		}
	}
}

impl surrealdb_types::ToSql for Fields {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		match self {
			Fields::Value(v) => {
				f.push_str("VALUE ");
				v.fmt_sql(f, fmt);
			}
			Fields::Select(x) => {
				for (i, item) in x.iter().enumerate() {
					if i > 0 {
						fmt.write_separator(f);
					}
					item.fmt_sql(f, fmt);
				}
			}
		}
	}
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) enum Field {
	/// The `*` in `SELECT * FROM ...`
	#[default]
	All,
	/// The 'rating' in `SELECT rating FROM ...`
	Single {
		expr: Expr,
		/// The `quality` in `SELECT rating AS quality FROM ...`
		alias: Option<Idiom>,
	},
}

impl surrealdb_types::ToSql for Field {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		match self {
			Self::All => f.push('*'),
			Self::Single {
				expr,
				alias,
			} => {
				expr.fmt_sql(f, fmt);
				if let Some(alias) = alias {
					f.push_str(" AS ");
					alias.fmt_sql(f, fmt);
				}
			}
		}
	}
}

impl From<Field> for crate::expr::field::Field {
	fn from(v: Field) -> Self {
		match v {
			Field::All => Self::All,
			Field::Single {
				expr,
				alias,
			} => Self::Single {
				expr: expr.into(),
				alias: alias.map(Into::into),
			},
		}
	}
}

impl From<crate::expr::field::Field> for Field {
	fn from(v: crate::expr::field::Field) -> Self {
		match v {
			crate::expr::field::Field::All => Self::All,
			crate::expr::field::Field::Single {
				expr,
				alias,
			} => Self::Single {
				expr: expr.into(),
				alias: alias.map(Into::into),
			},
		}
	}
}
