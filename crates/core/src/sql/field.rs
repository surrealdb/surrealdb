use std::fmt::{self, Display, Formatter, Write};

use crate::sql::fmt::Fmt;
use crate::sql::{Expr, Idiom};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Fields {
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

impl Display for Fields {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Fields::Value(v) => write!(f, "VALUE {}", &v),
			Fields::Select(x) => Display::fmt(&Fmt::comma_separated(x), f),
		}
	}
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Field {
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

impl Display for Field {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::All => f.write_char('*'),
			Self::Single {
				expr,
				alias,
			} => {
				Display::fmt(expr, f)?;
				if let Some(alias) = alias {
					f.write_str(" AS ")?;
					Display::fmt(alias, f)
				} else {
					Ok(())
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
