use std::fmt::{self, Display, Formatter, Write};

use crate::fmt::{CoverStmtsSql, Fmt};
use crate::sql::{Expr, Idiom};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(::arbitrary::Arbitrary))]
pub(crate) enum Fields {
	/// Fields had the `VALUE` clause and should only return the given selector
	Value(Box<Selector>),
	/// Normal fields where an object with the selected fields is expected
	Select(
		#[cfg_attr(feature = "arbitrary", arbitrary(with = crate::sql::arbitrary::atleast_one))]
		Vec<Field>,
	),
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
			Fields::Value(_) => false,
			Fields::Select(fields) => fields.iter().any(|x| matches!(x, Field::All)),
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
pub(crate) enum Field {
	/// The `*` in `SELECT * FROM ...`
	#[default]
	All,
	/// The 'rating' in `SELECT rating FROM ...`
	Single(Selector),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Selector {
	pub expr: Expr,
	pub alias: Option<Idiom>,
}

impl Display for Field {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::All => f.write_char('*'),
			Self::Single(s) => Display::fmt(s, f),
		}
	}
}

impl From<Field> for crate::expr::field::Field {
	fn from(v: Field) -> Self {
		match v {
			Field::All => Self::All,
			Field::Single(s) => crate::expr::field::Field::Single(s.into()),
		}
	}
}

impl From<crate::expr::field::Field> for Field {
	fn from(v: crate::expr::field::Field) -> Self {
		match v {
			crate::expr::field::Field::All => Self::All,
			crate::expr::field::Field::Single(s) => Self::Single(s.into()),
		}
	}
}

impl Display for Selector {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&CoverStmtsSql(&self.expr), f)?;
		if let Some(alias) = &self.alias {
			f.write_str(" AS ")?;
			Display::fmt(alias, f)
		} else {
			Ok(())
		}
	}
}

impl From<Selector> for crate::expr::field::Selector {
	fn from(v: Selector) -> Self {
		crate::expr::field::Selector {
			expr: v.expr.into(),
			alias: v.alias.map(Into::into),
		}
	}
}

impl From<crate::expr::field::Selector> for Selector {
	fn from(v: crate::expr::field::Selector) -> Self {
		Selector {
			expr: v.expr.into(),
			alias: v.alias.map(Into::into),
		}
	}
}
