use common::fmt::Fmt;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::{CoverStmts, Expr, Idiom};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(::arbitrary::Arbitrary))]
pub enum Fields {
	/// Fields had the `VALUE` clause and should only return the given selector
	Value(Box<Selector>),
	/// Normal fields where an object with the selected fields is expected
	Select(
		#[cfg_attr(feature = "arbitrary", arbitrary(with = crate::arbitrary::atleast_one))]
		Vec<Field>,
	),
}

impl Fields {
	// Shorthand for `Fields::Select(vec![Field::all])`
	pub fn all() -> Fields {
		Fields::Select(vec![Field::All])
	}

	pub fn contains_all(&self) -> bool {
		match self {
			Fields::Value(_) => false,
			Fields::Select(fields) => fields.iter().any(|x| matches!(x, Field::All)),
		}
	}
}

impl ToSql for Fields {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Fields::Value(v) => {
				f.push_str("VALUE ");
				v.fmt_sql(f, fmt);
			}
			Fields::Select(x) => write_sql!(f, fmt, "{}", Fmt::comma_separated(x)),
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
	Single(Selector),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Selector {
	pub expr: Expr,
	pub alias: Option<Idiom>,
}

impl ToSql for Field {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Self::All => f.push('*'),
			Self::Single(s) => s.fmt_sql(f, fmt),
		}
	}
}

impl ToSql for Selector {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "{}", CoverStmts(&self.expr));
		if let Some(alias) = &self.alias {
			f.push_str(" AS ");
			alias.fmt_sql(f, fmt);
		}
	}
}
