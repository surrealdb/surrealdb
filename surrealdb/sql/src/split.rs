use std::ops::Deref;

use common::fmt::Fmt;
use surrealdb_types::write_sql;

use crate::idiom::Idiom;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Splits(pub Vec<Split>);

impl surrealdb_types::ToSql for Splits {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		write_sql!(f, fmt, "SPLIT ON {}", Fmt::comma_separated(&self.0))
	}
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Split(pub Idiom);

impl Deref for Split {
	type Target = Idiom;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl surrealdb_types::ToSql for Split {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		self.0.fmt_sql(f, fmt);
	}
}
