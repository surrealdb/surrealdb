use std::ops::Deref;

use common::fmt::Fmt;
use surrealdb_types::write_sql;

use crate::expr::idiom::Idiom;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct Splits(pub Vec<Split>);

impl Splits {
	/// Whether evaluating every split target can be done on a read-only
	/// transaction.
	pub fn read_only(&self) -> bool {
		self.0.iter().all(|x| x.read_only())
	}
}

impl Deref for Splits {
	type Target = Vec<Split>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl IntoIterator for Splits {
	type Item = Split;
	type IntoIter = std::vec::IntoIter<Self::Item>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl surrealdb_types::ToSql for Splits {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		write_sql!(f, fmt, "SPLIT ON {}", Fmt::comma_separated(&self.0))
	}
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct Split(pub Idiom);

impl Split {
	/// Whether evaluating this split target can be done on a read-only
	/// transaction.
	pub fn read_only(&self) -> bool {
		self.0.read_only()
	}
}

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
