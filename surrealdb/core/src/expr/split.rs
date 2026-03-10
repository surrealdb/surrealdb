use std::ops::Deref;

use surrealdb_types::{SqlFormat, ToSql};

use crate::expr::idiom::Idiom;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct Splits(pub(crate) Vec<Split>);

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

impl ToSql for Splits {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let sql_splits: crate::sql::Splits = self.clone().into();
		sql_splits.fmt_sql(f, fmt);
	}
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct Split(pub(crate) Idiom);

impl Deref for Split {
	type Target = Idiom;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl surrealdb_types::ToSql for Split {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		let sql_split: crate::sql::Split = self.clone().into();
		sql_split.fmt_sql(f, fmt);
	}
}
