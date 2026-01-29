use priority_lfu::DeepSizeOf;
use revision::revisioned;
use surrealdb_types::{SqlFormat, ToSql};

use crate::expr::language::Language;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, DeepSizeOf)]
pub enum Filter {
	Ascii,
	EdgeNgram(u16, u16),
	Lowercase,
	Ngram(u16, u16),
	Snowball(Language),
	Uppercase,
	Mapper(String),
}

impl ToSql for Filter {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::filter::Filter = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
