use surrealdb_types::{SqlFormat, ToSql};

use crate::fmt::{EscapeKwFreeIdent, Fmt};

#[derive(Clone, Debug, Eq, PartialEq, Hash, priority_lfu::DeepSizeOf)]
pub enum With {
	NoIndex,
	Index(Vec<String>),
}

impl ToSql for With {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("WITH");
		match self {
			With::NoIndex => f.push_str(" NOINDEX"),
			With::Index(i) => {
				f.push_str(" INDEX ");
				Fmt::comma_separated(i.iter().map(|x| EscapeKwFreeIdent(x.as_str())))
					.fmt_sql(f, fmt)
			}
		}
	}
}
