use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::{EscapeIdent, Fmt};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum With {
	NoIndex,
	Index(Vec<String>),
}

impl ToSql for With {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "WITH");
		match self {
			With::NoIndex => write_sql!(f, sql_fmt, " NOINDEX"),
			With::Index(i) => {
				write_sql!(
					f,
					sql_fmt,
					" INDEX {}",
					Fmt::comma_separated(i.iter().map(EscapeIdent))
				);
			}
		}
	}
}

impl From<With> for crate::expr::With {
	fn from(v: With) -> Self {
		match v {
			With::NoIndex => Self::NoIndex,
			With::Index(i) => Self::Index(i),
		}
	}
}
impl From<crate::expr::With> for With {
	fn from(v: crate::expr::With) -> Self {
		match v {
			crate::expr::With::NoIndex => Self::NoIndex,
			crate::expr::With::Index(i) => Self::Index(i),
		}
	}
}
