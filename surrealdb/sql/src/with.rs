use common::fmt::{EscapeKwFreeIdent, Fmt};
use surrealdb_types::{SqlFormat, ToSql, write_sql};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum With {
	NoIndex,
	Index(
		#[cfg_attr(feature = "arbitrary", arbitrary(with = crate::arbitrary::atleast_one))]
		Vec<String>,
	),
}

impl ToSql for With {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("WITH");
		match self {
			With::NoIndex => f.push_str(" NOINDEX"),
			With::Index(i) => {
				f.push_str(" INDEX ");
				write_sql!(
					f,
					fmt,
					"{}",
					Fmt::comma_separated(i.iter().map(|x| EscapeKwFreeIdent(x)))
				);
			}
		}
	}
}
