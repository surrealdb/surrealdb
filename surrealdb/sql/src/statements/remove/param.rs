use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::Ident;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RemoveParamStatement {
	pub name: Ident,
	pub if_exists: bool,
}

impl ToSql for RemoveParamStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "REMOVE PARAM");
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		write_sql!(f, fmt, " ${}", self.name);
	}
}
