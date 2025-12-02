use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::EscapeKwFreeIdent;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RemoveParamStatement {
	pub name: String,
	pub if_exists: bool,
}

impl ToSql for RemoveParamStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "REMOVE PARAM");
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		write_sql!(f, fmt, " ${}", EscapeKwFreeIdent(&self.name));
	}
}

impl From<RemoveParamStatement> for crate::expr::statements::RemoveParamStatement {
	fn from(v: RemoveParamStatement) -> Self {
		crate::expr::statements::RemoveParamStatement {
			name: v.name,
			if_exists: v.if_exists,
		}
	}
}

impl From<crate::expr::statements::RemoveParamStatement> for RemoveParamStatement {
	fn from(v: crate::expr::statements::RemoveParamStatement) -> Self {
		RemoveParamStatement {
			name: v.name,
			if_exists: v.if_exists,
		}
	}
}
