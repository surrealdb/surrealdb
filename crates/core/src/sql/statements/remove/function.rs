use surrealdb_types::{SqlFormat, ToSql, write_sql};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RemoveFunctionStatement {
	pub name: String,
	pub if_exists: bool,
}

impl ToSql for RemoveFunctionStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "REMOVE FUNCTION");
		if self.if_exists {
			write_sql!(f, sql_fmt, " IF EXISTS");
		}
		write_sql!(f, sql_fmt, " fn::{}", self.name);
	}
}

impl From<RemoveFunctionStatement> for crate::expr::statements::RemoveFunctionStatement {
	fn from(v: RemoveFunctionStatement) -> Self {
		crate::expr::statements::RemoveFunctionStatement {
			name: v.name,
			if_exists: v.if_exists,
		}
	}
}
impl From<crate::expr::statements::RemoveFunctionStatement> for RemoveFunctionStatement {
	fn from(v: crate::expr::statements::RemoveFunctionStatement) -> Self {
		RemoveFunctionStatement {
			name: v.name,
			if_exists: v.if_exists,
		}
	}
}
