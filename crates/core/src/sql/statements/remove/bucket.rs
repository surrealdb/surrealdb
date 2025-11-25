use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::sql::{Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct RemoveBucketStatement {
	pub name: Expr,
	pub if_exists: bool,
}

impl Default for RemoveBucketStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			if_exists: false,
		}
	}
}

impl ToSql for RemoveBucketStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "REMOVE BUCKET");
		if self.if_exists {
			write_sql!(f, sql_fmt, " IF EXISTS");
		}
		write_sql!(f, sql_fmt, " {}", self.name);
	}
}

impl From<RemoveBucketStatement> for crate::expr::statements::remove::RemoveBucketStatement {
	fn from(v: RemoveBucketStatement) -> Self {
		crate::expr::statements::remove::RemoveBucketStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
		}
	}
}

impl From<crate::expr::statements::remove::RemoveBucketStatement> for RemoveBucketStatement {
	fn from(v: crate::expr::statements::remove::RemoveBucketStatement) -> Self {
		RemoveBucketStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
		}
	}
}
