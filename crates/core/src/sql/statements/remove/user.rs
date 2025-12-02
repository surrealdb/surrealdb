use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::sql::{Base, Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct RemoveUserStatement {
	pub name: Expr,
	pub base: Base,
	pub if_exists: bool,
}

impl Default for RemoveUserStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			base: Base::default(),
			if_exists: false,
		}
	}
}

impl ToSql for RemoveUserStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "REMOVE USER");
		if self.if_exists {
			write_sql!(f, sql_fmt, " IF EXISTS");
		}
		write_sql!(f, sql_fmt, " {} ON {}", self.name, self.base);
	}
}

impl From<RemoveUserStatement> for crate::expr::statements::RemoveUserStatement {
	fn from(v: RemoveUserStatement) -> Self {
		crate::expr::statements::RemoveUserStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			base: v.base.into(),
		}
	}
}

impl From<crate::expr::statements::RemoveUserStatement> for RemoveUserStatement {
	fn from(v: crate::expr::statements::RemoveUserStatement) -> Self {
		RemoveUserStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			base: v.base.into(),
		}
	}
}
