use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::sql::{Base, Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct RemoveAccessStatement {
	pub name: Expr,
	pub base: Base,
	pub if_exists: bool,
}

impl Default for RemoveAccessStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			base: Base::default(),
			if_exists: false,
		}
	}
}

impl ToSql for RemoveAccessStatement {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		write_sql!(f, "REMOVE ACCESS");
		if self.if_exists {
			write_sql!(f, " IF EXISTS");
		}
		write_sql!(f, " {} ON {}", self.name, self.base);
	}
}

impl From<RemoveAccessStatement> for crate::expr::statements::RemoveAccessStatement {
	fn from(v: RemoveAccessStatement) -> Self {
		crate::expr::statements::RemoveAccessStatement {
			name: v.name.into(),
			base: v.base.into(),
			if_exists: v.if_exists,
		}
	}
}

impl From<crate::expr::statements::RemoveAccessStatement> for RemoveAccessStatement {
	fn from(v: crate::expr::statements::RemoveAccessStatement) -> Self {
		RemoveAccessStatement {
			name: v.name.into(),
			base: v.base.into(),
			if_exists: v.if_exists,
		}
	}
}
