
use surrealdb_types::{SqlFormat, ToSql};

use crate::sql::{Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct RemoveEventStatement {
	pub name: Expr,
	pub what: Expr,
	pub if_exists: bool,
}

impl Default for RemoveEventStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			what: Expr::Literal(Literal::None),
			if_exists: false,
		}
	}
}

impl ToSql for RemoveEventStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::remove::RemoveEventStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}

impl From<RemoveEventStatement> for crate::expr::statements::RemoveEventStatement {
	fn from(v: RemoveEventStatement) -> Self {
		crate::expr::statements::RemoveEventStatement {
			name: v.name.into(),
			table_name: v.what.into(),
			if_exists: v.if_exists,
		}
	}
}

impl From<crate::expr::statements::RemoveEventStatement> for RemoveEventStatement {
	fn from(v: crate::expr::statements::RemoveEventStatement) -> Self {
		RemoveEventStatement {
			name: v.name.into(),
			what: v.table_name.into(),
			if_exists: v.if_exists,
		}
	}
}
