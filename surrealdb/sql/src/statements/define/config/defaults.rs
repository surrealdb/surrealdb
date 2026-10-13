use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::{Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DefaultConfig {
	pub namespace: Expr,
	pub database: Expr,
}

impl Default for DefaultConfig {
	fn default() -> Self {
		Self {
			namespace: Expr::Literal(Literal::None),
			database: Expr::Literal(Literal::None),
		}
	}
}

impl ToSql for DefaultConfig {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, " DEFAULT");
		write_sql!(f, fmt, " NAMESPACE {}", self.namespace);
		write_sql!(f, fmt, " DATABASE {}", self.database);
	}
}
