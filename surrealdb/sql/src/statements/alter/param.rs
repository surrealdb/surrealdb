use common::fmt::{EscapeKwFreeIdent, QuoteStr};
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::AlterKind;
use crate::{CoverStmts, Expr, Permission};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
/// AST node for `ALTER PARAM`.
pub struct AlterParamStatement {
	pub name: Strand,
	pub if_exists: bool,
	pub value: Option<Expr>,
	pub comment: AlterKind<String>,
	pub permissions: Option<Permission>,
}

impl ToSql for AlterParamStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "ALTER PARAM");
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		write_sql!(f, fmt, " ${}", EscapeKwFreeIdent(self.name.as_str()));

		if let Some(ref v) = self.value {
			write_sql!(f, fmt, " VALUE {}", CoverStmts(v));
		}

		match self.comment {
			AlterKind::Set(ref v) => write_sql!(f, fmt, " COMMENT {}", QuoteStr(v)),
			AlterKind::Drop => f.push_str(" DROP COMMENT"),
			AlterKind::None => {}
		}

		if let Some(ref p) = self.permissions {
			let fmt = fmt.increment();
			write_sql!(f, fmt, " PERMISSIONS {}", p);
		}
	}
}
