use common::fmt::{EscapeKwFreeIdent, Fmt, QuoteStr};
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::expr::permission::Permission;
use crate::expr::{CoverStmts, Expr, Literal};

/// `DEFINE ENUM name VALUES 'a', 'b', ...`: a named, ordered set of string labels.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineEnumStatement {
	pub kind: DefineKind,
	pub name: Strand,
	/// The labels in declaration order.
	pub labels: Vec<String>,
	pub comment: Expr,
	pub permissions: Permission,
}

impl Default for DefineEnumStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			name: Strand::default(),
			labels: Vec::new(),
			comment: Expr::Literal(Literal::None),
			permissions: Permission::default(),
		}
	}
}

impl ToSql for DefineEnumStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "DEFINE ENUM");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write_sql!(f, fmt, " OVERWRITE"),
			DefineKind::IfNotExists => write_sql!(f, fmt, " IF NOT EXISTS"),
		}
		write_sql!(
			f,
			fmt,
			" {} VALUES {}",
			EscapeKwFreeIdent(self.name.as_str()),
			Fmt::comma_separated(self.labels.iter().map(|l| QuoteStr(l)))
		);
		if !matches!(self.comment, Expr::Literal(Literal::None)) {
			write_sql!(f, fmt, " COMMENT {}", CoverStmts(&self.comment));
		}
		write_sql!(f, fmt, " PERMISSIONS {}", self.permissions);
	}
}
