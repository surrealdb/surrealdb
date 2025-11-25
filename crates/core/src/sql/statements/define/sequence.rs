use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::sql::{Expr, Literal, Timeout};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct DefineSequenceStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub batch: Expr,
	pub start: Expr,
	pub timeout: Option<Timeout>,
}

impl Default for DefineSequenceStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			name: Expr::Literal(Literal::None),
			batch: Expr::Literal(Literal::Integer(0)),
			start: Expr::Literal(Literal::Integer(0)),
			timeout: None,
		}
	}
}

impl ToSql for DefineSequenceStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "DEFINE SEQUENCE");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write_sql!(f, sql_fmt, " OVERWRITE"),
			DefineKind::IfNotExists => write_sql!(f, sql_fmt, " IF NOT EXISTS"),
		}
		write_sql!(f, sql_fmt, " {} BATCH {} START {}", self.name, self.batch, self.start);
		if let Some(ref v) = self.timeout {
			write_sql!(f, sql_fmt, " {v}");
		}
	}
}

impl From<DefineSequenceStatement> for crate::expr::statements::define::DefineSequenceStatement {
	fn from(v: DefineSequenceStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name.into(),
			batch: v.batch.into(),
			start: v.start.into(),
			timeout: v.timeout.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::define::DefineSequenceStatement> for DefineSequenceStatement {
	fn from(v: crate::expr::statements::define::DefineSequenceStatement) -> Self {
		DefineSequenceStatement {
			kind: v.kind.into(),
			name: v.name.into(),
			batch: v.batch.into(),
			start: v.start.into(),
			timeout: v.timeout.map(Into::into),
		}
	}
}
