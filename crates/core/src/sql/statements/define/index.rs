use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::fmt::Fmt;
use crate::sql::{Expr, Index};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DefineIndexStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub what: Expr,
	pub cols: Vec<Expr>,
	pub index: Index,
	pub comment: Option<Expr>,
	pub concurrently: bool,
}

impl ToSql for DefineIndexStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "DEFINE INDEX");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write_sql!(f, sql_fmt, " OVERWRITE"),
			DefineKind::IfNotExists => write_sql!(f, sql_fmt, " IF NOT EXISTS"),
		}
		write_sql!(f, sql_fmt, " {} ON {}", self.name, self.what);
		if !self.cols.is_empty() {
			write_sql!(f, sql_fmt, " FIELDS {}", Fmt::comma_separated(self.cols.iter()));
		}
		if Index::Idx != self.index {
			write_sql!(f, sql_fmt, " {}", self.index);
		}
		if let Some(ref v) = self.comment {
			write_sql!(f, sql_fmt, " COMMENT {}", v);
		}
		if self.concurrently {
			write_sql!(f, sql_fmt, " CONCURRENTLY");
		}
	}
}

impl From<DefineIndexStatement> for crate::expr::statements::DefineIndexStatement {
	fn from(v: DefineIndexStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name.into(),
			what: v.what.into(),
			cols: v.cols.into_iter().map(From::from).collect(),
			index: v.index.into(),
			comment: v.comment.map(|x| x.into()),
			concurrently: v.concurrently,
		}
	}
}

impl From<crate::expr::statements::DefineIndexStatement> for DefineIndexStatement {
	fn from(v: crate::expr::statements::DefineIndexStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name.into(),
			what: v.what.into(),
			cols: v.cols.into_iter().map(From::from).collect(),
			index: v.index.into(),
			comment: v.comment.map(|x| x.into()),
			concurrently: v.concurrently,
		}
	}
}
