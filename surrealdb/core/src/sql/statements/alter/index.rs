use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::{EscapeKwFreeIdent, EscapeKwIdent, QuoteStr};
use crate::sql::statements::alter::AlterKind;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AlterIndexStatement {
	pub name: String,
	pub table: String,
	pub if_exists: bool,
	pub prepare_remove: bool,
	pub comment: AlterKind<String>,
}

impl ToSql for AlterIndexStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "ALTER INDEX");
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		write_sql!(
			f,
			fmt,
			" {} ON {}",
			EscapeKwIdent(&self.name, &["IF"]),
			EscapeKwFreeIdent(&self.table)
		);

		if self.prepare_remove {
			write_sql!(f, fmt, " PREPARE REMOVE");
		}
		match self.comment {
			AlterKind::Set(ref x) => write_sql!(f, fmt, " COMMENT {}", QuoteStr(x)),
			AlterKind::Drop => write_sql!(f, fmt, " DROP COMMENT"),
			AlterKind::None => {}
		}
	}
}

impl From<AlterIndexStatement> for crate::expr::statements::alter::AlterIndexStatement {
	fn from(v: AlterIndexStatement) -> Self {
		crate::expr::statements::alter::AlterIndexStatement {
			name: v.name,
			table: v.table.into(),
			if_exists: v.if_exists,
			prepare_remove: v.prepare_remove,
			comment: v.comment.into(),
		}
	}
}
impl From<crate::expr::statements::alter::AlterIndexStatement> for AlterIndexStatement {
	fn from(v: crate::expr::statements::alter::AlterIndexStatement) -> Self {
		AlterIndexStatement {
			name: v.name,
			table: v.table.into_string(),
			if_exists: v.if_exists,
			prepare_remove: v.prepare_remove,
			comment: v.comment.into(),
		}
	}
}
