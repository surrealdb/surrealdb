use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::expr::changefeed::ChangeFeed;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{CoverStmts, Expr, Literal};
use crate::val::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineDatabaseStatement {
	pub kind: DefineKind,
	pub id: Option<u32>,
	pub name: Expr,
	pub strict: bool,
	/// `COMPATIBILITY <level>`: the database's compatibility level. Without
	/// it, a new database takes the store's default and a redefined one keeps
	/// its level.
	pub compatibility: Option<u8>,
	pub comment: Expr,
	pub changefeed: Option<ChangeFeed>,
}

impl Default for DefineDatabaseStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			id: None,
			name: Expr::Literal(Literal::None),
			comment: Expr::Literal(Literal::None),
			changefeed: None,
			strict: false,
			compatibility: None,
		}
	}
}
impl InfoStructure for DefineDatabaseStatement {
	fn structure(self) -> Value {
		Value::from(map! {
			"name" => self.name.structure(),
			"comment" => self.comment.structure(),
		})
	}
}

impl ToSql for DefineDatabaseStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "DEFINE DATABASE");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write_sql!(f, sql_fmt, " OVERWRITE"),
			DefineKind::IfNotExists => write_sql!(f, sql_fmt, " IF NOT EXISTS"),
		}
		write_sql!(f, sql_fmt, " {}", CoverStmts(&self.name));
		if self.strict {
			f.push_str(" STRICT");
		}
		if let Some(level) = self.compatibility {
			write_sql!(f, sql_fmt, " COMPATIBILITY {level}");
		}
		if !matches!(self.comment, Expr::Literal(Literal::None)) {
			write_sql!(f, sql_fmt, " COMMENT {}", CoverStmts(&self.comment));
		}
		if let Some(ref v) = self.changefeed {
			write_sql!(f, sql_fmt, " {v}");
		}
	}
}
