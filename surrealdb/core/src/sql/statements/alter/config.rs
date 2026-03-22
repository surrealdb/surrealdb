use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::AlterKind;
use crate::fmt::QuoteStr;
use crate::sql::statements::define::config::ConfigInner;

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
/// AST node for `ALTER CONFIG`.
pub struct AlterConfigStatement {
	pub if_exists: bool,
	pub inner: ConfigInner,
	pub comment: AlterKind<String>,
}

impl Default for AlterConfigStatement {
	fn default() -> Self {
		Self {
			if_exists: false,
			inner: ConfigInner::GraphQL(Default::default()),
			comment: AlterKind::None,
		}
	}
}

impl ToSql for AlterConfigStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "ALTER CONFIG");
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		f.push(' ');
		self.inner.fmt_sql(f, fmt);

		match self.comment {
			AlterKind::Set(ref v) => write_sql!(f, fmt, " COMMENT {}", QuoteStr(v)),
			AlterKind::Drop => f.push_str(" DROP COMMENT"),
			AlterKind::None => {}
		}
	}
}

impl From<AlterConfigStatement> for crate::expr::statements::alter::AlterConfigStatement {
	fn from(v: AlterConfigStatement) -> Self {
		crate::expr::statements::alter::AlterConfigStatement {
			if_exists: v.if_exists,
			inner: v.inner.into(),
			comment: v.comment.into(),
		}
	}
}

impl From<crate::expr::statements::alter::AlterConfigStatement> for AlterConfigStatement {
	fn from(v: crate::expr::statements::alter::AlterConfigStatement) -> Self {
		AlterConfigStatement {
			if_exists: v.if_exists,
			inner: v.inner.into(),
			comment: v.comment.into(),
		}
	}
}
