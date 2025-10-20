use surrealdb_types::{ToSql, write_sql};

use super::DefineKind;
use crate::sql::changefeed::ChangeFeed;
use crate::sql::{Expr, Literal};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct DefineDatabaseStatement {
	pub kind: DefineKind,
	pub id: Option<u32>,
	pub name: Expr,
	pub comment: Option<Expr>,
	pub changefeed: Option<ChangeFeed>,
}

impl Default for DefineDatabaseStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			id: None,
			name: Expr::Literal(Literal::None),
			comment: None,
			changefeed: None,
		}
	}
}

impl ToSql for DefineDatabaseStatement {
	fn fmt_sql(&self, f: &mut String, pretty: PrettyMode) {
		write_sql!(f, "DEFINE DATABASE");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write_sql!(f, " OVERWRITE"),
			DefineKind::IfNotExists => write_sql!(f, " IF NOT EXISTS"),
		}
		write_sql!(f, " ");
		self.name.fmt_sql(f, pretty);
		if let Some(ref v) = self.comment {
			write_sql!(f, " COMMENT ");
			v.fmt_sql(f, pretty);
		}
		if let Some(ref v) = self.changefeed {
			write_sql!(f, " {v}");
		}
	}
}

impl From<DefineDatabaseStatement> for crate::expr::statements::DefineDatabaseStatement {
	fn from(v: DefineDatabaseStatement) -> Self {
		crate::expr::statements::DefineDatabaseStatement {
			kind: v.kind.into(),
			id: v.id,
			name: v.name.into(),
			comment: v.comment.map(|x| x.into()),
			changefeed: v.changefeed.map(Into::into),
		}
	}
}

#[allow(clippy::fallible_impl_from)]
impl From<crate::expr::statements::DefineDatabaseStatement> for DefineDatabaseStatement {
	fn from(v: crate::expr::statements::DefineDatabaseStatement) -> Self {
		DefineDatabaseStatement {
			kind: v.kind.into(),
			id: v.id,
			name: v.name.into(),
			comment: v.comment.map(|x| x.into()),
			changefeed: v.changefeed.map(Into::into),
		}
	}
}
