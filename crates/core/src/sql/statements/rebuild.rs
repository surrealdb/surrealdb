use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::EscapeIdent;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum RebuildStatement {
	Index(RebuildIndexStatement),
}

impl ToSql for RebuildStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Self::Index(v) => v.fmt_sql(f, fmt),
		}
	}
}

impl From<RebuildStatement> for crate::expr::statements::rebuild::RebuildStatement {
	fn from(v: RebuildStatement) -> Self {
		match v {
			RebuildStatement::Index(v) => Self::Index(v.into()),
		}
	}
}

impl From<crate::expr::statements::rebuild::RebuildStatement> for RebuildStatement {
	fn from(v: crate::expr::statements::rebuild::RebuildStatement) -> Self {
		match v {
			crate::expr::statements::rebuild::RebuildStatement::Index(v) => Self::Index(v.into()),
		}
	}
}

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RebuildIndexStatement {
	pub name: String,
	pub what: String,
	pub if_exists: bool,
	pub concurrently: bool,
}

impl ToSql for RebuildIndexStatement {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str("REBUILD INDEX");
		if self.if_exists {
			f.push_str(" IF EXISTS");
		}
		write_sql!(f, " {} ON {}", EscapeIdent(&self.name), EscapeIdent(&self.what));
		if self.concurrently {
			f.push_str(" CONCURRENTLY");
		}
	}
}

impl From<RebuildIndexStatement> for crate::expr::statements::rebuild::RebuildIndexStatement {
	fn from(v: RebuildIndexStatement) -> Self {
		Self {
			name: v.name,
			what: v.what,
			if_exists: v.if_exists,
			concurrently: v.concurrently,
		}
	}
}

impl From<crate::expr::statements::rebuild::RebuildIndexStatement> for RebuildIndexStatement {
	fn from(v: crate::expr::statements::rebuild::RebuildIndexStatement) -> Self {
		Self {
			name: v.name,
			what: v.what,
			if_exists: v.if_exists,
			concurrently: v.concurrently,
		}
	}
}
