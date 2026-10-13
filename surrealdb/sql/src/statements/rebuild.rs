use common::fmt::{EscapeKwFreeIdent, EscapeKwIdent};
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::TableName;

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

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RebuildIndexStatement {
	pub name: Strand,
	pub what: TableName,
	pub if_exists: bool,
	pub concurrently: bool,
}

impl ToSql for RebuildIndexStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "REBUILD INDEX");
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		write_sql!(
			f,
			fmt,
			" {} ON {}",
			EscapeKwIdent(self.name.as_str(), &["IF"]),
			EscapeKwFreeIdent(self.what.as_str())
		);
		if self.concurrently {
			write_sql!(f, fmt, " CONCURRENTLY");
		}
	}
}
