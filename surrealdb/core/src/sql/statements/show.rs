use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::EscapeKwFreeIdent;
use crate::types::PublicDatetime;
use crate::val::TableName;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum ShowSince {
	Timestamp(PublicDatetime),
	Versionstamp(u64),
}

impl From<ShowSince> for crate::expr::statements::show::ShowSince {
	fn from(v: ShowSince) -> Self {
		match v {
			ShowSince::Timestamp(v) => Self::Timestamp(v.into()),
			ShowSince::Versionstamp(v) => Self::Versionstamp(v),
		}
	}
}

impl From<crate::expr::statements::show::ShowSince> for ShowSince {
	fn from(v: crate::expr::statements::show::ShowSince) -> Self {
		match v {
			crate::expr::statements::show::ShowSince::Timestamp(v) => {
				ShowSince::Timestamp(v.into())
			}
			crate::expr::statements::show::ShowSince::Versionstamp(v) => ShowSince::Versionstamp(v),
		}
	}
}

/// A SHOW CHANGES statement for displaying changes made to a table or database.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct ShowStatement {
	pub table: Option<String>,
	pub since: ShowSince,
	pub limit: Option<usize>,
}

impl ToSql for ShowStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "SHOW CHANGES FOR");
		match self.table {
			Some(ref v) => write_sql!(f, fmt, " TABLE {}", EscapeKwFreeIdent(v)),
			None => write_sql!(f, fmt, " DATABASE"),
		}
		match self.since {
			ShowSince::Timestamp(ref v) => write_sql!(f, fmt, " SINCE {}", v),
			ShowSince::Versionstamp(ref v) => write_sql!(f, fmt, " SINCE {}", v),
		}
		if let Some(ref v) = self.limit {
			write_sql!(f, fmt, " LIMIT {}", v)
		}
	}
}

impl From<ShowStatement> for crate::expr::statements::ShowStatement {
	fn from(v: ShowStatement) -> Self {
		crate::expr::statements::ShowStatement {
			table: v.table.map(TableName::new),
			since: v.since.into(),
			limit: v.limit,
		}
	}
}

impl From<crate::expr::statements::ShowStatement> for ShowStatement {
	fn from(v: crate::expr::statements::ShowStatement) -> Self {
		ShowStatement {
			table: v.table.map(TableName::into_string),
			since: v.since.into(),
			limit: v.limit,
		}
	}
}
