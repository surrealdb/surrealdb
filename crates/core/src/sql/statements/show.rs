use crate::sql::{Datetime, Table};
use crate::vs::VersionStamp;

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum ShowSince {
	Timestamp(Datetime),
	Versionstamp(u64),
}

impl ShowSince {
	pub fn versionstamp(vs: &VersionStamp) -> ShowSince {
		ShowSince::Versionstamp(vs.into_u64_lossy())
	}

	pub fn as_versionstamp(&self) -> Option<VersionStamp> {
		match self {
			ShowSince::Timestamp(_) => None,
			ShowSince::Versionstamp(v) => Some(VersionStamp::from_u64(*v)),
		}
	}
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
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct ShowStatement {
	pub table: Option<Table>,
	pub since: ShowSince,
	pub limit: Option<u32>,
}

impl From<ShowStatement> for crate::expr::statements::ShowStatement {
	fn from(v: ShowStatement) -> Self {
		crate::expr::statements::ShowStatement {
			table: v.table.map(Into::into),
			since: v.since.into(),
			limit: v.limit,
		}
	}
}

impl From<crate::expr::statements::ShowStatement> for ShowStatement {
	fn from(v: crate::expr::statements::ShowStatement) -> Self {
		ShowStatement {
			table: v.table.map(Into::into),
			since: v.since.into(),
			limit: v.limit,
		}
	}
}

crate::sql::impl_display_from_sql!(ShowStatement);

impl crate::sql::DisplaySql for ShowStatement {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "SHOW CHANGES FOR")?;
		match self.table {
			Some(ref v) => write!(f, " TABLE {}", v)?,
			None => write!(f, " DATABASE")?,
		}
		match self.since {
			ShowSince::Timestamp(ref v) => write!(f, " SINCE {}", v)?,
			ShowSince::Versionstamp(ref v) => write!(f, " SINCE {}", v)?,
		}
		if let Some(ref v) = self.limit {
			write!(f, " LIMIT {}", v)?
		}
		Ok(())
	}
}
