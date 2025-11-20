use std::fmt;

use surrealdb_types::ToSql;

use crate::{fmt::EscapeKwFreeIdent, types::PublicDatetime};

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
	pub limit: Option<u32>,
}

impl fmt::Display for ShowStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "SHOW CHANGES FOR")?;
		match self.table {
			Some(ref v) => write!(f, " TABLE {}", EscapeKwFreeIdent(v))?,
			None => write!(f, " DATABASE")?,
		}
		match self.since {
			ShowSince::Timestamp(ref v) => write!(f, " SINCE {}", v.to_sql())?,
			ShowSince::Versionstamp(ref v) => write!(f, " SINCE {}", v)?,
		}
		if let Some(ref v) = self.limit {
			write!(f, " LIMIT {}", v)?
		}
		Ok(())
	}
}

impl From<ShowStatement> for crate::expr::statements::ShowStatement {
	fn from(v: ShowStatement) -> Self {
		crate::expr::statements::ShowStatement {
			table: v.table,
			since: v.since.into(),
			limit: v.limit,
		}
	}
}

impl From<crate::expr::statements::ShowStatement> for ShowStatement {
	fn from(v: crate::expr::statements::ShowStatement) -> Self {
		ShowStatement {
			table: v.table,
			since: v.since.into(),
			limit: v.limit,
		}
	}
}
