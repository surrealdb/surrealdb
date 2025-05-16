use crate::sql::{changefeed::ChangeFeed, Ident, Strand};

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self};

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineDatabaseStatement {
	pub id: Option<u32>,
	pub name: Ident,
	pub comment: Option<Strand>,
	pub changefeed: Option<ChangeFeed>,
	#[revision(start = 2)]
	pub if_not_exists: bool,
	#[revision(start = 3)]
	pub overwrite: bool,
}

impl From<DefineDatabaseStatement> for crate::expr::statements::DefineDatabaseStatement {
	fn from(v: DefineDatabaseStatement) -> Self {
		crate::expr::statements::DefineDatabaseStatement {
			id: v.id,
			name: v.name.into(),
			comment: v.comment.map(Into::into),
			changefeed: v.changefeed.map(Into::into),
			if_not_exists: v.if_not_exists,
			overwrite: v.overwrite,
		}
	}
}

impl From<crate::expr::statements::DefineDatabaseStatement> for DefineDatabaseStatement {
	fn from(v: crate::expr::statements::DefineDatabaseStatement) -> Self {
		DefineDatabaseStatement {
			id: v.id,
			name: v.name.into(),
			comment: v.comment.map(Into::into),
			changefeed: v.changefeed.map(Into::into),
			if_not_exists: v.if_not_exists,
			overwrite: v.overwrite,
		}
	}
}

crate::sql::impl_display_from_sql!(DefineDatabaseStatement);

impl crate::sql::DisplaySql for DefineDatabaseStatement {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE DATABASE")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		if self.overwrite {
			write!(f, " OVERWRITE")?
		}
		write!(f, " {}", self.name)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		if let Some(ref v) = self.changefeed {
			write!(f, " {v}")?;
		}
		Ok(())
	}
}
