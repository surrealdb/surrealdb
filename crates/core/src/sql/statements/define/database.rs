use crate::sql::{Ident, Strand, ToSql, changefeed::ChangeFeed};

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

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

impl Display for DefineDatabaseStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE DATABASE")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		if self.overwrite {
			write!(f, " OVERWRITE")?
		}
		write!(f, " {}", self.name)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {}", v.to_sql())?
		}
		if let Some(ref v) = self.changefeed {
			write!(f, " {v}")?;
		}
		Ok(())
	}
}

impl crate::sql::ToSql for DefineDatabaseStatement {
	fn to_sql(&self) -> String {
		let mut out = "DEFINE DATABASE".to_string();
		if self.if_not_exists {
			out.push_str(" IF NOT EXISTS");
		}
		if self.overwrite {
			out.push_str(" OVERWRITE");
		}
		out.push_str(&format!(" {}", self.name));
		if let Some(ref v) = self.comment {
			out.push_str(&format!(" COMMENT {}", v.to_sql()));
		}
		if let Some(ref v) = self.changefeed {
			out.push_str(&format!(" {v}"));
		}
		out
	}
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
