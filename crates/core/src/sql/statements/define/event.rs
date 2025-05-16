use crate::sql::{Ident, SqlValue, Strand, Values};

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self};

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineEventStatement {
	pub name: Ident,
	pub what: Ident,
	pub when: SqlValue,
	pub then: Values,
	pub comment: Option<Strand>,
	#[revision(start = 2)]
	pub if_not_exists: bool,
	#[revision(start = 3)]
	pub overwrite: bool,
}

impl From<DefineEventStatement> for crate::expr::statements::DefineEventStatement {
	fn from(v: DefineEventStatement) -> Self {
		crate::expr::statements::DefineEventStatement {
			name: v.name.into(),
			what: v.what.into(),
			when: v.when.into(),
			then: v.then.into(),
			comment: v.comment.map(Into::into),
			if_not_exists: v.if_not_exists,
			overwrite: v.overwrite,
		}
	}
}

impl From<crate::expr::statements::DefineEventStatement> for DefineEventStatement {
	fn from(v: crate::expr::statements::DefineEventStatement) -> Self {
		DefineEventStatement {
			name: v.name.into(),
			what: v.what.into(),
			when: v.when.into(),
			then: v.then.into(),
			comment: v.comment.map(Into::into),
			if_not_exists: v.if_not_exists,
			overwrite: v.overwrite,
		}
	}
}

crate::sql::impl_display_from_sql!(DefineEventStatement);

impl crate::sql::DisplaySql for DefineEventStatement {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE EVENT",)?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		if self.overwrite {
			write!(f, " OVERWRITE")?
		}
		write!(f, " {} ON {} WHEN {} THEN {}", self.name, self.what, self.when, self.then)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		Ok(())
	}
}
