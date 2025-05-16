use crate::sql::Ident;

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Formatter};

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct RemoveTableStatement {
	pub name: Ident,
	#[revision(start = 2)]
	pub if_exists: bool,
	#[revision(start = 3)]
	pub expunge: bool,
}

impl From<RemoveTableStatement> for crate::expr::statements::RemoveTableStatement {
	fn from(v: RemoveTableStatement) -> Self {
		crate::expr::statements::RemoveTableStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			expunge: v.expunge,
		}
	}
}

impl From<crate::expr::statements::RemoveTableStatement> for RemoveTableStatement {
	fn from(v: crate::expr::statements::RemoveTableStatement) -> Self {
		RemoveTableStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			expunge: v.expunge,
		}
	}
}

crate::sql::impl_display_from_sql!(RemoveTableStatement);

impl crate::sql::DisplaySql for RemoveTableStatement {
	fn fmt_sql(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE TABLE")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {}", self.name)?;
		Ok(())
	}
}
