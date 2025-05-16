use crate::sql::Ident;

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Formatter};

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct RemoveEventStatement {
	pub name: Ident,
	pub what: Ident,
	#[revision(start = 2)]
	pub if_exists: bool,
}

impl From<RemoveEventStatement> for crate::expr::statements::RemoveEventStatement {
	fn from(v: RemoveEventStatement) -> Self {
		crate::expr::statements::RemoveEventStatement {
			name: v.name.into(),
			what: v.what.into(),
			if_exists: v.if_exists,
		}
	}
}

impl From<crate::expr::statements::RemoveEventStatement> for RemoveEventStatement {
	fn from(v: crate::expr::statements::RemoveEventStatement) -> Self {
		RemoveEventStatement {
			name: v.name.into(),
			what: v.what.into(),
			if_exists: v.if_exists,
		}
	}
}

crate::sql::impl_display_from_sql!(RemoveEventStatement);

impl crate::sql::DisplaySql for RemoveEventStatement {
	fn fmt_sql(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE EVENT")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {} ON {}", self.name, self.what)?;
		Ok(())
	}
}
