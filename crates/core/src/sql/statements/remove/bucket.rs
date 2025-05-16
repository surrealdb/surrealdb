use crate::sql::Ident;

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Formatter};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct RemoveBucketStatement {
	pub name: Ident,
	pub if_exists: bool,
}

impl From<RemoveBucketStatement> for crate::expr::statements::remove::RemoveBucketStatement {
	fn from(v: RemoveBucketStatement) -> Self {
		crate::expr::statements::remove::RemoveBucketStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
		}
	}
}

impl From<crate::expr::statements::remove::RemoveBucketStatement> for RemoveBucketStatement {
	fn from(v: crate::expr::statements::remove::RemoveBucketStatement) -> Self {
		RemoveBucketStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
		}
	}
}

crate::sql::impl_display_from_sql!(RemoveBucketStatement);

impl crate::sql::DisplaySql for RemoveBucketStatement {
	fn fmt_sql(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE BUCKET")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {}", self.name)?;
		Ok(())
	}
}
