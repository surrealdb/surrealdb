use crate::sql::Ident;

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct RemoveModelStatement {
	pub name: Ident,
	pub version: String,
	#[revision(start = 2)]
	pub if_exists: bool,
}

impl Display for RemoveModelStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		// Bypass ident display since we don't want backticks arround the ident.
		write!(f, "REMOVE MODEL")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " ml::{}<{}>", self.name.0, self.version)?;
		Ok(())
	}
}

impl From<RemoveModelStatement> for crate::expr::statements::RemoveModelStatement {
	fn from(v: RemoveModelStatement) -> Self {
		crate::expr::statements::RemoveModelStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			version: v.version,
		}
	}
}

impl From<crate::expr::statements::RemoveModelStatement> for RemoveModelStatement {
	fn from(v: crate::expr::statements::RemoveModelStatement) -> Self {
		RemoveModelStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			version: v.version,
		}
	}
}
