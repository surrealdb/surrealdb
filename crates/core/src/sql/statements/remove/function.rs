use crate::sql::Ident;

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct RemoveFunctionStatement {
	pub name: Ident,
	#[revision(start = 2)]
	pub if_exists: bool,
}

impl Display for RemoveFunctionStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		// Bypass ident display since we don't want backticks arround the ident.
		write!(f, "REMOVE FUNCTION")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " fn::{}", self.name.0)?;
		Ok(())
	}
}

impl From<RemoveFunctionStatement> for crate::expr::statements::RemoveFunctionStatement {
	fn from(v: RemoveFunctionStatement) -> Self {
		crate::expr::statements::RemoveFunctionStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
		}
	}
}
impl From<crate::expr::statements::RemoveFunctionStatement> for RemoveFunctionStatement {
	fn from(v: crate::expr::statements::RemoveFunctionStatement) -> Self {
		RemoveFunctionStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
		}
	}
}
