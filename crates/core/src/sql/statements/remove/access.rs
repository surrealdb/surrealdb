use crate::sql::{Base, Ident};

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct RemoveAccessStatement {
	pub name: Ident,
	pub base: Base,
	#[revision(start = 2)]
	pub if_exists: bool,
}

impl Display for RemoveAccessStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE ACCESS")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {} ON {}", self.name, self.base)?;
		Ok(())
	}
}

impl From<RemoveAccessStatement> for crate::expr::statements::RemoveAccessStatement {
	fn from(v: RemoveAccessStatement) -> Self {
		crate::expr::statements::RemoveAccessStatement {
			name: v.name.into(),
			base: v.base.into(),
			if_exists: v.if_exists,
		}
	}
}

impl From<crate::expr::statements::RemoveAccessStatement> for RemoveAccessStatement {
	fn from(v: crate::expr::statements::RemoveAccessStatement) -> Self {
		RemoveAccessStatement {
			name: v.name.into(),
			base: v.base.into(),
			if_exists: v.if_exists,
		}
	}
}
