use std::fmt::{self, Display, Formatter};

use crate::fmt::EscapeIdent;
use crate::sql::Base;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RemoveAccessStatement {
	pub name: String,
	pub base: Base,
	pub if_exists: bool,
}

impl Display for RemoveAccessStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE ACCESS")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {} ON {}", EscapeIdent(&self.name), self.base)?;
		Ok(())
	}
}

impl From<RemoveAccessStatement> for crate::expr::statements::RemoveAccessStatement {
	fn from(v: RemoveAccessStatement) -> Self {
		crate::expr::statements::RemoveAccessStatement {
			name: v.name,
			base: v.base.into(),
			if_exists: v.if_exists,
		}
	}
}

impl From<crate::expr::statements::RemoveAccessStatement> for RemoveAccessStatement {
	fn from(v: crate::expr::statements::RemoveAccessStatement) -> Self {
		RemoveAccessStatement {
			name: v.name,
			base: v.base.into(),
			if_exists: v.if_exists,
		}
	}
}
