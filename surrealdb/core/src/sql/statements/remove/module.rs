use std::fmt::{self, Display};

use crate::sql::ModuleName;

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RemoveModuleStatement {
	pub name: ModuleName,
	pub if_exists: bool,
}

impl Display for RemoveModuleStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		// Bypass ident display since we don't want backticks arround the ident.
		write!(f, "REMOVE MODULE")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {}", self.name)?;
		Ok(())
	}
}

impl From<RemoveModuleStatement> for crate::expr::statements::RemoveModuleStatement {
	fn from(v: RemoveModuleStatement) -> Self {
		crate::expr::statements::RemoveModuleStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
		}
	}
}
impl From<crate::expr::statements::RemoveModuleStatement> for RemoveModuleStatement {
	fn from(v: crate::expr::statements::RemoveModuleStatement) -> Self {
		RemoveModuleStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
		}
	}
}
