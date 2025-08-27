use std::fmt::{self, Display, Formatter};

use crate::sql::{Base, Ident};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RemoveUserStatement {
	pub name: Ident,
	pub base: Base,
	pub if_exists: bool,
}

impl Display for RemoveUserStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE USER")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {} ON {}", self.name, self.base)?;
		Ok(())
	}
}

impl From<RemoveUserStatement> for crate::expr::statements::RemoveUserStatement {
	fn from(v: RemoveUserStatement) -> Self {
		crate::expr::statements::RemoveUserStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			base: v.base.into(),
		}
	}
}

impl From<crate::expr::statements::RemoveUserStatement> for RemoveUserStatement {
	fn from(v: crate::expr::statements::RemoveUserStatement) -> Self {
		RemoveUserStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			base: v.base.into(),
		}
	}
}
