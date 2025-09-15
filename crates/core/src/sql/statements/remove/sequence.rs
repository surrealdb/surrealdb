use std::fmt::{self, Display, Formatter};

use crate::sql::Ident;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RemoveSequenceStatement {
	pub name: Ident,
	pub if_exists: bool,
}

impl Display for RemoveSequenceStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE SEQUENCE")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {}", self.name)?;
		Ok(())
	}
}

impl From<RemoveSequenceStatement> for crate::expr::statements::remove::RemoveSequenceStatement {
	fn from(v: RemoveSequenceStatement) -> Self {
		crate::expr::statements::remove::RemoveSequenceStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
		}
	}
}

impl From<crate::expr::statements::remove::RemoveSequenceStatement> for RemoveSequenceStatement {
	fn from(v: crate::expr::statements::remove::RemoveSequenceStatement) -> Self {
		RemoveSequenceStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
		}
	}
}
