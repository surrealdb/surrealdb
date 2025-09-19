use std::fmt::{self, Display, Formatter};

use crate::fmt::EscapeIdent;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RemoveTableStatement {
	pub name: String,
	pub if_exists: bool,
	pub expunge: bool,
}

impl Display for RemoveTableStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE TABLE")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {}", EscapeIdent(&self.name))?;
		Ok(())
	}
}

impl From<RemoveTableStatement> for crate::expr::statements::RemoveTableStatement {
	fn from(v: RemoveTableStatement) -> Self {
		crate::expr::statements::RemoveTableStatement {
			name: v.name,
			if_exists: v.if_exists,
			expunge: v.expunge,
		}
	}
}

impl From<crate::expr::statements::RemoveTableStatement> for RemoveTableStatement {
	fn from(v: crate::expr::statements::RemoveTableStatement) -> Self {
		RemoveTableStatement {
			name: v.name,
			if_exists: v.if_exists,
			expunge: v.expunge,
		}
	}
}
