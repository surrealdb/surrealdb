use std::fmt::{self, Display, Formatter};

use crate::sql::Ident;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RemoveDatabaseStatement {
	pub name: Ident,
	pub if_exists: bool,
	pub expunge: bool,
}

impl Display for RemoveDatabaseStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE DATABASE")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {}", self.name)?;
		Ok(())
	}
}

impl From<RemoveDatabaseStatement> for crate::expr::statements::RemoveDatabaseStatement {
	fn from(v: RemoveDatabaseStatement) -> Self {
		crate::expr::statements::RemoveDatabaseStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			expunge: v.expunge,
		}
	}
}

impl From<crate::expr::statements::RemoveDatabaseStatement> for RemoveDatabaseStatement {
	fn from(v: crate::expr::statements::RemoveDatabaseStatement) -> Self {
		RemoveDatabaseStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			expunge: v.expunge,
		}
	}
}
