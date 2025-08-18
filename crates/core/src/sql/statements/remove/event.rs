use std::fmt::{self, Display, Formatter};

use crate::sql::Ident;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RemoveEventStatement {
	pub name: Ident,
	pub what: Ident,
	pub if_exists: bool,
}

impl Display for RemoveEventStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE EVENT")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {} ON {}", self.name, self.what)?;
		Ok(())
	}
}

impl From<RemoveEventStatement> for crate::expr::statements::RemoveEventStatement {
	fn from(v: RemoveEventStatement) -> Self {
		crate::expr::statements::RemoveEventStatement {
			name: v.name.into(),
			table_name: v.what.into(),
			if_exists: v.if_exists,
		}
	}
}

impl From<crate::expr::statements::RemoveEventStatement> for RemoveEventStatement {
	fn from(v: crate::expr::statements::RemoveEventStatement) -> Self {
		RemoveEventStatement {
			name: v.name.into(),
			what: v.table_name.into(),
			if_exists: v.if_exists,
		}
	}
}
