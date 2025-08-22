use std::fmt::{self, Display, Formatter};

use crate::sql::{Ident, Idiom};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RemoveFieldStatement {
	pub name: Idiom,
	pub what: Ident,
	pub if_exists: bool,
}

impl Display for RemoveFieldStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE FIELD")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {} ON {}", self.name, self.what)?;
		Ok(())
	}
}

impl From<RemoveFieldStatement> for crate::expr::statements::RemoveFieldStatement {
	fn from(v: RemoveFieldStatement) -> Self {
		crate::expr::statements::RemoveFieldStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			table_name: v.what.into(),
		}
	}
}

impl From<crate::expr::statements::RemoveFieldStatement> for RemoveFieldStatement {
	fn from(v: crate::expr::statements::RemoveFieldStatement) -> Self {
		RemoveFieldStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			what: v.table_name.into(),
		}
	}
}
