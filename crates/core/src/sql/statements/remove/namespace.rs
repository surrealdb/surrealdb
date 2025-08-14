use std::fmt::{self, Display, Formatter};

use crate::sql::Ident;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RemoveNamespaceStatement {
	pub name: Ident,
	pub if_exists: bool,
	pub expunge: bool,
}

impl Display for RemoveNamespaceStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE NAMESPACE")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {}", self.name)?;
		Ok(())
	}
}

impl From<RemoveNamespaceStatement> for crate::expr::statements::RemoveNamespaceStatement {
	fn from(v: RemoveNamespaceStatement) -> Self {
		crate::expr::statements::RemoveNamespaceStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			expunge: v.expunge,
		}
	}
}

impl From<crate::expr::statements::RemoveNamespaceStatement> for RemoveNamespaceStatement {
	fn from(v: crate::expr::statements::RemoveNamespaceStatement) -> Self {
		RemoveNamespaceStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			expunge: v.expunge,
		}
	}
}
