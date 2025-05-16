use crate::sql::Ident;

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Formatter};

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct RemoveNamespaceStatement {
	pub name: Ident,
	#[revision(start = 2)]
	pub if_exists: bool,
	#[revision(start = 3)]
	pub expunge: bool,
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

crate::sql::impl_display_from_sql!(RemoveNamespaceStatement);

impl crate::sql::DisplaySql for RemoveNamespaceStatement {
	fn fmt_sql(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE NAMESPACE")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {}", self.name)?;
		Ok(())
	}
}
