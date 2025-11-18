use std::fmt::{self, Display, Formatter};
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::EscapeIdent;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RemoveParamStatement {
	pub name: String,
	pub if_exists: bool,
}

impl Display for RemoveParamStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE PARAM")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " ${}", EscapeIdent(&self.name))?;
		Ok(())
	}
}

impl ToSql for RemoveParamStatement {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		write_sql!(f, "{}", self)
	}
}

impl From<RemoveParamStatement> for crate::expr::statements::RemoveParamStatement {
	fn from(v: RemoveParamStatement) -> Self {
		crate::expr::statements::RemoveParamStatement {
			name: v.name,
			if_exists: v.if_exists,
		}
	}
}

impl From<crate::expr::statements::RemoveParamStatement> for RemoveParamStatement {
	fn from(v: crate::expr::statements::RemoveParamStatement) -> Self {
		RemoveParamStatement {
			name: v.name,
			if_exists: v.if_exists,
		}
	}
}
