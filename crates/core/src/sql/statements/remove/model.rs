use std::fmt::{self, Display};
use surrealdb_types::{SqlFormat, ToSql, write_sql};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RemoveModelStatement {
	pub name: String,
	pub version: String,
	pub if_exists: bool,
}

impl Display for RemoveModelStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		// Bypass ident display since we don't want backticks arround the ident.
		write!(f, "REMOVE MODEL")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " ml::{}<{}>", &self.name, self.version)?;
		Ok(())
	}
}

impl ToSql for RemoveModelStatement {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		write_sql!(f, "{}", self)
	}
}

impl From<RemoveModelStatement> for crate::expr::statements::RemoveModelStatement {
	fn from(v: RemoveModelStatement) -> Self {
		crate::expr::statements::RemoveModelStatement {
			name: v.name,
			if_exists: v.if_exists,
			version: v.version,
		}
	}
}

impl From<crate::expr::statements::RemoveModelStatement> for RemoveModelStatement {
	fn from(v: crate::expr::statements::RemoveModelStatement) -> Self {
		RemoveModelStatement {
			name: v.name,
			if_exists: v.if_exists,
			version: v.version,
		}
	}
}
