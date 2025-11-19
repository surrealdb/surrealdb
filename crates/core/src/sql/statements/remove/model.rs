use surrealdb_types::{SqlFormat, ToSql, write_sql};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RemoveModelStatement {
	pub name: String,
	pub version: String,
	pub if_exists: bool,
}

impl ToSql for RemoveModelStatement {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		write_sql!(f, "REMOVE MODEL");
		if self.if_exists {
			write_sql!(f, " IF EXISTS");
		}
		write_sql!(f, " ml::{}<{}>", &self.name, self.version);
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
