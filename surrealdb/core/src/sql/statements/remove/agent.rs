use surrealdb_types::{SqlFormat, ToSql, write_sql};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RemoveAgentStatement {
	pub name: String,
	pub if_exists: bool,
}

impl ToSql for RemoveAgentStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "REMOVE AGENT");
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		write_sql!(f, fmt, " {}", self.name);
	}
}

impl From<RemoveAgentStatement> for crate::expr::statements::RemoveAgentStatement {
	fn from(v: RemoveAgentStatement) -> Self {
		crate::expr::statements::RemoveAgentStatement {
			name: v.name,
			if_exists: v.if_exists,
		}
	}
}

impl From<crate::expr::statements::RemoveAgentStatement> for RemoveAgentStatement {
	fn from(v: crate::expr::statements::RemoveAgentStatement) -> Self {
		RemoveAgentStatement {
			name: v.name,
			if_exists: v.if_exists,
		}
	}
}
