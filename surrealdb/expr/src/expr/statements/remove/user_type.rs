use common::fmt::EscapeKwFreeIdent;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::statements::define::UserTypeKind;

/// Removes a user-defined SQL type of the selected database: the PostgreSQL `DROP DOMAIN` and the
/// `DROP TYPE` of a composite type. The type removed must be of `type_kind`.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct RemoveUserTypeStatement {
	pub name: Strand,
	pub type_kind: UserTypeKind,
	pub if_exists: bool,
}

/// The text of the removal for people, as for
/// [`DefineUserTypeStatement`](crate::expr::statements::define::DefineUserTypeStatement).
impl ToSql for RemoveUserTypeStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self.type_kind {
			UserTypeKind::Domain => write_sql!(f, fmt, "REMOVE DOMAIN"),
			UserTypeKind::Composite => write_sql!(f, fmt, "REMOVE COMPOSITE"),
		}
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		write_sql!(f, fmt, " {}", EscapeKwFreeIdent(self.name.as_str()));
	}
}
