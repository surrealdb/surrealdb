use common::fmt::EscapeKwFreeIdent;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

/// `REMOVE TYPE [IF EXISTS] name`: removes a type of the selected database. It is refused while a
/// field, a function signature or another type names it, and, when `object` is set, when the type
/// is not that kind of SQL object.
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct RemoveTypeStatement {
	pub name: Strand,
	pub if_exists: bool,
	/// The kind of SQL object the removal names the type as, which the stored type must be when
	/// the statement runs. `None` removes a type of any kind; SurrealQL has no syntax for it, so
	/// the statement's text never shows it.
	pub object: Option<SqlTypeObject>,
}

/// The kind of PostgreSQL object a type is, as `DROP TYPE` and `DROP DOMAIN` name it. A union of
/// string labels is an enum and an object literal kind is a composite type; any other type is a
/// domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum SqlTypeObject {
	/// An enum or a composite type: anything but a domain.
	Type,
	/// A domain.
	Domain,
}

impl ToSql for RemoveTypeStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "REMOVE TYPE");
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		write_sql!(f, fmt, " {}", EscapeKwFreeIdent(self.name.as_str()));
	}
}
