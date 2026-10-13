use surrealdb_types::{SqlFormat, ToSql};

use crate::expr::Expr;
use crate::expr::statements::alter::AlterKind;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
/// Alters system-wide settings and maintenance operations.
///
/// Supported operations:
/// - `query_timeout`: set/drop a global query timeout which is enforced across queries. The value
///   is evaluated as a `Duration` expression at runtime.
/// - `compact`: runs a storage compaction across the entire datastore.
pub struct AlterSystemStatement {
	/// Global query timeout alteration. `Set` evaluates an expression to a
	/// `Duration`; `Drop` clears the timeout; `None` leaves it unchanged.
	pub query_timeout: AlterKind<Expr>,
	/// When true, triggers a datastore-wide compaction.
	pub compact: bool,
}

impl ToSql for AlterSystemStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::AlterSystemStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
