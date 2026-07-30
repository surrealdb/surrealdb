use surrealdb_types::{SqlFormat, ToSql};

use super::AlterKind;
use crate::expr::permission::Permissions;
use crate::expr::table_type::TableType;
use crate::expr::{ChangeFeed, Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
/// Executes `ALTER TABLE` operations against an existing table.
///
/// Supported operations include:
/// - toggle `SCHEMAFULL`/`SCHEMALESS`
/// - update `PERMISSIONS`
/// - set/drop `CHANGEFEED`
/// - set/drop table `COMMENT`
/// - change table `TYPE` (`NORMAL`/`RELATION`/`ANY`)
/// - request a table-level storage `COMPACT`
///
/// Notes:
/// - When switching to a `RELATION` table type, in/out fields are created as needed via
///   `DefineTableStatement::add_in_out_fields`.
/// - When `compact` is true, underlying storage for this table is compacted.
pub struct AlterTableStatement {
	/// Table name.
	pub name: Expr,
	/// If true, do nothing (and succeed) when the table does not exist.
	pub if_exists: bool,
	/// Switch `SCHEMAFULL` on (`Set`) or switch to `SCHEMALESS` (`Drop`).
	pub schemafull: AlterKind<()>,
	/// New table permissions, if provided.
	pub permissions: Option<Permissions>,
	/// Set/drop changefeed definition.
	pub changefeed: AlterKind<ChangeFeed>,
	/// Set/drop human‑readable comment.
	pub comment: AlterKind<String>,
	/// Request a compaction of the table’s keyspace.
	pub compact: bool,
	/// Change the table type (`NORMAL` / `RELATION` / `ANY`).
	pub kind: Option<TableType>,
}

impl Default for AlterTableStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			if_exists: false,
			schemafull: AlterKind::None,
			permissions: None,
			changefeed: AlterKind::None,
			comment: AlterKind::None,
			compact: false,
			kind: None,
		}
	}
}

impl ToSql for AlterTableStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::AlterTableStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
