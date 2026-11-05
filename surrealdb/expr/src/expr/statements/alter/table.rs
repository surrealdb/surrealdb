use common::fmt::QuoteStr;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::AlterKind;
use crate::expr::permission::Permissions;
use crate::expr::table_type::TableType;
use crate::expr::{ChangeFeed, CoverStmts, Expr, Literal};

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
	/// Set/drop the inline adjacency cache cap.
	pub inline_edges_cap: AlterKind<u32>,
	/// Set/drop the inline reference cache cap.
	pub inline_refs_cap: AlterKind<u32>,
	/// `MIGRATE EXISTING ROWS AS <name> …`: a migration of the table's rows.
	pub migrate: Option<Box<TableMigrateClause>>,
}

/// `MIGRATE EXISTING ROWS AS <name> <action>` on `ALTER TABLE`: a named
/// migration of every row stored before the statement.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct TableMigrateClause {
	/// The migration's name, unique per table.
	pub name: String,
	/// What the migration does to each row.
	pub action: TableMigrateAction,
}

/// What a table-level migration does to each row stored before it.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum TableMigrateAction {
	/// `REMOVE UNDECLARED FIELDS`: clear every top-level field no definition
	/// of the table declares.
	RemoveUndeclaredFields,
}

impl ToSql for TableMigrateClause {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, " MIGRATE EXISTING ROWS AS {}", common::fmt::EscapeIdent(&self.name));
		match self.action {
			TableMigrateAction::RemoveUndeclaredFields => f.push_str(" REMOVE UNDECLARED FIELDS"),
		}
	}
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
			inline_edges_cap: AlterKind::None,
			inline_refs_cap: AlterKind::None,
			migrate: None,
		}
	}
}

impl ToSql for AlterTableStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "ALTER TABLE");
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		write_sql!(f, fmt, " {}", CoverStmts(&self.name));
		if let Some(kind) = &self.kind {
			write_sql!(f, fmt, " TYPE");
			match &kind {
				TableType::Normal => {
					write_sql!(f, fmt, " NORMAL");
				}
				TableType::Relation(rel) => {
					write_sql!(f, fmt, " RELATION");
					if !rel.from.is_empty() {
						f.push_str(" IN ");
						for (idx, k) in rel.from.iter().enumerate() {
							if idx != 0 {
								f.push_str(" | ");
							}
							write_sql!(f, fmt, "{}", common::fmt::EscapeIdent(k.as_str()));
						}
					}
					if !rel.to.is_empty() {
						f.push_str(" OUT ");
						for (idx, k) in rel.to.iter().enumerate() {
							if idx != 0 {
								f.push_str(" | ");
							}
							write_sql!(f, fmt, "{}", common::fmt::EscapeIdent(k.as_str()));
						}
					}
				}
				TableType::Any => {
					write_sql!(f, fmt, " ANY");
				}
			}
		}

		match self.schemafull {
			AlterKind::Set(_) => write_sql!(f, fmt, " SCHEMAFULL"),
			AlterKind::Drop => write_sql!(f, fmt, " SCHEMALESS"),
			AlterKind::None => {}
		}

		match self.comment {
			AlterKind::Set(ref comment) => write_sql!(f, fmt, " COMMENT {}", QuoteStr(comment)),
			AlterKind::Drop => f.push_str(" DROP COMMENT"),
			AlterKind::None => {}
		}

		match self.changefeed {
			AlterKind::Set(ref changefeed) => write_sql!(f, fmt, " {}", changefeed),
			AlterKind::Drop => f.push_str(" DROP CHANGEFEED"),
			AlterKind::None => {}
		}
		match self.inline_edges_cap {
			AlterKind::Set(cap) => write_sql!(f, fmt, " INLINE EDGES {}", cap),
			AlterKind::Drop => f.push_str(" DROP INLINE EDGES"),
			AlterKind::None => {}
		}

		match self.inline_refs_cap {
			AlterKind::Set(cap) => write_sql!(f, fmt, " INLINE REFERENCES {}", cap),
			AlterKind::Drop => f.push_str(" DROP INLINE REFERENCES"),
			AlterKind::None => {}
		}

		if let Some(permissions) = &self.permissions {
			write_sql!(f, fmt, " {permissions}");
		}

		if self.compact {
			write_sql!(f, fmt, " COMPACT");
		}

		if let Some(migrate) = &self.migrate {
			migrate.fmt_sql(f, fmt);
		}
	}
}
