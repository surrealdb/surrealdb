use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::AlterKind;
use crate::fmt::{EscapeKwFreeIdent, EscapeKwIdent, QuoteStr};
use crate::sql::{ChangeFeed, Permissions, TableType};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
/// AST node for `ALTER TABLE`.
///
/// Supported operations include (order-insensitive after the table name):
/// - `TYPE NORMAL | RELATION [IN <from> [OUT <to>]] | ANY`
/// - `SCHEMAFULL` / `SCHEMALESS`
/// - `PERMISSIONS ...`
/// - `CHANGEFEED ...` / `DROP CHANGEFEED`
/// - `COMMENT <string>` / `DROP COMMENT`
/// - `COMPACT` (request table keyspace compaction)
///
/// Note: `COMPACT` is parsed and preserved on the expression side, however it is
/// currently not rendered by this node's `ToSql` implementation.
pub struct AlterTableStatement {
	pub name: String,
	pub if_exists: bool,
	pub schemafull: AlterKind<()>,
	pub permissions: Option<Permissions>,
	pub changefeed: AlterKind<ChangeFeed>,
	pub comment: AlterKind<String>,
	pub kind: Option<TableType>,
	/// Request table‑level compaction when true.
	pub compact: bool,
}

impl ToSql for AlterTableStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "ALTER TABLE");
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		write_sql!(f, fmt, " {}", EscapeKwIdent(&self.name, &["IF"]));
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
							write_sql!(f, fmt, "{}", EscapeKwFreeIdent(k));
						}
					}
					if !rel.to.is_empty() {
						f.push_str(" OUT ");
						for (idx, k) in rel.to.iter().enumerate() {
							if idx != 0 {
								f.push_str(" | ");
							}
							write_sql!(f, fmt, "{}", EscapeKwFreeIdent(k));
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
		if let Some(permissions) = &self.permissions {
			write_sql!(f, fmt, " {permissions}");
		}

		if self.compact {
			write_sql!(f, fmt, " COMPACT");
		}
	}
}

impl From<AlterTableStatement> for crate::expr::statements::alter::AlterTableStatement {
	fn from(v: AlterTableStatement) -> Self {
		crate::expr::statements::alter::AlterTableStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			schemafull: v.schemafull.into(),
			permissions: v.permissions.map(Into::into),
			changefeed: v.changefeed.into(),
			comment: v.comment.into(),
			kind: v.kind.map(Into::into),
			compact: v.compact,
		}
	}
}

impl From<crate::expr::statements::alter::AlterTableStatement> for AlterTableStatement {
	fn from(v: crate::expr::statements::alter::AlterTableStatement) -> Self {
		AlterTableStatement {
			name: v.name.into_string(),
			if_exists: v.if_exists,
			schemafull: v.schemafull.into(),
			permissions: v.permissions.map(Into::into),
			changefeed: v.changefeed.into(),
			comment: v.comment.into(),
			kind: v.kind.map(Into::into),
			compact: v.compact,
		}
	}
}
