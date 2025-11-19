use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::AlterKind;
use crate::fmt::EscapeIdent;
use crate::sql::{ChangeFeed, Kind, Permissions, TableType};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct AlterTableStatement {
	pub name: String,
	pub if_exists: bool,
	pub schemafull: AlterKind<()>,
	pub permissions: Option<Permissions>,
	pub changefeed: AlterKind<ChangeFeed>,
	pub comment: AlterKind<String>,
	pub kind: Option<TableType>,
}

impl ToSql for AlterTableStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("ALTER TABLE");
		if self.if_exists {
			f.push_str(" IF EXISTS");
		}
		write_sql!(f, " {}", EscapeIdent(&self.name));
		if let Some(kind) = &self.kind {
			f.push_str(" TYPE");
			match &kind {
				TableType::Normal => {
					f.push_str(" NORMAL");
				}
				TableType::Relation(rel) => {
					f.push_str(" RELATION");
					if let Some(Kind::Record(kind)) = &rel.from {
						f.push_str(" IN ");
						for (idx, k) in kind.iter().enumerate() {
							if idx != 0 {
								f.push_str(" | ");
							}
							write_sql!(f, "{}", EscapeIdent(k));
						}
					}
					if let Some(Kind::Record(kind)) = &rel.to {
						f.push_str(" OUT ");
						for (idx, k) in kind.iter().enumerate() {
							if idx != 0 {
								f.push_str(" | ");
							}
							write_sql!(f, "{}", EscapeIdent(k));
						}
					}
				}
				TableType::Any => {
					f.push_str(" ANY");
				}
			}
		}

		match self.schemafull {
			AlterKind::Set(_) => f.push_str(" SCHEMAFULL"),
			AlterKind::Drop => f.push_str(" SCHEMALESS"),
			AlterKind::None => {}
		}

		match self.comment {
			AlterKind::Set(ref comment) => {
				f.push_str(" COMMENT ");
				comment.fmt_sql(f, fmt);
			}
			AlterKind::Drop => f.push_str(" DROP COMMENT"),
			AlterKind::None => {}
		}

		match self.changefeed {
			AlterKind::Set(ref changefeed) => {
				f.push_str(" CHANGEFEED ");
				changefeed.fmt_sql(f, fmt);
			}
			AlterKind::Drop => f.push_str(" DROP CHANGEFEED"),
			AlterKind::None => {}
		}

		if let Some(permissions) = &self.permissions {
			permissions.fmt_sql(f, fmt);
		}
	}
}

impl From<AlterTableStatement> for crate::expr::statements::alter::AlterTableStatement {
	fn from(v: AlterTableStatement) -> Self {
		crate::expr::statements::alter::AlterTableStatement {
			name: v.name,
			if_exists: v.if_exists,
			schemafull: v.schemafull.into(),
			permissions: v.permissions.map(Into::into),
			changefeed: v.changefeed.into(),
			comment: v.comment.into(),
			kind: v.kind.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::alter::AlterTableStatement> for AlterTableStatement {
	fn from(v: crate::expr::statements::alter::AlterTableStatement) -> Self {
		AlterTableStatement {
			name: v.name,
			if_exists: v.if_exists,
			schemafull: v.schemafull.into(),
			permissions: v.permissions.map(Into::into),
			changefeed: v.changefeed.into(),
			comment: v.comment.into(),
			kind: v.kind.map(Into::into),
		}
	}
}
