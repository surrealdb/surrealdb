use crate::sql::fmt::{is_pretty, pretty_indent};
use crate::sql::{ChangeFeed, Ident, Kind, Permissions, Strand, TableType};
use anyhow::Result;

use std::fmt::{self, Display, Write};

use super::AlterKind;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct AlterTableStatement {
	pub name: Ident,
	pub if_exists: bool,
	pub schemafull: AlterKind<()>,
	pub permissions: Option<Permissions>,
	pub changefeed: AlterKind<ChangeFeed>,
	pub comment: AlterKind<Strand>,
	pub kind: Option<TableType>,
}

impl AlterTableStatement {
	fn convert_drop(
		&mut self,
		_revision: u16,
		_value: Option<bool>,
	) -> Result<(), revision::Error> {
		Ok(())
	}
}

impl Display for AlterTableStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "ALTER TABLE")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {}", self.name)?;
		if let Some(kind) = &self.kind {
			write!(f, " TYPE")?;
			match &kind {
				TableType::Normal => {
					f.write_str(" NORMAL")?;
				}
				TableType::Relation(rel) => {
					f.write_str(" RELATION")?;
					if let Some(Kind::Record(kind)) = &rel.from {
						write!(
							f,
							" IN {}",
							kind.iter().map(|t| t.0.as_str()).collect::<Vec<_>>().join(" | ")
						)?;
					}
					if let Some(Kind::Record(kind)) = &rel.to {
						write!(
							f,
							" OUT {}",
							kind.iter().map(|t| t.0.as_str()).collect::<Vec<_>>().join(" | ")
						)?;
					}
				}
				TableType::Any => {
					f.write_str(" ANY")?;
				}
			}
		}
		if let Some(full) = self.schemafull {
			f.write_str(if full {
				" SCHEMAFULL"
			} else {
				" SCHEMALESS"
			})?;
		}
		if let Some(comment) = &self.comment {
			if let Some(comment) = comment {
				write!(f, " COMMENT {}", comment.clone())?;
			} else {
				write!(f, " DROP COMMENT")?;
			}
		}
		if let Some(changefeed) = &self.changefeed {
			if let Some(changefeed) = changefeed {
				write!(f, " CHANGEFEED {}", changefeed.clone())?;
			} else {
				write!(f, " DROP CHANGEFEED")?;
			}
		}
		let _indent = if is_pretty() {
			Some(pretty_indent())
		} else {
			f.write_char(' ')?;
			None
		};
		if let Some(permissions) = &self.permissions {
			write!(f, "{permissions}")?;
		}
		Ok(())
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
		}
	}
}

impl From<crate::expr::statements::alter::AlterTableStatement> for AlterTableStatement {
	fn from(v: crate::expr::statements::alter::AlterTableStatement) -> Self {
		AlterTableStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			schemafull: v.schemafull.into(),
			permissions: v.permissions.map(Into::into),
			changefeed: v.changefeed.into(),
			comment: v.comment.into(),
			kind: v.kind.map(Into::into),
		}
	}
}
