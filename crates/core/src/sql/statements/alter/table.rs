use std::fmt::{self, Display, Write};

use super::AlterKind;
use crate::sql::fmt::{is_pretty, pretty_indent};
use crate::sql::{ChangeFeed, Ident, Kind, Permissions, TableType};
use crate::val::Strand;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
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
						write!(f, " IN ",)?;
						for (idx, k) in kind.iter().enumerate() {
							if idx != 0 {
								write!(f, " | ")?;
							}
							write!(f, "{}", k)?;
						}
					}
					if let Some(Kind::Record(kind)) = &rel.to {
						write!(f, " OUT ",)?;
						for (idx, k) in kind.iter().enumerate() {
							if idx != 0 {
								write!(f, " | ")?;
							}
							write!(f, "{}", k)?;
						}
					}
				}
				TableType::Any => {
					f.write_str(" ANY")?;
				}
			}
		}

		match self.schemafull {
			AlterKind::Set(_) => " SCHEMAFULL".fmt(f)?,
			AlterKind::Drop => " SCHEMALESS".fmt(f)?,
			AlterKind::None => {}
		}

		match self.comment {
			AlterKind::Set(ref comment) => write!(f, " COMMENT {}", comment)?,
			AlterKind::Drop => write!(f, " DROP COMMENT")?,
			AlterKind::None => {}
		}

		match self.changefeed {
			AlterKind::Set(ref changefeed) => write!(f, " CHANGEFEED {}", changefeed)?,
			AlterKind::Drop => write!(f, " DROP CHANGEFEED")?,
			AlterKind::None => {}
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
