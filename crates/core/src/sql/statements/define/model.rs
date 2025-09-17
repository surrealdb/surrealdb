use std::fmt::{self, Write};

use super::DefineKind;
use crate::fmt::{EscapeIdent, QuoteStr, is_pretty, pretty_indent};
use crate::sql::Permission;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DefineModelStatement {
	pub kind: DefineKind,
	pub hash: String,
	pub name: String,
	pub version: String,
	pub comment: Option<String>,
	pub permissions: Permission,
}

impl fmt::Display for DefineModelStatement {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "DEFINE MODEL")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(f, " ml::{}<{}>", EscapeIdent(&self.name), self.version)?;
		if let Some(comment) = self.comment.as_ref() {
			write!(f, " COMMENT {}", QuoteStr(comment))?;
		}
		let _indent = if is_pretty() {
			Some(pretty_indent())
		} else {
			f.write_char(' ')?;
			None
		};
		write!(f, "PERMISSIONS {}", self.permissions)?;
		Ok(())
	}
}

impl From<DefineModelStatement> for crate::expr::statements::DefineModelStatement {
	fn from(v: DefineModelStatement) -> Self {
		Self {
			kind: v.kind.into(),
			hash: v.hash,
			name: v.name,
			version: v.version,
			comment: v.comment,
			permissions: v.permissions.into(),
		}
	}
}

impl From<crate::expr::statements::DefineModelStatement> for DefineModelStatement {
	fn from(v: crate::expr::statements::DefineModelStatement) -> Self {
		Self {
			kind: v.kind.into(),
			hash: v.hash,
			name: v.name,
			version: v.version,
			comment: v.comment,
			permissions: v.permissions.into(),
		}
	}
}
