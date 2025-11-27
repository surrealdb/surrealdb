use std::fmt::{self, Write};

use super::DefineKind;
use crate::fmt::{CoverStmts, is_pretty, pretty_indent};
use crate::sql::{Expr, ModuleExecutable, Permission};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct DefineModuleStatement {
	pub kind: DefineKind,
	pub name: Option<String>,
	pub executable: ModuleExecutable,
	pub comment: Expr,
	pub permissions: Permission,
}

impl fmt::Display for DefineModuleStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE MODULE")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		if let Some(name) = &self.name {
			write!(f, " mod::{name} AS")?;
		}
		write!(f, " {}", self.executable)?;
		write!(f, " COMMENT {}", CoverStmts(&self.comment))?;
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

impl From<DefineModuleStatement> for crate::expr::statements::DefineModuleStatement {
	fn from(v: DefineModuleStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name,
			executable: v.executable.into(),
			comment: v.comment.into(),
			permissions: v.permissions.into(),
		}
	}
}

impl From<crate::expr::statements::DefineModuleStatement> for DefineModuleStatement {
	fn from(v: crate::expr::statements::DefineModuleStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name,
			executable: v.executable.into(),
			comment: v.comment.into(),
			permissions: v.permissions.into(),
		}
	}
}
