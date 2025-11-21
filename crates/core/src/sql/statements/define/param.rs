use std::fmt::{self, Display, Write};

use super::DefineKind;
use crate::fmt::{EscapeKwFreeIdent, is_pretty, pretty_indent};
use crate::sql::{Expr, Permission};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct DefineParamStatement {
	pub kind: DefineKind,
	pub name: String,
	pub value: Expr,
	pub comment: Option<Expr>,
	pub permissions: Permission,
}

impl Display for DefineParamStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE PARAM")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(f, " ${} VALUE {}", EscapeKwFreeIdent(&self.name), self.value)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {}", v)?
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

impl From<DefineParamStatement> for crate::expr::statements::DefineParamStatement {
	fn from(v: DefineParamStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name,
			value: v.value.into(),
			comment: v.comment.map(|x| x.into()),
			permissions: v.permissions.into(),
		}
	}
}

impl From<crate::expr::statements::DefineParamStatement> for DefineParamStatement {
	fn from(v: crate::expr::statements::DefineParamStatement) -> Self {
		DefineParamStatement {
			kind: v.kind.into(),
			name: v.name,
			value: v.value.into(),
			comment: v.comment.map(|x| x.into()),
			permissions: v.permissions.into(),
		}
	}
}
