use std::fmt::{self, Display, Write};

use super::DefineKind;
use crate::sql::fmt::{is_pretty, pretty_indent};
use crate::sql::{Expr, Ident, Permission};
use crate::val::Strand;

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DefineParamStatement {
	pub kind: DefineKind,
	pub name: Ident,
	pub value: Expr,
	pub comment: Option<Strand>,
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
		write!(f, " ${} VALUE {}", self.name, self.value)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
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
			name: v.name.into(),
			value: v.value.into(),
			comment: v.comment,
			permissions: v.permissions.into(),
		}
	}
}

impl From<crate::expr::statements::DefineParamStatement> for DefineParamStatement {
	fn from(v: crate::expr::statements::DefineParamStatement) -> Self {
		DefineParamStatement {
			kind: v.kind.into(),
			name: v.name.into(),
			value: v.value.into(),
			comment: v.comment,
			permissions: v.permissions.into(),
		}
	}
}
