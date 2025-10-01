use std::fmt::{self, Display, Write};

use super::DefineKind;
use crate::fmt::{EscapeIdent, is_pretty, pretty_indent};
use crate::sql::{Block, Expr, Kind, Permission};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DefineFunctionStatement {
	pub kind: DefineKind,
	pub name: String,
	pub args: Vec<(String, Kind)>,
	pub block: Block,
	pub comment: Option<Expr>,
	pub permissions: Permission,
	pub returns: Option<Kind>,
}

impl fmt::Display for DefineFunctionStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE FUNCTION")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(f, " fn::{}(", &self.name)?;
		for (i, (name, kind)) in self.args.iter().enumerate() {
			if i > 0 {
				f.write_str(", ")?;
			}
			write!(f, "${}: {kind}", EscapeIdent(name))?;
		}
		f.write_str(") ")?;
		if let Some(ref v) = self.returns {
			write!(f, "-> {v} ")?;
		}
		Display::fmt(&self.block, f)?;
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

impl From<DefineFunctionStatement> for crate::expr::statements::DefineFunctionStatement {
	fn from(v: DefineFunctionStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name,
			args: v.args.into_iter().map(|(i, k)| (i, k.into())).collect(),
			block: v.block.into(),
			comment: v.comment.map(|x| x.into()),
			permissions: v.permissions.into(),
			returns: v.returns.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::DefineFunctionStatement> for DefineFunctionStatement {
	fn from(v: crate::expr::statements::DefineFunctionStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name,
			args: v.args.into_iter().map(|(i, k)| (i, k.into())).collect(),
			block: v.block.into(),
			comment: v.comment.map(|x| x.into()),
			permissions: v.permissions.into(),
			returns: v.returns.map(Into::into),
		}
	}
}
