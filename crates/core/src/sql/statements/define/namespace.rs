use std::fmt::{self, Display};

use super::DefineKind;
use crate::fmt::{EscapeIdent, QuoteStr};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DefineNamespaceStatement {
	pub kind: DefineKind,
	pub id: Option<u32>,
	pub name: String,
	pub comment: Option<String>,
}

impl Display for DefineNamespaceStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE NAMESPACE")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(f, " {}", EscapeIdent(&self.name))?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {}", QuoteStr(v))?
		}
		Ok(())
	}
}

impl From<DefineNamespaceStatement> for crate::expr::statements::DefineNamespaceStatement {
	fn from(v: DefineNamespaceStatement) -> Self {
		Self {
			kind: v.kind.into(),
			id: v.id,
			name: v.name,
			comment: v.comment,
		}
	}
}

impl From<crate::expr::statements::DefineNamespaceStatement> for DefineNamespaceStatement {
	fn from(v: crate::expr::statements::DefineNamespaceStatement) -> Self {
		Self {
			kind: v.kind.into(),
			id: v.id,
			name: v.name,
			comment: v.comment,
		}
	}
}
