use std::fmt::{self, Display};
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::sql::{Expr, Literal};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DefineNamespaceStatement {
	pub kind: DefineKind,
	pub id: Option<u32>,
	pub name: Expr,
	pub comment: Option<Expr>,
}

impl Default for DefineNamespaceStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			id: None,
			name: Expr::Literal(Literal::None),
			comment: None,
		}
	}
}

impl Display for DefineNamespaceStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE NAMESPACE")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(f, " {}", self.name)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {}", v)?
		}
		Ok(())
	}
}

impl ToSql for DefineNamespaceStatement {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		write_sql!(f, "{}", self)
	}
}

impl From<DefineNamespaceStatement> for crate::expr::statements::DefineNamespaceStatement {
	fn from(v: DefineNamespaceStatement) -> Self {
		Self {
			kind: v.kind.into(),
			id: v.id,
			name: v.name.into(),
			comment: v.comment.map(|x| x.into()),
		}
	}
}

#[allow(clippy::fallible_impl_from)]
impl From<crate::expr::statements::DefineNamespaceStatement> for DefineNamespaceStatement {
	fn from(v: crate::expr::statements::DefineNamespaceStatement) -> Self {
		Self {
			kind: v.kind.into(),
			id: v.id,
			name: v.name.into(),
			comment: v.comment.map(|x| x.into()),
		}
	}
}
