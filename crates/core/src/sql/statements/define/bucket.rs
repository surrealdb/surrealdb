use std::fmt::{self, Display};

use super::DefineKind;
use crate::sql::{Expr, Ident, Permission};
use crate::val::Strand;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DefineBucketStatement {
	pub kind: DefineKind,
	pub name: Ident,
	pub backend: Option<Expr>,
	pub permissions: Permission,
	pub readonly: bool,
	pub comment: Option<Strand>,
}

impl Display for DefineBucketStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE BUCKET")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(f, " {}", self.name)?;

		if self.readonly {
			write!(f, " READONLY")?;
		}

		if let Some(ref backend) = self.backend {
			write!(f, " BACKEND {}", backend)?;
		}

		write!(f, " PERMISSIONS {}", self.permissions)?;

		if let Some(ref comment) = self.comment {
			write!(f, " COMMENT {}", comment)?;
		}

		Ok(())
	}
}

impl From<DefineBucketStatement> for crate::expr::statements::define::DefineBucketStatement {
	fn from(v: DefineBucketStatement) -> Self {
		crate::expr::statements::define::DefineBucketStatement {
			kind: v.kind.into(),
			name: v.name.into(),
			backend: v.backend.map(Into::into),
			permissions: v.permissions.into(),
			readonly: v.readonly,
			comment: v.comment,
		}
	}
}

impl From<crate::expr::statements::define::DefineBucketStatement> for DefineBucketStatement {
	fn from(v: crate::expr::statements::define::DefineBucketStatement) -> Self {
		DefineBucketStatement {
			kind: v.kind.into(),
			name: v.name.into(),
			backend: v.backend.map(Into::into),
			permissions: v.permissions.into(),
			readonly: v.readonly,
			comment: v.comment,
		}
	}
}
