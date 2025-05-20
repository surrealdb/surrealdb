use crate::sql::{Ident, Permission, SqlValue, Strand};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};


#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineBucketStatement {
	pub if_not_exists: bool,
	pub overwrite: bool,
	pub name: Ident,
	pub backend: Option<SqlValue>,
	pub permissions: Permission,
	pub readonly: bool,
	pub comment: Option<Strand>,
}

impl Display for DefineBucketStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE BUCKET")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		if self.overwrite {
			write!(f, " OVERWRITE")?
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
			if_not_exists: v.if_not_exists,
			overwrite: v.overwrite,
			name: v.name.into(),
			backend: v.backend.map(Into::into),
			permissions: v.permissions.into(),
			readonly: v.readonly,
			comment: v.comment.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::define::DefineBucketStatement> for DefineBucketStatement {
	fn from(v: crate::expr::statements::define::DefineBucketStatement) -> Self {
		DefineBucketStatement {
			if_not_exists: v.if_not_exists,
			overwrite: v.overwrite,
			name: v.name.into(),
			backend: v.backend.map(Into::into),
			permissions: v.permissions.into(),
			readonly: v.readonly,
			comment: v.comment.map(Into::into),
		}
	}
}

// Computed bucket definition struct

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[non_exhaustive]
pub struct BucketDefinition {
	pub id: Option<u32>,
	pub name: Ident,
	pub backend: Option<String>,
	pub permissions: Permission,
	pub readonly: bool,
	pub comment: Option<Strand>,
}

impl From<BucketDefinition> for DefineBucketStatement {
	fn from(value: BucketDefinition) -> Self {
		DefineBucketStatement {
			if_not_exists: false,
			overwrite: false,
			name: value.name,
			backend: value.backend.map(|v| v.into()),
			permissions: value.permissions,
			readonly: value.readonly,
			comment: value.comment,
		}
	}
}

impl Display for BucketDefinition {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let db: DefineBucketStatement = self.clone().into();
		db.fmt(f)
	}
}
