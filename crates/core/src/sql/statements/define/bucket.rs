use crate::buc::{self, BucketConnectionKey};
use crate::dbs::Options;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::{Base, FlowResultExt, Ident, Permission, Strand, Value};
use crate::{ctx::Context, sql::statements::info::InfoStructure};
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

use super::CursorDoc;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineBucketStatement {
	pub if_not_exists: bool,
	pub overwrite: bool,
	pub name: Ident,
	pub backend: Option<Value>,
	pub permissions: Permission,
	pub readonly: bool,
	pub comment: Option<Strand>,
}

crate::sql::impl_display_from_sql!(DefineBucketStatement);

impl crate::sql::DisplaySql for DefineBucketStatement {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
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

impl InfoStructure for DefineBucketStatement {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.structure(),
			"permissions".to_string() => self.permissions.structure(),
			"backend".to_string(), if let Some(backend) = self.backend => backend,
			"readonly".to_string() => self.readonly.into(),
			"comment".to_string(), if let Some(comment) = self.comment => comment.into(),
		})
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

impl InfoStructure for BucketDefinition {
	fn structure(self) -> Value {
		let db: DefineBucketStatement = self.into();
		db.structure()
	}
}

crate::sql::impl_display_from_sql!(BucketDefinition);

impl crate::sql::DisplaySql for BucketDefinition {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let db: DefineBucketStatement = self.clone().into();
		db.fmt(f)
	}
}
