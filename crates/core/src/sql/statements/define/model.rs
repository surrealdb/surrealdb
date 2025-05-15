use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::fmt::{is_pretty, pretty_indent};
use crate::sql::statements::info::InfoStructure;
use crate::sql::{Base, Ident, Permission, Strand, Value};

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Write};

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineModelStatement {
	pub hash: String,
	pub name: Ident,
	pub version: String,
	pub comment: Option<Strand>,
	pub permissions: Permission,
	#[revision(start = 2)]
	pub if_not_exists: bool,
	#[revision(start = 3)]
	pub overwrite: bool,
}

impl From<DefineModelStatement> for crate::expr::statements::DefineModelStatement {
	fn from(v: DefineModelStatement) -> Self {
		Self {
			hash: v.hash,
			name: v.name.into(),
			version: v.version,
			comment: v.comment.map(Into::into),
			permissions: v.permissions.into(),
			if_not_exists: v.if_not_exists,
			overwrite: v.overwrite,
		}
	}
}

impl From<crate::expr::statements::DefineModelStatement> for DefineModelStatement {
	fn from(v: crate::expr::statements::DefineModelStatement) -> Self {
		Self {
			hash: v.hash,
			name: v.name.into(),
			version: v.version,
			comment: v.comment.map(Into::into),
			permissions: v.permissions.into(),
			if_not_exists: v.if_not_exists,
			overwrite: v.overwrite,
		}
	}
}

crate::sql::impl_display_from_sql!(DefineModelStatement);

impl crate::sql::DisplaySql for DefineModelStatement {
	fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "DEFINE MODEL")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		if self.overwrite {
			write!(f, " OVERWRITE")?
		}
		write!(f, " ml::{}<{}>", self.name, self.version)?;
		if let Some(comment) = self.comment.as_ref() {
			write!(f, " COMMENT {}", comment)?;
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

impl InfoStructure for DefineModelStatement {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.structure(),
			"version".to_string() => self.version.into(),
			"permissions".to_string() => self.permissions.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}
