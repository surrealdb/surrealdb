use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::fmt::{is_pretty, pretty_indent};

use crate::sql::{Base, FlowResultExt as _, Ident, Permission, Strand, SqlValue};
use crate::iam::{Action, ResourceKind};
use anyhow::{Result, bail};

use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Write};

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineParamStatement {
	pub name: Ident,
	pub value: SqlValue,
	pub comment: Option<Strand>,
	pub permissions: Permission,
	#[revision(start = 2)]
	pub if_not_exists: bool,
	#[revision(start = 3)]
	pub overwrite: bool,
}

impl Display for DefineParamStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE PARAM")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		if self.overwrite {
			write!(f, " OVERWRITE")?
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
			name: v.name.into(),
			value: v.value.into(),
			comment: v.comment.map(Into::into),
			permissions: v.permissions.into(),
			if_not_exists: v.if_not_exists,
			overwrite: v.overwrite,
		}
	}
}

impl From<crate::expr::statements::DefineParamStatement> for DefineParamStatement {
	fn from(v: crate::expr::statements::DefineParamStatement) -> Self {
		DefineParamStatement {
			name: v.name.into(),
			value: v.value.into(),
			comment: v.comment.map(Into::into),
			permissions: v.permissions.into(),
			if_not_exists: v.if_not_exists,
			overwrite: v.overwrite,
		}
	}
}
