use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::sql::{Base, Ident, SqlValue};
use crate::iam::{Action, ResourceKind};
use anyhow::Result;

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct RemoveParamStatement {
	pub name: Ident,
	#[revision(start = 2)]
	pub if_exists: bool,
}

impl Display for RemoveParamStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE PARAM")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " ${}", self.name)?;
		Ok(())
	}
}

impl From<RemoveParamStatement> for crate::expr::statements::RemoveParamStatement {
	fn from(v: RemoveParamStatement) -> Self {
		crate::expr::statements::RemoveParamStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
		}
	}
}

impl From<crate::expr::statements::RemoveParamStatement> for RemoveParamStatement {
	fn from(v: crate::expr::statements::RemoveParamStatement) -> Self {
		RemoveParamStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
		}
	}
}