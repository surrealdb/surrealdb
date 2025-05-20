use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::sql::statements::define::DefineTableStatement;
use crate::sql::{Base, Ident, Idiom, SqlValue};
use crate::iam::{Action, ResourceKind};
use anyhow::Result;

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use uuid::Uuid;

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct RemoveFieldStatement {
	pub name: Idiom,
	pub what: Ident,
	#[revision(start = 2)]
	pub if_exists: bool,
}

impl Display for RemoveFieldStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE FIELD")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {} ON {}", self.name, self.what)?;
		Ok(())
	}
}


impl From<RemoveFieldStatement> for crate::expr::statements::RemoveFieldStatement {
	fn from(v: RemoveFieldStatement) -> Self {
		crate::expr::statements::RemoveFieldStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			what: v.what.into(),
		}
	}
}

impl From<crate::expr::statements::RemoveFieldStatement> for RemoveFieldStatement {
	fn from(v: crate::expr::statements::RemoveFieldStatement) -> Self {
		RemoveFieldStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			what: v.what.into(),
		}
	}
}