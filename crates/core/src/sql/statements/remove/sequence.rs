use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::sql::{Base, Ident, SqlValue};
use crate::iam::{Action, ResourceKind};
use anyhow::Result;

use crate::key::database::sq::Sq;
use crate::key::sequence::Prefix;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct RemoveSequenceStatement {
	pub name: Ident,
	pub if_exists: bool,
}

impl Display for RemoveSequenceStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE SEQUENCE")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {}", self.name)?;
		Ok(())
	}
}

impl From<RemoveSequenceStatement> for crate::expr::statements::remove::RemoveSequenceStatement {
	fn from(v: RemoveSequenceStatement) -> Self {
		crate::expr::statements::remove::RemoveSequenceStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
		}
	}
}

impl From<crate::expr::statements::remove::RemoveSequenceStatement> for RemoveSequenceStatement {
	fn from(v: crate::expr::statements::remove::RemoveSequenceStatement) -> Self {
		RemoveSequenceStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
		}
	}
}