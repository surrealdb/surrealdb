use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::Base;
use crate::sql::ident::Ident;
use crate::sql::value::SqlValue;
use crate::iam::{Action, ResourceKind};
use anyhow::Result;

use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Display, Formatter};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum RebuildStatement {
	Index(RebuildIndexStatement),
}

impl Display for RebuildStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Index(v) => Display::fmt(v, f),
		}
	}
}

impl From<RebuildStatement> for crate::expr::statements::rebuild::RebuildStatement {
	fn from(v: RebuildStatement) -> Self {
		match v {
			RebuildStatement::Index(v) => Self::Index(v.into()),
		}
	}
}

impl From<crate::expr::statements::rebuild::RebuildStatement> for RebuildStatement {
	fn from(v: crate::expr::statements::rebuild::RebuildStatement) -> Self {
		match v {
			crate::expr::statements::rebuild::RebuildStatement::Index(v) => Self::Index(v.into()),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct RebuildIndexStatement {
	pub name: Ident,
	pub what: Ident,
	pub if_exists: bool,
}

impl Display for RebuildIndexStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REBUILD INDEX")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {} ON {}", self.name, self.what)?;
		Ok(())
	}
}

impl From<RebuildIndexStatement> for crate::expr::statements::rebuild::RebuildIndexStatement {
	fn from(v: RebuildIndexStatement) -> Self {
		Self {
			name: v.name.into(),
			what: v.what.into(),
			if_exists: v.if_exists,
		}
	}
}

impl From<crate::expr::statements::rebuild::RebuildIndexStatement> for RebuildIndexStatement {
	fn from(v: crate::expr::statements::rebuild::RebuildIndexStatement) -> Self {
		Self {
			name: v.name.into(),
			what: v.what.into(),
			if_exists: v.if_exists,
		}
	}
}