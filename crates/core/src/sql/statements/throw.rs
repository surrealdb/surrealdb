use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::{ControlFlow, SqlValue};
use crate::{ctx::Context, sql::FlowResult};

use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct ThrowStatement {
	pub error: SqlValue,
}

impl fmt::Display for ThrowStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "THROW {}", self.error)
	}
}


impl From<ThrowStatement> for crate::expr::statements::ThrowStatement {
	fn from(v: ThrowStatement) -> Self {
		Self {
			error: v.error.into(),
		}
	}
}

impl From<crate::expr::statements::ThrowStatement> for ThrowStatement {
	fn from(v: crate::expr::statements::ThrowStatement) -> Self {
		Self {
			error: v.error.into(),
		}
	}
}