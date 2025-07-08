use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct ContinueStatement;

impl fmt::Display for ContinueStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("CONTINUE")
	}
}

impl From<ContinueStatement> for crate::expr::statements::ContinueStatement {
	fn from(_v: ContinueStatement) -> Self {
		Self {}
	}
}

impl From<crate::expr::statements::ContinueStatement> for ContinueStatement {
	fn from(_v: crate::expr::statements::ContinueStatement) -> Self {
		Self {}
	}
}
