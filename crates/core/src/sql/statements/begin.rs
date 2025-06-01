use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct BeginStatement;

impl fmt::Display for BeginStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("BEGIN TRANSACTION")
	}
}

impl From<BeginStatement> for crate::expr::statements::begin::BeginStatement {
	fn from(_: BeginStatement) -> Self {
		Self
	}
}
impl From<crate::expr::statements::begin::BeginStatement> for BeginStatement {
	fn from(_: crate::expr::statements::begin::BeginStatement) -> Self {
		Self
	}
}
