use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct CommitStatement;

impl fmt::Display for CommitStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("COMMIT TRANSACTION")
	}
}

impl From<CommitStatement> for crate::expr::statements::commit::CommitStatement {
	fn from(_: CommitStatement) -> Self {
		Self
	}
}

impl From<crate::expr::statements::commit::CommitStatement> for CommitStatement {
	fn from(_: crate::expr::statements::commit::CommitStatement) -> Self {
		Self
	}
}
