use crate::sql::Ident;

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct RemoveAnalyzerStatement {
	pub name: Ident,
	#[revision(start = 2)]
	pub if_exists: bool,
}

impl Display for RemoveAnalyzerStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE ANALYZER")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {}", self.name)?;
		Ok(())
	}
}

impl From<RemoveAnalyzerStatement> for crate::expr::statements::RemoveAnalyzerStatement {
	fn from(v: RemoveAnalyzerStatement) -> Self {
		crate::expr::statements::RemoveAnalyzerStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
		}
	}
}

impl From<crate::expr::statements::RemoveAnalyzerStatement> for RemoveAnalyzerStatement {
	fn from(v: crate::expr::statements::RemoveAnalyzerStatement) -> Self {
		RemoveAnalyzerStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
		}
	}
}
