use std::fmt::{self, Display, Formatter};

use crate::fmt::EscapeIdent;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RemoveAnalyzerStatement {
	pub name: String,
	pub if_exists: bool,
}

impl Display for RemoveAnalyzerStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE ANALYZER")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {}", EscapeIdent(&self.name))?;
		Ok(())
	}
}

impl From<RemoveAnalyzerStatement> for crate::expr::statements::RemoveAnalyzerStatement {
	fn from(v: RemoveAnalyzerStatement) -> Self {
		crate::expr::statements::RemoveAnalyzerStatement {
			name: v.name,
			if_exists: v.if_exists,
		}
	}
}

impl From<crate::expr::statements::RemoveAnalyzerStatement> for RemoveAnalyzerStatement {
	fn from(v: crate::expr::statements::RemoveAnalyzerStatement) -> Self {
		RemoveAnalyzerStatement {
			name: v.name,
			if_exists: v.if_exists,
		}
	}
}
