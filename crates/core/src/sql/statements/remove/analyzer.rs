use std::fmt::{self, Display, Formatter};

use crate::{
	fmt::CoverStmts,
	sql::{Expr, Literal},
};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct RemoveAnalyzerStatement {
	pub name: Expr,
	pub if_exists: bool,
}

impl Default for RemoveAnalyzerStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			if_exists: false,
		}
	}
}

impl Display for RemoveAnalyzerStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE ANALYZER")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {}", CoverStmts(&self.name))?;
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
