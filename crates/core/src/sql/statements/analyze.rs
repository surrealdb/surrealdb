use crate::sql::ident::Ident;

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Formatter;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum AnalyzeStatement {
	Idx(Ident, Ident),
}

impl From<AnalyzeStatement> for crate::expr::statements::analyze::AnalyzeStatement {
	fn from(value: AnalyzeStatement) -> Self {
		match value {
			AnalyzeStatement::Idx(tb, idx) => Self::Idx(tb.into(), idx.into()),
		}
	}
}

impl From<crate::expr::statements::analyze::AnalyzeStatement> for AnalyzeStatement {
	fn from(value: crate::expr::statements::analyze::AnalyzeStatement) -> Self {
		match value {
			crate::expr::statements::analyze::AnalyzeStatement::Idx(tb, idx) => {
				Self::Idx(tb.into(), idx.into())
			}
		}
	}
}

crate::sql::impl_display_from_sql!(AnalyzeStatement);

impl crate::sql::DisplaySql for AnalyzeStatement {
	fn fmt_sql(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Idx(tb, idx) => write!(f, "ANALYZE INDEX {idx} ON {tb}"),
		}
	}
}
