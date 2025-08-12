use std::fmt;

use crate::sql::escape::EscapeIdent;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Mock {
	Count(String, u64),
	Range(String, u64, u64),
	// Add new variants here
}

impl fmt::Display for Mock {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Mock::Count(tb, c) => {
				write!(f, "|{}:{}|", EscapeIdent(tb), c)
			}
			Mock::Range(tb, b, e) => {
				write!(f, "|{}:{}..{}|", EscapeIdent(tb), b, e)
			}
		}
	}
}

impl From<Mock> for crate::expr::Mock {
	fn from(v: Mock) -> Self {
		match v {
			Mock::Count(tb, c) => crate::expr::Mock::Count(tb, c),
			Mock::Range(tb, b, e) => crate::expr::Mock::Range(tb, b, e),
		}
	}
}

impl From<crate::expr::Mock> for Mock {
	fn from(v: crate::expr::Mock) -> Self {
		match v {
			crate::expr::Mock::Count(tb, c) => Mock::Count(tb, c),
			crate::expr::Mock::Range(tb, b, e) => Mock::Range(tb, b, e),
		}
	}
}
