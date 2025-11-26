use std::fmt;
use std::ops::Bound;

use crate::fmt::EscapeKwFreeIdent;
use crate::val::range::TypedRange;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Mock {
	Count(String, i64),
	Range(String, TypedRange<i64>),
	// Add new variants here
}

impl fmt::Display for Mock {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Mock::Count(tb, c) => {
				write!(f, "|{}:{}|", EscapeKwFreeIdent(tb), c)
			}
			Mock::Range(tb, r) => {
				write!(f, "|{}:", EscapeKwFreeIdent(tb))?;
				match r.start {
					Bound::Included(x) => write!(f, "{x}..")?,
					Bound::Excluded(x) => write!(f, "{x}>..")?,
					Bound::Unbounded => write!(f, "..")?,
				}
				match r.end {
					Bound::Included(x) => write!(f, "={x}|"),
					Bound::Excluded(x) => write!(f, "{x}|"),
					Bound::Unbounded => write!(f, "|"),
				}
			}
		}
	}
}

impl From<Mock> for crate::expr::Mock {
	fn from(v: Mock) -> Self {
		match v {
			Mock::Count(tb, c) => crate::expr::Mock::Count(tb, c),
			Mock::Range(tb, r) => crate::expr::Mock::Range(tb, r),
		}
	}
}

impl From<crate::expr::Mock> for Mock {
	fn from(v: crate::expr::Mock) -> Self {
		match v {
			crate::expr::Mock::Count(tb, c) => Mock::Count(tb, c),
			crate::expr::Mock::Range(tb, r) => Mock::Range(tb, r),
		}
	}
}
