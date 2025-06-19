use crate::sql::Expr;
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Version(pub Expr);

impl fmt::Display for Version {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "VERSION {}", self.0)
	}
}

impl From<Version> for crate::expr::Version {
	fn from(v: Version) -> Self {
		Self(v.0.into())
	}
}

impl From<crate::expr::Version> for Version {
	fn from(v: crate::expr::Version) -> Self {
		Self(v.0.into())
	}
}
