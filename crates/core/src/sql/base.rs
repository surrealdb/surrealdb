use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Base {
	Root,
	Ns,
	Db,
}

impl Default for Base {
	fn default() -> Self {
		Self::Root
	}
}

impl fmt::Display for Base {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Ns => f.write_str("NAMESPACE"),
			Self::Db => f.write_str("DATABASE"),
			Self::Root => f.write_str("ROOT"),
		}
	}
}

impl From<Base> for crate::expr::Base {
	fn from(v: Base) -> Self {
		match v {
			Base::Root => Self::Root,
			Base::Ns => Self::Ns,
			Base::Db => Self::Db,
		}
	}
}

impl From<crate::expr::Base> for Base {
	fn from(v: crate::expr::Base) -> Self {
		match v {
			crate::expr::Base::Root => Self::Root,
			crate::expr::Base::Ns => Self::Ns,
			crate::expr::Base::Db => Self::Db,
		}
	}
}
