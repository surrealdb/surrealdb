use std::fmt;

use revision::revisioned;
use serde::{Deserialize, Serialize};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Dir {
	/// `<-`
	In,
	/// `->`
	Out,
	/// `<->`
	Both,
}

impl Default for Dir {
	fn default() -> Self {
		Self::Both
	}
}

impl fmt::Display for Dir {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str(match self {
			Self::In => "<-",
			Self::Out => "->",
			Self::Both => "<->",
		})
	}
}

impl From<Dir> for crate::expr::Dir {
	fn from(v: Dir) -> Self {
		match v {
			Dir::In => Self::In,
			Dir::Out => Self::Out,
			Dir::Both => Self::Both,
		}
	}
}

impl From<crate::expr::Dir> for Dir {
	fn from(v: crate::expr::Dir) -> Self {
		match v {
			crate::expr::Dir::In => Self::In,
			crate::expr::Dir::Out => Self::Out,
			crate::expr::Dir::Both => Self::Both,
		}
	}
}
