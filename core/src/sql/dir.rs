use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Dir {
	In,
	Out,
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
