use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[revisioned(revision = 1)]
pub enum ChangeFeedInclude {
	Original,
}

impl Default for ChangeFeedInclude {
	fn default() -> Self {
		Self::Hs512
	}
}

impl fmt::Display for ChangeFeedInclude {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str(match self {
			Self::Original => "Original",
		})
	}
}
