use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
/// ChangeFeedInclude statements are an appendix
#[non_exhaustive]
#[derive(Default)]
pub enum ChangeFeedInclude {
	#[default]
	Original,
}

impl fmt::Display for ChangeFeedInclude {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str(match self {
			Self::Original => "Original",
		})
	}
}
