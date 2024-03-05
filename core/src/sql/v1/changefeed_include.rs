use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[revisioned(revision = 1)]
/// ChangeFeedInclude statements are an appendix
pub enum ChangeFeedInclude {
	Original,
}

impl Default for crate::sql::changefeed_include::ChangeFeedInclude {
	fn default() -> Self {
		Self::Original
	}
}

impl fmt::Display for crate::sql::changefeed_include::ChangeFeedInclude {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str(match self {
			Self::Original => "Original",
		})
	}
}
