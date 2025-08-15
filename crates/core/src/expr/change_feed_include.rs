use std::fmt;

use revision::revisioned;
use serde::{Deserialize, Serialize};

#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
/// ChangeFeedInclude statements are an appendix
pub enum ChangeFeedInclude {
	Original,
}

impl Default for ChangeFeedInclude {
	fn default() -> Self {
		Self::Original
	}
}

impl fmt::Display for ChangeFeedInclude {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str(match self {
			Self::Original => "Original",
		})
	}
}
