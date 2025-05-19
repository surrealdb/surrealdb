use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
/// ChangeFeedInclude statements are an appendix
#[non_exhaustive]
pub enum ChangeFeedInclude {
	Original,
}

impl Default for ChangeFeedInclude {
	fn default() -> Self {
		Self::Original
	}
}

crate::sql::impl_display_from_sql!(ChangeFeedInclude);

impl crate::sql::DisplaySql for ChangeFeedInclude {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str(match self {
			Self::Original => "Original",
		})
	}
}
