use std::fmt;

/// ChangeFeedInclude statements are an appendix
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum ChangeFeedInclude {
	Original,
}

impl fmt::Display for ChangeFeedInclude {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str(match self {
			Self::Original => "Original",
		})
	}
}
