use crate::sql::language::Language;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Filter {
	Ascii,
	EdgeNgram(u16, u16),
	Lowercase,
	Ngram(u16, u16),
	Snowball(Language),
	Uppercase,
}

impl Display for Filter {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Ascii => f.write_str("ASCII"),
			Self::EdgeNgram(min, max) => write!(f, "EDGENGRAM({},{})", min, max),
			Self::Lowercase => f.write_str("LOWERCASE"),
			Self::Ngram(min, max) => write!(f, "NGRAM({},{})", min, max),
			Self::Snowball(lang) => write!(f, "SNOWBALL({})", lang),
			Self::Uppercase => f.write_str("UPPERCASE"),
		}
	}
}
