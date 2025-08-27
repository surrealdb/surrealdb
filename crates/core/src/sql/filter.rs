use std::fmt;
use std::fmt::Display;

use crate::sql::language::Language;

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Filter {
	Ascii,
	EdgeNgram(u16, u16),
	Lowercase,
	Ngram(u16, u16),
	Snowball(Language),
	Uppercase,
	Mapper(String),
}

impl Display for Filter {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Ascii => f.write_str("ASCII"),
			Self::EdgeNgram(min, max) => write!(f, "EDGENGRAM({min},{max})"),
			Self::Lowercase => f.write_str("LOWERCASE"),
			Self::Ngram(min, max) => write!(f, "NGRAM({min},{max})"),
			Self::Snowball(lang) => write!(f, "SNOWBALL({lang})"),
			Self::Uppercase => f.write_str("UPPERCASE"),
			Self::Mapper(path) => write!(f, "MAPPER({path})"),
		}
	}
}

impl From<Filter> for crate::expr::Filter {
	fn from(v: Filter) -> Self {
		match v {
			Filter::Ascii => Self::Ascii,
			Filter::EdgeNgram(min, max) => Self::EdgeNgram(min, max),
			Filter::Lowercase => Self::Lowercase,
			Filter::Ngram(min, max) => Self::Ngram(min, max),
			Filter::Snowball(lang) => Self::Snowball(lang.into()),
			Filter::Uppercase => Self::Uppercase,
			Filter::Mapper(path) => Self::Mapper(path),
		}
	}
}

impl From<crate::expr::Filter> for Filter {
	fn from(v: crate::expr::Filter) -> Self {
		match v {
			crate::expr::Filter::Ascii => Self::Ascii,
			crate::expr::Filter::EdgeNgram(min, max) => Self::EdgeNgram(min, max),
			crate::expr::Filter::Lowercase => Self::Lowercase,
			crate::expr::Filter::Ngram(min, max) => Self::Ngram(min, max),
			crate::expr::Filter::Snowball(lang) => Self::Snowball(lang.into()),
			crate::expr::Filter::Uppercase => Self::Uppercase,
			crate::expr::Filter::Mapper(path) => Self::Mapper(path),
		}
	}
}
