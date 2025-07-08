use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Tokenizer {
	Blank,
	Camel,
	Class,
	Punct,
}

impl Display for Tokenizer {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str(match self {
			Self::Blank => "BLANK",
			Self::Camel => "CAMEL",
			Self::Class => "CLASS",
			Self::Punct => "PUNCT",
		})
	}
}

impl From<Tokenizer> for crate::expr::Tokenizer {
	fn from(v: Tokenizer) -> Self {
		match v {
			Tokenizer::Blank => Self::Blank,
			Tokenizer::Camel => Self::Camel,
			Tokenizer::Class => Self::Class,
			Tokenizer::Punct => Self::Punct,
		}
	}
}

impl From<crate::expr::Tokenizer> for Tokenizer {
	fn from(v: crate::expr::Tokenizer) -> Self {
		match v {
			crate::expr::Tokenizer::Blank => Self::Blank,
			crate::expr::Tokenizer::Camel => Self::Camel,
			crate::expr::Tokenizer::Class => Self::Class,
			crate::expr::Tokenizer::Punct => Self::Punct,
		}
	}
}
