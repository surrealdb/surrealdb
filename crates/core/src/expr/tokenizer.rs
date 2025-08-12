use std::fmt;
use std::fmt::Display;

use revision::revisioned;
use serde::{Deserialize, Serialize};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
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
