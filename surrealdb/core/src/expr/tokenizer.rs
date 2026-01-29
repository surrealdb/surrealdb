use std::fmt;
use std::fmt::Display;

use priority_lfu::DeepSizeOf;
use revision::revisioned;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, DeepSizeOf)]
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
