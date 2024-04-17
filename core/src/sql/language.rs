use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Language {
	Arabic,
	Danish,
	Dutch,
	English,
	French,
	German,
	Greek,
	Hungarian,
	Italian,
	Norwegian,
	Portuguese,
	Romanian,
	Russian,
	Spanish,
	Swedish,
	Tamil,
	Turkish,
}

impl Language {
	pub fn as_str(&self) -> &'static str {
		match self {
			Self::Arabic => "ARABIC",
			Self::Danish => "DANISH",
			Self::Dutch => "DUTCH",
			Self::English => "ENGLISH",
			Self::French => "FRENCH",
			Self::German => "GERMAN",
			Self::Greek => "GREEK",
			Self::Hungarian => "HUNGARIAN",
			Self::Italian => "ITALIAN",
			Self::Norwegian => "NORWEGIAN",
			Self::Portuguese => "PORTUGUESE",
			Self::Romanian => "ROMANIAN",
			Self::Russian => "RUSSIAN",
			Self::Spanish => "SPANISH",
			Self::Swedish => "SWEDISH",
			Self::Tamil => "TAMIL",
			Self::Turkish => "TURKISH",
		}
	}
}

impl Display for Language {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str(self.as_str())
	}
}
