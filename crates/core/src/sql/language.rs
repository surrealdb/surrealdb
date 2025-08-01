use std::fmt;
use std::fmt::Display;

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Language {
	Arabic,
	Danish,
	Dutch,
	English,
	Finnish,
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
			Self::Finnish => "FINNISH",
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

impl From<Language> for crate::expr::language::Language {
	fn from(v: Language) -> Self {
		match v {
			Language::Arabic => Self::Arabic,
			Language::Danish => Self::Danish,
			Language::Dutch => Self::Dutch,
			Language::English => Self::English,
			Language::Finnish => Self::Finnish,
			Language::French => Self::French,
			Language::German => Self::German,
			Language::Greek => Self::Greek,
			Language::Hungarian => Self::Hungarian,
			Language::Italian => Self::Italian,
			Language::Norwegian => Self::Norwegian,
			Language::Portuguese => Self::Portuguese,
			Language::Romanian => Self::Romanian,
			Language::Russian => Self::Russian,
			Language::Spanish => Self::Spanish,
			Language::Swedish => Self::Swedish,
			Language::Tamil => Self::Tamil,
			Language::Turkish => Self::Turkish,
		}
	}
}

impl From<crate::expr::language::Language> for Language {
	fn from(v: crate::expr::language::Language) -> Self {
		match v {
			crate::expr::language::Language::Arabic => Self::Arabic,
			crate::expr::language::Language::Danish => Self::Danish,
			crate::expr::language::Language::Dutch => Self::Dutch,
			crate::expr::language::Language::English => Self::English,
			crate::expr::language::Language::Finnish => Self::Finnish,
			crate::expr::language::Language::French => Self::French,
			crate::expr::language::Language::German => Self::German,
			crate::expr::language::Language::Greek => Self::Greek,
			crate::expr::language::Language::Hungarian => Self::Hungarian,
			crate::expr::language::Language::Italian => Self::Italian,
			crate::expr::language::Language::Norwegian => Self::Norwegian,
			crate::expr::language::Language::Portuguese => Self::Portuguese,
			crate::expr::language::Language::Romanian => Self::Romanian,
			crate::expr::language::Language::Russian => Self::Russian,
			crate::expr::language::Language::Spanish => Self::Spanish,
			crate::expr::language::Language::Swedish => Self::Swedish,
			crate::expr::language::Language::Tamil => Self::Tamil,
			crate::expr::language::Language::Turkish => Self::Turkish,
		}
	}
}
