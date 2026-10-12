use std::fmt;
use std::fmt::Display;

use revision::revisioned;
use surrealdb_types::{SqlFormat, ToSql};

use crate::val::stored::schema::Stored;

#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub enum Language {
	#[stored(n = 1)]
	Arabic,
	#[stored(n = 2)]
	Danish,
	#[stored(n = 3)]
	Dutch,
	#[stored(n = 4)]
	English,
	#[stored(n = 5)]
	Finnish,
	#[stored(n = 6)]
	French,
	#[stored(n = 7)]
	German,
	#[stored(n = 8)]
	Greek,
	#[stored(n = 9)]
	Hungarian,
	#[stored(n = 10)]
	Italian,
	#[stored(n = 11)]
	Norwegian,
	#[stored(n = 12)]
	Portuguese,
	#[stored(n = 13)]
	Romanian,
	#[stored(n = 14)]
	Russian,
	#[stored(n = 15)]
	Spanish,
	#[stored(n = 16)]
	Swedish,
	#[stored(n = 17)]
	Tamil,
	#[stored(n = 18)]
	Turkish,
}

impl Language {
	pub fn as_str(self) -> &'static str {
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

impl ToSql for Language {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str(self.as_str())
	}
}
