use crate::sql::error::IResult;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::map;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[revisioned(revision = 1)]
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

impl Display for Language {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str(match self {
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
		})
	}
}

/// Language supports the english name and also ISO 639-1 (3 characters)
/// and ISO 639-2 (2 characters)
pub(super) fn language(i: &str) -> IResult<&str, Language> {
	alt((
		map(alt((tag_no_case("ARABIC"), tag_no_case("ARA"), tag_no_case("AR"))), |_| {
			Language::Arabic
		}),
		map(alt((tag_no_case("DANISH"), tag_no_case("DAN"), tag_no_case("DA"))), |_| {
			Language::Danish
		}),
		map(alt((tag_no_case("DUTCH"), tag_no_case("NLD"), tag_no_case("NL"))), |_| {
			Language::Dutch
		}),
		map(alt((tag_no_case("ENGLISH"), tag_no_case("ENG"), tag_no_case("EN"))), |_| {
			Language::English
		}),
		map(alt((tag_no_case("FRENCH"), tag_no_case("FRA"), tag_no_case("FR"))), |_| {
			Language::French
		}),
		map(alt((tag_no_case("GERMAN"), tag_no_case("DEU"), tag_no_case("DE"))), |_| {
			Language::German
		}),
		map(alt((tag_no_case("GREEK"), tag_no_case("ELL"), tag_no_case("EL"))), |_| {
			Language::Greek
		}),
		map(alt((tag_no_case("HUNGARIAN"), tag_no_case("HUN"), tag_no_case("HU"))), |_| {
			Language::Hungarian
		}),
		map(alt((tag_no_case("ITALIAN"), tag_no_case("ITA"), tag_no_case("IT"))), |_| {
			Language::Italian
		}),
		map(alt((tag_no_case("NORWEGIAN"), tag_no_case("NOR"), tag_no_case("NO"))), |_| {
			Language::Norwegian
		}),
		map(alt((tag_no_case("PORTUGUESE"), tag_no_case("POR"), tag_no_case("PT"))), |_| {
			Language::Portuguese
		}),
		map(alt((tag_no_case("ROMANIAN"), tag_no_case("RON"), tag_no_case("RO"))), |_| {
			Language::Romanian
		}),
		map(alt((tag_no_case("RUSSIAN"), tag_no_case("RUS"), tag_no_case("RU"))), |_| {
			Language::Russian
		}),
		map(alt((tag_no_case("SPANISH"), tag_no_case("SPA"), tag_no_case("ES"))), |_| {
			Language::Spanish
		}),
		map(alt((tag_no_case("SWEDISH"), tag_no_case("SWE"), tag_no_case("SV"))), |_| {
			Language::Swedish
		}),
		map(alt((tag_no_case("TAMIL"), tag_no_case("TAM"), tag_no_case("TA"))), |_| {
			Language::Tamil
		}),
		map(alt((tag_no_case("TURKISH"), tag_no_case("TUR"), tag_no_case("TR"))), |_| {
			Language::Turkish
		}),
	))(i)
}
