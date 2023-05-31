use crate::sql::error::IResult;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::map;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
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

pub(super) fn language(i: &str) -> IResult<&str, Language> {
	alt((
		map(tag_no_case("ARABIC"), |_| Language::Arabic),
		map(tag_no_case("DANISH"), |_| Language::Danish),
		map(tag_no_case("DUTCH"), |_| Language::Dutch),
		map(tag_no_case("ENGLISH"), |_| Language::English),
		map(tag_no_case("FRENCH"), |_| Language::French),
		map(tag_no_case("GERMAN"), |_| Language::German),
		map(tag_no_case("GREEK"), |_| Language::Greek),
		map(tag_no_case("HUNGARIAN"), |_| Language::Hungarian),
		map(tag_no_case("ITALIAN"), |_| Language::Italian),
		map(tag_no_case("NORWEGIAN"), |_| Language::Norwegian),
		map(tag_no_case("PORTUGUESE"), |_| Language::Portuguese),
		map(tag_no_case("ROMANIAN"), |_| Language::Romanian),
		map(tag_no_case("RUSSIAN"), |_| Language::Russian),
		map(tag_no_case("SPANISH"), |_| Language::Spanish),
		map(tag_no_case("SWEDISH"), |_| Language::Swedish),
		map(tag_no_case("TAMIL"), |_| Language::Tamil),
		map(tag_no_case("TURKISH"), |_| Language::Turkish),
	))(i)
}
