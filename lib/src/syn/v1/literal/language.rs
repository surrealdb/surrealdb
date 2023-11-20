use nom::{branch::alt, bytes::complete::tag_no_case, combinator::map};

use crate::sql::language::Language;

use super::super::IResult;

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
