use crate::sql::comment::shouldbespace;
use crate::sql::common::{closeparentheses, commas, openparentheses};
use crate::sql::error::IResult;
use crate::sql::language::{language, Language};
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::u16;
use nom::multi::separated_list1;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum Filter {
	Ascii,
	EdgeNgram(u16, u16),
	Lowercase,
	Ngram(u16, u16),
	Snowball(Language),
	Uppercase,
}

impl Display for Filter {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Ascii => f.write_str("ASCII"),
			Self::EdgeNgram(min, max) => write!(f, "EDGENGRAM({},{})", min, max),
			Self::Lowercase => f.write_str("LOWERCASE"),
			Self::Ngram(min, max) => write!(f, "NGRAM({},{})", min, max),
			Self::Snowball(lang) => write!(f, "SNOWBALL({})", lang),
			Self::Uppercase => f.write_str("UPPERCASE"),
		}
	}
}

fn ascii(i: &str) -> IResult<&str, Filter> {
	let (i, _) = tag_no_case("ASCII")(i)?;
	Ok((i, Filter::Ascii))
}

fn edgengram(i: &str) -> IResult<&str, Filter> {
	let (i, _) = tag_no_case("EDGENGRAM")(i)?;
	let (i, _) = openparentheses(i)?;
	let (i, min) = u16(i)?;
	let (i, _) = commas(i)?;
	let (i, max) = u16(i)?;
	let (i, _) = closeparentheses(i)?;
	Ok((i, Filter::EdgeNgram(min, max)))
}

fn ngram(i: &str) -> IResult<&str, Filter> {
	let (i, _) = tag_no_case("NGRAM")(i)?;
	let (i, _) = openparentheses(i)?;
	let (i, min) = u16(i)?;
	let (i, _) = commas(i)?;
	let (i, max) = u16(i)?;
	let (i, _) = closeparentheses(i)?;
	Ok((i, Filter::Ngram(min, max)))
}

fn lowercase(i: &str) -> IResult<&str, Filter> {
	let (i, _) = tag_no_case("LOWERCASE")(i)?;
	Ok((i, Filter::Lowercase))
}

fn snowball(i: &str) -> IResult<&str, Filter> {
	let (i, _) = tag_no_case("SNOWBALL")(i)?;
	let (i, _) = openparentheses(i)?;
	let (i, language) = language(i)?;
	let (i, _) = closeparentheses(i)?;
	Ok((i, Filter::Snowball(language)))
}

fn uppercase(i: &str) -> IResult<&str, Filter> {
	let (i, _) = tag_no_case("UPPERCASE")(i)?;
	Ok((i, Filter::Uppercase))
}

fn filter(i: &str) -> IResult<&str, Filter> {
	alt((ascii, edgengram, lowercase, ngram, snowball, uppercase))(i)
}

pub(super) fn filters(i: &str) -> IResult<&str, Vec<Filter>> {
	let (i, _) = tag_no_case("FILTERS")(i)?;
	let (i, _) = shouldbespace(i)?;
	separated_list1(commas, filter)(i)
}
