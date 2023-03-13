use crate::sql::comment::shouldbespace;
use crate::sql::common::{closeparenthese, commas, openparenthese};
use crate::sql::error::IResult;
use crate::sql::language::{language, Language};
use crate::sql::number::number;
use crate::sql::Number;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::multi::separated_list1;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum Filter {
	EdgeNgram(Number, Number),
	Lowercase,
	Snowball(Language),
}

impl Display for Filter {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::EdgeNgram(min, max) => write!(f, "EDGENGRAM({},{})", min, max),
			Self::Lowercase => f.write_str("LOWERCASE"),
			Self::Snowball(lang) => write!(f, "SNOWBALL({})", lang),
		}
	}
}

fn edgengram(i: &str) -> IResult<&str, Filter> {
	let (i, _) = tag_no_case("EDGENGRAM")(i)?;
	let (i, _) = openparenthese(i)?;
	let (i, min) = number(i)?;
	let (i, _) = commas(i)?;
	let (i, max) = number(i)?;
	let (i, _) = closeparenthese(i)?;
	Ok((i, Filter::EdgeNgram(min, max)))
}

fn snowball(i: &str) -> IResult<&str, Filter> {
	let (i, _) = tag_no_case("SNOWBALL")(i)?;
	let (i, _) = openparenthese(i)?;
	let (i, language) = language(i)?;
	let (i, _) = closeparenthese(i)?;
	Ok((i, Filter::Snowball(language)))
}

fn lowercase(i: &str) -> IResult<&str, Filter> {
	let (i, _) = tag_no_case("LOWERCASE")(i)?;
	Ok((i, Filter::Lowercase))
}

fn filter(i: &str) -> IResult<&str, Filter> {
	alt((edgengram, lowercase, snowball))(i)
}

pub(super) fn filters(i: &str) -> IResult<&str, Vec<Filter>> {
	let (i, _) = tag_no_case("FILTERS")(i)?;
	let (i, _) = shouldbespace(i)?;
	separated_list1(commas, filter)(i)
}
