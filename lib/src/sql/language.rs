use crate::sql::error::IResult;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::map;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum Language {
	English,
}

impl Display for Language {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str(match self {
			Self::English => "ENGLISH",
		})
	}
}

pub(super) fn language(i: &str) -> IResult<&str, Language> {
	alt((map(tag_no_case("ENGLISH"), |_| Language::English),))(i)
}
