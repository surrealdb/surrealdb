use crate::sql::comment::shouldbespace;
use crate::sql::common::commas;
use crate::sql::error::IResult;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::map;
use nom::multi::separated_list1;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum Tokenizer {
	Case,
	Space,
}

impl Display for Tokenizer {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str(match self {
			Self::Case => "CASE",
			Self::Space => "SPACE",
		})
	}
}

fn tokenizer(i: &str) -> IResult<&str, Tokenizer> {
	let (i, t) = alt((
		map(tag_no_case("CASE"), |_| Tokenizer::Case),
		map(tag_no_case("SPACE"), |_| Tokenizer::Space),
	))(i)?;
	Ok((i, t))
}

pub(super) fn tokenizers(i: &str) -> IResult<&str, Vec<Tokenizer>> {
	let (i, _) = tag_no_case("TOKENIZERS")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, t) = separated_list1(commas, tokenizer)(i)?;
	Ok((i, t))
}
