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
	Blank,
	Camel,
	Class,
	Punct,
}

impl Display for Tokenizer {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str(match self {
			Self::Blank => "BLANK",
			Self::Camel => "CAMEL",
			Self::Class => "CLASS",
			Self::Punct => "PUNCT",
		})
	}
}

fn tokenizer(i: &str) -> IResult<&str, Tokenizer> {
	let (i, t) = alt((
		map(tag_no_case("BLANK"), |_| Tokenizer::Blank),
		map(tag_no_case("CAMEL"), |_| Tokenizer::Camel),
		map(tag_no_case("CLASS"), |_| Tokenizer::Class),
		map(tag_no_case("PUNCT"), |_| Tokenizer::Punct),
	))(i)?;
	Ok((i, t))
}

pub(super) fn tokenizers(i: &str) -> IResult<&str, Vec<Tokenizer>> {
	let (i, _) = tag_no_case("TOKENIZERS")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, t) = separated_list1(commas, tokenizer)(i)?;
	Ok((i, t))
}
