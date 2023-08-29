use crate::sql::common::commas;
use crate::sql::error::IResult;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::value;
use nom::multi::separated_list1;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[revisioned(revision = 1)]
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
		value(Tokenizer::Blank, tag_no_case("BLANK")),
		value(Tokenizer::Camel, tag_no_case("CAMEL")),
		value(Tokenizer::Class, tag_no_case("CLASS")),
		value(Tokenizer::Punct, tag_no_case("PUNCT")),
	))(i)?;
	Ok((i, t))
}

pub(super) fn tokenizers(i: &str) -> IResult<&str, Vec<Tokenizer>> {
	separated_list1(commas, tokenizer)(i)
}
