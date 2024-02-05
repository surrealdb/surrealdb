use super::super::{common::commas, IResult};
use crate::sql::Tokenizer;
use nom::{branch::alt, bytes::complete::tag_no_case, combinator::value, multi::separated_list1};

pub fn tokenizer(i: &str) -> IResult<&str, Tokenizer> {
	let (i, t) = alt((
		value(Tokenizer::Blank, tag_no_case("BLANK")),
		value(Tokenizer::Camel, tag_no_case("CAMEL")),
		value(Tokenizer::Class, tag_no_case("CLASS")),
		value(Tokenizer::Punct, tag_no_case("PUNCT")),
	))(i)?;
	Ok((i, t))
}

pub fn tokenizers(i: &str) -> IResult<&str, Vec<Tokenizer>> {
	separated_list1(commas, tokenizer)(i)
}
