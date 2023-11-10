use super::{
	super::{
		comment::shouldbespace,
		common::{closeparentheses, commas, expect_delimited, openparentheses},
		error::expected,
		thing::id,
		IResult, ParseError,
	},
	duration::duration,
	ident_raw,
};
use crate::sql::Tokenizer;
use nom::{
	branch::alt,
	bytes::complete::{escaped, escaped_transform, is_not, tag, tag_no_case, take, take_while_m_n},
	character::complete::{anychar, char},
	combinator::{cut, map, map_res, opt, value},
	multi::separated_list1,
	number::complete::recognize_float,
	sequence::{preceded, terminated},
	Err,
};

fn tokenizer(i: &str) -> IResult<&str, Tokenizer> {
	let (i, t) = alt((
		value(Tokenizer::Blank, tag_no_case("BLANK")),
		value(Tokenizer::Camel, tag_no_case("CAMEL")),
		value(Tokenizer::Class, tag_no_case("CLASS")),
		value(Tokenizer::Punct, tag_no_case("PUNCT")),
	))(i)?;
	Ok((i, t))
}

fn tokenizers(i: &str) -> IResult<&str, Vec<Tokenizer>> {
	separated_list1(commas, tokenizer)(i)
}
