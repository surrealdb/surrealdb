use crate::sql::comment::comment;
use crate::sql::error::IResult;
use crate::sql::operator::{assigner, operator};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::char;
use nom::character::complete::multispace1;
use nom::combinator::eof;
use nom::combinator::map;
use nom::combinator::peek;

pub fn number(i: &str) -> IResult<&str, ()> {
	peek(alt((
		map(multispace1, |_| ()),
		map(operator, |_| ()),
		map(assigner, |_| ()),
		map(comment, |_| ()),
		map(char(')'), |_| ()),
		map(char(']'), |_| ()),
		map(char('}'), |_| ()),
		map(char('"'), |_| ()),
		map(char('\''), |_| ()),
		map(char(';'), |_| ()),
		map(char(','), |_| ()),
		map(tag(".."), |_| ()),
		map(eof, |_| ()),
	)))(i)
}

pub fn ident(i: &str) -> IResult<&str, ()> {
	peek(alt((
		map(multispace1, |_| ()),
		map(operator, |_| ()),
		map(assigner, |_| ()),
		map(comment, |_| ()),
		map(char(')'), |_| ()),
		map(char(']'), |_| ()),
		map(char('}'), |_| ()),
		map(char(';'), |_| ()),
		map(char(','), |_| ()),
		map(char('.'), |_| ()),
		map(char('['), |_| ()),
		map(char('-'), |_| ()),
		map(eof, |_| ()),
	)))(i)
}

pub fn duration(i: &str) -> IResult<&str, ()> {
	peek(alt((
		map(multispace1, |_| ()),
		map(operator, |_| ()),
		map(assigner, |_| ()),
		map(comment, |_| ()),
		map(char(')'), |_| ()),
		map(char(']'), |_| ()),
		map(char('}'), |_| ()),
		map(char(';'), |_| ()),
		map(char(','), |_| ()),
		map(char('.'), |_| ()),
		map(char('-'), |_| ()),
		map(eof, |_| ()),
	)))(i)
}
