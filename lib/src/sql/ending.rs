use crate::sql::comment::comment;
use crate::sql::comment::{mightbespace, shouldbespace};
use crate::sql::error::IResult;
use crate::sql::operator::{assigner, operator};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::char;
use nom::character::complete::multispace1;
use nom::combinator::eof;
use nom::combinator::map;
use nom::combinator::peek;
use nom::sequence::preceded;

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
		map(char('.'), |_| ()),
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

pub fn field(i: &str) -> IResult<&str, ()> {
	peek(alt((
		map(preceded(shouldbespace, tag_no_case("FROM")), |_| ()),
		map(preceded(comment, tag_no_case("FROM")), |_| ()),
		map(char(';'), |_| ()),
		map(eof, |_| ()),
	)))(i)
}

pub fn subquery(i: &str) -> IResult<&str, ()> {
	alt((
		|i| {
			let (i, _) = mightbespace(i)?;
			let (i, _) = char(';')(i)?;
			let (i, _) = peek(alt((
				preceded(shouldbespace, tag_no_case("THEN")),
				preceded(shouldbespace, tag_no_case("ELSE")),
				preceded(shouldbespace, tag_no_case("END")),
			)))(i)?;
			Ok((i, ()))
		},
		peek(alt((
			map(preceded(shouldbespace, tag_no_case("THEN")), |_| ()),
			map(preceded(shouldbespace, tag_no_case("ELSE")), |_| ()),
			map(preceded(shouldbespace, tag_no_case("END")), |_| ()),
			map(comment, |_| ()),
			map(char(']'), |_| ()),
			map(char('}'), |_| ()),
			map(char(';'), |_| ()),
			map(char(','), |_| ()),
			map(eof, |_| ()),
		))),
	))(i)
}
