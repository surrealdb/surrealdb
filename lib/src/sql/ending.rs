use crate::sql::comment::comment;
use crate::sql::comment::{mightbespace, shouldbespace};
use crate::sql::error::IResult;
use crate::sql::operator::{assigner, binary};
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
		map(multispace1, |_| ()), // 1 + 1
		map(binary, |_| ()),      // 1+1
		map(assigner, |_| ()),    // 1=1
		map(comment, |_| ()),     // 1/*comment*/
		map(char(')'), |_| ()),   // (1)
		map(char(']'), |_| ()),   // a[1]
		map(char('}'), |_| ()),   // {k: 1}
		map(char('"'), |_| ()),
		map(char('\''), |_| ()),
		map(char(';'), |_| ()), // SET a = 1;
		map(char(','), |_| ()), // [1, 2]
		map(tag(".."), |_| ()), // thing:1..2
		map(eof, |_| ()),       // SET a = 1
	)))(i)
}

pub fn ident(i: &str) -> IResult<&str, ()> {
	peek(alt((
		map(multispace1, |_| ()), // a + 1
		map(binary, |_| ()),      // a+1
		map(assigner, |_| ()),    // a+=1
		map(comment, |_| ()),     // a/*comment*/
		map(char(')'), |_| ()),   // (a)
		map(char(']'), |_| ()),   // foo[a]
		map(char('}'), |_| ()),   // {k: a}
		map(char(';'), |_| ()),   // SET k = a;
		map(char(','), |_| ()),   // [a, b]
		map(char('.'), |_| ()),   // a.k
		map(char('…'), |_| ()),   // a…
		map(char('['), |_| ()),   // a[0]
		map(eof, |_| ()),         // SET k = a
	)))(i)
}

/// none, false, etc.
pub fn keyword(i: &str) -> IResult<&str, ()> {
	peek(alt((
		map(multispace1, |_| ()), // false || true
		map(binary, |_| ()),      // false||true
		map(comment, |_| ()),     // false/*comment*/
		map(char(')'), |_| ()),   // (false)
		map(char(']'), |_| ()),   // [WHERE k = false]
		map(char('}'), |_| ()),   // {k: false}
		map(char(';'), |_| ()),   // SET a = false;
		map(char(','), |_| ()),   // [false, true]
		map(eof, |_| ()),         // SET a = false
	)))(i)
}

pub fn duration(i: &str) -> IResult<&str, ()> {
	peek(alt((
		map(multispace1, |_| ()),
		map(binary, |_| ()),
		map(assigner, |_| ()),
		map(comment, |_| ()),
		map(char(')'), |_| ()),
		map(char(']'), |_| ()),
		map(char('}'), |_| ()),
		map(char(';'), |_| ()),
		map(char(','), |_| ()),
		map(char('.'), |_| ()),
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
