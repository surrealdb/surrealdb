use super::{
	comment::{comment, mightbespace, shouldbespace},
	operator::{assigner, binary},
	IResult,
};
use nom::{
	branch::alt,
	bytes::complete::{tag, tag_no_case},
	character::complete::{char, multispace1, satisfy},
	combinator::{eof, peek, value},
	sequence::{preceded, tuple},
};

pub fn number(i: &str) -> IResult<&str, ()> {
	peek(alt((
		value((), multispace1), // 1 + 1
		value((), binary),      // 1+1
		value((), assigner),    // 1=1
		value((), comment),     // 1/*comment*/
		value((), char(')')),   // (1)
		value((), char(']')),   // a[1]
		value((), char('}')),   // {k: 1}
		value((), char('"')),   // r"foo:1"
		value((), char('\'')),  // r'foo:1'
		value((), char(';')),   // SET a = 1;
		value((), char(',')),   // [1, 2]
		value((), char('[')),   // thing:1[foo]
		value((), tag("..")),   // thing:1..2
		value(
			(),
			tuple((
				char('.'),
				mightbespace,
				satisfy(|x| x.is_alphanumeric() || x == '$' || x == '*'),
			)),
		), // thing:1.foo
		value((), eof),         // SET a = 1
	)))(i)
}

pub fn ident(i: &str) -> IResult<&str, ()> {
	peek(alt((
		value((), multispace1), // a + 1
		value((), binary),      // a+1
		value((), assigner),    // a+=1
		value((), comment),     // a/*comment*/
		value((), char(')')),   // (a)
		value((), char(']')),   // foo[a]
		value((), char('}')),   // {k: a}
		value((), char(';')),   // SET k = a;
		value((), char(',')),   // [a, b]
		value((), char('.')),   // a.k
		value((), char('…')),   // a…
		value((), char('[')),   // a[0]
		value((), eof),         // SET k = a
	)))(i)
}

/// none, false, etc.
pub fn keyword(i: &str) -> IResult<&str, ()> {
	peek(alt((
		value((), multispace1), // false || true
		value((), binary),      // false||true
		value((), comment),     // false/*comment*/
		value((), char(')')),   // (false)
		value((), char(']')),   // [WHERE k = false]
		value((), char('}')),   // {k: false}
		value((), char(';')),   // SET a = false;
		value((), char(',')),   // [false, true]
		value((), eof),         // SET a = false
	)))(i)
}

pub fn duration(i: &str) -> IResult<&str, ()> {
	peek(alt((
		value((), multispace1),
		value((), binary),
		value((), assigner),
		value((), comment),
		value((), char(')')),
		value((), char(']')),
		value((), char('}')),
		value((), char(';')),
		value((), char(',')),
		value((), char('.')),
		value((), eof),
	)))(i)
}

pub fn field(i: &str) -> IResult<&str, ()> {
	peek(alt((
		value(
			(),
			preceded(
				shouldbespace,
				alt((tag_no_case("FROM"), tag_no_case("TIMEOUT"), tag_no_case("PARALLEL"))),
			),
		),
		value((), char(';')),
		value((), char(')')),
		value((), char('}')),
		value((), eof),
	)))(i)
}

pub fn subquery(i: &str) -> IResult<&str, ()> {
	peek(alt((
		value((), preceded(shouldbespace, tag_no_case("THEN"))),
		value((), preceded(shouldbespace, tag_no_case("ELSE"))),
		value((), preceded(shouldbespace, tag_no_case("END"))),
		|i| {
			let (i, _) = mightbespace(i)?;
			alt((
				value((), eof),
				value((), char(';')),
				value((), char(',')),
				value((), char('}')),
				value((), char(')')),
				value((), char(']')),
			))(i)
		},
	)))(i)
}

pub fn query(i: &str) -> IResult<&str, ()> {
	peek(alt((
		value((), preceded(shouldbespace, tag_no_case("THEN"))),
		value((), preceded(shouldbespace, tag_no_case("ELSE"))),
		value((), preceded(shouldbespace, tag_no_case("END"))),
		|i| {
			let (i, _) = mightbespace(i)?;
			alt((
				value((), eof),
				value((), char(';')),
				value((), char(',')),
				value((), char('}')),
				value((), char(')')),
				value((), char(']')),
			))(i)
		},
	)))(i)
}
