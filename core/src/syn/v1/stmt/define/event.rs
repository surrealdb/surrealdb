use super::super::super::{
	comment::shouldbespace,
	ending,
	error::{expect_tag_no_case, expected},
	literal::{ident, strand},
	value::{value, values},
	IResult,
};
use crate::sql::{statements::DefineEventStatement, Strand, Value, Values};
use nom::{
	branch::alt,
	bytes::complete::tag_no_case,
	combinator::{cut, opt},
	multi::many0,
	sequence::tuple,
};

pub fn event(i: &str) -> IResult<&str, DefineEventStatement> {
	let (i, _) = tag_no_case("EVENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, (name, what, opts)) = cut(|i| {
		let (i, name) = ident(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, _) = expect_tag_no_case("ON")(i)?;
		let (i, _) = opt(tuple((shouldbespace, tag_no_case("TABLE"))))(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, what) = ident(i)?;
		let (i, opts) = many0(event_opts)(i)?;
		let (i, _) = expected("WHEN, THEN, or COMMENT", ending::query)(i)?;
		Ok((i, (name, what, opts)))
	})(i)?;
	// Create the base statement
	let mut res = DefineEventStatement {
		name,
		what,
		when: Value::Bool(true),
		..Default::default()
	};
	// Assign any defined options
	for opt in opts {
		match opt {
			DefineEventOption::When(v) => {
				res.when = v;
			}
			DefineEventOption::Then(v) => {
				res.then = v;
			}
			DefineEventOption::Comment(v) => {
				res.comment = Some(v);
			}
		}
	}
	// Check necessary options
	if res.then.is_empty() {
		// TODO throw error
	}
	// Return the statement
	Ok((i, res))
}

enum DefineEventOption {
	When(Value),
	Then(Values),
	Comment(Strand),
}

fn event_opts(i: &str) -> IResult<&str, DefineEventOption> {
	alt((event_when, event_then, event_comment))(i)
}

fn event_when(i: &str) -> IResult<&str, DefineEventOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("WHEN")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(value)(i)?;
	Ok((i, DefineEventOption::When(v)))
}

fn event_then(i: &str) -> IResult<&str, DefineEventOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("THEN")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(values)(i)?;
	Ok((i, DefineEventOption::Then(v)))
}

fn event_comment(i: &str) -> IResult<&str, DefineEventOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("COMMENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(strand)(i)?;
	Ok((i, DefineEventOption::Comment(v)))
}
