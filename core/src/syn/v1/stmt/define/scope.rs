use super::super::super::{
	comment::shouldbespace,
	ending,
	error::expected,
	literal::{duration, ident, strand},
	value::value,
	IResult,
};
use crate::sql::{statements::DefineScopeStatement, Duration, Strand, Value};
use nom::{branch::alt, bytes::complete::tag_no_case, combinator::cut, multi::many0};

pub fn scope(i: &str) -> IResult<&str, DefineScopeStatement> {
	let (i, _) = tag_no_case("SCOPE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = cut(ident)(i)?;
	let (i, opts) = many0(scope_opts)(i)?;
	let (i, _) = expected("SESSION, SIGNUP, SIGNIN, or COMMENT", ending::query)(i)?;
	// Create the base statement
	let mut res = DefineScopeStatement {
		name,
		code: DefineScopeStatement::random_code(),
		..Default::default()
	};
	// Assign any defined options
	for opt in opts {
		match opt {
			DefineScopeOption::Session(v) => {
				res.session = Some(v);
			}
			DefineScopeOption::Signup(v) => {
				res.signup = Some(v);
			}
			DefineScopeOption::Signin(v) => {
				res.signin = Some(v);
			}
			DefineScopeOption::Comment(v) => {
				res.comment = Some(v);
			}
		}
	}
	// Return the statement
	Ok((i, res))
}

enum DefineScopeOption {
	Session(Duration),
	Signup(Value),
	Signin(Value),
	Comment(Strand),
}

fn scope_opts(i: &str) -> IResult<&str, DefineScopeOption> {
	alt((scope_session, scope_signup, scope_signin, scope_comment))(i)
}

fn scope_session(i: &str) -> IResult<&str, DefineScopeOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("SESSION")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(duration)(i)?;
	Ok((i, DefineScopeOption::Session(v)))
}

fn scope_signup(i: &str) -> IResult<&str, DefineScopeOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("SIGNUP")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(value)(i)?;
	Ok((i, DefineScopeOption::Signup(v)))
}

fn scope_signin(i: &str) -> IResult<&str, DefineScopeOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("SIGNIN")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(value)(i)?;
	Ok((i, DefineScopeOption::Signin(v)))
}

fn scope_comment(i: &str) -> IResult<&str, DefineScopeOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("COMMENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(strand)(i)?;
	Ok((i, DefineScopeOption::Comment(v)))
}
