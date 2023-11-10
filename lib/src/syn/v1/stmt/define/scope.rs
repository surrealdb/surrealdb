use super::super::super::{
	block::block,
	comment::{mightbespace, shouldbespace},
	common::{closeparentheses, commas, commasorspace, openparentheses},
	ending,
	error::{expect_tag_no_case, expected, ExplainResultExt},
	idiom::{self, basic, plain},
	literal::{
		datetime, duration, filters, ident, param, scoring, strand, table, tables, timeout,
		tokenizer,
	},
	operator::{assigner, dir},
	part::{
		changefeed, cond, data,
		data::{single, update},
		output,
		permission::permissions,
		view,
	},
	thing::thing,
	value::{value, values, whats},
	IResult,
};
use crate::sql::{
	statements::DefineScopeStatement, Duration, Idioms, Index, Kind, Permissions, Strand, Value,
};
use nom::{
	branch::alt,
	bytes::complete::{escaped, escaped_transform, is_not, tag, tag_no_case, take, take_while_m_n},
	character::complete::{anychar, char, u16, u32},
	combinator::{cut, into, map, map_res, opt, recognize, value as map_value},
	multi::{many0, separated_list1},
	number::complete::recognize_float,
	sequence::{delimited, preceded, terminated, tuple},
	Err,
};

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
