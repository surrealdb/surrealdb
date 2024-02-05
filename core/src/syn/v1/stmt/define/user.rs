use super::super::super::{
	comment::shouldbespace,
	common::commas,
	ending,
	error::{expect_tag_no_case, expected},
	literal::{ident, strand, strand::strand_raw},
	part::base,
	IResult, ParseError,
};
use crate::{
	iam::Role,
	sql::{statements::DefineUserStatement, Ident, Strand},
};
use nom::{
	branch::alt,
	bytes::complete::tag_no_case,
	combinator::cut,
	multi::{many0, separated_list1},
	Err,
};

pub fn user(i: &str) -> IResult<&str, DefineUserStatement> {
	let (i, _) = tag_no_case("USER")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, (name, base, opts)) = cut(|i| {
		let (i, name) = ident(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, _) = expect_tag_no_case("ON")(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, base) = base(i)?;
		let (i, opts) = user_opts(i)?;
		let (i, _) = expected("PASSWORD, PASSHASH, ROLES, or COMMENT", ending::query)(i)?;
		Ok((i, (name, base, opts)))
	})(i)?;
	// Create the base statement
	let mut res = DefineUserStatement::from_parsed_values(
		name,
		base,
		vec!["Viewer".into()], // New users get the viewer role by default
	);
	// Assign any defined options
	for opt in opts {
		match opt {
			DefineUserOption::Password(v) => {
				res.set_password(&v);
			}
			DefineUserOption::Passhash(v) => {
				res.set_passhash(v);
			}
			DefineUserOption::Roles(v) => {
				res.roles = v;
			}
			DefineUserOption::Comment(v) => {
				res.comment = Some(v);
			}
		}
	}
	// Return the statement
	Ok((i, res))
}

enum DefineUserOption {
	Password(String),
	Passhash(String),
	Roles(Vec<Ident>),
	Comment(Strand),
}

fn user_opts(i: &str) -> IResult<&str, Vec<DefineUserOption>> {
	many0(alt((user_pass, user_hash, user_roles, user_comment)))(i)
}

fn user_pass(i: &str) -> IResult<&str, DefineUserOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("PASSWORD")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(strand_raw)(i)?;
	Ok((i, DefineUserOption::Password(v)))
}

fn user_hash(i: &str) -> IResult<&str, DefineUserOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("PASSHASH")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(strand_raw)(i)?;
	Ok((i, DefineUserOption::Passhash(v)))
}

fn user_comment(i: &str) -> IResult<&str, DefineUserOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("COMMENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(strand)(i)?;
	Ok((i, DefineUserOption::Comment(v)))
}

fn user_roles(i: &str) -> IResult<&str, DefineUserOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("ROLES")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, roles) = separated_list1(commas, |i| {
		let (i, v) = cut(ident)(i)?;
		// Verify the role is valid
		v.as_str().parse::<Role>().map_err(|_| Err::Failure(ParseError::Role(i, v.to_string())))?;

		Ok((i, v))
	})(i)?;

	Ok((i, DefineUserOption::Roles(roles)))
}
