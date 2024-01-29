use super::super::super::{
	comment::shouldbespace,
	ending,
	error::expected,
	literal::{ident, strand},
	part::permission::permission,
	value::value,
	IResult,
};
use crate::sql::{statements::DefineParamStatement, Permission, Strand, Value};
use nom::{
	branch::alt, bytes::complete::tag_no_case, character::complete::char, combinator::cut,
	multi::many0,
};

pub fn param(i: &str) -> IResult<&str, DefineParamStatement> {
	let (i, _) = tag_no_case("PARAM")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = cut(char('$'))(i)?;
	let (i, name) = cut(ident)(i)?;
	let (i, opts) = many0(param_opts)(i)?;
	let (i, _) = expected("VALUE, PERMISSIONS, or COMMENT", ending::query)(i)?;
	// Create the base statement
	let mut res = DefineParamStatement {
		name,
		..Default::default()
	};
	// Assign any defined options
	for opt in opts {
		match opt {
			DefineParamOption::Value(v) => {
				res.value = v;
			}
			DefineParamOption::Comment(v) => {
				res.comment = Some(v);
			}
			DefineParamOption::Permissions(v) => {
				res.permissions = v;
			}
		}
	}
	// Check necessary options
	if res.value.is_none() {
		// TODO throw error
	}
	// Return the statement
	Ok((i, res))
}

enum DefineParamOption {
	Value(Value),
	Comment(Strand),
	Permissions(Permission),
}

fn param_opts(i: &str) -> IResult<&str, DefineParamOption> {
	alt((param_value, param_comment, param_permissions))(i)
}

fn param_value(i: &str) -> IResult<&str, DefineParamOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("VALUE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(value)(i)?;
	Ok((i, DefineParamOption::Value(v)))
}

fn param_comment(i: &str) -> IResult<&str, DefineParamOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("COMMENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(strand)(i)?;
	Ok((i, DefineParamOption::Comment(v)))
}

fn param_permissions(i: &str) -> IResult<&str, DefineParamOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("PERMISSIONS")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(permission)(i)?;
	Ok((i, DefineParamOption::Permissions(v)))
}
