use super::super::super::{
	block::block,
	comment::{mightbespace, shouldbespace},
	common::{closeparentheses, commas, delimited_list0, openparentheses},
	ending,
	error::expected,
	kind::kind,
	literal::{ident, ident_path, strand},
	part::permission::permission,
	IResult,
};
use crate::sql::{statements::DefineFunctionStatement, Permission, Strand};
use nom::{
	branch::alt,
	bytes::complete::{tag, tag_no_case},
	character::complete::char,
	combinator::cut,
	multi::many0,
};

pub fn function(i: &str) -> IResult<&str, DefineFunctionStatement> {
	let (i, _) = tag_no_case("FUNCTION")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag("fn::")(i)?;
	let (i, name) = ident_path(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, args) = delimited_list0(
		openparentheses,
		commas,
		|i| {
			let (i, _) = char('$')(i)?;
			let (i, name) = ident(i)?;
			let (i, _) = mightbespace(i)?;
			let (i, _) = char(':')(i)?;
			let (i, _) = mightbespace(i)?;
			let (i, kind) = kind(i)?;
			Ok((i, (name, kind)))
		},
		closeparentheses,
	)(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, block) = block(i)?;
	let (i, opts) = many0(function_opts)(i)?;
	let (i, _) = expected("PERMISSIONS or COMMENT", ending::query)(i)?;
	// Create the base statement
	let mut res = DefineFunctionStatement {
		name,
		args,
		block,
		..Default::default()
	};
	// Assign any defined options
	for opt in opts {
		match opt {
			DefineFunctionOption::Comment(v) => {
				res.comment = Some(v);
			}
			DefineFunctionOption::Permissions(v) => {
				res.permissions = v;
			}
		}
	}
	// Return the statement
	Ok((i, res))
}

enum DefineFunctionOption {
	Comment(Strand),
	Permissions(Permission),
}

fn function_opts(i: &str) -> IResult<&str, DefineFunctionOption> {
	alt((function_comment, function_permissions))(i)
}

fn function_comment(i: &str) -> IResult<&str, DefineFunctionOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("COMMENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(strand)(i)?;
	Ok((i, DefineFunctionOption::Comment(v)))
}

fn function_permissions(i: &str) -> IResult<&str, DefineFunctionOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("PERMISSIONS")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(permission)(i)?;
	Ok((i, DefineFunctionOption::Permissions(v)))
}
