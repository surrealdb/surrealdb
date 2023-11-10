use super::super::super::{
	block::block,
	comment::{mightbespace, shouldbespace},
	common::{closeparentheses, commas, commasorspace, delimited_list0, openparentheses},
	ending,
	error::{expect_tag_no_case, expected, ExplainResultExt},
	idiom::{self, basic, plain},
	kind::kind,
	literal::{
		datetime, duration, filters, ident, param, scoring, strand, table, tables, timeout,
		tokenizer,
	},
	operator::{assigner, dir},
	part::{
		changefeed, cond, data,
		data::{single, update},
		output,
		permission::permission,
	},
	thing::thing,
	value::{value, values, whats},
	IResult,
};
use crate::sql::{
	filter::Filter, statements::DefineFunctionStatement, ChangeFeed, Permission, Strand, Tokenizer,
	Value,
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

pub fn function(i: &str) -> IResult<&str, DefineFunctionStatement> {
	let (i, _) = tag_no_case("FUNCTION")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag("fn::")(i)?;
	let (i, name) = ident::multi(i)?;
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
