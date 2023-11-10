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
		cond, data,
		data::{single, update},
		output,
		permission::permissions,
	},
	thing::thing,
	value::{value, values, whats},
	IResult,
};
use crate::sql::{
	statements::DefineNamespaceStatement, Idioms, Index, Kind, Permissions, Strand, Value,
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

pub fn namespace(i: &str) -> IResult<&str, DefineNamespaceStatement> {
	let (i, _) = alt((tag_no_case("NS"), tag_no_case("NAMESPACE")))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = cut(ident)(i)?;
	let (i, opts) = many0(namespace_opts)(i)?;
	let (i, _) = expected("COMMENT", ending::query)(i)?;
	// Create the base statement
	let mut res = DefineNamespaceStatement {
		name,
		..Default::default()
	};
	// Assign any defined options
	for opt in opts {
		match opt {
			DefineNamespaceOption::Comment(v) => {
				res.comment = Some(v);
			}
		}
	}
	// Return the statement
	Ok((i, res))
}

enum DefineNamespaceOption {
	Comment(Strand),
}

fn namespace_opts(i: &str) -> IResult<&str, DefineNamespaceOption> {
	namespace_comment(i)
}

fn namespace_comment(i: &str) -> IResult<&str, DefineNamespaceOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("COMMENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(strand)(i)?;
	Ok((i, DefineNamespaceOption::Comment(v)))
}
