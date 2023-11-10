use super::super::super::{
	block::block,
	comment::{mightbespace, shouldbespace},
	common::{closeparentheses, commas, commasorspace, openparentheses},
	ending,
	error::{expect_tag_no_case, expected, ExplainResultExt},
	idiom::{self, basic, plain},
	literal::{
		algorithm, datetime, duration, filters, ident, param, scoring, strand, strand::strand_raw,
		table, tables, timeout, tokenizer,
	},
	operator::{assigner, dir},
	part::{
		base, base_or_scope, cond, data,
		data::{single, update},
		output,
		permission::permissions,
	},
	thing::thing,
	value::{value, values, whats},
	IResult,
};
use crate::sql::{
	statements::DefineTokenStatement, Algorithm, Idioms, Index, Kind, Permissions, Strand, Value,
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

pub fn token(i: &str) -> IResult<&str, DefineTokenStatement> {
	let (i, _) = tag_no_case("TOKEN")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, (name, base, opts)) = cut(|i| {
		let (i, name) = ident(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, _) = expect_tag_no_case("ON")(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, base) = base_or_scope(i)?;
		let (i, opts) = many0(token_opts)(i)?;
		let (i, _) = expected("TYPE, VALUE, or COMMENT", ending::query)(i)?;
		Ok((i, (name, base, opts)))
	})(i)?;
	// Create the base statement
	let mut res = DefineTokenStatement {
		name,
		base,
		..Default::default()
	};
	// Assign any defined options
	for opt in opts {
		match opt {
			DefineTokenOption::Type(v) => {
				res.kind = v;
			}
			DefineTokenOption::Value(v) => {
				res.code = v;
			}
			DefineTokenOption::Comment(v) => {
				res.comment = Some(v);
			}
		}
	}
	// Check necessary options
	if res.code.is_empty() {
		// TODO throw error
	}
	// Return the statement
	Ok((i, res))
}

enum DefineTokenOption {
	Type(Algorithm),
	Value(String),
	Comment(Strand),
}

fn token_opts(i: &str) -> IResult<&str, DefineTokenOption> {
	alt((token_type, token_value, token_comment))(i)
}

fn token_type(i: &str) -> IResult<&str, DefineTokenOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("TYPE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(algorithm)(i)?;
	Ok((i, DefineTokenOption::Type(v)))
}

fn token_value(i: &str) -> IResult<&str, DefineTokenOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("VALUE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(strand_raw)(i)?;
	Ok((i, DefineTokenOption::Value(v)))
}

fn token_comment(i: &str) -> IResult<&str, DefineTokenOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("COMMENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(strand)(i)?;
	Ok((i, DefineTokenOption::Comment(v)))
}
