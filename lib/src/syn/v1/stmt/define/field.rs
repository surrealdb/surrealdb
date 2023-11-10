use super::super::super::{
	block::block,
	comment::{mightbespace, shouldbespace},
	common::{closeparentheses, commas, commasorspace, openparentheses},
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
		cond, data,
		data::{single, update},
		output,
		permission::permissions,
	},
	thing::thing,
	value::{value, values, whats},
	IResult,
};
use crate::sql::{statements::DefineFieldStatement, Kind, Permissions, Strand, Value};
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

pub fn field(i: &str) -> IResult<&str, DefineFieldStatement> {
	let (i, _) = tag_no_case("FIELD")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, (name, what, opts)) = cut(|i| {
		let (i, name) = idiom::local(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, _) = expect_tag_no_case("ON")(i)?;
		let (i, _) = opt(tuple((shouldbespace, tag_no_case("TABLE"))))(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, what) = ident(i)?;
		let (i, opts) = many0(field_opts)(i)?;
		let (i, _) = expected(
			"one of FLEX(IBLE), TYPE, VALUE, ASSERT, DEFAULT, or COMMENT",
			cut(ending::query),
		)(i)?;
		Ok((i, (name, what, opts)))
	})(i)?;
	// Create the base statement
	let mut res = DefineFieldStatement {
		name,
		what,
		..Default::default()
	};
	// Assign any defined options
	for opt in opts {
		match opt {
			DefineFieldOption::Flex => {
				res.flex = true;
			}
			DefineFieldOption::Kind(v) => {
				res.kind = Some(v);
			}
			DefineFieldOption::Value(v) => {
				res.value = Some(v);
			}
			DefineFieldOption::Assert(v) => {
				res.assert = Some(v);
			}
			DefineFieldOption::Default(v) => {
				res.default = Some(v);
			}
			DefineFieldOption::Comment(v) => {
				res.comment = Some(v);
			}
			DefineFieldOption::Permissions(v) => {
				res.permissions = v;
			}
		}
	}
	// Return the statement
	Ok((i, res))
}

enum DefineFieldOption {
	Flex,
	Kind(Kind),
	Value(Value),
	Assert(Value),
	Default(Value),
	Comment(Strand),
	Permissions(Permissions),
}

fn field_opts(i: &str) -> IResult<&str, DefineFieldOption> {
	alt((
		field_flex,
		field_kind,
		field_value,
		field_assert,
		field_default,
		field_comment,
		field_permissions,
	))(i)
}

fn field_flex(i: &str) -> IResult<&str, DefineFieldOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = alt((tag_no_case("FLEXIBLE"), tag_no_case("FLEXI"), tag_no_case("FLEX")))(i)?;
	Ok((i, DefineFieldOption::Flex))
}

fn field_kind(i: &str) -> IResult<&str, DefineFieldOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("TYPE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(kind)(i)?;
	Ok((i, DefineFieldOption::Kind(v)))
}

fn field_value(i: &str) -> IResult<&str, DefineFieldOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("VALUE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(value)(i)?;
	Ok((i, DefineFieldOption::Value(v)))
}

fn field_assert(i: &str) -> IResult<&str, DefineFieldOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("ASSERT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(value)(i)?;
	Ok((i, DefineFieldOption::Assert(v)))
}

fn field_default(i: &str) -> IResult<&str, DefineFieldOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("DEFAULT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(value)(i)?;
	Ok((i, DefineFieldOption::Default(v)))
}

fn field_comment(i: &str) -> IResult<&str, DefineFieldOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("COMMENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = strand(i)?;
	Ok((i, DefineFieldOption::Comment(v)))
}

fn field_permissions(i: &str) -> IResult<&str, DefineFieldOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, v) = permissions(i)?;
	Ok((i, DefineFieldOption::Permissions(v)))
}
