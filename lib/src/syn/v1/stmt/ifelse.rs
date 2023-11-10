use super::super::{
	block::block,
	comment::{mightbespace, shouldbespace},
	common::{closeparentheses, commas, commasorspace, openparentheses},
	error::{expect_tag_no_case, expected},
	idiom::{basic, plain},
	literal::{datetime, duration, ident, param, scoring, table, tables, timeout},
	operator::{assigner, dir},
	part::{cond, data, output},
	thing::thing,
	value::{value, whats},
	IResult,
};
use crate::sql::{statements::IfelseStatement, Value};
use nom::{
	branch::alt,
	bytes::complete::{escaped, escaped_transform, is_not, tag, tag_no_case, take, take_while_m_n},
	character::complete::{anychar, char, u16, u32},
	combinator::{cut, into, map, map_res, opt, recognize, value as map_value},
	multi::separated_list1,
	number::complete::recognize_float,
	sequence::{delimited, preceded, terminated, tuple},
	Err,
};

pub fn ifelse(i: &str) -> IResult<&str, IfelseStatement> {
	let (i, _) = tag_no_case("IF")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, cond) = value(i)?;
	let (i, _) = shouldbespace(i)?;
	if let (i, Some(_)) = opt(terminated(tag_no_case("THEN"), shouldbespace))(i)? {
		worded(i, cond)
	} else {
		bracketed(i, cond)
	}
}

fn worded(i: &str, initial_cond: Value) -> IResult<&str, IfelseStatement> {
	//
	fn expr(i: &str) -> IResult<&str, (Value, Value)> {
		let (i, cond) = value(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, _) = tag_no_case("THEN")(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, then) = value(i)?;
		Ok((i, (cond, then)))
	}

	fn split(i: &str) -> IResult<&str, ()> {
		let (i, _) = shouldbespace(i)?;
		let (i, _) = tag_no_case("ELSE")(i)?;
		let (i, _) = shouldbespace(i)?;
		Ok((i, ()))
	}

	let (mut input, then) = value(i)?;
	let mut exprs = vec![(initial_cond, then)];
	let mut close = None;

	loop {
		let (i, Some(_)) = opt(split)(input)? else {
			break;
		};
		let (i, Some(_)) = opt(terminated(tag_no_case("IF"), shouldbespace))(i)? else {
			let (i, v) = cut(value)(i)?;
			close = Some(v);
			input = i;
			break;
		};
		let (i, branch) = cut(expr)(i)?;
		exprs.push(branch);
		input = i;
	}

	let (i, _) = shouldbespace(input)?;
	let (i, _) = tag_no_case("END")(i)?;
	Ok((
		i,
		IfelseStatement {
			exprs,
			close,
		},
	))
}

fn bracketed(i: &str, initial_cond: Value) -> IResult<&str, IfelseStatement> {
	//
	fn expr(i: &str) -> IResult<&str, (Value, Value)> {
		let (i, cond) = value(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, then) = into(block)(i)?;
		Ok((i, (cond, then)))
	}
	//
	fn split(i: &str) -> IResult<&str, ()> {
		let (i, _) = shouldbespace(i)?;
		let (i, _) = tag_no_case("ELSE")(i)?;
		let (i, _) = shouldbespace(i)?;
		Ok((i, ()))
	}

	let (mut input, then) = into(block)(i)?;
	let mut exprs = vec![(initial_cond, then)];
	let mut close = None;

	loop {
		let (i, Some(_)) = opt(split)(input)? else {
			break;
		};
		let (i, Some(_)) = opt(terminated(tag_no_case("IF"), shouldbespace))(i)? else {
			let (i, c) = into(cut(block))(i)?;
			close = Some(c);
			input = i;
			break;
		};
		let (i, branch) = cut(expr)(i)?;
		exprs.push(branch);
		input = i;
	}

	Ok((
		input,
		IfelseStatement {
			exprs,
			close,
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn ifelse_worded_statement_first() {
		let sql = "IF this THEN that END";
		let res = ifelse(sql);
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn ifelse_worded_statement_close() {
		let sql = "IF this THEN that ELSE that END";
		let res = ifelse(sql);
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn ifelse_worded_statement_other() {
		let sql = "IF this THEN that ELSE IF this THEN that END";
		let res = ifelse(sql);
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn ifelse_worded_statement_other_close() {
		let sql = "IF this THEN that ELSE IF this THEN that ELSE that END";
		let res = ifelse(sql);
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn ifelse_bracketed_statement_first() {
		let sql = "IF this { that }";
		let res = ifelse(sql);
		let res = res.unwrap().1.to_string();
		assert_eq!(sql, res)
	}

	#[test]
	fn ifelse_bracketed_statement_close() {
		let sql = "IF this { that } ELSE { that }";
		let res = ifelse(sql);
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn ifelse_bracketed_statement_other() {
		let sql = "IF this { that } ELSE IF this { that }";
		let res = ifelse(sql);
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn ifelse_bracketed_statement_other_close() {
		let sql = "IF this { that } ELSE IF this { that } ELSE { that }";
		let res = ifelse(sql);
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}
}
