use super::super::{
	block::block,
	comment::{mightbespace, shouldbespace},
	common::{closeparentheses, commas, commasorspace, openparentheses},
	ending,
	error::{expect_tag_no_case, expected, ExplainResultExt},
	idiom::{basic, plain},
	literal::{datetime, duration, ident, ident_raw, param, scoring, table, tables, timeout},
	omit::omit,
	operator::{assigner, dir},
	part::{
		cond, data,
		data::{single, update},
		explain, fetch, fields, group, limit, order, output, split, start, version, with,
	},
	special::{check_group_by_fields, check_order_by_fields, check_split_on_fields},
	thing::thing,
	value::{selects, value, values, whats},
	IResult,
};
use crate::sql::{statements::SelectStatement, Fields, Value};
use nom::{
	branch::alt,
	bytes::complete::{escaped, escaped_transform, is_not, tag, tag_no_case, take, take_while_m_n},
	character::complete::{anychar, char, u16, u32},
	combinator::{cut, into, map, map_res, opt, peek, recognize, value as map_value},
	multi::separated_list1,
	number::complete::recognize_float,
	sequence::{delimited, preceded, terminated, tuple},
	Err,
};

pub fn select(i: &str) -> IResult<&str, SelectStatement> {
	let (i, _) = tag_no_case("SELECT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, expr) = fields(i)?;
	let (i, omit) = opt(preceded(shouldbespace, omit))(i)?;
	let (i, _) = cut(shouldbespace)(i)?;
	let (i, _) = expect_tag_no_case("FROM")(i)?;
	let (i, only) = opt(preceded(shouldbespace, tag_no_case("ONLY")))(i)?;
	let (i, _) = cut(shouldbespace)(i)?;
	let (i, what) = cut(selects)(i)?;
	let (i, with) = opt(preceded(shouldbespace, with))(i)?;
	let (i, cond) = opt(preceded(shouldbespace, cond))(i)?;
	let (i, split) = opt(preceded(shouldbespace, split))(i)?;
	check_split_on_fields(i, &expr, &split)?;
	let (i, group) = opt(preceded(shouldbespace, group))(i)?;
	check_group_by_fields(i, &expr, &group)?;
	let (i, order) = opt(preceded(shouldbespace, order))(i)?;
	check_order_by_fields(i, &expr, &order)?;

	let (i, (limit, start)) = if let Ok((i, limit)) = preceded(shouldbespace, limit)(i) {
		let (i, start) = opt(preceded(shouldbespace, start))(i)?;
		(i, (Some(limit), start))
	} else if let Ok((i, start)) = preceded(shouldbespace, start)(i) {
		let (i, limit) = opt(preceded(shouldbespace, limit))(i)?;
		(i, (limit, Some(start)))
	} else {
		(i, (None, None))
	};

	let (i, fetch) = opt(preceded(shouldbespace, fetch))(i)?;
	let (i, version) = opt(preceded(shouldbespace, version))(i)?;
	let (i, timeout) = opt(preceded(shouldbespace, timeout))(i)?;
	let (i, parallel) = opt(preceded(shouldbespace, tag_no_case("PARALLEL")))(i)?;
	let (i, explain) = opt(preceded(shouldbespace, explain))(i)?;
	let (i, _) = expected(
		"one of WITH, WHERE, SPLIT, GROUP, ORDER, LIMIT, START, FETCH, VERSION, TIMEOUT, PARALLEL, or EXPLAIN",
		cut(peek(ending::query))
	)(i)?;

	Ok((
		i,
		SelectStatement {
			expr,
			omit,
			only: only.is_some(),
			what,
			with,
			cond,
			split,
			group,
			order,
			limit,
			start,
			fetch,
			version,
			timeout,
			parallel: parallel.is_some(),
			explain,
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;

	fn assert_parsable(sql: &str) {
		let res = select(sql);
		assert!(res.is_ok());
		let (_, out) = res.unwrap();
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn select_statement_param() {
		assert_parsable("SELECT * FROM $test");
	}

	#[test]
	fn select_statement_table() {
		assert_parsable("SELECT * FROM test");
	}

	#[test]
	fn select_statement_omit() {
		assert_parsable("SELECT * OMIT password FROM test");
	}

	#[test]
	fn select_statement_thing() {
		assert_parsable("SELECT * FROM test:thingy ORDER BY name");
	}

	#[test]
	fn select_statement_clash() {
		assert_parsable("SELECT * FROM order ORDER BY order");
	}

	#[test]
	fn select_statement_limit_select() {
		assert_parsable("SELECT * FROM table LIMIT 3 START 2");
	}

	#[test]
	fn select_statement_limit_select_unordered() {
		let res = select("SELECT * FROM table START 2 LIMIT 1");
		assert!(res.is_ok());
		let (_, out) = res.unwrap();
		assert_eq!("SELECT * FROM table LIMIT 1 START 2", format!("{}", out))
	}

	#[test]
	fn select_statement_table_thing() {
		assert_parsable("SELECT *, ((1 + 3) / 4), 1.3999f AS tester FROM test, test:thingy");
	}

	#[test]
	fn select_with_function() {}
}
