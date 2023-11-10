use super::super::{
	block::block,
	comment::{mightbespace, shouldbespace},
	common::{closeparentheses, commas, commasorspace, openparentheses},
	error::{expect_tag_no_case, expected, ExplainResultExt},
	idiom::{basic, plain},
	literal::{datetime, duration, ident, ident_raw, param, scoring, table, tables, timeout},
	operator::{assigner, dir},
	part::{
		cond, data,
		data::{single, update},
		fetch, fields, output,
	},
	thing::thing,
	value::{value, values, whats},
	IResult,
};
use crate::sql::{statements::UseStatement, Fields, Value};
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

pub fn r#use(i: &str) -> IResult<&str, UseStatement> {
	let (i, _) = tag_no_case("USE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, (ns, db)) = alt((
		map(tuple((namespace, opt(preceded(shouldbespace, database)))), |x| (Some(x.0), x.1)),
		map(database, |x| (None, Some(x))),
	))(i)?;
	Ok((
		i,
		UseStatement {
			ns,
			db,
		},
	))
}

fn namespace(i: &str) -> IResult<&str, String> {
	let (i, _) = alt((tag_no_case("NAMESPACE"), tag_no_case("NS")))(i)?;
	let (i, _) = shouldbespace(i)?;
	cut(ident_raw)(i)
}

fn database(i: &str) -> IResult<&str, String> {
	let (i, _) = alt((tag_no_case("DATABASE"), tag_no_case("DB")))(i)?;
	let (i, _) = shouldbespace(i)?;
	cut(ident_raw)(i)
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn use_query_ns() {
		let sql = "USE NS test";
		let res = r#use(sql);
		let out = res.unwrap().1;
		assert_eq!(
			out,
			UseStatement {
				ns: Some(String::from("test")),
				db: None,
			}
		);
		assert_eq!("USE NS test", format!("{}", out));
	}

	#[test]
	fn use_query_db() {
		let sql = "USE DB test";
		let res = r#use(sql);
		let out = res.unwrap().1;
		assert_eq!(
			out,
			UseStatement {
				ns: None,
				db: Some(String::from("test")),
			}
		);
		assert_eq!("USE DB test", format!("{}", out));
	}

	#[test]
	fn use_query_both() {
		let sql = "USE NS test DB test";
		let res = r#use(sql);
		let out = res.unwrap().1;
		assert_eq!(
			out,
			UseStatement {
				ns: Some(String::from("test")),
				db: Some(String::from("test")),
			}
		);
		assert_eq!("USE NS test DB test", format!("{}", out));
	}
}
