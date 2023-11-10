use super::super::{
	block::block,
	comment::{mightbespace, shouldbespace},
	common::{closeparentheses, commas, commasorspace, openparentheses},
	error::{expect_tag_no_case, expected, ExplainResultExt},
	idiom::{basic, plain},
	literal::{datetime, duration, ident, param, scoring, table, tables, timeout, uuid},
	operator::{assigner, dir},
	part::{
		cond, data,
		data::{single, update},
		output,
	},
	thing::thing,
	value::{value, values, whats},
	IResult,
};
use crate::sql::{statements::KillStatement, Value};
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

pub fn kill(i: &str) -> IResult<&str, KillStatement> {
	let (i, _) = tag_no_case("KILL")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = alt((into(uuid), into(param)))(i)?;
	Ok((
		i,
		KillStatement {
			id: v,
		},
	))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql::{Ident, Param, Uuid};

	#[test]
	fn kill_uuid() {
		let uuid_str = "c005b8da-63a4-48bc-a371-07e95b39d58e";
		let uuid_str_wrapped = format!("'{}'", uuid_str);
		let sql = format!("kill {}", uuid_str_wrapped);
		let res = kill(&sql);
		assert!(res.is_ok(), "{:?}", res);
		let out = res.unwrap().1;
		assert_eq!(
			out,
			KillStatement {
				id: Value::Uuid(Uuid::from(uuid::Uuid::parse_str(uuid_str).unwrap()))
			}
		);
		assert_eq!("KILL 'c005b8da-63a4-48bc-a371-07e95b39d58e'", format!("{}", out));
	}

	#[test]
	fn kill_param() {
		let sql = "kill $id";
		let res = kill(sql);
		let out = res.unwrap().1;
		assert_eq!(
			out,
			KillStatement {
				id: Value::Param(Param(Ident("id".to_string()))),
			}
		);
		assert_eq!("KILL $id", format!("{}", out));
	}
}
