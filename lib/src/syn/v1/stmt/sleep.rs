use super::super::{
	block::block,
	comment::{mightbespace, shouldbespace},
	common::{closeparentheses, commas, commasorspace, openparentheses},
	error::{expect_tag_no_case, expected, ExplainResultExt},
	idiom::{basic, plain},
	literal::{datetime, duration, ident, param, scoring, table, tables, timeout},
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
use crate::sql::{statements::SleepStatement, Fields, Value};
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

pub fn sleep(i: &str) -> IResult<&str, SleepStatement> {
	let (i, _) = tag_no_case("SLEEP")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = duration(i)?;
	Ok((
		i,
		SleepStatement {
			duration: v,
		},
	))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::dbs::test::mock;
	use std::time::SystemTime;

	#[test]
	fn test_sleep_statement_sec() {
		let sql = "SLEEP 2s";
		let res = sleep(sql);
		let out = res.unwrap().1;
		assert_eq!("SLEEP 2s", format!("{}", out))
	}

	#[test]
	fn test_sleep_statement_ms() {
		let sql = "SLEEP 500ms";
		let res = sleep(sql);
		let out = res.unwrap().1;
		assert_eq!("SLEEP 500ms", format!("{}", out))
	}

	#[tokio::test]
	async fn test_sleep_compute() {
		let sql = "SLEEP 500ms";
		let time = SystemTime::now();
		let (ctx, opt, txn) = mock().await;
		let (_, stm) = sleep(sql).unwrap();
		let value = stm.compute(&ctx, &opt, &txn, None).await.unwrap();
		assert!(time.elapsed().unwrap() >= time::Duration::microseconds(500));
		assert_eq!(value, Value::None);
	}
}
