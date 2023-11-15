use super::super::{
	comment::shouldbespace,
	error::{expected, ExplainResultExt},
	literal::{param, table},
	part::{
		data::{single, update, values},
		output, timeout,
	},
	value::value,
	IResult,
};
use crate::sql::{statements::InsertStatement, Value};
use nom::{
	branch::alt,
	bytes::complete::tag_no_case,
	combinator::{cut, map, opt},
	sequence::{preceded, terminated},
};

pub fn insert(i: &str) -> IResult<&str, InsertStatement> {
	let (i, _) = tag_no_case("INSERT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, ignore) = opt(terminated(tag_no_case("IGNORE"), shouldbespace))(i)?;
	let (i, _) = tag_no_case("INTO")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, into) = expected(
		"a parameter or a table name",
		cut(alt((
			map(terminated(table, shouldbespace), Value::Table),
			map(terminated(param, shouldbespace), Value::Param),
		))),
	)(i)
	.explain("expressions aren't allowed here.", value)?;
	let (i, data) = cut(alt((values, single)))(i)?;
	let (i, update) = opt(preceded(shouldbespace, update))(i)?;
	let (i, output) = opt(preceded(shouldbespace, output))(i)?;
	let (i, timeout) = opt(preceded(shouldbespace, timeout))(i)?;
	let (i, parallel) = opt(preceded(shouldbespace, tag_no_case("PARALLEL")))(i)?;
	Ok((
		i,
		InsertStatement {
			into,
			data,
			ignore: ignore.is_some(),
			update,
			output,
			timeout,
			parallel: parallel.is_some(),
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn insert_statement_basic() {
		let sql = "INSERT INTO test (field) VALUES ($value)";
		let res = insert(sql);
		let out = res.unwrap().1;
		assert_eq!("INSERT INTO test (field) VALUES ($value)", format!("{}", out))
	}

	#[test]
	fn insert_statement_ignore() {
		let sql = "INSERT IGNORE INTO test (field) VALUES ($value)";
		let res = insert(sql);
		let out = res.unwrap().1;
		assert_eq!("INSERT IGNORE INTO test (field) VALUES ($value)", format!("{}", out))
	}

	#[test]
	fn insert_statement_ignore_update() {
		let sql = "INSERT IGNORE INTO test (field) VALUES ($value) ON DUPLICATE KEY UPDATE field = $value";
		let res = insert(sql);
		let out = res.unwrap().1;
		assert_eq!("INSERT IGNORE INTO test (field) VALUES ($value) ON DUPLICATE KEY UPDATE field = $value", format!("{}", out))
	}
}
