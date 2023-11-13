use super::super::{
	comment::shouldbespace,
	error::{expect_tag_no_case, expected},
	literal::{datetime, table},
	IResult,
};
use crate::sql::{
	statements::{show::ShowSince, ShowStatement},
	Table,
};
use nom::{
	branch::alt,
	bytes::complete::tag_no_case,
	character::complete::{u32, u64},
	combinator::{cut, map, opt, value},
	sequence::preceded,
};

pub fn table_or_database(i: &str) -> IResult<&str, Option<Table>> {
	let (i, v) = expected(
		"one of TABLE, DATABASE",
		alt((
			map(preceded(tag_no_case("TABLE"), preceded(shouldbespace, cut(table))), Some),
			value(None, tag_no_case("DATABASE")),
		)),
	)(i)?;
	Ok((i, v))
}

pub fn since(i: &str) -> IResult<&str, ShowSince> {
	let (i, _) = expect_tag_no_case("SINCE")(i)?;
	let (i, _) = shouldbespace(i)?;

	cut(alt((map(u64, ShowSince::Versionstamp), map(datetime, ShowSince::Timestamp))))(i)
}

pub fn limit(i: &str) -> IResult<&str, u32> {
	let (i, _) = tag_no_case("LIMIT")(i)?;
	let (i, _) = shouldbespace(i)?;
	cut(u32)(i)
}

pub fn show(i: &str) -> IResult<&str, ShowStatement> {
	let (i, _) = tag_no_case("SHOW")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("CHANGES")(i)?;
	let (i, _) = shouldbespace(i)?;
	cut(|i| {
		let (i, _) = tag_no_case("FOR")(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, table) = table_or_database(i)?;
		let (i, since) = preceded(shouldbespace, since)(i)?;
		let (i, limit) = opt(preceded(shouldbespace, limit))(i)?;
		Ok((
			i,
			ShowStatement {
				table,
				since,
				limit,
			},
		))
	})(i)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn tb() {
		let sql = "TABLE person";
		let res = table_or_database(sql);
		let out = res.unwrap().1.unwrap();
		assert_eq!("person", format!("{}", out))
	}

	#[test]
	fn db() {
		let sql = "DATABASE";
		let res = table_or_database(sql);
		assert!(res.unwrap().1.is_none())
	}

	#[test]
	fn show_table_changes() {
		let sql = "SHOW CHANGES FOR TABLE person";
		show(sql).unwrap_err();
	}

	#[test]
	fn show_table_changes_since() {
		let sql = "SHOW CHANGES FOR TABLE person SINCE 0";
		let res = show(sql);
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn show_table_changes_since_ts() {
		let sql = "SHOW CHANGES FOR TABLE person SINCE '2022-07-03T07:18:52Z'";
		let res = show(sql);
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn show_table_changes_limit() {
		let sql = "SHOW CHANGES FOR TABLE person LIMIT 10";
		show(sql).unwrap_err();
	}

	#[test]
	fn show_table_changes_since_limit() {
		let sql = "SHOW CHANGES FOR TABLE person SINCE 0 LIMIT 10";
		let res = show(sql);
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn show_database_changes() {
		let sql = "SHOW CHANGES FOR DATABASE";
		show(sql).unwrap_err();
	}

	#[test]
	fn show_database_changes_since() {
		let sql = "SHOW CHANGES FOR DATABASE SINCE 0";
		let res = show(sql);
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn show_database_changes_since_ts() {
		let sql = "SHOW CHANGES FOR DATABASE SINCE '2022-07-03T07:18:52Z'";
		let res = show(sql);
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn show_database_changes_limit() {
		let sql = "SHOW CHANGES FOR DATABASE LIMIT 10";
		show(sql).unwrap_err();
	}

	#[test]
	fn show_database_changes_since_limit() {
		let sql = "SHOW CHANGES FOR DATABASE SINCE 0 LIMIT 10";
		let res = show(sql);
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}
}
