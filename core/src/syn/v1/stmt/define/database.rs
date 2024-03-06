use super::super::super::{
	comment::shouldbespace,
	ending,
	error::expected,
	literal::{ident, strand},
	part::changefeed,
	IResult,
};
use crate::sql::{statements::DefineDatabaseStatement, ChangeFeed, Strand};
use nom::{
	branch::alt, bytes::complete::tag_no_case, combinator::cut, combinator::opt, multi::many0,
	sequence::tuple,
};

pub fn database(i: &str) -> IResult<&str, DefineDatabaseStatement> {
	let (i, _) = alt((tag_no_case("DB"), tag_no_case("DATABASE")))(i)?;
	#[cfg(feature = "sql2")]
	let (i, if_not_exists) = opt(tuple((
		shouldbespace,
		tag_no_case("IF"),
		cut(tuple((shouldbespace, tag_no_case("NOT"), shouldbespace, tag_no_case("EXISTS")))),
	)))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = cut(ident)(i)?;
	let (i, opts) = many0(database_opts)(i)?;
	let (i, _) = expected("COMMENT or CHANGEFEED", ending::query)(i)?;

	// Create the base statement
	let mut res = DefineDatabaseStatement {
		name,
		#[cfg(feature = "sql2")]
		if_not_exists: if_not_exists.is_some(),
		..Default::default()
	};
	// Assign any defined options
	for opt in opts {
		match opt {
			DefineDatabaseOption::Comment(v) => {
				res.comment = Some(v);
			}
			DefineDatabaseOption::ChangeFeed(v) => {
				res.changefeed = Some(v);
			}
		}
	}
	// Return the statement
	Ok((i, res))
}

enum DefineDatabaseOption {
	Comment(Strand),
	ChangeFeed(ChangeFeed),
}

fn database_opts(i: &str) -> IResult<&str, DefineDatabaseOption> {
	alt((database_comment, database_changefeed))(i)
}

fn database_comment(i: &str) -> IResult<&str, DefineDatabaseOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("COMMENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(strand)(i)?;
	Ok((i, DefineDatabaseOption::Comment(v)))
}

fn database_changefeed(i: &str) -> IResult<&str, DefineDatabaseOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, v) = changefeed(i)?;
	Ok((i, DefineDatabaseOption::ChangeFeed(v)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn define_database_with_changefeed() {
		let sql = "DATABASE mydatabase CHANGEFEED 1h";
		let res = database(sql);
		let out = res.unwrap().1;
		assert_eq!(format!("DEFINE {sql}"), format!("{}", out));

		let serialized: Vec<u8> = (&out).into();
		let deserialized = DefineDatabaseStatement::from(&serialized);
		assert_eq!(out, deserialized);
	}
}
