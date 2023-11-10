use super::super::super::{
	block::block,
	comment::{mightbespace, shouldbespace},
	common::{closeparentheses, commas, commasorspace, openparentheses},
	ending,
	error::{expect_tag_no_case, expected, ExplainResultExt},
	idiom::{basic, plain},
	literal::{
		datetime, duration, filters, ident, param, scoring, strand, table, tables, timeout,
		tokenizer,
	},
	operator::{assigner, dir},
	part::{
		changefeed, cond, data,
		data::{single, update},
		output,
	},
	thing::thing,
	value::{value, values, whats},
	IResult,
};
use crate::sql::{
	filter::Filter, statements::DefineDatabaseStatement, ChangeFeed, Strand, Tokenizer, Value,
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

pub fn database(i: &str) -> IResult<&str, DefineDatabaseStatement> {
	let (i, _) = alt((tag_no_case("DB"), tag_no_case("DATABASE")))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = cut(ident)(i)?;
	let (i, opts) = many0(database_opts)(i)?;
	let (i, _) = expected("COMMENT or CHANGEFEED", ending::query)(i)?;

	// Create the base statement
	let mut res = DefineDatabaseStatement {
		name,
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

		let serialized: Vec<u8> = (&out).try_into().unwrap();
		let deserialized = DefineDatabaseStatement::try_from(&serialized).unwrap();
		assert_eq!(out, deserialized);
	}
}
