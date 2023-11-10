use super::super::super::{
	block::block,
	comment::{mightbespace, shouldbespace},
	common::{closeparentheses, commas, commasorspace, openparentheses},
	ending,
	error::{expect_tag_no_case, expected, ExplainResultExt},
	idiom::{self, basic, plain},
	literal::{datetime, duration, filters, ident, param, scoring, strand, timeout, tokenizer},
	operator::{assigner, dir},
	part::{
		changefeed, cond, data,
		data::{single, update},
		output,
		permission::permissions,
		view,
	},
	thing::thing,
	value::{value, values, whats},
	IResult,
};
use crate::sql::{
	statements::DefineTableStatement, ChangeFeed, Idioms, Index, Kind, Permissions, Strand, Value,
	View,
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

pub fn table(i: &str) -> IResult<&str, DefineTableStatement> {
	let (i, _) = tag_no_case("TABLE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = cut(ident)(i)?;
	let (i, opts) = many0(table_opts)(i)?;
	let (i, _) = expected(
		"DROP, SCHEMALESS, SCHEMAFUL(L), VIEW, CHANGEFEED, PERMISSIONS, or COMMENT",
		ending::query,
	)(i)?;
	// Create the base statement
	let mut res = DefineTableStatement {
		name,
		..Default::default()
	};
	// Assign any defined options
	for opt in opts {
		match opt {
			DefineTableOption::Drop => {
				res.drop = true;
			}
			DefineTableOption::Schemafull => {
				res.full = true;
			}
			DefineTableOption::Schemaless => {
				res.full = false;
			}
			DefineTableOption::View(v) => {
				res.view = Some(v);
			}
			DefineTableOption::Comment(v) => {
				res.comment = Some(v);
			}
			DefineTableOption::ChangeFeed(v) => {
				res.changefeed = Some(v);
			}
			DefineTableOption::Permissions(v) => {
				res.permissions = v;
			}
		}
	}
	// Return the statement
	Ok((i, res))
}

#[derive(Debug)]
enum DefineTableOption {
	Drop,
	View(View),
	Schemaless,
	Schemafull,
	Comment(Strand),
	Permissions(Permissions),
	ChangeFeed(ChangeFeed),
}

fn table_opts(i: &str) -> IResult<&str, DefineTableOption> {
	alt((
		table_drop,
		table_view,
		table_comment,
		table_schemaless,
		table_schemafull,
		table_permissions,
		table_changefeed,
	))(i)
}

fn table_drop(i: &str) -> IResult<&str, DefineTableOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("DROP")(i)?;
	Ok((i, DefineTableOption::Drop))
}

fn table_changefeed(i: &str) -> IResult<&str, DefineTableOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, v) = changefeed(i)?;
	Ok((i, DefineTableOption::ChangeFeed(v)))
}

fn table_view(i: &str) -> IResult<&str, DefineTableOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, v) = view(i)?;
	Ok((i, DefineTableOption::View(v)))
}

fn table_schemaless(i: &str) -> IResult<&str, DefineTableOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("SCHEMALESS")(i)?;
	Ok((i, DefineTableOption::Schemaless))
}

fn table_schemafull(i: &str) -> IResult<&str, DefineTableOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = alt((tag_no_case("SCHEMAFULL"), tag_no_case("SCHEMAFUL")))(i)?;
	Ok((i, DefineTableOption::Schemafull))
}

fn table_comment(i: &str) -> IResult<&str, DefineTableOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("COMMENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = strand(i)?;
	Ok((i, DefineTableOption::Comment(v)))
}

fn table_permissions(i: &str) -> IResult<&str, DefineTableOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, v) = permissions(i)?;
	Ok((i, DefineTableOption::Permissions(v)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn define_table_with_changefeed() {
		let sql = "TABLE mytable SCHEMALESS CHANGEFEED 1h";
		let res = table(sql);
		let out = res.unwrap().1;
		assert_eq!(format!("DEFINE {sql}"), format!("{}", out));

		let serialized: Vec<u8> = (&out).try_into().unwrap();
		let deserialized = DefineTableStatement::try_from(&serialized).unwrap();
		assert_eq!(out, deserialized);
	}
}
