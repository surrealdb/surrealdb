use super::{
	comment::{mightbespace, shouldbespace},
	common::{closeparentheses, expect_delimited, openparentheses},
	ending,
	error::ExplainResultExt,
	stmt::{
		create, define, delete, ifelse, insert, output, rebuild, relate, remove, select, update,
	},
	value::value,
	IResult,
};
use crate::sql::Subquery;
use nom::{
	branch::alt,
	bytes::complete::{tag, tag_no_case},
	combinator::{map, opt, peek},
	sequence::tuple,
};

pub fn subquery(i: &str) -> IResult<&str, Subquery> {
	alt((subquery_ifelse, subquery_other, subquery_value))(i)
}

fn subquery_ifelse(i: &str) -> IResult<&str, Subquery> {
	let (i, v) = map(ifelse, Subquery::Ifelse)(i)?;
	Ok((i, v))
}

fn subquery_value(i: &str) -> IResult<&str, Subquery> {
	expect_delimited(openparentheses, map(value, Subquery::Value), closeparentheses)(i)
}

fn subquery_other(i: &str) -> IResult<&str, Subquery> {
	alt((expect_delimited(openparentheses, subquery_inner, closeparentheses), |i| {
		let (i, v) = subquery_inner(i)?;
		let (i, _) = ending::subquery(i)?;
		let (i, _) = eat_semicolon(i)?;
		Ok((i, v))
	}))(i)
}

fn eat_semicolon(i: &str) -> IResult<&str, ()> {
	let (i, _) = opt(|i| {
		let (i, _) = mightbespace(i)?;
		let (i, _) = tag(";")(i)?;
		let (i, _) = peek(tuple((
			shouldbespace,
			alt((tag_no_case("THEN"), tag_no_case("ELSE"), tag_no_case("END"))),
		)))(i)?;
		Ok((i, ()))
	})(i)?;
	Ok((i, ()))
}

pub fn subquery_inner(i: &str) -> IResult<&str, Subquery> {
	alt((
		map(output, Subquery::Output),
		map(select, Subquery::Select),
		map(create, Subquery::Create),
		map(update, Subquery::Update),
		map(delete, Subquery::Delete),
		map(relate, Subquery::Relate),
		map(insert, Subquery::Insert),
		map(define, Subquery::Define),
		map(rebuild, Subquery::Rebuild),
		map(remove, Subquery::Remove),
	))(i)
	.explain("This statement is not allowed in a subquery", disallowed_subquery_statements)
}

fn disallowed_subquery_statements(i: &str) -> IResult<&str, ()> {
	let (i, _) = alt((
		tag_no_case("ANALYZED"),
		tag_no_case("BEGIN"),
		tag_no_case("BREAK"),
		tag_no_case("CONTINUE"),
		tag_no_case("COMMIT"),
		tag_no_case("FOR"),
		tag_no_case("INFO"),
		tag_no_case("KILL"),
		tag_no_case("LIVE"),
		tag_no_case("OPTION"),
		tag_no_case("RELATE"),
		tag_no_case("SLEEP"),
		tag_no_case("THROW"),
		tag_no_case("USE"),
	))(i)?;
	Ok((i, ()))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn subquery_expression_statement() {
		let sql = "(1 + 2 + 3)";
		let res = subquery(sql);
		let out = res.unwrap().1;
		assert_eq!("(1 + 2 + 3)", format!("{}", out))
	}

	#[test]
	fn subquery_ifelse_statement() {
		let sql = "IF true THEN false END";
		let res = subquery(sql);
		let out = res.unwrap().1;
		assert_eq!("IF true THEN false END", format!("{}", out))
	}

	#[test]
	fn subquery_select_statement() {
		let sql = "(SELECT * FROM test)";
		let res = subquery(sql);
		let out = res.unwrap().1;
		assert_eq!("(SELECT * FROM test)", format!("{}", out))
	}

	#[test]
	fn subquery_define_statement() {
		let sql = "(DEFINE EVENT foo ON bar WHEN $event = 'CREATE' THEN (CREATE x SET y = 1))";
		let res = subquery(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			"(DEFINE EVENT foo ON bar WHEN $event = 'CREATE' THEN (CREATE x SET y = 1))",
			format!("{}", out)
		)
	}

	#[test]
	fn subquery_rebuild_statement() {
		let sql = "(REBUILD INDEX foo_event ON foo)";
		let res = subquery(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("(REBUILD INDEX foo_event ON foo)", format!("{}", out))
	}

	#[test]
	fn subquery_remove_statement() {
		let sql = "(REMOVE EVENT foo_event ON foo)";
		let res = subquery(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("(REMOVE EVENT foo_event ON foo)", format!("{}", out))
	}
}
