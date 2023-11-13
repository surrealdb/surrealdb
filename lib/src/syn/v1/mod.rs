use crate::sql::{Datetime, Duration, Idiom, Query, Range, Thing, Value};
use crate::{err::Error, sql::Subquery};
use nom::{Err, Finish};

mod literal;
mod part;
mod stmt;

mod block;
pub(crate) mod builtin;
mod comment;
mod common;
mod depth;
mod ending;
mod error;
mod expression;
mod function;
mod idiom;
mod kind;
mod omit;
mod operator;
mod special;
mod subquery;
mod thing;
mod value;

pub use error::{IResult, ParseError};

#[cfg(test)]
pub(crate) mod test;

fn query(i: &str) -> IResult<&str, Query> {
	let (i, v) = stmt::statements(i)?;
	if !i.is_empty() {
		return Err(Err::Failure(ParseError::ExplainedExpected {
			tried: i,
			expected: "query to end",
			explained: "perhaps missing a semicolon on the previous statement?",
		}));
	}
	Ok((i, Query(v)))
}

pub fn parse(i: &str) -> Result<Query, Error> {
	parse_impl(i, query)
}

pub fn value(i: &str) -> Result<Value, Error> {
	parse_impl(i, value::value)
}

pub fn json(i: &str) -> Result<Value, Error> {
	parse_impl(i, value::json)
}

pub fn subquery(i: &str) -> Result<Subquery, Error> {
	parse_impl(i, subquery::subquery)
}

pub fn idiom(i: &str) -> Result<Idiom, Error> {
	parse_impl(i, idiom::plain)
}

pub fn datetime(i: &str) -> Result<Datetime, Error> {
	parse_impl(i, literal::datetime)
}

pub fn datetime_raw(i: &str) -> Result<Datetime, Error> {
	parse_impl(i, literal::datetime_all_raw)
}

pub fn duration(i: &str) -> Result<Duration, Error> {
	parse_impl(i, literal::duration)
}

pub fn range(i: &str) -> Result<Range, Error> {
	parse_impl(i, literal::range)
}

pub fn thing(i: &str) -> Result<Thing, Error> {
	parse_impl(i, thing::thing)
}

pub fn thing_raw(i: &str) -> Result<Thing, Error> {
	parse_impl(i, thing::thing_raw)
}

fn parse_impl<O>(input: &str, parser: impl Fn(&str) -> IResult<&str, O>) -> Result<O, Error> {
	// Reset the parse depth limiter
	depth::reset();

	// Check the length of the input
	match input.trim().len() {
		// The input query was empty
		0 => Err(Error::QueryEmpty),
		// Continue parsing the query
		_ => match parser(input).finish() {
			// The query was parsed successfully
			Ok((v, parsed)) if v.is_empty() => Ok(parsed),
			// There was unparsed SQL remaining
			Ok((_, _)) => Err(Error::QueryRemaining),
			// There was an error when parsing the query
			Err(e) => Err(Error::InvalidQuery(e.render_on(input))),
		},
	}
}

#[cfg(not(feature = "experimental_parser"))]
#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn single_query() {
		let sql = "CREATE test";
		let res = query(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("CREATE test;", format!("{}", out))
	}

	#[test]
	fn multiple_query() {
		let sql = "CREATE test; CREATE temp;";
		let res = query(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("CREATE test;\nCREATE temp;", format!("{}", out))
	}

	#[test]
	fn multiple_query_semicolons() {
		let sql = "CREATE test;;;CREATE temp;;;";
		let res = query(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("CREATE test;\nCREATE temp;", format!("{}", out))
	}

	#[test]
	fn multiple_query_semicolons_comments() {
		let sql = "CREATE test;;;CREATE temp;;;/* some comment */";
		let res = query(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("CREATE test;\nCREATE temp;", format!("{}", out))
	}

	#[test]
	fn multiple_query_semicolons_multi_comments() {
		let sql = "CREATE test;;;CREATE temp;;;/* some comment */;;;/* other comment */";
		let res = query(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("CREATE test;\nCREATE temp;", format!("{}", out))
	}
}
