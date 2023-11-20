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

/// Parses a SurrealQL [`Query`]
///
/// During query parsing, the total depth of calls to parse values (including arrays, expressions,
/// functions, objects, sub-queries), Javascript values, and geometry collections count against
/// a computation depth limit. If the limit is reached, parsing will return
/// [`Error::ComputationDepthExceeded`], as opposed to spending more time and potentially
/// overflowing the call stack.
///
/// If you encounter this limit and believe that it should be increased,
/// please [open an issue](https://github.com/surrealdb/surrealdb/issues)!
#[instrument(level = "debug", name = "parser", skip_all, fields(length = input.len()))]
pub fn parse(input: &str) -> Result<Query, Error> {
	parse_impl(input, query)
}

/// Parses a SurrealQL [`Value`].
#[instrument(level = "debug", name = "parser", skip_all, fields(length = input.len()))]
pub fn value(input: &str) -> Result<Value, Error> {
	parse_impl(input, value::value)
}

/// Parses JSON into an inert SurrealQL [`Value`]
#[instrument(level = "debug", name = "parser", skip_all, fields(length = input.len()))]
pub fn json(input: &str) -> Result<Value, Error> {
	parse_impl(input, value::json)
}
/// Parses a SurrealQL Subquery [`Subquery`]
#[instrument(level = "debug", name = "parser", skip_all, fields(length = input.len()))]
pub fn subquery(input: &str) -> Result<Subquery, Error> {
	parse_impl(input, subquery::subquery)
}

/// Parses a SurrealQL [`Idiom`]
#[instrument(level = "debug", name = "parser", skip_all, fields(length = input.len()))]
pub fn idiom(input: &str) -> Result<Idiom, Error> {
	parse_impl(input, idiom::plain)
}

pub fn datetime(input: &str) -> Result<Datetime, Error> {
	parse_impl(input, literal::datetime)
}

pub fn datetime_raw(input: &str) -> Result<Datetime, Error> {
	parse_impl(input, literal::datetime_all_raw)
}

pub fn duration(input: &str) -> Result<Duration, Error> {
	parse_impl(input, literal::duration)
}

pub fn path_like(input: &str) -> Result<Value, Error> {
	parse_impl(input, value::path_like)
}

pub fn range(input: &str) -> Result<Range, Error> {
	parse_impl(input, literal::range)
}

pub fn thing(input: &str) -> Result<Thing, Error> {
	parse_impl(input, thing::thing)
}

pub fn thing_raw(input: &str) -> Result<Thing, Error> {
	parse_impl(input, thing::thing_raw)
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
