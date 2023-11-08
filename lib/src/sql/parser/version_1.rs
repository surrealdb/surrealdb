use crate::err::Error;
use crate::sql::depth;
use crate::sql::error::IResult;
use crate::sql::idiom::Idiom;
use crate::sql::query::{query, Query};
use crate::sql::subquery::Subquery;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use nom::Finish;
use std::str;
use tracing::instrument;

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

/// Parses a SurrealQL [`Thing`]
#[instrument(level = "debug", name = "parser", skip_all, fields(length = input.len()))]
pub fn thing(input: &str) -> Result<Thing, Error> {
	parse_impl(input, super::super::thing::thing)
}

/// Parses a SurrealQL [`Idiom`]
#[instrument(level = "debug", name = "parser", skip_all, fields(length = input.len()))]
pub fn idiom(input: &str) -> Result<Idiom, Error> {
	parse_impl(input, super::super::idiom::plain)
}

/// Parses a SurrealQL [`Value`].
#[instrument(level = "debug", name = "parser", skip_all, fields(length = input.len()))]
pub fn value(input: &str) -> Result<Value, Error> {
	parse_impl(input, super::super::value::value)
}

/// Parses a SurrealQL Subquery [`Subquery`]
#[instrument(level = "debug", name = "parser", skip_all, fields(length = input.len()))]
pub fn subquery(input: &str) -> Result<Subquery, Error> {
	parse_impl(input, super::super::subquery::subquery)
}

/// Parses JSON into an inert SurrealQL [`Value`]
#[instrument(level = "debug", name = "parser", skip_all, fields(length = input.len()))]
pub fn json(input: &str) -> Result<Value, Error> {
	parse_impl(input.trim(), super::super::value::json)
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
