#![allow(dead_code)]

use crate::{
	err::Error,
	sql::{Datetime, Duration, Idiom, Query, Range, Subquery, Thing, Value},
};

pub mod lexer;
pub mod parser;
pub mod token;

#[cfg(test)]
pub mod test;

use lexer::Lexer;
use parser::{ParseError, ParseErrorKind, Parser};

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
	dbg!(input);
	let mut parser = Parser::new(input.as_bytes());
	parser.parse_query().map_err(|e| e.render_on(input)).map_err(Error::InvalidQuery)
}

/// Parses a SurrealQL [`Value`].
#[instrument(level = "debug", name = "parser", skip_all, fields(length = input.len()))]
pub fn value(input: &str) -> Result<Value, Error> {
	dbg!(input);
	let mut parser = Parser::new(input.as_bytes());
	parser.parse_value().map_err(|e| e.render_on(input)).map_err(Error::InvalidQuery)
}

/// Parses JSON into an inert SurrealQL [`Value`]
#[instrument(level = "debug", name = "parser", skip_all, fields(length = input.len()))]
pub fn json(input: &str) -> Result<Value, Error> {
	dbg!(input);
	let mut parser = Parser::new(input.as_bytes());
	parser.parse_json().map_err(|e| e.render_on(input)).map_err(Error::InvalidQuery)
}
/// Parses a SurrealQL Subquery [`Subquery`]
#[instrument(level = "debug", name = "parser", skip_all, fields(length = input.len()))]
pub fn subquery(input: &str) -> Result<Subquery, Error> {
	dbg!(input);
	let mut parser = Parser::new(input.as_bytes());
	parser.parse_full_subquery().map_err(|e| e.render_on(input)).map_err(Error::InvalidQuery)
}

/// Parses a SurrealQL [`Idiom`]
#[instrument(level = "debug", name = "parser", skip_all, fields(length = input.len()))]
pub fn idiom(input: &str) -> Result<Idiom, Error> {
	dbg!(input);
	let mut parser = Parser::new(input.as_bytes());
	parser.parse_plain_idiom().map_err(|e| e.render_on(input)).map_err(Error::InvalidQuery)
}

pub fn datetime_raw(input: &str) -> Result<Datetime, Error> {
	dbg!(input);
	let mut lexer = Lexer::new(input.as_bytes());
	lexer
		.lex_datetime_raw_err()
		.map_err(|e| {
			ParseError::new(
				ParseErrorKind::InvalidToken(lexer::Error::DateTime(e)),
				lexer.current_span(),
			)
		})
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
}

pub fn duration(input: &str) -> Result<Duration, Error> {
	dbg!(input);
	let mut lexer = Lexer::new(input.as_bytes());
	lexer
		.lex_only_duration()
		.map_err(|e| ParseError::new(ParseErrorKind::InvalidToken(e), lexer.current_span()))
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
}

pub fn range(input: &str) -> Result<Range, Error> {
	dbg!(input);
	let mut parser = Parser::new(input.as_bytes());
	parser.parse_range().map_err(|e| e.render_on(input)).map_err(Error::InvalidQuery)
}

pub fn thing(input: &str) -> Result<Thing, Error> {
	dbg!(input);
	let mut parser = Parser::new(input.as_bytes());
	parser.parse_thing().map_err(|e| e.render_on(input)).map_err(Error::InvalidQuery)
}
