use crate::{
	err::Error,
	sql::{Datetime, Duration, Idiom, Query, Range, Subquery, Thing, Value},
};

pub mod lexer;
pub mod parser;
pub mod token;

#[cfg(test)]
mod test;

use lexer::Lexer;
use parser::{ParseError, ParseErrorKind, Parser};
use reblessive::Stack;

/// Takes a string and returns if it could be a reserved keyword in certain contexts.
pub fn could_be_reserved_keyword(s: &str) -> bool {
	lexer::keywords::could_be_reserved(s)
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
	debug!("parsing query, input = {input}");
	let mut parser = Parser::new(input.as_bytes());
	let mut stack = Stack::new();
	stack
		.enter(|stk| parser.parse_query(stk))
		.finish()
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
}

/// Parses a SurrealQL [`Value`].
#[instrument(level = "debug", name = "parser", skip_all, fields(length = input.len()))]
pub fn value(input: &str) -> Result<Value, Error> {
	debug!("parsing value, input = {input}");
	let mut parser = Parser::new(input.as_bytes());
	let mut stack = Stack::new();
	stack
		.enter(|stk| parser.parse_value_field(stk))
		.finish()
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
}

/// Parses a SurrealQL [`Value`].
#[instrument(level = "debug", name = "parser", skip_all, fields(length = input.len()))]
pub fn value_legacy_strand(input: &str) -> Result<Value, Error> {
	debug!("parsing value, input = {input}");
	let mut parser = Parser::new(input.as_bytes());
	let mut stack = Stack::new();
	parser.allow_legacy_strand(true);
	stack
		.enter(|stk| parser.parse_value(stk))
		.finish()
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
}

/// Parses JSON into an inert SurrealQL [`Value`]
#[instrument(level = "debug", name = "parser", skip_all, fields(length = input.len()))]
pub fn json(input: &str) -> Result<Value, Error> {
	debug!("parsing json, input = {input}");
	let mut parser = Parser::new(input.as_bytes());
	let mut stack = Stack::new();
	stack
		.enter(|stk| parser.parse_json(stk))
		.finish()
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
}

/// Parses JSON into an inert SurrealQL [`Value`]
#[instrument(level = "debug", name = "parser", skip_all, fields(length = input.len()))]
pub fn json_legacy_strand(input: &str) -> Result<Value, Error> {
	debug!("parsing json, input = {input}");
	let mut parser = Parser::new(input.as_bytes());
	let mut stack = Stack::new();
	parser.allow_legacy_strand(true);
	stack
		.enter(|stk| parser.parse_json(stk))
		.finish()
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
}
/// Parses a SurrealQL Subquery [`Subquery`]
#[instrument(level = "debug", name = "parser", skip_all, fields(length = input.len()))]
pub fn subquery(input: &str) -> Result<Subquery, Error> {
	debug!("parsing subquery, input = {input}");
	let mut parser = Parser::new(input.as_bytes());
	let mut stack = Stack::new();
	stack
		.enter(|stk| parser.parse_full_subquery(stk))
		.finish()
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
}

/// Parses a SurrealQL [`Idiom`]
#[instrument(level = "debug", name = "parser", skip_all, fields(length = input.len()))]
pub fn idiom(input: &str) -> Result<Idiom, Error> {
	debug!("parsing idiom, input = {input}");
	let mut parser = Parser::new(input.as_bytes());
	let mut stack = Stack::new();
	stack
		.enter(|stk| parser.parse_plain_idiom(stk))
		.finish()
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
}

/// Parse a datetime without enclosing delimiters from a string.
pub fn datetime_raw(input: &str) -> Result<Datetime, Error> {
	debug!("parsing datetime, input = {input}");
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

/// Parse a duration from a string.
pub fn duration(input: &str) -> Result<Duration, Error> {
	debug!("parsing duration, input = {input}");
	let mut lexer = Lexer::new(input.as_bytes());
	lexer
		.lex_only_duration()
		.map_err(|e| ParseError::new(ParseErrorKind::InvalidToken(e), lexer.current_span()))
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
}

/// Parse a range
pub fn range(input: &str) -> Result<Range, Error> {
	debug!("parsing range, input = {input}");
	let mut parser = Parser::new(input.as_bytes());
	let mut stack = Stack::new();
	stack
		.enter(|stk| parser.parse_range(stk))
		.finish()
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
}

/// Parse a record id.
pub fn thing(input: &str) -> Result<Thing, Error> {
	debug!("parsing thing, input = {input}");
	let mut parser = Parser::new(input.as_bytes());
	let mut stack = Stack::new();
	stack
		.enter(|stk| parser.parse_thing(stk))
		.finish()
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
}
