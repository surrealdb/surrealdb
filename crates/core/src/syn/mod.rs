//! Module containing the implementation of the surrealql tokens, lexer, and parser.

use crate::{
	err::Error,
	sql::{Block, Datetime, Duration, Idiom, Query, Range, Subquery, Thing, Value},
};

pub mod common;
pub mod error;
pub mod lexer;
pub mod parser;
pub mod token;

#[cfg(test)]
pub trait Parse<T> {
	fn parse(val: &str) -> T;
}

#[cfg(test)]
mod test;

use parser::Parser;
use reblessive::Stack;
use token::t;

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
	debug!("parsing value with legacy strings, input = {input}");
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
	debug!("parsing json with legacy strings, input = {input}");
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
	let mut parser = Parser::new(input.as_bytes());
	parser.parse_inner_datetime().map_err(|e| e.render_on(input)).map_err(Error::InvalidQuery)
}

/// Parse a duration from a string.
pub fn duration(input: &str) -> Result<Duration, Error> {
	debug!("parsing duration, input = {input}");
	let mut parser = Parser::new(input.as_bytes());
	parser
		.next_token_value::<Duration>()
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

/// Parse a block, expects the value to be wrapped in `{}`.
pub fn block(input: &str) -> Result<Block, Error> {
	debug!("parsing block, input = {input}");

	let mut parser = Parser::new(input.as_bytes());
	let mut stack = Stack::new();

	match parser.peek_kind() {
		t!("{") => {
			let start = parser.pop_peek().span;
			stack
				.enter(|stk| parser.parse_block(stk, start))
				.finish()
				.map_err(|e| e.render_on(input))
				.map_err(Error::InvalidQuery)
		}
		found => Err(Error::InvalidQuery(
			crate::syn::parser::ParseError::new(
				crate::syn::parser::ParseErrorKind::Unexpected {
					expected: "{",
					found,
				},
				parser.last_span(),
			)
			.render_on(input),
		)),
	}
}
