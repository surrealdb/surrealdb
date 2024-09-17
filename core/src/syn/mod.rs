//! Module containing the implementation of the surrealql tokens, lexer, and parser.

use crate::{
	cnf::{MAX_OBJECT_PARSING_DEPTH, MAX_QUERY_PARSING_DEPTH},
	err::Error,
	sql::{Block, Datetime, Duration, Idiom, Query, Range, Subquery, Thing, Value},
};

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

use lexer::{compound, Lexer};
use parser::Parser;
use reblessive::Stack;
use token::t;

const TARGET: &str = "surrealdb::core::syn";

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
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn parse(input: &str) -> Result<Query, Error> {
	trace!(target: TARGET, "Parsing SurrealQL query");
	let mut parser = Parser::new(input.as_bytes())
		.with_object_recursion_limit(*MAX_OBJECT_PARSING_DEPTH as usize)
		.with_query_recursion_limit(*MAX_QUERY_PARSING_DEPTH as usize);
	let mut stack = Stack::new();
	stack
		.enter(|stk| parser.parse_query(stk))
		.finish()
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
}

/// Parses a SurrealQL [`Value`].
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn value(input: &str) -> Result<Value, Error> {
	trace!(target: TARGET, "Parsing SurrealQL value");
	let mut parser = Parser::new(input.as_bytes())
		.with_object_recursion_limit(*MAX_OBJECT_PARSING_DEPTH as usize)
		.with_query_recursion_limit(*MAX_QUERY_PARSING_DEPTH as usize);
	let mut stack = Stack::new();
	stack
		.enter(|stk| parser.parse_value_table(stk))
		.finish()
		.and_then(|e| parser.assert_finished().map(|_| e))
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
}

/// Parses JSON into an inert SurrealQL [`Value`]
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn json(input: &str) -> Result<Value, Error> {
	trace!(target: TARGET, "Parsing inert JSON value");
	let mut parser = Parser::new(input.as_bytes())
		.with_object_recursion_limit(*MAX_OBJECT_PARSING_DEPTH as usize)
		.with_query_recursion_limit(*MAX_QUERY_PARSING_DEPTH as usize);
	let mut stack = Stack::new();
	stack
		.enter(|stk| parser.parse_json(stk))
		.finish()
		.and_then(|e| parser.assert_finished().map(|_| e))
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
}

/// Parses a SurrealQL Subquery [`Subquery`]
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn subquery(input: &str) -> Result<Subquery, Error> {
	trace!(target: TARGET, "Parsing SurrealQL subquery");
	let mut parser = Parser::new(input.as_bytes())
		.with_object_recursion_limit(*MAX_OBJECT_PARSING_DEPTH as usize)
		.with_query_recursion_limit(*MAX_QUERY_PARSING_DEPTH as usize);
	let mut stack = Stack::new();
	stack
		.enter(|stk| parser.parse_full_subquery(stk))
		.finish()
		.and_then(|e| parser.assert_finished().map(|_| e))
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
}

/// Parses a SurrealQL [`Idiom`]
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn idiom(input: &str) -> Result<Idiom, Error> {
	trace!(target: TARGET, "Parsing SurrealQL idiom");
	let mut parser = Parser::new(input.as_bytes())
		.with_object_recursion_limit(*MAX_OBJECT_PARSING_DEPTH as usize)
		.with_query_recursion_limit(*MAX_QUERY_PARSING_DEPTH as usize);
	parser.table_as_field = true;
	let mut stack = Stack::new();
	stack
		.enter(|stk| parser.parse_plain_idiom(stk))
		.finish()
		.and_then(|e| parser.assert_finished().map(|_| e))
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
}

/// Parse a datetime without enclosing delimiters from a string.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn datetime(input: &str) -> Result<Datetime, Error> {
	trace!(target: TARGET, "Parsing SurrealQL datetime");
	let mut lexer = Lexer::new(input.as_bytes());
	let res = compound::datetime_inner(&mut lexer);
	if let Err(e) = lexer.assert_finished() {
		return Err(Error::InvalidQuery(e.render_on(input)));
	}
	res.map(Datetime).map_err(|e| e.render_on(input)).map_err(Error::InvalidQuery)
}

/// Parse a duration from a string.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn duration(input: &str) -> Result<Duration, Error> {
	trace!(target: TARGET, "Parsing SurrealQL duration");
	let mut parser = Parser::new(input.as_bytes());
	parser
		.next_token_value::<Duration>()
		.and_then(|e| parser.assert_finished().map(|_| e))
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
}

/// Parse a range.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn range(input: &str) -> Result<Range, Error> {
	trace!(target: TARGET, "Parsing SurrealQL range");
	let mut parser = Parser::new(input.as_bytes());
	let mut stack = Stack::new();
	stack
		.enter(|stk| parser.parse_range(stk))
		.finish()
		.and_then(|e| parser.assert_finished().map(|_| e))
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
}

/// Parse a record id.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn thing(input: &str) -> Result<Thing, Error> {
	trace!(target: TARGET, "Parsing SurrealQL thing");
	let mut parser = Parser::new(input.as_bytes())
		.with_object_recursion_limit(*MAX_OBJECT_PARSING_DEPTH as usize)
		.with_query_recursion_limit(*MAX_QUERY_PARSING_DEPTH as usize);
	let mut stack = Stack::new();
	stack
		.enter(|stk| parser.parse_thing(stk))
		.finish()
		.and_then(|e| parser.assert_finished().map(|_| e))
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
}

/// Parse a block, expects the value to be wrapped in `{}`.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn block(input: &str) -> Result<Block, Error> {
	trace!(target: TARGET, "Parsing SurrealQL block");
	let mut parser = Parser::new(input.as_bytes())
		.with_object_recursion_limit(*MAX_OBJECT_PARSING_DEPTH as usize)
		.with_query_recursion_limit(*MAX_QUERY_PARSING_DEPTH as usize);
	let mut stack = Stack::new();
	let token = parser.peek();
	match token.kind {
		t!("{") => {
			let start = parser.pop_peek().span;
			stack
				.enter(|stk| parser.parse_block(stk, start))
				.finish()
				.and_then(|e| parser.assert_finished().map(|_| e))
				.map_err(|e| e.render_on(input))
				.map_err(Error::InvalidQuery)
		}
		found => Err(Error::InvalidQuery(
			error::SyntaxError::new(format_args!("Unexpected token `{found}` expected `{{`"))
				.with_span(token.span, error::MessageKind::Error)
				.render_on(input),
		)),
	}
}

/// Parses a SurrealQL [`Value`] and parses values within strings.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn value_legacy_strand(input: &str) -> Result<Value, Error> {
	trace!(target: TARGET, "Parsing SurrealQL value, with legacy strings");
	let mut parser = Parser::new(input.as_bytes())
		.with_object_recursion_limit(*MAX_OBJECT_PARSING_DEPTH as usize)
		.with_query_recursion_limit(*MAX_QUERY_PARSING_DEPTH as usize);
	let mut stack = Stack::new();
	parser.allow_legacy_strand(true);
	stack
		.enter(|stk| parser.parse_value_table(stk))
		.finish()
		.and_then(|e| parser.assert_finished().map(|_| e))
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
}

/// Parses JSON into an inert SurrealQL [`Value`] and parses values within strings.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn json_legacy_strand(input: &str) -> Result<Value, Error> {
	trace!(target: TARGET, "Parsing inert JSON value, with legacy strings");
	let mut parser = Parser::new(input.as_bytes())
		.with_object_recursion_limit(*MAX_OBJECT_PARSING_DEPTH as usize)
		.with_query_recursion_limit(*MAX_QUERY_PARSING_DEPTH as usize);
	let mut stack = Stack::new();
	parser.allow_legacy_strand(true);
	stack
		.enter(|stk| parser.parse_json(stk))
		.finish()
		.and_then(|e| parser.assert_finished().map(|_| e))
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
}
