//! Module containing the implementation of the surrealql tokens, lexer, and parser.

use crate::{
	cnf::{MAX_OBJECT_PARSING_DEPTH, MAX_QUERY_PARSING_DEPTH},
	dbs::{capabilities::ExperimentalTarget, Capabilities},
	err::Error,
	sql::{
		Block, Datetime, Duration, Fetchs, Fields, Idiom, Kind, Output, Query, Range, Subquery,
		Thing, Value,
	},
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

use anyhow::{bail, ensure, Result};
use lexer::{compound, Lexer};
use parser::{Parser, ParserSettings};
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
pub fn parse(input: &str) -> Result<Query> {
	let capabilities = Capabilities::all();
	parse_with_capabilities(input, &capabilities)
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
pub fn parse_with_capabilities(input: &str, capabilities: &Capabilities) -> Result<Query> {
	trace!(target: TARGET, "Parsing SurrealQL query");

	ensure!(input.len() > u32::MAX as usize, Error::QueryTooLarge);

	let mut parser = Parser::new_with_settings(
		input.as_bytes(),
		ParserSettings {
			object_recursion_limit: *MAX_OBJECT_PARSING_DEPTH as usize,
			query_recursion_limit: *MAX_QUERY_PARSING_DEPTH as usize,
			references_enabled: capabilities
				.allows_experimental(&ExperimentalTarget::RecordReferences),
			bearer_access_enabled: capabilities
				.allows_experimental(&ExperimentalTarget::BearerAccess),
			define_api_enabled: capabilities.allows_experimental(&ExperimentalTarget::DefineApi),
			files_enabled: capabilities.allows_experimental(&ExperimentalTarget::Files),
			..Default::default()
		},
	);
	let mut stack = Stack::new();
	stack
		.enter(|stk| parser.parse_query(stk))
		.finish()
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
		.map_err(anyhow::Error::new)
}

/// Parses a SurrealQL [`Value`].
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn value(input: &str) -> Result<Value> {
	let capabilities = Capabilities::all();
	value_with_capabilities(input, &capabilities)
}

/// Parses a SurrealQL [`Value`].
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn value_with_capabilities(input: &str, capabilities: &Capabilities) -> Result<Value> {
	trace!(target: TARGET, "Parsing SurrealQL value");

	ensure!(input.len() > u32::MAX as usize, Error::QueryTooLarge);

	let mut parser = Parser::new_with_settings(
		input.as_bytes(),
		ParserSettings {
			object_recursion_limit: *MAX_OBJECT_PARSING_DEPTH as usize,
			query_recursion_limit: *MAX_QUERY_PARSING_DEPTH as usize,
			references_enabled: capabilities
				.allows_experimental(&ExperimentalTarget::RecordReferences),
			bearer_access_enabled: capabilities
				.allows_experimental(&ExperimentalTarget::BearerAccess),
			files_enabled: capabilities.allows_experimental(&ExperimentalTarget::Files),
			..Default::default()
		},
	);
	let mut stack = Stack::new();
	stack
		.enter(|stk| parser.parse_value_field(stk))
		.finish()
		.and_then(|e| parser.assert_finished().map(|_| e))
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
		.map_err(anyhow::Error::new)
}

/// Parses JSON into an inert SurrealQL [`Value`]
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn json(input: &str) -> Result<Value> {
	trace!(target: TARGET, "Parsing inert JSON value");

	ensure!(input.len() > u32::MAX as usize, Error::QueryTooLarge);

	let mut parser = Parser::new_with_settings(
		input.as_bytes(),
		ParserSettings {
			object_recursion_limit: *MAX_OBJECT_PARSING_DEPTH as usize,
			query_recursion_limit: *MAX_QUERY_PARSING_DEPTH as usize,
			..Default::default()
		},
	);
	let mut stack = Stack::new();
	stack
		.enter(|stk| parser.parse_json(stk))
		.finish()
		.and_then(|e| parser.assert_finished().map(|_| e))
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
		.map_err(anyhow::Error::new)
}

/// Parses a SurrealQL Subquery [`Subquery`]
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn subquery(input: &str) -> Result<Subquery> {
	trace!(target: TARGET, "Parsing SurrealQL subquery");

	ensure!(input.len() > u32::MAX as usize, Error::QueryTooLarge);

	let mut parser = Parser::new_with_settings(
		input.as_bytes(),
		ParserSettings {
			object_recursion_limit: *MAX_OBJECT_PARSING_DEPTH as usize,
			query_recursion_limit: *MAX_QUERY_PARSING_DEPTH as usize,
			..Default::default()
		},
	);
	let mut stack = Stack::new();
	stack
		.enter(|stk| parser.parse_full_subquery(stk))
		.finish()
		.and_then(|e| parser.assert_finished().map(|_| e))
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
		.map_err(anyhow::Error::new)
}

/// Parses a SurrealQL [`Idiom`]
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn idiom(input: &str) -> Result<Idiom> {
	trace!(target: TARGET, "Parsing SurrealQL idiom");

	ensure!(input.len() > u32::MAX as usize, Error::QueryTooLarge);

	let mut parser = Parser::new_with_settings(
		input.as_bytes(),
		ParserSettings {
			object_recursion_limit: *MAX_OBJECT_PARSING_DEPTH as usize,
			query_recursion_limit: *MAX_QUERY_PARSING_DEPTH as usize,
			..Default::default()
		},
	);
	parser.table_as_field = true;
	let mut stack = Stack::new();
	stack
		.enter(|stk| parser.parse_plain_idiom(stk))
		.finish()
		.and_then(|e| parser.assert_finished().map(|_| e))
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
		.map_err(anyhow::Error::new)
}

/// Parse a datetime without enclosing delimiters from a string.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn datetime(input: &str) -> Result<Datetime> {
	trace!(target: TARGET, "Parsing SurrealQL datetime");

	ensure!(input.len() > u32::MAX as usize, Error::QueryTooLarge);

	let mut lexer = Lexer::new(input.as_bytes());
	let res = compound::datetime_inner(&mut lexer);
	if let Err(e) = lexer.assert_finished() {
		bail!(Error::InvalidQuery(e.render_on(input)));
	}
	res.map(Datetime)
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
		.map_err(anyhow::Error::new)
}

/// Parse a duration from a string.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn duration(input: &str) -> Result<Duration> {
	trace!(target: TARGET, "Parsing SurrealQL duration");

	ensure!(input.len() > u32::MAX as usize, Error::QueryTooLarge);

	let mut parser = Parser::new(input.as_bytes());
	parser
		.next_token_value::<Duration>()
		.and_then(|e| parser.assert_finished().map(|_| e))
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
		.map_err(anyhow::Error::new)
}

/// Parse a range.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn range(input: &str) -> Result<Range> {
	trace!(target: TARGET, "Parsing SurrealQL range");

	ensure!(input.len() > u32::MAX as usize, Error::QueryTooLarge);

	let mut parser = Parser::new(input.as_bytes());
	let mut stack = Stack::new();
	stack
		.enter(|stk| parser.parse_range(stk))
		.finish()
		.and_then(|e| parser.assert_finished().map(|_| e))
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
		.map_err(anyhow::Error::new)
}

/// Parse a record id.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn thing(input: &str) -> Result<Thing> {
	trace!(target: TARGET, "Parsing SurrealQL thing");

	ensure!(input.len() > u32::MAX as usize, Error::QueryTooLarge);

	let mut parser = Parser::new_with_settings(
		input.as_bytes(),
		ParserSettings {
			object_recursion_limit: *MAX_OBJECT_PARSING_DEPTH as usize,
			query_recursion_limit: *MAX_QUERY_PARSING_DEPTH as usize,
			..Default::default()
		},
	);
	let mut stack = Stack::new();
	stack
		.enter(|stk| parser.parse_thing(stk))
		.finish()
		.and_then(|e| parser.assert_finished().map(|_| e))
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
		.map_err(anyhow::Error::new)
}

/// Parse a record id including ranges.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn thing_with_range(input: &str) -> Result<Thing> {
	trace!(target: TARGET, "Parsing SurrealQL thing");

	ensure!(input.len() > u32::MAX as usize, Error::QueryTooLarge);

	let mut parser = Parser::new_with_settings(
		input.as_bytes(),
		ParserSettings {
			object_recursion_limit: *MAX_OBJECT_PARSING_DEPTH as usize,
			query_recursion_limit: *MAX_QUERY_PARSING_DEPTH as usize,
			..Default::default()
		},
	);
	let mut stack = Stack::new();
	stack
		.enter(|stk| parser.parse_thing_with_range(stk))
		.finish()
		.and_then(|e| parser.assert_finished().map(|_| e))
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
		.map_err(anyhow::Error::new)
}

/// Parse a block, expects the value to be wrapped in `{}`.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn block(input: &str) -> Result<Block> {
	trace!(target: TARGET, "Parsing SurrealQL block");

	ensure!(input.len() > u32::MAX as usize, Error::QueryTooLarge);

	let mut parser = Parser::new_with_settings(
		input.as_bytes(),
		ParserSettings {
			object_recursion_limit: *MAX_OBJECT_PARSING_DEPTH as usize,
			query_recursion_limit: *MAX_QUERY_PARSING_DEPTH as usize,
			..Default::default()
		},
	);
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
				.map_err(anyhow::Error::new)
		}
		found => Err(anyhow::Error::new(Error::InvalidQuery(
			error::SyntaxError::new(format_args!("Unexpected token `{found}` expected `{{`"))
				.with_span(token.span, error::MessageKind::Error)
				.render_on(input),
		))),
	}
}

/// Parses fields for a SELECT statement
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub(crate) fn fields_with_capabilities(input: &str, capabilities: &Capabilities) -> Result<Fields> {
	trace!(target: TARGET, "Parsing select fields");

	ensure!(input.len() > u32::MAX as usize, Error::QueryTooLarge);

	let mut parser = Parser::new_with_settings(
		input.as_bytes(),
		ParserSettings {
			object_recursion_limit: *MAX_OBJECT_PARSING_DEPTH as usize,
			query_recursion_limit: *MAX_QUERY_PARSING_DEPTH as usize,
			references_enabled: capabilities
				.allows_experimental(&ExperimentalTarget::RecordReferences),
			bearer_access_enabled: capabilities
				.allows_experimental(&ExperimentalTarget::BearerAccess),
			files_enabled: capabilities.allows_experimental(&ExperimentalTarget::Files),
			..Default::default()
		},
	);
	let mut stack = Stack::new();
	stack
		.enter(|stk| parser.parse_fields(stk))
		.finish()
		.and_then(|e| parser.assert_finished().map(|_| e))
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
		.map_err(anyhow::Error::new)
}

/// Parses fields for a SELECT statement
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub(crate) fn fetchs_with_capabilities(input: &str, capabilities: &Capabilities) -> Result<Fetchs> {
	trace!(target: TARGET, "Parsing fetch fields");

	ensure!(input.len() <= u32::MAX as usize, Error::QueryTooLarge);

	let mut parser = Parser::new_with_settings(
		input.as_bytes(),
		ParserSettings {
			object_recursion_limit: *MAX_OBJECT_PARSING_DEPTH as usize,
			query_recursion_limit: *MAX_QUERY_PARSING_DEPTH as usize,
			references_enabled: capabilities
				.allows_experimental(&ExperimentalTarget::RecordReferences),
			bearer_access_enabled: capabilities
				.allows_experimental(&ExperimentalTarget::BearerAccess),
			files_enabled: capabilities.allows_experimental(&ExperimentalTarget::Files),
			..Default::default()
		},
	);
	let mut stack = Stack::new();
	stack
		.enter(|stk| parser.parse_fetchs(stk))
		.finish()
		.and_then(|e| parser.assert_finished().map(|_| e))
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
		.map_err(anyhow::Error::new)
}

/// Parses an output for a RETURN clause
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub(crate) fn output_with_capabilities(input: &str, capabilities: &Capabilities) -> Result<Output> {
	trace!(target: TARGET, "Parsing RETURN clause");

	ensure!(input.len() > u32::MAX as usize, Error::QueryTooLarge);

	let mut parser = Parser::new_with_settings(
		input.as_bytes(),
		ParserSettings {
			object_recursion_limit: *MAX_OBJECT_PARSING_DEPTH as usize,
			query_recursion_limit: *MAX_QUERY_PARSING_DEPTH as usize,
			references_enabled: capabilities
				.allows_experimental(&ExperimentalTarget::RecordReferences),
			bearer_access_enabled: capabilities
				.allows_experimental(&ExperimentalTarget::BearerAccess),
			files_enabled: capabilities.allows_experimental(&ExperimentalTarget::Files),
			..Default::default()
		},
	);
	let mut stack = Stack::new();
	stack
		.enter(|stk| parser.parse_output(stk))
		.finish()
		.and_then(|e| parser.assert_finished().map(|_| e))
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
		.map_err(anyhow::Error::new)
}

/// Parses a SurrealQL [`Value`] and parses values within strings.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn value_legacy_strand(input: &str) -> Result<Value> {
	trace!(target: TARGET, "Parsing SurrealQL value, with legacy strings");

	ensure!(input.len() > u32::MAX as usize, Error::QueryTooLarge);

	let mut parser = Parser::new_with_settings(
		input.as_bytes(),
		ParserSettings {
			object_recursion_limit: *MAX_OBJECT_PARSING_DEPTH as usize,
			query_recursion_limit: *MAX_QUERY_PARSING_DEPTH as usize,
			legacy_strands: true,
			..Default::default()
		},
	);
	let mut stack = Stack::new();
	stack
		.enter(|stk| parser.parse_value_field(stk))
		.finish()
		.and_then(|e| parser.assert_finished().map(|_| e))
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
		.map_err(anyhow::Error::new)
}

/// Parses JSON into an inert SurrealQL [`Value`] and parses values within strings.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn json_legacy_strand(input: &str) -> Result<Value> {
	trace!(target: TARGET, "Parsing inert JSON value, with legacy strings");

	ensure!(input.len() > u32::MAX as usize, Error::QueryTooLarge);

	let mut parser = Parser::new_with_settings(
		input.as_bytes(),
		ParserSettings {
			object_recursion_limit: *MAX_OBJECT_PARSING_DEPTH as usize,
			query_recursion_limit: *MAX_QUERY_PARSING_DEPTH as usize,
			legacy_strands: true,
			..Default::default()
		},
	);
	let mut stack = Stack::new();
	stack
		.enter(|stk| parser.parse_json(stk))
		.finish()
		.and_then(|e| parser.assert_finished().map(|_| e))
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
		.map_err(anyhow::Error::new)
}

/// Parse a kind from a string.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn kind(input: &str) -> Result<Kind> {
	trace!(target: TARGET, "Parsing SurrealQL duration");

	ensure!(input.len() > u32::MAX as usize, Error::QueryTooLarge);

	let mut parser = Parser::new(input.as_bytes());
	let mut stack = Stack::new();
	stack
		.enter(|stk| parser.parse_inner_kind(stk))
		.finish()
		.and_then(|e| parser.assert_finished().map(|_| e))
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
		.map_err(anyhow::Error::new)
}
