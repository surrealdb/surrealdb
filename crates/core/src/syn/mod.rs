//! Module containing the implementation of the surrealql tokens, lexer, and
//! parser.

use crate::cnf::{MAX_OBJECT_PARSING_DEPTH, MAX_QUERY_PARSING_DEPTH};
use crate::dbs::Capabilities;
use crate::dbs::capabilities::ExperimentalTarget;
use crate::err::Error;
use crate::sql::{Ast, Block, Expr, Fetchs, Fields, Idiom, Kind, Output, RecordIdLit};
use crate::val::{Datetime, Duration, RecordId, Value};

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

use anyhow::{Result, bail, ensure};
use lexer::{Lexer, compound};
use parser::{ParseResult, Parser, ParserSettings};
use reblessive::{Stack, Stk};
use token::t;

const TARGET: &str = "surrealdb::core::syn";

/// Takes a string and returns if it could be a reserved keyword in certain
/// contexts.
pub fn could_be_reserved_keyword(s: &str) -> bool {
	lexer::keywords::could_be_reserved(s)
}

pub fn parse_with<F, R>(input: &[u8], f: F) -> Result<R>
where
	F: AsyncFnOnce(&mut Parser, &mut Stk) -> ParseResult<R>,
{
	parse_with_settings(input, settings_from_capabilities(&Capabilities::all()), f)
}

pub fn parse_with_settings<F, R>(input: &[u8], settings: ParserSettings, f: F) -> Result<R>
where
	F: for<'a> AsyncFnOnce(&'a mut Parser<'a>, &'a mut Stk) -> ParseResult<R>,
{
	ensure!(input.len() <= u32::MAX as usize, Error::QueryTooLarge);
	let mut parser = Parser::new_with_settings(input, settings);
	let mut stack = Stack::new();
	stack
		.enter(|stk| f(&mut parser, stk))
		.finish()
		.map_err(|e| e.render_on_bytes(input))
		.map_err(Error::InvalidQuery)
		.map_err(anyhow::Error::new)
}

/// Creates the parser settings struct from the global configuration values as
/// wel as the capabilities  struct.
pub fn settings_from_capabilities(cap: &Capabilities) -> ParserSettings {
	ParserSettings {
		object_recursion_limit: *MAX_OBJECT_PARSING_DEPTH as usize,
		query_recursion_limit: *MAX_QUERY_PARSING_DEPTH as usize,
		references_enabled: cap.allows_experimental(&ExperimentalTarget::RecordReferences),
		bearer_access_enabled: cap.allows_experimental(&ExperimentalTarget::BearerAccess),
		define_api_enabled: cap.allows_experimental(&ExperimentalTarget::DefineApi),
		files_enabled: cap.allows_experimental(&ExperimentalTarget::Files),
		..Default::default()
	}
}

/// Parses a SurrealQL [`Query`]
///
/// During query parsing, the total depth of calls to parse values (including
/// arrays, expressions, functions, objects, sub-queries), Javascript values,
/// and geometry collections count against a computation depth limit. If the
/// limit is reached, parsing will return [`Error::ComputationDepthExceeded`],
/// as opposed to spending more time and potentially overflowing the call stack.
///
/// If you encounter this limit and believe that it should be increased,
/// please [open an issue](https://github.com/surrealdb/surrealdb/issues)!
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn parse(input: &str) -> Result<Ast> {
	let capabilities = Capabilities::all();
	parse_with_capabilities(input, &capabilities)
}

/// Parses a SurrealQL [`Query`]
///
/// During query parsing, the total depth of calls to parse values (including
/// arrays, expressions, functions, objects, sub-queries), Javascript values,
/// and geometry collections count against a computation depth limit. If the
/// limit is reached, parsing will return [`Error::ComputationDepthExceeded`],
/// as opposed to spending more time and potentially overflowing the call stack.
///
/// If you encounter this limit and believe that it should be increased,
/// please [open an issue](https://github.com/surrealdb/surrealdb/issues)!
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn parse_with_capabilities(input: &str, capabilities: &Capabilities) -> Result<Ast> {
	trace!(target: TARGET, "Parsing SurrealQL query");

	parse_with_settings(
		input.as_bytes(),
		settings_from_capabilities(capabilities),
		async |parser, stk| parser.parse_query(stk).await,
	)
}

/// Parses a SurrealQL [`Value`].
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn expr(input: &str) -> Result<Expr> {
	let capabilities = Capabilities::all();
	expr_with_capabilities(input, &capabilities)
}

/// Parses a SurrealQL [`Value`].
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn expr_with_capabilities(input: &str, capabilities: &Capabilities) -> Result<Expr> {
	trace!(target: TARGET, "Parsing SurrealQL value");

	parse_with_settings(
		input.as_bytes(),
		settings_from_capabilities(capabilities),
		async |parser, stk| parser.parse_expr_field(stk).await,
	)
}

/// Parses JSON into an inert SurrealQL [`Value`]
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn json(input: &str) -> Result<Value> {
	trace!(target: TARGET, "Parsing inert JSON value");

	parse_with(input.as_bytes(), async |parser, stk| parser.parse_json(stk).await)
}

/// Parses a SurrealQL [`Idiom`]
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn idiom(input: &str) -> Result<Idiom> {
	trace!(target: TARGET, "Parsing SurrealQL idiom");

	parse_with(input.as_bytes(), async |parser, stk| parser.parse_plain_idiom(stk).await)
}

/// Parse a datetime without enclosing delimiters from a string.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn datetime(input: &str) -> Result<Datetime> {
	trace!(target: TARGET, "Parsing SurrealQL datetime");

	ensure!(input.len() <= u32::MAX as usize, Error::QueryTooLarge);

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

	ensure!(input.len() <= u32::MAX as usize, Error::QueryTooLarge);

	let mut parser = Parser::new(input.as_bytes());
	parser
		.next_token_value::<Duration>()
		.and_then(|e| parser.assert_finished().map(|_| e))
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
		.map_err(anyhow::Error::new)
}

/// Parse a record id.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn record_id(input: &str) -> Result<RecordId> {
	trace!(target: TARGET, "Parsing SurrealQL record id");

	parse_with(input.as_bytes(), async |parser, stk| parser.parse_value_record_id(stk).await)
}

/// Parse a record id including ranges.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn record_id_with_range(input: &str) -> Result<RecordIdLit> {
	trace!(target: TARGET, "Parsing SurrealQL record id with range");

	parse_with(input.as_bytes(), async |parser, stk| parser.parse_record_id_with_range(stk).await)
}

/// Parse a block, expects the value to be wrapped in `{}`.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn block(input: &str) -> Result<Block> {
	trace!(target: TARGET, "Parsing SurrealQL block");

	parse_with(input.as_bytes(), async |parser, stk| {
		let token = parser.peek();
		match token.kind {
			t!("{") => {
				let start = parser.pop_peek().span;
				parser.parse_block(stk, start).await
			}
			found => Err(error::SyntaxError::new(format_args!(
				"Unexpected token `{found}` expected `{{`"
			))
			.with_span(token.span, error::MessageKind::Error)),
		}
	})
}

/// Parses fields for a SELECT statement
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub(crate) fn fields_with_capabilities(input: &str, capabilities: &Capabilities) -> Result<Fields> {
	trace!(target: TARGET, "Parsing select fields");

	parse_with_settings(
		input.as_bytes(),
		settings_from_capabilities(capabilities),
		async |parser, stk| parser.parse_fields(stk).await,
	)
}

/// Parses fields for a SELECT statement
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub(crate) fn fetchs_with_capabilities(input: &str, capabilities: &Capabilities) -> Result<Fetchs> {
	trace!(target: TARGET, "Parsing fetch fields");

	parse_with_settings(
		input.as_bytes(),
		settings_from_capabilities(capabilities),
		async |parser, stk| parser.parse_fetchs(stk).await,
	)
}

/// Parses an output for a RETURN clause
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub(crate) fn output_with_capabilities(input: &str, capabilities: &Capabilities) -> Result<Output> {
	trace!(target: TARGET, "Parsing RETURN clause");

	ensure!(input.len() <= u32::MAX as usize, Error::QueryTooLarge);

	parse_with_settings(
		input.as_bytes(),
		settings_from_capabilities(capabilities),
		async |parser, stk| parser.parse_output(stk).await,
	)
}

/// Parses a SurrealQL [`Value`] and parses values within strings.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn expr_legacy_strand(input: &str) -> Result<Expr> {
	trace!(target: TARGET, "Parsing SurrealQL value, with legacy strings");

	let settings = ParserSettings {
		object_recursion_limit: *MAX_OBJECT_PARSING_DEPTH as usize,
		query_recursion_limit: *MAX_QUERY_PARSING_DEPTH as usize,
		legacy_strands: true,
		..Default::default()
	};

	parse_with_settings(input.as_bytes(), settings, async |parser, stk| {
		parser.parse_expr_field(stk).await
	})
}

/// Parses a SurrealQL [`Value`] and parses values within strings.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn value(input: &str) -> Result<Value> {
	trace!(target: TARGET, "Parsing SurrealQL value, with legacy strings");

	let settings = ParserSettings {
		object_recursion_limit: *MAX_OBJECT_PARSING_DEPTH as usize,
		query_recursion_limit: *MAX_QUERY_PARSING_DEPTH as usize,
		..Default::default()
	};

	parse_with_settings(input.as_bytes(), settings, async |parser, stk| {
		parser.parse_value(stk).await
	})
}

/// Parses a SurrealQL [`Value`] and parses values within strings.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn value_legacy_strand(input: &str) -> Result<Value> {
	trace!(target: TARGET, "Parsing SurrealQL value, with legacy strings");

	let settings = ParserSettings {
		object_recursion_limit: *MAX_OBJECT_PARSING_DEPTH as usize,
		query_recursion_limit: *MAX_QUERY_PARSING_DEPTH as usize,
		legacy_strands: true,
		..Default::default()
	};

	parse_with_settings(input.as_bytes(), settings, async |parser, stk| {
		parser.parse_value(stk).await
	})
}

/// Parses JSON into an inert SurrealQL [`Value`] and parses values within
/// strings.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn json_legacy_strand(input: &str) -> Result<Value> {
	trace!(target: TARGET, "Parsing inert JSON value, with legacy strings");

	let settings = ParserSettings {
		object_recursion_limit: *MAX_OBJECT_PARSING_DEPTH as usize,
		query_recursion_limit: *MAX_QUERY_PARSING_DEPTH as usize,
		legacy_strands: true,
		..Default::default()
	};

	parse_with_settings(input.as_bytes(), settings, async |parser, stk| {
		parser.parse_json(stk).await
	})
}

/// Parse a kind from a string.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn kind(input: &str) -> Result<Kind> {
	trace!(target: TARGET, "Parsing SurrealQL duration");

	parse_with(input.as_bytes(), async |parser, stk| parser.parse_inner_kind(stk).await)
}
