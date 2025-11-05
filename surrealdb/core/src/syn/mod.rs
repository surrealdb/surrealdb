//! Module containing the implementation of the surrealql tokens, lexer, and
//! parser.

use std::collections::HashSet;

use crate::cnf::{MAX_OBJECT_PARSING_DEPTH, MAX_QUERY_PARSING_DEPTH};
use crate::dbs::Capabilities;
use crate::dbs::capabilities::ExperimentalTarget;
use crate::err::Error;
use crate::sql::kind::KindLiteral;
use crate::sql::{Ast, Block, Expr, Fetchs, Fields, Function, Idiom, Kind, Output, RecordIdLit};
use crate::types::{PublicDatetime, PublicDuration, PublicRecordId, PublicValue};

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
use lexer::Lexer;
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
	F: AsyncFnOnce(&mut Parser<'_>, &mut Stk) -> ParseResult<R>,
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
		define_api_enabled: cap.allows_experimental(&ExperimentalTarget::DefineApi),
		files_enabled: cap.allows_experimental(&ExperimentalTarget::Files),
		surrealism_enabled: cap.allows_experimental(&ExperimentalTarget::Surrealism),
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

/// Parses a SurrealQL [`Expr`].
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub(crate) fn expr(input: &str) -> Result<Expr> {
	let capabilities = Capabilities::all();
	expr_with_capabilities(input, &capabilities)
}

/// Validates a SurrealQL [`Expr`]
pub fn validate_expr(input: &str) -> Result<()> {
	expr(input)?;
	Ok(())
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

/// Parses a SurrealQL [`Value`].
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn function(input: &str) -> Result<Function> {
	let capabilities = Capabilities::all();
	function_with_capabilities(input, &capabilities)
}

/// Parses a SurrealQL [`Value`].
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn function_with_capabilities(input: &str, capabilities: &Capabilities) -> Result<Function> {
	trace!(target: TARGET, "Parsing SurrealQL function name");

	parse_with_settings(
		input.as_bytes(),
		settings_from_capabilities(capabilities),
		async |parser, _stk| parser.parse_function_name().await,
	)
}

/// Parses JSON into an inert SurrealQL [`Value`]
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn json(input: &str) -> Result<PublicValue> {
	trace!(target: TARGET, "Parsing inert JSON value");

	parse_with(input.as_bytes(), async |parser, stk| parser.parse_json(stk).await)
}

/// Parses a SurrealQL [`Idiom`]
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub(crate) fn idiom(input: &str) -> Result<Idiom> {
	trace!(target: TARGET, "Parsing SurrealQL idiom");

	parse_with(input.as_bytes(), async |parser, stk| parser.parse_plain_idiom(stk).await)
}

/// Validates a SurrealQL [`Idiom`]
pub fn validate_idiom(input: &str) -> Result<()> {
	idiom(input)?;
	Ok(())
}

/// Parse a datetime without enclosing delimiters from a string.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn datetime(input: &str) -> Result<PublicDatetime> {
	trace!(target: TARGET, "Parsing SurrealQL datetime");

	ensure!(input.len() <= u32::MAX as usize, Error::QueryTooLarge);

	match Lexer::lex_datetime(input) {
		Ok(x) => Ok(x),
		Err(e) => {
			bail!(Error::InvalidQuery(e.render_on(input)))
		}
	}
}

/// Parse a duration from a string.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn duration(input: &str) -> Result<PublicDuration> {
	trace!(target: TARGET, "Parsing SurrealQL duration");

	ensure!(input.len() <= u32::MAX as usize, Error::QueryTooLarge);

	let mut parser = Parser::new(input.as_bytes());
	parser
		.next_token_value::<PublicDuration>()
		.and_then(|e| parser.assert_finished().map(|_| e))
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
		.map_err(anyhow::Error::new)
}

/// Parse a record id.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn record_id(input: &str) -> Result<PublicRecordId> {
	trace!(target: TARGET, "Parsing SurrealQL record id");

	parse_with(input.as_bytes(), async |parser, stk| parser.parse_value_record_id(stk).await)
}

/// Parse a record id including ranges.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn record_id_with_range(input: &str) -> Result<RecordIdLit> {
	trace!(target: TARGET, "Parsing SurrealQL record id with range");

	parse_with(input.as_bytes(), async |parser, stk| parser.parse_record_id_with_range(stk).await)
}

/// Parse a table name from a string.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn table(input: &str) -> Result<crate::val::Table> {
	trace!(target: TARGET, "Parsing SurrealQL table name");

	parse_with(input.as_bytes(), async |parser, _stk| {
		let ident = parser.parse_ident()?;
		Ok(crate::val::Table::new(ident))
	})
}

/// Parse a block, expects the value to be wrapped in `{}`.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn block(input: &str) -> Result<Block> {
	trace!(target: TARGET, "Parsing SurrealQL block");

	parse_with_settings(
		input.as_bytes(),
		ParserSettings {
			legacy_strands: true,
			flexible_record_id: true,
			references_enabled: true,
			define_api_enabled: true,
			files_enabled: true,
			surrealism_enabled: true,
			..Default::default()
		},
		async |parser, stk| {
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
		},
	)
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
pub fn value(input: &str) -> Result<PublicValue> {
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
pub fn value_legacy_strand(input: &str) -> Result<PublicValue> {
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
pub fn json_legacy_strand(input: &str) -> Result<PublicValue> {
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

/// Extracts the tables from the given kind definition string.
///
/// Note: This is only used by surrealql.wasm for use in Surrealist.
///
/// # Examples
///
/// ```
/// let tables = extract_tables_from_kind("record<users | posts>");
/// assert_eq!(tables, vec!["posts", "users"]);
/// ```
#[doc(hidden)]
pub fn extract_tables_from_kind(sql: &str) -> Result<Vec<String>> {
	let kind = kind(sql)?;
	let mut found_tables = HashSet::new();
	extract_tables_from_kind_impl(&kind, &mut found_tables);

	let mut tables_sorted: Vec<String> = found_tables.into_iter().collect();
	tables_sorted.sort();

	Ok(tables_sorted)
}

fn extract_tables_from_kind_impl(kind: &Kind, tables: &mut HashSet<String>) {
	match kind {
		Kind::Any
		| Kind::None
		| Kind::Null
		| Kind::Bool
		| Kind::Bytes
		| Kind::Datetime
		| Kind::Decimal
		| Kind::Duration
		| Kind::Float
		| Kind::Int
		| Kind::Number
		| Kind::Object
		| Kind::String
		| Kind::Uuid
		| Kind::Regex
		| Kind::Geometry(_) => {}
		Kind::Table(ts) => {
			for table in ts {
				tables.insert(table.clone());
			}
		}
		Kind::Record(ts) => {
			for table in ts {
				tables.insert(table.clone());
			}
		}
		Kind::Either(kinds) => {
			for kind in kinds {
				extract_tables_from_kind_impl(kind, tables);
			}
		}
		Kind::Set(kind, _) => {
			extract_tables_from_kind_impl(kind, tables);
		}
		Kind::Array(kind, _) => {
			extract_tables_from_kind_impl(kind, tables);
		}
		Kind::Function(_, _) => {}
		Kind::Range => {}
		Kind::Literal(literal) => match literal {
			KindLiteral::Array(kinds) => {
				for kind in kinds {
					extract_tables_from_kind_impl(kind, tables);
				}
			}
			KindLiteral::Object(kinds) => {
				for kind in kinds.values() {
					extract_tables_from_kind_impl(kind, tables);
				}
			}
			_ => {}
		},
		Kind::File(_) => {}
	}
}

#[cfg(test)]
mod tests {
	use rstest::rstest;

	use super::*;

	#[rstest]
	#[case::record("record", vec![])]
	#[case::record("record<users>", vec!["users"])]
	#[case::record("record<users | posts>", vec!["posts", "users"])]
	#[case::record("record<users | posts | users>", vec!["posts", "users"])]
	#[case::table("table", vec![])]
	#[case::table("table<users>", vec!["users"])]
	#[case::option("option<record<users>>", vec!["users"])]
	#[case::array("array<record<users>>", vec!["users"])]
	#[case::nested_array("array<array<record<users>>>", vec!["users"])]
	#[case::either("record<users> | record<posts>", vec!["posts", "users"])]
	#[case::complex("record<a> | table<b> | array<record<c | d> | record<e>>", vec!["a", "b", "c", "d", "e"])]
	fn test_extract_tables_from_expr(
		#[case] sql: &str,
		#[case] expected_tables: Vec<&'static str>,
	) {
		let expected_tables: Vec<String> =
			expected_tables.into_iter().map(|s| s.to_string()).collect();
		let extracted = extract_tables_from_kind(sql).unwrap();
		assert_eq!(extracted, expected_tables);
	}
}
