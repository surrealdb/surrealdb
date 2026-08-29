//! Module containing the implementation of the surrealql tokens, lexer, and
//! parser.

use std::collections::HashSet;

pub(crate) use surrealdb_syn::{ParseError, ParserConfig, lexer, parser};

use crate::dbs::Capabilities;
use crate::dbs::capabilities::ExperimentalTarget;
use crate::sql::kind::KindLiteral;
use crate::sql::{Ast, Block, Expr, Function, Idiom, Kind};
use crate::types::{PublicDatetime, PublicDuration, PublicRecordId, PublicValue};

#[cfg(test)]
pub trait Parse<T> {
	fn parse(val: &str) -> T;
}

#[cfg(test)]
mod format_test;
#[cfg(test)]
mod test;

use anyhow::{Result, bail, ensure};
use lexer::Lexer;
pub(crate) use parser::ParserSettings;
use parser::{ParseResult, Parser};
use reblessive::Stk;

const TARGET: &str = "surrealdb::core::syn";

/// Takes a string and returns if it could be a reserved keyword in certain
/// contexts.
pub fn could_be_reserved_keyword(s: &str) -> bool {
	surrealdb_syn::could_be_reserved_keyword(s)
}

pub fn parse_with<F, R>(input: &[u8], f: F) -> Result<R>
where
	F: AsyncFnOnce(&mut Parser<'_>, &mut Stk) -> ParseResult<R>,
{
	lift(surrealdb_syn::parse_with(input, f))
}

pub fn parse_with_settings<F, R>(input: &[u8], settings: ParserSettings, f: F) -> Result<R>
where
	F: for<'a> AsyncFnOnce(&'a mut Parser<'a>, &'a mut Stk) -> ParseResult<R>,
{
	lift(surrealdb_syn::parse_with_settings(input, settings, f))
}

/// Raises a parse failure into the `anyhow` chain unchanged.
///
/// [`ParseError`] is already the public form of a parse failure - core neither
/// reclassifies it nor restates its message - so this only changes the error
/// type, never its meaning.
fn lift<T>(result: std::result::Result<T, ParseError>) -> Result<T> {
	result.map_err(anyhow::Error::new)
}

/// Pretty-prints a SurrealQL script.
///
/// Statements are re-emitted from their parsed AST via
/// [`ToSql::to_sql_pretty`](surrealdb_types::ToSql::to_sql_pretty). Standalone
/// comment lines and single blank lines between statements are preserved, and
/// every line-comment marker (`--`, `//`, `#`) is normalized to `#`.
///
/// The AST carries no comments, so a statement whose own source contains a
/// comment is emitted verbatim rather than reflowed - this guarantees a comment
/// is never dropped, at the cost of not reformatting that statement. The output
/// ends with exactly one newline; empty input yields an empty string.
///
/// Returns a [`ParseError`] (lifted into `anyhow`) if the input is not valid
/// SurrealQL, leaving the caller free to keep the original source untouched.
pub fn format(input: &[u8]) -> Result<String> {
	use std::fmt::Write;

	use surrealdb_syn::token::{Span, Token, TokenKind};
	use surrealdb_types::ToSql;

	parse_with(input, async |parser, stk| {
		let mut output = String::new();
		let mut carry_token = Token::invalid();

		loop {
			let is_eof = parser.lex_compound(carry_token, |lexer, _| Ok(lexer.is_eof()))?;

			if is_eof.value {
				break;
			}

			let result = parser.lex_compound(carry_token, |lexer, _| format_line(lexer))?;

			carry_token = Token {
				kind: TokenKind::Invalid,
				span: result.span.after(),
			};

			// Anything sharing a line with earlier output is separated by a
			// single space so a trailing comment or statement never glues onto
			// the preceding token. Blank lines carry their own newline.
			if !matches!(result.value, FormatLine::Whitespace) {
				separate_same_line(&mut output);
			}

			match result.value {
				FormatLine::SingleComment(s) => _ = write!(&mut output, "#{s}"),
				FormatLine::MultiComment(s) => output.push_str(&s),
				FormatLine::Whitespace => output.push('\n'),
				FormatLine::Statement => {
					let start = parser.peek().span.offset;
					let parsed = stk.run(|ctx| parser.parse_statement(ctx)).await?;
					parser.eat(TokenKind::SemiColon);

					let end_span = parser.recent_span();
					carry_token = Token {
						kind: TokenKind::Invalid,
						span: end_span,
					};

					let end = end_span.offset.saturating_add(end_span.len);
					let source =
						parser.span_str(Span::from_range(start..end.max(start))).to_string();

					if statement_has_comment(source.as_bytes()) {
						// The AST dropped the comment, so keep the original text
						// rather than reformatting; ensure it stays terminated.
						let trimmed = source.trim();
						output.push_str(trimmed);
						if !trimmed.ends_with(';') {
							output.push(';');
						}
					} else {
						output += &parsed.to_sql_pretty();
					}
				}
			}

			let result = parser.lex_compound(carry_token, |lexer, _| {
				let cr = lexer.eat(b'\r');
				let lf = lexer.eat(b'\n');
				Ok(cr || lf)
			})?;

			carry_token = Token {
				kind: TokenKind::Invalid,
				span: result.span.after(),
			};

			if result.value {
				output.push('\n');
			}
		}

		// Normalize to exactly one trailing newline, dropping leading and
		// trailing blank lines. Empty input stays empty.
		let trimmed = output.trim();
		Ok(if trimmed.is_empty() {
			String::new()
		} else {
			format!("{trimmed}\n")
		})
	})
}

/// The state of a source line, and how [`format`] should reproduce it.
enum FormatLine {
	/// Normalize to a single-line comment.
	SingleComment(String),
	/// Normalize to a multi-line comment.
	MultiComment(String),
	/// Collapse a run of whitespace-only lines into a single blank line.
	Whitespace,
	/// Parse and pretty-print a statement.
	Statement,
}

/// Inserts a single space when `output` already holds content on the current
/// line, so the next token is never appended directly onto the previous one.
fn separate_same_line(output: &mut String) {
	if !output.is_empty() && !output.ends_with('\n') {
		output.push(' ');
	}
}

/// Whether a statement's source contains a comment in any inter-token gap.
///
/// Between two lexed tokens the lexer only ever skips whitespace and comments,
/// so a gap holding a non-whitespace byte marks a comment. Content inside string
/// or quoted-identifier literals lives within a token span, never in a gap, so
/// it cannot be mistaken for one. A non-ASCII (e.g. Unicode whitespace) gap is
/// treated conservatively as a comment, which only keeps the statement verbatim.
fn statement_has_comment(source: &[u8]) -> bool {
	let mut prev_end = 0usize;
	for token in Lexer::new(source) {
		let start = token.span.offset as usize;
		if source[prev_end..start].iter().any(|b| !b.is_ascii_whitespace()) {
			return true;
		}
		prev_end = start + token.span.len as usize;
	}
	source[prev_end..].iter().any(|b| !b.is_ascii_whitespace())
}

fn format_line(lexer: &mut Lexer) -> ParseResult<FormatLine> {
	lexer.eat_single_line_whitespace();
	lexer.advance_span();

	let mut span = lexer.current_span();
	span.len = 1;

	let lookahead = lexer.span_bytes(span);

	match lookahead {
		b"#" => {
			lexer.eat(b'#');
			lexer.advance_span();
			Ok(format_single_line_comment(lexer))
		}
		b"-" => {
			let mut span = span.after();
			span.len = 1;
			let lookahead = lexer.span_bytes(span);

			if lookahead == b"-" {
				lexer.eat(b'-');
				lexer.eat(b'-');
				lexer.advance_span();
				Ok(format_single_line_comment(lexer))
			} else {
				Ok(FormatLine::Statement)
			}
		}
		b"/" => {
			let mut span = span.after();
			span.len = 1;
			let lookahead = lexer.span_bytes(span);

			match lookahead {
				b"/" => {
					lexer.eat(b'/');
					lexer.eat(b'/');
					lexer.advance_span();
					Ok(format_single_line_comment(lexer))
				}
				b"*" => {
					lexer.eat(b'/');
					lexer.eat(b'*');
					lexer.eat_multi_line_comment()?;
					let span = lexer.advance_span();
					let comment = lexer.span_str(span).to_string();
					Ok(FormatLine::MultiComment(comment))
				}
				_ => Ok(FormatLine::Statement),
			}
		}
		b"\n" | b"\r" => {
			lexer.eat_whitespace();
			lexer.advance_span();
			Ok(FormatLine::Whitespace)
		}
		_ => Ok(FormatLine::Statement),
	}
}

fn format_single_line_comment(lexer: &mut Lexer) -> FormatLine {
	lexer.eat_single_line_comment();
	let span = lexer.advance_span();
	let comment = lexer.span_str(span).to_string();
	FormatLine::SingleComment(comment)
}

/// Creates the parser settings struct from the global configuration values as
/// wel as the capabilities  struct.
pub fn settings_from_capabilities_config(
	cap: &Capabilities,
	config: &ParserConfig,
) -> ParserSettings {
	ParserSettings {
		files_enabled: cap.allows_experimental(&ExperimentalTarget::Files),
		surrealism_enabled: cap.allows_experimental(&ExperimentalTarget::Surrealism),
		..ParserSettings::from_config(config)
	}
}

/// Parses a SurrealQL query.
///
/// During query parsing, the total depth of calls to parse values (including
/// arrays, expressions, functions, objects, sub-queries), Javascript values,
/// and geometry collections count against a computation depth limit. If the
/// limit is reached, parsing will return an error,
/// as opposed to spending more time and potentially overflowing the call stack.
///
/// If you encounter this limit and believe that it should be increased,
/// please [open an issue](https://github.com/surrealdb/surrealdb/issues)!
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn parse(input: &str) -> Result<Ast> {
	let capabilities = Capabilities::all();
	parse_with_capabilities(input, &capabilities, &ParserConfig::default())
}

/// Parses a SurrealQL query.
///
/// During query parsing, the total depth of calls to parse values (including
/// arrays, expressions, functions, objects, sub-queries), Javascript values,
/// and geometry collections count against a computation depth limit. If the
/// limit is reached, parsing will return an error,
/// as opposed to spending more time and potentially overflowing the call stack.
///
/// If you encounter this limit and believe that it should be increased,
/// please [open an issue](https://github.com/surrealdb/surrealdb/issues)!
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn parse_with_capabilities(
	input: &str,
	capabilities: &Capabilities,
	config: &ParserConfig,
) -> Result<Ast> {
	trace!(target: TARGET, "Parsing SurrealQL query");

	parse_with_settings(
		input.as_bytes(),
		settings_from_capabilities_config(capabilities, config),
		async |parser, stk| parser.parse_query(stk).await,
	)
}

/// Parses a SurrealQL [`Expr`].
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
#[allow(dead_code)]
pub(crate) fn expr(input: &str) -> Result<Expr> {
	let capabilities = Capabilities::all();
	expr_with_capabilities(input, &capabilities, &ParserConfig::default())
}

/// Parses a SurrealQL [`Value`].
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
#[allow(dead_code)]
pub(crate) fn expr_with_capabilities(
	input: &str,
	capabilities: &Capabilities,
	config: &ParserConfig,
) -> Result<Expr> {
	trace!(target: TARGET, "Parsing SurrealQL value");

	parse_with_settings(
		input.as_bytes(),
		settings_from_capabilities_config(capabilities, config),
		async |parser, stk| parser.parse_expr_field(stk).await,
	)
}

/// Parses a SurrealQL function name.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn function_with_capabilities(
	input: &str,
	capabilities: &Capabilities,
	config: &ParserConfig,
) -> Result<Function> {
	trace!(target: TARGET, "Parsing SurrealQL function name");

	parse_with_settings(
		input.as_bytes(),
		settings_from_capabilities_config(capabilities, config),
		async |parser, _stk| parser.parse_function_name().await,
	)
}

/// Parses JSON into an inert SurrealQL [`PublicValue`].
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn json(input: &str) -> Result<PublicValue> {
	trace!(target: TARGET, "Parsing inert JSON value");

	let settings = ParserSettings {
		json_string_escapes: true,
		..Default::default()
	};

	parse_with_settings(input.as_bytes(), settings, async |parser, stk| {
		parser.parse_json(stk).await
	})
}

/// Parses a SurrealQL [`Idiom`]
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub(crate) fn idiom(input: &str) -> Result<Idiom> {
	trace!(target: TARGET, "Parsing SurrealQL idiom");

	parse_with(input.as_bytes(), async |parser, stk| parser.parse_plain_idiom(stk).await)
}

/// Parse a datetime without enclosing delimiters from a string.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn datetime(input: &str) -> Result<PublicDatetime> {
	trace!(target: TARGET, "Parsing SurrealQL datetime");

	ensure!(input.len() <= u32::MAX as usize, ParseError::QueryTooLarge);

	match Lexer::lex_datetime(input) {
		Ok(x) => Ok(x),
		Err(e) => {
			bail!(ParseError::InvalidQuery(e.render_on(input)))
		}
	}
}

/// Parse a duration from a string.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn duration(input: &str) -> Result<PublicDuration> {
	trace!(target: TARGET, "Parsing SurrealQL duration");

	ensure!(input.len() <= u32::MAX as usize, ParseError::QueryTooLarge);

	let mut parser = Parser::new(input.as_bytes());
	parser
		.next_token_value::<PublicDuration>()
		.and_then(|e| parser.assert_finished().map(|_| e))
		.map_err(|e| e.render_on(input))
		.map_err(ParseError::InvalidQuery)
		.map_err(anyhow::Error::new)
}

/// Parse a record id.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn record_id(input: &str) -> Result<PublicRecordId> {
	trace!(target: TARGET, "Parsing SurrealQL record id");

	parse_with(input.as_bytes(), async |parser, stk| parser.parse_value_record_id(stk).await)
}

/// Parse a table name from a string.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn table(input: &str) -> Result<surrealdb_strand::TableName> {
	trace!(target: TARGET, "Parsing SurrealQL table name");

	parse_with(input.as_bytes(), async |parser, _stk| {
		let ident = parser.parse_ident()?;
		Ok(surrealdb_strand::TableName::new(ident))
	})
}

/// Parses a `{ ... }` block of fresh input, under the configured depth limits.
///
/// Distinct from [`surrealdb_syn::block_for_definition`], which parses under the
/// unbounded stored-text profile. That profile is safe only for text this engine
/// wrote: parsing is heap-stacked and survives arbitrary nesting, but the
/// lowering to `expr::Block` and the value's recursive `Drop` descend the call
/// stack, and a stack overflow aborts the process instead of returning an error.
/// Fresh input therefore keeps the limits, whoever hands it in.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn block(input: &str) -> Result<Block> {
	trace!(target: TARGET, "Parsing SurrealQL block");

	lift(surrealdb_syn::block_with_settings(
		input,
		ParserSettings::all_features(&ParserConfig::default()),
		false,
	))
}

/// Parses a SurrealQL [`Value`] and parses values within strings.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
#[allow(dead_code)]
pub(crate) fn expr_legacy_strand(input: &str) -> Result<Expr> {
	trace!(target: TARGET, "Parsing SurrealQL value, with legacy strings");

	let settings = ParserSettings {
		object_recursion_limit: usize::MAX,
		query_recursion_limit: usize::MAX,
		expr_recursion_limit: usize::MAX,
		legacy_strands: true,
		..Default::default()
	};

	parse_with_settings(input.as_bytes(), settings, async |parser, stk| {
		parser.parse_expr_field(stk).await
	})
}

/// Parses a SurrealQL [`PublicValue`] and parses values within strings.
///
/// This function is for testing only, don't use it outside of tests!
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn value(input: &str) -> Result<PublicValue> {
	trace!(target: TARGET, "Parsing SurrealQL value, with legacy strings");

	let settings = ParserSettings::default();

	parse_with_settings(input.as_bytes(), settings, async |parser, stk| {
		parser.parse_value(stk).await
	})
}

/// Parses a SurrealQL [`PublicValue`] and parses values within strings.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn value_legacy_strand(input: &str, config: &ParserConfig) -> Result<PublicValue> {
	trace!(target: TARGET, "Parsing SurrealQL value, with legacy strings");

	let settings = ParserSettings {
		object_recursion_limit: config.max_object_parsing_depth as usize,
		query_recursion_limit: config.max_query_parsing_depth as usize,
		legacy_strands: true,
		..Default::default()
	};

	parse_with_settings(input.as_bytes(), settings, async |parser, stk| {
		parser.parse_value(stk).await
	})
}

/// Parses JSON into an inert SurrealQL [`PublicValue`] and parses values within
/// strings.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn json_legacy_strand(input: &str, config: &ParserConfig) -> Result<PublicValue> {
	trace!(target: TARGET, "Parsing inert JSON value, with legacy strings");

	let settings = ParserSettings {
		// Values are unused on the value parsing path.
		object_recursion_limit: 0,
		query_recursion_limit: 0,
		legacy_strands: true,
		json_string_escapes: true,
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
				tables.insert(table.as_str().to_owned());
			}
		}
		Kind::Record(ts) => {
			for table in ts {
				tables.insert(table.as_str().to_owned());
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
