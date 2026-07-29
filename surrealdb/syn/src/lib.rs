//! The SurrealQL lexer and parser: text in, [`surrealdb_sql`] AST out.
//!
//! # Layer
//!
//! This crate sits directly above the AST and below the engine. It parses
//! against an explicit [`ParserSettings`] and knows nothing about capabilities
//! or datastore configuration; `surrealdb-core` derives the settings from those
//! and keeps the `syn::parse*` entry points callers use.
//!
//! It owns [`ParseError`], the failure a caller sees when a query does not
//! parse. The engine raises it unchanged rather than restating it, so the
//! [`common::LeafError`] impl here is what decides its public form.
//!
//! # Stability
//!
//! This crate is an internal implementation detail of SurrealDB with no
//! stability guarantee. It is scheduled for deletion once the greenfield
//! parser reaches parity and takes over the same layer, so its API surface
//! should not grow. Depend on `surrealdb` or `surrealdb-core` instead.
//!
//! <section class="warning">
//! <h3>Unstable!</h3>
//! This crate is <b>SurrealDB internal API</b>. It does not adhere to SemVer and its API is
//! free to change and break code even between patch versions. If you are looking for a stable
//! interface to the SurrealDB library please have a look at
//! <a href="https://crates.io/crates/surrealdb">the Rust SDK</a>.
//! </section>

pub mod error;
pub mod lexer;
pub mod parser;
pub mod token;

use common::{LeafError, internal_todo};
use reblessive::{Stack, Stk};
use surrealdb_cnf::CommonConfig;
use surrealdb_sql::{Ast, Block, Expr};
use surrealdb_types::Error as TypesError;
use tracing::instrument;

pub use self::error::RenderedError;
pub use self::parser::{ParseResult, Parser, ParserSettings};

/// Why a parse did not produce an AST.
///
/// This is what a client sees when a query fails to parse: the engine raises it
/// as-is rather than restating it, so both the message and the classification
/// below are the public contract.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ParseError {
	/// The input is longer than the `u32` span space the lexer addresses.
	#[error("Size of query script exceeded maximum supported size of 4,294,967,295 bytes.")]
	QueryTooLarge,
	/// The input is not valid SurrealQL, rendered against its source.
	#[error("Parse error: {0}")]
	InvalidQuery(RenderedError),
}

impl LeafError for ParseError {
	fn map_kind(self, message: String) -> TypesError {
		match self {
			// A query too large to address is a limit on the input, not a
			// statement about its contents, so it carries no parse diagnostic.
			ParseError::QueryTooLarge => internal_todo(message),
			ParseError::InvalidQuery(_) => TypesError::validation(message, None),
		}
	}
}

/// Takes a string and returns if it could be a reserved keyword in certain
/// contexts.
pub fn could_be_reserved_keyword(s: &str) -> bool {
	common::keywords::could_be_reserved(s)
}

/// Runs `f` against a parser over `input` using the default settings.
pub fn parse_with<F, R>(input: &[u8], f: F) -> Result<R, ParseError>
where
	F: AsyncFnOnce(&mut Parser<'_>, &mut Stk) -> ParseResult<R>,
{
	parse_with_settings(input, ParserSettings::default(), f)
}

/// Runs `f` against a parser over `input` using the given settings.
///
/// Spans are `u32`-indexed, so an input longer than `u32::MAX` cannot be
/// pointed at in a diagnostic and is rejected before lexing starts.
pub fn parse_with_settings<F, R>(
	input: &[u8],
	settings: ParserSettings,
	f: F,
) -> Result<R, ParseError>
where
	F: for<'a> AsyncFnOnce(&'a mut Parser<'a>, &'a mut Stk) -> ParseResult<R>,
{
	if input.len() > u32::MAX as usize {
		return Err(ParseError::QueryTooLarge);
	}
	let mut parser = Parser::new_with_settings(input, settings);
	let mut stack = Stack::new();
	stack
		.enter(|stk| f(&mut parser, stk))
		.finish()
		.map_err(|e| e.render_on_bytes(input))
		.map_err(ParseError::InvalidQuery)
}

impl ParserSettings {
	/// Parser limits taken from configuration, with every optional grammar
	/// disabled. Core layers the capability-gated grammars on top.
	pub fn from_config(config: &CommonConfig) -> Self {
		ParserSettings {
			object_recursion_limit: config.max_object_parsing_depth as usize,
			query_recursion_limit: config.max_query_parsing_depth as usize,
			expr_recursion_limit: config.max_expression_parsing_depth as usize,
			..Default::default()
		}
	}

	/// Configured limits with every optional grammar enabled, matching what an
	/// unrestricted capability set produces.
	pub fn all_features(config: &CommonConfig) -> Self {
		ParserSettings {
			files_enabled: true,
			surrealism_enabled: true,
			..Self::from_config(config)
		}
	}
}

/// Parses a SurrealQL query with every optional grammar enabled.
///
/// Core's `syn::parse` narrows this to the datastore's live capabilities.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn parse(input: &str) -> Result<Ast, ParseError> {
	parse_with_settings(
		input.as_bytes(),
		ParserSettings::all_features(&CommonConfig::default()),
		async |parser, stk| parser.parse_query(stk).await,
	)
}

/// Parses a single SurrealQL expression with every optional grammar enabled.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn expr(input: &str) -> Result<Expr, ParseError> {
	parse_with_settings(
		input.as_bytes(),
		ParserSettings::all_features(&CommonConfig::default()),
		async |parser, stk| parser.parse_expr_field(stk).await,
	)
}

/// Parses a SurrealQL expression, also parsing values found inside strings.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn expr_legacy_strand(input: &str) -> Result<Expr, ParseError> {
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

/// Re-parses the canonical block text a catalog definition stores, for
/// example a stored function body. Expects the input wrapped in `{}`.
///
/// The whole of `input` must be consumed: `parse_block` returns at the closing
/// brace, so without the [`Parser::assert_finished`] check stored text holding
/// anything after it would silently compile to the leading block alone.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn block(input: &str) -> Result<Block, ParseError> {
	block_with_settings(input, ParserSettings::STORED_TEXT, true)
}

/// Parses a `{ ... }` block under caller-chosen limits.
///
/// The unbounded [`ParserSettings::STORED_TEXT`] profile is only safe for text
/// this engine wrote. Parsing is heap-stacked, so a deeply nested block parses
/// fine, but the `sql::Block -> expr::Block` lowering every caller runs, and the
/// value's own recursive `Drop`, descend the call stack per node — and a Rust
/// stack overflow aborts the process rather than raising an error. Fresh input
/// therefore has to keep the configured depth limits.
///
/// `require_all_input` is separate because it is a real behaviour difference,
/// not a safety one: stored text must be consumed whole or a definition with
/// trailing content silently compiles to its leading block, while fresh input
/// has always been allowed to stop at the closing brace.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn block_with_settings(
	input: &str,
	settings: ParserSettings,
	require_all_input: bool,
) -> Result<Block, ParseError> {
	parse_with_settings(input.as_bytes(), settings, async |parser, stk| {
		let found = parser.peek();
		match found.kind {
			token::t!("{") => {
				let start = parser.pop_peek().span;
				let block = parser.parse_block(stk, start).await?;
				if require_all_input {
					parser.assert_finished()?;
				}
				Ok(block)
			}
			kind => Err(error::SyntaxError::new(format_args!(
				"Unexpected token `{kind}` expected `{{`"
			))
			.with_span(found.span, error::MessageKind::Error)),
		}
	})
}
