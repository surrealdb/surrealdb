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
use surrealdb_sql::{Ast, Block, Expr, Fields, Idiom, Kind};
use surrealdb_types::{
	Datetime as PublicDatetime, Duration as PublicDuration, Error as TypesError,
	RecordId as PublicRecordId, Value as PublicValue,
};
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

/// Parses a SurrealQL [`Idiom`]
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn idiom(input: &str) -> Result<Idiom, ParseError> {
	parse_with(input.as_bytes(), async |parser, stk| parser.parse_plain_idiom(stk).await)
}

/// Parses a SurrealQL [`PublicValue`] and parses values within strings.
///
/// This function is for testing only, don't use it outside of tests!
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn value(input: &str) -> Result<PublicValue, ParseError> {
	let settings = ParserSettings::default();

	parse_with_settings(input.as_bytes(), settings, async |parser, stk| {
		parser.parse_value(stk).await
	})
}

/// Parse a record id.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn record_id(input: &str) -> Result<PublicRecordId, ParseError> {
	parse_with(input.as_bytes(), async |parser, stk| parser.parse_value_record_id(stk).await)
}

/// Re-parses the canonical kind-grammar text a catalog definition stores for
/// one of its type-annotated fields (e.g. `StoredFieldDefinition.field_kind`,
/// `StoredFunctionDefinition.args`/`.returns`). A kind can embed a `<file>` bucket
/// restriction or other experimental-gated grammar, so this parses under
/// [`ParserSettings::STORED_TEXT`] — the storage wire contract's parser
/// profile — unlike [`kind`], which parses fresh input. The whole of `input`
/// must be consumed, for the reason [`expr_for_definition`] documents.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn kind_for_definition(input: &str) -> Result<Kind, ParseError> {
	parse_with_settings(input.as_bytes(), ParserSettings::STORED_TEXT, async |parser, stk| {
		let kind = parser.parse_inner_kind(stk).await?;
		parser.assert_finished()?;
		Ok(kind)
	})
}

/// Parses a SurrealQL [`PublicValue`] and parses values within strings.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn value_legacy_strand(input: &str, config: &CommonConfig) -> Result<PublicValue, ParseError> {
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

/// Re-parses the canonical SurrealQL text a catalog definition stores for one
/// of its expression fields (e.g. `StoredFieldDefinition.value`, `Permission::Specific`).
///
/// Unlike [`expr`], which parses fresh input under the live capabilities and
/// configured limits, this uses [`ParserSettings::STORED_TEXT`] — the storage
/// wire contract's parser profile. See that constant for the full reasoning.
///
/// # Field context
///
/// This parses with [`Parser::parse_expr_field`], so a *top-level* bare
/// identifier compiles to `Expr::Idiom([Part::Field(..)])`, never
/// `Expr::Table`. That is the correct reading for every clause stored as
/// expression text: `VALUE`, `ASSERT`, `DEFAULT`, `COMPUTED`, `WHEN`, `THEN`,
/// permission guards, API actions and fetch targets all name a field of the
/// document, and all are parsed in field context when the statement that
/// defines them runs.
///
/// It is safe for *nested* table positions too, and the reason is worth
/// stating because it is not obvious: the two context-setting entry points
/// each save and restore the flag rather than latching it (see
/// `Parser::parse_expr_table` and `Parser::parse_expr_field`). A statement
/// inside stored text therefore re-establishes table context for its own
/// source list regardless of how the parse was entered, so
/// `StoredEventDefinition.then` holding `CREATE person` recovers
/// `Expr::Table("person")` even though this funnel started in field context.
///
/// The consequence is that only a top-level bare identifier depends on which
/// funnel is used. `crate::catalog::text` pins both halves of that.
///
/// The whole of `input` must be consumed. Every one of these `*_for_definition`
/// funnels parses with a routine that stops at the first token it cannot
/// continue with — the Pratt expression parser exits on a token with no
/// continuation binding power, and the idiom/field-list/kind parsers stop at
/// the first token that is not a further part, field or union arm — so without
/// the [`Parser::assert_finished`] check a stored definition holding trailing
/// content (`"a.b bogus"`) would silently compile to its prefix (`a.b`) and be
/// used as if that were what the user defined.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn expr_for_definition(input: &str) -> Result<Expr, ParseError> {
	parse_with_settings(input.as_bytes(), ParserSettings::STORED_TEXT, async |parser, stk| {
		let expr = parser.parse_expr_field(stk).await?;
		parser.assert_finished()?;
		Ok(expr)
	})
}

/// Parse a duration from a string.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn duration(input: &str) -> Result<PublicDuration, ParseError> {
	if input.len() > u32::MAX as usize {
		return Err(ParseError::QueryTooLarge);
	}

	let mut parser = Parser::new(input.as_bytes());
	parser
		.next_token_value::<PublicDuration>()
		.and_then(|e| parser.assert_finished().map(|_| e))
		.map_err(|e| e.render_on(input))
		.map_err(ParseError::InvalidQuery)
}

/// Parse a datetime without enclosing delimiters from a string.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn datetime(input: &str) -> Result<PublicDatetime, ParseError> {
	if input.len() > u32::MAX as usize {
		return Err(ParseError::QueryTooLarge);
	}

	match lexer::Lexer::lex_datetime(input) {
		Ok(x) => Ok(x),
		Err(e) => Err(ParseError::InvalidQuery(e.render_on(input))),
	}
}

/// Re-parses canonical `{ ... }` block text this engine previously rendered:
/// `Block`'s own text-on-the-wire encoding and the stored function-body text
/// (`StoredFunctionDefinition.block`). Both are engine-authored, so this parses
/// under [`ParserSettings::STORED_TEXT`] — the storage wire contract's parser
/// profile — and requires the whole input to be consumed. Expects the input to
/// be wrapped in `{}`.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn block_for_definition(input: &str) -> Result<Block, ParseError> {
	block(input)
}

/// Re-parses the canonical idiom-path text a catalog definition stores for
/// one of its indexed-column fields (e.g. `StoredIndexDefinition.cols`). An idiom
/// can embed arbitrary sub-expressions (a `[WHERE ...]` filter part, a
/// computed bracket index), so this parses under
/// [`ParserSettings::STORED_TEXT`] — the storage wire contract's parser
/// profile — unlike [`idiom`], which parses fresh input. The whole of `input`
/// must be consumed, for the reason [`expr_for_definition`] documents.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn idiom_for_definition(input: &str) -> Result<Idiom, ParseError> {
	parse_with_settings(input.as_bytes(), ParserSettings::STORED_TEXT, async |parser, stk| {
		let idiom = parser.parse_plain_idiom(stk).await?;
		parser.assert_finished()?;
		Ok(idiom)
	})
}

/// Re-parses the canonical `SELECT`-clause field-list text a catalog
/// definition stores for one of its field-selection clauses (e.g.
/// `StoredSubscriptionDefinition.fields`), under [`ParserSettings::STORED_TEXT`] —
/// the storage wire contract's parser profile. The whole of `input` must be
/// consumed, for the reason [`expr_for_definition`] documents.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn fields_for_definition(input: &str) -> Result<Fields, ParseError> {
	parse_with_settings(input.as_bytes(), ParserSettings::STORED_TEXT, async |parser, stk| {
		let fields = parser.parse_fields(stk).await?;
		parser.assert_finished()?;
		Ok(fields)
	})
}

/// Parse a kind from a string.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn kind(input: &str) -> Result<Kind, ParseError> {
	parse_with(input.as_bytes(), async |parser, stk| parser.parse_inner_kind(stk).await)
}
