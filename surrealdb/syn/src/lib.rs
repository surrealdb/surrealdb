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

// TODO: Remove once this crate no longer needs it; see `absolute_paths` in the workspace lints.
#![allow(clippy::absolute_paths)]

pub mod config;
pub mod error;
pub mod lexer;
pub mod next;
pub mod parser;
pub mod token;

use common::{LeafError, internal_todo};
use surrealdb_parser_next_lower::{entry as next_entry, value as next_value};
use surrealdb_sql::{Ast, Block, Expr, Fields, Idiom, Kind};
use surrealdb_types::{
	Datetime as PublicDatetime, Duration as PublicDuration, Error as TypesError,
	RecordId as PublicRecordId, Value as PublicValue,
};

pub use self::config::ParserConfig;
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
	F: FnOnce(&mut Parser<'_>) -> ParseResult<R>,
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
	F: for<'a> FnOnce(&'a mut Parser<'a>) -> ParseResult<R>,
{
	if input.len() > u32::MAX as usize {
		return Err(ParseError::QueryTooLarge);
	}
	let mut parser = Parser::new_with_settings(input, settings);
	f(&mut parser).map_err(|e| e.render_on_bytes(input)).map_err(ParseError::InvalidQuery)
}

impl ParserSettings {
	/// Parser limits taken from configuration, with every optional grammar
	/// disabled. Core layers the capability-gated grammars on top.
	pub fn from_config(config: &ParserConfig) -> Self {
		ParserSettings {
			object_recursion_limit: config.max_object_parsing_depth as usize,
			query_recursion_limit: config.max_query_parsing_depth as usize,
			expr_recursion_limit: config.max_expression_parsing_depth as usize,
			..Default::default()
		}
	}

	/// Configured limits with every optional grammar enabled, matching what an
	/// unrestricted capability set produces.
	pub fn all_features(config: &ParserConfig) -> Self {
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
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len())))]
pub fn parse(input: &str) -> Result<Ast, ParseError> {
	query_with_settings(input, ParserSettings::all_features(&ParserConfig::default()))
}

/// Parses a SurrealQL query under `settings`.
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len())))]
pub fn query_with_settings(input: &str, settings: ParserSettings) -> Result<Ast, ParseError> {
	if next::routed() {
		let n = next::settings(&settings);
		return next::route(
			input,
			|| next_entry::query(input, &n),
			|| parse_with_settings(input.as_bytes(), settings, |parser| parser.parse_query()),
		);
	}
	parse_with_settings(input.as_bytes(), settings, |parser| parser.parse_query())
}

/// Parses an expression in which a bare identifier is a field, under
/// `settings`. With `require_all`, anything after the expression is an error.
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len())))]
pub fn expr_with_settings(
	input: &str,
	settings: ParserSettings,
	require_all: bool,
) -> Result<Expr, ParseError> {
	let old = |settings| {
		parse_with_settings(input.as_bytes(), settings, |parser| {
			let expr = parser.parse_expr_field()?;
			if require_all {
				parser.assert_finished()?;
			}
			Ok(expr)
		})
	};
	if next::routed() {
		let n = next::settings(&settings);
		return next::route(input, || next_entry::expr(input, &n, require_all), || old(settings));
	}
	old(settings)
}

/// Parses an idiom without a leading value under `settings`. With
/// `require_all`, anything after the idiom is an error.
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len())))]
pub fn idiom_with_settings(
	input: &str,
	settings: ParserSettings,
	require_all: bool,
) -> Result<Idiom, ParseError> {
	let old = |settings| {
		parse_with_settings(input.as_bytes(), settings, |parser| {
			let idiom = parser.parse_plain_idiom()?;
			if require_all {
				parser.assert_finished()?;
			}
			Ok(idiom)
		})
	};
	if next::routed() {
		let n = next::settings(&settings);
		return next::route(input, || next_entry::idiom(input, &n, require_all), || old(settings));
	}
	old(settings)
}

/// Parses a kind without the enclosing `<` `>` under `settings`. With
/// `require_all`, anything after the kind is an error.
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len())))]
pub fn kind_with_settings(
	input: &str,
	settings: ParserSettings,
	require_all: bool,
) -> Result<Kind, ParseError> {
	let old = |settings| {
		parse_with_settings(input.as_bytes(), settings, |parser| {
			let kind = parser.parse_inner_kind()?;
			if require_all {
				parser.assert_finished()?;
			}
			Ok(kind)
		})
	};
	if next::routed() {
		let n = next::settings(&settings);
		return next::route(input, || next_entry::kind(input, &n, require_all), || old(settings));
	}
	old(settings)
}

/// Parses a projection list, or `VALUE` and one selector, under `settings`.
/// With `require_all`, anything after it is an error.
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len())))]
pub fn fields_with_settings(
	input: &str,
	settings: ParserSettings,
	require_all: bool,
) -> Result<Fields, ParseError> {
	let old = |settings| {
		parse_with_settings(input.as_bytes(), settings, |parser| {
			let fields = parser.parse_fields()?;
			if require_all {
				parser.assert_finished()?;
			}
			Ok(fields)
		})
	};
	if next::routed() {
		let n = next::settings(&settings);
		return next::route(input, || next_entry::fields(input, &n, require_all), || old(settings));
	}
	old(settings)
}

/// Parses a single SurrealQL expression with every optional grammar enabled.
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len())))]
pub fn expr(input: &str) -> Result<Expr, ParseError> {
	expr_with_settings(input, ParserSettings::all_features(&ParserConfig::default()), false)
}

/// Parses a SurrealQL expression, also parsing values found inside strings.
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len())))]
pub fn expr_legacy_strand(input: &str) -> Result<Expr, ParseError> {
	// Parsing recurses on the call stack, so the depth limits stay bounded:
	// the stored-text ceilings are the most the parser admits anywhere.
	let settings = ParserSettings {
		object_recursion_limit: ParserSettings::STORED_TEXT.object_recursion_limit,
		query_recursion_limit: ParserSettings::STORED_TEXT.query_recursion_limit,
		expr_recursion_limit: ParserSettings::STORED_TEXT.expr_recursion_limit,
		legacy_strands: true,
		..Default::default()
	};

	expr_with_settings(input, settings, false)
}

/// Re-parses the canonical block text a catalog definition stores, for
/// example a stored function body. Expects the input wrapped in `{}`.
///
/// The whole of `input` must be consumed: `parse_block` returns at the closing
/// brace, so without the [`Parser::assert_finished`] check stored text holding
/// anything after it would silently compile to the leading block alone.
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len())))]
pub fn block(input: &str) -> Result<Block, ParseError> {
	block_with_settings(input, ParserSettings::STORED_TEXT, true)
}

/// Parses a `{ ... }` block under caller-chosen limits.
///
/// The [`ParserSettings::STORED_TEXT`] profile, whose ceilings sit far above
/// the configured defaults, is only meant for text this engine wrote. Parsing,
/// the `sql::Block -> expr::Block` lowering every caller runs, and the value's
/// own recursive `Drop` all descend the call stack per nesting level — and a
/// Rust stack overflow aborts the process rather than raising an error. Fresh
/// input therefore has to keep the configured depth limits.
///
/// `require_all_input` is separate because it is a real behaviour difference,
/// not a safety one: stored text must be consumed whole or a definition with
/// trailing content silently compiles to its leading block, while fresh input
/// has always been allowed to stop at the closing brace.
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len())))]
pub fn block_with_settings(
	input: &str,
	settings: ParserSettings,
	require_all_input: bool,
) -> Result<Block, ParseError> {
	if next::routed() {
		let n = next::settings(&settings);
		return next::route(
			input,
			|| next_entry::block(input, &n, require_all_input),
			|| block_with_settings_old(input, settings, require_all_input),
		);
	}
	block_with_settings_old(input, settings, require_all_input)
}

fn block_with_settings_old(
	input: &str,
	settings: ParserSettings,
	require_all_input: bool,
) -> Result<Block, ParseError> {
	parse_with_settings(input.as_bytes(), settings, |parser| {
		let found = parser.peek();
		match found.kind {
			token::t!("{") => {
				let start = parser.pop_peek().span;
				let block = parser.parse_block(start)?;
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
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len())))]
pub fn idiom(input: &str) -> Result<Idiom, ParseError> {
	idiom_with_settings(input, ParserSettings::default(), false)
}

/// Parses a SurrealQL [`PublicValue`] and parses values within strings.
///
/// This function is for testing only, don't use it outside of tests!
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len())))]
pub fn value(input: &str) -> Result<PublicValue, ParseError> {
	value_with_settings(input, ParserSettings::default())
}

/// Parses a SurrealQL literal value under `settings`.
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len())))]
pub fn value_with_settings(
	input: &str,
	settings: ParserSettings,
) -> Result<PublicValue, ParseError> {
	let old =
		|settings| parse_with_settings(input.as_bytes(), settings, |parser| parser.parse_value());
	if next::routed() {
		let n = next::settings(&settings);
		return next::route(input, || next_value::value(input, &n), || old(settings));
	}
	old(settings)
}

/// Parses a JSON value under `settings`.
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len())))]
pub fn json_with_settings(
	input: &str,
	settings: ParserSettings,
) -> Result<PublicValue, ParseError> {
	let old =
		|settings| parse_with_settings(input.as_bytes(), settings, |parser| parser.parse_json());
	if next::routed() {
		let n = next::settings(&settings);
		return next::route(input, || next_value::json(input, &n), || old(settings));
	}
	old(settings)
}

/// Parse a record id.
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len())))]
pub fn record_id(input: &str) -> Result<PublicRecordId, ParseError> {
	let old = || parse_with(input.as_bytes(), |parser| parser.parse_value_record_id());
	if next::routed() {
		let n = next::settings(&ParserSettings::default());
		return next::route(input, || next_value::record_id(input, &n), old);
	}
	old()
}

/// Re-parses the canonical kind-grammar text a catalog definition stores for
/// one of its type-annotated fields (e.g. `StoredFieldDefinition.field_kind`,
/// `StoredFunctionDefinition.args`/`.returns`). A kind can embed a `<file>` bucket
/// restriction or other experimental-gated grammar, so this parses under
/// [`ParserSettings::STORED_TEXT`] — the storage wire contract's parser
/// profile — unlike [`kind`], which parses fresh input. The whole of `input`
/// must be consumed, for the reason [`expr_for_definition`] documents.
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len())))]
pub fn kind_for_definition(input: &str) -> Result<Kind, ParseError> {
	kind_with_settings(input, ParserSettings::STORED_TEXT, true)
}

/// Parses a SurrealQL [`PublicValue`] and parses values within strings.
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len())))]
pub fn value_legacy_strand(input: &str, config: &ParserConfig) -> Result<PublicValue, ParseError> {
	let settings = ParserSettings {
		object_recursion_limit: config.max_object_parsing_depth as usize,
		query_recursion_limit: config.max_query_parsing_depth as usize,
		legacy_strands: true,
		..Default::default()
	};

	value_with_settings(input, settings)
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
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len())))]
pub fn expr_for_definition(input: &str) -> Result<Expr, ParseError> {
	expr_with_settings(input, ParserSettings::STORED_TEXT, true)
}

/// Parse a duration from a string.
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len())))]
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
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len())))]
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
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len())))]
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
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len())))]
pub fn idiom_for_definition(input: &str) -> Result<Idiom, ParseError> {
	idiom_with_settings(input, ParserSettings::STORED_TEXT, true)
}

/// Re-parses the canonical `SELECT`-clause field-list text a catalog
/// definition stores for one of its field-selection clauses (e.g.
/// `StoredSubscriptionDefinition.fields`), under [`ParserSettings::STORED_TEXT`] —
/// the storage wire contract's parser profile. The whole of `input` must be
/// consumed, for the reason [`expr_for_definition`] documents.
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len())))]
pub fn fields_for_definition(input: &str) -> Result<Fields, ParseError> {
	fields_with_settings(input, ParserSettings::STORED_TEXT, true)
}

/// Parse a kind from a string.
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len())))]
pub fn kind(input: &str) -> Result<Kind, ParseError> {
	kind_with_settings(input, ParserSettings::default(), false)
}
