//! Text to runtime expressions or public values, with source-rendered errors.
//!
//! Request entry points leave credentials unprepared. Stored-text entry points
//! derive credentials under the catalog storage contract.
//!
//! Each function parses with parser-next under the given [`ParserSettings`],
//! lowers the flat tree, and renders a rejection in the established wording
//! (`--> [line:col]`, a snippet, carets and labels). This module knows nothing
//! about capabilities or datastore configuration; `surrealdb-core` derives the
//! settings from those.
//!
//! [`ParseError`] is the public form of a parse failure: the engine raises it
//! unchanged rather than restating it, so its [`common::LeafError`] impl decides
//! how a client sees it.

use common::keywords::could_be_reserved;
use common::{LeafError, internal_todo};
use parse_common::error::{MessageKind, RenderedError, SyntaxError};
use parse_common::span::Span;
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

use surrealdb_strand::TableName;
use surrealdb_types::{
	Datetime as PublicDatetime, Duration as PublicDuration, Error as TypesError,
	RecordId as PublicRecordId, Value as PublicValue,
};
use tracing::instrument;

use super::query::Query as Ast;
use super::settings::{ParserConfig, ParserSettings};
use super::{entry, value as values};
use crate::expr::{Block, Expr, Fields, Function, Idiom, Kind};

/// Takes a string and returns if it could be a reserved keyword in certain
/// contexts.
pub fn could_be_reserved_keyword(s: &str) -> bool {
	could_be_reserved(s)
}

/// Renders a parser-next error against `input`.
pub fn render(input: &str, e: &entry::Error) -> RenderedError {
	let span = |start: u32, end: u32| Span {
		offset: start,
		len: end.saturating_sub(start),
	};
	let err = match e {
		entry::Error::Syntax(d) => {
			let d = *d;
			let mut err = SyntaxError::new(super::diagnostic::message(input, &d));
			err = if d.label.is_empty() {
				err.with_span(span(d.start, d.end), MessageKind::Error)
			} else {
				err.with_labeled_span(
					span(d.start, d.end),
					MessageKind::Error,
					super::diagnostic::fill(d.label, input, d.start, d.end),
				)
			};
			if let Some(x) = d.extra {
				err = err.with_labeled_span(
					span(x.start, x.end),
					MessageKind::Error,
					super::diagnostic::fill(x.label, input, x.start, x.end),
				);
			}
			err
		}
		entry::Error::Lower(r) => {
			let mut err = SyntaxError::new(&r.msg);
			err = match (r.span, &r.label) {
				(None, _) => err,
				(Some((start, end)), None) => err.with_span(span(start, end), MessageKind::Error),
				(Some((start, end)), Some(label)) => {
					err.with_labeled_span(span(start, end), MessageKind::Error, label)
				}
			};
			if let Some((start, end, label)) = &r.extra {
				err = err.with_labeled_span(span(*start, *end), MessageKind::Error, label);
			}
			err
		}
	};
	err.render_on(input)
}

/// Runs `parse` on `input`, rendering a rejection against it. Spans are
/// `u32`-indexed, so an input longer than `u32::MAX` cannot be pointed at in
/// a diagnostic and is rejected before lexing starts.
fn run<T>(input: &str, parse: impl FnOnce() -> Result<T, entry::Error>) -> Result<T, ParseError> {
	if input.len() > u32::MAX as usize {
		return Err(ParseError::QueryTooLarge);
	}
	parse().map_err(|e| ParseError::InvalidQuery(render(input, &e)))
}

/// Parses a SurrealQL query with every optional grammar enabled.
///
/// Core's `syn::parse` narrows this to the datastore's live capabilities.
#[instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len()))]
pub fn parse(input: &str) -> Result<Ast, ParseError> {
	query_with_settings(input, ParserSettings::all_features(&ParserConfig::default()))
}

/// Parses a SurrealQL query under `settings`.
#[instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len()))]
pub fn query_with_settings(input: &str, settings: ParserSettings) -> Result<Ast, ParseError> {
	run(input, || entry::query(input, &settings.next()))
}

/// Compile request text into engine operators and driver directives.
#[instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len()))]
pub fn execution_query_with_settings(
	input: &str,
	settings: ParserSettings,
) -> Result<Vec<super::ExecutionStatement>, ParseError> {
	run(input, || entry::execution_query(input, settings))
}

/// Parses an expression in which a bare identifier is a field, under
/// `settings`. With `require_all`, anything after the expression is an error.
#[instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len()))]
pub fn expr_with_settings(
	input: &str,
	settings: ParserSettings,
	require_all: bool,
) -> Result<Expr, ParseError> {
	run(input, || entry::expr(input, &settings.next(), require_all))
}

/// Parses a fresh scalar directly into logical form under the caller's limits.
/// Credentials remain inert until execution reaches their definition.
#[instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len()))]
pub fn logical_expr_with_settings(
	input: &str,
	settings: ParserSettings,
	require_all: bool,
) -> Result<crate::expr::logical::LogicalExpr, ParseError> {
	run(input, || entry::logical_expr(input, &settings.next(), require_all, false))
}

/// Parses an idiom without a leading value under `settings`. With
/// `require_all`, anything after the idiom is an error.
#[instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len()))]
pub fn idiom_with_settings(
	input: &str,
	settings: ParserSettings,
	require_all: bool,
) -> Result<Idiom, ParseError> {
	run(input, || entry::idiom(input, &settings.next(), require_all))
}

/// Parses a kind without the enclosing `<` `>` under `settings`. With
/// `require_all`, anything after the kind is an error.
#[instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len()))]
pub fn kind_with_settings(
	input: &str,
	settings: ParserSettings,
	require_all: bool,
) -> Result<Kind, ParseError> {
	run(input, || entry::kind(input, &settings.next(), require_all))
}

/// Parses a projection list, or `VALUE` and one selector, under `settings`.
/// With `require_all`, anything after it is an error.
#[instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len()))]
pub fn fields_with_settings(
	input: &str,
	settings: ParserSettings,
	require_all: bool,
) -> Result<Fields, ParseError> {
	run(input, || entry::fields(input, &settings.next(), require_all))
}

/// Parses a single SurrealQL expression with every optional grammar enabled.
#[instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len()))]
pub fn expr(input: &str) -> Result<Expr, ParseError> {
	expr_with_settings(input, ParserSettings::all_features(&ParserConfig::default()), false)
}

/// Parses a SurrealQL expression, also parsing values found inside strings.
#[instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len()))]
pub fn expr_legacy_strand(input: &str) -> Result<Expr, ParseError> {
	let settings = ParserSettings {
		object_recursion_limit: usize::MAX,
		query_recursion_limit: usize::MAX,
		expr_recursion_limit: usize::MAX,
		legacy_strands: true,
		..Default::default()
	};
	expr_with_settings(input, settings, false)
}

/// Re-parses the canonical block text a catalog definition stores, for
/// example a stored function body. Expects the input wrapped in `{}`, and
/// requires the whole of it to be consumed, for the reason
/// [`expr_for_definition`] documents.
#[instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len()))]
pub fn block(input: &str) -> Result<Block, ParseError> {
	block_with_settings(input, ParserSettings::STORED_TEXT, true).map(|mut value| {
		crate::expr::credentials::prepare(&mut value);
		value
	})
}

/// Parses a `{ ... }` block under caller-chosen limits.
///
/// The unbounded [`ParserSettings::STORED_TEXT`] profile is only safe for text
/// this engine wrote. Parsing keeps its nesting on the heap, so a deeply
/// nested block parses fine, but the lowering every caller runs, and the
/// value's own recursive `Drop`, descend the call stack per node — and a Rust
/// stack overflow aborts the process rather than raising an error. Fresh input
/// therefore has to keep the configured depth limits.
///
/// `require_all_input` is separate because it is a real behaviour difference,
/// not a safety one: stored text must be consumed whole or a definition with
/// trailing content silently compiles to its leading block, while fresh input
/// is allowed to stop at the closing brace.
#[instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len()))]
pub fn block_with_settings(
	input: &str,
	settings: ParserSettings,
	require_all_input: bool,
) -> Result<Block, ParseError> {
	run(input, || entry::block(input, &settings.next(), require_all_input))
}

/// Parses a SurrealQL [`Idiom`].
#[instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len()))]
pub fn idiom(input: &str) -> Result<Idiom, ParseError> {
	idiom_with_settings(input, ParserSettings::default(), false)
}

/// Parses a SurrealQL literal value under the default settings.
#[instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len()))]
pub fn value(input: &str) -> Result<PublicValue, ParseError> {
	value_with_settings(input, ParserSettings::default())
}

/// Parses a SurrealQL literal value under `settings`.
#[instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len()))]
pub fn value_with_settings(
	input: &str,
	settings: ParserSettings,
) -> Result<PublicValue, ParseError> {
	run(input, || values::value(input, &settings.next()))
}

/// Parses a JSON value under `settings`.
#[instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len()))]
pub fn json_with_settings(
	input: &str,
	settings: ParserSettings,
) -> Result<PublicValue, ParseError> {
	run(input, || values::json(input, &settings.next()))
}

/// Parses a record id.
#[instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len()))]
pub fn record_id(input: &str) -> Result<PublicRecordId, ParseError> {
	run(input, || values::record_id(input, &ParserSettings::default().next()))
}

/// Re-parses the canonical kind-grammar text a catalog definition stores for
/// one of its type-annotated fields (e.g. `StoredFieldDefinition.field_kind`,
/// `StoredFunctionDefinition.args`/`.returns`). A kind can embed a `<file>`
/// bucket restriction or other experimental-gated grammar, so this parses
/// under [`ParserSettings::STORED_TEXT`] — the storage wire contract's parser
/// profile — unlike [`kind`], which parses fresh input. The whole of `input`
/// must be consumed, for the reason [`expr_for_definition`] documents.
#[instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len()))]
pub fn kind_for_definition(input: &str) -> Result<Kind, ParseError> {
	kind_with_settings(input, ParserSettings::STORED_TEXT, true).map(|mut value| {
		crate::expr::credentials::prepare(&mut value);
		value
	})
}

/// Parses a SurrealQL [`PublicValue`], also parsing values within strings.
#[instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len()))]
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
/// of its expression fields (e.g. `StoredFieldDefinition.value`,
/// `Permission::Specific`).
///
/// Unlike [`expr`], which parses fresh input under the live capabilities and
/// configured limits, this uses [`ParserSettings::STORED_TEXT`] — the storage
/// wire contract's parser profile. See that constant for the full reasoning.
///
/// # Field context
///
/// A *top-level* bare identifier compiles to `Expr::Idiom([Part::Field(..)])`,
/// never `Expr::Table`. That is the correct reading for every clause stored as
/// expression text: `VALUE`, `ASSERT`, `DEFAULT`, `COMPUTED`, `WHEN`, `THEN`,
/// permission guards, API actions and fetch targets all name a field of the
/// document, and all are parsed in field context when the statement that
/// defines them runs. A statement inside stored text re-establishes table
/// context for its own source list, so `StoredEventDefinition.then` holding
/// `CREATE person` recovers `Expr::Table("person")`; only a top-level bare
/// identifier depends on the entry point. `surrealdb_catalog::text` pins both
/// halves of that.
///
/// The whole of `input` must be consumed. Every `*_for_definition` entry point
/// reads a construct that stops at the first token it cannot continue with,
/// so without that check a stored definition holding trailing content
/// (`"a.b bogus"`) would silently compile to its prefix (`a.b`) and be used as
/// if that were what the user defined.
#[instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len()))]
pub fn expr_for_definition(input: &str) -> Result<Expr, ParseError> {
	expr_with_settings(input, ParserSettings::STORED_TEXT, true).map(|mut value| {
		crate::expr::credentials::prepare(&mut value);
		value
	})
}

/// Compiles stored expression text to its logical scalar without materializing
/// collections and operators as runtime syntax. Uses the same depth limits,
/// complete-input check and credential preparation as [`expr_for_definition`].
#[instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len()))]
pub fn logical_expr_for_definition(
	input: &str,
) -> Result<crate::expr::logical::LogicalExpr, ParseError> {
	run(input, || entry::logical_expr(input, &ParserSettings::STORED_TEXT.next(), true, true))
}

/// Compiles canonical stored block text to its logical sequence.
#[instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len()))]
pub fn logical_block_for_definition(
	input: &str,
) -> Result<crate::expr::logical::LogicalOp, ParseError> {
	run(input, || entry::logical_block(input, &ParserSettings::STORED_TEXT.next()))
}

/// Parses a duration literal and nothing after it.
#[instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len()))]
pub fn duration(input: &str) -> Result<PublicDuration, ParseError> {
	run(input, || entry::duration(input, &ParserSettings::default().next()))
		.map(PublicDuration::from)
}

/// Parses a datetime written without its `d"…"` delimiters.
#[instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len()))]
pub fn datetime(input: &str) -> Result<PublicDatetime, ParseError> {
	run(input, || entry::datetime(input))
}

/// Parses an identifier naming a table. Anything after it is ignored.
#[instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len()))]
pub fn table(input: &str) -> Result<TableName, ParseError> {
	run(input, || entry::table(input, &ParserSettings::default().next()))
}

/// Parses a function name without arguments under `settings`: `fn::…`,
/// `mod::…`, `silo::…`, `ml::…` or a builtin path, which is not checked
/// against the function registry. Anything after the name is ignored.
#[instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len()))]
pub fn function_with_settings(
	input: &str,
	settings: ParserSettings,
) -> Result<Function, ParseError> {
	run(input, || entry::function(input, &settings.next()))
}

/// Re-parses canonical `{ ... }` block text this engine previously rendered:
/// `Block`'s own text-on-the-wire encoding and the stored function-body text
/// (`StoredFunctionDefinition.block`), under [`ParserSettings::STORED_TEXT`],
/// requiring the whole input to be consumed.
#[instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len()))]
pub fn block_for_definition(input: &str) -> Result<Block, ParseError> {
	block(input)
}

/// Re-parses the canonical idiom-path text a catalog definition stores for
/// one of its indexed-column fields (e.g. `StoredIndexDefinition.cols`). An
/// idiom can embed arbitrary sub-expressions (a `[WHERE ...]` filter part, a
/// computed bracket index), so this parses under
/// [`ParserSettings::STORED_TEXT`], unlike [`idiom`], which parses fresh
/// input. The whole of `input` must be consumed, for the reason
/// [`expr_for_definition`] documents.
#[instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len()))]
pub fn idiom_for_definition(input: &str) -> Result<Idiom, ParseError> {
	idiom_with_settings(input, ParserSettings::STORED_TEXT, true).map(|mut value| {
		crate::expr::credentials::prepare(&mut value);
		value
	})
}

/// Re-parses the canonical `SELECT`-clause field-list text a catalog
/// definition stores for one of its field-selection clauses (e.g.
/// `StoredSubscriptionDefinition.fields`), under
/// [`ParserSettings::STORED_TEXT`]. The whole of `input` must be consumed, for
/// the reason [`expr_for_definition`] documents.
#[instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len()))]
pub fn fields_for_definition(input: &str) -> Result<Fields, ParseError> {
	fields_with_settings(input, ParserSettings::STORED_TEXT, true).map(|mut value| {
		crate::expr::credentials::prepare(&mut value);
		value
	})
}

/// Parses a kind without the enclosing `<` `>`.
#[instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len()))]
pub fn kind(input: &str) -> Result<Kind, ParseError> {
	kind_with_settings(input, ParserSettings::default(), false)
}
