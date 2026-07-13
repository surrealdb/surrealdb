//! GQL (ISO/IEC 39075) front-end for SurrealDB.
//!
//! This module implements a second query language alongside SurrealQL: the
//! ISO GQL property-graph query language (the Cypher-style `MATCH … RETURN …`
//! language standardised as ISO/IEC 39075:2024). Queries are lexed and parsed
//! into a GQL-specific AST ([`ast::GqlQuery`]) which is then lowered to a
//! [`MatchPlan`](crate::expr::match_plan::MatchPlan) — a language-neutral
//! binding-table IR embedded as [`Expr::Match`](crate::expr::Expr::Match) —
//! and executed by the streaming engine. No SurrealQL surface AST is produced
//! along the way; the GQL front-end shares the parser conventions, error
//! types and execution operators of SurrealQL but not its statement AST.
//!
//! The normative v2 contracts live in `doc/gql/`: `V2_DESIGN.md` (the
//! `MatchPlan` IR, operators, planner and plumbing), `LOWERING.md` (the
//! lowering's own responsibilities) and `REFERENCE.md` (the grammar plus the
//! v2 semantic rules R1–R8 and the v1→v2 behaviour-change table).
//!
//! The normative grammar reference lives in `doc/gql/` at the repository
//! root: `REFERENCE.md` is the distilled specification of the supported
//! subset (lexical rules, keyword classes, pattern grammar, expression
//! precedence) and `GQL.g4` is the vendored opengql ANTLR grammar it is
//! derived from. The grammar is **reference-only** — it is never used for
//! code generation; the lexer and parser here are hand-written, mirroring the
//! conventions of the SurrealQL parser in [`crate::syn`].
//!
//! Error reporting reuses [`crate::syn::error::SyntaxError`] and
//! [`crate::syn::token::Span`] so GQL errors render identically to SurrealQL
//! errors.

pub mod ast;
pub mod lexer;
mod lower;
pub mod parser;
pub mod token;

use anyhow::{Result, bail, ensure};
use reblessive::Stack;
use surrealdb_cnf::CommonConfig;

pub use self::lower::PreparedGqlQuery;
use crate::dbs::Capabilities;
use crate::dbs::capabilities::ExperimentalTarget;
use crate::err::Error;
use crate::syn::error::{SyntaxError, syntax_error};
use crate::syn::token::Span;

const TARGET: &str = "surrealdb::core::gql";

/// Parses a GQL query with the default parser settings.
pub fn parse_str(input: &str) -> Result<ast::GqlQuery, SyntaxError> {
	parse_with_settings(input, GqlParserSettings::default())
}

/// Parses a GQL query with the given parser settings.
///
/// During parsing the nesting depth of expressions counts against the limit
/// in the settings; exceeding it is a parse error rather than unbounded
/// recursion.
pub fn parse_with_settings(
	input: &str,
	settings: GqlParserSettings,
) -> Result<ast::GqlQuery, SyntaxError> {
	// `parse_with_capabilities` rejects oversized input with the dedicated
	// `Error::QueryTooLarge` (mirroring `syn`); this guard keeps the `u32`
	// span arithmetic safe for direct callers of the raw parser API.
	if input.len() > u32::MAX as usize {
		return Err(syntax_error!(
			"Cannot parse query, the query exceeded the maximum size of 4GB",
			@Span::empty()
		));
	}
	let mut parser = parser::Parser::new_with_settings(input, settings);
	let mut stack = Stack::new();
	stack.enter(|stk| parser.parse_query(stk)).finish()
}

/// Lowers a parsed GQL query into a [`PreparedGqlQuery`] (the declarative
/// [`MatchPlan`](crate::expr::match_plan::MatchPlan) embedded in a logical
/// plan; `doc/gql/V2_DESIGN.md` §8).
///
/// The returned plan executes through the streaming execution engine; no
/// SurrealQL surface AST is generated.
pub fn lower(query: ast::GqlQuery) -> Result<PreparedGqlQuery, SyntaxError> {
	Ok(PreparedGqlQuery(lower::lower(query)?))
}

/// Parses a GQL query and lowers it into a [`PreparedGqlQuery`], with the given
/// parser settings.
pub fn parse_to_plan_with_settings(
	input: &str,
	settings: GqlParserSettings,
) -> Result<PreparedGqlQuery, SyntaxError> {
	let query = parse_with_settings(input, settings)?;
	lower(query)
}

/// Creates the GQL parser settings from the global configuration values as
/// well as the capabilities struct, mirroring
/// [`crate::syn::settings_from_capabilities_config`].
pub fn settings_from_capabilities_config(
	_capabilities: &Capabilities,
	config: &CommonConfig,
) -> GqlParserSettings {
	// `CommonConfig::max_query_parsing_depth` has no GQL analog: a GQL v1
	// query is a single linear statement with no statement nesting, so all
	// nesting is expression nesting, counted against `max_object_parsing_depth`
	// via `object_recursion_limit`.
	GqlParserSettings {
		object_recursion_limit: config.max_object_parsing_depth as usize,
		expr_recursion_limit: config.max_expression_parsing_depth as usize,
	}
}

/// Parses a GQL query and lowers it into a [`PreparedGqlQuery`] (a
/// [`MatchPlan`](crate::expr::match_plan::MatchPlan) embedded in a logical
/// plan), enforcing the experimental capability gate.
///
/// The whole language is gated behind the `gql` experimental capability,
/// so unlike [`crate::syn::parse_with_capabilities`] (which only derives
/// syntax gating from the capabilities) this enforces the gate itself: every
/// caller routing untrusted input through the capabilities-aware entry point
/// is covered, whether or not it adds its own check.
///
/// During parsing the nesting depth of expressions counts against the
/// configured limit; exceeding it is a parse error rather than unbounded
/// recursion. Errors render exactly like SurrealQL parse errors
/// ([`Error::InvalidQuery`] with a [`crate::syn::error::RenderedError`]).
#[instrument(level = "trace", target = "surrealdb::core::gql", fields(length = input.len()))]
pub fn parse_with_capabilities(
	input: &str,
	capabilities: &Capabilities,
	config: &CommonConfig,
) -> Result<PreparedGqlQuery> {
	trace!(target: TARGET, "Parsing GQL query");

	if !capabilities.allows_experimental(&ExperimentalTarget::Gql) {
		// Deliberately matches the wording of the existing experimental-gate
		// errors (`surrealism`, `files`) rather than naming the server's
		// `--allow-experimental` flag: core is also used embedded, where the
		// capability is enabled programmatically and no CLI flag exists.
		bail!("Experimental capability `gql` is not enabled");
	}
	ensure!(input.len() <= u32::MAX as usize, Error::QueryTooLarge);
	parse_to_plan_with_settings(input, settings_from_capabilities_config(capabilities, config))
		.map_err(|e| e.render_on(input))
		.map_err(Error::InvalidQuery)
		.map_err(anyhow::Error::new)
}

/// Settings which control the behaviour of the GQL parser.
#[derive(Clone, Debug)]
pub struct GqlParserSettings {
	/// Disallow a query to have objects/expressions nested deeper than the
	/// limit. Lists and property maps count towards this limit.
	pub object_recursion_limit: usize,
	/// Bounds the depth of the expression operator tree, including flat
	/// left-associative spines (`1 + 1 + 1 + …`) and prefix chains, which
	/// are otherwise unbounded and overflow the call stack when the lowered
	/// `sql::Expr` tree is later walked recursively (dropped, formatted, or
	/// converted to `expr::Expr`). Mirrors
	/// [`crate::syn::parser::ParserSettings::expr_recursion_limit`].
	pub expr_recursion_limit: usize,
}

impl Default for GqlParserSettings {
	fn default() -> Self {
		GqlParserSettings {
			object_recursion_limit: 100,
			expr_recursion_limit: 128,
		}
	}
}
