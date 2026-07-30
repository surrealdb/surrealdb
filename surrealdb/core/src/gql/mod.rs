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

use anyhow::{Result, ensure};
use surrealdb_cnf::CommonConfig;
pub use surrealdb_gql::{
	GqlParserSettings, PreparedGqlQuery, ast, lexer, lower, parse_str, parse_to_plan_with_settings,
	parse_with_settings, parser, token,
};

use crate::dbs::Capabilities;
use crate::syn::ParseError;

const TARGET: &str = "surrealdb::core::gql";

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
/// plan).
///
/// Like [`crate::syn::parse_with_capabilities`], this derives syntax gating
/// from the capabilities.
///
/// During parsing the nesting depth of expressions counts against the
/// configured limit; exceeding it is a parse error rather than unbounded
/// recursion. Errors render exactly like SurrealQL parse errors
/// ([`ParseError::InvalidQuery`] with a [`crate::syn::error::RenderedError`]).
#[instrument(level = "trace", target = "surrealdb::core::gql", fields(length = input.len()))]
pub fn parse_with_capabilities(
	input: &str,
	capabilities: &Capabilities,
	config: &CommonConfig,
) -> Result<PreparedGqlQuery> {
	trace!(target: TARGET, "Parsing GQL query");

	ensure!(input.len() <= u32::MAX as usize, ParseError::QueryTooLarge);
	parse_to_plan_with_settings(input, settings_from_capabilities_config(capabilities, config))
		.map_err(|e| e.render_on(input))
		.map_err(ParseError::InvalidQuery)
		.map_err(anyhow::Error::new)
}

#[cfg(test)]
mod tests {
	#[test]
	fn parse_with_capabilities_renders_errors_like_surrealql() {
		use surrealdb_cnf::CommonConfig;

		use crate::dbs::Capabilities;
		use crate::dbs::capabilities::Targets;
		let error = crate::gql::parse_with_capabilities(
			"MATCH (n:person) RETURN m",
			&Capabilities::all().with_experimental(Targets::All),
			&CommonConfig::default(),
		)
		.expect_err("should fail");
		let rendered = format!("{error}");
		assert!(rendered.contains("Parse error"), "unexpected error: {rendered}");
		assert!(rendered.contains("Unknown variable `m`"), "unexpected error: {rendered}");
	}

	#[test]
	fn parse_with_capabilities_available_without_experimental_flag() {
		use surrealdb_cnf::CommonConfig;

		use crate::dbs::Capabilities;
		// GQL is on by default: parsing lowers successfully without any experimental
		// capability being enabled.
		crate::gql::parse_with_capabilities(
			"MATCH (n:person) RETURN n AS n",
			&Capabilities::all(),
			&CommonConfig::default(),
		)
		.expect("GQL should lower without an experimental capability");
	}
}
