//! The GQL dialect of SurrealQL.
//!
//! Lexer, parser and AST for GQL queries, and the lowering that turns a
//! parsed query into a `PreparedGqlQuery` — a match plan embedded in the
//! expression layer's logical plan.
//!
//! This crate is an internal implementation detail of SurrealDB: its API is
//! unstable and changes without notice. Depend on the `surrealdb` SDK or
//! `surrealdb-core` instead.

pub mod ast;
pub mod lexer;
pub mod lower;
pub mod parser;
pub mod token;

// The module tree spells intra-layer paths as `crate::gql::<name>` and names
// its neighbours as `crate::<layer>`; these aliases keep those paths valid.
pub use surrealdb_expr::{expr, val};
pub(crate) use surrealdb_sql as sql;
pub(crate) use surrealdb_syn as syn;

pub use self::lower::PreparedGqlQuery;

pub(crate) mod gql {
	//! Self-alias so `crate::gql::` paths written inside the module tree
	//! resolve against the crate root.
	pub(crate) use crate::*;
}

impl Default for GqlParserSettings {
	fn default() -> Self {
		GqlParserSettings {
			object_recursion_limit: 100,
			expr_recursion_limit: 128,
		}
	}
}

use reblessive::Stack;
use surrealdb_syn::error::SyntaxError;
use surrealdb_syn::syntax_error;
use surrealdb_syn::token::Span;

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
	// `ParseError::QueryTooLarge` (mirroring `syn`); this guard keeps the `u32`
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
