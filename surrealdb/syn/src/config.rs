//! The parser's configurable depth limits.
//!
//! These three limits bound how deep a query's text may nest before parsing
//! fails, and they live here because this crate is the lowest layer that reads
//! them. Everything above — core's capability-gated wrappers, the GQL dialect,
//! the API-request decoder, and the server's RPC/WebSocket/export body
//! decoders — reads them downward from here.
//!
//! Each limit exists to keep a recursive walk within a conservative worker
//! stack, so raising one trades a wider accepted grammar for a smaller safety
//! margin against stack exhaustion from query text alone.

use surrealdb_cnf as cnf;

/// Configurable parser depth limits.
#[derive(Clone, Debug)]
pub struct ParserConfig {
	/// How deep the parser will parse nested objects and arrays. Type
	/// annotations (`array<option<array<int>>>`) count against this too.
	pub max_object_parsing_depth: u32,
	/// How deep the parser will parse recursive queries — a statement nested
	/// inside another statement, as in subqueries and blocks.
	pub max_query_parsing_depth: u32,
	/// How deep the parser will build an expression operator tree. Bounds
	/// left-associative operator spines (`1 + 1 + 1 + …`) and prefix/postfix
	/// chains, which consume neither of the other two budgets and would
	/// otherwise build an arbitrarily deep tree that later recursive walks
	/// (`Drop`, `ToSql`, the lowering to `expr::Expr`) overflow the stack on.
	pub max_expression_parsing_depth: u32,
}

impl Default for ParserConfig {
	fn default() -> Self {
		Self {
			max_object_parsing_depth: 100,
			max_query_parsing_depth: 20,
			max_expression_parsing_depth: 128,
		}
	}
}

impl cnf::Config for ParserConfig {
	fn parse(&mut self, map: &cnf::ConfigMap) {
		map.parse_key("max_object_parsing_depth", &mut self.max_object_parsing_depth)
			.parse_key("max_query_parsing_depth", &mut self.max_query_parsing_depth)
			.parse_key("max_expression_parsing_depth", &mut self.max_expression_parsing_depth);
	}
}
