//! Module containing the implementation of the surrealql tokens, lexer, and
//! parser.

use std::collections::HashSet;

use surrealdb_cnf::CommonConfig;
pub use surrealdb_syn::{ParseError, error, lexer, parser, token};

use crate::dbs::Capabilities;
use crate::dbs::capabilities::ExperimentalTarget;
use crate::sql::kind::KindLiteral;
use crate::sql::{Ast, Block, Expr, Fields, Function, Idiom, Kind};
use crate::types::{PublicDatetime, PublicDuration, PublicRecordId, PublicValue};

#[cfg(test)]
pub trait Parse<T> {
	fn parse(val: &str) -> T;
}

#[cfg(test)]
mod test;

use anyhow::{Result, bail, ensure};
use lexer::Lexer;
pub use parser::ParserSettings;
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

/// Creates the parser settings struct from the global configuration values as
/// wel as the capabilities  struct.
pub fn settings_from_capabilities_config(
	cap: &Capabilities,
	config: &CommonConfig,
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
	parse_with_capabilities(input, &capabilities, &CommonConfig::default())
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
	config: &CommonConfig,
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
	expr_with_capabilities(input, &capabilities, &CommonConfig::default())
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
pub(crate) fn expr_for_definition(input: &str) -> Result<Expr> {
	trace!(target: TARGET, "Parsing SurrealQL expression for a stored catalog definition");

	parse_with_settings(input.as_bytes(), ParserSettings::STORED_TEXT, async |parser, stk| {
		let expr = parser.parse_expr_field(stk).await?;
		parser.assert_finished()?;
		Ok(expr)
	})
}

/// Parses a SurrealQL [`Value`].
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
#[allow(dead_code)]
pub(crate) fn expr_with_capabilities(
	input: &str,
	capabilities: &Capabilities,
	config: &CommonConfig,
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
	config: &CommonConfig,
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

/// Re-parses the canonical idiom-path text a catalog definition stores for
/// one of its indexed-column fields (e.g. `StoredIndexDefinition.cols`). An idiom
/// can embed arbitrary sub-expressions (a `[WHERE ...]` filter part, a
/// computed bracket index), so this parses under
/// [`ParserSettings::STORED_TEXT`] — the storage wire contract's parser
/// profile — unlike [`idiom`], which parses fresh input. The whole of `input`
/// must be consumed, for the reason [`expr_for_definition`] documents.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub(crate) fn idiom_for_definition(input: &str) -> Result<Idiom> {
	trace!(target: TARGET, "Parsing SurrealQL idiom for a stored catalog definition");

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
pub(crate) fn fields_for_definition(input: &str) -> Result<Fields> {
	trace!(target: TARGET, "Parsing SurrealQL field list for a stored catalog definition");

	parse_with_settings(input.as_bytes(), ParserSettings::STORED_TEXT, async |parser, stk| {
		let fields = parser.parse_fields(stk).await?;
		parser.assert_finished()?;
		Ok(fields)
	})
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

/// Re-parses canonical `{ ... }` block text this engine previously rendered:
/// `Block`'s own text-on-the-wire encoding and the stored function-body text
/// (`StoredFunctionDefinition.block`). Both are engine-authored, so this parses
/// under [`ParserSettings::STORED_TEXT`] — the storage wire contract's parser
/// profile — and requires the whole input to be consumed. Expects the input to
/// be wrapped in `{}`.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub(crate) fn block_for_definition(input: &str) -> Result<Block> {
	trace!(target: TARGET, "Parsing SurrealQL block for a stored definition");

	lift(surrealdb_syn::block(input))
}

/// Parses a `{ ... }` block of fresh input.
///
/// Distinct from [`block_for_definition`] because the unbounded stored-text
/// profile must not be reachable from outside: this module is `pub`, so a
/// caller elsewhere in the workspace (or out of tree) can hand it arbitrary
/// input. Parsing that is heap-stacked and survives, but the lowering to
/// `expr::Block` and the value's recursive `Drop` are not, and a stack
/// overflow aborts the process instead of returning an error.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub fn block(input: &str) -> Result<Block> {
	trace!(target: TARGET, "Parsing SurrealQL block");

	lift(surrealdb_syn::block_with_settings(
		input,
		ParserSettings::all_features(&CommonConfig::default()),
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
pub fn value_legacy_strand(input: &str, config: &CommonConfig) -> Result<PublicValue> {
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
pub fn json_legacy_strand(input: &str, config: &CommonConfig) -> Result<PublicValue> {
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

/// Re-parses the canonical kind-grammar text a catalog definition stores for
/// one of its type-annotated fields (e.g. `StoredFieldDefinition.field_kind`,
/// `StoredFunctionDefinition.args`/`.returns`). A kind can embed a `<file>` bucket
/// restriction or other experimental-gated grammar, so this parses under
/// [`ParserSettings::STORED_TEXT`] — the storage wire contract's parser
/// profile — unlike [`kind`], which parses fresh input. The whole of `input`
/// must be consumed, for the reason [`expr_for_definition`] documents.
#[instrument(level = "trace", target = "surrealdb::core::syn", fields(length = input.len()))]
pub(crate) fn kind_for_definition(input: &str) -> Result<Kind> {
	trace!(target: TARGET, "Parsing SurrealQL kind for a stored catalog definition");

	parse_with_settings(input.as_bytes(), ParserSettings::STORED_TEXT, async |parser, stk| {
		let kind = parser.parse_inner_kind(stk).await?;
		parser.assert_finished()?;
		Ok(kind)
	})
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
