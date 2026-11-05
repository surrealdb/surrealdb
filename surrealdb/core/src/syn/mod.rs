//! Core's SurrealQL front end: the parser's entry points with settings
//! derived from the datastore's capabilities and configuration.

use std::collections::HashSet;

use Query as Ast;
pub use surrealdb_expr::parser as runtime;
pub use surrealdb_expr::parser::{
	ParseError, ParserConfig, ParserSettings, Query, StatementStream, TopLevelExpr,
};

use crate::dbs::Capabilities;
use crate::dbs::capabilities::ExperimentalTarget;
use crate::expr::kind::KindLiteral;
use crate::expr::{Block, Expr, Function, Idiom, Kind};
use crate::types::{PublicDatetime, PublicDuration, PublicRecordId, PublicValue};

#[cfg(test)]
pub trait Parse<T> {
	fn parse(val: &str) -> T;
}

#[cfg(test)]
mod test;

use anyhow::Result;

const TARGET: &str = "surrealdb::core::syn";

/// Takes a string and returns if it could be a reserved keyword in certain
/// contexts.
pub fn could_be_reserved_keyword(s: &str) -> bool {
	surrealdb_expr::parser::could_be_reserved_keyword(s)
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
	config: &ParserConfig,
) -> ParserSettings {
	ParserSettings {
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
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len())))]
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
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len())))]
pub fn parse_with_capabilities(
	input: &str,
	capabilities: &Capabilities,
	config: &ParserConfig,
) -> Result<Ast> {
	trace!(target: TARGET, "Parsing SurrealQL query");

	lift(surrealdb_expr::parser::query_with_settings(
		input,
		settings_from_capabilities_config(capabilities, config),
	))
}

/// Parses a SurrealQL [`Expr`].
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len())))]
#[allow(dead_code)]
pub(crate) fn expr(input: &str) -> Result<Expr> {
	let capabilities = Capabilities::all();
	expr_with_capabilities(input, &capabilities, &ParserConfig::default())
}

/// Parses a SurrealQL [`Value`].
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len())))]
#[allow(dead_code)]
pub(crate) fn expr_with_capabilities(
	input: &str,
	capabilities: &Capabilities,
	config: &ParserConfig,
) -> Result<Expr> {
	trace!(target: TARGET, "Parsing SurrealQL value");

	lift(surrealdb_expr::parser::expr_with_settings(
		input,
		settings_from_capabilities_config(capabilities, config),
		false,
	))
}

/// Parses a SurrealQL function name.
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len())))]
pub fn function_with_capabilities(
	input: &str,
	capabilities: &Capabilities,
	config: &ParserConfig,
) -> Result<Function> {
	trace!(target: TARGET, "Parsing SurrealQL function name");

	lift(surrealdb_expr::parser::function_with_settings(
		input,
		settings_from_capabilities_config(capabilities, config),
	))
}

/// Parses JSON into an inert SurrealQL [`PublicValue`].
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len())))]
pub fn json(input: &str) -> Result<PublicValue> {
	trace!(target: TARGET, "Parsing inert JSON value");

	let settings = ParserSettings {
		json_string_escapes: true,
		import: false,
		..Default::default()
	};

	lift(surrealdb_expr::parser::json_with_settings(input, settings))
}

/// Parses a SurrealQL [`Idiom`]
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len())))]
pub(crate) fn idiom(input: &str) -> Result<Idiom> {
	trace!(target: TARGET, "Parsing SurrealQL idiom");

	lift(surrealdb_expr::parser::idiom(input))
}

/// Parse a datetime without enclosing delimiters from a string.
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len())))]
pub fn datetime(input: &str) -> Result<PublicDatetime> {
	trace!(target: TARGET, "Parsing SurrealQL datetime");

	lift(surrealdb_expr::parser::datetime(input))
}

/// Parse a duration from a string.
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len())))]
pub fn duration(input: &str) -> Result<PublicDuration> {
	trace!(target: TARGET, "Parsing SurrealQL duration");

	lift(surrealdb_expr::parser::duration(input))
}

/// Parse a record id.
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len())))]
pub fn record_id(input: &str) -> Result<PublicRecordId> {
	trace!(target: TARGET, "Parsing SurrealQL record id");

	lift(surrealdb_expr::parser::record_id(input))
}

/// Parse a table name from a string.
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len())))]
pub fn table(input: &str) -> Result<surrealdb_strand::TableName> {
	trace!(target: TARGET, "Parsing SurrealQL table name");

	lift(surrealdb_expr::parser::table(input))
}

/// Parses a `{ ... }` block of fresh input, under the configured depth limits.
///
/// Distinct from [`surrealdb_expr::parser::block_for_definition`], which parses under the
/// bounded stored-text profile. That profile is safe only for text this engine
/// wrote: parsing is heap-stacked and survives arbitrary nesting, but the
/// lowering to `expr::Block` and the value's recursive `Drop` descend the call
/// stack, and a stack overflow aborts the process instead of returning an error.
/// Fresh input therefore keeps the limits, whoever hands it in.
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len())))]
pub fn block(input: &str) -> Result<Block> {
	trace!(target: TARGET, "Parsing SurrealQL block");

	lift(surrealdb_expr::parser::block_with_settings(
		input,
		ParserSettings::all_features(&ParserConfig::default()),
		false,
	))
}

/// Parses a SurrealQL [`Value`] and parses values within strings.
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len())))]
#[allow(dead_code)]
pub(crate) fn expr_legacy_strand(input: &str) -> Result<Expr> {
	trace!(target: TARGET, "Parsing SurrealQL value, with legacy strings");

	lift(surrealdb_expr::parser::expr_legacy_strand(input))
}

/// Parses a SurrealQL [`PublicValue`] and parses values within strings.
///
/// This function is for testing only, don't use it outside of tests!
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len())))]
pub fn value(input: &str) -> Result<PublicValue> {
	trace!(target: TARGET, "Parsing SurrealQL value");

	lift(surrealdb_expr::parser::value(input))
}

/// Parses a SurrealQL [`PublicValue`] and parses values within strings.
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len())))]
pub fn value_legacy_strand(input: &str, config: &ParserConfig) -> Result<PublicValue> {
	trace!(target: TARGET, "Parsing SurrealQL value, with legacy strings");

	lift(surrealdb_expr::parser::value_legacy_strand(input, config))
}

/// Parses JSON into an inert SurrealQL [`PublicValue`] and parses values within
/// strings.
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len())))]
pub fn json_legacy_strand(input: &str, config: &ParserConfig) -> Result<PublicValue> {
	trace!(target: TARGET, "Parsing inert JSON value, with legacy strings");

	let settings = ParserSettings {
		object_recursion_limit: config.max_object_parsing_depth as usize,
		query_recursion_limit: config.max_query_parsing_depth as usize,
		legacy_strands: true,
		json_string_escapes: true,
		import: false,
		..Default::default()
	};

	lift(surrealdb_expr::parser::json_with_settings(input, settings))
}

/// Parse a kind from a string.
#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::syn", skip(input), fields(length = input.len())))]
pub fn kind(input: &str) -> Result<Kind> {
	trace!(target: TARGET, "Parsing SurrealQL kind");

	lift(surrealdb_expr::parser::kind(input))
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
		| Kind::Named(_)
		| Kind::None
		| Kind::Null
		| Kind::Bool
		| Kind::Bytes
		| Kind::Datetime
		| Kind::Date
		| Kind::Time
		| Kind::Timestamp
		| Kind::Interval
		| Kind::Decimal
		| Kind::BigDecimal
		| Kind::Duration
		| Kind::Float
		| Kind::Int
		| Kind::Number
		| Kind::Object
		| Kind::String
		| Kind::Uuid
		| Kind::Regex
		| Kind::I8
		| Kind::U8
		| Kind::I16
		| Kind::U16
		| Kind::I32
		| Kind::U32
		| Kind::U64
		| Kind::F16
		| Kind::F32
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
			KindLiteral::String(_)
			| KindLiteral::Integer(_)
			| KindLiteral::Float(_)
			| KindLiteral::Decimal(_)
			| KindLiteral::Duration(_)
			| KindLiteral::Bool(_) => {}
		},
		Kind::File(_) | Kind::Enum(_) => {}
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

	/// An inert JSON value nests as deep as the config's object depth allows,
	/// and no deeper: the depth bounds the parse of a JSON body as it does a
	/// SurrealQL value's.
	#[test]
	fn json_legacy_strand_nests_to_the_configured_depth() {
		let config = ParserConfig::default();
		let nested = r#"{"a": [1, {"b": "2020-01-01T00:00:00Z"}]}"#;
		let value = json_legacy_strand(nested, &config).unwrap();
		assert!(matches!(value, PublicValue::Object(_)), "{value:?}");

		let shallow = ParserConfig {
			max_object_parsing_depth: 1,
			..config
		};
		json_legacy_strand(r#"{"a": 1}"#, &shallow).unwrap();
		assert!(json_legacy_strand(r#"{"a": [1]}"#, &shallow).is_err(), "two levels under one");
	}
}
