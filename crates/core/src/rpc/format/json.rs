use surrealdb_types::Value;

use crate::cnf::{MAX_OBJECT_PARSING_DEPTH, MAX_QUERY_PARSING_DEPTH};
use crate::dbs::executor::convert_value_to_public_value;
use crate::sql::expression::convert_public_value_to_internal;
use crate::syn;
use crate::syn::parser::ParserSettings;

pub fn encode(value: Value) -> anyhow::Result<Vec<u8>> {
	encode_str(value).map(|x| x.into_bytes())
}

pub fn encode_str(value: Value) -> anyhow::Result<String> {
	// Convert public value to internal value
	let internal_value = convert_public_value_to_internal(value);
	let v = internal_value
		.into_json_value()
		.ok_or_else(|| anyhow::anyhow!("value cannot be converted into json"))?;
	// Because we convert to serde_json::Value first we can guarantee that
	// serialization wont fail.
	Ok(serde_json::to_string(&v).unwrap())
}

pub fn decode(value: &[u8]) -> anyhow::Result<Value> {
	let settings = ParserSettings {
		object_recursion_limit: *MAX_OBJECT_PARSING_DEPTH as usize,
		query_recursion_limit: *MAX_QUERY_PARSING_DEPTH as usize,
		legacy_strands: true,
		..Default::default()
	};
	let internal_value = syn::parse_with_settings(value, settings, async |parser, stk| {
		parser.parse_value(stk).await
	})
	.map_err(|err| anyhow::anyhow!(err.to_string()))?;
	// Convert internal value to public value
	convert_value_to_public_value(internal_value)
}
