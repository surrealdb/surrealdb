use crate::cnf::{MAX_OBJECT_PARSING_DEPTH, MAX_QUERY_PARSING_DEPTH};
use crate::dbs::executor::convert_value_to_public_value;
use crate::sql::expression::convert_public_value_to_internal;
use crate::syn;
use crate::syn::parser::ParserSettings;
use crate::types::PublicValue;

pub fn encode(value: PublicValue) -> anyhow::Result<Vec<u8>> {
	encode_str(value).map(|x| x.into_bytes())
}

pub fn encode_str(value: PublicValue) -> anyhow::Result<String> {
	let v = value
		.into_json_value()
		.ok_or_else(|| anyhow::anyhow!("value cannot be converted into json"))?;
	// Because we convert to serde_json::Value first we can guarantee that
	// serialization wont fail.
	Ok(serde_json::to_string(&v).unwrap())
}

pub fn decode(value: &[u8]) -> anyhow::Result<PublicValue> {
	let settings = ParserSettings {
		object_recursion_limit: *MAX_OBJECT_PARSING_DEPTH as usize,
		query_recursion_limit: *MAX_QUERY_PARSING_DEPTH as usize,
		legacy_strands: true,
		..Default::default()
	};

	syn::parse_with_settings(value, settings, async |parser, stk| parser.parse_value(stk).await)
		.map_err(|err| anyhow::anyhow!(err.to_string()))
}
