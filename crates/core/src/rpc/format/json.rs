use crate::cnf::{MAX_OBJECT_PARSING_DEPTH, MAX_QUERY_PARSING_DEPTH};
use crate::syn;
use crate::syn::parser::ParserSettings;
use crate::types::PublicValue;

pub fn encode(value: PublicValue) -> anyhow::Result<Vec<u8>> {
	encode_str(value).map(|x| x.into_bytes())
}

pub fn encode_str(value: PublicValue) -> anyhow::Result<String> {
	let v = value.into_json_value();
	// Because we convert to serde_json::Value first we can guarantee that
	// serialization wont fail.
	Ok(serde_json::to_string(&v).expect("serialization to json string should not fail"))
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
