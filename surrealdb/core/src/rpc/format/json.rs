use crate::syn;
use crate::syn::parser::ParserSettings;
use crate::types::PublicValue;

pub fn encode(value: PublicValue) -> anyhow::Result<Vec<u8>> {
	encode_str(value).map(|x| x.into_bytes())
}

pub fn encode_str(value: PublicValue) -> anyhow::Result<String> {
	let v = value.into_json_value();
	Ok(serde_json::to_string(&v).expect("serialization to json string should not fail"))
}

pub fn decode(value: &[u8]) -> anyhow::Result<PublicValue> {
	decode_with_limits(
		value,
		*surrealdb_cfg::MAX_OBJECT_PARSING_DEPTH,
		*surrealdb_cfg::MAX_QUERY_PARSING_DEPTH,
	)
}

pub fn decode_with_limits(
	value: &[u8],
	max_object_parsing_depth: u32,
	max_query_parsing_depth: u32,
) -> anyhow::Result<PublicValue> {
	let settings = ParserSettings {
		object_recursion_limit: max_object_parsing_depth as usize,
		query_recursion_limit: max_query_parsing_depth as usize,
		legacy_strands: true,
		..Default::default()
	};

	syn::parse_with_settings(value, settings, async |parser, stk| parser.parse_value(stk).await)
		.map_err(|err| anyhow::anyhow!(err.to_string()))
}
