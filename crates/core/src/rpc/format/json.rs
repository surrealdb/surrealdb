use surrealdb_types::Value;

use crate::cnf::{MAX_OBJECT_PARSING_DEPTH, MAX_QUERY_PARSING_DEPTH};
use crate::syn;
use crate::syn::parser::ParserSettings;

pub fn encode(value: Value) -> anyhow::Result<Vec<u8>> {
	encode_str(value).map(|x| x.into_bytes())
}

pub fn encode_str(value: Value) -> anyhow::Result<String> {
	todo!("STU")
	// let v =
	// 	value.into_json_value().ok_or_else(|| "value cannot be converted into json".to_owned())?;
	// // Because we convert to serde_json::Value first we can guarentee that
	// // serialization wont fail.
	// Ok(serde_json::to_string(&v).unwrap())
}

pub fn decode(value: &[u8]) -> anyhow::Result<Value> {
	todo!("STU")
	// let settings = ParserSettings {
	// 	object_recursion_limit: *MAX_OBJECT_PARSING_DEPTH as usize,
	// 	query_recursion_limit: *MAX_QUERY_PARSING_DEPTH as usize,
	// 	legacy_strands: true,
	// 	..Default::default()
	// };
	// syn::parse_with_settings(value, settings, async |parser, stk| parser.parse_value(stk).await)
	//     .map_err(|err| err.to_string())
}
