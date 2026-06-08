use std::collections::BTreeMap;

use serde::Deserialize;
use surrealdb_core::syn::ParserSettings;
use surrealdb_types::Value;

#[derive(Default, Deserialize, Debug, Clone)]
pub struct Params {
	#[serde(flatten)]
	pub inner: BTreeMap<String, String>,
}

impl Params {
	pub fn parse(self) -> BTreeMap<String, Value> {
		self.into()
	}
}

impl From<Params> for BTreeMap<String, Value> {
	fn from(v: Params) -> BTreeMap<String, Value> {
		v.inner
			.into_iter()
			.map(|(k, v)| {
				let value = surrealdb_core::syn::parse_with_settings(
					v.as_bytes(),
					ParserSettings {
						legacy_strands: true,
						..Default::default()
					},
					async |p, stk| p.parse_json(stk).await,
				)
				.unwrap_or_else(|_| Value::String(v));
				(k, value)
			})
			.collect::<BTreeMap<_, _>>()
	}
}
