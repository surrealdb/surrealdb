use std::collections::BTreeMap;

use serde::Deserialize;
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
				let value = surrealdb_core::syn::json_legacy_strand(&v)
					.unwrap_or_else(|_| Value::String(v));
				(k, value)
			})
			.collect::<BTreeMap<_, _>>()
	}
}
