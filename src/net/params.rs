use serde::Deserialize;
use std::collections::BTreeMap;
use surrealdb::sql::Value;

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
			.map(|(k, v)| (k, surrealdb::sql::json(&v).unwrap_or_else(|_| Value::from(v))))
			.collect::<BTreeMap<_, _>>()
	}
}
