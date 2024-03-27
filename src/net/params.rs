use crate::err::Error;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::ops::Deref;
use std::str::FromStr;
use surrealdb::sql::Value;

#[derive(Debug, Clone)]
pub struct Param(pub String);

impl Deref for Param {
	type Target = str;
	#[inline]
	fn deref(&self) -> &Self::Target {
		self.0.as_str()
	}
}

impl FromStr for Param {
	type Err = Error;
	#[inline]
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let s = urlencoding::decode(s)?.into_owned();
		Ok(Param(s))
	}
}

impl From<Param> for Value {
	#[inline]
	fn from(v: Param) -> Self {
		Value::from(v.0)
	}
}

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
				#[cfg(feature = "parser2")]
				let value = surrealdb::syn::json_legacy_strand(&v);
				#[cfg(not(feature = "parser2"))]
				let value = surrealdb::syn::json(&v);

				(k, value.unwrap_or_else(|_| Value::from(v)))
			})
			.collect::<BTreeMap<_, _>>()
	}
}
