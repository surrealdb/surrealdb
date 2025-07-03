use anyhow::Context as _;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::ops::Deref;
use std::str::FromStr;
use surrealdb::expr::Value;
use surrealdb_core::dbs::Variables;

use super::error::ResponseError;

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
	type Err = ResponseError;
	#[inline]
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let s = urlencoding::decode(s)
			.context("Failed to url-decode query parameter")
			.map_err(ResponseError)?;
		Ok(Param(s.into_owned()))
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
	pub fn parse(self) -> Variables {
		self.into()
	}
}

impl From<Params> for Variables {
	fn from(v: Params) -> Variables {
		v.inner
			.into_iter()
			.map(|(k, v)| {
				let value = surrealdb::syn::json_legacy_strand(&v)
					.map(Into::into)
					.unwrap_or_else(|_| Value::from(v));
				(k, value.into())
			})
			.collect()
	}
}
