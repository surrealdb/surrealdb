use anyhow::Context as _;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::ops::Deref;
use std::str::FromStr;
use surrealdb::sql::SqlValue;

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

impl From<Param> for SqlValue {
	#[inline]
	fn from(v: Param) -> Self {
		SqlValue::from(v.0)
	}
}

#[derive(Default, Deserialize, Debug, Clone)]
pub struct Params {
	#[serde(flatten)]
	pub inner: BTreeMap<String, String>,
}

impl Params {
	pub fn parse(self) -> BTreeMap<String, SqlValue> {
		self.into()
	}
}

impl From<Params> for BTreeMap<String, SqlValue> {
	fn from(v: Params) -> BTreeMap<String, SqlValue> {
		v.inner
			.into_iter()
			.map(|(k, v)| {
				let value = surrealdb::syn::json_legacy_strand(&v);
				(k, value.unwrap_or_else(|_| SqlValue::from(v)))
			})
			.collect::<BTreeMap<_, _>>()
	}
}
