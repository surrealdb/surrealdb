use crate::rpc::Method;
use crate::sql::Array;
use crate::sql::Part;
use crate::sql::SqlValue;
use anyhow::{Context, anyhow};
use ciborium::Value as CborValue;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::sync::LazyLock;

pub static ID: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::from("id")]);
pub static METHOD: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::from("method")]);
pub static PARAMS: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::from("params")]);
pub static VERSION: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::from("version")]);

#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
	pub id: Option<String>,
	pub version: Option<u8>,
	pub method: Method,
	pub params: Array,
}

impl TryFrom<CborValue> for Request {
	type Error = anyhow::Error;

	fn try_from(val: CborValue) -> Result<Self, Self::Error> {
		let map = val.into_map().map_err(|_| anyhow!("Invalid CBOR map"))?;

		let mut id = None;
		let mut version = None;
		let mut method = None;
		let mut params = None;
		for (key, value) in map {
			let key = key.into_text().map_err(|_| anyhow!("Invalid key format"))?;
			match key.as_str() {
				"id" => {
					id = Some(value.into_text().map_err(|_| anyhow!("Invalid id format"))?);
				}
				"version" => {
					let int =
						value.into_integer().map_err(|_| anyhow!("Invalid version format"))?;
					version = Some(u8::try_from(int).context("Failed to convert version to u8")?);
				}
				"method" => {
					let method_str =
						value.into_text().map_err(|_| anyhow!("Invalid method format"))?;
					method = Some(Method::parse_case_insensitive(method_str));
				}
				"params" => {
					let params_cbor =
						value.into_array().map_err(|_| anyhow!("Invalid params format"))?;
					let params_expr_array = params_cbor
						.into_iter()
						.map(|cv| SqlValue::try_from(cv).context("Failed to convert CBOR to Value"))
						.collect::<anyhow::Result<Vec<_>>>()?;
					params = Some(Array(params_expr_array));
				}
				_ => return Err(anyhow!("Unknown key in request: {}", key)),
			}
		}

		let method = method.context("Missing method")?;
		let params = params.context("Missing params")?;

		Ok(Self {
			id,
			version,
			method,
			params,
		})
	}
}
