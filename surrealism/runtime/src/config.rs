use anyhow::Result;
use semver::Version;
use serde::{Deserialize, Serialize};
use surrealism_types::err::PrefixError;

use crate::capabilities::SurrealismCapabilities;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SurrealismConfig {
	#[serde(rename = "package")]
	pub meta: SurrealismMeta,
	#[serde(default)]
	pub capabilities: SurrealismCapabilities,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SurrealismMeta {
	pub organisation: String,
	pub name: String,
	pub version: Version,
}

impl SurrealismConfig {
	pub fn parse(s: &str) -> Result<Self> {
		toml::from_str(s).prefix_err(|| "Failed to parse Surrealism config")
	}

	pub fn to_string(&self) -> Result<String> {
		toml::to_string(self).prefix_err(|| "Failed to serialize Surrealism config")
	}

	pub fn file_name(&self) -> String {
		format!("{}-{}-{}.surli", self.meta.organisation, self.meta.name, self.meta.version)
	}
}
