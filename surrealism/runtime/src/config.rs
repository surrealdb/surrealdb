use semver::Version;
use serde::{Deserialize, Serialize};
use surrealism_types::err::{PrefixErr, SurrealismResult};

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
	pub fn parse(s: &str) -> SurrealismResult<Self> {
		toml::from_str(s).prefix_err(|| "Failed to parse Surrealism config")
	}

	pub fn to_string(&self) -> SurrealismResult<String> {
		toml::to_string(self).prefix_err(|| "Failed to serialize Surrealism config")
	}

	pub fn file_name(&self) -> String {
		format!("{}-{}-{}.surli", self.meta.organisation, self.meta.name, self.meta.version)
	}
}
