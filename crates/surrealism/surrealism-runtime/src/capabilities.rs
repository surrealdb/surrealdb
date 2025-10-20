use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SurrealismCapabilities {
	#[serde(default)]
	pub allow_scripting: bool,
	#[serde(default)]
	pub allow_arbitrary_queries: bool,
	#[serde(default)]
	pub allow_functions: Vec<String>,
	#[serde(default)]
	pub allow_net: Vec<String>,
}

impl Default for SurrealismCapabilities {
	fn default() -> Self {
		Self {
			allow_scripting: false,
			allow_arbitrary_queries: false,
			allow_functions: Vec::new(),
			allow_net: Vec::new(),
		}
	}
}
