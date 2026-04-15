use serde::{Deserialize, Serialize};

/// Module-level capabilities declared in `surrealism.toml`.
///
/// These are validated against the server-level capabilities when the module is
/// loaded. The server can further restrict what a module is allowed to do, but
/// a module cannot exceed the server's limits.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SurrealismCapabilities {
	/// Whether the module requires the scripting capability.
	#[serde(default)]
	pub allow_scripting: bool,
	/// Whether the module is allowed to execute arbitrary SurrealQL queries via
	/// the `sql()` host function. Checked at runtime, not at load time.
	#[serde(default)]
	pub allow_arbitrary_queries: bool,
	/// Which SurrealDB functions the module is allowed to call via `run()`.
	///
	/// - Omitted / empty array: **deny all** function calls (default).
	/// - `["*"]`: allow all functions.
	/// - `["http::*", "fn::user_exists"]`: allow specific families or functions.
	///
	/// Pattern matching follows SurrealDB's `FuncTarget` conventions:
	/// - `"family::*"` or `"family"` matches any function in that family.
	/// - `"family::name"` matches a specific function.
	#[serde(default)]
	pub allow_functions: FunctionTargets,
	/// Network targets the module is allowed to connect to.
	#[serde(default)]
	pub allow_net: Vec<String>,
	/// Maximum WASM linear memory in bytes. `None` means wasmtime default.
	#[serde(default)]
	pub max_memory_bytes: Option<usize>,
	/// Maximum wall-clock execution time per invocation. `None` means unlimited.
	#[serde(default, with = "optional_duration")]
	pub max_execution_time: Option<std::time::Duration>,
	/// Maximum number of controllers to pool per module. `None` uses server default.
	#[serde(default)]
	pub max_pool_size: Option<usize>,
	/// Maximum number of entries in the per-module KV store. `None` means unlimited.
	#[serde(default)]
	pub max_kv_entries: Option<usize>,
	/// Maximum size in bytes for a single KV value. `None` means unlimited.
	#[serde(default)]
	pub max_kv_value_bytes: Option<usize>,
	/// Whether to enforce execution timeouts via epoch-based interruption.
	///
	/// When `true` (default), WASM is compiled with epoch checks at every loop
	/// back-edge and function call, enabling accurate timeout enforcement but
	/// adding overhead (~2x on tight numerical loops, ~10% on typical code).
	///
	/// When `false`, WASM runs at full native speed with no compiled-in checks.
	/// Timeouts declared via `max_execution_time` are **not enforced** — the
	/// module must be trusted to complete within a reasonable time.
	///
	/// # Security implications
	///
	/// A module with `strict_timeout = false` can run indefinitely and
	/// monopolise a thread. It must be **explicitly trusted** by the server
	/// operator. Future code-signing support will gate which modules are
	/// allowed to request this; until then, the operator accepts full
	/// responsibility when loading such a module.
	///
	/// Set to `false` only for compute-heavy, trusted modules (e.g. ML
	/// inference) where performance matters more than timeout enforcement.
	#[serde(default = "default_true")]
	pub strict_timeout: bool,
}

fn default_true() -> bool {
	true
}

impl Default for SurrealismCapabilities {
	fn default() -> Self {
		Self {
			allow_scripting: false,
			allow_arbitrary_queries: false,
			allow_functions: FunctionTargets::default(),
			allow_net: Vec::new(),
			max_memory_bytes: None,
			max_execution_time: None,
			max_pool_size: None,
			max_kv_entries: None,
			max_kv_value_bytes: None,
			strict_timeout: true,
		}
	}
}

/// Function allowlist for a Surrealism module.
///
/// Default is `None` (deny all). Aligns with SurrealDB's `FuncTarget` pattern
/// matching so that `"http::*"` matches any function in the `http` family.
#[derive(Debug, Default, Clone)]
pub enum FunctionTargets {
	/// Deny all function calls (default when omitted or empty).
	#[default]
	None,
	/// Allow all function calls (`["*"]` in config).
	All,
	/// Allow specific functions/families. Each entry is a pattern:
	/// - `"family"` or `"family::*"` — all functions in a family
	/// - `"family::name"` — a specific function
	Some(Vec<String>),
}

impl FunctionTargets {
	/// Check whether a fully-qualified function name is allowed by this target set.
	pub fn allows(&self, fnc: &str) -> bool {
		match self {
			Self::None => false,
			Self::All => true,
			Self::Some(patterns) => patterns.iter().any(|p| func_pattern_matches(p, fnc)),
		}
	}
}

/// Match a function pattern against a fully-qualified function name.
///
/// Mirrors SurrealDB's `FuncTarget::matches` logic:
/// - `"family::name"` requires exact match on both family and name.
/// - `"family::*"` or `"family"` (no `::name`) matches any function in that family.
fn func_pattern_matches(pattern: &str, fnc: &str) -> bool {
	if let Some(family) = pattern.strip_suffix("::*") {
		let f = fnc.split_once("::").map(|(f, _)| f).unwrap_or(fnc);
		f == family
	} else if let Some((pfam, pname)) = pattern.split_once("::") {
		let Some((ffam, fname)) = fnc.split_once("::") else {
			return false;
		};
		pfam == ffam && pname == fname
	} else {
		let f = fnc.split_once("::").map(|(f, _)| f).unwrap_or(fnc);
		f == pattern
	}
}

impl Serialize for FunctionTargets {
	fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		match self {
			Self::None => {
				let empty: Vec<String> = Vec::new();
				empty.serialize(serializer)
			}
			Self::All => vec!["*".to_string()].serialize(serializer),
			Self::Some(patterns) => patterns.serialize(serializer),
		}
	}
}

impl<'de> Deserialize<'de> for FunctionTargets {
	fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		let entries: Vec<String> = Vec::deserialize(deserializer)?;
		if entries.is_empty() {
			return Ok(Self::None);
		}
		if entries.len() == 1 && entries[0] == "*" {
			return Ok(Self::All);
		}
		if entries.iter().any(|e| e == "*") {
			return Ok(Self::All);
		}
		Ok(Self::Some(entries))
	}
}

mod optional_duration {
	use std::time::Duration;

	use serde::{Deserialize, Deserializer, Serialize, Serializer};

	pub fn serialize<S: Serializer>(val: &Option<Duration>, s: S) -> Result<S::Ok, S::Error> {
		match val {
			Some(d) => d.as_millis().serialize(s),
			None => s.serialize_none(),
		}
	}

	pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
		let ms: Option<u64> = Option::deserialize(d)?;
		Ok(ms.map(Duration::from_millis))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn func_pattern_exact_match() {
		assert!(func_pattern_matches("http::get", "http::get"));
		assert!(!func_pattern_matches("http::get", "http::post"));
		assert!(!func_pattern_matches("http::get", "string::len"));
	}

	#[test]
	fn func_pattern_family_wildcard() {
		assert!(func_pattern_matches("http::*", "http::get"));
		assert!(func_pattern_matches("http::*", "http::post"));
		assert!(!func_pattern_matches("http::*", "string::len"));
	}

	#[test]
	fn func_pattern_bare_family() {
		assert!(func_pattern_matches("http", "http::get"));
		assert!(func_pattern_matches("http", "http::post"));
		assert!(!func_pattern_matches("http", "string::len"));
		assert!(func_pattern_matches("fn", "fn::user_exists"));
	}

	#[test]
	fn func_pattern_fn_prefix() {
		assert!(func_pattern_matches("fn::*", "fn::user_exists"));
		assert!(func_pattern_matches("fn::user_exists", "fn::user_exists"));
		assert!(!func_pattern_matches("fn::user_exists", "fn::other"));
	}

	#[test]
	fn function_targets_none_denies_all() {
		let targets = FunctionTargets::None;
		assert!(!targets.allows("http::get"));
		assert!(!targets.allows("fn::anything"));
	}

	#[test]
	fn function_targets_all_allows_all() {
		let targets = FunctionTargets::All;
		assert!(targets.allows("http::get"));
		assert!(targets.allows("fn::anything"));
		assert!(targets.allows("string::len"));
	}

	#[test]
	fn function_targets_some_patterns() {
		let targets = FunctionTargets::Some(vec!["http::*".into(), "fn::user_exists".into()]);
		assert!(targets.allows("http::get"));
		assert!(targets.allows("http::post"));
		assert!(targets.allows("fn::user_exists"));
		assert!(!targets.allows("fn::other"));
		assert!(!targets.allows("string::len"));
	}

	#[test]
	fn function_targets_serde_empty_is_none() {
		let toml_str = r#"
[capabilities]
allow_functions = []
"#;
		#[derive(Deserialize)]
		struct Wrapper {
			capabilities: SurrealismCapabilities,
		}
		let w: Wrapper = toml::from_str(toml_str).unwrap();
		assert!(matches!(w.capabilities.allow_functions, FunctionTargets::None));
	}

	#[test]
	fn function_targets_serde_star_is_all() {
		let toml_str = r#"
[capabilities]
allow_functions = ["*"]
"#;
		#[derive(Deserialize)]
		struct Wrapper {
			capabilities: SurrealismCapabilities,
		}
		let w: Wrapper = toml::from_str(toml_str).unwrap();
		assert!(matches!(w.capabilities.allow_functions, FunctionTargets::All));
	}

	#[test]
	fn function_targets_serde_patterns() {
		let toml_str = r#"
[capabilities]
allow_functions = ["http::*", "fn::user_exists"]
"#;
		#[derive(Deserialize)]
		struct Wrapper {
			capabilities: SurrealismCapabilities,
		}
		let w: Wrapper = toml::from_str(toml_str).unwrap();
		assert!(matches!(w.capabilities.allow_functions, FunctionTargets::Some(_)));
		assert!(w.capabilities.allow_functions.allows("http::get"));
		assert!(w.capabilities.allow_functions.allows("fn::user_exists"));
		assert!(!w.capabilities.allow_functions.allows("string::len"));
	}

	#[test]
	fn function_targets_serde_omitted_is_none() {
		let toml_str = r#"
[capabilities]
allow_scripting = false
"#;
		#[derive(Deserialize)]
		struct Wrapper {
			capabilities: SurrealismCapabilities,
		}
		let w: Wrapper = toml::from_str(toml_str).unwrap();
		assert!(matches!(w.capabilities.allow_functions, FunctionTargets::None));
	}

	#[test]
	fn function_targets_roundtrip() {
		#[derive(Serialize, Deserialize)]
		struct Wrapper {
			targets: FunctionTargets,
		}
		let wrapper = Wrapper {
			targets: FunctionTargets::Some(vec!["http::*".into(), "fn::check".into()]),
		};
		let serialized = toml::to_string(&wrapper).unwrap();
		let deserialized: Wrapper = toml::from_str(&serialized).unwrap();
		assert!(deserialized.targets.allows("http::get"));
		assert!(deserialized.targets.allows("fn::check"));
		assert!(!deserialized.targets.allows("string::len"));
	}

	#[test]
	fn kv_limits_parse() {
		let toml_str = r#"
[capabilities]
max_kv_entries = 1000
max_kv_value_bytes = 65536
"#;
		#[derive(Deserialize)]
		struct Wrapper {
			capabilities: SurrealismCapabilities,
		}
		let w: Wrapper = toml::from_str(toml_str).unwrap();
		assert_eq!(w.capabilities.max_kv_entries, Some(1000));
		assert_eq!(w.capabilities.max_kv_value_bytes, Some(65536));
	}

	#[test]
	fn duration_serde() {
		let toml_str = r#"
[capabilities]
max_execution_time = 5000
"#;
		#[derive(Deserialize)]
		struct Wrapper {
			capabilities: SurrealismCapabilities,
		}
		let w: Wrapper = toml::from_str(toml_str).unwrap();
		assert_eq!(w.capabilities.max_execution_time, Some(std::time::Duration::from_millis(5000)));
	}

	#[test]
	fn strict_timeout_defaults_true() {
		let caps = SurrealismCapabilities::default();
		assert!(caps.strict_timeout);
	}

	#[test]
	fn strict_timeout_serde_default_true() {
		let toml_str = r#"
[capabilities]
allow_scripting = false
"#;
		#[derive(Deserialize)]
		struct Wrapper {
			capabilities: SurrealismCapabilities,
		}
		let w: Wrapper = toml::from_str(toml_str).unwrap();
		assert!(w.capabilities.strict_timeout);
	}

	#[test]
	fn strict_timeout_serde_false() {
		let toml_str = r#"
[capabilities]
strict_timeout = false
"#;
		#[derive(Deserialize)]
		struct Wrapper {
			capabilities: SurrealismCapabilities,
		}
		let w: Wrapper = toml::from_str(toml_str).unwrap();
		assert!(!w.capabilities.strict_timeout);
	}
}
