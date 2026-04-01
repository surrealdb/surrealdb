use semver::Version;
use serde::{Deserialize, Serialize};
use surrealism_types::err::{PrefixErr, SurrealismResult};

use crate::capabilities::SurrealismCapabilities;

/// Source language / toolchain the module is written in.
///
/// Only `Rust` is supported today. The field is used by `init` (to choose
/// which scaffold to generate) and `build` (to choose which toolchain to
/// invoke). Once compiled to WASM the target is irrelevant at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Target {
	#[default]
	Rust,
}

impl std::fmt::Display for Target {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Target::Rust => write!(f, "rust"),
		}
	}
}

/// Which ABI version the plugin targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AbiVersion(pub u32);

impl AbiVersion {
	pub const CURRENT: Self = Self(2);
}

impl Serialize for AbiVersion {
	fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		serializer.serialize_u32(self.0)
	}
}

impl<'de> Deserialize<'de> for AbiVersion {
	fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		struct Visitor;
		impl serde::de::Visitor<'_> for Visitor {
			type Value = AbiVersion;

			fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
				write!(f, "an integer ABI version (2) or legacy string (\"p1\", \"p2\")")
			}

			fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<AbiVersion, E> {
				u32::try_from(v)
					.map(AbiVersion)
					.map_err(|_| E::custom(format!("ABI version out of range: {v}")))
			}

			fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<AbiVersion, E> {
				u32::try_from(v)
					.map(AbiVersion)
					.map_err(|_| E::custom(format!("ABI version out of range: {v}")))
			}

			fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<AbiVersion, E> {
				match v {
					"p1" | "1" => Ok(AbiVersion(1)),
					"p2" | "2" => Ok(AbiVersion(2)),
					other => Err(E::custom(format!("unknown ABI version: {other}"))),
				}
			}
		}
		deserializer.deserialize_any(Visitor)
	}
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SurrealismConfig {
	#[serde(default)]
	pub target: Target,
	#[serde(rename = "package")]
	pub meta: SurrealismMeta,
	#[serde(default)]
	pub capabilities: SurrealismCapabilities,
	#[serde(default)]
	pub abi: AbiVersion,
	#[serde(default)]
	pub attach: SurrealismAttach,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct SurrealismAttach {
	/// Path to a directory whose contents are bundled into the archive and
	/// mounted as a read-only filesystem for the WASM module. Can be relative
	/// (resolved against the project root at build time) or absolute.
	pub fs: Option<String>,
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

	pub fn to_toml(&self) -> SurrealismResult<String> {
		toml::to_string(self).prefix_err(|| "Failed to serialize Surrealism config")
	}

	pub fn file_name(&self) -> String {
		format!("{}-{}-{}.surli", self.meta.organisation, self.meta.name, self.meta.version)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parse_minimal_config() {
		let toml = r#"
[package]
organisation = "test"
name = "demo"
version = "1.0.0"
"#;
		let config = SurrealismConfig::parse(toml).unwrap();
		assert_eq!(config.target, Target::Rust);
		assert_eq!(config.meta.organisation, "test");
		assert_eq!(config.meta.name, "demo");
		assert_eq!(config.meta.version, Version::new(1, 0, 0));
		assert_eq!(config.abi, AbiVersion::default());
	}

	#[test]
	fn parse_explicit_target() {
		let toml = r#"
target = "rust"

[package]
organisation = "t"
name = "t"
version = "0.1.0"
"#;
		let config = SurrealismConfig::parse(toml).unwrap();
		assert_eq!(config.target, Target::Rust);
	}

	#[test]
	fn parse_full_config() {
		let toml = r#"
abi = 2

[package]
organisation = "acme"
name = "widget"
version = "2.3.1"

[capabilities]
allow_scripting = true
allow_arbitrary_queries = true
allow_functions = ["http::*", "fn::check"]

[attach]
fs = "static"
"#;
		let config = SurrealismConfig::parse(toml).unwrap();
		assert_eq!(config.meta.name, "widget");
		assert_eq!(config.abi, AbiVersion::CURRENT);
		assert!(config.capabilities.allow_scripting);
		assert!(config.capabilities.allow_arbitrary_queries);
		assert!(config.capabilities.allow_functions.allows("http::get"));
		assert!(config.capabilities.allow_functions.allows("fn::check"));
		assert!(!config.capabilities.allow_functions.allows("string::len"));
		assert_eq!(config.attach.fs.as_deref(), Some("static"));
	}

	#[test]
	fn abi_version_integer() {
		let toml = r#"
abi = 2

[package]
organisation = "t"
name = "t"
version = "0.1.0"
"#;
		let config = SurrealismConfig::parse(toml).unwrap();
		assert_eq!(config.abi, AbiVersion(2));
	}

	#[test]
	fn abi_version_legacy_string() {
		let toml = r#"
abi = "p2"

[package]
organisation = "t"
name = "t"
version = "0.1.0"
"#;
		let config = SurrealismConfig::parse(toml).unwrap();
		assert_eq!(config.abi, AbiVersion(2));
	}

	#[test]
	fn abi_version_legacy_p1() {
		let toml = r#"
abi = "p1"

[package]
organisation = "t"
name = "t"
version = "0.1.0"
"#;
		let config = SurrealismConfig::parse(toml).unwrap();
		assert_eq!(config.abi, AbiVersion(1));
	}

	#[test]
	fn config_roundtrip() {
		let config = SurrealismConfig {
			target: Target::Rust,
			meta: SurrealismMeta {
				organisation: "test".to_string(),
				name: "roundtrip".to_string(),
				version: Version::new(0, 1, 0),
			},
			capabilities: Default::default(),
			abi: AbiVersion::CURRENT,
			attach: SurrealismAttach {
				fs: Some("data".to_string()),
			},
		};

		let toml_str = config.to_toml().unwrap();
		let parsed = SurrealismConfig::parse(&toml_str).unwrap();
		assert_eq!(parsed.target, Target::Rust);
		assert_eq!(parsed.meta.organisation, "test");
		assert_eq!(parsed.meta.name, "roundtrip");
		assert_eq!(parsed.abi, AbiVersion::CURRENT);
		assert_eq!(parsed.attach.fs.as_deref(), Some("data"));
	}

	#[test]
	fn file_name_format() {
		let config = SurrealismConfig {
			target: Target::default(),
			meta: SurrealismMeta {
				organisation: "surrealdb".to_string(),
				name: "demo".to_string(),
				version: Version::new(1, 2, 3),
			},
			capabilities: Default::default(),
			abi: AbiVersion::CURRENT,
			attach: Default::default(),
		};
		assert_eq!(config.file_name(), "surrealdb-demo-1.2.3.surli");
	}

	#[test]
	fn attach_default_is_none() {
		let toml = r#"
[package]
organisation = "t"
name = "t"
version = "0.1.0"
"#;
		let config = SurrealismConfig::parse(toml).unwrap();
		assert!(config.attach.fs.is_none());
	}
}
