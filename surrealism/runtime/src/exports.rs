//! Exports manifest: function signatures baked into `.surli` archives.
//!
//! Stored as `surrealism/exports.toml` inside the package. Args and returns
//! use hex-encoded Kind blobs for stable serialization across SDK versions.

use serde::{Deserialize, Serialize};
use surrealdb_types::Kind;
use surrealism_types::err::{PrefixErr, SurrealismResult};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportsManifest {
	pub functions: Vec<FunctionExport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionExport {
	/// `None` for the default export, `Some("name")` for named exports.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub name: Option<String>,
	/// Named argument list: each entry is `(arg_name, kind)`.
	#[serde(with = "hex_argument_list")]
	pub args: Vec<(String, Kind)>,
	#[serde(with = "hex_kind")]
	pub returns: Kind,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub args_text: Option<Vec<String>>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub returns_text: Option<String>,
	/// Whether this function may perform writes. Opt-in via `#[surrealism(writeable)]`.
	/// Defaults to `false` (read-only) for backward compatibility.
	#[serde(default)]
	pub writeable: bool,
	/// Human-readable comment for this function, aligned with SurrealQL's `COMMENT`
	/// clause. Sourced from Rust doc comments or `#[surrealism(comment = "...")]`.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub comment: Option<String>,
}

impl FunctionExport {
	pub fn args_display(&self) -> String {
		self.args_text.as_ref().map(|v| v.join(", ")).unwrap_or_else(|| {
			self.args
				.iter()
				.map(|(name, kind)| format!("{name}: {kind}"))
				.collect::<Vec<_>>()
				.join(", ")
		})
	}

	pub fn returns_display(&self) -> String {
		self.returns_text
			.as_deref()
			.map(|s| s.to_string())
			.unwrap_or_else(|| format!("{}", self.returns))
	}
}

impl ExportsManifest {
	/// Create an empty manifest. Used during the build step before signatures
	/// have been extracted.
	pub fn empty() -> Self {
		Self {
			functions: Vec::new(),
		}
	}

	pub fn parse(s: &str) -> SurrealismResult<Self> {
		toml::from_str(s).prefix_err(|| "Failed to parse exports manifest")
	}

	pub fn to_toml(&self) -> SurrealismResult<String> {
		toml::to_string(self).prefix_err(|| "Failed to serialize exports manifest")
	}

	/// Look up a function by name. `None` matches the default export.
	pub fn get_signature(&self, name: Option<&str>) -> Option<&FunctionExport> {
		self.functions.iter().find(|f| f.name.as_deref() == name)
	}
}

mod hex_kind {
	use serde::{Deserialize, Deserializer, Serializer};
	use surrealdb_types::Kind;

	pub fn serialize<S: Serializer>(kind: &Kind, serializer: S) -> Result<S::Ok, S::Error> {
		let bytes = surrealdb_types::encode_kind(kind).map_err(serde::ser::Error::custom)?;
		serializer.serialize_str(&hex::encode(bytes))
	}

	pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Kind, D::Error> {
		let s = String::deserialize(deserializer)?;
		let bytes = hex::decode(s).map_err(serde::de::Error::custom)?;
		surrealdb_types::decode_kind(&bytes).map_err(serde::de::Error::custom)
	}
}

mod hex_argument_list {
	use serde::{Deserialize, Deserializer, Serializer};
	use surrealdb_types::Kind;

	pub fn serialize<S: Serializer>(
		args: &[(String, Kind)],
		serializer: S,
	) -> Result<S::Ok, S::Error> {
		let pairs: Vec<(&str, Kind)> = args.iter().map(|(n, k)| (n.as_str(), k.clone())).collect();
		let bytes =
			surrealdb_types::encode_argument_list(&pairs).map_err(serde::ser::Error::custom)?;
		serializer.serialize_str(&hex::encode(bytes))
	}

	pub fn deserialize<'de, D: Deserializer<'de>>(
		deserializer: D,
	) -> Result<Vec<(String, Kind)>, D::Error> {
		let s = String::deserialize(deserializer)?;
		let bytes = hex::decode(s).map_err(serde::de::Error::custom)?;
		surrealdb_types::decode_argument_list(&bytes).map_err(serde::de::Error::custom)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn roundtrip_manifest() {
		let manifest = ExportsManifest {
			functions: vec![
				FunctionExport {
					name: None,
					args: vec![("value".to_string(), Kind::Int)],
					returns: Kind::Bool,
					args_text: Some(vec!["value: int".to_string()]),
					returns_text: Some("bool".to_string()),
					writeable: false,
					comment: Some("Checks whether a value is valid.".to_string()),
				},
				FunctionExport {
					name: Some("foo::bar".to_string()),
					args: vec![
						("input".to_string(), Kind::String),
						("tags".to_string(), Kind::Array(Box::new(Kind::String), None)),
					],
					returns: Kind::Object,
					args_text: None,
					returns_text: None,
					writeable: true,
					comment: None,
				},
			],
		};

		let toml_str = manifest.to_toml().unwrap();
		let parsed = ExportsManifest::parse(&toml_str).unwrap();
		assert_eq!(manifest.functions.len(), parsed.functions.len());

		assert!(parsed.functions[0].name.is_none());
		assert_eq!(parsed.functions[0].args, vec![("value".to_string(), Kind::Int)]);
		assert_eq!(parsed.functions[0].returns, Kind::Bool);
		assert!(!parsed.functions[0].writeable);
		assert_eq!(
			parsed.functions[0].comment.as_deref(),
			Some("Checks whether a value is valid.")
		);

		assert_eq!(parsed.functions[1].name.as_deref(), Some("foo::bar"));
		assert_eq!(parsed.functions[1].args.len(), 2);
		assert_eq!(parsed.functions[1].args[0].0, "input");
		assert_eq!(parsed.functions[1].args[1].0, "tags");
		assert_eq!(parsed.functions[1].returns, Kind::Object);
		assert!(parsed.functions[1].writeable);
		assert!(parsed.functions[1].comment.is_none());
	}

	#[test]
	fn get_signature_default() {
		let manifest = ExportsManifest {
			functions: vec![FunctionExport {
				name: None,
				args: vec![("n".to_string(), Kind::Int)],
				returns: Kind::Bool,
				args_text: None,
				returns_text: None,
				writeable: false,
				comment: None,
			}],
		};

		assert!(manifest.get_signature(None).is_some());
		assert!(manifest.get_signature(Some("nonexistent")).is_none());
	}

	#[test]
	fn hex_roundtrip() {
		let bytes = vec![0x0c, 0x00, 0xff, 0xab];
		let encoded = hex::encode(&bytes);
		assert_eq!(encoded, "0c00ffab");
		let decoded = hex::decode(&encoded).unwrap();
		assert_eq!(bytes, decoded);
	}
}
