#![forbid(unsafe_code)]

//! Test harness and specification runner for SurrealDB SQL front ends.
//!
//! Provides utilities for loading conformance datasets, validating SHA-256 manifests,
//! verifying conformance locks, and running differential tests against reference baselines.

use std::fs;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

/// Representation of a `conformance.lock` file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConformanceLock {
	/// Lockfile layout version.
	pub layout: u32,
	/// Source repository URL.
	pub repo: String,
	/// Git commit hash pinned by the lockfile.
	pub commit: String,
	/// SHA-256 digest of the dataset manifest.
	pub manifest_sha256: String,
	/// Pinned dataset relative paths.
	pub datasets: Vec<String>,
}

impl ConformanceLock {
	/// Parse a `conformance.lock` string.
	pub fn parse(content: &str) -> Result<Self, String> {
		let table: toml::Table =
			content.parse().map_err(|e| format!("failed to parse toml lockfile: {e}"))?;

		let layout = table
			.get("layout")
			.and_then(|v| v.as_integer())
			.ok_or_else(|| "missing or invalid 'layout' field".to_string())? as u32;

		let source = table
			.get("source")
			.and_then(|v| v.as_table())
			.ok_or_else(|| "missing or invalid '[source]' table".to_string())?;

		let repo = source
			.get("repo")
			.and_then(|v| v.as_str())
			.ok_or_else(|| "missing or invalid 'source.repo'".to_string())?
			.to_string();

		let commit = source
			.get("commit")
			.and_then(|v| v.as_str())
			.ok_or_else(|| "missing or invalid 'source.commit'".to_string())?
			.to_string();

		let manifest_sha256 = source
			.get("manifest_sha256")
			.and_then(|v| v.as_str())
			.ok_or_else(|| "missing or invalid 'source.manifest_sha256'".to_string())?
			.to_string();

		let mut datasets = Vec::new();
		if let Some(arr) = table.get("dataset").and_then(|v| v.as_array()) {
			for item in arr {
				if let Some(p) = item.get("path").and_then(|v| v.as_str()) {
					datasets.push(p.to_string());
				}
			}
		}

		Ok(Self {
			layout,
			repo,
			commit,
			manifest_sha256,
			datasets,
		})
	}

	/// Load and parse a `conformance.lock` from a file path.
	pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, String> {
		let content =
			fs::read_to_string(path).map_err(|e| format!("failed to read lockfile: {e}"))?;
		Self::parse(&content)
	}

	/// Locate the fetched dataset directory in `target/sql-conformance/<commit>`.
	pub fn dataset_dir(&self, target_dir: &Path) -> PathBuf {
		target_dir.join("sql-conformance").join(&self.commit)
	}

	/// Verify that the fetched directory has a matching MANIFEST.sha256.
	pub fn verify_checksum<P: AsRef<Path>>(&self, dataset_dir: P) -> Result<bool, String> {
		let manifest_path = dataset_dir.as_ref().join("MANIFEST.sha256");
		let manifest_bytes = fs::read(&manifest_path).map_err(|e| {
			format!("failed to read MANIFEST.sha256 at {}: {e}", manifest_path.display())
		})?;

		let mut hasher = Sha256::new();
		hasher.update(&manifest_bytes);
		let computed = hex::encode(hasher.finalize());

		Ok(computed.eq_ignore_ascii_case(&self.manifest_sha256))
	}
}

/// Metadata parsed from a dataset's `manifest.toml`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuiteManifest {
	/// Suite name (e.g. "postgresql-v18").
	pub name: String,
	/// Description of the suite.
	pub description: String,
	/// Upstream git ref or branch.
	pub upstream_ref: String,
	/// Reference oracle/parser version.
	pub pg_query_version: String,
	/// Total statement count.
	pub total_statements: u64,
	/// Unique statement count.
	pub unique_statements: u64,
	/// Statements accepted by the reference parser.
	pub accepted_statements: u64,
	/// Statements rejected by the reference parser.
	pub rejected_statements: u64,
}

impl SuiteManifest {
	/// Parse a `manifest.toml` content string.
	pub fn parse(content: &str) -> Result<Self, String> {
		let table: toml::Table =
			content.parse().map_err(|e| format!("failed to parse manifest.toml: {e}"))?;

		let suite = table
			.get("suite")
			.and_then(|v| v.as_table())
			.ok_or_else(|| "missing '[suite]' table in manifest.toml".to_string())?;

		let get_str = |key: &str| -> Result<String, String> {
			suite
				.get(key)
				.and_then(|v| v.as_str())
				.map(|s| s.to_string())
				.ok_or_else(|| format!("missing string key '{key}' in manifest.toml"))
		};

		let get_int = |key: &str| -> Result<u64, String> {
			suite
				.get(key)
				.and_then(|v| v.as_integer())
				.map(|i| i as u64)
				.ok_or_else(|| format!("missing integer key '{key}' in manifest.toml"))
		};

		Ok(Self {
			name: get_str("name")?,
			description: get_str("description")?,
			upstream_ref: get_str("upstream_ref")?,
			pg_query_version: get_str("pg_query_version")?,
			total_statements: get_int("total_statements")?,
			unique_statements: get_int("unique_statements")?,
			accepted_statements: get_int("accepted_statements")?,
			rejected_statements: get_int("rejected_statements")?,
		})
	}

	/// Load and parse a `manifest.toml` file.
	pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, String> {
		let content =
			fs::read_to_string(path).map_err(|e| format!("failed to read manifest.toml: {e}"))?;
		Self::parse(&content)
	}
}

/// Summary metrics parsed from a dataset's `summary.json`.
#[derive(Debug, Clone, PartialEq)]
pub struct SuiteSummary {
	/// Total records processed.
	pub records: u64,
	/// Unique SQL texts.
	pub unique_texts: u64,
	/// Number of distinct AST node types observed.
	pub distinct_node_types: u64,
	/// Reference accept count.
	pub accepted: u64,
	/// Reference reject count.
	pub rejected: u64,
}

impl SuiteSummary {
	/// Parse a `summary.json` string.
	pub fn parse(content: &str) -> Result<Self, String> {
		let val: serde_json::Value = serde_json::from_str(content)
			.map_err(|e| format!("failed to parse summary.json: {e}"))?;

		let records = val.get("records").and_then(|v| v.as_u64()).unwrap_or(0);
		let unique_texts = val.get("unique_texts").and_then(|v| v.as_u64()).unwrap_or(0);
		let distinct_node_types =
			val.get("distinct_node_types").and_then(|v| v.as_u64()).unwrap_or(0);
		let accepted = val.get("pg18_accept").and_then(|v| v.as_u64()).unwrap_or(0);
		let rejected = val.get("pg18_reject").and_then(|v| v.as_u64()).unwrap_or(0);

		Ok(Self {
			records,
			unique_texts,
			distinct_node_types,
			accepted,
			rejected,
		})
	}

	/// Load and parse a `summary.json` file.
	pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, String> {
		let content =
			fs::read_to_string(path).map_err(|e| format!("failed to read summary.json: {e}"))?;
		Self::parse(&content)
	}
}
