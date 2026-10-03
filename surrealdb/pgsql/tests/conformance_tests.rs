//! Specification and conformance test suite for surrealdb-pgsql.

use std::path::PathBuf;

use surrealdb_sqlfront_spec::{ConformanceLock, SuiteManifest, SuiteSummary};

fn find_workspace_root() -> PathBuf {
	let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
	while !path.join("Cargo.lock").exists() {
		if !path.pop() {
			panic!("could not find workspace root containing Cargo.lock");
		}
	}
	path
}

#[test]
fn test_parse_and_verify_conformance_lock() {
	let root = find_workspace_root();
	let lock_path = root.join("surrealdb/pgsql/conformance.lock");
	assert!(lock_path.exists(), "conformance.lock must exist at {}", lock_path.display());

	let lock = ConformanceLock::from_file(&lock_path).expect("valid conformance.lock");
	assert_eq!(lock.layout, 1);
	assert_eq!(lock.repo, "https://github.com/surrealdb/sql-conformance.git");
	assert_eq!(lock.commit, "dde2c174a582b3fbe9fd25bb35965ec0fd5195b9");
	assert_eq!(
		lock.manifest_sha256,
		"0675b507d223d89fb94a6dec3dae72ab7ed4ff9dc1a459437e3bd828285092a9"
	);
	assert!(lock.datasets.contains(&"suites/postgresql-v18".to_string()));

	// If the dataset has been fetched to target/, verify its checksum
	let target_dir = root.join("target");
	let dataset_dir = lock.dataset_dir(&target_dir);
	if dataset_dir.exists() {
		let matches = lock.verify_checksum(&dataset_dir).expect("verify checksum");
		assert!(matches, "MANIFEST.sha256 checksum must match pinned hash");
	}
}

#[test]
fn test_parse_suite_manifest_and_summary() {
	let root = find_workspace_root();
	let target_dir = root.join("target");
	let lock_path = root.join("surrealdb/pgsql/conformance.lock");
	let lock = ConformanceLock::from_file(&lock_path).expect("valid conformance.lock");
	let dataset_dir = lock.dataset_dir(&target_dir);

	let suite_dir = dataset_dir.join("suites/postgresql-v18");
	if !suite_dir.exists() {
		// Dataset not yet fetched in this environment; test is non-blocking
		return;
	}

	let manifest =
		SuiteManifest::from_file(suite_dir.join("manifest.toml")).expect("valid manifest.toml");
	assert_eq!(manifest.name, "postgresql-v18");
	assert_eq!(manifest.upstream_ref, "REL_18_STABLE");
	assert_eq!(manifest.total_statements, 50435);
	assert_eq!(manifest.unique_statements, 39298);
	assert_eq!(manifest.accepted_statements, 39117);
	assert_eq!(manifest.rejected_statements, 181);

	let summary =
		SuiteSummary::from_file(suite_dir.join("summary.json")).expect("valid summary.json");
	assert_eq!(summary.records, 50435);
	assert_eq!(summary.unique_texts, 39298);
	assert_eq!(summary.distinct_node_types, 192);
	assert_eq!(summary.accepted, 39117);
	assert_eq!(summary.rejected, 181);
}
