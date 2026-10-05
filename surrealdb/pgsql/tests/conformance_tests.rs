//! Specification and conformance test suite for surrealdb-pgsql.

use std::path::{Path, PathBuf};

use surrealdb_pgsql::parse_statements;
use surrealdb_pgsql::printer::ToPgSql;
use surrealdb_sqlfront_spec::{ConformanceLock, ConformanceRatchet, SuiteManifest, SuiteSummary};

fn find_workspace_root() -> PathBuf {
	let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
	while !path.join("Cargo.lock").exists() {
		if !path.pop() {
			panic!("could not find workspace root containing Cargo.lock");
		}
	}
	path
}

fn target_dir(root: &Path) -> PathBuf {
	if let Ok(dir) = std::env::var("CARGO_TARGET_DIR") {
		PathBuf::from(dir)
	} else {
		root.join("target")
	}
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

	// If the dataset has been fetched, verify its checksum
	let target = target_dir(&root);
	let dataset_dir = lock.dataset_dir(&target);
	if dataset_dir.exists() {
		let matches = lock.verify_checksum(&dataset_dir).expect("verify checksum");
		assert!(matches, "MANIFEST.sha256 checksum must match pinned hash");
	}
}

#[test]
fn test_parse_suite_manifest_and_summary() {
	let root = find_workspace_root();
	let target = target_dir(&root);
	let lock_path = root.join("surrealdb/pgsql/conformance.lock");
	let lock = ConformanceLock::from_file(&lock_path).expect("valid conformance.lock");
	let dataset_dir = lock.dataset_dir(&target);

	let suite_dir = dataset_dir.join("suites/postgresql-v18");
	if !suite_dir.exists() {
		if std::env::var("CI").is_ok() {
			panic!(
				"conformance dataset missing under CI at {}. Run scripts/sqlfront-conformance/fetch.sh",
				suite_dir.display()
			);
		}
		// Dataset not yet fetched in non-CI environment; test is non-blocking
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

#[test]
fn test_conformance_ratchet_evaluation() {
	let mut ratchet = ConformanceRatchet::new();
	ratchet.record_parse_success();
	ratchet.record_parse_success();
	ratchet.record_parse_failure();
	assert_eq!(ratchet.total_evaluated, 3);
	assert_eq!(ratchet.parse_success, 2);
	assert_eq!(ratchet.parse_failure, 1);
	assert!((ratchet.parse_rate() - 66.666).abs() < 0.01);

	ratchet.record_projection_success();
	ratchet.record_projection_mismatch();
	assert_eq!(ratchet.projection_success, 1);
	assert_eq!(ratchet.projection_mismatch, 1);
	assert!((ratchet.projection_rate() - 33.333).abs() < 0.01);

	assert!(ratchet.assert_floor(2).is_ok());
	assert!(ratchet.assert_floor(5).is_err());
}

#[test]
fn test_canonical_statements_parse_and_roundtrip() {
	let queries = [
		"SELECT 1;",
		"SELECT u.name, u.age FROM users u WHERE u.age > 18 ORDER BY u.name ASC;",
		"SELECT count(*) FROM orders;",
		"SELECT p.category, sum(p.price) FROM products p GROUP BY p.category HAVING count(*) > 1;",
		"INSERT INTO users (id, name) VALUES (1, 'Alice');",
		"UPDATE users SET age = age + 1 WHERE id = 1;",
		"DELETE FROM users WHERE age < 0;",
	];

	let mut ratchet = ConformanceRatchet::new();
	for q in queries {
		match parse_statements(q) {
			Ok(stmts) => {
				ratchet.record_parse_success();
				let printed =
					stmts.iter().map(|s| s.to_pgsql_string()).collect::<Vec<_>>().join(";\n");
				match parse_statements(&printed) {
					Ok(roundtrip_stmts) => {
						if roundtrip_stmts.len() == stmts.len() {
							ratchet.record_projection_success();
						} else {
							ratchet.record_projection_mismatch();
						}
					}
					Err(_) => {
						ratchet.record_projection_mismatch();
					}
				}
			}
			Err(_) => {
				ratchet.record_parse_failure();
			}
		}
	}

	let q_count = queries.len() as u64;
	assert_eq!(ratchet.total_evaluated, q_count);
	assert_eq!(ratchet.parse_success, q_count);
	assert_eq!(ratchet.projection_success, q_count);
	assert!(ratchet.assert_floor(q_count).is_ok());
}
