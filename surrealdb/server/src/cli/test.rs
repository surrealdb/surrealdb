//! Tests for CLI version upgrade checking functionality.

use std::collections::BTreeMap;

use anyhow::Result;

use crate::cli::check_upgrade;
use crate::cli::version_client::MapVersionClient;

/// Tests the version upgrade checking logic with different version scenarios.
///
/// This test verifies three cases:
/// - Same version: should return None (no upgrade needed)
/// - Older version: should return Some(new_version) (upgrade available)
/// - Newer version: should return None (local version is ahead, no upgrade needed)
#[test_log::test(tokio::test)]
pub async fn test_version_upgrade() {
	let mut client = MapVersionClient {
		fetch_mock: BTreeMap::new(),
	};
	client
		.fetch_mock
		.insert("latest".to_string(), || -> Result<String> { Ok("1.0.0".to_string()) });
	assert_eq!(
		check_upgrade(&client, "1.0.0").await.unwrap(),
		None,
		"Expected the versions to be the same and not require an upgrade"
	);
	assert_eq!(
		check_upgrade(&client, "0.9.0").await.unwrap(),
		Some(semver::Version::parse("1.0.0").unwrap()),
		"Expected the versions to be different and require an upgrade"
	);
	assert_eq!(
		check_upgrade(&client, "1.1.0").await.unwrap(),
		None,
		"Expected the local version to be newer, so no upgrade is required"
	);
}
