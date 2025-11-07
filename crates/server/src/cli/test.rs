use std::collections::BTreeMap;

use anyhow::Result;

use crate::cli::check_upgrade;
use crate::cli::version_client::MapVersionClient;

#[test_log::test(tokio::test)]
pub async fn test_version_upgrade() {
	let mut client = MapVersionClient {
		fetch_mock: BTreeMap::new(),
	};
	client
		.fetch_mock
		.insert("latest".to_string(), || -> Result<String> { Ok("1.0.0".to_string()) });
	check_upgrade(&client, "1.0.0")
		.await
		.expect("Expected the versions to be the same and not require an upgrade");
	check_upgrade(&client, "0.9.0")
		.await
		.expect_err("Expected the versions to be different and require an upgrade");
	check_upgrade(&client, "1.1.0")
		.await
		.expect("Expected the versions to be illogical, and not require and upgrade");
}
