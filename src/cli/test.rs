use crate::cli::check_upgrade;
use crate::cli::version_client::MapVersionClient;
use std::borrow::Cow;

#[test_log::test(tokio::test)]
pub async fn test_version_upgrade() {
	let client = MapVersionClient {
		fetch_mock: map!(
			"latest".to_string() => Ok("1.0.0".to_string()),
		),
	};
	check_upgrade(&client, "latest").await;
}
