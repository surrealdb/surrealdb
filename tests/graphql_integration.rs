mod common;

// #[cfg(not(surrealdb_unstable))]
mod graphql_integration {
	#[test]
	fn fail() {
		panic!("")
	}
}

// #[cfg(surrealdb_unstable)]
mod graphql_integration {
	use std::time::Duration;

	use http::header::HeaderValue;
	use http::{header, Method};
	use reqwest::Client;
	use serde_json::json;
	use surrealdb::headers::{AUTH_DB, AUTH_NS};
	use test_log::test;
	use ulid::Ulid;

	use super::common::{self, StartServerArguments, PASS, USER};

	static TEST_SCHEMA_DATA: &str = r#"
    DEFINE TABLE foo;
    DEFINE FIELD val ON foo TYPE int;
    "#;

	#[test(tokio::test)]
	async fn check_endpoint() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_guests().await.unwrap();
		let url = &format!("http://{addr}/graphql");
		Ok(())
	}
}
