use std::time::{Duration, SystemTime};

use http::{HeaderMap, StatusCode, header};
use reqwest::Client;
use serde_json::Value as JsonValue;
use tokio::time::sleep;
use tracing::{debug, error, warn};

use crate::common::expected::Expected;

// A very basic Rest client.
// The goal is to have a client that can connect to any version of SurrealDB.
// Using the REST API / sql endpoint
pub struct RestClient {
	client: Client,
	url: String,
	user: String,
	pass: String,
}

impl RestClient {
	pub fn new(ns: &str, db: &str, user: &str, pass: &str, port: u16) -> Self {
		let mut headers = HeaderMap::new();
		headers.insert("NS", ns.parse().unwrap());
		headers.insert("DB", db.parse().unwrap());
		headers.insert(header::ACCEPT, "application/json".parse().unwrap());
		let client = Client::builder()
			.connect_timeout(Duration::from_millis(10))
			.default_headers(headers)
			.build()
			.expect("Client::builder()...build()");
		Self {
			client,
			url: format!("http://127.0.0.1:{port}/sql"),
			user: user.to_string(),
			pass: pass.to_string(),
		}
	}

	pub async fn wait_for_connection(self, time_out: &Duration) -> Option<Self> {
		sleep(Duration::from_secs(2)).await;
		let start = SystemTime::now();
		while start.elapsed().unwrap().le(time_out) {
			sleep(Duration::from_secs(2)).await;
			if let Some(r) = self.query("INFO FOR ROOT").await {
				if r.status() == StatusCode::OK {
					return Some(self);
				}
			}
			warn!("DB not yet responding");
			sleep(Duration::from_secs(2)).await;
		}
		None
	}

	pub async fn query(&self, q: &str) -> Option<reqwest::Response> {
		match self
			.client
			.post(&self.url)
			.basic_auth(&self.user, Some(&self.pass))
			.body(q.to_string())
			.send()
			.await
		{
			Ok(r) => Some(r),
			Err(e) => {
				error!("{e}");
				None
			}
		}
	}

	pub async fn checked_query(&self, q: &str, expected: &Expected) {
		let r = self.query(q).await.unwrap_or_else(|| panic!("No response for {q}"));
		assert_eq!(
			r.status(),
			StatusCode::OK,
			"Wrong response for {q} -> {}",
			r.text().await.expect(q)
		);
		// Convert the result to JSON
		let j: JsonValue = r.json().await.expect(q);
		debug!("{q} => {j:#}");
		// The result should be an array
		let results_with_status = j.as_array().expect(q);
		assert_eq!(results_with_status.len(), 1, "Wrong number of results on query {q}");
		let result_with_status = &results_with_status[0];
		// Check the status
		let status =
			result_with_status.get("status").unwrap_or_else(|| panic!("No status on query: {q}"));
		assert_eq!(status.as_str(), Some("OK"), "Wrong status for {q} => {status:#}");
		// Extract the results
		let results =
			result_with_status.get("result").unwrap_or_else(|| panic!("No result for query: {q}"));
		if !matches!(expected, Expected::Any) {
			// Check the results
			let results = results.as_array().expect(q);
			expected.check_results(q, results);
		}
	}
}
