// RUST_LOG=warn cargo make ci-ml-integration
mod common;

#[cfg(feature = "ml")]
mod ml_integration {

	use std::sync::atomic::{AtomicBool, Ordering};
	use std::time::Duration;

	use http::{StatusCode, header};
	use reqwest::Body;
	use serde::{Deserialize, Serialize};
	use surrealdb_core::ml::storage::stream_adapter::StreamAdapter;
	use test_log::test;
	use ulid::Ulid;

	use super::*;

	#[derive(Serialize, Deserialize, Debug)]
	struct ErrorResponse {
		code: u16,
		details: String,
		description: String,
		information: String,
	}

	static LOCK: AtomicBool = AtomicBool::new(false);

	#[derive(Serialize, Deserialize, Debug)]
	struct Data {
		result: Vec<f64>,
		status: String,
		time: String,
	}

	struct LockHandle;

	impl LockHandle {
		fn acquire_lock() -> Self {
			while LOCK.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
				!= Ok(false)
			{
				std::thread::sleep(Duration::from_millis(100));
			}
			LockHandle
		}
	}

	impl Drop for LockHandle {
		fn drop(&mut self) {
			LOCK.store(false, Ordering::Release);
		}
	}

	async fn upload_file(addr: &str, ns: &str, db: &str) -> Result<(), Box<dyn std::error::Error>> {
		let generator = StreamAdapter::new(5, "./tests/linear_test.surml".to_string()).unwrap();
		let body = Body::wrap_stream(generator);
		// Prepare HTTP client
		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_secs(1))
			.default_headers(headers)
			.build()?;
		// Send HTTP request
		let res = client
			.post(format!("http://{addr}/ml/import"))
			.basic_auth(common::USER, Some(common::PASS))
			.body(body)
			.send()
			.await?;
		// Check response code
		assert_eq!(res.status(), StatusCode::OK);
		Ok(())
	}

	#[test(tokio::test)]
	async fn upload_model() -> Result<(), Box<dyn std::error::Error>> {
		let _lock = LockHandle::acquire_lock();
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		upload_file(&addr, &ns, &db).await?;
		Ok(())
	}

	#[test(tokio::test)]
	async fn upload_bad_file() -> Result<(), Box<dyn std::error::Error>> {
		let _lock = LockHandle::acquire_lock();
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		let generator = StreamAdapter::new(5, "./tests/should_crash.surml".to_string()).unwrap();
		let body = Body::wrap_stream(generator);
		// Prepare HTTP client
		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_secs(1))
			.default_headers(headers)
			.build()?;
		// Send HTTP request
		let res = client
			.post(format!("http://{addr}/ml/import"))
			.basic_auth(common::USER, Some(common::PASS))
			.body(body)
			.send()
			.await?;
		// Check response code
		let raw_data = res.text().await?;
		let response: ErrorResponse = serde_json::from_str(&raw_data)?;

		assert_eq!(response.code, 400);
		assert_eq!(
			"Not enough bytes to read for header, maybe the file format is not correct".to_string(),
			response.information
		);
		Ok(())
	}

	#[test(tokio::test)]
	async fn upload_file_with_no_name() -> Result<(), Box<dyn std::error::Error>> {
		let _lock = LockHandle::acquire_lock();
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		let generator = StreamAdapter::new(5, "./tests/no_name.surml".to_string()).unwrap();
		let body = Body::wrap_stream(generator);
		// Prepare HTTP client
		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_secs(1))
			.default_headers(headers)
			.build()?;
		// Send HTTP request
		let res = client
			.post(format!("http://{addr}/ml/import"))
			.basic_auth(common::USER, Some(common::PASS))
			.body(body)
			.send()
			.await?;
		// Check response code
		let raw_data = res.text().await?;
		let response: ErrorResponse = serde_json::from_str(&raw_data)?;

		assert_eq!(response.code, 400);
		assert_eq!("Model name and version must be set".to_string(), response.information);
		Ok(())
	}

	#[test(tokio::test)]
	async fn upload_file_with_no_version() -> Result<(), Box<dyn std::error::Error>> {
		let _lock = LockHandle::acquire_lock();
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		let generator = StreamAdapter::new(5, "./tests/no_version.surml".to_string()).unwrap();
		let body = Body::wrap_stream(generator);
		// Prepare HTTP client
		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_secs(1))
			.default_headers(headers)
			.build()?;
		// Send HTTP request
		let res = client
			.post(format!("http://{addr}/ml/import"))
			.basic_auth(common::USER, Some(common::PASS))
			.body(body)
			.send()
			.await?;
		// Check response code
		let raw_data = res.text().await?;
		let response: ErrorResponse = serde_json::from_str(&raw_data)?;

		assert_eq!(response.code, 400);
		assert_eq!("Model name and version must be set".to_string(), response.information);
		Ok(())
	}

	#[test(tokio::test)]
	async fn upload_file_with_no_version_or_name() -> Result<(), Box<dyn std::error::Error>> {
		let _lock = LockHandle::acquire_lock();
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		let generator =
			StreamAdapter::new(5, "./tests/no_name_or_version.surml".to_string()).unwrap();
		let body = Body::wrap_stream(generator);
		// Prepare HTTP client
		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_secs(1))
			.default_headers(headers)
			.build()?;
		// Send HTTP request
		let res = client
			.post(format!("http://{addr}/ml/import"))
			.basic_auth(common::USER, Some(common::PASS))
			.body(body)
			.send()
			.await?;
		// Check response code
		let raw_data = res.text().await?;
		let response: ErrorResponse = serde_json::from_str(&raw_data)?;

		assert_eq!(response.code, 400);
		assert_eq!("Model name and version must be set".to_string(), response.information);
		Ok(())
	}

	#[test(tokio::test)]
	async fn raw_compute() -> Result<(), Box<dyn std::error::Error>> {
		let _lock = LockHandle::acquire_lock();
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();

		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		upload_file(&addr, &ns, &db).await?;

		// Prepare HTTP client
		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_millis(10))
			.default_headers(headers)
			.build()?;

		// perform an SQL query to check if the model is available
		{
			let res = client
				.post(format!("http://{addr}/sql"))
				.basic_auth(common::USER, Some(common::PASS))
				.body(r#"ml::Prediction<0.0.1>([1.0, 1.0]);"#)
				.send()
				.await?;
			assert!(res.status().is_success(), "body: {}", res.text().await?);
			let body = res.text().await?;

			let deserialized_data: Vec<Data> = serde_json::from_str(&body)?;
			assert_eq!(deserialized_data[0].result[0], 0.9998061656951904);
		}
		Ok(())
	}

	#[test(tokio::test)]
	async fn buffered_compute() -> Result<(), Box<dyn std::error::Error>> {
		let _lock = LockHandle::acquire_lock();
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();

		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		upload_file(&addr, &ns, &db).await?;

		// Prepare HTTP client
		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_millis(10))
			.default_headers(headers)
			.build()?;

		// perform an SQL query to check if the model is available
		{
			let res = client
				.post(format!("http://{addr}/sql"))
				.basic_auth(common::USER, Some(common::PASS))
				.body(r#"ml::Prediction<0.0.1>({squarefoot: 500.0, num_floors: 1.0});"#)
				.send()
				.await?;
			assert!(res.status().is_success(), "body: {}", res.text().await?);
			let body = res.text().await?;
			let deserialized_data: Vec<Data> = serde_json::from_str(&body)?;
			assert_eq!(deserialized_data[0].result[0], 177206.21875);
		}
		Ok(())
	}
}
