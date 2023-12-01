// RUST_LOG=warn cargo make ci-ml-integration
mod common;

#[cfg(feature = "ml")]
mod ml_integration {

	use super::*;
	use http::header;
	use hyper::body::to_bytes;
	use hyper::{Body, Request};
	use hyper::{Client, Uri};
	use serde::{Deserialize, Serialize};
	use std::sync::atomic::{AtomicBool, Ordering};
	use std::time::Duration;
	use surrealml_core::storage::stream_adapter::StreamAdapter;
	use test_log::test;
	use ulid::Ulid;

	static LOCK: AtomicBool = AtomicBool::new(false);

	#[derive(Serialize, Deserialize, Debug)]
	struct Data {
		result: f64,
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

	async fn upload_file(uri: Uri) -> Result<(), Box<dyn std::error::Error>> {
		let client = Client::new();
		let generator = StreamAdapter::new(5, "./tests/linear_test.surml".to_string());
		let body = Body::wrap_stream(generator);
		let req = Request::post(uri).body(body)?;
		let response = client.request(req).await?;
		let body_bytes = to_bytes(response.into_body()).await?;

		// assert the response is correct
		let body_string = String::from_utf8(body_bytes.to_vec())?;
		assert_eq!(body_string, "Prediction-0.0.1");
		Ok(())
	}

	#[test(tokio::test)]
	async fn upload_model() -> Result<(), Box<dyn std::error::Error>> {
		let _lock = LockHandle::acquire_lock();
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let uri: Uri = format!("http://{addr}/ml/import").parse()?;
		upload_file(uri).await?;
		Ok(())
	}

	#[test(tokio::test)]
	async fn raw_compute() -> Result<(), Box<dyn std::error::Error>> {
		let _lock = LockHandle::acquire_lock();
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let uri: Uri = format!("http://{addr}/ml/import").parse()?;
		upload_file(uri).await?;

		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		// Prepare HTTP client
		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("NS", ns.parse()?);
		headers.insert("DB", db.parse()?);
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
				.body(r#"ml::Prediction<0.0.1>(1.0, 1.0);"#)
				.send()
				.await?;
			assert!(res.status().is_success(), "body: {}", res.text().await?);
			let body = res.text().await?;
			let deserialized_data: Vec<Data> = serde_json::from_str(&body)?;
			assert_eq!(deserialized_data[0].result, 0.9998061656951904);
		}
		Ok(())
	}

	#[test(tokio::test)]
	async fn buffered_compute() -> Result<(), Box<dyn std::error::Error>> {
		let _lock = LockHandle::acquire_lock();
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let uri: Uri = format!("http://{addr}/ml/import").parse()?;
		upload_file(uri).await?;

		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		// Prepare HTTP client
		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("NS", ns.parse()?);
		headers.insert("DB", db.parse()?);
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
			assert_eq!(deserialized_data[0].result, 177206.21875);
		}
		Ok(())
	}
}
