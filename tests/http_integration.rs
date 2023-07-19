mod common;

use std::time::Duration;

use http::{header, Method};
use reqwest::Client;
use serde_json::json;
use serial_test::serial;

use crate::common::{PASS, USER};

#[tokio::test]
#[serial]
async fn basic_auth() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let url = &format!("http://{addr}/sql");

	// Prepare HTTP client
	let mut headers = reqwest::header::HeaderMap::new();
	headers.insert("NS", "N".parse()?);
	headers.insert("DB", "D".parse()?);
	headers.insert(header::ACCEPT, "application/json".parse()?);
	let client = reqwest::Client::builder()
		.connect_timeout(Duration::from_millis(10))
		.default_headers(headers)
		.build()?;

	// Request without credentials, gives an anonymous session
	{
		let res = client.post(url).body("CREATE foo").send().await?;
		assert_eq!(res.status(), 200);
		let body = res.text().await?;
		assert!(
			body.contains("You don't have permission to perform this query type"),
			"body: {}",
			body
		);
	}

	// Request with invalid credentials, returns 401
	{
		let res =
			client.post(url).basic_auth("user", Some("pass")).body("CREATE foo").send().await?;
		assert_eq!(res.status(), 401);
	}

	// Request with valid root credentials, gives a ROOT session
	{
		let res = client.post(url).basic_auth(USER, Some(PASS)).body("CREATE foo").send().await?;
		assert_eq!(res.status(), 200);
		let body = res.text().await?;
		assert!(body.contains(r#"[{"result":[{"id":"foo:"#), "body: {}", body);
	}

	Ok(())
}

#[tokio::test]
#[serial]
async fn bearer_auth() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let url = &format!("http://{addr}/sql");

	// Prepare HTTP client
	let mut headers = reqwest::header::HeaderMap::new();
	headers.insert("NS", "N".parse()?);
	headers.insert("DB", "D".parse()?);
	headers.insert(header::ACCEPT, "application/json".parse()?);
	let client = reqwest::Client::builder()
		.connect_timeout(Duration::from_millis(10))
		.default_headers(headers)
		.build()?;

	// Create user
	{
		let res = client
			.post(url)
			.basic_auth(USER, Some(PASS))
			.body(r#"DEFINE LOGIN user ON DB PASSWORD 'pass'"#)
			.send()
			.await?;
		assert!(res.status().is_success(), "body: {}", res.text().await?);
	}

	// Signin with user and get the token
	let token: String;
	{
		let req_body = serde_json::to_string(
			json!({
				"ns": "N",
				"db": "D",
				"user": "user",
				"pass": "pass",
			})
			.as_object()
			.unwrap(),
		)
		.unwrap();

		let res = client.post(format!("http://{addr}/signin")).body(req_body).send().await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);

		let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
		token = body["token"].as_str().unwrap().to_owned();
	}

	// Request with valid token, gives a LOGIN session
	{
		let res = client.post(url).bearer_auth(&token).body("CREATE foo").send().await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);
		let body = res.text().await?;
		assert!(body.contains(r#"[{"result":[{"id":"foo:"#), "body: {}", body);

		// Check the selected namespace and database
		let res = client
			.post(url)
			.header("NS", "N2")
			.header("DB", "D2")
			.bearer_auth(&token)
			.body("SELECT * FROM session::ns(); SELECT * FROM session::db()")
			.send()
			.await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);
		let body = res.text().await?;
		assert!(body.contains(r#""result":["N"]"#), "body: {}", body);
		assert!(body.contains(r#""result":["D"]"#), "body: {}", body);
	}

	// Request with invalid token, returns 401
	{
		let res = client.post(url).bearer_auth("token").body("CREATE foo").send().await?;
		assert_eq!(res.status(), 401, "body: {}", res.text().await?);
	}

	Ok(())
}

#[tokio::test]
#[serial]
async fn client_ip_extractor() -> Result<(), Box<dyn std::error::Error>> {
	// TODO: test the client IP extractor
	Ok(())
}

#[tokio::test]
#[serial]
async fn export_endpoint() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let url = &format!("http://{addr}/export");

	// Prepare HTTP client
	let mut headers = reqwest::header::HeaderMap::new();
	headers.insert("NS", "N".parse()?);
	headers.insert("DB", "D".parse()?);
	headers.insert(header::ACCEPT, "application/json".parse()?);
	let client = reqwest::Client::builder()
		.connect_timeout(Duration::from_millis(10))
		.default_headers(headers)
		.build()?;

	// Create some data
	{
		let res = client
			.post(format!("http://{addr}/sql"))
			.basic_auth(USER, Some(PASS))
			.body("CREATE foo")
			.send()
			.await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);
	}

	// When no auth is provided, the endpoint returns a 403
	{
		let res = client.get(url).send().await?;
		assert_eq!(res.status(), 403, "body: {}", res.text().await?);
	}

	// When auth is provided, it returns the contents of the DB
	{
		let res = client.get(url).basic_auth(USER, Some(PASS)).send().await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);
		let body = res.text().await?;
		assert!(body.contains("DEFINE TABLE foo"), "body: {}", body);
	}

	Ok(())
}

#[tokio::test]
#[serial]
async fn health_endpoint() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let url = &format!("http://{addr}/health");

	let res = Client::default().get(url).send().await?;
	assert_eq!(res.status(), 200, "response: {:#?}", res);

	Ok(())
}

#[tokio::test]
#[serial]
async fn import_endpoint() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let url = &format!("http://{addr}/import");

	// Prepare HTTP client
	let mut headers = reqwest::header::HeaderMap::new();
	headers.insert("NS", "N".parse()?);
	headers.insert("DB", "D".parse()?);
	headers.insert(header::ACCEPT, "application/json".parse()?);
	let client = reqwest::Client::builder()
		.connect_timeout(Duration::from_millis(10))
		.default_headers(headers)
		.build()?;

	// When no auth is provided, the endpoint returns a 403
	{
		let res = client.post(url).body("").send().await?;
		assert_eq!(res.status(), 401, "body: {}", res.text().await?);
	}

	// When auth is provided, it persists the import data
	{
		let data = r#"
			-- --------------------------------
			-- OPTION
			-- ------------------------------

			OPTION IMPORT;

			-- ------------------------------
			-- TABLE: foo
			-- ------------------------------

			DEFINE TABLE foo SCHEMALESS PERMISSIONS NONE;

			-- ------------------------------
			-- TRANSACTION
			-- ------------------------------

			BEGIN TRANSACTION;

			-- ------------------------------
			-- TABLE DATA: foo
			-- ------------------------------

			UPDATE foo:bvklxkhtxumyrfzqoc5i CONTENT { id: foo:bvklxkhtxumyrfzqoc5i };

			-- ------------------------------
			-- TRANSACTION
			-- ------------------------------

			COMMIT TRANSACTION;
		"#;
		let res = client.post(url).basic_auth(USER, Some(PASS)).body(data).send().await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);

		// Check that the data was persisted
		let res = client
			.post(format!("http://{addr}/sql"))
			.basic_auth(USER, Some(PASS))
			.body("SELECT * FROM foo")
			.send()
			.await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);
		let body = res.text().await?;
		assert!(body.contains("foo:bvklxkhtxumyrfzqoc5i"), "body: {}", body);
	}

	Ok(())
}

#[tokio::test]
#[serial]
async fn rpc_endpoint() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let url = &format!("http://{addr}/rpc");

	// Prepare HTTP client
	let mut headers = reqwest::header::HeaderMap::new();
	headers.insert("NS", "N".parse()?);
	headers.insert("DB", "D".parse()?);
	headers.insert(header::ACCEPT, "application/json".parse()?);
	let client = reqwest::Client::builder()
		.connect_timeout(Duration::from_millis(10))
		.default_headers(headers)
		.build()?;

	// Test WebSocket upgrade
	{
		let res = client
			.get(url)
			.header(header::CONNECTION, "Upgrade")
			.header(header::UPGRADE, "websocket")
			.header(header::SEC_WEBSOCKET_VERSION, "13")
			.header(header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
			.send()
			.await?
			.upgrade()
			.await;
		assert!(res.is_ok(), "upgrade err: {}", res.unwrap_err());
	}

	Ok(())
}

#[tokio::test]
#[serial]
async fn signin_endpoint() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let url = &format!("http://{addr}/signin");

	// Prepare HTTP client
	let mut headers = reqwest::header::HeaderMap::new();
	headers.insert("NS", "N".parse()?);
	headers.insert("DB", "D".parse()?);
	headers.insert(header::ACCEPT, "application/json".parse()?);
	let client = reqwest::Client::builder()
		.connect_timeout(Duration::from_millis(10))
		.default_headers(headers)
		.build()?;

	// Create a user
	{
		let res = client
			.post(format!("http://{addr}/sql"))
			.basic_auth(USER, Some(PASS))
			.body(r#"DEFINE LOGIN user ON DB PASSWORD 'pass'"#)
			.send()
			.await?;
		assert!(res.status().is_success(), "body: {}", res.text().await?);
	}

	// Signin with valid credentials and get the token
	{
		let req_body = serde_json::to_string(
			json!({
				"ns": "N",
				"db": "D",
				"user": "user",
				"pass": "pass",
			})
			.as_object()
			.unwrap(),
		)
		.unwrap();

		let res = client.post(url).body(req_body).send().await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);

		let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
		assert!(!body["token"].as_str().unwrap().to_string().is_empty(), "body: {}", body);
	}

	// Signin with invalid credentials returns 403
	{
		let req_body = serde_json::to_string(
			json!({
				"ns": "N",
				"db": "D",
				"user": "user",
				"pass": "invalid_pass",
			})
			.as_object()
			.unwrap(),
		)
		.unwrap();

		let res = client.post(url).body(req_body).send().await?;
		assert_eq!(res.status(), 401, "body: {}", res.text().await?);
	}

	Ok(())
}

#[tokio::test]
#[serial]
async fn signup_endpoint() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let url = &format!("http://{addr}/signup");

	// Prepare HTTP client
	let mut headers = reqwest::header::HeaderMap::new();
	headers.insert("NS", "N".parse()?);
	headers.insert("DB", "D".parse()?);
	headers.insert(header::ACCEPT, "application/json".parse()?);
	let client = reqwest::Client::builder()
		.connect_timeout(Duration::from_millis(10))
		.default_headers(headers)
		.build()?;

	// Create a scope
	{
		let res = client
			.post(format!("http://{addr}/sql"))
			.basic_auth(USER, Some(PASS))
			.body(
				r#"
				DEFINE SCOPE scope SESSION 24h
					SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
					SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
				;
			"#,
			)
			.send()
			.await?;
		assert!(res.status().is_success(), "body: {}", res.text().await?);
	}

	// Signup into the scope
	{
		let req_body = serde_json::to_string(
			json!({
				"ns": "N",
				"db": "D",
				"sc": "scope",
				"email": "email@email.com",
				"pass": "pass",
			})
			.as_object()
			.unwrap(),
		)
		.unwrap();

		let res = client.post(url).body(req_body).send().await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);

		let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
		assert!(!body["token"].as_str().unwrap().to_string().is_empty(), "body: {}", body);
	}

	Ok(())
}

#[tokio::test]
#[serial]
async fn sql_endpoint() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let url = &format!("http://{addr}/sql");

	// Prepare HTTP client
	let mut headers = reqwest::header::HeaderMap::new();
	headers.insert("NS", "N".parse()?);
	headers.insert("DB", "D".parse()?);
	headers.insert(header::ACCEPT, "application/json".parse()?);
	let client = reqwest::Client::builder()
		.connect_timeout(Duration::from_millis(10))
		.default_headers(headers)
		.build()?;

	// Options method works
	{
		let res = client.request(Method::OPTIONS, url).send().await?;
		assert_eq!(res.status(), 200);
	}

	// Creating a record without credentials is not allowed
	{
		let res = client.post(url).body("CREATE foo").send().await?;
		assert_eq!(res.status(), 200);

		let body = res.text().await?;
		assert!(
			body.contains("You don't have permission to perform this query type"),
			"body: {}",
			body
		);
	}

	// Creating a record with Accept JSON encoding is allowed
	{
		let res = client.post(url).basic_auth(USER, Some(PASS)).body("CREATE foo").send().await?;
		assert_eq!(res.status(), 200);

		let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
		assert_eq!(body[0]["status"], "OK", "body: {}", body);
	}

	// Creating a record with Accept CBOR encoding is allowed
	{
		let res = client
			.post(url)
			.basic_auth(USER, Some(PASS))
			.header(header::ACCEPT, "application/cbor")
			.body("CREATE foo")
			.send()
			.await?;
		assert_eq!(res.status(), 200);

		let _: serde_cbor::Value = serde_cbor::from_slice(&res.bytes().await?).unwrap();
	}

	// Creating a record with Accept PACK encoding is allowed
	{
		let res = client
			.post(url)
			.basic_auth(USER, Some(PASS))
			.header(header::ACCEPT, "application/pack")
			.body("CREATE foo")
			.send()
			.await?;
		assert_eq!(res.status(), 200);

		let _: serde_cbor::Value = serde_pack::from_slice(&res.bytes().await?).unwrap();
	}

	// Creating a record with Accept Surrealdb encoding is allowed
	{
		let res = client
			.post(url)
			.basic_auth(USER, Some(PASS))
			.header(header::ACCEPT, "application/surrealdb")
			.body("CREATE foo")
			.send()
			.await?;
		assert_eq!(res.status(), 200);

		// TODO: parse the result
	}

	// Creating a record with an unsupported Accept header, returns a 415
	{
		let res = client
			.post(url)
			.basic_auth(USER, Some(PASS))
			.header(header::ACCEPT, "text/plain")
			.body("CREATE foo")
			.send()
			.await?;
		assert_eq!(res.status(), 415);
	}

	// Test WebSocket upgrade
	{
		let res = client
			.get(url)
			.header(header::CONNECTION, "Upgrade")
			.header(header::UPGRADE, "websocket")
			.header(header::SEC_WEBSOCKET_VERSION, "13")
			.header(header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
			.send()
			.await?
			.upgrade()
			.await;
		assert!(res.is_ok(), "upgrade err: {}", res.unwrap_err());
	}

	Ok(())
}

#[tokio::test]
#[serial]
async fn sync_endpoint() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let url = &format!("http://{addr}/sync");

	// Prepare HTTP client
	let mut headers = reqwest::header::HeaderMap::new();
	headers.insert("NS", "N".parse()?);
	headers.insert("DB", "D".parse()?);
	headers.insert(header::ACCEPT, "application/json".parse()?);
	let client = reqwest::Client::builder()
		.connect_timeout(Duration::from_millis(10))
		.default_headers(headers)
		.build()?;

	// GET
	{
		let res = client.get(url).send().await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);
		let body = res.text().await?;
		assert_eq!(body, r#"Save"#, "body: {}", body);
	}
	// POST
	{
		let res = client.post(url).body("").send().await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);
		let body = res.text().await?;
		assert_eq!(body, r#"Load"#, "body: {}", body);
	}

	Ok(())
}

#[tokio::test]
#[serial]
async fn version_endpoint() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let url = &format!("http://{addr}/version");

	let res = Client::default().get(url).send().await?;
	assert_eq!(res.status(), 200, "response: {:#?}", res);
	let body = res.text().await?;
	assert!(body.starts_with("surrealdb-"), "body: {}", body);

	Ok(())
}

///
/// Key endpoint tests
///

async fn seed_table(
	client: &Client,
	addr: &str,
	table: &str,
	num_records: usize,
) -> Result<(), Box<dyn std::error::Error>> {
	let res = client
		.post(format!("http://{addr}/sql"))
		.basic_auth(USER, Some(PASS))
		.body(format!("CREATE |{table}:1..{num_records}| SET default = 'content'"))
		.send()
		.await?;
	let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();

	assert_eq!(
		body[0]["result"].as_array().unwrap().len(),
		num_records,
		"error seeding the table: {}",
		body
	);

	Ok(())
}

#[tokio::test]
#[serial]
async fn key_endpoint_select_all() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let table_name = "table";
	let num_records = 50;
	let url = &format!("http://{addr}/key/{table_name}");

	// Prepare HTTP client
	let mut headers = reqwest::header::HeaderMap::new();
	headers.insert("NS", "N".parse()?);
	headers.insert("DB", "D".parse()?);
	headers.insert(header::ACCEPT, "application/json".parse()?);
	let client = reqwest::Client::builder()
		.connect_timeout(Duration::from_millis(10))
		.default_headers(headers)
		.build()?;

	// Seed the table
	seed_table(&client, &addr, table_name, num_records).await?;

	// GET all records
	{
		let res = client.get(url).basic_auth(USER, Some(PASS)).send().await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);

		let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
		assert_eq!(body[0]["result"].as_array().unwrap().len(), num_records, "body: {}", body);
	}

	// GET records with a limit
	{
		let res =
			client.get(format!("{}?limit=10", url)).basic_auth(USER, Some(PASS)).send().await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);

		let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
		assert_eq!(body[0]["result"].as_array().unwrap().len(), 10, "body: {}", body);
	}

	// GET records with a start
	{
		let res =
			client.get(format!("{}?start=10", url)).basic_auth(USER, Some(PASS)).send().await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);

		let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
		assert_eq!(body[0]["result"].as_array().unwrap().len(), num_records - 10, "body: {}", body);
		assert_eq!(body[0]["result"].as_array().unwrap()[0]["id"], "table:11", "body: {}", body);
	}

	// GET records with a start and limit
	{
		let res = client
			.get(format!("{}?start=10&limit=10", url))
			.basic_auth(USER, Some(PASS))
			.send()
			.await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);

		let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
		assert_eq!(body[0]["result"].as_array().unwrap().len(), 10, "body: {}", body);
		assert_eq!(body[0]["result"].as_array().unwrap()[0]["id"], "table:11", "body: {}", body);
	}

	// GET without authentication returns no records
	{
		let res = client.get(url).send().await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);

		let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
		assert_eq!(body[0]["result"].as_array().unwrap().len(), 0, "body: {}", body);
	}

	Ok(())
}

#[tokio::test]
#[serial]
async fn key_endpoint_create_all() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();

	// Prepare HTTP client
	let mut headers = reqwest::header::HeaderMap::new();
	headers.insert("NS", "N".parse()?);
	headers.insert("DB", "D".parse()?);
	headers.insert(header::ACCEPT, "application/json".parse()?);
	let client = reqwest::Client::builder()
		.connect_timeout(Duration::from_millis(10))
		.default_headers(headers)
		.build()?;

	// Create record with random ID
	{
		let table_name = "table";
		let url = &format!("http://{addr}/key/{table_name}");

		// Verify there are no records
		let res = client.get(url).basic_auth(USER, Some(PASS)).send().await?;
		let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
		assert_eq!(body[0]["result"].as_array().unwrap().len(), 0, "body: {}", body);

		// Try to create the record
		let res = client
			.post(url)
			.basic_auth(USER, Some(PASS))
			.body(r#"{"name": "record_name"}"#)
			.send()
			.await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);

		// Verify the record was created
		let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
		assert_eq!(body[0]["result"].as_array().unwrap().len(), 1, "body: {}", body);
		assert_eq!(
			body[0]["result"].as_array().unwrap()[0]["name"],
			"record_name",
			"body: {}",
			body
		);
	}

	// POST without authentication creates no records
	{
		let table_name = "table_noauth";
		let url = &format!("http://{addr}/key/{table_name}");

		// Try to create the record
		let res = client.post(url).body(r#"{"name": "record_name"}"#).send().await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);

		// Verify the table is empty
		let res = client.get(url).basic_auth(USER, Some(PASS)).send().await?;
		let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
		assert_eq!(body[0]["result"].as_array().unwrap().len(), 0, "body: {}", body);
	}

	Ok(())
}

#[tokio::test]
#[serial]
async fn key_endpoint_update_all() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let table_name = "table";
	let num_records = 10;
	let url = &format!("http://{addr}/key/{table_name}");

	// Prepare HTTP client
	let mut headers = reqwest::header::HeaderMap::new();
	headers.insert("NS", "N".parse()?);
	headers.insert("DB", "D".parse()?);
	headers.insert(header::ACCEPT, "application/json".parse()?);
	let client = reqwest::Client::builder()
		.connect_timeout(Duration::from_millis(10))
		.default_headers(headers)
		.build()?;

	seed_table(&client, &addr, table_name, num_records).await?;

	// Update all records
	{
		// Try to update the records
		let res = client
			.put(url)
			.basic_auth(USER, Some(PASS))
			.body(r#"{"name": "record_name"}"#)
			.send()
			.await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);

		// Verify the records were updated
		let res = client.get(url).basic_auth(USER, Some(PASS)).send().await?;
		let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
		assert_eq!(body[0]["result"].as_array().unwrap().len(), num_records, "body: {}", body);

		// Verify the records have the new data
		for record in body[0]["result"].as_array().unwrap() {
			assert_eq!(record["name"], "record_name", "body: {}", body);
		}
		// Verify the records don't have the original data
		for record in body[0]["result"].as_array().unwrap() {
			assert!(record["default"].is_null(), "body: {}", body);
		}
	}

	// Update all records without authentication
	{
		// Try to update the records
		let res = client.put(url).body(r#"{"noauth": "yes"}"#).send().await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);

		// Verify the records were not updated
		let res = client.get(url).basic_auth(USER, Some(PASS)).send().await?;
		let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
		assert_eq!(body[0]["result"].as_array().unwrap().len(), num_records, "body: {}", body);

		// Verify the records don't have the new data
		for record in body[0]["result"].as_array().unwrap() {
			assert!(record["noauth"].is_null(), "body: {}", body);
		}
		// Verify the records have the original data
		for record in body[0]["result"].as_array().unwrap() {
			assert_eq!(record["name"], "record_name", "body: {}", body);
		}
	}

	Ok(())
}

#[tokio::test]
#[serial]
async fn key_endpoint_modify_all() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let table_name = "table";
	let num_records = 10;
	let url = &format!("http://{addr}/key/{table_name}");

	// Prepare HTTP client
	let mut headers = reqwest::header::HeaderMap::new();
	headers.insert("NS", "N".parse()?);
	headers.insert("DB", "D".parse()?);
	headers.insert(header::ACCEPT, "application/json".parse()?);
	let client = reqwest::Client::builder()
		.connect_timeout(Duration::from_millis(10))
		.default_headers(headers)
		.build()?;

	seed_table(&client, &addr, table_name, num_records).await?;

	// Modify all records
	{
		// Try to modify the records
		let res = client
			.patch(url)
			.basic_auth(USER, Some(PASS))
			.body(r#"{"name": "record_name"}"#)
			.send()
			.await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);

		// Verify the records were modified
		let res = client.get(url).basic_auth(USER, Some(PASS)).send().await?;
		let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
		assert_eq!(body[0]["result"].as_array().unwrap().len(), num_records, "body: {}", body);

		// Verify the records have the new data
		for record in body[0]["result"].as_array().unwrap() {
			assert_eq!(record["name"], "record_name", "body: {}", body);
		}
		// Verify the records also have the original data
		for record in body[0]["result"].as_array().unwrap() {
			assert_eq!(record["default"], "content", "body: {}", body);
		}
	}

	// Modify all records without authentication
	{
		// Try to modify the records
		let res = client.patch(url).body(r#"{"noauth": "yes"}"#).send().await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);

		// Verify the records were not modified
		let res = client.get(url).basic_auth(USER, Some(PASS)).send().await?;
		let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
		assert_eq!(body[0]["result"].as_array().unwrap().len(), num_records, "body: {}", body);

		// Verify the records don't have the new data
		for record in body[0]["result"].as_array().unwrap() {
			assert!(record["noauth"].is_null(), "body: {}", body);
		}
		// Verify the records have the original data
		for record in body[0]["result"].as_array().unwrap() {
			assert_eq!(record["name"], "record_name", "body: {}", body);
		}
	}

	Ok(())
}

#[tokio::test]
#[serial]
async fn key_endpoint_delete_all() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let table_name = "table";
	let num_records = 10;
	let url = &format!("http://{addr}/key/{table_name}");

	// Prepare HTTP client
	let mut headers = reqwest::header::HeaderMap::new();
	headers.insert("NS", "N".parse()?);
	headers.insert("DB", "D".parse()?);
	headers.insert(header::ACCEPT, "application/json".parse()?);
	let client = reqwest::Client::builder()
		.connect_timeout(Duration::from_millis(10))
		.default_headers(headers)
		.build()?;

	// Delete all records
	{
		seed_table(&client, &addr, table_name, num_records).await?;

		// Verify there are records
		let res = client.get(url).basic_auth(USER, Some(PASS)).send().await?;
		let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
		assert_eq!(body[0]["result"].as_array().unwrap().len(), num_records, "body: {}", body);

		// Try to delete the records
		let res = client.delete(url).basic_auth(USER, Some(PASS)).send().await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);

		// Verify the records were deleted
		let res = client.get(url).basic_auth(USER, Some(PASS)).send().await?;
		let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
		assert_eq!(body[0]["result"].as_array().unwrap().len(), 0, "body: {}", body);
	}

	// Delete all records without authentication
	{
		seed_table(&client, &addr, table_name, num_records).await?;

		// Try to delete the records
		let res = client.delete(url).send().await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);

		// Verify the records were not deleted
		let res = client.get(url).basic_auth(USER, Some(PASS)).send().await?;
		let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
		assert_eq!(body[0]["result"].as_array().unwrap().len(), num_records, "body: {}", body);
	}

	Ok(())
}

#[tokio::test]
#[serial]
async fn key_endpoint_select_one() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let table_name = "table";
	let url = &format!("http://{addr}/key/{table_name}/1");

	// Prepare HTTP client
	let mut headers = reqwest::header::HeaderMap::new();
	headers.insert("NS", "N".parse()?);
	headers.insert("DB", "D".parse()?);
	headers.insert(header::ACCEPT, "application/json".parse()?);
	let client = reqwest::Client::builder()
		.connect_timeout(Duration::from_millis(10))
		.default_headers(headers)
		.build()?;

	// Seed the table
	seed_table(&client, &addr, table_name, 1).await?;

	// GET one record
	{
		let res = client.get(url).basic_auth(USER, Some(PASS)).send().await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);

		let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
		assert_eq!(body[0]["result"].as_array().unwrap().len(), 1, "body: {}", body);
	}

	// GET without authentication returns no record
	{
		let res = client.get(url).send().await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);

		let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
		assert_eq!(body[0]["result"].as_array().unwrap().len(), 0, "body: {}", body);
	}

	Ok(())
}

#[tokio::test]
#[serial]
async fn key_endpoint_create_one() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let table_name = "table";

	// Prepare HTTP client
	let mut headers = reqwest::header::HeaderMap::new();
	headers.insert("NS", "N".parse()?);
	headers.insert("DB", "D".parse()?);
	headers.insert(header::ACCEPT, "application/json".parse()?);
	let client = reqwest::Client::builder()
		.connect_timeout(Duration::from_millis(10))
		.default_headers(headers)
		.build()?;

	// Create record with known ID
	{
		let url = &format!("http://{addr}/key/{table_name}/new_id");

		// Try to create the record
		let res = client
			.post(url)
			.basic_auth(USER, Some(PASS))
			.body(r#"{"name": "record_name"}"#)
			.send()
			.await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);

		// Verify the record was created with the given ID
		let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
		assert_eq!(body[0]["result"].as_array().unwrap().len(), 1, "body: {}", body);
		assert_eq!(
			body[0]["result"].as_array().unwrap()[0]["id"],
			"table:new_id",
			"body: {}",
			body
		);
	}

	// Create record with known ID and query params
	{
		let url = &format!(
			"http://{addr}/key/{table_name}/new_id_query?{params}",
			params = "age=45&elems=[1,2,3]&other={test: true}"
		);

		// Try to create the record
		let res = client
			.post(url)
			.basic_auth(USER, Some(PASS))
			.body(r#"{ age: $age, elems: $elems, other: $other }"#)
			.send()
			.await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);

		// Verify the record was created with the given ID
		let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
		assert_eq!(body[0]["result"].as_array().unwrap().len(), 1, "body: {}", body);
		assert_eq!(
			body[0]["result"].as_array().unwrap()[0]["id"],
			"table:new_id_query",
			"body: {}",
			body
		);
		assert_eq!(body[0]["result"].as_array().unwrap()[0]["age"], 45, "body: {}", body);
		assert_eq!(
			body[0]["result"].as_array().unwrap()[0]["elems"].as_array().unwrap().len(),
			3,
			"body: {}",
			body
		);
		assert_eq!(
			body[0]["result"].as_array().unwrap()[0]["other"].as_object().unwrap()["test"],
			true,
			"body: {}",
			body
		);
	}

	// POST without authentication creates no records
	{
		let url = &format!("http://{addr}/key/{table_name}/noauth_id");

		// Try to create the record
		let res = client.post(url).body(r#"{"name": "record_name"}"#).send().await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);

		// Verify the table is empty
		let res = client.get(url).basic_auth(USER, Some(PASS)).send().await?;
		let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
		assert_eq!(body[0]["result"].as_array().unwrap().len(), 0, "body: {}", body);
	}

	Ok(())
}

#[tokio::test]
#[serial]
async fn key_endpoint_update_one() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let table_name = "table";
	let url = &format!("http://{addr}/key/{table_name}/1");

	// Prepare HTTP client
	let mut headers = reqwest::header::HeaderMap::new();
	headers.insert("NS", "N".parse()?);
	headers.insert("DB", "D".parse()?);
	headers.insert(header::ACCEPT, "application/json".parse()?);
	let client = reqwest::Client::builder()
		.connect_timeout(Duration::from_millis(10))
		.default_headers(headers)
		.build()?;

	seed_table(&client, &addr, table_name, 1).await?;

	// Update one record
	{
		// Try to update the record
		let res = client
			.put(url)
			.basic_auth(USER, Some(PASS))
			.body(r#"{"name": "record_name"}"#)
			.send()
			.await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);

		// Verify the record was updated
		let res = client.get(url).basic_auth(USER, Some(PASS)).send().await?;
		let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
		assert_eq!(body[0]["result"].as_array().unwrap()[0]["id"], "table:1", "body: {}", body);

		// Verify the record has the new data
		assert_eq!(
			body[0]["result"].as_array().unwrap()[0]["name"],
			"record_name",
			"body: {}",
			body
		);

		// Verify the record doesn't have the original data
		assert!(body[0]["result"].as_array().unwrap()[0]["default"].is_null(), "body: {}", body);
	}

	// Update one record without authentication
	{
		// Try to update the record
		let res = client.put(url).body(r#"{"noauth": "yes"}"#).send().await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);

		// Verify the record was not updated
		let res = client.get(url).basic_auth(USER, Some(PASS)).send().await?;
		let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
		assert_eq!(body[0]["result"].as_array().unwrap()[0]["id"], "table:1", "body: {}", body);

		// Verify the record doesn't have the new data
		assert!(body[0]["result"].as_array().unwrap()[0]["noauth"].is_null(), "body: {}", body);

		// Verify the record has the original data
		assert_eq!(
			body[0]["result"].as_array().unwrap()[0]["name"],
			"record_name",
			"body: {}",
			body
		);
	}

	Ok(())
}

#[tokio::test]
#[serial]
async fn key_endpoint_modify_one() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let table_name = "table";
	let url = &format!("http://{addr}/key/{table_name}/1");

	// Prepare HTTP client
	let mut headers = reqwest::header::HeaderMap::new();
	headers.insert("NS", "N".parse()?);
	headers.insert("DB", "D".parse()?);
	headers.insert(header::ACCEPT, "application/json".parse()?);
	let client = reqwest::Client::builder()
		.connect_timeout(Duration::from_millis(10))
		.default_headers(headers)
		.build()?;

	seed_table(&client, &addr, table_name, 1).await?;

	// Modify one record
	{
		// Try to modify one record
		let res = client
			.patch(url)
			.basic_auth(USER, Some(PASS))
			.body(r#"{"name": "record_name"}"#)
			.send()
			.await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);

		// Verify the records were modified
		let res = client.get(url).basic_auth(USER, Some(PASS)).send().await?;
		let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
		assert_eq!(body[0]["result"].as_array().unwrap()[0]["id"], "table:1", "body: {}", body);

		// Verify the record has the new data
		assert_eq!(
			body[0]["result"].as_array().unwrap()[0]["name"],
			"record_name",
			"body: {}",
			body
		);

		// Verify the record has the original data too
		assert_eq!(
			body[0]["result"].as_array().unwrap()[0]["default"],
			"content",
			"body: {}",
			body
		);
	}

	// Modify one record without authentication
	{
		// Try to modify the record
		let res = client.patch(url).body(r#"{"noauth": "yes"}"#).send().await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);

		// Verify the record was not modified
		let res = client.get(url).basic_auth(USER, Some(PASS)).send().await?;
		let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
		assert_eq!(body[0]["result"].as_array().unwrap()[0]["id"], "table:1", "body: {}", body);

		// Verify the record doesn't have the new data
		assert!(body[0]["result"].as_array().unwrap()[0]["noauth"].is_null(), "body: {}", body);

		// Verify the record has the original data too
		assert_eq!(
			body[0]["result"].as_array().unwrap()[0]["default"],
			"content",
			"body: {}",
			body
		);
	}

	Ok(())
}

#[tokio::test]
#[serial]
async fn key_endpoint_delete_one() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let table_name = "table";
	let base_url = &format!("http://{addr}/key/{table_name}");

	// Prepare HTTP client
	let mut headers = reqwest::header::HeaderMap::new();
	headers.insert("NS", "N".parse()?);
	headers.insert("DB", "D".parse()?);
	headers.insert(header::ACCEPT, "application/json".parse()?);
	let client = reqwest::Client::builder()
		.connect_timeout(Duration::from_millis(10))
		.default_headers(headers)
		.build()?;

	// Delete all records
	{
		seed_table(&client, &addr, table_name, 2).await?;

		// Verify there are records
		let res = client.get(base_url).basic_auth(USER, Some(PASS)).send().await?;
		let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
		assert_eq!(body[0]["result"].as_array().unwrap().len(), 2, "body: {}", body);

		// Try to delete the record
		let res =
			client.delete(format!("{}/1", base_url)).basic_auth(USER, Some(PASS)).send().await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);

		// Verify only one record was deleted
		let res = client.get(base_url).basic_auth(USER, Some(PASS)).send().await?;
		let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
		assert_eq!(body[0]["result"].as_array().unwrap().len(), 1, "body: {}", body);
		assert_eq!(body[0]["result"].as_array().unwrap()[0]["id"], "table:2", "body: {}", body);
	}

	// Delete one record without authentication
	{
		// Try to delete the record
		let res = client.delete(format!("{}/2", base_url)).send().await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);

		// Verify the record was not deleted
		let res = client.get(base_url).basic_auth(USER, Some(PASS)).send().await?;
		let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
		assert_eq!(body[0]["result"].as_array().unwrap().len(), 1, "body: {}", body);
	}

	Ok(())
}
