// RUST_LOG=warn cargo make ci-http-integration
mod common;

mod http_integration {
	use std::time::Duration;

	use http::header::HeaderValue;
	use http::{Method, header};
	use reqwest::Client;
	use serde_json::json;
	use surrealdb::headers::{AUTH_DB, AUTH_NS};
	use test_log::test;
	use ulid::Ulid;

	use super::common::{self, PASS, StartServerArguments, USER};

	#[test(tokio::test)]
	async fn basic_auth() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_guests().await.unwrap();
		let url = &format!("http://{addr}/sql");

		// Prepare HTTP client
		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
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
			assert!(body.contains("Not enough permissions"), "body: {body}");
		}

		// Request with invalid credentials, returns 401
		{
			let res =
				client.post(url).basic_auth("user", Some("pass")).body("CREATE foo").send().await?;
			assert_eq!(res.status(), 401);
		}

		// Request with valid root credentials, gives a ROOT session
		{
			let res =
				client.post(url).basic_auth(USER, Some(PASS)).body("CREATE foo").send().await?;
			assert_eq!(res.status(), 200);
			let body = res.text().await?;
			assert!(body.contains(r#"[{"result":[{"id":"foo:"#), "body: {body}");
		}

		// Prepare users with identical credentials on ROOT, NAMESPACE and DATABASE
		// levels
		{
			let res =
				client.post(url).basic_auth(USER, Some(PASS))
                                .body(format!("DEFINE USER {USER} ON ROOT PASSWORD '{PASS}' ROLES OWNER;
                                                DEFINE USER {USER} ON NAMESPACE PASSWORD '{PASS}' ROLES OWNER;
                                                DEFINE USER {USER} ON DATABASE PASSWORD '{PASS}' ROLES OWNER",
                                )).send().await?;
			assert_eq!(res.status(), 200);
		}

		// Request with ROOT level access to access ROOT, returns 200 and succeeds
		{
			let res =
				client.post(url).basic_auth(USER, Some(PASS)).body("INFO FOR ROOT").send().await?;
			assert_eq!(res.status(), 200);
			let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
			assert_eq!(body[0]["status"], "OK", "body: {body}");
		}

		// Request with ROOT level access to access NS, returns 200 and succeeds
		{
			let res =
				client.post(url).basic_auth(USER, Some(PASS)).body("INFO FOR NS").send().await?;
			assert_eq!(res.status(), 200);
			let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
			assert_eq!(body[0]["status"], "OK", "body: {body}");
		}

		// Request with ROOT level access to access DB, returns 200 and succeeds
		{
			let res =
				client.post(url).basic_auth(USER, Some(PASS)).body("INFO FOR DB").send().await?;
			assert_eq!(res.status(), 200);
			let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
			assert_eq!(body[0]["status"], "OK", "body: {body}");
		}

		// Request with NS level access to access ROOT, returns 200 but fails
		{
			let res = client
				.post(url)
				.header(&AUTH_NS, &ns)
				.basic_auth(USER, Some(PASS))
				.body("INFO FOR ROOT")
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
			assert_eq!(body[0]["status"], "ERR", "body: {body}");
			assert_eq!(
				body[0]["result"], "IAM error: Not enough permissions to perform this action",
				"body: {body}"
			);
		}

		// Request with NS level access to access NS, returns 200 and succeeds
		{
			let res = client
				.post(url)
				.header(&AUTH_NS, &ns)
				.basic_auth(USER, Some(PASS))
				.body("INFO FOR NS")
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
			assert_eq!(body[0]["status"], "OK", "body: {body}");
		}

		// Request with NS level access to access DB, returns 200 and succeeds
		{
			let res = client
				.post(url)
				.header(&AUTH_NS, &ns)
				.basic_auth(USER, Some(PASS))
				.body("INFO FOR DB")
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
			assert_eq!(body[0]["status"], "OK", "body: {body}");
		}

		// Request with DB level access to access ROOT, returns 200 but fails
		{
			let res = client
				.post(url)
				.header(&AUTH_NS, &ns)
				.header(&AUTH_DB, &db)
				.basic_auth(USER, Some(PASS))
				.body("INFO FOR ROOT")
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
			assert_eq!(body[0]["status"], "ERR", "body: {body}");
			assert_eq!(
				body[0]["result"], "IAM error: Not enough permissions to perform this action",
				"body: {body}"
			);
		}

		// Request with DB level access to access NS, returns 200 but fails
		{
			let res = client
				.post(url)
				.header(&AUTH_NS, &ns)
				.header(&AUTH_DB, &db)
				.basic_auth(USER, Some(PASS))
				.body("INFO FOR NS")
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
			assert_eq!(body[0]["status"], "ERR", "body: {body}");
			assert_eq!(
				body[0]["result"], "IAM error: Not enough permissions to perform this action",
				"body: {body}"
			);
		}

		// Request with DB level access to access DB, returns 200 and succeeds
		{
			let res = client
				.post(url)
				.header(&AUTH_NS, &ns)
				.header(&AUTH_DB, &db)
				.basic_auth(USER, Some(PASS))
				.body("INFO FOR DB")
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
			assert_eq!(body[0]["status"], "OK", "body: {body}");
		}

		// Request with DB level access missing NS level header, returns 401
		{
			let res = client
				.post(url)
				.header(&AUTH_DB, &db)
				.basic_auth(USER, Some(PASS))
				.body("INFO FOR DB")
				.send()
				.await?;
			assert_eq!(res.status(), 401);
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn bearer_auth() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_guests().await.unwrap();
		let url = &format!("http://{addr}/sql");

		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		// Prepare HTTP client
		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
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
				.body(r#"DEFINE USER user ON DB PASSWORD 'pass' ROLES OWNER"#)
				.send()
				.await?;
			let body = res.text().await?;
			assert!(body.contains(r#""status":"OK"#), "body: {body}");
		}

		// Signin with user and get the token
		let token: String;
		{
			let req_body = serde_json::to_string(
				json!({
					"ns": ns,
					"db": db,
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

		// Request with valid token, gives a USER session
		{
			let res = client.post(url).bearer_auth(&token).body("CREATE foo").send().await?;
			assert_eq!(res.status(), 200, "body: {}", res.text().await?);
			let body = res.text().await?;
			assert!(body.contains(r#"[{"result":[{"id":"foo:"#), "body: {body}");

			// Check the selected namespace and database
			let res = client
				.post(url)
				.header("NS", Ulid::new().to_string())
				.header("DB", Ulid::new().to_string())
				.bearer_auth(&token)
				.body("SELECT * FROM session::ns(); SELECT * FROM session::db()")
				.send()
				.await?;
			assert_eq!(res.status(), 200, "body: {}", res.text().await?);
			let body = res.text().await?;
			assert!(body.contains(&format!(r#""result":["{ns}"]"#)), "body: {body}");
			assert!(body.contains(&format!(r#""result":["{db}"]"#)), "body: {body}");
		}

		// Request with invalid token, returns 401
		{
			let res = client.post(url).bearer_auth("token").body("CREATE foo").send().await?;
			assert_eq!(res.status(), 401, "body: {}", res.text().await?);
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn client_ip_extractor() -> Result<(), Box<dyn std::error::Error>> {
		// TODO: test the client IP extractor
		Ok(())
	}

	#[test(tokio::test)]
	async fn session_id() {
		let (addr, _server) = common::start_server_with_guests().await.unwrap();
		let url = &format!("http://{addr}/sql");

		// Request without header, gives a randomly generated session identifier
		{
			// Prepare HTTP client without header
			let mut headers = reqwest::header::HeaderMap::new();
			let ns = Ulid::new().to_string();
			let db = Ulid::new().to_string();
			headers.insert("surreal-ns", ns.parse().unwrap());
			headers.insert("surreal-db", db.parse().unwrap());
			headers.insert(header::ACCEPT, "application/json".parse().unwrap());
			let client = reqwest::Client::builder()
				.connect_timeout(Duration::from_millis(10))
				.default_headers(headers)
				.build()
				.unwrap();

			let res = client.post(url).body("SELECT VALUE id FROM $session").send().await.unwrap();
			assert_eq!(res.status(), 200);
			let body = res.text().await.unwrap();
			// Any randomly generated UUIDv4 will be in the format:
			// xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
			assert!(body.contains("-4"), "body: {body}");
		}

		// Request with header, gives a the session identifier specified in the header
		{
			// Prepare HTTP client with header
			let mut headers = reqwest::header::HeaderMap::new();
			let ns = Ulid::new().to_string();
			let db = Ulid::new().to_string();
			headers.insert("surreal-ns", ns.parse().unwrap());
			headers.insert("surreal-db", db.parse().unwrap());
			headers.insert(
				"surreal-id",
				HeaderValue::from_static("00000000-0000-0000-0000-000000000000"),
			);
			headers.insert(header::ACCEPT, "application/json".parse().unwrap());
			let client = reqwest::Client::builder()
				.connect_timeout(Duration::from_millis(10))
				.default_headers(headers)
				.build()
				.unwrap();

			let res = client.post(url).body("SELECT VALUE id FROM $session").send().await.unwrap();
			assert_eq!(res.status(), 200);
			let body = res.text().await.unwrap();
			assert!(body.contains("00000000-0000-0000-0000-000000000000"), "body: {body}");
		}

		// Request with invalid header, should fail
		{
			// Prepare HTTP client with header
			let mut headers = reqwest::header::HeaderMap::new();
			let ns = Ulid::new().to_string();
			let db = Ulid::new().to_string();
			headers.insert("surreal-ns", ns.parse().unwrap());
			headers.insert("surreal-db", db.parse().unwrap());
			headers.insert(
				"surreal-id",
				HeaderValue::from_static("123"), // Not a valid UUIDv4
			);
			headers.insert(header::ACCEPT, "application/json".parse().unwrap());
			let client = reqwest::Client::builder()
				.connect_timeout(Duration::from_millis(10))
				.default_headers(headers)
				.build()
				.unwrap();

			let res = client.post(url).body("SELECT VALUE id FROM $session").send().await.unwrap();
			assert_eq!(res.status(), 401);
		}
	}

	#[test(tokio::test)]
	async fn export_endpoint() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let url = &format!("http://{addr}/export");

		// Prepare HTTP client
		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("surreal-ns", Ulid::new().to_string().parse()?);
		headers.insert("surreal-db", Ulid::new().to_string().parse()?);
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
			assert!(body.contains("DEFINE TABLE foo"), "body: {body}");
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn health_endpoint() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let url = &format!("http://{addr}/health");

		let res = Client::default().get(url).send().await?;
		assert_eq!(res.status(), 200, "response: {res:#?}");

		Ok(())
	}

	#[test(tokio::test)]
	async fn no_server_id_headers() -> Result<(), Box<dyn std::error::Error>> {
		// default server has the id headers
		{
			let (addr, _server) = common::start_server_with_defaults().await.unwrap();
			let url = &format!("http://{addr}/health");

			let res = Client::default().get(url).send().await?;
			assert!(res.headers().contains_key("server"));
			assert!(res.headers().contains_key("surreal-version"));
		}

		// turn on the no-identification-headers option to suppress headers
		{
			let mut start_server_arguments = StartServerArguments::default();
			start_server_arguments.args.push_str(" --no-identification-headers");
			let (addr, _server) = common::start_server(start_server_arguments).await.unwrap();
			let url = &format!("http://{addr}/health");

			let res = Client::default().get(url).send().await?;
			assert!(!res.headers().contains_key("server"));
			assert!(!res.headers().contains_key("surreal-version"));
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn import_endpoint() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let url = &format!("http://{addr}/import");

		// Prepare HTTP client
		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("surreal-ns", Ulid::new().to_string().parse()?);
		headers.insert("surreal-db", Ulid::new().to_string().parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_millis(10))
			.default_headers(headers)
			.build()?;

		// When no auth is provided, the endpoint returns a 403
		{
			let res = client.post(url).body("").send().await?;
			assert_eq!(res.status(), 403, "body: {}", res.text().await?);
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

				INSERT { id: foo:bvklxkhtxumyrfzqoc5i };

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
			assert!(body.contains("foo:bvklxkhtxumyrfzqoc5i"), "body: {body}");
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn rpc_endpoint() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let url = &format!("http://{addr}/rpc");

		// Prepare HTTP client
		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("surreal-ns", Ulid::new().to_string().parse()?);
		headers.insert("surreal-db", Ulid::new().to_string().parse()?);
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

	#[test(tokio::test)]
	async fn signin_endpoint() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let url = &format!("http://{addr}/signin");

		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		// Prepare HTTP client
		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_millis(10))
			.default_headers(headers)
			.build()?;

		// Create a DB user
		{
			let res = client
				.post(format!("http://{addr}/sql"))
				.basic_auth(USER, Some(PASS))
				.body(r#"DEFINE USER user_db ON DB PASSWORD 'pass_db'"#)
				.send()
				.await?;
			assert!(res.status().is_success(), "body: {}", res.text().await?);
		}

		// Signin with valid DB credentials and get the token
		{
			let req_body = serde_json::to_string(
				json!({
					"ns": ns,
					"db": db,
					"user": "user_db",
					"pass": "pass_db",
				})
				.as_object()
				.unwrap(),
			)
			.unwrap();

			let res = client.post(url).body(req_body).send().await?;
			assert_eq!(res.status(), 200, "body: {}", res.text().await?);

			let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
			assert!(!body["token"].as_str().unwrap().to_string().is_empty(), "body: {body}");
		}

		// Signin with invalid DB credentials returns 401
		{
			let req_body = serde_json::to_string(
				json!({
					"ns": ns,
					"db": db,
					"user": "user_db",
					"pass": "invalid_pass",
				})
				.as_object()
				.unwrap(),
			)
			.unwrap();

			let res = client.post(url).body(req_body).send().await?;
			assert_eq!(res.status(), 401, "body: {}", res.text().await?);
		}

		// Create a NS user
		{
			let res = client
				.post(format!("http://{addr}/sql"))
				.basic_auth(USER, Some(PASS))
				.body(r#"DEFINE USER user_ns ON NS PASSWORD 'pass_ns'"#)
				.send()
				.await?;
			assert!(res.status().is_success(), "body: {}", res.text().await?);
		}

		// Signin with valid NS credentials specifying NS and DB and get the token
		// This should fail because authentication will be attempted at DB level
		{
			let req_body = serde_json::to_string(
				json!({
					"ns": ns,
					"db": db,
					"user": "user_ns",
					"pass": "pass_ns",
				})
				.as_object()
				.unwrap(),
			)
			.unwrap();

			let res = client.post(url).body(req_body).send().await?;
			assert_eq!(res.status(), 401, "body: {}", res.text().await?);
		}

		// Signin with valid NS credentials specifying NS and get the token
		{
			let req_body = serde_json::to_string(
				json!({
					"ns": ns,
					"user": "user_ns",
					"pass": "pass_ns",
				})
				.as_object()
				.unwrap(),
			)
			.unwrap();

			let res = client.post(url).body(req_body).send().await?;
			assert_eq!(res.status(), 200, "body: {}", res.text().await?);

			let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
			assert!(!body["token"].as_str().unwrap().to_string().is_empty(), "body: {body}");
		}

		// Signin with invalid NS credentials returns 401
		{
			let req_body = serde_json::to_string(
				json!({
					"ns": ns,
					"db": db,
					"user": "user_ns",
					"pass": "invalid_pass",
				})
				.as_object()
				.unwrap(),
			)
			.unwrap();

			let res = client.post(url).body(req_body).send().await?;
			assert_eq!(res.status(), 401, "body: {}", res.text().await?);
		}

		// Create a ROOT user
		{
			let res = client
				.post(format!("http://{addr}/sql"))
				.basic_auth(USER, Some(PASS))
				.body(r#"DEFINE USER user_root ON ROOT PASSWORD 'pass_root'"#)
				.send()
				.await?;
			assert!(res.status().is_success(), "body: {}", res.text().await?);
		}

		// Signin with valid ROOT credentials specifying NS and DB and get the token
		// This should fail because authentication will be attempted at DB level
		{
			let req_body = serde_json::to_string(
				json!({
					"ns": ns,
					"db": db,
					"user": "user_root",
					"pass": "pass_root",
				})
				.as_object()
				.unwrap(),
			)
			.unwrap();

			let res = client.post(url).body(req_body).send().await?;
			assert_eq!(res.status(), 401, "body: {}", res.text().await?);
		}

		// Signin with valid ROOT credentials specifying NS and get the token
		// This should fail because authentication will be attempted at NS level
		{
			let req_body = serde_json::to_string(
				json!({
					"ns": ns,
					"user": "user_root",
					"pass": "pass_root",
				})
				.as_object()
				.unwrap(),
			)
			.unwrap();

			let res = client.post(url).body(req_body).send().await?;
			assert_eq!(res.status(), 401, "body: {}", res.text().await?);
		}

		// Signin with valid ROOT credentials without specifying NS nor DB and get the
		// token
		{
			let req_body = serde_json::to_string(
				json!({
					"user": "user_root",
					"pass": "pass_root",
				})
				.as_object()
				.unwrap(),
			)
			.unwrap();

			let res = client.post(url).body(req_body).send().await?;
			assert_eq!(res.status(), 200, "body: {}", res.text().await?);

			let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
			assert!(!body["token"].as_str().unwrap().to_string().is_empty(), "body: {body}");
		}

		// Signin with invalid ROOT credentials returns 401
		{
			let req_body = serde_json::to_string(
				json!({
					"ns": ns,
					"db": db,
					"user": "user_root",
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

	#[test(tokio::test)]
	async fn signup_endpoint() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let url = &format!("http://{addr}/signup");

		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		// Prepare HTTP client
		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_millis(10))
			.default_headers(headers)
			.build()?;

		// Define a record access method
		{
			let res = client
				.post(format!("http://{addr}/sql"))
				.basic_auth(USER, Some(PASS))
				.body(
					r#"
					DEFINE ACCESS user ON DATABASE TYPE RECORD
						SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
						SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
						DURATION FOR SESSION 12h
					;
				"#,
				)
				.send()
				.await?;
			assert!(res.status().is_success(), "body: {}", res.text().await?);
		}

		// Signup using the defined record access method
		{
			let req_body = serde_json::to_string(
				json!({
					"ns": ns,
					"db": db,
					"ac": "user",
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
			assert!(
				body["token"].as_str().unwrap().starts_with("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9"),
				"body: {body}"
			);
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn sql_endpoint() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_guests().await.unwrap();
		let url = &format!("http://{addr}/sql");

		// Prepare HTTP client
		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("surreal-ns", Ulid::new().to_string().parse()?);
		headers.insert("surreal-db", Ulid::new().to_string().parse()?);
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
			assert!(body.contains("Not enough permissions"), "body: {body}");
		}

		// Creating a record with Accept JSON encoding is allowed
		{
			let res =
				client.post(url).basic_auth(USER, Some(PASS)).body("CREATE foo").send().await?;
			assert_eq!(res.status(), 200);

			let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
			assert_eq!(body[0]["status"], "OK", "body: {body}");
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
			let res = res.bytes().await?.to_vec();
			let _: ciborium::Value = ciborium::from_reader(res.as_slice()).unwrap();
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

		// Test nul character
		{
			let res =
				client.post(url).body("parse::email::user('\\u0000@example.com')").send().await?;
			assert_eq!(res.status(), 400);

			let body = res.text().await?;
			assert!(body.contains("Null bytes are not allowed"), "body: {body}");
		}

		Ok(())
	}

	#[test(tokio::test)]
	#[cfg(feature = "http-compression")]
	async fn sql_endpoint_with_compression() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let url = &format!("http://{addr}/sql");

		// Prepare HTTP client
		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("surreal-ns", Ulid::new().to_string().parse()?);
		headers.insert("surreal-db", Ulid::new().to_string().parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		headers.insert(header::ACCEPT_ENCODING, "gzip".parse()?);

		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_millis(10))
			.gzip(false) // So that the content-encoding header is not removed by Reqwest
			.default_headers(headers.clone())
			.build()?;

		// Check that the content is gzip encoded
		{
			let res = client
				.post(url)
				.basic_auth(USER, Some(PASS))
				.body("CREATE |foo:100|")
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			assert_eq!(res.headers()["content-encoding"], "gzip");
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn sync_endpoint() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let url = &format!("http://{addr}/sync");

		// Prepare HTTP client
		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("surreal-ns", Ulid::new().to_string().parse()?);
		headers.insert("surreal-db", Ulid::new().to_string().parse()?);
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
			assert_eq!(body, r#"Save"#, "body: {body}");
		}
		// POST
		{
			let res = client.post(url).body("").send().await?;
			assert_eq!(res.status(), 200, "body: {}", res.text().await?);
			let body = res.text().await?;
			assert_eq!(body, r#"Load"#, "body: {body}");
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn version_endpoint() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let url = &format!("http://{addr}/version");

		let res = Client::default().get(url).send().await?;
		assert_eq!(res.status(), 200, "response: {res:#?}");
		let body = res.text().await?;
		assert!(body.starts_with("surrealdb-"), "body: {body}");

		Ok(())
	}

	//
	// Key endpoint tests
	//

	async fn seed_table(
		client: &Client,
		addr: &str,
		table: &str,
		num_records: usize,
	) -> Result<(), Box<dyn std::error::Error>> {
		let end = num_records + 1;
		let res = client
			.post(format!("http://{addr}/sql"))
			.basic_auth(USER, Some(PASS))
			.body(format!("CREATE |`{table}`:1..{end}| SET default = 'content'"))
			.send()
			.await?;

		let text = res.text().await?;
		println!("{text}");
		let body: serde_json::Value = serde_json::from_str(&text).unwrap();

		assert_eq!(
			body[0]["result"].as_array().unwrap().len(),
			num_records,
			"error seeding the table: {body}"
		);

		Ok(())
	}

	#[test(tokio::test)]
	async fn key_endpoint_select_all() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_guests().await.unwrap();
		let table_name = "table";
		let num_records = 50;
		let url = &format!("http://{addr}/key/{table_name}");

		// Prepare HTTP client
		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("surreal-ns", Ulid::new().to_string().parse()?);
		headers.insert("surreal-db", Ulid::new().to_string().parse()?);
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
			assert_eq!(body[0]["result"].as_array().unwrap().len(), num_records, "body: {body}");
		}

		// GET records with a limit
		{
			let res =
				client.get(format!("{url}?limit=10")).basic_auth(USER, Some(PASS)).send().await?;
			assert_eq!(res.status(), 200, "body: {}", res.text().await?);

			let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
			assert_eq!(body[0]["result"].as_array().unwrap().len(), 10, "body: {body}");
		}

		// GET records with a start
		{
			let res =
				client.get(format!("{url}?start=10")).basic_auth(USER, Some(PASS)).send().await?;
			assert_eq!(res.status(), 200, "body: {}", res.text().await?);

			let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
			assert_eq!(
				body[0]["result"].as_array().unwrap().len(),
				num_records - 10,
				"body: {body}"
			);
			assert_eq!(body[0]["result"].as_array().unwrap()[0]["id"], "table:11", "body: {body}");
		}

		// GET records with a start and limit
		{
			let res = client
				.get(format!("{url}?start=10&limit=10"))
				.basic_auth(USER, Some(PASS))
				.send()
				.await?;
			assert_eq!(res.status(), 200, "body: {}", res.text().await?);

			let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
			assert_eq!(body[0]["result"].as_array().unwrap().len(), 10, "body: {body}");
			assert_eq!(body[0]["result"].as_array().unwrap()[0]["id"], "table:11", "body: {body}");
		}

		// GET without authentication returns no records
		{
			let res = client.get(url).send().await?;
			assert_eq!(res.status(), 200, "body: {}", res.text().await?);

			let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
			assert_eq!(body[0]["result"].as_array().unwrap().len(), 0, "body: {body}");
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn key_endpoint_create_all() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_guests().await.unwrap();

		// Prepare HTTP client
		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("surreal-ns", Ulid::new().to_string().parse()?);
		headers.insert("surreal-db", Ulid::new().to_string().parse()?);
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
			assert_eq!(body["information"], "Table `table` not found", "body: {body}");

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
			assert_eq!(body[0]["result"].as_array().unwrap().len(), 1, "body: {body}");
			assert_eq!(
				body[0]["result"].as_array().unwrap()[0]["name"],
				"record_name",
				"body: {body}"
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
			assert_eq!(body["information"], "Table `table_noauth` not found", "body: {body}");
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn key_endpoint_update_all() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_guests().await.unwrap();
		let table_name = "table";
		let num_records = 10;
		let url = &format!("http://{addr}/key/{table_name}");

		// Prepare HTTP client
		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("surreal-ns", Ulid::new().to_string().parse()?);
		headers.insert("surreal-db", Ulid::new().to_string().parse()?);
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
			let status = res.status();
			let body = res.text().await?;
			println!("{}", body);
			assert_eq!(status, 200);

			// Verify the records were updated
			let res = client.get(url).basic_auth(USER, Some(PASS)).send().await?;
			let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
			assert_eq!(body[0]["result"].as_array().unwrap().len(), num_records, "body: {body}");

			// Verify the records have the new data
			for record in body[0]["result"].as_array().unwrap() {
				assert_eq!(record["name"], "record_name", "body: {body}");
			}
			// Verify the records don't have the original data
			for record in body[0]["result"].as_array().unwrap() {
				assert!(record["default"].is_null(), "body: {body}");
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
			assert_eq!(body[0]["result"].as_array().unwrap().len(), num_records, "body: {body}");

			// Verify the records don't have the new data
			for record in body[0]["result"].as_array().unwrap() {
				assert!(record["noauth"].is_null(), "body: {body}");
			}
			// Verify the records have the original data
			for record in body[0]["result"].as_array().unwrap() {
				assert_eq!(record["name"], "record_name", "body: {body}");
			}
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn key_endpoint_modify_all() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_guests().await.unwrap();
		let table_name = Ulid::new().to_string();
		let num_records = 10;
		let url = &format!("http://{addr}/key/{table_name}");

		// Prepare HTTP client
		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("surreal-ns", Ulid::new().to_string().parse()?);
		headers.insert("surreal-db", Ulid::new().to_string().parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_millis(10))
			.default_headers(headers)
			.build()?;

		seed_table(&client, &addr, &table_name, num_records).await?;

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
			assert_eq!(body[0]["result"].as_array().unwrap().len(), num_records, "body: {body}");

			// Verify the records have the new data
			for record in body[0]["result"].as_array().unwrap() {
				assert_eq!(record["name"], "record_name", "body: {body}");
			}
			// Verify the records also have the original data
			for record in body[0]["result"].as_array().unwrap() {
				assert_eq!(record["default"], "content", "body: {body}");
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
			assert_eq!(body[0]["result"].as_array().unwrap().len(), num_records, "body: {body}");

			// Verify the records don't have the new data
			for record in body[0]["result"].as_array().unwrap() {
				assert!(record["noauth"].is_null(), "body: {body}");
			}
			// Verify the records have the original data
			for record in body[0]["result"].as_array().unwrap() {
				assert_eq!(record["name"], "record_name", "body: {body}");
			}
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn key_endpoint_delete_all() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_guests().await.unwrap();
		let table_name = "table";
		let num_records = 10;
		let url = &format!("http://{addr}/key/{table_name}");

		// Prepare HTTP client
		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("surreal-ns", Ulid::new().to_string().parse()?);
		headers.insert("surreal-db", Ulid::new().to_string().parse()?);
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
			assert_eq!(body[0]["result"].as_array().unwrap().len(), num_records, "body: {body}");

			// Try to delete the records
			let res = client.delete(url).basic_auth(USER, Some(PASS)).send().await?;
			assert_eq!(res.status(), 200, "body: {}", res.text().await?);

			// Verify the records were deleted
			let res = client.get(url).basic_auth(USER, Some(PASS)).send().await?;
			let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
			assert_eq!(body[0]["result"].as_array().unwrap().len(), 0, "body: {body}");
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
			assert_eq!(body[0]["result"].as_array().unwrap().len(), num_records, "body: {body}");
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn key_endpoint_select_one() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_guests().await.unwrap();
		let table_name = "table";
		let url = &format!("http://{addr}/key/{table_name}/1");

		// Prepare HTTP client
		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("surreal-ns", Ulid::new().to_string().parse()?);
		headers.insert("surreal-db", Ulid::new().to_string().parse()?);
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
			assert_eq!(body[0]["result"].as_array().unwrap().len(), 1, "body: {body}");
		}

		// GET without authentication returns no record
		{
			let res = client.get(url).send().await?;
			assert_eq!(res.status(), 200, "body: {}", res.text().await?);

			let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
			assert_eq!(body[0]["result"].as_array().unwrap().len(), 0, "body: {body}");
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn key_endpoint_create_one() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_guests().await.unwrap();
		let table_name = "table";

		// Prepare HTTP client
		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("surreal-ns", Ulid::new().to_string().parse()?);
		headers.insert("surreal-db", Ulid::new().to_string().parse()?);
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
			assert_eq!(body[0]["result"].as_array().unwrap().len(), 1, "body: {body}");
			assert_eq!(
				body[0]["result"].as_array().unwrap()[0]["id"],
				"table:new_id",
				"body: {body}"
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
			assert_eq!(body[0]["result"].as_array().unwrap().len(), 1, "body: {body}");
			assert_eq!(
				body[0]["result"].as_array().unwrap()[0]["id"],
				"table:new_id_query",
				"body: {body}"
			);
			assert_eq!(body[0]["result"].as_array().unwrap()[0]["age"], 45, "body: {body}");
			assert_eq!(
				body[0]["result"].as_array().unwrap()[0]["elems"].as_array().unwrap().len(),
				3,
				"body: {body}"
			);
			assert_eq!(
				body[0]["result"].as_array().unwrap()[0]["other"].as_object().unwrap()["test"],
				true,
				"body: {body}"
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
			assert_eq!(body[0]["result"].as_array().unwrap().len(), 0, "body: {body}");
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn key_endpoint_update_one() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_guests().await.unwrap();
		let table_name = "table";
		let url = &format!("http://{addr}/key/{table_name}/1");

		// Prepare HTTP client
		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("surreal-ns", Ulid::new().to_string().parse()?);
		headers.insert("surreal-db", Ulid::new().to_string().parse()?);
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
			assert_eq!(body[0]["result"].as_array().unwrap()[0]["id"], "table:1", "body: {body}");

			// Verify the record has the new data
			assert_eq!(
				body[0]["result"].as_array().unwrap()[0]["name"],
				"record_name",
				"body: {body}"
			);

			// Verify the record doesn't have the original data
			assert!(body[0]["result"].as_array().unwrap()[0]["default"].is_null(), "body: {body}");
		}

		// Update one record without authentication
		{
			// Try to update the record
			let res = client.put(url).body(r#"{"noauth": "yes"}"#).send().await?;
			assert_eq!(res.status(), 200, "body: {}", res.text().await?);

			// Verify the record was not updated
			let res = client.get(url).basic_auth(USER, Some(PASS)).send().await?;
			let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
			assert_eq!(body[0]["result"].as_array().unwrap()[0]["id"], "table:1", "body: {body}");

			// Verify the record doesn't have the new data
			assert!(body[0]["result"].as_array().unwrap()[0]["noauth"].is_null(), "body: {body}");

			// Verify the record has the original data
			assert_eq!(
				body[0]["result"].as_array().unwrap()[0]["name"],
				"record_name",
				"body: {body}"
			);
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn key_endpoint_modify_one() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_guests().await.unwrap();
		let table_name = "table";
		let url = &format!("http://{addr}/key/{table_name}/1");

		// Prepare HTTP client
		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("surreal-ns", Ulid::new().to_string().parse()?);
		headers.insert("surreal-db", Ulid::new().to_string().parse()?);
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
			assert_eq!(body[0]["result"].as_array().unwrap()[0]["id"], "table:1", "body: {body}");

			// Verify the record has the new data
			assert_eq!(
				body[0]["result"].as_array().unwrap()[0]["name"],
				"record_name",
				"body: {body}"
			);

			// Verify the record has the original data too
			assert_eq!(
				body[0]["result"].as_array().unwrap()[0]["default"],
				"content",
				"body: {body}"
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
			assert_eq!(body[0]["result"].as_array().unwrap()[0]["id"], "table:1", "body: {body}");

			// Verify the record doesn't have the new data
			assert!(body[0]["result"].as_array().unwrap()[0]["noauth"].is_null(), "body: {body}");

			// Verify the record has the original data too
			assert_eq!(
				body[0]["result"].as_array().unwrap()[0]["default"],
				"content",
				"body: {body}"
			);
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn key_endpoint_delete_one() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_guests().await.unwrap();
		let table_name = "table";
		let base_url = &format!("http://{addr}/key/{table_name}");

		// Prepare HTTP client
		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("surreal-ns", Ulid::new().to_string().parse()?);
		headers.insert("surreal-db", Ulid::new().to_string().parse()?);
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
			assert_eq!(body[0]["result"].as_array().unwrap().len(), 2, "body: {body}");

			// Try to delete the record
			let res =
				client.delete(format!("{base_url}/1")).basic_auth(USER, Some(PASS)).send().await?;
			assert_eq!(res.status(), 200, "body: {}", res.text().await?);

			// Verify only one record was deleted
			let res = client.get(base_url).basic_auth(USER, Some(PASS)).send().await?;
			let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
			assert_eq!(body[0]["result"].as_array().unwrap().len(), 1, "body: {body}");
			assert_eq!(body[0]["result"].as_array().unwrap()[0]["id"], "table:2", "body: {body}");
		}

		// Delete one record without authentication
		{
			// Try to delete the record
			let res = client.delete(format!("{base_url}/2")).send().await?;
			assert_eq!(res.status(), 200, "body: {}", res.text().await?);

			// Verify the record was not deleted
			let res = client.get(base_url).basic_auth(USER, Some(PASS)).send().await?;
			let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
			assert_eq!(body[0]["result"].as_array().unwrap().len(), 1, "body: {body}");
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn signup_mal() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		// Prepare HTTP client
		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/surrealdb".parse()?);
		headers.insert(header::CONTENT_TYPE, "application/surrealdb".parse()?);
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_millis(10))
			.default_headers(headers)
			.build()?;

		// Define a record access method
		{
			let res = client
				.post(format!("http://{addr}/sql"))
				.basic_auth(USER, Some(PASS))
				.body(
					r#"
					DEFINE ACCESS user ON DATABASE TYPE RECORD
						SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
						SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
						DURATION FOR SESSION 12h
					;
				"#,
				)
				.send()
				.await?;
			assert!(res.status().is_success(), "body: {}", res.text().await?);
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn http_capabilities() {
		use tokio::time;
		// Deny some
		{
			// Start server disallowing routes for queries, exporting and importing
			let (addr, _server) = common::start_server(StartServerArguments {
				args: "--deny-http sql,export,import".to_string(),
				// Auth disabled to ensure unauthorized errors are due to capabilities
				auth: false,
				..Default::default()
			})
			.await
			.unwrap();

			// Prepare HTTP client
			let mut headers = reqwest::header::HeaderMap::new();
			let ns = Ulid::new().to_string();
			let db = Ulid::new().to_string();
			headers.insert("surreal-ns", ns.parse().unwrap());
			headers.insert("surreal-db", db.parse().unwrap());
			headers.insert(header::ACCEPT, "application/json".parse().unwrap());
			let client = reqwest::Client::builder()
				.connect_timeout(Duration::from_millis(10))
				.default_headers(headers)
				.build()
				.unwrap();
			let base_url = &format!("http://{addr}");

			// Check that denied routes are disallowed
			let res = client
				.post(format!("{base_url}/sql"))
				.basic_auth(USER, Some(PASS))
				.send()
				.await
				.unwrap();
			assert_eq!(res.status(), 403, "body: {}", res.text().await.unwrap());
			let res = client
				.post(format!("{base_url}/import"))
				.basic_auth(USER, Some(PASS))
				.send()
				.await
				.unwrap();
			assert_eq!(res.status(), 403, "body: {}", res.text().await.unwrap());
			let res = client
				.get(format!("{base_url}/export"))
				.basic_auth(USER, Some(PASS))
				.send()
				.await
				.unwrap();
			assert_eq!(res.status(), 403, "body: {}", res.text().await.unwrap());

			// Check that other routes are allowed
			// GET
			for route in ["status", "health", "version", "sync", "ml/export/test/1.0.0"] {
				println!("Testing \"/{route}\" route...");

				let res = client
					.get(format!("{base_url}/{route}"))
					.basic_auth(USER, Some(PASS))
					.send()
					.await
					.unwrap();
				assert_ne!(res.status(), 403, "body: {}", res.text().await.unwrap());
			}
			// POST
			for route in ["signin", "signup", "key/test", "ml/import"] {
				println!("Testing \"/{route}\" route...");

				let res = client
					.post(format!("{base_url}/{route}"))
					.basic_auth(USER, Some(PASS))
					.send()
					.await
					.unwrap();
				assert_ne!(res.status(), 403, "body: {}", res.text().await.unwrap());
			}
			// WebSocket
			println!("Testing \"/rpc\" route...");
			client
				.get(format!("{base_url}/rpc"))
				.header(header::CONNECTION, "Upgrade")
				.header(header::UPGRADE, "websocket")
				.header(header::SEC_WEBSOCKET_VERSION, "13")
				.header(header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
				.send()
				.await
				.unwrap()
				.upgrade()
				.await
				.unwrap();
		}
		// Deny all
		{
			// Start server disallowing all routes except for RPC and health
			let (addr, _server) = common::start_server(StartServerArguments {
				args: "--deny-http --allow-http rpc,health".to_string(),
				// Auth disabled to ensure unauthorized errors are due to capabilities
				auth: false,
				..Default::default()
			})
			.await
			.unwrap();

			// Prepare HTTP client
			let mut headers = reqwest::header::HeaderMap::new();
			let ns = Ulid::new().to_string();
			let db = Ulid::new().to_string();
			headers.insert("surreal-ns", ns.parse().unwrap());
			headers.insert("surreal-db", db.parse().unwrap());
			headers.insert(header::ACCEPT, "application/json".parse().unwrap());
			let client = reqwest::Client::builder()
				.connect_timeout(Duration::from_millis(10))
				.default_headers(headers)
				.build()
				.unwrap();
			let base_url = &format!("http://{addr}");

			// Check that denied routes are disallowed
			// GET
			for route in ["version", "sync", "export", "ml/export/test/1.0.0"] {
				println!("Testing \"/{route}\" route...");

				let res = client
					.get(format!("{base_url}/{route}"))
					.basic_auth(USER, Some(PASS))
					.send()
					.await
					.unwrap();
				assert_eq!(res.status(), 403, "body: {}", res.text().await.unwrap());
			}
			// POST
			for route in ["sql", "signin", "signup", "key/test", "import", "ml/import"] {
				println!("Testing \"/{route}\" route...");

				let res = client
					.post(format!("{base_url}/{route}"))
					.basic_auth(USER, Some(PASS))
					.send()
					.await
					.unwrap();
				assert_eq!(res.status(), 403, "body: {}", res.text().await.unwrap());
			}
			// WebSocket
			println!("Testing \"/rpc\" route...");
			client
				.get(format!("{base_url}/rpc"))
				.header(header::CONNECTION, "Upgrade")
				.header(header::UPGRADE, "websocket")
				.header(header::SEC_WEBSOCKET_VERSION, "13")
				.header(header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
				.send()
				.await
				.unwrap()
				.upgrade()
				.await
				.unwrap();
		}
		// Deny RPC and health endpoints
		{
			// Start server disallowing the RPC and health routes
			let (addr, _server) = common::start_server(StartServerArguments {
				args: "--deny-http rpc,health".to_string(),
				// Auth disabled to ensure unauthorized errors are due to capabilities
				auth: false,
				// Ready check disabled as healtcheck is disallowed
				wait_is_ready: false,
				..Default::default()
			})
			.await
			.unwrap();
			// The "is-ready" command uses the RPC and health routes
			// We must wait for server startup rudimentarily
			// If this introduces flakiness, drop this test case
			time::sleep(time::Duration::from_millis(5000)).await;

			// Prepare HTTP client
			let mut headers = reqwest::header::HeaderMap::new();
			let ns = Ulid::new().to_string();
			let db = Ulid::new().to_string();
			headers.insert("surreal-ns", ns.parse().unwrap());
			headers.insert("surreal-db", db.parse().unwrap());
			headers.insert(header::ACCEPT, "application/json".parse().unwrap());
			let client = reqwest::Client::builder()
				.connect_timeout(Duration::from_millis(10))
				.default_headers(headers)
				.build()
				.unwrap();
			let base_url = &format!("http://{addr}");

			// Check that health requests are disallowed
			let res = client
				.get(format!("{base_url}/health"))
				.basic_auth(USER, Some(PASS))
				.send()
				.await
				.unwrap();
			assert_eq!(res.status(), 403, "body: {}", res.text().await.unwrap());

			// Check that RPC requests are disallowed
			println!("Testing \"/rpc\" route...");
			let res = client
				.get(format!("{base_url}/rpc"))
				.header(header::CONNECTION, "Upgrade")
				.header(header::UPGRADE, "websocket")
				.header(header::SEC_WEBSOCKET_VERSION, "13")
				.header(header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
				.send()
				.await
				.unwrap()
				.upgrade()
				.await;
			assert!(res.is_err(), "Request to \"/rpc\" endpoint unexpectedly succeeded")
		}
	}

	#[test(tokio::test)]
	async fn experimental_capabilities() {
		// Allow 1
		{
			// Start server disallowing routes for queries, exporting and importing
			let (addr, _server) = common::start_server(StartServerArguments {
				args: "--deny-experimental * --allow-experimental record_references".to_string(),
				// Auth disabled to ensure unauthorized errors are due to capabilities
				auth: false,
				..Default::default()
			})
			.await
			.unwrap();

			// Prepare HTTP client
			let mut headers = reqwest::header::HeaderMap::new();
			let ns = Ulid::new().to_string();
			let db = Ulid::new().to_string();
			headers.insert("surreal-ns", ns.parse().unwrap());
			headers.insert("surreal-db", db.parse().unwrap());
			headers.insert(header::ACCEPT, "application/json".parse().unwrap());
			let client = reqwest::Client::builder()
				.connect_timeout(Duration::from_millis(10))
				.default_headers(headers)
				.build()
				.unwrap();
			let base_url = &format!("http://{addr}");

			// Check that denied routes are disallowed
			let res = client
				.post(format!("{base_url}/sql"))
				.basic_auth(USER, Some(PASS))
				.body("DEFINE FIELD a ON deny_all_allow_references TYPE record REFERENCE")
				.send()
				.await
				.unwrap();
			let res = res.text().await.unwrap();
			assert!(res.contains("[{\"result\":null,\"status\":\"OK\""), "body: {}", res);
		}
		// Deny 1
		{
			// Start server disallowing routes for queries, exporting and importing
			let (addr, _server) = common::start_server(StartServerArguments {
				args: "--deny-experimental record_references --allow-experimental *".to_string(),
				// Auth disabled to ensure unauthorized errors are due to capabilities
				auth: false,
				..Default::default()
			})
			.await
			.unwrap();

			// Prepare HTTP client
			let mut headers = reqwest::header::HeaderMap::new();
			let ns = Ulid::new().to_string();
			let db = Ulid::new().to_string();
			headers.insert("surreal-ns", ns.parse().unwrap());
			headers.insert("surreal-db", db.parse().unwrap());
			headers.insert(header::ACCEPT, "application/json".parse().unwrap());
			let client = reqwest::Client::builder()
				.connect_timeout(Duration::from_millis(10))
				.default_headers(headers)
				.build()
				.unwrap();
			let base_url = &format!("http://{addr}");

			// Check that denied routes are disallowed
			let res = client
				.post(format!("{base_url}/sql"))
				.basic_auth(USER, Some(PASS))
				.body("DEFINE FIELD a ON deny_all_allow_references TYPE record REFERENCE")
				.send()
				.await
				.unwrap();
			let res = res.text().await.unwrap();
			assert!(
				res.contains("Experimental capability `record_references` is not enabled"),
				"body: {}",
				res
			);
		}
	}

	#[test(tokio::test)]
	async fn arbitrary_query_capabilities() {
		// Allow system
		{
			// Start server disallowing routes for queries, exporting and importing
			let (addr, _server) = common::start_server(StartServerArguments {
				args: "--allow-arbitrary-query system".to_string(),
				// Auth disabled to ensure unauthorized errors are due to capabilities
				auth: false,
				..Default::default()
			})
			.await
			.unwrap();

			// Prepare HTTP client
			let mut headers = reqwest::header::HeaderMap::new();
			let ns = Ulid::new().to_string();
			let db = Ulid::new().to_string();
			headers.insert("surreal-ns", ns.parse().unwrap());
			headers.insert("surreal-db", db.parse().unwrap());
			headers.insert(header::ACCEPT, "application/json".parse().unwrap());
			let client = reqwest::Client::builder()
				.connect_timeout(Duration::from_millis(10))
				.default_headers(headers)
				.build()
				.unwrap();
			let base_url = &format!("http://{addr}");

			// Check that denied routes are disallowed
			let res = client
				.post(format!("{base_url}/sql"))
				.basic_auth(USER, Some(PASS))
				.body("123")
				.send()
				.await
				.unwrap();
			let res = res.text().await.unwrap();
			assert!(res.contains("[{\"result\":123,\"status\":\"OK\""), "body: {}", res);
		}
		// Allow record
		{
			// Start server disallowing routes for queries, exporting and importing
			let (addr, _server) = common::start_server(StartServerArguments {
				args: "--allow-arbitrary-query record".to_string(),
				// Auth disabled to ensure unauthorized errors are due to capabilities
				auth: false,
				..Default::default()
			})
			.await
			.unwrap();

			// Prepare HTTP client
			let mut headers = reqwest::header::HeaderMap::new();
			let ns = Ulid::new().to_string();
			let db = Ulid::new().to_string();
			headers.insert("surreal-ns", ns.parse().unwrap());
			headers.insert("surreal-db", db.parse().unwrap());
			headers.insert(header::ACCEPT, "application/json".parse().unwrap());
			let client = reqwest::Client::builder()
				.connect_timeout(Duration::from_millis(10))
				.default_headers(headers)
				.build()
				.unwrap();
			let base_url = &format!("http://{addr}");

			// Check that denied routes are disallowed
			let res = client
				.post(format!("{base_url}/sql"))
				.basic_auth(USER, Some(PASS))
				.body("123")
				.send()
				.await
				.unwrap();
			let res = res.text().await.unwrap();
			assert!(res.contains("The HTTP route 'sql' is forbidden"), "body: {}", res);
		}
		// Deny arbitrary querying
		{
			// Start server disallowing routes for queries, exporting and importing
			let (addr, _server) = common::start_server(StartServerArguments {
				args: "--deny-arbitrary-query *".to_string(),
				// Auth disabled to ensure unauthorized errors are due to capabilities
				auth: false,
				..Default::default()
			})
			.await
			.unwrap();

			// Prepare HTTP client
			let mut headers = reqwest::header::HeaderMap::new();
			let ns = Ulid::new().to_string();
			let db = Ulid::new().to_string();
			headers.insert("surreal-ns", ns.parse().unwrap());
			headers.insert("surreal-db", db.parse().unwrap());
			headers.insert(header::ACCEPT, "application/json".parse().unwrap());
			let client = reqwest::Client::builder()
				.connect_timeout(Duration::from_millis(10))
				.default_headers(headers)
				.build()
				.unwrap();
			let base_url = &format!("http://{addr}");

			// Check that denied routes are disallowed
			let res = client
				.post(format!("{base_url}/sql"))
				.basic_auth(USER, Some(PASS))
				.body("123")
				.send()
				.await
				.unwrap();
			let res = res.text().await.unwrap();
			assert!(res.contains("The HTTP route 'sql' is forbidden"), "body: {}", res);
		}
	}
}
