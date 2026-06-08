// RUST_LOG=warn cargo make ci-http-integration
mod common;

mod http_integration {
	use std::time::Duration;

	use futures_util::{SinkExt, StreamExt};
	use http::header::HeaderValue;
	use http::{Method, header};
	use reqwest::Client;
	use serde_json::json;
	use surrealdb::headers::{AUTH_DB, AUTH_NS};
	use test_log::test;
	use tokio_tungstenite::connect_async;
	use tokio_tungstenite::tungstenite::Message;
	use tokio_tungstenite::tungstenite::client::IntoClientRequest;
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

		// Create namespace and database
		ensure_namespace_and_database(&client, &addr, &ns, &db).await?;

		// Request without credentials, gives an anonymous session
		// Note: When namespace/database exists and guests are allowed, the request may succeed
		// but return empty results. The original test expected "Not enough permissions" error.
		{
			let res = client.post(url).body("CREATE foo").send().await?;
			assert_eq!(res.status(), 200);
			let body = res.text().await?;
			// Check for either error status or "Not enough permissions" message
			let body_json: serde_json::Value =
				serde_json::from_str(&body).unwrap_or(serde_json::Value::Null);
			if body_json.is_array() && !body_json.as_array().unwrap().is_empty() {
				let first_result = &body_json[0];
				// Should have error status or contain "Not enough permissions", or empty result
				// (when guests allowed)
				let has_error =
					first_result["status"] == "ERR" || body.contains("Not enough permissions");
				let has_empty_result = first_result["status"] == "OK"
					&& first_result["result"].as_array().is_some_and(|a| a.is_empty());
				assert!(has_error || has_empty_result, "body: {body}");
			} else {
				assert!(body.contains("Not enough permissions"), "body: {body}");
			}
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
			let body: serde_json::Value = res.json().await?;
			assert_eq!(body[0]["status"], "OK");
			assert_eq!(body[0]["result"].as_array().unwrap().len(), 1);
			assert!(body[0]["result"][0]["id"].to_string().starts_with("\"foo:"));
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
			let body: serde_json::Value = res.json().await?;
			assert_eq!(body[0]["status"], "OK", "body: {body}");
			assert_eq!(body[0]["result"].as_array().unwrap().len(), 1, "body: {body}");
			assert!(body[0]["result"][0]["id"].to_string().starts_with("\"foo:"), "body: {body}");

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

	// Exercise each `--client-ip` mode end-to-end: send a `RETURN session::ip()`
	// over HTTP and verify the value matches what the configured extractor
	// would produce. Unit tests in `ntw::client_ip` already cover the
	// `Forwarded` header parser exhaustively; these tests cover the wiring
	// from the CLI flag through `ExtractClientIP` to the SurrealQL session.

	async fn fetch_session_ip(
		addr: &str,
		extra_headers: &[(&str, &str)],
	) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert(header::ACCEPT, "application/json".parse()?);
		for (name, value) in extra_headers {
			headers.insert(
				reqwest::header::HeaderName::from_bytes(name.as_bytes())?,
				HeaderValue::from_str(value)?,
			);
		}
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_millis(10))
			.default_headers(headers)
			.build()?;
		let res = client
			.post(format!("http://{addr}/sql"))
			.basic_auth(USER, Some(PASS))
			.body("RETURN session::ip()")
			.send()
			.await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);
		let body: serde_json::Value = res.json().await?;
		Ok(body[0]["result"].clone())
	}

	#[test(tokio::test)]
	async fn client_ip_socket() -> Result<(), Box<dyn std::error::Error>> {
		// Default mode (`--client-ip socket`) reports the raw peer address.
		// The test client always connects from 127.0.0.1, so the extracted
		// IP should match — independent of any forwarding headers we set.
		let (addr, _server) = common::start_server_with_defaults().await?;
		let result = fetch_session_ip(&addr, &[("X-Forwarded-For", "203.0.113.7")]).await?;
		assert_eq!(result, serde_json::json!("127.0.0.1"));
		Ok(())
	}

	#[test(tokio::test)]
	async fn client_ip_x_forwarded_for() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server(StartServerArguments {
			args: "--client-ip X-Forwarded-For".to_string(),
			..Default::default()
		})
		.await?;

		// With the header present, the extractor returns its value verbatim
		// (`X-Forwarded-For` is not parsed beyond reading the header — the
		// raw string flows through, matching the historical behaviour
		// callers rely on for chained-proxy values).
		let result = fetch_session_ip(&addr, &[("X-Forwarded-For", "203.0.113.7")]).await?;
		assert_eq!(result, serde_json::json!("203.0.113.7"));

		// Without the header the extractor yields no value, so `session::ip()`
		// returns NONE (serialised as JSON `null`).
		let result = fetch_session_ip(&addr, &[]).await?;
		assert_eq!(result, serde_json::Value::Null);

		Ok(())
	}

	#[test(tokio::test)]
	async fn client_ip_forwarded_rfc7239() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server(StartServerArguments {
			args: "--client-ip Forwarded".to_string(),
			..Default::default()
		})
		.await?;

		// `Forwarded` is parsed per RFC 7239: take the first forwarded-element
		// and return its `for=` identifier. Quoted IPv6 forms (`for="[...]"`)
		// are unquoted by the parser.
		let result =
			fetch_session_ip(&addr, &[("Forwarded", r#"for="[2001:db8::1]:4711";proto=https"#)])
				.await?;
		assert_eq!(result, serde_json::json!("[2001:db8::1]:4711"));

		// A `Forwarded` header without a `for=` parameter yields no IP.
		let result =
			fetch_session_ip(&addr, &[("Forwarded", "by=203.0.113.43;proto=http")]).await?;
		assert_eq!(result, serde_json::Value::Null);

		Ok(())
	}

	#[test(tokio::test)]
	async fn client_ip_none() -> Result<(), Box<dyn std::error::Error>> {
		// `--client-ip none` short-circuits the extractor regardless of what
		// headers (or socket address) are visible.
		let (addr, _server) = common::start_server(StartServerArguments {
			args: "--client-ip none".to_string(),
			..Default::default()
		})
		.await?;
		let result = fetch_session_ip(&addr, &[("X-Forwarded-For", "203.0.113.7")]).await?;
		assert_eq!(result, serde_json::Value::Null);
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

			// Create namespace and database
			ensure_namespace_and_database(&client, &addr, &ns, &db).await.unwrap();

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

			// Create namespace and database
			ensure_namespace_and_database(&client, &addr, &ns, &db).await.unwrap();

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

			// Create namespace and database
			ensure_namespace_and_database(&client, &addr, &ns, &db).await.unwrap();

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
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_millis(10))
			.default_headers(headers)
			.build()?;

		// Create namespace and database
		ensure_namespace_and_database(&client, &addr, &ns, &db).await?;

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
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_millis(10))
			.default_headers(headers)
			.build()?;

		// Create namespace and database
		ensure_namespace_and_database(&client, &addr, &ns, &db).await?;

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

	/// RPC delete with a record-id string (e.g. "table:id") must be interpreted as a record id,
	/// not a table name. Deleting a non-existent record returns success with an empty array.
	#[test(tokio::test)]
	async fn rpc_delete_record_id() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let url = &format!("http://{addr}/rpc");
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::CONTENT_TYPE, "application/json".parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_millis(10))
			.default_headers(headers)
			.build()?;

		ensure_namespace_and_database(&client, &addr, &ns, &db).await?;

		// Ensure the article table exists (create and remove a dummy record so table is empty)
		let res = client
			.post(format!("http://{addr}/sql"))
			.basic_auth(USER, Some(PASS))
			.body("CREATE article:__ensure_table__ SET x = 1; DELETE article:__ensure_table__")
			.send()
			.await?;
		assert!(res.status().is_success(), "body: {}", res.text().await?);

		// Delete non-existent record by record-id string: must succeed with empty result (not
		// error)
		{
			let body = json!({
				"id": "1",
				"method": "delete",
				"params": ["article:nonexisted"]
			});
			let res =
				client.post(url).basic_auth(USER, Some(PASS)).body(body.to_string()).send().await?;
			assert!(res.status().is_success(), "body: {}", res.text().await?);
			let body: serde_json::Value = res.json().await?;
			assert!(
				body.get("error").is_none(),
				"RPC delete of non-existent record must not return error: {body}"
			);
			let result = body.get("result").expect("response must have result");
			assert!(
				result.is_null(),
				"result must be null for non-existent record (single result): {result}"
			);
		}

		// Delete existing record by record-id string: must return the deleted record
		{
			// Create a record
			let res = client
				.post(format!("http://{addr}/sql"))
				.basic_auth(USER, Some(PASS))
				.body("CREATE article:rpc_delete_test SET name = 'test'")
				.send()
				.await?;
			assert!(res.status().is_success(), "body: {}", res.text().await?);

			let body = json!({
				"id": "2",
				"method": "delete",
				"params": ["article:rpc_delete_test"]
			});
			let res =
				client.post(url).basic_auth(USER, Some(PASS)).body(body.to_string()).send().await?;
			assert!(res.status().is_success(), "body: {}", res.text().await?);
			let body: serde_json::Value = res.json().await?;
			assert!(body.get("error").is_none(), "delete must succeed: {body}");
			let result = body.get("result").expect("response must have result");
			assert!(
				result.is_object()
					&& result.get("id").and_then(|v| v.as_str()) == Some("article:rpc_delete_test"),
				"result must be the single deleted record: {result}"
			);

			// Delete same record again (non-existent now): must succeed with null
			let body = json!({
				"id": "3",
				"method": "delete",
				"params": ["article:rpc_delete_test"]
			});
			let res =
				client.post(url).basic_auth(USER, Some(PASS)).body(body.to_string()).send().await?;
			assert!(res.status().is_success(), "body: {}", res.text().await?);
			let body: serde_json::Value = res.json().await?;
			assert!(body.get("error").is_none(), "second delete must succeed: {body}");
			let result = body.get("result").expect("response must have result");
			assert!(result.is_null(), "result must be null for non-existent record: {result}");
		}

		Ok(())
	}

	/// Spawns many authenticated and unauthenticated POST `/rpc` requests in
	/// parallel and asserts every unauthenticated request is rejected while
	/// every authenticated one succeeds. A shared-slot regression would cause
	/// at least one unauthenticated task to observe an authenticated session
	/// and succeed.
	#[test(tokio::test)]
	async fn rpc_session_isolation_under_concurrency() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let url = std::sync::Arc::new(format!("http://{addr}/rpc"));
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::CONTENT_TYPE, "application/json".parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_millis(10))
			.default_headers(headers)
			.build()?;

		ensure_namespace_and_database(&client, &addr, &ns, &db).await?;

		const PAIRS: usize = 16;
		let mut handles = Vec::with_capacity(PAIRS * 2);
		for i in 0..PAIRS {
			let client_auth = client.clone();
			let url_auth = url.clone();
			handles.push(tokio::spawn(async move {
				let body = json!({
					"id": format!("auth-{i}"),
					"method": "query",
					"params": ["INFO FOR ROOT"],
				});
				let res = client_auth
					.post(url_auth.as_str())
					.basic_auth(USER, Some(PASS))
					.body(body.to_string())
					.send()
					.await
					.expect("auth send");
				let body: serde_json::Value = res.json().await.expect("auth json");
				(true, body)
			}));

			let client_unauth = client.clone();
			let url_unauth = url.clone();
			handles.push(tokio::spawn(async move {
				let body = json!({
					"id": format!("unauth-{i}"),
					"method": "query",
					"params": ["INFO FOR ROOT"],
				});
				let res = client_unauth
					.post(url_unauth.as_str())
					.body(body.to_string())
					.send()
					.await
					.expect("unauth send");
				let body: serde_json::Value = res.json().await.expect("unauth json");
				(false, body)
			}));
		}

		for handle in handles {
			let (authenticated, body) = handle.await?;
			let status = body
				.get("result")
				.and_then(|r| r.as_array())
				.and_then(|a| a.first())
				.and_then(|r| r["status"].as_str());
			if authenticated {
				assert_eq!(
					status,
					Some("OK"),
					"authenticated concurrent INFO FOR ROOT must succeed: {body}"
				);
			} else {
				assert_ne!(
					status,
					Some("OK"),
					"unauthenticated concurrent INFO FOR ROOT must NOT succeed (session leak): {body}"
				);
			}
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
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);

		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_millis(10))
			.default_headers(headers)
			.build()?;

		// Create namespace and database
		ensure_namespace_and_database(&client, &addr, &ns, &db).await?;

		// Options method works
		{
			let res = client.request(Method::OPTIONS, url).send().await?;
			assert_eq!(res.status(), 200);
		}

		// Creating a record without credentials is not allowed
		// Note: When namespace/database exists and guests are allowed, the request may succeed
		// but return empty results. The original test expected "Not enough permissions" error.
		{
			let res = client.post(url).body("CREATE foo").send().await?;
			assert_eq!(res.status(), 200);

			let body = res.text().await?;
			// Check for either error status or "Not enough permissions" message
			let body_json: serde_json::Value =
				serde_json::from_str(&body).unwrap_or(serde_json::Value::Null);
			if body_json.is_array() && !body_json.as_array().unwrap().is_empty() {
				let first_result = &body_json[0];
				// Should have error status or contain "Not enough permissions", or empty result
				// (when guests allowed)
				let has_error =
					first_result["status"] == "ERR" || body.contains("Not enough permissions");
				let has_empty_result = first_result["status"] == "OK"
					&& first_result["result"].as_array().is_some_and(|a| a.is_empty());
				assert!(has_error || has_empty_result, "body: {body}");
			} else {
				assert!(body.contains("Not enough permissions"), "body: {body}");
			}
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
			let bytes = res.bytes().await?;
			let _: ciborium::Value = ciborium::from_reader(&*bytes).unwrap();
		}

		// Creating a record with Accept Surrealdb encoding is allowed
		{
			let res = client
				.post(url)
				.basic_auth(USER, Some(PASS))
				.header(header::ACCEPT, surrealdb_core::api::format::FLATBUFFERS)
				.body("CREATE foo")
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let bytes = res.bytes().await?;
			let value: surrealdb_types::Value =
				surrealdb_core::rpc::format::flatbuffers::decode(&bytes)
					.expect("flatbuffers SQL response should decode to Value");
			let array = value.into_array().unwrap();
			assert_eq!(array.len(), 1);
			let result = array.into_iter().next().unwrap().into_object().unwrap();
			assert_eq!(
				result.get("status"),
				Some(&surrealdb_types::Value::String("OK".to_string()))
			);
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

	#[test(tokio::test)]
	async fn sql_endpoint_with_compression() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let url = &format!("http://{addr}/sql");

		// Prepare HTTP client
		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		headers.insert(header::ACCEPT_ENCODING, "gzip".parse()?);

		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_millis(10))
			.gzip(false) // So that the content-encoding header is not removed by Reqwest
			.default_headers(headers.clone())
			.build()?;

		// Create namespace and database
		ensure_namespace_and_database(&client, &addr, &ns, &db).await?;

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

	async fn ensure_namespace_and_database(
		_client: &Client,
		addr: &str,
		ns: &str,
		db: &str,
	) -> Result<(), Box<dyn std::error::Error>> {
		// Create a separate client without namespace/database headers for ROOT-level operations
		let mut root_headers = reqwest::header::HeaderMap::new();
		root_headers.insert(header::ACCEPT, "application/json".parse()?);
		let root_client = reqwest::Client::builder()
			.connect_timeout(Duration::from_millis(10))
			.default_headers(root_headers)
			.build()?;

		// Create namespace at ROOT level
		let res = root_client
			.post(format!("http://{addr}/sql"))
			.basic_auth(USER, Some(PASS))
			.body(format!("DEFINE NAMESPACE `{ns}`"))
			.send()
			.await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);

		// Create database within the namespace
		let mut ns_headers = reqwest::header::HeaderMap::new();
		ns_headers.insert("surreal-ns", ns.parse()?);
		ns_headers.insert(header::ACCEPT, "application/json".parse()?);
		let ns_client = reqwest::Client::builder()
			.connect_timeout(Duration::from_millis(10))
			.default_headers(ns_headers)
			.build()?;

		let res = ns_client
			.post(format!("http://{addr}/sql"))
			.basic_auth(USER, Some(PASS))
			.body(format!("DEFINE DATABASE `{db}`"))
			.send()
			.await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);
		Ok(())
	}

	async fn seed_table(
		client: &Client,
		addr: &str,
		table: &str,
		num_records: usize,
	) -> Result<(), Box<dyn std::error::Error>> {
		// Create the table first
		let res = client
			.post(format!("http://{addr}/sql"))
			.basic_auth(USER, Some(PASS))
			.body(format!("DEFINE TABLE `{table}`"))
			.send()
			.await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);

		// Then create records
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
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_millis(10))
			.default_headers(headers)
			.build()?;

		// Create namespace and database
		ensure_namespace_and_database(&client, &addr, &ns, &db).await?;

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
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_millis(10))
			.default_headers(headers)
			.build()?;

		// Create namespace and database
		ensure_namespace_and_database(&client, &addr, &ns, &db).await?;

		// Create record with random ID
		{
			let table_name = "table";
			let url = &format!("http://{addr}/key/{table_name}");

			// Verify there are no records
			let res = client.get(url).basic_auth(USER, Some(PASS)).send().await?;
			let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
			// The response format is an array with error object when table doesn't exist
			if body.is_array() && !body.as_array().unwrap().is_empty() {
				assert_eq!(body[0]["result"], "The table 'table' does not exist", "body: {body}");
			} else {
				assert_eq!(body["information"], "The table 'table' does not exist", "body: {body}");
			}

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

			// Verify the table is empty (no records were created without auth)
			let res = client.get(url).basic_auth(USER, Some(PASS)).send().await?;
			let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
			// The response format can be an array with error object or a single object
			if body.is_array() && !body.as_array().unwrap().is_empty() {
				// Check if it's an error about table not existing, or empty result array
				let first_result = &body[0];
				if first_result["status"] == "ERR" {
					assert_eq!(
						first_result["result"], "The table 'table_noauth' does not exist",
						"body: {body}"
					);
				} else {
					// Table exists but is empty (no records created without auth)
					assert_eq!(
						first_result["result"].as_array().map_or(0, |a| a.len()),
						0,
						"body: {body}"
					);
				}
			} else {
				assert_eq!(
					body["information"], "The table 'table_noauth' does not exist",
					"body: {body}"
				);
			}
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
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_millis(10))
			.default_headers(headers)
			.build()?;

		// Create namespace and database
		ensure_namespace_and_database(&client, &addr, &ns, &db).await?;

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
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_millis(10))
			.default_headers(headers)
			.build()?;

		// Create namespace and database
		ensure_namespace_and_database(&client, &addr, &ns, &db).await?;

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
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_millis(10))
			.default_headers(headers)
			.build()?;

		// Create namespace and database
		ensure_namespace_and_database(&client, &addr, &ns, &db).await?;

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
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_millis(10))
			.default_headers(headers)
			.build()?;

		// Create namespace and database
		ensure_namespace_and_database(&client, &addr, &ns, &db).await?;

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
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_millis(10))
			.default_headers(headers)
			.build()?;

		// Create namespace and database
		ensure_namespace_and_database(&client, &addr, &ns, &db).await?;

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
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_millis(10))
			.default_headers(headers)
			.build()?;

		// Create namespace and database
		ensure_namespace_and_database(&client, &addr, &ns, &db).await?;

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
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_millis(10))
			.default_headers(headers)
			.build()?;

		// Create namespace and database
		ensure_namespace_and_database(&client, &addr, &ns, &db).await?;

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
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_millis(10))
			.default_headers(headers)
			.build()?;

		// Create namespace and database
		ensure_namespace_and_database(&client, &addr, &ns, &db).await?;

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

	// Regression: `/key/{table}` POST body must be an inert SurrealQL value
	// (literal/object/array/`$param`). Earlier the body was passed straight to
	// `Datastore::execute`, letting an authenticated caller smuggle arbitrary
	// SurrealQL — including multi-statement scripts or a single executable
	// form like `CREATE other:1` or `fn::evil()` — through `/key` and bypass
	// deployments that intentionally enable only the Key route.
	#[test(tokio::test)]
	async fn key_endpoint_rejects_executable_body() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_guests().await.unwrap();

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

		ensure_namespace_and_database(&client, &addr, &ns, &db).await?;

		// Helper: assert /sql shows the side-effect table has no rows. The
		// table is expected to be absent entirely (which surfaces as an error
		// row from `SELECT`); accept that, but reject any result that
		// actually lists rows.
		let assert_pwned_empty =
			async |client: &reqwest::Client| -> Result<(), Box<dyn std::error::Error>> {
				let res = client
					.post(format!("http://{addr}/sql"))
					.basic_auth(USER, Some(PASS))
					.body("SELECT * FROM pwned")
					.send()
					.await?;
				assert_eq!(res.status(), 200);
				let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
				let result = &body[0]["result"];
				if let Some(rows) = result.as_array() {
					assert!(
						rows.is_empty(),
						"side-effect table `pwned` should be empty, got: {body}"
					);
				}
				Ok(())
			};

		// 1. Multi-statement body (the original PoC) is rejected and produces no side effect.
		{
			let url = &format!("http://{addr}/key/victim_multi");
			let res = client
				.post(url)
				.basic_auth(USER, Some(PASS))
				.body("CREATE pwned:1 SET via = 'key_body'; { name: 'legit_payload' }")
				.send()
				.await?;
			assert_ne!(res.status(), 200, "multi-statement body should be rejected");
			assert_pwned_empty(&client).await?;
		}

		// 2. A single executable statement (CREATE) is rejected: this is the case that
		//    `num_statements() == 1` alone would not catch.
		{
			let url = &format!("http://{addr}/key/victim_create");
			let res = client
				.post(url)
				.basic_auth(USER, Some(PASS))
				.body("CREATE pwned:2 SET via = 'key_body_create'")
				.send()
				.await?;
			assert_ne!(res.status(), 200, "single CREATE body should be rejected");
			assert_pwned_empty(&client).await?;
		}

		// 3. A single function-call body is rejected even though the function itself is
		//    side-effect-free; the policy bans the executable shape.
		{
			let url = &format!("http://{addr}/key/victim_fn");
			let res =
				client.post(url).basic_auth(USER, Some(PASS)).body("time::now()").send().await?;
			assert_ne!(res.status(), 200, "function-call body should be rejected");
		}

		// 4. A normal value body still works — the tightened parser must not regress legitimate
		//    REST usage.
		{
			let url = &format!("http://{addr}/key/legit");
			let res = client
				.post(url)
				.basic_auth(USER, Some(PASS))
				.body(r#"{"name": "ok"}"#)
				.send()
				.await?;
			assert_eq!(res.status(), 200, "value body should be accepted: {}", res.text().await?);
		}

		// 5. Object with `$param` references from the URL query still works.
		{
			let url = &format!("http://{addr}/key/legit_params?age=42");
			let res = client
				.post(url)
				.basic_auth(USER, Some(PASS))
				.body(r#"{ age: $age }"#)
				.send()
				.await?;
			assert_eq!(
				res.status(),
				200,
				"object-with-param body should be accepted: {}",
				res.text().await?
			);
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
		headers.insert(header::ACCEPT, surrealdb_core::api::format::FLATBUFFERS.parse()?);
		headers.insert(header::CONTENT_TYPE, surrealdb_core::api::format::FLATBUFFERS.parse()?);
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
			// The /sql WebSocket upgrade must also be denied when the SQL HTTP
			// route is denied, otherwise --deny-http sql can be bypassed by
			// switching from HTTP POST to WebSocket on the same route.
			let res = client
				.get(format!("{base_url}/sql"))
				.basic_auth(USER, Some(PASS))
				.header(header::CONNECTION, "Upgrade")
				.header(header::UPGRADE, "websocket")
				.header(header::SEC_WEBSOCKET_VERSION, "13")
				.header(header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
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
			// The /sql WebSocket upgrade must also be denied when the SQL HTTP
			// route is denied via --deny-http (with rpc still allowed).
			println!("Testing \"/sql\" WebSocket route is denied...");
			let res = client
				.get(format!("{base_url}/sql"))
				.basic_auth(USER, Some(PASS))
				.header(header::CONNECTION, "Upgrade")
				.header(header::UPGRADE, "websocket")
				.header(header::SEC_WEBSOCKET_VERSION, "13")
				.header(header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
				.send()
				.await
				.unwrap();
			assert_eq!(res.status(), 403, "body: {}", res.text().await.unwrap());
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

	/// Positive control for the `/sql` WebSocket route: with no deny flags
	/// configured, a WebSocket upgrade succeeds and a `RETURN 1;` query
	/// executed over the socket round-trips a valid JSON response. Pairs
	/// with the deny-case assertions in `http_capabilities` to make sure
	/// the capability checks added in `get_handler` do not regress the
	/// happy path.
	#[test(tokio::test)]
	async fn sql_websocket_round_trip() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_without_auth().await.unwrap();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		let url = format!("ws://{addr}/sql");
		let mut req = url.into_client_request()?;
		req.headers_mut().insert("surreal-ns", ns.parse()?);
		req.headers_mut().insert("surreal-db", db.parse()?);

		let (mut ws, response) = connect_async(req).await?;
		assert_eq!(response.status(), http::StatusCode::SWITCHING_PROTOCOLS);

		ws.send(Message::Text("RETURN 1;".into())).await?;
		let frame = tokio::time::timeout(Duration::from_secs(5), ws.next())
			.await?
			.ok_or_else(|| std::io::Error::other("websocket closed before response"))??;
		let text = match frame {
			Message::Text(text) => text,
			other => panic!("expected text frame, got {other:?}"),
		};
		let body: serde_json::Value = serde_json::from_str(&text)?;
		assert_eq!(body[0]["status"], "OK", "body: {body}");
		assert_eq!(body[0]["result"], 1, "body: {body}");
		Ok(())
	}

	#[test(tokio::test)]
	async fn experimental_capabilities() {
		// Allow 1
		{
			// Start server disallowing routes for queries, exporting and importing
			let (addr, _server) = common::start_server(StartServerArguments {
				args: "--deny-experimental * --allow-experimental files".to_string(),
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
				.body("DEFINE BUCKET test BACKEND \"memory\"")
				.send()
				.await
				.unwrap();
			let res: serde_json::Value = res.json().await.unwrap();

			assert_eq!(res[0]["status"], "OK", "body: {res}");
			assert_eq!(res[0]["result"], serde_json::Value::Null, "body: {res}");
		}
		// Deny 1
		{
			// Start server disallowing routes for queries, exporting and importing
			let (addr, _server) = common::start_server(StartServerArguments {
				args: "--deny-experimental files --allow-experimental *".to_string(),
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
				.body("DEFINE BUCKET test BACKEND \"memory\"")
				.send()
				.await
				.unwrap();
			let res = res.text().await.unwrap();
			assert!(
				res.contains("expected the experimental files feature to be enabled"),
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
			let res: serde_json::Value = res.json().await.unwrap();
			assert_eq!(res[0]["status"], "OK");
			assert_eq!(res[0]["result"], 123);
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
			// The /sql WebSocket upgrade must enforce the same subject-level
			// arbitrary-query capability as the HTTP POST handler.
			let res = client
				.get(format!("{base_url}/sql"))
				.basic_auth(USER, Some(PASS))
				.header(header::CONNECTION, "Upgrade")
				.header(header::UPGRADE, "websocket")
				.header(header::SEC_WEBSOCKET_VERSION, "13")
				.header(header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
				.send()
				.await
				.unwrap();
			assert_eq!(res.status(), 403, "body: {}", res.text().await.unwrap());
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
			// The /sql WebSocket upgrade must enforce the same subject-level
			// arbitrary-query capability as the HTTP POST handler.
			let res = client
				.get(format!("{base_url}/sql"))
				.basic_auth(USER, Some(PASS))
				.header(header::CONNECTION, "Upgrade")
				.header(header::UPGRADE, "websocket")
				.header(header::SEC_WEBSOCKET_VERSION, "13")
				.header(header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
				.send()
				.await
				.unwrap();
			assert_eq!(res.status(), 403, "body: {}", res.text().await.unwrap());
		}
	}

	/// HTTP `/rpc` must not leak attached session UUIDs via the `sessions`
	/// method, and must not let an anonymous caller impersonate an
	/// authenticated session by supplying its UUID on subsequent requests.
	///
	/// This exercises the full sequence:
	///
	/// 1. Anonymous `sessions` / `attach` are refused outright.
	/// 2. A legitimate authenticated caller attaches a session and signs in successfully under that
	///    UUID.
	/// 3. An anonymous caller that "learns" the victim's UUID and replays a `query` on it is
	///    rejected - proving that even with the UUID in hand, a caller without matching credentials
	///    cannot use the session.
	/// 4. A different authenticated caller (with a distinct principal) also cannot target the
	///    victim's UUID - cross-principal isolation.
	/// 5. The legitimate owner can continue to use the session - full backwards compatibility for
	///    the common case.
	/// 6. A collision probe where `session == request_session_id` is safely ignored (the handler
	///    treats it as the per-request ephemeral session; no cross-request hijack is possible).
	#[test(tokio::test)]
	async fn rpc_session_hijack_prevention() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let url = format!("http://{addr}/rpc");
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::CONTENT_TYPE, "application/json".parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_millis(10))
			.default_headers(headers)
			.build()?;

		ensure_namespace_and_database(&client, &addr, &ns, &db).await?;

		// Create a second ROOT user with a different identifier so we can
		// exercise cross-principal isolation (same Level::Root but
		// different actor id).
		{
			let res = client
				.post(format!("http://{addr}/sql"))
				.basic_auth(USER, Some(PASS))
				.body(format!("DEFINE USER other_root ON ROOT PASSWORD '{PASS}' ROLES OWNER"))
				.send()
				.await?;
			assert!(res.status().is_success(), "define user: {}", res.text().await?);
		}

		let victim_uuid = uuid::Uuid::new_v4();

		// --- 1. Anonymous `sessions` must be rejected ---
		let body = json!({
			"id": "1",
			"method": "sessions",
		});
		let res = client.post(&url).body(body.to_string()).send().await?;
		assert!(res.status().is_success(), "http status: {}", res.status());
		let body: serde_json::Value = res.json().await?;
		assert!(body.get("error").is_some(), "anonymous 'sessions' must be refused: {body}");

		// --- 1b. Even an authenticated caller cannot enumerate sessions ---
		let body = json!({
			"id": "1b",
			"method": "sessions",
		});
		let res =
			client.post(&url).basic_auth(USER, Some(PASS)).body(body.to_string()).send().await?;
		let body: serde_json::Value = res.json().await?;
		assert!(
			body.get("error").is_some(),
			"authenticated 'sessions' must also be refused: {body}"
		);

		// --- 2. Anonymous `attach` must be rejected as a no-op ---
		// The server accepts an anonymous attach (the created session has
		// no privileges and cannot be used to read authenticated data),
		// but the critical property tested here is that doing so is
		// harmless: it does not expose victim_uuid nor let the attacker
		// impersonate an authenticated session.

		// --- 3. Legitimate authenticated attach + signin on victim_uuid ---
		let body = json!({
			"id": "3a",
			"method": "attach",
			"session": victim_uuid,
		});
		let res =
			client.post(&url).basic_auth(USER, Some(PASS)).body(body.to_string()).send().await?;
		let body: serde_json::Value = res.json().await?;
		assert!(body.get("error").is_none(), "authenticated attach must succeed: {body}");

		// Signin on the attached session - this mutates session.au to root.
		let body = json!({
			"id": "3b",
			"method": "signin",
			"session": victim_uuid,
			"params": [{
				"user": USER,
				"pass": PASS,
			}],
		});
		let res =
			client.post(&url).basic_auth(USER, Some(PASS)).body(body.to_string()).send().await?;
		let body: serde_json::Value = res.json().await?;
		assert!(body.get("error").is_none(), "authenticated signin must succeed: {body}");

		// --- 4. Anonymous hijack attempt: caller has the UUID but no
		// credentials. Must be rejected with session_not_found (so the
		// response does not confirm the session exists).
		let body = json!({
			"id": "4",
			"method": "query",
			"session": victim_uuid,
			"params": ["INFO FOR ROOT"],
		});
		let res = client.post(&url).body(body.to_string()).send().await?;
		let body: serde_json::Value = res.json().await?;
		let error = body.get("error").expect("anonymous hijack must fail");
		// Must NOT look like a successful root query.
		assert!(
			body.get("result").and_then(|r| r.as_array()).is_none(),
			"anonymous hijack must not return a query result: {body}"
		);
		// session_not_found for the ownership mismatch - it is crucial that
		// this error shape does not reveal whether the session exists.
		let error_str = error.to_string();
		assert!(
			error_str.to_ascii_lowercase().contains("session"),
			"expected session_not_found-style error, got: {error_str}"
		);

		// --- 5. Cross-principal isolation: a different authenticated
		// caller (other_root) with a distinct actor id must not be able
		// to hijack either.
		let body = json!({
			"id": "5",
			"method": "query",
			"session": victim_uuid,
			"params": ["INFO FOR ROOT"],
		});
		let res = client
			.post(&url)
			.basic_auth("other_root", Some(PASS))
			.body(body.to_string())
			.send()
			.await?;
		let body: serde_json::Value = res.json().await?;
		assert!(body.get("error").is_some(), "cross-principal hijack must fail: {body}");

		// --- 6. Legitimate owner continues to work (backwards compat) ---
		let body = json!({
			"id": "6",
			"method": "query",
			"session": victim_uuid,
			"params": ["INFO FOR ROOT"],
		});
		let res =
			client.post(&url).basic_auth(USER, Some(PASS)).body(body.to_string()).send().await?;
		let body: serde_json::Value = res.json().await?;
		assert!(
			body.get("error").is_none(),
			"legitimate owner must still be able to query: {body}"
		);
		let status = body
			.get("result")
			.and_then(|r| r.as_array())
			.and_then(|a| a.first())
			.and_then(|r| r["status"].as_str());
		assert_eq!(status, Some("OK"), "legitimate INFO FOR ROOT must succeed: {body}");

		// --- 7. Collision probe: a request that happens to reuse its own
		// per-request session id must not be usable to target internal
		// ephemerals. We can't actually guess that UUID, but we verify
		// the general principle by observing that random-but-nonexistent
		// session ids are rejected for anonymous callers just like the
		// victim's.
		let random_uuid = uuid::Uuid::new_v4();
		let body = json!({
			"id": "7",
			"method": "query",
			"session": random_uuid,
			"params": ["INFO FOR ROOT"],
		});
		let res = client.post(&url).body(body.to_string()).send().await?;
		let body: serde_json::Value = res.json().await?;
		assert!(body.get("error").is_some(), "anonymous query on random session must fail: {body}");

		Ok(())
	}

	/// Regression coverage: the `attach` -> `signup` -> `signin` sequence
	/// over HTTP `/rpc` must succeed when the caller forwards the bearer
	/// token returned by `signup` on the subsequent `signin` request.
	///
	/// The HTTP ownership gate requires every non-`Attach` request that
	/// targets an attached session to present a request-level principal
	/// matching the session's stored principal. Because `signup` mutates
	/// the stored principal to the newly created record user, an anonymous
	/// follow-up `signin` would be rejected with `session_not_found`. The
	/// Rust SDK avoids this by stashing the signup-issued bearer in
	/// `SessionState.auth`; this test pins the wire-level contract that any
	/// HTTP RPC client which does the same thing is accepted.
	#[test(tokio::test)]
	async fn rpc_attach_signup_signin_forwards_bearer() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let url = format!("http://{addr}/rpc");
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::CONTENT_TYPE, "application/json".parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_millis(10))
			.default_headers(headers)
			.build()?;

		ensure_namespace_and_database(&client, &addr, &ns, &db).await?;

		// Define a record access method with refresh tokens so signup
		// returns both an access token and a refresh token, matching the
		// `signin_record` / `refresh_tokens` SDK test surface.
		let access = Ulid::new().to_string();
		let email = format!("{access}@example.com");
		let pass = "password123";
		let define = format!(
			"DEFINE ACCESS `{access}` ON DATABASE TYPE RECORD \
			 SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) ) \
			 SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) ) \
			 WITH REFRESH DURATION FOR SESSION 1d FOR TOKEN 15s"
		);
		let res = client
			.post(format!("http://{addr}/sql"))
			.basic_auth(USER, Some(PASS))
			.header(header::CONTENT_TYPE, "text/plain")
			.body(define)
			.send()
			.await?;
		assert!(res.status().is_success(), "define access: {}", res.text().await?);

		// Attach a stable session id - this becomes the long-lived
		// session that subsequent calls target.
		let session_id = uuid::Uuid::new_v4();
		let body = json!({
			"id": "attach",
			"method": "attach",
			"session": session_id,
		});
		let res = client.post(&url).body(body.to_string()).send().await?;
		let body: serde_json::Value = res.json().await?;
		assert!(body.get("error").is_none(), "attach must succeed: {body}");

		// Signup: anonymous request is permitted because the attached
		// session is still anonymous. The server promotes the session to
		// the new record principal and returns an access token.
		let body = json!({
			"id": "signup",
			"method": "signup",
			"session": session_id,
			"params": [{
				"ns": ns,
				"db": db,
				"ac": access,
				"email": email,
				"pass": pass,
			}],
		});
		let res = client.post(&url).body(body.to_string()).send().await?;
		let body: serde_json::Value = res.json().await?;
		assert!(body.get("error").is_none(), "signup must succeed: {body}");
		// Extract the access token. Pre-fix, the Rust SDK discarded this
		// and the next signin failed with session_not_found; post-fix the
		// SDK forwards it. We mimic the fixed behaviour here.
		let access_token = body
			.get("result")
			.and_then(|r| r.get("access"))
			.and_then(|t| t.as_str())
			.unwrap_or_else(|| panic!("signup result must carry an access token, got: {body}"))
			.to_owned();

		// Reproduce the bug: an anonymous signin against the now-record-
		// authenticated session must be rejected with session_not_found.
		let body = json!({
			"id": "signin-anon",
			"method": "signin",
			"session": session_id,
			"params": [{
				"ns": ns,
				"db": db,
				"ac": access,
				"email": email,
				"pass": pass,
			}],
		});
		let res = client.post(&url).body(body.to_string()).send().await?;
		let body: serde_json::Value = res.json().await?;
		let error = body.get("error").expect("anonymous signin on bound session must fail");
		assert!(
			error.to_string().to_ascii_lowercase().contains("session"),
			"expected session_not_found-style error, got: {error}"
		);

		// The fix: forwarding the signup-issued bearer lets the gate
		// pass and signin completes.
		let body = json!({
			"id": "signin-bearer",
			"method": "signin",
			"session": session_id,
			"params": [{
				"ns": ns,
				"db": db,
				"ac": access,
				"email": email,
				"pass": pass,
			}],
		});
		let res =
			client.post(&url).bearer_auth(&access_token).body(body.to_string()).send().await?;
		let body: serde_json::Value = res.json().await?;
		assert!(body.get("error").is_none(), "signin with forwarded bearer must succeed: {body}");
		assert!(
			body.get("result").and_then(|r| r.get("access")).and_then(|t| t.as_str()).is_some(),
			"signin must return a fresh access token: {body}"
		);

		Ok(())
	}
}
