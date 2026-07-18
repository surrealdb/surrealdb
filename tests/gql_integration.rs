// RUST_LOG=warn cargo test --locked --features storage-mem --workspace --test gql_integration
// -- gql_integration --nocapture
mod common;

#[cfg(feature = "gql")]
mod gql_integration {
	use std::error::Error;
	use std::time::Duration;

	use futures_util::{SinkExt, StreamExt};
	use http::{Method, header};
	use reqwest::Client;
	use serde_json::json;
	use test_log::test;
	use tokio_tungstenite::connect_async;
	use tokio_tungstenite::tungstenite::Message;
	use tokio_tungstenite::tungstenite::client::IntoClientRequest;
	use ulid::Ulid;

	use super::common::{self, Format, PASS, Socket, StartServerArguments, USER};

	/// Deterministic person/knows graph. Only k12 (A->B, since 2021) and
	/// k23 (B->C, since 2022) satisfy `since > 2020`; k31 carries no `since`
	/// property at all, so it also exercises the NONE guard on the edge
	/// predicate.
	const SEED: &str = r#"
		CREATE person:1 SET name = 'A' RETURN NONE;
		CREATE person:2 SET name = 'B' RETURN NONE;
		CREATE person:3 SET name = 'C' RETURN NONE;
		INSERT RELATION INTO knows [
			{ id: knows:k12, in: person:1, out: person:2, since: 2021 },
			{ id: knows:k21, in: person:2, out: person:1, since: 2018 },
			{ id: knows:k23, in: person:2, out: person:3, since: 2022 },
			{ id: knows:k31, in: person:3, out: person:1 }
		] RETURN NONE;
	"#;

	/// Unaliased RETURN items use the verbatim GQL expression text as the
	/// column name, so the result rows have `a.name` / `b.name` keys and
	/// ORDER BY references the items by that same text.
	const MATCH_QUERY: &str = "MATCH (a:person)-[k:knows]->(b:person) WHERE k.since > 2020 RETURN a.name, b.name ORDER BY a.name";

	/// Start a server for GQL tests. GQL is available by default (no longer an
	/// experimental capability), so this passes only the caller's extra flags.
	async fn start_gql_server(extra_args: &str) -> Result<(String, common::Child), Box<dyn Error>> {
		common::start_server(StartServerArguments {
			args: extra_args.to_string(),
			..Default::default()
		})
		.await
	}

	/// Build an HTTP client with the given `surreal-ns`/`surreal-db` headers
	/// and a JSON `Accept` header, mirroring the `/sql` endpoint tests.
	fn http_client(ns: &str, db: &str) -> Result<Client, Box<dyn Error>> {
		let mut headers = reqwest::header::HeaderMap::new();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		Ok(Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?)
	}

	/// Create the namespace and database before seeding: header-scoped `/sql`
	/// requests do not auto-create them, so the definitions must be issued
	/// explicitly at the appropriate auth levels.
	async fn ensure_namespace_and_database(
		addr: &str,
		ns: &str,
		db: &str,
	) -> Result<(), Box<dyn Error>> {
		// Create a separate client without namespace/database headers for ROOT-level operations
		let mut root_headers = reqwest::header::HeaderMap::new();
		root_headers.insert(header::ACCEPT, "application/json".parse()?);
		let root_client = reqwest::Client::builder()
			.connect_timeout(Duration::from_secs(10))
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
			.connect_timeout(Duration::from_secs(10))
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

	/// Seed the person/knows graph through the `/sql` endpoint. The `/sql`
	/// endpoint reports statement failures in the body with HTTP 200, so
	/// every statement's status is asserted to keep seed failures visible.
	async fn seed_via_sql(client: &Client, addr: &str) -> Result<(), Box<dyn Error>> {
		let res = client
			.post(format!("http://{addr}/sql"))
			.basic_auth(USER, Some(PASS))
			.body(SEED)
			.send()
			.await?;
		assert_eq!(res.status(), 200, "body: {}", res.text().await?);
		let body: serde_json::Value = res.json().await?;
		let results = body.as_array().expect("seed response must be an array");
		assert!(!results.is_empty(), "seed response must not be empty: {body}");
		for result in results {
			assert_eq!(result["status"], "OK", "seed statement failed: {body}");
		}
		Ok(())
	}

	#[test(tokio::test)]
	async fn gql_endpoint_happy_path() -> Result<(), Box<dyn Error>> {
		let (addr, _server) = start_gql_server("").await.unwrap();
		let url = &format!("http://{addr}/gql");
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		let client = http_client(&ns, &db)?;

		// Create the namespace and database, then seed records and
		// relations via the SQL endpoint
		ensure_namespace_and_database(&addr, &ns, &db).await?;
		seed_via_sql(&client, &addr).await?;

		// Options method works
		{
			let res = client.request(Method::OPTIONS, url).send().await?;
			assert_eq!(res.status(), 200);
		}

		// A MATCH query over the seeded graph returns one row per binding,
		// with the verbatim GQL expression text as column names
		{
			let res =
				client.post(url).basic_auth(USER, Some(PASS)).body(MATCH_QUERY).send().await?;
			assert_eq!(res.status(), 200, "body: {}", res.text().await?);
			let body: serde_json::Value = res.json().await?;
			assert_eq!(body[0]["status"], "OK", "body: {body}");
			assert_eq!(
				body[0]["result"],
				json!([
					{ "a.name": "A", "b.name": "B" },
					{ "a.name": "B", "b.name": "C" }
				]),
				"body: {body}"
			);
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn gql_available_without_experimental_flag() -> Result<(), Box<dyn Error>> {
		// GQL is on by default: a plain server (no `--allow-experimental`)
		// serves the `/gql` endpoint. It must NOT reject with the old
		// experimental-gate error.
		let (addr, _server) = common::start_server(StartServerArguments::default()).await.unwrap();
		let url = &format!("http://{addr}/gql");
		let client = http_client(&Ulid::new().to_string(), &Ulid::new().to_string())?;

		let res = client.post(url).basic_auth(USER, Some(PASS)).body(MATCH_QUERY).send().await?;
		assert_ne!(res.status(), 403, "GQL should not be gated behind an experimental flag");
		let body = res.text().await?;
		assert!(
			!body.contains("Experimental capability `gql` is not enabled"),
			"GQL was still gated: {body}"
		);

		Ok(())
	}

	#[test(tokio::test)]
	async fn gql_route_can_be_denied() -> Result<(), Box<dyn Error>> {
		let (addr, _server) = start_gql_server("--deny-http gql").await.unwrap();
		let url = &format!("http://{addr}/gql");
		let client = http_client(&Ulid::new().to_string(), &Ulid::new().to_string())?;

		let res = client.post(url).basic_auth(USER, Some(PASS)).body(MATCH_QUERY).send().await?;
		assert_eq!(res.status(), 403);
		let body = res.text().await?;
		assert!(body.contains("The HTTP route 'gql' is forbidden"), "body: {body}");

		Ok(())
	}

	#[test(tokio::test)]
	async fn gql_accept_negotiation() -> Result<(), Box<dyn Error>> {
		let (addr, _server) = start_gql_server("").await.unwrap();
		let url = &format!("http://{addr}/gql");
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		let client = http_client(&ns, &db)?;

		// Create the namespace and database, then seed records and
		// relations via the SQL endpoint
		ensure_namespace_and_database(&addr, &ns, &db).await?;
		seed_via_sql(&client, &addr).await?;

		// Querying with Accept CBOR encoding returns a CBOR body
		{
			let res = client
				.post(url)
				.basic_auth(USER, Some(PASS))
				.header(header::ACCEPT, "application/cbor")
				.body(MATCH_QUERY)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let bytes = res.bytes().await?;
			// The body must be CBOR, not JSON text
			assert_ne!(bytes.first(), Some(&b'['), "body looks like JSON: {bytes:?}");
			// The decoded CBOR must contain the seeded result rows, not
			// just any decodable value. The body carries CBOR semantic
			// tags (e.g. for the `time` duration), so it is traversed as
			// a raw `ciborium::Value` rather than converted to JSON.
			let body: ciborium::Value = ciborium::from_reader(&*bytes).unwrap();
			let results = body.as_array().expect("CBOR body must be an array");
			let first = results[0].as_map().expect("CBOR result must be a map");
			let field = |name: &str| {
				first
					.iter()
					.find(|(k, _)| k.as_text() == Some(name))
					.map(|(_, v)| v)
					.unwrap_or_else(|| panic!("missing `{name}` field: {first:?}"))
			};
			assert_eq!(field("status").as_text(), Some("OK"), "body: {first:?}");
			let rows = field("result").as_array().expect("CBOR rows must be an array");
			let names = rows
				.iter()
				.map(|row| {
					let row = row.as_map().expect("CBOR row must be a map");
					let get = |name: &str| {
						row.iter()
							.find(|(k, _)| k.as_text() == Some(name))
							.and_then(|(_, v)| v.as_text())
							.map(str::to_owned)
					};
					(get("a.name"), get("b.name"))
				})
				.collect::<Vec<_>>();
			assert_eq!(
				names,
				vec![
					(Some("A".to_string()), Some("B".to_string())),
					(Some("B".to_string()), Some("C".to_string())),
				],
				"rows: {rows:?}"
			);
		}

		// Querying with Accept flatbuffers encoding returns the internal
		// serialization, which must also decode to the seeded result rows
		{
			let res = client
				.post(url)
				.basic_auth(USER, Some(PASS))
				.header(header::ACCEPT, surrealdb_core::api::format::FLATBUFFERS)
				.body(MATCH_QUERY)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let bytes = res.bytes().await?;
			let value: surrealdb_types::Value =
				surrealdb_core::rpc::format::flatbuffers::decode(&bytes)
					.expect("flatbuffers GQL response should decode to Value");
			let array = value.into_array().unwrap();
			assert_eq!(array.len(), 1);
			let result = array.into_iter().next().unwrap().into_object().unwrap();
			assert_eq!(
				result.get("status"),
				Some(&surrealdb_types::Value::String("OK".to_string()))
			);
			let rows = result.get("result").cloned().unwrap().into_array().unwrap();
			let names = rows
				.into_iter()
				.map(|row| {
					let row = row.into_object().unwrap();
					(row.get("a.name").cloned().unwrap(), row.get("b.name").cloned().unwrap())
				})
				.collect::<Vec<_>>();
			assert_eq!(
				names,
				vec![
					(
						surrealdb_types::Value::String("A".to_string()),
						surrealdb_types::Value::String("B".to_string())
					),
					(
						surrealdb_types::Value::String("B".to_string()),
						surrealdb_types::Value::String("C".to_string())
					),
				]
			);
		}

		// Querying with an unsupported Accept header returns a 415
		{
			let res = client
				.post(url)
				.basic_auth(USER, Some(PASS))
				.header(header::ACCEPT, "text/plain")
				.body(MATCH_QUERY)
				.send()
				.await?;
			assert_eq!(res.status(), 415);
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn gql_rejects_invalid_utf8() -> Result<(), Box<dyn Error>> {
		let (addr, _server) = start_gql_server("").await.unwrap();
		let url = &format!("http://{addr}/gql");
		let client = http_client(&Ulid::new().to_string(), &Ulid::new().to_string())?;

		let res = client
			.post(url)
			.basic_auth(USER, Some(PASS))
			.body(vec![0xff, 0xfe, 0xfd])
			.send()
			.await?;
		assert_eq!(res.status(), 400);
		let body = res.text().await?;
		assert!(body.contains("Non UTF-8 request body"), "body: {body}");

		Ok(())
	}

	#[test(tokio::test)]
	async fn gql_parse_error_shape() -> Result<(), Box<dyn Error>> {
		let (addr, _server) = start_gql_server("").await.unwrap();
		let url = &format!("http://{addr}/gql");
		let client = http_client(&Ulid::new().to_string(), &Ulid::new().to_string())?;

		let res = client.post(url).basic_auth(USER, Some(PASS)).body("MATCH (").send().await?;
		assert_eq!(res.status(), 400);
		let body = res.text().await?;
		// The rendered GQL parse error must reach the wire
		assert!(body.contains("expected"), "body: {body}");

		Ok(())
	}

	#[test(tokio::test)]
	async fn gql_rpc_method() -> Result<(), Box<dyn Error>> {
		let (addr, _server) = start_gql_server("").await.unwrap();
		// Connect to WebSocket
		let mut socket = Socket::connect(&addr, Some(Format::Json), Format::Json).await?;
		// Authenticate the connection
		socket.send_message_signin(USER, PASS, None, None, None).await?;
		// Specify a namespace and database
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		socket.send_message_use(Some(&ns), Some(&db)).await?;
		// Seed records and relations
		socket.send_message_query(SEED).await?;
		// Send a GQL query with parameters used in WHERE and LIMIT
		let res = socket
			.send_request(
				"gql",
				json!([
					"MATCH (a:person)-[k:knows]->(b:person) WHERE k.since > $min RETURN a.name, b.name ORDER BY a.name LIMIT $lim",
					{ "min": 2020, "lim": 1 }
				]),
			)
			.await?;
		assert!(res["error"].is_null(), "result: {res:?}");
		let result = &res["result"];
		assert_eq!(result[0]["status"], "OK", "result: {res:?}");
		assert_eq!(
			result[0]["result"],
			json!([{ "a.name": "A", "b.name": "B" }]),
			"result: {res:?}"
		);

		Ok(())
	}

	#[test(tokio::test)]
	async fn gql_rpc_available_without_experimental_flag() -> Result<(), Box<dyn Error>> {
		// GQL is on by default: the `gql` RPC method must NOT surface the old
		// not-allowed experimental-gate error on a plain server.
		let (addr, _server) = common::start_server(StartServerArguments::default()).await.unwrap();
		// Connect to WebSocket
		let mut socket = Socket::connect(&addr, Some(Format::Json), Format::Json).await?;
		// Authenticate the connection
		socket.send_message_signin(USER, PASS, None, None, None).await?;
		// Specify a namespace and database
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		socket.send_message_use(Some(&ns), Some(&db)).await?;
		// A valid GQL query must not be refused as an experimental capability.
		let res = socket.send_request("gql", json!([MATCH_QUERY])).await?;
		let err = &res["error"];
		if err.is_object() {
			assert_ne!(err["kind"], "NotAllowed", "GQL RPC was gated: {err}");
			assert!(
				!err["message"]
					.as_str()
					.unwrap_or_default()
					.contains("Experimental capability `gql` is not enabled"),
				"GQL RPC was still gated: {err}"
			);
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn gql_rpc_parse_error_shape() -> Result<(), Box<dyn Error>> {
		let (addr, _server) = start_gql_server("").await.unwrap();
		// Connect to WebSocket
		let mut socket = Socket::connect(&addr, Some(Format::Json), Format::Json).await?;
		// Authenticate the connection
		socket.send_message_signin(USER, PASS, None, None, None).await?;
		// Specify a namespace and database
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		socket.send_message_use(Some(&ns), Some(&db)).await?;
		// A malformed GQL query must fail with the validation error kind,
		// not the not-allowed kind used for the capability gate
		let res = socket.send_request("gql", json!(["MATCH ("])).await?;
		let err = &res["error"];
		assert!(err.is_object(), "result: {res:?}");
		assert_eq!(err["kind"], "Validation", "error: {err}");
		assert!(err["message"].as_str().unwrap_or_default().contains("expected"), "error: {err}");

		Ok(())
	}

	#[test(tokio::test)]
	async fn rpc_gql_unknown_when_method_misspelled() -> Result<(), Box<dyn Error>> {
		let (addr, _server) = start_gql_server("").await.unwrap();
		// Connect to WebSocket
		let mut socket = Socket::connect(&addr, Some(Format::Json), Format::Json).await?;
		// Authenticate the connection
		socket.send_message_signin(USER, PASS, None, None, None).await?;
		// A misspelled method name must surface the method-not-found error
		let res = socket.send_request("gqll", json!([])).await?;
		let err = &res["error"];
		assert!(err.is_object(), "result: {res:?}");
		assert!(err.to_string().contains("Method not found"), "error: {err}");

		Ok(())
	}

	/// Send a raw RPC envelope over the WebSocket and wait for the response
	/// with the matching request id. The common `Socket` helper builds its
	/// own envelopes and cannot attach the top-level `txn` field, so the
	/// transaction-interop test drives the socket directly. The exchange is
	/// bounded by a timeout, like the common `Socket` helpers, so a missing
	/// response fails the test instead of hanging it.
	async fn raw_rpc_request(
		ws: &mut tokio_tungstenite::WebSocketStream<
			tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
		>,
		msg: serde_json::Value,
	) -> Result<serde_json::Value, Box<dyn Error>> {
		tokio::time::timeout(Duration::from_secs(10), async {
			let id = msg["id"].clone();
			ws.send(Message::Text(msg.to_string().into())).await?;
			loop {
				let Some(frame) = ws.next().await else {
					return Err("websocket closed unexpectedly".into());
				};
				let Message::Text(text) = frame? else {
					continue;
				};
				let res: serde_json::Value = serde_json::from_str(&text)?;
				if res["id"] == id {
					return Ok(res);
				}
			}
		})
		.await
		.map_err(|_| "timed-out waiting for the RPC response")?
	}

	#[test(tokio::test)]
	async fn gql_rpc_txn_interop() -> Result<(), Box<dyn Error>> {
		let (addr, _server) = start_gql_server("").await.unwrap();
		// Connect a raw WebSocket so the request envelope can carry the
		// top-level `txn` field
		let url = format!("ws://{addr}/rpc");
		let mut req = url.into_client_request()?;
		req.headers_mut().insert("Sec-WebSocket-Protocol", "json".parse()?);
		let (mut ws, _) = connect_async(req).await?;
		// Authenticate the connection
		let res = raw_rpc_request(
			&mut ws,
			json!({ "id": 1, "method": "signin", "params": [{ "user": USER, "pass": PASS }] }),
		)
		.await?;
		assert!(res["error"].is_null(), "signin: {res:?}");
		// Specify a namespace and database
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		let res = raw_rpc_request(&mut ws, json!({ "id": 2, "method": "use", "params": [ns, db] }))
			.await?;
		assert!(res["error"].is_null(), "use: {res:?}");
		// Seed a record outside the transaction
		let res = raw_rpc_request(
			&mut ws,
			json!({ "id": 3, "method": "query", "params": ["CREATE person:1 SET name = 'A' RETURN NONE"] }),
		)
		.await?;
		assert!(res["error"].is_null(), "query: {res:?}");
		// Begin a transaction; the result is the transaction id
		let res = raw_rpc_request(&mut ws, json!({ "id": 4, "method": "begin" })).await?;
		assert!(res["error"].is_null(), "begin: {res:?}");
		let txn = res["result"].as_str().expect("begin must return a transaction id").to_string();
		// A GQL read inside the transaction proves the txn id is accepted
		let res = raw_rpc_request(
			&mut ws,
			json!({
				"id": 5,
				"method": "gql",
				"params": ["MATCH (n:person) RETURN n.name"],
				"txn": txn,
			}),
		)
		.await?;
		assert!(res["error"].is_null(), "gql in txn: {res:?}");
		assert_eq!(res["result"][0]["status"], "OK", "gql in txn: {res:?}");
		assert_eq!(res["result"][0]["result"], json!([{ "n.name": "A" }]), "gql in txn: {res:?}");
		// Commit the transaction
		let res = raw_rpc_request(&mut ws, json!({ "id": 6, "method": "commit", "params": [txn] }))
			.await?;
		assert!(res["error"].is_null(), "commit: {res:?}");

		Ok(())
	}
}
