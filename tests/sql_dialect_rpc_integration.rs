// RUST_LOG=warn cargo test --locked --features storage-mem,postgres,mysql --test
// sql_dialect_rpc_integration -- --nocapture
//
// `postgres` and `mysql` are not implied by the root package's default features. Without them the
// modules below are compiled away and the run reports "running 0 tests".
mod common;

#[cfg(any(feature = "postgres", feature = "mysql"))]
mod sql_dialect_rpc_integration {
	use std::error::Error;
	use std::time::Duration;

	use serde_json::{Value, json};
	use test_log::test;
	use ulid::Ulid;

	use super::common::{self, Format, PASS, Socket, StartServerArguments, USER};

	/// Each dialect method, its capability, and a parameterised lookup.
	#[allow(unused_mut, clippy::vec_init_then_push)]
	fn dialects() -> Vec<(&'static str, &'static str, &'static str)> {
		let mut dialects = Vec::new();
		#[cfg(feature = "postgres")]
		dialects.push(("pgsql", "postgres", "SELECT name FROM person WHERE name = $1"));
		#[cfg(feature = "mysql")]
		dialects.push(("mysql", "mysql", "SELECT name FROM person WHERE name = ?"));
		dialects
	}

	async fn start(args: &str) -> Result<(String, common::Child), Box<dyn Error>> {
		common::start_server(StartServerArguments {
			args: args.to_string(),
			..Default::default()
		})
		.await
	}

	async fn signed_in_socket(addr: &str) -> Result<(Socket, String, String), Box<dyn Error>> {
		let mut socket = Socket::connect(addr, Some(Format::Json), Format::Json).await?;
		socket.send_message_signin(USER, PASS, None, None, None).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		socket.send_message_use(Some(&ns), Some(&db)).await?;
		Ok((socket, ns, db))
	}

	#[test(tokio::test)]
	async fn dialect_methods_run_over_websocket() -> Result<(), Box<dyn Error>> {
		let (addr, _server) = start("").await?;
		let (mut socket, _, _) = signed_in_socket(&addr).await?;
		socket.send_message_query("CREATE person:tobie SET name = 'Tobie' RETURN NONE").await?;
		for (method, _, lookup) in dialects() {
			let res = socket.send_request(method, json!([lookup, { "_1": "Tobie" }])).await?;
			assert!(res["error"].is_null(), "{method}: {res:?}");
			let statement = &res["result"]["statements"][0];
			assert_eq!(statement["status"], "OK", "{method}: {res:?}");
			assert_eq!(statement["rows"], json!([{ "name": "Tobie" }]), "{method}: {res:?}");
			assert_eq!(res["result"]["transaction"]["status"], "idle", "{method}: {res:?}");
			let res = socket.send_request(method, json!(["SELEC (("])).await?;
			assert!(res["error"].is_null(), "{method}: {res:?}");
			let statement = &res["result"]["statements"][0];
			assert_eq!(statement["status"], "ERR", "{method}: {res:?}");
			assert!(statement["error"]["sqlstate"].is_string(), "{method}: {res:?}");
		}
		Ok(())
	}

	#[test(tokio::test)]
	async fn dialect_transactions_persist_over_websocket() -> Result<(), Box<dyn Error>> {
		let (addr, _server) = start("").await?;
		let (socket, _, _) = signed_in_socket(&addr).await?;
		for (method, _, _) in dialects() {
			let res = socket.send_request(method, json!(["BEGIN"])).await?;
			assert_eq!(res["result"]["statements"][0]["status"], "OK", "{method}: {res:?}");
			assert_eq!(res["result"]["transaction"]["status"], "active", "{method}: {res:?}");
			let res = socket.send_request(method, json!(["SELECT 1"])).await?;
			assert_eq!(res["result"]["transaction"]["status"], "active", "{method}: {res:?}");
			let res = socket.send_request(method, json!(["SELEC (("])).await?;
			let expected = if method == "pgsql" {
				"failed"
			} else {
				"active"
			};
			assert_eq!(res["result"]["transaction"]["status"], expected, "{method}: {res:?}");
			if method == "pgsql" {
				let res = socket.send_request(method, json!(["ROLLBACK"])).await?;
				assert_eq!(res["result"]["transaction"]["status"], "idle", "{method}: {res:?}");
				socket.send_request(method, json!(["BEGIN"])).await?;
				let res = socket
					.send_request(method, json!(["SELECT * FROM missing_table; SELECT 1"]))
					.await?;
				let statements = &res["result"]["statements"];
				assert_eq!(statements[0]["status"], "ERR", "{method}: {res:?}");
				assert_eq!(statements[1]["error"]["sqlstate"], "25P02", "{method}: {res:?}");
				let res = socket.send_request(method, json!(["SELECT 1"])).await?;
				assert_eq!(
					res["result"]["statements"][0]["error"]["sqlstate"], "25P02",
					"{method}: {res:?}"
				);
				let res = socket.send_request(method, json!(["BEGIN"])).await?;
				assert_eq!(
					res["result"]["statements"][0]["error"]["sqlstate"], "25P02",
					"a nested BEGIN in a failed transaction is refused: {res:?}"
				);
				socket.send_request(method, json!(["ROLLBACK"])).await?;
				socket.send_request(method, json!(["BEGIN"])).await?;
				let res = socket.send_request(method, json!(["SHOW no_such_setting"])).await?;
				assert_eq!(res["result"]["statements"][0]["status"], "ERR", "{method}: {res:?}");
				assert_eq!(
					res["result"]["transaction"]["status"], "failed",
					"a failing setting statement fails the transaction: {res:?}"
				);
			}
			let res = socket.send_request(method, json!(["ROLLBACK"])).await?;
			assert_eq!(res["result"]["transaction"]["status"], "idle", "{method}: {res:?}");
		}
		Ok(())
	}

	#[test(tokio::test)]
	async fn dialect_methods_run_over_http() -> Result<(), Box<dyn Error>> {
		let (addr, _server) = start("").await?;
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.timeout(Duration::from_secs(30))
			.build()?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		let rpc = |body: Value| {
			client
				.post(format!("http://{addr}/rpc"))
				.basic_auth(USER, Some(PASS))
				.header("surreal-ns", &ns)
				.header("surreal-db", &db)
				.header("accept", "application/json")
				.json(&body)
				.send()
		};
		let sql = format!("DEFINE NAMESPACE `{ns}`; USE NS `{ns}`; DEFINE DATABASE `{db}`;");
		let res = client
			.post(format!("http://{addr}/sql"))
			.basic_auth(USER, Some(PASS))
			.header("accept", "application/json")
			.body(sql)
			.send()
			.await?;
		assert_eq!(res.status(), 200);
		let res: Value = rpc(json!({
			"id": 1, "method": "query",
			"params": ["CREATE person:tobie SET name = 'Tobie' RETURN NONE"]
		}))
		.await?
		.json()
		.await?;
		assert!(res["error"].is_null(), "seed: {res:?}");
		for (method, _, lookup) in dialects() {
			let res: Value =
				rpc(json!({ "id": 2, "method": method, "params": [lookup, { "_1": "Tobie" }] }))
					.await?
					.json()
					.await?;
			assert!(res["error"].is_null(), "{method}: {res:?}");
			assert_eq!(
				res["result"]["statements"][0]["rows"],
				json!([{ "name": "Tobie" }]),
				"{method}: {res:?}"
			);
			assert_eq!(res["result"]["statements"][0]["command"]["rows_affected"], 1);
			let res: Value = rpc(json!({
				"id": 3, "method": method,
				"params": [[lookup, "SELECT 1"], { "_1": "Tobie" }, { "atomic": true }]
			}))
			.await?
			.json()
			.await?;
			assert!(res["error"].is_null(), "{method}: {res:?}");
			assert_eq!(res["result"]["transaction"]["committed"], true, "{method}: {res:?}");
			assert_eq!(res["result"]["statements"].as_array().map(Vec::len), Some(2));
			let res: Value = rpc(json!({
				"id": 4, "method": method,
				"params": [["SELECT 1", "SELEC (("], null, { "atomic": true }]
			}))
			.await?
			.json()
			.await?;
			assert_eq!(res["result"]["transaction"]["committed"], false, "{method}: {res:?}");
		}
		Ok(())
	}

	#[test(tokio::test)]
	async fn denied_capability_refuses_the_method() -> Result<(), Box<dyn Error>> {
		let (addr, _server) = start("--deny-http postgres,mysql").await?;
		let (socket, _, _) = signed_in_socket(&addr).await?;
		for (method, capability, _) in dialects() {
			let res = socket.send_request(method, json!(["SELECT 1"])).await?;
			assert_eq!(res["error"]["kind"], "NotAllowed", "{method}: {res:?}");
			let message = res["error"]["message"].as_str().unwrap_or_default();
			assert!(message.contains(capability), "{method}: {res:?}");
		}
		let res = socket.send_request("query", json!(["RETURN 1"])).await?;
		assert!(res["error"].is_null(), "SurrealQL stays available: {res:?}");
		Ok(())
	}

	#[test(tokio::test)]
	async fn denied_method_name_refuses_the_method() -> Result<(), Box<dyn Error>> {
		let (addr, _server) = start("--deny-rpc pgsql,mysql").await?;
		let (socket, _, _) = signed_in_socket(&addr).await?;
		for (method, _, _) in dialects() {
			let res = socket.send_request(method, json!(["SELECT 1"])).await?;
			assert_eq!(res["error"]["kind"], "NotAllowed", "{method}: {res:?}");
		}
		Ok(())
	}
}
