// RUST_LOG=warn cargo test --locked --features storage-mem --workspace --test pg_integration
// -- pg_integration --nocapture
mod common;

#[cfg(feature = "postgres")]
mod pg_integration {
	use std::error::Error;
	use std::time::Duration;

	use rand::Rng;
	use test_log::test;
	use tokio::io::{AsyncReadExt, AsyncWriteExt};
	use tokio::net::TcpStream;
	use tokio_postgres::error::SqlState;
	use tokio_postgres::{Client, NoTls, SimpleQueryMessage, SimpleQueryRow};

	use super::common::{self, PASS, StartServerArguments, USER};

	/// The Postgres SSLRequest packet (length 8, magic code 80877103).
	const SSL_REQUEST: [u8; 8] = {
		let len = 8i32.to_be_bytes();
		let code = 80877103i32.to_be_bytes();
		[len[0], len[1], len[2], len[3], code[0], code[1], code[2], code[3]]
	};

	/// Send an SSLRequest to the port and return the server's single-byte reply
	/// ('S' = TLS accepted, 'N' = declined), retrying while the listener binds.
	async fn ssl_request_reply(port: u16) -> u8 {
		for _ in 0..50 {
			if let Ok(mut sock) = TcpStream::connect(("127.0.0.1", port)).await
				&& sock.write_all(&SSL_REQUEST).await.is_ok()
			{
				let mut byte = [0u8; 1];
				if sock.read_exact(&mut byte).await.is_ok() {
					return byte[0];
				}
			}
			tokio::time::sleep(Duration::from_millis(100)).await;
		}
		panic!("no SSLRequest reply from port {port}");
	}

	/// Build a v3.0 StartupMessage carrying the given `key\0value` parameters.
	fn startup_packet(params: &[(&str, &str)]) -> Vec<u8> {
		let mut body = Vec::new();
		body.extend_from_slice(&196608i32.to_be_bytes()); // protocol 3.0
		for (key, value) in params {
			body.extend_from_slice(key.as_bytes());
			body.push(0);
			body.extend_from_slice(value.as_bytes());
			body.push(0);
		}
		body.push(0); // parameter-list terminator
		let mut packet = ((body.len() + 4) as i32).to_be_bytes().to_vec();
		packet.extend_from_slice(&body);
		packet
	}

	/// Read one backend message, returning its tag byte and payload.
	async fn read_backend_message(sock: &mut TcpStream) -> (u8, Vec<u8>) {
		let mut tag = [0u8; 1];
		sock.read_exact(&mut tag).await.expect("read message tag");
		let mut len = [0u8; 4];
		sock.read_exact(&mut len).await.expect("read message length");
		let len = i32::from_be_bytes(len) as usize - 4;
		let mut payload = vec![0u8; len];
		sock.read_exact(&mut payload).await.expect("read message payload");
		(tag[0], payload)
	}

	/// Send a StartupMessage for the root user and return the first backend
	/// message that is not a "starting up" rejection, retrying while the ready
	/// gate flips.
	async fn startup_auth_request(port: u16) -> (u8, Vec<u8>) {
		for _ in 0..50 {
			let Ok(mut sock) = TcpStream::connect(("127.0.0.1", port)).await else {
				tokio::time::sleep(Duration::from_millis(100)).await;
				continue;
			};
			let packet = startup_packet(&[("user", USER), ("database", "testns/testdb")]);
			if sock.write_all(&packet).await.is_err() {
				tokio::time::sleep(Duration::from_millis(100)).await;
				continue;
			}
			let (tag, payload) = read_backend_message(&mut sock).await;
			// An ErrorResponse here is the pre-ready "cannot connect now"; retry.
			if tag == b'E' {
				tokio::time::sleep(Duration::from_millis(100)).await;
				continue;
			}
			return (tag, payload);
		}
		panic!("no auth request from port {port}");
	}

	async fn start_pg_server(auth: bool) -> Result<(u16, common::Child), Box<dyn Error>> {
		start_pg_server_with_args(auth, "").await
	}

	async fn start_pg_server_with_args(
		auth: bool,
		extra: &str,
	) -> Result<(u16, common::Child), Box<dyn Error>> {
		let port: u16 = rand::rng().random_range(24000..33000);
		let (_, child) = common::start_server(StartServerArguments {
			auth,
			args: format!("--postgres-bind 127.0.0.1:{port} {extra}"),
			..Default::default()
		})
		.await?;
		Ok((port, child))
	}

	async fn connect(port: u16, config: &str) -> Result<Client, tokio_postgres::Error> {
		let conn = format!("host=127.0.0.1 port={port} {config}");
		// The credential initialisation that flips the server's ready gate
		// runs after the listeners bind, so early connections can see a
		// clean "starting up" rejection; retry those.
		let mut attempt = 0;
		loop {
			match tokio_postgres::connect(&conn, NoTls).await {
				Ok((client, connection)) => {
					tokio::spawn(connection);
					return Ok(client);
				}
				Err(err) if attempt < 20 && err.code() == Some(&SqlState::CANNOT_CONNECT_NOW) => {
					attempt += 1;
					tokio::time::sleep(Duration::from_millis(100)).await;
				}
				Err(err) => return Err(err),
			}
		}
	}

	async fn connect_root(port: u16) -> Result<Client, tokio_postgres::Error> {
		connect(port, &format!("user={USER} password={PASS} dbname=testns/testdb")).await
	}

	fn rows(messages: Vec<SimpleQueryMessage>) -> Vec<SimpleQueryRow> {
		messages
			.into_iter()
			.filter_map(|m| match m {
				SimpleQueryMessage::Row(row) => Some(row),
				_ => None,
			})
			.collect()
	}

	/// The server must challenge a user that has SCRAM verifier material (the
	/// bootstrap root user does) with SASL/SCRAM-SHA-256, not cleartext — so a
	/// regression to cleartext for such users is caught even though
	/// tokio-postgres negotiates either transparently.
	#[test(tokio::test)]
	async fn root_user_is_challenged_with_scram() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let (tag, payload) = startup_auth_request(port).await;
		// AuthenticationSASL: tag 'R', i32 auth-type 10, then NUL-terminated
		// mechanism names ending with an empty string.
		assert_eq!(tag, b'R', "expected an authentication request");
		let auth_type = i32::from_be_bytes(payload[..4].try_into().unwrap());
		assert_eq!(auth_type, 10, "expected AuthenticationSASL (not cleartext)");
		let mechanisms = String::from_utf8_lossy(&payload[4..]);
		assert!(
			mechanisms.contains("SCRAM-SHA-256"),
			"SCRAM-SHA-256 must be offered, got {mechanisms:?}"
		);
		Ok(())
	}

	#[test(tokio::test)]
	async fn simple_query_returns_typed_columns() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		client.simple_query("CREATE person:tobie SET name = 'Tobie', age = 42 RETURN NONE").await?;
		let rows = rows(client.simple_query("SELECT * FROM person").await?);
		assert_eq!(rows.len(), 1);
		let row = &rows[0];
		let names: Vec<&str> = row.columns().iter().map(|c| c.name()).collect();
		assert_eq!(names, vec!["age", "id", "name"]);
		assert_eq!(row.get("age"), Some("42"));
		assert_eq!(row.get("id"), Some("person:tobie"));
		assert_eq!(row.get("name"), Some("Tobie"));
		Ok(())
	}

	#[test(tokio::test)]
	async fn values_render_in_postgres_text_formats() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		let rows = rows(
			client
				.simple_query(
					r#"RETURN {
						b: true,
						f: 1.5,
						i: 123,
						s: 'hello',
						dt: d'2024-01-15T12:34:56.123456789Z',
						du: 1d2h,
						arr: [1, 2],
						obj: { a: 1 }
					}"#,
				)
				.await?,
		);
		assert_eq!(rows.len(), 1);
		let row = &rows[0];
		assert_eq!(row.get("b"), Some("t"));
		assert_eq!(row.get("f"), Some("1.5"));
		assert_eq!(row.get("i"), Some("123"));
		assert_eq!(row.get("s"), Some("hello"));
		// Nanoseconds truncate to Postgres microsecond precision.
		assert_eq!(row.get("dt"), Some("2024-01-15 12:34:56.123456+00"));
		assert_eq!(row.get("du"), Some("1 day 02:00:00"));
		assert_eq!(row.get("arr"), Some("[1,2]"));
		assert_eq!(row.get("obj"), Some("{\"a\":1}"));
		Ok(())
	}

	#[test(tokio::test)]
	async fn missing_fields_are_null_cells() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		let rows = rows(client.simple_query("RETURN [{ a: 1 }, { b: 2 }]").await?);
		assert_eq!(rows.len(), 2);
		assert_eq!(rows[0].get("a"), Some("1"));
		assert_eq!(rows[0].get("b"), None);
		assert_eq!(rows[1].get("a"), None);
		assert_eq!(rows[1].get("b"), Some("2"));
		Ok(())
	}

	#[test(tokio::test)]
	async fn scalar_results_use_a_value_column() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		let rows = rows(client.simple_query("RETURN 9").await?);
		assert_eq!(rows.len(), 1);
		assert_eq!(rows[0].columns()[0].name(), "value");
		assert_eq!(rows[0].get(0), Some("9"));
		Ok(())
	}

	#[test(tokio::test)]
	async fn unicode_round_trips() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		let rows = rows(client.simple_query("RETURN 'héllo 🦀'").await?);
		assert_eq!(rows[0].get(0), Some("héllo 🦀"));
		Ok(())
	}

	#[test(tokio::test)]
	async fn dbname_selects_namespace_and_database() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		let rows = rows(client.simple_query("RETURN [session::ns(), session::db()]").await?);
		let values: Vec<Option<&str>> = rows.iter().map(|r| r.get(0)).collect();
		assert_eq!(values, vec![Some("testns"), Some("testdb")]);
		Ok(())
	}

	#[test(tokio::test)]
	async fn use_statement_persists_across_queries() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		client.simple_query("USE NS otherns DB otherdb").await?;
		let rows = rows(client.simple_query("RETURN [session::ns(), session::db()]").await?);
		let values: Vec<Option<&str>> = rows.iter().map(|r| r.get(0)).collect();
		assert_eq!(values, vec![Some("otherns"), Some("otherdb")]);
		Ok(())
	}

	#[test(tokio::test)]
	async fn let_statement_persists_across_queries() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		client.simple_query("LET $x = 42").await?;
		let rows = rows(client.simple_query("RETURN $x + 1").await?);
		assert_eq!(rows[0].get(0), Some("43"));
		Ok(())
	}

	#[test(tokio::test)]
	async fn multiple_statements_return_multiple_results() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		let messages = client.simple_query("RETURN 1; RETURN 2").await?;
		let complete =
			messages.iter().filter(|m| matches!(m, SimpleQueryMessage::CommandComplete(_))).count();
		assert_eq!(complete, 2);
		assert_eq!(rows(messages).len(), 2);
		Ok(())
	}

	#[test(tokio::test)]
	async fn parse_errors_report_and_recover() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		let err = client.simple_query("SELECT FROM WHERE").await.unwrap_err();
		assert_eq!(err.code(), Some(&SqlState::SYNTAX_ERROR));
		// The session survives the error.
		let rows = rows(client.simple_query("RETURN 1").await?);
		assert_eq!(rows[0].get(0), Some("1"));
		Ok(())
	}

	#[test(tokio::test)]
	async fn runtime_errors_report_and_recover() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		let err = client.simple_query("RETURN 1; THROW 'boom'").await.unwrap_err();
		let message = err.as_db_error().map(|e| e.message().to_string()).unwrap_or_default();
		assert!(message.contains("boom"), "unexpected error: {err:?}");
		let rows = rows(client.simple_query("RETURN 2").await?);
		assert_eq!(rows[0].get(0), Some("2"));
		Ok(())
	}

	#[test(tokio::test)]
	async fn wrong_password_is_rejected() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let err =
			connect(port, &format!("user={USER} password=wrong dbname={USER}")).await.unwrap_err();
		assert_eq!(err.code(), Some(&SqlState::INVALID_PASSWORD));
		Ok(())
	}

	#[test(tokio::test)]
	async fn unauthenticated_server_uses_trust_auth() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(false).await?;
		let client = connect(port, &format!("user={USER} dbname=testns/testdb")).await?;
		let rows = rows(client.simple_query("RETURN 1").await?);
		assert_eq!(rows[0].get(0), Some("1"));
		Ok(())
	}

	#[test(tokio::test)]
	async fn extended_query_returns_jsonb() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		// The extended protocol (used by `query`) advertises a single `result`
		// jsonb column for driver-prepared statements.
		let rows = client.query("RETURN { a: 1, b: [2, 3] }", &[]).await?;
		assert_eq!(rows.len(), 1);
		assert_eq!(rows[0].columns()[0].name(), "result");
		let value: serde_json::Value = rows[0].get(0);
		assert_eq!(value, serde_json::json!({ "a": 1, "b": [2, 3] }));
		Ok(())
	}

	#[test(tokio::test)]
	async fn extended_query_one_row_per_element() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		client.simple_query("CREATE t:1 SET n = 1 RETURN NONE").await?;
		client.simple_query("CREATE t:2 SET n = 2 RETURN NONE").await?;
		// An array result becomes one jsonb row per element.
		let rows = client.query("SELECT n FROM t ORDER BY n", &[]).await?;
		assert_eq!(rows.len(), 2);
		let first: serde_json::Value = rows[0].get(0);
		let second: serde_json::Value = rows[1].get(0);
		assert_eq!(first, serde_json::json!({ "n": 1 }));
		assert_eq!(second, serde_json::json!({ "n": 2 }));
		Ok(())
	}

	#[test(tokio::test)]
	async fn prepared_typed_params_bind() -> Result<(), Box<dyn Error>> {
		use tokio_postgres::types::Type;
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		// Declaring the parameter type lets the driver bind `$1` and the
		// server decode it into a SurrealQL Int.
		let stmt = client.prepare_typed("RETURN $1 + 1", &[Type::INT8]).await?;
		let rows = client.query(&stmt, &[&41i64]).await?;
		let value: serde_json::Value = rows[0].get(0);
		assert_eq!(value, serde_json::json!(42));
		Ok(())
	}

	#[test(tokio::test)]
	async fn prepared_text_param_with_cast() -> Result<(), Box<dyn Error>> {
		use tokio_postgres::types::Type;
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		// A text parameter cast in-query is the escape hatch for untyped binds.
		let stmt = client.prepare_typed("RETURN <int> $1 * 2", &[Type::TEXT]).await?;
		let rows = client.query(&stmt, &[&"21"]).await?;
		let value: serde_json::Value = rows[0].get(0);
		assert_eq!(value, serde_json::json!(42));
		Ok(())
	}

	#[test(tokio::test)]
	async fn prepared_statement_reused() -> Result<(), Box<dyn Error>> {
		use tokio_postgres::types::Type;
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		let stmt = client.prepare_typed("RETURN $1", &[Type::INT8]).await?;
		for n in [1i64, 2, 3] {
			let rows = client.query(&stmt, &[&n]).await?;
			let value: serde_json::Value = rows[0].get(0);
			assert_eq!(value, serde_json::json!(n));
		}
		Ok(())
	}

	#[test(tokio::test)]
	async fn extended_parse_error_recovers() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		let err = client.query("SELECT FROM WHERE", &[]).await.unwrap_err();
		assert_eq!(err.code(), Some(&SqlState::SYNTAX_ERROR));
		// The connection is usable after the error + Sync.
		let rows = client.query("RETURN 1", &[]).await?;
		let value: serde_json::Value = rows[0].get(0);
		assert_eq!(value, serde_json::json!(1));
		Ok(())
	}

	#[test(tokio::test)]
	async fn transaction_block_does_not_leak_session_object() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		// A RETURN inside a transaction block skips later statements, so the
		// executor returns fewer results than statements — the handler must
		// not fall back to streaming the internal $session object.
		let messages = client.simple_query("BEGIN; RETURN 1; RETURN 2; COMMIT").await?;
		for row in rows(messages) {
			// No result set may carry session-internal columns.
			for col in row.columns() {
				assert!(
					!["ns", "db", "ac", "rd", "tk", "ip", "or", "id"].contains(&col.name()),
					"leaked session column: {}",
					col.name()
				);
			}
		}
		Ok(())
	}

	#[test(tokio::test)]
	async fn transaction_use_persists() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		client.simple_query("BEGIN; USE NS blockns DB blockdb; COMMIT").await?;
		let rows = rows(client.simple_query("RETURN [session::ns(), session::db()]").await?);
		let values: Vec<Option<&str>> = rows.iter().map(|r| r.get(0)).collect();
		assert_eq!(values, vec![Some("blockns"), Some("blockdb")]);
		Ok(())
	}

	#[test(tokio::test)]
	async fn error_stops_later_statements() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		// The second statement errors; the third must never execute.
		let err =
			client.simple_query("CREATE thing:a; THROW 'stop'; CREATE thing:c").await.unwrap_err();
		let message = err.as_db_error().map(|e| e.message().to_string()).unwrap_or_default();
		assert!(message.contains("stop"), "unexpected error: {err:?}");
		let rows = rows(client.simple_query("SELECT VALUE id FROM thing").await?);
		let ids: Vec<Option<&str>> = rows.iter().map(|r| r.get(0)).collect();
		assert_eq!(ids, vec![Some("thing:a")], "thing:c must not have been created");
		Ok(())
	}

	#[test(tokio::test)]
	async fn protected_let_does_not_poison_connection() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		// Attempting to LET a protected variable errors, but must not corrupt
		// the session such that later queries fail.
		let _ = client.simple_query("LET $session = 1").await;
		let rows = rows(client.simple_query("RETURN 1").await?);
		assert_eq!(rows[0].get(0), Some("1"));
		Ok(())
	}

	#[test(tokio::test)]
	async fn empty_query_is_answered() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		let messages = client.simple_query("").await?;
		assert!(rows(messages).is_empty());
		let rows = rows(client.simple_query("RETURN 1").await?);
		assert_eq!(rows[0].get(0), Some("1"));
		Ok(())
	}

	#[test(tokio::test)]
	async fn extended_empty_query_is_answered() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		// An empty prepared query returns no rows and does not error.
		let rows = client.query("", &[]).await?;
		assert!(rows.is_empty());
		// The connection remains usable.
		let rows = client.query("RETURN 1", &[]).await?;
		let value: serde_json::Value = rows[0].get(0);
		assert_eq!(value, serde_json::json!(1));
		Ok(())
	}

	#[test(tokio::test)]
	async fn interactive_transaction_commits() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		// Each statement is a separate simple-query message inside the
		// interactive transaction opened by the standalone BEGIN.
		client.simple_query("BEGIN").await?;
		client.simple_query("CREATE txn:1 SET n = 1 RETURN NONE").await?;
		client.simple_query("COMMIT").await?;
		let rows = rows(client.simple_query("SELECT VALUE n FROM txn").await?);
		assert_eq!(rows.len(), 1);
		assert_eq!(rows[0].get(0), Some("1"));
		Ok(())
	}

	#[test(tokio::test)]
	async fn interactive_transaction_rolls_back() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		// A committed baseline row so the table exists for the final SELECT.
		client.simple_query("CREATE rb:base SET n = 0 RETURN NONE").await?;
		client.simple_query("BEGIN").await?;
		client.simple_query("CREATE rb:1 SET n = 1 RETURN NONE").await?;
		client.simple_query("ROLLBACK").await?;
		let rows = rows(client.simple_query("SELECT VALUE id FROM rb").await?);
		assert_eq!(rows.len(), 1, "only the committed baseline row should remain");
		assert_eq!(rows[0].get(0), Some("rb:base"));
		Ok(())
	}

	#[test(tokio::test)]
	async fn extended_protocol_transaction_commit_and_rollback() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		// BEGIN/CREATE/COMMIT issued through the extended protocol (execute()
		// prepares each statement) must drive the same interactive transaction
		// as the simple-query path.
		client.execute("BEGIN", &[]).await?;
		client.execute("CREATE ext:commit SET n = 1 RETURN NONE", &[]).await?;
		client.execute("COMMIT", &[]).await?;
		assert_eq!(
			rows(client.simple_query("SELECT VALUE id FROM ext:commit").await?).len(),
			1,
			"a committed extended-protocol transaction should persist"
		);
		// The rollback path likewise discards the row.
		client.execute("BEGIN", &[]).await?;
		client.execute("CREATE ext:rollback SET n = 1 RETURN NONE", &[]).await?;
		client.execute("ROLLBACK", &[]).await?;
		assert_eq!(
			rows(client.simple_query("SELECT VALUE id FROM ext:rollback").await?).len(),
			0,
			"a rolled-back extended-protocol transaction should discard its writes"
		);
		Ok(())
	}

	#[test(tokio::test)]
	async fn extended_protocol_rollback_recovers_failed_transaction() -> Result<(), Box<dyn Error>>
	{
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		// Open a transaction and poison it with an error, all over the extended
		// protocol. ROLLBACK must then escape the failed transaction — the
		// aborted-transaction guard must not block transaction-control statements.
		client.execute("BEGIN", &[]).await?;
		assert!(client.execute("THROW 'boom'", &[]).await.is_err(), "THROW should error");
		client.execute("ROLLBACK", &[]).await?;
		// The connection is usable again after recovery.
		let rows = rows(client.simple_query("RETURN 1").await?);
		assert_eq!(rows.len(), 1, "connection should be usable after extended-protocol ROLLBACK");
		Ok(())
	}

	#[test(tokio::test)]
	async fn extended_protocol_intercepts_set() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server_with_args(true, "--allow-experimental gql").await?;
		let client = connect_root(port).await?;
		// A prepared `SET` is intercepted rather than handed to the SurrealQL
		// parser (which would reject it). Dialect switching and no-op GUCs work.
		client.execute("SET dialect = 'gql'", &[]).await?;
		// A driver GUC set while in the GQL dialect is still accepted as a no-op
		// (it must not fall through to the GQL executor and error).
		client.execute("SET client_encoding = 'UTF8'", &[]).await?;
		client.execute("SET dialect = 'surrealql'", &[]).await?;
		client.execute("SET extra_float_digits = 3", &[]).await?;
		// An unknown GUC is still rejected.
		assert!(client.execute("SET bogus_param = 1", &[]).await.is_err());
		Ok(())
	}

	#[test(tokio::test)]
	async fn cancelled_transaction_let_does_not_leak() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		// A LET inside a rolled-back (CANCEL) block must not leak into the
		// connection session; a committed block's LET does persist. The
		// cancelled batch itself reports a cancelled-transaction error, so
		// ignore its result and check the session afterwards.
		let _ = client.simple_query("BEGIN; LET $leaked = 99; CANCEL").await;
		let leaked = rows(client.simple_query("RETURN $leaked ?? 'unset'").await?);
		assert_eq!(leaked[0].get(0), Some("unset"), "cancelled LET must not persist");
		client.simple_query("BEGIN; LET $kept = 7; COMMIT").await?;
		let kept = rows(client.simple_query("RETURN $kept").await?);
		assert_eq!(kept[0].get(0), Some("7"), "committed LET should persist");
		Ok(())
	}

	#[test(tokio::test)]
	async fn transaction_preamble_is_recognized() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		// A BEGIN with an isolation-level clause (as ORMs/JDBC send) opens the
		// transaction rather than failing to parse.
		client.simple_query("BEGIN ISOLATION LEVEL SERIALIZABLE").await?;
		client.simple_query("CREATE pre:1 SET n = 1 RETURN NONE").await?;
		client.simple_query("COMMIT").await?;
		let rows = rows(client.simple_query("SELECT VALUE n FROM pre").await?);
		assert_eq!(rows.len(), 1);
		Ok(())
	}

	#[test(tokio::test)]
	async fn stray_commit_warns_and_continues() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		// A stray COMMIT with no open transaction is a warning, not a fatal
		// error — the following statement still runs (Postgres semantics).
		let messages = client.simple_query("COMMIT; RETURN 42").await?;
		let value = rows(messages).into_iter().find_map(|r| r.get(0).map(str::to_owned));
		assert_eq!(value.as_deref(), Some("42"), "statement after stray COMMIT must run");
		Ok(())
	}

	#[test(tokio::test)]
	async fn bind_rejects_too_few_params() -> Result<(), Box<dyn Error>> {
		use tokio_postgres::types::Type;
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		// Binding fewer values than the statement requires must fail rather
		// than silently under-run. tokio-postgres enforces the count
		// client-side (so this may not reach the server-side guard added for
		// drivers that don't), but the operation must error either way.
		let stmt = client.prepare_typed("RETURN $1 + $2", &[Type::INT8, Type::INT8]).await?;
		assert!(client.query(&stmt, &[&1i64]).await.is_err(), "under-supplied Bind should error");
		Ok(())
	}

	#[test(tokio::test)]
	async fn aborted_transaction_refuses_until_rollback() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		client.simple_query("BEGIN").await?;
		// A failing statement poisons the transaction.
		let _ = client.simple_query("THROW 'boom'").await;
		// Further statements are refused with 25P02.
		let err = client.simple_query("RETURN 1").await.unwrap_err();
		assert_eq!(err.code(), Some(&SqlState::IN_FAILED_SQL_TRANSACTION));
		// ROLLBACK clears the failed state and the session recovers.
		client.simple_query("ROLLBACK").await?;
		let rows = rows(client.simple_query("RETURN 7").await?);
		assert_eq!(rows[0].get(0), Some("7"));
		Ok(())
	}

	#[test(tokio::test)]
	async fn set_dialect_switches_language() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server_with_args(true, "--allow-experimental gql").await?;
		let client = connect_root(port).await?;
		client.simple_query("CREATE person:aeon SET name = 'Aeon' RETURN NONE").await?;
		// Switch to GQL and run a MATCH; then switch back.
		client.simple_query("SET dialect = 'gql'").await?;
		let matched = rows(client.simple_query("MATCH (p:person) RETURN p.name").await?);
		assert!(!matched.is_empty(), "GQL MATCH should return the created person");
		client.simple_query("SET dialect = 'surrealql'").await?;
		let back = rows(client.simple_query("RETURN 1").await?);
		assert_eq!(back[0].get(0), Some("1"));
		Ok(())
	}

	#[test(tokio::test)]
	async fn extended_error_poisons_transaction() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		client.simple_query("BEGIN").await?;
		// An extended-protocol statement error must abort the transaction, so a
		// subsequent statement is refused with 25P02 (as in the simple path).
		let _ = client.query("THROW 'boom'", &[]).await;
		let err = client.simple_query("RETURN 1").await.unwrap_err();
		assert_eq!(err.code(), Some(&SqlState::IN_FAILED_SQL_TRANSACTION));
		client.simple_query("ROLLBACK").await?;
		let rows = rows(client.simple_query("RETURN 5").await?);
		assert_eq!(rows[0].get(0), Some("5"));
		Ok(())
	}

	#[test(tokio::test)]
	async fn gql_in_transaction_is_rejected() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server_with_args(true, "--allow-experimental gql").await?;
		let client = connect_root(port).await?;
		client.simple_query("SET dialect = 'gql'").await?;
		client.simple_query("BEGIN").await?;
		// GQL cannot run inside an interactive transaction; it must be refused
		// rather than silently auto-committing outside the block.
		let err = client.simple_query("MATCH (p:person) RETURN p").await.unwrap_err();
		assert_eq!(err.code(), Some(&SqlState::FEATURE_NOT_SUPPORTED));
		client.simple_query("ROLLBACK").await?;
		Ok(())
	}

	#[test(tokio::test)]
	async fn gql_over_extended_protocol() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server_with_args(true, "--allow-experimental gql").await?;
		let client = connect_root(port).await?;
		client.simple_query("CREATE person:nova SET name = 'Nova' RETURN NONE").await?;
		client.simple_query("SET dialect = 'gql'").await?;
		// The extended protocol (query()) must not validate GQL with the
		// SurrealQL parser; it returns the jsonb result shape.
		let dbrows = client.query("MATCH (p:person) RETURN p.name", &[]).await?;
		assert!(!dbrows.is_empty(), "GQL over the extended protocol should return rows");
		Ok(())
	}

	#[test(tokio::test)]
	async fn gql_disabled_reports_cleanly() -> Result<(), Box<dyn Error>> {
		// Without --allow-experimental gql, selecting the dialect is fine but
		// running a GQL query reports a clean feature error, not a crash.
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		client.simple_query("SET dialect = 'gql'").await?;
		let err = client.simple_query("MATCH (p:person) RETURN p").await.unwrap_err();
		assert!(err.code().is_some(), "expected a clean SQLSTATE error");
		Ok(())
	}

	#[test(tokio::test)]
	async fn unknown_set_parameter_errors() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		// A known GUC is a no-op; an unknown one is rejected.
		client.simple_query("SET extra_float_digits = 3").await?;
		let err = client.simple_query("SET made_up_param = 1").await.unwrap_err();
		assert!(err.code().is_some());
		Ok(())
	}

	#[test(tokio::test)]
	async fn ssl_request_declined_without_tls() -> Result<(), Box<dyn Error>> {
		// A server without a certificate declines TLS ('N'); the client then
		// continues in plaintext (verified by the many plaintext tests above).
		let (port, _server) = start_pg_server(true).await?;
		assert_eq!(ssl_request_reply(port).await, b'N');
		Ok(())
	}

	#[test(tokio::test)]
	async fn ssl_request_accepted_with_tls() -> Result<(), Box<dyn Error>> {
		// With a certificate configured (via --web-crt/--web-key), the listener
		// accepts the SSLRequest ('S') and upgrades. The HTTP readiness probe
		// can't speak HTTPS here, so bind without waiting and poll the port.
		let port: u16 = rand::rng().random_range(24000..33000);
		let (_, _server) = common::start_server(StartServerArguments {
			auth: true,
			tls: true,
			wait_is_ready: false,
			args: format!("--postgres-bind 127.0.0.1:{port}"),
			..Default::default()
		})
		.await?;
		assert_eq!(ssl_request_reply(port).await, b'S');
		Ok(())
	}

	#[test(tokio::test)]
	async fn cancel_request_interrupts_query() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		let cancel_token = client.cancel_token();
		// Start a slow query, then cancel it from a side connection.
		let query = tokio::spawn(async move { client.simple_query("SLEEP 5s").await });
		tokio::time::sleep(Duration::from_millis(500)).await;
		cancel_token.cancel_query(NoTls).await?;
		let result = query.await?;
		// The query must return (cancelled or errored) well before its 5s sleep.
		assert!(result.is_err(), "the sleeping query should have been cancelled");
		Ok(())
	}
}
