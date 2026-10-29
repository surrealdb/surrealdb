// RUST_LOG=warn cargo test --locked --features storage-mem,postgres --test pg_integration
// -- --nocapture
//
// `postgres` is required and is NOT implied by the root package's default
// features: `default` forwards to `surrealdb-server/default`, which does not
// enable this package's own `postgres` feature. Without it the module below is
// compiled away and the run reports "running 0 tests" — a green result that
// tested nothing. Check the count, not just the exit status.
mod common;

#[cfg(feature = "postgres")]
mod pg_integration {
	use std::error::Error;
	use std::time::Duration;

	use test_log::test;
	use tokio::io::{AsyncReadExt, AsyncWriteExt};
	use tokio::net::TcpStream;
	use tokio_postgres::error::SqlState;
	use tokio_postgres::{Client, NoTls, SimpleQueryMessage, SimpleQueryRow};

	use super::common::{self, PASS, StartServerArguments, USER};

	static HTTP_PORTS: std::sync::Mutex<Vec<(u16, u16)>> = std::sync::Mutex::new(Vec::new());

	/// Runs SurrealQL in `testns`/`testdb` of the server whose Postgres listener is on `port`.
	/// The Postgres port speaks only PostgreSQL, so SurrealQL fixtures go in over HTTP.
	async fn seed(port: u16, sql: &str) {
		query_http(port, sql).await;
	}

	/// Runs SurrealQL as `seed` does and returns the response body.
	async fn query_http(port: u16, sql: &str) -> String {
		let http = HTTP_PORTS
			.lock()
			.expect("lock the ports")
			.iter()
			.find(|(pg, _)| *pg == port)
			.map(|(_, http)| *http)
			.expect("a server started by start_pg_server_with_args");
		let body = reqwest::Client::builder()
			.timeout(Duration::from_secs(30))
			.connect_timeout(Duration::from_secs(5))
			.build()
			.expect("build the HTTP client")
			.post(format!("http://127.0.0.1:{http}/sql"))
			.basic_auth(USER, Some(PASS))
			.header("surreal-ns", "testns")
			.header("surreal-db", "testdb")
			.header("accept", "application/json")
			.body(sql.to_string())
			.send()
			.await
			.expect("seed response")
			.text()
			.await
			.expect("seed body");
		assert!(!body.contains("\"status\":\"ERR\""), "{sql}: {body}");
		body
	}

	/// The Postgres SSLRequest packet (length 8, magic code 80877103).
	const SSL_REQUEST: [u8; 8] = {
		let len = 8i32.to_be_bytes();
		let code = 80877103i32.to_be_bytes();
		[len[0], len[1], len[2], len[3], code[0], code[1], code[2], code[3]]
	};

	/// Send an SSLRequest to the port and return the server's single-byte reply
	/// ('S' = TLS accepted, 'N' = declined), retrying while the listener binds.
	async fn ssl_request_reply(port: u16) -> u8 {
		try_ssl_request_reply(port)
			.await
			.unwrap_or_else(|| panic!("no SSLRequest reply from port {port}"))
	}

	/// As [`ssl_request_reply`], but `None` rather than a panic when the port
	/// never answers — so a caller that can restart the server on a fresh port
	/// can retry instead of failing.
	async fn try_ssl_request_reply(port: u16) -> Option<u8> {
		for _ in 0..50 {
			if let Ok(mut sock) = TcpStream::connect(("127.0.0.1", port)).await
				&& sock.write_all(&SSL_REQUEST).await.is_ok()
			{
				let mut byte = [0u8; 1];
				if sock.read_exact(&mut byte).await.is_ok() {
					return Some(byte[0]);
				}
			}
			tokio::time::sleep(Duration::from_millis(100)).await;
		}
		None
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

	/// A port the OS reports as free, taken from the ephemeral range by binding
	/// and immediately releasing it. The listener is dropped before the server
	/// binds the port, so this narrows the collision window rather than closing
	/// it — callers that can retry should.
	fn free_port() -> u16 {
		std::net::TcpListener::bind("127.0.0.1:0")
			.expect("bind an ephemeral port")
			.local_addr()
			.expect("read the bound port")
			.port()
	}

	/// Start a server with the Postgres listener bound to a free port.
	///
	/// A `--postgres-bind` collision fails the whole server startup (the
	/// listener binds before startup completes), and `common::start_server`
	/// retries only its own HTTP port — the Postgres one is fixed in the args
	/// it is given. So a collision is retried here, on a fresh port.
	async fn start_pg_server_with_args(
		auth: bool,
		extra: &str,
	) -> Result<(u16, common::Child), Box<dyn Error>> {
		let mut last = None;
		for _ in 0..3 {
			let port = free_port();
			match common::start_server(StartServerArguments {
				auth,
				args: format!("--postgres-bind 127.0.0.1:{port} {extra}"),
				..Default::default()
			})
			.await
			{
				Ok((addr, child)) => {
					let http = addr.parse::<std::net::SocketAddr>()?.port();
					HTTP_PORTS.lock().expect("lock the ports").push((port, http));
					return Ok((port, child));
				}
				Err(err) => last = Some(err),
			}
		}
		Err(last.unwrap_or_else(|| "server failed to start".into()))
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
		connect(port, &root_config("")).await
	}

	/// A root connection string for `testns/testdb`, plus any extra settings.
	fn root_config(extra: &str) -> String {
		format!("user={USER} password={PASS} dbname=testns/testdb {extra}")
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
		seed(port, "CREATE person:tobie SET name = 'Tobie', age = 42 RETURN NONE").await;
		let rows = rows(client.simple_query("SELECT * FROM person").await?);
		assert_eq!(rows.len(), 1);
		let row = &rows[0];
		let names: Vec<&str> = row.columns().iter().map(|c| c.name()).collect();
		assert_eq!(names, vec!["age", "id", "name"]);
		assert_eq!(row.get("age"), Some("42"));
		assert_eq!(row.get("id"), Some("tobie"));
		assert_eq!(row.get("name"), Some("Tobie"));
		Ok(())
	}

	#[test(tokio::test)]
	async fn values_render_in_postgres_text_formats() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		seed(
			port,
			"CREATE fmt:1 CONTENT { b: true, f: 1.5, i: 123, s: 'hello', \
			 dt: d'2024-01-15T12:34:56.123456789Z', du: 1d2h, arr: [1, 2], obj: { a: 1 } } \
			 RETURN NONE",
		)
		.await;
		let rows = rows(client.simple_query("SELECT b, f, i, s, dt, du, arr, obj FROM fmt").await?);
		assert_eq!(rows.len(), 1);
		let row = &rows[0];
		assert_eq!(row.get("b"), Some("t"));
		assert_eq!(row.get("f"), Some("1.5"));
		assert_eq!(row.get("i"), Some("123"));
		assert_eq!(row.get("s"), Some("hello"));
		assert_eq!(row.get("dt"), Some("2024-01-15 12:34:56.123456+00"));
		assert_eq!(row.get("du"), Some("1 day 02:00:00"));
		assert_eq!(row.get("arr"), Some("{1,2}"));
		assert_eq!(row.get("obj"), Some("{\"a\":1}"));
		Ok(())
	}

	#[test(tokio::test)]
	async fn scalar_results_are_one_cell() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		let rows = rows(client.simple_query("SELECT 9").await?);
		assert_eq!(rows.len(), 1);
		assert_eq!(rows[0].columns().len(), 1);
		assert_eq!(rows[0].get(0), Some("9"));
		Ok(())
	}

	#[test(tokio::test)]
	async fn unicode_round_trips() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		let rows = rows(client.simple_query("SELECT 'héllo 🦀'").await?);
		assert_eq!(rows[0].get(0), Some("héllo 🦀"));
		Ok(())
	}

	#[test(tokio::test)]
	async fn dbname_selects_namespace_and_database() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		seed(port, "CREATE marker:1 SET n = 1 RETURN NONE").await;
		let rows = rows(client.simple_query("SELECT n FROM marker").await?);
		assert_eq!(rows.len(), 1, "the connection reads the database named at startup");
		Ok(())
	}

	#[test(tokio::test)]
	async fn multiple_statements_return_multiple_results() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		let messages = client.simple_query("SELECT 1; SELECT 2").await?;
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
		let rows = rows(client.simple_query("SELECT 1").await?);
		assert_eq!(rows[0].get(0), Some("1"));
		Ok(())
	}

	#[test(tokio::test)]
	async fn runtime_errors_report_and_recover() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		let err = client.simple_query("SELECT 1; SELECT 1 / 0").await.unwrap_err();
		assert_eq!(err.code(), Some(&SqlState::DIVISION_BY_ZERO), "unexpected error: {err:?}");
		let rows = rows(client.simple_query("SELECT 2").await?);
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
		let rows = rows(client.simple_query("SELECT 1").await?);
		assert_eq!(rows[0].get(0), Some("1"));
		Ok(())
	}

	#[test(tokio::test)]
	async fn extended_query_one_row_per_element() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		seed(port, "CREATE t:1 SET n = 1 RETURN NONE; CREATE t:2 SET n = 2 RETURN NONE").await;
		let rows = client.query("SELECT n FROM t ORDER BY n", &[]).await?;
		assert_eq!(rows.len(), 2);
		assert_eq!(rows[0].try_get::<_, String>(0)?, "1");
		assert_eq!(rows[1].try_get::<_, String>(0)?, "2");
		Ok(())
	}

	#[test(tokio::test)]
	async fn prepared_typed_params_bind() -> Result<(), Box<dyn Error>> {
		use tokio_postgres::types::Type;
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		// Declaring the parameter type lets the driver bind `$1` and the
		// server decode it into an integer.
		let stmt = client.prepare_typed("SELECT $1::int8 + 1", &[Type::INT8]).await?;
		let rows = client.query(&stmt, &[&41i64]).await?;
		let value: i64 = rows[0].get(0);
		assert_eq!(value, 42);
		Ok(())
	}

	#[test(tokio::test)]
	async fn prepared_text_param_with_cast() -> Result<(), Box<dyn Error>> {
		use tokio_postgres::types::Type;
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		// A text parameter cast in-query is the escape hatch for untyped binds.
		let stmt = client.prepare_typed("SELECT CAST($1 AS int8) * 2", &[Type::TEXT]).await?;
		let rows = client.query(&stmt, &[&"21"]).await?;
		let value: i64 = rows[0].get(0);
		assert_eq!(value, 42);
		Ok(())
	}

	#[test(tokio::test)]
	async fn prepared_statement_reused() -> Result<(), Box<dyn Error>> {
		use tokio_postgres::types::Type;
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		let stmt = client.prepare_typed("SELECT $1::int8", &[Type::INT8]).await?;
		for n in [1i64, 2, 3] {
			let rows = client.query(&stmt, &[&n]).await?;
			let value: i64 = rows[0].get(0);
			assert_eq!(value, n);
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
		let rows = client.query("SELECT 1::int8", &[]).await?;
		let value: i64 = rows[0].get(0);
		assert_eq!(value, 1);
		Ok(())
	}

	#[test(tokio::test)]
	async fn empty_query_is_answered() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		let messages = client.simple_query("").await?;
		assert!(rows(messages).is_empty());
		let rows = rows(client.simple_query("SELECT 1").await?);
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
		let rows = client.query("SELECT 1::int8", &[]).await?;
		let value: i64 = rows[0].get(0);
		assert_eq!(value, 1);
		Ok(())
	}

	#[test(tokio::test)]
	async fn interactive_transaction_commits() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		client.simple_query("CREATE TABLE txn (id INT, n INT)").await?;
		client.simple_query("BEGIN").await?;
		client.simple_query("INSERT INTO txn (id, n) VALUES (1, 1)").await?;
		client.simple_query("COMMIT").await?;
		let rows = rows(client.simple_query("SELECT n FROM txn").await?);
		assert_eq!(rows.len(), 1);
		assert_eq!(rows[0].get(0), Some("1"));
		Ok(())
	}

	#[test(tokio::test)]
	async fn interactive_transaction_rolls_back() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		client.simple_query("CREATE TABLE rb (id INT, n INT)").await?;
		client.simple_query("INSERT INTO rb (id, n) VALUES (0, 0)").await?;
		client.simple_query("BEGIN").await?;
		client.simple_query("INSERT INTO rb (id, n) VALUES (1, 1)").await?;
		client.simple_query("ROLLBACK").await?;
		let rows = rows(client.simple_query("SELECT id FROM rb").await?);
		assert_eq!(rows.len(), 1, "only the committed baseline row should remain");
		assert_eq!(rows[0].get(0), Some("0"));
		Ok(())
	}

	#[test(tokio::test)]
	async fn extended_protocol_transaction_commit_and_rollback() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		client.simple_query("CREATE TABLE ext (id INT, n INT)").await?;
		client.execute("BEGIN", &[]).await?;
		client.execute("INSERT INTO ext (id, n) VALUES (1, 1)", &[]).await?;
		client.execute("COMMIT", &[]).await?;
		assert_eq!(
			rows(client.simple_query("SELECT id FROM ext WHERE id = 1").await?).len(),
			1,
			"a committed extended-protocol transaction should persist"
		);
		client.execute("BEGIN", &[]).await?;
		client.execute("INSERT INTO ext (id, n) VALUES (2, 1)", &[]).await?;
		client.execute("ROLLBACK", &[]).await?;
		assert_eq!(
			rows(client.simple_query("SELECT id FROM ext WHERE id = 2").await?).len(),
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
		assert!(
			client.execute("SELECT 1 / 0", &[]).await.is_err(),
			"division by zero should error"
		);
		client.execute("ROLLBACK", &[]).await?;
		// The connection is usable again after recovery.
		let rows = rows(client.simple_query("SELECT 1").await?);
		assert_eq!(rows.len(), 1, "connection should be usable after extended-protocol ROLLBACK");
		Ok(())
	}

	#[test(tokio::test)]
	async fn extended_protocol_intercepts_set() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server_with_args(true, "").await?;
		let client = connect_root(port).await?;
		// A prepared `SET` is intercepted rather than handed to the planner. Dialect switching and
		// no-op GUCs work.
		client.execute("SET dialect = 'gql'", &[]).await?;
		// A driver GUC set while in the GQL dialect is still accepted as a no-op
		// (it must not fall through to the GQL executor and error).
		client.execute("SET client_encoding = 'UTF8'", &[]).await?;
		client.execute("SET dialect = 'pgsql'", &[]).await?;
		client.execute("SET extra_float_digits = 3", &[]).await?;
		// An unknown GUC is still rejected.
		assert!(client.execute("SET bogus_param = 1", &[]).await.is_err());
		Ok(())
	}

	#[test(tokio::test)]
	async fn transaction_preamble_is_recognized() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		// A BEGIN with an isolation-level clause (as ORMs/JDBC send) opens the
		// transaction rather than failing to parse.
		client.simple_query("CREATE TABLE pre (id INT, n INT)").await?;
		client.simple_query("BEGIN ISOLATION LEVEL SERIALIZABLE").await?;
		client.simple_query("INSERT INTO pre (id, n) VALUES (1, 1)").await?;
		client.simple_query("COMMIT").await?;
		let rows = rows(client.simple_query("SELECT n FROM pre").await?);
		assert_eq!(rows.len(), 1);
		Ok(())
	}

	#[test(tokio::test)]
	async fn stray_commit_warns_and_continues() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		// A stray COMMIT with no open transaction is a warning, not a fatal
		// error — the following statement still runs (Postgres semantics).
		let messages = client.simple_query("COMMIT; SELECT 42").await?;
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
		let stmt =
			client.prepare_typed("SELECT $1::int8 + $2::int8", &[Type::INT8, Type::INT8]).await?;
		assert!(client.query(&stmt, &[&1i64]).await.is_err(), "under-supplied Bind should error");
		Ok(())
	}

	#[test(tokio::test)]
	async fn aborted_transaction_refuses_until_rollback() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		client.simple_query("BEGIN").await?;
		// A failing statement poisons the transaction.
		let _ = client.simple_query("SELECT 1 / 0").await;
		// Further statements are refused with 25P02.
		let err = client.simple_query("SELECT 1").await.unwrap_err();
		assert_eq!(err.code(), Some(&SqlState::IN_FAILED_SQL_TRANSACTION));
		// ROLLBACK clears the failed state and the session recovers.
		client.simple_query("ROLLBACK").await?;
		let rows = rows(client.simple_query("SELECT 7").await?);
		assert_eq!(rows[0].get(0), Some("7"));
		Ok(())
	}

	#[test(tokio::test)]
	async fn set_dialect_switches_language() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server_with_args(true, "").await?;
		let client = connect_root(port).await?;
		seed(port, "CREATE person:aeon SET name = 'Aeon' RETURN NONE").await;
		client.simple_query("SET dialect = 'gql'").await?;
		let matched = rows(client.simple_query("MATCH (p:person) RETURN p.name").await?);
		assert!(!matched.is_empty(), "GQL MATCH should return the created person");
		client.simple_query("SET dialect = 'pgsql'").await?;
		let back = rows(client.simple_query("SELECT 1").await?);
		assert_eq!(back[0].get(0), Some("1"));
		Ok(())
	}

	#[test(tokio::test)]
	async fn other_protocols_dialects_are_refused() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		for dialect in ["surrealql", "mysql"] {
			let err = client
				.simple_query(&format!("SET dialect = '{dialect}'"))
				.await
				.expect_err("switched dialect");
			assert_eq!(err.code(), Some(&SqlState::FEATURE_NOT_SUPPORTED), "{dialect}");
			let err = connect(port, &root_config(&format!("options='-c dialect={dialect}'")))
				.await
				.expect_err("connected");
			assert_eq!(err.code(), Some(&SqlState::FEATURE_NOT_SUPPORTED), "{dialect}");
		}
		let err = client.simple_query("CREATE person SET name = 'x'").await.unwrap_err();
		assert_eq!(err.code(), Some(&SqlState::SYNTAX_ERROR), "SurrealQL is not PostgreSQL");
		let back = rows(client.simple_query("SELECT 1").await?);
		assert_eq!(back[0].get(0), Some("1"), "a refused switch leaves the connection usable");
		Ok(())
	}

	#[test(tokio::test)]
	async fn extended_error_poisons_transaction() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		client.simple_query("BEGIN").await?;
		// An extended-protocol statement error must abort the transaction, so a
		// subsequent statement is refused with 25P02 (as in the simple path).
		let _ = client.query("SELECT 1 / 0", &[]).await;
		let err = client.simple_query("SELECT 1").await.unwrap_err();
		assert_eq!(err.code(), Some(&SqlState::IN_FAILED_SQL_TRANSACTION));
		client.simple_query("ROLLBACK").await?;
		let rows = rows(client.simple_query("SELECT 5").await?);
		assert_eq!(rows[0].get(0), Some("5"));
		Ok(())
	}

	#[test(tokio::test)]
	async fn gql_in_transaction_is_rejected() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server_with_args(true, "").await?;
		let client = connect_root(port).await?;
		client.simple_query("SET dialect = 'gql'").await?;
		client.simple_query("BEGIN").await?;
		// GQL cannot run inside an interactive transaction; it must be refused
		// rather than silently auto-committing outside the block.
		let err = client.simple_query("MATCH (p:person) RETURN p").await.unwrap_err();
		assert_eq!(err.code(), Some(&SqlState::FEATURE_NOT_SUPPORTED));
		// The refusal is an error inside a transaction block, so it aborts the
		// block like any other; the next statement is refused until ROLLBACK.
		let err = client.simple_query("MATCH (p:person) RETURN p").await.expect_err("aborted txn");
		assert_eq!(err.code(), Some(&SqlState::IN_FAILED_SQL_TRANSACTION));
		client.simple_query("ROLLBACK").await?;
		// The connection is still in the GQL dialect, so switch back before
		// checking it serves PostgreSQL again.
		client.simple_query("SET dialect = 'pgsql'").await?;
		let back = rows(client.simple_query("SELECT 1").await?);
		assert_eq!(back[0].get(0), Some("1"));
		Ok(())
	}

	#[test(tokio::test)]
	async fn denied_gql_dialect_in_transaction_aborts_it() -> Result<(), Box<dyn Error>> {
		// A rejected `SET` is intercepted rather than executed, but it is still
		// an error inside a transaction block and must abort it — otherwise the
		// connection keeps reporting status 'T' and accepting statements.
		let (port, _server) = start_pg_server_with_args(true, "--deny-http gql").await?;
		let client = connect_root(port).await?;
		client.simple_query("BEGIN").await?;
		let err = client.simple_query("SET dialect = 'gql'").await.expect_err("switched dialect");
		assert_eq!(err.code(), Some(&SqlState::INSUFFICIENT_PRIVILEGE));
		let err = client.simple_query("SELECT 1").await.expect_err("ran in aborted txn");
		assert_eq!(err.code(), Some(&SqlState::IN_FAILED_SQL_TRANSACTION));
		client.simple_query("ROLLBACK").await?;
		let back = rows(client.simple_query("SELECT 1").await?);
		assert_eq!(back[0].get(0), Some("1"));
		Ok(())
	}

	#[test(tokio::test)]
	async fn unknown_set_parameter_in_transaction_aborts_it() -> Result<(), Box<dyn Error>> {
		// Same contract for the other intercepted-`SET` rejections, which are
		// not GQL-specific.
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		client.simple_query("BEGIN").await?;
		let err = client.simple_query("SET made_up_param = 1").await.expect_err("accepted");
		assert_eq!(err.code(), Some(&SqlState::UNDEFINED_OBJECT));
		let err = client.simple_query("SELECT 1").await.expect_err("ran in aborted txn");
		assert_eq!(err.code(), Some(&SqlState::IN_FAILED_SQL_TRANSACTION));
		client.simple_query("ROLLBACK").await?;
		let back = rows(client.simple_query("SELECT 1").await?);
		assert_eq!(back[0].get(0), Some("1"));
		Ok(())
	}

	#[test(tokio::test)]
	async fn gql_over_extended_protocol() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server_with_args(true, "").await?;
		let client = connect_root(port).await?;
		seed(port, "CREATE person:nova SET name = 'Nova' RETURN NONE").await;
		client.simple_query("SET dialect = 'gql'").await?;
		// The extended protocol (query()) must not validate GQL with the
		// PostgreSQL parser; it returns the jsonb result shape.
		let dbrows = client.query("MATCH (p:person) RETURN p.name", &[]).await?;
		assert!(!dbrows.is_empty(), "GQL over the extended protocol should return rows");
		Ok(())
	}

	#[test(tokio::test)]
	async fn gql_query_error_reports_cleanly() -> Result<(), Box<dyn Error>> {
		// A failing GQL query must reach the client as a Postgres error with a
		// specific SQLSTATE, and leave the connection usable — GQL errors are
		// mapped onto the wire protocol, not surfaced as a dropped connection
		// or a generic internal error.
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		client.simple_query("SET dialect = 'gql'").await?;
		// `person` is not defined in this database.
		let err = client.simple_query("MATCH (p:person) RETURN p").await.expect_err("matched");
		assert_eq!(err.code(), Some(&SqlState::UNDEFINED_TABLE));
		client.simple_query("SET dialect = 'pgsql'").await?;
		let back = rows(client.simple_query("SELECT 1").await?);
		assert_eq!(back[0].get(0), Some("1"));
		Ok(())
	}

	#[test(tokio::test)]
	async fn startup_options_select_gql_dialect() -> Result<(), Box<dyn Error>> {
		// `options=-c dialect=gql` puts the connection in GQL from its first
		// query, with no `SET`.
		let (port, _server) = start_pg_server(true).await?;
		connect_root(port).await?;
		seed(port, "CREATE person:orion SET name = 'Orion' RETURN NONE").await;
		let client = connect(port, &root_config("options='-c dialect=gql'")).await?;
		let matched = rows(client.simple_query("MATCH (p:person) RETURN p.name").await?);
		assert!(!matched.is_empty(), "startup dialect selection should run the query as GQL");
		Ok(())
	}

	#[test(tokio::test)]
	async fn denied_gql_capability_refuses_startup_dialect() -> Result<(), Box<dyn Error>> {
		// `RouteTarget::Gql` gates the GQL entry surface on this port as well as
		// the `/gql` HTTP route, so a denied dialect refuses the connection
		// rather than silently leaving it in PostgreSQL.
		let (port, _server) = start_pg_server_with_args(true, "--deny-http gql").await?;
		let err =
			connect(port, &root_config("options='-c dialect=gql'")).await.expect_err("connected");
		assert_eq!(err.code(), Some(&SqlState::INSUFFICIENT_PRIVILEGE));
		Ok(())
	}

	#[test(tokio::test)]
	async fn denied_gql_capability_refuses_set_dialect() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server_with_args(true, "--deny-http gql").await?;
		let client = connect_root(port).await?;
		let err = client.simple_query("SET dialect = 'gql'").await.expect_err("switched dialect");
		assert_eq!(err.code(), Some(&SqlState::INSUFFICIENT_PRIVILEGE));
		// The refusal leaves the connection in PostgreSQL and usable: denying
		// `gql` must not disable the Postgres port itself.
		let back = rows(client.simple_query("SELECT 1").await?);
		assert_eq!(back[0].get(0), Some("1"));
		Ok(())
	}

	#[test(tokio::test)]
	async fn denied_postgres_capability_refuses_connection() -> Result<(), Box<dyn Error>> {
		// The port's own capability is separate from the dialect's: denying
		// `postgres` closes the port outright, in either dialect.
		let (port, _server) = start_pg_server_with_args(true, "--deny-http postgres").await?;
		let err = connect_root(port).await.expect_err("connected");
		assert_eq!(err.code(), Some(&SqlState::INSUFFICIENT_PRIVILEGE));
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
		// `wait_is_ready: false` means a bind collision is invisible at startup,
		// so a port that never answers is retried as a fresh start rather than
		// failing the test.
		for _ in 0..3 {
			let port = free_port();
			let (_, _server) = common::start_server(StartServerArguments {
				auth: true,
				tls: true,
				wait_is_ready: false,
				args: format!("--postgres-bind 127.0.0.1:{port}"),
				..Default::default()
			})
			.await?;
			if let Some(reply) = try_ssl_request_reply(port).await {
				assert_eq!(reply, b'S');
				return Ok(());
			}
		}
		panic!("the postgres TLS listener never answered on any port");
	}

	#[test(tokio::test)]
	async fn cancel_request_interrupts_query() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		let cancel_token = client.cancel_token();
		// Start a slow query, then cancel it from a side connection.
		let query = tokio::spawn(async move { client.simple_query("SELECT pg_sleep(5)").await });
		tokio::time::sleep(Duration::from_millis(500)).await;
		cancel_token.cancel_query(NoTls).await?;
		let result = query.await?;
		// The query must return (cancelled or errored) well before its 5s sleep.
		assert!(result.is_err(), "the sleeping query should have been cancelled");
		Ok(())
	}

	#[test(tokio::test)]
	async fn standard_postgres_ddl_and_dml() {
		let (port, _server) = start_pg_server(true).await.unwrap();
		let client = connect_root(port).await.unwrap();
		let res1 = client.simple_query("CREATE TABLE pg_users (id INT, name VARCHAR(50))").await;
		if let Err(e) = &res1 {
			panic!("RES1 error: {e:?}");
		}
		let res2 = client.simple_query("INSERT INTO pg_users (id, name) VALUES (1, 'Alice')").await;
		if let Err(e) = &res2 {
			panic!("RES2 error: {e:?}");
		}
		let res3 = client.simple_query("SELECT id, name FROM pg_users").await;
		if let Err(e) = &res3 {
			panic!("RES3 error: {e:?}");
		}
		let rows = rows(res3.unwrap());
		assert_eq!(rows.len(), 1, "expected 1 row, got {:?}", rows.len());
		assert_eq!(rows[0].get(0), Some("1"));
		assert_eq!(rows[0].get(1), Some("Alice"));
	}

	/// A result cell read as the bytes the server sent, whatever its type.
	struct RawCell(Vec<u8>);

	impl<'a> tokio_postgres::types::FromSql<'a> for RawCell {
		fn from_sql(
			_: &tokio_postgres::types::Type,
			raw: &'a [u8],
		) -> Result<Self, Box<dyn Error + Sync + Send>> {
			Ok(RawCell(raw.to_vec()))
		}

		fn accepts(_: &tokio_postgres::types::Type) -> bool {
			true
		}
	}

	/// The text of a `jsonb` cell received in binary format: a version byte, then JSON.
	fn jsonb_text(cell: &RawCell) -> String {
		assert_eq!(cell.0.first(), Some(&1), "a binary jsonb cell starts with version 1");
		String::from_utf8(cell.0[1..].to_vec()).expect("jsonb text is utf-8")
	}

	/// A schemaless `person:tobie` whose fields cover the shapes a client meets: scalars,
	/// flat and nested arrays, objects, record links, a datetime and a NULL.
	async fn seed_person_with_json_and_arrays(port: u16) {
		seed(
			port,
			"DEFINE TABLE person SCHEMALESS; \
			 CREATE person:tobie CONTENT { name: 'Tobie', tags: ['rust', 'db'], \
			 scores: [1, 2, 3], mixed: [1, 'a', true], \
			 profile: { city: 'London', langs: ['en', 'fr'], geo: { lat: 51.5, lon: -0.12 } }, \
			 friends: [person:jaime, person:silvana], objs: [{ a: 1 }, { b: 2 }], \
			 joined: [1, NONE, 3], empty: [], born: d'1980-01-01T00:00:00Z', nothing: NULL, \
			 quoted: ['a,b', 'say \"hi\"', 'NULL', ''] } RETURN NONE; \
			 CREATE person:grid CONTENT { name: 'Grid', grid: [[1, 2], [3, 4]], \
			 jagged: [[1], [2, 3]] } RETURN NONE",
		)
		.await;
	}

	fn cell<'a>(row: &'a SimpleQueryRow, name: &str) -> Option<&'a str> {
		let idx = row
			.columns()
			.iter()
			.position(|c| c.name() == name)
			.unwrap_or_else(|| panic!("no column {name}"));
		row.get(idx)
	}

	#[test(tokio::test)]
	async fn pgsql_dialect_select_star_over_a_schemaless_table_returns_its_columns()
	-> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		seed_person_with_json_and_arrays(port).await;
		let found = rows(client.simple_query("SELECT * FROM person WHERE name = 'Tobie'").await?);
		assert_eq!(found.len(), 1);
		let names: Vec<&str> = found[0].columns().iter().map(|c| c.name()).collect();
		assert!(!names.contains(&"*"), "the wildcard must not surface as a column: {names:?}");
		for expected in ["name", "tags", "profile", "born", "friends", "nothing"] {
			assert!(names.contains(&expected), "missing column {expected} in {names:?}");
		}
		assert_eq!(cell(&found[0], "name"), Some("Tobie"));
		assert_eq!(cell(&found[0], "nothing"), None);
		let all = rows(client.simple_query("SELECT * FROM person ORDER BY name").await?);
		assert_eq!(all.len(), 2);
		Ok(())
	}

	#[test(tokio::test)]
	async fn pgsql_dialect_select_star_in_the_extended_protocol_is_one_jsonb_document()
	-> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		seed_person_with_json_and_arrays(port).await;
		let found = client.query("SELECT * FROM person WHERE name = 'Tobie'", &[]).await?;
		assert_eq!(found.len(), 1);
		assert_eq!(found[0].columns().len(), 1);
		let doc = jsonb_text(&found[0].try_get::<_, RawCell>(0)?);
		assert!(
			doc.contains("\"name\":\"Tobie\"") && doc.contains("\"tags\":[\"rust\",\"db\"]"),
			"{doc}"
		);
		Ok(())
	}

	#[test(tokio::test)]
	async fn pgsql_dialect_reads_arrays_objects_and_datetimes_over_both_protocols()
	-> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		seed_person_with_json_and_arrays(port).await;
		let expected: &[(&str, &str)] = &[
			("tags", "{rust,db}"),
			("scores", "{1,2,3}"),
			("mixed", "{1,\"\\\"a\\\"\",true}"),
			(
				"profile",
				"{\"city\":\"London\",\"geo\":{\"lat\":51.5,\"lon\":-0.12},\"langs\":[\"en\",\"fr\"]}",
			),
			("friends", "{jaime,silvana}"),
			("objs", "{\"{\\\"a\\\":1}\",\"{\\\"b\\\":2}\"}"),
			("joined", "{1,NULL,3}"),
			("empty", "{}"),
			("born", "1980-01-01 00:00:00+00"),
		];
		for (column, text) in expected {
			let sql = format!("SELECT {column} FROM person WHERE name = 'Tobie'");
			let simple = rows(client.simple_query(&sql).await?);
			assert_eq!(simple.len(), 1, "{sql}");
			assert_eq!(simple[0].get(0), Some(*text), "simple protocol, {sql}");
			let extended = client.query(&sql, &[]).await?;
			assert_eq!(extended.len(), 1, "{sql}");
			let value: String = extended[0].try_get(0)?;
			assert_eq!(value, *text, "extended protocol, {sql}");
		}
		let null = client.query("SELECT nothing FROM person WHERE name = 'Tobie'", &[]).await?;
		assert_eq!(null[0].try_get::<_, Option<String>>(0)?, None);
		let grid = client.query("SELECT grid FROM person WHERE name = 'Grid'", &[]).await?;
		assert_eq!(grid[0].try_get::<_, String>(0)?, "[[1,2],[3,4]]");
		// A named column and the same field under `SELECT *` carry one text.
		let star = rows(client.simple_query("SELECT * FROM person WHERE name = 'Tobie'").await?);
		assert_eq!(star.len(), 1);
		for (column, text) in expected {
			assert_eq!(cell(&star[0], column), Some(*text), "SELECT *, {column}");
		}
		Ok(())
	}

	#[test(tokio::test)]
	async fn pgsql_dialect_refuses_json_containment_instead_of_answering_wrongly()
	-> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		seed_person_with_json_and_arrays(port).await;
		let err = client
			.simple_query("SELECT name FROM person WHERE profile @> '{\"city\":\"London\"}'")
			.await
			.expect_err("containment is not implemented");
		let message = err.as_db_error().map(|d| d.message().to_string()).unwrap_or_default();
		assert!(message.contains("containment"), "{message}");
		assert_eq!(err.code(), Some(&SqlState::FEATURE_NOT_SUPPORTED));
		let back = rows(client.simple_query("SELECT name FROM person WHERE name = 'Tobie'").await?);
		assert_eq!(back.len(), 1, "the connection stays usable");
		Ok(())
	}

	#[test(tokio::test)]
	async fn pgsql_dialect_reports_a_refused_json_operator_with_its_sqlstate()
	-> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		seed_person_with_json_and_arrays(port).await;
		let malformed = "SELECT profile #>> '{city' FROM person";
		let err = client.simple_query(malformed).await.expect_err("a malformed path");
		assert_eq!(err.code(), Some(&SqlState::INVALID_TEXT_REPRESENTATION), "{err:?}");
		let err = client.simple_query("SELECT profile #> name FROM person").await.unwrap_err();
		assert_eq!(err.code(), Some(&SqlState::FEATURE_NOT_SUPPORTED), "{err:?}");
		// Parse refuses a SELECT the planner cannot lower, before anything is bound.
		let err = client.prepare(malformed).await.expect_err("a malformed path");
		assert_eq!(err.code(), Some(&SqlState::INVALID_TEXT_REPRESENTATION), "{err:?}");
		let err = client
			.prepare("SELECT name FROM person WHERE profile @> '{\"city\":\"London\"}'")
			.await
			.expect_err("containment");
		assert_eq!(err.code(), Some(&SqlState::FEATURE_NOT_SUPPORTED), "{err:?}");
		let back = client.query("SELECT name FROM person WHERE name = 'Tobie'", &[]).await?;
		assert_eq!(back.len(), 1, "the connection stays usable");
		Ok(())
	}

	#[test(tokio::test)]
	async fn pgsql_dialect_extracts_json_text_and_paths() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		seed_person_with_json_and_arrays(port).await;
		for (select, expected) in [
			("profile ->> 'city'", Some("London")),
			("profile #>> '{geo,lat}'", Some("51.5")),
			("profile #>> '{langs,1}'", Some("fr")),
			("profile ->> 'langs'", Some("[\"en\",\"fr\"]")),
			("profile ->> 'missing'", None),
			("profile #>> '{geo,lon,x}'", None),
		] {
			let sql = format!("SELECT {select} AS v FROM person WHERE name = 'Tobie'");
			let found = rows(client.simple_query(&sql).await?);
			assert_eq!(found.len(), 1, "{sql}");
			assert_eq!(found[0].get(0), expected, "{sql}");
		}
		Ok(())
	}

	/// `dept` links to `dept:1`, `friends` and `meta.boss` link to other employees.
	async fn seed_employees_with_record_links(port: u16) {
		seed(
			port,
			"DEFINE TABLE emp SCHEMALESS; \
			 CREATE emp:7 CONTENT { name: 'Eve', dept: dept:1, friends: [emp:8, emp:9], \
			 meta: { boss: emp:8, tags: ['a'] }, handle: emp:tobie } RETURN NONE",
		)
		.await;
	}

	#[test(tokio::test)]
	async fn sql_dialects_show_every_record_id_as_its_key() -> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		seed_employees_with_record_links(port).await;
		for dialect in ["pgsql"] {
			let sql = "SELECT id, dept, friends, meta, handle FROM emp WHERE name = 'Eve'";
			let found = rows(client.simple_query(sql).await?);
			assert_eq!(found.len(), 1, "{dialect}");
			assert_eq!(cell(&found[0], "id"), Some("7"), "{dialect}");
			assert_eq!(cell(&found[0], "dept"), Some("1"), "{dialect}");
			assert_eq!(cell(&found[0], "friends"), Some("{8,9}"), "{dialect}");
			assert_eq!(cell(&found[0], "meta"), Some("{\"boss\":8,\"tags\":[\"a\"]}"), "{dialect}");
			assert_eq!(cell(&found[0], "handle"), Some("tobie"), "{dialect}");
		}
		Ok(())
	}

	#[test(tokio::test)]
	async fn sql_dialects_show_record_ids_as_keys_over_the_extended_protocol()
	-> Result<(), Box<dyn Error>> {
		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		seed_employees_with_record_links(port).await;
		let found =
			client.query("SELECT id, dept, friends FROM emp WHERE name = 'Eve'", &[]).await?;
		assert_eq!(found.len(), 1);
		assert_eq!(found[0].try_get::<_, String>(0)?, "7");
		assert_eq!(found[0].try_get::<_, String>(1)?, "1");
		assert_eq!(found[0].try_get::<_, String>(2)?, "{8,9}");
		Ok(())
	}

	async fn seed_people_for_key_parameters(port: u16) {
		seed(
			port,
			"DEFINE TABLE person SCHEMALESS; \
			 CREATE person:1 SET name = 'Tobie' RETURN NONE; \
			 CREATE person:tobie SET name = 'Named' RETURN NONE; \
			 CREATE person:3 SET name = 'Third' RETURN NONE",
		)
		.await;
	}

	#[test(tokio::test)]
	async fn pgsql_dialect_integer_key_parameters_match_the_record_they_name()
	-> Result<(), Box<dyn Error>> {
		use tokio_postgres::types::Type;

		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		seed_people_for_key_parameters(port).await;
		let select =
			client.prepare_typed("SELECT name FROM person WHERE id = $1", &[Type::INT8]).await?;
		let found = client.query(&select, &[&1i64]).await?;
		assert_eq!(found.len(), 1);
		assert_eq!(found[0].try_get::<_, String>(0)?, "Tobie");
		assert!(client.query(&select, &[&99i64]).await?.is_empty());
		let nothing: Option<i64> = None;
		assert!(client.query(&select, &[&nothing]).await?.is_empty(), "NULL names no record");
		let members = client
			.prepare_typed(
				"SELECT name FROM person WHERE id IN ($1, $2) ORDER BY name",
				&[Type::INT8, Type::INT8],
			)
			.await?;
		let found = client.query(&members, &[&1i64, &3i64]).await?;
		let names: Vec<String> = found.iter().map(|r| r.get(0)).collect();
		assert_eq!(names, ["Third", "Tobie"]);
		let update = client
			.prepare_typed("UPDATE person SET name = 'Changed' WHERE id = $1", &[Type::INT8])
			.await?;
		assert_eq!(client.execute(&update, &[&3i64]).await?, 1);
		let delete =
			client.prepare_typed("DELETE FROM person WHERE id = $1", &[Type::INT8]).await?;
		assert_eq!(client.execute(&delete, &[&1i64]).await?, 1);
		assert_eq!(client.execute(&delete, &[&1i64]).await?, 0);
		Ok(())
	}

	#[test(tokio::test)]
	async fn pgsql_dialect_text_key_parameters_match_the_record_they_name_and_empty_text_names_none()
	-> Result<(), Box<dyn Error>> {
		use tokio_postgres::types::Type;

		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		seed_people_for_key_parameters(port).await;
		let select =
			client.prepare_typed("SELECT name FROM person WHERE id = $1", &[Type::TEXT]).await?;
		let found = client.query(&select, &[&"tobie"]).await?;
		assert_eq!(found.len(), 1);
		assert_eq!(found[0].try_get::<_, String>(0)?, "Named");
		assert!(client.query(&select, &[&""]).await?.is_empty(), "an empty key names no record");
		let nothing: Option<&str> = None;
		assert!(client.query(&select, &[&nothing]).await?.is_empty());
		let delete =
			client.prepare_typed("DELETE FROM person WHERE id = $1", &[Type::TEXT]).await?;
		assert_eq!(client.execute(&delete, &[&""]).await?, 0, "an empty key deletes nothing");
		assert_eq!(client.execute(&delete, &[&"tobie"]).await?, 1);
		Ok(())
	}

	#[test(tokio::test)]
	async fn pgsql_dialect_key_parameters_of_other_kinds_name_no_record()
	-> Result<(), Box<dyn Error>> {
		use tokio_postgres::types::Type;

		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		seed(
			port,
			"DEFINE TABLE price SCHEMALESS; \
			 CREATE price:⟨1.5⟩ SET name = 'float-looking text key' RETURN NONE; \
			 CREATE price:⟨true⟩ SET name = 'bool-looking text key' RETURN NONE",
		)
		.await;
		let by_float =
			client.prepare_typed("SELECT name FROM price WHERE id = $1", &[Type::FLOAT8]).await?;
		assert!(client.query(&by_float, &[&1.5f64]).await?.is_empty(), "a float is no record key");
		let by_bool =
			client.prepare_typed("SELECT name FROM price WHERE id = $1", &[Type::BOOL]).await?;
		assert!(client.query(&by_bool, &[&true]).await?.is_empty(), "a boolean is no record key");
		let delete =
			client.prepare_typed("DELETE FROM price WHERE id = $1", &[Type::FLOAT8]).await?;
		assert_eq!(client.execute(&delete, &[&1.5f64]).await?, 0, "a float deletes no record");
		let by_text =
			client.prepare_typed("SELECT name FROM price WHERE id = $1", &[Type::TEXT]).await?;
		assert_eq!(client.query(&by_text, &[&"1.5"]).await?.len(), 1, "text is the string key");
		Ok(())
	}

	async fn seed_employees_with_link_field(port: u16) {
		seed(
			port,
			"DEFINE TABLE dept SCHEMALESS; \
			 CREATE dept:1 SET dname = 'Engineering' RETURN NONE; \
			 CREATE dept:ops SET dname = 'Operations' RETURN NONE; \
			 DEFINE TABLE emp SCHEMAFULL; \
			 DEFINE FIELD name ON emp TYPE string; \
			 DEFINE FIELD dept ON emp TYPE record<dept>",
		)
		.await;
	}

	#[test(tokio::test)]
	async fn pgsql_dialect_writes_a_key_parameter_into_a_link_field_and_finds_it_again()
	-> Result<(), Box<dyn Error>> {
		use tokio_postgres::types::Type;

		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		seed_employees_with_link_field(port).await;
		let insert = client
			.prepare_typed(
				"INSERT INTO emp (id, name, dept) VALUES ($1, $2, $3)",
				&[Type::INT8, Type::TEXT, Type::INT8],
			)
			.await?;
		assert_eq!(client.execute(&insert, &[&1i64, &"Eve", &1i64]).await?, 1);
		let by_text = client
			.prepare_typed(
				"INSERT INTO emp (id, name, dept) VALUES ($1, $2, $3)",
				&[Type::INT8, Type::TEXT, Type::TEXT],
			)
			.await?;
		assert_eq!(client.execute(&by_text, &[&2i64, &"Ora", &"ops"]).await?, 1);
		let select =
			client.prepare_typed("SELECT name FROM emp WHERE dept = $1", &[Type::INT8]).await?;
		let found = client.query(&select, &[&1i64]).await?;
		assert_eq!(found.len(), 1);
		assert_eq!(found[0].try_get::<_, String>(0)?, "Eve");
		assert!(client.query(&select, &[&99i64]).await?.is_empty());
		let nothing: Option<i64> = None;
		assert!(client.query(&select, &[&nothing]).await?.is_empty(), "NULL names no record");
		let select_text =
			client.prepare_typed("SELECT name FROM emp WHERE dept = $1", &[Type::TEXT]).await?;
		let found = client.query(&select_text, &[&"ops"]).await?;
		assert_eq!(found.len(), 1);
		assert_eq!(found[0].try_get::<_, String>(0)?, "Ora");
		let update = client
			.prepare_typed("UPDATE emp SET dept = $1 WHERE id = $2", &[Type::INT8, Type::INT8])
			.await?;
		assert_eq!(client.execute(&update, &[&1i64, &2i64]).await?, 1);
		assert_eq!(client.query(&select, &[&1i64]).await?.len(), 2);
		let shown = client.query("SELECT dept FROM emp WHERE id = 2", &[]).await?;
		assert_eq!(shown[0].try_get::<_, String>(0)?, "1", "SQL shows the key of the link");
		let linked = query_http(port, "SELECT VALUE dept.dname FROM emp:2").await;
		assert!(linked.contains("Engineering"), "the stored value is a record link: {linked}");
		Ok(())
	}

	#[test(tokio::test)]
	async fn pgsql_dialect_writes_a_link_field_inside_a_transaction() -> Result<(), Box<dyn Error>>
	{
		use tokio_postgres::types::Type;

		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		seed_employees_with_link_field(port).await;
		client.simple_query("BEGIN").await?;
		let insert = client
			.prepare_typed(
				"INSERT INTO emp (id, name, dept) VALUES ($1, $2, $3)",
				&[Type::INT8, Type::TEXT, Type::INT8],
			)
			.await?;
		assert_eq!(client.execute(&insert, &[&1i64, &"Eve", &1i64]).await?, 1);
		let inside = client.query("SELECT name FROM emp WHERE dept = 1", &[]).await?;
		assert_eq!(inside.len(), 1, "the transaction sees its own write by key");
		client.simple_query("ROLLBACK").await?;
		assert!(client.query("SELECT name FROM emp WHERE dept = 1", &[]).await?.is_empty());
		Ok(())
	}

	#[test(tokio::test)]
	async fn pgsql_dialect_refuses_what_is_not_a_key_for_a_link_field() -> Result<(), Box<dyn Error>>
	{
		use tokio_postgres::types::Type;

		let (port, _server) = start_pg_server(true).await?;
		let client = connect_root(port).await?;
		seed_employees_with_link_field(port).await;
		let by_float = client
			.prepare_typed(
				"INSERT INTO emp (id, name, dept) VALUES ($1, $2, $3)",
				&[Type::INT8, Type::TEXT, Type::FLOAT8],
			)
			.await?;
		assert!(client.execute(&by_float, &[&1i64, &"Bad", &1.5f64]).await.is_err());
		let by_text = client
			.prepare_typed(
				"INSERT INTO emp (id, name, dept) VALUES ($1, $2, $3)",
				&[Type::INT8, Type::TEXT, Type::TEXT],
			)
			.await?;
		assert!(client.execute(&by_text, &[&1i64, &"Bad", &""]).await.is_err());
		let nothing: Option<&str> = None;
		assert!(client.execute(&by_text, &[&1i64, &"Bad", &nothing]).await.is_err());
		assert!(client.query("SELECT name FROM emp", &[]).await?.is_empty());
		Ok(())
	}
}
