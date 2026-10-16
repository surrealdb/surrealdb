mod common;

#[cfg(feature = "surrealism")]
mod surrealism_integration {

	use std::collections::HashMap;
	use std::io::{Read, Write};
	use std::net::{Ipv4Addr, Shutdown, SocketAddr, TcpListener, TcpStream};
	use std::path::{Path, PathBuf};
	use std::process::Command;
	use std::sync::{LazyLock, mpsc};
	use std::thread;
	use std::time::{Duration, Instant};

	use serde::Deserialize;
	use surrealism_runtime::config::{
		AbiVersion, SurrealismAttach, SurrealismConfig, SurrealismMeta, Target,
	};
	use surrealism_runtime::package::SurrealismPackage;
	use test_log::test;
	use ulid::Ulid;

	use super::*;

	#[derive(Deserialize, Debug)]
	struct QueryResult {
		result: serde_json::Value,
		status: String,
	}

	fn has_wasm_target(target: &str) -> bool {
		Command::new("rustup")
			.args(["target", "list", "--installed"])
			.output()
			.map(|o| String::from_utf8_lossy(&o.stdout).lines().any(|l| l.trim() == target))
			.unwrap_or(false)
	}

	/// Path to the `surreal` CLI binary (built alongside integration tests).
	///
	/// Uses `CARGO_BIN_EXE_surreal` when present (set by `cargo test` / cargo-nextest) so the path
	/// stays valid under nextest's per-test directory layout. Falls back to deriving
	/// `target/debug/surreal` from `std::env::current_exe()` for runners that omit it.
	fn surreal_bin() -> PathBuf {
		if let Some(p) = std::env::var_os("CARGO_BIN_EXE_surreal")
			.or_else(|| std::env::var_os("NEXTEST_BIN_EXE_surreal"))
		{
			return PathBuf::from(p);
		}

		let mut path = std::env::current_exe().expect("Failed to get current exe path");
		assert!(path.pop());
		if path.ends_with("deps") {
			assert!(path.pop());
		}
		path.push(format!("{}{}", env!("CARGO_PKG_NAME"), std::env::consts::EXE_SUFFIX));
		path
	}

	/// Build and pack the demo module using `surreal module build`. The build
	/// logs at `warn` whatever `SURREAL_LOG` the test inherits: at `full`,
	/// compiling the module logs every wasmtime and cranelift pass.
	fn build_and_pack_demo(output_dir: &Path) {
		let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"));
		let demo_dir = workspace_root.join("surrealism/demo");
		let output = output_dir.join("demo.surli");

		let result = Command::new(surreal_bin())
			.args([
				"module",
				"build",
				"--log",
				"warn",
				"--debug",
				"-o",
				&output.to_string_lossy(),
				&demo_dir.to_string_lossy(),
			])
			.current_dir(workspace_root)
			.output()
			.expect("Failed to execute surreal module build");

		assert!(
			result.status.success(),
			"surreal module build failed.\nstdout: {}\nstderr: {}",
			String::from_utf8_lossy(&result.stdout),
			String::from_utf8_lossy(&result.stderr),
		);

		assert!(output.exists(), "demo.surli not created at {}", output.display());
	}

	/// Environment variable naming a `demo.surli` built once for the whole run
	/// by the nextest setup script `.config/scripts/build-demo-module.sh`.
	const PREBUILT_DEMO_ENV: &str = "SURREALISM_DEMO_SURLI";

	/// Copy the run-wide prebuilt `demo.surli` into `output_dir`.
	///
	/// Returns `false` when no usable prebuilt archive is available (the
	/// variable is unset, as under plain `cargo test`, or it points at a
	/// missing or empty file), in which case the caller builds the module. The
	/// copy keeps each process's directory private, so tests never share
	/// mutable files.
	fn copy_prebuilt_demo(output_dir: &Path) -> bool {
		let Some(src) = std::env::var_os(PREBUILT_DEMO_ENV).map(PathBuf::from) else {
			return false;
		};
		if !std::fs::metadata(&src).map(|m| m.len() > 0).unwrap_or(false) {
			return false;
		}
		std::fs::copy(&src, output_dir.join("demo.surli")).is_ok()
	}

	struct DemoModuleDir {
		_tmp: tempfile::TempDir,
		/// Canonical (symlink-resolved) path. On macOS `/var` is a symlink to
		/// `/private/var`, so the raw TempDir path would be rejected by
		/// `SURREAL_BUCKET_FOLDER_ALLOWLIST`.
		canonical: PathBuf,
	}

	fn build_demo_dir() -> DemoModuleDir {
		let target = "wasm32-wasip2";
		if !has_wasm_target(target) {
			panic!("{target} target not installed — install with: rustup target add {target}");
		}
		let tmp = tempfile::TempDir::new().expect("Failed to create temp dir for demo module");
		let canonical =
			std::fs::canonicalize(tmp.path()).expect("Failed to canonicalize temp dir path");
		if !copy_prebuilt_demo(&canonical) {
			build_and_pack_demo(&canonical);
		}
		repack_without_localhost(&canonical);
		DemoModuleDir {
			_tmp: tmp,
			canonical,
		}
	}

	/// The demo, repacked with `localhost` taken out of the network request in
	/// its package file, as `restricted.surli`.
	///
	/// The same component, so it runs exactly like `demo.surli`, but its
	/// MANIFEST, and so its DIGEST, differ: the trust tests use it as a second
	/// archive. The request it changes grants nothing; only a definition's
	/// `CAPABILITIES` does. Repacked from the built archive rather than built
	/// from a second crate, so it costs no second wasm build.
	fn repack_without_localhost(output_dir: &Path) {
		use surrealism_runtime::package::UnpackOptions;

		let surli = std::fs::read(output_dir.join("demo.surli")).expect("read demo.surli");
		let mut package =
			SurrealismPackage::from_reader(std::io::Cursor::new(surli), &UnpackOptions::default())
				.expect("unpack demo.surli");
		package.config.capabilities.allow_net = vec!["127.0.0.1".to_string()];
		// No attached filesystem is packed here, so drop the declaration with it.
		package.config.attach.fs = None;
		package.pack(output_dir.join("restricted.surli"), None).expect("pack restricted.surli");
	}

	static DEMO_DIR: LazyLock<DemoModuleDir> = LazyLock::new(build_demo_dir);

	/// Start a SurrealDB server with the `files` and `surrealism` experimental
	/// capabilities enabled, and a bucket folder allowlist pointing at the given
	/// directory.
	async fn start_surrealism_server(
		bucket_dir: &Path,
	) -> Result<(String, common::Child), Box<dyn std::error::Error>> {
		start_surrealism_server_with(bucket_dir, HashMap::new()).await
	}

	/// As [`start_surrealism_server`], with extra environment variables merged
	/// in — used to redirect silo resolution at a local registry.
	async fn start_surrealism_server_with(
		bucket_dir: &Path,
		extra: HashMap<String, String>,
	) -> Result<(String, common::Child), Box<dyn std::error::Error>> {
		start_surrealism_server_full(bucket_dir, extra, "").await
	}

	/// As [`start_surrealism_server_with`], with extra command-line arguments —
	/// used to give one server a deny list the others do not have.
	async fn start_surrealism_server_full(
		bucket_dir: &Path,
		extra: HashMap<String, String>,
		extra_args: &str,
	) -> Result<(String, common::Child), Box<dyn std::error::Error>> {
		let mut vars = HashMap::new();
		vars.insert(
			"SURREAL_BUCKET_FOLDER_ALLOWLIST".to_string(),
			bucket_dir.to_string_lossy().to_string(),
		);
		vars.extend(extra);

		common::start_server(common::StartServerArguments {
			args: format!(
				"--allow-experimental files,surrealism --allow-net 127.0.0.1,localhost{}",
				if extra_args.is_empty() {
					String::new()
				} else {
					format!(" {extra_args}")
				}
			),
			vars: Some(vars),
			..Default::default()
		})
		.await
	}

	struct LocalPokemonApi {
		addr: SocketAddr,
		shutdown: Option<mpsc::Sender<()>>,
		handle: Option<thread::JoinHandle<()>>,
	}

	impl LocalPokemonApi {
		fn start() -> Result<Self, Box<dyn std::error::Error>> {
			let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
			listener.set_nonblocking(true)?;
			let addr = listener.local_addr()?;
			let (shutdown_tx, shutdown_rx) = mpsc::channel();
			let handle = thread::spawn(move || run_local_pokemon_api(listener, shutdown_rx));

			Ok(Self {
				addr,
				shutdown: Some(shutdown_tx),
				handle: Some(handle),
			})
		}

		fn base_url(&self) -> String {
			format!("http://{}", self.addr)
		}
	}

	impl Drop for LocalPokemonApi {
		fn drop(&mut self) {
			if let Some(shutdown) = self.shutdown.take() {
				let _ = shutdown.send(());
			}
			if let Some(handle) = self.handle.take() {
				let _ = handle.join();
			}
		}
	}

	fn run_local_pokemon_api(listener: TcpListener, shutdown_rx: mpsc::Receiver<()>) {
		loop {
			if shutdown_rx.try_recv().is_ok() {
				return;
			}

			match listener.accept() {
				// Handle each connection on its own thread so a slow or partial
				// client never stalls the accept loop (which also polls the
				// shutdown channel between connections).
				Ok((stream, _)) => {
					// The accepted socket inherits the listener's non-blocking flag
					// on BSD-derived platforms, and a non-blocking `write_all` that
					// meets a full send buffer stops short at `WouldBlock` and serves
					// a truncated body. These responses are small enough to usually
					// fit, which makes the truncation load-dependent rather than
					// absent.
					if stream.set_nonblocking(false).is_err() {
						continue;
					}
					thread::spawn(move || handle_pokemon_connection(stream));
				}
				Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
					thread::sleep(Duration::from_millis(10));
				}
				Err(_) => return,
			}
		}
	}

	/// Serve a single request on an accepted connection.
	///
	/// The guest writes its request line and headers as several small writes,
	/// which arrive as several TCP segments. We must drain the full request head
	/// (up to the terminating `\r\n\r\n`) before replying: responding after a
	/// single partial read and then closing a socket that still has inbound bytes
	/// makes the kernel reset the connection, which surfaces in the guest as a
	/// write failure ("Broken pipe" / "I/O error"). Reading to the end of the
	/// headers first, then half-closing the write side, lets the client read a
	/// clean EOF.
	fn handle_pokemon_connection(mut stream: TcpStream) {
		// Bound the handler so a misbehaving client can't wedge the thread.
		let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));

		let mut buf = Vec::with_capacity(1024);
		let mut chunk = [0u8; 1024];
		loop {
			match stream.read(&mut chunk) {
				Ok(0) => break, // client closed early
				Ok(n) => {
					buf.extend_from_slice(&chunk[..n]);
					// GET requests have no body, so the header terminator marks
					// the end of everything the client will send.
					if buf.windows(4).any(|w| w == b"\r\n\r\n") {
						break;
					}
				}
				Err(_) => return, // timeout or read error: drop quietly
			}
		}

		let request = String::from_utf8_lossy(&buf);
		let path =
			request.lines().next().and_then(|line| line.split_whitespace().nth(1)).unwrap_or("/");

		let (status, reason, body) = match path {
			"/pokemon/pikachu" => (200, "OK", serde_json::json!({ "name": "pikachu" })),
			"/pokemon/1" => (200, "OK", serde_json::json!({ "name": "bulbasaur" })),
			_ => (404, "Not Found", serde_json::json!({ "error": "not found" })),
		};
		let body = body.to_string();
		let response = format!(
			"HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
			body.len()
		);
		let _ = stream.write_all(response.as_bytes());
		let _ = stream.flush();
		// Half-close the write side so the client sees EOF cleanly; the read
		// half is already drained, so the final close won't trigger an RST.
		let _ = stream.shutdown(Shutdown::Write);
	}

	/// A stand-in for the Silo package host, serving one `.surli` at the path a
	/// `silo::` executable resolves to.
	struct LocalSiloRegistry {
		addr: SocketAddr,
		shutdown: Option<mpsc::Sender<()>>,
		handle: Option<thread::JoinHandle<()>>,
	}

	impl LocalSiloRegistry {
		/// Serve `package` at `/{org}/{pkg}/{version}.surli`; every other path
		/// 404s, so a wrong version or name is a miss rather than a silent hit.
		fn start(path: &str, package: Vec<u8>) -> Result<Self, Box<dyn std::error::Error>> {
			let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
			listener.set_nonblocking(true)?;
			let addr = listener.local_addr()?;
			let (shutdown_tx, shutdown_rx) = mpsc::channel();
			let path = path.to_string();
			let handle = thread::spawn(move || {
				run_local_silo_registry(listener, shutdown_rx, path, package)
			});

			Ok(Self {
				addr,
				shutdown: Some(shutdown_tx),
				handle: Some(handle),
			})
		}

		fn endpoint(&self) -> String {
			format!("http://{}", self.addr)
		}
	}

	impl Drop for LocalSiloRegistry {
		fn drop(&mut self) {
			if let Some(shutdown) = self.shutdown.take() {
				let _ = shutdown.send(());
			}
			if let Some(handle) = self.handle.take() {
				let _ = handle.join();
			}
		}
	}

	fn run_local_silo_registry(
		listener: TcpListener,
		shutdown_rx: mpsc::Receiver<()>,
		path: String,
		package: Vec<u8>,
	) {
		loop {
			if shutdown_rx.try_recv().is_ok() {
				return;
			}

			match listener.accept() {
				Ok((stream, _)) => {
					// The accepted socket inherits the listener's non-blocking flag on
					// BSD-derived platforms. A package is far larger than the send
					// buffer, so a non-blocking `write_all` would stop short at
					// `WouldBlock` and serve a truncated body.
					if stream.set_nonblocking(false).is_err() {
						continue;
					}
					let path = path.clone();
					let package = package.clone();
					thread::spawn(move || handle_silo_connection(stream, &path, &package));
				}
				Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
					thread::sleep(Duration::from_millis(10));
				}
				Err(_) => return,
			}
		}
	}

	/// Serve a single request. Drains the full request head before replying, for
	/// the same reason [`handle_pokemon_connection`] does.
	fn handle_silo_connection(mut stream: TcpStream, package_path: &str, package: &[u8]) {
		let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));

		let mut buf = Vec::with_capacity(1024);
		let mut chunk = [0u8; 1024];
		loop {
			match stream.read(&mut chunk) {
				Ok(0) => break,
				Ok(n) => {
					buf.extend_from_slice(&chunk[..n]);
					if buf.windows(4).any(|w| w == b"\r\n\r\n") {
						break;
					}
				}
				Err(_) => return,
			}
		}

		let request = String::from_utf8_lossy(&buf);
		let path =
			request.lines().next().and_then(|line| line.split_whitespace().nth(1)).unwrap_or("/");

		let response = if path == package_path {
			let mut head = format!(
				"HTTP/1.1 200 OK\r\ncontent-type: application/octet-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
				package.len()
			)
			.into_bytes();
			head.extend_from_slice(package);
			head
		} else {
			b"HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".to_vec()
		};

		let _ = stream.write_all(&response);
		let _ = stream.flush();
		let _ = stream.shutdown(Shutdown::Write);
	}

	/// Execute one or more SurrealQL statements via the HTTP `/sql` endpoint and
	/// return the parsed results.
	async fn sql_query(addr: &str, ns: &str, db: &str, query: &str) -> Vec<QueryResult> {
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_secs(5))
			.build()
			.expect("Failed to build HTTP client");

		let res = client
			.post(format!("http://{addr}/sql"))
			.basic_auth(common::USER, Some(common::PASS))
			.header("surreal-ns", ns)
			.header("surreal-db", db)
			.header("Accept", "application/json")
			.body(query.to_string())
			.send()
			.await
			.unwrap_or_else(|e| panic!("HTTP request failed: {e}"));

		let status = res.status();
		let body = res.text().await.expect("Failed to read response body");
		assert!(status.is_success(), "HTTP {status} for query: {query}\nbody: {body}");

		serde_json::from_str(&body)
			.unwrap_or_else(|e| panic!("Failed to parse response JSON: {e}\nbody: {body}"))
	}

	/// What the demo is granted: every function family its exports call, its
	/// own exports through `tx.run`, arbitrary queries, writes, and the local
	/// test listeners by name and by address.
	///
	/// `localhost` is matched by name by the DNS shim; `127.0.0.1` is the
	/// explicit IP rule that readmits the loopback address the name resolves
	/// to, which the shim would otherwise reject as special.
	const DEMO_CAPABILITIES: &str = "CAPABILITIES { net: [\"localhost\", \"127.0.0.1\"], \
		functions: [\"fn::*\", \"file::*\", \"sequence::*\", \"ext::*\"], queries: true, write: true }";

	/// Run the DEFINE BUCKET + DEFINE EXTENSION setup queries for a fresh
	/// namespace/database. The tests call the extension through the `mod::`
	/// alias, so every call also covers the alias resolving an `ext::`
	/// definition.
	async fn setup_module(addr: &str, ns: &str, db: &str, bucket_dir: &Path) {
		setup_named_module(addr, ns, db, bucket_dir, "demo", "demo.surli", DEMO_CAPABILITIES).await;
	}

	/// Define one module in the bucket under a chosen name and archive,
	/// granted `capabilities`, a `CAPABILITIES { … }` clause.
	async fn setup_named_module(
		addr: &str,
		ns: &str,
		db: &str,
		bucket_dir: &Path,
		module: &str,
		archive: &str,
		capabilities: &str,
	) {
		let dir = bucket_dir.to_string_lossy();
		let setup = format!(
			"DEFINE BUCKET IF NOT EXISTS test BACKEND \"file:{dir}\";\
			 DEFINE EXTENSION ext::{module} FROM f\"test:/{archive}\" UNSIGNED {capabilities};"
		);
		let results = sql_query(addr, ns, db, &setup).await;
		for (i, r) in results.iter().enumerate() {
			assert_eq!(r.status, "OK", "Setup statement {i} failed: {:?}", r.result);
		}
	}

	async fn check_function_calls(bucket_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		setup_module(&addr, &ns, &db, bucket_dir).await;

		// can_drive(21) -> true (age >= 18)
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::can_drive(21);").await;
		assert_eq!(results[0].status, "OK", "can_drive(21): {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::Value::Bool(true));

		// can_drive(15) -> false (age < 18)
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::can_drive(15);").await;
		assert_eq!(results[0].status, "OK", "can_drive(15): {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::Value::Bool(false));

		// safe_divide(10, 2) -> 5
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::safe_divide(10, 2);").await;
		assert_eq!(results[0].status, "OK", "safe_divide(10,2): {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!(5));

		// safe_divide(10, 0) -> error (division by zero)
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::safe_divide(10, 0);").await;
		assert_eq!(results[0].status, "ERR", "Expected error for division by zero");

		// Named export: other(21) -> true
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::other(21);").await;
		assert_eq!(results[0].status, "OK", "other(21): {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::Value::Bool(true));

		// Default export: mod::demo(21) -> true
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo(21);").await;
		assert_eq!(results[0].status, "OK", "default(21): {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::Value::Bool(true));

		Ok(())
	}

	async fn check_result_type_handling(
		bucket_dir: &Path,
	) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		setup_module(&addr, &ns, &db, bucket_dir).await;

		// result(false) -> Ok("Success")
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::result(false);").await;
		assert_eq!(results[0].status, "OK", "result(false): {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!("Success"));

		// result(true) -> Err("Failed") propagated as module error
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::result(true);").await;
		assert_eq!(results[0].status, "ERR", "Expected error from result(true)");

		// parse_number("42") -> Ok(42)
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::parse_number('42');").await;
		assert_eq!(results[0].status, "OK", "parse_number('42'): {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!(42));

		// parse_number("not_a_number") -> Err
		let results =
			sql_query(&addr, &ns, &db, "RETURN mod::demo::parse_number('not_a_number');").await;
		assert_eq!(results[0].status, "ERR", "Expected error from parse_number('not_a_number')");

		Ok(())
	}

	#[test(tokio::test)]
	async fn module_function_calls() -> Result<(), Box<dyn std::error::Error>> {
		check_function_calls(&DEMO_DIR.canonical).await
	}

	#[test(tokio::test)]
	async fn module_result_type_handling() -> Result<(), Box<dyn std::error::Error>> {
		check_result_type_handling(&DEMO_DIR.canonical).await
	}

	// -------------------------------------------------------------------
	// Filesystem attach tests
	// -------------------------------------------------------------------

	async fn check_fs_read(bucket_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		setup_module(&addr, &ns, &db, bucket_dir).await;

		// read_greeting() should return the contents of /greeting.txt
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::read_greeting();").await;
		assert_eq!(results[0].status, "OK", "read_greeting: {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!("Hello from the attached filesystem!"));

		// read_config_version() should parse /data/config.json and return version
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::read_config_version();").await;
		assert_eq!(results[0].status, "OK", "read_config_version: {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!(1));

		// list_fs_root() should list the root directory entries
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::list_fs_root();").await;
		assert_eq!(results[0].status, "OK", "list_fs_root: {:?}", results[0].result);
		let entries = results[0].result.as_array().expect("list_fs_root should return array");
		assert!(
			entries.contains(&serde_json::json!("greeting.txt")),
			"root should contain greeting.txt, got: {entries:?}"
		);
		assert!(
			entries.contains(&serde_json::json!("data")),
			"root should contain data/, got: {entries:?}"
		);

		Ok(())
	}

	#[test(tokio::test)]
	async fn module_fs_read() -> Result<(), Box<dyn std::error::Error>> {
		check_fs_read(&DEMO_DIR.canonical).await
	}

	// -------------------------------------------------------------------
	// Persistent state tests
	// -------------------------------------------------------------------

	async fn check_persistent_state(bucket_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		setup_module(&addr, &ns, &db, bucket_dir).await;

		// Every invocation runs `init` on a fresh instance, which populates the
		// OnceLock that cached_greeting() reads.
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::cached_greeting();").await;
		assert_eq!(results[0].status, "OK", "cached_greeting (1st): {:?}", results[0].result);
		let first_value = results[0].result.clone();
		assert!(first_value.is_string(), "cached_greeting should return a string");

		// Second call: a fresh instance initialises to the same value
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::cached_greeting();").await;
		assert_eq!(results[0].status, "OK", "cached_greeting (2nd): {:?}", results[0].result);
		assert_eq!(
			results[0].result, first_value,
			"init should produce the same state for every invocation"
		);

		// Third call: one more round to be sure
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::cached_greeting();").await;
		assert_eq!(results[0].status, "OK", "cached_greeting (3rd): {:?}", results[0].result);
		assert_eq!(
			results[0].result, first_value,
			"persistent state should be consistent across multiple invocations"
		);

		Ok(())
	}

	#[test(tokio::test)]
	async fn module_persistent_state() -> Result<(), Box<dyn std::error::Error>> {
		check_persistent_state(&DEMO_DIR.canonical).await
	}

	// -------------------------------------------------------------------
	// I/O tests (stdout / stderr piping)
	// -------------------------------------------------------------------

	async fn check_io(bucket_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		setup_module(&addr, &ns, &db, bucket_dir).await;

		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::test_io();").await;
		assert_eq!(results[0].status, "OK", "test_io failed: {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!("I/O test completed"));

		Ok(())
	}

	#[test(tokio::test)]
	async fn module_io() -> Result<(), Box<dyn std::error::Error>> {
		check_io(&DEMO_DIR.canonical).await
	}

	// -------------------------------------------------------------------
	// None value handling
	// -------------------------------------------------------------------

	async fn check_none_value(bucket_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		setup_module(&addr, &ns, &db, bucket_dir).await;

		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::test_none_value();").await;
		assert_eq!(results[0].status, "OK", "test_none_value failed: {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!([null]));

		Ok(())
	}

	#[test(tokio::test)]
	async fn module_none_value() -> Result<(), Box<dyn std::error::Error>> {
		check_none_value(&DEMO_DIR.canonical).await
	}

	// -------------------------------------------------------------------
	// run() cross-function calls + custom struct arguments
	// -------------------------------------------------------------------

	async fn check_run_function(bucket_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		setup_module(&addr, &ns, &db, bucket_dir).await;

		// Define the SurrealQL function that create_user calls via tx.run
		let define_fn =
			"DEFINE FUNCTION fn::user_exists($name: string, $age: int) { RETURN false; };";
		let results = sql_query(&addr, &ns, &db, define_fn).await;
		assert_eq!(results[0].status, "OK", "DEFINE FUNCTION failed: {:?}", results[0].result);

		let results = sql_query(
			&addr,
			&ns,
			&db,
			"RETURN mod::demo::create_user({ name: 'Alice', age: 30, enabled: true });",
		)
		.await;
		assert_eq!(results[0].status, "OK", "create_user failed: {:?}", results[0].result);
		let result_str = results[0].result.as_str().expect("create_user should return a string");
		assert!(
			result_str.contains("Created user Alice"),
			"Expected 'Created user Alice', got: {result_str}"
		);

		Ok(())
	}

	#[test(tokio::test)]
	async fn module_run_function() -> Result<(), Box<dyn std::error::Error>> {
		check_run_function(&DEMO_DIR.canonical).await
	}

	// -------------------------------------------------------------------
	// Module namespace tests (#[surrealism] on mod blocks)
	// -------------------------------------------------------------------

	async fn check_mod_default_export(bucket_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		setup_module(&addr, &ns, &db, bucket_dir).await;

		// math default export: double(5) -> 10
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::math(5);").await;
		assert_eq!(results[0].status, "OK", "math default(5): {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!(10));

		// math default export: double(0) -> 0
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::math(0);").await;
		assert_eq!(results[0].status, "OK", "math default(0): {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!(0));

		// util default export (name override): identity(42) -> 42
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::util(42);").await;
		assert_eq!(results[0].status, "OK", "util default(42): {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!(42));

		Ok(())
	}

	#[test(tokio::test)]
	async fn module_mod_default_export() -> Result<(), Box<dyn std::error::Error>> {
		check_mod_default_export(&DEMO_DIR.canonical).await
	}

	async fn check_mod_named_exports(bucket_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		setup_module(&addr, &ns, &db, bucket_dir).await;

		// math::add(3, 4) -> 7
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::math::add(3, 4);").await;
		assert_eq!(results[0].status, "OK", "math::add(3,4): {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!(7));

		// math::add(0, 0) -> 0
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::math::add(0, 0);").await;
		assert_eq!(results[0].status, "OK", "math::add(0,0): {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!(0));

		// math::add with negative numbers
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::math::add(-10, 3);").await;
		assert_eq!(results[0].status, "OK", "math::add(-10,3): {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!(-7));

		// math::multiply(3, 4) -> 12 (name override inside mod)
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::math::multiply(3, 4);").await;
		assert_eq!(results[0].status, "OK", "math::multiply(3,4): {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!(12));

		// math::multiply(0, 999) -> 0
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::math::multiply(0, 999);").await;
		assert_eq!(results[0].status, "OK", "math::multiply(0,999): {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!(0));

		Ok(())
	}

	#[test(tokio::test)]
	async fn module_mod_named_exports() -> Result<(), Box<dyn std::error::Error>> {
		check_mod_named_exports(&DEMO_DIR.canonical).await
	}

	async fn check_mod_name_override(bucket_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		setup_module(&addr, &ns, &db, bucket_dir).await;

		// util::negate(5) -> -5 (mod with name override + fn with name override)
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::util::negate(5);").await;
		assert_eq!(results[0].status, "OK", "util::negate(5): {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!(-5));

		// util::negate(0) -> 0
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::util::negate(0);").await;
		assert_eq!(results[0].status, "OK", "util::negate(0): {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!(0));

		// util::negate(-42) -> 42
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::util::negate(-42);").await;
		assert_eq!(results[0].status, "OK", "util::negate(-42): {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!(42));

		Ok(())
	}

	#[test(tokio::test)]
	async fn module_mod_name_override() -> Result<(), Box<dyn std::error::Error>> {
		check_mod_name_override(&DEMO_DIR.canonical).await
	}

	async fn check_mod_nested(bucket_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		setup_module(&addr, &ns, &db, bucket_dir).await;

		// util::nested::deep(1) -> 101 (nested mod support, multi-segment sub name)
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::util::nested::deep(1);").await;
		assert_eq!(results[0].status, "OK", "util::nested::deep(1): {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!(101));

		// util::nested::deep(0) -> 100
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::util::nested::deep(0);").await;
		assert_eq!(results[0].status, "OK", "util::nested::deep(0): {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!(100));

		// util::nested::deep(-50) -> 50
		let results =
			sql_query(&addr, &ns, &db, "RETURN mod::demo::util::nested::deep(-50);").await;
		assert_eq!(results[0].status, "OK", "util::nested::deep(-50): {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!(50));

		Ok(())
	}

	#[test(tokio::test)]
	async fn module_mod_nested() -> Result<(), Box<dyn std::error::Error>> {
		check_mod_nested(&DEMO_DIR.canonical).await
	}

	async fn check_mod_nonexistent_function(
		bucket_dir: &Path,
	) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		setup_module(&addr, &ns, &db, bucket_dir).await;

		// Calling a non-existent function inside a mod namespace should fail
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::math::nonexistent(1);").await;
		assert_eq!(
			results[0].status, "ERR",
			"Expected error for nonexistent mod function, got: {:?}",
			results[0].result
		);

		// Calling a non-existent nested path should fail
		let results =
			sql_query(&addr, &ns, &db, "RETURN mod::demo::util::nested::nonexistent(1);").await;
		assert_eq!(
			results[0].status, "ERR",
			"Expected error for nonexistent nested mod function, got: {:?}",
			results[0].result
		);

		Ok(())
	}

	#[test(tokio::test)]
	async fn module_mod_nonexistent_function() -> Result<(), Box<dyn std::error::Error>> {
		check_mod_nonexistent_function(&DEMO_DIR.canonical).await
	}

	async fn check_mod_wrong_arg_count(
		bucket_dir: &Path,
	) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		setup_module(&addr, &ns, &db, bucket_dir).await;

		// math::add expects 2 args, passing 1 should fail
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::math::add(1);").await;
		assert_eq!(
			results[0].status, "ERR",
			"Expected error for wrong arg count, got: {:?}",
			results[0].result
		);

		// math::add expects 2 args, passing 3 should fail
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::math::add(1, 2, 3);").await;
		assert_eq!(
			results[0].status, "ERR",
			"Expected error for wrong arg count, got: {:?}",
			results[0].result
		);

		Ok(())
	}

	#[test(tokio::test)]
	async fn module_mod_wrong_arg_count() -> Result<(), Box<dyn std::error::Error>> {
		check_mod_wrong_arg_count(&DEMO_DIR.canonical).await
	}

	async fn check_mod_mixed_with_top_level(
		bucket_dir: &Path,
	) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		setup_module(&addr, &ns, &db, bucket_dir).await;

		// Top-level functions still work alongside mod-namespaced functions
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::can_drive(21);").await;
		assert_eq!(results[0].status, "OK", "can_drive(21): {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::Value::Bool(true));

		// Top-level default still works
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo(18);").await;
		assert_eq!(results[0].status, "OK", "default(18): {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::Value::Bool(true));

		// Mod-namespaced functions work in the same module
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::math::add(10, 20);").await;
		assert_eq!(results[0].status, "OK", "math::add(10,20): {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!(30));

		Ok(())
	}

	#[test(tokio::test)]
	async fn module_mod_mixed_with_top_level() -> Result<(), Box<dyn std::error::Error>> {
		check_mod_mixed_with_top_level(&DEMO_DIR.canonical).await
	}

	async fn check_mod_concurrent(bucket_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		setup_module(&addr, &ns, &db, bucket_dir).await;

		let mut handles = Vec::new();
		for i in 0..10 {
			let addr = addr.clone();
			let ns = ns.clone();
			let db = db.clone();
			handles.push(tokio::spawn(async move {
				let query = format!("RETURN mod::demo::math::add({i}, {});", i * 10);
				let results = sql_query(&addr, &ns, &db, &query).await;
				assert_eq!(
					results[0].status,
					"OK",
					"concurrent math::add({i}, {}): {:?}",
					i * 10,
					results[0].result
				);
				let expected = i + i * 10;
				assert_eq!(
					results[0].result,
					serde_json::json!(expected),
					"concurrent math::add({i}, {}) expected {expected}",
					i * 10,
				);
			}));
		}

		for handle in handles {
			handle.await?;
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn module_mod_concurrent() -> Result<(), Box<dyn std::error::Error>> {
		check_mod_concurrent(&DEMO_DIR.canonical).await
	}

	// -------------------------------------------------------------------
	// Concurrent async invocation test
	// -------------------------------------------------------------------

	async fn check_concurrent_async(bucket_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		setup_module(&addr, &ns, &db, bucket_dir).await;

		let mut handles = Vec::new();
		for age in [15, 16, 17, 18, 19, 20, 21] {
			let addr = addr.clone();
			let ns = ns.clone();
			let db = db.clone();
			handles.push(tokio::spawn(async move {
				let query = format!("RETURN mod::demo::can_drive({age});");
				let results = sql_query(&addr, &ns, &db, &query).await;
				assert_eq!(results[0].status, "OK", "can_drive({age}): {:?}", results[0].result);
				let expected = age >= 18;
				assert_eq!(
					results[0].result,
					serde_json::Value::Bool(expected),
					"can_drive({age}) expected {expected}"
				);
			}));
		}

		for handle in handles {
			handle.await?;
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn module_concurrent_async() -> Result<(), Box<dyn std::error::Error>> {
		check_concurrent_async(&DEMO_DIR.canonical).await
	}

	// -------------------------------------------------------------------
	// Transaction handle tests
	// -------------------------------------------------------------------

	/// Two reads awaited together must overlap, and two opaque calls must not.
	///
	/// This is the regression test for the whole async change, and it has to
	/// measure overlap rather than observe results: `try_join` returns the same
	/// values in the same order whether the host ran the subtasks side by side
	/// or back to back, so an assertion on the results passes just as well with
	/// a blocking executor in the guest or with every call serialised.
	///
	/// The two halves are measured against each other rather than against a
	/// fixed threshold, so a loaded runner slows both and the comparison holds.
	/// `concurrent_sleeps` issues two `SLEEP` reads, which the host may overlap
	/// because neither can write; `concurrent_runs` issues two `fn::slow` calls,
	/// whose bodies the host cannot see and therefore has to serialise. If the
	/// host stopped overlapping, or stopped serialising, the two would converge.
	async fn check_concurrent_host_calls(
		bucket_dir: &Path,
	) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		setup_module(&addr, &ns, &db, bucket_dir).await;

		const SLEEP_MS: u64 = 400;

		let results = sql_query(
			&addr,
			&ns,
			&db,
			&format!("DEFINE FUNCTION fn::slow($ms: int) {{ SLEEP {SLEEP_MS}ms; RETURN $ms; }};"),
		)
		.await;
		assert_eq!(results[0].status, "OK", "DEFINE FUNCTION: {:?}", results[0].result);

		// Warm the module: the first call pays instantiation and `init`, which
		// would otherwise land entirely on whichever half ran first.
		let warm = sql_query(&addr, &ns, &db, "RETURN mod::demo::can_drive(21);").await;
		assert_eq!(warm[0].status, "OK", "warm-up: {:?}", warm[0].result);

		let started = Instant::now();
		let results = sql_query(
			&addr,
			&ns,
			&db,
			&format!("RETURN mod::demo::concurrent_sleeps({SLEEP_MS});"),
		)
		.await;
		let overlapped = started.elapsed();
		assert_eq!(results[0].status, "OK", "concurrent_sleeps: {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!(2));

		let started = Instant::now();
		let results =
			sql_query(&addr, &ns, &db, &format!("RETURN mod::demo::concurrent_runs({SLEEP_MS});"))
				.await;
		let serialised = started.elapsed();
		assert_eq!(results[0].status, "OK", "concurrent_runs: {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!(2));

		// Two overlapping sleeps cost about one of them; two serialised calls
		// cost both. Half a sleep of separation is far more than the scheduling
		// noise either measurement carries.
		assert!(
			serialised > overlapped + Duration::from_millis(SLEEP_MS / 2),
			"two reads did not overlap, or two calls were not serialised: \
			 reads {overlapped:?}, calls {serialised:?}"
		);

		// The results still come back, in order, through the overlapping path.
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::concurrent_queries();").await;
		assert_eq!(results[0].status, "OK", "concurrent_queries: {:?}", results[0].result);
		let arr = results[0].result.as_array().expect("concurrent_queries should return an array");
		assert_eq!(arr.len(), 2, "expected both query results: {:?}", results[0].result);

		Ok(())
	}

	#[test(tokio::test)]
	async fn module_concurrent_host_calls() -> Result<(), Box<dyn std::error::Error>> {
		check_concurrent_host_calls(&DEMO_DIR.canonical).await
	}

	/// A function that declared no transaction parameter still resolves, and
	/// its SurrealQL arity does not include the handle.
	async fn check_pure_function_arity(
		bucket_dir: &Path,
	) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		setup_module(&addr, &ns, &db, bucket_dir).await;

		// `can_drive` takes one SurrealQL argument and no transaction.
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::can_drive(21);").await;
		assert_eq!(results[0].status, "OK", "can_drive: {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::Value::Bool(true));

		// `create_user` reaches `fn::user_exists` through `tx.run`.
		let results = sql_query(
			&addr,
			&ns,
			&db,
			"DEFINE FUNCTION fn::user_exists($name: string, $age: int) { RETURN false; };",
		)
		.await;
		assert_eq!(results[0].status, "OK", "DEFINE FUNCTION: {:?}", results[0].result);

		// `create_user` takes a `Transaction<ReadWrite>` plus one argument; the
		// handle must not appear as a second SurrealQL argument.
		let results = sql_query(
			&addr,
			&ns,
			&db,
			"RETURN mod::demo::create_user({ name: 'ada', age: 36, enabled: true });",
		)
		.await;
		assert_eq!(results[0].status, "OK", "create_user: {:?}", results[0].result);

		Ok(())
	}

	#[test(tokio::test)]
	async fn module_pure_function_arity() -> Result<(), Box<dyn std::error::Error>> {
		check_pure_function_arity(&DEMO_DIR.canonical).await
	}

	/// A write issued by a function that declared a read-only transaction must be
	/// refused.
	///
	/// `Host::new` installs `NoWriteFrame::SurrealismReadOnly` when the
	/// function's manifest entry is not `writeable`, and the frame fires where
	/// the write is reached — the same enforcement point that stops a `SELECT`
	/// `PERMISSIONS` predicate and a `COMPUTED` body from writing.
	///
	/// The assertions check *why* the call failed: a definition without
	/// `queries` would refuse the write by capability, which says nothing
	/// about read-only enforcement.
	async fn check_read_only_write_refused(
		bucket_dir: &Path,
	) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		setup_module(&addr, &ns, &db, bucket_dir).await;

		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::sneaky_write();").await;
		assert_ne!(
			results[0].status, "OK",
			"a write through a read-only transaction must be refused, got: {:?}",
			results[0].result
		);

		// The refusal must be the read-only frame, not a missing capability: the
		// demo is granted `queries`, so a `permission denied` here
		// would mean this test is passing for the wrong reason and proving
		// nothing about read-only enforcement.
		let message = results[0].result.to_string();
		assert!(
			message.contains("read-only Surrealism function"),
			"expected the read-only frame to refuse the write, got: {message}"
		);
		assert!(
			!message.contains("permission denied"),
			"refused by capability, not by the read-only frame: {message}"
		);

		Ok(())
	}

	#[test(tokio::test)]
	async fn module_read_only_write_refused() -> Result<(), Box<dyn std::error::Error>> {
		check_read_only_write_refused(&DEMO_DIR.canonical).await
	}

	/// A writing export of a definition that does not grant `write` runs under
	/// the read-only frame, so its write is refused there.
	async fn check_write_needs_the_grant(
		bucket_dir: &Path,
	) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		setup_named_module(
			&addr,
			&ns,
			&db,
			bucket_dir,
			"nowrite",
			"demo.surli",
			"CAPABILITIES { queries: true }",
		)
		.await;

		let results =
			sql_query(&addr, &ns, &db, "RETURN ext::nowrite::registered::add_note('hi');").await;
		assert_ne!(results[0].status, "OK", "the write must be refused: {:?}", results[0].result);
		let message = results[0].result.to_string();
		assert!(message.contains("read-only Surrealism function"), "{message}");

		// The same export of a definition that grants `write` writes.
		setup_named_module(
			&addr,
			&ns,
			&db,
			bucket_dir,
			"writer",
			"demo.surli",
			"CAPABILITIES { queries: true, write: true }",
		)
		.await;
		let results =
			sql_query(&addr, &ns, &db, "RETURN ext::writer::registered::add_note('hi');").await;
		assert_eq!(results[0].status, "OK", "{:?}", results[0].result);
		Ok(())
	}

	#[test(tokio::test)]
	async fn module_write_needs_the_grant() -> Result<(), Box<dyn std::error::Error>> {
		check_write_needs_the_grant(&DEMO_DIR.canonical).await
	}

	/// A definition's `memory` limit stops a guest that grows past it, and one
	/// that stays under it runs.
	async fn check_memory_limit_stops_a_guest(
		bucket_dir: &Path,
	) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		setup_named_module(
			&addr,
			&ns,
			&db,
			bucket_dir,
			"small",
			"demo.surli",
			"CAPABILITIES { memory: 33554432 }",
		)
		.await;

		let results = sql_query(&addr, &ns, &db, "RETURN ext::small::allocate(1);").await;
		assert_eq!(results[0].status, "OK", "1 MiB fits: {:?}", results[0].result);
		let results = sql_query(&addr, &ns, &db, "RETURN ext::small::allocate(64);").await;
		assert_ne!(results[0].status, "OK", "64 MiB must not: {:?}", results[0].result);
		// The refused grow surfaces in the guest as an allocation failure.
		let message = results[0].result.to_string();
		assert!(message.contains("rust_oom"), "{message}");
		Ok(())
	}

	#[test(tokio::test)]
	async fn module_memory_limit_stops_a_guest() -> Result<(), Box<dyn std::error::Error>> {
		check_memory_limit_stops_a_guest(&DEMO_DIR.canonical).await
	}

	/// A definition's `timeout` stops a guest that computes without calling
	/// the host, well before it would finish.
	async fn check_timeout_stops_a_guest(
		bucket_dir: &Path,
	) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		setup_named_module(
			&addr,
			&ns,
			&db,
			bucket_dir,
			"quick",
			"demo.surli",
			"CAPABILITIES { timeout: 200ms }",
		)
		.await;

		let start = std::time::Instant::now();
		let results = sql_query(&addr, &ns, &db, "RETURN ext::quick::spin(20000);").await;
		assert_ne!(results[0].status, "OK", "the spin must time out: {:?}", results[0].result);
		let message = results[0].result.to_string();
		assert!(message.contains("timed out (epoch interrupt)"), "{message}");
		assert!(start.elapsed() < Duration::from_secs(10), "took {:?}", start.elapsed());
		Ok(())
	}

	#[test(tokio::test)]
	async fn module_timeout_stops_a_guest() -> Result<(), Box<dyn std::error::Error>> {
		check_timeout_stops_a_guest(&DEMO_DIR.canonical).await
	}

	/// `strict_timeout: false` selects the engine without timeouts only on a
	/// server that allows unbounded execution; elsewhere the `timeout` still
	/// stops the guest.
	async fn check_unbounded_execution_needs_the_server(
		bucket_dir: &Path,
	) -> Result<(), Box<dyn std::error::Error>> {
		const UNBOUNDED: &str = "CAPABILITIES { timeout: 200ms, strict_timeout: false }";
		let query = "RETURN ext::loose::spin(1000);";

		let (addr, server) = start_surrealism_server(bucket_dir).await?;
		let (ns, db) = (Ulid::new().to_string(), Ulid::new().to_string());
		setup_named_module(&addr, &ns, &db, bucket_dir, "loose", "demo.surli", UNBOUNDED).await;
		let results = sql_query(&addr, &ns, &db, query).await;
		assert_ne!(
			results[0].status, "OK",
			"without the server setting the timeout still applies: {:?}",
			results[0].result
		);
		let message = results[0].result.to_string();
		assert!(message.contains("timed out (epoch interrupt)"), "{message}");
		drop(server);

		let allowed = HashMap::from([(
			"SURREAL_SURREALISM_ALLOW_UNBOUNDED_EXECUTION".to_string(),
			"true".to_string(),
		)]);
		let (addr, _server) = start_surrealism_server_with(bucket_dir, allowed).await?;
		let (ns, db) = (Ulid::new().to_string(), Ulid::new().to_string());
		setup_named_module(&addr, &ns, &db, bucket_dir, "loose", "demo.surli", UNBOUNDED).await;
		let results = sql_query(&addr, &ns, &db, query).await;
		assert_eq!(
			results[0].status, "OK",
			"with it the fast engine runs past the timeout: {:?}",
			results[0].result
		);
		Ok(())
	}

	#[test(tokio::test)]
	async fn module_unbounded_execution_needs_the_server() -> Result<(), Box<dyn std::error::Error>>
	{
		check_unbounded_execution_needs_the_server(&DEMO_DIR.canonical).await
	}

	/// A read-only function cannot advance a sequence either.
	///
	/// `sequence::nextval` is a *call*, not a statement, and it moves a counter
	/// the transaction does not snapshot. The refusal has to classify calls as
	/// well as statement forms, or the WIT's promise — that a non-writeable
	/// function cannot write whatever query string it builds — holds only for
	/// the strings that happen to contain a `CREATE`.
	async fn check_read_only_sequence_refused(
		bucket_dir: &Path,
	) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		setup_module(&addr, &ns, &db, bucket_dir).await;
		sql_query(&addr, &ns, &db, "DEFINE SEQUENCE order_no;").await;

		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::sneaky_sequence();").await;
		assert_ne!(
			results[0].status, "OK",
			"advancing a sequence through a read-only transaction must be refused, got: {:?}",
			results[0].result
		);
		let message = results[0].result.to_string();
		assert!(
			message.contains("read-only Surrealism function"),
			"expected the read-only frame to refuse the call, got: {message}"
		);

		Ok(())
	}

	#[test(tokio::test)]
	async fn module_read_only_sequence_refused() -> Result<(), Box<dyn std::error::Error>> {
		check_read_only_sequence_refused(&DEMO_DIR.canonical).await
	}

	/// A read-only function cannot write to a bucket through *method* syntax.
	///
	/// `f'bucket:/k'.put(..)` is a `Part::Method`. It never becomes an
	/// `Expr::FunctionCall`, so the refusal in `expr_compute` never sees it —
	/// the dispatch in `fnc::idiom` is the only point that can, and the only
	/// point where the receiver's kind is known well enough to name the builtin.
	/// The call-form fixtures cannot reach this route, so without this the
	/// whole `file::*` family is refused on paper and reachable in practice.
	///
	/// The demo declares `file::*`, so a refusal here is the read-only frame
	/// rather than the capability gate — which is asserted, not assumed.
	async fn check_read_only_file_method_refused(
		bucket_dir: &Path,
	) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		setup_module(&addr, &ns, &db, bucket_dir).await;
		let dir = bucket_dir.to_string_lossy();
		let results =
			sql_query(&addr, &ns, &db, &format!("DEFINE BUCKET evil BACKEND \"file:{dir}\";"))
				.await;
		assert_eq!(results[0].status, "OK", "DEFINE BUCKET: {:?}", results[0].result);

		let results =
			sql_query(&addr, &ns, &db, "RETURN mod::demo::sneaky_file_method('evil');").await;
		assert_ne!(
			results[0].status, "OK",
			"writing to a bucket through a read-only transaction must be refused, got: {:?}",
			results[0].result
		);
		let message = results[0].result.to_string();
		assert!(
			message.contains("read-only Surrealism function"),
			"expected the read-only frame to refuse the method call, got: {message}"
		);

		Ok(())
	}

	#[test(tokio::test)]
	async fn module_read_only_file_method_refused() -> Result<(), Box<dyn std::error::Error>> {
		check_read_only_file_method_refused(&DEMO_DIR.canonical).await
	}

	// -------------------------------------------------------------------
	// Error propagation tests
	// -------------------------------------------------------------------

	async fn check_error_propagation(bucket_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		setup_module(&addr, &ns, &db, bucket_dir).await;

		// result(true) returns Err -- should propagate as module error
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::result(true);").await;
		assert_eq!(results[0].status, "ERR", "Expected error from result(true)");

		// safe_divide(1, 0) returns Err("Division by zero")
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::safe_divide(1, 0);").await;
		assert_eq!(results[0].status, "ERR", "Expected division by zero error");

		// parse_number with invalid input
		let results =
			sql_query(&addr, &ns, &db, "RETURN mod::demo::parse_number('not_a_number');").await;
		assert_eq!(results[0].status, "ERR", "Expected parse error");

		// Calling a completely nonexistent module function
		let results =
			sql_query(&addr, &ns, &db, "RETURN mod::demo::nonexistent_function(1);").await;
		assert_eq!(results[0].status, "ERR", "Expected error for nonexistent function");

		Ok(())
	}

	#[test(tokio::test)]
	async fn module_error_propagation() -> Result<(), Box<dyn std::error::Error>> {
		check_error_propagation(&DEMO_DIR.canonical).await
	}

	async fn check_info_db_structure_exports(
		bucket_dir: &Path,
	) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		setup_module(&addr, &ns, &db, bucket_dir).await;

		let results = sql_query(&addr, &ns, &db, "INFO FOR DB STRUCTURE;").await;
		assert_eq!(results[0].status, "OK", "INFO FOR DB STRUCTURE: {:?}", results[0].result);

		let db_info = &results[0].result;
		let modules = db_info["extensions"].as_array().expect("extensions should be an array");
		assert!(!modules.is_empty(), "at least one extension should be defined");
		assert!(db_info.get("modules").is_none(), "INFO answers no `modules` key");

		let module = &modules[0];
		assert_eq!(module["name"], "demo");

		let exports = module["exports"].as_array().expect("exports should be an array");
		assert!(!exports.is_empty(), "demo module should have exports");

		// The default export has no "name" key in its object
		let default_export = exports.iter().find(|e| !e.as_object().unwrap().contains_key("name"));
		assert!(default_export.is_some(), "default export should be present");

		// Named export: can_drive should be read-only
		let can_drive = exports.iter().find(|e| e["name"] == "can_drive");
		assert!(can_drive.is_some(), "can_drive should be in exports");
		assert_eq!(can_drive.unwrap()["writeable"], false);

		// Named export: create_user should be writeable
		let create_user = exports.iter().find(|e| e["name"] == "create_user");
		assert!(create_user.is_some(), "create_user should be in exports");
		assert_eq!(create_user.unwrap()["writeable"], true);

		// Every export should have args, returns, and writeable keys
		for export in exports {
			let obj = export.as_object().expect("each export should be an object");
			assert!(obj.contains_key("args"), "export should have args: {export:?}");
			assert!(obj.contains_key("returns"), "export should have returns: {export:?}");
			assert!(obj.contains_key("writeable"), "export should have writeable: {export:?}");
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn module_info_db_structure_exports() -> Result<(), Box<dyn std::error::Error>> {
		check_info_db_structure_exports(&DEMO_DIR.canonical).await
	}

	// -------------------------------------------------------------------
	// Functions added with `surrealism::register` during init
	// -------------------------------------------------------------------

	/// The build reads the function table from the snapshot taken after
	/// `init`, so functions the demo registers there reach `exports.toml`
	/// beside the macro's, with the metadata they were registered with.
	async fn check_registered_exports(bucket_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		setup_module(&addr, &ns, &db, bucket_dir).await;

		let results = sql_query(&addr, &ns, &db, "INFO FOR DB STRUCTURE;").await;
		assert_eq!(results[0].status, "OK", "INFO FOR DB STRUCTURE: {:?}", results[0].result);
		let exports = results[0].result["extensions"][0]["exports"]
			.as_array()
			.expect("exports should be an array");
		let export = |name: &str| {
			exports
				.iter()
				.find(|e| e["name"] == name)
				.unwrap_or_else(|| panic!("{name} missing from exports: {exports:?}"))
		};

		assert!(exports.iter().any(|e| e["name"] == "can_drive"), "macro exports remain");

		let shout = export("registered::shout");
		assert_eq!(shout["writeable"], false);
		assert_eq!(shout["comment"], "Upper-case a string.", "{shout:?}");
		assert_eq!(shout["args"][0]["name"], "text", "{shout:?}");
		assert_eq!(shout["args"][0]["kind"], "string", "{shout:?}");
		assert_eq!(shout["returns"], "string", "{shout:?}");

		let add_note = export("registered::add_note");
		assert_eq!(add_note["writeable"], true, "{add_note:?}");

		let sneaky = export("registered::sneaky_write");
		assert_eq!(sneaky["writeable"], false, "{sneaky:?}");

		Ok(())
	}

	#[test(tokio::test)]
	async fn module_registered_exports() -> Result<(), Box<dyn std::error::Error>> {
		check_registered_exports(&DEMO_DIR.canonical).await
	}

	/// Registered functions are invoked through the same dispatch as the
	/// macro's, and the host enforces their declared transaction mode.
	async fn check_registered_calls(bucket_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		setup_module(&addr, &ns, &db, bucket_dir).await;

		let results =
			sql_query(&addr, &ns, &db, "RETURN mod::demo::registered::shout('hello');").await;
		assert_eq!(results[0].status, "OK", "shout: {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!("HELLO"));

		// Declares a writeable transaction: the write lands.
		let results =
			sql_query(&addr, &ns, &db, "RETURN mod::demo::registered::add_note('kept');").await;
		assert_eq!(results[0].status, "OK", "add_note: {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!("kept"));
		let results = sql_query(&addr, &ns, &db, "SELECT VALUE text FROM note;").await;
		assert_eq!(results[0].result, serde_json::json!(["kept"]));

		// Declares a read-only transaction: the read-only frame refuses it.
		let results =
			sql_query(&addr, &ns, &db, "RETURN mod::demo::registered::sneaky_write();").await;
		assert_ne!(results[0].status, "OK", "write must be refused: {:?}", results[0].result);
		let message = results[0].result.to_string();
		assert!(
			message.contains("read-only Surrealism function"),
			"expected the read-only frame to refuse the write, got: {message}"
		);
		let results = sql_query(&addr, &ns, &db, "SELECT VALUE text FROM note;").await;
		assert_eq!(results[0].result, serde_json::json!(["kept"]));

		Ok(())
	}

	#[test(tokio::test)]
	async fn module_registered_calls() -> Result<(), Box<dyn std::error::Error>> {
		check_registered_calls(&DEMO_DIR.canonical).await
	}

	// -------------------------------------------------------------------
	// Async networking test (local API)
	// -------------------------------------------------------------------

	async fn check_fetch_pokemon(bucket_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
		let api = LocalPokemonApi::start()?;
		let api_base = api.base_url();
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		setup_module(&addr, &ns, &db, bucket_dir).await;

		let results = sql_query(
			&addr,
			&ns,
			&db,
			&format!("RETURN mod::demo::fetch_pokemon('{api_base}', 'pikachu');"),
		)
		.await;
		assert_eq!(results[0].status, "OK", "fetch_pokemon failed: {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!("pikachu"));

		// Also works with numeric Pokédex ID
		let results = sql_query(
			&addr,
			&ns,
			&db,
			&format!("RETURN mod::demo::fetch_pokemon('{api_base}', '1');"),
		)
		.await;
		assert_eq!(results[0].status, "OK", "fetch_pokemon('1') failed: {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!("bulbasaur"));

		Ok(())
	}

	#[test(tokio::test)]
	async fn module_fetch_pokemon() -> Result<(), Box<dyn std::error::Error>> {
		check_fetch_pokemon(&DEMO_DIR.canonical).await
	}

	// -------------------------------------------------------------------
	// DNS tests (the filtering shim in surrealism_runtime::dns)
	// -------------------------------------------------------------------

	/// A guest can reach a host by **name**, not just by literal address.
	///
	/// This is what the DNS shim buys: granted `net` entries are matched as names
	/// at resolution time rather than pre-resolved at module load, so a record
	/// that rotates does not strand the module on a stale address.
	async fn check_dns_allowed_hostname(
		bucket_dir: &Path,
	) -> Result<(), Box<dyn std::error::Error>> {
		let api = LocalPokemonApi::start()?;
		let port = api.base_url().rsplit(':').next().unwrap().to_string();
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		setup_module(&addr, &ns, &db, bucket_dir).await;

		let results = sql_query(
			&addr,
			&ns,
			&db,
			&format!("RETURN mod::demo::fetch_pokemon('http://localhost:{port}', 'pikachu');"),
		)
		.await;
		assert_eq!(results[0].status, "OK", "fetch by hostname failed: {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!("pikachu"));

		Ok(())
	}

	#[test(tokio::test)]
	async fn module_dns_allowed_hostname() -> Result<(), Box<dyn std::error::Error>> {
		check_dns_allowed_hostname(&DEMO_DIR.canonical).await
	}

	/// A name outside the definition's `net` must not reach a listener that is
	/// running and reachable under exactly that name.
	///
	/// The refused name has to be one that genuinely resolves, or the test
	/// cannot tell a refusal from an NXDOMAIN that any resolver would produce —
	/// a reserved `.example` name fails to resolve through wasmtime's own
	/// resolver too, so a test using one would pass with the name filter
	/// deleted. So both halves fetch `localhost`, the live listener, through the
	/// same archive; the only difference is that one definition grants
	/// `localhost` in `net` and the other does not.
	///
	/// Deleting the `allows_name` check in `surrealism/runtime/src/dns.rs`, or
	/// unbinding the shim, makes the denied half start succeeding: the address
	/// `localhost` resolves to is covered by the `127.0.0.1` rule both
	/// definitions grant, so nothing downstream refuses the connect.
	async fn check_dns_denied_hostname(
		bucket_dir: &Path,
	) -> Result<(), Box<dyn std::error::Error>> {
		let api = LocalPokemonApi::start()?;
		let port = api.base_url().rsplit(':').next().unwrap().to_string();
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		setup_module(&addr, &ns, &db, bucket_dir).await;
		setup_named_module(
			&addr,
			&ns,
			&db,
			bucket_dir,
			"restricted",
			"demo.surli",
			"CAPABILITIES { net: [\"127.0.0.1\"] }",
		)
		.await;

		// The control: the definition that grants `localhost`.
		let allowed = sql_query(
			&addr,
			&ns,
			&db,
			&format!("RETURN mod::demo::fetch_pokemon('http://localhost:{port}', 'pikachu');"),
		)
		.await;
		assert_eq!(
			allowed[0].status, "OK",
			"the control fetch through an allowed name must succeed: {:?}",
			allowed[0].result
		);
		assert_eq!(allowed[0].result, serde_json::json!("pikachu"));

		// The same archive, the same name, the same listener — without the
		// name in `net`.
		let denied = sql_query(
			&addr,
			&ns,
			&db,
			&format!(
				"RETURN mod::restricted::fetch_pokemon('http://localhost:{port}', 'pikachu');"
			),
		)
		.await;
		assert_ne!(
			denied[0].status, "OK",
			"a hostname outside the granted net must not resolve, got: {:?}",
			denied[0].result
		);

		Ok(())
	}

	/// An extension called by another one through `tx.run` runs with its own
	/// definition's grant, even where that is more than its caller's.
	///
	/// `ext::caller` may call extensions but reach no network; `ext::netter`
	/// may reach the local listener. Both are the demo archive. The caller's
	/// `fetch_via` reaches the listener through `netter`, and the caller's own
	/// `fetch_pokemon` does not. Each order is run on a fresh server, so the
	/// grant cache is first filled by the nested call in one and by the direct
	/// call in the other, and the outcome is the same in both.
	async fn check_nested_extension_has_its_own_grant(
		bucket_dir: &Path,
		nested_first: bool,
	) -> Result<(), Box<dyn std::error::Error>> {
		let api = LocalPokemonApi::start()?;
		let base = api.base_url();
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		setup_named_module(
			&addr,
			&ns,
			&db,
			bucket_dir,
			"caller",
			"demo.surli",
			"CAPABILITIES { functions: [\"ext::*\"] }",
		)
		.await;
		setup_named_module(
			&addr,
			&ns,
			&db,
			bucket_dir,
			"netter",
			"demo.surli",
			"CAPABILITIES { net: [\"127.0.0.1\"] }",
		)
		.await;

		let nested = format!("RETURN ext::caller::fetch_via('netter', '{base}', 'pikachu');");
		let direct = format!("RETURN ext::netter::fetch_pokemon('{base}', 'pikachu');");
		let own = format!("RETURN ext::caller::fetch_pokemon('{base}', 'pikachu');");
		let order = if nested_first {
			[&nested, &direct]
		} else {
			[&direct, &nested]
		};
		for query in order {
			let results = sql_query(&addr, &ns, &db, query).await;
			assert_eq!(results[0].status, "OK", "{query} failed: {:?}", results[0].result);
			assert_eq!(results[0].result, serde_json::json!("pikachu"));
		}

		let results = sql_query(&addr, &ns, &db, &own).await;
		assert_ne!(
			results[0].status, "OK",
			"the caller's own grant has no network, got: {:?}",
			results[0].result
		);

		Ok(())
	}

	#[test(tokio::test)]
	async fn module_nested_extension_has_its_own_grant_nested_first()
	-> Result<(), Box<dyn std::error::Error>> {
		check_nested_extension_has_its_own_grant(&DEMO_DIR.canonical, true).await
	}

	#[test(tokio::test)]
	async fn module_nested_extension_has_its_own_grant_direct_first()
	-> Result<(), Box<dyn std::error::Error>> {
		check_nested_extension_has_its_own_grant(&DEMO_DIR.canonical, false).await
	}

	/// A port-bearing `--deny-net` rule binds for a guest socket, as it does for
	/// `http::*`.
	///
	/// Such a rule names a port rather than a host, so the module still loads —
	/// load-time validation probes the bare name, which a port-bearing target
	/// cannot match — and the name still resolves. The refusal travels with the
	/// addresses that resolution produced and lands on the connect.
	///
	/// `module_dns_allowed_hostname` is the control: the same fetch, the same
	/// listener, on a server without the deny rule.
	async fn check_operator_port_deny(bucket_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
		let api = LocalPokemonApi::start()?;
		let port = api.base_url().rsplit(':').next().unwrap().to_string();
		let (addr, _server) = start_surrealism_server_full(
			bucket_dir,
			HashMap::new(),
			&format!("--deny-net localhost:{port}"),
		)
		.await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		// The definition grants `net: ["localhost", "127.0.0.1"]` and still
		// loads: the operator denied one port, not the host.
		setup_module(&addr, &ns, &db, bucket_dir).await;

		let denied = sql_query(
			&addr,
			&ns,
			&db,
			&format!("RETURN mod::demo::fetch_pokemon('http://localhost:{port}', 'pikachu');"),
		)
		.await;
		assert_ne!(
			denied[0].status, "OK",
			"a port the operator denied must not be reachable, got: {:?}",
			denied[0].result
		);

		Ok(())
	}

	#[test(tokio::test)]
	async fn module_operator_port_deny() -> Result<(), Box<dyn std::error::Error>> {
		check_operator_port_deny(&DEMO_DIR.canonical).await
	}

	/// A function capability written in either extension spelling covers a
	/// call written in either: `ext::` and `mod::` name the same extension,
	/// an allow entry in one admits a call in the other, and a deny entry in
	/// one refuses a call in the other.
	async fn check_extension_capability_spellings(
		bucket_dir: &Path,
	) -> Result<(), Box<dyn std::error::Error>> {
		let calls = ["RETURN ext::demo::can_drive(21);", "RETURN mod::demo::can_drive(21);"];

		// Denied under `ext::`: both spellings of the call are refused.
		let (addr, _server) =
			start_surrealism_server_full(bucket_dir, HashMap::new(), "--deny-funcs ext::*").await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		setup_module(&addr, &ns, &db, bucket_dir).await;
		for call in calls {
			let results = sql_query(&addr, &ns, &db, call).await;
			assert_ne!(results[0].status, "OK", "{call} under --deny-funcs ext::*");
			let error = results[0].result.to_string();
			assert!(
				error.contains("ext::demo::can_drive") && error.contains("not allowed"),
				"{call}: unexpected error: {error}"
			);
		}

		// Allowed under the `mod` family only, beside the functions the package
		// itself requests: both spellings of the call run.
		let (addr, _server) = start_surrealism_server_full(
			bucket_dir,
			HashMap::new(),
			"--allow-funcs fn::*,file::*,sequence::*,mod::*",
		)
		.await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		setup_module(&addr, &ns, &db, bucket_dir).await;
		for call in calls {
			let results = sql_query(&addr, &ns, &db, call).await;
			assert_eq!(results[0].status, "OK", "{call}: {:?}", results[0].result);
			assert_eq!(results[0].result, serde_json::json!(true), "{call}");
		}

		Ok(())
	}

	/// A hostname in `--deny-net` holds against a guest that connects to the
	/// host's address literal, under a grant that allows by address.
	///
	/// `net: true` grants what the server allows, `127.0.0.1` and `localhost`
	/// here, so `127.0.0.1` is allowed by address. The operator denies
	/// `localhost`, which resolves to it: the connect to `127.0.0.1` is
	/// refused. The control is the same fetch on a server without the deny.
	async fn check_operator_host_deny_by_address(
		bucket_dir: &Path,
	) -> Result<(), Box<dyn std::error::Error>> {
		const OPEN: &str = "CAPABILITIES { net: true }";
		let api = LocalPokemonApi::start()?;
		let fetch = format!(
			"RETURN ext::open::fetch_pokemon('http://127.0.0.1:{}', 'pikachu');",
			api.addr.port()
		);

		let (addr, server) = start_surrealism_server(bucket_dir).await?;
		let (ns, db) = (Ulid::new().to_string(), Ulid::new().to_string());
		setup_named_module(&addr, &ns, &db, bucket_dir, "open", "demo.surli", OPEN).await;
		let allowed = sql_query(&addr, &ns, &db, &fetch).await;
		assert_eq!(allowed[0].status, "OK", "the control must connect: {:?}", allowed[0].result);
		drop(server);

		let (addr, _server) =
			start_surrealism_server_full(bucket_dir, HashMap::new(), "--deny-net localhost")
				.await?;
		let (ns, db) = (Ulid::new().to_string(), Ulid::new().to_string());
		setup_named_module(&addr, &ns, &db, bucket_dir, "open", "demo.surli", OPEN).await;
		let denied = sql_query(&addr, &ns, &db, &fetch).await;
		assert_ne!(
			denied[0].status, "OK",
			"the address of a denied host must not be reachable: {:?}",
			denied[0].result
		);
		Ok(())
	}

	#[test(tokio::test)]
	async fn module_capability_spellings() -> Result<(), Box<dyn std::error::Error>> {
		check_extension_capability_spellings(&DEMO_DIR.canonical).await
	}

	#[test(tokio::test)]
	async fn module_operator_host_deny_by_address() -> Result<(), Box<dyn std::error::Error>> {
		check_operator_host_deny_by_address(&DEMO_DIR.canonical).await
	}

	#[test(tokio::test)]
	async fn module_dns_denied_hostname() -> Result<(), Box<dyn std::error::Error>> {
		check_dns_denied_hostname(&DEMO_DIR.canonical).await
	}

	/// The filtering resolver is bound for the module the other DNS tests use.
	///
	/// Without this, both of those tests pass just as well with the shim
	/// missing: an allowed name resolves through wasmtime's own resolver, and a
	/// name under the reserved `.example` TLD fails there too. This asserts the
	/// shim is what answers, and it is the only check that can — the failure
	/// mode has no other signal.
	#[test(tokio::test)]
	async fn module_dns_filter_is_bound() -> Result<(), Box<dyn std::error::Error>> {
		use surrealism_runtime::epoch::{EngineSettings, engines};
		use surrealism_runtime::package::{SurrealismPackage, UnpackOptions};
		use surrealism_runtime::runtime::Runtime;

		let surli = std::fs::read(DEMO_DIR.canonical.join("demo.surli"))?;
		let package =
			SurrealismPackage::from_reader(std::io::Cursor::new(surli), &UnpackOptions::default())?;
		let runtime = Runtime::new(package, engines(EngineSettings::default()))?;
		assert!(
			runtime.dns_filtered(),
			"the demo imports wasi:sockets/ip-name-lookup, so the filtering \
			 resolver must have shadowed it"
		);
		Ok(())
	}

	// -------------------------------------------------------------------
	// Silo executable tests (local registry standing in for the package host)
	// -------------------------------------------------------------------

	/// A `silo::` executable resolves against the configured endpoint, needing
	/// neither a bucket nor a locally built archive, and the module it yields
	/// runs like any other.
	async fn check_silo_module(bucket_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
		// The demo package's own coordinates: organisation `surrealdb`, name
		// `demo`, version 1.0.0 -- see surrealism/demo/package.toml.
		let package = std::fs::read(bucket_dir.join("demo.surli"))?;
		let registry = LocalSiloRegistry::start("/surrealdb/demo/1.0.0.surli", package)?;

		let mut vars = HashMap::new();
		vars.insert("SURREAL_SURREALISM_SILO_ENDPOINT".to_string(), registry.endpoint());
		let (addr, _server) = start_surrealism_server_with(bucket_dir, vars).await?;

		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		let results = sql_query(
			&addr,
			&ns,
			&db,
			"DEFINE EXTENSION ext::demo FROM silo::surrealdb::demo::<1.0.0> UNSIGNED;",
		)
		.await;
		assert_eq!(
			results[0].status, "OK",
			"silo DEFINE EXTENSION failed: {:?}",
			results[0].result
		);

		// A silo extension is named like any other, and called through that name.
		let results = sql_query(&addr, &ns, &db, "RETURN ext::demo::math(5);").await;
		assert_eq!(results[0].status, "OK", "silo math(5) failed: {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!(10));

		// INFO renders the definition in the EXTENSION spelling, with the
		// DIGEST the definition computed from the package it fetched.
		let results = sql_query(&addr, &ns, &db, "INFO FOR DB;").await;
		assert_eq!(results[0].status, "OK", "INFO FOR DB failed: {:?}", results[0].result);
		let rendered = results[0].result["extensions"]["ext::demo"]
			.as_str()
			.expect("silo extension missing from INFO FOR DB");
		let digest = rendered
			.strip_prefix(
				"DEFINE EXTENSION ext::demo FROM silo::surrealdb::demo<1.0.0> DIGEST 'sha256:",
			)
			.and_then(|rest| rest.strip_suffix("' UNSIGNED CAPABILITIES {} PERMISSIONS FULL"))
			.unwrap_or_else(|| panic!("unexpected rendering: {rendered}"));
		// The DIGEST is sha256 of the archive's MANIFEST, not of its bytes.
		let archive = std::fs::read(bucket_dir.join("demo.surli"))?;
		let manifest = surrealism_runtime::package::trust_files_from_reader(
			archive.as_slice(),
			surrealism_runtime::package::DEFAULT_MAX_FS_BYTES,
		)?;
		assert_eq!(format!("sha256:{digest}"), manifest.manifest.digest().to_string());

		Ok(())
	}

	#[test(tokio::test)]
	async fn module_silo_executable() -> Result<(), Box<dyn std::error::Error>> {
		check_silo_module(&DEMO_DIR.canonical).await
	}

	/// A version the registry does not carry fails at definition time rather
	/// than being recorded and failing on first call.
	async fn check_silo_missing_version(
		bucket_dir: &Path,
	) -> Result<(), Box<dyn std::error::Error>> {
		let package = std::fs::read(bucket_dir.join("demo.surli"))?;
		let registry = LocalSiloRegistry::start("/surrealdb/demo/1.0.0.surli", package)?;

		let mut vars = HashMap::new();
		vars.insert("SURREAL_SURREALISM_SILO_ENDPOINT".to_string(), registry.endpoint());
		let (addr, _server) = start_surrealism_server_with(bucket_dir, vars).await?;

		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		// Without a DIGEST the definition fetches the package to pin it, so the
		// miss is reported by DEFINE and nothing is recorded.
		let results = sql_query(
			&addr,
			&ns,
			&db,
			"DEFINE EXTENSION ext::demo FROM silo::surrealdb::demo::<9.9.9> UNSIGNED;",
		)
		.await;
		assert_eq!(results[0].status, "ERR", "expected a miss: {:?}", results[0].result);
		let error = results[0].result.to_string();
		assert!(error.contains("was not found"), "unexpected error: {error}");

		// The failed DEFINE wrote nothing, not even the database it would
		// have created.
		let results = sql_query(&addr, &ns, &db, "INFO FOR DB;").await;
		assert!(
			results[0].result["extensions"]["ext::demo"].is_null(),
			"the failed definition was recorded: {:?}",
			results[0].result
		);

		Ok(())
	}

	#[test(tokio::test)]
	async fn module_silo_missing_version() -> Result<(), Box<dyn std::error::Error>> {
		check_silo_missing_version(&DEMO_DIR.canonical).await
	}

	/// Two databases defining the same silo package share its compiled code,
	/// but never its memory: each invocation starts from the module's image.
	/// What one invocation writes into module state is visible to nothing
	/// after it — not the same tenant's next call, and not another tenant.
	async fn check_silo_state_is_per_invocation(
		bucket_dir: &Path,
	) -> Result<(), Box<dyn std::error::Error>> {
		let package = std::fs::read(bucket_dir.join("demo.surli"))?;
		let registry = LocalSiloRegistry::start("/surrealdb/demo/1.0.0.surli", package)?;

		let mut vars = HashMap::new();
		vars.insert("SURREAL_SURREALISM_SILO_ENDPOINT".to_string(), registry.endpoint());
		let (addr, _server) = start_surrealism_server_with(bucket_dir, vars).await?;

		// Two tenants on one server, both resolving the same package coordinates.
		let tenants = [
			(Ulid::new().to_string(), Ulid::new().to_string()),
			(Ulid::new().to_string(), Ulid::new().to_string()),
		];
		for (ns, db) in &tenants {
			let results = sql_query(
				&addr,
				ns,
				db,
				"DEFINE EXTENSION ext::demo FROM silo::surrealdb::demo::<1.0.0> UNSIGNED;",
			)
			.await;
			assert_eq!(
				results[0].status, "OK",
				"silo DEFINE EXTENSION failed: {:?}",
				results[0].result
			);
		}

		let (ns_a, db_a) = &tenants[0];
		let (ns_b, db_b) = &tenants[1];

		// `set_state` returns the value it replaced: the image's zero, every time.
		for _ in 0..2 {
			let results = sql_query(&addr, ns_a, db_a, "RETURN ext::demo::set_state(42);").await;
			assert_eq!(results[0].status, "OK", "set_state: {:?}", results[0].result);
			assert_eq!(results[0].result, serde_json::json!(0));
		}

		for (ns, db) in [(ns_a, db_a), (ns_b, db_b)] {
			let results = sql_query(&addr, ns, db, "RETURN ext::demo::get_state();").await;
			assert_eq!(results[0].status, "OK", "get_state: {:?}", results[0].result);
			assert_eq!(
				results[0].result,
				serde_json::json!(0),
				"module state survived its invocation"
			);
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn module_silo_state_is_per_invocation() -> Result<(), Box<dyn std::error::Error>> {
		check_silo_state_is_per_invocation(&DEMO_DIR.canonical).await
	}

	// -------------------------------------------------------------------
	// Isolation: fresh memory per invocation
	// -------------------------------------------------------------------

	/// A static counter returns zero on every call, sequential or concurrent:
	/// each invocation increments its own fresh copy of the module's memory.
	async fn check_static_counter(bucket_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		setup_module(&addr, &ns, &db, bucket_dir).await;

		// Several calls in one query, then several queries.
		let results = sql_query(
			&addr,
			&ns,
			&db,
			"RETURN [mod::demo::hit(), mod::demo::hit(), mod::demo::hit()];",
		)
		.await;
		assert_eq!(results[0].status, "OK", "hit: {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!([0, 0, 0]));

		let mut handles = Vec::new();
		for _ in 0..16 {
			let (addr, ns, db) = (addr.clone(), ns.clone(), db.clone());
			handles.push(tokio::spawn(async move {
				let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::hit();").await;
				assert_eq!(results[0].status, "OK", "hit: {:?}", results[0].result);
				results[0].result.clone()
			}));
		}
		for handle in handles {
			assert_eq!(handle.await?, serde_json::json!(0));
		}
		Ok(())
	}

	#[test(tokio::test)]
	async fn module_static_counter_is_per_invocation() -> Result<(), Box<dyn std::error::Error>> {
		check_static_counter(&DEMO_DIR.canonical).await
	}

	/// With one invocation slot, a nested call finds the only slot held by its
	/// caller. It must fail at once rather than wait for a slot that can only
	/// free once it has itself returned.
	async fn check_nested_fails_fast(bucket_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
		let mut vars = HashMap::new();
		vars.insert("SURREAL_SURREALISM_MAX_CONCURRENT_INVOCATIONS".to_string(), "1".to_string());
		let (addr, _server) = start_surrealism_server_with(bucket_dir, vars).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		setup_module(&addr, &ns, &db, bucket_dir).await;

		// The control: a top-level call takes the one slot and returns it.
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::hit();").await;
		assert_eq!(results[0].status, "OK", "hit: {:?}", results[0].result);

		let started = Instant::now();
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::nested_hit();").await;
		let elapsed = started.elapsed();
		assert_ne!(results[0].status, "OK", "nested call ran with no free slot");
		let message = results[0].result.to_string();
		assert!(
			message.contains("nested Surrealism invocation"),
			"expected the nested-slot error, got: {message}"
		);
		assert!(elapsed < Duration::from_secs(10), "the nested call waited {elapsed:?}");
		Ok(())
	}

	#[test(tokio::test)]
	async fn module_nested_invocation_fails_fast() -> Result<(), Box<dyn std::error::Error>> {
		check_nested_fails_fast(&DEMO_DIR.canonical).await
	}

	/// An extension may call extensions up to eight invocations deep, counting
	/// the outermost; one more fails.
	async fn check_depth_limit(bucket_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		setup_module(&addr, &ns, &db, bucket_dir).await;

		// `recurse(7)` runs eight invocations: the outermost and seven nested.
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::recurse(7);").await;
		assert_eq!(results[0].status, "OK", "recurse(7): {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!(7));

		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::recurse(8);").await;
		assert_ne!(results[0].status, "OK", "recurse(8) went past the depth limit");
		let message = results[0].result.to_string();
		assert!(message.contains("depth exceeded"), "expected the depth error, got: {message}");
		Ok(())
	}

	#[test(tokio::test)]
	async fn module_nesting_depth_is_limited() -> Result<(), Box<dyn std::error::Error>> {
		check_depth_limit(&DEMO_DIR.canonical).await
	}

	/// One compiled component, two invocations granted different network
	/// policies. The policy travels with the invocation rather than living in
	/// the linker, so the same `InstancePre` reaches the listener in one and is
	/// refused in the other.
	#[test(tokio::test)]
	async fn module_net_policy_is_per_invocation() -> Result<(), Box<dyn std::error::Error>> {
		use std::sync::Arc;

		use surrealism_runtime::epoch::{EngineSettings, engines};
		use surrealism_runtime::package::UnpackOptions;
		use surrealism_runtime::runtime::{Call, InstanceSettings, Runtime};
		use surrealism_runtime::{NetDeny, NetPolicy};

		let api = LocalPokemonApi::start()?;
		let base = format!("http://localhost:{}", api.base_url().rsplit(':').next().unwrap());

		let surli = std::fs::read(DEMO_DIR.canonical.join("demo.surli"))?;
		let mut package =
			SurrealismPackage::from_reader(std::io::Cursor::new(surli), &UnpackOptions::default())?;
		package.config.attach.fs = None;
		let runtime = Arc::new(Runtime::new(package, engines(EngineSettings::default()))?);

		let fetch = |net: Arc<NetPolicy>| {
			let runtime = &runtime;
			let base = base.clone();
			async move {
				runtime
					.invoke(Call {
						function: Some("fetch_pokemon".to_string()),
						rows: vec![vec![
							surrealdb_types::Value::String(base),
							surrealdb_types::Value::String("pikachu".to_string()),
						]],
						tx: None,
						settings: InstanceSettings {
							net,
							..InstanceSettings::local()
						},
						deadline: None,
						nested: false,
					})
					.await
			}
		};

		let allowed = NetPolicy::parse(
			&["localhost".to_string(), "127.0.0.1".to_string()],
			NetDeny::Nothing,
		)?;
		let refused = NetPolicy::parse(&["127.0.0.1".to_string()], NetDeny::Nothing)?;

		let values = fetch(allowed).await?;
		assert_eq!(values, vec![surrealdb_types::Value::String("pikachu".to_string())]);
		assert!(
			fetch(refused).await.is_err(),
			"the second invocation reached a name its own policy does not allow"
		);
		Ok(())
	}

	/// Nothing the replaced definition's invocations wrote serves the
	/// redefined extension. `OVERWRITE` drops the source entry the stored
	/// definition resolved, keyed by the DIGEST that definition pinned (the new
	/// statement gives none), and every invocation starts from fresh memory,
	/// so state written before the overwrite is never read after it.
	async fn check_overwrite_evicts_the_replaced_runtime(
		bucket_dir: &Path,
	) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		setup_module(&addr, &ns, &db, bucket_dir).await;

		let results = sql_query(&addr, &ns, &db, "RETURN ext::demo::set_state(42);").await;
		assert_eq!(results[0].status, "OK", "set_state: {:?}", results[0].result);

		let overwrite = "DEFINE EXTENSION OVERWRITE ext::demo FROM f\"test:/demo.surli\" UNSIGNED;";
		let results = sql_query(&addr, &ns, &db, overwrite).await;
		assert_eq!(results[0].status, "OK", "overwrite: {:?}", results[0].result);

		let results = sql_query(&addr, &ns, &db, "RETURN ext::demo::get_state();").await;
		assert_eq!(results[0].status, "OK", "get_state: {:?}", results[0].result);
		assert_eq!(
			results[0].result,
			serde_json::json!(0),
			"the runtime the overwritten definition loaded still serves calls"
		);
		Ok(())
	}

	#[test(tokio::test)]
	async fn module_overwrite_evicts_the_replaced_runtime() -> Result<(), Box<dyn std::error::Error>>
	{
		check_overwrite_evicts_the_replaced_runtime(&DEMO_DIR.canonical).await
	}

	/// An extension defined and called in one block: the call is planned
	/// while the definition is not yet committed and has no DIGEST pinned, and
	/// must still plan and run.
	async fn check_define_and_call_in_one_block(
		bucket_dir: &Path,
	) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		let dir = bucket_dir.to_string_lossy();
		let results =
			sql_query(&addr, &ns, &db, &format!("DEFINE BUCKET test BACKEND \"file:{dir}\";"))
				.await;
		assert_eq!(results[0].status, "OK", "DEFINE BUCKET: {:?}", results[0].result);

		let transaction = "BEGIN;\
			DEFINE EXTENSION ext::txn FROM f\"test:/demo.surli\" UNSIGNED;\
			RETURN ext::txn::math(5);\
			COMMIT;";
		let results = sql_query(&addr, &ns, &db, transaction).await;
		for (i, r) in results.iter().enumerate() {
			assert_eq!(r.status, "OK", "transaction statement {i}: {:?}", r.result);
		}
		// BEGIN, DEFINE, RETURN, COMMIT.
		assert_eq!(results[2].result, serde_json::json!(10));

		let block = "{ DEFINE EXTENSION ext::blk FROM f\"test:/demo.surli\" UNSIGNED; \
			RETURN ext::blk::math(5); };";
		let results = sql_query(&addr, &ns, &db, block).await;
		assert_eq!(results[0].status, "OK", "block: {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!(10));
		Ok(())
	}

	#[test(tokio::test)]
	async fn module_define_and_call_in_one_block() -> Result<(), Box<dyn std::error::Error>> {
		check_define_and_call_in_one_block(&DEMO_DIR.canonical).await
	}

	// -------------------------------------------------------------------
	// DIGEST and SIGNER: checked at DEFINE and before every load
	// -------------------------------------------------------------------

	/// An SSH key made for one test, the demo archive repacked to name it as
	/// `[signer.ssh]`, and that archive signed with it by
	/// `surreal extension sign --key`. `None` when `ssh-keygen` is missing.
	struct SignedDemo {
		_keys: tempfile::TempDir,
		/// The public key line, for `SIGNER { key: … }`.
		key: String,
		/// Another key's public line, which signed nothing.
		other_key: String,
		/// The repacked archive's file name in the bucket directory, unsigned.
		unsigned: String,
		/// The signed archive's file name in the bucket directory.
		signed: String,
		/// `sha256(MANIFEST)` of the repacked demo archive, signed or not.
		digest: String,
	}

	fn ssh_keygen(dir: &Path, name: &str) -> Option<String> {
		let path = dir.join(name);
		let status = Command::new("ssh-keygen")
			.args(["-q", "-t", "ed25519", "-N", "", "-C", "trust@test", "-f"])
			.arg(&path)
			.status()
			.ok()?;
		assert!(status.success(), "ssh-keygen failed");
		Some(std::fs::read_to_string(path.with_extension("pub")).ok()?.trim().to_owned())
	}

	fn sign_demo(bucket_dir: &Path) -> Option<SignedDemo> {
		// `surreal extension sign` exists only with `extension-sign`.
		if cfg!(not(feature = "extension-sign")) {
			return None;
		}
		let keys = tempfile::TempDir::new().expect("temp dir for keys");
		let key = ssh_keygen(keys.path(), "release")?;
		let other_key = ssh_keygen(keys.path(), "other")?;

		// `sign` signs only as the package's [signer].
		use surrealism_runtime::package::UnpackOptions;
		let surli = std::fs::read(bucket_dir.join("demo.surli")).expect("read demo.surli");
		let mut package =
			SurrealismPackage::from_reader(std::io::Cursor::new(surli), &UnpackOptions::default())
				.expect("unpack demo.surli");
		package.config.attach.fs = None;
		package.config.signer = Some(surrealism_runtime::config::PackageSigner::Ssh(
			surrealism_runtime::config::SshSigner {
				public_key: surrealism_runtime::config::SshSigner::normalise(&key)
					.expect("an SSH public key"),
			},
		));
		let unsigned = format!("signable-{}.surli", Ulid::new());
		package.pack(bucket_dir.join(&unsigned), None).expect("pack the signable demo");

		let signed = format!("signed-{}.surli", Ulid::new());
		let result = Command::new(surreal_bin())
			.args(["extension", "sign", "--key"])
			.arg(keys.path().join("release"))
			.arg("-o")
			.arg(bucket_dir.join(&signed))
			.arg(bucket_dir.join(&unsigned))
			.output()
			.expect("run surreal extension sign");
		assert!(
			result.status.success(),
			"surreal extension sign failed: {}",
			String::from_utf8_lossy(&result.stderr)
		);
		let archive = std::fs::read(bucket_dir.join(&unsigned)).expect("read the signable demo");
		let files = surrealism_runtime::package::trust_files_from_reader(
			archive.as_slice(),
			surrealism_runtime::package::DEFAULT_MAX_FS_BYTES,
		)
		.expect("read demo MANIFEST");
		Some(SignedDemo {
			_keys: keys,
			key,
			other_key,
			unsigned,
			signed,
			digest: files.manifest.digest().to_string(),
		})
	}

	fn first_error(results: &[QueryResult]) -> String {
		assert_eq!(results[0].status, "ERR", "expected an error: {:?}", results[0].result);
		results[0].result.to_string()
	}

	/// `DEFINE EXTENSION` fetches the archive and refuses it unless its
	/// MANIFEST digest is the DIGEST and, with a SIGNER, its SIGNATURE proves
	/// that signer.
	async fn check_trust_at_define(bucket_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
		let Some(demo) = sign_demo(bucket_dir) else {
			eprintln!("skipping: needs ssh-keygen on PATH and the `extension-sign` feature");
			return Ok(());
		};
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		let dir = bucket_dir.to_string_lossy();
		let results =
			sql_query(&addr, &ns, &db, &format!("DEFINE BUCKET test BACKEND \"file:{dir}\";"))
				.await;
		assert_eq!(results[0].status, "OK", "DEFINE BUCKET: {:?}", results[0].result);

		// Signed by the recorded key: defined, pinned and callable.
		let define = format!(
			"DEFINE EXTENSION ext::signed FROM f\"test:/{}\" SIGNER {{ key: \"{}\" }};",
			demo.signed, demo.key
		);
		let results = sql_query(&addr, &ns, &db, &define).await;
		assert_eq!(results[0].status, "OK", "signed DEFINE: {:?}", results[0].result);
		let results = sql_query(&addr, &ns, &db, "RETURN ext::signed::math(5);").await;
		assert_eq!(results[0].status, "OK", "signed call: {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!(10));
		let results = sql_query(&addr, &ns, &db, "INFO FOR DB;").await;
		let rendered = results[0].result["extensions"]["ext::signed"].as_str().unwrap_or_default();
		assert!(rendered.contains(&format!("DIGEST '{}'", demo.digest)), "{rendered}");

		// Another key signed nothing here.
		let define = format!(
			"DEFINE EXTENSION ext::other FROM f\"test:/{}\" SIGNER {{ key: \"{}\" }};",
			demo.signed, demo.other_key
		);
		let error = first_error(&sql_query(&addr, &ns, &db, &define).await);
		assert!(error.contains("does not prove its SIGNER"), "{error}");

		// An archive no signature was ever seen for.
		let define = format!(
			"DEFINE EXTENSION ext::nosig FROM f\"test:/restricted.surli\" SIGNER {{ key: \"{}\" }};",
			demo.key
		);
		let error = first_error(&sql_query(&addr, &ns, &db, &define).await);
		assert!(error.contains("carries no SIGNATURE"), "{error}");

		// The unsigned copy of the signed code. This server has verified the
		// signed copy of the same DIGEST, but only the definition's own source
		// counts, so every node reaches the same verdict.
		let define = format!(
			"DEFINE EXTENSION ext::copy FROM f\"test:/{}\" SIGNER {{ key: \"{}\" }};",
			demo.unsigned, demo.key
		);
		let error = first_error(&sql_query(&addr, &ns, &db, &define).await);
		assert!(error.contains("carries no SIGNATURE"), "{error}");

		// A DIGEST that is not the archive's is refused, signed or not.
		let wrong = format!("sha256:{}", "0".repeat(64));
		let define = format!(
			"DEFINE EXTENSION ext::pinned FROM f\"test:/demo.surli\" DIGEST \"{wrong}\" UNSIGNED;"
		);
		let error = first_error(&sql_query(&addr, &ns, &db, &define).await);
		assert!(error.contains("MANIFEST digest is"), "{error}");

		let results = sql_query(&addr, &ns, &db, "RETURN (INFO FOR DB).extensions.keys();").await;
		assert_eq!(results[0].result, serde_json::json!(["ext::signed"]));
		Ok(())
	}

	#[test(tokio::test)]
	async fn module_trust_at_define() -> Result<(), Box<dyn std::error::Error>> {
		check_trust_at_define(&DEMO_DIR.canonical).await
	}

	/// A definition whose archive cannot be fetched is accepted when it gives
	/// DIGEST, as an import does, and the archive is checked when it loads:
	/// whatever the source holds by then must be the pinned code, signed by
	/// the recorded signer. A refusal is repeated without fetching for
	/// `REFETCH_AFTER_REFUSAL`, and the source is read again after it.
	async fn check_trust_at_load(bucket_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
		let Some(demo) = sign_demo(bucket_dir) else {
			eprintln!("skipping: needs ssh-keygen on PATH and the `extension-sign` feature");
			return Ok(());
		};
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		let dir = bucket_dir.to_string_lossy();
		let late = format!("late-{}.surli", Ulid::new());
		let define = format!(
			"DEFINE BUCKET test BACKEND \"file:{dir}\";\
			 DEFINE EXTENSION ext::late FROM f\"test:/{late}\" DIGEST \"{}\" SIGNER {{ key: \"{}\" }};",
			demo.digest, demo.key
		);
		for (i, r) in sql_query(&addr, &ns, &db, &define).await.iter().enumerate() {
			assert_eq!(r.status, "OK", "statement {i}: {:?}", r.result);
		}
		let call = "RETURN ext::late::math(5);";
		// A failure to load is repeated without fetching the archive again
		// until `REFETCH_AFTER_REFUSAL` (10 s) has passed, so each change to the
		// archive is first met with the previous refusal.
		let after_refusal = || tokio::time::sleep(Duration::from_secs(11));

		// The eager load after DEFINE found no archive, and that refusal holds.
		std::fs::copy(bucket_dir.join("restricted.surli"), bucket_dir.join(&late))?;
		let error = first_error(&sql_query(&addr, &ns, &db, call).await);
		assert!(error.contains("not found"), "{error}");

		// Other code under the pinned name.
		after_refusal().await;
		let error = first_error(&sql_query(&addr, &ns, &db, call).await);
		assert!(error.contains("MANIFEST digest is"), "{error}");

		// The pinned code, but unsigned.
		std::fs::copy(bucket_dir.join(&demo.unsigned), bucket_dir.join(&late))?;
		after_refusal().await;
		let error = first_error(&sql_query(&addr, &ns, &db, call).await);
		assert!(error.contains("carries no SIGNATURE"), "{error}");

		// The pinned code, signed by the recorded key.
		std::fs::copy(bucket_dir.join(&demo.signed), bucket_dir.join(&late))?;
		after_refusal().await;
		let results = sql_query(&addr, &ns, &db, call).await;
		assert_eq!(results[0].status, "OK", "signed call: {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!(10));
		Ok(())
	}

	#[test(tokio::test)]
	async fn module_trust_at_load() -> Result<(), Box<dyn std::error::Error>> {
		check_trust_at_load(&DEMO_DIR.canonical).await
	}

	/// A module cached from a copy without a signature does not serve a
	/// definition with a SIGNER. Once its source is signed, that definition
	/// loads from the source's current signature, and the stale module is
	/// replaced by one built with that signature.
	async fn check_a_resigned_source_replaces_the_cached_module(
		bucket_dir: &Path,
	) -> Result<(), Box<dyn std::error::Error>> {
		let Some(demo) = sign_demo(bucket_dir) else {
			eprintln!("skipping: needs ssh-keygen on PATH and the `extension-sign` feature");
			return Ok(());
		};
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		let dir = bucket_dir.to_string_lossy();
		let shared = format!("shared-{}.surli", Ulid::new());
		std::fs::copy(bucket_dir.join("demo.surli"), bucket_dir.join(&shared))?;

		let define = format!(
			"DEFINE BUCKET test BACKEND \"file:{dir}\";\
			 DEFINE EXTENSION ext::open FROM f\"test:/{shared}\" UNSIGNED;"
		);
		for (i, r) in sql_query(&addr, &ns, &db, &define).await.iter().enumerate() {
			assert_eq!(r.status, "OK", "statement {i}: {:?}", r.result);
		}
		let results = sql_query(&addr, &ns, &db, "RETURN ext::open::math(5);").await;
		assert_eq!(results[0].result, serde_json::json!(10), "{:?}", results[0].result);

		// The same code, now signed at the same source.
		std::fs::copy(bucket_dir.join(&demo.signed), bucket_dir.join(&shared))?;
		let define = format!(
			"DEFINE EXTENSION ext::signed FROM f\"test:/{shared}\" SIGNER {{ key: \"{}\" }};",
			demo.key
		);
		let results = sql_query(&addr, &ns, &db, &define).await;
		assert_eq!(results[0].status, "OK", "signed DEFINE: {:?}", results[0].result);
		for _ in 0..2 {
			let results = sql_query(&addr, &ns, &db, "RETURN ext::signed::math(5);").await;
			assert_eq!(results[0].status, "OK", "signed call: {:?}", results[0].result);
			assert_eq!(results[0].result, serde_json::json!(10));
		}
		// The UNSIGNED definition of the same source still runs.
		let results = sql_query(&addr, &ns, &db, "RETURN ext::open::math(5);").await;
		assert_eq!(results[0].result, serde_json::json!(10), "{:?}", results[0].result);
		Ok(())
	}

	#[test(tokio::test)]
	async fn module_a_resigned_source_replaces_the_cached_module()
	-> Result<(), Box<dyn std::error::Error>> {
		check_a_resigned_source_replaces_the_cached_module(&DEMO_DIR.canonical).await
	}

	// -------------------------------------------------------------------
	// Pack/unpack round-trip test (unit-level, no server needed)
	// -------------------------------------------------------------------

	#[test]
	fn pack_unpack_preserves_fs() {
		let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"));
		let fs_dir = workspace_root.join("surrealism/demo/fs");
		if !fs_dir.is_dir() {
			panic!("surrealism/demo/fs directory not found");
		}

		let config = SurrealismConfig {
			target: Target::default(),
			meta: SurrealismMeta {
				organisation: "test".to_string(),
				name: "roundtrip".to_string(),
				version: semver::Version::new(0, 1, 0),
				repository: None,
			},
			capabilities: Default::default(),
			abi: AbiVersion::CURRENT,
			attach: SurrealismAttach {
				fs: Some(surrealism_runtime::config::FsMounts::root("fs")),
			},
			signer: None,
		};

		// Component preamble (layer 1, version 0x0d)
		let package = SurrealismPackage {
			config,
			wasm: vec![0x00, 0x61, 0x73, 0x6d, 0x0d, 0x00, 0x01, 0x00],
			exports: surrealism_runtime::exports::ExportsManifest::empty(),
			fs: None,
			logo: None,
			manifest: None,
			signature: None,
		};

		let tmp = tempfile::TempDir::new().expect("Failed to create temp dir");
		let surli_path = tmp.path().join("test.surli");
		let digest = package.pack(surli_path.clone(), Some(&fs_dir)).expect("pack failed");

		let unpacked = SurrealismPackage::from_file(surli_path).expect("from_file failed");
		assert_eq!(unpacked.digest(), Some(digest), "digest survives the round trip");

		assert!(unpacked.fs.is_some(), "unpacked package should have fs");
		let fs_temp = unpacked.fs.as_ref().unwrap();
		let greeting =
			std::fs::read_to_string(fs_temp.path().join("greeting.txt")).expect("read greeting");
		assert_eq!(greeting, "Hello from the attached filesystem!");

		let config_json =
			std::fs::read_to_string(fs_temp.path().join("data/config.json")).expect("read config");
		assert!(config_json.contains("\"version\":1"), "config.json should contain version:1");

		assert!(unpacked.config.attach.fs.is_some(), "config should preserve attach.fs");
	}
}
