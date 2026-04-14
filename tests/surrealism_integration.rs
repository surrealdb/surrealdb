mod common;

#[cfg(feature = "surrealism")]
mod surrealism_integration {

	use std::collections::HashMap;
	use std::path::{Path, PathBuf};
	use std::process::Command;
	use std::sync::LazyLock;
	use std::time::Duration;

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

	/// Path to the `surreal` binary built by cargo for integration tests.
	fn surreal_bin() -> PathBuf {
		let mut path = std::env::current_exe().expect("Failed to get current exe path");
		assert!(path.pop());
		if path.ends_with("deps") {
			assert!(path.pop());
		}
		path.push(format!("{}{}", env!("CARGO_PKG_NAME"), std::env::consts::EXE_SUFFIX));
		path
	}

	/// Build and pack the demo module using `surreal module build`.
	fn build_and_pack_demo(output_dir: &Path) {
		let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"));
		let demo_dir = workspace_root.join("surrealism/demo");
		let output = output_dir.join("demo.surli");

		let result = Command::new(surreal_bin())
			.args([
				"module",
				"build",
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
		build_and_pack_demo(&canonical);
		DemoModuleDir {
			_tmp: tmp,
			canonical,
		}
	}

	static DEMO_DIR: LazyLock<DemoModuleDir> = LazyLock::new(build_demo_dir);

	/// Start a SurrealDB server with the `files` and `surrealism` experimental
	/// capabilities enabled, and a bucket folder allowlist pointing at the given
	/// directory.
	async fn start_surrealism_server(
		bucket_dir: &Path,
	) -> Result<(String, common::Child), Box<dyn std::error::Error>> {
		let mut vars = HashMap::new();
		vars.insert(
			"SURREAL_BUCKET_FOLDER_ALLOWLIST".to_string(),
			bucket_dir.to_string_lossy().to_string(),
		);

		common::start_server(common::StartServerArguments {
			args: "--allow-experimental files,surrealism".to_string(),
			vars: Some(vars),
			..Default::default()
		})
		.await
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

	/// Run the DEFINE BUCKET + DEFINE MODULE setup queries for a fresh
	/// namespace/database.
	async fn setup_module(addr: &str, ns: &str, db: &str, bucket_dir: &Path) {
		let dir = bucket_dir.to_string_lossy();
		let setup = format!(
			"DEFINE BUCKET test BACKEND \"file:{dir}\";\
			 DEFINE MODULE mod::demo AS f\"test:/demo.surli\";"
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

		// First call: init() populates the OnceLock, cached_greeting() reads it
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::cached_greeting();").await;
		assert_eq!(results[0].status, "OK", "cached_greeting (1st): {:?}", results[0].result);
		let first_value = results[0].result.clone();
		assert!(first_value.is_string(), "cached_greeting should return a string");

		// Second call: persistent state survives, returns the same value
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::cached_greeting();").await;
		assert_eq!(results[0].status, "OK", "cached_greeting (2nd): {:?}", results[0].result);
		assert_eq!(
			results[0].result, first_value,
			"persistent state should survive across invocations"
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
	// KV store tests
	// -------------------------------------------------------------------

	async fn check_kv_operations(bucket_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		setup_module(&addr, &ns, &db, bucket_dir).await;

		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::test_kv();").await;
		assert_eq!(results[0].status, "OK", "test_kv failed: {:?}", results[0].result);

		Ok(())
	}

	#[test(tokio::test)]
	async fn module_kv_operations() -> Result<(), Box<dyn std::error::Error>> {
		check_kv_operations(&DEMO_DIR.canonical).await
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

		// Define the SurrealQL function that create_user calls via surrealism::run
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
	// KV persistence across invocations
	// -------------------------------------------------------------------

	async fn check_kv_persistence(bucket_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		setup_module(&addr, &ns, &db, bucket_dir).await;

		// Set a KV value in one invocation
		let results =
			sql_query(&addr, &ns, &db, "RETURN mod::demo::kv_set_value('persist_test', 42);").await;
		assert_eq!(results[0].status, "OK", "kv_set_value: {:?}", results[0].result);

		// Read it back in a separate invocation -- should persist across calls
		let results =
			sql_query(&addr, &ns, &db, "RETURN mod::demo::kv_get_value('persist_test');").await;
		assert_eq!(results[0].status, "OK", "kv_get_value: {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!(42));

		// Overwrite and verify
		let results =
			sql_query(&addr, &ns, &db, "RETURN mod::demo::kv_set_value('persist_test', 99);").await;
		assert_eq!(results[0].status, "OK", "kv_set_value(99): {:?}", results[0].result);

		let results =
			sql_query(&addr, &ns, &db, "RETURN mod::demo::kv_get_value('persist_test');").await;
		assert_eq!(results[0].status, "OK", "kv_get_value(99): {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::json!(99));

		// Non-existent key should return None/null
		let results =
			sql_query(&addr, &ns, &db, "RETURN mod::demo::kv_get_value('nonexistent');").await;
		assert_eq!(results[0].status, "OK", "kv_get_value(nonexistent): {:?}", results[0].result);
		assert_eq!(results[0].result, serde_json::Value::Null);

		Ok(())
	}

	#[test(tokio::test)]
	async fn module_kv_persistence() -> Result<(), Box<dyn std::error::Error>> {
		check_kv_persistence(&DEMO_DIR.canonical).await
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
		let modules = db_info["modules"].as_array().expect("modules should be an array");
		assert!(!modules.is_empty(), "at least one module should be defined");

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
			},
			capabilities: Default::default(),
			abi: AbiVersion::CURRENT,
			attach: SurrealismAttach {
				fs: Some("fs".to_string()),
			},
		};

		// Component preamble (layer 1, version 0x0d)
		let package = SurrealismPackage {
			config,
			wasm: vec![0x00, 0x61, 0x73, 0x6d, 0x0d, 0x00, 0x01, 0x00],
			exports: surrealism_runtime::exports::ExportsManifest::empty(),
			fs: None,
			logo: None,
		};

		let tmp = tempfile::TempDir::new().expect("Failed to create temp dir");
		let surli_path = tmp.path().join("test.surli");
		package.pack(surli_path.clone(), Some(&fs_dir)).expect("pack failed");

		let unpacked = SurrealismPackage::from_file(surli_path).expect("from_file failed");

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
