mod common;

#[cfg(feature = "surrealism")]
mod surrealism_integration {

	use std::collections::HashMap;
	use std::path::{Path, PathBuf};
	use std::process::Command;
	use std::sync::LazyLock;
	use std::time::Duration;

	use serde::Deserialize;
	use surrealism_runtime::config::{AbiVersion, SurrealismConfig, SurrealismMeta};
	use surrealism_runtime::package::{SurrealismPackage, detect_module_kind};
	use test_log::test;
	use ulid::Ulid;

	use super::*;

	#[derive(Deserialize, Debug)]
	struct QueryResult {
		result: serde_json::Value,
		status: String,
	}

	/// Check whether the `wasm32-wasip1` target is installed via rustup.
	fn has_wasm_target() -> bool {
		Command::new("rustup")
			.args(["target", "list", "--installed"])
			.output()
			.map(|o| String::from_utf8_lossy(&o.stdout).contains("wasm32-wasip1"))
			.unwrap_or(false)
	}

	/// Build the demo surrealism module as a P1 core module and pack it into
	/// `output_dir/demo.surli`.
	fn build_demo_module(output_dir: &Path) {
		let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"));

		let result = Command::new("cargo")
			.args(["build", "-p", "demo", "--target", "wasm32-wasip1"])
			.current_dir(workspace_root)
			.output()
			.expect("Failed to execute cargo build for demo module");

		assert!(
			result.status.success(),
			"cargo build -p demo failed.\nstdout: {}\nstderr: {}",
			String::from_utf8_lossy(&result.stdout),
			String::from_utf8_lossy(&result.stderr),
		);

		let wasm_path =
			workspace_root.join("target/wasm32-wasip1/debug/demo.wasm");
		assert!(wasm_path.exists(), "demo.wasm not found at {}", wasm_path.display());

		let wasm = std::fs::read(&wasm_path).expect("Failed to read demo.wasm");
		let kind = detect_module_kind(&wasm);

		let config = SurrealismConfig {
			meta: SurrealismMeta {
				organisation: "surrealdb".to_string(),
				name: "demo".to_string(),
				version: semver::Version::new(1, 0, 0),
			},
			capabilities: Default::default(),
			abi: AbiVersion::P1,
		};

		let package = SurrealismPackage {
			config,
			wasm,
			kind,
		};

		let output = output_dir.join("demo.surli");
		package.pack(output.clone()).expect("Failed to pack demo.surli");
		assert!(output.exists(), "demo.surli not created at {}", output.display());
	}

	struct DemoModuleDir {
		_tmp: tempfile::TempDir,
		/// Canonical (symlink-resolved) path. On macOS `/var` is a symlink to
		/// `/private/var`, so the raw TempDir path would be rejected by
		/// `SURREAL_BUCKET_FOLDER_ALLOWLIST`.
		canonical: PathBuf,
	}

	/// Shared demo module build directory. The module is built once and reused
	/// across all tests in this module.
	static DEMO_DIR: LazyLock<DemoModuleDir> = LazyLock::new(|| {
		if !has_wasm_target() {
			panic!(
				"wasm32-wasip1 target not installed — install with: rustup target add wasm32-wasip1"
			);
		}
		let tmp = tempfile::TempDir::new().expect("Failed to create temp dir for demo module");
		let canonical =
			std::fs::canonicalize(tmp.path()).expect("Failed to canonicalize temp dir path");
		build_demo_module(&canonical);
		DemoModuleDir {
			_tmp: tmp,
			canonical,
		}
	});

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

	// -------------------------------------------------------------------
	// Tests
	// -------------------------------------------------------------------

	#[test(tokio::test)]
	async fn module_function_calls() -> Result<(), Box<dyn std::error::Error>> {
		let bucket_dir = &DEMO_DIR.canonical;
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

	#[test(tokio::test)]
	async fn module_kv_operations() -> Result<(), Box<dyn std::error::Error>> {
		let bucket_dir = &DEMO_DIR.canonical;
		let (addr, _server) = start_surrealism_server(bucket_dir).await?;
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		setup_module(&addr, &ns, &db, bucket_dir).await;

		// test_kv() exercises set/get/del/exists, range queries, batch ops, and
		// range deletes internally via assertions. A successful return means the
		// full KV integration works.
		let results = sql_query(&addr, &ns, &db, "RETURN mod::demo::test_kv();").await;
		assert_eq!(results[0].status, "OK", "test_kv failed: {:?}", results[0].result);

		Ok(())
	}
}
