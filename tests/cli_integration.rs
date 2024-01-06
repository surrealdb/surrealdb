// RUST_LOG=warn cargo make ci-cli-integration
mod common;

mod cli_integration {
	use assert_fs::prelude::{FileTouch, FileWriteStr, PathChild};
	use std::fs;
	use std::fs::File;
	use std::time;
	use test_log::test;
	use tokio::time::sleep;
	use tracing::info;
	use ulid::Ulid;

	use super::common::{self, StartServerArguments, PASS, USER};

	const ONE_SEC: time::Duration = time::Duration::new(1, 0);
	const TWO_SECS: time::Duration = time::Duration::new(2, 0);

	#[test]
	fn version() {
		assert!(common::run("version").output().is_ok());
	}

	#[test]
	fn help() {
		assert!(common::run("help").output().is_ok());
	}

	#[test]
	fn nonexistent_subcommand() {
		assert!(common::run("nonexistent").output().is_err());
	}

	#[test]
	fn nonexistent_option() {
		assert!(common::run("version --turbo").output().is_err());
	}

	#[test(tokio::test)]
	async fn all_commands() {
		// Commands without credentials when auth is disabled, should succeed
		let (addr, _server) = common::start_server(StartServerArguments {
			auth: false,
			args: "--allow-all".to_string(),
			..Default::default()
		})
		.await
		.unwrap();
		let creds = ""; // Anonymous user
		let ns = Ulid::new();
		let db = Ulid::new();

		info!("* Create a record");
		{
			let args = format!(
				"sql --conn http://{addr} {creds} --ns {ns} --db {db} --multi --hide-welcome"
			);
			assert_eq!(
				common::run(&args).input("CREATE thing:one;\n").output(),
				Ok("[[{ id: thing:one }]]\n\n".to_owned()),
				"failed to send sql: {args}"
			);
		}

		info!("* Export to stdout");
		{
			let args = format!("export --conn http://{addr} {creds} --ns {ns} --db {db} -");
			let output = common::run(&args).output().expect("failed to run stdout export: {args}");
			assert!(output.contains("DEFINE TABLE thing SCHEMALESS PERMISSIONS NONE;"));
			assert!(output.contains("UPDATE thing:one CONTENT { id: thing:one };"));
		}

		info!("* Export to file");
		let exported = {
			let exported = common::tmp_file("exported.surql");
			let args =
				format!("export --conn http://{addr} {creds} --ns {ns} --db {db} {exported}");
			common::run(&args).output().expect("failed to run file export: {args}");
			exported
		};

		let db2 = Ulid::new();

		info!("* Import the exported file");
		{
			let args =
				format!("import --conn http://{addr} {creds} --ns {ns} --db {db2} {exported}");
			common::run(&args).output().expect("failed to run import: {args}");
		}

		info!("* Query from the import (pretty-printed this time)");
		{
			let args = format!(
				"sql --conn http://{addr} {creds} --ns {ns} --db {db2} --pretty --hide-welcome"
			);
			let output = common::run(&args).input("SELECT * FROM thing;\n").output().unwrap();
			let (line1, rest) = output.split_once('\n').expect("response to have multiple lines");
			assert!(line1.starts_with("-- Query 1"));
			assert!(line1.contains("execution time"));
			assert_eq!(rest, "[\n\t{\n\t\tid: thing:one\n\t}\n]\n\n", "failed to send sql: {args}");
		}

		info!("* Unfinished backup CLI");
		{
			let file = common::tmp_file("backup.db");
			let args = format!("backup {creds}  http://{addr} {file}");
			common::run(&args).output().expect("failed to run backup: {args}");

			// TODO: Once backups are functional, update this test.
			assert_eq!(fs::read_to_string(file).unwrap(), "Save");
		}

		info!("* Multi-statement (and multi-line) query including error(s) over WS");
		{
			let args = format!(
				"sql --conn ws://{addr} {creds} --ns {throwaway} --db {throwaway} --multi --pretty",
				throwaway = Ulid::new()
			);
			let output = common::run(&args)
				.input(
					"CREATE thing:success; \
				CREATE thing:fail SET bad=rand('evil'); \
				SELECT * FROM sleep(10ms) TIMEOUT 1ms; \
				CREATE thing:also_success;
				",
				)
				.output()
				.unwrap();

			assert!(output.contains("thing:success"), "missing success in {output}");
			assert!(output.contains("rgument"), "missing argument error in {output}");
			assert!(
				output.contains("time") && output.contains("out"),
				"missing timeout error in {output}"
			);
			assert!(output.contains("thing:also_success"), "missing also_success in {output}")
		}

		info!("* Multi-statement (and multi-line) transaction including error(s) over WS");
		{
			let args = format!(
				"sql --conn ws://{addr} {creds} --ns {throwaway} --db {throwaway} --multi --pretty",
				throwaway = Ulid::new()
			);
			let output = common::run(&args)
				.input(
					"BEGIN; \
				CREATE thing:success; \
				CREATE thing:fail SET bad=rand('evil'); \
				SELECT * FROM sleep(10ms) TIMEOUT 1ms; \
				CREATE thing:also_success; \
				COMMIT;
				",
				)
				.output()
				.unwrap();

			assert_eq!(
				output.lines().filter(|s| s.contains("transaction")).count(),
				3,
				"missing failed txn errors in {output:?}"
			);
			assert!(output.contains("rgument"), "missing argument error in {output}");
		}

		info!("* Pass neither ns nor db");
		{
			let args = format!("sql --conn http://{addr} {creds}");
			let output = common::run(&args)
				.input(&format!(
					"USE NS {throwaway} DB {throwaway}; CREATE thing:one;\n",
					throwaway = Ulid::new()
				))
				.output()
				.expect("neither ns nor db");
			assert!(output.contains("thing:one"), "missing thing:one in {output}");
		}

		info!("* Pass only ns");
		{
			let throwaway = Ulid::new();
			let args = format!("sql --conn http://{addr} {creds} --ns {throwaway}");
			let output = common::run(&args)
				.input("USE DB {throwaway}; SELECT * FROM thing:one;\n")
				.output()
				.expect("only ns");
			assert!(output.contains("thing:one"), "missing thing:one in {output}");
		}

		info!("* Pass only db and expect an error");
		{
			let args = format!(
				"sql --conn http://{addr} {creds} --db {throwaway}",
				throwaway = Ulid::new()
			);
			common::run(&args).output().expect_err("only db");
		}
	}

	#[test(tokio::test)]
	async fn start_tls() {
		let (_, server) = common::start_server(StartServerArguments {
			tls: true,
			wait_is_ready: false,
			..Default::default()
		})
		.await
		.unwrap();

		std::thread::sleep(std::time::Duration::from_millis(5000));
		let output = server.kill().output().err().unwrap();

		// Test the crt/key args but the keys are self signed so don't actually connect.
		assert!(output.contains("Started web server"), "couldn't start web server: {output}");
	}

	#[test(tokio::test)]
	async fn with_root_auth() {
		// Commands with credentials when auth is enabled, should succeed
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let creds = format!("--user {USER} --pass {PASS}");
		let sql_args = format!("sql --conn http://{addr} --multi --pretty");

		info!("* Query over HTTP");
		{
			let args = format!("{sql_args} {creds}");
			let input = "INFO FOR ROOT;";
			let output = common::run(&args).input(input).output();
			assert!(output.is_ok(), "failed to query over HTTP: {}", output.err().unwrap());
		}

		info!("* Query over WS");
		{
			let args = format!("sql --conn ws://{addr} --multi --pretty {creds}");
			let input = "INFO FOR ROOT;";
			let output = common::run(&args).input(input).output();
			assert!(output.is_ok(), "failed to query over WS: {}", output.err().unwrap());
		}

		info!("* Root user can do exports");
		let exported = {
			let exported = common::tmp_file("exported.surql");
			let args = format!(
				"export --conn http://{addr} {creds} --ns {throwaway} --db {throwaway} {exported}",
				throwaway = Ulid::new()
			);

			common::run(&args).output().expect("failed to run export");
			exported
		};

		info!("* Root user can do imports");
		{
			let args = format!(
				"import --conn http://{addr} {creds} --ns {throwaway} --db {throwaway} {exported}",
				throwaway = Ulid::new()
			);
			common::run(&args).output().unwrap_or_else(|_| panic!("failed to run import: {args}"));
		}

		info!("* Root user can do backups");
		{
			let file = common::tmp_file("backup.db");
			let args = format!("backup {creds} http://{addr} {file}");
			common::run(&args).output().unwrap_or_else(|_| panic!("failed to run backup: {args}"));

			// TODO: Once backups are functional, update this test.
			assert_eq!(fs::read_to_string(file).unwrap(), "Save");
		}
	}

	#[test(tokio::test)]
	async fn with_auth_level() {
		// Commands with credentials for different auth levels
		let (addr, _server) = common::start_server_with_auth_level().await.unwrap();
		let creds = format!("--user {USER} --pass {PASS}");
		let ns = Ulid::new();
		let db = Ulid::new();

		info!("* Create users with identical credentials at ROOT, NS and DB levels");
		{
			let args = format!("sql --conn http://{addr} --db {db} --ns {ns} {creds}");
			let _ = common::run(&args)
				.input(format!("DEFINE USER {USER} ON ROOT PASSWORD '{PASS}' ROLES OWNER;
                                                DEFINE USER {USER} ON NAMESPACE PASSWORD '{PASS}' ROLES OWNER;
                                                DEFINE USER {USER} ON DATABASE PASSWORD '{PASS}' ROLES OWNER;\n").as_str())
				.output()
				.expect("success");
		}

		info!("* Pass root auth level and access root info");
		{
			let args =
				format!("sql --conn http://{addr} --db {db} --ns {ns} --auth-level root {creds}");
			let output = common::run(&args)
				.input(format!("USE NS {ns} DB {db}; INFO FOR ROOT;\n").as_str())
				.output()
				.expect("success");
			assert!(
				output.contains("namespaces: {"),
				"auth level root should be able to access root info: {output}"
			);
		}

		info!("* Pass root auth level and access namespace info");
		{
			let args =
				format!("sql --conn http://{addr} --db {db} --ns {ns} --auth-level root {creds}");
			let output = common::run(&args)
				.input(format!("USE NS {ns} DB {db}; INFO FOR NS;\n").as_str())
				.output()
				.expect("success");
			assert!(
				output.contains("databases: {"),
				"auth level root should be able to access namespace info: {output}"
			);
		}

		info!("* Pass root auth level and access database info");
		{
			let args =
				format!("sql --conn http://{addr} --db {db} --ns {ns} --auth-level root {creds}");
			let output = common::run(&args)
				.input(format!("USE NS {ns} DB {db}; INFO FOR DB;\n").as_str())
				.output()
				.expect("success");
			assert!(
				output.contains("tables: {"),
				"auth level root should be able to access database info: {output}"
			);
		}

		info!("* Pass namespace auth level and access root info");
		{
			let args = format!(
				"sql --conn http://{addr} --db {db} --ns {ns} --auth-level namespace {creds}"
			);
			let output = common::run(&args)
				.input(format!("USE NS {ns} DB {db}; INFO FOR ROOT;\n").as_str())
				.output()
				.expect("success");
			assert!(
				output.contains("IAM error: Not enough permissions to perform this action"),
				"auth level namespace should not be able to access root info: {output}"
			);
		}

		info!("* Pass namespace auth level and access namespace info");
		{
			let args = format!(
				"sql --conn http://{addr} --db {db} --ns {ns} --auth-level namespace {creds}"
			);
			let output = common::run(&args)
				.input(format!("USE NS {ns} DB {db}; INFO FOR NS;\n").as_str())
				.output()
				.expect("success");
			assert!(
				output.contains("databases: {"),
				"auth level namespace should be able to access namespace info: {output}"
			);
		}

		info!("* Pass namespace auth level and access database info");
		{
			let args = format!(
				"sql --conn http://{addr} --db {db} --ns {ns} --auth-level namespace {creds}"
			);
			let output = common::run(&args)
				.input(format!("USE NS {ns} DB {db}; INFO FOR DB;\n").as_str())
				.output()
				.expect("success");
			assert!(
				output.contains("tables: {"),
				"auth level namespace should be able to access database info: {output}"
			);
		}

		info!("* Pass database auth level and access root info");
		{
			let args = format!(
				"sql --conn http://{addr} --db {db} --ns {ns} --auth-level database {creds}"
			);
			let output = common::run(&args)
				.input(format!("USE NS {ns} DB {db}; INFO FOR ROOT;\n").as_str())
				.output()
				.expect("success");
			assert!(
				output.contains("IAM error: Not enough permissions to perform this action"),
				"auth level database should not be able to access root info: {output}",
			);
		}

		info!("* Pass database auth level and access namespace info");
		{
			let args = format!(
				"sql --conn http://{addr} --db {db} --ns {ns} --auth-level database {creds}"
			);
			let output = common::run(&args)
				.input(format!("USE NS {ns} DB {db}; INFO FOR NS;\n").as_str())
				.output()
				.expect("success");
			assert!(
				output.contains("IAM error: Not enough permissions to perform this action"),
				"auth level database should not be able to access namespace info: {output}",
			);
		}

		info!("* Pass database auth level and access database info");
		{
			let args = format!(
				"sql --conn http://{addr} --db {db} --ns {ns} --auth-level database {creds}"
			);
			let output = common::run(&args)
				.input(format!("USE NS {ns} DB {db}; INFO FOR DB;\n").as_str())
				.output()
				.expect("success");
			assert!(
				output.contains("tables: {"),
				"auth level database should be able to access database info: {output}"
			);
		}

		info!("* Pass namespace auth level without specifying namespace");
		{
			let args = format!("sql --conn http://{addr} --auth-level database {creds}");
			let output = common::run(&args)
				.input(format!("USE NS {ns} DB {db}; INFO FOR NS;\n").as_str())
				.output();
			assert!(
				output
					.clone()
					.unwrap_err()
					.contains("Namespace is needed for authentication but it was not provided"),
				"auth level namespace requires providing a namespace: {:?}",
				output
			);
		}

		info!("* Pass database auth level without specifying database");
		{
			let args = format!("sql --conn http://{addr} --ns {ns} --auth-level database {creds}");
			let output = common::run(&args)
				.input(format!("USE NS {ns} DB {db}; INFO FOR DB;\n").as_str())
				.output();
			assert!(
				output
					.clone()
					.unwrap_err()
					.contains("Database is needed for authentication but it was not provided"),
				"auth level database requires providing a namespace and database: {:?}",
				output
			);
		}
	}

	#[test(tokio::test)]
	// TODO(gguillemas): Remove this test once the legacy authentication is deprecated in v2.0.0
	async fn without_auth_level() {
		// Commands with credentials for different auth levels
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let creds = format!("--user {USER} --pass {PASS}");
		let ns = Ulid::new();
		let db = Ulid::new();

		info!("* Create users with identical credentials at ROOT, NS and DB levels");
		{
			let args = format!("sql --conn http://{addr} --db {db} --ns {ns} {creds}");
			let _ = common::run(&args)
				.input(format!("DEFINE USER {USER}_root ON ROOT PASSWORD '{PASS}' ROLES OWNER;
                                                DEFINE USER {USER}_ns ON NAMESPACE PASSWORD '{PASS}' ROLES OWNER;
                                                DEFINE USER {USER}_db ON DATABASE PASSWORD '{PASS}' ROLES OWNER;\n").as_str())
				.output()
				.expect("success");
		}

		info!("* Pass root level credentials and access root info");
		{
			let args = format!(
				"sql --conn http://{addr} --db {db} --ns {ns} --user {USER}_root --pass {PASS}"
			);
			let output = common::run(&args)
				.input(format!("USE NS {ns} DB {db}; INFO FOR ROOT;\n").as_str())
				.output()
				.expect("success");
			assert!(
				output.contains("namespaces: {"),
				"auth level root should be able to access root info: {output}"
			);
		}

		info!("* Pass namespace level credentials and access namespace info");
		{
			let args = format!(
				"sql --conn http://{addr} --db {db} --ns {ns} --user {USER}_ns --pass {PASS}"
			);
			let output = common::run(&args)
				.input(format!("USE NS {ns} DB {db}; INFO FOR NS;\n").as_str())
				.output();
			assert!(
				output.clone().unwrap_err().contains("401 Unauthorized"),
				"namespace level credentials should not work with CLI authentication: {:?}",
				output
			);
		}

		info!("* Pass database level credentials and access database info");
		{
			let args = format!(
				"sql --conn http://{addr} --db {db} --ns {ns} --user {USER}_db --pass {PASS}"
			);
			let output = common::run(&args)
				.input(format!("USE NS {ns} DB {db}; INFO FOR DB;\n").as_str())
				.output();
			assert!(
				output.clone().unwrap_err().contains("401 Unauthorized"),
				"database level credentials should not work with CLI authentication: {:?}",
				output
			);
		}
	}

	#[test(tokio::test)]
	async fn with_anon_auth() {
		// Commands without credentials when auth is enabled, should fail
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let creds = ""; // Anonymous user
		let sql_args = format!("sql --conn http://{addr} --multi --pretty");

		info!("* Query over HTTP");
		{
			let args = format!("{sql_args} {creds}");
			let input = "";
			let output = common::run(&args).input(input).output();
			assert!(output.is_ok(), "anonymous user should be able to query: {:?}", output);
		}

		info!("* Query over WS");
		{
			let args = format!("sql --conn ws://{addr} --multi --pretty {creds}");
			let input = "";
			let output = common::run(&args).input(input).output();
			assert!(output.is_ok(), "anonymous user should be able to query: {:?}", output);
		}

		info!("* Can't do exports");
		{
			let args = format!(
				"export --conn http://{addr} {creds} --ns {throwaway} --db {throwaway} -",
				throwaway = Ulid::new()
			);
			let output = common::run(&args).output();
			assert!(
				output.clone().unwrap_err().contains("Forbidden"),
				"anonymous user shouldn't be able to export: {:?}",
				output
			);
		}

		info!("* Can't do imports");
		{
			let tmp_file = common::tmp_file("exported.surql");
			File::create(&tmp_file).expect("failed to create tmp file");
			let args = format!(
				"import --conn http://{addr} {creds} --ns {throwaway} --db {throwaway} {tmp_file}",
				throwaway = Ulid::new()
			);
			let output = common::run(&args).output();
			assert!(
				output.clone().unwrap_err().contains("Forbidden"),
				"anonymous user shouldn't be able to import: {:?}",
				output
			);
		}

		info!("* Can't do backups");
		{
			let args = format!("backup {creds} http://{addr}");
			let output = common::run(&args).output();
			// TODO(sgirones): Once backups are functional, update this test.
			// assert!(
			// 	output.unwrap_err().contains("Forbidden"),
			// 	"anonymous user shouldn't be able to backup",
			// 	output
			// );
			assert!(output.is_ok(), "anonymous user can do backups: {:?}", output);
		}
	}

	#[test(tokio::test)]
	async fn node() {
		// Commands without credentials when auth is disabled, should succeed
		let (addr, _server) = common::start_server(StartServerArguments {
			auth: false,
			tls: false,
			wait_is_ready: true,
			tick_interval: ONE_SEC,
			..Default::default()
		})
		.await
		.unwrap();
		let creds = ""; // Anonymous user

		let ns = Ulid::new();
		let db = Ulid::new();

		info!("* Define a table");
		{
			let args = format!(
				"sql --conn http://{addr} {creds} --ns {ns} --db {db} --multi --hide-welcome"
			);
			assert_eq!(
				common::run(&args).input("DEFINE TABLE thing CHANGEFEED 1s;\n").output(),
				Ok("[NONE]\n\n".to_owned()),
				"failed to send sql: {args}"
			);
		}

		info!("* Create a record");
		{
			let args = format!(
				"sql --conn http://{addr} {creds} --ns {ns} --db {db} --multi --hide-welcome"
			);
			assert_eq!(
				common::run(&args).input("BEGIN TRANSACTION; CREATE thing:one; COMMIT;\n").output(),
				Ok("[[{ id: thing:one }]]\n\n".to_owned()),
				"failed to send sql: {args}"
			);
		}

		info!("* Show changes");
		{
			let args = format!(
				"sql --conn http://{addr} {creds} --ns {ns} --db {db} --multi --hide-welcome"
			);
			assert_eq!(
				common::run(&args)
					.input("SHOW CHANGES FOR TABLE thing SINCE 0 LIMIT 10;\n")
					.output(),
				Ok("[[{ changes: [{ define_table: { name: 'thing' } }], versionstamp: 65536 }, { changes: [{ update: { id: thing:one } }], versionstamp: 131072 }]]\n\n"
					.to_owned()),
				"failed to send sql: {args}"
			);
		}

		sleep(TWO_SECS).await;

		info!("* Show changes after GC");
		{
			let args = format!(
				"sql --conn http://{addr} {creds} --ns {ns} --db {db} --multi --hide-welcome"
			);
			assert_eq!(
				common::run(&args)
					.input("SHOW CHANGES FOR TABLE thing SINCE 0 LIMIT 10;\n")
					.output(),
				Ok("[[]]\n\n".to_owned()),
				"failed to send sql: {args}"
			);
		}
	}

	#[test]
	fn validate_found_no_files() {
		let temp_dir = assert_fs::TempDir::new().unwrap();

		temp_dir.child("file.txt").touch().unwrap();

		assert!(common::run_in_dir("validate", &temp_dir).output().is_err());
	}

	#[test]
	fn validate_succeed_for_valid_surql_files() {
		let temp_dir = assert_fs::TempDir::new().unwrap();

		let statement_file = temp_dir.child("statement.surql");

		statement_file.touch().unwrap();
		statement_file.write_str("CREATE thing:success;").unwrap();

		assert!(common::run_in_dir("validate", &temp_dir).output().is_ok());
	}

	#[test]
	fn validate_failed_due_to_invalid_glob_pattern() {
		let temp_dir = assert_fs::TempDir::new().unwrap();

		const WRONG_GLOB_PATTERN: &str = "**/*{.txt";

		let args = format!("validate \"{}\"", WRONG_GLOB_PATTERN);

		assert!(common::run_in_dir(&args, &temp_dir).output().is_err());
	}

	#[test]
	fn validate_failed_due_to_invalid_surql_files_syntax() {
		let temp_dir = assert_fs::TempDir::new().unwrap();

		let statement_file = temp_dir.child("statement.surql");

		statement_file.touch().unwrap();
		statement_file.write_str("CREATE $thing WHERE value = '';").unwrap();

		assert!(common::run_in_dir("validate", &temp_dir).output().is_err());
	}

	#[test(tokio::test)]
	async fn test_server_graceful_shutdown() {
		let (_, mut server) = common::start_server_with_defaults().await.unwrap();

		info!("* Send SIGINT signal");
		server
			.send_signal(nix::sys::signal::Signal::SIGINT)
			.expect("Failed to send SIGINT to server");

		info!("* Waiting for server to exit gracefully ...");
		tokio::select! {
			_ = async {
				loop {
					if let Ok(Some(exit)) = server.status() {
						assert!(exit.success(), "Server didn't shutdown successfully:\n{}", server.output().unwrap_err());
						break;
					}
					tokio::time::sleep(time::Duration::from_secs(1)).await;
				}
			} => {},
			// Timeout after 5 seconds
			_ = tokio::time::sleep(time::Duration::from_secs(5)) => {
				panic!("Server didn't exit after receiving SIGINT");
			}
		}
	}

	#[test(tokio::test)]
	async fn test_server_second_signal_handling() {
		let (addr, mut server) = common::start_server_without_auth().await.unwrap();

		// Create a long-lived WS connection so the server don't shutdown gracefully
		let mut socket = common::connect_ws(&addr).await.expect("Failed to connect to server");
		let json = serde_json::json!({
			"id": "1",
			"method": "query",
			"params": ["SLEEP 30s;"],
		});
		common::ws_send_msg(&mut socket, serde_json::to_string(&json).unwrap())
			.await
			.expect("Failed to send WS message");

		// Make sure the SLEEP query is being executed
		tokio::select! {
			_ = async {
				loop {
					if server.stderr().contains("Executing: SLEEP 30s") {
						break;
					}
					tokio::time::sleep(time::Duration::from_secs(1)).await;
				}
			} => {},
			// Timeout after 10 seconds
			_ = tokio::time::sleep(time::Duration::from_secs(10)) => panic!("Server didn't start executing the SLEEP query"),
		}

		info!("* Send first SIGINT signal");
		server
			.send_signal(nix::sys::signal::Signal::SIGINT)
			.expect("Failed to send SIGINT to server");

		tokio::select! {
			_ = async {
				loop {
					if let Ok(Some(exit)) = server.status() {
						panic!("Server unexpectedly exited after receiving first SIGINT: {:?}", exit);
					}
					tokio::time::sleep(time::Duration::from_secs(1)).await;
				}
			} => {},
			// Timeout after 5 seconds
			_ = tokio::time::sleep(time::Duration::from_secs(5)) => ()
		}

		info!("* Send second SIGINT signal");
		server
			.send_signal(nix::sys::signal::Signal::SIGINT)
			.expect("Failed to send SIGINT to server");

		tokio::select! {
			_ = async {
				loop {
					if let Ok(Some(exit)) = server.status() {
						assert!(exit.success(), "Server shutted down successfully");
						break;
					}
					tokio::time::sleep(time::Duration::from_secs(1)).await;
				}
			} => {},
			// Timeout after 5 seconds
			_ = tokio::time::sleep(time::Duration::from_secs(5)) => {
				panic!("Server didn't exit after receiving two SIGINT signals");
			}
		}
	}

	#[test(tokio::test)]
	async fn test_capabilities() {
		// Default capabilities only allow functions
		info!("* When default capabilities");
		{
			let (addr, _server) = common::start_server(StartServerArguments {
				args: "".to_owned(),
				..Default::default()
			})
			.await
			.unwrap();

			let cmd = format!(
				"sql --conn ws://{addr} -u root -p root --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = "RETURN http::get('http://127.0.0.1/');\n\n";
			let output = common::run(&cmd).input(query).output().unwrap();
			assert!(
				output.contains("Access to network target 'http://127.0.0.1/' is not allowed"),
				"unexpected output: {output:?}"
			);

			let query = "RETURN function() { return '1' };";
			let output = common::run(&cmd).input(query).output().unwrap();
			assert!(
				output.contains("Scripting functions are not allowed"),
				"unexpected output: {output:?}"
			);
		}

		// Deny all, denies all users to execute functions and access any network address
		info!("* When all capabilities are denied");
		{
			let (addr, _server) = common::start_server(StartServerArguments {
				args: "--deny-all".to_owned(),
				..Default::default()
			})
			.await
			.unwrap();

			let cmd = format!(
				"sql --conn ws://{addr} -u root -p root --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = format!("RETURN http::get('http://{}/version');\n\n", addr);
			let output = common::run(&cmd).input(&query).output().unwrap();
			assert!(
				output.contains("Function 'http::get' is not allowed"),
				"unexpected output: {output:?}"
			);

			let query = "RETURN function() { return '1' };";
			let output = common::run(&cmd).input(query).output().unwrap();
			assert!(
				output.contains("Scripting functions are not allowed"),
				"unexpected output: {output:?}"
			);
		}

		// When all capabilities are allowed, anyone (including non-authenticated users) can execute functions and access any network address
		info!("* When all capabilities are allowed");
		{
			let (addr, _server) = common::start_server(StartServerArguments {
				args: "--allow-all".to_owned(),
				..Default::default()
			})
			.await
			.unwrap();

			let cmd = format!(
				"sql --conn ws://{addr} --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = format!("RETURN http::get('http://{}/version');\n\n", addr);
			let output = common::run(&cmd).input(&query).output().unwrap();
			assert!(output.starts_with("['surrealdb"), "unexpected output: {output:?}");

			let query = "RETURN function() { return '1' };";
			let output = common::run(&cmd).input(query).output().unwrap();
			assert!(output.starts_with("['1']"), "unexpected output: {output:?}");
		}

		info!("* When scripting is denied");
		{
			let (addr, _server) = common::start_server(StartServerArguments {
				args: "--deny-scripting".to_owned(),
				..Default::default()
			})
			.await
			.unwrap();

			let cmd = format!(
				"sql --conn ws://{addr} -u root -p root --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = "RETURN function() { return '1' };";
			let output = common::run(&cmd).input(query).output().unwrap();
			assert!(
				output.contains("Scripting functions are not allowed"),
				"unexpected output: {output:?}"
			);
		}

		info!("* When net is denied and function is enabled");
		{
			let (addr, _server) = common::start_server(StartServerArguments {
				args: "--deny-net 127.0.0.1 --allow-funcs http::get".to_owned(),
				..Default::default()
			})
			.await
			.unwrap();

			let cmd = format!(
				"sql --conn ws://{addr} -u root -p root --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = format!("RETURN http::get('http://{}/version');\n\n", addr);
			let output = common::run(&cmd).input(&query).output().unwrap();
			assert!(
				output.contains(
					format!("Access to network target 'http://{addr}/version' is not allowed")
						.as_str()
				),
				"unexpected output: {output:?}"
			);
		}

		info!("* When net is enabled for an IP and also denied for a specific port that doesn't match");
		{
			let (addr, _server) = common::start_server(StartServerArguments {
				args: "--allow-net 127.0.0.1 --deny-net 127.0.0.1:80 --allow-funcs http::get"
					.to_owned(),
				..Default::default()
			})
			.await
			.unwrap();

			let cmd = format!(
				"sql --conn ws://{addr} -u root -p root --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = format!("RETURN http::get('http://{}/version');\n\n", addr);
			let output = common::run(&cmd).input(&query).output().unwrap();
			assert!(output.starts_with("['surrealdb"), "unexpected output: {output:?}");
		}

		info!("* When a function family is denied");
		{
			let (addr, _server) = common::start_server(StartServerArguments {
				args: "--deny-funcs http".to_owned(),
				..Default::default()
			})
			.await
			.unwrap();

			let cmd = format!(
				"sql --conn ws://{addr} -u root -p root --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = "RETURN http::get('https://surrealdb.com');\n\n";
			let output = common::run(&cmd).input(query).output().unwrap();
			assert!(
				output.contains("Function 'http::get' is not allowed"),
				"unexpected output: {output:?}"
			);
		}

		info!("* When auth is enabled and guest access is allowed");
		{
			let (addr, _server) = common::start_server(StartServerArguments {
				auth: true,
				args: "--allow-guests".to_owned(),
				..Default::default()
			})
			.await
			.unwrap();

			let cmd = format!(
				"sql --conn ws://{addr} --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = "RETURN 1;\n\n";
			let output = common::run(&cmd).input(query).output().unwrap();
			assert!(output.contains("[1]"), "unexpected output: {output:?}");
		}

		info!("* When auth is enabled and guest access is denied");
		{
			let (addr, _server) = common::start_server(StartServerArguments {
				auth: true,
				args: "--deny-guests".to_owned(),
				..Default::default()
			})
			.await
			.unwrap();

			let cmd = format!(
				"sql --conn ws://{addr} --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = "RETURN 1;\n\n";
			let output = common::run(&cmd).input(query).output().unwrap();
			assert!(
				output.contains("Not enough permissions to perform this action"),
				"unexpected output: {output:?}"
			);
		}

		info!("* When auth is disabled, guest access is always allowed");
		{
			let (addr, _server) = common::start_server(StartServerArguments {
				auth: false,
				args: "--deny-guests".to_owned(),
				..Default::default()
			})
			.await
			.unwrap();

			let cmd = format!(
				"sql --conn ws://{addr} --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = "RETURN 1;\n\n";
			let output = common::run(&cmd).input(query).output().unwrap();
			assert!(output.contains("[1]"), "unexpected output: {output:?}");
		}
	}
}
