// RUST_LOG=warn cargo make ci-cli-integration
mod common;

mod cli_integration {
	use crate::remove_debug_info;
	use assert_fs::prelude::{FileTouch, FileWriteStr, PathChild};
	use common::Format;
	use common::Socket;
	use serde_json::json;
	use std::fs::File;
	use std::time;
	use std::time::Duration;
	use surrealdb::fflags::FFLAGS;
	use test_log::test;
	use tokio::time::sleep;
	use tracing::info;
	use ulid::Ulid;

	use super::common::{self, StartServerArguments, PASS, USER};

	/// This depends on the interval configuration that we cannot yet inject
	const ONE_PERIOD: Duration = Duration::new(10, 0);
	const TWO_PERIODS: Duration = Duration::new(20, 0);

	#[test]
	fn version_command() {
		assert!(common::run("version").output().is_ok());
	}

	#[test]
	fn version_flag_short() {
		assert!(common::run("-V").output().is_ok());
	}

	#[test]
	fn version_flag_long() {
		assert!(common::run("--version").output().is_ok());
	}

	#[test]
	fn help_command() {
		assert!(common::run("help").output().is_ok());
	}

	#[test]
	fn help_flag_short() {
		assert!(common::run("-h").output().is_ok());
	}

	#[test]
	fn help_flag_long() {
		assert!(common::run("--help").output().is_ok());
	}

	#[test]
	fn nonexistent_subcommand() {
		assert!(common::run("nonexistent").output().is_err());
	}

	#[test]
	fn nonexistent_option() {
		assert!(common::run("version --turbo").output().is_err());
	}

	fn debug_builds_contain_debug_message(addr: &str, creds: &str, ns: &Ulid, db: &Ulid) {
		info!("* Debug builds contain debug message");
		let args =
			format!("sql --conn http://{addr} {creds} --ns {ns} --db {db} --multi --hide-welcome");
		let res = common::run(&args).input("CREATE not_a_table:not_a_record;\n").output().unwrap();
		assert!(res.contains("Debug builds are not intended for production use"));
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

		#[cfg(debug_assertions)]
		debug_builds_contain_debug_message(&addr, creds, &ns, &db);

		info!("* Create a record");
		{
			let args = format!(
				"sql --conn http://{addr} {creds} --ns {ns} --db {db} --multi --hide-welcome"
			);
			let output = common::run(&args).input("CREATE thing:one;\n").output().unwrap();
			assert!(output.contains("[[{ id: thing:one }]]\n\n"), "failed to send sql: {args}");
		}

		info!("* Export to stdout");
		{
			let args = format!("export --conn http://{addr} {creds} --ns {ns} --db {db} -");
			let output = common::run(&args).output().expect("failed to run stdout export: {args}");
			assert!(output.contains("DEFINE TABLE thing TYPE ANY SCHEMALESS PERMISSIONS NONE;"));
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
			let output = remove_debug_info(output);
			let (line1, rest) = output.split_once('\n').expect("response to have multiple lines");
			assert!(line1.starts_with("-- Query 1"), "Expected on {line1}, and rest was {rest}");
			assert!(line1.contains("execution time"));
			assert_eq!(rest, "[\n\t{\n\t\tid: thing:one\n\t}\n]\n\n", "failed to send sql: {args}");
		}

		info!("* Advanced uncomputed variable to be computed before saving");
		{
			let args = format!(
				"sql --conn ws://{addr} {creds} --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);
			let output = common::run(&args)
				.input(
					"DEFINE PARAM $something VALUE <set>[1, 2, 3]; \
				$something;
				",
				)
				.output()
				.unwrap();

			assert!(output.contains("[1, 2, 3]"), "missing success in {output}");
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
					"USE NS `{throwaway}` DB `{throwaway}`; CREATE thing:one;\n",
					throwaway = Ulid::new()
				))
				.output()
				.expect("neither ns nor db");
			assert!(output.contains("thing:one"), "missing thing:one in {output}");
		}

		info!("* Pass only ns");
		{
			let args = format!("sql --conn http://{addr} {creds} --ns {ns}");
			let output = common::run(&args)
				.input(&format!("USE DB `{db}`; SELECT * FROM thing:one;\n"))
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
		let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
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

		server.finish().unwrap();
	}

	#[test(tokio::test)]
	async fn with_auth_level() {
		// Commands with credentials for different auth levels
		let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
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
				.input(format!("USE NS `{ns}` DB `{db}`; INFO FOR ROOT;\n").as_str())
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
				.input(format!("USE NS `{ns}` DB `{db}`; INFO FOR NS;\n").as_str())
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
				.input(format!("USE NS `{ns}` DB `{db}`; INFO FOR DB;\n").as_str())
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
				.input(format!("USE NS `{ns}` DB `{db}`; INFO FOR ROOT;\n").as_str())
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
				.input(format!("USE NS `{ns}` DB `{db}`; INFO FOR NS;\n").as_str())
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
				.input(format!("USE NS `{ns}` DB `{db}`; INFO FOR DB;\n").as_str())
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
				.input(format!("USE NS `{ns}` DB `{db}`; INFO FOR ROOT;\n").as_str())
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
				.input(format!("USE NS `{ns}` DB `{db}`; INFO FOR NS;\n").as_str())
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
				.input(format!("USE NS `{ns}` DB `{db}`; INFO FOR DB;\n").as_str())
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
				.input(format!("USE NS `{ns}` DB `{db}`; INFO FOR NS;\n").as_str())
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
				.input(format!("USE NS `{ns}` DB `{db}`; INFO FOR DB;\n").as_str())
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
		server.finish().unwrap();
	}

	#[test(tokio::test)]
	async fn with_anon_auth() {
		// Commands without credentials when auth is enabled, should fail
		let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
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
		server.finish().unwrap();
	}

	#[test(tokio::test)]
	async fn node() {
		// Commands without credentials when auth is disabled, should succeed
		let (addr, mut server) = common::start_server(StartServerArguments {
			auth: false,
			tls: false,
			wait_is_ready: true,
			tick_interval: ONE_PERIOD,
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
			let output = common::run(&args)
				.input("DEFINE TABLE thing TYPE ANY CHANGEFEED 1s;\n")
				.output()
				.unwrap();
			let output = remove_debug_info(output);
			assert_eq!(output, "[NONE]\n\n".to_owned(), "failed to send sql: {args}");
		}

		info!("* Create a record");
		{
			let args = format!(
				"sql --conn http://{addr} {creds} --ns {ns} --db {db} --multi --hide-welcome"
			);
			let output = common::run(&args)
				.input("BEGIN TRANSACTION; CREATE thing:one; COMMIT;\n")
				.output()
				.unwrap();
			let output = remove_debug_info(output);
			assert_eq!(
				output,
				"[[{ id: thing:one }]]\n\n".to_owned(),
				"failed to send sql: {args}"
			);
		}

		info!("* Show changes");
		{
			let args = format!(
				"sql --conn http://{addr} {creds} --ns {ns} --db {db} --multi --hide-welcome"
			);
			if FFLAGS.change_feed_live_queries.enabled() {
				let output = common::run(&args)
					.input("SHOW CHANGES FOR TABLE thing SINCE 0 LIMIT 10;\n")
					.output()
					.unwrap();
				let output = remove_debug_info(output).replace('\n', "");
				// TODO: when enabling the feature flag, turn these to `create` not `update`
				let allowed = [
					// Delete these
					"[[{ changes: [{ define_table: { name: 'thing' } }], versionstamp: 1 }, { changes: [{ update: { id: thing:one } }], versionstamp: 2 }]]",
					"[[{ changes: [{ define_table: { name: 'thing' } }], versionstamp: 1 }, { changes: [{ update: { id: thing:one } }], versionstamp: 3 }]]",
					"[[{ changes: [{ define_table: { name: 'thing' } }], versionstamp: 2 }, { changes: [{ update: { id: thing:one } }], versionstamp: 3 }]]",
					"[[{ changes: [{ define_table: { name: 'thing' } }], versionstamp: 2 }, { changes: [{ update: { id: thing:one } }], versionstamp: 4 }]]",
					// Keep these
					"[[{ changes: [{ define_table: { name: 'thing' } }], versionstamp: 65536 }, { changes: [{ update: { id: thing:one } }], versionstamp: 131072 }]]",
					"[[{ changes: [{ define_table: { name: 'thing' } }], versionstamp: 65536 }, { changes: [{ update: { id: thing:one } }], versionstamp: 196608 }]]",
					"[[{ changes: [{ define_table: { name: 'thing' } }], versionstamp: 131072 }, { changes: [{ update: { id: thing:one } }], versionstamp: 196608 }]]",
					"[[{ changes: [{ define_table: { name: 'thing' } }], versionstamp: 131072 }, { changes: [{ update: { id: thing:one } }], versionstamp: 262144 }]]",
				];
				allowed
					.into_iter()
					.find(|case| {
						println!("Comparing 2:\n{case}\n{output}");
						*case == output
					})
					.ok_or(format!("Output didnt match an example output: {output}"))
					.unwrap();
			} else {
				let output = common::run(&args)
					.input("SHOW CHANGES FOR TABLE thing SINCE 0 LIMIT 10;\n")
					.output()
					.unwrap();
				let output = remove_debug_info(output).replace('\n', "");
				let allowed = [
					// Delete these
					"[[{ changes: [{ define_table: { name: 'thing' } }], versionstamp: 1 }, { changes: [{ update: { id: thing:one } }], versionstamp: 2 }]]",
					"[[{ changes: [{ define_table: { name: 'thing' } }], versionstamp: 1 }, { changes: [{ update: { id: thing:one } }], versionstamp: 3 }]]",
					"[[{ changes: [{ define_table: { name: 'thing' } }], versionstamp: 2 }, { changes: [{ update: { id: thing:one } }], versionstamp: 3 }]]",
					"[[{ changes: [{ define_table: { name: 'thing' } }], versionstamp: 2 }, { changes: [{ update: { id: thing:one } }], versionstamp: 4 }]]",
					// Keep these
					"[[{ changes: [{ define_table: { name: 'thing' } }], versionstamp: 65536 }, { changes: [{ update: { id: thing:one } }], versionstamp: 131072 }]]",
					"[[{ changes: [{ define_table: { name: 'thing' } }], versionstamp: 65536 }, { changes: [{ update: { id: thing:one } }], versionstamp: 196608 }]]",
					"[[{ changes: [{ define_table: { name: 'thing' } }], versionstamp: 131072 }, { changes: [{ update: { id: thing:one } }], versionstamp: 196608 }]]",
					"[[{ changes: [{ define_table: { name: 'thing' } }], versionstamp: 131072 }, { changes: [{ update: { id: thing:one } }], versionstamp: 262144 }]]",
				];
				allowed
					.into_iter()
					.find(|case| {
						let a = *case == output;
						println!("Comparing\n{case}\n{output}\n{a}");
						a
					})
					.ok_or(format!("Output didnt match an example output: {output}"))
					.unwrap();
			}
		};

		sleep(TWO_PERIODS).await;

		info!("* Show changes after GC");
		{
			let args = format!(
				"sql --conn http://{addr} {creds} --ns {ns} --db {db} --multi --hide-welcome"
			);
			let output = common::run(&args)
				.input("SHOW CHANGES FOR TABLE thing SINCE 0 LIMIT 10;\n")
				.output()
				.unwrap();
			let output = remove_debug_info(output);
			assert_eq!(output, "[[]]\n\n".to_owned(), "failed to send sql: {args}");
		}
		server.finish().unwrap();
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
		let socket =
			Socket::connect(&addr, None, Format::Json).await.expect("Failed to connect to server");

		let send_future = socket.send_request("query", json!(["SLEEP 30s;"]));

		let signal_send_fut = async {
			// Make sure the SLEEP query is being executed
			tokio::time::timeout(time::Duration::from_secs(10), async {
				loop {
					let err = server.stderr();
					if err.contains("SLEEP 30s") {
						break;
					}
					tokio::time::sleep(time::Duration::from_secs(1)).await;
				}
			})
			.await
			.expect("Server didn't start executing the SLEEP query");

			info!("* Send first SIGINT signal");
			server
				.send_signal(nix::sys::signal::Signal::SIGINT)
				.expect("Failed to send SIGINT to server");

			tokio::time::timeout(time::Duration::from_secs(10), async {
				loop {
					if let Ok(Some(exit)) = server.status() {
						panic!(
							"Server unexpectedly exited after receiving first SIGINT: {:?}",
							exit
						);
					}
					tokio::time::sleep(time::Duration::from_millis(100)).await;
				}
			})
			.await
			.unwrap_err();

			info!("* Send second SIGINT signal");

			server
				.send_signal(nix::sys::signal::Signal::SIGINT)
				.expect("Failed to send SIGINT to server");

			tokio::time::timeout(time::Duration::from_secs(5), async {
				loop {
					if let Ok(Some(exit)) = server.status() {
						assert!(exit.success(), "Server shutted down successfully");
						break;
					}
					tokio::time::sleep(time::Duration::from_millis(100)).await;
				}
			})
			.await
			.expect("Server didn't exit after receiving two SIGINT signals");
		};

		let _ =
			futures::future::join(async { send_future.await.unwrap_err() }, signal_send_fut).await;

		server.finish().unwrap();
	}

	#[test(tokio::test)]
	#[ignore]
	async fn test_capabilities() {
		// Default capabilities only allow functions
		info!("* When default capabilities");
		{
			let (addr, mut server) = common::start_server(StartServerArguments {
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
				output.contains("Scripting functions are not allowed")
					|| output.contains("Embedded functions are not enabled"),
				"unexpected output: {output:?}"
			);

			server.finish().unwrap();
		}

		// Deny all, denies all users to execute functions and access any network address
		info!("* When all capabilities are denied");
		{
			let (addr, mut server) = common::start_server(StartServerArguments {
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
				output.contains("Scripting functions are not allowed")
					|| output.contains("Embedded functions are not enabled"),
				"unexpected output: {output:?}"
			);
			server.finish().unwrap();
		}

		// When all capabilities are allowed, anyone (including non-authenticated users) can execute functions and access any network address
		info!("* When all capabilities are allowed");
		{
			let (addr, mut server) = common::start_server(StartServerArguments {
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

			server.finish().unwrap();
		}

		info!("* When scripting is denied");
		{
			let (addr, mut server) = common::start_server(StartServerArguments {
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
				output.contains("Scripting functions are not allowed")
					|| output.contains("Embedded functions are not enabled"),
				"unexpected output: {output:?}"
			);
			server.finish().unwrap();
		}

		info!("* When net is denied and function is enabled");
		{
			let (addr, mut server) = common::start_server(StartServerArguments {
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
			server.finish().unwrap();
		}

		info!("* When net is enabled for an IP and also denied for a specific port that doesn't match");
		{
			let (addr, mut server) = common::start_server(StartServerArguments {
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
			server.finish().unwrap();
		}

		info!("* When a function family is denied");
		{
			let (addr, mut server) = common::start_server(StartServerArguments {
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
			server.finish().unwrap();
		}

		info!("* When auth is enabled and guest access is allowed");
		{
			let (addr, mut server) = common::start_server(StartServerArguments {
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
			server.finish().unwrap();
		}

		info!("* When auth is enabled and guest access is denied");
		{
			let (addr, mut server) = common::start_server(StartServerArguments {
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
			server.finish().unwrap();
		}

		info!("* When auth is disabled, guest access is always allowed");
		{
			let (addr, mut server) = common::start_server(StartServerArguments {
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
			server.finish().unwrap();
		}
	}

	#[test(tokio::test)]
	async fn test_temporary_directory() {
		info!("* The path is a non-existing directory");
		{
			let path = format!("surrealkv:{}", tempfile::tempdir().unwrap().path().display());
			let res = common::start_server(StartServerArguments {
				path: Some(path),
				args: "".to_owned(),
				temporary_directory: Some("/tmp/TELL-ME-THIS-FILE-DOES-NOT-EXISTS".to_owned()),
				..Default::default()
			})
			.await;
			match res {
				Ok((_, mut server)) => {
					server.finish().unwrap();
					panic!("Should not be ok!");
				}
				Err(e) => {
					assert_eq!(e.to_string(), "server failed to start", "{:?}", e);
				}
			}
		}

		info!("* The path is a file");
		{
			let path = format!("surrealkv:{}", tempfile::tempdir().unwrap().path().display());
			let temp_file = tempfile::NamedTempFile::new().unwrap();
			let res = common::start_server(StartServerArguments {
				path: Some(path),
				args: "".to_owned(),
				temporary_directory: Some(format!("{}", temp_file.path().display())),
				..Default::default()
			})
			.await;
			match res {
				Ok((_, mut server)) => {
					server.finish().unwrap();
					panic!("Should not be ok!");
				}
				Err(e) => {
					assert_eq!(e.to_string(), "server failed to start", "{:?}", e);
				}
			}
			temp_file.close().unwrap();
		}

		info!("* The path is a valid directory");
		{
			let path = format!("surrealkv:{}", tempfile::tempdir().unwrap().path().display());
			let temp_dir = tempfile::tempdir().unwrap();
			let (_, mut server) = common::start_server(StartServerArguments {
				path: Some(path),
				args: "".to_owned(),
				temporary_directory: Some(format!("{}", temp_dir.path().display())),
				..Default::default()
			})
			.await
			.unwrap();
			temp_dir.close().unwrap();
			server.finish().unwrap();
		}
	}
}

fn remove_debug_info(output: String) -> String {
	// Look... sometimes you just gotta copy paste
	let output_warning = "\
┌─────────────────────────────────────────────────────────────────────────────┐
│                        !!! THIS IS A DEBUG BUILD !!!                        │
│        Debug builds are not intended for production use and include         │
│       tooling and features that we would not recommend people run on        │
│                                  live data.                                 │
└─────────────────────────────────────────────────────────────────────────────┘
";
	// The last line in the above is important
	output.replace(output_warning, "")
}
