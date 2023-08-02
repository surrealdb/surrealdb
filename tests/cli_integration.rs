// cargo test --package surreal --bin surreal --no-default-features --features storage-mem --test cli_integration -- --nocapture

mod common;

use assert_fs::prelude::{FileTouch, FileWriteStr, PathChild};
use serial_test::serial;
use std::fs;
use test_log::test;
use tracing::info;

use common::{PASS, USER};

#[test]
#[serial]
fn version() {
	assert!(common::run("version").output().is_ok());
}

#[test]
#[serial]
fn help() {
	assert!(common::run("help").output().is_ok());
}

#[test]
#[serial]
fn nonexistent_subcommand() {
	assert!(common::run("nonexistent").output().is_err());
}

#[test]
#[serial]
fn nonexistent_option() {
	assert!(common::run("version --turbo").output().is_err());
}

#[test(tokio::test)]
#[serial]
async fn all_commands() {
	// Commands without credentials when auth is disabled, should succeed
	let (addr, _server) = common::start_server(false, false, true).await.unwrap();
	let creds = ""; // Anonymous user

	info!("* Create a record");
	{
		let args = format!("sql --conn http://{addr} {creds} --ns N --db D --multi");
		assert_eq!(
			common::run(&args).input("CREATE thing:one;\n").output(),
			Ok("[{ id: thing:one }]\n\n".to_owned()),
			"failed to send sql: {args}"
		);
	}

	info!("* Export to stdout");
	{
		let args = format!("export --conn http://{addr} {creds} --ns N --db D -");
		let output = common::run(&args).output().expect("failed to run stdout export: {args}");
		assert!(output.contains("DEFINE TABLE thing SCHEMALESS PERMISSIONS NONE;"));
		assert!(output.contains("UPDATE thing:one CONTENT { id: thing:one };"));
	}

	info!("* Export to file");
	let exported = {
		let exported = common::tmp_file("exported.surql");
		let args = format!("export --conn http://{addr} {creds} --ns N --db D {exported}");
		common::run(&args).output().expect("failed to run file export: {args}");
		exported
	};

	info!("* Import the exported file");
	{
		let args = format!("import --conn http://{addr} {creds} --ns N --db D2 {exported}");
		common::run(&args).output().expect("failed to run import: {args}");
	}

	info!("* Query from the import (pretty-printed this time)");
	{
		let args = format!("sql --conn http://{addr} {creds} --ns N --db D2 --pretty");
		assert_eq!(
			common::run(&args).input("SELECT * FROM thing;\n").output(),
			Ok("[\n\t{\n\t\tid: thing:one\n\t}\n]\n\n".to_owned()),
			"failed to send sql: {args}"
		);
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
		let args = format!("sql --conn ws://{addr} {creds} --ns N3 --db D3 --multi --pretty");
		let output = common::run(&args)
			.input(
				r#"CREATE thing:success; \
			CREATE thing:fail SET bad=rand('evil'); \
			SELECT * FROM sleep(10ms) TIMEOUT 1ms; \
			CREATE thing:also_success;
			"#,
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
		let args = format!("sql --conn ws://{addr} {creds} --ns N4 --db D4 --multi --pretty");
		let output = common::run(&args)
			.input(
				r#"BEGIN; \
			CREATE thing:success; \
			CREATE thing:fail SET bad=rand('evil'); \
			SELECT * FROM sleep(10ms) TIMEOUT 1ms; \
			CREATE thing:also_success; \
			COMMIT;
			"#,
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
			.input("USE NS N5 DB D5; CREATE thing:one;\n")
			.output()
			.expect("neither ns nor db");
		assert!(output.contains("thing:one"), "missing thing:one in {output}");
	}

	info!("* Pass only ns");
	{
		let args = format!("sql --conn http://{addr} {creds} --ns N5");
		let output = common::run(&args)
			.input("USE DB D5; SELECT * FROM thing:one;\n")
			.output()
			.expect("only ns");
		assert!(output.contains("thing:one"), "missing thing:one in {output}");
	}

	info!("* Pass only db and expect an error");
	{
		let args = format!("sql --conn http://{addr} {creds} --db D5");
		common::run(&args).output().expect_err("only db");
	}
}

#[test(tokio::test)]
#[serial]
async fn start_tls() {
	// Capute the server's stdout/stderr
	temp_env::async_with_vars(
		[
			("SURREAL_TEST_SERVER_STDOUT", Some("piped")),
			("SURREAL_TEST_SERVER_STDERR", Some("piped")),
		],
		async {
			let (_, server) = common::start_server(false, true, false).await.unwrap();

			std::thread::sleep(std::time::Duration::from_millis(2000));
			let output = server.kill().output().err().unwrap();

			// Test the crt/key args but the keys are self signed so don't actually connect.
			assert!(output.contains("Started web server"), "couldn't start web server: {output}");
		},
	)
	.await;
}

#[test(tokio::test)]
#[serial]
async fn with_root_auth() {
	// Commands with credentials when auth is enabled, should succeed
	let (addr, _server) = common::start_server(true, false, true).await.unwrap();
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
		let args = format!("export --conn http://{addr} {creds} --ns N --db D {exported}");

		common::run(&args).output().unwrap_or_else(|_| panic!("failed to run export: {args}"));
		exported
	};

	info!("* Root user can do imports");
	{
		let args = format!("import --conn http://{addr} {creds} --ns N --db D2 {exported}");
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
#[serial]
async fn with_anon_auth() {
	// Commands without credentials when auth is enabled, should fail
	let (addr, _server) = common::start_server(true, false, true).await.unwrap();
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
		let args = format!("export --conn http://{addr} {creds} --ns N --db D -");
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
		let args = format!("import --conn http://{addr} {creds} --ns N --db D2 {tmp_file}");
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

#[test]
#[serial]
fn validate_found_no_files() {
	let temp_dir = assert_fs::TempDir::new().unwrap();

	temp_dir.child("file.txt").touch().unwrap();

	assert!(common::run_in_dir("validate", &temp_dir).output().is_err());
}

#[test]
#[serial]
fn validate_succeed_for_valid_surql_files() {
	let temp_dir = assert_fs::TempDir::new().unwrap();

	let statement_file = temp_dir.child("statement.surql");

	statement_file.touch().unwrap();
	statement_file.write_str("CREATE thing:success;").unwrap();

	assert!(common::run_in_dir("validate", &temp_dir).output().is_ok());
}

#[test]
#[serial]
fn validate_failed_due_to_invalid_glob_pattern() {
	let temp_dir = assert_fs::TempDir::new().unwrap();

	const WRONG_GLOB_PATTERN: &str = "**/*{.txt";

	let args = format!("validate \"{}\"", WRONG_GLOB_PATTERN);

	assert!(common::run_in_dir(&args, &temp_dir).output().is_err());
}

#[test]
#[serial]
fn validate_failed_due_to_invalid_surql_files_syntax() {
	let temp_dir = assert_fs::TempDir::new().unwrap();

	let statement_file = temp_dir.child("statement.surql");

	statement_file.touch().unwrap();
	statement_file.write_str("CREATE $thing WHERE value = '';").unwrap();

	assert!(common::run_in_dir("validate", &temp_dir).output().is_err());
}
