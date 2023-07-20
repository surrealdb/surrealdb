// cargo test --package surreal --bin surreal --no-default-features --features storage-mem --test cli -- cli_integration --nocapture

mod common;

use assert_fs::prelude::{FileTouch, FileWriteStr, PathChild};
use serial_test::serial;
use std::fs;

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

#[tokio::test]
#[serial]
async fn all_commands() {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let creds = format!("--user {USER} --pass {PASS}");
	// Create a record
	{
		let args = format!("sql --conn http://{addr} {creds} --ns N --db D --multi");
		assert_eq!(
			common::run(&args).input("CREATE thing:one;\n").output(),
			Ok("[{ id: thing:one }]\n\n".to_owned()),
			"failed to send sql: {args}"
		);
	}

	// Export to stdout
	{
		let args = format!("export --conn http://{addr} {creds} --ns N --db D -");
		let output = common::run(&args).output().expect("failed to run stdout export: {args}");
		assert!(output.contains("DEFINE TABLE thing SCHEMALESS PERMISSIONS NONE;"));
		assert!(output.contains("UPDATE thing:one CONTENT { id: thing:one };"));
	}

	// Export to file
	let exported = {
		let exported = common::tmp_file("exported.surql");
		let args = format!("export --conn http://{addr} {creds} --ns N --db D {exported}");
		common::run(&args).output().expect("failed to run file export: {args}");
		exported
	};

	// Import the exported file
	{
		let args = format!("import --conn http://{addr} {creds} --ns N --db D2 {exported}");
		common::run(&args).output().expect("failed to run import: {args}");
	}

	// Query from the import (pretty-printed this time)
	{
		let args = format!("sql --conn http://{addr} {creds} --ns N --db D2 --pretty");
		assert_eq!(
			common::run(&args).input("SELECT * FROM thing;\n").output(),
			Ok("[\n\t{\n\t\tid: thing:one\n\t}\n]\n\n".to_owned()),
			"failed to send sql: {args}"
		);
	}

	// Unfinished backup CLI
	{
		let file = common::tmp_file("backup.db");
		let args = format!("backup {creds}  http://{addr} {file}");
		common::run(&args).output().expect("failed to run backup: {args}");

		// TODO: Once backups are functional, update this test.
		assert_eq!(fs::read_to_string(file).unwrap(), "Save");
	}

	// Multi-statement (and multi-line) query including error(s) over WS
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

	// Multi-statement (and multi-line) transaction including error(s) over WS
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

	// Pass neither ns nor db
	{
		let args = format!("sql --conn http://{addr} {creds}");
		let output = common::run(&args)
			.input("USE NS N5 DB D5; CREATE thing:one;\n")
			.output()
			.expect("neither ns nor db");
		assert!(output.contains("thing:one"), "missing thing:one in {output}");
	}

	// Pass only ns
	{
		let args = format!("sql --conn http://{addr} {creds} --ns N5");
		let output = common::run(&args)
			.input("USE DB D5; SELECT * FROM thing:one;\n")
			.output()
			.expect("only ns");
		assert!(output.contains("thing:one"), "missing thing:one in {output}");
	}

	// Pass only db and expect an error
	{
		let args = format!("sql --conn http://{addr} {creds} --db D5");
		common::run(&args).output().expect_err("only db");
	}
}

#[tokio::test]
#[serial]
async fn start_tls() {
	let (_, server) = common::start_server(true, false).await.unwrap();

	std::thread::sleep(std::time::Duration::from_millis(2000));
	let output = server.kill().output().err().unwrap();

	// Test the crt/key args but the keys are self signed so don't actually connect.
	assert!(output.contains("Started web server"), "couldn't start web server: {output}");
}

#[tokio::test]
#[serial]
async fn with_root_auth() {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let creds = format!("--user {USER} --pass {PASS}");
	let sql_args = format!("sql --conn http://{addr} --multi --pretty");

	// Can query /sql over HTTP
	{
		let args = format!("{sql_args} {creds}");
		let input = "INFO FOR ROOT;";
		let output = common::run(&args).input(input).output();
		assert!(output.is_ok(), "failed to query over HTTP: {}", output.err().unwrap());
	}

	// Can query /sql over WS
	{
		let args = format!("sql --conn ws://{addr} --multi --pretty {creds}");
		let input = "INFO FOR ROOT;";
		let output = common::run(&args).input(input).output();
		assert!(output.is_ok(), "failed to query over WS: {}", output.err().unwrap());
	}

	// KV user can do exports
	let exported = {
		let exported = common::tmp_file("exported.surql");
		let args = format!("export --conn http://{addr} {creds} --ns N --db D {exported}");

		common::run(&args).output().unwrap_or_else(|_| panic!("failed to run export: {args}"));
		exported
	};

	// KV user can do imports
	{
		let args = format!("import --conn http://{addr} {creds} --ns N --db D2 {exported}");
		common::run(&args).output().unwrap_or_else(|_| panic!("failed to run import: {args}"));
	}

	// KV user can do backups
	{
		let file = common::tmp_file("backup.db");
		let args = format!("backup {creds} http://{addr} {file}");
		common::run(&args).output().unwrap_or_else(|_| panic!("failed to run backup: {args}"));

		// TODO: Once backups are functional, update this test.
		assert_eq!(fs::read_to_string(file).unwrap(), "Save");
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
