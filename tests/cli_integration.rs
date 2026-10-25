// RUST_LOG=warn cargo make ci-cli-integration
mod common;

mod cli_integration {
	use std::collections::HashMap;
	use std::fs::File;
	use std::io::Write;
	use std::time;

	use assert_fs::prelude::{FileTouch, FileWriteStr, PathChild};
	use chrono::{Duration as ChronoDuration, Utc};
	#[cfg(unix)]
	use common::Format;
	#[cfg(unix)]
	use common::Socket;
	use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
	use serde::{Deserialize, Serialize};
	#[cfg(unix)]
	use serde_json::json;
	use test_log::test;
	use tracing::info;
	use ulid::Ulid;

	use super::common::{self, PASS, StartServerArguments, USER};
	use crate::remove_debug_info;

	/// `<semver> for <os> on <arch>`, with a newline, and nothing else.
	fn assert_release(text: &str) -> String {
		let text = remove_debug_info(text.to_owned());
		let version = text.split(' ').next().unwrap_or_default().to_owned();
		assert!(semver::Version::parse(&version).is_ok(), "{text}");
		assert_eq!(
			text,
			format!("{version} for {} on {}\n", std::env::consts::OS, std::env::consts::ARCH)
		);
		version
	}

	#[test]
	fn version_command() {
		assert_release(&common::run("version").output().unwrap());
	}

	/// `-v`, `--version` and the hidden `-V` print the release alone, the same
	/// text as `version`, with no command and nothing on stderr.
	#[test]
	fn version_flags_print_the_release() {
		let version = remove_debug_info(common::run("version").output().unwrap());
		for args in ["-v", "--version", "-V", "--log debug --version"] {
			let mut child = common::run(args);
			let text = child.output().unwrap();
			assert_release(&text);
			assert_eq!(remove_debug_info(text), version, "{args}");
			assert_eq!(child.stderr(), "", "{args}");
		}
	}

	/// The flags work the same through the Surreal CLI, and `--version` runs
	/// even where an environment variable gives clap a value.
	#[test]
	fn version_flag_through_the_launcher() {
		let vars = HashMap::from([
			("SURREAL_INVOKED_AS".to_owned(), "surreal db".to_owned()),
			("SURREAL_ONLINE_VERSION_CHECK".to_owned(), "false".to_owned()),
		]);
		let text = common::run_internal::<String>("--version", None, Some(vars)).output().unwrap();
		assert_release(&text);
	}

	/// Run the CLI as the Surreal CLI launcher would, with `SURREAL_INVOKED_AS`
	/// set to `invoked_as`, or as a standalone install when it is `None`.
	fn run_invoked_as(args: &str, invoked_as: Option<&str>) -> common::Child {
		let vars = invoked_as.map(|invoked_as| {
			HashMap::from([("SURREAL_INVOKED_AS".to_owned(), invoked_as.to_owned())])
		});
		common::run_internal::<String>(args, None, vars)
	}

	/// Run the command to completion, asserting it fails, and return its stderr.
	/// Its stdout must carry nothing but the debug-build banner.
	fn failure_stderr(mut child: common::Child) -> String {
		assert!(child.output().is_err(), "the command should fail");
		assert_eq!(remove_debug_info(child.stdout()).trim(), "", "nothing belongs on stdout");
		child.stderr()
	}

	/// `surreal db` alone, as the Surreal CLI runs it, shows the help: the same
	/// help `surreal help` prints, on stdout, exiting 0, even when an
	/// environment variable bound to an argument — `SURREAL_ONLINE_VERSION_CHECK`,
	/// which some Surreal CLI versions set — gives clap a value, so it does not
	/// see the command line as empty.
	#[test]
	fn a_bare_invocation_shows_the_help_whatever_the_environment() {
		let vars = HashMap::from([
			("SURREAL_INVOKED_AS".to_owned(), "surreal db".to_owned()),
			("SURREAL_ONLINE_VERSION_CHECK".to_owned(), "false".to_owned()),
		]);
		let bare = common::run_internal::<String>("", None, Some(vars.clone()))
			.output()
			.expect("a bare invocation shows the help and exits 0");
		let help = common::run_internal::<String>("help", None, Some(vars))
			.output()
			.expect("`help` exits 0");
		let bare = remove_debug_info(bare);
		assert!(
			bare.starts_with("Start, query, import and export SurrealDB from the command line"),
			"{bare}"
		);
		assert!(bare.contains("Usage: surreal db"), "{bare}");
		assert_eq!(
			bare,
			remove_debug_info(help),
			"a bare invocation should print what `help` does"
		);
	}

	/// A standalone install is told how to move to the Surreal CLI, with this
	/// platform's install command. The message's full text is pinned by the
	/// unit tests in `upgrade.rs`.
	#[test]
	fn upgrade_standalone_explains_how_to_move_to_the_surreal_cli() {
		let stderr = failure_stderr(run_invoked_as("upgrade", None));
		assert!(stderr.starts_with("Upgrading SurrealDB has moved to the Surreal CLI"), "{stderr}");
		let install = if cfg!(windows) {
			"irm https://download.surrealdb.com/cli/launcher/install.ps1 | iex\n"
		} else {
			"curl -sSf https://download.surrealdb.com/cli/launcher/install.sh | sh\n"
		};
		assert!(stderr.contains(install), "{stderr}");
	}

	#[test]
	fn upgrade_removal_step_names_this_executable() {
		let stderr = failure_stderr(run_invoked_as("upgrade", None));
		let line = stderr
			.lines()
			.find(|line| line.contains("1. Remove this executable:"))
			.expect("a removal step");
		let exe = format!("surreal{}", std::env::consts::EXE_SUFFIX);
		assert!(line.trim_end().trim_end_matches(['"', '\'']).ends_with(&exe), "{line}");
	}

	#[test]
	fn upgrade_through_the_launcher_points_at_its_upgrade() {
		let stderr = failure_stderr(run_invoked_as("upgrade", Some("surreal db")));
		assert_eq!(
			stderr.trim(),
			"Upgrading is done by the Surreal CLI; run `surreal upgrade db`."
		);
	}

	#[test]
	fn upgrade_accepts_its_options() {
		for args in [
			"upgrade --nightly",
			"upgrade --alpha",
			"upgrade --beta",
			"upgrade --version 2.3.1",
			"upgrade --version v3.0.0 --dry-run",
		] {
			let stderr = failure_stderr(run_invoked_as(args, Some("surreal db")));
			assert_eq!(
				stderr.trim(),
				"Upgrading is done by the Surreal CLI; run `surreal upgrade db`.",
				"`{args}` should explain, not fail to parse"
			);
		}
	}

	#[test]
	fn online_version_check_is_accepted() {
		common::run("--online-version-check version").output().unwrap();
		let vars = HashMap::from([("SURREAL_ONLINE_VERSION_CHECK".to_owned(), "false".to_owned())]);
		common::run_internal::<String>("version", None, Some(vars)).output().unwrap();
	}

	#[test]
	fn usage_names_the_executable() {
		let output = run_invoked_as("start --help", None).output().unwrap();
		assert!(output.contains("Usage: surreal start "), "{output}");
	}

	#[test]
	fn usage_names_the_launcher_command() {
		let output = run_invoked_as("start --help", Some("surreal db")).output().unwrap();
		assert!(output.contains("Usage: surreal db start "), "{output}");
		let output = run_invoked_as("--help", Some("surreal db")).output().unwrap();
		assert!(output.contains("Usage: surreal db [OPTIONS] <COMMAND>"), "{output}");
	}

	/// The install line the standalone explanations end with.
	fn launcher_install() -> &'static str {
		if cfg!(windows) {
			"irm https://download.surrealdb.com/cli/launcher/install.ps1 | iex"
		} else {
			"curl -sSf https://download.surrealdb.com/cli/launcher/install.sh | sh"
		}
	}

	/// Every old spelling of the extension tooling says how to install
	/// `surreal ext`, naming what was typed, and runs nothing.
	#[test]
	fn extension_standalone_says_how_to_install_surreal_ext() {
		let install = launcher_install();
		for spelling in ["extension", "module", "mod"] {
			let expected = format!(
				"`surreal {spelling}` has moved to `surreal ext` in the Surreal CLI. Install it with: {install}\nthen run `surreal ext …`.\n"
			);
			for rest in ["", " version", " build --debug -o demo.surli .", " install color"] {
				let args = format!("{spelling}{rest}");
				let stderr = failure_stderr(run_invoked_as(&args, None));
				assert_eq!(stderr, expected, "`{args}`");
			}
		}
	}

	/// `surreal ext` is already the new spelling; only where it runs moved.
	#[test]
	fn ext_standalone_says_where_it_runs() {
		let stderr = failure_stderr(run_invoked_as("ext info x", None));
		assert_eq!(
			stderr,
			format!(
				"`surreal ext` has moved to the Surreal CLI. Install it with: {}\nthen run `surreal ext …`.\n",
				launcher_install()
			)
		);
	}

	#[test]
	fn extension_through_the_launcher_points_at_surreal_ext() {
		for spelling in ["extension", "ext", "module", "mod"] {
			for rest in [" build", " --help", " -h", " run --fnc f x.surli"] {
				let args = format!("{spelling}{rest}");
				let stderr = failure_stderr(run_invoked_as(&args, Some("surreal db")));
				assert_eq!(
					stderr,
					format!(
						"`surreal db {spelling}` has moved to `surreal ext`. Run `surreal ext …` instead.\n"
					),
					"`{args}`"
				);
			}
		}
	}

	/// The launcher no longer runs `surreal module` itself, but if anything
	/// still sets `SURREAL_INVOKED_AS=surreal`, the failure still explains itself.
	#[test]
	fn module_explains_itself_whatever_the_launcher_command() {
		let stderr = failure_stderr(run_invoked_as("module version", Some("surreal")));
		assert_eq!(
			stderr,
			"`surreal module` has moved to `surreal ext`. Run `surreal ext …` instead.\n"
		);
	}

	/// Run the command to completion and return its exit code.
	fn exit_code(child: &mut common::Child) -> Option<i32> {
		let _ = child.output();
		child.status().unwrap().expect("the command has exited").code()
	}

	/// Run the command to completion, asserting it succeeds with one JSON
	/// document on stdout and nothing on stderr, and return the document.
	fn json_success(mut child: common::Child) -> serde_json::Value {
		let output = child.output();
		assert!(output.is_ok(), "the command should succeed: {output:?}");
		assert_eq!(child.stderr(), "", "nothing belongs on stderr");
		serde_json::from_str(&child.stdout()).expect("stdout is one JSON document")
	}

	/// Run the command to completion, asserting it exits `code` with nothing on
	/// stdout and one JSON error envelope on stderr, and return the envelope.
	fn json_failure(mut child: common::Child, code: i32) -> serde_json::Value {
		assert_eq!(exit_code(&mut child), Some(code), "{}", child.stderr());
		assert_eq!(child.stdout(), "", "nothing belongs on stdout");
		let envelope: serde_json::Value =
			serde_json::from_str(&child.stderr()).expect("stderr is one JSON document");
		for key in [
			"kind",
			"message",
			"status",
			"code",
			"request_id",
			"hint",
			"command",
			"docs",
			"retry_after_secs",
			"exit_code",
		] {
			assert!(envelope.get(key).is_some(), "the envelope has no `{key}`: {envelope}");
		}
		assert_eq!(envelope["exit_code"], code);
		envelope
	}

	/// `version`'s text is the release, the OS and the architecture, and
	/// `--json` reports the same three values.
	#[test]
	fn version_text_and_json_report_the_same_values() {
		let document = json_success(common::run("version --json"));
		assert_eq!(document["os"], std::env::consts::OS);
		assert_eq!(document["arch"], std::env::consts::ARCH);
		let version = document["version"].as_str().expect("a version string");
		assert!(semver::Version::parse(version).is_ok(), "{version}");
		assert!(document.get("server").is_none(), "{document}");

		let text = remove_debug_info(common::run("version").output().unwrap());
		assert_eq!(
			text,
			format!("{version} for {} on {}\n", std::env::consts::OS, std::env::consts::ARCH)
		);
	}

	#[test]
	fn version_json_short_flag() {
		let document = json_success(common::run("version -j"));
		assert!(document["version"].is_string(), "{document}");
	}

	/// Run the command, asserting it succeeds with the version document alone
	/// on stdout, its keys `arch`, `os` and `version` in that order, and return
	/// the document.
	fn version_document(args: &str) -> serde_json::Value {
		let mut child = common::run(args);
		assert!(child.output().is_ok(), "`{args}` should succeed");
		assert_eq!(child.stderr(), "", "{args}");
		let stdout = child.stdout();
		let keys: Vec<_> = stdout
			.lines()
			.filter_map(|line| line.trim().strip_prefix('"'))
			.filter_map(|line| line.split('"').next())
			.collect();
		assert_eq!(keys, ["arch", "os", "version"], "{args}: {stdout}");
		serde_json::from_str(&stdout).expect("stdout is one JSON document")
	}

	/// `--version --json`, in either order and either spelling, prints the
	/// same document as `version --json`.
	#[test]
	fn version_flag_json_is_the_version_document() {
		let expected = version_document("version --json");
		for args in ["--version --json", "--json --version", "-v -j", "-j -v", "-V --json"] {
			assert_eq!(version_document(args), expected, "{args}");
		}
	}

	/// The root's `--json` goes with `--version`: before a command it is a
	/// usage error, as it was before the root took it.
	#[test]
	fn root_json_without_the_version_flag_is_a_usage_error() {
		let envelope = json_failure(common::run("--json version"), 2);
		assert_eq!(envelope["kind"], "usage");
		assert_eq!(envelope["command"], "surreal version --help");
	}

	/// With `--endpoint`, the text is the server's version alone, and `--json`
	/// adds it under `server`.
	#[test(tokio::test)]
	async fn version_json_with_an_endpoint_reports_the_server() {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let text = remove_debug_info(
			common::run(&format!("version --endpoint http://{addr}")).output().unwrap(),
		);
		let document =
			json_success(common::run(&format!("version --endpoint http://{addr} --json")));
		assert_eq!(document["server"]["version"], text.trim(), "{document}");
		assert!(document["version"].is_string(), "{document}");
	}

	#[test]
	fn version_json_reports_an_unreachable_server_as_the_envelope() {
		let envelope = json_failure(common::run("version --endpoint http://127.0.0.1:1 --json"), 1);
		assert_eq!(envelope["kind"], "network");
	}

	#[test(tokio::test)]
	async fn is_ready_json_reports_the_endpoint() {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let output = remove_debug_info(
			common::run(&format!("is-ready --endpoint http://{addr}")).output().unwrap(),
		);
		assert_eq!(output, "OK\n");
		for command in ["is-ready", "isready"] {
			let endpoint = format!("http://{addr}");
			let document =
				json_success(common::run(&format!("{command} --endpoint {endpoint} --json")));
			assert_eq!(document, serde_json::json!({ "ready": true, "endpoint": endpoint }));
		}
	}

	#[test]
	fn is_ready_json_reports_an_unreachable_server_as_the_envelope() {
		let envelope =
			json_failure(common::run("is-ready --endpoint http://127.0.0.1:1 --json"), 1);
		assert_eq!(envelope["kind"], "network");
		assert_eq!(envelope["command"], serde_json::Value::Null);
	}

	#[test]
	fn validate_json_reports_every_file() {
		let temp_dir = assert_fs::TempDir::new().unwrap();
		temp_dir.child("a.surql").write_str("CREATE thing:success;").unwrap();
		let document = json_success(common::run_in_dir("validate --json", &temp_dir));
		assert_eq!(
			document,
			serde_json::json!({ "valid": true, "files": [{ "path": "a.surql", "valid": true }] })
		);
	}

	#[test]
	fn validate_json_reports_invalid_files_in_the_envelope() {
		let temp_dir = assert_fs::TempDir::new().unwrap();
		temp_dir.child("a.surql").write_str("CREATE thing:success;").unwrap();
		temp_dir.child("b.surql").write_str("CREATE $thing WHERE value = '';").unwrap();
		let envelope = json_failure(common::run_in_dir("validate *.surql --json", &temp_dir), 1);
		assert_eq!(envelope["kind"], "invalid");
		assert_eq!(envelope["message"], "Some queries failed to validate");
		let files = envelope["files"].as_array().expect("per-file results");
		assert_eq!(files.len(), 2, "{envelope}");
		assert_eq!(files[0], serde_json::json!({ "path": "a.surql", "valid": true }));
		assert_eq!(files[1]["path"], "b.surql");
		assert_eq!(files[1]["valid"], false);
		assert!(files[1]["error"].is_string(), "{envelope}");
	}

	#[test]
	fn validate_json_reads_stdin() {
		let child = common::run("validate --stdin --json").input("CREATE thing:success;");
		let document = json_success(child);
		assert_eq!(
			document,
			serde_json::json!({ "valid": true, "files": [{ "path": "<stdin>", "valid": true }] })
		);
		let child = common::run("validate --stdin --json").input("CREATE $thing WHERE value = '';");
		let envelope = json_failure(child, 1);
		assert_eq!(envelope["kind"], "invalid");
		assert_eq!(envelope["files"][0]["valid"], false);
	}

	#[test]
	fn validate_json_reports_no_files_as_the_envelope() {
		let temp_dir = assert_fs::TempDir::new().unwrap();
		let envelope = json_failure(common::run_in_dir("validate --json", &temp_dir), 1);
		assert_eq!(envelope["kind"], "unknown");
		assert_eq!(envelope["message"], "No files found");
	}

	/// Under `--json`, a file that cannot be read is `not_found` when it does
	/// not exist and `unknown` for any other error, rather than `invalid`.
	#[test]
	fn validate_json_names_why_a_file_could_not_be_read() {
		let temp_dir = assert_fs::TempDir::new().unwrap();
		std::fs::create_dir(temp_dir.path().join("a.surql")).unwrap();
		let envelope = json_failure(common::run_in_dir("validate --json", &temp_dir), 1);
		assert_eq!(envelope["kind"], "unknown", "{envelope}");
		assert_eq!(envelope["files"][0]["valid"], false, "{envelope}");
		#[cfg(unix)]
		{
			let temp_dir = assert_fs::TempDir::new().unwrap();
			let link = temp_dir.path().join("b.surql");
			std::os::unix::fs::symlink(temp_dir.path().join("missing"), link).unwrap();
			let envelope = json_failure(common::run_in_dir("validate --json", &temp_dir), 1);
			assert_eq!(envelope["kind"], "not_found", "{envelope}");
		}
	}

	/// A pattern that is not a glob is a usage error under `--json`, exiting 2,
	/// and without `--json` exits 1.
	#[test]
	fn validate_json_reports_a_bad_pattern_as_a_usage_error() {
		let envelope = json_failure(run_invoked_as("validate [ --json", Some("surreal db")), 2);
		assert_eq!(envelope["kind"], "usage", "{envelope}");
		assert_eq!(envelope["command"], "surreal db validate --help", "{envelope}");
		assert_eq!(exit_code(&mut common::run("validate [")), Some(1));
	}

	/// Under `--json`, stdout is the document alone however verbose the logs:
	/// every log line goes to stderr, and a failure leaves stdout empty.
	#[test]
	fn json_stdout_carries_no_logs() {
		let vars = HashMap::from([("SURREAL_LOG".to_owned(), "trace".to_owned())]);
		let temp_dir = assert_fs::TempDir::new().unwrap();
		temp_dir.child("a.surql").write_str("CREATE thing:success;").unwrap();
		let mut child =
			common::run_internal("validate --json", Some(temp_dir.path()), Some(vars.clone()));
		assert_eq!(exit_code(&mut child), Some(0), "{}", child.stderr());
		let document: serde_json::Value =
			serde_json::from_str(&child.stdout()).expect("stdout is one JSON document");
		assert_eq!(document["valid"], true, "{document}");
		assert!(child.stderr().contains("TRACE"), "the logs go to stderr: {}", child.stderr());

		temp_dir.child("b.surql").write_str("CREATE $thing WHERE value = '';").unwrap();
		for args in ["validate --json", "is-ready --endpoint http://127.0.0.1:1 --json"] {
			let mut child = common::run_internal(args, Some(temp_dir.path()), Some(vars.clone()));
			assert_eq!(exit_code(&mut child), Some(1), "{args}: {}", child.stderr());
			assert_eq!(child.stdout(), "", "{args}: nothing belongs on stdout");
		}
	}

	/// A usage error in a command that takes `--json` is the envelope, naming
	/// that command's help as the launcher's user would type it.
	#[test]
	fn a_usage_error_under_json_is_the_envelope() {
		for (args, command) in [
			("version --bogus --json", "surreal db version --help"),
			("is-ready -j --bogus", "surreal db is-ready --help"),
			("isready --json --bogus", "surreal db is-ready --help"),
			("validate --stdin --json x.surql", "surreal db validate --help"),
			("version --endpoint nope --json", "surreal db version --help"),
		] {
			let envelope = json_failure(run_invoked_as(args, Some("surreal db")), 2);
			assert_eq!(envelope["kind"], "usage", "{args}");
			assert_eq!(envelope["hint"], "See the usage with:", "{args}");
			assert_eq!(envelope["command"], command, "{args}");
			let message = envelope["message"].as_str().unwrap();
			assert!(!message.starts_with("error:"), "{args}: {message}");
		}
		let envelope = json_failure(run_invoked_as("version --bogus --json", None), 2);
		assert_eq!(envelope["message"], "unexpected argument '--bogus' found");
		assert_eq!(envelope["command"], "surreal version --help");
	}

	/// Without `--json`, or in a command that does not take it, a usage error
	/// is clap's text.
	#[test]
	fn a_usage_error_without_json_is_text() {
		for args in
			["version --bogus", "export --json", "sql --json --bogus", "version --bogus -- --json"]
		{
			let mut child = common::run(args);
			assert_eq!(exit_code(&mut child), Some(2), "{args}");
			let stderr = child.stderr();
			assert!(stderr.starts_with("error: "), "{args}: {stderr}");
		}
	}

	/// Help never prints a credential an environment variable holds.
	#[test]
	fn help_hides_environment_values() {
		let secret = "s3cr3t-value-in-the-environment";
		let vars = HashMap::from(
			[
				"SURREAL_USER",
				"SURREAL_PASS",
				"SURREAL_TOKEN",
				"SURREAL_KEY",
				"SURREAL_NAMESPACE",
				"SURREAL_DATABASE",
			]
			.map(|name| (name.to_owned(), secret.to_owned())),
		);
		for args in ["sql --help", "import --help", "export --help", "start --help", "--help"] {
			let output =
				common::run_internal::<String>(args, None, Some(vars.clone())).output().unwrap();
			assert!(output.contains("SURREAL_PASS") || args == "--help", "{args}: {output}");
			assert!(!output.contains(secret), "`{args}` printed a variable's value: {output}");
		}
	}

	#[test]
	fn help_command() {
		common::run("help").output().unwrap();
	}

	#[test]
	fn help_flag_short() {
		common::run("-h").output().unwrap();
	}

	#[test]
	fn help_flag_long() {
		common::run("--help").output().unwrap();
	}

	#[test]
	fn nonexistent_subcommand() {
		assert!(common::run("nonexistent").output().is_err());
	}

	#[test]
	fn nonexistent_option() {
		assert!(common::run("version --turbo").output().is_err());
	}

	/// `--only` deselects every section except the database's own definition,
	/// and the dump it writes is the same over every transport.
	///
	/// The gRPC wire format carries no field for that section, so a client asked
	/// to leave it out refuses the export rather than answering with it anyway.
	/// Deselecting it on the caller's behalf would make a plain `--only` a
	/// transport-dependent command — a dump over HTTP, a refusal and no file at
	/// all over gRPC — so `--only` leaves that one section alone. Excluding it
	/// stays available, explicitly, and the refusal then belongs to a caller who
	/// asked for it.
	#[test(tokio::test)]
	async fn export_only_writes_the_same_dump_over_every_transport() {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let creds = format!("--user {USER} --pass {PASS}");
		let ns = Ulid::new();
		let db = Ulid::new();

		// A clause of the database's own, so the section `--only` must not drop
		// is one the export actually has something to write.
		let args =
			format!("sql --endpoint http://{addr} {creds} --ns {ns} --db {db} --hide-welcome");
		common::run(&args)
			.input(&format!("DEFINE DATABASE OVERWRITE `{db}` COMMENT 'kept'; CREATE thing:one;\n"))
			.output()
			.unwrap();

		let mut dumps = Vec::new();
		for scheme in ["http", "grpc"] {
			let dump = common::tmp_file("only.surql");
			let args = format!(
				"export --endpoint {scheme}://{addr} {creds} --ns {ns} --db {db} --only --tables \
				 thing --records true {dump}"
			);
			common::run(&args).output().unwrap_or_else(|err| {
				panic!("`--only` must not be refused over {scheme}: {err:?}")
			});
			dumps.push(std::fs::read_to_string(&dump).unwrap());
		}
		assert_eq!(dumps[0], dumps[1], "`--only` wrote a different dump over gRPC than over HTTP");
		assert!(
			dumps[0]
				.contains("DEFINE DATABASE OVERWRITE $session.db COMPATIBILITY 4 COMMENT 'kept'"),
			"`--only` leaves the database definition selected, so the dump carries it: {}",
			dumps[0]
		);

		// Asking for the exclusion is still the caller's to make, and is still
		// what the gRPC transport cannot express.
		let dump = common::tmp_file("only-no-db.surql");
		let args = format!(
			"export --endpoint http://{addr} {creds} --ns {ns} --db {db} --only --tables thing \
			 --database-definition false {dump}"
		);
		common::run(&args).output().unwrap();
		assert!(
			!std::fs::read_to_string(&dump).unwrap().contains("DEFINE DATABASE"),
			"an explicit exclusion must still drop the section"
		);
		let args = format!(
			"export --endpoint grpc://{addr} {creds} --ns {ns} --db {db} --only --tables thing \
			 --database-definition false {dump}"
		);
		assert!(
			common::run(&args).output().is_err(),
			"gRPC carries no field for the exclusion, so asking for it must still be refused"
		);
	}

	/// `export` writing to stdout is the dump and nothing else: at the default
	/// log level the command's own INFO lines go to stderr, so that redirecting
	/// stdout to a file yields a file `import` reads back. Text outside ASCII
	/// must come back unchanged.
	#[test(tokio::test)]
	async fn export_to_stdout_is_the_dump_alone() {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let creds = format!("--user {USER} --pass {PASS}");
		let ns = Ulid::new();
		let db = Ulid::new();
		let db2 = Ulid::new();

		let args =
			format!("sql --endpoint http://{addr} {creds} --ns {ns} --db {db} --hide-welcome");
		common::run(&args)
			.input("CREATE thing:one SET text = '✓ 🧘‍♀️ ⟨x⟩'; CREATE thing:two SET n = 2;\n")
			.output()
			.unwrap();

		let args = format!("export --endpoint http://{addr} {creds} --ns {ns} --db {db}");
		let mut child = common::run(&args);
		child.output().unwrap();
		let stdout = remove_debug_info(child.stdout());
		assert!(
			!stdout.lines().any(|line| line.contains(" INFO ")),
			"log lines leaked into the dump on stdout:\n{stdout}"
		);
		assert!(stdout.starts_with("-- ---"), "the dump must begin with its own header:\n{stdout}");
		assert!(child.stderr().contains("exported successfully"));

		let dump = common::tmp_file("stdout-dump.surql");
		std::fs::write(&dump, &stdout).unwrap();
		let args = format!("import --endpoint http://{addr} {creds} --ns {ns} --db {db2} {dump}");
		common::run(&args).output().unwrap_or_else(|err| panic!("{args}: {err:?}"));
		let args =
			format!("sql --endpoint http://{addr} {creds} --ns {ns} --db {db2} --hide-welcome");
		let output = common::run(&args)
			.input("SELECT VALUE text FROM thing:one; SELECT count() FROM thing GROUP ALL;\n")
			.output()
			.unwrap();
		assert!(output.contains("'✓ 🧘‍♀️ ⟨x⟩'"), "{output}");
		assert!(output.contains("count: 2"), "{output}");
	}

	/// `mcp` speaks JSON-RPC over stdout, so at the default log level its INFO
	/// lines go to stderr and every line on stdout is a JSON-RPC message. A log
	/// line on stdout (with ANSI colour codes under a terminal) breaks the client
	/// (surrealdb/surrealdb#6740).
	#[cfg(feature = "mcp")]
	#[test(tokio::test)]
	async fn mcp_stdout_is_json_rpc_alone() {
		let request = json!({
			"jsonrpc": "2.0",
			"id": 1,
			"method": "initialize",
			"params": {
				"protocolVersion": "2025-06-18",
				"capabilities": {},
				"clientInfo": { "name": "cli-test", "version": "0.0.0" }
			}
		});
		let mut child = common::run("mcp memory --log info").input(&format!("{request}\n"));
		let deadline = time::Instant::now() + time::Duration::from_secs(30);
		while !child.stdout().contains("\"id\":1") && time::Instant::now() < deadline {
			tokio::time::sleep(time::Duration::from_millis(100)).await;
		}
		let child = child.kill();
		let stdout = remove_debug_info(child.stdout());
		assert!(stdout.contains("\"id\":1"), "no reply to initialize on stdout:\n{stdout}");
		for line in stdout.lines().filter(|line| !line.trim().is_empty()) {
			serde_json::from_str::<serde_json::Value>(line)
				.unwrap_or_else(|_| panic!("a line on stdout is not JSON-RPC: {line:?}"));
		}
		assert!(child.stderr().contains(" INFO "), "the log lines belong on stderr");
	}

	/// A record can hold a value nested deeper than a query may write as a
	/// literal. Its export is read back by `import` in full, and an import that
	/// does not apply says so in its exit status rather than reporting success.
	#[test(tokio::test)]
	async fn export_of_a_deeply_nested_value_imports_back() {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let creds = format!("--user {USER} --pass {PASS}");
		let ns = Ulid::new();
		let db = Ulid::new();
		let db2 = Ulid::new();

		let args =
			format!("sql --endpoint http://{addr} {creds} --ns {ns} --db {db} --hide-welcome");
		common::run(&args)
			.input(
				"DEFINE TABLE a SCHEMALESS; CREATE a:1; DEFINE TABLE t SCHEMALESS; \
				 CREATE t:deep SET content = {leaf: true}; \
				 FOR $i IN 0..110 { UPDATE t:deep SET content = {children: [content]} RETURN NONE; }; \
				 DEFINE TABLE b SCHEMALESS; CREATE b:1;\n",
			)
			.output()
			.unwrap();

		let dump = common::tmp_file("deep.surql");
		let args = format!("export --endpoint http://{addr} {creds} --ns {ns} --db {db} {dump}");
		common::run(&args).output().unwrap_or_else(|err| panic!("{args}: {err:?}"));
		let args = format!("import --endpoint http://{addr} {creds} --ns {ns} --db {db2} {dump}");
		common::run(&args).output().unwrap_or_else(|err| panic!("{args}: {err:?}"));

		let args =
			format!("sql --endpoint http://{addr} {creds} --ns {ns} --db {db2} --hide-welcome");
		let output = common::run(&args)
			.input(
				"SELECT count() FROM a GROUP ALL; SELECT count() FROM t GROUP ALL; \
				 SELECT count() FROM b GROUP ALL;\n",
			)
			.output()
			.unwrap();
		assert_eq!(output.matches("count: 1").count(), 3, "{output}");

		// A dump with a statement that cannot apply fails the command.
		let bad = common::tmp_file("bad.surql");
		std::fs::write(
			&bad,
			"OPTION IMPORT;\nDEFINE TABLE x SCHEMALESS;\nINSERT [ { id: x:1 } ];\nCREATE x:1;\n",
		)
		.unwrap();
		let args = format!("import --endpoint http://{addr} {creds} --ns {ns} --db {db2} {bad}");
		assert!(common::run(&args).output().is_err(), "a failed statement must fail the import");
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
		{
			info!("* Development builds contain debug message");
			let args = format!(
				"sql --endpoint http://{addr} {creds} --ns {ns} --db {db} --multi --hide-welcome"
			);
			let output = common::run(&args).input("CREATE any:any;\n").output().unwrap();
			assert!(
				output.contains("Development builds are not intended for production use"),
				"{output}"
			);
		}

		info!("* Create a record");
		{
			let args = format!(
				"sql --endpoint http://{addr} {creds} --ns {ns} --db {db} --multi --hide-welcome"
			);
			let output = common::run(&args).input("CREATE thing:one;\n").output().unwrap();
			assert!(
				output.contains("[[{ id: thing:one }]]\n\n"),
				"failed to send sql: {args} - output: {output}"
			);
		}

		info!("* Export to stdout");
		{
			let args = format!(
				"export --endpoint http://{addr} {creds} --ns {ns} --db {db} - --only --tables thing --records"
			);
			let output = common::run(&args)
				.output()
				.unwrap_or_else(|_| panic!("failed to run stdout export: {args}"));
			assert!(
				output.contains("DEFINE TABLE thing TYPE ANY SCHEMALESS PERMISSIONS NONE;"),
				"{output}"
			);
			assert!(output.contains("INSERT [ { id: thing:one } ]"), "{output}");
		}

		info!("* Export to file");
		let exported = {
			let exported = common::tmp_file("exported.surql");
			let args =
				format!("export --endpoint http://{addr} {creds} --ns {ns} --db {db} {exported}");
			common::run(&args)
				.output()
				.unwrap_or_else(|_| panic!("failed to run file export: {args}"));
			exported
		};

		let db2 = Ulid::new();

		info!("* Import the exported file");
		{
			let args =
				format!("import --endpoint http://{addr} {creds} --ns {ns} --db {db2} {exported}");
			common::run(&args).output().unwrap_or_else(|_| panic!("failed to run import: {args}"));
		}

		info!("* Query from the import (pretty-printed this time)");
		{
			let args = format!(
				"sql --endpoint http://{addr} {creds} --ns {ns} --db {db2} --pretty --hide-welcome"
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
				"sql --endpoint ws://{addr} {creds} --ns {throwaway} --db {throwaway} --multi",
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

			assert!(output.contains("{1, 2, 3}"), "missing success in {output}");
		}

		info!("* Multi-statement (and multi-line) query including error(s) over WS");
		{
			let args = format!(
				"sql --endpoint ws://{addr} {creds} --ns {throwaway} --db {throwaway} --multi --pretty",
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
				.output_even_on_failure();

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
				"sql --endpoint ws://{addr} {creds} --ns {throwaway} --db {throwaway} --multi --pretty",
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
				.output_even_on_failure();

			// BEGIN batch: failed txn, then cancelled (skipped stmts), then explicit COMMIT error
			// (#7207).
			assert_eq!(
				output.lines().filter(|s| s.contains("transaction")).count(),
				4,
				"missing txn-related errors in {output:?}"
			);
			assert!(output.contains("rgument"), "missing argument error in {output}");
		}

		info!("* Pass neither ns nor db");
		{
			let args = format!("sql --endpoint http://{addr} {creds}");
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
			let args = format!("sql --endpoint http://{addr} {creds} --ns {ns}");
			let output = common::run(&args)
				.input(&format!("USE DB `{db}`; SELECT * FROM thing:one;\n"))
				.output()
				.expect("only ns");
			assert!(output.contains("thing:one"), "missing thing:one in {output}");
		}

		info!("* Pass only db and expect an error");
		{
			let args = format!(
				"sql --endpoint http://{addr} {creds} --db {throwaway}",
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

		std::thread::sleep(std::time::Duration::from_millis(10000));
		let output = server.kill().output().err().unwrap();

		// Test the crt/key args but the keys are self signed so don't actually connect.
		assert!(output.contains("Started web server"), "couldn't start web server: {output}");
	}

	#[test(tokio::test)]
	async fn with_root_auth() {
		// Commands with credentials when auth is enabled, should succeed
		let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
		let creds = format!("--user {USER} --pass {PASS}");
		let sql_args = format!("sql --endpoint http://{addr} --multi --pretty");

		info!("* Query over HTTP");
		{
			let args = format!("{sql_args} {creds}");
			let input = "INFO FOR ROOT;";
			let output = common::run(&args).input(input).output();
			assert!(output.is_ok(), "failed to query over HTTP: {}", output.err().unwrap());
		}

		info!("* Query over WS");
		{
			let args = format!("sql --endpoint ws://{addr} --multi --pretty {creds}");
			let input = "INFO FOR ROOT;";
			let output = common::run(&args).input(input).output();
			assert!(output.is_ok(), "failed to query over WS: {}", output.err().unwrap());
		}

		info!("* Root user can do exports");
		let exported = {
			let exported = common::tmp_file("exported.surql");
			let args = format!(
				"export --endpoint http://{addr} {creds} --ns {throwaway} --db {throwaway} {exported}",
				throwaway = Ulid::new()
			);

			common::run(&args).output().expect("failed to run export");
			exported
		};

		info!("* Root user can do imports");
		{
			let args = format!(
				"import --endpoint http://{addr} {creds} --ns {throwaway} --db {throwaway} {exported}",
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
			let args = format!("sql --endpoint http://{addr} --db {db} --ns {ns} {creds}");
			let _ = common::run(&args)
				.input(format!("DEFINE USER {USER} ON ROOT PASSWORD '{PASS}' ROLES OWNER;
                                                DEFINE USER {USER} ON NAMESPACE PASSWORD '{PASS}' ROLES OWNER;
                                                DEFINE USER {USER} ON DATABASE PASSWORD '{PASS}' ROLES OWNER;\n").as_str())
				.output_even_on_failure();
		}

		info!("* Pass root auth level and access root info");
		{
			let args = format!(
				"sql --endpoint http://{addr} --db {db} --ns {ns} --auth-level root {creds}"
			);
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
			let args = format!(
				"sql --endpoint http://{addr} --db {db} --ns {ns} --auth-level root {creds}"
			);
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
			let args = format!(
				"sql --endpoint http://{addr} --db {db} --ns {ns} --auth-level root {creds}"
			);
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
				"sql --endpoint http://{addr} --db {db} --ns {ns} --auth-level namespace {creds}"
			);
			let output = common::run(&args)
				.input(format!("USE NS `{ns}` DB `{db}`; INFO FOR ROOT;\n").as_str())
				.output_even_on_failure();
			assert!(
				output.contains("IAM error: Not enough permissions to perform this action"),
				"auth level namespace should not be able to access root info: {output}"
			);
		}

		info!("* Pass namespace auth level and access namespace info");
		{
			let args = format!(
				"sql --endpoint http://{addr} --db {db} --ns {ns} --auth-level namespace {creds}"
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
				"sql --endpoint http://{addr} --db {db} --ns {ns} --auth-level namespace {creds}"
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
				"sql --endpoint http://{addr} --db {db} --ns {ns} --auth-level database {creds}"
			);
			let output = common::run(&args)
				.input(format!("USE NS `{ns}` DB `{db}`; INFO FOR ROOT;\n").as_str())
				.output_even_on_failure();
			assert!(
				output.contains("IAM error: Not enough permissions to perform this action"),
				"auth level database should not be able to access root info: {output}",
			);
		}

		info!("* Pass database auth level and access namespace info");
		{
			let args = format!(
				"sql --endpoint http://{addr} --db {db} --ns {ns} --auth-level database {creds}"
			);
			let output = common::run(&args)
				.input(format!("USE NS `{ns}` DB `{db}`; INFO FOR NS;\n").as_str())
				.output_even_on_failure();
			assert!(
				output.contains("IAM error: Not enough permissions to perform this action"),
				"auth level database should not be able to access namespace info: {output}",
			);
		}

		info!("* Pass database auth level and access database info");
		{
			let args = format!(
				"sql --endpoint http://{addr} --db {db} --ns {ns} --auth-level database {creds}"
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
			let args = format!("sql --endpoint http://{addr} --auth-level database {creds}");
			let output = common::run(&args)
				.input(format!("USE NS `{ns}` DB `{db}`; INFO FOR NS;\n").as_str())
				.output();
			assert!(
				output
					.clone()
					.unwrap_err()
					.contains("Namespace is needed for authentication but it was not provided"),
				"auth level namespace requires providing a namespace: {output:?}"
			);
		}

		info!("* Pass database auth level without specifying database");
		{
			let args =
				format!("sql --endpoint http://{addr} --ns {ns} --auth-level database {creds}");
			let output = common::run(&args)
				.input(format!("USE NS `{ns}` DB `{db}`; INFO FOR DB;\n").as_str())
				.output();
			assert!(
				output
					.clone()
					.unwrap_err()
					.contains("Database is needed for authentication but it was not provided"),
				"auth level database requires providing a namespace and database: {output:?}"
			);
		}

		info!("* Pass auth level without providing credentials");
		{
			let args = format!("sql --endpoint http://{addr} --ns {ns} --auth-level database");
			let output = common::run(&args)
				.input(format!("USE NS `{ns}` DB `{db}`; INFO FOR DB;\n").as_str())
				.output();
			assert!(
				output
					.clone()
					.unwrap_err()
					.contains("error: the following required arguments were not provided:\n  --password <PASSWORD>\n  --username <USERNAME>"),
				"auth level database requires credentials: {output:?}"
			);
		}
		server.finish().unwrap();
	}

	#[test(tokio::test)]
	async fn with_auth_level_token() {
		// Commands with token for different auth levels
		let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
		let ac = "test";
		let ns = Ulid::new();
		let db = Ulid::new();
		let exp = (Utc::now() + ChronoDuration::days(1)).timestamp();
		let key = "secret";
		let header = Header::new(Algorithm::HS512);
		let key_enc = EncodingKey::from_secret(key.as_ref());
		#[derive(Debug, Serialize, Deserialize)]
		struct Claims {
			exp: Option<i64>,
			iss: Option<String>,
			#[serde(skip_serializing_if = "Option::is_none")]
			ns: Option<String>,
			#[serde(skip_serializing_if = "Option::is_none")]
			db: Option<String>,
			ac: Option<String>,
			rl: Option<Vec<String>>,
		}
		let claims_root = serde_json::from_str::<Claims>(
			format!(
				r#"
			{{
				"iss": "surrealdb-test",
				"exp": {exp},
				"ac": "{ac}",
				"rl": ["Owner"]
			}}
			"#
			)
			.as_ref(),
		)
		.unwrap();
		let token_root = match encode(&header, &claims_root, &key_enc) {
			Ok(enc) => enc,
			Err(err) => panic!("Failed to encode token: {:?}", err),
		};
		let claims_ns = serde_json::from_str::<Claims>(
			format!(
				r#"
			{{
				"iss": "surrealdb-test",
				"exp": {exp},
				"ac": "{ac}",
				"ns": "{ns}",
				"rl": ["Owner"]
			}}
			"#
			)
			.as_ref(),
		)
		.unwrap();
		let token_ns = match encode(&header, &claims_ns, &key_enc) {
			Ok(enc) => enc,
			Err(err) => panic!("Failed to encode token: {:?}", err),
		};
		let claims_db = serde_json::from_str::<Claims>(
			format!(
				r#"
			{{
				"iss": "surrealdb-test",
				"exp": {exp},
				"ac": "{ac}",
				"ns": "{ns}",
				"db": "{db}",
				"rl": ["Owner"]
			}}
			"#
			)
			.as_ref(),
		)
		.unwrap();
		let token_db = match encode(&header, &claims_db, &key_enc) {
			Ok(enc) => enc,
			Err(err) => panic!("Failed to encode token: {:?}", err),
		};
		let record_user = "user:1";
		let claims_record = serde_json::from_str::<Claims>(
			format!(
				r#"
			{{
				"iss": "surrealdb-test",
				"exp": {exp},
				"ac": "{ac}",
				"ns": "{ns}",
				"db": "{db}",
				"id": "{record_user}"
			}}
			"#
			)
			.as_ref(),
		)
		.unwrap();
		let token_record = match encode(&header, &claims_record, &key_enc) {
			Ok(enc) => enc,
			Err(err) => panic!("Failed to encode token: {:?}", err),
		};

		info!("* Create access methods with identical names at ROOT, NS and DB levels");
		{
			let args = format!(
				"sql --endpoint http://{addr} --db {db} --ns {ns} --user {USER} --pass {PASS}"
			);
			let _ = common::run(&args)
				.input(format!("DEFINE ACCESS {ac} ON ROOT TYPE JWT ALGORITHM HS512 KEY '{key}';
                                                DEFINE ACCESS {ac} ON NAMESPACE TYPE JWT ALGORITHM HS512 KEY '{key}';
                                                DEFINE ACCESS {ac} ON DATABASE TYPE JWT ALGORITHM HS512 KEY '{key}';\n").as_str())
				.output()
				.expect("success");
		}

		info!("* Create record that will be used as record user for authentication");
		{
			let args = format!(
				"sql --endpoint http://{addr} --db {db} --ns {ns} --user {USER} --pass {PASS}"
			);
			let _ = common::run(&args)
				.input(format!("CREATE {record_user};").as_str())
				.output()
				.expect("success");
		}

		info!("* Pass root token and access root info");
		{
			let args =
				format!("sql --endpoint http://{addr} --db {db} --ns {ns} --token {token_root}");
			let output = common::run(&args)
				.input(format!("USE NS `{ns}` DB `{db}`; INFO FOR ROOT;\n").as_str())
				.output()
				.expect("success");
			assert!(
				output.contains("namespaces: {"),
				"root token should be able to access root info: {output}"
			);
		}

		info!("* Pass root token and access namespace info");
		{
			let args =
				format!("sql --endpoint http://{addr} --db {db} --ns {ns} --token {token_root}");
			let output = common::run(&args)
				.input(format!("USE NS `{ns}` DB `{db}`; INFO FOR NS;\n").as_str())
				.output()
				.expect("success");
			assert!(
				output.contains("databases: {"),
				"root token should be able to access namespace info: {output}"
			);
		}

		info!("* Pass root auth level and access database info");
		{
			let args =
				format!("sql --endpoint http://{addr} --db {db} --ns {ns} --token {token_root}");
			let output = common::run(&args)
				.input(format!("USE NS `{ns}` DB `{db}`; INFO FOR DB;\n").as_str())
				.output()
				.expect("success");
			assert!(
				output.contains("tables: {"),
				"root token should be able to access database info: {output}"
			);
		}

		info!("* Pass namespace token and access root info");
		{
			let args =
				format!("sql --endpoint http://{addr} --db {db} --ns {ns} --token {token_ns}");
			let output = common::run(&args)
				.input(format!("USE NS `{ns}` DB `{db}`; INFO FOR ROOT;\n").as_str())
				.output_even_on_failure();
			assert!(
				output.contains("IAM error: Not enough permissions to perform this action"),
				"namespace token should not be able to access root info: {output}"
			);
		}

		info!("* Pass namespace token and access namespace info");
		{
			let args =
				format!("sql --endpoint http://{addr} --db {db} --ns {ns} --token {token_ns}");
			let output = common::run(&args)
				.input(format!("USE NS `{ns}` DB `{db}`; INFO FOR NS;\n").as_str())
				.output()
				.expect("success");
			assert!(
				output.contains("databases: {"),
				"namespace token should be able to access namespace info: {output}"
			);
		}

		info!("* Pass namespace token and access database info");
		{
			let args =
				format!("sql --endpoint http://{addr} --db {db} --ns {ns} --token {token_ns}");
			let output = common::run(&args)
				.input(format!("USE NS `{ns}` DB `{db}`; INFO FOR DB;\n").as_str())
				.output()
				.expect("success");
			assert!(
				output.contains("tables: {"),
				"namespace token should be able to access database info: {output}"
			);
		}

		info!("* Pass database token and access root info");
		{
			let args =
				format!("sql --endpoint http://{addr} --db {db} --ns {ns} --token {token_db}");
			let output = common::run(&args)
				.input(format!("USE NS `{ns}` DB `{db}`; INFO FOR ROOT;\n").as_str())
				.output_even_on_failure();
			assert!(
				output.contains("IAM error: Not enough permissions to perform this action"),
				"database token should not be able to access root info: {output}",
			);
		}

		info!("* Pass database token and access namespace info");
		{
			let args =
				format!("sql --endpoint http://{addr} --db {db} --ns {ns} --token {token_db}");
			let output = common::run(&args)
				.input(format!("USE NS `{ns}` DB `{db}`; INFO FOR NS;\n").as_str())
				.output_even_on_failure();
			assert!(
				output.contains("IAM error: Not enough permissions to perform this action"),
				"database token should not be able to access namespace info: {output}",
			);
		}

		info!("* Pass database token and access database info");
		{
			let args =
				format!("sql --endpoint http://{addr} --db {db} --ns {ns} --token {token_db}");
			let output = common::run(&args)
				.input(format!("USE NS `{ns}` DB `{db}`; INFO FOR DB;\n").as_str())
				.output()
				.expect("success");
			assert!(
				output.contains("tables: {"),
				"database token should be able to access database info: {output}"
			);
		}

		info!("* Pass database token without specifying namespace or database");
		{
			let args = format!("sql --endpoint http://{addr} --token {token_db}");
			let output = common::run(&args).input("INFO FOR DB;\n").output().expect("success");
			assert!(
				output.contains("tables: {"),
				"namespace and database to use should be selected from token: {output}"
			);
		}

		info!("* Pass record user token and access database info");
		{
			let args =
				format!("sql --endpoint http://{addr} --db {db} --ns {ns} --token {token_record}");
			let output = common::run(&args)
				.input(format!("USE NS `{ns}` DB `{db}`; INFO FOR DB;\n").as_str())
				.output()
				.expect("success");
			assert!(
				output.contains("tables: {"),
				"database token should be able to access database info: {output}"
			);
		}

		info!("* Pass namespace token without specifying namespace");
		{
			let args = format!("sql --endpoint http://{addr} --token {token_ns}");
			let output = common::run(&args)
				.input(format!("USE NS `{ns}` DB `{db}`; INFO FOR NS;\n").as_str())
				.output()
				.expect("success");
			assert!(
				output.contains("databases: {"),
				"namespace should be selected from token: {output}"
			);
		}

		info!("* Pass database token without specifying database");
		{
			let args = format!("sql --endpoint http://{addr} --ns {ns} --token {token_db}");
			let output = common::run(&args)
				.input(format!("USE NS `{ns}` DB `{db}`; INFO FOR DB;\n").as_str())
				.output()
				.expect("success");
			assert!(
				output.contains("tables: {"),
				"database should be selected from token: {output}"
			);
		}

		info!("* Pass token at the same time as credentials");
		{
			let args = format!(
				"sql --endpoint http://{addr} --ns {ns} --token {token_db} -u {USER} -p {PASS}"
			);
			let output = common::run(&args)
				.input(format!("USE NS `{ns}` DB `{db}`; INFO FOR DB;\n").as_str())
				.output();
			assert!(
				output
					.clone()
					.unwrap_err()
					.contains("error: the argument '--token <TOKEN>' cannot be used with"),
				"the token argument should not be compatible with credentials: {:?}",
				output
			);
		}

		info!("* Pass token at the same time as different auth level");
		{
			let args = format!(
				"sql --endpoint http://{addr} --ns {ns} --token {token_db} --auth-level namespace"
			);
			let output = common::run(&args)
				.input(format!("USE NS `{ns}` DB `{db}`; INFO FOR DB;\n").as_str())
				.output();
			assert!(
				output
					.clone()
					.unwrap_err()
					.contains("error: the argument '--token <TOKEN>' cannot be used with"),
				"the token argument should not be compatible with authentication level: {:?}",
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
		let sql_args = format!("sql --endpoint http://{addr} --multi --pretty");

		info!("* Query over HTTP");
		{
			let args = format!("{sql_args} {creds}");
			let input = "";
			let output = common::run(&args).input(input).output();
			assert!(output.is_ok(), "anonymous user should be able to query: {output:?}");
		}

		info!("* Query over WS");
		{
			let args = format!("sql --endpoint ws://{addr} --multi --pretty {creds}");
			let input = "";
			let output = common::run(&args).input(input).output();
			assert!(output.is_ok(), "anonymous user should be able to query: {output:?}");
		}

		info!("* Can't do exports");
		{
			let args = format!(
				"export --endpoint http://{addr} {creds} --ns {throwaway} --db {throwaway} -",
				throwaway = Ulid::new()
			);
			let output = common::run(&args).output();
			assert!(
				output.clone().unwrap_err().contains("Forbidden"),
				"anonymous user shouldn't be able to export: {output:?}"
			);
		}

		info!("* Can't do imports");
		{
			let tmp_file = common::tmp_file("exported.surql");
			File::create(&tmp_file).expect("failed to create tmp file");
			let args = format!(
				"import --endpoint http://{addr} {creds} --ns {throwaway} --db {throwaway} {tmp_file}",
				throwaway = Ulid::new()
			);
			let output = common::run(&args).output();
			assert!(
				output.clone().unwrap_err().contains("Forbidden"),
				"anonymous user shouldn't be able to import: {output:?}"
			);
		}
		server.finish().unwrap();
	}

	#[test(tokio::test)]
	async fn with_import_file() {
		let ns = Ulid::new();
		let db = Ulid::new();

		info!("* Import and reimport root-level file");
		{
			// Start the server with an import file containing the following query:
			let query = r#"DEFINE ACCESS OVERWRITE admin ON ROOT TYPE JWT URL "https://www.surrealdb.com/jwks.json";"#;
			let import_file = common::tmp_file("import.surql");
			let mut file = File::create(&import_file).unwrap();
			file.write_all(query.as_bytes()).unwrap();
			let (addr, mut server) =
				common::start_server_with_import_file(&import_file).await.unwrap();
			// Define connection arguments.
			let args = format!("sql --endpoint http://{addr} --user {USER} --pass {PASS}");
			// Verify that the file has been imported correctly.
			let output = common::run(&args).input("INFO FOR ROOT").output().expect("success");
			assert!(output.contains(
				r#"DEFINE ACCESS admin ON ROOT TYPE JWT URL 'https://www.surrealdb.com/jwks.json'"#
			));
			// Modify the resource that was imported.
			common::run(&args)
				.input(
					"DEFINE ACCESS OVERWRITE admin ON ROOT TYPE JWT URL 'https://www.example.com/jwks.json'",
				)
				.output()
				.expect("success");
			// Verify that the resource has been modified correctly.
			let output = common::run(&args).input("INFO FOR ROOT").output().expect("success");
			assert!(output.contains(
				r#"DEFINE ACCESS admin ON ROOT TYPE JWT URL 'https://www.example.com/jwks.json'"#
			));
			// Restart the server.
			server.finish().unwrap();
			let (addr, mut server) =
				common::start_server_with_import_file(&import_file).await.unwrap();
			// Verify that the resource has been recreated correctly.
			let args = format!("sql --endpoint http://{addr} --user {USER} --pass {PASS}");
			let output = common::run(&args).input("INFO FOR ROOT").output().expect("success");
			assert!(output.contains(
				r#"DEFINE ACCESS admin ON ROOT TYPE JWT URL 'https://www.surrealdb.com/jwks.json'"#
			));
			server.finish().unwrap();
		}

		info!("* Multi-level import file");
		{
			// Start the server with an import file containing the following query:
			let query = format!(
				r#"
				USE NS `{ns}`;
				DEFINE USER test ON NAMESPACE PASSWORD "secret";
				USE NS `{ns}` DB `{db}`;
				CREATE user:1;
			"#
			);
			let import_file = common::tmp_file("import.surql");
			let mut file = File::create(&import_file).unwrap();
			file.write_all(query.as_bytes()).unwrap();
			let (addr, mut server) =
				common::start_server_with_import_file(&import_file).await.unwrap();
			// Verify that the file has been imported correctly.
			let args = format!(
				"sql --endpoint http://{addr} --user {USER} --pass {PASS} --namespace {ns}"
			);
			let output = common::run(&args).input("INFO FOR NAMESPACE").output().expect("success");
			assert!(output.contains(r#"DEFINE USER test ON NAMESPACE PASSHASH"#));
			let args = format!(
				"sql --endpoint http://{addr} --user {USER} --pass {PASS} --namespace {ns} --database {db}"
			);
			let output = common::run(&args).input("SELECT * FROM user").output().expect("success");
			assert!(output.contains(r#"{ id: user:1 }"#));
			server.finish().unwrap();
		}

		info!("* Invalid import file");
		{
			// Start the server with an import file which contains invalid SurrealQL.
			let query = r#"
				DEFINE USER test ON ROOT PASSWORD "secret"; -- Valid
				DEFINE USER ON ROOT test PASSWORD "secret"; -- Invalid
			"#;
			let import_file = common::tmp_file("import.surql");
			let mut file = File::create(&import_file).unwrap();
			file.write_all(query.as_bytes()).unwrap();
			let res = common::start_server_with_import_file(&import_file).await;
			match res {
				Ok(_) => panic!("import file should contain parseable SurrealQL"),
				Err(e) => {
					assert!(e.to_string().contains("server failed to start"))
				}
			}
			// Verify that no data has been created on the datastore.
			let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
			let args = format!(
				"sql --endpoint http://{addr} --user {USER} --pass {PASS} --namespace {ns}"
			);
			let output = common::run(&args).input("INFO FOR ROOT").output().expect("success");
			assert!(!output.contains(r#"DEFINE USER test ON ROOT PASSHASH"#));
			server.finish().unwrap();
		}

		info!("* Nonexistent import file");
		{
			// Start the server with an import file which does not exist.
			let import_file = common::tmp_file("import.surql");
			let res = common::start_server_with_import_file(&import_file).await;
			match res {
				Ok(_) => panic!("import file should require an existent file"),
				Err(e) => {
					assert!(e.to_string().contains("server failed to start"))
				}
			}
		}
	}

	/// A startup import file need not open with `USE`: its statements apply to the
	/// default namespace and database the server was started with.
	#[test(tokio::test)]
	async fn a_startup_import_applies_to_the_default_database() {
		let import_file = common::tmp_file("no_use.surql");
		std::fs::write(&import_file, "DEFINE TABLE hi; CREATE hi:1 SET greeting = 'hello';")
			.unwrap();
		let (addr, mut server) = common::start_server_with_import_file(&import_file).await.unwrap();
		let args = format!(
			"sql --endpoint http://{addr} --user {USER} --pass {PASS} --ns main --db main --hide-welcome"
		);
		let output = common::run(&args).input("SELECT * FROM hi;\n").output().expect("success");
		assert!(output.contains("hello"), "the imported record is missing: {output}");
		server.finish().unwrap();
	}

	/// The import runs in the default database the datastore records, which a
	/// later start with different (or no) default flags must not override.
	#[test(tokio::test)]
	async fn a_startup_import_applies_to_the_recorded_default_database() {
		let path = format!("surrealkv://{}", common::tmp_file("recorded_default.db"));
		let (_addr, mut first) = common::start_server(common::StartServerArguments {
			path: Some(path.clone()),
			args: "--default-namespace acme --default-database prod".to_string(),
			..Default::default()
		})
		.await
		.unwrap();
		first.finish().unwrap();

		let import_file = common::tmp_file("recorded_default.surql");
		std::fs::write(&import_file, "DEFINE TABLE hi; CREATE hi:1 SET greeting = 'hello';")
			.unwrap();
		let (addr, mut second) = common::start_server(common::StartServerArguments {
			path: Some(path),
			import_file: Some(import_file),
			..Default::default()
		})
		.await
		.unwrap();
		let args = format!(
			"sql --endpoint http://{addr} --user {USER} --pass {PASS} --ns acme --db prod --hide-welcome"
		);
		let output = common::run(&args).input("SELECT * FROM hi;\n").output().expect("success");
		assert!(output.contains("hello"), "the imported record is missing: {output}");
		second.finish().unwrap();
	}

	/// A startup import file that does not parse reads the same on every attempt,
	/// so the failure must be reported at once rather than after the whole
	/// startup retry budget, and the instance must not become ready.
	#[test(tokio::test)]
	async fn an_unparseable_import_file_is_reported_without_retrying() {
		let import_file = common::tmp_file("unparseable.surql");
		std::fs::write(&import_file, "DEFINE USER ON ROOT test PASSWORD 'secret';").unwrap();
		let server = common::run(&format!(
			"start --bind 127.0.0.1:0 memory --log trace --user {USER} --pass {PASS} --import-file {import_file}"
		));
		let started = time::Instant::now();
		loop {
			let output = server.stdout_and_stderr();
			if output.contains("Startup failed; instance will not become ready") {
				assert!(
					!output.contains("Startup complete"),
					"an instance whose import failed must not become ready:\n{output}"
				);
				break;
			}
			assert!(
				started.elapsed() < time::Duration::from_secs(20),
				"the server kept retrying an import file that can never parse:\n{output}"
			);
			tokio::time::sleep(time::Duration::from_millis(100)).await;
		}
	}

	/// The shell reads back a record nested forty-eight levels deep over a
	/// WebSocket, and the session stays usable after it.
	#[test(tokio::test)]
	async fn shell_reads_a_deeply_nested_record_over_websocket() {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let creds = format!("--user {USER} --pass {PASS}");
		let (ns, db) = (Ulid::new(), Ulid::new());
		let mut nested = "{ name: 'first', inner: [] }".to_string();
		for level in 1..=48 {
			nested = format!("{{ level: {level}, inner: [{nested}] }}");
		}

		let args = format!("sql --endpoint ws://{addr} {creds} --ns {ns} --db {db}");
		let input = format!(
			"CREATE ws_article:1 CONTENT {nested} RETURN NONE;\nSELECT * FROM ws_article;\nRETURN 'still here';\n"
		);
		let output = common::run(&args).input(&input).output().unwrap();
		assert!(output.contains("'first'"), "the innermost level should be printed: {output}");
		assert!(output.contains("'still here'"), "the shell should answer after it: {output}");
		assert!(!output.contains("Failed to deserialise"), "{output}");
	}

	/// The shell parses each line to validate it and to count the results it
	/// should expect, then sends the line itself. Rendering the parsed
	/// statements back instead would put every gap in `ToSql` between the user
	/// and the server, silently changing what they asked for or sending
	/// something the server rejects.
	///
	/// The probe is a parenthesised equality as a statement of its own, which
	/// the renderer writes back without its parentheses — the server reads that
	/// as an assignment and refuses it. The shell must not depend on the
	/// renderer being faithful, which is what this asserts; the appended
	/// capture expressions still have to line up with the results.
	#[test(tokio::test)]
	async fn shell_sends_the_statement_it_was_given() {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let ns = Ulid::new();
		let db = Ulid::new();
		let args =
			format!("sql --endpoint http://{addr} --user {USER} --pass {PASS} --ns {ns} --db {db}");

		info!("* A statement the renderer does not round-trip still reaches the server");
		{
			let output =
				common::run(&args).input("LET $p = 1;\n($p = 1);\n").output().expect("success");
			assert!(
				output.contains("true"),
				"the shell sent something other than what it was given: {output}"
			);
		}

		info!("* A LET is still read back into the session");
		{
			let output = common::run(&args)
				.input("LET $x = 41;\nRETURN $x + 1;\n")
				.output()
				.expect("success");
			assert!(output.contains("42"), "the session variable was lost: {output}");
		}

		info!("* A LET whose name needs escaping is read back too");
		{
			let output = common::run(&args)
				.input("LET $`R-X` = 41;\nRETURN $`R-X` + 1;\n")
				.output()
				.expect("success");
			assert!(
				output.contains("42"),
				"the capture expression did not name the variable: {output}"
			);
		}

		info!("* A trailing line comment cannot swallow the statements the shell appends");
		{
			let output = common::run(&args).input("RETURN 7 -- seven\n").output().expect("success");
			assert!(output.contains('7'), "the commented line did not run: {output}");
		}

		info!("* A USE still switches the session");
		{
			let db2 = Ulid::new();
			let output = common::run(&args)
				.input(&format!("USE DB `{db2}`;\nCREATE ONLY thing:one;\n"))
				.output()
				.expect("success");
			assert!(output.contains("thing:one"), "the USE did not take effect: {output}");
		}
	}

	/// A script piping statements into `surreal sql` learns of a failure only
	/// through the exit status: a statement that errors at runtime and a line that
	/// does not parse must both end the command unsuccessfully, and a clean run
	/// must still succeed.
	#[test(tokio::test)]
	async fn sql_with_piped_input_exits_nonzero_when_a_statement_fails() {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let ns = Ulid::new();
		let db = Ulid::new();
		let args = format!(
			"sql --endpoint http://{addr} --user {USER} --pass {PASS} --ns {ns} --db {db} --hide-welcome"
		);

		info!("* A runtime error fails the command");
		let mut child = common::run(&args).input("THROW 'boom';\n");
		assert!(child.output().is_err(), "a failed statement must not exit 0");

		info!("* A statement that fails after earlier ones succeeded fails the command");
		let mut child = common::run(&args).input("CREATE a:1;\nTHROW 'boom';\nCREATE a:2;\n");
		assert!(child.output().is_err(), "a later failure must not be masked by earlier successes");

		info!("* A parse error fails the command");
		let mut child = common::run(&args).input("SELEKT 1;\n");
		assert!(child.output().is_err(), "a parse error must not exit 0");

		info!("* A clean run still succeeds");
		let mut child = common::run(&args).input("CREATE b:1;\nRETURN 1;\n");
		child.output().expect("success");
	}

	#[test(tokio::test)]
	async fn node() {
		// Commands without credentials when auth is disabled, should succeed
		let (addr, mut server) = common::start_server(StartServerArguments {
			auth: false,
			tls: false,
			wait_is_ready: true,
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
				"sql --endpoint http://{addr} {creds} --ns {ns} --db {db} --multi --hide-welcome"
			);
			let output =
				common::run(&args).input("DEFINE TABLE thing TYPE ANY;\n").output().unwrap();
			let output = remove_debug_info(output);
			assert_eq!(output, "[NONE]\n\n".to_owned(), "failed to send sql: {args}");
		}

		info!("* Create a record");
		{
			let args = format!(
				"sql --endpoint http://{addr} {creds} --ns {ns} --db {db} --multi --hide-welcome"
			);
			let output = common::run(&args).input("CREATE thing:one;\n").output().unwrap();
			let output = remove_debug_info(output);
			assert_eq!(
				output,
				"[[{ id: thing:one }]]\n\n".to_owned(),
				"failed to send sql: {args}"
			);
		}

		info!("* Query the record");
		{
			let args = format!(
				"sql --endpoint http://{addr} {creds} --ns {ns} --db {db} --multi --hide-welcome"
			);
			let output = common::run(&args).input("SELECT * FROM thing:one;\n").output().unwrap();
			let output = remove_debug_info(output);
			assert_eq!(
				output,
				"[[{ id: thing:one }]]\n\n".to_owned(),
				"failed to send sql: {args}"
			);
		}

		info!("* Delete the record");
		{
			let args = format!(
				"sql --endpoint http://{addr} {creds} --ns {ns} --db {db} --multi --hide-welcome"
			);
			let output = common::run(&args).input("DELETE thing:one;\n").output().unwrap();
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

		common::run_in_dir("validate", &temp_dir).output().unwrap();
	}

	#[test]
	fn validate_failed_due_to_invalid_glob_pattern() {
		let temp_dir = assert_fs::TempDir::new().unwrap();

		const WRONG_GLOB_PATTERN: &str = "**/*{.txt";

		let args = format!("validate \"{WRONG_GLOB_PATTERN}\"");

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

	#[test]
	fn validate_stdin_with_valid_query() {
		let mut child = common::run("validate --stdin").input("CREATE thing:success;");
		let output = child.output().unwrap();
		assert!(output.contains("<stdin>: OK"), "expected OK, got: {output}");
	}

	#[test]
	fn validate_stdin_with_invalid_query() {
		let mut child = common::run("validate --stdin").input("CREATE $thing WHERE value = '';");
		assert!(child.output().is_err());
	}

	#[cfg(unix)]
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

	#[cfg(unix)]
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
					let err = server.stdout_and_stderr();
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
						panic!("Server unexpectedly exited after receiving first SIGINT: {exit:?}");
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
				"sql --endpoint ws://{addr} -u root -p root --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = "RETURN http::get('http://127.0.0.1/');\n\n";
			let output = common::run(&cmd).input(query).output_even_on_failure();
			assert!(
				output.contains("Access to network target '127.0.0.1:80' is not allowed"),
				"unexpected output: {output:?}"
			);

			let query = "RETURN function() { return '1' };";
			let output = common::run(&cmd).input(query).output_even_on_failure();
			assert!(
				output.contains("Scripting functions are not allowed")
					|| output.contains("Embedded functions are not enabled"),
				"unexpected output: {output:?}"
			);

			server.finish().unwrap();
		}

		// Deny all, denies all users to execute functions and access any network
		// address
		info!("* When all capabilities are denied");
		{
			let (addr, mut server) = common::start_server(StartServerArguments {
				args: "--deny-all".to_owned(),
				..Default::default()
			})
			.await
			.unwrap();

			let cmd = format!(
				"sql --endpoint ws://{addr} -u root -p root --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = format!("RETURN http::get('http://{addr}/version');\n\n");
			let output = common::run(&cmd).input(&query).output_even_on_failure();
			assert!(
				output.contains("Function 'http::get' is not allowed"),
				"unexpected output: {output:?}"
			);

			let query = "RETURN function() { return '1' };";
			let output = common::run(&cmd).input(query).output_even_on_failure();
			assert!(
				output.contains("Scripting functions are not allowed")
					|| output.contains("Embedded functions are not enabled"),
				"unexpected output: {output:?}"
			);
			server.finish().unwrap();
		}

		// When all capabilities are allowed, anyone (including non-authenticated users)
		// can execute functions and access any network address
		info!("* When all capabilities are allowed");
		{
			let (addr, mut server) = common::start_server(StartServerArguments {
				args: "--allow-all".to_owned(),
				..Default::default()
			})
			.await
			.unwrap();

			let cmd = format!(
				"sql --endpoint ws://{addr} --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = format!("RETURN http::get('http://{addr}/version');\n\n");
			let output = common::run(&cmd).input(&query).output_even_on_failure();
			assert!(output.contains("['surrealdb-"), "unexpected output: {output:?}");

			let query = "RETURN function() { return '1' };";
			let output = common::run(&cmd).input(query).output_even_on_failure();
			assert!(output.contains("['1']"), "unexpected output: {output:?}");

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
				"sql --endpoint ws://{addr} -u root -p root --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = "RETURN function() { return '1' };";
			let output = common::run(&cmd).input(query).output_even_on_failure();
			assert!(
				output.contains("Scripting functions are not allowed")
					|| output.contains("Embedded functions are not enabled"),
				"unexpected output: {output:?}"
			);
			server.finish().unwrap();
		}

		info!("* When capabilities are denied globally, but functions are allowed generally");
		{
			let (addr, mut server) = common::start_server(StartServerArguments {
				args: "--deny-all --allow-funcs".to_owned(),
				..Default::default()
			})
			.await
			.unwrap();

			let cmd = format!(
				"sql --endpoint ws://{addr} -u root -p root --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = "RETURN string::len('123');\n\n".to_string();
			let output = common::run(&cmd).input(&query).output_even_on_failure();
			assert!(output.contains("[3]"), "unexpected output: {output:?}");

			server.finish().unwrap();
		}

		info!(
			"* When capabilities are denied globally, but a function family is allowed specifically"
		);
		{
			let (addr, mut server) = common::start_server(StartServerArguments {
				args: "--deny-all --allow-funcs string::len".to_owned(),
				..Default::default()
			})
			.await
			.unwrap();

			let cmd = format!(
				"sql --endpoint ws://{addr} -u root -p root --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = "RETURN string::len('123');\n\n".to_string();
			let output = common::run(&cmd).input(&query).output_even_on_failure();
			assert!(output.contains("[3]"), "unexpected output: {output:?}");

			server.finish().unwrap();
		}

		info!("* When capabilities are allowed globally, but functions are denied generally");
		{
			let (addr, mut server) = common::start_server(StartServerArguments {
				args: "--allow-all --deny-funcs".to_owned(),
				..Default::default()
			})
			.await
			.unwrap();

			let cmd = format!(
				"sql --endpoint ws://{addr} -u root -p root --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = "RETURN string::lowercase('SURREALDB');\n\n".to_string();
			let output = common::run(&cmd).input(&query).output_even_on_failure();
			assert!(
				output.contains("Function 'string::lowercase' is not allowed"),
				"unexpected output: {output:?}"
			);

			server.finish().unwrap();
		}

		info!(
			"* When capabilities are allowed globally, but a function family is denied specifically"
		);
		{
			let (addr, mut server) = common::start_server(StartServerArguments {
				args: "--allow-all --deny-funcs string::lowercase".to_owned(),
				..Default::default()
			})
			.await
			.unwrap();

			let cmd = format!(
				"sql --endpoint ws://{addr} -u root -p root --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = "RETURN string::lowercase('SURREALDB');\n\n".to_string();
			let output = common::run(&cmd).input(&query).output_even_on_failure();
			assert!(
				output.contains("Function 'string::lowercase' is not allowed"),
				"unexpected output: {output:?}"
			);

			let query = "RETURN string::len('123');\n\n".to_string();
			let output = common::run(&cmd).input(&query).output_even_on_failure();
			assert!(output.contains("[3]"), "unexpected output: {output:?}");

			server.finish().unwrap();
		}

		info!("* When functions are denied generally, but allowed for a specific family");
		{
			let (addr, mut server) = common::start_server(StartServerArguments {
				args: "--deny-funcs --allow-funcs string::len".to_owned(),
				..Default::default()
			})
			.await
			.unwrap();

			let cmd = format!(
				"sql --endpoint ws://{addr} -u root -p root --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = "RETURN string::lowercase('SURREALDB');\n\n".to_string();
			let output = common::run(&cmd).input(&query).output_even_on_failure();
			assert!(
				output.contains("Function 'string::lowercase' is not allowed"),
				"unexpected output: {output:?}"
			);

			let query = "RETURN string::len('123');\n\n".to_string();
			let output = common::run(&cmd).input(&query).output_even_on_failure();
			assert!(output.contains("[3]"), "unexpected output: {output:?}");

			server.finish().unwrap();
		}

		info!("* When functions are allowed generally, but denied for a specific family");
		{
			let (addr, mut server) = common::start_server(StartServerArguments {
				args: "--allow-funcs --deny-funcs string::lowercase".to_owned(),
				..Default::default()
			})
			.await
			.unwrap();

			let cmd = format!(
				"sql --endpoint ws://{addr} -u root -p root --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = "RETURN string::lowercase('SURREALDB');\n\n".to_string();
			let output = common::run(&cmd).input(&query).output_even_on_failure();
			assert!(
				output.contains("Function 'string::lowercase' is not allowed"),
				"unexpected output: {output:?}"
			);

			let query = "RETURN string::len('123');\n\n".to_string();
			let output = common::run(&cmd).input(&query).output_even_on_failure();
			assert!(output.contains("[3]"), "unexpected output: {output:?}");

			server.finish().unwrap();
		}

		info!(
			"* When functions are both allowed and denied specifically but denies are more specific"
		);
		{
			let (addr, mut server) = common::start_server(StartServerArguments {
				args: "--allow-funcs string --deny-funcs string::lowercase".to_owned(),
				..Default::default()
			})
			.await
			.unwrap();

			let cmd = format!(
				"sql --endpoint ws://{addr} -u root -p root --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = "RETURN string::lowercase('SURREALDB');\n\n".to_string();
			let output = common::run(&cmd).input(&query).output_even_on_failure();
			assert!(
				output.contains("Function 'string::lowercase' is not allowed"),
				"unexpected output: {output:?}"
			);

			let query = "RETURN string::len('123');\n\n".to_string();
			let output = common::run(&cmd).input(&query).output_even_on_failure();
			assert!(output.contains("[3]"), "unexpected output: {output:?}");

			server.finish().unwrap();
		}

		info!(
			"* When functions are both allowed and denied specifically but allows are more specific"
		);
		{
			let (addr, mut server) = common::start_server(StartServerArguments {
				args: "--deny-funcs string --allow-funcs string::lowercase".to_owned(),
				..Default::default()
			})
			.await
			.unwrap();

			let cmd = format!(
				"sql --endpoint ws://{addr} -u root -p root --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = "RETURN string::lowercase('SURREALDB');\n\n".to_string();
			let output = common::run(&cmd).input(&query).output_even_on_failure();
			assert!(
				output.contains("Function 'string::lowercase' is not allowed"),
				"unexpected output: {output:?}"
			);

			let query = "RETURN string::len('123');\n\n".to_string();
			let output = common::run(&cmd).input(&query).output_even_on_failure();
			assert!(
				output.contains("Function 'string::len' is not allowed"),
				"unexpected output: {output:?}"
			);

			server.finish().unwrap();
		}

		info!("* When capabilities are denied globally, but net is allowed generally");
		{
			let (addr, mut server) = common::start_server(StartServerArguments {
				args: "--deny-all --allow-net --allow-funcs http::get".to_owned(),
				..Default::default()
			})
			.await
			.unwrap();

			let cmd = format!(
				"sql --endpoint ws://{addr} -u root -p root --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = format!("RETURN http::get('http://{addr}/version');\n\n");
			let output = common::run(&cmd).input(&query).output_even_on_failure();
			assert!(output.contains("['surrealdb-"), "unexpected output: {output:?}");
			server.finish().unwrap();
		}

		info!("* When capabilities are denied globally, but net is allowed specifically");
		{
			let (addr, mut server) = common::start_server(StartServerArguments {
				args: "--deny-all --allow-net 127.0.0.1 --allow-funcs http::get".to_owned(),
				..Default::default()
			})
			.await
			.unwrap();

			let cmd = format!(
				"sql --endpoint ws://{addr} -u root -p root --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = format!("RETURN http::get('http://{addr}/version');\n\n");
			let output = common::run(&cmd).input(&query).output_even_on_failure();
			assert!(output.contains("['surrealdb-"), "unexpected output: {output:?}");
			server.finish().unwrap();
		}

		info!("* When capabilities are allowed globally, but net is denied generally");
		{
			let (addr, mut server) = common::start_server(StartServerArguments {
				args: "--allow-all --deny-net --allow-funcs http::get".to_owned(),
				..Default::default()
			})
			.await
			.unwrap();

			let cmd = format!(
				"sql --endpoint ws://{addr} -u root -p root --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = format!("RETURN http::get('http://{addr}/version');\n\n");
			let output = common::run(&cmd).input(&query).output_even_on_failure();
			assert!(
				output
					.contains(format!("Access to network target '{addr}' is not allowed").as_str()),
				"unexpected output: {output:?}"
			);
			server.finish().unwrap();
		}

		info!("* When capabilities are allowed globally, but net is denied specifically");
		{
			let (addr, mut server) = common::start_server(StartServerArguments {
				args: "--allow-all --deny-net 127.0.0.1 --allow-funcs http::get".to_owned(),
				..Default::default()
			})
			.await
			.unwrap();

			let cmd = format!(
				"sql --endpoint ws://{addr} -u root -p root --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = format!("RETURN http::get('http://{addr}/version');\n\n");
			let output = common::run(&cmd).input(&query).output_even_on_failure();
			assert!(
				output
					.contains(format!("Access to network target '{addr}' is not allowed").as_str()),
				"unexpected output: {output:?}"
			);
			server.finish().unwrap();
		}

		info!("* When net is denied generally, but allowed for a specific address");
		{
			let (addr, mut server) = common::start_server(StartServerArguments {
				args: "--deny-net --allow-net 127.0.0.1 --allow-funcs http::get".to_owned(),
				..Default::default()
			})
			.await
			.unwrap();

			let cmd = format!(
				"sql --endpoint ws://{addr} -u root -p root --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = format!("RETURN http::get('http://{addr}/version');\n\n");
			let output = common::run(&cmd).input(&query).output_even_on_failure();
			assert!(output.contains("['surrealdb-"), "unexpected output: {output:?}");
			server.finish().unwrap();
		}

		info!("* When net is allowed generally, but denied for a specific address");
		{
			let (addr, mut server) = common::start_server(StartServerArguments {
				args: "--allow-net --deny-net 127.0.0.1 --allow-funcs http::get".to_owned(),
				..Default::default()
			})
			.await
			.unwrap();

			let cmd = format!(
				"sql --endpoint ws://{addr} -u root -p root --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = format!("RETURN http::get('http://{addr}/version');\n\n");
			let output = common::run(&cmd).input(&query).output_even_on_failure();
			assert!(
				output
					.contains(format!("Access to network target '{addr}' is not allowed").as_str()),
				"unexpected output: {output:?}"
			);
			server.finish().unwrap();
		}

		info!("* When net is both allowed and denied specifically but denies are more specific");
		{
			let (addr, mut server) = common::start_server(StartServerArguments {
				args: "--allow-net 127.0.0.1 --deny-net 127.0.0.1:80 --allow-funcs http::get"
					.to_owned(),
				..Default::default()
			})
			.await
			.unwrap();

			let cmd = format!(
				"sql --endpoint ws://{addr} -u root -p root --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = format!("RETURN http::get('http://{addr}/version');\n\n");
			let output = common::run(&cmd).input(&query).output_even_on_failure();
			assert!(output.contains("['surrealdb-"), "unexpected output: {output:?}");
			server.finish().unwrap();
		}

		info!("* When net is both allowed and denied specifically but allows are more specific");
		{
			let (addr, mut server) = common::start_server(StartServerArguments {
				args: "--deny-net 127.0.0.1 --allow-net 127.0.0.1:80 --allow-funcs http::get"
					.to_owned(),
				..Default::default()
			})
			.await
			.unwrap();

			let cmd = format!(
				"sql --endpoint ws://{addr} -u root -p root --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = format!("RETURN http::get('http://{addr}/version');\n\n");
			let output = common::run(&cmd).input(&query).output_even_on_failure();
			assert!(
				output
					.contains(format!("Access to network target '{addr}' is not allowed").as_str()),
				"unexpected output: {output:?}"
			);
			server.finish().unwrap();
		}

		info!("* When net is denied specifically and functions are enabled specifically");
		{
			let (addr, mut server) = common::start_server(StartServerArguments {
				args: "--deny-net 127.0.0.1 --allow-funcs http::get".to_owned(),
				..Default::default()
			})
			.await
			.unwrap();

			let cmd = format!(
				"sql --endpoint ws://{addr} -u root -p root --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = format!("RETURN http::get('http://{addr}/version');\n\n");
			let output = common::run(&cmd).input(&query).output_even_on_failure();
			assert!(
				output
					.contains(format!("Access to network target '{addr}' is not allowed").as_str()),
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
				"sql --endpoint ws://{addr} --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = "RETURN 1;\n\n";
			let output = common::run(&cmd).input(query).output_even_on_failure();
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
				"sql --endpoint ws://{addr} --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = "RETURN 1;\n\n";
			let output = common::run(&cmd).input(query).output_even_on_failure();
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
				"sql --endpoint ws://{addr} --ns {throwaway} --db {throwaway} --multi",
				throwaway = Ulid::new()
			);

			let query = "RETURN 1;\n\n";
			let output = common::run(&cmd).input(query).output_even_on_failure();
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
					assert_eq!(e.to_string(), "server failed to start", "{e:?}");
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
					assert_eq!(e.to_string(), "server failed to start", "{e:?}");
				}
			}
			temp_file.close().unwrap();
		}

		info!("* The path is a valid directory");
		#[cfg(feature = "storage-surrealkv")]
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

	#[test(tokio::test)]
	async fn double_create() {
		info!("* check only one output created");
		{
			let args =
				"sql --endpoint memory --ns test --db test --pretty --hide-welcome".to_string();
			let output = common::run(&args)
				.input("let $a = create foo;\n")
				.input("select * from foo;\n")
				.output()
				.unwrap();
			let output = remove_debug_info(output);
			assert_eq!(output.matches("foo:").count(), 1);
		}
	}

	/// Runs a query that outlasts the one-second slow-query threshold and binds
	/// `$public = 'foo'` and `$secret = 'bar'` on a server started with `args`, and
	/// returns the line the server logged for it.
	async fn slow_query_log_entry(args: &str) -> String {
		// Start the server
		let (addr, mut server) = common::start_server(StartServerArguments {
			auth: false,
			args: format!("--slow-log-threshold=1s {args}"),
			..Default::default()
		})
		.await
		.unwrap();

		// Connect the client
		let cmd = format!(
			"sql --endpoint ws://{addr} --ns {throwaway} --db {throwaway} --multi",
			throwaway = Ulid::new()
		);

		// Start a slow query containing a line feed
		let query = "
			LET $public = 'foo'; LET $secret = 'bar';
			RETURN string::concat(sleep(1200ms), '/', $public, '/', $secret);
		";
		let _ = common::run(&cmd).input(query).output().unwrap();

		// Extract the stderr
		let stderr = server.finish().unwrap().stderr();
		println!("{stderr}");

		// Check the log is present
		stderr
			.lines()
			.find(|line| line.contains("Slow query detected - time: "))
			.unwrap_or_else(|| panic!("no slow query entry in:\n{stderr}"))
			.to_owned()
	}

	/// A slow-query entry lists the value of a parameter named by
	/// `--slow-log-param-allow`, unless `--slow-log-param-deny` names it too.
	#[tokio::test]
	async fn test_slow_query_logging() {
		let entry = slow_query_log_entry(
			"--slow-log-param-allow=public,secret --slow-log-param-deny=secret",
		)
		.await;
		assert!(
			entry.contains(
				"s - query: RETURN string::concat(`sleep`(1s200ms), '/', $public, '/', $secret) - params: [ $public='foo' ]"
			),
			"{entry}"
		);
		assert!(!entry.contains("bar"), "{entry}");
	}

	/// Without `--slow-log-param-allow` a slow-query entry lists no parameter
	/// value, with or without a deny list.
	#[tokio::test]
	async fn test_slow_query_logging_without_allow_list() {
		for args in ["", "--slow-log-param-deny=secret"] {
			let entry = slow_query_log_entry(args).await;
			assert!(
				entry.contains(
					"s - query: RETURN string::concat(`sleep`(1s200ms), '/', $public, '/', $secret) - params: [  ]"
				),
				"{args}: {entry}"
			);
			assert!(!entry.contains("foo") && !entry.contains("bar"), "{args}: {entry}");
		}
	}

	/// `SURREAL_MEMORY_THRESHOLD` with a byte-suffix (e.g. `256m`) must start the
	/// server without a config-parse warning (regression test for #7380).
	///
	/// A previous fix restored suffix parsing for the legacy env-var static but
	/// missed the configmap path, leaving the warning intact for suffixed values.
	#[test(tokio::test)]
	async fn memory_threshold_suffix_no_parse_warning() {
		let vars: HashMap<String, String> =
			[("SURREAL_MEMORY_THRESHOLD".to_string(), "256m".to_string())].into();
		let (_addr, mut server) = common::start_server(StartServerArguments {
			vars: Some(vars),
			..Default::default()
		})
		.await
		.unwrap();

		let stderr = server.finish().unwrap().stderr();
		assert!(
			!stderr.contains("Could not parse configuration value for key `MEMORY_THRESHOLD`"),
			"byte-suffix memory threshold should parse without a warning; stderr:\n{stderr}"
		);
	}
}

fn remove_debug_info(output: String) -> String {
	// Look... sometimes you just gotta copy paste
	let output_warning = "\
┌─────────────────────────────────────────────────────────────────────────────┐
│                     !!! THIS IS A DEVELOPMENT BUILD !!!                     │
│     Development builds are not intended for production use and include      │
│    tooling and features that may affect the performance of the database.    │
└─────────────────────────────────────────────────────────────────────────────┘
";
	// The last line in the above is important
	output.replace(output_warning, "")
}
