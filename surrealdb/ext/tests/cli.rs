//! The `surreal-ext` executable as the Surreal CLI and its users see it.

use std::path::PathBuf;
use std::process::{Command, Output};

/// The executable under test. CI runs these tests from a nextest archive on
/// runners that never built them, so nextest's run-time path comes first; the
/// compile-time path serves `cargo test`.
fn executable() -> PathBuf {
	["NEXTEST_BIN_EXE_surreal-ext", "NEXTEST_BIN_EXE_surreal_ext"]
		.into_iter()
		.find_map(std::env::var_os)
		.map_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_surreal-ext")), PathBuf::from)
}

fn surreal_ext(args: &[&str], invoked_as: Option<&str>) -> Output {
	let mut command = Command::new(executable());
	command.args(args).env_remove("SURREAL_INVOKED_AS");
	if let Some(invoked_as) = invoked_as {
		command.env("SURREAL_INVOKED_AS", invoked_as);
	}
	command.output().expect("surreal-ext runs")
}

fn stdout(output: &Output) -> String {
	String::from_utf8(output.stdout.clone()).expect("stdout is UTF-8")
}

/// `<semver> for <os> on <arch>`, with a newline, and nothing else; returns
/// the version.
fn assert_release(text: &str) -> semver::Version {
	let first = text.split(' ').next().unwrap_or_default();
	let version = semver::Version::parse(first).unwrap_or_else(|e| panic!("{text}: {e}"));
	assert_eq!(
		text,
		format!("{first} for {} on {}\n", std::env::consts::OS, std::env::consts::ARCH)
	);
	version
}

#[test]
fn version_flag_reports_the_package_version() {
	let output = surreal_ext(&["--version"], None);
	assert!(output.status.success(), "{output:?}");
	let version = assert_release(&stdout(&output));
	let expected = semver::Version::parse(env!("CARGO_PKG_VERSION")).unwrap();
	assert_eq!(
		(version.major, version.minor, version.patch),
		(expected.major, expected.minor, expected.patch)
	);
	assert_eq!(version.pre, expected.pre);
}

/// `-v`, `--version` and the hidden `-V` print the same line, with nothing on
/// stderr, standalone and through the Surreal CLI.
#[test]
fn every_spelling_of_the_version_flag_prints_the_release() {
	let expected = stdout(&surreal_ext(&["--version"], None));
	for invoked_as in [None, Some("surreal ext")] {
		for flag in ["-v", "--version", "-V"] {
			let output = surreal_ext(&[flag], invoked_as);
			assert!(output.status.success(), "{flag}: {output:?}");
			assert_eq!(stdout(&output), expected, "{flag}");
			assert!(output.stderr.is_empty(), "{flag}: {output:?}");
		}
	}
}

/// `--version --json`, in either order and either spelling, prints one JSON
/// document whose keys are `arch`, `os` and `version`, in that order.
#[test]
fn version_flag_json_is_the_version_document() {
	let expected = assert_release(&stdout(&surreal_ext(&["-v"], None))).to_string();
	for args in [
		&["--version", "--json"][..],
		&["--json", "--version"],
		&["-v", "-j"],
		&["-j", "-v"],
		&["-V", "--json"],
	] {
		let output = surreal_ext(args, None);
		assert!(output.status.success(), "{args:?}: {output:?}");
		assert!(output.stderr.is_empty(), "{args:?}: {output:?}");
		let text = stdout(&output);
		let keys: Vec<_> = text
			.lines()
			.filter_map(|line| line.trim().strip_prefix('"'))
			.filter_map(|line| line.split('"').next())
			.collect();
		assert_eq!(keys, ["arch", "os", "version"], "{args:?}: {text}");
		let document: serde_json::Value = serde_json::from_str(&text).expect("one JSON document");
		assert_eq!(document["arch"], std::env::consts::ARCH);
		assert_eq!(document["os"], std::env::consts::OS);
		assert_eq!(document["version"], expected.as_str(), "{args:?}");
	}
}

/// `--json` without `--version` is a usage error, reported as the envelope.
#[test]
fn json_without_the_version_flag_is_a_usage_error() {
	let output = surreal_ext(&["--json"], None);
	assert_eq!(output.status.code(), Some(2), "{output:?}");
	assert!(output.stdout.is_empty(), "{output:?}");
	let envelope = envelope(&output);
	assert_eq!(envelope["kind"], "usage");
	assert_eq!(envelope["command"], "surreal-ext --help");
}

/// The JSON error envelope on stderr, whose `exit_code` is the process's.
fn envelope(output: &Output) -> serde_json::Value {
	let envelope: serde_json::Value = serde_json::from_slice(&output.stderr)
		.unwrap_or_else(|e| panic!("stderr is not one JSON document ({e}): {output:?}"));
	assert_eq!(envelope["exit_code"], output.status.code().expect("an exit code"), "{output:?}");
	envelope
}

/// Under `--json`, a usage error is the envelope on stderr, naming the failing
/// command's help as the Surreal CLI runs it, with exit 2 and nothing on
/// stdout.
#[test]
fn a_usage_error_under_json_is_the_envelope() {
	for args in [&["info", "--json"][..], &["info", "-j", "a.surli", "--bogus"], &["sig", "-j"]] {
		let output = surreal_ext(args, Some("surreal ext"));
		assert_eq!(output.status.code(), Some(2), "{args:?}: {output:?}");
		assert!(output.stdout.is_empty(), "{args:?}: {output:?}");
		let envelope = envelope(&output);
		assert_eq!(envelope["kind"], "usage", "{args:?}");
		assert_eq!(envelope["command"], format!("surreal ext {} --help", args[0]), "{args:?}");
	}
}

/// Without `--json`, a usage error stays clap's text.
#[test]
fn a_usage_error_without_json_is_text() {
	let output = surreal_ext(&["info"], None);
	assert_eq!(output.status.code(), Some(2), "{output:?}");
	assert!(String::from_utf8_lossy(&output.stderr).contains("Usage: surreal-ext info"));
}

/// Under `--json`, a failed `info` or `sig` leaves stdout empty and reports
/// the envelope, with the class of the failure as its `kind`.
#[test]
fn a_failure_under_json_is_the_envelope() {
	let dir = tempfile::tempdir().unwrap();
	let corrupt = dir.path().join("corrupt.surli");
	std::fs::write(&corrupt, b"not an archive").unwrap();
	let corrupt = corrupt.to_str().unwrap();
	for (args, kind) in [
		(&["info", "--json", "does-not-exist.surli"][..], "not_found"),
		(&["sig", "-j", "does-not-exist.surli"], "not_found"),
		(&["info", "--json", corrupt], "invalid"),
	] {
		let output = surreal_ext(args, None);
		assert_eq!(output.status.code(), Some(1), "{args:?}: {output:?}");
		assert!(output.stdout.is_empty(), "{args:?}: {output:?}");
		assert_eq!(envelope(&output)["kind"], kind, "{args:?}");
	}
}

/// `version` is not a command: the version is the flag's alone.
#[test]
fn version_is_not_a_command() {
	let output = surreal_ext(&["version"], None);
	assert_eq!(output.status.code(), Some(2), "{output:?}");
	assert!(output.stdout.is_empty(), "{output:?}");
}

/// Help lists `-v, --version` and `-j, --json`, and not `-V`.
#[test]
fn help_lists_the_version_flag() {
	let help = stdout(&surreal_ext(&["--help"], None));
	assert!(help.contains("-v, --version"), "{help}");
	assert!(help.contains("-j, --json"), "{help}");
	assert!(!help.contains("-V"), "{help}");
}

#[test]
fn usage_names_the_executable() {
	let output = surreal_ext(&["build", "--help"], None);
	assert!(output.status.success());
	assert!(stdout(&output).contains("Usage: surreal-ext build "), "{}", stdout(&output));
}

#[test]
fn usage_names_the_launcher_command() {
	let output = surreal_ext(&["build", "--help"], Some("surreal ext"));
	assert!(output.status.success());
	assert!(stdout(&output).contains("Usage: surreal ext build "), "{}", stdout(&output));
}

#[test]
fn every_extension_subcommand_is_there() {
	let output = surreal_ext(&["--help"], None);
	let help = stdout(&output);
	let mut subcommands =
		vec!["init", "run", "sig", "info", "build", "config", "install", "upgrade"];
	// `sign` is built only with the `sign` feature, on by default.
	if cfg!(feature = "sign") {
		subcommands.push("sign");
	}
	for subcommand in subcommands {
		assert!(help.contains(&format!("  {subcommand} ")), "`{subcommand}` missing from:\n{help}");
	}
	// `upgrade` upgrades an extension in a database; this executable has no
	// command that updates itself or prints its version.
	for retired in ["update", "version"] {
		assert!(
			!help.contains(&format!("  {retired} ")),
			"`{retired}` is not a command of this component"
		);
	}
}

#[test]
fn a_missing_package_fails() {
	let output = surreal_ext(&["info", "does-not-exist.surli"], None);
	assert!(!output.status.success());
	assert!(!output.stderr.is_empty());
}

#[test]
fn run_rejects_an_unparseable_argument() {
	let output = surreal_ext(&["run", "--arg", "{ a: ", "x.surli"], None);
	assert!(!output.status.success());
	assert!(String::from_utf8_lossy(&output.stderr).contains("Invalid value"));
}

#[test]
fn init_suggests_the_launcher_command() {
	let dir = tempfile::tempdir().unwrap();
	let output = surreal_ext(
		&[
			"init",
			"--headless",
			"--org",
			"acme",
			"--name",
			"demo",
			dir.path().join("demo").to_str().unwrap(),
		],
		Some("surreal ext"),
	);
	assert!(output.status.success(), "{output:?}");
	assert!(stdout(&output).contains("  surreal ext build"), "{}", stdout(&output));
	assert!(dir.path().join("demo/package.toml").is_file());
	assert!(dir.path().join("demo/.cargo/config.toml").is_file());
}
