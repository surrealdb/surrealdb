//! `--json`: one document on stdout for the commands that return data, and the
//! JSON error envelope on stderr for any failure while the command line asks
//! for JSON.
//!
//! The envelope is the shape every component of the Surreal CLI uses:
//!
//! ```json
//! {"kind":"usage","message":"unexpected argument '--bogus' found","status":null,"code":null,
//!  "request_id":null,"hint":"See the usage with:","command":"surreal ext info --help",
//!  "docs":null,"retry_after_secs":null,"exit_code":2}
//! ```
//!
//! Every key is present, `null` when unknown. On failure stdout stays empty.

use std::ffi::OsString;
use std::fmt;
use std::io::Write;
use std::process::ExitCode;

use serde::Serialize;
use serde_json::{Value, json};

/// The help text of `--json`, shared by every component of the Surreal CLI.
pub(crate) const HELP: &str = "Emit machine-readable JSON on stdout";

/// The id of [`JsonArg`]'s argument, distinct from the root's `--json`, which
/// only goes with `--version`.
const ID: &str = "json_document";

/// `--json`, flattened into the arguments of each command that returns data.
#[derive(clap::Args, Debug, Clone, Copy)]
pub(crate) struct JsonArg {
	#[arg(id = ID, help = HELP, short = 'j', long = "json")]
	pub(crate) json: bool,
}

/// The exit code of a usage error.
const USAGE_EXIT: u8 = 2;

/// The exit code of every other failure.
const FAILURE_EXIT: u8 = 1;

/// Whether `--json` or `-j` is among the unparsed arguments, up to a `--`.
///
/// Read from the raw arguments because a usage error, or `--version`, ends the
/// parse that would have read the flag.
pub(crate) fn asks_for_json(argv: &[OsString]) -> bool {
	argv.iter().take_while(|arg| *arg != "--").any(|arg| arg == "--json" || arg == "-j")
}

/// The envelope's `kind`: the class of a failure, from the vocabulary every
/// component of the Surreal CLI shares.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Kind {
	/// The command line is wrong.
	Usage,
	/// The input was read, and is not valid.
	Invalid,
	/// Something the command needs does not exist.
	NotFound,
	/// Any other failure.
	Unknown,
}

impl Kind {
	fn exit_code(self) -> u8 {
		match self {
			Kind::Usage => USAGE_EXIT,
			Kind::Invalid | Kind::NotFound | Kind::Unknown => FAILURE_EXIT,
		}
	}
}

/// A failure with the `kind` its envelope reports. A command returns it inside
/// its [`anyhow::Error`]; any other error is reported as kind `unknown`.
///
/// It displays as its message alone, so the text error reads the same.
#[derive(Debug)]
pub(crate) struct Failure {
	pub(crate) kind: Kind,
	pub(crate) message: String,
}

impl fmt::Display for Failure {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(&self.message)
	}
}

impl std::error::Error for Failure {}

impl Failure {
	pub(crate) fn new(kind: Kind, message: impl Into<String>) -> Self {
		Self {
			kind,
			message: message.into(),
		}
	}
}

/// Write `value` as the one document on stdout.
pub(crate) fn print(value: &impl Serialize) -> anyhow::Result<()> {
	let mut stdout = std::io::stdout().lock();
	serde_json::to_writer_pretty(&mut stdout, value)?;
	writeln!(stdout)?;
	stdout.flush()?;
	Ok(())
}

/// Write `envelope` on stderr.
fn eprint(envelope: &Value) {
	let mut stderr = std::io::stderr().lock();
	// Nothing is left to report a failure to write the report to.
	let _ = serde_json::to_writer_pretty(&mut stderr, envelope);
	let _ = writeln!(stderr);
	let _ = stderr.flush();
}

/// The envelope with every shared key. `command` is where the usage is, for a
/// usage error.
fn envelope(kind: Kind, message: &str, command: Option<&str>) -> Value {
	let help = command.map(|command| format!("{command} --help"));
	json!({
		"kind": kind,
		"message": message,
		"status": null,
		"code": null,
		"request_id": null,
		"hint": help.as_ref().map(|_| "See the usage with:"),
		"command": help,
		"docs": null,
		"retry_after_secs": null,
		"exit_code": kind.exit_code(),
	})
}

/// The envelope for the failure `err` of `command`, the command's usage path.
fn failure_envelope(err: &anyhow::Error, command: &str) -> Value {
	match err.downcast_ref::<Failure>() {
		Some(failure) => envelope(
			failure.kind,
			&failure.message,
			(failure.kind == Kind::Usage).then_some(command),
		),
		None => envelope(Kind::Unknown, &format!("{err:#}"), None),
	}
}

/// Report the failure `err` of `command` as its envelope on stderr, and return
/// the exit code the envelope names.
pub(crate) fn report_failure(err: &anyhow::Error, command: &str) -> ExitCode {
	let envelope = failure_envelope(err, command);
	eprint(&envelope);
	let code = envelope["exit_code"].as_u64().and_then(|code| u8::try_from(code).ok());
	ExitCode::from(code.unwrap_or(FAILURE_EXIT))
}

/// The usage path of the command the command line names: `bin_name` followed
/// by the first argument that is not a flag, when it names one of `root`'s
/// subcommands, or `bin_name` alone when it does not.
///
/// Only the first such argument can be the subcommand, since the root takes no
/// value-taking flags, so a misspelled one is not passed over for a later
/// argument that happens to share a subcommand's name (`rnu info`). It is read
/// from the raw arguments because clap's error does not always name the
/// command it failed in.
pub(crate) fn command_path(argv: &[OsString], root: &clap::Command, bin_name: &str) -> String {
	argv.iter()
		.skip(1)
		.take_while(|arg| *arg != "--")
		.filter_map(|arg| arg.to_str())
		.find(|arg| !arg.starts_with('-'))
		.and_then(|arg| root.find_subcommand(arg))
		.map_or_else(|| bin_name.to_owned(), |command| format!("{bin_name} {}", command.get_name()))
}

/// The envelope for clap's error `err` in `command`, or `None` for help and
/// `--version`, which are not errors.
fn usage_envelope(err: &clap::Error, command: &str) -> Option<Value> {
	if !err.use_stderr() {
		return None;
	}
	Some(envelope(Kind::Usage, &diagnostic(&err.to_string()), Some(command)))
}

/// clap's first line without its `error: `, with the indented lines under it
/// joined on: the missing arguments under "the following required arguments
/// were not provided:", which the sentence alone leaves out, and any tip. The
/// same reading as the launcher's and `surreal cloud`'s.
///
/// Stops at the first line that is not indented, the `Usage:` block or the
/// closing "For more information": the envelope's `command` says where help is.
fn diagnostic(rendered: &str) -> String {
	let mut lines = rendered.lines().skip_while(|line| line.trim().is_empty());
	let first = lines.next().unwrap_or("");
	let mut message = first.strip_prefix("error: ").unwrap_or(first).trim_end().to_owned();
	// A blank line separates clap's paragraphs, the list from a tip; it
	// becomes a `;` so the two do not read as one sentence.
	let mut paragraph = false;
	for line in lines {
		if line.trim().is_empty() {
			paragraph = true;
			continue;
		}
		if !line.starts_with(char::is_whitespace) {
			break;
		}
		message.push_str(if paragraph {
			"; "
		} else {
			" "
		});
		message.push_str(line.trim());
		paragraph = false;
	}
	message
}

/// Exit after clap's error `err` in `command` with its envelope on stderr, if
/// [`usage_envelope`] has one for it.
pub(crate) fn exit_on_usage_error(err: &clap::Error, command: &str) {
	if let Some(envelope) = usage_envelope(err, command) {
		eprint(&envelope);
		std::process::exit(USAGE_EXIT.into());
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn argv(args: &[&str]) -> Vec<OsString> {
		args.iter().map(OsString::from).collect()
	}

	#[test]
	fn json_is_asked_for_by_either_spelling_before_a_double_dash() {
		assert!(asks_for_json(&argv(&["surreal-ext", "--version", "--json"])));
		assert!(asks_for_json(&argv(&["surreal-ext", "info", "-j", "x.surli"])));
		assert!(!asks_for_json(&argv(&["surreal-ext", "-v"])));
		assert!(!asks_for_json(&argv(&["surreal-ext", "-v", "--", "--json"])));
	}

	/// Every key the other components send is present, so a script reading one
	/// component's envelope reads this one's.
	#[test]
	fn the_envelope_carries_every_shared_key() {
		let envelope = envelope(Kind::Usage, "bad", Some("surreal ext info"));
		let mut keys: Vec<_> = envelope.as_object().unwrap().keys().cloned().collect();
		keys.sort();
		assert_eq!(
			keys,
			[
				"code",
				"command",
				"docs",
				"exit_code",
				"hint",
				"kind",
				"message",
				"request_id",
				"retry_after_secs",
				"status"
			]
		);
		assert_eq!(envelope["kind"], "usage");
		assert_eq!(envelope["command"], "surreal ext info --help");
		assert_eq!(envelope["exit_code"], 2);
	}

	#[test]
	fn a_failure_keeps_its_kind_and_any_other_error_is_unknown() {
		let not_found = anyhow::Error::from(Failure::new(Kind::NotFound, "gone"));
		let envelope = failure_envelope(&not_found, "surreal ext info");
		assert_eq!(envelope["kind"], "not_found");
		assert_eq!(envelope["message"], "gone");
		assert_eq!(envelope["command"], Value::Null);
		assert_eq!(envelope["exit_code"], 1);

		let other = anyhow::anyhow!("inner").context("outer");
		let envelope = failure_envelope(&other, "surreal ext info");
		assert_eq!(envelope["kind"], "unknown");
		assert_eq!(envelope["message"], "outer: inner");
	}

	#[test]
	fn a_usage_message_names_the_missing_arguments() {
		let rendered = "error: the following required arguments were not provided:\n  \
		                <FILE>\n\nUsage: surreal-ext sig <FILE>\n\nFor more information, try \
		                '--help'.\n";
		assert_eq!(
			diagnostic(rendered),
			"the following required arguments were not provided: <FILE>"
		);
		let rendered = "error: unexpected argument '--bogus' found\n\n  tip: to pass '--bogus' \
		                as a value, use '-- --bogus'\n\nUsage: surreal-ext info\n";
		assert_eq!(
			diagnostic(rendered),
			"unexpected argument '--bogus' found; tip: to pass '--bogus' as a value, use '-- --bogus'"
		);
	}

	#[test]
	fn the_command_path_is_the_first_subcommand_named() {
		let root = clap::Command::new("surreal-ext")
			.subcommand(clap::Command::new("info"))
			.subcommand(clap::Command::new("sig"));
		let path = |args: &[&str]| command_path(&argv(args), &root, "surreal ext");
		assert_eq!(path(&["surreal-ext", "-j", "info", "x.surli"]), "surreal ext info");
		assert_eq!(path(&["surreal-ext", "--json"]), "surreal ext");
		assert_eq!(path(&["surreal-ext", "--", "sig"]), "surreal ext");
		assert_eq!(path(&["surreal-ext", "rnu", "info", "--json"]), "surreal ext");
	}
}
