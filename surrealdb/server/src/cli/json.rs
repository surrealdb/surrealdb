//! `--json` for the commands that return data: those that flatten
//! [`JsonArg`] into their arguments.
//!
//! Under `--json` such a command writes one JSON document on stdout when it
//! succeeds. When it fails, stdout is empty and stderr carries one JSON error
//! envelope, the shape every component of the Surreal CLI uses:
//!
//! ```json
//! {"kind":"usage","message":"unexpected argument '--bogus' found","status":null,"code":null,
//!  "request_id":null,"hint":"See the usage with:","command":"surreal version --help",
//!  "docs":null,"retry_after_secs":null,"exit_code":2}
//! ```
//!
//! Every key is present, `null` when unknown. A failure may add keys of its own,
//! such as `validate`'s per-file `files`.

use std::ffi::OsString;
use std::fmt;
use std::io::Write;
use std::process::ExitCode;

use serde::Serialize;
use serde_json::{Map, Value, json};

/// The help text of `--json`, shared by every component of the Surreal CLI.
pub(crate) const HELP: &str = "Emit machine-readable JSON on stdout";

/// The id of [`JsonArg`]'s argument, distinct from `sql`'s own `--json`,
/// which selects the REPL's output format.
const ID: &str = "json_document";

/// `--json`, flattened into the arguments of each command that returns data.
/// Taking it is what makes a command report through this module.
#[derive(clap::Args, Debug, Clone, Copy)]
pub(crate) struct JsonArg {
	#[arg(id = ID, help = HELP, short = 'j', long = "json")]
	pub(crate) json: bool,
}

/// Whether `command` takes [`JsonArg`].
pub(crate) fn takes_json(command: &clap::Command) -> bool {
	command.get_arguments().any(|arg| arg.get_id() == ID)
}

/// Whether `matches`, one command's parsed arguments, were given `--json`.
pub(crate) fn given(matches: &clap::ArgMatches) -> bool {
	matches!(matches.try_get_one::<bool>(ID), Ok(Some(true)))
}

/// The exit code of a usage error.
pub(crate) const USAGE_EXIT: u8 = 2;

/// The exit code of every other failure.
pub(crate) const FAILURE_EXIT: u8 = 1;

/// Whether `--json` or `-j` is among the unparsed arguments, up to a `--`.
///
/// Read from the raw arguments because a usage error comes from the parse that
/// would have read the flag.
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
	/// A server could not be reached, or did not answer.
	Network,
	/// The input was read, and is not valid.
	Invalid,
	/// Something the command needs does not exist.
	NotFound,
	/// Any other failure.
	Unknown,
}

impl Kind {
	/// The exit code of a failure of this kind.
	pub(crate) fn exit_code(self) -> u8 {
		match self {
			Kind::Usage => USAGE_EXIT,
			Kind::Network | Kind::Invalid | Kind::NotFound | Kind::Unknown => FAILURE_EXIT,
		}
	}
}

/// A failure under `--json`, with what its envelope says beyond the message.
///
/// A command returns it inside its [`anyhow::Error`]; any other error is
/// reported as kind `unknown`.
#[derive(Debug)]
pub(crate) struct Failure {
	/// The envelope's `kind`.
	pub(crate) kind: Kind,
	/// The envelope's `message`.
	pub(crate) message: String,
	/// Keys added to the envelope.
	pub(crate) extra: Map<String, Value>,
}

impl fmt::Display for Failure {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(&self.message)
	}
}

impl std::error::Error for Failure {}

impl Failure {
	/// A failure of `kind` with nothing beyond its message.
	pub(crate) fn new(kind: Kind, message: impl Into<String>) -> Self {
		Self {
			kind,
			message: message.into(),
			extra: Map::new(),
		}
	}
}

/// `err`, from reaching a server, as a failure of kind `network`.
pub(crate) fn network(err: impl Into<anyhow::Error>) -> anyhow::Error {
	Failure::new(Kind::Network, format!("{:#}", err.into())).into()
}

/// Write `value` as the one document on stdout, its keys in the order it
/// serialises them.
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

/// The envelope with every shared key, `extra` added.
fn envelope(
	kind: Kind,
	message: &str,
	command: Option<&str>,
	exit_code: u8,
	extra: Map<String, Value>,
) -> Value {
	let mut envelope = Map::new();
	envelope.insert("kind".into(), json!(kind));
	envelope.insert("message".into(), json!(message));
	envelope.insert("status".into(), Value::Null);
	envelope.insert("code".into(), Value::Null);
	envelope.insert("request_id".into(), Value::Null);
	envelope.insert("hint".into(), json!(command.as_ref().map(|_| "See the usage with:")));
	envelope.insert("command".into(), json!(command));
	envelope.insert("docs".into(), Value::Null);
	envelope.insert("retry_after_secs".into(), Value::Null);
	envelope.insert("exit_code".into(), json!(exit_code));
	envelope.extend(extra);
	Value::Object(envelope)
}

/// The kind of a command's failure: a [`Failure`]'s own, else `unknown`.
fn kind(err: &anyhow::Error) -> Kind {
	err.downcast_ref::<Failure>().map_or(Kind::Unknown, |failure| failure.kind)
}

/// The envelope for the failure `err` of `command`, the command's usage path.
/// A usage failure points at the command's help.
pub(crate) fn failure_envelope(err: &anyhow::Error, command: &str) -> Value {
	let kind = kind(err);
	let help = (kind == Kind::Usage).then(|| format!("{command} --help"));
	let (message, extra) = match err.downcast_ref::<Failure>() {
		Some(failure) => (failure.message.clone(), failure.extra.clone()),
		None => (format!("{err:#}"), Map::new()),
	};
	envelope(kind, &message, help.as_deref(), kind.exit_code(), extra)
}

/// Report the failure `err` of `command` as its envelope on stderr, and return
/// the exit code the envelope names.
pub(crate) fn report_failure(err: &anyhow::Error, command: &str) -> ExitCode {
	eprint(&failure_envelope(err, command));
	ExitCode::from(kind(err).exit_code())
}

/// The command line's command, by name, if it takes `--json` and the command
/// line asks for it.
///
/// The command is the first argument, up to a `--`, that names one of `root`'s
/// subcommands, by name or alias. It is read from the raw arguments for the
/// same reason as [`asks_for_json`], and because clap's error does not always
/// name the command it failed in.
pub(crate) fn json_command(argv: &[OsString], root: &clap::Command) -> Option<String> {
	if !asks_for_json(argv) {
		return None;
	}
	let command = argv
		.iter()
		.skip(1)
		.take_while(|arg| *arg != "--")
		.filter_map(|arg| arg.to_str())
		.filter(|arg| !arg.starts_with('-'))
		.find_map(|arg| root.find_subcommand(arg))?;
	takes_json(command).then(|| command.get_name().to_owned())
}

/// The envelope for clap's error `err` in `command`, the usage path of a
/// command that takes `--json`, or `None` for help and `--version`, which are
/// not errors.
///
/// The message is clap's first line, without its `error: `.
pub(crate) fn usage_envelope(err: &clap::Error, command: &str) -> Option<Value> {
	if !err.use_stderr() {
		return None;
	}
	let rendered = err.to_string();
	let first = rendered.lines().find(|line| !line.trim().is_empty()).unwrap_or("");
	let message = first.strip_prefix("error: ").unwrap_or(first);
	let command = format!("{command} --help");
	Some(envelope(Kind::Usage, message, Some(&command), USAGE_EXIT, Map::new()))
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
		assert!(asks_for_json(&argv(&["surreal", "version", "--json"])));
		assert!(asks_for_json(&argv(&["surreal", "-j", "version"])));
		assert!(!asks_for_json(&argv(&["surreal", "version"])));
		assert!(!asks_for_json(&argv(&["surreal", "validate", "--", "--json"])));
	}

	#[test]
	fn the_json_command_is_the_first_argument_naming_a_command() {
		use clap::Args;
		let root = clap::Command::new("surreal")
			.subcommand(JsonArg::augment_args(
				clap::Command::new("is-ready").visible_alias("isready"),
			))
			.subcommand(JsonArg::augment_args(clap::Command::new("version")))
			.subcommand(
				clap::Command::new("sql").arg(clap::Arg::new("json").long("json").num_args(0)),
			);
		let name = |args: &[&str]| json_command(&argv(args), &root);
		assert_eq!(name(&["surreal", "isready", "--json"]).as_deref(), Some("is-ready"));
		assert_eq!(name(&["surreal", "--json", "version", "--bogus"]).as_deref(), Some("version"));
		assert_eq!(name(&["surreal", "version"]), None);
		assert_eq!(name(&["surreal", "sql", "--ns", "version", "--json"]), None);
		assert_eq!(name(&["surreal", "--", "version", "--json"]), None);
		assert_eq!(name(&["surreal", "sql", "--json"]), None);
	}

	#[test]
	fn the_envelope_has_every_key() {
		let envelope = failure_envelope(&anyhow::anyhow!("boom"), "surreal version");
		let keys: Vec<_> = envelope.as_object().unwrap().keys().cloned().collect();
		let mut expected = vec![
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
		];
		expected.sort_unstable();
		let mut keys = keys;
		keys.sort_unstable();
		assert_eq!(keys, expected);
		assert_eq!(envelope["kind"], "unknown");
		assert_eq!(envelope["message"], "boom");
		assert_eq!(envelope["exit_code"], 1);
	}

	#[test]
	fn a_failure_names_its_kind_and_adds_its_keys() {
		let mut extra = Map::new();
		extra.insert("files".into(), json!([]));
		let err = anyhow::Error::new(Failure {
			kind: Kind::Network,
			message: "no route".into(),
			extra,
		});
		let envelope = failure_envelope(&err, "surreal version");
		assert_eq!(envelope["kind"], "network");
		assert_eq!(envelope["message"], "no route");
		assert_eq!(envelope["files"], json!([]));
		assert_eq!(envelope["command"], Value::Null);
		assert_eq!(envelope["exit_code"], 1);
	}

	#[test]
	fn kinds_are_snake_case() {
		assert_eq!(json!(Kind::NotFound), "not_found");
		assert_eq!(json!(Kind::Usage), "usage");
	}

	/// A command's usage failure exits 2 and points at the command's help, as
	/// clap's usage errors do.
	#[test]
	fn a_usage_failure_points_at_the_help() {
		let err = anyhow::Error::new(Failure::new(Kind::Usage, "bad pattern"));
		let envelope = failure_envelope(&err, "surreal db validate");
		assert_eq!(envelope["kind"], "usage");
		assert_eq!(envelope["hint"], "See the usage with:");
		assert_eq!(envelope["command"], "surreal db validate --help");
		assert_eq!(envelope["exit_code"], 2);
	}
}
