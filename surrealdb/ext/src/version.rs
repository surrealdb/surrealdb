//! The version: `--version` (`-v`, and the hidden `-V`) on the root command,
//! with `--json` (`-j`) for it as a document.

use std::io::Write;
use std::sync::LazyLock;

use clap::{Arg, ArgAction, Command};
use serde_json::json;

/// This component's version: the database's, as the two are released together.
///
/// `SURREAL_BUILD_VERSION` and `SURREAL_BUILD_METADATA`, read at compile time,
/// override it the same way they do for the database.
pub static PKG_VERSION: LazyLock<String> = LazyLock::new(|| {
	let version = option_env!("SURREAL_BUILD_VERSION")
		.filter(|v| !v.trim().is_empty())
		.unwrap_or(env!("CARGO_PKG_VERSION"));
	match option_env!("SURREAL_BUILD_METADATA") {
		Some(metadata) if !metadata.trim().is_empty() => format!("{version}+{metadata}"),
		_ => version.to_owned(),
	}
});

/// What `--version` prints: `<version> for <os> on <arch>` and nothing else.
///
/// It starts with [`PKG_VERSION`], which is what the Surreal CLI reads to learn
/// which version is installed.
pub static RELEASE: LazyLock<String> = LazyLock::new(|| {
	format!("{} for {} on {}", *PKG_VERSION, std::env::consts::OS, std::env::consts::ARCH)
});

/// The id of `--version`.
const FLAG: &str = "version";

/// The id of `--json`, which only goes with `--version`.
const JSON_FLAG: &str = "json";

/// `command` with `-v`/`--version` (`-V` hidden) in place of clap's own, and
/// `-j`/`--json` to print the version as a document.
///
/// The flag is clap's [`ArgAction::Version`], so it ends the parse wherever it
/// appears among the root's arguments and needs no command; the parse's
/// [`clap::error::ErrorKind::DisplayVersion`] error is the cue to [`print`].
pub(crate) fn with_flag(command: Command) -> Command {
	command
		.disable_version_flag(true)
		.arg(
			Arg::new(FLAG)
				.short('v')
				.long("version")
				.short_alias('V')
				.action(ArgAction::Version)
				.help("Print version"),
		)
		.arg(
			Arg::new(JSON_FLAG)
				.short('j')
				.long("json")
				.action(ArgAction::SetTrue)
				.requires(FLAG)
				.help(crate::json::HELP),
		)
}

/// The version document: `arch`, `os` and `version`, in that order.
pub(crate) fn document() -> serde_json::Value {
	json!({
		"arch": std::env::consts::ARCH,
		"os": std::env::consts::OS,
		"version": *PKG_VERSION,
	})
}

/// Print [`RELEASE`], or the document when `json`, on stdout, and return the
/// exit code.
pub(crate) fn print(json: bool) -> i32 {
	let mut stdout = std::io::stdout().lock();
	let written = if json {
		serde_json::to_writer_pretty(&mut stdout, &document())
			.map_err(std::io::Error::from)
			.and_then(|()| writeln!(stdout))
	} else {
		writeln!(stdout, "{}", *RELEASE)
	};
	match written.and_then(|()| stdout.flush()) {
		Ok(()) => 0,
		Err(_) => 1,
	}
}
