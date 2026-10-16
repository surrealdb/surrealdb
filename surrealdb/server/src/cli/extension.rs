//! The database CLI's `extension` command, and the spellings the extension
//! tooling has gone by (`ext`, `module`, `mod`): the tooling is the Surreal
//! CLI's `ext` component (`surreal ext`), so these only say how to get there,
//! naming the spelling that was typed. They build, sign, install and run
//! nothing, and always exit non-zero.

use std::process::ExitCode;

use clap::Args;

use crate::cli::launcher;

/// Whatever followed the command, help flags included. It is accepted so that
/// every old invocation reaches the explanation rather than a usage error or a
/// help page for a command that no longer exists, and has no effect.
#[derive(Args, Debug)]
#[allow(dead_code, reason = "accepted for compatibility; the values are never used")]
pub struct ExtensionCommandArguments {
	#[arg(trailing_var_arg = true, allow_hyphen_values = true, hide = true)]
	args: Vec<String>,
}

/// `spelling` is what was typed: `extension`, `ext`, `module` or `mod`.
pub fn init(_: ExtensionCommandArguments, spelling: &str) -> ExitCode {
	eprintln!("{}", message(launcher::invoked_as().as_deref(), spelling, cfg!(windows)));
	ExitCode::FAILURE
}

fn message(invoked_as: Option<&str>, spelling: &str, windows: bool) -> String {
	let install = launcher::install_command(windows);
	match invoked_as {
		Some(invoked_as) => format!(
			"`{invoked_as} {spelling}` has moved to `surreal ext`. Run `surreal ext …` instead."
		),
		// `surreal ext` is already the new spelling; only where it runs moved.
		None if spelling == "ext" => format!(
			"`{} ext` has moved to the Surreal CLI. Install it with: {install}\nthen run `surreal ext …`.",
			launcher::BIN_NAME
		),
		None => format!(
			"`{} {spelling}` has moved to `surreal ext` in the Surreal CLI. Install it with: {install}\nthen run `surreal ext …`.",
			launcher::BIN_NAME
		),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn message_through_the_launcher() {
		assert_eq!(
			message(Some("surreal db"), "extension", false),
			"`surreal db extension` has moved to `surreal ext`. Run `surreal ext …` instead."
		);
		assert_eq!(
			message(Some("surreal db"), "extension", true),
			message(Some("surreal db"), "extension", false)
		);
	}

	#[test]
	fn message_names_the_spelling_typed() {
		for spelling in ["extension", "ext", "module", "mod"] {
			assert_eq!(
				message(Some("surreal db"), spelling, false),
				format!(
					"`surreal db {spelling}` has moved to `surreal ext`. Run `surreal ext …` instead."
				)
			);
		}
		assert_eq!(
			message(None, "ext", false),
			"`surreal ext` has moved to the Surreal CLI. Install it with: curl -sSf https://download.surrealdb.com/cli/launcher/install.sh | sh
then run `surreal ext …`."
		);
	}

	#[test]
	fn message_standalone_on_unix() {
		assert_eq!(
			message(None, "module", false),
			"`surreal module` has moved to `surreal ext` in the Surreal CLI. Install it with: curl -sSf https://download.surrealdb.com/cli/launcher/install.sh | sh
then run `surreal ext …`."
		);
	}

	#[test]
	fn message_standalone_on_windows() {
		assert_eq!(
			message(None, "extension", true),
			"`surreal extension` has moved to `surreal ext` in the Surreal CLI. Install it with: irm https://download.surrealdb.com/cli/launcher/install.ps1 | iex
then run `surreal ext …`."
		);
	}
}
