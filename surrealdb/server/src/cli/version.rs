//! The version, as `--version` (`-v`, and the hidden `-V`) on the root command
//! and as the `version` command.
//!
//! The text is [`RELEASE`], `<version> for <os> on <arch>` and nothing else: the
//! Surreal CLI learns which version is installed from its first token. Under
//! `--json` it is one [`Document`].

use anyhow::Result;
use clap::{Arg, ArgAction, Args, Command};
use serde::Serialize;
use surrealdb::engine::any::connect;

use crate::cli::abstraction::OptionalDatabaseConnectionArguments;
use crate::cli::json;
use crate::cnf::PKG_VERSION;
use crate::env::RELEASE;

/// The id of the root command's `--version`.
const FLAG: &str = "version";

/// The id of the root command's `--json`, which only goes with `--version`.
const JSON_FLAG: &str = "version_json";

#[derive(Args, Debug)]
pub struct VersionCommandArguments {
	#[command(flatten)]
	conn: OptionalDatabaseConnectionArguments,
	#[command(flatten)]
	json: json::JsonArg,
}

/// The version document. The keys are in this order, and `server` is present
/// only when a server was asked for its version.
#[derive(Debug, Serialize)]
struct Document {
	arch: &'static str,
	os: &'static str,
	version: &'static str,
	#[serde(skip_serializing_if = "Option::is_none")]
	server: Option<Server>,
}

#[derive(Debug, Serialize)]
struct Server {
	version: String,
}

impl Document {
	/// This executable's version, with no server.
	fn local() -> Self {
		Self {
			arch: std::env::consts::ARCH,
			os: std::env::consts::OS,
			version: PKG_VERSION.as_str(),
			server: None,
		}
	}
}

/// `command` with the root's `-v`/`--version` (`-V` hidden) in place of clap's
/// own, and `-j`/`--json` to print it as the document.
///
/// The flag is clap's [`ArgAction::Version`], so it ends the parse wherever it
/// appears and needs no command; [`exit_with_flag`] prints what it reports.
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
				.help(json::HELP),
		)
}

/// Whether clap's `err` is the root's `--version` rather than a subcommand's
/// own. `root_version` is the root command's [`Command::render_version`].
pub(crate) fn is_flag(err: &clap::Error, root_version: &str) -> bool {
	err.kind() == clap::error::ErrorKind::DisplayVersion && err.render().to_string() == root_version
}

/// Print what the root's `--version` reports, [`RELEASE`] or, when `json`, the
/// document, and exit.
pub(crate) fn exit_with_flag(json: bool) -> ! {
	if json {
		let code = match json::print(&Document::local()) {
			Ok(()) => 0,
			Err(_) => json::FAILURE_EXIT,
		};
		std::process::exit(code.into());
	}
	println!("{}", *RELEASE);
	std::process::exit(0)
}

pub async fn init(
	VersionCommandArguments {
		conn: OptionalDatabaseConnectionArguments {
			endpoint,
		},
		json: json::JsonArg {
			json,
		},
	}: VersionCommandArguments,
) -> Result<()> {
	if json {
		return json::print(&version_document(endpoint).await?);
	}
	// Print server version if endpoint supplied else CLI version
	if let Some(e) = endpoint {
		// Print remote server version
		println!("{}", get_server_version_string(e).await?);
	} else {
		// Print local CLI version
		println!("{}", *RELEASE);
	}
	// All ok
	Ok(())
}

/// `version --json`: the document `--version --json` prints, with the server's
/// version under `server` when `endpoint` is given.
async fn version_document(endpoint: Option<String>) -> Result<Document> {
	let mut document = Document::local();
	if let Some(endpoint) = endpoint {
		let version = get_server_version_string(endpoint).await.map_err(json::network)?;
		document.server = Some(Server {
			version,
		});
	}
	Ok(document)
}

async fn get_server_version_string(endpoint: String) -> Result<String> {
	// Connect to the database engine
	let client = connect(endpoint).await?;
	// Query database version info
	let server_version = client.version().await?;
	// Convert version info to formatted string
	Ok(server_version.to_string())
}

#[cfg(test)]
mod tests {
	use super::*;

	/// The text is the version, the OS and the architecture, and nothing else.
	#[test]
	fn the_text_is_the_release_alone() {
		assert_eq!(
			*RELEASE,
			format!("{} for {} on {}", *PKG_VERSION, std::env::consts::OS, std::env::consts::ARCH)
		);
	}

	/// The document's keys are `arch`, `os` and `version`, in that order, and
	/// `server` follows them when a server was asked.
	#[test]
	fn the_document_keys_are_in_order() {
		let text = serde_json::to_string(&Document::local()).unwrap();
		assert_eq!(
			text,
			format!(
				r#"{{"arch":"{}","os":"{}","version":"{}"}}"#,
				std::env::consts::ARCH,
				std::env::consts::OS,
				*PKG_VERSION
			)
		);
		let document = Document {
			server: Some(Server {
				version: "surrealdb-3.0.0".into(),
			}),
			..Document::local()
		};
		let text = serde_json::to_string(&document).unwrap();
		assert!(text.ends_with(r#","server":{"version":"surrealdb-3.0.0"}}"#), "{text}");
	}
}
