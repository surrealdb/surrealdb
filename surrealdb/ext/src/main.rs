//! `surreal-ext`: the Surreal CLI's `ext` component, which develops, builds,
//! signs, inspects and runs Surrealism extensions, and installs them into a
//! database.
//!
//! The Surreal CLI runs this executable as `surreal ext` and sets
//! [`INVOKED_AS_ENV`] to that command line, which is how the executable names
//! itself in usage and in the commands it suggests. The Surreal CLI installs and
//! upgrades this executable, so it has no self-install, self-upgrade or
//! update-check logic of its own.

// TODO: Remove once this crate no longer needs it; see `absolute_paths` in the workspace lints.
#![allow(clippy::absolute_paths)]

mod build;
mod config_cmd;
mod connect;
mod host;
mod info;
mod init_cmd;
mod install;
mod json;
mod run;
mod sig;
#[cfg(feature = "sign")]
mod sign;
/// Without `sign`, only the `[signer]` checks the installer shares with `sign`;
/// signing itself is not built.
#[cfg(not(feature = "sign"))]
#[allow(dead_code)]
mod sign {
	mod archive;
	pub(crate) mod signer;
}
mod snapshot;
mod version;
mod wasm_target;

use std::ffi::OsString;
use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::Result;
use clap::error::ErrorKind;
use clap::{CommandFactory, FromArgMatches, Parser, Subcommand};
use surrealdb_expr::parser;
use tokio::runtime::Builder;

/// Set by the Surreal CLI to the command it ran this executable as.
const INVOKED_AS_ENV: &str = "SURREAL_INVOKED_AS";

/// The executable's own name, used when [`INVOKED_AS_ENV`] is not set.
const BIN_NAME: &str = "surreal-ext";

/// Stack size for the runtime's worker threads, which parse SurrealQL values
/// read from stdin. The parser recurses, and frames are larger in debug builds.
const STACK_SIZE: usize = if cfg!(debug_assertions) {
	20 * 1024 * 1024
} else {
	10 * 1024 * 1024
};

#[derive(Debug, Parser)]
#[command(
	name = BIN_NAME,
	bin_name = BIN_NAME,
	about = "Develop, build, inspect and run Surrealism WASM modules",
	version = version::RELEASE.as_str(),
	arg_required_else_help = true,
)]
struct Cli {
	#[command(subcommand)]
	command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
	/// Initialize a new Surrealism module project
	Init {
		/// Non-interactive mode (requires --org)
		#[arg(long)]
		headless: bool,

		/// Organisation name
		#[arg(long)]
		org: Option<String>,

		/// Module name (defaults to directory name)
		#[arg(long)]
		name: Option<String>,

		/// Path to create the project (defaults to current directory)
		#[arg(value_name = "PATH")]
		path: Option<PathBuf>,
	},

	/// Run a function with arguments
	Run {
		/// Arguments passed to function (repeatable)
		#[arg(long = "arg", value_parser = parse_value)]
		args: Vec<surrealdb_types::Value>,

		/// The value of a name the package declares under [bindings]:
		/// NAME=VALUE, a SurrealQL value (repeatable). A name left out reads
		/// as NONE.
		#[arg(long = "bind", value_name = "NAME=VALUE", value_parser = parse_binding)]
		bindings: Vec<(String, surrealdb_types::Value)>,

		/// Required name
		#[arg(long)]
		fnc: Option<String>,

		/// Path to WASM file
		#[arg(value_name = "FILE")]
		file: PathBuf,
	},

	/// Show the function signature
	Sig {
		/// Required name
		#[arg(long)]
		fnc: Option<String>,

		/// Path to WASM file
		#[arg(value_name = "FILE")]
		file: PathBuf,

		#[command(flatten)]
		json: json::JsonArg,
	},

	/// Show the module information
	Info {
		/// Path to WASM file
		#[arg(value_name = "FILE")]
		file: PathBuf,

		#[command(flatten)]
		json: json::JsonArg,
	},

	/// Build a WASM module
	Build {
		/// Build in debug mode (default is release)
		#[arg(long)]
		debug: bool,

		/// Output file path or filename
		#[arg(short = 'o', long)]
		out: Option<PathBuf>,

		/// Path to source directory (defaults to current directory)
		#[arg(value_name = "SOURCE_PATH")]
		path: Option<PathBuf>,
	},

	/// Sign a .surli archive: Sigstore keyless from CI, an SSH key, or an imported bundle
	#[cfg(feature = "sign")]
	Sign(sign::SignArgs),

	/// Read and change the project's package.toml
	Config(config_cmd::ConfigArgs),

	/// Install an extension into a database: inspect, approve, define
	#[command(long_about = install::INSTALL_HELP)]
	Install(install::InstallArgs),

	/// Upgrade an installed extension: compare signer and capabilities, redefine
	#[command(long_about = install::UPGRADE_HELP)]
	Upgrade(install::UpgradeArgs),
}

/// Custom parser for `surrealdb_types::Value`
pub(crate) fn parse_value(s: &str) -> Result<surrealdb_types::Value, String> {
	parser::value(s).map_err(|e| format!("Invalid value: {e}"))
}

/// `NAME=VALUE` from `run --bind`.
fn parse_binding(s: &str) -> Result<(String, surrealdb_types::Value), String> {
	let (name, value) = s
		.split_once('=')
		.ok_or_else(|| "--bind takes NAME=VALUE, as in --bind 'api_key=\"k\"'".to_owned())?;
	let name = name.trim();
	if name.is_empty() {
		return Err("--bind: the name is empty".to_owned());
	}
	Ok((name.to_owned(), parse_value(value.trim())?))
}

/// The name this executable goes by: the Surreal CLI's command line when it
/// ran this executable (`surreal ext`), else [`BIN_NAME`]. An empty or blank
/// [`INVOKED_AS_ENV`] counts as unset.
pub(crate) fn bin_name() -> String {
	std::env::var(INVOKED_AS_ENV)
		.ok()
		.map(|v| v.trim().to_owned())
		.filter(|v| !v.is_empty())
		.unwrap_or_else(|| BIN_NAME.to_owned())
}

fn main() -> ExitCode {
	let (cli, json) = parse_args(&bin_name(), std::env::args_os().collect());
	let runtime = Builder::new_multi_thread().enable_all().thread_stack_size(STACK_SIZE).build();
	let runtime = match runtime {
		Ok(runtime) => runtime,
		Err(e) => {
			let e = anyhow::Error::from(e).context("failed to start the async runtime");
			return fail(&e, json.as_deref());
		}
	};
	match runtime.block_on(execute(cli.command)) {
		Ok(()) => ExitCode::SUCCESS,
		Err(e) => fail(&e, json.as_deref()),
	}
}

/// Report `err` as the JSON error envelope when the command line asked for
/// JSON (`json` is then the command's usage path), else as text.
fn fail(err: &anyhow::Error, json: Option<&str>) -> ExitCode {
	match json {
		Some(command) => json::report_failure(err, command),
		None => {
			eprintln!("Error: {err:?}");
			ExitCode::FAILURE
		}
	}
}

async fn execute(command: Command) -> Result<()> {
	match command {
		Command::Init {
			headless,
			org,
			name,
			path,
		} => init_cmd::init(path, org, name, headless).await,
		Command::Run {
			args,
			bindings,
			fnc,
			file,
		} => run::init(file, fnc, args, bindings).await,
		Command::Sig {
			fnc,
			file,
			json,
		} => sig::init(file, fnc, json.json).await,
		Command::Info {
			file,
			json,
		} => info::init(file, json.json).await,
		Command::Build {
			debug,
			out,
			path,
		} => build::init(path, out, debug).await,
		#[cfg(feature = "sign")]
		Command::Sign(args) => sign::init(args).await,
		Command::Config(args) => config_cmd::init(args).await,
		Command::Install(args) => install::install(args).await,
		Command::Upgrade(args) => install::upgrade(args).await,
	}
}

/// Parses the command line, naming the executable `bin_name` in usage and help.
/// When the command line asks for JSON, it also returns the usage path of the
/// command it names, such as `surreal ext info`, for the error envelope.
///
/// `--version` (`-v`, `-V`) prints the version and exits 0, whether or not a
/// command follows; with `--json` (`-j`), before or after it, it prints the
/// version document.
///
/// With `--json` (`-j`) among the arguments, a usage error is the JSON error
/// envelope on stderr, exiting 2, as in every component of the Surreal CLI.
/// Help stays clap's, on stdout.
fn parse_args(bin_name: &str, argv: Vec<OsString>) -> (Cli, Option<String>) {
	let mut command = command(bin_name.to_owned());
	command.build();
	let json = json::asks_for_json(&argv).then(|| json::command_path(&argv, &command, bin_name));
	let exit = |err: clap::Error| -> ! {
		if err.kind() == ErrorKind::DisplayVersion {
			std::process::exit(version::print(json.is_some()));
		}
		if let Some(command) = &json {
			json::exit_on_usage_error(&err, command);
		}
		err.exit()
	};
	let mut matches = command.try_get_matches_from_mut(argv).unwrap_or_else(|e| exit(e));
	let cli =
		Cli::from_arg_matches_mut(&mut matches).unwrap_or_else(|e| exit(e.format(&mut command)));
	(cli, json)
}

/// The clap command, named `bin_name` in usage and help.
fn command(bin_name: String) -> clap::Command {
	hide_env_values(version::with_flag(Cli::command().bin_name(bin_name)))
}

/// `command` with no argument's environment value shown in help, in it or any
/// command under it: the variables hold credentials.
fn hide_env_values(command: clap::Command) -> clap::Command {
	command.mut_args(|arg| arg.hide_env_values(true)).mut_subcommands(hide_env_values)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn usage(bin_name: &str, subcommand: &str) -> String {
		let mut command = command(bin_name.to_owned());
		command.build();
		let subcommand = command.find_subcommand_mut(subcommand).unwrap();
		subcommand.render_usage().to_string()
	}

	#[test]
	fn command_is_well_formed() {
		command(BIN_NAME.to_owned()).debug_assert();
	}

	#[test]
	fn usage_names_the_executable() {
		assert!(usage(BIN_NAME, "build").starts_with("Usage: surreal-ext build "));
	}

	#[test]
	fn usage_names_the_launcher_command() {
		assert!(usage("surreal ext", "build").starts_with("Usage: surreal ext build "));
		assert!(usage("surreal ext", "run").starts_with("Usage: surreal ext run "));
	}

	/// The version is `<version> for <os> on <arch>` and nothing else.
	#[test]
	fn the_version_is_the_release_alone() {
		semver::Version::parse(&version::PKG_VERSION).unwrap();
		assert_eq!(
			*version::RELEASE,
			format!(
				"{} for {} on {}",
				*version::PKG_VERSION,
				std::env::consts::OS,
				std::env::consts::ARCH
			)
		);
	}

	/// The document's keys are `arch`, `os` and `version`, in that order.
	#[test]
	fn the_version_document_keys_are_in_order() {
		assert_eq!(
			serde_json::to_string(&version::document()).unwrap(),
			format!(
				r#"{{"arch":"{}","os":"{}","version":"{}"}}"#,
				std::env::consts::ARCH,
				std::env::consts::OS,
				*version::PKG_VERSION
			)
		);
	}

	fn parse_error(args: &[&str]) -> clap::Error {
		command(BIN_NAME.to_owned()).try_get_matches_from(args).expect_err("the parse should stop")
	}

	/// `-v`, `--version` and the hidden `-V` are the version flag, with or
	/// without `--json` on either side, and need no command.
	#[test]
	fn every_spelling_of_the_version_flag_displays_the_version() {
		for args in [
			&["surreal-ext", "-v"][..],
			&["surreal-ext", "-V"],
			&["surreal-ext", "--version"],
			&["surreal-ext", "--version", "--json"],
			&["surreal-ext", "--json", "--version"],
			&["surreal-ext", "-v", "-j"],
			&["surreal-ext", "-j", "-v"],
		] {
			assert_eq!(parse_error(args).kind(), ErrorKind::DisplayVersion, "{args:?}");
		}
	}

	/// `--json` goes with `--version` and nothing else.
	#[test]
	fn json_needs_the_version_flag() {
		for args in [&["surreal-ext", "--json"][..], &["surreal-ext", "--json", "info", "x.surli"]]
		{
			let err = parse_error(args);
			assert!(err.use_stderr(), "{args:?} should be a usage error: {err}");
		}
	}

	/// `version` is not a command; the version is the flag's alone.
	#[test]
	fn version_is_not_a_command() {
		let err = parse_error(&["surreal-ext", "version"]);
		assert_eq!(err.kind(), ErrorKind::InvalidSubcommand);
	}

	/// Help never shows the value of an environment variable: every argument
	/// bound to one, in every command, hides it.
	#[test]
	fn every_argument_with_an_environment_variable_hides_its_value() {
		fn walk(
			command: &clap::Command,
			path: &str,
			seen: &mut Vec<String>,
			shown: &mut Vec<String>,
		) {
			for arg in command.get_arguments() {
				let Some(env) = arg.get_env() else {
					continue;
				};
				let env = env.to_string_lossy().into_owned();
				if !arg.is_hide_env_values_set() {
					shown.push(format!("{path} --{} ({env})", arg.get_id()));
				}
				seen.push(env);
			}
			for subcommand in command.get_subcommands() {
				walk(subcommand, &format!("{path} {}", subcommand.get_name()), seen, shown);
			}
		}
		let mut command = command(BIN_NAME.to_owned());
		command.build();
		let (mut seen, mut shown) = (Vec::new(), Vec::new());
		walk(&command, BIN_NAME, &mut seen, &mut shown);
		assert!(shown.is_empty(), "these show their variable's value in help: {shown:#?}");
		for credential in ["SURREAL_USER", "SURREAL_PASS", "SURREAL_TOKEN"] {
			assert!(seen.iter().any(|env| env == credential), "{credential} was not reached");
		}
	}

	/// The commands that return data take `--json`, described in the words
	/// every Surreal CLI component uses.
	#[test]
	fn the_data_commands_take_json_with_the_shared_help() {
		let mut command = command(BIN_NAME.to_owned());
		command.build();
		for name in ["info", "sig"] {
			let subcommand = command.find_subcommand(name).unwrap();
			let arg = subcommand
				.get_arguments()
				.find(|arg| arg.get_long() == Some("json"))
				.unwrap_or_else(|| panic!("`{name}` takes no --json"));
			assert_eq!(arg.get_short(), Some('j'), "{name}");
			assert_eq!(arg.get_help().map(ToString::to_string).as_deref(), Some(json::HELP));
		}
	}

	#[test]
	fn parses_values_as_surrealql() {
		assert_eq!(parse_value("1").unwrap(), surrealdb_types::Value::from_t(1i64));
		assert!(parse_value("{ a: 1, b: [true, 'two'] }").is_ok());
		assert!(parse_value("{ a: ").is_err());
	}
}
