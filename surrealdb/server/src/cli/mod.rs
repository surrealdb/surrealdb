#![allow(deprecated)]

pub(crate) mod abstraction;
mod config;
mod export;
mod extension;
mod fix;
mod import;
mod isready;
mod json;
mod launcher;
#[cfg(feature = "mcp")]
pub(crate) mod mcp;
mod ml;
#[cfg(feature = "cli")]
mod sql;
mod start;
mod upgrade;
mod validate;
pub(crate) mod validator;
mod version;

use std::ffi::OsString;
use std::process::ExitCode;

use clap::{CommandFactory, FromArgMatches, Parser, Subcommand, ValueEnum};
pub use config::{Config, ConfigCheck};
use export::ExportCommandArguments;
use extension::ExtensionCommandArguments;
use fix::FixCommandArguments;
use import::ImportCommandArguments;
use isready::IsReadyCommandArguments;
#[cfg(feature = "mcp")]
use mcp::McpCommandArguments;
use ml::MlCommand;
#[cfg(feature = "cli")]
use sql::SqlCommandArguments;
use start::StartCommandArguments;
use surrealdb_core::kvs::TransactionBuilderFactory;
use upgrade::UpgradeCommandArguments;
use validate::ValidateCommandArguments;
use validator::parser::tracing::{CustomFilter, CustomFilterParser};
use version::VersionCommandArguments;

#[cfg(debug_assertions)]
use crate::cnf::DEBUG_BUILD_WARNING;
use crate::env::RELEASE;
use crate::ntw::RouterFactory;

/// The one line at the top of `--help`. The banner and the project links belong
/// to the Surreal CLI, which runs this executable as its `db` component.
const ABOUT: &str = "Start, query, import and export SurrealDB from the command line";

#[derive(Parser, Debug)]
#[command(name = "SurrealDB command-line interface and server", bin_name = "surreal")]
#[command(version = RELEASE.as_str(), about = ABOUT)]
#[command(arg_required_else_help = true)]
struct Cli {
	//
	// Commands
	#[command(subcommand)]
	command: Commands,
	//
	// Logging
	#[arg(help = "The logging level for the command-line tool", help_heading = "Logging")]
	#[arg(env = "SURREAL_LOG", short = 'l', long = "log")]
	#[arg(global = true)]
	#[arg(default_value = "info")]
	#[arg(value_parser = CustomFilterParser::new())]
	log: CustomFilter,
	#[arg(help = "The format for terminal log output", help_heading = "Logging")]
	#[arg(env = "SURREAL_LOG_FORMAT", long = "log-format")]
	#[arg(global = true)]
	#[arg(default_value = "text")]
	#[arg(value_enum)]
	log_format: LogFormat,
	#[arg(help = "Send logs to the specified host:port", help_heading = "Logging")]
	#[arg(env = "SURREAL_LOG_SOCKET", long = "log-socket")]
	#[arg(global = true)]
	log_socket: Option<String>,
	//
	// Log level overrides
	#[arg(help = "Override the logging level for file output", help_heading = "Logging")]
	#[arg(env = "SURREAL_LOG_FILE_LEVEL", long = "log-file-level")]
	#[arg(global = true)]
	#[arg(value_parser = CustomFilterParser::new())]
	log_file_level: Option<CustomFilter>,
	#[arg(help = "Override the logging level for OpenTelemetry output", help_heading = "Logging")]
	#[arg(env = "SURREAL_LOG_OTEL_LEVEL", long = "log-otel-level")]
	#[arg(global = true)]
	#[arg(value_parser = CustomFilterParser::new())]
	log_otel_level: Option<CustomFilter>,
	#[arg(help = "Override the logging level for unix socket output", help_heading = "Logging")]
	#[arg(env = "SURREAL_LOG_SOCKET_LEVEL", long = "log-socket-level")]
	#[arg(global = true)]
	#[arg(value_parser = CustomFilterParser::new())]
	log_socket_level: Option<CustomFilter>,
	//
	// Log socket options
	#[arg(help = "The format for socket output", help_heading = "Logging")]
	#[arg(env = "SURREAL_LOG_SOCKET_FORMAT", long = "log-socket-format")]
	#[arg(global = true)]
	#[arg(default_value = "text")]
	#[arg(value_enum)]
	log_socket_format: LogFormat,
	//
	// Log file options
	#[arg(help = "Whether to enable log file output", help_heading = "Logging")]
	#[arg(env = "SURREAL_LOG_FILE_ENABLED", long = "log-file-enabled")]
	#[arg(global = true)]
	#[arg(default_value_t = false)]
	log_file_enabled: bool,
	#[arg(help = "The directory where log files will be stored", help_heading = "Logging")]
	#[arg(env = "SURREAL_LOG_FILE_PATH", long = "log-file-path")]
	#[arg(global = true)]
	#[arg(default_value = "logs")]
	log_file_path: String,
	#[arg(help = "The name of the log file", help_heading = "Logging")]
	#[arg(env = "SURREAL_LOG_FILE_NAME", long = "log-file-name")]
	#[arg(global = true)]
	#[arg(default_value = "surrealdb.log")]
	log_file_name: String,
	#[arg(help = "The format for log file output", help_heading = "Logging")]
	#[arg(env = "SURREAL_LOG_FILE_FORMAT", long = "log-file-format")]
	#[arg(global = true)]
	#[arg(default_value = "text")]
	#[arg(value_enum)]
	log_file_format: LogFormat,
	#[arg(help = "The log file rotation interval", help_heading = "Logging")]
	#[arg(env = "SURREAL_LOG_FILE_ROTATION", long = "log-file-rotation")]
	#[arg(global = true)]
	#[arg(default_value = "daily")]
	#[arg(value_enum)]
	log_file_rotation: LogFileRotation,
	//
	// Accepted so that invocations passing it parse. This executable never
	// checks online for a newer version; the Surreal CLI handles upgrades.
	#[arg(env = "SURREAL_ONLINE_VERSION_CHECK", long, hide = true)]
	#[arg(default_value_t = true)]
	#[allow(dead_code, reason = "accepted for compatibility; the value is never used")]
	online_version_check: bool,
}

#[expect(clippy::large_enum_variant)]
#[derive(Debug, Subcommand)]
enum Commands {
	#[command(about = "Start the database server")]
	Start(StartCommandArguments),
	/* Not implemented yet
	#[command(about = "Backup data to or from an existing database")]
	Backup(BackupCommandArguments),
	*/
	#[command(about = "Import a SurrealQL script into an existing database")]
	Import(ImportCommandArguments),
	#[command(about = "Export an existing database as a SurrealQL script")]
	Export(ExportCommandArguments),
	#[command(about = "Output the command-line tool and remote server version information")]
	Version(VersionCommandArguments),
	#[command(about = "Explain how to upgrade with the Surreal CLI", hide = true)]
	Upgrade(UpgradeCommandArguments),
	#[cfg(feature = "cli")]
	#[command(about = "Start an SQL REPL in your terminal with pipe support")]
	Sql(SqlCommandArguments),
	#[cfg(feature = "mcp")]
	#[command(about = "Start the MCP server over stdio for AI assistant integration")]
	Mcp(McpCommandArguments),
	#[command(subcommand, about = "Manage SurrealML models within an existing database")]
	Ml(MlCommand),
	// The extension tooling is the Surreal CLI's `ext` component. Each spelling
	// it has gone by is its own hidden stub, so the explanation names what was
	// typed. No help flag on any: `--help` reaches the explanation, not a help
	// page for a command that no longer does anything.
	#[command(
		about = "Explain how to build, sign and install Surrealism extensions, which the Surreal CLI now does",
		disable_help_flag = true,
		hide = true
	)]
	Extension(ExtensionCommandArguments),
	#[command(disable_help_flag = true, hide = true)]
	Ext(ExtensionCommandArguments),
	#[command(disable_help_flag = true, hide = true)]
	Module(ExtensionCommandArguments),
	#[command(name = "mod", disable_help_flag = true, hide = true)]
	Mod(ExtensionCommandArguments),
	#[command(
		about = "Check if the SurrealDB server is ready to accept connections",
		visible_alias = "isready"
	)]
	IsReady(IsReadyCommandArguments),
	#[command(about = "Validate SurrealQL query files")]
	Validate(ValidateCommandArguments),
	#[command(about = "Fix database storage issues")]
	Fix(FixCommandArguments),
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum LogFormat {
	Text,
	Json,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum LogFileRotation {
	Daily,
	Hourly,
	Never,
}

impl LogFileRotation {
	pub fn as_str(self) -> &'static str {
		match self {
			LogFileRotation::Daily => "daily",
			LogFileRotation::Hourly => "hourly",
			LogFileRotation::Never => "never",
		}
	}
}

/// CLI entrypoint.
///
/// Processes command-line arguments and dispatches to the appropriate subcommand handler.
///
/// # Parameters
/// - `composer`: A composer implementing the required traits for dependency injection
///
/// # Generic parameters
/// - `C`: A composer type that implements:
///   - `TransactionBuilderFactory` (builds datastore transactions, allowing embedders to pick a
///     storage backend)
///   - `RouterFactory` (constructs the HTTP router, allowing embedders to customize server routes)
///   - `ConfigCheck` (validates configuration before initialization)
pub async fn init<
	C: TransactionBuilderFactory + RouterFactory + ConfigCheck + crate::observe::ObservabilityProvider,
>(
	composer: C,
) -> ExitCode {
	// Enables ANSI code support on Windows
	#[cfg(windows)]
	nu_ansi_term::enable_ansi_support().ok();
	let argv: Vec<OsString> = std::env::args_os().collect();
	// Print debug mode warning, except where stdout is a JSON document
	#[cfg(debug_assertions)]
	if !json::asks_for_json(&argv) {
		println!("{DEBUG_BUILD_WARNING}");
	}
	// Parse the CLI arguments
	let (args, json) = parse_args(&launcher::bin_name(), argv);
	// `upgrade` prints its explanation and nothing else, so it runs before
	// logging is set up
	let args = match args {
		Cli {
			command: Commands::Upgrade(args),
			..
		} => return upgrade::init(args),
		Cli {
			command: Commands::Extension(args),
			..
		} => return extension::init(args, "extension"),
		Cli {
			command: Commands::Ext(args),
			..
		} => return extension::init(args, "ext"),
		Cli {
			command: Commands::Module(args),
			..
		} => return extension::init(args, "module"),
		Cli {
			command: Commands::Mod(args),
			..
		} => return extension::init(args, "mod"),
		args => args,
	};
	// Check if we are running the server
	let server = matches!(args.command, Commands::Start(_));
	// Under `--json`, stdout is the command's one document, so every log line
	// goes to stderr and a failure is reported as the JSON envelope
	// Initialize opentelemetry and logging
	let telemetry = crate::telemetry::builder()
		.with_log_level("info")
		.with_log_format(args.log_format)
		.with_stdout_logs(json.is_none())
		.with_filter(args.log.clone())
		.with_file_filter(args.log_file_level.clone())
		.with_otel_filter(args.log_otel_level.clone())
		.with_socket_filter(args.log_socket_level.clone())
		.with_socket(args.log_socket.clone())
		.with_socket_format(args.log_socket_format)
		.with_file_enabled(args.log_file_enabled)
		.with_file_path(Some(args.log_file_path.clone()))
		.with_file_name(Some(args.log_file_name.clone()))
		.with_file_format(args.log_file_format)
		.with_file_rotation(Some(args.log_file_rotation.as_str().to_string()));
	// Initialise telemetry: install the tracing subscriber and receive
	// the log guards plus the `ObservabilityRuntime` that carries every
	// provider observers register against.
	let crate::telemetry::TelemetryHandles {
		guards,
		runtime,
	} = telemetry.init().expect("Unable to configure logs");
	// Install the process-wide rustls crypto provider before any command can
	// open a TLS connection. Both the server and the remote client commands
	// (sql, import, export, isready, ...) rely on this; without it rustls 0.23
	// aborts the process on the first handshake.
	if let Err(e) = crate::tls::install_default_crypto_provider() {
		let code = match &json {
			Some(command) => json::report_failure(&e, command),
			None => {
				error!("{:?}", e);
				ExitCode::FAILURE
			}
		};
		runtime.shutdown();
		return code;
	}
	// After version warning we can run the respective command
	let output = match args.command {
		Commands::Start(args) => start::init::<C>(composer, args, runtime.clone()).await,
		Commands::Import(args) => import::init(args).await,
		Commands::Export(args) => export::init(args).await,
		Commands::Version(args) => version::init(args).await,
		#[cfg(feature = "cli")]
		Commands::Sql(args) => sql::init(args).await,
		#[cfg(feature = "mcp")]
		Commands::Mcp(args) => mcp::init::<C>(composer, args, runtime.clone()).await,
		Commands::Ml(args) => ml::init(args).await,
		Commands::IsReady(args) => isready::init(args).await,
		Commands::Validate(args) => validate::init(args).await,
		Commands::Fix(args) => fix::init::<C>(composer, args).await,
		Commands::Upgrade(_)
		| Commands::Extension(_)
		| Commands::Ext(_)
		| Commands::Module(_)
		| Commands::Mod(_) => {
			unreachable!("handled before logging is set up")
		}
	};
	// Flush every provider's batch processor so audit / slow-query
	// records and metric exports are written before the runtime is
	// dropped.
	runtime.shutdown();
	// Error and exit the program
	let res = if let Err(e) = output {
		// Output any error, and return failure
		match &json {
			Some(command) => json::report_failure(&e, command),
			None => {
				error!("{:?}", e);
				ExitCode::FAILURE
			}
		}
	} else {
		// Return success
		ExitCode::SUCCESS
	};
	// Drop the log guards
	for guard in guards {
		drop(guard);
	}
	// Final message
	if server {
		println!("Goodbye!");
	}
	res
}

/// Parses the command line, naming the executable `bin_name` in usage and help.
/// When the command was given `--json`, it also returns the command's usage
/// path, such as `surreal db validate`.
///
/// A bare invocation shows the help, whatever the environment holds. Clap's own
/// `arg_required_else_help` only does that when no argument has a value at all,
/// and an environment variable bound to one — `SURREAL_ONLINE_VERSION_CHECK`,
/// which some Surreal CLI versions set, or a user's `SURREAL_LOG` — gives it
/// one, which would make a bare `surreal db` a "requires a subcommand" error.
///
/// It runs as `--help` does, through clap, so it prints exactly what
/// `surreal help` prints, styled the same way, on stdout, and exits 0, as the
/// Surreal CLI itself does when run with nothing.
///
/// A usage error in a command that takes `--json`, with `--json` among the
/// arguments, is that command's JSON error envelope on stderr, exiting 2.
///
/// `--version` (`-v`, `-V`) prints the version and exits 0, wherever it appears
/// among the root's arguments and whether or not a command follows; with
/// `--json` (`-j`), before or after it, it prints the version document.
fn parse_args(bin_name: &str, argv: Vec<OsString>) -> (Cli, Option<String>) {
	let mut command = command(bin_name.to_owned());
	if is_bare(argv.iter().cloned()) {
		let argv0 = argv.first().cloned().unwrap_or_else(|| launcher::BIN_NAME.into());
		if let Err(help) = command.try_get_matches_from_mut([argv0, "--help".into()]) {
			help.exit();
		}
	}
	command.build();
	let root_version = command.render_version();
	let version_json = json::asks_for_json(&argv);
	let json_command = json::json_command(&argv, &command).map(|name| format!("{bin_name} {name}"));
	let exit = |err: clap::Error| -> ! {
		if version::is_flag(&err, &root_version) {
			version::exit_with_flag(version_json);
		}
		if let Some(json_command) = &json_command {
			json::exit_on_usage_error(&err, json_command);
		}
		err.exit()
	};
	let mut matches = command.try_get_matches_from_mut(argv).unwrap_or_else(|e| exit(e));
	let json = matches
		.subcommand()
		.filter(|(_, matches)| json::given(matches))
		.map(|(name, _)| format!("{bin_name} {name}"));
	let cli =
		Cli::from_arg_matches_mut(&mut matches).unwrap_or_else(|e| exit(e.format(&mut command)));
	(cli, json)
}

/// Whether the command line names nothing beyond the executable.
fn is_bare(args: impl IntoIterator<Item = std::ffi::OsString>) -> bool {
	args.into_iter().nth(1).is_none()
}

/// The clap command, named `bin_name` in usage and help.
fn command(bin_name: String) -> clap::Command {
	hide_env_values(version::with_flag(Cli::command().bin_name(bin_name)))
}

/// `command` with no argument's environment value shown in help, in it or any
/// command under it: the variables hold credentials and keys.
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
	fn only_the_executable_alone_is_bare() {
		assert!(is_bare(["surreal".into()]));
		assert!(!is_bare(["surreal".into(), "start".into()]));
		assert!(!is_bare(["surreal".into(), "--help".into()]));
	}

	/// One line at the top of the help, and no banner.
	#[test]
	fn help_opens_with_one_line() {
		let help = command(launcher::BIN_NAME.to_owned()).render_help().to_string();
		assert!(help.starts_with(ABOUT), "{help}");
		assert!(!help.contains("d8888b"), "help shows a banner: {help}");
	}

	/// The stubs left for `upgrade` and the extension tooling run, but are not
	/// advertised.
	#[test]
	fn the_redirecting_stubs_are_hidden() {
		let help = command(launcher::BIN_NAME.to_owned()).render_help().to_string();
		assert!(!help.contains("upgrade"), "{help}");
		assert!(!help.contains("extension"), "{help}");
		assert!(!help.contains(" ext "), "{help}");
		assert!(!help.contains("module"), "{help}");
		assert!(!help.contains(" mod "), "{help}");
	}

	#[test]
	fn usage_names_the_executable() {
		assert!(usage("surreal", "start").starts_with("Usage: surreal start "));
	}

	#[test]
	fn usage_names_the_launcher_command() {
		assert!(usage("surreal db", "start").starts_with("Usage: surreal db start "));
		assert!(usage("surreal db", "export").starts_with("Usage: surreal db export "));
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
		let mut command = command(launcher::BIN_NAME.to_owned());
		command.build();
		let (mut seen, mut shown) = (Vec::new(), Vec::new());
		walk(&command, launcher::BIN_NAME, &mut seen, &mut shown);
		assert!(shown.is_empty(), "these show their variable's value in help: {shown:#?}");
		for credential in ["SURREAL_USER", "SURREAL_PASS", "SURREAL_TOKEN", "SURREAL_KEY"] {
			assert!(seen.iter().any(|env| env == credential), "{credential} was not reached");
		}
	}

	/// The commands that take `--json` say so in the words every Surreal CLI
	/// component uses.
	#[test]
	fn json_flags_use_the_shared_help() {
		let mut command = command(launcher::BIN_NAME.to_owned());
		command.build();
		let mut taking = 0;
		for subcommand in command.get_subcommands().filter(|c| json::takes_json(c)) {
			let arg =
				subcommand.get_arguments().find(|arg| arg.get_long() == Some("json")).unwrap();
			assert_eq!(arg.get_help().map(ToString::to_string).as_deref(), Some(json::HELP));
			assert_eq!(arg.get_short(), Some('j'));
			taking += 1;
		}
		assert!(taking > 0);
	}

	/// Parse `args` with the root command, returning clap's error.
	fn parse_error(args: &[&str]) -> clap::Error {
		let mut command = command(launcher::BIN_NAME.to_owned());
		command.try_get_matches_from_mut(args).expect_err("the parse should stop")
	}

	fn root_version() -> String {
		let mut command = command(launcher::BIN_NAME.to_owned());
		command.build();
		command.render_version()
	}

	/// `-v`, `--version` and the hidden `-V` are the root's version flag, with
	/// or without `--json` on either side, and need no command.
	#[test]
	fn every_spelling_of_the_version_flag_is_the_root_flag() {
		let root_version = root_version();
		for args in [
			&["surreal", "-v"][..],
			&["surreal", "-V"],
			&["surreal", "--version"],
			&["surreal", "--version", "--json"],
			&["surreal", "--json", "--version"],
			&["surreal", "-v", "-j"],
			&["surreal", "-j", "-v"],
			&["surreal", "--log", "debug", "-v"],
		] {
			let err = parse_error(args);
			assert!(version::is_flag(&err, &root_version), "{args:?}: {err}");
		}
	}

	/// Help lists `-v, --version` and `-j, --json`, and not `-V`.
	#[test]
	fn help_lists_the_version_flag() {
		let help = command(launcher::BIN_NAME.to_owned()).render_help().to_string();
		assert!(help.contains("-v, --version"), "{help}");
		assert!(help.contains("-j, --json"), "{help}");
		assert!(!help.contains("-V"), "{help}");
	}

	/// The root's `--json` goes with `--version` and nothing else.
	#[test]
	fn the_root_json_flag_needs_the_version_flag() {
		let root_version = root_version();
		for args in [&["surreal", "--json"][..], &["surreal", "--json", "version"]] {
			let err = parse_error(args);
			assert!(err.use_stderr(), "{args:?} should be a usage error: {err}");
			assert!(!version::is_flag(&err, &root_version), "{args:?}");
		}
		let err = parse_error(&["surreal", "--json", "version"]);
		assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
	}

	/// `version` stays a command, and takes its own `--json`.
	#[test]
	fn version_is_a_command() {
		for args in [&["surreal", "version"][..], &["surreal", "version", "--json"]] {
			let parsed = command(launcher::BIN_NAME.to_owned()).try_get_matches_from(args);
			assert_eq!(parsed.unwrap().subcommand_name(), Some("version"), "{args:?}");
		}
	}

	#[test]
	fn command_is_well_formed() {
		command(launcher::BIN_NAME.to_owned()).debug_assert();
	}

	#[test]
	fn ignored_options_parse() {
		for args in [
			&["surreal", "upgrade"][..],
			&["surreal", "upgrade", "--nightly"],
			&["surreal", "upgrade", "--alpha"],
			&["surreal", "upgrade", "--beta"],
			&["surreal", "upgrade", "--version", "2.3.1"],
			&["surreal", "upgrade", "--dry-run"],
			&["surreal", "--online-version-check", "version"],
			&["surreal", "start", "--no-banner"],
			&["surreal", "module", "build", "--debug", "-o", "demo.surli", "surrealism/demo"],
			&["surreal", "module", "run", "--fnc", "can_drive", "--arg", "17", "demo.surli"],
			&["surreal", "module", "--version"],
			&["surreal", "mod", "info", "demo.surli"],
			&["surreal", "mod"],
			&["surreal", "extension", "install", "--trust-signer", "any", "f\"b:/x.surli\""],
			&["surreal", "ext", "sign", "--key", "release", "demo.surli"],
		] {
			let parsed = command(launcher::BIN_NAME.to_owned()).try_get_matches_from(args);
			assert!(parsed.is_ok(), "{args:?} should parse: {parsed:?}");
		}
	}

	/// `--help` under any spelling reaches the explanation instead of printing a
	/// help page for a command that no longer does anything.
	#[test]
	fn extension_help_reaches_the_explanation() {
		for args in [
			&["surreal", "extension", "--help"][..],
			&["surreal", "ext", "-h"],
			&["surreal", "module", "--help"],
			&["surreal", "module", "-h"],
			&["surreal", "mod", "build", "--help"],
		] {
			let mut command = command(launcher::BIN_NAME.to_owned());
			let mut matches = command.try_get_matches_from_mut(args).unwrap();
			let cli = Cli::from_arg_matches_mut(&mut matches).unwrap();
			assert!(
				matches!(
					cli.command,
					Commands::Extension(_)
						| Commands::Ext(_) | Commands::Module(_)
						| Commands::Mod(_)
				),
				"{args:?}"
			);
		}
	}

	/// Each spelling reaches its own stub, so the explanation names what was
	/// typed.
	#[test]
	fn each_spelling_is_told_apart() {
		let parse = |args: &[&str]| {
			let mut command = command(launcher::BIN_NAME.to_owned());
			let mut matches = command.try_get_matches_from_mut(args).unwrap();
			Cli::from_arg_matches_mut(&mut matches).unwrap().command
		};
		assert!(matches!(parse(&["surreal", "extension", "build"]), Commands::Extension(_)));
		assert!(matches!(parse(&["surreal", "ext", "build"]), Commands::Ext(_)));
		assert!(matches!(parse(&["surreal", "module", "build"]), Commands::Module(_)));
		assert!(matches!(parse(&["surreal", "mod", "build"]), Commands::Mod(_)));
	}
}
