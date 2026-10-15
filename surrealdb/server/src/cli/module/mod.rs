mod build;
mod config_cmd;
mod host;
mod info;
mod init_cmd;
mod run;
mod sig;
#[cfg(feature = "extension-sign")]
pub(crate) mod sign;
/// Without `extension-sign`, only the `[signer]` checks the installer shares
/// with `sign`; signing itself is not built.
#[cfg(not(feature = "extension-sign"))]
#[allow(dead_code)]
pub(crate) mod sign {
	mod archive;
	pub(crate) mod signer;
}
mod snapshot;
mod version;
mod wasm_target;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

use self::version::MODULE_RELEASE;

/// Arguments of `surreal extension`, which `surreal module` also names.
#[derive(Debug, Parser)]
#[command(
	name = "extension",
	display_name = "Surrealism command-line interface",
	about = "Build, inspect and run Surrealism extensions",
	version = MODULE_RELEASE.as_str(),
	arg_required_else_help = true,
)]
pub struct ModuleCommandArgs {
	#[command(subcommand)]
	command: ModuleCommand,
}

/// Module subcommands
#[derive(Debug, Subcommand)]
enum ModuleCommand {
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
	},

	/// Show the module information
	Info {
		/// Path to WASM file
		#[arg(value_name = "FILE")]
		file: PathBuf,
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
	#[cfg(feature = "extension-sign")]
	Sign(sign::SignArgs),

	/// Read and change the project's package.toml
	Config(config_cmd::ConfigArgs),

	/// Install an extension into a database: inspect, approve, define
	#[command(long_about = crate::cli::extension::INSTALL_HELP)]
	Install(crate::cli::extension::InstallArgs),

	/// Upgrade an installed extension: compare signer and capabilities, redefine
	#[command(long_about = crate::cli::extension::UPGRADE_HELP)]
	Upgrade(crate::cli::extension::UpgradeArgs),

	/// Output the Surrealism version information
	Version,
}

/// Custom parser for `surrealdb_types::Value`
pub(super) fn parse_value(s: &str) -> Result<surrealdb_types::Value, String> {
	crate::core::syn::value(s).map_err(|e| format!("Invalid value: {e}"))
}

/// Initialize the module subcommand
pub async fn init(
	ModuleCommandArgs {
		command,
	}: ModuleCommandArgs,
) -> Result<()> {
	match command {
		ModuleCommand::Init {
			headless,
			org,
			name,
			path,
		} => init_cmd::init(path, org, name, headless).await,
		ModuleCommand::Run {
			args,
			fnc,
			file,
		} => run::init(file, fnc, args).await,
		ModuleCommand::Sig {
			fnc,
			file,
		} => sig::init(file, fnc).await,
		ModuleCommand::Info {
			file,
		} => info::init(file).await,
		ModuleCommand::Build {
			debug,
			out,
			path,
		} => build::init(path, out, debug).await,
		#[cfg(feature = "extension-sign")]
		ModuleCommand::Sign(args) => sign::init(args).await,
		ModuleCommand::Config(args) => config_cmd::init(args).await,
		ModuleCommand::Install(args) => crate::cli::extension::install(args).await,
		ModuleCommand::Upgrade(args) => crate::cli::extension::upgrade(args).await,
		ModuleCommand::Version => version::init().await,
	}
}

#[cfg(test)]
mod tests {
	use clap::CommandFactory as _;

	use super::*;

	#[test]
	fn install_and_upgrade_help_explain_the_connection() {
		for name in ["install", "upgrade"] {
			let mut cmd = ModuleCommandArgs::command();
			let sub = cmd.find_subcommand_mut(name).expect("subcommand");
			let help = sub.render_long_help().to_string();
			for needle in ["--auth-level", "SURREAL_USER", "DEFINE EXTENSION", "--trust-signer"] {
				assert!(help.contains(needle), "`{needle}` missing from `{name} --help`:\n{help}");
			}
			for gone in ["--yes", "--upload-to", "--grant "] {
				assert!(!help.contains(gone), "`{gone}` in `{name} --help`:\n{help}");
			}
		}
	}
}
