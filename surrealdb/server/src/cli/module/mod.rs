mod build;
mod config_cmd;
mod host;
mod info;
mod init_cmd;
mod run;
mod sig;
#[cfg(feature = "extension-sign")]
mod sign;
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
		ModuleCommand::Version => version::init().await,
	}
}
