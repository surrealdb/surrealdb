mod build;
mod host;
mod info;
mod run;
mod sig;

use std::path::PathBuf;

use anyhow::Result;
use clap::Subcommand;

/// Module command arguments
#[derive(Debug, Subcommand)]
pub enum ModuleCommand {
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
		/// Output file path or filename
		#[arg(short = 'o', long)]
		out: Option<PathBuf>,

		/// Path to source directory (defaults to current directory)
		#[arg(value_name = "SOURCE_PATH")]
		path: Option<PathBuf>,
	},
}

/// Custom parser for `surrealdb_types::Value`
fn parse_value(s: &str) -> Result<surrealdb_types::Value, String> {
	crate::core::syn::value(s).map_err(|e| format!("Invalid value: {e}"))
}

/// Initialize the module subcommand
pub async fn init(cmd: ModuleCommand) -> Result<()> {
	match cmd {
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
			out,
			path,
		} => build::init(path, out).await,
	}
}
