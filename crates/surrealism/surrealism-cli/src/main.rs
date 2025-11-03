mod commands;
pub(crate) mod host;

use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::commands::build::BuildCommand;
use crate::commands::info::InfoCommand;
use crate::commands::run::RunCommand;
use crate::commands::sig::SigCommand;
use crate::commands::SurrealismCommand;

/// CLI definition
#[derive(Debug, Parser)]
#[command(name = "cli-name")]
struct Cli {
	#[command(subcommand)]
	command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
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
	surrealdb_core::syn::value(s).map_err(|e| format!("Invalid value: {e}"))
}

#[tokio::main]
async fn main() {
	let cli = Cli::parse();

	match cli.command {
		Commands::Run {
			args,
			fnc,
			file,
		} => {
			let run_command = RunCommand {
				file,
				fnc,
				args,
			};

			if let Err(e) = run_command.run().await {
				eprintln!("Error: {e}");
				std::process::exit(1);
			}
		}
		Commands::Sig {
			fnc,
			file,
		} => {
			let run_command = SigCommand {
				file,
				fnc,
			};

			if let Err(e) = run_command.run().await {
				eprintln!("Error: {e}");
				std::process::exit(1);
			}
		}
		Commands::Info {
			file,
		} => {
			let info_command = InfoCommand {
				file,
			};
			if let Err(e) = info_command.run().await {
				eprintln!("Error: {e}");
				std::process::exit(1);
			}
		}
		Commands::Build {
			out,
			path,
		} => {
			let build_command = BuildCommand {
				path,
				out,
			};
			if let Err(e) = build_command.run().await {
				eprintln!("Error: {e}");
				std::process::exit(1);
			}
		}
	}
}
