pub(crate) mod abstraction;
mod backup;
mod config;
mod export;
mod import;
mod isready;
mod sql;
mod start;
pub(crate) mod validator;
mod version;

pub use config::CF;

use crate::cnf::LOGO;
use backup::BackupCommandArguments;
use clap::{Parser, Subcommand};
use export::ExportCommandArguments;
use import::ImportCommandArguments;
use isready::IsReadyCommandArguments;
use sql::SqlCommandArguments;
use start::StartCommandArguments;
use std::process::ExitCode;

pub const LOG: &str = "surrealdb::cli";

const INFO: &str = "
To get started using SurrealDB, and for guides on connecting to and building applications
on top of SurrealDB, check out the SurrealDB documentation (https://surrealdb.com/docs).

If you have questions or ideas, join the SurrealDB community (https://surrealdb.com/community).

If you find a bug, submit an issue on Github (https://github.com/surrealdb/surrealdb/issues).

We would love it if you could star the repository (https://github.com/surrealdb/surrealdb).

----------
";

#[derive(Parser, Debug)]
#[command(name = "SurrealDB command-line interface and server")]
#[command(about = INFO, before_help = LOGO)]
#[command(disable_version_flag = true, arg_required_else_help = true)]
struct Cli {
	#[command(subcommand)]
	command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
	#[command(about = "Start the database server")]
	Start(StartCommandArguments),
	#[command(about = "Backup data to or from an existing database")]
	Backup(BackupCommandArguments),
	#[command(about = "Import a SurrealQL script into an existing database")]
	Import(ImportCommandArguments),
	#[command(about = "Export an existing database as a SurrealQL script")]
	Export(ExportCommandArguments),
	#[command(about = "Output the command-line tool version information")]
	Version,
	#[command(about = "Start an SQL REPL in your terminal with pipe support")]
	Sql(SqlCommandArguments),
	#[command(about = "Check if the SurrealDB server is ready to accept connections", visible_alias = "isready")]
	IsReady(IsReadyCommandArguments),
}

#[tokio::main]
pub async fn init() -> ExitCode {
	let args = Cli::parse();
	let output = match args.command {
		Commands::Start(args) => start::init(args).await,
		Commands::Backup(args) => backup::init(args).await,
		Commands::Import(args) => import::init(args).await,
		Commands::Export(args) => export::init(args).await,
		Commands::Version => version::init(),
		Commands::Sql(args) => sql::init(args).await,
		Commands::IsReady(args) => isready::init(args).await,
	};
	if let Err(e) = output {
		error!(target: LOG, "{}", e);
		return ExitCode::FAILURE;
	}
	ExitCode::SUCCESS
}
