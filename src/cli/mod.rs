pub(crate) mod abstraction;
mod backup;
mod config;
mod export;
mod import;
mod isready;
mod ml;
mod sql;
mod start;
mod upgrade;
mod validate;
pub(crate) mod validator;
mod version;

use crate::cnf::LOGO;
use backup::BackupCommandArguments;
use clap::{Parser, Subcommand};
pub use config::CF;
use export::ExportCommandArguments;
use import::ImportCommandArguments;
use isready::IsReadyCommandArguments;
use ml::MlCommand;
use sql::SqlCommandArguments;
use start::StartCommandArguments;
use std::process::ExitCode;
use upgrade::UpgradeCommandArguments;
use validate::ValidateCommandArguments;
use version::VersionCommandArguments;

const INFO: &str = "
To get started using SurrealDB, and for guides on connecting to and building applications
on top of SurrealDB, check out the SurrealDB documentation (https://surrealdb.com/docs).

If you have questions or ideas, join the SurrealDB community (https://surrealdb.com/community).

If you find a bug, submit an issue on GitHub (https://github.com/surrealdb/surrealdb/issues).

We would love it if you could star the repository (https://github.com/surrealdb/surrealdb).

----------
";

#[derive(Parser, Debug)]
#[command(name = "SurrealDB command-line interface and server", bin_name = "surreal")]
#[command(about = INFO, before_help = LOGO)]
#[command(disable_version_flag = true, arg_required_else_help = true)]
struct Cli {
	#[command(subcommand)]
	command: Commands,
}

#[allow(clippy::large_enum_variant)]
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
	#[command(about = "Output the command-line tool and remote server version information")]
	Version(VersionCommandArguments),
	#[command(about = "Upgrade to the latest stable version")]
	Upgrade(UpgradeCommandArguments),
	#[command(about = "Start an SQL REPL in your terminal with pipe support")]
	Sql(SqlCommandArguments),
	#[command(subcommand, about = "Manage SurrealML models within an existing database")]
	Ml(MlCommand),
	#[command(
		about = "Check if the SurrealDB server is ready to accept connections",
		visible_alias = "isready"
	)]
	IsReady(IsReadyCommandArguments),
	#[command(about = "Validate SurrealQL query files")]
	Validate(ValidateCommandArguments),
}

pub async fn init() -> ExitCode {
	let args = Cli::parse();
	let output = match args.command {
		Commands::Start(args) => start::init(args).await,
		Commands::Backup(args) => backup::init(args).await,
		Commands::Import(args) => import::init(args).await,
		Commands::Export(args) => export::init(args).await,
		Commands::Version(args) => version::init(args).await,
		Commands::Upgrade(args) => upgrade::init(args).await,
		Commands::Sql(args) => sql::init(args).await,
		Commands::Ml(args) => ml::init(args).await,
		Commands::IsReady(args) => isready::init(args).await,
		Commands::Validate(args) => validate::init(args).await,
	};
	if let Err(e) = output {
		error!("{}", e);
		ExitCode::FAILURE
	} else {
		ExitCode::SUCCESS
	}
}
