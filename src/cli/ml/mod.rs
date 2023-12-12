mod export;
mod import;

use self::export::ExportCommandArguments;
use self::import::ImportCommandArguments;
use crate::err::Error;
use clap::Subcommand;

#[derive(Debug, Subcommand)]
pub enum MlCommand {
	#[command(about = "Import a SurrealML model into an existing database")]
	Import(ImportCommandArguments),
	#[command(about = "Export a SurrealML model from an existing database")]
	Export(ExportCommandArguments),
}

pub async fn init(command: MlCommand) -> Result<(), Error> {
	match command {
		MlCommand::Import(args) => import::init(args).await,
		MlCommand::Export(args) => export::init(args).await,
	}
}
