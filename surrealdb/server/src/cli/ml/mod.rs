mod export;
mod import;

use anyhow::Result;
use clap::Subcommand;

use self::export::ExportCommandArguments;
use self::import::ImportCommandArguments;

#[derive(Debug, Subcommand)]
pub enum MlCommand {
	#[command(about = "Import a SurrealML model into an existing database")]
	Import(ImportCommandArguments),
	#[command(about = "Export a SurrealML model from an existing database")]
	Export(ExportCommandArguments),
}

impl MlCommand {
	/// Whether the command writes its result to standard output, which then
	/// carries that result and nothing else.
	pub(crate) fn writes_to_stdout(&self) -> bool {
		matches!(self, MlCommand::Export(args) if args.writes_to_stdout())
	}
}

pub async fn init(command: MlCommand) -> Result<()> {
	match command {
		MlCommand::Import(args) => import::init(args).await,
		MlCommand::Export(args) => export::init(args).await,
	}
}
