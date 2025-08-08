use anyhow::Result;
use clap::Args;

#[derive(Args, Debug)]
pub struct FixCommandArguments {
	#[arg(help = "Database path used for storing data")]
	#[arg(env = "SURREAL_PATH", index = 1)]
	#[arg(default_value = "memory")]
	#[arg(value_parser = super::validator::path_valid)]
	path: String,
}

pub async fn init(
	FixCommandArguments {
		path: _,
	}: FixCommandArguments,
) -> Result<()> {
	// All ok
	Err(anyhow::anyhow!("Fix is not implemented"))
}
