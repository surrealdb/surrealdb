use anyhow::{bail, Result};
use clap::ArgMatches;

use crate::cli::ColorMode;

pub async fn run(_color: ColorMode, _matches: &ArgMatches) -> Result<()> {
	bail!("This command is not yet implemented")
}
