mod cli;
mod cmd;
mod format;
mod runner;
mod temp_dir;
mod tests;

use std::path::Path;

use anyhow::{self, Result};
use clap::Parser;
use cli::Commands;

#[cfg(all(feature = "backend-foundation-7_1", feature = "backend-foundation-7_3"))]
compile_error!(
	"The two foundation db version backends are mutually exclusive, they cannot both be enabled"
);

#[tokio::main]
async fn main() -> Result<()> {
	let cli::Args {
		color,
		command,
	} = cli::Args::parse();

	change_directory_to_language_tests_root()?;

	match command {
		Commands::Test(args) => cmd::run::run(color, args).await,
		#[cfg(feature = "upgrade")]
		Commands::Upgrade(args) => cmd::upgrade::run(color, args).await,
		Commands::List(args) => cmd::list::run(args).await,
	}
}

/// Change the current directory to the language tests root directory.
///
/// This is useful for running the tests either from the root of the repository
/// or from the `language-tests` crate directory. The tests expect to always be run
/// from the `language-tests` crate directory.
fn change_directory_to_language_tests_root() -> Result<()> {
	let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
	eprintln!("CARGO_MANIFEST_DIR: {manifest_dir}");
	let language_tests_root = Path::new(&manifest_dir).parent().unwrap();
	std::env::set_current_dir(language_tests_root)?;
	Ok(())
}
