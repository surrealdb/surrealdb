mod cli;
mod cmd;
mod format;
mod runner;
mod temp_dir;
mod tests;

use anyhow::{self, Result};
use cli::ColorMode;

#[cfg(all(feature = "backend-foundation-7_1", feature = "backend-foundation-7_3"))]
compile_error!(
	"The two foundation db version backends are mutually exclusive, they cannot both be enabled"
);

#[tokio::main]
async fn main() -> Result<()> {
	let matches = cli::parse();

	let color: ColorMode = matches.get_one("color").copied().unwrap();

	let (sub, args) = matches.subcommand().unwrap();

	//log::init(Level::INFO);

	match sub {
		"test" => cmd::run::run(color, args).await,
		#[cfg(not(feature = "upgrade"))]
		"upgrade" => {
			anyhow::bail!(
				"Upgrade subcommand is only implemented when the 'upgrade' feature is enabled"
			)
		}
		#[cfg(feature = "upgrade")]
		"upgrade" => cmd::upgrade::run(color, args).await,
		"list" => cmd::list::run(args).await,
		_ => unreachable!(),
	}
}
