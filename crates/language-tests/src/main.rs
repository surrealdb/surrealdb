mod cli;
mod cmd;
mod format;
mod runner;
mod tests;

use anyhow::Result;
use cli::ColorMode;

#[tokio::main]
async fn main() -> Result<()> {
	let matches = cli::parse();

	let color: ColorMode = matches.get_one("color").copied().unwrap();

	let (sub, args) = matches.subcommand().unwrap();

	//log::init(Level::INFO);

	match sub {
		"run" => cmd::run::run(color, args).await,
		//#[cfg(feature = "fuzzing")]
		//"fuzz" => cmd::fuzz::run(args).await,
		//#[cfg(not(feature = "fuzzing"))]
		"fuzz" => {
			anyhow::bail!(
				"Fuzzing subcommand is only implemented when the fuzzing feature is enabled"
			)
		}
		"list" => cmd::list::run(args).await,
		_ => panic!(),
	}
}
