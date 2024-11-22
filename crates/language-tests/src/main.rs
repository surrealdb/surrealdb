#[macro_use]
mod mac;

mod cli;
mod cmd;
mod log;
mod runner;
mod tests;

use anyhow::Result;
use tracing::Level;
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};

#[tokio::main]
async fn main() -> Result<()> {
    let matches = cli::parse();

    let (sub, args) = matches.subcommand().unwrap();

    match sub {
        "run" => cmd::run::run(args).await,
        #[cfg(feature = "fuzzing")]
        "fuzz" => cmd::fuzz::run(args).await,
        #[cfg(not(feature = "fuzzing"))]
        "fuzz" => {
            anyhow::bail!(
                "Fuzzing subcommand is only implemented when the fuzzing feature is enabled"
            )
        }
        "list" => cmd::list::run(args).await,
        _ => panic!(),
    }
}
