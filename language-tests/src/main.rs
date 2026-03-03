#![recursion_limit = "256"]
#![cfg_attr(target_family = "wasm", allow(unused))]

#[cfg(not(target_family = "wasm"))]
use anyhow::{self, Result};
#[cfg(not(target_family = "wasm"))]
use surrealql_test::cli::ColorMode;

#[cfg(not(target_family = "wasm"))]
#[tokio::main]
async fn main() -> Result<()> {
	let matches = surrealql_test::cli::parse();

	let color: ColorMode = matches.get_one("color").copied().unwrap();

	let (sub, args) = matches.subcommand().unwrap();

	match sub {
		"test" => surrealql_test::cmd::run::run(color, args).await,
		#[cfg(not(feature = "upgrade"))]
		"upgrade" => {
			anyhow::bail!(
				"Upgrade subcommand is only implemented when the 'upgrade' feature is enabled"
			)
		}
		#[cfg(feature = "upgrade")]
		"upgrade" => surrealql_test::cmd::upgrade::run(color, args).await,
		"list" => surrealql_test::cmd::list::run(args).await,
		_ => unreachable!(),
	}
}

#[cfg(target_family = "wasm")]
fn main() {}
