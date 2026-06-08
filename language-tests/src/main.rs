#![recursion_limit = "256"]

mod cli;
mod cmd;
mod format;
mod runner;
mod tests;
mod util;

use anyhow::{self, Result};
use cli::ColorMode;

/// Tokio worker stack size (`surreal` uses the same defaults via `SURREAL_RUNTIME_STACK_SIZE`).
///
/// rustc/llvm can grow per-frame stack usage across versions; instrumentation builds (`llvm-cov`)
/// consume more stack than plain debug. The default ~2 MiB thread stack is insufficient for deep
/// parser / planner / executor recursion.
fn runtime_worker_stack_size() -> usize {
	std::env::var("SURREAL_RUNTIME_STACK_SIZE").ok().and_then(|v| v.parse().ok()).unwrap_or({
		if cfg!(debug_assertions) {
			20 * 1024 * 1024
		} else {
			10 * 1024 * 1024
		}
	})
}

fn main() -> Result<()> {
	tokio::runtime::Builder::new_multi_thread()
		.enable_all()
		.thread_stack_size(runtime_worker_stack_size())
		.build()
		.map_err(anyhow::Error::from)?
		.block_on(async_main())
}

async fn async_main() -> Result<()> {
	let matches = cli::parse();

	let color: ColorMode = matches.get_one("color").copied().unwrap();

	let (sub, args) = matches.subcommand().unwrap();

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
		#[cfg(not(feature = "bench"))]
		"bench" => {
			anyhow::bail!(
				"Bench subcommand is only implemented when the 'bench' feature is enabled"
			)
		}
		#[cfg(feature = "bench")]
		"bench" => cmd::bench::run(color, args).await,
		"list" => cmd::list::run(args).await,
		_ => unreachable!(),
	}
}
