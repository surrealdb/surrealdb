//#![allow(dead_code)]

use anyhow::Result;
use clap::ArgMatches;

use crate::cli::ColorMode;

mod cli;
mod store;
pub use cli::cmd;
mod run;

mod stats;

const DEFAULT_RESAMPLES: usize = 10_000;
const DEFAULT_CONFIDENCE_LEVEL: f64 = 0.95;
const DEFAULT_SIGNIFICANCE_THRESHOLD: f64 = 0.05;
const DEFAULT_NOISE_THRESHOLD: f64 = 0.01;

/// Main subcommand function, runs the actual subcommand.
pub async fn run(color: ColorMode, matches: &ArgMatches) -> Result<()> {
	let (sub, args) = matches.subcommand().unwrap();
	match sub {
		"run" => run::run(color, matches, args).await,
		_ => unreachable!(),
	}
}
