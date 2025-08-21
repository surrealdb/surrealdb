use std::process::Command;
use std::{env, str};

use semver::BuildMetadata;

const BUILD_METADATA: &str = "SURREAL_BUILD_METADATA";

fn main() {
	println!("cargo:rerun-if-env-changed={BUILD_METADATA}");
	println!("cargo:rerun-if-changed=crates/core");
	println!("cargo:rerun-if-changed=crates/macro");
	println!("cargo:rerun-if-changed=crates/sdk");
	println!("cargo:rerun-if-changed=src");
	println!("cargo:rerun-if-changed=build.rs");
	println!("cargo:rerun-if-changed=Cargo.toml");
	println!("cargo:rerun-if-changed=Cargo.lock");
	if let Some(metadata) = build_metadata() {
		println!("cargo:rustc-env={BUILD_METADATA}={metadata}");
	}
}

fn build_metadata() -> Option<String> {
	if let Ok(input) = env::var(BUILD_METADATA) {
		let metadata = input.trim();
		if let Err(error) = BuildMetadata::new(metadata) {
			panic!("invalid build metadata `{input}`: {error}");
		}
		return Some(metadata.to_owned());
	}
	let date = git()
		.args(["show", "--no-patch", "--format=%ad", "--date=format:%Y%m%d"])
		.output_string()?;
	let rev = git().args(["rev-parse", "--short", "HEAD"]).output_string()?;
	let repo_clean = git()
		.args(["diff", "--quiet"])
		.output()
		.map(|output| output.status.success())
		.unwrap_or_default();
	let metadata = if repo_clean {
		format!("{date}.{rev}")
	} else {
		format!("{date}.{rev}.dirty")
	};
	Some(metadata)
}

fn git() -> Command {
	Command::new("git")
}

trait CommandExt {
	fn output_string(&mut self) -> Option<String>;
}

impl CommandExt for Command {
	fn output_string(&mut self) -> Option<String> {
		self.output()
			.ok()
			.filter(|output| output.status.success())
			.and_then(|output| {
				str::from_utf8(&output.stdout).ok().map(|output| output.trim().to_string())
			})
			.filter(|output| !output.is_empty())
	}
}
