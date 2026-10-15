use std::fs;
use std::path::Path;

use anyhow::Result;
use surrealism_runtime::PrefixErr;

/// The Rust target every Surrealism module is compiled for.
///
/// WASI Preview 2, so the output is a component the host can instantiate
/// directly. The guest SDK needs no build flags: its `async fn` support comes
/// from the component model's async ABI, not from an embedded executor.
pub const TARGET: &str = "wasm32-wasip2";

/// Scaffolded `.cargo/config.toml`.
///
/// The flag is present but commented out. Nothing in the SDK needs it, and
/// enabling it unconditionally would be misleading — but a module that pulls in
/// a tokio-based crate cannot compile without it, and the error tokio emits
/// (`Only features sync,macros,io-util,rt,time are supported on wasm.`) does
/// not say what to do about it. Leaving the line here with the reason attached
/// is cheaper than making every author find that out.
const CARGO_CONFIG: &str = r#"# Uncomment when a dependency needs Tokio's `net` feature — a tokio-based HTTP
# client, for instance. Tokio gates that behind `--cfg tokio_unstable` for the
# whole `wasm` target family, which lumps wasm32-wasip2 (real sockets, works)
# together with the browser (no sockets). Nothing in the Surrealism SDK needs
# it, so it is off by default.
#
# Note that a Tokio-driven export must run its own executor, and must not await
# a `Transaction` method inside it: a `tx` call completes only once the guest
# yields back to the host, which a blocking executor never does. Keep such
# exports free of a `Transaction` parameter.
#
# [build]
# rustflags = ["--cfg", "tokio_unstable"]
"#;

/// Write `.cargo/config.toml` into a freshly scaffolded module project.
///
/// Only called by `surreal extension init`. An existing file is left untouched, so
/// re-running init never overwrites an author's own flags.
pub fn write_cargo_config(project_dir: &Path) -> Result<()> {
	let cargo_dir = project_dir.join(".cargo");
	let config_path = cargo_dir.join("config.toml");
	if config_path.exists() {
		return Ok(());
	}

	fs::create_dir_all(&cargo_dir)
		.prefix_err(|| format!("Failed to create {}", cargo_dir.display()))?;
	fs::write(&config_path, CARGO_CONFIG)
		.prefix_err(|| format!("Failed to write {}", config_path.display()))?;

	Ok(())
}
