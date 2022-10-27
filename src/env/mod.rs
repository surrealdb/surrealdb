use crate::cnf::PKG_VERSION;
use crate::err::Error;

const LOG: &str = "surrealdb::env";

pub async fn init() -> Result<(), Error> {
	// Log version
	info!(target: LOG, "Running {}", release());
	// All ok
	Ok(())
}

/// Get the target operating system
pub fn os() -> &'static str {
	get_cfg!(target_os: "windows", "macos", "ios", "linux", "android", "freebsd", "openbsd", "netbsd");
	target_os()
}
/// Get the target system architecture
pub fn arch() -> &'static str {
	get_cfg!(target_arch: "x86", "x86_64", "mips", "powerpc", "powerpc64", "arm", "aarch64");
	target_arch()
}
/// Get the current release identifier
pub fn release() -> String {
	format!("{} for {} on {}", *PKG_VERSION, os(), arch())
}
