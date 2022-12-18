/// The SurrealDB package version identifier
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The operating system of the current machine
pub fn os() -> &'static str {
	get_cfg!(os: "windows", "macos", "ios", "linux", "android", "freebsd", "openbsd", "netbsd");
	os()
}
/// The system architecture of the current machine
pub fn arch() -> &'static str {
	get_cfg!(arch: "x86", "x86_64", "mips", "powerpc", "powerpc64", "arm", "aarch64");
	arch()
}
