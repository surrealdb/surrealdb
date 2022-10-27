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
