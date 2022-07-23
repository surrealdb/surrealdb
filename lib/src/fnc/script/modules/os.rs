#[js::bind(module, public)]
#[quickjs(bare)]
#[allow(non_upper_case_globals)]
pub mod package {
	// Get the target system architecture
	pub fn arch() -> &'static str {
		get_cfg!(target_arch: "x86", "x86_64", "mips", "powerpc", "powerpc64", "arm", "aarch64");
		target_arch()
	}
	// Get the target operating system
	pub fn platform() -> &'static str {
		get_cfg!(target_os: "windows", "macos", "ios", "linux", "android", "freebsd", "openbsd", "netbsd");
		target_os()
	}
	// Get the target release text
	pub fn release() -> String {
		get_cfg!(target_os: "windows", "macos", "ios", "linux", "android", "freebsd", "openbsd", "netbsd");
		get_cfg!(target_arch: "x86", "x86_64", "mips", "powerpc", "powerpc64", "arm", "aarch64");
		format!("{} for {} on {}", crate::cnf::VERSION, target_os(), target_arch())
	}
	// Get the current version
	pub fn version() -> &'static str {
		crate::cnf::VERSION
	}
}
