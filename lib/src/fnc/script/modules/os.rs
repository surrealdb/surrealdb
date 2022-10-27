#[js::bind(module, public)]
#[quickjs(bare)]
#[allow(non_upper_case_globals)]
pub mod package {
	// Get the target system architecture
	pub fn arch() -> &'static str {
		crate::env::arch()
	}
	// Get the target operating system
	pub fn platform() -> &'static str {
		crate::env::os()
	}
}
