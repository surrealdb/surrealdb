#[js::bind(module, public)]
#[quickjs(bare)]
#[allow(non_upper_case_globals)]
pub mod package {
	pub const version: &str = env!("CARGO_PKG_VERSION");
}
