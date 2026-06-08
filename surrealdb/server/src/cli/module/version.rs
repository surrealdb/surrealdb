use std::sync::LazyLock;

use anyhow::Result;

/// The Surrealism version string shown by `surreal module version` and
/// `surreal module --version`.
pub static MODULE_RELEASE: LazyLock<String> = LazyLock::new(|| {
	format!(
		"{} for {} on {}",
		surrealism_runtime::SDK_VERSION,
		std::env::consts::OS,
		std::env::consts::ARCH
	)
});

pub async fn init() -> Result<()> {
	println!("{}", *MODULE_RELEASE);
	Ok(())
}
