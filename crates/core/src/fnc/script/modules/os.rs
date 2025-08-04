use js::Result;
use js::module::{Declarations, Exports, ModuleDef};

/// Get the target system architecture
#[js::function]
pub fn arch() -> &'static str {
	crate::env::arch()
}
/// Get the target operating system
#[js::function]
pub fn platform() -> &'static str {
	crate::env::os()
}

pub struct Package;

impl ModuleDef for Package {
	fn declare(declare: &Declarations) -> Result<()> {
		declare.declare("arch")?;
		declare.declare("platform")?;
		Ok(())
	}

	fn evaluate<'js>(_ctx: &js::Ctx<'js>, exports: &Exports<'js>) -> Result<()> {
		exports.export("arch", js_arch)?;
		exports.export("platform", js_platform)?;
		Ok(())
	}
}
