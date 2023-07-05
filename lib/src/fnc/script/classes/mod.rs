use js::{Ctx, Result};

pub mod duration;
pub mod record;
pub mod uuid;

pub fn init(ctx: Ctx<'_>) -> Result<()> {
	let globals = ctx.globals();
	globals.init_def::<duration::Duration>()?;
	globals.init_def::<record::Record>()?;
	globals.init_def::<uuid::Uuid>()?;
	Ok(())
}
