use js::{Class, Ctx, Result};

pub mod duration;
pub mod file;
pub mod record;
pub mod uuid;

pub fn init(ctx: &Ctx<'_>) -> Result<()> {
	let globals = ctx.globals();
	Class::<duration::Duration>::define(&globals)?;
	Class::<record::Record>::define(&globals)?;
	Class::<uuid::Uuid>::define(&globals)?;
	Class::<file::File>::define(&globals)?;
	Ok(())
}
