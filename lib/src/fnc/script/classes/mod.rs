use js::{Ctx, Result};

pub mod blob;
pub mod duration;
pub mod headers;
pub mod record;
pub mod request;
pub mod response;
pub mod uuid;

pub fn init(ctx: Ctx<'_>) -> Result<()> {
	let globals = ctx.globals();
	globals.init_def::<blob::Blob>()?;
	globals.init_def::<duration::Duration>()?;
	globals.init_def::<headers::Headers>()?;
	globals.init_def::<record::Record>()?;
	globals.init_def::<request::Request>()?;
	globals.init_def::<response::Response>()?;
	globals.init_def::<uuid::Uuid>()?;
	Ok(())
}
