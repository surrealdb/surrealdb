use js::{Class, Ctx, Result};

pub mod blob;
pub mod duration;
pub mod headers;
pub mod record;
pub mod request;
pub mod response;
pub mod uuid;

pub fn register(ctx: Ctx<'_>) -> Result<()> {
	Class::<blob::blob::Blob>::register(ctx)?;
	Class::<duration::duration::Duration>::register(ctx)?;
	Class::<headers::headers::Headers>::register(ctx)?;
	Class::<record::record::Record>::register(ctx)?;
	Class::<request::request::Request>::register(ctx)?;
	Class::<response::response::Response>::register(ctx)?;
	Class::<uuid::uuid::Uuid>::register(ctx)?;
	Ok(())
}
