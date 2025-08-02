pub mod req;
pub mod res;

use crate::api::context::InvocationContext;
use crate::val::Duration;
use anyhow::Result;

pub fn timeout(context: &mut InvocationContext, (timeout,): (Duration,)) -> Result<()> {
	context.timeout = Some(timeout);
	Ok(())
}
