pub mod req;
pub mod res;

use anyhow::Result;

use crate::api::context::InvocationContext;
use crate::val::Duration;

pub fn timeout(context: &mut InvocationContext, (timeout,): (Duration,)) -> Result<()> {
	context.timeout = Some(timeout);
	Ok(())
}
