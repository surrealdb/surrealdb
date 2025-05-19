pub mod req;
pub mod res;

use crate::{api::context::InvocationContext, sql::Duration};
use anyhow::Result;

pub fn timeout(context: &mut InvocationContext, (timeout,): (Duration,)) -> Result<()> {
	context.timeout = Some(timeout);
	Ok(())
}
