pub mod req;
pub mod res;

use crate::{api::context::InvocationContext, err::Error, sql::Duration};

pub fn timeout(context: &mut InvocationContext, (timeout,): (Duration,)) -> Result<(), Error> {
	context.timeout = Some(timeout);
	Ok(())
}
