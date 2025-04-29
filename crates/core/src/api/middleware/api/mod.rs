pub mod req;
pub mod res;

use crate::api::context::InvocationContext;
use crate::err::Error;
use crate::sql::Duration;

pub fn timeout(context: &mut InvocationContext, (timeout,): (Duration,)) -> Result<(), Error> {
	context.timeout = Some(timeout);
	Ok(())
}
