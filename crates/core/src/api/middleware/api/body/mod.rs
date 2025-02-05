use crate::{api::context::RequestContext, err::Error, sql::Bytesize};

pub fn max_size(context: &mut RequestContext, (max_size,): (Bytesize,)) -> Result<(), Error> {
	context.max_body_size = Some(max_size);
	Ok(())
}
