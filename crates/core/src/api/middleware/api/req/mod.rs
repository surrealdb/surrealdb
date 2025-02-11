use crate::{api::context::InvocationContext, err::Error, sql::Bytesize};

pub fn max_body(context: &mut InvocationContext, (max_size,): (Bytesize,)) -> Result<(), Error> {
	context.request_body_max = Some(max_size);
	Ok(())
}

pub fn raw_body(context: &mut InvocationContext, (raw,): (Option<bool>,)) -> Result<(), Error> {
	context.request_body_raw = raw.unwrap_or(true);
	Ok(())
}
