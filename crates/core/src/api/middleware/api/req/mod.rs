use crate::{api::context::InvocationContext, err::Error, fnc::args::Optional};

pub fn max_body(context: &mut InvocationContext, (max_size,): (String,)) -> Result<(), Error> {
	let bytesize = max_size.parse().map_err(|_| Error::InvalidArguments {
		name: "max_body".to_string(),
		message: "Argument 1 was the wrong type, expected bytes size string".to_string(),
	})?;
	context.request_body_max = Some(bytesize);
	Ok(())
}

pub fn raw_body(
	context: &mut InvocationContext,
	(Optional(raw),): (Optional<bool>,),
) -> Result<(), Error> {
	context.request_body_raw = raw.unwrap_or(true);
	Ok(())
}
