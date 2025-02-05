use std::collections::BTreeMap;

use crate::{
	api::context::RequestContext,
	err::Error,
	sql::{Duration, Object},
};

pub fn timeout(context: &mut RequestContext, (timeout,): (Duration,)) -> Result<(), Error> {
	context.timeout = Some(timeout);
	Ok(())
}

pub fn headers(context: &mut RequestContext, (headers,): (Object,)) -> Result<(), Error> {
	let headers = headers
		.into_iter()
		.map(|(k, v)| Ok((k, v.convert_to_strand()?.0)))
		.collect::<Result<BTreeMap<String, String>, Error>>()?;

	if let Some(v) = context.headers.as_mut() {
		v.extend(headers);
	} else {
		context.headers = Some(headers);
	}

	Ok(())
}
