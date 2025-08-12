use anyhow::Result;
use http::{HeaderMap, HeaderName, HeaderValue};

use crate::api::context::InvocationContext;
use crate::fnc::args::Optional;
use crate::val::{Object, Strand, Value};

pub fn raw_body(
	context: &mut InvocationContext,
	(Optional(raw),): (Optional<bool>,),
) -> Result<()> {
	context.response_body_raw = raw.unwrap_or(true);
	Ok(())
}

pub fn header(context: &mut InvocationContext, (name, value): (Strand, Value)) -> Result<()> {
	let name: HeaderName = name.parse()?;
	if let Value::None = value {
		if let Some(v) = context.response_headers.as_mut() {
			v.remove(&name);
		}
	} else {
		// TODO: Decide on whether to use cast or coerce.
		let value: HeaderValue = value.coerce_to::<String>()?.parse()?;
		if let Some(v) = context.response_headers.as_mut() {
			v.insert(name, value);
		} else {
			let mut headermap = HeaderMap::new();
			headermap.insert(name, value);
			context.response_headers = Some(headermap);
		}
	}

	Ok(())
}

pub fn headers(context: &mut InvocationContext, (headers,): (Object,)) -> Result<()> {
	let mut unset: Vec<String> = Vec::new();
	let mut headermap = HeaderMap::new();

	for (name, value) in headers.into_iter() {
		match value {
			Value::None => {
				unset.push(name);
			}
			value => {
				let name: HeaderName = name.parse()?;
				// TODO: Decide on whether to use cast or coerce.
				let value: HeaderValue = value.cast_to::<String>()?.parse()?;
				headermap.insert(name, value);
			}
		}
	}

	if let Some(v) = context.response_headers.as_mut() {
		v.extend(headermap);

		for k in unset.iter() {
			v.remove(k);
		}
	} else {
		context.response_headers = Some(headermap);
	}

	Ok(())
}
