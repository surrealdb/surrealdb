use std::collections::BTreeMap;

pub mod body;

use crate::{
	api::context::RequestContext,
	err::Error,
	sql::{Duration, Object, Value},
};

pub fn timeout(context: &mut RequestContext, (timeout,): (Duration,)) -> Result<(), Error> {
	context.timeout = Some(timeout);
	Ok(())
}

pub fn header(context: &mut RequestContext, (name, value): (String, Value)) -> Result<(), Error> {
	if let Value::None = value {
		if let Some(v) = context.headers.as_mut() {
			v.remove(&name);
		}
	} else {
		let value = value.coerce_to_string()?;
		if let Some(v) = context.headers.as_mut() {
			v.insert(name, value);
		} else {
			context.headers = Some(map!(
				name => value
			));
		}
	}

	Ok(())
}

pub fn headers(context: &mut RequestContext, (headers,): (Object,)) -> Result<(), Error> {
	let mut unset: Vec<String> = Vec::new();
	let headers = headers
		.into_iter()
		.filter_map(|(k, v)| match v {
			Value::None => {
				unset.push(k);
				None
			}
			v => Some((k, v)),
		})
		.map(|(k, v)| Ok((k, v.coerce_to_string()?)))
		.collect::<Result<BTreeMap<String, String>, Error>>()?;

	if let Some(v) = context.headers.as_mut() {
		v.extend(headers);

		for k in unset.iter() {
			v.remove(k);
		}
	} else {
		context.headers = Some(headers);
	}

	Ok(())
}
