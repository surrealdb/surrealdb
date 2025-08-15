use std::collections::BTreeMap;
use std::mem;

use anyhow::Result;
use http::HeaderMap;

use crate::err::Error;
use crate::val::{Object, Value};

pub(crate) fn headermap_to_object(headers: HeaderMap) -> Result<Object> {
	let mut next_key = None;
	let mut next_value = Value::None;
	let mut first_value = true;
	let mut res = BTreeMap::new();

	// Header map can contain multiple values for each header.
	// This is handled by returning the key name first and then return multiple
	// values with key name = None.
	for (k, v) in headers.into_iter() {
		let v = Value::Strand(v.to_str().map_err(Error::from)?.to_owned().into());

		if let Some(k) = k {
			let k = k.as_str().to_owned();
			// new key, if we had accumulated a key insert it first and then update
			// accumulated state.
			if let Some(k) = next_key.take() {
				let v = mem::replace(&mut next_value, Value::None);
				res.insert(k, v);
			}
			next_key = Some(k);
			next_value = v;
			first_value = true;
		} else if first_value {
			// no new key, but this is directly after the first value, turn the header value
			// into an array of values.
			first_value = false;
			next_value = Value::Array(vec![next_value, v].into())
		} else {
			// Since it is not a new key and a new value it must be atleast a third header
			// value and `next_value` is already updated to an array.
			let Value::Array(ref mut array) = next_value else {
				unreachable!()
			};
			array.push(v);
		}
	}

	// Insert final key if there is one.
	if let Some(x) = next_key {
		let v = mem::replace(&mut next_value, Value::None);
		res.insert(x, v);
	}

	Ok(Object(res))
}
