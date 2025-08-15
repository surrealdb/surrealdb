use std::collections::BTreeMap;

use anyhow::{Result, bail};

use crate::err::Error;
use crate::val::{Array, Object, Strand, Value};

pub fn entries((object,): (Object,)) -> Result<Value> {
	Ok(Value::Array(Array(
		object
			.iter()
			.map(|(k, v)| {
				let k = Value::Strand(unsafe { Strand::new_unchecked(k.to_owned()) });
				let v = v.clone();
				Value::Array(Array(vec![k, v]))
			})
			.collect(),
	)))
}

pub fn from_entries((array,): (Array,)) -> Result<Value> {
	let mut obj: BTreeMap<String, Value> = BTreeMap::default();

	for v in array.iter() {
		match v {
			Value::Array(Array(entry)) if entry.len() == 2 => {
				let key = match entry.first() {
					Some(v) => match v {
						Value::Strand(v) => v.clone().into_string(),
						v => v.to_string(),
					},
					_ => {
						bail!(Error::InvalidArguments {
							name: "object::from_entries".to_string(),
							message: "Expected entries, found invalid entry".to_string(),
						})
					}
				};

				let value = match entry.get(1) {
					Some(v) => v,
					_ => {
						bail!(Error::InvalidArguments {
							name: "object::from_entries".to_string(),
							message: "Expected entries, found invalid entry".to_string(),
						})
					}
				};

				obj.insert(key, value.to_owned());
			}
			_ => {
				bail!(Error::InvalidArguments {
					name: "object::from_entries".to_string(),
					message: format!("Expected entries, found {}", v.kind_of()),
				})
			}
		}
	}

	Ok(Value::Object(Object(obj)))
}

pub fn extend((mut object, other): (Object, Object)) -> Result<Value> {
	object.0.extend(other.0);
	Ok(Value::Object(object))
}

pub fn is_empty((object,): (Object,)) -> Result<Value> {
	Ok(Value::Bool(object.0.is_empty()))
}

pub fn len((object,): (Object,)) -> Result<Value> {
	Ok(Value::from(object.len()))
}

pub fn keys((object,): (Object,)) -> Result<Value> {
	Ok(Value::Array(Array(
		object
			.keys()
			.map(|v| {
				//TODO: Null bytes
				let strand = unsafe { Strand::new_unchecked(v.clone()) };
				Value::Strand(strand)
			})
			.collect(),
	)))
}

pub fn remove((mut object, targets): (Object, Value)) -> Result<Value> {
	match targets {
		Value::Strand(target) => {
			object.remove(target.as_str());
		}
		Value::Array(targets) => {
			for target in targets {
				let Value::Strand(s) = target else {
					bail!(Error::InvalidArguments {
						name: "object::remove".to_string(),
						message: format!(
							"'{target}' cannot be used as a key. Please use a string instead."
						),
					});
				};
				object.remove(s.as_str());
			}
		}
		other => {
			bail!(Error::InvalidArguments {
				name: "object::remove".to_string(),
				message: format!("'{other}' cannot be used as a key. Please use a string instead."),
			})
		}
	}
	Ok(Value::Object(object))
}

pub fn values((object,): (Object,)) -> Result<Value> {
	Ok(Value::Array(Array(object.values().map(|v| v.to_owned()).collect())))
}
