use crate::err::Error;
use crate::expr::value::Value;
use crate::expr::{Array, Object, Strand};
use std::collections::BTreeMap;

pub fn entries((object,): (Object,)) -> Result<Value, Error> {
	Ok(Value::Array(Array(
		object
			.iter()
			.map(|(k, v)| {
				Value::Array(Array(vec![Value::Strand(Strand(k.to_owned())), v.to_owned()]))
			})
			.collect(),
	)))
}

pub fn from_entries((array,): (Array,)) -> Result<Value, Error> {
	let mut obj: BTreeMap<String, Value> = BTreeMap::default();

	for v in array.iter() {
		match v {
			Value::Array(Array(entry)) if entry.len() == 2 => {
				let key = match entry.first() {
					Some(v) => match v {
						Value::Strand(v) => v.to_owned().to_raw(),
						v => v.to_string(),
					},
					_ => {
						return Err(Error::InvalidArguments {
							name: "object::from_entries".to_string(),
							message: "Expected entries, found invalid entry".to_string(),
						})
					}
				};

				let value = match entry.get(1) {
					Some(v) => v,
					_ => {
						return Err(Error::InvalidArguments {
							name: "object::from_entries".to_string(),
							message: "Expected entries, found invalid entry".to_string(),
						})
					}
				};

				obj.insert(key, value.to_owned());
			}
			_ => {
				return Err(Error::InvalidArguments {
					name: "object::from_entries".to_string(),
					message: format!("Expected entries, found {}", v.kindof()),
				})
			}
		}
	}

	Ok(Value::Object(Object(obj)))
}

pub fn extend((mut object, other): (Object, Object)) -> Result<Value, Error> {
	object.0.extend(other.0);
	Ok(Value::Object(object))
}

pub fn is_empty((object,): (Object,)) -> Result<Value, Error> {
	Ok(Value::Bool(object.0.is_empty()))
}

pub fn len((object,): (Object,)) -> Result<Value, Error> {
	Ok(Value::from(object.len()))
}

pub fn keys((object,): (Object,)) -> Result<Value, Error> {
	Ok(Value::Array(Array(object.keys().map(|v| Value::Strand(Strand(v.to_owned()))).collect())))
}

pub fn remove((mut object, targets): (Object, Value)) -> Result<Value, Error> {
	match targets {
		Value::Strand(target) => {
			object.remove(&target.0);
		}
		Value::Array(targets) => {
			let mut remove_targets = Vec::with_capacity(targets.len());
			for target in targets {
				let Value::Strand(s) = target else {
					return Err(Error::InvalidArguments {
						name: "object::remove".to_string(),
						message: format!(
							"'{target}' cannot be used as a key. Please use a string instead."
						),
					});
				};
				remove_targets.push(s.0);
			}
			for target in remove_targets {
				object.remove(&target);
			}
		}
		other => {
			return Err(Error::InvalidArguments {
				name: "object::remove".to_string(),
				message: format!("'{other}' cannot be used as a key. Please use a string instead."),
			})
		}
	}
	Ok(Value::Object(object))
}

pub fn values((object,): (Object,)) -> Result<Value, Error> {
	Ok(Value::Array(Array(object.values().map(|v| v.to_owned()).collect())))
}
