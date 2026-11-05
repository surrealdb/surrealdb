use std::collections::BTreeMap;

use anyhow::{Result, bail};
use surrealdb_expr::expr::Error;
use surrealdb_expr::val::{Array, Object, Value};
use surrealdb_strand::Strand;
use surrealdb_types::ToSql;

use crate::fuel;

/// Define each synchronous function as its walk driven to its end at once
/// ([`fuel::now`]).
macro_rules! now {
	($($name:ident = $walk:ident($args:ty);)+) => {
		$(
			pub fn $name(args: $args) -> Result<Value> {
				fuel::now($walk(args))
			}
		)+
	};
}

now! {
	entries = entries_walk((Object,));
	from_entries = from_entries_walk((Array,));
	extend = extend_walk((Object, Object));
	keys = keys_walk((Object,));
	remove = remove_walk((Object, Value));
	values = values_walk((Object,));
}

pub async fn entries_walk((object,): (Object,)) -> Result<Value> {
	let entries = fuel::map(object.iter(), |(k, v)| {
		Value::Array(Array(vec![Value::String(k.to_owned()), v.clone()]))
	})
	.await;
	Ok(Value::Array(Array(entries)))
}

pub async fn from_entries_walk((array,): (Array,)) -> Result<Value> {
	let mut obj: BTreeMap<Strand, Value> = BTreeMap::default();

	for v in array.iter() {
		fuel::tick(1).await;
		if let Value::Array(Array(entry)) = v
			&& entry.len() == 2
		{
			let key: Strand = match entry.first() {
				Some(v) => {
					if let Value::String(v) = v {
						v.clone()
					} else {
						v.to_sql().into()
					}
				}
				_ => {
					bail!(Error::InvalidFunctionArguments {
						name: "object::from_entries".to_string(),
						message: "Expected entries, found invalid entry".to_string(),
					})
				}
			};

			let value = match entry.get(1) {
				Some(v) => v,
				_ => {
					bail!(Error::InvalidFunctionArguments {
						name: "object::from_entries".to_string(),
						message: "Expected entries, found invalid entry".to_string(),
					})
				}
			};

			obj.insert(key, value.to_owned());
			continue;
		}
		bail!(Error::InvalidFunctionArguments {
			name: "object::from_entries".to_string(),
			message: format!("Expected entries, found {}", v.kind_of()),
		})
	}

	Ok(Value::Object(Object::from(obj)))
}

pub async fn extend_walk((mut object, other): (Object, Object)) -> Result<Value> {
	fuel::walk(other.0, |(key, value)| {
		object.0.insert(key, value);
		std::ops::ControlFlow::Continue(())
	})
	.await;
	Ok(Value::Object(object))
}

pub fn is_empty((object,): (Object,)) -> Result<Value> {
	Ok(Value::Bool(object.0.is_empty()))
}

pub fn len((object,): (Object,)) -> Result<Value> {
	Ok(Value::from(object.len()))
}

pub async fn keys_walk((object,): (Object,)) -> Result<Value> {
	Ok(Value::Array(Array(fuel::map(object.keys(), |k| Value::String(k.clone())).await)))
}

pub async fn remove_walk((mut object, targets): (Object, Value)) -> Result<Value> {
	if let Value::String(target) = targets {
		object.remove(target.as_str());
	} else if let Value::Array(targets) = targets {
		for target in targets {
			fuel::tick(1).await;
			let Value::String(s) = target else {
				bail!(Error::InvalidFunctionArguments {
					name: "object::remove".to_string(),
					message: format!(
						"'{}' cannot be used as a key. Please use a string instead.",
						target.to_sql()
					),
				});
			};
			object.remove(s.as_str());
		}
	} else {
		bail!(Error::InvalidFunctionArguments {
			name: "object::remove".to_string(),
			message: format!(
				"'{}' cannot be used as a key. Please use a string instead.",
				targets.to_sql()
			),
		})
	}
	Ok(Value::Object(object))
}

pub async fn values_walk((object,): (Object,)) -> Result<Value> {
	Ok(Value::Array(Array(fuel::map(object.values(), Value::clone).await)))
}
