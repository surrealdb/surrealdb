use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::sql::operation::{Operation, Operations};
use crate::sql::value::Value;

impl Value {
	pub fn diff(self, _: &Runtime, _: &Options, _: &mut Executor, val: Value) -> Operations {
		let mut ops: Operations = Operations::default();
		match (self, val) {
			(Value::Object(a), Value::Object(b)) => {
				// Loop over old keys
				for (key, val) in a.value.iter() {
					if b.value.contains_key(key) == false {
						ops.0.push(Operation {
							op: String::from("remove"),
							prev: None,
							path: String::from(key),
							value: val.clone(),
						})
					}
				}
				// Loop over new keys
				for (key, val) in b.value.iter() {
					match a.value.contains_key(key) {
						true => ops.0.push(Operation {
							op: String::from("replace"),
							prev: None,
							path: String::from(key),
							value: val.clone(),
						}),
						false => ops.0.push(Operation {
							op: String::from("add"),
							prev: None,
							path: String::from(key),
							value: val.clone(),
						}),
					}
				}
				// Return operations
				ops
			}
			_ => unreachable!(),
		}
	}
}
