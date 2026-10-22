#![deny(clippy::wildcard_enum_match_arm)]

use crate::val::{Array, Value};

impl Value {
	pub fn flatten(self) -> Self {
		let Value::Array(v) = self else {
			return self;
		};

		let mut res = Vec::with_capacity(v.len());

		for v in v {
			if let Value::Array(x) = v {
				for x in x {
					res.push(x);
				}
			} else {
				res.push(v);
			}
		}

		Value::Array(Array(res))
	}
}
