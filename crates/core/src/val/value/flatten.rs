use crate::val::{Array, Value};

impl Value {
	pub fn flatten(self) -> Self {
		match self {
		Value::Array(v) => {
			let mut res = Vec::with_capacity(v.len());

			for v in v {
					match v {
						Value::Array(x) => {
							for x in x {
								res.push(x);
							}
						}
						x => res.push(x),
					}
				}

				Value::Array(Array(res))
			}
			v => v,
		}
	}
}
