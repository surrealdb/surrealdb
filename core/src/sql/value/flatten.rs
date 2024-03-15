use crate::sql::array::Array;
use crate::sql::value::Value;

impl Value {
	pub fn flatten(self) -> Self {
		match self {
			Value::Array(v) => {
				v.0.into_iter()
					.flat_map(|v| match v {
						Value::Array(v) => v,
						_ => Array::from(v),
					})
					.collect::<Vec<_>>()
					.into()
			}
			v => v,
		}
	}
}
