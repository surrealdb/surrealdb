use crate::sql::value::Value;

impl Value {
	pub fn single(&self) -> &Self {
		match self {
			Value::Array(v) => match v.first() {
				None => &Value::None,
				Some(v) => v,
			},
			v => v,
		}
	}
}
